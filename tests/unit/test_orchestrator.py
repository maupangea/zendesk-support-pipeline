from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from mypy_boto3_dynamodb.service_resource import Table

from zendesk_ingestion import orchestrator
from zendesk_ingestion.config.models import (
    ConnectorConfig,
    RuntimeConfig,
    S3Config,
    StateConfig,
    StreamConfig,
    SyncMode,
)
from zendesk_ingestion.exceptions import ZendeskAPIError

_RAW_TICKETS = [
    {"id": 1, "tags": ["alpha", "beta"], "updated_at": "2024-01-01T00:00:00Z"},
    {"id": 2, "tags": ["alpha"], "updated_at": "2024-01-02T00:00:00Z"},
]


class FakeClient:
    """Stand-in for ZendeskClient: yields canned ticket records, tracks its cursor."""

    def __init__(self, **_: Any) -> None:
        self.last_cursor: str | None = None

    def paginate_incremental(
        self, path: str, cursor: str | None = None
    ) -> Iterator[dict[str, Any]]:
        for rec in _RAW_TICKETS:
            self.last_cursor = f"cursor-{rec['id']}"
            yield dict(rec)

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        return iter(())

    def close(self) -> None:
        pass


class FailingClient(FakeClient):
    def paginate_incremental(
        self, path: str, cursor: str | None = None
    ) -> Iterator[dict[str, Any]]:
        raise ZendeskAPIError(500, "exploded")
        yield  # pragma: no cover  (makes this a generator)


def _config(bucket: str, table: str, streams: dict[str, StreamConfig]) -> ConnectorConfig:
    return ConnectorConfig(
        connector_id="acme",
        s3=S3Config(bucket=bucket, prefix="zd", region="us-east-1"),
        state=StateConfig(dynamodb_table=table, region="us-east-1"),
        runtime=RuntimeConfig(max_parallelism=4, batch_size_records=100),
        streams=streams,
        zendesk_subdomain="sub",
        zendesk_email="e@x.com",
        zendesk_api_token="tok",
    )


def _keys(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3", region_name="us-east-1")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", [])]


# ----- plan_streams / _resolve_streams (pure logic) -----------------------


def test_plan_streams_splits_sources_and_derived() -> None:
    cfg = _config(
        "b",
        "t",
        {
            "ticket": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
            "ticket_tag": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
        },
    )
    plan = orchestrator.plan_streams(cfg, ["ticket", "ticket_tag"])
    assert plan.wave1 == ["ticket"]
    assert plan.wave2 == ["ticket_tag"]
    assert plan.auto_parents == []


def test_plan_streams_auto_includes_missing_parent_as_cache_only() -> None:
    cfg = _config("b", "t", {"ticket_tag": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED)})
    plan = orchestrator.plan_streams(cfg, ["ticket_tag"])
    assert plan.wave1 == ["ticket"]  # auto-added
    assert plan.wave2 == ["ticket_tag"]
    assert plan.auto_parents == ["ticket"]


def test_resolve_streams_unknown_raises() -> None:
    cfg = _config("b", "t", {"ticket": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED)})
    with pytest.raises(ValueError, match="Unknown streams"):
        orchestrator.plan_streams(cfg, ["ticket", "not_a_stream"])


def test_resolve_streams_drops_disabled() -> None:
    cfg = _config(
        "b",
        "t",
        {
            "ticket": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
            "user": StreamConfig(enabled=False, sync_mode=SyncMode.INCREMENTAL_DEDUPED),
        },
    )
    plan = orchestrator.plan_streams(cfg, None)
    assert plan.requested == ["ticket"]


# ----- run() end-to-end with moto + fake client ---------------------------


def test_run_parent_and_derived_share_records(
    mock_s3: str, mock_dynamo: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(orchestrator, "ZendeskClient", FakeClient)
    cfg = _config(
        mock_s3,
        "zendesk_ingestion_state",
        {
            "ticket": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
            "ticket_tag": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
        },
    )
    report = orchestrator.run(cfg, ["ticket", "ticket_tag"])

    assert report.success
    # 2 tickets + 3 tags (ticket 1 has 2 tags, ticket 2 has 1)
    assert report.total_records == 5
    assert _keys(mock_s3, "zd/ticket/data/")
    assert _keys(mock_s3, "zd/ticket_tag/data/")

    # The parent committed its post-pagination cursor (not a stale one).
    state = orchestrator.StateManager("zendesk_ingestion_state").get_state("acme", "ticket")
    assert state is not None
    assert state["cursor"] == "cursor-2"


def test_run_auto_parent_is_cache_only_not_written(
    mock_s3: str, mock_dynamo: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(orchestrator, "ZendeskClient", FakeClient)
    cfg = _config(
        mock_s3,
        "zendesk_ingestion_state",
        {"ticket_tag": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED)},
    )
    report = orchestrator.run(cfg, ["ticket_tag"])

    assert report.success
    assert _keys(mock_s3, "zd/ticket_tag/data/")  # child written
    assert not _keys(mock_s3, "zd/ticket/data/")  # parent fetched cache-only

    by_name = {r.stream_name: r for r in report.results}
    assert by_name["ticket"].cache_only is True
    assert by_name["ticket"].records_written == 0
    assert by_name["ticket_tag"].records_written == 3


def test_run_marks_failed_stream_and_reports_failure(
    mock_s3: str, mock_dynamo: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(orchestrator, "ZendeskClient", FailingClient)
    cfg = _config(
        mock_s3,
        "zendesk_ingestion_state",
        {"ticket": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED)},
    )
    report = orchestrator.run(cfg, ["ticket"])

    assert not report.success
    assert report.failed_streams == ["ticket"]
    state = orchestrator.StateManager("zendesk_ingestion_state").get_state("acme", "ticket")
    assert state is not None
    assert state["status"] == "failed"
