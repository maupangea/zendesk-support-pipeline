from __future__ import annotations

import boto3
import pyarrow as pa
import pytest
from mypy_boto3_dynamodb.service_resource import Table

from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.sync_modes.base import SyncContext
from zendesk_ingestion.sync_modes.incremental_deduped import IncrementalDedupedSyncMode
from zendesk_ingestion.writers.s3 import S3Writer

SCHEMA = pa.schema([pa.field("id", pa.int64())])


@pytest.fixture
def ctx(mock_s3: str, mock_dynamo: Table) -> SyncContext:
    return SyncContext(
        connector_id="acme",
        stream_name="ticket",
        run_id="run-1",
        writer=S3Writer(bucket=mock_s3, prefix="zd"),
        state=StateManager(table_name="zendesk_ingestion_state"),
        schema=SCHEMA,
        batch_size_records=2,
    )


def test_writes_raw_and_deduped_paths(ctx: SyncContext, mock_s3: str) -> None:
    sync = IncrementalDedupedSyncMode()
    ctx.state.begin_run("acme", "ticket", "run-1", "incremental_append_deduped")
    total = sync.execute(iter([{"id": 1}, {"id": 2}, {"id": 3}]), ctx, final_cursor="c1")
    assert total == 3

    s3 = boto3.client("s3", region_name="us-east-1")
    data_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/ticket/data/").get("Contents", [])
    ]
    deduped_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/ticket/deduped/").get("Contents", [])
    ]
    staging_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/ticket/_staging/").get(
            "Contents", []
        )
    ]
    assert len(data_keys) >= 1
    assert len(deduped_keys) >= 1
    assert staging_keys == []

    state = ctx.state.get_state("acme", "ticket")
    assert state is not None
    assert state["cursor"] == "c1"
