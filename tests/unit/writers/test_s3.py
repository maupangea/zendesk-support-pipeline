from __future__ import annotations

import boto3
import pyarrow as pa
import pytest

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.writers.s3 import S3Writer

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ]
)


def _writer(bucket: str) -> S3Writer:
    return S3Writer(bucket=bucket, prefix="zendesk_support", region="us-east-1")


def _list_keys(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3", region_name="us-east-1")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", [])]


def test_write_batch_incremental_append_correct_path(mock_s3: str) -> None:
    w = _writer(mock_s3)
    result = w.write_batch(
        records=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        schema=SCHEMA,
        stream_name="ticket",
        run_id="run-1",
        sync_mode=SyncMode.INCREMENTAL_APPEND,
        shard=0,
        partition={"year": "2024", "month": "01", "day": "15"},
    )
    assert result.records_written == 2
    expected = "zendesk_support/ticket/data/year=2024/month=01/day=15/ticket_run-1_0.parquet"
    keys = _list_keys(mock_s3, "zendesk_support/")
    assert expected in keys


def test_write_batch_full_refresh_overwrite_writes_to_staging(mock_s3: str) -> None:
    w = _writer(mock_s3)
    w.write_batch(
        records=[{"id": 1, "name": "a"}],
        schema=SCHEMA,
        stream_name="ticket",
        run_id="run-1",
        sync_mode=SyncMode.FULL_REFRESH_OVERWRITE,
        shard=0,
    )
    keys = _list_keys(mock_s3, "zendesk_support/")
    assert keys == [
        "zendesk_support/ticket/_staging/run-1/ticket_0.parquet",
    ]


def test_commit_staging_promotes_to_final_path(mock_s3: str) -> None:
    w = _writer(mock_s3)
    w.write_batch(
        records=[{"id": 1, "name": "a"}],
        schema=SCHEMA,
        stream_name="ticket",
        run_id="run-1",
        sync_mode=SyncMode.FULL_REFRESH_OVERWRITE,
        shard=0,
    )
    w.commit_staging(stream_name="ticket", run_id="run-1")
    keys = _list_keys(mock_s3, "zendesk_support/")
    assert keys == ["zendesk_support/ticket/data/ticket_0.parquet"]


def test_commit_staging_deletes_staging_objects(mock_s3: str) -> None:
    w = _writer(mock_s3)
    w.write_batch(
        records=[{"id": 1, "name": "a"}],
        schema=SCHEMA,
        stream_name="ticket",
        run_id="run-1",
        sync_mode=SyncMode.FULL_REFRESH_OVERWRITE,
        shard=0,
    )
    w.commit_staging(stream_name="ticket", run_id="run-1")
    staging_keys = _list_keys(mock_s3, "zendesk_support/ticket/_staging/")
    assert staging_keys == []


def test_delete_stream_prefix_removes_all_data_objects(mock_s3: str) -> None:
    w = _writer(mock_s3)
    for shard in range(3):
        w.write_batch(
            records=[{"id": shard, "name": "x"}],
            schema=SCHEMA,
            stream_name="ticket",
            run_id="run-1",
            sync_mode=SyncMode.INCREMENTAL_APPEND,
            shard=shard,
            partition={"year": "2024", "month": "01", "day": "15"},
        )
    assert len(_list_keys(mock_s3, "zendesk_support/ticket/data/")) == 3
    w.delete_stream_prefix("ticket")
    assert _list_keys(mock_s3, "zendesk_support/ticket/data/") == []


def test_parquet_schema_enforced(mock_s3: str) -> None:
    w = _writer(mock_s3)
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError, TypeError)):
        w.write_batch(
            records=[{"id": "not-an-int", "name": "a"}],
            schema=SCHEMA,
            stream_name="ticket",
            run_id="run-1",
            sync_mode=SyncMode.INCREMENTAL_APPEND,
            partition={"year": "2024", "month": "01", "day": "15"},
        )


def test_commit_staging_promotes_deduped_to_deduped_path(mock_s3: str) -> None:
    w = _writer(mock_s3)
    # Write a raw incremental record (data/...) and a deduped staged record.
    w.write_batch(
        records=[{"id": 1, "name": "a"}],
        schema=SCHEMA,
        stream_name="ticket",
        run_id="run-1",
        sync_mode=SyncMode.INCREMENTAL_DEDUPED,
        partition={"year": "2024", "month": "01", "day": "15"},
    )
    w.write_deduped_staging(
        records=[{"id": 1, "name": "a"}],
        schema=SCHEMA,
        stream_name="ticket",
        run_id="run-1",
        shard=0,
    )
    w.commit_staging(stream_name="ticket", run_id="run-1")
    deduped_keys = _list_keys(mock_s3, "zendesk_support/ticket/deduped/")
    assert deduped_keys == ["zendesk_support/ticket/deduped/ticket_0.parquet"]
