from __future__ import annotations

import boto3
import pyarrow as pa
import pytest
from mypy_boto3_dynamodb.service_resource import Table

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.sync_modes.base import SyncContext
from zendesk_ingestion.sync_modes.full_refresh_overwrite import (
    FullRefreshOverwriteSyncMode,
)
from zendesk_ingestion.writers.s3 import S3Writer

SCHEMA = pa.schema([pa.field("id", pa.int64())])


@pytest.fixture
def ctx(mock_s3: str, mock_dynamo: Table) -> SyncContext:
    return SyncContext(
        connector_id="acme",
        stream_name="tag",
        run_id="run-2",
        writer=S3Writer(bucket=mock_s3, prefix="zd"),
        state=StateManager(table_name="zendesk_ingestion_state"),
        schema=SCHEMA,
        batch_size_records=2,
    )


def test_overwrite_replaces_existing_data(ctx: SyncContext, mock_s3: str) -> None:
    # Seed with stale data using INCREMENTAL_APPEND with same prefix.
    stale_writer = S3Writer(bucket=mock_s3, prefix="zd")
    stale_writer.write_batch(
        records=[{"id": 99}],
        schema=SCHEMA,
        stream_name="tag",
        run_id="run-old",
        sync_mode=SyncMode.INCREMENTAL_APPEND,
        partition={"year": "2024", "month": "01", "day": "01"},
    )
    s3 = boto3.client("s3", region_name="us-east-1")
    pre_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/tag/data/").get("Contents", [])
    ]
    assert len(pre_keys) == 1

    sync = FullRefreshOverwriteSyncMode()
    ctx.state.begin_run("acme", "tag", "run-2", "full_refresh_overwrite")
    total = sync.execute(iter([{"id": i} for i in range(3)]), ctx, final_cursor=None)
    assert total == 3

    final_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/tag/data/").get("Contents", [])
    ]
    # New shards present, stale partition removed
    assert all("year=" not in k for k in final_keys)
    assert len(final_keys) == 2  # batches of 2, 1
