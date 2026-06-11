from __future__ import annotations

import boto3
import pyarrow as pa
import pytest
from mypy_boto3_dynamodb.service_resource import Table

from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.sync_modes.base import SyncContext
from zendesk_ingestion.sync_modes.full_refresh_append import FullRefreshAppendSyncMode
from zendesk_ingestion.writers.s3 import S3Writer

SCHEMA = pa.schema([pa.field("id", pa.int64())])


@pytest.fixture
def ctx(mock_s3: str, mock_dynamo: Table) -> SyncContext:
    return SyncContext(
        connector_id="acme",
        stream_name="brand",
        run_id="run-1",
        writer=S3Writer(bucket=mock_s3, prefix="zd"),
        state=StateManager(table_name="zendesk_ingestion_state"),
        schema=SCHEMA,
        batch_size_records=2,
    )


def test_writes_partitioned_and_updates_last_sync(ctx: SyncContext, mock_s3: str) -> None:
    sync = FullRefreshAppendSyncMode()
    ctx.state.begin_run("acme", "brand", "run-1", "full_refresh_append")
    total = sync.execute(iter([{"id": i} for i in range(3)]), ctx, final_cursor=None)
    assert total == 3

    s3 = boto3.client("s3", region_name="us-east-1")
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/brand/data/").get("Contents", [])
    ]
    assert len(keys) == 2  # batches of 2, 1

    state = ctx.state.get_state("acme", "brand")
    assert state is not None
    assert state["last_sync_at"] is not None
