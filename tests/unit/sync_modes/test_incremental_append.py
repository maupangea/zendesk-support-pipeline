from __future__ import annotations

import pyarrow as pa
import pytest
from mypy_boto3_dynamodb.service_resource import Table

from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.sync_modes.base import SyncContext
from zendesk_ingestion.sync_modes.incremental_append import IncrementalAppendSyncMode
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
        batch_size_records=3,
    )


def test_writes_records_and_commits_cursor(ctx: SyncContext, mock_s3: str) -> None:
    records = iter([{"id": i} for i in range(7)])
    sync = IncrementalAppendSyncMode()
    ctx.state.begin_run("acme", "ticket", "run-1", "incremental_append")
    total = sync.execute(records, ctx, final_cursor="cur-1")
    assert total == 7

    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=mock_s3, Prefix="zd/ticket/data/").get("Contents", [])
    ]
    assert len(keys) == 3  # batches of 3,3,1

    state = ctx.state.get_state("acme", "ticket")
    assert state is not None
    assert state["cursor"] == "cur-1"
    assert state["records_synced"] == 7


def test_no_cursor_commit_when_final_cursor_none(ctx: SyncContext) -> None:
    sync = IncrementalAppendSyncMode()
    ctx.state.begin_run("acme", "ticket", "run-1", "incremental_append")
    total = sync.execute(iter([{"id": 1}]), ctx, final_cursor=None)
    assert total == 1
    state = ctx.state.get_state("acme", "ticket")
    assert state is not None
    assert state["cursor"] is None
