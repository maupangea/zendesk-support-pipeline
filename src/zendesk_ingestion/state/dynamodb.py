"""DynamoDB-backed state manager for stream cursors."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import boto3
import structlog
from botocore.exceptions import ClientError

from zendesk_ingestion.exceptions import StateConflictError

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table

log = structlog.get_logger()


class StreamState(TypedDict):
    connector_id: str
    stream_name: str
    cursor: str | None
    last_sync_at: str | None  # ISO 8601
    last_run_id: str | None
    records_synced: int
    sync_mode: str
    status: Literal["success", "in_progress", "failed"]


class StateManager:
    """boto3 resource objects are not thread-safe and the orchestrator shares one
    StateManager across a ThreadPoolExecutor, so each worker thread lazily gets its
    own Table resource via thread-local storage.
    """

    def __init__(self, table_name: str, region: str = "us-east-1") -> None:
        self._table_name = table_name
        self._region = region
        self._local = threading.local()

    @property
    def _table(self) -> Table:
        table: Table | None = getattr(self._local, "table", None)
        if table is None:
            table = boto3.resource("dynamodb", region_name=self._region).Table(self._table_name)
            self._local.table = table
        return table

    def get_state(self, connector_id: str, stream_name: str) -> StreamState | None:
        """Return current state, or None if the stream has never been synced."""
        resp = self._table.get_item(Key={"connector_id": connector_id, "stream_name": stream_name})
        item = resp.get("Item")
        if item is None:
            return None
        return _item_to_state(item)

    def begin_run(
        self,
        connector_id: str,
        stream_name: str,
        run_id: str,
        sync_mode: str,
    ) -> None:
        """Mark run as in_progress. Unconditional write."""
        self._table.update_item(
            Key={"connector_id": connector_id, "stream_name": stream_name},
            UpdateExpression=(
                "SET #status = :status, last_run_id = :run_id, sync_mode = :sync_mode"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "in_progress",
                ":run_id": run_id,
                ":sync_mode": sync_mode,
            },
        )

    def commit_cursor(
        self,
        connector_id: str,
        stream_name: str,
        run_id: str,
        new_cursor: str,
        records_synced: int,
    ) -> None:
        """
        Advance cursor on successful completion.
        Uses ConditionExpression to ensure monotonic cursor advancement.
        """
        now = datetime.now(tz=UTC).isoformat()
        try:
            self._table.update_item(
                Key={"connector_id": connector_id, "stream_name": stream_name},
                UpdateExpression=(
                    "SET #cursor = :new_cursor, "
                    "last_sync_at = :last_sync_at, "
                    "last_run_id = :run_id, "
                    "records_synced = :records_synced, "
                    "#status = :status"
                ),
                ConditionExpression=(
                    "attribute_not_exists(#cursor) "
                    "OR #cursor = :null_cursor "
                    "OR #cursor < :new_cursor"
                ),
                ExpressionAttributeNames={
                    "#status": "status",
                    "#cursor": "cursor",
                },
                ExpressionAttributeValues={
                    ":new_cursor": new_cursor,
                    ":null_cursor": None,
                    ":last_sync_at": now,
                    ":run_id": run_id,
                    ":records_synced": records_synced,
                    ":status": "success",
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise StateConflictError(
                    f"Stale cursor write for {connector_id}/{stream_name}: "
                    f"attempted to set cursor={new_cursor}"
                ) from exc
            raise

    def reset_cursor(self, connector_id: str, stream_name: str) -> None:
        """Delete stored state so the next sync starts from scratch (full re-sync)."""
        self._table.delete_item(
            Key={"connector_id": connector_id, "stream_name": stream_name},
        )

    def mark_failed(
        self,
        connector_id: str,
        stream_name: str,
        run_id: str,
    ) -> None:
        """Set status=failed without advancing cursor."""
        self._table.update_item(
            Key={"connector_id": connector_id, "stream_name": stream_name},
            UpdateExpression="SET #status = :status, last_run_id = :run_id",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "failed",
                ":run_id": run_id,
            },
        )


def _item_to_state(item: dict[str, Any]) -> StreamState:
    return StreamState(
        connector_id=item["connector_id"],
        stream_name=item["stream_name"],
        cursor=item.get("cursor"),
        last_sync_at=item.get("last_sync_at"),
        last_run_id=item.get("last_run_id"),
        records_synced=int(item.get("records_synced", 0)),
        sync_mode=item.get("sync_mode", ""),
        status=item.get("status", "success"),
    )
