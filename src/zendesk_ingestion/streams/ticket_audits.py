"""Ticket audit stream and its derived child streams (email CCs, followers)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class TicketAudit(AbstractStream):
    name = "ticket_audit"
    endpoint = "/api/v2/incremental/ticket_events.json"
    cursor_field = "created_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_APPEND

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("ticket_id", pa.int64()),
                pa.field("timestamp", pa.int64()),
                pa.field("created_at", pa.timestamp("us", tz="UTC")),
                pa.field("updater_id", pa.int64()),
                pa.field("via_channel", pa.string()),
                pa.field("_fivetran_synced", pa.timestamp("us", tz="UTC")),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for rec in client.paginate_incremental(self.endpoint, cursor):
            yield self._transformer.transform_record(rec)


@register
class TicketEmailCc(AbstractStream):
    name = "ticket_email_cc"
    parent_stream = "ticket_audit"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.INCREMENTAL_APPEND

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ticket_id", pa.int64()),
                pa.field("user_id", pa.int64()),
                pa.field("created_at", pa.timestamp("us", tz="UTC")),
                pa.field("_fivetran_id", pa.string()),
                pa.field("_fivetran_synced", pa.timestamp("us", tz="UTC")),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for audit in parent_records or []:
            for child in audit.get("child_events") or audit.get("events") or []:
                if child.get("type") == "Cc" or child.get("field_name") == "cc":
                    raw = {
                        "ticket_id": audit.get("ticket_id"),
                        "user_id": child.get("user_id") or child.get("value"),
                        "created_at": audit.get("created_at"),
                    }
                    self._transformer.add_synthetic_id(raw, "ticket_id", "user_id")
                    yield self._transformer.transform_record(raw)


@register
class TicketFollower(AbstractStream):
    name = "ticket_follower"
    parent_stream = "ticket_audit"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.INCREMENTAL_APPEND

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ticket_id", pa.int64()),
                pa.field("user_id", pa.int64()),
                pa.field("created_at", pa.timestamp("us", tz="UTC")),
                pa.field("_fivetran_id", pa.string()),
                pa.field("_fivetran_synced", pa.timestamp("us", tz="UTC")),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for audit in parent_records or []:
            for child in audit.get("child_events") or audit.get("events") or []:
                if child.get("type") == "Follower" or child.get("field_name") == "follower":
                    raw = {
                        "ticket_id": audit.get("ticket_id"),
                        "user_id": child.get("user_id") or child.get("value"),
                        "created_at": audit.get("created_at"),
                    }
                    self._transformer.add_synthetic_id(raw, "ticket_id", "user_id")
                    yield self._transformer.transform_record(raw)
