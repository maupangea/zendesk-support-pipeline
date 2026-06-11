"""Ticket field and ticket field option streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register

_TICKET_FIELDS_ENDPOINT = "/api/v2/ticket_fields.json"


@register
class TicketField(AbstractStream):
    name = "ticket_field"
    endpoint = _TICKET_FIELDS_ENDPOINT
    cursor_field = "updated_at"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("type", pa.string()),
                pa.field("title", pa.string()),
                pa.field("raw_title", pa.string()),
                pa.field("description", pa.string()),
                pa.field("raw_description", pa.string()),
                pa.field("position", pa.int64()),
                pa.field("active", pa.bool_()),
                pa.field("required", pa.bool_()),
                pa.field("collapsed_for_agents", pa.bool_()),
                pa.field("regexp_for_validation", pa.string()),
                pa.field("title_in_portal", pa.string()),
                pa.field("visible_in_portal", pa.bool_()),
                pa.field("editable_in_portal", pa.bool_()),
                pa.field("required_in_portal", pa.bool_()),
                pa.field("tag", pa.string()),
                pa.field("removable", pa.bool_()),
                pa.field("agent_description", pa.string()),
                pa.field("created_at", pa.timestamp("us", tz="UTC")),
                pa.field("updated_at", pa.timestamp("us", tz="UTC")),
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
        for rec in client.paginate(self.endpoint):
            yield self._transformer.transform_record(rec)


@register
class TicketFieldOption(AbstractStream):
    name = "ticket_field_option"
    endpoint = None
    cursor_field = None
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("ticket_field_id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("raw_name", pa.string()),
                pa.field("value", pa.string()),
                pa.field("position", pa.int64()),
                pa.field("default", pa.bool_()),
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
        for field in client.paginate(_TICKET_FIELDS_ENDPOINT):
            for option in field.get("custom_field_options") or []:
                raw: dict[str, Any] = {
                    "id": option.get("id"),
                    "ticket_field_id": field.get("id"),
                    "name": option.get("name"),
                    "raw_name": option.get("raw_name"),
                    "value": option.get("value"),
                    "position": option.get("position"),
                    "default": option.get("default"),
                }
                yield self._transformer.transform_record(raw)
