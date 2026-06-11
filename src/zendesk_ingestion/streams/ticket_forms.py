"""Ticket form and ticket form condition streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class TicketForm(AbstractStream):
    name = "ticket_form"
    endpoint = "/api/v2/ticket_forms.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        ts = pa.timestamp("us", tz="UTC")
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("name", pa.string()),
                pa.field("raw_name", pa.string()),
                pa.field("display_name", pa.string()),
                pa.field("raw_display_name", pa.string()),
                pa.field("position", pa.int64()),
                pa.field("active", pa.bool_()),
                pa.field("default", pa.bool_()),
                pa.field("in_all_brands", pa.bool_()),
                pa.field("end_user_visible", pa.bool_()),
                pa.field("created_at", ts),
                pa.field("updated_at", ts),
                pa.field("ticket_field_ids", pa.list_(pa.int64())),
                pa.field("_fivetran_synced", ts),
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
class TicketFormCondition(AbstractStream):
    name = "ticket_form_condition"
    parent_stream = "ticket_form"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        ts = pa.timestamp("us", tz="UTC")
        return pa.schema(
            [
                pa.field("ticket_form_id", pa.int64()),
                pa.field("parent_field_id", pa.int64()),
                pa.field("value", pa.string()),
                pa.field("_fivetran_id", pa.string()),
                pa.field("_fivetran_synced", ts),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for form in parent_records or []:
            conditions = (form.get("agent_conditions") or []) + (
                form.get("end_user_conditions") or []
            )
            for cond in conditions:
                raw: dict[str, Any] = {
                    "ticket_form_id": form.get("id"),
                    "parent_field_id": cond.get("parent_field_id"),
                    "value": str(cond.get("value")),
                }
                self._transformer.add_synthetic_id(
                    raw, "ticket_form_id", "parent_field_id", "value"
                )
                yield self._transformer.transform_record(raw)
