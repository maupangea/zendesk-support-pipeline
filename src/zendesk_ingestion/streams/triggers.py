"""Trigger stream and its derived action/condition child streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class Trigger(AbstractStream):
    name = "trigger"
    endpoint = "/api/v2/triggers.json"
    cursor_field = "updated_at"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("title", pa.string()),
                pa.field("active", pa.bool_()),
                pa.field("position", pa.int64()),
                pa.field("category_id", pa.string()),
                pa.field("description", pa.string()),
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
        for rec in client.paginate(self.endpoint, params={}):
            yield self._transformer.transform_record(rec)


@register
class TriggerAction(AbstractStream):
    name = "trigger_action"
    endpoint = None
    cursor_field = None
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE
    primary_key = ["_fivetran_id"]
    parent_stream = "trigger"

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("trigger_id", pa.int64()),
                pa.field("field", pa.string()),
                pa.field("value", pa.string()),
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
        for trig in parent_records or []:
            for action in trig.get("actions", []):
                raw = {
                    "trigger_id": trig["id"],
                    "field": action.get("field"),
                    "value": str(action.get("value")),
                }
                self._transformer.add_synthetic_id(raw, "trigger_id", "field", "value")
                yield self._transformer.transform_record(raw)


@register
class TriggerCondition(AbstractStream):
    name = "trigger_condition"
    endpoint = None
    cursor_field = None
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE
    primary_key = ["_fivetran_id"]
    parent_stream = "trigger"

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("trigger_id", pa.int64()),
                pa.field("field", pa.string()),
                pa.field("operator", pa.string()),
                pa.field("value", pa.string()),
                pa.field("condition_type", pa.string()),
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
        for trig in parent_records or []:
            conds = trig.get("conditions") or {}
            for ctype in ("all", "any"):
                for cond in conds.get(ctype) or []:
                    raw = {
                        "trigger_id": trig["id"],
                        "field": cond.get("field"),
                        "operator": cond.get("operator"),
                        "value": str(cond.get("value")),
                        "condition_type": ctype,
                    }
                    self._transformer.add_synthetic_id(
                        raw, "trigger_id", "condition_type", "field", "operator", "value"
                    )
                    yield self._transformer.transform_record(raw)
