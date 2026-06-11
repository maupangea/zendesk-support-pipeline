"""Macro and macro attachment streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class Macro(AbstractStream):
    name = "macro"
    endpoint = "/api/v2/macros.json"
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
                pa.field("description", pa.string()),
                pa.field("position", pa.int64()),
                pa.field("restriction_type", pa.string()),
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
class MacroAttachment(AbstractStream):
    name = "macro_attachment"
    endpoint = "/api/v2/macros/{id}/attachments.json"
    cursor_field = None
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("macro_id", pa.int64()),
                pa.field("filename", pa.string()),
                pa.field("content_type", pa.string()),
                pa.field("content_url", pa.string()),
                pa.field("size", pa.int64()),
                pa.field("created_at", pa.timestamp("us", tz="UTC")),
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
        for macro in client.paginate("/api/v2/macros.json"):
            macro_id = macro["id"]
            for attachment in client.paginate(self.endpoint.format(id=macro_id)):
                attachment["macro_id"] = macro_id
                yield self._transformer.transform_record(attachment)
