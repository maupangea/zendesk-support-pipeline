"""Brand stream — Zendesk account brands."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class Brand(AbstractStream):
    name = "brand"
    endpoint = "/api/v2/brands.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("name", pa.string()),
                pa.field("brand_url", pa.string()),
                pa.field("subdomain", pa.string()),
                pa.field("host_mapping", pa.string()),
                pa.field("has_help_center", pa.bool_()),
                pa.field("help_center_state", pa.string()),
                pa.field("active", pa.bool_()),
                pa.field("default", pa.bool_()),
                pa.field("is_deleted", pa.bool_()),
                pa.field("signature_template", pa.string()),
                pa.field("logo_content_url", pa.string()),
                pa.field("created_at", pa.timestamp("us", tz="UTC")),
                pa.field("updated_at", pa.timestamp("us", tz="UTC")),
                pa.field("ticket_form_ids", pa.list_(pa.int64())),
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
        for rec in client.paginate(self.endpoint, params=None):
            yield self._transformer.transform_record(rec)
