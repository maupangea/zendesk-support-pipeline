"""Zendesk tags stream."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class Tag(AbstractStream):
    name = "tag"
    endpoint = "/api/v2/tags.json"
    cursor_field = None
    primary_key = ["name"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("name", pa.string()),
                pa.field("count", pa.int64()),
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
        assert self.endpoint is not None
        for rec in client.paginate(self.endpoint):
            yield self._transformer.transform_record(rec)
