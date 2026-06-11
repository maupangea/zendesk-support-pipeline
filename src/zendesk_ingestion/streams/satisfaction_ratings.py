"""Satisfaction ratings stream."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class SatisfactionRating(AbstractStream):
    name = "satisfaction_rating"
    endpoint = "/api/v2/satisfaction_ratings.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("assignee_id", pa.int64()),
                pa.field("group_id", pa.int64()),
                pa.field("requester_id", pa.int64()),
                pa.field("ticket_id", pa.int64()),
                pa.field("score", pa.string()),
                pa.field("comment", pa.string()),
                pa.field("reason", pa.string()),
                pa.field("reason_id", pa.int64()),
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
        params = {"start_time": int(cursor)} if cursor else None
        for rec in client.paginate(self.endpoint, params=params):
            yield self._transformer.transform_record(rec)
