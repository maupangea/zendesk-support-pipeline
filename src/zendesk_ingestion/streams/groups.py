"""Group and group membership streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class Group(AbstractStream):
    name = "group"
    endpoint = "/api/v2/groups.json"
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
                pa.field("description", pa.string()),
                pa.field("default", pa.bool_()),
                pa.field("deleted", pa.bool_()),
                pa.field("is_public", pa.bool_()),
                pa.field("created_at", ts),
                pa.field("updated_at", ts),
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
class GroupMembership(AbstractStream):
    name = "group_membership"
    endpoint = "/api/v2/group_memberships.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        ts = pa.timestamp("us", tz="UTC")
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("user_id", pa.int64()),
                pa.field("group_id", pa.int64()),
                pa.field("default", pa.bool_()),
                pa.field("created_at", ts),
                pa.field("updated_at", ts),
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
