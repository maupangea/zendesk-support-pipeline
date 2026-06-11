"""Organization streams: organizations and their derived tag, field, and member rows."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register

_TS = pa.timestamp("us", tz="UTC")


@register
class Organization(AbstractStream):
    name = "organization"
    endpoint = "/api/v2/incremental/organizations/cursor.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("external_id", pa.string()),
                pa.field("name", pa.string()),
                pa.field("group_id", pa.int64()),
                pa.field("shared_tickets", pa.bool_()),
                pa.field("shared_comments", pa.bool_()),
                pa.field("details", pa.string()),
                pa.field("notes", pa.string()),
                pa.field("domain_names", pa.list_(pa.string())),
                pa.field("created_at", _TS),
                pa.field("updated_at", _TS),
                pa.field("_fivetran_synced", _TS),
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
class OrganizationTag(AbstractStream):
    name = "organization_tag"
    parent_stream = "organization"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("organization_id", pa.int64()),
                pa.field("tag", pa.string()),
                pa.field("_fivetran_id", pa.string()),
                pa.field("_fivetran_synced", _TS),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for org in parent_records or []:
            for tag in org.get("tags", []):
                raw = {"organization_id": org["id"], "tag": tag}
                self._transformer.add_synthetic_id(raw, "organization_id", "tag")
                yield self._transformer.transform_record(raw)


@register
class OrganizationField(AbstractStream):
    name = "organization_field"
    endpoint = "/api/v2/organization_fields.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("type", pa.string()),
                pa.field("key", pa.string()),
                pa.field("title", pa.string()),
                pa.field("raw_title", pa.string()),
                pa.field("description", pa.string()),
                pa.field("raw_description", pa.string()),
                pa.field("position", pa.int64()),
                pa.field("active", pa.bool_()),
                pa.field("system", pa.bool_()),
                pa.field("regexp_for_validation", pa.string()),
                pa.field("tag", pa.string()),
                pa.field("created_at", _TS),
                pa.field("updated_at", _TS),
                pa.field("_fivetran_synced", _TS),
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
class OrganizationFieldOption(AbstractStream):
    name = "organization_field_option"
    endpoint = "/api/v2/organization_fields/{id}/options.json"
    cursor_field = None
    primary_key = ["id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("organization_field_id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("raw_name", pa.string()),
                pa.field("value", pa.string()),
                pa.field("position", pa.int64()),
                pa.field("default", pa.bool_()),
                pa.field("_fivetran_synced", _TS),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for field in client.paginate("/api/v2/organization_fields.json"):
            path = self.endpoint.format(id=field["id"])
            for option in client.paginate(path):
                option["organization_field_id"] = field["id"]
                yield self._transformer.transform_record(option)


@register
class OrganizationMember(AbstractStream):
    name = "organization_member"
    endpoint = "/api/v2/organizations/{id}/users.json"
    parent_stream = "organization"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("organization_id", pa.int64()),
                pa.field("user_id", pa.int64()),
                pa.field("_fivetran_id", pa.string()),
                pa.field("_fivetran_synced", _TS),
                pa.field("_fivetran_deleted", pa.bool_()),
            ]
        )

    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for org in parent_records or []:
            path = self.endpoint.format(id=org["id"])
            for user in client.paginate(path):
                raw = {"organization_id": org["id"], "user_id": user.get("id")}
                self._transformer.add_synthetic_id(raw, "organization_id", "user_id")
                yield self._transformer.transform_record(raw)
