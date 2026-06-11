"""User and user-derived streams."""

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
class User(AbstractStream):
    name = "user"
    endpoint = "/api/v2/incremental/users/cursor.json"
    cursor_field = "updated_at"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("external_id", pa.string()),
                pa.field("name", pa.string()),
                pa.field("email", pa.string()),
                pa.field("alias", pa.string()),
                pa.field("time_zone", pa.string()),
                pa.field("iana_time_zone", pa.string()),
                pa.field("phone", pa.string()),
                pa.field("shared_phone_number", pa.bool_()),
                pa.field("locale_id", pa.int64()),
                pa.field("locale", pa.string()),
                pa.field("organization_id", pa.int64()),
                pa.field("role", pa.string()),
                pa.field("role_type", pa.int64()),
                pa.field("verified", pa.bool_()),
                pa.field("active", pa.bool_()),
                pa.field("shared", pa.bool_()),
                pa.field("shared_agent", pa.bool_()),
                pa.field("last_login_at", _TS),
                pa.field("two_factor_auth_enabled", pa.bool_()),
                pa.field("signature", pa.string()),
                pa.field("details", pa.string()),
                pa.field("notes", pa.string()),
                pa.field("custom_role_id", pa.int64()),
                pa.field("moderator", pa.bool_()),
                pa.field("ticket_restriction", pa.string()),
                pa.field("only_private_comments", pa.bool_()),
                pa.field("restricted_agent", pa.bool_()),
                pa.field("suspended", pa.bool_()),
                pa.field("default_group_id", pa.int64()),
                pa.field("report_csv", pa.bool_()),
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
class UserTag(AbstractStream):
    name = "user_tag"
    parent_stream = "user"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    primary_key = ["_fivetran_id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("user_id", pa.int64()),
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
        for user in parent_records or []:
            for tag in user.get("tags", []):
                raw = {"user_id": user["id"], "tag": tag}
                self._transformer.add_synthetic_id(raw, "user_id", "tag")
                yield self._transformer.transform_record(raw)


@register
class UserField(AbstractStream):
    name = "user_field"
    endpoint = "/api/v2/user_fields.json"
    cursor_field = "updated_at"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    primary_key = ["id"]

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
class UserFieldOption(AbstractStream):
    name = "user_field_option"
    endpoint = "/api/v2/user_fields/{id}/options.json"
    cursor_field = None
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("user_field_id", pa.int64()),
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
        for field in client.paginate("/api/v2/user_fields.json"):
            for option in client.paginate(self.endpoint.format(id=field["id"])):
                option["user_field_id"] = field["id"]
                yield self._transformer.transform_record(option)


@register
class UserIdentity(AbstractStream):
    name = "user_identity"
    endpoint = "/api/v2/users/{id}/identities.json"
    cursor_field = "updated_at"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    parent_stream = "user"
    primary_key = ["id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("user_id", pa.int64()),
                pa.field("type", pa.string()),
                pa.field("value", pa.string()),
                pa.field("verified", pa.bool_()),
                pa.field("primary", pa.bool_()),
                pa.field("deliverable_state", pa.string()),
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
        for user in parent_records or []:
            for rec in client.paginate(self.endpoint.format(id=user["id"])):
                yield self._transformer.transform_record(rec)
