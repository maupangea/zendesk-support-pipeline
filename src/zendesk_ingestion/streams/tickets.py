"""Ticket stream and its derived streams (tags, comments, comment attachments)."""

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
class Ticket(AbstractStream):
    name = "ticket"
    endpoint = "/api/v2/incremental/tickets/cursor.json"
    cursor_field = "updated_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("external_id", pa.string()),
                pa.field("type", pa.string()),
                pa.field("subject", pa.string()),
                pa.field("raw_subject", pa.string()),
                pa.field("description", pa.string()),
                pa.field("priority", pa.string()),
                pa.field("status", pa.string()),
                pa.field("recipient", pa.string()),
                pa.field("requester_id", pa.int64()),
                pa.field("submitter_id", pa.int64()),
                pa.field("assignee_id", pa.int64()),
                pa.field("organization_id", pa.int64()),
                pa.field("group_id", pa.int64()),
                pa.field("brand_id", pa.int64()),
                pa.field("forum_topic_id", pa.int64()),
                pa.field("problem_id", pa.int64()),
                pa.field("has_incidents", pa.bool_()),
                pa.field("is_public", pa.bool_()),
                pa.field("due_at", _TS),
                pa.field("created_at", _TS),
                pa.field("updated_at", _TS),
                pa.field("via_channel", pa.string()),
                pa.field("via_source_from_address", pa.string()),
                pa.field("via_source_from_name", pa.string()),
                pa.field("via_source_to_address", pa.string()),
                pa.field("via_source_to_name", pa.string()),
                pa.field("via_source_rel", pa.string()),
                pa.field("ticket_form_id", pa.int64()),
                pa.field("allow_channelback", pa.bool_()),
                pa.field("allow_attachments", pa.bool_()),
                pa.field("generated_timestamp", pa.int64()),
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
class TicketTag(AbstractStream):
    name = "ticket_tag"
    parent_stream = "ticket"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ticket_id", pa.int64()),
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
        for ticket in parent_records or []:
            for tag in ticket.get("tags", []):
                raw: dict[str, Any] = {"ticket_id": ticket["id"], "tag": tag}
                self._transformer.add_synthetic_id(raw, "ticket_id", "tag")
                yield self._transformer.transform_record(raw)


@register
class TicketComment(AbstractStream):
    name = "ticket_comment"
    endpoint = "/api/v2/incremental/ticket_events.json"
    cursor_field = "created_at"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_APPEND

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("ticket_id", pa.int64()),
                pa.field("type", pa.string()),
                pa.field("author_id", pa.int64()),
                pa.field("body", pa.string()),
                pa.field("html_body", pa.string()),
                pa.field("plain_body", pa.string()),
                pa.field("public", pa.bool_()),
                pa.field("created_at", _TS),
                pa.field("via_channel", pa.string()),
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
        for event in client.paginate_incremental(self.endpoint, cursor):
            child_events = event.get("child_events", [])
            if event.get("event_type") == "Comment":
                yield self._transformer.transform_record(event)
                continue
            for child in child_events:
                if child.get("event_type") == "Comment":
                    yield self._transformer.transform_record(child)


@register
class TicketCommentAttachment(AbstractStream):
    name = "ticket_comment_attachment"
    parent_stream = "ticket_comment"
    primary_key = ["id"]
    default_sync_mode = SyncMode.INCREMENTAL_APPEND

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("comment_id", pa.int64()),
                pa.field("file_name", pa.string()),
                pa.field("content_type", pa.string()),
                pa.field("content_url", pa.string()),
                pa.field("size", pa.int64()),
                pa.field("inline", pa.bool_()),
                pa.field("height", pa.int64()),
                pa.field("width", pa.int64()),
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
        for comment in parent_records or []:
            for attachment in comment.get("attachments", []):
                raw = dict(attachment)
                raw["comment_id"] = comment.get("id")
                yield self._transformer.transform_record(raw)
