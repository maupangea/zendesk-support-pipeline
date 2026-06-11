"""Business hours schedule and schedule holiday streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register

_SCHEDULES_ENDPOINT = "/api/v2/business_hours/schedules.json"


@register
class Schedule(AbstractStream):
    name = "schedule"
    endpoint = _SCHEDULES_ENDPOINT
    cursor_field = None
    primary_key = ["id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("time_zone", pa.string()),
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
        assert self.endpoint is not None
        for rec in client.paginate(self.endpoint):
            yield self._transformer.transform_record(rec)


@register
class ScheduleHoliday(AbstractStream):
    name = "schedule_holiday"
    endpoint = "/api/v2/business_hours/schedules/{id}/holidays.json"
    cursor_field = None
    primary_key = ["id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("schedule_id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("start_date", pa.string()),
                pa.field("end_date", pa.string()),
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
        for schedule in client.paginate(_SCHEDULES_ENDPOINT):
            schedule_id = schedule["id"]
            for holiday in client.paginate(self.endpoint.format(id=schedule_id)):
                holiday["schedule_id"] = schedule_id
                yield self._transformer.transform_record(holiday)
