"""Ticket metric event and aggregated ticket metric streams."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register
from zendesk_ingestion.transform.fivetran import FivetranTransformer


@register
class TicketMetricEvent(AbstractStream):
    name = "ticket_metric_event"
    endpoint = "/api/v2/incremental/ticket_metric_events.json"
    cursor_field = "time"
    default_sync_mode = SyncMode.INCREMENTAL_APPEND
    primary_key = ["id"]

    def __init__(self, transformer: FivetranTransformer) -> None:
        super().__init__(transformer)
        self._max_epoch: float | None = None

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("ticket_id", pa.int64()),
                pa.field("metric", pa.string()),
                pa.field("instance_id", pa.int64()),
                pa.field("type", pa.string()),
                pa.field("time", pa.timestamp("us", tz="UTC")),
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
        params: dict[str, Any] = {"start_time": int(cursor)} if cursor else {"start_time": 0}
        for rec in client.paginate(self.endpoint, params=params):
            time_value = rec.get("time")
            if isinstance(time_value, str):
                epoch = _iso_to_epoch(time_value)
                if self._max_epoch is None or epoch > self._max_epoch:
                    self._max_epoch = epoch
            yield self._transformer.transform_record(rec)

    def get_final_cursor(self, client: ZendeskClient) -> str | None:
        if self._max_epoch is None:
            return None
        return str(int(self._max_epoch))


@register
class TicketMetric(AbstractStream):
    name = "ticket_metric"
    parent_stream = "ticket_metric_event"
    default_sync_mode = SyncMode.INCREMENTAL_DEDUPED
    primary_key = ["ticket_id"]

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ticket_id", pa.int64()),
                pa.field("event_count", pa.int64()),
                pa.field("metrics", pa.string()),
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
        counts: dict[Any, int] = {}
        metrics: dict[Any, set[str]] = {}
        for rec in parent_records or []:
            ticket_id = rec.get("ticket_id")
            counts[ticket_id] = counts.get(ticket_id, 0) + 1
            metric_name = rec.get("metric")
            if metric_name is not None:
                metrics.setdefault(ticket_id, set()).add(str(metric_name))

        for ticket_id, count in counts.items():
            record = {
                "ticket_id": ticket_id,
                "event_count": count,
                "metrics": ",".join(sorted(metrics.get(ticket_id, set()))),
            }
            yield self._transformer.transform_record(record)


def _iso_to_epoch(value: str) -> float:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).timestamp()
