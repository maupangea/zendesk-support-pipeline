"""SLA policy stream and its derived filter/condition child streams."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.streams.registry import register


@register
class SlaPolicy(AbstractStream):
    name = "sla_policy"
    endpoint = "/api/v2/slas/policies.json"
    cursor_field = None
    primary_key = ["id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("url", pa.string()),
                pa.field("title", pa.string()),
                pa.field("description", pa.string()),
                pa.field("position", pa.int64()),
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
class SlaPolicyFilter(AbstractStream):
    name = "sla_policy_filter"
    parent_stream = "sla_policy"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("sla_policy_id", pa.int64()),
                pa.field("field", pa.string()),
                pa.field("operator", pa.string()),
                pa.field("value", pa.string()),
                pa.field("filter_type", pa.string()),
                pa.field("_fivetran_id", pa.string()),
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
        for sla in parent_records or []:
            filt = sla.get("filter") or {}
            for ftype in ("all", "any"):
                for cond in filt.get(ftype) or []:
                    raw: dict[str, Any] = {
                        "sla_policy_id": sla["id"],
                        "field": cond.get("field"),
                        "operator": cond.get("operator"),
                        "value": str(cond.get("value")),
                        "filter_type": ftype,
                    }
                    self._transformer.add_synthetic_id(
                        raw, "sla_policy_id", "filter_type", "field", "operator", "value"
                    )
                    yield self._transformer.transform_record(raw)


@register
class SlaPolicyCondition(AbstractStream):
    name = "sla_policy_condition"
    parent_stream = "sla_policy"
    primary_key = ["_fivetran_id"]
    default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE

    def get_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("sla_policy_id", pa.int64()),
                pa.field("priority", pa.string()),
                pa.field("metric", pa.string()),
                pa.field("target", pa.int64()),
                pa.field("business_hours", pa.bool_()),
                pa.field("_fivetran_id", pa.string()),
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
        for sla in parent_records or []:
            for metric in sla.get("policy_metrics", []):
                raw: dict[str, Any] = {
                    "sla_policy_id": sla["id"],
                    "priority": metric.get("priority"),
                    "metric": metric.get("metric"),
                    "target": metric.get("target"),
                    "business_hours": metric.get("business_hours"),
                }
                self._transformer.add_synthetic_id(raw, "sla_policy_id", "priority", "metric")
                yield self._transformer.transform_record(raw)
