"""Thin wrapper around CloudWatch PutMetricData.

All metrics are emitted under the 'ZendeskIngestion' namespace.
Dimensions: ConnectorId (added to every point) and Stream (optional).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import boto3
import structlog

log = structlog.get_logger()


@dataclass
class MetricPoint:
    name: str
    value: float
    unit: str = "Count"  # Count | Seconds | Bytes | None
    dimensions: dict[str, str] = field(default_factory=dict)


class MetricsClient:
    NAMESPACE = "ZendeskIngestion"

    def __init__(self, connector_id: str, region: str = "us-east-1") -> None:
        self._connector_id = connector_id
        # The cloudwatch boto3-stubs extra is not installed; treat the client as untyped.
        client_factory: Any = boto3.client
        self._cw = client_factory("cloudwatch", region_name=region)

    def emit(self, points: list[MetricPoint]) -> None:
        """Batch-emit metric data points to CloudWatch.

        Adds the ConnectorId dimension to every point. Errors are logged and
        suppressed — metrics must never fail a sync.
        """
        metric_data: list[dict[str, Any]] = []
        for p in points:
            dims: list[dict[str, str]] = [{"Name": "ConnectorId", "Value": self._connector_id}]
            for k, v in p.dimensions.items():
                dims.append({"Name": k, "Value": v})
            metric_data.append(
                {"MetricName": p.name, "Value": p.value, "Unit": p.unit, "Dimensions": dims}
            )
        try:
            # CloudWatch accepts at most 1000 metrics per PutMetricData call.
            for i in range(0, len(metric_data), 1000):
                chunk = metric_data[i : i + 1000]
                self._cw.put_metric_data(Namespace=self.NAMESPACE, MetricData=chunk)
        except Exception:
            log.warning("metrics_emit_failed", exc_info=True)

    def emit_stream_result(self, stream: str, records: int, duration_s: float, errors: int) -> None:
        self.emit(
            [
                MetricPoint("RecordsSynced", float(records), "Count", {"Stream": stream}),
                MetricPoint("SyncDurationSeconds", duration_s, "Seconds", {"Stream": stream}),
                MetricPoint("APIErrors", float(errors), "Count", {"Stream": stream}),
            ]
        )
