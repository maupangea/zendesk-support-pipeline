from __future__ import annotations

from typing import Any

import pytest

from zendesk_ingestion.metrics import MetricPoint, MetricsClient


class _RecordingCW:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class _RaisingCW:
    def put_metric_data(self, **kwargs: Any) -> None:
        raise RuntimeError("boom")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> MetricsClient:
    # Avoid constructing a real boto3 cloudwatch client.
    monkeypatch.setattr("boto3.client", lambda *a, **k: object())
    return MetricsClient("acme", region="us-east-1")


def test_emit_prepends_connector_id_dimension_to_every_point(client: MetricsClient) -> None:
    cw = _RecordingCW()
    client._cw = cw  # type: ignore[assignment]
    client.emit([MetricPoint("RecordsSynced", 5, "Count", {"Stream": "ticket"})])

    assert len(cw.calls) == 1
    md = cw.calls[0]["MetricData"]
    assert md[0]["MetricName"] == "RecordsSynced"
    dims = md[0]["Dimensions"]
    assert dims[0] == {"Name": "ConnectorId", "Value": "acme"}
    assert {"Name": "Stream", "Value": "ticket"} in dims


def test_emit_chunks_at_1000_points(client: MetricsClient) -> None:
    cw = _RecordingCW()
    client._cw = cw  # type: ignore[assignment]
    client.emit([MetricPoint(f"m{i}", float(i)) for i in range(1500)])
    assert len(cw.calls) == 2
    assert len(cw.calls[0]["MetricData"]) == 1000
    assert len(cw.calls[1]["MetricData"]) == 500


def test_emit_suppresses_client_errors(client: MetricsClient) -> None:
    client._cw = _RaisingCW()  # type: ignore[assignment]
    # Must not raise — metrics are best-effort.
    client.emit([MetricPoint("RecordsSynced", 1)])


def test_emit_stream_result_emits_three_named_metrics(client: MetricsClient) -> None:
    cw = _RecordingCW()
    client._cw = cw  # type: ignore[assignment]
    client.emit_stream_result("ticket", records=10, duration_s=2.5, errors=0)

    md = cw.calls[0]["MetricData"]
    names = {p["MetricName"]: p for p in md}
    assert set(names) == {"RecordsSynced", "SyncDurationSeconds", "APIErrors"}
    assert names["SyncDurationSeconds"]["Unit"] == "Seconds"
    assert names["RecordsSynced"]["Value"] == 10.0
