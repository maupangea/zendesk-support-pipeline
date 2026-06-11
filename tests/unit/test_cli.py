from __future__ import annotations

import boto3
import pytest
from mypy_boto3_dynamodb.service_resource import Table
from typer.testing import CliRunner

from zendesk_ingestion import orchestrator
from zendesk_ingestion.cli import app
from zendesk_ingestion.orchestrator import RunReport, StreamResult

runner = CliRunner()

CONFIG = "config/connector.yaml"


# ----- sync --------------------------------------------------------------


def test_sync_dry_run_prints_plan_and_exits_zero() -> None:
    result = runner.invoke(app, ["sync", "--dry-run", "--no-resolve-secrets", "--config", CONFIG])
    assert result.exit_code == 0
    assert "Stream plan" in result.stdout
    assert "wave 1" in result.stdout


def test_sync_unknown_stream_exits_two() -> None:
    result = runner.invoke(
        app, ["sync", "--dry-run", "--no-resolve-secrets", "--streams", "nope", "--config", CONFIG]
    )
    assert result.exit_code == 2
    assert "Unknown streams" in result.stderr


def test_sync_missing_config_exits_two() -> None:
    result = runner.invoke(
        app, ["sync", "--dry-run", "--no-resolve-secrets", "--config", "/no/such/file.yaml"]
    )
    assert result.exit_code == 2
    assert "not found" in result.stderr


def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZENDESK_SUBDOMAIN", "sub")
    monkeypatch.setenv("ZENDESK_EMAIL", "e@x.com")
    monkeypatch.setenv("ZENDESK_API_TOKEN", "tok")


def test_sync_exit_code_one_on_stream_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    report = RunReport(run_id="r", connector_id="acme")
    report.results.append(StreamResult("ticket", success=False, error="boom"))
    monkeypatch.setattr(orchestrator, "run", lambda cfg, streams: report)
    result = runner.invoke(app, ["sync", "--no-resolve-secrets", "--config", CONFIG])
    assert result.exit_code == 1
    assert "FAILED" in result.stdout


def test_sync_exit_code_zero_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    report = RunReport(run_id="r", connector_id="acme")
    report.results.append(StreamResult("ticket", success=True, records_written=7, duration_s=1.0))
    monkeypatch.setattr(orchestrator, "run", lambda cfg, streams: report)
    result = runner.invoke(app, ["sync", "--no-resolve-secrets", "--config", CONFIG])
    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout


def test_sync_missing_credentials_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("ZENDESK_SUBDOMAIN", "ZENDESK_EMAIL", "ZENDESK_API_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    # A real sync (not dry-run) skipping Secrets Manager must fail fast, not run.
    monkeypatch.setattr(
        orchestrator, "run", lambda cfg, streams: pytest.fail("run should not be called")
    )
    result = runner.invoke(app, ["sync", "--no-resolve-secrets", "--config", CONFIG])
    assert result.exit_code == 2
    assert "requires these env vars" in result.stderr


# ----- state -------------------------------------------------------------


def test_state_shows_never_synced(mock_dynamo: Table) -> None:
    result = runner.invoke(app, ["state", "--stream", "ticket", "--config", CONFIG])
    assert result.exit_code == 0
    assert "never synced" in result.stdout


def test_state_reset_requires_confirm(mock_dynamo: Table) -> None:
    result = runner.invoke(app, ["state", "--reset", "ticket", "--config", CONFIG])
    assert result.exit_code == 2
    assert "--confirm" in result.stderr


def test_state_reset_with_confirm_deletes_state(mock_dynamo: Table) -> None:
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("zendesk_ingestion_state")
    table.put_item(Item={"connector_id": "zendesk_support", "stream_name": "ticket", "cursor": "c"})

    result = runner.invoke(app, ["state", "--reset", "ticket", "--confirm", "--config", CONFIG])
    assert result.exit_code == 0
    assert "Reset state" in result.stdout
    assert "Item" not in table.get_item(
        Key={"connector_id": "zendesk_support", "stream_name": "ticket"}
    )


def test_state_reset_unknown_stream_exits_two(mock_dynamo: Table) -> None:
    result = runner.invoke(
        app, ["state", "--reset", "not_a_stream", "--confirm", "--config", CONFIG]
    )
    assert result.exit_code == 2
    assert "Unknown stream" in result.stderr
