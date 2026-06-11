"""Typer CLI entry point for the Zendesk ingestion service.

Exit codes: 0 success, 1 on any stream failure, 2 on config / argument error.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import NoReturn

import structlog
import typer

from zendesk_ingestion import orchestrator
from zendesk_ingestion.config.loader import load_config
from zendesk_ingestion.config.models import ConnectorConfig
from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.streams.registry import STREAM_REGISTRY

app = typer.Typer(add_completion=False, help="Zendesk Support data ingestion service.")
log = structlog.get_logger()

DEFAULT_CONFIG = Path("config/connector.yaml")


_CREDENTIAL_ENV_VARS = ("ZENDESK_SUBDOMAIN", "ZENDESK_EMAIL", "ZENDESK_API_TOKEN")


def _load(
    path: Path, *, resolve_secrets: bool, require_credentials: bool = False
) -> ConnectorConfig:
    """Load config; when not resolving secrets, fill credentials from env vars.

    With ``require_credentials`` set (a real sync that skips Secrets Manager), missing
    credential env vars are an error rather than silently leaving the raw ARN placeholders.
    """
    if not path.exists():
        raise FileNotFoundError(str(path))
    config = load_config(path, resolve_secrets=resolve_secrets)
    if not resolve_secrets:
        if require_credentials:
            missing = [v for v in _CREDENTIAL_ENV_VARS if not os.environ.get(v)]
            if missing:
                raise ValueError(
                    f"--no-resolve-secrets requires these env vars to be set: {missing}"
                )
        config.zendesk_subdomain = os.environ.get("ZENDESK_SUBDOMAIN", config.zendesk_subdomain)
        config.zendesk_email = os.environ.get("ZENDESK_EMAIL", config.zendesk_email)
        config.zendesk_api_token = os.environ.get("ZENDESK_API_TOKEN", config.zendesk_api_token)
    return config


def _parse_streams(streams: str | None) -> list[str] | None:
    if not streams:
        return None
    parsed = [s.strip() for s in streams.split(",") if s.strip()]
    return parsed or None


def _fail(message: str, code: int) -> NoReturn:
    typer.secho(message, fg=typer.colors.RED, err=True)
    raise typer.Exit(code=code)


@app.command()
def sync(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to connector.yaml"),
    streams: str | None = typer.Option(
        None, "--streams", help="Comma-separated stream names (all enabled if omitted)"
    ),
    no_resolve_secrets: bool = typer.Option(
        False, "--no-resolve-secrets", help="Skip Secrets Manager resolution; use env vars"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print resolved config and stream plan without syncing"
    ),
) -> None:
    """Sync Zendesk streams to S3."""
    try:
        cfg = _load(
            config,
            resolve_secrets=not no_resolve_secrets,
            require_credentials=no_resolve_secrets and not dry_run,
        )
    except FileNotFoundError:
        _fail(f"Config file not found: {config}", code=2)
    except Exception as exc:  # noqa: BLE001 — any load/validation error is a config error
        _fail(f"Failed to load config: {exc}", code=2)

    stream_list = _parse_streams(streams)
    if stream_list:
        unknown = sorted(set(stream_list) - set(STREAM_REGISTRY))
        if unknown:
            _fail(f"Unknown streams: {unknown}", code=2)

    if dry_run:
        _print_dry_run(cfg, stream_list)
        raise typer.Exit(code=0)

    report = orchestrator.run(cfg, stream_list)
    _print_report(report)
    raise typer.Exit(code=0 if report.success else 1)


@app.command()
def state(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to connector.yaml"),
    stream: str | None = typer.Option(None, "--stream", help="Show state for a single stream"),
    reset: str | None = typer.Option(None, "--reset", help="Reset cursor for a stream"),
    confirm: bool = typer.Option(False, "--confirm", help="Required for destructive operations"),
) -> None:
    """Inspect or reset per-stream sync state."""
    try:
        cfg = _load(config, resolve_secrets=False)
    except FileNotFoundError:
        _fail(f"Config file not found: {config}", code=2)
    except Exception as exc:  # noqa: BLE001
        _fail(f"Failed to load config: {exc}", code=2)

    state_mgr = StateManager(cfg.state.dynamodb_table, cfg.state.region)

    if reset is not None:
        if reset not in STREAM_REGISTRY:
            _fail(f"Unknown stream: {reset}", code=2)
        if not confirm:
            _fail("Refusing to reset state without --confirm", code=2)
        state_mgr.reset_cursor(cfg.connector_id, reset)
        typer.secho(f"Reset state for stream '{reset}'", fg=typer.colors.YELLOW)
        raise typer.Exit(code=0)

    names = [stream] if stream else sorted(STREAM_REGISTRY)
    if stream is not None and stream not in STREAM_REGISTRY:
        _fail(f"Unknown stream: {stream}", code=2)

    for name in names:
        current = state_mgr.get_state(cfg.connector_id, name)
        if current is None:
            typer.echo(f"{name:32} (never synced)")
        else:
            typer.echo(
                f"{name:32} status={current['status']:12} "
                f"cursor={current['cursor']} "
                f"records={current['records_synced']} "
                f"last_sync={current['last_sync_at']}"
            )
    raise typer.Exit(code=0)


def _print_dry_run(config: ConnectorConfig, requested: list[str] | None) -> None:
    plan = orchestrator.plan_streams(config, requested)
    token = config.zendesk_api_token
    masked = f"{token[:4]}…" if token and not token.startswith("arn:") else "<unresolved>"
    typer.echo("=== Resolved config ===")
    typer.echo(f"connector_id : {config.connector_id}")
    typer.echo(f"s3           : s3://{config.s3.bucket}/{config.s3.prefix} ({config.s3.region})")
    typer.echo(f"dynamodb     : {config.state.dynamodb_table} ({config.state.region})")
    typer.echo(f"subdomain    : {config.zendesk_subdomain}")
    typer.echo(f"api_token    : {masked}")
    typer.echo(
        f"runtime      : parallelism={config.runtime.max_parallelism} "
        f"batch={config.runtime.batch_size_records}rec/{config.runtime.batch_size_mb}MB "
        f"rate={config.runtime.api_rate_limit}rpm"
    )
    typer.echo(f"\n=== Stream plan ({len(plan.requested)} streams) ===")
    typer.echo(f"wave 1 (sources)     : {plan.wave1}")
    typer.echo(f"wave 2 (derived)     : {plan.wave2}")
    if plan.auto_parents:
        typer.echo(f"auto parents (cache) : {plan.auto_parents}")


def _print_report(report: orchestrator.RunReport) -> None:
    typer.echo(f"\n=== Run {report.run_id} ===")
    for res in sorted(report.results, key=lambda r: r.stream_name):
        status = "ok " if res.success else "FAIL"
        suffix = " (cache-only)" if res.cache_only else ""
        line = (
            f"  [{status}] {res.stream_name:32} {res.records_written:>8} rec  "
            f"{res.duration_s:6.2f}s{suffix}"
        )
        color = None if res.success else typer.colors.RED
        typer.secho(line, fg=color)
        if res.error:
            typer.secho(f"          error: {res.error}", fg=typer.colors.RED)
    typer.echo(
        f"\nTotal: {report.total_records} records across {len(report.results)} streams. "
        f"{'SUCCESS' if report.success else 'FAILED: ' + ', '.join(report.failed_streams)}"
    )


if __name__ == "__main__":
    app()
