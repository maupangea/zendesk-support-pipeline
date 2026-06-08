# Phase 3 — Orchestration, CLI & Infrastructure

Wire everything together: the orchestrator that fans out streams, the CLI entry point, CloudWatch metrics, Dockerfile, and Terraform for all AWS resources.

Phases 1 and 2 must be complete and all tests passing before starting.

---

## Task 3.1 — CloudWatch metrics helper

**`src/zendesk_ingestion/metrics.py`**:

```python
"""
Thin wrapper around CloudWatch PutMetricData.
All metrics are emitted under the 'ZendeskIngestion' namespace.
Dimensions: ConnectorId, Stream (optional).
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
    unit: str = "Count"       # Count | Seconds | Bytes | None
    dimensions: dict[str, str] = field(default_factory=dict)


class MetricsClient:
    NAMESPACE = "ZendeskIngestion"

    def __init__(self, connector_id: str, region: str = "us-east-1") -> None:
        self._connector_id = connector_id
        self._cw = boto3.client("cloudwatch", region_name=region)

    def emit(self, points: list[MetricPoint]) -> None:
        """
        Batch-emit metric data points to CloudWatch.
        Automatically adds ConnectorId dimension to every point.
        Silently logs and suppresses errors — metrics must never fail a sync.
        """
        metric_data = []
        for p in points:
            dims = [{"Name": "ConnectorId", "Value": self._connector_id}]
            for k, v in p.dimensions.items():
                dims.append({"Name": k, "Value": v})
            metric_data.append({
                "MetricName": p.name,
                "Value": p.value,
                "Unit": p.unit,
                "Dimensions": dims,
            })
        try:
            # CloudWatch max 1000 metrics per call
            for chunk in [metric_data[i:i+1000] for i in range(0, len(metric_data), 1000)]:
                self._cw.put_metric_data(Namespace=self.NAMESPACE, MetricData=chunk)
        except Exception:
            log.warning("metrics_emit_failed", exc_info=True)

    def emit_stream_result(
        self, stream: str, records: int, duration_s: float, errors: int
    ) -> None:
        self.emit([
            MetricPoint("RecordsSynced", records, "Count", {"Stream": stream}),
            MetricPoint("SyncDurationSeconds", duration_s, "Seconds", {"Stream": stream}),
            MetricPoint("APIErrors", errors, "Count", {"Stream": stream}),
        ])
```

---

## Task 3.2 — Orchestrator

**`src/zendesk_ingestion/orchestrator.py`**:

```python
from __future__ import annotations
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import structlog

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.api.rate_limiter import RateLimiter
from zendesk_ingestion.config.models import ConnectorConfig, SyncMode
from zendesk_ingestion.metrics import MetricsClient
from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.streams.registry import STREAM_REGISTRY
from zendesk_ingestion.sync_modes.base import SyncContext
from zendesk_ingestion.sync_modes import (
    IncrementalAppend,
    IncrementalDeduped,
    FullRefreshAppend,
    FullRefreshOverwrite,
)
from zendesk_ingestion.transform.fivetran import FivetranTransformer
from zendesk_ingestion.writers.s3 import S3Writer


SYNC_MODE_CLASSES = {
    SyncMode.INCREMENTAL_APPEND: IncrementalAppend,
    SyncMode.INCREMENTAL_DEDUPED: IncrementalDeduped,
    SyncMode.FULL_REFRESH_APPEND: FullRefreshAppend,
    SyncMode.FULL_REFRESH_OVERWRITE: FullRefreshOverwrite,
}


@dataclass
class StreamResult:
    stream_name: str
    success: bool
    records_written: int = 0
    duration_s: float = 0.0
    error: str | None = None


@dataclass
class RunReport:
    run_id: str
    connector_id: str
    results: list[StreamResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results)

    @property
    def total_records(self) -> int:
        return sum(r.records_written for r in self.results)

    @property
    def failed_streams(self) -> list[str]:
        return [r.stream_name for r in self.results if not r.success]


def run(config: ConnectorConfig, streams: list[str] | None = None) -> RunReport:
    """
    Main entry point. Fan out stream syncs using ThreadPoolExecutor.

    Args:
        config: Fully resolved ConnectorConfig (secrets already loaded).
        streams: Optional subset of stream names to sync. If None, sync all enabled streams.

    Returns:
        RunReport with per-stream results.
    """
    run_id = str(uuid.uuid4())
    log = structlog.get_logger().bind(connector_id=config.connector_id, run_id=run_id)
    log.info("run_started")

    # Resolve which streams to run
    enabled = _resolve_streams(config, streams)
    log.info("streams_resolved", count=len(enabled), names=enabled)

    # Shared client (thread-safe: httpx.Client uses a connection pool)
    rate_limiter = RateLimiter(config.runtime.api_rate_limit)
    client = ZendeskClient(
        subdomain=config.zendesk_subdomain,
        email=config.zendesk_email,
        api_token=config.zendesk_api_token,
        rate_limiter=rate_limiter,
    )
    state_mgr = StateManager(config.state.dynamodb_table, config.state.region)
    writer = S3Writer(
        bucket=config.s3.bucket,
        prefix=config.s3.prefix,
        region=config.s3.region,
        batch_size_records=config.runtime.batch_size_records,
        batch_size_mb=config.runtime.batch_size_mb,
    )
    metrics = MetricsClient(config.connector_id)
    report = RunReport(run_id=run_id, connector_id=config.connector_id)

    # Parent stream cache: if a parent stream ran in this job, share its raw records
    # with derived streams to avoid duplicate API calls.
    parent_record_cache: dict[str, list[dict[str, Any]]] = {}

    with ThreadPoolExecutor(max_workers=config.runtime.max_parallelism) as executor:
        futures = {
            executor.submit(
                _sync_stream,
                stream_name=name,
                config=config,
                run_id=run_id,
                client=client,
                state_mgr=state_mgr,
                writer=writer,
                parent_record_cache=parent_record_cache,
            ): name
            for name in enabled
        }
        for future in as_completed(futures):
            result = future.result()  # StreamResult (never raises — errors are caught inside)
            report.results.append(result)
            metrics.emit_stream_result(
                result.stream_name, result.records_written, result.duration_s,
                0 if result.success else 1,
            )
            log.info(
                "stream_completed",
                stream=result.stream_name,
                success=result.success,
                records=result.records_written,
            )

    log.info(
        "run_completed",
        success=report.success,
        total_records=report.total_records,
        failed=report.failed_streams,
    )
    return report


def _sync_stream(
    stream_name: str,
    config: ConnectorConfig,
    run_id: str,
    client: ZendeskClient,
    state_mgr: StateManager,
    writer: S3Writer,
    parent_record_cache: dict[str, list[dict[str, Any]]],
) -> StreamResult:
    """
    Execute one stream's full sync cycle. Catches all exceptions and returns
    a StreamResult — never propagates exceptions to the ThreadPoolExecutor.
    """
    log = structlog.get_logger().bind(stream=stream_name, run_id=run_id)
    start = time.monotonic()

    stream_cfg = config.streams.get(stream_name)
    if stream_cfg is None:
        return StreamResult(stream_name, success=False, error="No config found")

    stream_cls = STREAM_REGISTRY.get(stream_name)
    if stream_cls is None:
        return StreamResult(stream_name, success=False, error="Stream class not found in registry")

    try:
        state = state_mgr.get_state(config.connector_id, stream_name)
        cursor = state["cursor"] if state else None

        transformer = FivetranTransformer(stream_name)
        stream = stream_cls(transformer)
        schema = stream.get_schema()

        # Resolve parent records if this is a derived stream
        parent_records: list[dict[str, Any]] | None = None
        if stream_cls.parent_stream:
            parent_records = parent_record_cache.get(stream_cls.parent_stream)

        state_mgr.begin_run(config.connector_id, stream_name, run_id, stream_cfg.sync_mode)
        records_iter = stream.get_records(client, cursor, parent_records)

        sync_mode_cls = SYNC_MODE_CLASSES[stream_cfg.sync_mode]
        sync_mode = sync_mode_cls()
        ctx = SyncContext(
            connector_id=config.connector_id,
            stream_name=stream_name,
            run_id=run_id,
            writer=writer,
            state=state_mgr,
            schema=schema,
            batch_size_records=config.runtime.batch_size_records,
        )
        final_cursor = stream.get_final_cursor(client)
        records_written = sync_mode.execute(records_iter, ctx, final_cursor)

        duration = time.monotonic() - start
        log.info("stream_sync_success", records=records_written, duration_s=round(duration, 2))
        return StreamResult(stream_name, success=True, records_written=records_written, duration_s=duration)

    except Exception as exc:
        duration = time.monotonic() - start
        log.error("stream_sync_failed", exc_info=True, duration_s=round(duration, 2))
        state_mgr.mark_failed(config.connector_id, stream_name, run_id)
        return StreamResult(stream_name, success=False, duration_s=duration, error=str(exc))


def _resolve_streams(config: ConnectorConfig, requested: list[str] | None) -> list[str]:
    """
    Return sorted list of stream names to sync.
    If `requested` is given, validate all names exist and are enabled.
    Otherwise return all enabled streams from config.
    Parent streams always run before their derived children (sort by parent_stream presence).
    """
    all_enabled = [name for name, cfg in config.streams.items() if cfg.enabled]

    if requested:
        unknown = set(requested) - set(STREAM_REGISTRY)
        if unknown:
            raise ValueError(f"Unknown streams: {unknown}")
        to_run = [s for s in requested if s in all_enabled]
    else:
        to_run = all_enabled

    # Sort: non-derived (parent_stream=None) first so cache is populated before derived streams
    return sorted(to_run, key=lambda s: (STREAM_REGISTRY[s].parent_stream is not None, s))
```

---

## Task 3.3 — CLI entry point

**`src/zendesk_ingestion/cli.py`**:

Use `typer`. Implement two commands:

```
zendesk-ingestion sync [OPTIONS]
  --config PATH         Path to connector.yaml [default: config/connector.yaml]
  --streams TEXT        Comma-separated stream names (all enabled if omitted)
  --no-resolve-secrets  Skip AWS Secrets Manager resolution (use env vars)
  --dry-run             Print resolved config and stream list without syncing

zendesk-ingestion state [OPTIONS]
  --config PATH
  --stream TEXT         Show state for one stream
  --reset TEXT          Reset cursor for a stream (requires --confirm)
  --confirm             Required flag for destructive operations
```

Exit code 0 on success, 1 on any stream failure, 2 on config/arg error.

Example:
```bash
# Sync all enabled streams
zendesk-ingestion sync

# Sync only tickets and users
zendesk-ingestion sync --streams ticket,user

# Show current state for all streams
zendesk-ingestion state

# Reset ticket cursor (for full re-sync)
zendesk-ingestion state --reset ticket --confirm
```

---

## Task 3.4 — Dockerfile

**`Dockerfile`**:

```dockerfile
FROM python:3.12-slim AS builder

RUN pip install uv
WORKDIR /app
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --frozen

FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /app/.venv .venv
COPY src/ src/
COPY config/ config/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["zendesk-ingestion"]
CMD ["sync"]
```

Build and smoke-test locally:
```bash
docker build -t zendesk-ingestion:local .
docker run --rm zendesk-ingestion:local sync --dry-run --no-resolve-secrets
```

---

## Task 3.5 — Terraform infrastructure

Create all files under `infra/terraform/`. Use Terraform ~>1.8, AWS provider ~>5.0.

**`infra/terraform/variables.tf`**:
```hcl
variable "env"            { type = string }                     # dev, staging, prod
variable "aws_region"     { type = string; default = "us-east-1" }
variable "s3_bucket"      { type = string }
variable "ecr_image_uri"  { type = string }                     # set by CI
variable "connector_id"   { type = string; default = "zendesk_support" }
```

**`infra/terraform/dynamodb.tf`**:
- Table `zendesk_ingestion_state_{env}`
- PK: `connector_id` (String), SK: `stream_name` (String)
- Billing: `PAY_PER_REQUEST`
- `point_in_time_recovery { enabled = true }`

**`infra/terraform/ecs.tf`**:
- ECS cluster `zendesk-ingestion-{env}`
- Task definition: Fargate, 1 vCPU, 2048 MB
- Container: image from `var.ecr_image_uri`, command `["sync"]`
- Environment variables: `CONNECTOR_ID`, `AWS_REGION`, `DYNAMODB_TABLE`, `S3_BUCKET`, `S3_PREFIX`
- Log configuration: awslogs driver → `/zendesk-ingestion/{env}` log group, 30-day retention

**`infra/terraform/iam.tf`**:

Create a task IAM role with least-privilege inline policy. Grant ONLY:
```
s3:PutObject, s3:DeleteObject, s3:GetObject, s3:ListBucket
  → arn:aws:s3:::${var.s3_bucket}/zendesk_support/*
dynamodb:GetItem, dynamodb:PutItem, dynamodb:UpdateItem
  → arn:aws:dynamodb:*:*:table/zendesk_ingestion_state_${var.env}
secretsmanager:GetSecretValue
  → arn:aws:secretsmanager:*:*:secret:zendesk/*
ssm:GetParameter
  → arn:aws:ssm:*:*:parameter/zendesk/*
cloudwatch:PutMetricData
  → * (CloudWatch does not support resource-level restrictions)
logs:CreateLogStream, logs:PutLogEvents
  → arn:aws:logs:*:*:log-group:/zendesk-ingestion/${var.env}:*
```

**`infra/terraform/eventbridge.tf`**:

Two schedules:
1. `zendesk-high-frequency-{env}`: every 15 minutes, runs `ticket,user,organization,ticket_comment`
2. `zendesk-config-{env}`: every 6 hours, runs all remaining streams

Both targets: ECS RunTask on the Fargate task definition. Add SQS DLQ for failed invocations.

**`infra/terraform/s3.tf`**:

If the bucket doesn't already exist, create it with:
- Versioning enabled
- SSE-S3 encryption
- Lifecycle rule: transition to INTELLIGENT_TIERING after 30 days (applies to `zendesk_support/` prefix only)
- Block all public access

**`infra/terraform/main.tf`**:
```hcl
terraform {
  required_version = "~> 1.8"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {}   # configure via -backend-config in CI
}

provider "aws" {
  region = var.aws_region
  default_tags { tags = { Project = "zendesk-ingestion", Env = var.env } }
}
```

---

## Task 3.6 — GitHub Actions CI/CD

**`.github/workflows/ci.yml`**:

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
        with: { python-version: "3.12" }
      - run: uv sync --all-extras
      - run: uv run ruff check src/ tests/
      - run: uv run ruff format --check src/ tests/
      - run: uv run mypy src/
      - run: uv run pytest --cov=src --cov-fail-under=80

  build:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE_ARN }}
          aws-region: us-east-1
      - uses: aws-actions/amazon-ecr-login@v2
      - name: Build and push
        run: |
          IMAGE=${{ secrets.ECR_REPO }}:${{ github.sha }}
          docker build -t $IMAGE .
          docker push $IMAGE
          echo "ECR_IMAGE_URI=$IMAGE" >> $GITHUB_ENV
      - name: Terraform deploy (staging)
        working-directory: infra/terraform
        run: |
          terraform init -backend-config=backend-staging.hcl
          terraform apply -auto-approve \
            -var="env=staging" \
            -var="ecr_image_uri=$ECR_IMAGE_URI"
```

---

## Verification checklist for Phase 3

```bash
# Full test suite still passes
uv run pytest -v

# CLI smoke test (local, no AWS)
uv run zendesk-ingestion sync --dry-run --no-resolve-secrets --config config/connector.yaml

# Docker build succeeds
docker build -t zendesk-ingestion:local .
docker run --rm zendesk-ingestion:local --help

# Terraform validate
cd infra/terraform && terraform init -backend=false && terraform validate

# Import check — all modules importable
uv run python -c "from zendesk_ingestion.orchestrator import run; print('OK')"
```
