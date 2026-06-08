# Zendesk Support Ingestion Service

Custom Python service that replaces Fivetran for Zendesk Support data ingestion. Pulls data from the Zendesk REST API, transforms records to match Fivetran's output schema, and writes Parquet files to S3 for Snowpipe consumption.

## Quick Reference

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest

# Run a single stream sync locally (uses .env)
uv run python -m zendesk_ingestion.cli sync --streams ticket,user

# Type-check
uv run mypy src/

# Lint
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

## Tech Stack

| Layer | Choice | Reason |
|---|---|---|
| Python | 3.12 | Walrus operator, tomllib, better type hints |
| Package manager | `uv` | Fast, lock-file reproducible |
| HTTP client | `httpx` | Sync client with connection pooling; easier to mock than `requests` |
| Parquet | `pyarrow` | Required for schema enforcement; do NOT use `fastparquet` |
| AWS SDK | `boto3` + `boto3-stubs[s3,dynamodb,secretsmanager,ssm]` | Typed stubs required |
| Config / validation | `pydantic v2` | All config models use `BaseModel`; all settings use `BaseSettings` |
| Logging | `structlog` | JSON-structured logs; all log calls must include `stream=` and `run_id=` |
| Testing | `pytest` + `pytest-mock` + `moto` | `moto` for S3/DynamoDB mocking; no live AWS calls in unit tests |
| Linting | `ruff` | Single tool for lint + format |
| Type checking | `mypy` (strict) | `disallow_untyped_defs = true` |

## Repository Layout

```
zendesk-ingestion/
├── CLAUDE.md                        ← you are here
├── pyproject.toml
├── .env.example
├── src/
│   └── zendesk_ingestion/
│       ├── __init__.py
│       ├── cli.py                   # typer CLI entry point
│       ├── orchestrator.py          # ThreadPoolExecutor fan-out
│       ├── metrics.py               # CloudWatch PutMetricData helper
│       │
│       ├── api/
│       │   ├── __init__.py
│       │   ├── client.py            # ZendeskClient
│       │   ├── rate_limiter.py      # Token-bucket, thread-safe
│       │   └── pagination.py        # CursorPage, OffsetPage dataclasses
│       │
│       ├── streams/
│       │   ├── __init__.py
│       │   ├── base.py              # AbstractStream ABC
│       │   ├── registry.py          # STREAM_REGISTRY: dict[str, type[AbstractStream]]
│       │   ├── tickets.py
│       │   ├── ticket_metrics.py
│       │   ├── ticket_audits.py
│       │   ├── ticket_fields.py
│       │   ├── ticket_forms.py
│       │   ├── users.py
│       │   ├── organizations.py
│       │   ├── groups.py
│       │   ├── satisfaction_ratings.py
│       │   ├── brands.py
│       │   ├── macros.py
│       │   ├── views.py
│       │   ├── automations.py
│       │   ├── triggers.py
│       │   ├── sla_policies.py
│       │   ├── schedules.py
│       │   └── tags.py
│       │
│       ├── sync_modes/
│       │   ├── __init__.py
│       │   ├── base.py              # SyncMode enum + AbstractSyncMode ABC
│       │   ├── incremental_append.py
│       │   ├── incremental_deduped.py
│       │   ├── full_refresh_append.py
│       │   └── full_refresh_overwrite.py
│       │
│       ├── transform/
│       │   ├── __init__.py
│       │   ├── base.py              # AbstractTransformer
│       │   └── fivetran.py          # FivetranTransformer
│       │
│       ├── writers/
│       │   ├── __init__.py
│       │   └── s3.py                # S3Writer
│       │
│       ├── state/
│       │   ├── __init__.py
│       │   └── dynamodb.py          # StateManager
│       │
│       └── config/
│           ├── __init__.py
│           ├── loader.py            # resolve SSM/Secrets refs, return ConnectorConfig
│           └── models.py            # ConnectorConfig, StreamConfig, S3Config pydantic models
│
├── config/
│   └── connector.yaml               # stream definitions; checked into git (no secrets)
│
├── tests/
│   ├── conftest.py                  # shared fixtures: mock_s3, mock_dynamo, sample_records
│   ├── unit/
│   │   ├── api/
│   │   │   ├── test_client.py
│   │   │   └── test_rate_limiter.py
│   │   ├── streams/
│   │   │   └── test_tickets.py      # one test file per stream module
│   │   ├── sync_modes/
│   │   │   ├── test_incremental_append.py
│   │   │   ├── test_incremental_deduped.py
│   │   │   ├── test_full_refresh_append.py
│   │   │   └── test_full_refresh_overwrite.py
│   │   ├── transform/
│   │   │   └── test_fivetran.py
│   │   └── state/
│   │       └── test_dynamodb.py
│   ├── integration/
│   │   └── test_full_sync.py        # requires ZENDESK_* env vars; skipped by default
│   └── fixtures/
│       └── zendesk/                 # real API response snapshots (anonymised)
│           ├── tickets.json
│           ├── users.json
│           └── ...
│
├── infra/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── ecs.tf
│       ├── dynamodb.tf
│       ├── eventbridge.tf
│       ├── iam.tf
│       └── s3.tf
│
├── Dockerfile
└── tasks/                           # Claude Code phase task files
    ├── phase-1-foundation.md
    ├── phase-2-streams.md
    ├── phase-3-orchestration.md
    └── phase-4-tests.md
```

## pyproject.toml (reference — create this exactly)

```toml
[project]
name = "zendesk-ingestion"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "httpx>=0.27",
    "pyarrow>=16.0",
    "boto3>=1.34",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "structlog>=24.1",
    "typer>=0.12",
    "pyyaml>=6.0",
    "tenacity>=8.3",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2",
    "pytest-mock>=3.14",
    "pytest-cov>=5.0",
    "moto[s3,dynamodb,secretsmanager,ssm]>=5.0",
    "boto3-stubs[s3,dynamodb,secretsmanager,ssm]>=1.34",
    "mypy>=1.10",
    "ruff>=0.4",
    "types-pyyaml>=6.0",
    "types-boto3>=1.0",
]

[project.scripts]
zendesk-ingestion = "zendesk_ingestion.cli:app"

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "ANN", "B", "SIM"]
ignore = ["ANN101"]

[tool.mypy]
python_version = "3.12"
strict = true
plugins = ["pydantic.mypy"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["integration: requires live Zendesk credentials (deselected by default)"]
addopts = "-m 'not integration' --cov=src --cov-report=term-missing"
```

## Code Conventions

### Imports
Always use absolute imports from `zendesk_ingestion.*`. Never use relative imports.

### Logging
Every module gets its own logger via `structlog.get_logger()`. Every log call at `info` or above must include `stream` and `run_id` context. Bind them at the top of each sync function:

```python
log = structlog.get_logger().bind(stream="ticket", run_id=run_id)
log.info("sync_started", cursor=cursor)
```

### Error handling
- Raise domain-specific exceptions defined in `zendesk_ingestion/exceptions.py`
- Never swallow exceptions silently — log at `error` level, then re-raise or propagate to orchestrator
- `ZendeskRateLimitError`, `ZendeskAPIError`, `StateConflictError`, `S3WriteError` are the four you'll need

### Type annotations
All functions must have complete type annotations (mypy strict). Use `Iterator[dict[str, Any]]` for record generators. Use `TypedDict` for structured dict shapes that cross module boundaries.

### Tests
- Unit tests use `moto` for all AWS services — never call live AWS
- Fixture JSON files in `tests/fixtures/zendesk/` contain anonymised real API responses
- Each test must be fully self-contained; no shared mutable state between tests
- Name tests `test_{what}_{condition}_{expected_outcome}`

### Environment variables (`.env.example`)
```bash
ZENDESK_SUBDOMAIN=mycompany
ZENDESK_EMAIL=user@example.com
ZENDESK_API_TOKEN=your_token_here
AWS_REGION=us-east-1
S3_BUCKET=my-data-lake-raw
S3_PREFIX=zendesk_support
DYNAMODB_TABLE=zendesk_ingestion_state
# For local dev only — in production these come from Secrets Manager
```

## Architecture Decisions (do not re-debate these)

1. **`httpx` sync client, not async.** The parallelism model is `ThreadPoolExecutor` (one thread per stream). Async would complicate stream isolation and error handling without meaningful throughput gain given Zendesk's rate limits.

2. **`pyarrow` for Parquet, not `pandas`.** We write Parquet directly from `pyarrow.Table` to avoid the pandas dependency and memory overhead. Schema is enforced at write time via `pyarrow.Schema`.

3. **Buffer flush at 50,000 records OR 128 MB, whichever comes first.** Configurable via `ConnectorConfig`. Implemented in `S3Writer.write_batch`.

4. **Two-phase staging for overwrite/deduped paths.** Write to `{prefix}/_staging/{run_id}/` first, then `s3.copy_object` to final path, then delete staging. Never overwrite in place.

5. **DynamoDB conditional writes for cursor advancement.** The `UpdateItem` call uses a `ConditionExpression` that requires the new cursor to be strictly greater than the stored one. This guarantees idempotency on retry.

6. **`tenacity` for retry logic, not hand-rolled loops.** All retry decorators live in `api/client.py`. Do not duplicate retry logic elsewhere.

7. **Derived streams share their parent's API response.** For example, `ticket_tag` and `ticket_comment` are derived from `ticket`. When the orchestrator runs both in the same job, the parent stream's fetched records are cached in memory and shared with derived streams to avoid duplicate API calls.
