# Phase 1 — Foundation

Build the core infrastructure layer: project scaffold, API client, state manager, S3 writer, and sync mode strategy classes. No stream implementations yet.

Complete tasks in order. Run `uv run pytest` after each task — it must pass (or show only expected "not implemented" skips) before moving to the next.

---

## Task 1.1 — Project scaffold

Create the following files with correct content. Do not skip any.

**`pyproject.toml`** — copy exactly from CLAUDE.md.

**`src/zendesk_ingestion/__init__.py`** — empty.

**`src/zendesk_ingestion/exceptions.py`**:
```python
class ZendeskIngestionError(Exception):
    """Base exception for all ingestion errors."""

class ZendeskRateLimitError(ZendeskIngestionError):
    """Raised when Zendesk returns 429. Includes retry_after seconds."""
    def __init__(self, retry_after: int = 60) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after}s.")

class ZendeskAPIError(ZendeskIngestionError):
    """Raised on non-retryable Zendesk API errors (4xx except 429)."""
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Zendesk API error {status_code}: {message}")

class StateConflictError(ZendeskIngestionError):
    """Raised when a DynamoDB conditional write fails (stale cursor)."""

class S3WriteError(ZendeskIngestionError):
    """Raised on unrecoverable S3 write failure."""
```

**`src/zendesk_ingestion/config/models.py`**:

```python
from __future__ import annotations
from enum import StrEnum
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class SyncMode(StrEnum):
    INCREMENTAL_APPEND = "incremental_append"
    INCREMENTAL_DEDUPED = "incremental_append_deduped"
    FULL_REFRESH_APPEND = "full_refresh_append"
    FULL_REFRESH_OVERWRITE = "full_refresh_overwrite"


class StreamConfig(BaseModel):
    enabled: bool = True
    sync_mode: SyncMode
    cursor_lookback_hours: int = 1


class S3Config(BaseModel):
    bucket: str
    prefix: str
    region: str = "us-east-1"


class StateConfig(BaseModel):
    dynamodb_table: str
    region: str = "us-east-1"


class RuntimeConfig(BaseModel):
    max_parallelism: int = 8
    batch_size_records: int = 50_000
    batch_size_mb: int = 128
    api_rate_limit: int = 700  # requests per minute


class ConnectorConfig(BaseModel):
    connector_id: str
    s3: S3Config
    state: StateConfig
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    streams: dict[str, StreamConfig] = Field(default_factory=dict)
    # Resolved at load time from Secrets Manager — not in YAML
    zendesk_subdomain: str = ""
    zendesk_email: str = ""
    zendesk_api_token: str = ""
```

**`src/zendesk_ingestion/config/loader.py`**:

```python
"""Load ConnectorConfig from a YAML file, resolving secrets from AWS SSM / Secrets Manager."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import boto3
import yaml

from zendesk_ingestion.config.models import ConnectorConfig


def load_config(yaml_path: Path, *, resolve_secrets: bool = True) -> ConnectorConfig:
    """
    Parse connector.yaml and optionally resolve ARN references in the zendesk block.
    ARN values (starting with 'arn:aws:secretsmanager' or 'arn:aws:ssm') are fetched
    from AWS and their values substituted in-place before validation.
    """
    raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())

    if resolve_secrets:
        raw = _resolve_aws_refs(raw)

    return ConnectorConfig.model_validate(raw)


def _resolve_aws_refs(config: dict[str, Any]) -> dict[str, Any]:
    """Walk the config dict and replace ARN string values with their resolved secrets."""
    sm_client = boto3.client("secretsmanager")
    ssm_client = boto3.client("ssm")

    def resolve(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        if value.startswith("arn:aws:secretsmanager"):
            resp = sm_client.get_secret_value(SecretId=value)
            secret = resp.get("SecretString", "")
            try:
                return json.loads(secret)
            except json.JSONDecodeError:
                return secret
        if value.startswith("arn:aws:ssm") or value.startswith("/"):
            resp = ssm_client.get_parameter(Name=value, WithDecryption=True)
            return resp["Parameter"]["Value"]
        return value

    def walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [walk(i) for i in obj]
        return resolve(obj)

    return walk(config)  # type: ignore[return-value]
```

**`config/connector.yaml`** — create this with all streams listed in CLAUDE.md under the Stream Catalog. Set sensible default sync modes matching the table in the plan. Use placeholder ARN strings for secrets.

---

## Task 1.2 — Rate limiter

**`src/zendesk_ingestion/api/rate_limiter.py`**:

Implement a thread-safe token bucket rate limiter.

Requirements:
- Constructor: `RateLimiter(requests_per_minute: int)`
- Method: `acquire() -> None` — blocks until a token is available
- Uses `threading.Lock` for thread safety
- Replenishes tokens based on elapsed wall time since last check (continuous replenishment, not fixed windows)
- Tokens never exceed `requests_per_minute` (the bucket capacity)

Write tests in `tests/unit/api/test_rate_limiter.py`:
- `test_acquire_does_not_block_when_tokens_available`
- `test_acquire_blocks_when_bucket_empty` — use `time.monotonic` before/after, assert delay ≥ expected
- `test_thread_safety` — 20 threads each call `acquire()` 5 times; assert total elapsed time is consistent with the configured rate

---

## Task 1.3 — Zendesk API client

**`src/zendesk_ingestion/api/pagination.py`**:

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass
class CursorPage:
    """Result of a cursor-based paginated request."""
    records: list[dict[str, Any]]
    next_cursor: str | None      # None means last page
    end_of_stream: bool = False  # True when Zendesk signals no more incremental data


@dataclass
class OffsetPage:
    """Result of an offset/link-based paginated request."""
    records: list[dict[str, Any]]
    next_url: str | None         # None means last page
```

**`src/zendesk_ingestion/api/client.py`**:

Implement `ZendeskClient` with these exact signatures:

```python
class ZendeskClient:
    def __init__(
        self,
        subdomain: str,
        email: str,
        api_token: str,
        rate_limiter: RateLimiter,
        base_url: str | None = None,  # override for tests
    ) -> None: ...

    def get_page(self, path: str, params: dict[str, Any] | None = None) -> OffsetPage:
        """
        Single GET request. Returns one OffsetPage (records + next_url).
        Raises ZendeskRateLimitError on 429, ZendeskAPIError on other 4xx/5xx.
        """

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """
        Yield all records across all pages for a standard list endpoint.
        Follows 'next' links until exhausted.
        """

    def get_incremental_cursor(
        self, path: str, cursor: str | None = None
    ) -> CursorPage:
        """
        Single call to a Zendesk incremental cursor endpoint.
        Returns CursorPage with next_cursor and end_of_stream flag.
        Sets end_of_stream=True when response has 'end_of_stream: true'.
        """

    def paginate_incremental(
        self, path: str, cursor: str | None = None
    ) -> Iterator[dict[str, Any]]:
        """
        Yield all records from an incremental cursor endpoint.
        Stops when end_of_stream is True.
        Yields (record, final_cursor) — actually just records; final cursor
        is accessible via self.last_cursor after iteration completes.
        """
```

Implementation notes:
- Auth: HTTP Basic with `{email}/token:{api_token}` as username, empty password
- Base URL: `https://{subdomain}.zendesk.com`
- Use `httpx.Client` with `timeout=httpx.Timeout(30.0)` and connection pooling (`limits=httpx.Limits(max_keepalive_connections=10)`)
- All requests call `self._rate_limiter.acquire()` before executing
- Retry logic uses `tenacity`:
  - Retry on `ZendeskRateLimitError` (wait = `retry_after` from exception, max 5 retries)
  - Retry on `httpx.TransportError` or 5xx (exponential backoff, base 2s, max 60s, 5 retries, jitter)
  - Do NOT retry on 4xx (except 429)
- After a successful incremental page, store `self.last_cursor: str | None`

Write tests in `tests/unit/api/test_client.py`:
- `test_paginate_follows_next_links` — mock `httpx` to return 3 pages, assert all records yielded
- `test_get_incremental_sets_end_of_stream` — response with `end_of_stream: true`
- `test_retries_on_429` — assert retry is attempted with correct wait
- `test_raises_zendesk_api_error_on_404` — no retry
- `test_rate_limiter_called_before_each_request` — mock rate limiter, assert call count

---

## Task 1.4 — State manager

**`src/zendesk_ingestion/state/dynamodb.py`**:

```python
class StreamState(TypedDict):
    connector_id: str
    stream_name: str
    cursor: str | None
    last_sync_at: str | None        # ISO 8601
    last_run_id: str | None
    records_synced: int
    sync_mode: str
    status: Literal["success", "in_progress", "failed"]


class StateManager:
    def __init__(self, table_name: str, region: str = "us-east-1") -> None: ...

    def get_state(self, connector_id: str, stream_name: str) -> StreamState | None:
        """Return current state, or None if stream has never been synced."""

    def begin_run(self, connector_id: str, stream_name: str, run_id: str, sync_mode: str) -> None:
        """
        Set status='in_progress'. Uses unconditional write.
        Called at the START of a sync run.
        """

    def commit_cursor(
        self,
        connector_id: str,
        stream_name: str,
        run_id: str,
        new_cursor: str,
        records_synced: int,
    ) -> None:
        """
        Advance the cursor on successful completion.
        Uses a DynamoDB ConditionExpression:
          attribute_not_exists(cursor) OR cursor < :new_cursor
        Raises StateConflictError if condition fails (stale/concurrent write).
        Sets status='success', last_sync_at=utcnow().
        """

    def mark_failed(self, connector_id: str, stream_name: str, run_id: str) -> None:
        """Set status='failed'. Cursor is NOT advanced."""
```

DynamoDB key schema:
- Partition key: `connector_id` (String)
- Sort key: `stream_name` (String)

Write tests in `tests/unit/state/test_dynamodb.py` using `moto`:
- `test_get_state_returns_none_for_new_stream`
- `test_begin_run_sets_in_progress`
- `test_commit_cursor_advances_cursor`
- `test_commit_cursor_raises_on_stale_write` — call `commit_cursor` with a cursor LESS than the current one; assert `StateConflictError`
- `test_mark_failed_does_not_advance_cursor`

---

## Task 1.5 — S3 writer

**`src/zendesk_ingestion/writers/s3.py`**:

```python
@dataclass
class WriteResult:
    s3_paths: list[str]       # all files written (staging or final)
    records_written: int
    shards: int


class S3Writer:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        region: str = "us-east-1",
        batch_size_records: int = 50_000,
        batch_size_mb: int = 128,
    ) -> None: ...

    def write_batch(
        self,
        records: list[dict[str, Any]],
        schema: pa.Schema,
        stream_name: str,
        run_id: str,
        sync_mode: SyncMode,
        shard: int = 0,
        partition: dict[str, str] | None = None,  # {"year": "2024", "month": "01", "day": "15"}
    ) -> WriteResult:
        """
        Serialize records to Parquet using the provided pyarrow.Schema (strict).
        Upload to S3 at the correct path for the sync_mode (see path rules below).
        For FULL_REFRESH_OVERWRITE and INCREMENTAL_DEDUPED: write to staging path.
        For INCREMENTAL_APPEND and FULL_REFRESH_APPEND: write to final path directly.
        """

    def commit_staging(self, stream_name: str, run_id: str) -> None:
        """
        Atomic promotion for FULL_REFRESH_OVERWRITE / INCREMENTAL_DEDUPED.
        1. List all objects under {prefix}/{stream}/_staging/{run_id}/
        2. s3.copy_object each to final path (strip _staging/{run_id}/)
        3. Delete staging objects
        Called only after ALL records are written successfully.
        """

    def delete_stream_prefix(self, stream_name: str) -> None:
        """
        Bulk delete of {prefix}/{stream}/data/ for FULL_REFRESH_OVERWRITE.
        Called BEFORE commit_staging. Uses paginated DeleteObjects (1000/req max).
        """
```

S3 path rules (implement exactly):
```
INCREMENTAL_APPEND / FULL_REFRESH_APPEND (partitioned):
  {prefix}/{stream}/data/year={Y}/month={M}/day={D}/{stream}_{run_id}_{shard}.parquet

FULL_REFRESH_OVERWRITE (staging then final):
  staging: {prefix}/{stream}/_staging/{run_id}/{stream}_{shard}.parquet
  final:   {prefix}/{stream}/data/{stream}_{shard}.parquet

INCREMENTAL_DEDUPED:
  raw (same as incremental_append above)
  deduped staging: {prefix}/{stream}/_staging/{run_id}/deduped/{stream}_{shard}.parquet
  deduped final:   {prefix}/{stream}/deduped/{stream}_{shard}.parquet
```

Write tests in `tests/unit/writers/test_s3.py` using `moto`:
- `test_write_batch_incremental_append_correct_path`
- `test_write_batch_full_refresh_overwrite_writes_to_staging`
- `test_commit_staging_promotes_to_final_path`
- `test_commit_staging_deletes_staging_objects`
- `test_delete_stream_prefix_removes_all_data_objects`
- `test_parquet_schema_enforced` — pass a record with wrong type; assert `pyarrow.ArrowTypeError` or similar

---

## Task 1.6 — Sync mode strategy classes

**`src/zendesk_ingestion/sync_modes/base.py`**:

```python
from abc import ABC, abstractmethod
from typing import Any, Iterator
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.writers.s3 import S3Writer
from zendesk_ingestion.state.dynamodb import StateManager
import pyarrow as pa


class SyncContext:
    """All dependencies a sync mode needs to execute."""
    connector_id: str
    stream_name: str
    run_id: str
    writer: S3Writer
    state: StateManager
    schema: pa.Schema
    batch_size_records: int


class AbstractSyncMode(ABC):
    mode: SyncMode

    @abstractmethod
    def execute(
        self,
        records: Iterator[dict[str, Any]],
        ctx: SyncContext,
        final_cursor: str | None,
    ) -> int:
        """
        Consume the records iterator, write to S3, update state.
        Returns total records written.
        `final_cursor` is the cursor value to commit (provided by the stream after iteration).
        """
```

Implement all four sync modes. Each class lives in its own file and inherits `AbstractSyncMode`.

**`incremental_append.py`**: Batch records into groups of `ctx.batch_size_records`, call `ctx.writer.write_batch` with today's partition, commit cursor after all batches written.

**`incremental_deduped.py`**: Same as incremental append for the raw path. Additionally, accumulate ALL records in memory (warn via structlog if total exceeds 1M), write deduped shard(s) to staging, call `commit_staging` after raw + deduped both written. Commit cursor last.

**`full_refresh_append.py`**: Same as incremental append but cursor is always `None` (full re-read, no cursor advancement needed — still write `last_sync_at`).

**`full_refresh_overwrite.py`**: Accumulate all records, write to staging, call `delete_stream_prefix`, then `commit_staging`. If an exception occurs after `delete_stream_prefix` but before `commit_staging`, log a critical-level alert (data loss window). Never suppress this exception.

---

## Task 1.7 — Fivetran transformer

**`src/zendesk_ingestion/transform/fivetran.py`**:

```python
from datetime import datetime, timezone
from typing import Any
import hashlib


FIVETRAN_SYNCED_COL = "_fivetran_synced"
FIVETRAN_DELETED_COL = "_fivetran_deleted"
FIVETRAN_ID_COL = "_fivetran_id"


class FivetranTransformer:
    """
    Applies Fivetran-compatible transformations to raw Zendesk API records.
    Each stream can subclass and override `transform_record` for stream-specific logic.
    """

    def __init__(self, stream_name: str, deleted_field: str | None = None) -> None:
        self._stream_name = stream_name
        self._deleted_field = deleted_field  # e.g. "status" for tickets
        self._synced_at = datetime.now(tz=timezone.utc)

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        out = self._flatten(record)
        out = self._cast_timestamps(out)
        out[FIVETRAN_SYNCED_COL] = self._synced_at
        out[FIVETRAN_DELETED_COL] = self._is_deleted(record)
        return out

    def add_synthetic_id(self, record: dict[str, Any], *key_fields: str) -> dict[str, Any]:
        """
        For streams with no natural PK (e.g. ticket_tag), add _fivetran_id
        as the MD5 hex digest of the concatenated key field values.
        """
        key = ":".join(str(record.get(f, "")) for f in key_fields)
        record[FIVETRAN_ID_COL] = hashlib.md5(key.encode()).hexdigest()
        return record

    def _flatten(self, record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        """
        Recursively flatten nested dicts using underscore-joined keys.
        e.g. {"via": {"channel": "web"}} → {"via_channel": "web"}
        Lists are NOT flattened (they become separate derived streams).
        """
        ...

    def _cast_timestamps(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Convert all ISO 8601 string values that look like timestamps
        (pattern: YYYY-MM-DDTHH:MM:SSZ or with offset) to datetime objects.
        """
        ...

    def _is_deleted(self, record: dict[str, Any]) -> bool:
        if self._deleted_field is None:
            return False
        return record.get(self._deleted_field) == "deleted"
```

Write tests in `tests/unit/transform/test_fivetran.py`:
- `test_flatten_nested_via_object`
- `test_flatten_does_not_flatten_lists`
- `test_cast_timestamps_converts_iso_strings`
- `test_fivetran_synced_is_utc_datetime`
- `test_is_deleted_true_when_status_deleted` (for ticket transformer)
- `test_synthetic_id_is_stable` — same input → same MD5 every time
- `test_synthetic_id_differs_for_different_inputs`

---

## Verification checklist for Phase 1

Run these before declaring Phase 1 complete:

```bash
uv run pytest tests/unit/ -v                    # all unit tests pass
uv run mypy src/                                # no type errors
uv run ruff check src/ tests/                   # no lint errors
uv run ruff format --check src/ tests/          # no format diff
uv run pytest --cov=src --cov-fail-under=80     # ≥80% coverage on foundation modules
```

All imports between modules must resolve without circular dependencies. Verify with:
```bash
uv run python -c "from zendesk_ingestion.orchestrator import run"
# (orchestrator.py doesn't exist yet — that's fine, just check the foundation modules)
uv run python -c "from zendesk_ingestion.api.client import ZendeskClient"
uv run python -c "from zendesk_ingestion.writers.s3 import S3Writer"
uv run python -c "from zendesk_ingestion.state.dynamodb import StateManager"
```
