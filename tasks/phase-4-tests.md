# Phase 4 — Integration Tests & Schema Validation

Complete the testing layer: integration tests, Fivetran schema contract tests, and the parallel-validation harness for cutover. Phases 1–3 must be complete.

---

## Task 4.1 — Shared test fixtures

**`tests/conftest.py`**:

```python
import json
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.api.rate_limiter import RateLimiter
from zendesk_ingestion.config.models import (
    ConnectorConfig, RuntimeConfig, S3Config, StateConfig, StreamConfig, SyncMode,
)
from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.writers.s3 import S3Writer

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "zendesk"


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text())


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent any real AWS calls in unit tests."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def mock_s3(aws_credentials: None) -> Generator[Any, None, None]:
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        yield s3


@pytest.fixture
def mock_dynamo(aws_credentials: None) -> Generator[Any, None, None]:
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="test-state",
            KeySchema=[
                {"AttributeName": "connector_id", "KeyType": "HASH"},
                {"AttributeName": "stream_name", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "connector_id", "AttributeType": "S"},
                {"AttributeName": "stream_name", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


@pytest.fixture
def state_manager(mock_dynamo: Any) -> StateManager:
    return StateManager(table_name="test-state", region="us-east-1")


@pytest.fixture
def s3_writer(mock_s3: Any) -> S3Writer:
    return S3Writer(bucket="test-bucket", prefix="zendesk_support", region="us-east-1")


@pytest.fixture
def base_config(tmp_path: Path) -> ConnectorConfig:
    return ConnectorConfig(
        connector_id="test_connector",
        zendesk_subdomain="testco",
        zendesk_email="test@example.com",
        zendesk_api_token="test_token",
        s3=S3Config(bucket="test-bucket", prefix="zendesk_support"),
        state=StateConfig(dynamodb_table="test-state"),
        runtime=RuntimeConfig(max_parallelism=2, batch_size_records=100),
        streams={
            "ticket": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
            "user": StreamConfig(sync_mode=SyncMode.INCREMENTAL_DEDUPED),
        },
    )


@pytest.fixture
def mock_zendesk_client() -> MagicMock:
    client = MagicMock(spec=ZendeskClient)
    client.last_cursor = "2024-01-15T12:00:00Z"
    return client
```

---

## Task 4.2 — Orchestrator unit tests

**`tests/unit/test_orchestrator.py`**:

```python
"""
Tests for orchestrator.py. All AWS calls are mocked.
Zendesk API calls are mocked at the ZendeskClient level.
"""
```

Write these tests:

- `test_run_syncs_all_enabled_streams` — config with 3 enabled streams; mock all streams to return 10 records each; assert `report.total_records == 30` and `report.success is True`

- `test_run_syncs_only_requested_streams` — pass `streams=["ticket"]`; assert only ticket stream's `get_records` is called

- `test_run_returns_partial_success_on_stream_failure` — one stream raises an exception; assert `report.success is False`, other streams still have `success=True`

- `test_run_raises_on_unknown_stream_name` — pass `streams=["nonexistent"]`; assert `ValueError`

- `test_derived_streams_receive_parent_records` — run `ticket` and `ticket_tag` together; assert `ticket_tag.get_records` is called with non-None `parent_records`

- `test_parent_streams_run_before_derived` — assert ordering in the futures submitted to executor (parent first)

---

## Task 4.3 — End-to-end sync mode tests

**`tests/unit/test_sync_e2e.py`**:

These tests wire the real sync mode classes with mocked AWS (moto) and a mock ZendeskClient. They test the full cycle: records → transformer → writer → state.

For each sync mode, write a test that:
1. Creates a mock stream that yields 150 records (> default batch size of 100 in test config)
2. Runs the sync mode's `execute` method
3. Asserts the correct number of Parquet files in S3
4. Asserts the correct S3 paths for the sync mode
5. Asserts the cursor is advanced in DynamoDB
6. Runs `execute` a SECOND time with the same cursor — asserts idempotent (no duplicate state)

Test names:
- `test_incremental_append_writes_partitioned_parquet`
- `test_incremental_append_advances_cursor`
- `test_incremental_deduped_writes_both_raw_and_deduped`
- `test_incremental_deduped_deduped_prefix_replaced_atomically`
- `test_full_refresh_overwrite_replaces_all_data`
- `test_full_refresh_overwrite_is_atomic_on_failure` — simulate exception during staging write; assert original data still present in S3

---

## Task 4.4 — Fivetran schema contract tests

**`tests/contract/test_fivetran_schema.py`**:

These tests protect against regressions that would break downstream dbt models built on Fivetran's schema.

Create `tests/contract/fivetran_baseline_schema.json` — a JSON file mapping stream name to expected column list and types:

```json
{
  "ticket": {
    "id": "int64",
    "subject": "string",
    "status": "string",
    "requester_id": "int64",
    "assignee_id": "int64",
    "organization_id": "int64",
    "group_id": "int64",
    "created_at": "timestamp[us, tz=UTC]",
    "updated_at": "timestamp[us, tz=UTC]",
    "_fivetran_synced": "timestamp[us, tz=UTC]",
    "_fivetran_deleted": "bool"
  },
  "ticket_tag": {
    "ticket_id": "int64",
    "tag": "string",
    "_fivetran_id": "string",
    "_fivetran_synced": "timestamp[us, tz=UTC]",
    "_fivetran_deleted": "bool"
  }
}
```

Add all 40+ streams to this baseline before running the contract tests.

Test logic:
```python
import pytest
import pyarrow as pa
from zendesk_ingestion.streams.registry import STREAM_REGISTRY
from zendesk_ingestion.transform.fivetran import FivetranTransformer

@pytest.mark.parametrize("stream_name", list(STREAM_REGISTRY.keys()))
def test_stream_schema_matches_fivetran_baseline(stream_name, fivetran_baseline):
    if stream_name not in fivetran_baseline:
        pytest.skip(f"No baseline for {stream_name} — add it to fivetran_baseline_schema.json")

    stream_cls = STREAM_REGISTRY[stream_name]
    transformer = FivetranTransformer(stream_name)
    stream = stream_cls(transformer)
    actual_schema: pa.Schema = stream.get_schema()

    expected = fivetran_baseline[stream_name]
    for col_name, expected_type_str in expected.items():
        field_idx = actual_schema.get_field_index(col_name)
        assert field_idx >= 0, (
            f"Stream '{stream_name}' is missing required column '{col_name}' "
            f"(Fivetran baseline requires it)"
        )
        actual_type_str = str(actual_schema.field(col_name).type)
        assert actual_type_str == expected_type_str, (
            f"Stream '{stream_name}' column '{col_name}': "
            f"expected type '{expected_type_str}', got '{actual_type_str}'"
        )
```

Run contract tests in CI as a separate job that fails loudly:
```yaml
# in .github/workflows/ci.yml
contract:
  needs: test
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: astral-sh/setup-uv@v3
    - run: uv sync --all-extras
    - run: uv run pytest tests/contract/ -v --tb=short
```

---

## Task 4.5 — Integration tests (sandbox)

**`tests/integration/test_full_sync.py`**:

These tests require real Zendesk credentials and a test Zendesk subdomain. They are marked `@pytest.mark.integration` and skipped in CI unless `ZENDESK_RUN_INTEGRATION=true` is set.

```python
import os
import pytest
from zendesk_ingestion.config.loader import load_config
from zendesk_ingestion.orchestrator import run

pytestmark = pytest.mark.integration

@pytest.fixture
def integration_config():
    """Load config from environment variables for sandbox."""
    ...

def test_ticket_incremental_sync_produces_parquet(integration_config, tmp_s3_bucket):
    """Full sync of tickets stream; verify Parquet files exist in S3 with correct schema."""
    report = run(integration_config, streams=["ticket"])
    assert report.success
    assert report.total_records > 0
    # verify S3 objects exist with correct prefix
    ...

def test_second_incremental_sync_only_fetches_new_records(integration_config, tmp_s3_bucket):
    """Run twice; assert second run produces fewer (or zero) records."""
    run(integration_config, streams=["ticket"])
    first_cursor = _get_cursor(integration_config, "ticket")

    run(integration_config, streams=["ticket"])
    second_cursor = _get_cursor(integration_config, "ticket")

    assert second_cursor >= first_cursor

def test_full_refresh_overwrite_replaces_all_s3_objects(integration_config, tmp_s3_bucket):
    """Run full refresh twice; assert second run fully replaced first run's files."""
    ...
```

---

## Task 4.6 — Side-by-side validation script

**`scripts/validate_vs_fivetran.py`**:

A standalone script (not a test) used during the cutover process. Compares this service's S3 output against Fivetran's S3 output for the same time window.

```
Usage:
  python scripts/validate_vs_fivetran.py \
    --custom-s3-prefix s3://bucket/zendesk_support/ \
    --fivetran-s3-prefix s3://bucket/fivetran_zendesk/ \
    --streams ticket,user,organization \
    --date 2024-01-15
```

For each stream, the script:
1. Lists Parquet files from both prefixes for the given date
2. Reads them into `pyarrow.Table`
3. Compares row counts (allow ±0.1% tolerance for replication lag)
4. Compares column names (must be identical)
5. Compares column types (must be identical)
6. Samples 100 random records from each and reports field-level diffs
7. Writes a JSON diff report to `./validation_report_{date}.json`

Exit code 0 if all streams pass. Exit code 1 if any stream has >0.1% row count diff or any schema mismatch.

---

## Verification checklist for Phase 4

```bash
# All unit + contract tests pass
uv run pytest tests/unit/ tests/contract/ -v

# Coverage ≥80% across all source modules
uv run pytest --cov=src --cov-fail-under=80 --cov-report=html

# Contract tests parametrize across all registered streams (40+ cases)
uv run pytest tests/contract/ -v | grep "PASSED\|FAILED\|SKIPPED" | wc -l

# Integration tests (requires sandbox credentials)
ZENDESK_RUN_INTEGRATION=true uv run pytest tests/integration/ -v -s

# Validation script help
uv run python scripts/validate_vs_fivetran.py --help
```

---

## Final pre-cutover checklist

Before switching production traffic to this service:

- [ ] All unit tests pass (`uv run pytest tests/unit/`)
- [ ] All contract tests pass (`uv run pytest tests/contract/`)
- [ ] Integration tests pass against Zendesk sandbox
- [ ] Docker image builds and `--dry-run` succeeds
- [ ] Terraform plan shows no unexpected destroy operations
- [ ] Side-by-side validation script run against staging for 48 hours, diff <0.1%
- [ ] CloudWatch dashboard created and alarms are firing test notifications correctly
- [ ] Runbook written: how to reset a stream cursor, how to trigger a manual full refresh
- [ ] Fivetran connector set to "paused" (not deleted) during shadow period
