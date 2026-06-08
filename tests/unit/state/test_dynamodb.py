from __future__ import annotations

import pytest
from mypy_boto3_dynamodb.service_resource import Table

from zendesk_ingestion.exceptions import StateConflictError
from zendesk_ingestion.state.dynamodb import StateManager


@pytest.fixture
def manager(mock_dynamo: Table) -> StateManager:
    return StateManager(table_name="zendesk_ingestion_state", region="us-east-1")


def test_get_state_returns_none_for_new_stream(manager: StateManager) -> None:
    assert manager.get_state("acme", "ticket") is None


def test_begin_run_sets_in_progress(manager: StateManager) -> None:
    manager.begin_run("acme", "ticket", run_id="run-1", sync_mode="incremental_append")
    state = manager.get_state("acme", "ticket")
    assert state is not None
    assert state["status"] == "in_progress"
    assert state["last_run_id"] == "run-1"
    assert state["sync_mode"] == "incremental_append"


def test_commit_cursor_advances_cursor(manager: StateManager) -> None:
    manager.begin_run("acme", "ticket", run_id="run-1", sync_mode="incremental_append")
    manager.commit_cursor(
        "acme",
        "ticket",
        run_id="run-1",
        new_cursor="2024-01-01T00:00:00Z",
        records_synced=10,
    )
    state = manager.get_state("acme", "ticket")
    assert state is not None
    assert state["cursor"] == "2024-01-01T00:00:00Z"
    assert state["status"] == "success"
    assert state["records_synced"] == 10
    assert state["last_sync_at"] is not None


def test_commit_cursor_raises_on_stale_write(manager: StateManager) -> None:
    manager.begin_run("acme", "ticket", run_id="run-1", sync_mode="incremental_append")
    manager.commit_cursor(
        "acme",
        "ticket",
        run_id="run-1",
        new_cursor="2024-06-01T00:00:00Z",
        records_synced=5,
    )
    with pytest.raises(StateConflictError):
        manager.commit_cursor(
            "acme",
            "ticket",
            run_id="run-2",
            new_cursor="2024-01-01T00:00:00Z",
            records_synced=5,
        )


def test_mark_failed_does_not_advance_cursor(manager: StateManager) -> None:
    manager.begin_run("acme", "ticket", run_id="run-1", sync_mode="incremental_append")
    manager.commit_cursor(
        "acme",
        "ticket",
        run_id="run-1",
        new_cursor="2024-01-01T00:00:00Z",
        records_synced=5,
    )
    manager.mark_failed("acme", "ticket", run_id="run-2")
    state = manager.get_state("acme", "ticket")
    assert state is not None
    assert state["status"] == "failed"
    assert state["cursor"] == "2024-01-01T00:00:00Z"  # unchanged
