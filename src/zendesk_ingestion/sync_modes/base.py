"""Abstract base for sync mode strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.writers.s3 import S3Writer


@dataclass
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
