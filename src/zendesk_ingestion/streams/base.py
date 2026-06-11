"""Abstract base class for all Zendesk streams."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import structlog

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.transform.fivetran import FivetranTransformer


class AbstractStream(ABC):
    # Override in each subclass — used as S3 path component and DynamoDB sort key
    name: str

    # Zendesk API path for this stream (may be None for derived streams)
    endpoint: str | None = None

    # Field used as cursor for incremental syncs (None = no incremental support)
    cursor_field: str | None = None

    # Fields that uniquely identify a record (used for deduplication key)
    primary_key: list[str] = ["id"]

    # Default sync mode — overridden per-stream in connector.yaml
    default_sync_mode: SyncMode = SyncMode.INCREMENTAL_DEDUPED

    # Whether this stream is derived from a parent stream's response.
    # If set, the orchestrator will skip the direct API call and pass
    # parent_records instead.
    parent_stream: str | None = None

    def __init__(self, transformer: FivetranTransformer) -> None:
        self._transformer = transformer
        self._log = structlog.get_logger().bind(stream=self.name)

    @abstractmethod
    def get_schema(self) -> pa.Schema:
        """Return the pyarrow schema for this stream's output Parquet files."""

    @abstractmethod
    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Yield transformed records ready for Parquet serialization.
        For derived streams, `parent_records` contains the parent's raw API records.
        For API streams, `client` and `cursor` are used directly.
        Must apply self._transformer.transform_record() to each record before yielding.
        """

    def get_final_cursor(self, client: ZendeskClient) -> str | None:
        """
        Return the cursor value to persist after a successful sync.
        Default: return client.last_cursor (set by paginate_incremental).
        Override for streams that derive cursor differently.
        """
        return client.last_cursor
