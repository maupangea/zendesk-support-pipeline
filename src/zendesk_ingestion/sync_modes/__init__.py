"""Sync mode strategies and the shared SyncContext."""

from __future__ import annotations

from zendesk_ingestion.sync_modes.base import AbstractSyncMode, SyncContext
from zendesk_ingestion.sync_modes.full_refresh_append import FullRefreshAppendSyncMode
from zendesk_ingestion.sync_modes.full_refresh_overwrite import FullRefreshOverwriteSyncMode
from zendesk_ingestion.sync_modes.incremental_append import IncrementalAppendSyncMode
from zendesk_ingestion.sync_modes.incremental_deduped import IncrementalDedupedSyncMode

__all__ = [
    "AbstractSyncMode",
    "FullRefreshAppendSyncMode",
    "FullRefreshOverwriteSyncMode",
    "IncrementalAppendSyncMode",
    "IncrementalDedupedSyncMode",
    "SyncContext",
]
