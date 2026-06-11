"""Fivetran-compatible record transformer."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any

FIVETRAN_SYNCED_COL = "_fivetran_synced"
FIVETRAN_DELETED_COL = "_fivetran_deleted"
FIVETRAN_ID_COL = "_fivetran_id"


# ISO 8601 with Z, +HH:MM, +HHMM, or no offset
_ISO_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)


class FivetranTransformer:
    """
    Applies Fivetran-compatible transformations to raw Zendesk API records.
    Each stream can subclass and override `transform_record` for stream-specific logic.
    """

    def __init__(self, stream_name: str, deleted_field: str | None = None) -> None:
        self._stream_name = stream_name
        self._deleted_field = deleted_field
        self._synced_at = datetime.now(tz=UTC)

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        out = self._flatten(record)
        out = self._cast_timestamps(out)
        out[FIVETRAN_SYNCED_COL] = self._synced_at
        out[FIVETRAN_DELETED_COL] = self._is_deleted(record)
        return out

    def add_synthetic_id(self, record: dict[str, Any], *key_fields: str) -> dict[str, Any]:
        """MD5 hex digest of concatenated key field values."""
        key = ":".join(str(record.get(f, "")) for f in key_fields)
        record[FIVETRAN_ID_COL] = hashlib.md5(key.encode()).hexdigest()
        return record

    def _flatten(self, record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        """Recursively flatten nested dicts using underscore-joined keys. Lists pass through."""
        result: dict[str, Any] = {}
        for key, value in record.items():
            new_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, dict):
                result.update(self._flatten(value, new_key))
            else:
                result[new_key] = value
        return result

    def _cast_timestamps(self, record: dict[str, Any]) -> dict[str, Any]:
        """Convert ISO 8601 timestamp strings to timezone-aware datetimes."""
        for key, value in record.items():
            if isinstance(value, str) and _ISO_TIMESTAMP_RE.match(value):
                record[key] = _parse_iso(value)
        return record

    def _is_deleted(self, record: dict[str, Any]) -> bool:
        if self._deleted_field is None:
            return False
        return record.get(self._deleted_field) == "deleted"


def _parse_iso(value: str) -> datetime:
    """Parse an ISO 8601 string. Trailing Z is treated as +00:00."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)
