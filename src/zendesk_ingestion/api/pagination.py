from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CursorPage:
    """Result of a cursor-based paginated request."""

    records: list[dict[str, Any]]
    next_cursor: str | None  # None means last page
    end_of_stream: bool = False  # True when Zendesk signals no more incremental data


@dataclass
class OffsetPage:
    """Result of an offset/link-based paginated request."""

    records: list[dict[str, Any]]
    next_url: str | None  # None means last page
