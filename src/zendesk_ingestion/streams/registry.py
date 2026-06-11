"""Registry mapping stream names to stream classes."""

from __future__ import annotations

from zendesk_ingestion.streams.base import AbstractStream

# Populated after all stream modules are imported.
# Maps stream name (str) → stream class (type[AbstractStream])
STREAM_REGISTRY: dict[str, type[AbstractStream]] = {}


def register(cls: type[AbstractStream]) -> type[AbstractStream]:
    """Class decorator that registers a stream class in STREAM_REGISTRY."""
    STREAM_REGISTRY[cls.name] = cls
    return cls
