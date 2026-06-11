from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


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
