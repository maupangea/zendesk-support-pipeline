"""Stream sync orchestration.

Fans stream syncs out across a ``ThreadPoolExecutor``. Execution happens in two
waves so that derived streams (those with a ``parent_stream``) always see their
parent's records:

* Wave 1 — every stream without a parent, plus any parent that a requested
  derived stream depends on but that was not itself requested (fetched
  *cache-only*: paginated and cached, but not written or cursor-advanced).
* Wave 2 — the requested derived streams, fed from the wave-1 cache.

Two correctness properties this module guarantees that a naive single-wave
fan-out does not:

* **Per-stream API client.** ``ZendeskClient.last_cursor`` is mutable per-client
  state. A single shared client mutated by concurrent threads would let one
  stream's cursor bleed into another's commit, so each stream gets its own
  client (the thread-safe ``RateLimiter`` is shared).
* **Cursor captured after pagination.** ``get_final_cursor`` reads
  ``client.last_cursor``, which is only set as the record generator is consumed.
  Records are therefore materialized before the cursor is read. This also backs
  the parent-record cache and matches the deduped sync mode, which already
  buffers all records in memory.
"""

from __future__ import annotations

import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import structlog

import zendesk_ingestion.streams  # noqa: F401  (import registers every stream class)
from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.api.rate_limiter import RateLimiter
from zendesk_ingestion.config.models import ConnectorConfig, SyncMode
from zendesk_ingestion.metrics import MetricsClient
from zendesk_ingestion.state.dynamodb import StateManager
from zendesk_ingestion.streams.registry import STREAM_REGISTRY
from zendesk_ingestion.sync_modes import (
    AbstractSyncMode,
    FullRefreshAppendSyncMode,
    FullRefreshOverwriteSyncMode,
    IncrementalAppendSyncMode,
    IncrementalDedupedSyncMode,
    SyncContext,
)
from zendesk_ingestion.transform.fivetran import FivetranTransformer
from zendesk_ingestion.writers.s3 import S3Writer

SYNC_MODE_CLASSES: dict[SyncMode, type[AbstractSyncMode]] = {
    SyncMode.INCREMENTAL_APPEND: IncrementalAppendSyncMode,
    SyncMode.INCREMENTAL_DEDUPED: IncrementalDedupedSyncMode,
    SyncMode.FULL_REFRESH_APPEND: FullRefreshAppendSyncMode,
    SyncMode.FULL_REFRESH_OVERWRITE: FullRefreshOverwriteSyncMode,
}


@dataclass
class StreamResult:
    stream_name: str
    success: bool
    records_written: int = 0
    duration_s: float = 0.0
    error: str | None = None
    cache_only: bool = False  # fetched only to feed derived streams; nothing written


@dataclass
class RunReport:
    run_id: str
    connector_id: str
    results: list[StreamResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results)

    @property
    def total_records(self) -> int:
        return sum(r.records_written for r in self.results)

    @property
    def failed_streams(self) -> list[str]:
        return [r.stream_name for r in self.results if not r.success]


@dataclass
class StreamPlan:
    """The set of streams to sync and the wave each belongs to."""

    requested: list[str]
    wave1: list[str]
    wave2: list[str]
    auto_parents: list[str]  # parents fetched cache-only to feed requested children
    needed_parents: list[str]  # every parent whose records must be cached this run


@dataclass
class _SyncOutcome:
    """Internal: a stream's result plus its records to cache for derived streams."""

    result: StreamResult
    records_for_cache: list[dict[str, Any]] | None = None


def plan_streams(config: ConnectorConfig, requested: list[str] | None) -> StreamPlan:
    """Resolve which streams to sync and split them into dependency waves."""
    resolved = _resolve_streams(config, requested)
    derived = [s for s in resolved if STREAM_REGISTRY[s].parent_stream is not None]
    needed_parents = {
        p for p in (STREAM_REGISTRY[s].parent_stream for s in derived) if p is not None
    }
    auto_parents = sorted(needed_parents - set(resolved))
    wave1 = sorted(
        {s for s in resolved if STREAM_REGISTRY[s].parent_stream is None} | set(auto_parents)
    )
    wave2 = sorted(derived)
    return StreamPlan(
        requested=resolved,
        wave1=wave1,
        wave2=wave2,
        auto_parents=auto_parents,
        needed_parents=sorted(needed_parents),
    )


def run(config: ConnectorConfig, streams: list[str] | None = None) -> RunReport:
    """Sync the requested streams (or all enabled streams) and return a report.

    Args:
        config: Fully resolved ConnectorConfig (secrets already loaded).
        streams: Optional subset of stream names. If None, sync all enabled streams.
    """
    run_id = str(uuid.uuid4())
    log = structlog.get_logger().bind(connector_id=config.connector_id, run_id=run_id)
    log.info("run_started")

    plan = plan_streams(config, streams)
    log.info(
        "streams_resolved",
        count=len(plan.requested),
        wave1=plan.wave1,
        wave2=plan.wave2,
        auto_parents=plan.auto_parents,
    )

    rate_limiter = RateLimiter(config.runtime.api_rate_limit)
    state_mgr = StateManager(config.state.dynamodb_table, config.state.region)
    writer = S3Writer(
        bucket=config.s3.bucket,
        prefix=config.s3.prefix,
        region=config.s3.region,
        batch_size_records=config.runtime.batch_size_records,
        batch_size_mb=config.runtime.batch_size_mb,
    )
    metrics = MetricsClient(config.connector_id, region=config.s3.region)
    report = RunReport(run_id=run_id, connector_id=config.connector_id)

    needed = set(plan.needed_parents)
    auto = set(plan.auto_parents)
    parent_cache: dict[str, list[dict[str, Any]]] = {}

    def run_wave(names: list[str], cache_in: dict[str, list[dict[str, Any]]]) -> None:
        if not names:
            return
        with ThreadPoolExecutor(max_workers=config.runtime.max_parallelism) as executor:
            futures: dict[Future[_SyncOutcome], str] = {
                executor.submit(
                    _sync_stream,
                    stream_name=name,
                    config=config,
                    run_id=run_id,
                    rate_limiter=rate_limiter,
                    state_mgr=state_mgr,
                    writer=writer,
                    parent_records=cache_in.get(STREAM_REGISTRY[name].parent_stream or ""),
                    cache_records=name in needed,
                    write_output=name not in auto,
                ): name
                for name in names
            }
            # as_completed yields on the main thread, so cache writes are race-free.
            for future in as_completed(futures):
                outcome = future.result()
                res = outcome.result
                report.results.append(res)
                if outcome.records_for_cache is not None:
                    parent_cache[res.stream_name] = outcome.records_for_cache
                metrics.emit_stream_result(
                    res.stream_name, res.records_written, res.duration_s, 0 if res.success else 1
                )
                log.info(
                    "stream_completed",
                    stream=res.stream_name,
                    success=res.success,
                    records=res.records_written,
                    cache_only=res.cache_only,
                )

    run_wave(plan.wave1, {})
    run_wave(plan.wave2, parent_cache)

    log.info(
        "run_completed",
        success=report.success,
        total_records=report.total_records,
        failed=report.failed_streams,
    )
    return report


def _sync_stream(
    *,
    stream_name: str,
    config: ConnectorConfig,
    run_id: str,
    rate_limiter: RateLimiter,
    state_mgr: StateManager,
    writer: S3Writer,
    parent_records: list[dict[str, Any]] | None,
    cache_records: bool,
    write_output: bool,
) -> _SyncOutcome:
    """Execute one stream's sync cycle.

    Never propagates exceptions to the executor — always returns a ``_SyncOutcome``.
    When ``write_output`` is False the stream is fetched cache-only: records are
    paginated and (if ``cache_records``) returned for the parent cache, but nothing
    is written to S3 and no state is touched.
    """
    log = structlog.get_logger().bind(stream=stream_name, run_id=run_id)
    start = time.monotonic()

    stream_cls = STREAM_REGISTRY.get(stream_name)
    if stream_cls is None:
        return _SyncOutcome(
            StreamResult(stream_name, success=False, error="Stream class not found in registry")
        )

    stream_cfg = config.streams.get(stream_name)
    if write_output and stream_cfg is None:
        return _SyncOutcome(StreamResult(stream_name, success=False, error="No config found"))

    if stream_cls.parent_stream and parent_records is None:
        log.warning("derived_stream_missing_parent_records", parent=stream_cls.parent_stream)

    client = ZendeskClient(
        subdomain=config.zendesk_subdomain,
        email=config.zendesk_email,
        api_token=config.zendesk_api_token,
        rate_limiter=rate_limiter,
    )
    try:
        transformer = FivetranTransformer(stream_name)
        stream = stream_cls(transformer)

        state = state_mgr.get_state(config.connector_id, stream_name) if write_output else None
        cursor = state["cursor"] if state else None

        # Materialize so the cursor reflects completed pagination and the records
        # can be shared with derived streams (see module docstring).
        records = list(stream.get_records(client, cursor, parent_records))
        records_for_cache = records if cache_records else None

        if not write_output:
            duration = time.monotonic() - start
            log.info(
                "stream_cache_only_fetched", records=len(records), duration_s=round(duration, 2)
            )
            return _SyncOutcome(
                StreamResult(
                    stream_name,
                    success=True,
                    records_written=0,
                    duration_s=duration,
                    cache_only=True,
                ),
                records_for_cache=records_for_cache,
            )

        assert stream_cfg is not None  # guaranteed above when write_output is True
        schema = stream.get_schema()
        final_cursor = stream.get_final_cursor(client)

        state_mgr.begin_run(config.connector_id, stream_name, run_id, stream_cfg.sync_mode)
        sync_mode = SYNC_MODE_CLASSES[stream_cfg.sync_mode]()
        ctx = SyncContext(
            connector_id=config.connector_id,
            stream_name=stream_name,
            run_id=run_id,
            writer=writer,
            state=state_mgr,
            schema=schema,
            batch_size_records=config.runtime.batch_size_records,
        )
        records_written = sync_mode.execute(iter(records), ctx, final_cursor)

        duration = time.monotonic() - start
        log.info("stream_sync_success", records=records_written, duration_s=round(duration, 2))
        return _SyncOutcome(
            StreamResult(
                stream_name, success=True, records_written=records_written, duration_s=duration
            ),
            records_for_cache=records_for_cache,
        )

    except Exception as exc:
        duration = time.monotonic() - start
        log.error("stream_sync_failed", exc_info=True, duration_s=round(duration, 2))
        if write_output:
            try:
                state_mgr.mark_failed(config.connector_id, stream_name, run_id)
            except Exception:
                log.error("mark_failed_errored", exc_info=True)
        return _SyncOutcome(
            StreamResult(
                stream_name,
                success=False,
                duration_s=duration,
                error=str(exc),
                cache_only=not write_output,
            )
        )
    finally:
        client.close()


def _resolve_streams(config: ConnectorConfig, requested: list[str] | None) -> list[str]:
    """Return the sorted list of enabled stream names to sync.

    If ``requested`` is given, unknown names raise ``ValueError``; known-but-disabled
    names are dropped. Otherwise every enabled stream is returned.
    """
    all_enabled = [name for name, cfg in config.streams.items() if cfg.enabled]
    if requested:
        unknown = set(requested) - set(STREAM_REGISTRY)
        if unknown:
            raise ValueError(f"Unknown streams: {sorted(unknown)}")
        to_run = [s for s in requested if s in all_enabled]
    else:
        to_run = all_enabled
    return sorted(to_run)
