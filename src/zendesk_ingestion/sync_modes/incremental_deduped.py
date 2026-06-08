"""INCREMENTAL_DEDUPED sync mode."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import structlog

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.sync_modes.base import AbstractSyncMode, SyncContext

log = structlog.get_logger()

# Threshold above which we emit a warning about in-memory dedup
_LARGE_DEDUP_THRESHOLD = 1_000_000


class IncrementalDedupedSyncMode(AbstractSyncMode):
    mode = SyncMode.INCREMENTAL_DEDUPED

    def execute(
        self,
        records: Iterator[dict[str, Any]],
        ctx: SyncContext,
        final_cursor: str | None,
    ) -> int:
        bound_log = log.bind(stream=ctx.stream_name, run_id=ctx.run_id)
        partition = _today_partition()
        total = 0
        shard = 0
        batch: list[dict[str, Any]] = []
        all_records: list[dict[str, Any]] = []

        for record in records:
            batch.append(record)
            all_records.append(record)
            if len(batch) >= ctx.batch_size_records:
                ctx.writer.write_batch(
                    records=batch,
                    schema=ctx.schema,
                    stream_name=ctx.stream_name,
                    run_id=ctx.run_id,
                    sync_mode=self.mode,
                    shard=shard,
                    partition=partition,
                )
                total += len(batch)
                shard += 1
                batch = []

        if batch:
            ctx.writer.write_batch(
                records=batch,
                schema=ctx.schema,
                stream_name=ctx.stream_name,
                run_id=ctx.run_id,
                sync_mode=self.mode,
                shard=shard,
                partition=partition,
            )
            total += len(batch)

        if len(all_records) > _LARGE_DEDUP_THRESHOLD:
            bound_log.warning(
                "deduped_in_memory_large",
                records=len(all_records),
                threshold=_LARGE_DEDUP_THRESHOLD,
            )

        # Write deduped shard(s) to staging — full set; downstream can dedupe via
        # COPY INTO with QUALIFY ROW_NUMBER. Shard by batch_size_records.
        dedup_shard = 0
        for i in range(0, len(all_records), ctx.batch_size_records):
            chunk = all_records[i : i + ctx.batch_size_records]
            ctx.writer.write_deduped_staging(
                records=chunk,
                schema=ctx.schema,
                stream_name=ctx.stream_name,
                run_id=ctx.run_id,
                shard=dedup_shard,
            )
            dedup_shard += 1

        ctx.writer.commit_staging(stream_name=ctx.stream_name, run_id=ctx.run_id)

        if final_cursor is not None:
            ctx.state.commit_cursor(
                connector_id=ctx.connector_id,
                stream_name=ctx.stream_name,
                run_id=ctx.run_id,
                new_cursor=final_cursor,
                records_synced=total,
            )
        bound_log.info(
            "incremental_deduped_complete",
            records=total,
            dedup_shards=dedup_shard,
        )
        return total


def _today_partition() -> dict[str, str]:
    now = datetime.now(tz=UTC)
    return {"year": f"{now.year:04d}", "month": f"{now.month:02d}", "day": f"{now.day:02d}"}
