"""FULL_REFRESH_APPEND sync mode."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import structlog

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.sync_modes.base import AbstractSyncMode, SyncContext

log = structlog.get_logger()


class FullRefreshAppendSyncMode(AbstractSyncMode):
    mode = SyncMode.FULL_REFRESH_APPEND

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

        for record in records:
            batch.append(record)
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

        # No cursor for full refresh, but still mark success + last_sync_at.
        # commit_cursor needs a non-null cursor argument; pass a synthetic timestamp.
        now_iso = datetime.now(tz=UTC).isoformat()
        try:
            ctx.state.commit_cursor(
                connector_id=ctx.connector_id,
                stream_name=ctx.stream_name,
                run_id=ctx.run_id,
                new_cursor=now_iso,
                records_synced=total,
            )
        except Exception:  # noqa: BLE001
            # Cursor advancement isn't critical for full refresh; log and continue.
            bound_log.warning("full_refresh_state_update_failed", exc_info=True)

        bound_log.info("full_refresh_append_complete", records=total, shards=shard + 1)
        return total


def _today_partition() -> dict[str, str]:
    now = datetime.now(tz=UTC)
    return {"year": f"{now.year:04d}", "month": f"{now.month:02d}", "day": f"{now.day:02d}"}
