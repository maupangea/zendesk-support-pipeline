"""FULL_REFRESH_OVERWRITE sync mode."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import structlog

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.sync_modes.base import AbstractSyncMode, SyncContext

log = structlog.get_logger()


class FullRefreshOverwriteSyncMode(AbstractSyncMode):
    mode = SyncMode.FULL_REFRESH_OVERWRITE

    def execute(
        self,
        records: Iterator[dict[str, Any]],
        ctx: SyncContext,
        final_cursor: str | None,
    ) -> int:
        bound_log = log.bind(stream=ctx.stream_name, run_id=ctx.run_id)
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
            )
            total += len(batch)

        # Atomic-ish promotion: delete old data, then promote staging.
        ctx.writer.delete_stream_prefix(ctx.stream_name)
        try:
            ctx.writer.commit_staging(stream_name=ctx.stream_name, run_id=ctx.run_id)
        except Exception:
            bound_log.critical(
                "full_refresh_overwrite_data_loss_window",
                detail=(
                    "delete_stream_prefix succeeded but commit_staging failed — "
                    "destination directory is empty"
                ),
            )
            raise

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
            bound_log.warning("full_refresh_state_update_failed", exc_info=True)

        bound_log.info("full_refresh_overwrite_complete", records=total, shards=shard + 1)
        return total
