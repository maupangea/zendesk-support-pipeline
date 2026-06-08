"""S3 Parquet writer with sync-mode-aware path layout and staging promotion."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.exceptions import S3WriteError

log = structlog.get_logger()


@dataclass
class WriteResult:
    s3_paths: list[str]  # all files written (staging or final)
    records_written: int
    shards: int


class S3Writer:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        region: str = "us-east-1",
        batch_size_records: int = 50_000,
        batch_size_mb: int = 128,
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._region = region
        self._batch_size_records = batch_size_records
        self._batch_size_mb = batch_size_mb
        self._s3 = boto3.client("s3", region_name=region)

    def write_batch(
        self,
        records: list[dict[str, Any]],
        schema: pa.Schema,
        stream_name: str,
        run_id: str,
        sync_mode: SyncMode,
        shard: int = 0,
        partition: dict[str, str] | None = None,
    ) -> WriteResult:
        """Serialize to Parquet and upload to the path appropriate for the sync mode."""
        table = pa.Table.from_pylist(records, schema=schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")  # type: ignore[no-untyped-call]
        buffer.seek(0)

        key = self._key_for(
            sync_mode=sync_mode,
            stream_name=stream_name,
            run_id=run_id,
            shard=shard,
            partition=partition,
        )
        try:
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=buffer.getvalue())
        except Exception as exc:  # noqa: BLE001
            raise S3WriteError(f"Failed to upload {key}: {exc}") from exc

        return WriteResult(
            s3_paths=[f"s3://{self._bucket}/{key}"],
            records_written=len(records),
            shards=1,
        )

    def commit_staging(self, stream_name: str, run_id: str) -> None:
        """
        Promote staged objects under {prefix}/{stream}/_staging/{run_id}/ to final.
        For top-level staging files → {prefix}/{stream}/data/...
        For nested deduped/ → {prefix}/{stream}/deduped/...
        """
        staging_prefix = f"{self._prefix}/{stream_name}/_staging/{run_id}/"
        keys = self._list_keys(staging_prefix)
        if not keys:
            log.warning(
                "commit_staging_no_objects",
                stream=stream_name,
                run_id=run_id,
                prefix=staging_prefix,
            )
            return

        for src_key in keys:
            rel = src_key[len(staging_prefix) :]
            if rel.startswith("deduped/"):
                dst_key = f"{self._prefix}/{stream_name}/deduped/{rel[len('deduped/') :]}"
            else:
                dst_key = f"{self._prefix}/{stream_name}/data/{rel}"
            self._s3.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": src_key},
                Key=dst_key,
            )

        # Delete staging objects (batched 1000 at a time per API limit).
        self._delete_keys(keys)

    def delete_stream_prefix(self, stream_name: str) -> None:
        """Bulk delete {prefix}/{stream}/data/ for FULL_REFRESH_OVERWRITE."""
        data_prefix = f"{self._prefix}/{stream_name}/data/"
        keys = self._list_keys(data_prefix)
        if not keys:
            return
        self._delete_keys(keys)

    # ----- internal helpers ----------------------------------------------

    def _key_for(
        self,
        *,
        sync_mode: SyncMode,
        stream_name: str,
        run_id: str,
        shard: int,
        partition: dict[str, str] | None,
    ) -> str:
        if sync_mode in (SyncMode.INCREMENTAL_APPEND, SyncMode.FULL_REFRESH_APPEND):
            part_path = _partition_path(partition)
            return (
                f"{self._prefix}/{stream_name}/data/{part_path}"
                f"{stream_name}_{run_id}_{shard}.parquet"
            )
        if sync_mode == SyncMode.FULL_REFRESH_OVERWRITE:
            return f"{self._prefix}/{stream_name}/_staging/{run_id}/{stream_name}_{shard}.parquet"
        if sync_mode == SyncMode.INCREMENTAL_DEDUPED:
            # Default path for the raw incremental records is partitioned same as
            # incremental_append. The deduped shards are written through
            # `write_deduped_staging`. The strategy class is responsible for picking
            # the right entry point.
            part_path = _partition_path(partition)
            return (
                f"{self._prefix}/{stream_name}/data/{part_path}"
                f"{stream_name}_{run_id}_{shard}.parquet"
            )
        raise ValueError(f"Unknown sync_mode: {sync_mode}")

    def write_deduped_staging(
        self,
        records: list[dict[str, Any]],
        schema: pa.Schema,
        stream_name: str,
        run_id: str,
        shard: int = 0,
    ) -> WriteResult:
        """Write a deduped shard to the staging deduped/ directory."""
        table = pa.Table.from_pylist(records, schema=schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")  # type: ignore[no-untyped-call]

        key = (
            f"{self._prefix}/{stream_name}/_staging/{run_id}/deduped/{stream_name}_{shard}.parquet"
        )
        try:
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=buffer.getvalue())
        except Exception as exc:  # noqa: BLE001
            raise S3WriteError(f"Failed to upload {key}: {exc}") from exc

        return WriteResult(
            s3_paths=[f"s3://{self._bucket}/{key}"],
            records_written=len(records),
            shards=1,
        )

    def _list_keys(self, prefix: str) -> list[str]:
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def _delete_keys(self, keys: list[str]) -> None:
        for i in range(0, len(keys), 1000):
            chunk = keys[i : i + 1000]
            self._s3.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": [{"Key": k} for k in chunk]},
            )


def _partition_path(partition: dict[str, str] | None) -> str:
    if not partition:
        return ""
    parts = [f"{k}={v}" for k, v in partition.items()]
    return "/".join(parts) + "/"
