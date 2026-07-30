"""Atomic publication of immutable raw Parquet partitions."""

import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Mapping
from datetime import UTC
from pathlib import Path
from typing import Self

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import (
    JsonValue,
    PublishedPartition,
    RawBatch,
    canonical_json_bytes,
)
from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail, QuantError

_PROVIDER_OR_DATASET = re.compile(r"[a-z0-9_-]+\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9._-]+\Z")


class _PartitionLock:
    """A Windows-compatible inter-process lock backed by atomic directory creation."""

    _OWNER_FILE = "owner.json"

    def __init__(
        self,
        path: Path,
        *,
        timeout_seconds: float = 30.0,
        poll_seconds: float = 0.05,
        stale_after_seconds: float = 300.0,
    ) -> None:
        self._path = path
        self._timeout_seconds = timeout_seconds
        self._poll_seconds = poll_seconds
        self._stale_after_seconds = stale_after_seconds
        self._owned = False

    def __enter__(self) -> Self:
        """Acquire the lock, recovering only a demonstrably dead stale owner."""
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            try:
                self._path.mkdir()
            except FileExistsError:
                self._reclaim_stale_lock()
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for partition lock: {self._path}")
                time.sleep(self._poll_seconds)
            else:
                try:
                    self._owner_path.write_text(
                        json.dumps({"pid": os.getpid()}), encoding="utf-8"
                    )
                except Exception:
                    self._path.rmdir()
                    raise
                self._owned = True
                return self

    def __exit__(self, *_: object) -> None:
        """Release only the lock directory created by this context."""
        if not self._owned:
            return
        try:
            self._owner_path.unlink(missing_ok=True)
            self._path.rmdir()
        finally:
            self._owned = False

    @property
    def _owner_path(self) -> Path:
        return self._path / self._OWNER_FILE

    def _reclaim_stale_lock(self) -> None:
        """Remove a lock only after its age and dead owner make it safe to reclaim."""
        try:
            age_seconds = time.time() - self._path.stat().st_mtime
        except FileNotFoundError:
            return
        if age_seconds < self._stale_after_seconds:
            return
        try:
            owner = json.loads(self._owner_path.read_text(encoding="utf-8"))
            process_id = owner["pid"]
        except (KeyError, OSError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
            return
        if not isinstance(process_id, int) or self._process_is_alive(process_id):
            return
        try:
            self._owner_path.unlink()
            self._path.rmdir()
        except FileNotFoundError:
            return
        except OSError:
            return

    @staticmethod
    def _process_is_alive(process_id: int) -> bool:
        try:
            os.kill(process_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True


class RawPartitionStore:
    """Publishes raw batches with a manifest as the atomic visibility marker."""

    def __init__(self, raw_root: Path) -> None:
        self._raw_root = raw_root

    def publish(self, batch: RawBatch, *, run_id: str) -> PublishedPartition:
        """Write, verify, and atomically publish one immutable raw partition."""
        self._validate_path_segment(batch.provider, "provider", _PROVIDER_OR_DATASET)
        self._validate_path_segment(batch.dataset, "dataset", _PROVIDER_OR_DATASET)
        self._validate_path_segment(run_id, "run_id", _RUN_ID)
        if ".." in run_id:
            raise ValueError("run_id must not contain '..'")

        request_hash = hashlib.sha256(canonical_json_bytes(batch.request)).hexdigest()
        table = self._table_from_batch(batch)
        content_hash = self._content_hash(table)
        schema_fingerprint = self._schema_fingerprint(table.schema)
        partition_dir = (
            self._raw_root
            / f"provider={batch.provider}"
            / f"dataset={batch.dataset}"
            / f"ingest_date={batch.retrieved_at.astimezone(UTC).date().isoformat()}"
            / f"run_id={run_id}"
        )
        data_path = partition_dir / f"{request_hash}.parquet"
        manifest_path = partition_dir / f"{request_hash}.manifest.json"
        published = PublishedPartition(
            provider=batch.provider,
            dataset=batch.dataset,
            request=batch.request,
            retrieved_at=batch.retrieved_at,
            data_path=data_path,
            manifest_path=manifest_path,
            request_hash=request_hash,
            content_hash=content_hash,
            schema_fingerprint=schema_fingerprint,
            row_count=table.num_rows,
        )
        manifest = self._manifest(published)

        partition_dir.mkdir(parents=True, exist_ok=True)
        lock_path = partition_dir / f".{request_hash}.lock"
        with _PartitionLock(lock_path):
            if data_path.exists() or manifest_path.exists():
                return self._existing_or_conflict(published, manifest)

            data_temp = partition_dir / f".{uuid.uuid4().hex}.parquet.tmp"
            manifest_temp = partition_dir / f".{uuid.uuid4().hex}.manifest.tmp"
            data_installed = False
            manifest_installed = False
            try:
                pq.write_table(table, data_temp, compression="zstd")
                self._verify_data_file(
                    data_temp,
                    content_hash=content_hash,
                    schema_fingerprint=schema_fingerprint,
                    row_count=table.num_rows,
                )
                manifest_temp.write_text(
                    json.dumps(
                        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    ),
                    encoding="utf-8",
                )
                data_temp.replace(data_path)
                data_installed = True
                manifest_temp.replace(manifest_path)
                manifest_installed = True
                return published
            finally:
                if data_temp.exists():
                    data_temp.unlink()
                if manifest_temp.exists():
                    manifest_temp.unlink()
                if not manifest_installed and data_installed and data_path.exists():
                    data_path.unlink()

    @staticmethod
    def _validate_path_segment(value: str, label: str, pattern: re.Pattern[str]) -> None:
        if not pattern.fullmatch(value):
            raise ValueError(f"{label} contains unsupported characters")

    @staticmethod
    def _table_from_batch(batch: RawBatch) -> pa.Table:
        if len(set(batch.schema)) != len(batch.schema):
            raise ValueError("schema must not contain duplicate column names")
        expected_keys = set(batch.schema)
        rows: list[dict[str, JsonValue]] = []
        for row in batch.rows:
            if set(row) != expected_keys:
                raise ValueError("row keys must match schema exactly")
            rows.append({column: row[column] for column in batch.schema})
        if not rows:
            return pa.table({column: [] for column in batch.schema})
        return pa.Table.from_pylist(rows)

    @staticmethod
    def _content_hash(table: pa.Table) -> str:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()

    @staticmethod
    def _schema_fingerprint(schema: pa.Schema) -> str:
        return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()

    @classmethod
    def _verify_data_file(
        cls,
        path: Path,
        *,
        content_hash: str,
        schema_fingerprint: str,
        row_count: int,
    ) -> None:
        table = pq.read_table(path)
        if (
            cls._content_hash(table) != content_hash
            or cls._schema_fingerprint(table.schema) != schema_fingerprint
            or table.num_rows != row_count
        ):
            raise ValueError("written Parquet file did not pass integrity verification")

    def _existing_or_conflict(
        self,
        published: PublishedPartition,
        expected_manifest: Mapping[str, object],
    ) -> PublishedPartition:
        if not published.data_path.is_file() or not published.manifest_path.is_file():
            self._raise_conflict(published, "partition is incomplete")
        try:
            manifest = json.loads(published.manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._raise_conflict(published, "manifest is unreadable", error)
        if manifest != expected_manifest:
            self._raise_conflict(published, "content or metadata differs")
        try:
            self._verify_data_file(
                published.data_path,
                content_hash=published.content_hash,
                schema_fingerprint=published.schema_fingerprint,
                row_count=published.row_count,
            )
        except (OSError, pa.ArrowException, ValueError) as error:
            self._raise_conflict(published, "published data fails integrity checks", error)
        return published

    @staticmethod
    def _manifest(published: PublishedPartition) -> dict[str, object]:
        return {
            "provider": published.provider,
            "dataset": published.dataset,
            "request_hash": published.request_hash,
            "content_hash": published.content_hash,
            "row_count": published.row_count,
            "schema_fingerprint": published.schema_fingerprint,
            "retrieved_at": published.retrieved_at.isoformat(),
        }

    @staticmethod
    def _raise_conflict(
        published: PublishedPartition,
        reason: str,
        cause: Exception | None = None,
    ) -> None:
        detail = ErrorDetail(
            code="raw_partition_conflict",
            severity=Severity.SEVERE,
            message=f"raw partition already exists: {reason}",
            context={
                "data_path": str(published.data_path),
                "manifest_path": str(published.manifest_path),
                "request_hash": published.request_hash,
            },
            remediation="publish a distinct partition or investigate the existing data",
            retryable=False,
        )
        if cause is None:
            raise QuantError(detail)
        raise QuantError(detail) from cause
