"""Atomic publication of immutable raw Parquet partitions."""

import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
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
from quant_core.data.storage import resolved_storage_root, validate_storage_path
from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail, QuantError

_PROVIDER_OR_DATASET = re.compile(r"[a-z0-9_-]+\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9._-]+\Z")


class _PartitionLock:
    """A Windows-compatible inter-process lock backed by atomic directory creation."""

    _OWNER_FILE = "owner.json"
    _TOKEN = re.compile(r"[0-9a-f]{32}\Z")

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
        self._token: str | None = None

    def __enter__(self) -> Self:
        """Acquire the lock, recovering only a demonstrably dead stale owner."""
        if self._owned:
            raise RuntimeError("partition lock is already owned")
        token = uuid.uuid4().hex
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            temporary_path = self._path.parent / f".locktmp-{token}-{uuid.uuid4().hex}"
            try:
                temporary_path.mkdir()
                self._owner_path_for(temporary_path).write_text(
                    json.dumps(
                        {"pid": os.getpid(), "token": token},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    encoding="utf-8",
                )
            except Exception:
                self._remove_private_directory(temporary_path)
                raise
            try:
                temporary_path.rename(self._path)
            except FileExistsError:
                self._remove_private_directory(temporary_path)
                self._reclaim_stale_lock(token)
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"timed out waiting for partition lock: {self._path}"
                    )
                time.sleep(self._poll_seconds)
            except Exception:
                self._remove_private_directory(temporary_path)
                raise
            else:
                self._owned = True
                self._token = token
                return self

    def __exit__(self, *_: object) -> None:
        """Atomically detach and remove only this owner's tokenized directory."""
        if not self._owned:
            return
        token = self._token
        if token is None:
            raise RuntimeError("partition lock ownership token is missing")
        tombstone = self._path.parent / f"{self._path.name}.release-{token}"
        try:
            owner = self._read_owner(self._path)
            if owner != (os.getpid(), token):
                raise RuntimeError("partition lock ownership changed before release")
            self._path.rename(tombstone)
            self._remove_token_directory(tombstone, token)
        finally:
            self._owned = False
            self._token = None

    def _owner_path_for(self, directory: Path) -> Path:
        return directory / self._OWNER_FILE

    def _reclaim_stale_lock(self, claimant_token: str) -> None:
        """Atomically claim a stale dead/invalid lock before deleting its directory."""
        try:
            age_seconds = time.time() - self._path.stat().st_mtime
        except FileNotFoundError:
            return
        if age_seconds < self._stale_after_seconds:
            return
        owner = self._read_owner(self._path)
        if owner is not None and self._process_is_alive(owner[0]):
            return
        tombstone = self._path.parent / (
            f"{self._path.name}.stale-{claimant_token}-{uuid.uuid4().hex}"
        )
        try:
            self._path.rename(tombstone)
        except FileNotFoundError:
            return
        if owner is None:
            self._remove_confirmed_stale_directory(tombstone)
        else:
            self._remove_token_directory(tombstone, owner[1])

    @classmethod
    def _read_owner(cls, directory: Path) -> tuple[int, str] | None:
        try:
            owner = json.loads(
                (directory / cls._OWNER_FILE).read_text(encoding="utf-8")
            )
            if not isinstance(owner, Mapping) or set(owner) != {"pid", "token"}:
                return None
            process_id = owner["pid"]
            token = owner["token"]
            if (
                type(process_id) is not int
                or not isinstance(token, str)
                or cls._TOKEN.fullmatch(token) is None
            ):
                return None
            return process_id, token
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None

    @classmethod
    def _remove_token_directory(cls, path: Path, expected_token: str) -> None:
        """Delete a claimed directory only while its owner token still matches."""
        owner = cls._read_owner(path)
        if owner is None or owner[1] != expected_token:
            raise RuntimeError("partition lock tombstone ownership changed")
        children = tuple(path.iterdir())
        if {child.name for child in children} != {cls._OWNER_FILE}:
            raise RuntimeError("partition lock directory contains unexpected paths")
        (path / cls._OWNER_FILE).unlink()
        path.rmdir()

    @classmethod
    def _remove_confirmed_stale_directory(cls, path: Path) -> None:
        """Delete only the malformed lock directory atomically claimed as stale."""
        children = tuple(path.iterdir())
        if {child.name for child in children} - {cls._OWNER_FILE}:
            raise RuntimeError("stale partition lock contains unexpected paths")
        (path / cls._OWNER_FILE).unlink(missing_ok=True)
        path.rmdir()

    @classmethod
    def _remove_private_directory(cls, path: Path) -> None:
        """Clean an unpublished temporary directory created by this acquisition."""
        try:
            (path / cls._OWNER_FILE).unlink(missing_ok=True)
            path.rmdir()
        except FileNotFoundError:
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
        self._raw_root = resolved_storage_root(raw_root)

    @property
    def root(self) -> Path:
        return self._raw_root

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
        dataset_dir = (
            self._raw_root / f"provider={batch.provider}" / f"dataset={batch.dataset}"
        )
        partition_dir = (
            dataset_dir
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
        validate_storage_path(self._raw_root, dataset_dir)
        dataset_dir.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._raw_root, dataset_dir)
        lock_path = self._identity_lock_path(dataset_dir, run_id, request_hash)
        validate_storage_path(self._raw_root, lock_path)
        with _PartitionLock(lock_path):
            existing = self._find_existing_partition(published, dataset_dir, run_id)
            if existing is not None:
                return existing
            validate_storage_path(self._raw_root, partition_dir)
            partition_dir.mkdir(parents=True, exist_ok=True)
            validate_storage_path(self._raw_root, partition_dir)
            if data_path.exists() or manifest_path.exists():
                return self._existing_or_conflict(
                    published,
                    data_path=data_path,
                    manifest_path=manifest_path,
                    run_id=run_id,
                )

            data_temp = partition_dir / f".{uuid.uuid4().hex}.parquet.tmp"
            manifest_temp = partition_dir / f".{uuid.uuid4().hex}.manifest.tmp"
            data_installed = False
            manifest_installed = False
            try:
                validate_storage_path(self._raw_root, data_temp)
                validate_storage_path(self._raw_root, manifest_temp)
                pq.write_table(table, data_temp, compression="zstd")
                validate_storage_path(self._raw_root, data_temp, require_file=True)
                self._verify_data_file(
                    data_temp,
                    content_hash=content_hash,
                    schema_fingerprint=schema_fingerprint,
                    row_count=table.num_rows,
                )
                manifest_temp.write_text(
                    json.dumps(
                        self._manifest(published),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    encoding="utf-8",
                )
                validate_storage_path(self._raw_root, manifest_temp, require_file=True)
                validate_storage_path(self._raw_root, data_path)
                data_temp.replace(data_path)
                data_installed = True
                validate_storage_path(self._raw_root, data_path, require_file=True)
                validate_storage_path(self._raw_root, manifest_path)
                manifest_temp.replace(manifest_path)
                manifest_installed = True
                validate_storage_path(self._raw_root, manifest_path, require_file=True)
                return published
            finally:
                if data_temp.exists():
                    data_temp.unlink()
                if manifest_temp.exists():
                    manifest_temp.unlink()
                if not manifest_installed and data_installed and data_path.exists():
                    data_path.unlink()

    @staticmethod
    def _validate_path_segment(
        value: str, label: str, pattern: re.Pattern[str]
    ) -> None:
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

    @staticmethod
    def _identity_lock_path(dataset_dir: Path, run_id: str, request_hash: str) -> Path:
        identity = hashlib.sha256(f"{run_id}\0{request_hash}".encode()).hexdigest()
        return dataset_dir / f".{identity}.lock"

    def _find_existing_partition(
        self,
        published: PublishedPartition,
        dataset_dir: Path,
        run_id: str,
    ) -> PublishedPartition | None:
        manifest_name = f"{published.request_hash}.manifest.json"
        data_name = f"{published.request_hash}.parquet"
        directories = {
            path.parent
            for pattern in (
                f"ingest_date=*/run_id={run_id}/{manifest_name}",
                f"ingest_date=*/run_id={run_id}/{data_name}",
            )
            for path in dataset_dir.glob(pattern)
        }
        if not directories:
            return None
        if len(directories) != 1:
            self._raise_conflict(
                published, "request identity has multiple published partitions"
            )
        directory = directories.pop()
        return self._existing_or_conflict(
            published,
            data_path=directory / data_name,
            manifest_path=directory / manifest_name,
            run_id=run_id,
        )

    def _existing_or_conflict(
        self,
        published: PublishedPartition,
        *,
        data_path: Path,
        manifest_path: Path,
        run_id: str,
    ) -> PublishedPartition:
        validate_storage_path(self._raw_root, data_path)
        validate_storage_path(self._raw_root, manifest_path)
        if not data_path.is_file() or not manifest_path.is_file():
            self._raise_conflict(published, "partition is incomplete")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._raise_conflict(published, "manifest is unreadable", error)
        immutable_manifest = self._manifest(published)
        immutable_manifest.pop("retrieved_at")
        if (
            not isinstance(manifest, Mapping)
            or set(manifest) != {*immutable_manifest, "retrieved_at"}
            or any(
                manifest.get(key) != value for key, value in immutable_manifest.items()
            )
        ):
            self._raise_conflict(published, "content or metadata differs")
        try:
            retrieved_value = manifest["retrieved_at"]
            if not isinstance(retrieved_value, str):
                raise TypeError("retrieved_at is not a string")
            retrieved_at = datetime.fromisoformat(retrieved_value)
            if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
                raise ValueError("retrieved_at is not timezone-aware")
            retrieved_at = retrieved_at.astimezone(UTC)
        except (KeyError, TypeError, ValueError) as error:
            self._raise_conflict(published, "retrieval metadata is invalid", error)
        if (
            data_path.parent != manifest_path.parent
            or manifest_path.parent.name != f"run_id={run_id}"
            or manifest_path.parent.parent.name
            != f"ingest_date={retrieved_at.date().isoformat()}"
        ):
            self._raise_conflict(
                published, "retrieval metadata does not match its path"
            )
        try:
            self._verify_data_file(
                data_path,
                content_hash=published.content_hash,
                schema_fingerprint=published.schema_fingerprint,
                row_count=published.row_count,
            )
        except (OSError, pa.ArrowException, ValueError) as error:
            self._raise_conflict(
                published, "published data fails integrity checks", error
            )
        return PublishedPartition(
            provider=published.provider,
            dataset=published.dataset,
            request=published.request,
            retrieved_at=retrieved_at,
            data_path=data_path,
            manifest_path=manifest_path,
            request_hash=published.request_hash,
            content_hash=published.content_hash,
            schema_fingerprint=published.schema_fingerprint,
            row_count=published.row_count,
        )

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
