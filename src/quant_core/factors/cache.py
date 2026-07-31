"""Content-addressed atomic Parquet cache for factor artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.partitions import _PartitionLock
from quant_core.data.storage import resolved_storage_root, validate_storage_path
from quant_core.domain.identifiers import SnapshotId
from quant_core.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    FactorArtifact,
    FactorContext,
    FactorSpec,
    canonical_factor_ref,
    factor_table_content_hash,
    thaw_json,
    validate_sha256,
)

_PRIMARY_KEY = ("trade_date", "instrument_id", "factor_id", "factor_version")
_MANIFEST_FIELDS = {
    "cache_key",
    "code_hash",
    "content_hash",
    "data_path",
    "dependency_hashes",
    "end",
    "factor_id",
    "factor_version",
    "parameters",
    "row_count",
    "schema_fingerprint",
    "snapshot_id",
    "start",
    "universe_hash",
}


@dataclass(frozen=True, slots=True)
class _ArtifactMetadata:
    factor_ref: str
    cache_key: str
    content_hash: str
    row_count: int
    snapshot_id: SnapshotId
    universe_hash: str
    start: date
    end: date

    def artifact(self, table: pa.Table) -> FactorArtifact:
        return FactorArtifact(
            factor_ref=self.factor_ref,
            cache_key=self.cache_key,
            content_hash=self.content_hash,
            row_count=self.row_count,
            snapshot_id=self.snapshot_id,
            universe_hash=self.universe_hash,
            start=self.start,
            end=self.end,
            table=table,
        )


def build_cache_key(
    spec: FactorSpec,
    ctx: FactorContext,
    code_hash: str,
    dependency_hashes: Mapping[str, str],
) -> str:
    """Hash every reproducibility input into one canonical feature address."""
    payload = _cache_key_payload(spec, ctx, code_hash, dependency_hashes)
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class FeatureCache:
    """Publish and revalidate immutable exact-schema factor artifacts."""

    def __init__(self, root: Path) -> None:
        self._root = resolved_storage_root(root)
        self._root.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._root, self._root)

    @property
    def root(self) -> Path:
        return self._root

    def load(self, cache_key: str) -> FactorArtifact | None:
        """Return a fully revalidated cache hit, or ``None`` when absent."""
        validate_sha256(cache_key, "cache_key")
        entry_path = self._root / cache_key
        validate_storage_path(self._root, entry_path)
        if not entry_path.exists():
            return None
        data_path = entry_path / "data.parquet"
        manifest_path = entry_path / "manifest.json"
        self._validate_entry_paths(entry_path, data_path, manifest_path)
        if {path.name for path in entry_path.iterdir()} != {
            "data.parquet",
            "manifest.json",
        }:
            raise ValueError("feature cache entry contains unexpected paths")

        try:
            raw_manifest = manifest_path.read_bytes()
            loaded = json.loads(raw_manifest)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("feature cache manifest is unreadable") from error
        self._validate_entry_paths(entry_path, data_path, manifest_path)
        if not isinstance(loaded, Mapping) or set(loaded) != _MANIFEST_FIELDS:
            raise ValueError("feature cache manifest has invalid fields")
        manifest = cast(Mapping[str, object], loaded)
        try:
            canonical = canonical_json_bytes(cast(JsonValue, manifest))
        except (TypeError, ValueError) as error:
            raise ValueError("feature cache manifest is not canonical JSON") from error
        if raw_manifest != canonical:
            raise ValueError("feature cache manifest is not canonical JSON")

        try:
            metadata = _parse_manifest(manifest, cache_key)
        except (TypeError, ValueError) as error:
            raise ValueError("feature cache manifest is invalid") from error
        try:
            table = pq.read_table(data_path)
            frame = cast(pl.DataFrame, pl.from_arrow(table))
            canonical_frame = _validated_frame(
                frame,
                factor_id=metadata.factor_ref.partition("@")[0],
                factor_version=metadata.factor_ref.partition("@")[2],
            )
        except (OSError, pa.ArrowException, TypeError, ValueError) as error:
            raise ValueError("feature cache Parquet integrity check failed") from error
        self._validate_entry_paths(entry_path, data_path, manifest_path)
        if (
            frame.select(_PRIMARY_KEY).rows()
            != canonical_frame.select(_PRIMARY_KEY).rows()
        ):
            raise ValueError("feature cache Parquet is not canonically sorted")
        schema_fingerprint = _schema_fingerprint(table.schema)
        content_hash = _content_hash(table)
        expected_schema = _manifest_string(manifest, "schema_fingerprint")
        if (
            table.num_rows != metadata.row_count
            or schema_fingerprint != expected_schema
            or content_hash != metadata.content_hash
        ):
            raise ValueError("feature cache Parquet integrity metadata differs")
        self._validate_entry_paths(entry_path, data_path, manifest_path)
        return metadata.artifact(table)

    def publish(
        self,
        cache_key: str,
        frame: pl.LazyFrame,
        *,
        spec: FactorSpec,
        ctx: FactorContext,
        code_hash: str,
        dependency_hashes: Mapping[str, str],
    ) -> FactorArtifact:
        """Validate, stage, fsync, and atomically install one immutable entry."""
        expected_key = build_cache_key(spec, ctx, code_hash, dependency_hashes)
        validate_sha256(cache_key, "cache_key")
        if cache_key != expected_key:
            raise ValueError("cache_key does not match factor inputs")
        if not isinstance(frame, pl.LazyFrame):
            raise TypeError("factor output must be a polars LazyFrame")
        canonical_frame = _validated_frame(
            frame.collect(), factor_id=spec.factor_id, factor_version=spec.version
        )
        table = canonical_frame.to_arrow().combine_chunks()
        row_count = table.num_rows
        content_hash = _content_hash(table)
        schema_fingerprint = _schema_fingerprint(table.schema)
        entry_path = self._root / cache_key
        lock_path = self._root / f".{cache_key}.lock"
        validate_storage_path(self._root, entry_path)
        validate_storage_path(self._root, lock_path)

        with _PartitionLock(lock_path):
            self._recover_stale_staging(cache_key)
            existing = self.load(cache_key)
            if existing is not None:
                if (
                    existing.content_hash != content_hash
                    or existing.row_count != row_count
                ):
                    raise ValueError("feature cache conflict: existing content differs")
                _fsync_directory(self._root)
                return existing
            staging_path = self._root / f".{cache_key}.{uuid.uuid4().hex}.tmp"
            data_path = staging_path / "data.parquet"
            manifest_path = staging_path / "manifest.json"
            installed = False
            try:
                validate_storage_path(self._root, staging_path)
                staging_path.mkdir()
                validate_storage_path(self._root, staging_path)
                validate_storage_path(self._root, data_path)
                validate_storage_path(self._root, manifest_path)
                pq.write_table(table, data_path, compression="zstd")
                validate_storage_path(self._root, data_path, require_file=True)
                _fsync_file(data_path)
                written = pq.read_table(data_path)
                validate_storage_path(self._root, staging_path)
                validate_storage_path(self._root, data_path, require_file=True)
                if (
                    written.num_rows != row_count
                    or _schema_fingerprint(written.schema) != schema_fingerprint
                    or _content_hash(written) != content_hash
                ):
                    raise ValueError(
                        "written feature Parquet failed integrity verification"
                    )
                manifest = _manifest(
                    cache_key=cache_key,
                    spec=spec,
                    ctx=ctx,
                    code_hash=code_hash,
                    dependency_hashes=dependency_hashes,
                    content_hash=content_hash,
                    schema_fingerprint=schema_fingerprint,
                    row_count=row_count,
                )
                manifest_path.write_bytes(canonical_json_bytes(manifest))
                validate_storage_path(self._root, manifest_path, require_file=True)
                _fsync_file(manifest_path)
                _fsync_directory(staging_path)
                validate_storage_path(self._root, staging_path)
                validate_storage_path(self._root, data_path, require_file=True)
                validate_storage_path(self._root, manifest_path, require_file=True)
                if entry_path.exists():
                    raise ValueError("feature cache entry appeared during publication")
                staging_path.rename(entry_path)
                installed = True
                validate_storage_path(self._root, entry_path)
                _fsync_directory(self._root)
                artifact = self.load(cache_key)
                if artifact is None:
                    raise ValueError("published feature cache entry is not visible")
                return artifact
            finally:
                if not installed:
                    data_path.unlink(missing_ok=True)
                    manifest_path.unlink(missing_ok=True)
                    try:
                        staging_path.rmdir()
                    except FileNotFoundError:
                        pass

    def _recover_stale_staging(self, cache_key: str) -> None:
        """Remove only dead same-key staging directories while holding its lock."""
        staging_name = re.compile(rf"\.{re.escape(cache_key)}\.[0-9a-f]{{32}}\.tmp\Z")
        recovered = False
        for path in self._root.iterdir():
            if staging_name.fullmatch(path.name) is None:
                continue
            validate_storage_path(self._root, path)
            if not path.is_dir():
                raise ValueError("stale feature staging path is not a directory")
            children = tuple(path.iterdir())
            if {child.name for child in children} - {"data.parquet", "manifest.json"}:
                raise ValueError("stale feature staging contains unexpected paths")
            for child in children:
                validate_storage_path(self._root, child, require_file=True)
                child.unlink()
            path.rmdir()
            recovered = True
        if recovered:
            _fsync_directory(self._root)

    def _validate_entry_paths(
        self, entry_path: Path, data_path: Path, manifest_path: Path
    ) -> None:
        """Recheck containment and reparse state around every cache read."""
        validate_storage_path(self._root, entry_path)
        if not entry_path.is_dir():
            raise ValueError("feature cache entry is not a directory")
        validate_storage_path(self._root, data_path, require_file=True)
        validate_storage_path(self._root, manifest_path, require_file=True)
        if data_path.stat(follow_symlinks=False).st_nlink != 1:
            raise ValueError("feature cache data file has an additional hard link")
        if manifest_path.stat(follow_symlinks=False).st_nlink != 1:
            raise ValueError("feature cache manifest file has an additional hard link")


def _cache_key_payload(
    spec: FactorSpec,
    ctx: FactorContext,
    code_hash: str,
    dependency_hashes: Mapping[str, str],
) -> dict[str, JsonValue]:
    validate_sha256(code_hash, "code_hash")
    if not isinstance(dependency_hashes, Mapping):
        raise TypeError("dependency_hashes must be a mapping")
    normalized: dict[str, str] = {}
    for reference, artifact_hash in dependency_hashes.items():
        canonical_ref = canonical_factor_ref(reference)
        validate_sha256(artifact_hash, f"dependency hash for {canonical_ref}")
        if canonical_ref in normalized:
            raise ValueError(f"duplicate dependency hash for {canonical_ref}")
        normalized[canonical_ref] = artifact_hash
    expected = set(spec.dependencies)
    if set(normalized) != expected:
        raise ValueError("dependency hashes must match FactorSpec dependencies")
    parameters = thaw_json(spec.parameters)
    if not isinstance(parameters, Mapping):
        raise TypeError("factor parameters must be a mapping")
    return {
        "code_hash": code_hash,
        "dependency_hashes": dict(sorted(normalized.items())),
        "end": ctx.end.isoformat(),
        "factor_id": spec.factor_id,
        "factor_version": spec.version,
        "parameters": parameters,
        "snapshot_id": str(ctx.snapshot_id),
        "start": ctx.start.isoformat(),
        "universe_hash": ctx.universe_hash,
    }


def _manifest(
    *,
    cache_key: str,
    spec: FactorSpec,
    ctx: FactorContext,
    code_hash: str,
    dependency_hashes: Mapping[str, str],
    content_hash: str,
    schema_fingerprint: str,
    row_count: int,
) -> dict[str, JsonValue]:
    manifest = _cache_key_payload(spec, ctx, code_hash, dependency_hashes)
    manifest.update(
        {
            "cache_key": cache_key,
            "content_hash": content_hash,
            "data_path": "data.parquet",
            "row_count": row_count,
            "schema_fingerprint": schema_fingerprint,
        }
    )
    return manifest


def _parse_manifest(
    manifest: Mapping[str, object], expected_key: str
) -> _ArtifactMetadata:
    cache_key = _manifest_string(manifest, "cache_key")
    if cache_key != expected_key:
        raise ValueError("feature cache manifest key does not match its path")
    validate_sha256(cache_key, "cache_key")
    factor_id = _manifest_string(manifest, "factor_id")
    factor_version = _manifest_string(manifest, "factor_version")
    canonical_factor_ref(f"{factor_id}@{factor_version}")
    code_hash = validate_sha256(_manifest_string(manifest, "code_hash"), "code_hash")
    content_hash = validate_sha256(
        _manifest_string(manifest, "content_hash"), "content_hash"
    )
    universe_hash = validate_sha256(
        _manifest_string(manifest, "universe_hash"), "universe_hash"
    )
    schema_fingerprint = validate_sha256(
        _manifest_string(manifest, "schema_fingerprint"), "schema_fingerprint"
    )
    del schema_fingerprint
    snapshot_id = SnapshotId.parse(_manifest_string(manifest, "snapshot_id"))
    start = _parse_date(_manifest_string(manifest, "start"), "start")
    end = _parse_date(_manifest_string(manifest, "end"), "end")
    row_count = manifest["row_count"]
    if type(row_count) is not int or row_count < 0:
        raise ValueError("feature cache manifest row_count is invalid")
    if manifest["data_path"] != "data.parquet":
        raise ValueError("feature cache manifest path is invalid")
    parameters = manifest["parameters"]
    dependencies = manifest["dependency_hashes"]
    if not isinstance(parameters, Mapping) or not isinstance(dependencies, Mapping):
        raise TypeError("feature cache manifest key material is invalid")
    dependency_hashes: dict[str, str] = {}
    for reference, value in dependencies.items():
        if not isinstance(reference, str) or not isinstance(value, str):
            raise TypeError("feature cache dependency hashes are invalid")
        canonical_factor_ref(reference)
        dependency_hashes[reference] = validate_sha256(
            value, f"dependency hash for {reference}"
        )
    key_payload: dict[str, JsonValue] = {
        "code_hash": code_hash,
        "dependency_hashes": dict(sorted(dependency_hashes.items())),
        "end": end.isoformat(),
        "factor_id": factor_id,
        "factor_version": factor_version,
        "parameters": cast(Mapping[str, JsonValue], parameters),
        "snapshot_id": str(snapshot_id),
        "start": start.isoformat(),
        "universe_hash": universe_hash,
    }
    derived_key = hashlib.sha256(canonical_json_bytes(key_payload)).hexdigest()
    if derived_key != cache_key:
        raise ValueError("feature cache manifest does not match its cache key")
    return _ArtifactMetadata(
        factor_ref=f"{factor_id}@{factor_version}",
        cache_key=cache_key,
        content_hash=content_hash,
        row_count=row_count,
        snapshot_id=snapshot_id,
        universe_hash=universe_hash,
        start=start,
        end=end,
    )


def _validated_frame(
    frame: pl.DataFrame, *, factor_id: str, factor_version: str
) -> pl.DataFrame:
    if frame.schema != FACTOR_OUTPUT_SCHEMA:
        raise ValueError(f"factor output schema must be exactly {FACTOR_OUTPUT_SCHEMA}")
    required_non_null = (
        "trade_date",
        "instrument_id",
        "factor_id",
        "factor_version",
        "available_at",
        "is_valid",
    )
    if any(frame.select(required_non_null).null_count().row(0)):
        raise ValueError("factor output identity and audit fields must not be null")
    if frame.select(pl.struct(_PRIMARY_KEY).is_duplicated().any()).item():
        raise ValueError("factor output contains a duplicate primary key")
    if frame.filter(pl.col("factor_id") != factor_id).height:
        raise ValueError("factor output factor_id does not match FactorSpec")
    if frame.filter(pl.col("factor_version") != factor_version).height:
        raise ValueError("factor output factor_version does not match FactorSpec")
    invalid_value = (
        pl.col("value").is_null()
        | pl.col("value").is_nan()
        | pl.col("value").is_infinite()
    )
    if frame.filter(pl.col("is_valid") & invalid_value).height:
        raise ValueError("valid factor output value must be finite")
    return frame.sort(_PRIMARY_KEY, maintain_order=True)


def _content_hash(table: pa.Table) -> str:
    return factor_table_content_hash(table)


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as file:
        os.fsync(file.fileno())


def _fsync_directory(path: Path) -> None:
    """Flush directory metadata on POSIX and Windows before/after publication."""
    if os.name == "nt":
        _fsync_windows_directory(path)
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_windows_directory(path: Path) -> None:
    """Flush one directory handle using the Windows backup-semantics API."""
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    flush_file_buffers = kernel32.FlushFileBuffers
    flush_file_buffers.argtypes = [wintypes.HANDLE]
    flush_file_buffers.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    handle = create_file(
        str(path),
        0x40000000,
        0x00000001 | 0x00000002 | 0x00000004,
        None,
        3,
        0x02000000,
        None,
    )
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        if not flush_file_buffers(handle):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        close_handle(handle)


def _manifest_string(manifest: Mapping[str, object], field: str) -> str:
    value = manifest[field]
    if not isinstance(value, str):
        raise TypeError(f"feature cache manifest {field} is invalid")
    return value


def _parse_date(value: str, field: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"feature cache manifest {field} is invalid") from error
    if parsed.isoformat() != value:
        raise ValueError(f"feature cache manifest {field} is invalid")
    return parsed
