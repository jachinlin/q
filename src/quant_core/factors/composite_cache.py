"""Atomic storage for canonical metadata-only composite factor manifests."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.partitions import _PartitionLock
from quant_core.data.storage import resolved_storage_root, validate_storage_path
from quant_core.factors.base import validate_sha256
from quant_core.factors.cache import (
    _compact_sha256,
    _fsync_directory,
    _fsync_file,
)


class CompositeManifestCache:
    """Atomically install and revalidate an immutable canonical JSON manifest."""

    def __init__(self, root: Path, *, manifest_fields: frozenset[str]) -> None:
        self._root = resolved_storage_root(root)
        self._root.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._root, self._root)
        self._manifest_fields = manifest_fields

    def load(self, composite_key: str) -> tuple[Mapping[str, object], Path] | None:
        validate_sha256(composite_key, "composite_key")
        entry_path = self._root / composite_key
        validate_storage_path(self._root, entry_path)
        if not entry_path.exists():
            return None
        manifest_path = entry_path / "manifest.json"
        self._validate_entry(entry_path, manifest_path)
        if {item.name for item in entry_path.iterdir()} != {"manifest.json"}:
            raise ValueError("composite cache entry contains unexpected paths")
        try:
            raw = manifest_path.read_bytes()
            loaded = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("composite manifest is unreadable") from error
        if not isinstance(loaded, Mapping) or set(loaded) != self._manifest_fields:
            raise ValueError("composite manifest has invalid fields")
        try:
            canonical = canonical_json_bytes(cast(JsonValue, loaded))
        except (TypeError, ValueError) as error:
            raise ValueError("composite manifest is not canonical JSON") from error
        if raw != canonical:
            raise ValueError("composite manifest is not canonical JSON")
        self._validate_entry(entry_path, manifest_path)
        return cast(Mapping[str, object], loaded), manifest_path

    def publish(
        self, manifest: Mapping[str, JsonValue]
    ) -> tuple[Mapping[str, object], Path]:
        composite_key = _required_string(manifest, "composite_key")
        validate_sha256(composite_key, "composite_key")
        entry_path = self._root / composite_key
        lock_path = self._root / f".{composite_key}.lock"
        validate_storage_path(self._root, entry_path)
        validate_storage_path(self._root, lock_path)
        with _PartitionLock(lock_path):
            existing = self.load(composite_key)
            if existing is not None:
                if _required_string(existing[0], "content_hash") != _required_string(
                    manifest, "content_hash"
                ):
                    raise ValueError(
                        "composite cache conflict: existing content differs"
                    )
                return existing
            self._recover_staging(composite_key)
            staging_token = _compact_sha256(composite_key)
            while True:
                staging_path = self._root / (
                    f".{staging_token}.{uuid.uuid4().hex[:16]}.tmp"
                )
                validate_storage_path(self._root, staging_path)
                try:
                    staging_path.mkdir()
                except FileExistsError:
                    continue
                break
            manifest_path = staging_path / "manifest.json"
            installed = False
            try:
                validate_storage_path(self._root, manifest_path)
                manifest_path.write_bytes(
                    canonical_json_bytes(cast(JsonValue, manifest))
                )
                validate_storage_path(self._root, manifest_path, require_file=True)
                _fsync_file(manifest_path)
                _fsync_directory(staging_path)
                if entry_path.exists():
                    raise ValueError(
                        "composite cache entry appeared during publication"
                    )
                staging_path.rename(entry_path)
                installed = True
                _fsync_directory(self._root)
                loaded = self.load(composite_key)
                if loaded is None:
                    raise ValueError("published composite cache entry is not visible")
                return loaded
            finally:
                if not installed:
                    manifest_path.unlink(missing_ok=True)
                    try:
                        staging_path.rmdir()
                    except FileNotFoundError:
                        pass

    def _recover_staging(self, composite_key: str) -> None:
        pattern = re.compile(
            rf"\.{re.escape(_compact_sha256(composite_key))}\."
            rf"[0-9a-f]{{16}}\.tmp\Z"
        )
        recovered = False
        for path in self._root.iterdir():
            if pattern.fullmatch(path.name) is None:
                continue
            validate_storage_path(self._root, path)
            if not path.is_dir():
                raise ValueError("stale composite staging path is not a directory")
            children = tuple(path.iterdir())
            if {item.name for item in children} - {"manifest.json"}:
                raise ValueError("stale composite staging contains unexpected paths")
            for child in children:
                validate_storage_path(self._root, child, require_file=True)
                child.unlink()
            path.rmdir()
            recovered = True
        if recovered:
            _fsync_directory(self._root)

    def _validate_entry(self, entry_path: Path, manifest_path: Path) -> None:
        validate_storage_path(self._root, entry_path)
        if not entry_path.is_dir():
            raise ValueError("composite cache entry is not a directory")
        validate_storage_path(self._root, manifest_path, require_file=True)
        if manifest_path.stat(follow_symlinks=False).st_nlink != 1:
            raise ValueError("composite manifest has an additional hard link")


def _required_string(mapping: Mapping[str, object], field: str) -> str:
    value = mapping[field]
    if not isinstance(value, str):
        raise TypeError(f"composite manifest {field} must be a string")
    return value


__all__ = ["CompositeManifestCache"]
