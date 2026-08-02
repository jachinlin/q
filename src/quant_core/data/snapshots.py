"""Quality-gated, crash-recoverable publication of immutable snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Never
from uuid import NAMESPACE_URL, uuid5

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import Severity, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    QualityRunId,
    SnapshotId,
)
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.repositories import (
    MetadataRepository,
    SnapshotRecord,
    SnapshotWriteSpec,
)

_TEMP_MANIFEST = re.compile(r"\.[0-9a-f]{32}\.manifest\.tmp\Z")
SNAPSHOT_MANIFEST_FORMAT_VERSION = 1
SNAPSHOT_MANIFEST_VERSION = "snapshot-manifest-v1"
_SNAPSHOT_MANIFEST_FIELDS = {
    "as_of",
    "created_at",
    "datasets",
    "format_version",
    "quality_run_id",
    "snapshot_id",
    "status",
}


class SnapshotPublisher:
    """Publish immutable manifests under a SQLite quality gate."""

    def __init__(
        self,
        repository: MetadataRepository,
        snapshot_root: Path,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        after_manifest_replace: Callable[[], None] = lambda: None,
    ) -> None:
        self._repository = repository
        self._snapshot_root = snapshot_root.resolve()
        self._clock = clock
        self._after_manifest_replace = after_manifest_replace

    def publish(
        self,
        dataset_versions: Mapping[str, DatasetVersionId],
        quality_run_id: QualityRunId,
    ) -> SnapshotId:
        """Publish one stable snapshot or return an existing identical publication."""
        normalized = dict(sorted(dataset_versions.items()))
        fingerprint = _publication_fingerprint(normalized, quality_run_id)
        identifier = SnapshotId(uuid5(NAMESPACE_URL, f"quant:snapshot:{fingerprint}"))
        existing = self._repository.find_snapshot_by_fingerprint(fingerprint)
        if existing is not None:
            self._verify_published(existing)
            return existing.id

        quality_run = self._repository.get_quality_run(quality_run_id)
        _validate_quality_gate(
            normalized,
            quality_run.status,
            quality_run.dataset_versions,
            quality_run.issues,
        )
        versions = {
            dataset: self._repository.get_dataset_version(version_id)
            for dataset, version_id in normalized.items()
        }
        now = _utc(self._clock())
        manifest: JsonValue = {
            "as_of": now.isoformat(),
            "created_at": now.isoformat(),
            "datasets": {
                dataset: {
                    "dataset_version_id": str(record.id),
                    "partitions": [
                        {
                            "content_hash": partition.content_hash,
                            "path": partition.path.resolve().as_posix(),
                            "row_count": partition.row_count,
                            "schema_fingerprint": partition.schema_fingerprint,
                        }
                        for partition in record.partitions
                    ],
                }
                for dataset, record in sorted(versions.items())
            },
            "format_version": SNAPSHOT_MANIFEST_FORMAT_VERSION,
            "quality_run_id": str(quality_run_id),
            "snapshot_id": str(identifier),
            "status": SnapshotStatus.PUBLISHED.value,
        }
        manifest_bytes = canonical_json_bytes(manifest)
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        directory, final_path = self._snapshot_paths(identifier)
        temporary_path = directory / f".{uuid.uuid4().hex}.manifest.tmp"
        directory.mkdir(parents=True, exist_ok=True)
        try:
            with temporary_path.open("xb") as temporary_file:
                temporary_file.write(manifest_bytes)
        except OSError:
            _remove_temporary_manifest(temporary_path)
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISMATCH",
                Severity.FATAL,
                "temporary snapshot manifest cannot be written",
                {"snapshot_id": str(identifier)},
            )
        try:
            readback_hash = hashlib.sha256(temporary_path.read_bytes()).hexdigest()
        except OSError:
            _remove_temporary_manifest(temporary_path)
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISMATCH",
                Severity.FATAL,
                "temporary snapshot manifest cannot be verified",
                {"snapshot_id": str(identifier)},
            )
        if temporary_path.is_symlink() or readback_hash != manifest_hash:
            _remove_temporary_manifest(temporary_path)
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISMATCH",
                Severity.FATAL,
                "temporary snapshot manifest failed integrity verification",
                {"snapshot_id": str(identifier)},
            )

        def install_manifest() -> None:
            temporary_path.replace(final_path)
            self._after_manifest_replace()

        try:
            snapshot = self._repository.publish_snapshot(
                SnapshotWriteSpec(
                    id=identifier,
                    publication_fingerprint=fingerprint,
                    as_of=now,
                    manifest_path=final_path,
                    manifest_hash=manifest_hash,
                    quality_run_id=quality_run_id,
                    dataset_versions=normalized,
                    created_at=now,
                ),
                install_manifest,
            )
        finally:
            temporary_path.unlink(missing_ok=True)
        self._verify_published(snapshot)
        return snapshot.id

    def recover(self) -> None:
        """Reconcile temporary/final manifests with durable SQLite state."""
        snapshots = self._repository.list_snapshots()
        known_ids = {snapshot.id for snapshot in snapshots}
        for snapshot in snapshots:
            self._snapshot_paths(snapshot.id, snapshot.manifest_path)
            if snapshot.status is SnapshotStatus.PUBLISHED:
                self._verify_published(snapshot)
                continue
            self._recover_draft(snapshot)

        if self._snapshot_root.is_dir():
            for directory in self._snapshot_root.iterdir():
                if not directory.name.startswith("snapshot_id="):
                    continue
                try:
                    identifier = SnapshotId.parse(
                        directory.name.removeprefix("snapshot_id=")
                    )
                except ValueError:
                    continue
                if identifier in known_ids:
                    continue
                _, final_path = self._snapshot_paths(identifier)
                final_path.unlink(missing_ok=True)
                for temporary_path in self._temporary_manifests(directory, identifier):
                    temporary_path.unlink(missing_ok=True)

    def verify_published(
        self,
        identifier: SnapshotId,
        dataset_versions: Mapping[str, DatasetVersionId],
        quality_run_id: QualityRunId,
    ) -> SnapshotRecord:
        """Revalidate a published snapshot and its complete quality-gated scope."""
        snapshot = self._repository.get_snapshot(identifier)
        if snapshot.quality_run_id != quality_run_id or dict(
            snapshot.dataset_versions
        ) != dict(dataset_versions):
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISMATCH",
                Severity.FATAL,
                "published snapshot does not match the checkpoint scope",
                {"snapshot_id": str(identifier)},
            )
        quality = self._repository.get_quality_run(quality_run_id)
        _validate_quality_gate(
            dataset_versions,
            quality.status,
            quality.dataset_versions,
            quality.issues,
        )
        self._verify_published(snapshot)
        return snapshot

    def _recover_draft(self, snapshot: SnapshotRecord) -> None:
        directory, final_path = self._snapshot_paths(
            snapshot.id, snapshot.manifest_path
        )
        candidates = [final_path, *self._temporary_manifests(directory, snapshot.id)]
        valid = next(
            (
                candidate
                for candidate in candidates
                if candidate.is_file()
                and self._manifest_matches_record(candidate, snapshot)
            ),
            None,
        )
        if valid is None:
            self._repository.discard_draft_snapshot(
                snapshot.id, "SNAP_MANIFEST_MISMATCH", _utc(self._clock())
            )
            for candidate in candidates:
                candidate.unlink(missing_ok=True)
            return

        def install_manifest() -> None:
            if valid != final_path:
                valid.replace(final_path)

        outcome = self._repository.recover_draft_snapshot(
            snapshot.id, install_manifest, _utc(self._clock())
        )
        if outcome == "DISCARDED":
            final_path.unlink(missing_ok=True)
        for candidate in self._temporary_manifests(directory, snapshot.id):
            candidate.unlink(missing_ok=True)

    def _verify_published(self, snapshot: SnapshotRecord) -> None:
        if snapshot.status is not SnapshotStatus.PUBLISHED:
            _raise_snapshot_error(
                "SNAP_NOT_PUBLISHED",
                Severity.FATAL,
                "an existing publication fingerprint is not published",
                {"snapshot_id": str(snapshot.id)},
            )
        _, manifest_path = self._snapshot_paths(snapshot.id, snapshot.manifest_path)
        if not manifest_path.is_file():
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISSING",
                Severity.FATAL,
                "published snapshot manifest is missing",
                {
                    "snapshot_id": str(snapshot.id),
                    "manifest_path": str(manifest_path),
                },
            )
        if not self._manifest_matches_record(manifest_path, snapshot):
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISMATCH",
                Severity.FATAL,
                "published snapshot manifest fails catalog integrity checks",
                {"snapshot_id": str(snapshot.id)},
            )

    def _manifest_matches_record(self, path: Path, snapshot: SnapshotRecord) -> bool:
        try:
            manifest_bytes = path.read_bytes()
            if hashlib.sha256(manifest_bytes).hexdigest() != snapshot.manifest_hash:
                return False
            manifest = json.loads(manifest_bytes.decode("utf-8"))
            if (
                not isinstance(manifest, dict)
                or set(manifest) != _SNAPSHOT_MANIFEST_FIELDS
            ):
                return False
            datasets = manifest["datasets"]
            canonical = canonical_json_bytes(manifest)
        except (
            KeyError,
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ):
            return False
        if not isinstance(datasets, dict) or canonical != manifest_bytes:
            return False
        if (
            manifest.get("format_version") != SNAPSHOT_MANIFEST_FORMAT_VERSION
            or manifest.get("snapshot_id") != str(snapshot.id)
            or manifest.get("quality_run_id") != str(snapshot.quality_run_id)
            or manifest.get("status") != SnapshotStatus.PUBLISHED.value
            or manifest.get("as_of") != snapshot.as_of.isoformat()
            or manifest.get("created_at") != snapshot.created_at.isoformat()
            or set(datasets) != set(snapshot.dataset_versions)
        ):
            return False
        try:
            for dataset, version_id in snapshot.dataset_versions.items():
                version = self._repository.get_dataset_version(version_id)
                expected = {
                    "dataset_version_id": str(version_id),
                    "partitions": [
                        {
                            "content_hash": partition.content_hash,
                            "path": partition.path.resolve().as_posix(),
                            "row_count": partition.row_count,
                            "schema_fingerprint": partition.schema_fingerprint,
                        }
                        for partition in version.partitions
                    ],
                }
                if datasets.get(dataset) != expected:
                    return False
        except (KeyError, OSError, TypeError, ValueError):
            return False
        return True

    def _snapshot_paths(
        self, identifier: SnapshotId, recorded_path: Path | None = None
    ) -> tuple[Path, Path]:
        directory = self._snapshot_root / f"snapshot_id={identifier}"
        final_path = directory / "manifest.json"
        resolved_directory = _normalized_resolved_path(directory.resolve(strict=False))
        try:
            resolved_directory.relative_to(self._snapshot_root)
        except (OSError, ValueError):
            self._raise_path_error(identifier)
        if resolved_directory != directory:
            self._raise_path_error(identifier)
        resolved_final = _normalized_resolved_path(final_path.resolve(strict=False))
        if final_path.is_symlink() or resolved_final != final_path:
            self._raise_path_error(identifier)
        if recorded_path is not None and recorded_path.absolute() != final_path:
            self._raise_path_error(identifier)
        return directory, final_path

    def _temporary_manifests(
        self, directory: Path, identifier: SnapshotId
    ) -> tuple[Path, ...]:
        if not directory.is_dir():
            return ()
        temporary_paths = tuple(
            sorted(
                path
                for path in directory.iterdir()
                if _TEMP_MANIFEST.fullmatch(path.name)
                and (path.is_file() or path.is_symlink())
            )
        )
        if any(
            path.is_symlink() or path.resolve(strict=False) != path
            for path in temporary_paths
        ):
            self._raise_path_error(identifier)
        return temporary_paths

    @staticmethod
    def _raise_path_error(identifier: SnapshotId) -> Never:
        _raise_snapshot_error(
            "SNAP_PATH_INVALID",
            Severity.FATAL,
            "snapshot catalog path escapes the configured snapshot root",
            {"snapshot_id": str(identifier)},
        )


def _publication_fingerprint(
    dataset_versions: Mapping[str, DatasetVersionId],
    quality_run_id: QualityRunId,
) -> str:
    payload: JsonValue = {
        "dataset_versions": {
            dataset: str(identifier)
            for dataset, identifier in sorted(dataset_versions.items())
        },
        "quality_run_id": str(quality_run_id),
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _normalized_resolved_path(path: Path) -> Path:
    """Normalize Windows' equivalent extended path spelling after resolution."""
    if os.name != "nt":
        return path
    value = str(path)
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return Path(value)


def _validate_quality_gate(
    dataset_versions: Mapping[str, DatasetVersionId],
    status: str,
    checked_versions: Mapping[str, DatasetVersionId],
    issues: tuple[object, ...],
) -> None:
    if status != "COMPLETED":
        _raise_snapshot_error(
            "SNAP_QUALITY_INCOMPLETE",
            Severity.FATAL,
            "quality run is not complete",
            {},
        )
    if dict(dataset_versions) != dict(checked_versions):
        _raise_snapshot_error(
            "SNAP_QUALITY_SCOPE_MISMATCH",
            Severity.FATAL,
            "quality run does not cover the exact snapshot version set",
            {},
        )
    blocking = [
        issue
        for issue in issues
        if getattr(issue, "severity", None) in (Severity.SEVERE, Severity.FATAL)
    ]
    if blocking:
        _raise_snapshot_error(
            "SNAP_QUALITY_BLOCKED",
            Severity.FATAL,
            "blocking quality issues prevent snapshot publication",
            {"blocking_issue_count": len(blocking)},
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("snapshot clock must return a timezone-aware timestamp")
    return value.astimezone(UTC)


def _remove_temporary_manifest(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _raise_snapshot_error(
    code: str,
    severity: Severity,
    message: str,
    context: Mapping[str, object],
) -> Never:
    raise QuantError(
        ErrorDetail(
            code=code,
            severity=severity,
            message=message,
            context=context,
            remediation="inspect quality metadata and snapshot storage before retrying",
            retryable=False,
        )
    )
