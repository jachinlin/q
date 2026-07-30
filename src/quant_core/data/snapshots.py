"""Quality-gated, crash-recoverable publication of immutable snapshots."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Never
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import Severity, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    QualityRunId,
    SnapshotId,
)
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.orm import (
    AuditLogORM,
    QualityIssueORM,
    QualityRunDatasetORM,
    QualityRunORM,
    SnapshotDatasetORM,
    SnapshotORM,
)
from quant_core.persistence.repositories import MetadataRepository, SnapshotRecord


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
            "format_version": 1,
            "quality_run_id": str(quality_run_id),
            "snapshot_id": str(identifier),
            "status": SnapshotStatus.PUBLISHED.value,
        }
        manifest_bytes = canonical_json_bytes(manifest)
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        directory = self._snapshot_root / f"snapshot_id={identifier}"
        final_path = directory / "manifest.json"
        temporary_path = directory / f".{uuid.uuid4().hex}.manifest.tmp"
        directory.mkdir(parents=True, exist_ok=True)
        temporary_path.write_bytes(manifest_bytes)
        if hashlib.sha256(temporary_path.read_bytes()).hexdigest() != manifest_hash:
            temporary_path.unlink(missing_ok=True)
            raise OSError("temporary snapshot manifest failed integrity verification")

        try:
            with Session(self._repository.engine) as session, session.begin():
                _validate_quality_gate_in_transaction(
                    session, normalized, quality_run_id
                )
                session.add(
                    SnapshotORM(
                        id=str(identifier),
                        publication_fingerprint=fingerprint,
                        as_of=now.isoformat(),
                        status=SnapshotStatus.DRAFT.value,
                        manifest_path=final_path.as_posix(),
                        manifest_hash=manifest_hash,
                        quality_run_id=str(quality_run_id),
                        created_at=now.isoformat(),
                        published_at=None,
                    )
                )
                session.flush()
                for dataset, version_id in normalized.items():
                    session.add(
                        SnapshotDatasetORM(
                            snapshot_id=str(identifier),
                            dataset=dataset,
                            dataset_version_id=str(version_id),
                        )
                    )
                session.flush()
                temporary_path.replace(final_path)
                self._after_manifest_replace()
                snapshot_row = session.get(SnapshotORM, str(identifier))
                if snapshot_row is None:
                    raise RuntimeError("draft snapshot disappeared during publication")
                snapshot_row.status = SnapshotStatus.PUBLISHED.value
                snapshot_row.published_at = now.isoformat()
                session.add(
                    AuditLogORM(
                        action="SNAPSHOT_PUBLISHED",
                        object_type="snapshot",
                        object_id=str(identifier),
                        details_json=canonical_json_bytes(
                            {"manifest_hash": manifest_hash}
                        ).decode("utf-8"),
                        created_at=now.isoformat(),
                    )
                )
                session.flush()
        finally:
            temporary_path.unlink(missing_ok=True)
        return identifier

    def recover(self) -> None:
        """Reconcile temporary/final manifests with durable SQLite state."""
        snapshots = self._repository.list_snapshots()
        known_paths = {snapshot.manifest_path.resolve() for snapshot in snapshots}
        for snapshot in snapshots:
            if snapshot.status is SnapshotStatus.PUBLISHED:
                self._verify_published(snapshot)
                continue
            self._recover_draft(snapshot)

        if self._snapshot_root.exists():
            for final_path in self._snapshot_root.rglob("manifest.json"):
                if final_path.resolve() not in known_paths:
                    final_path.unlink()
            for temporary_path in self._snapshot_root.rglob("*.tmp"):
                temporary_path.unlink()

    def _recover_draft(self, snapshot: SnapshotRecord) -> None:
        final_path = snapshot.manifest_path
        candidates = [final_path, *sorted(final_path.parent.glob("*.tmp"))]
        valid = next(
            (
                candidate
                for candidate in candidates
                if candidate.is_file()
                and hashlib.sha256(candidate.read_bytes()).hexdigest()
                == snapshot.manifest_hash
                and self._manifest_matches_record(candidate, snapshot)
            ),
            None,
        )
        if valid is None:
            self._repository.delete_draft_snapshot(snapshot.id)
            for candidate in candidates:
                candidate.unlink(missing_ok=True)
            return
        if valid != final_path:
            valid.replace(final_path)
        now = _utc(self._clock()).isoformat()
        with Session(self._repository.engine) as session, session.begin():
            row = session.get(SnapshotORM, str(snapshot.id))
            if row is None or row.status != SnapshotStatus.DRAFT.value:
                _raise_snapshot_error(
                    "SNAP_RECOVERY_CONFLICT",
                    Severity.FATAL,
                    "snapshot state changed during recovery",
                    {"snapshot_id": str(snapshot.id)},
                )
            row.status = SnapshotStatus.PUBLISHED.value
            row.published_at = now
            session.flush()
        for candidate in final_path.parent.glob("*.tmp"):
            candidate.unlink()

    def _verify_published(self, snapshot: SnapshotRecord) -> None:
        if snapshot.status is not SnapshotStatus.PUBLISHED:
            _raise_snapshot_error(
                "SNAP_NOT_PUBLISHED",
                Severity.FATAL,
                "an existing publication fingerprint is not published",
                {"snapshot_id": str(snapshot.id)},
            )
        if not snapshot.manifest_path.is_file():
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISSING",
                Severity.FATAL,
                "published snapshot manifest is missing",
                {
                    "snapshot_id": str(snapshot.id),
                    "manifest_path": str(snapshot.manifest_path),
                },
            )
        if hashlib.sha256(
            snapshot.manifest_path.read_bytes()
        ).hexdigest() != snapshot.manifest_hash or not self._manifest_matches_record(
            snapshot.manifest_path, snapshot
        ):
            _raise_snapshot_error(
                "SNAP_MANIFEST_MISMATCH",
                Severity.FATAL,
                "published snapshot manifest fails catalog integrity checks",
                {"snapshot_id": str(snapshot.id)},
            )

    def _manifest_matches_record(self, path: Path, snapshot: SnapshotRecord) -> bool:
        try:
            manifest_bytes = path.read_bytes()
            manifest = json.loads(manifest_bytes.decode("utf-8"))
            datasets = manifest["datasets"]
        except (KeyError, OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
            return False
        if (
            not isinstance(manifest, dict)
            or not isinstance(datasets, dict)
            or canonical_json_bytes(manifest) != manifest_bytes
        ):
            return False
        if (
            manifest.get("format_version") != 1
            or manifest.get("snapshot_id") != str(snapshot.id)
            or manifest.get("quality_run_id") != str(snapshot.quality_run_id)
            or manifest.get("status") != SnapshotStatus.PUBLISHED.value
            or manifest.get("as_of") != snapshot.as_of.isoformat()
            or manifest.get("created_at") != snapshot.created_at.isoformat()
            or set(datasets) != set(snapshot.dataset_versions)
        ):
            return False
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
        return True


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


def _validate_quality_gate_in_transaction(
    session: Session,
    dataset_versions: Mapping[str, DatasetVersionId],
    quality_run_id: QualityRunId,
) -> None:
    run = session.get(QualityRunORM, str(quality_run_id))
    if run is None:
        raise KeyError(f"quality run does not exist: {quality_run_id}")
    checked_rows = session.scalars(
        select(QualityRunDatasetORM).where(
            QualityRunDatasetORM.quality_run_id == str(quality_run_id)
        )
    )
    checked = {
        row.dataset: DatasetVersionId.parse(row.dataset_version_id)
        for row in checked_rows
    }
    blocking = session.scalars(
        select(QualityIssueORM).where(
            QualityIssueORM.quality_run_id == str(quality_run_id),
            QualityIssueORM.severity.in_([Severity.SEVERE.value, Severity.FATAL.value]),
        )
    ).first()
    if run.status != "COMPLETED":
        _raise_snapshot_error(
            "SNAP_QUALITY_INCOMPLETE", Severity.FATAL, "quality run is not complete", {}
        )
    if checked != dict(dataset_versions):
        _raise_snapshot_error(
            "SNAP_QUALITY_SCOPE_MISMATCH",
            Severity.FATAL,
            "quality run does not cover the exact snapshot version set",
            {},
        )
    if blocking is not None:
        _raise_snapshot_error(
            "SNAP_QUALITY_BLOCKED",
            Severity.FATAL,
            "blocking quality issues prevent snapshot publication",
            {},
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("snapshot clock must return a timezone-aware timestamp")
    return value.astimezone(UTC)


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
