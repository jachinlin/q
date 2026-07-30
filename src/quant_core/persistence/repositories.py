"""Transactional repositories returning immutable domain-facing DTOs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.quality.models import QualityIssue, QualityRunSpec
from quant_core.domain.enums import DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    QualityRunId,
    SnapshotId,
)
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.orm import (
    DatasetPartitionORM,
    DatasetVersionORM,
    QualityIssueORM,
    QualityRunDatasetORM,
    QualityRunORM,
    SnapshotDatasetORM,
    SnapshotORM,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class DatasetPartitionSpec:
    """One immutable physical partition in a canonical dataset version."""

    content_hash: str
    path: Path
    schema_fingerprint: str
    row_count: int

    def __post_init__(self) -> None:
        _validate_hash(self.content_hash, "content_hash")
        _validate_hash(self.schema_fingerprint, "schema_fingerprint")
        if self.row_count < 0:
            raise ValueError("row_count must be non-negative")
        object.__setattr__(self, "path", self.path.resolve())


@dataclass(frozen=True, slots=True)
class DatasetVersionSpec:
    """Complete content description used to identify a dataset version."""

    dataset: DatasetKind
    source: str
    partitions: tuple[DatasetPartitionSpec, ...]
    start_date: date | None
    end_date: date | None
    created_run_id: str

    def __post_init__(self) -> None:
        if not self.source or not self.created_run_id:
            raise ValueError("source and created_run_id must not be empty")
        if not self.partitions:
            raise ValueError("dataset version must contain at least one partition")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")


@dataclass(frozen=True, slots=True)
class DatasetPartitionRecord:
    content_hash: str
    path: Path
    schema_fingerprint: str
    row_count: int


@dataclass(frozen=True, slots=True)
class DatasetVersionRecord:
    id: DatasetVersionId
    dataset: DatasetKind
    fingerprint: str
    source: str
    status: str
    partitions: tuple[DatasetPartitionRecord, ...]
    start_date: date | None
    end_date: date | None
    created_run_id: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class QualityRunRecord:
    id: QualityRunId
    status: str
    dataset_versions: Mapping[str, DatasetVersionId]
    started_at: datetime
    completed_at: datetime | None
    issues: tuple[QualityIssue, ...]


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    id: SnapshotId
    publication_fingerprint: str
    as_of: datetime
    status: SnapshotStatus
    manifest_path: Path
    manifest_hash: str
    quality_run_id: QualityRunId
    dataset_versions: Mapping[str, DatasetVersionId]
    created_at: datetime
    published_at: datetime | None


class MetadataRepository:
    """Own metadata transactions without leaking mapped SQLAlchemy objects."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    def register_dataset_version(
        self, spec: DatasetVersionSpec
    ) -> DatasetVersionRecord:
        """Idempotently register one complete version under a stable UUIDv5."""
        fingerprint = dataset_version_fingerprint(spec)
        identifier = DatasetVersionId(
            uuid5(
                NAMESPACE_URL,
                f"quant:dataset-version:{spec.dataset.value}:{fingerprint}",
            )
        )
        with Session(self._engine) as session, session.begin():
            existing = session.get(DatasetVersionORM, str(identifier))
            if existing is None:
                existing = DatasetVersionORM(
                    id=str(identifier),
                    dataset=spec.dataset.value,
                    fingerprint=fingerprint,
                    source=spec.source,
                    status=SnapshotStatus.DRAFT.value,
                    start_date=_date_text(spec.start_date),
                    end_date=_date_text(spec.end_date),
                    created_run_id=spec.created_run_id,
                    created_at=_timestamp(datetime.now(UTC)),
                )
                session.add(existing)
                session.flush()
                for ordinal, partition in enumerate(
                    _sorted_partitions(spec.partitions)
                ):
                    session.add(
                        DatasetPartitionORM(
                            dataset_version_id=str(identifier),
                            ordinal=ordinal,
                            content_hash=partition.content_hash,
                            path=partition.path.as_posix(),
                            schema_fingerprint=partition.schema_fingerprint,
                            row_count=partition.row_count,
                        )
                    )
                session.flush()
                existing.status = SnapshotStatus.PUBLISHED.value
                session.flush()
            elif (
                existing.dataset != spec.dataset.value
                or existing.fingerprint != fingerprint
                or existing.status != SnapshotStatus.PUBLISHED.value
            ):
                _raise_repository_conflict("dataset version UUID collision")
            return self._dataset_record(session, str(identifier))

    def get_dataset_version(self, identifier: DatasetVersionId) -> DatasetVersionRecord:
        with Session(self._engine) as session:
            return self._dataset_record(session, str(identifier))

    def count_dataset_versions(self) -> int:
        with Session(self._engine) as session:
            return (
                session.scalar(select(func.count()).select_from(DatasetVersionORM)) or 0
            )

    def register_quality_run(self, spec: QualityRunSpec) -> QualityRunRecord:
        """Persist a quality run and its exact version scope atomically."""
        identifier = QualityRunId.new()
        with Session(self._engine) as session, session.begin():
            normalized = self._validate_version_mapping(session, spec.dataset_versions)
            run = QualityRunORM(
                id=str(identifier),
                status="RUNNING",
                started_at=_timestamp(spec.started_at),
                completed_at=(
                    _timestamp(spec.completed_at)
                    if spec.completed_at is not None
                    else None
                ),
                created_at=_timestamp(datetime.now(UTC)),
            )
            session.add(run)
            session.flush()
            for dataset, version_id in normalized.items():
                session.add(
                    QualityRunDatasetORM(
                        quality_run_id=str(identifier),
                        dataset=dataset,
                        dataset_version_id=str(version_id),
                    )
                )
            for issue in spec.issues:
                session.add(
                    QualityIssueORM(
                        quality_run_id=str(identifier),
                        rule_id=issue.rule_id,
                        severity=issue.severity.value,
                        dataset=issue.dataset.value,
                        scope_json=_json_text(issue.scope),
                        actual_json=_json_text(issue.actual),
                        threshold_json=_json_text(issue.threshold),
                        message=issue.message,
                        remediation=issue.remediation,
                    )
                )
            session.flush()
            if spec.completed_at is not None:
                run.status = "COMPLETED"
                session.flush()
            return self._quality_record(session, str(identifier))

    def get_quality_run(self, identifier: QualityRunId) -> QualityRunRecord:
        with Session(self._engine) as session:
            return self._quality_record(session, str(identifier))

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord:
        with Session(self._engine) as session:
            return self._snapshot_record(session, str(identifier))

    def find_snapshot_by_fingerprint(self, fingerprint: str) -> SnapshotRecord | None:
        with Session(self._engine) as session:
            row = session.scalar(
                select(SnapshotORM).where(
                    SnapshotORM.publication_fingerprint == fingerprint
                )
            )
            return None if row is None else self._snapshot_record(session, row.id)

    def list_snapshots(self) -> tuple[SnapshotRecord, ...]:
        with Session(self._engine) as session:
            identifiers = session.scalars(
                select(SnapshotORM.id).order_by(SnapshotORM.id)
            )
            return tuple(self._snapshot_record(session, value) for value in identifiers)

    def count_snapshots(self) -> int:
        with Session(self._engine) as session:
            return session.scalar(select(func.count()).select_from(SnapshotORM)) or 0

    def delete_draft_snapshot(self, identifier: SnapshotId) -> None:
        with Session(self._engine) as session, session.begin():
            row = session.get(SnapshotORM, str(identifier))
            if row is not None and row.status == SnapshotStatus.DRAFT.value:
                session.delete(row)

    def _validate_version_mapping(
        self,
        session: Session,
        dataset_versions: Mapping[str, DatasetVersionId],
    ) -> dict[str, DatasetVersionId]:
        if not dataset_versions:
            raise ValueError("dataset version mapping must not be empty")
        normalized: dict[str, DatasetVersionId] = {}
        for dataset, identifier in sorted(dataset_versions.items()):
            try:
                kind = DatasetKind(dataset)
            except ValueError as error:
                raise ValueError(f"unsupported dataset: {dataset}") from error
            row = session.get(DatasetVersionORM, str(identifier))
            if row is None:
                raise KeyError(f"dataset version does not exist: {identifier}")
            if row.dataset != kind.value:
                raise ValueError(
                    "dataset version mapping key does not match its version"
                )
            if row.status != SnapshotStatus.PUBLISHED.value:
                raise ValueError(
                    "quality runs can bind only published dataset versions"
                )
            normalized[kind.value] = identifier
        return normalized

    def _dataset_record(
        self, session: Session, identifier: str
    ) -> DatasetVersionRecord:
        row = session.get(DatasetVersionORM, identifier)
        if row is None:
            raise KeyError(f"dataset version does not exist: {identifier}")
        partitions = session.scalars(
            select(DatasetPartitionORM)
            .where(DatasetPartitionORM.dataset_version_id == identifier)
            .order_by(DatasetPartitionORM.ordinal)
        )
        return DatasetVersionRecord(
            id=DatasetVersionId.parse(row.id),
            dataset=DatasetKind(row.dataset),
            fingerprint=row.fingerprint,
            source=row.source,
            status=row.status,
            partitions=tuple(
                DatasetPartitionRecord(
                    content_hash=partition.content_hash,
                    path=Path(partition.path),
                    schema_fingerprint=partition.schema_fingerprint,
                    row_count=partition.row_count,
                )
                for partition in partitions
            ),
            start_date=_parse_date(row.start_date),
            end_date=_parse_date(row.end_date),
            created_run_id=row.created_run_id,
            created_at=_parse_timestamp(row.created_at),
        )

    def _quality_record(self, session: Session, identifier: str) -> QualityRunRecord:
        row = session.get(QualityRunORM, identifier)
        if row is None:
            raise KeyError(f"quality run does not exist: {identifier}")
        versions = session.scalars(
            select(QualityRunDatasetORM)
            .where(QualityRunDatasetORM.quality_run_id == identifier)
            .order_by(QualityRunDatasetORM.dataset)
        )
        issue_rows = session.scalars(
            select(QualityIssueORM)
            .where(QualityIssueORM.quality_run_id == identifier)
            .order_by(QualityIssueORM.id)
        )
        return QualityRunRecord(
            id=QualityRunId.parse(row.id),
            status=row.status,
            dataset_versions=MappingProxyType(
                {
                    item.dataset: DatasetVersionId.parse(item.dataset_version_id)
                    for item in versions
                }
            ),
            started_at=_parse_timestamp(row.started_at),
            completed_at=(
                _parse_timestamp(row.completed_at)
                if row.completed_at is not None
                else None
            ),
            issues=tuple(
                QualityIssue(
                    rule_id=item.rule_id,
                    severity=Severity(item.severity),
                    dataset=DatasetKind(item.dataset),
                    scope=json.loads(item.scope_json),
                    actual=json.loads(item.actual_json),
                    threshold=json.loads(item.threshold_json),
                    message=item.message,
                    remediation=item.remediation,
                )
                for item in issue_rows
            ),
        )

    def _snapshot_record(self, session: Session, identifier: str) -> SnapshotRecord:
        row = session.get(SnapshotORM, identifier)
        if row is None:
            raise KeyError(f"snapshot does not exist: {identifier}")
        versions = session.scalars(
            select(SnapshotDatasetORM)
            .where(SnapshotDatasetORM.snapshot_id == identifier)
            .order_by(SnapshotDatasetORM.dataset)
        )
        return SnapshotRecord(
            id=SnapshotId.parse(row.id),
            publication_fingerprint=row.publication_fingerprint,
            as_of=_parse_timestamp(row.as_of),
            status=SnapshotStatus(row.status),
            manifest_path=Path(row.manifest_path),
            manifest_hash=row.manifest_hash,
            quality_run_id=QualityRunId.parse(row.quality_run_id),
            dataset_versions=MappingProxyType(
                {
                    item.dataset: DatasetVersionId.parse(item.dataset_version_id)
                    for item in versions
                }
            ),
            created_at=_parse_timestamp(row.created_at),
            published_at=(
                _parse_timestamp(row.published_at)
                if row.published_at is not None
                else None
            ),
        )


def dataset_version_fingerprint(spec: DatasetVersionSpec) -> str:
    payload: JsonValue = {
        "created_run_id": spec.created_run_id,
        "dataset": spec.dataset.value,
        "end_date": _date_text(spec.end_date),
        "partitions": [
            {
                "content_hash": partition.content_hash,
                "path": partition.path.as_posix(),
                "row_count": partition.row_count,
                "schema_fingerprint": partition.schema_fingerprint,
            }
            for partition in _sorted_partitions(spec.partitions)
        ],
        "source": spec.source,
        "start_date": _date_text(spec.start_date),
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _sorted_partitions(
    partitions: tuple[DatasetPartitionSpec, ...],
) -> tuple[DatasetPartitionSpec, ...]:
    return tuple(
        sorted(
            partitions,
            key=lambda item: (
                item.path.as_posix(),
                item.content_hash,
                item.schema_fingerprint,
            ),
        )
    )


def _validate_hash(value: str, field: str) -> None:
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("persisted timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _date_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None


def _json_text(value: JsonValue) -> str:
    normalized = dict(value) if isinstance(value, Mapping) else value
    return canonical_json_bytes(normalized).decode("utf-8")


def _raise_repository_conflict(message: str) -> None:
    raise QuantError(
        ErrorDetail(
            code="DATA_CATALOG_CONFLICT",
            severity=Severity.FATAL,
            message=message,
            context={},
            remediation="inspect the immutable data catalog",
            retryable=False,
        )
    )
