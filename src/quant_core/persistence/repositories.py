"""Transactional repositories returning immutable domain-facing DTOs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Never
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import Engine, func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, aliased

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.quality.models import (
    FrozenJsonValue,
    QualityIssue,
    QualityRunSpec,
    freeze_json,
    thaw_json,
)
from quant_core.domain.enums import DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    QualityRunId,
    SnapshotId,
)
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.orm import (
    AuditLogORM,
    DatasetPartitionORM,
    DatasetVersionORM,
    PipelineRunORM,
    PipelineStageORM,
    QualityIssueORM,
    QualityRunDatasetORM,
    QualityRunORM,
    SnapshotDatasetORM,
    SnapshotORM,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PipelineStageName(StrEnum):
    """The fixed, durable order of the foundation data pipeline."""

    INGEST_RAW = "INGEST_RAW"
    CURATE = "CURATE"
    VALIDATE = "VALIDATE"
    PUBLISH_SNAPSHOT = "PUBLISH_SNAPSHOT"


@dataclass(frozen=True, slots=True)
class PipelineRunSpec:
    mode: str
    provider: str
    request_hash: str
    requested_start: date | None
    requested_end: date | None
    resolved_start: date
    resolved_end: date
    created_at: datetime
    pipeline_fingerprint: str = "pipeline-v1"

    def __post_init__(self) -> None:
        if self.mode not in {"BOOTSTRAP", "UPDATE"}:
            raise ValueError("pipeline mode must be BOOTSTRAP or UPDATE")
        if not self.provider:
            raise ValueError("provider must not be empty")
        if not self.pipeline_fingerprint:
            raise ValueError("pipeline_fingerprint must not be empty")
        _validate_hash(self.request_hash, "request_hash")
        if (self.requested_start is None) != (self.requested_end is None):
            raise ValueError("requested dates must be supplied together")
        if self.resolved_start > self.resolved_end:
            raise ValueError("resolved_start must not follow resolved_end")
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at))


@dataclass(frozen=True, slots=True)
class PipelineRunRecord:
    id: str
    mode: str
    provider: str
    request_hash: str
    requested_start: date | None
    requested_end: date | None
    resolved_start: date
    resolved_end: date
    status: str
    created_at: datetime
    completed_at: datetime | None
    pipeline_fingerprint: str


@dataclass(frozen=True, slots=True)
class PipelineStageRecord:
    run_id: str
    stage: PipelineStageName
    status: str
    input_hash: str
    output_hash: str | None
    output: FrozenJsonValue | None
    started_at: datetime
    completed_at: datetime | None
    error: FrozenJsonValue | None
    owner_id: str | None
    lease_expires_at: datetime | None
    attempt: int


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


@dataclass(frozen=True, slots=True)
class SnapshotWriteSpec:
    id: SnapshotId
    publication_fingerprint: str
    as_of: datetime
    manifest_path: Path
    manifest_hash: str
    quality_run_id: QualityRunId
    dataset_versions: Mapping[str, DatasetVersionId]
    created_at: datetime


class MetadataRepository:
    """Own metadata transactions without leaking mapped SQLAlchemy objects."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def register_pipeline_run(self, spec: PipelineRunSpec) -> PipelineRunRecord:
        """Create or return the stable run identified by its canonical request."""
        identifier = str(
            uuid5(NAMESPACE_URL, f"quant:pipeline-run:{spec.request_hash}")
        )
        with Session(self._engine) as session, session.begin():
            session.execute(
                sqlite_insert(PipelineRunORM)
                .values(
                    id=identifier,
                    mode=spec.mode,
                    provider=spec.provider,
                    request_hash=spec.request_hash,
                    pipeline_fingerprint=spec.pipeline_fingerprint,
                    requested_start=_date_text(spec.requested_start),
                    requested_end=_date_text(spec.requested_end),
                    resolved_start=spec.resolved_start.isoformat(),
                    resolved_end=spec.resolved_end.isoformat(),
                    status="CREATED",
                    created_at=_timestamp(spec.created_at),
                    completed_at=None,
                )
                .on_conflict_do_nothing(index_elements=["request_hash"])
            )
            row = session.scalar(
                select(PipelineRunORM).where(
                    PipelineRunORM.request_hash == spec.request_hash
                )
            )
            if row is None or row.id != identifier:
                _raise_repository_conflict("pipeline run request hash collision")
            expected = (
                spec.mode,
                spec.provider,
                spec.pipeline_fingerprint,
                _date_text(spec.requested_start),
                _date_text(spec.requested_end),
                spec.resolved_start.isoformat(),
                spec.resolved_end.isoformat(),
            )
            actual = (
                row.mode,
                row.provider,
                row.pipeline_fingerprint,
                row.requested_start,
                row.requested_end,
                row.resolved_start,
                row.resolved_end,
            )
            if actual != expected:
                _raise_repository_conflict("pipeline run metadata differs")
            return self._pipeline_run_record(row)

    def start_pipeline_stage(
        self,
        run_id: str,
        stage: PipelineStageName,
        *,
        input_hash: str,
        started_at: datetime,
        owner_id: str = "legacy-owner",
        lease_expires_at: datetime | None = None,
    ) -> PipelineStageRecord:
        """Claim a stage with compare-and-swap owner and expiry semantics."""
        _validate_hash(input_hash, "input_hash")
        started_at = _utc_datetime(started_at)
        lease_expires_at = _utc_datetime(
            lease_expires_at or started_at + timedelta(minutes=30)
        )
        if not owner_id or lease_expires_at <= started_at:
            raise ValueError("stage owner and future lease are required")
        with Session(self._engine) as session, session.begin():
            run = session.get(PipelineRunORM, run_id)
            if run is None:
                raise KeyError(f"pipeline run does not exist: {run_id}")
            values = {
                "run_id": run_id,
                "stage": stage.value,
                "status": "RUNNING",
                "input_hash": input_hash,
                "output_hash": None,
                "output_json": None,
                "started_at": _timestamp(started_at),
                "completed_at": None,
                "error_json": None,
                "owner_id": owner_id,
                "lease_expires_at": _timestamp(lease_expires_at),
                "attempt": 1,
            }
            inserted = session.execute(
                sqlite_insert(PipelineStageORM)
                .values(**values)
                .on_conflict_do_nothing(index_elements=["run_id", "stage"])
            )
            if getattr(inserted, "rowcount", 0):
                run.status = "RUNNING"
                session.flush()
                return self._pipeline_stage_record(
                    session.get_one(PipelineStageORM, (run_id, stage.value))
                )
            row = session.get_one(PipelineStageORM, (run_id, stage.value))
            if row.status == "SUCCEEDED":
                if row.input_hash != input_hash:
                    raise ValueError("successful pipeline stage input hash differs")
                return self._pipeline_stage_record(row)
            lease_active = (
                row.status == "RUNNING"
                and row.owner_id != owner_id
                and row.lease_expires_at is not None
                and _parse_timestamp(row.lease_expires_at) > started_at
            )
            if lease_active:
                _raise_pipeline_busy(run_id, stage, row.owner_id)
            claimed = session.execute(
                update(PipelineStageORM)
                .where(
                    PipelineStageORM.run_id == run_id,
                    PipelineStageORM.stage == stage.value,
                    PipelineStageORM.status != "SUCCEEDED",
                )
                .values(
                    **{key: value for key, value in values.items() if key != "attempt"},
                    attempt=PipelineStageORM.attempt + 1,
                )
            )
            if getattr(claimed, "rowcount", 0) != 1:
                _raise_pipeline_busy(run_id, stage, row.owner_id)
            run.status = "RUNNING"
            session.flush()
            return self._pipeline_stage_record(
                session.get_one(PipelineStageORM, (run_id, stage.value))
            )

    def complete_pipeline_stage(
        self,
        run_id: str,
        stage: PipelineStageName,
        *,
        input_hash: str,
        output_hash: str,
        output: JsonValue,
        completed_at: datetime,
        owner_id: str | None = None,
    ) -> PipelineStageRecord:
        """Seal one stage checkpoint; successful rows are immutable afterwards."""
        _validate_hash(input_hash, "input_hash")
        _validate_hash(output_hash, "output_hash")
        output_json = canonical_json_bytes(output).decode("utf-8")
        with Session(self._engine) as session, session.begin():
            row = session.get(PipelineStageORM, (run_id, stage.value))
            if (
                row is None
                or row.status != "RUNNING"
                or row.input_hash != input_hash
                or (owner_id is not None and row.owner_id != owner_id)
            ):
                raise ValueError("pipeline stage is not running with this input hash")
            row.status = "SUCCEEDED"
            row.output_hash = output_hash
            row.output_json = output_json
            row.completed_at = _timestamp(completed_at)
            row.error_json = None
            session.flush()
            return self._pipeline_stage_record(row)

    def get_pipeline_stage(
        self, run_id: str, stage: PipelineStageName
    ) -> PipelineStageRecord:
        with Session(self._engine) as session:
            row = session.get(PipelineStageORM, (run_id, stage.value))
            if row is None:
                raise KeyError(f"pipeline stage does not exist: {run_id}/{stage.value}")
            return self._pipeline_stage_record(row)

    def fail_pipeline_stage(
        self,
        run_id: str,
        stage: PipelineStageName,
        *,
        input_hash: str,
        error: JsonValue,
        completed_at: datetime,
        blocked: bool = False,
        owner_id: str | None = None,
    ) -> PipelineStageRecord:
        """Persist a structured stage failure so another process can resume it."""
        with Session(self._engine) as session, session.begin():
            row = session.get(PipelineStageORM, (run_id, stage.value))
            run = session.get(PipelineRunORM, run_id)
            if (
                row is None
                or run is None
                or row.input_hash != input_hash
                or row.status != "RUNNING"
                or (owner_id is not None and row.owner_id != owner_id)
            ):
                raise ValueError("pipeline stage is not running with this input hash")
            row.status = "BLOCKED" if blocked else "FAILED"
            row.completed_at = _timestamp(completed_at)
            row.error_json = canonical_json_bytes(error).decode("utf-8")
            run.status = row.status
            session.flush()
            return self._pipeline_stage_record(row)

    def complete_pipeline_run(self, run_id: str, completed_at: datetime) -> None:
        with Session(self._engine) as session, session.begin():
            row = session.get(PipelineRunORM, run_id)
            if row is None:
                raise KeyError(f"pipeline run does not exist: {run_id}")
            row.status = "SUCCEEDED"
            row.completed_at = _timestamp(completed_at)

    def get_pipeline_run(self, run_id: str) -> PipelineRunRecord:
        with Session(self._engine) as session:
            row = session.get(PipelineRunORM, run_id)
            if row is None:
                raise KeyError(f"pipeline run does not exist: {run_id}")
            return self._pipeline_run_record(row)

    def latest_recoverable_pipeline_run(
        self, provider: str | None = None
    ) -> PipelineRunRecord | None:
        with Session(self._engine) as session:
            query = select(PipelineRunORM).where(PipelineRunORM.status != "SUCCEEDED")
            if provider is not None:
                query = query.where(PipelineRunORM.provider == provider)
            row = session.scalar(query.order_by(PipelineRunORM.created_at.desc()))
            return None if row is None else self._pipeline_run_record(row)

    def latest_pipeline_run_ready_for(
        self,
        target: PipelineStageName,
        *,
        provider: str,
        pipeline_fingerprint: str,
    ) -> PipelineRunRecord | None:
        prerequisite = {
            PipelineStageName.VALIDATE: PipelineStageName.CURATE,
            PipelineStageName.PUBLISH_SNAPSHOT: PipelineStageName.VALIDATE,
        }.get(target)
        if prerequisite is None:
            raise ValueError("target stage has no resumable prerequisite")
        with Session(self._engine) as session:
            ready = aliased(PipelineStageORM)
            row = session.scalar(
                select(PipelineRunORM)
                .join(ready, ready.run_id == PipelineRunORM.id)
                .where(
                    PipelineRunORM.status != "SUCCEEDED",
                    PipelineRunORM.provider == provider,
                    PipelineRunORM.pipeline_fingerprint == pipeline_fingerprint,
                    ready.stage == prerequisite.value,
                    ready.status == "SUCCEEDED",
                )
                .order_by(PipelineRunORM.created_at.desc())
            )
            return None if row is None else self._pipeline_run_record(row)

    @staticmethod
    def _pipeline_run_record(row: PipelineRunORM) -> PipelineRunRecord:
        return PipelineRunRecord(
            id=row.id,
            mode=row.mode,
            provider=row.provider,
            request_hash=row.request_hash,
            requested_start=_parse_date(row.requested_start),
            requested_end=_parse_date(row.requested_end),
            resolved_start=date.fromisoformat(row.resolved_start),
            resolved_end=date.fromisoformat(row.resolved_end),
            status=row.status,
            created_at=_parse_timestamp(row.created_at),
            completed_at=(
                _parse_timestamp(row.completed_at)
                if row.completed_at is not None
                else None
            ),
            pipeline_fingerprint=row.pipeline_fingerprint,
        )

    @staticmethod
    def _pipeline_stage_record(row: PipelineStageORM) -> PipelineStageRecord:
        return PipelineStageRecord(
            run_id=row.run_id,
            stage=PipelineStageName(row.stage),
            status=row.status,
            input_hash=row.input_hash,
            output_hash=row.output_hash,
            output=(
                freeze_json(json.loads(row.output_json)) if row.output_json else None
            ),
            started_at=_parse_timestamp(row.started_at),
            completed_at=(
                _parse_timestamp(row.completed_at)
                if row.completed_at is not None
                else None
            ),
            error=(freeze_json(json.loads(row.error_json)) if row.error_json else None),
            owner_id=row.owner_id,
            lease_expires_at=(
                _parse_timestamp(row.lease_expires_at)
                if row.lease_expires_at is not None
                else None
            ),
            attempt=row.attempt,
        )

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
            insert_result = session.execute(
                sqlite_insert(DatasetVersionORM)
                .values(
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
                .on_conflict_do_nothing(index_elements=["id"])
            )
            inserted = getattr(insert_result, "rowcount", 0)
            if inserted:
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
                session.execute(
                    update(DatasetVersionORM)
                    .where(DatasetVersionORM.id == str(identifier))
                    .values(status=SnapshotStatus.PUBLISHED.value)
                )
            existing = session.get(DatasetVersionORM, str(identifier))
            if existing is None or (
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

    def latest_snapshot(self) -> SnapshotRecord | None:
        with Session(self._engine) as session:
            row = session.scalar(
                select(SnapshotORM)
                .where(SnapshotORM.status == SnapshotStatus.PUBLISHED.value)
                .order_by(SnapshotORM.published_at.desc())
            )
            return None if row is None else self._snapshot_record(session, row.id)

    def count_snapshots(self) -> int:
        with Session(self._engine) as session:
            return session.scalar(select(func.count()).select_from(SnapshotORM)) or 0

    def publish_snapshot(
        self,
        spec: SnapshotWriteSpec,
        install_manifest: Callable[[], None],
    ) -> SnapshotRecord:
        """Commit the quality gate, manifest install, and catalog publish as one UoW."""
        with Session(self._engine) as session, session.begin():
            normalized = self._validate_version_mapping(session, spec.dataset_versions)
            _validate_snapshot_quality_gate(session, normalized, spec.quality_run_id)
            insert_result = session.execute(
                sqlite_insert(SnapshotORM)
                .values(
                    id=str(spec.id),
                    publication_fingerprint=spec.publication_fingerprint,
                    as_of=_timestamp(spec.as_of),
                    status=SnapshotStatus.DRAFT.value,
                    manifest_path=spec.manifest_path.as_posix(),
                    manifest_hash=spec.manifest_hash,
                    quality_run_id=str(spec.quality_run_id),
                    created_at=_timestamp(spec.created_at),
                    published_at=None,
                )
                .on_conflict_do_nothing(index_elements=["publication_fingerprint"])
            )
            inserted = getattr(insert_result, "rowcount", 0)
            if inserted:
                for dataset, version_id in normalized.items():
                    session.add(
                        SnapshotDatasetORM(
                            snapshot_id=str(spec.id),
                            dataset=dataset,
                            dataset_version_id=str(version_id),
                        )
                    )
                session.flush()
                install_manifest()
                session.execute(
                    update(SnapshotORM)
                    .where(SnapshotORM.id == str(spec.id))
                    .values(
                        status=SnapshotStatus.PUBLISHED.value,
                        published_at=_timestamp(spec.created_at),
                    )
                )
                self._add_audit(
                    session,
                    "SNAPSHOT_PUBLISHED",
                    spec.id,
                    {"manifest_hash": spec.manifest_hash},
                    spec.created_at,
                )
                identifier = str(spec.id)
            else:
                row = session.scalar(
                    select(SnapshotORM).where(
                        SnapshotORM.publication_fingerprint
                        == spec.publication_fingerprint
                    )
                )
                if row is None or row.status != SnapshotStatus.PUBLISHED.value:
                    _raise_repository_conflict("snapshot publication collision")
                identifier = row.id
            session.flush()
            return self._snapshot_record(session, identifier)

    def recover_draft_snapshot(
        self,
        identifier: SnapshotId,
        install_manifest: Callable[[], None],
        published_at: datetime,
    ) -> str:
        """Re-run the quality gate and reconcile one DRAFT in one transaction."""
        with Session(self._engine) as session, session.begin():
            row = session.get(SnapshotORM, str(identifier))
            if row is None:
                return "ABSENT"
            if row.status == SnapshotStatus.PUBLISHED.value:
                return "PUBLISHED"
            versions = {
                item.dataset: DatasetVersionId.parse(item.dataset_version_id)
                for item in session.scalars(
                    select(SnapshotDatasetORM).where(
                        SnapshotDatasetORM.snapshot_id == str(identifier)
                    )
                )
            }
            try:
                _validate_snapshot_quality_gate(
                    session, versions, QualityRunId.parse(row.quality_run_id)
                )
            except QuantError as error:
                self._add_audit(
                    session,
                    "SNAPSHOT_RECOVERY_DISCARDED",
                    identifier,
                    {"reason": error.detail.code},
                    published_at,
                )
                session.delete(row)
                return "DISCARDED"
            install_manifest()
            row.status = SnapshotStatus.PUBLISHED.value
            row.published_at = _timestamp(published_at)
            self._add_audit(
                session,
                "SNAPSHOT_RECOVERED",
                identifier,
                {"manifest_hash": row.manifest_hash},
                published_at,
            )
            session.flush()
            return "RECOVERED"

    def discard_draft_snapshot(
        self, identifier: SnapshotId, reason: str, discarded_at: datetime
    ) -> None:
        with Session(self._engine) as session, session.begin():
            row = session.get(SnapshotORM, str(identifier))
            if row is not None and row.status == SnapshotStatus.DRAFT.value:
                self._add_audit(
                    session,
                    "SNAPSHOT_RECOVERY_DISCARDED",
                    identifier,
                    {"reason": reason},
                    discarded_at,
                )
                session.delete(row)

    def _add_audit(
        self,
        session: Session,
        action: str,
        identifier: SnapshotId,
        details: JsonValue,
        created_at: datetime,
    ) -> None:
        session.add(
            AuditLogORM(
                action=action,
                object_type="snapshot",
                object_id=str(identifier),
                details_json=canonical_json_bytes(details).decode("utf-8"),
                created_at=_timestamp(created_at),
            )
        )

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


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("persisted timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _date_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None


def _json_text(value: object) -> str:
    return canonical_json_bytes(thaw_json(value)).decode("utf-8")


def _validate_snapshot_quality_gate(
    session: Session,
    dataset_versions: Mapping[str, DatasetVersionId],
    quality_run_id: QualityRunId,
) -> None:
    run = session.get(QualityRunORM, str(quality_run_id))
    if run is None:
        raise KeyError(f"quality run does not exist: {quality_run_id}")
    checked = {
        row.dataset: DatasetVersionId.parse(row.dataset_version_id)
        for row in session.scalars(
            select(QualityRunDatasetORM).where(
                QualityRunDatasetORM.quality_run_id == str(quality_run_id)
            )
        )
    }
    if run.status != "COMPLETED":
        _raise_snapshot_gate_error(
            "SNAP_QUALITY_INCOMPLETE", "quality run is not complete"
        )
    if checked != dict(dataset_versions):
        _raise_snapshot_gate_error(
            "SNAP_QUALITY_SCOPE_MISMATCH",
            "quality run does not cover the exact snapshot version set",
        )
    blocking = session.scalar(
        select(QualityIssueORM.id).where(
            QualityIssueORM.quality_run_id == str(quality_run_id),
            QualityIssueORM.severity.in_([Severity.SEVERE.value, Severity.FATAL.value]),
        )
    )
    if blocking is not None:
        _raise_snapshot_gate_error(
            "SNAP_QUALITY_BLOCKED",
            "blocking quality issues prevent snapshot publication",
        )


def _raise_snapshot_gate_error(code: str, message: str) -> Never:
    raise QuantError(
        ErrorDetail(
            code=code,
            severity=Severity.FATAL,
            message=message,
            context={},
            remediation="inspect quality metadata before retrying",
            retryable=False,
        )
    )


def _raise_repository_conflict(message: str) -> Never:
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


def _raise_pipeline_busy(
    run_id: str, stage: PipelineStageName, owner_id: str | None
) -> Never:
    raise QuantError(
        ErrorDetail(
            code="DATA_PIPELINE_BUSY",
            severity=Severity.SEVERE,
            message="pipeline stage is owned by another active attempt",
            context={"run_id": run_id, "stage": stage.value, "owner_id": owner_id},
            remediation="wait for the active lease to complete or expire",
            retryable=True,
        )
    )
