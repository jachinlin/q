"""Read-only experiment queries returning immutable domain DTOs."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.experiments.models import (
    ExperimentArtifact,
    ExperimentMetric,
    ExperimentRecord,
    ExperimentStatus,
    ResearchMark,
)
from quant_core.experiments.registry import ExperimentNotFound
from quant_core.persistence.orm import (
    AuditEventORM,
    ExperimentArtifactORM,
    ExperimentMetricORM,
    ExperimentORM,
    ExperimentTagORM,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class ExperimentAuditEvent:
    event_type: str
    actor: str | None
    details: Mapping[str, JsonValue]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ExperimentDetail:
    record: ExperimentRecord
    metrics: tuple[ExperimentMetric, ...]
    artifacts: tuple[ExperimentArtifact, ...]
    tags: tuple[str, ...]
    note: str | None
    audit: tuple[ExperimentAuditEvent, ...]


class ExperimentQuery:
    """Validate and execute stable read-only experiment queries."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def list(
        self,
        *,
        statuses: ExperimentStatus | Sequence[ExperimentStatus] | None = None,
        strategy_id: str | None = None,
        strategy_version: str | None = None,
        snapshot_id: str | None = None,
        research_mark: ResearchMark | None = None,
        tags: Sequence[str] = (),
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        fingerprint: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ExperimentRecord, ...]:
        """List experiments in stable newest-first order with validated filters."""
        normalized_statuses = _statuses(statuses)
        normalized_tags = _filter_tags(tags)
        normalized_from = _optional_utc(created_from, "created_from")
        normalized_to = _optional_utc(created_to, "created_to")
        if normalized_from is not None and normalized_to is not None and normalized_from > normalized_to:
            raise ValueError("created_from must not follow created_to")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("limit must be an integer from 1 through 500")
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        normalized_strategy = _optional_text(strategy_id, "strategy_id", 128)
        normalized_version = _optional_text(
            strategy_version, "strategy_version", 64
        )
        if normalized_version is not None and normalized_strategy is None:
            raise ValueError("strategy_version requires strategy_id")
        normalized_snapshot = _optional_text(snapshot_id, "snapshot_id", 128)
        if research_mark is not None and not isinstance(research_mark, ResearchMark):
            raise TypeError("research_mark must be a ResearchMark")
        if fingerprint is not None:
            _fingerprint(fingerprint)

        statement = select(ExperimentORM)
        if normalized_statuses is not None:
            statement = statement.where(
                ExperimentORM.status.in_(
                    [status.value for status in normalized_statuses]
                )
            )
        if normalized_strategy is not None:
            statement = statement.where(
                ExperimentORM.strategy_id == normalized_strategy
            )
        if normalized_version is not None:
            statement = statement.where(
                ExperimentORM.strategy_version == normalized_version
            )
        if normalized_snapshot is not None:
            statement = statement.where(
                ExperimentORM.snapshot_id == normalized_snapshot
            )
        if research_mark is not None:
            statement = statement.where(
                ExperimentORM.research_mark == research_mark.value
            )
        if normalized_tags:
            tagged = (
                select(ExperimentTagORM.experiment_id)
                .where(ExperimentTagORM.tag.in_(normalized_tags))
                .group_by(ExperimentTagORM.experiment_id)
                .having(func.count(ExperimentTagORM.tag) == len(normalized_tags))
            )
            statement = statement.where(ExperimentORM.id.in_(tagged))
        if normalized_from is not None:
            statement = statement.where(
                ExperimentORM.created_at >= normalized_from.isoformat()
            )
        if normalized_to is not None:
            statement = statement.where(
                ExperimentORM.created_at <= normalized_to.isoformat()
            )
        if fingerprint is not None:
            statement = statement.where(ExperimentORM.fingerprint == fingerprint)
        statement = statement.order_by(
            ExperimentORM.created_at.desc(), ExperimentORM.id.desc()
        ).limit(limit).offset(offset)
        with Session(self._engine) as session:
            return tuple(_record(row) for row in session.scalars(statement))

    def find_duplicates(
        self,
        fingerprint: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[ExperimentRecord, ...]:
        """Return repeated-research candidates without collapsing identities."""
        _fingerprint(fingerprint)
        return self.list(fingerprint=fingerprint, limit=limit, offset=offset)

    def get(self, experiment_id: str) -> ExperimentDetail:
        """Assemble one experiment, its result indexes, annotations, and timeline."""
        identifier = _text(experiment_id, "experiment_id", 128)
        with Session(self._engine) as session:
            row = session.get(ExperimentORM, identifier)
            if row is None:
                raise ExperimentNotFound(identifier)
            record = _record(row)
            metrics = tuple(
                _metric(item)
                for item in session.scalars(
                    select(ExperimentMetricORM)
                    .where(ExperimentMetricORM.experiment_id == identifier)
                    .order_by(ExperimentMetricORM.name)
                )
            )
            artifacts = tuple(
                _artifact(item)
                for item in session.scalars(
                    select(ExperimentArtifactORM)
                    .where(ExperimentArtifactORM.experiment_id == identifier)
                    .order_by(ExperimentArtifactORM.name)
                )
            )
            tags = tuple(
                session.scalars(
                    select(ExperimentTagORM.tag)
                    .where(ExperimentTagORM.experiment_id == identifier)
                    .order_by(ExperimentTagORM.tag)
                )
            )
            audit = tuple(
                _audit(item)
                for item in session.scalars(
                    select(AuditEventORM)
                    .where(AuditEventORM.experiment_id == identifier)
                    .order_by(AuditEventORM.created_at, AuditEventORM.id)
                )
            )
        note = _note_from_timeline(audit)
        return ExperimentDetail(record, metrics, artifacts, tags, note, audit)

    get_detail = get


ExperimentQueryService = ExperimentQuery


def _record(row: ExperimentORM) -> ExperimentRecord:
    config = json.loads(row.config_json)
    canonical_json_bytes(cast(JsonValue, config))
    return ExperimentRecord(
        id=row.id,
        strategy_id=row.strategy_id,
        strategy_version=row.strategy_version,
        config=cast(dict[str, JsonValue], config),
        config_hash=row.config_hash,
        snapshot_id=row.snapshot_id,
        snapshot_manifest_hash=row.snapshot_manifest_hash,
        source_tree_hash=row.source_tree_hash,
        git_commit_hash=row.git_commit_hash,
        lockfile_hash=row.lockfile_hash,
        rulebook_version=row.rulebook_version,
        fingerprint=row.fingerprint,
        status=ExperimentStatus(row.status),
        research_mark=ResearchMark(row.research_mark),
        created_at=_parse_timestamp(row.created_at),
        queued_at=_parse_optional_timestamp(row.queued_at),
        started_at=_parse_optional_timestamp(row.started_at),
        completed_at=_parse_optional_timestamp(row.completed_at),
    )


def _metric(row: ExperimentMetricORM) -> ExperimentMetric:
    return ExperimentMetric(
        experiment_id=row.experiment_id,
        name=row.name,
        value=row.value,
        unit=row.unit,
        created_at=_parse_timestamp(row.created_at),
    )


def _artifact(row: ExperimentArtifactORM) -> ExperimentArtifact:
    metadata = json.loads(row.metadata_json)
    canonical_json_bytes(cast(JsonValue, metadata))
    return ExperimentArtifact(
        experiment_id=row.experiment_id,
        name=row.name,
        artifact_type=row.artifact_type,
        path=row.path,
        content_hash=row.content_hash,
        metadata=cast(dict[str, JsonValue], metadata),
        created_at=_parse_timestamp(row.created_at),
    )


def _audit(row: AuditEventORM) -> ExperimentAuditEvent:
    details = json.loads(row.details_json)
    canonical_json_bytes(cast(JsonValue, details))
    if not isinstance(details, dict):
        raise TypeError("persisted audit details must be a JSON object")
    return ExperimentAuditEvent(
        event_type=row.event_type,
        actor=row.actor,
        details=cast(dict[str, JsonValue], details),
        created_at=_parse_timestamp(row.created_at),
    )


def _note_from_timeline(audit: tuple[ExperimentAuditEvent, ...]) -> str | None:
    for entry in reversed(audit):
        if entry.event_type != "EXPERIMENT_RESEARCH_UPDATED":
            continue
        new_value = entry.details.get("new_value")
        if isinstance(new_value, Mapping):
            note = new_value.get("note")
            if isinstance(note, str):
                return note
    return None


def _statuses(
    value: ExperimentStatus | Sequence[ExperimentStatus] | None,
) -> tuple[ExperimentStatus, ...] | None:
    if value is None:
        return None
    if isinstance(value, ExperimentStatus):
        return (value,)
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("statuses must be an ExperimentStatus or sequence")
    if not value:
        raise ValueError("statuses must not be empty")
    if any(not isinstance(status, ExperimentStatus) for status in value):
        raise TypeError("statuses must contain ExperimentStatus values")
    return tuple(dict.fromkeys(cast(Sequence[ExperimentStatus], value)))


def _filter_tags(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError("tags must be a sequence of strings")
    normalized: set[str] = set()
    for value in values:
        tag = _text(value, "tag", 64).strip()
        if not tag:
            raise ValueError("tag must not be empty")
        normalized.add(tag)
    return tuple(sorted(normalized))


def _fingerprint(value: str) -> None:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ValueError("fingerprint must be a lowercase SHA-256 hex digest")


def _optional_text(value: str | None, field: str, limit: int) -> str | None:
    return _text(value, field, limit) if value is not None else None


def _text(value: str, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if not value.strip():
        raise ValueError(f"{field} must not be empty")
    if value != value.strip():
        raise ValueError(f"{field} must be trimmed")
    if len(value) > limit:
        raise ValueError(f"{field} is too long")
    return value


def _optional_utc(value: datetime | None, field: str) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("persisted timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _parse_optional_timestamp(value: str | None) -> datetime | None:
    return _parse_timestamp(value) if value is not None else None
