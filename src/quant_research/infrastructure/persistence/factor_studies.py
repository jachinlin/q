"""实现独立 FactorStudy 聚合的 SQLite 登记簿。"""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import Engine, delete, select, update
from sqlalchemy.orm import Session

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.factor_studies.analysis import LABEL_KINDS
from quant_research.factor_studies.models import (
    FactorDecisionMark,
    FactorStudyArtifactRecord,
    FactorStudyDecisionKey,
    FactorStudyDecisionRecord,
    FactorStudyDefinition,
    FactorStudyMetricRecord,
    FactorStudyRecord,
    FactorStudyStage,
    FactorStudyStatus,
)
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    FactorStudyArtifactORM,
    FactorStudyDecisionORM,
    FactorStudyMetricORM,
    FactorStudyORM,
    FactorStudyTagORM,
    TaskORM,
)
from quant_research.tasks.models import TaskProgress

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_TERMINAL = frozenset(
    {
        FactorStudyStatus.SUCCEEDED.value,
        FactorStudyStatus.FAILED.value,
        FactorStudyStatus.CANCELLED.value,
    }
)


class FactorStudyRegistry:
    """维护研究聚合。入参：数据库引擎。返回值：登记簿实例。异常：数据库不可用时抛出。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create(
        self,
        definition: FactorStudyDefinition,
        config_hash: str,
        catalog_hash: str,
        *,
        actor: str = "system",
        now: datetime | None = None,
    ) -> tuple[str, str]:
        """原子创建研究和任务。入参：定义、身份、操作者和时间。返回值：研究与任务 ID。异常：哈希或事务非法时抛出。"""
        if len(config_hash) != 64 or len(catalog_hash) != 64:
            raise ValueError("factor study hashes must be SHA-256 digests")
        instant = self._now(now)
        study_id, task_id = self._id(), self._id()
        stamp = instant.isoformat()
        progress = TaskProgress(stage="QUEUED", completed=0, total=0, message="")
        with Session(self._engine) as session, session.begin():
            session.add(
                TaskORM(
                    id=task_id,
                    subject_kind="FACTOR_STUDY",
                    subject_id=study_id,
                    task_type="FACTOR_STUDY",
                    payload_json=canonical_json_bytes(
                        {"factor_study_id": study_id}
                    ).decode("utf-8"),
                    status="QUEUED",
                    priority=0,
                    progress_json=canonical_json_bytes(
                        progress.model_dump(mode="json")
                    ).decode("utf-8"),
                    created_at=stamp,
                    available_at=stamp,
                    updated_at=stamp,
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=f"factor-study:{study_id}",
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            session.flush()
            session.add(
                FactorStudyORM(
                    id=study_id,
                    name=definition.name,
                    description=definition.description,
                    definition_json=canonical_json_bytes(
                        definition.model_dump(mode="json")
                    ).decode("utf-8"),
                    config_hash=config_hash,
                    catalog_hash=catalog_hash,
                    status=FactorStudyStatus.QUEUED.value,
                    stage=FactorStudyStage.VALIDATE.value,
                    task_id=task_id,
                    artifact_dir=None,
                    manifest_hash=None,
                    error_json=None,
                    created_at=stamp,
                    started_at=None,
                    completed_at=None,
                )
            )
            for tag in definition.tags:
                session.add(
                    FactorStudyTagORM(factor_study_id=study_id, tag=tag)
                )
            self._audit(
                session,
                study_id,
                task_id,
                "FACTOR_STUDY_CREATED",
                actor,
                {},
                instant,
            )
        return study_id, task_id

    def get(self, study_id: str) -> FactorStudyRecord:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：研究不存在时抛出值错误。"""
        with Session(self._engine) as session:
            row = self._row(session, study_id)
            return self._record(session, row)

    def list(
        self,
        *,
        limit: int,
        offset: int,
        status: FactorStudyStatus | None = None,
        decision: FactorDecisionMark | None = None,
    ) -> tuple[FactorStudyRecord, ...]:
        """列出研究。入参：分页和筛选条件。返回值：有序快照。异常：分页非法时抛出值错误。"""
        if limit <= 0 or offset < 0:
            raise ValueError("invalid pagination")
        with Session(self._engine) as session:
            statement = select(FactorStudyORM)
            if status is not None:
                statement = statement.where(FactorStudyORM.status == status.value)
            if decision is not None and decision is not FactorDecisionMark.UNREVIEWED:
                statement = (
                    statement.join(
                        FactorStudyDecisionORM,
                        FactorStudyDecisionORM.factor_study_id == FactorStudyORM.id,
                    )
                    .where(FactorStudyDecisionORM.mark == decision.value)
                    .distinct()
                )
            rows = session.scalars(
                statement.order_by(
                    FactorStudyORM.created_at.desc(), FactorStudyORM.id.desc()
                )
                .limit(limit)
                .offset(offset)
            ).all()
            records = tuple(self._record(session, row) for row in rows)
            if decision is FactorDecisionMark.UNREVIEWED:
                return tuple(
                    record
                    for record in records
                    if not record.decisions
                    or len(record.decisions)
                    < self._decision_unit_count(record.definition)
                )
            return records

    def update_stage(self, study_id: str, stage: FactorStudyStage) -> None:
        """更新阶段。入参：研究 ID 和阶段。返回值：无。异常：研究非运行中时抛出值错误。"""
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != FactorStudyStatus.RUNNING.value:
                raise ValueError("factor study must be running to update stage")
            row.stage = stage.value

    def transition(
        self,
        study_id: str,
        expected: FactorStudyStatus,
        target: FactorStudyStatus,
        *,
        stage: FactorStudyStage,
        error: dict[str, JsonValue] | None = None,
        artifact_dir: str | None = None,
        manifest_hash: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """迁移状态。入参：研究、前后状态和终态证据。返回值：无。异常：CAS 或证据非法时抛出。"""
        instant = self._now(now)
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != expected.value:
                raise ValueError("factor study status conflict")
            if target is FactorStudyStatus.SUCCEEDED and (
                not artifact_dir or not manifest_hash or len(manifest_hash) != 64
            ):
                raise ValueError("successful factor study requires artifact evidence")
            row.status = target.value
            row.stage = stage.value
            row.error_json = (
                canonical_json_bytes(error).decode("utf-8") if error else None
            )
            row.artifact_dir = artifact_dir
            row.manifest_hash = manifest_hash
            if target is FactorStudyStatus.RUNNING:
                row.started_at = instant.isoformat()
                row.completed_at = None
            if target.value in _TERMINAL:
                row.completed_at = instant.isoformat()

    def register_outputs(
        self,
        study_id: str,
        metrics: Mapping[
            str, tuple[float, str | None, float | None, float | None]
        ],
        artifacts: tuple[dict[str, JsonValue], ...],
    ) -> None:
        """登记输出。入参：研究 ID、指标和产物证据。返回值：无。异常：状态或事务非法时抛出。"""
        instant = self._now(None).isoformat()
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != FactorStudyStatus.RUNNING.value:
                raise ValueError("outputs require a running factor study")
            session.execute(
                delete(FactorStudyMetricORM).where(
                    FactorStudyMetricORM.factor_study_id == study_id
                )
            )
            session.execute(
                delete(FactorStudyArtifactORM).where(
                    FactorStudyArtifactORM.factor_study_id == study_id
                )
            )
            for name, values in sorted(metrics.items()):
                session.add(
                    FactorStudyMetricORM(
                        factor_study_id=study_id,
                        name=name,
                        value=values[0],
                        unit=values[1],
                        p_value=values[2],
                        adjusted_p_value=values[3],
                        created_at=instant,
                    )
                )
            for item in sorted(artifacts, key=lambda value: str(value["artifact_type"])):
                schema = item.get("schema")
                session.add(
                    FactorStudyArtifactORM(
                        factor_study_id=study_id,
                        artifact_type=cast(str, item["artifact_type"]),
                        relative_path=cast(str, item["relative_path"]),
                        content_hash=cast(str, item["content_hash"]),
                        byte_count=cast(int, item["byte_count"]),
                        row_count=cast(int | None, item.get("row_count")),
                        schema_json=(
                            canonical_json_bytes(cast(JsonValue, schema)).decode(
                                "utf-8"
                            )
                            if schema is not None
                            else None
                        ),
                        created_at=instant,
                    )
                )

    def discard_outputs(self, study_id: str) -> None:
        """丢弃输出登记。入参：研究 ID。返回值：无。异常：研究不存在或事务失败时抛出。"""
        with Session(self._engine) as session, session.begin():
            self._row(session, study_id)
            session.execute(
                delete(FactorStudyMetricORM).where(
                    FactorStudyMetricORM.factor_study_id == study_id
                )
            )
            session.execute(
                delete(FactorStudyArtifactORM).where(
                    FactorStudyArtifactORM.factor_study_id == study_id
                )
            )

    def decide(
        self,
        study_id: str,
        key: FactorStudyDecisionKey,
        mark: FactorDecisionMark,
        note: str,
        *,
        actor: str = "system",
        now: datetime | None = None,
    ) -> None:
        """保存人工结论。入参：研究、决策键、结论和审计内容。返回值：无。异常：状态或维度非法时抛出。"""
        if len(note) > 4000:
            raise ValueError("decision note exceeds 4000 characters")
        instant = self._now(now)
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != FactorStudyStatus.SUCCEEDED.value:
                raise ValueError("only successful studies can be reviewed")
            definition = FactorStudyDefinition.model_validate_json(row.definition_json)
            self._validate_decision_key(definition, key)
            persisted = session.get(
                FactorStudyDecisionORM,
                (
                    study_id,
                    key.signal_variant,
                    key.label_kind,
                    key.factor_ref,
                    key.horizon,
                ),
            )
            if mark is FactorDecisionMark.UNREVIEWED:
                if persisted is not None:
                    session.delete(persisted)
            elif persisted is None:
                session.add(
                    FactorStudyDecisionORM(
                        factor_study_id=study_id,
                        signal_variant=key.signal_variant,
                        label_kind=key.label_kind,
                        factor_ref=key.factor_ref,
                        horizon=key.horizon,
                        mark=mark.value,
                        note=note,
                        actor=actor,
                        updated_at=instant.isoformat(),
                    )
                )
            else:
                persisted.mark = mark.value
                persisted.note = note
                persisted.actor = actor
                persisted.updated_at = instant.isoformat()
            self._audit(
                session,
                study_id,
                row.task_id,
                "FACTOR_STUDY_DECISION_CHANGED",
                actor,
                {
                    "factor_study_id": study_id,
                    "signal_variant": key.signal_variant,
                    "label_kind": key.label_kind,
                    "factor_ref": key.factor_ref,
                    "horizon": key.horizon,
                    "mark": mark.value,
                },
                instant,
            )

    def delete(self, study_id: str, *, actor: str = "system") -> None:
        """删除终态研究。入参：研究 ID 和操作者。返回值：无。异常：研究非终态或不存在时抛出。"""
        instant = self._now(None)
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status not in _TERMINAL:
                raise ValueError("active factor study cannot be deleted")
            self._audit(
                session,
                study_id,
                row.task_id,
                "FACTOR_STUDY_DELETED",
                actor,
                {},
                instant,
            )
            session.execute(
                update(TaskORM)
                .where(TaskORM.id == row.task_id)
                .values(subject_kind=None, subject_id=None, idempotency_key=None)
            )
            session.delete(row)

    def retry(self, study_id: str, *, actor: str = "system") -> str:
        """重试研究。入参：研究 ID 和操作者。返回值：复用任务 ID。异常：研究或任务不可重试时抛出。"""
        instant = self._now(None)
        progress = TaskProgress(stage="QUEUED", completed=0, total=0, message="")
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status not in {
                FactorStudyStatus.FAILED.value,
                FactorStudyStatus.CANCELLED.value,
            }:
                raise ValueError("only failed or cancelled factor studies can retry")
            task = session.get(TaskORM, row.task_id)
            if task is None or task.status not in {
                "FAILED",
                "CANCELLED",
                "ORPHANED",
            }:
                raise ValueError("factor study task is not retryable")
            stamp = instant.isoformat()
            task.status = "QUEUED"
            task.available_at = stamp
            task.updated_at = stamp
            task.completed_at = None
            task.heartbeat_at = None
            task.worker_id = None
            task.locked_at = None
            task.error_json = None
            task.result_json = None
            task.progress_json = canonical_json_bytes(
                progress.model_dump(mode="json")
            ).decode("utf-8")
            row.status = FactorStudyStatus.QUEUED.value
            row.stage = FactorStudyStage.VALIDATE.value
            row.error_json = None
            row.started_at = None
            row.completed_at = None
            row.artifact_dir = None
            row.manifest_hash = None
            self._audit(
                session,
                study_id,
                row.task_id,
                "FACTOR_STUDY_RETRIED",
                actor,
                {},
                instant,
            )
            return row.task_id

    @staticmethod
    def _validate_decision_key(
        definition: FactorStudyDefinition, key: FactorStudyDecisionKey
    ) -> None:
        variants = {"DIRECTION_ADJUSTED"}
        if definition.industry is not None:
            variants.add("INDUSTRY_NEUTRALIZED")
        if key.signal_variant not in variants:
            raise ValueError("decision signal_variant is not part of the study")
        if key.label_kind not in LABEL_KINDS:
            raise ValueError("decision label_kind is not part of the study")
        if key.factor_ref not in definition.factor_ids:
            raise ValueError("decision factor_ref is not part of the study")
        if key.horizon not in definition.horizons:
            raise ValueError("decision horizon is not part of the study")

    @staticmethod
    def _decision_unit_count(definition: FactorStudyDefinition) -> int:
        variants = 2 if definition.industry is not None else 1
        return variants * len(LABEL_KINDS) * len(definition.factor_ids) * len(
            definition.horizons
        )

    @staticmethod
    def _record(session: Session, row: FactorStudyORM) -> FactorStudyRecord:
        metrics = tuple(
            FactorStudyMetricRecord(
                name=item.name,
                value=item.value,
                unit=item.unit,
                p_value=item.p_value,
                adjusted_p_value=item.adjusted_p_value,
            )
            for item in session.scalars(
                select(FactorStudyMetricORM)
                .where(FactorStudyMetricORM.factor_study_id == row.id)
                .order_by(FactorStudyMetricORM.name)
            ).all()
        )
        artifacts = tuple(
            FactorStudyArtifactRecord(
                artifact_type=item.artifact_type,
                relative_path=item.relative_path,
                content_hash=item.content_hash,
                byte_count=item.byte_count,
                row_count=item.row_count,
                schema=json.loads(item.schema_json) if item.schema_json else None,
            )
            for item in session.scalars(
                select(FactorStudyArtifactORM)
                .where(FactorStudyArtifactORM.factor_study_id == row.id)
                .order_by(FactorStudyArtifactORM.artifact_type)
            ).all()
        )
        decisions = tuple(
            FactorStudyDecisionRecord(
                signal_variant=item.signal_variant,
                label_kind=item.label_kind,
                factor_ref=item.factor_ref,
                horizon=item.horizon,
                mark=FactorDecisionMark(item.mark),
                note=item.note,
                actor=item.actor,
                updated_at=datetime.fromisoformat(item.updated_at),
            )
            for item in session.scalars(
                select(FactorStudyDecisionORM)
                .where(FactorStudyDecisionORM.factor_study_id == row.id)
                .order_by(
                    FactorStudyDecisionORM.signal_variant,
                    FactorStudyDecisionORM.label_kind,
                    FactorStudyDecisionORM.factor_ref,
                    FactorStudyDecisionORM.horizon,
                )
            ).all()
        )
        return FactorStudyRecord(
            id=row.id,
            definition=FactorStudyDefinition.model_validate_json(row.definition_json),
            config_hash=row.config_hash,
            catalog_hash=row.catalog_hash,
            status=FactorStudyStatus(row.status),
            stage=FactorStudyStage(row.stage),
            task_id=row.task_id,
            artifact_dir=row.artifact_dir,
            manifest_hash=row.manifest_hash,
            error=cast(
                dict[str, JsonValue] | None,
                json.loads(row.error_json) if row.error_json else None,
            ),
            created_at=datetime.fromisoformat(row.created_at),
            started_at=datetime.fromisoformat(row.started_at) if row.started_at else None,
            completed_at=(
                datetime.fromisoformat(row.completed_at) if row.completed_at else None
            ),
            metrics=metrics,
            artifacts=artifacts,
            decisions=decisions,
        )

    @staticmethod
    def _audit(
        session: Session,
        study_id: str,
        task_id: str | None,
        event: str,
        actor: str,
        details: dict[str, JsonValue],
        now: datetime,
    ) -> None:
        session.add(
            AuditEventORM(
                run_id=None,
                subject_kind="FACTOR_STUDY",
                subject_id=study_id,
                task_id=task_id,
                event_type=event,
                actor=actor,
                details_json=canonical_json_bytes(details).decode("utf-8"),
                created_at=now.isoformat(),
            )
        )

    @staticmethod
    def _row(session: Session, study_id: str) -> FactorStudyORM:
        row = session.get(FactorStudyORM, study_id)
        if row is None:
            raise KeyError(f"factor study does not exist: {study_id}")
        return row

    @staticmethod
    def _now(value: datetime | None) -> datetime:
        result = value or datetime.now(UTC)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return result.astimezone(UTC)

    @staticmethod
    def _id() -> str:
        value = ((time.time_ns() // 1_000_000) & ((1 << 48) - 1)) << 80
        value |= secrets.randbits(80)
        chars: list[str] = []
        for _ in range(26):
            chars.append(_CROCKFORD[value & 31])
            value >>= 5
        return "".join(reversed(chars))


__all__ = ["FactorStudyRegistry"]
