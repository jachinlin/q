"""实现单一 StrategyStudy 的 SQLite 登记簿。"""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import Engine, delete, select
from sqlalchemy.orm import Session

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    StrategyStudyArtifactORM,
    StrategyStudyMetricORM,
    StrategyStudyORM,
    StrategyStudyTagORM,
    TaskORM,
)
from quant_research.strategy_studies.models import (
    StrategyStudyArtifactRecord,
    StrategyStudyDefinition,
    StrategyStudyMetricRecord,
    StrategyStudyRecord,
    StrategyStudyStage,
    StrategyStudyStatus,
)
from quant_research.tasks.models import TaskProgress

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_TERMINAL = frozenset(
    {
        StrategyStudyStatus.SUCCEEDED.value,
        StrategyStudyStatus.FAILED.value,
        StrategyStudyStatus.CANCELLED.value,
    }
)


class StrategyStudyRegistry:
    """维护研究事务。入参：数据库引擎。返回值：登记簿实例。异常：数据库失败时传播。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create(
        self,
        definition: StrategyStudyDefinition,
        config_hash: str,
        catalog_hash: str,
        *,
        actor: str = "system",
        now: datetime | None = None,
    ) -> tuple[str, str]:
        """原子创建研究和任务。入参：定义、哈希、操作者和时间。返回值：研究与任务 ID。异常：身份或事务非法时传播。"""

        if len(config_hash) != 64 or len(catalog_hash) != 64:
            raise ValueError("strategy study hashes must be SHA-256 digests")
        instant = self._now(now)
        study_id, task_id = self._id(), self._id()
        stamp = instant.isoformat()
        progress = TaskProgress(stage="QUEUED", completed=0, total=0, message="")
        definition_json = canonical_json_bytes(
            definition.model_dump(mode="json")
        ).decode("utf-8")
        with Session(self._engine) as session, session.begin():
            session.add(
                TaskORM(
                    id=task_id,
                    subject_kind="STRATEGY_STUDY",
                    subject_id=study_id,
                    task_type="STRATEGY_STUDY",
                    payload_json=canonical_json_bytes(
                        {"strategy_study_id": study_id}
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
                    idempotency_key=f"strategy-study:{study_id}",
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            session.flush()
            session.add(
                StrategyStudyORM(
                    id=study_id,
                    name=definition.name,
                    description=definition.description,
                    definition_json=definition_json,
                    config_hash=config_hash,
                    catalog_hash=catalog_hash,
                    status=StrategyStudyStatus.QUEUED.value,
                    stage=StrategyStudyStage.VALIDATE.value,
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
                    StrategyStudyTagORM(strategy_study_id=study_id, tag=tag)
                )
            self._audit(
                session,
                study_id,
                task_id,
                "STRATEGY_STUDY_CREATED",
                actor,
                {},
                instant,
            )
        return study_id, task_id

    def get(self, study_id: str) -> StrategyStudyRecord:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：不存在时抛出键错误。"""

        with Session(self._engine) as session:
            return self._record(session, self._row(session, study_id))

    def list(
        self,
        *,
        limit: int,
        offset: int,
        status: StrategyStudyStatus | None = None,
    ) -> tuple[StrategyStudyRecord, ...]:
        """分页列出研究。入参：分页和状态。返回值：稳定快照元组。异常：分页非法时抛出值错误。"""

        if limit <= 0 or offset < 0:
            raise ValueError("invalid pagination")
        with Session(self._engine) as session:
            statement = select(StrategyStudyORM)
            if status is not None:
                statement = statement.where(StrategyStudyORM.status == status.value)
            rows = session.scalars(
                statement.order_by(
                    StrategyStudyORM.created_at.desc(), StrategyStudyORM.id.desc()
                )
                .limit(limit)
                .offset(offset)
            ).all()
            return tuple(self._record(session, row) for row in rows)

    def update_stage(self, study_id: str, stage: StrategyStudyStage) -> None:
        """更新阶段。入参：研究 ID 和阶段。返回值：无。异常：非运行中状态时抛出值错误。"""

        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != StrategyStudyStatus.RUNNING.value:
                raise ValueError("strategy study must be running to update stage")
            row.stage = stage.value

    def transition(
        self,
        study_id: str,
        expected: StrategyStudyStatus,
        target: StrategyStudyStatus,
        *,
        stage: StrategyStudyStage,
        error: dict[str, JsonValue] | None = None,
        artifact_dir: str | None = None,
        manifest_hash: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """提交状态迁移。入参：研究 ID、状态和终态证据。返回值：无。异常：冲突或证据缺失时抛出值错误。"""

        instant = self._now(now)
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != expected.value:
                raise ValueError("strategy study status conflict")
            if target is StrategyStudyStatus.SUCCEEDED and (
                not artifact_dir or not manifest_hash or len(manifest_hash) != 64
            ):
                raise ValueError("successful strategy study requires artifact evidence")
            row.status = target.value
            row.stage = stage.value
            row.error_json = (
                canonical_json_bytes(error).decode("utf-8") if error else None
            )
            row.artifact_dir = artifact_dir
            row.manifest_hash = manifest_hash
            if target is StrategyStudyStatus.RUNNING:
                row.started_at = instant.isoformat()
                row.completed_at = None
            if target.value in _TERMINAL:
                row.completed_at = instant.isoformat()

    def register_outputs(
        self,
        study_id: str,
        metrics: Mapping[str, tuple[float, str | None]],
        artifacts: tuple[dict[str, JsonValue], ...],
    ) -> None:
        """登记输出。入参：研究 ID、指标和产物。返回值：无。异常：状态或事务非法时传播。"""

        instant = self._now(None).isoformat()
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status != StrategyStudyStatus.RUNNING.value:
                raise ValueError("outputs require a running strategy study")
            self._delete_outputs(session, study_id)
            for name, values in sorted(metrics.items()):
                session.add(
                    StrategyStudyMetricORM(
                        strategy_study_id=study_id,
                        name=name,
                        value=values[0],
                        unit=values[1],
                        created_at=instant,
                    )
                )
            for item in sorted(artifacts, key=lambda value: str(value["artifact_type"])):
                schema = item.get("schema")
                session.add(
                    StrategyStudyArtifactORM(
                        strategy_study_id=study_id,
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
        """清理输出登记。入参：研究 ID。返回值：无。异常：研究不存在或事务失败时传播。"""

        with Session(self._engine) as session, session.begin():
            self._row(session, study_id)
            self._delete_outputs(session, study_id)

    def delete(self, study_id: str, *, actor: str = "system") -> None:
        """删除终态研究。入参：研究 ID 和操作者。返回值：无。异常：活动研究或事务失败时传播。"""

        instant = self._now(None)
        with Session(self._engine) as session, session.begin():
            row = self._row(session, study_id)
            if row.status not in _TERMINAL:
                raise ValueError("active strategy study cannot be deleted")
            task = session.get(TaskORM, row.task_id)
            self._audit(
                session,
                study_id,
                task.id if task is not None else None,
                "STRATEGY_STUDY_DELETED",
                actor,
                {},
                instant,
            )
            if task is not None:
                task.subject_kind = None
                task.subject_id = None
                task.idempotency_key = None
            session.delete(row)

    @staticmethod
    def _delete_outputs(session: Session, study_id: str) -> None:
        session.execute(
            delete(StrategyStudyMetricORM).where(
                StrategyStudyMetricORM.strategy_study_id == study_id
            )
        )
        session.execute(
            delete(StrategyStudyArtifactORM).where(
                StrategyStudyArtifactORM.strategy_study_id == study_id
            )
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
                subject_kind="STRATEGY_STUDY",
                subject_id=study_id,
                task_id=task_id,
                event_type=event,
                actor=actor,
                details_json=canonical_json_bytes(details).decode("utf-8"),
                created_at=now.isoformat(),
            )
        )

    @staticmethod
    def _record(session: Session, row: StrategyStudyORM) -> StrategyStudyRecord:
        metrics = tuple(
            StrategyStudyMetricRecord(name=item.name, value=item.value, unit=item.unit)
            for item in session.scalars(
                select(StrategyStudyMetricORM)
                .where(StrategyStudyMetricORM.strategy_study_id == row.id)
                .order_by(StrategyStudyMetricORM.name)
            ).all()
        )
        artifacts = tuple(
            StrategyStudyArtifactRecord(
                artifact_type=item.artifact_type,
                relative_path=item.relative_path,
                content_hash=item.content_hash,
                byte_count=item.byte_count,
                row_count=item.row_count,
                schema=json.loads(item.schema_json) if item.schema_json else None,
            )
            for item in session.scalars(
                select(StrategyStudyArtifactORM)
                .where(StrategyStudyArtifactORM.strategy_study_id == row.id)
                .order_by(StrategyStudyArtifactORM.artifact_type)
            ).all()
        )
        return StrategyStudyRecord(
            id=row.id,
            definition=StrategyStudyDefinition.model_validate_json(
                row.definition_json
            ),
            config_hash=row.config_hash,
            catalog_hash=row.catalog_hash,
            status=StrategyStudyStatus(row.status),
            stage=StrategyStudyStage(row.stage),
            task_id=row.task_id,
            artifact_dir=row.artifact_dir,
            manifest_hash=row.manifest_hash,
            error=json.loads(row.error_json) if row.error_json else None,
            created_at=datetime.fromisoformat(row.created_at),
            started_at=datetime.fromisoformat(row.started_at)
            if row.started_at
            else None,
            completed_at=datetime.fromisoformat(row.completed_at)
            if row.completed_at
            else None,
            metrics=metrics,
            artifacts=artifacts,
        )

    @staticmethod
    def _row(session: Session, study_id: str) -> StrategyStudyORM:
        row = session.get(StrategyStudyORM, study_id)
        if row is None:
            raise KeyError(f"strategy study does not exist: {study_id}")
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


__all__ = ["StrategyStudyRegistry"]
