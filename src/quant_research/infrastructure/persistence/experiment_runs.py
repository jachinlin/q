"""实现统一 Experiment → Run 聚合的 SQLite 登记簿。"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import Engine, delete, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.experiments.models import (
    ExperimentAggregate,
    ExperimentDefinition,
    ExperimentRecord,
    ResearchMark,
    RunArtifactRecord,
    RunConfig,
    RunMetricRecord,
    RunRecord,
    RunStage,
    RunStatus,
)
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    ExperimentORM,
    ExperimentTagORM,
    RunArtifactORM,
    RunMetricORM,
    RunORM,
    RunTagORM,
    TaskORM,
)
from quant_research.tasks.models import TaskProgress, TaskStatus

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.SUCCEEDED.value, RunStatus.FAILED.value, RunStatus.CANCELLED.value}
)
_TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED.value,
        TaskStatus.FAILED.value,
        TaskStatus.CANCELLED.value,
        TaskStatus.ORPHANED.value,
    }
)


class ExperimentRunRegistry:
    """以事务维护实验、Run、任务、指标、产物和审计。

    入参：
        engine：已迁移到当前 Schema 的 SQLite 引擎。
    返回值：
        创建统一实验登记簿实例。
    异常：
        构造不访问数据库；各操作在约束冲突或存储失败时抛出明确异常。
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create(
        self,
        definition: ExperimentDefinition,
        catalog_hash: str,
        *,
        actor: str = "system",
        now: datetime | None = None,
    ) -> tuple[str, str, str]:
        """原子创建实验、首个 Run 和唯一 EXPERIMENT_RUN 任务。

        入参：
            definition：冻结实验定义；catalog_hash：提交时数据目录身份；
            actor、now：审计操作者和可注入时间。
        返回值：
            返回 experiment_id、run_id 和 task_id。
        异常：
            ValueError：定义或目录身份非法时抛出；数据库写入失败时事务回滚。
        """
        instant = self._now(now)
        experiment_id, run_id, task_id = self._id(), self._id(), self._id()
        definition_json = self._model_json(definition)
        with Session(self._engine) as session, session.begin():
            session.add(
                ExperimentORM(
                    id=experiment_id,
                    name=definition.name,
                    description=definition.description,
                    kind=definition.kind.value,
                    definition_json=definition_json,
                    definition_hash=self._hash(definition_json),
                    baseline_run_id=None,
                    created_at=instant.isoformat(),
                )
            )
            for tag in definition.tags:
                session.add(ExperimentTagORM(experiment_id=experiment_id, tag=tag))
            session.flush()
            self._insert_run_task(
                session,
                experiment_id,
                run_id,
                task_id,
                definition,
                definition.initial_run,
                catalog_hash,
                instant,
            )
            self._audit(
                session,
                run_id,
                task_id,
                "EXPERIMENT_CREATED",
                actor,
                {"experiment_id": experiment_id},
                instant,
            )
        return experiment_id, run_id, task_id

    def add_run(
        self,
        experiment_id: str,
        config: RunConfig,
        catalog_hash: str,
        *,
        tags: tuple[str, ...] = (),
        actor: str = "system",
        now: datetime | None = None,
    ) -> tuple[str, str]:
        """验证协议后创建不可变派生 Run 和任务。

        入参：
            experiment_id：所属实验；config：冻结 Run 配置；catalog_hash：数据身份；
            tags、actor、now：标签与审计信息。
        返回值：
            返回新 run_id 和 task_id。
        异常：
            KeyError：实验不存在时抛出；ValueError：配置越过协议或类型不匹配时抛出。
        """
        instant, run_id, task_id = self._now(now), self._id(), self._id()
        with Session(self._engine) as session, session.begin():
            experiment = self._experiment_row(session, experiment_id)
            definition = ExperimentDefinition.model_validate_json(
                experiment.definition_json
            )
            definition.validate_run(config)
            self._insert_run_task(
                session,
                experiment_id,
                run_id,
                task_id,
                definition,
                config,
                catalog_hash,
                instant,
            )
            for tag in tuple(sorted(set(tags))):
                session.add(RunTagORM(run_id=run_id, tag=tag))
            used = cast(
                int,
                session.scalar(
                    select(func.count())
                    .select_from(RunORM)
                    .where(
                        RunORM.experiment_id == experiment_id,
                        RunORM.uses_test_region.is_(True),
                    )
                ),
            )
            if (
                definition.uses_test_region(config)
                and used > definition.governance.test_budget
            ):
                self._audit(
                    session,
                    run_id,
                    task_id,
                    "TEST_BUDGET_EXCEEDED",
                    actor,
                    {
                        "budget": definition.governance.test_budget,
                        "uses": used,
                    },
                    instant,
                )
            self._audit(session, run_id, task_id, "RUN_CREATED", actor, {}, instant)
        return run_id, task_id

    def rerun(
        self, run_id: str, catalog_hash: str, *, actor: str = "system"
    ) -> tuple[str, str]:
        """复制冻结配置并创建新 Run；旧 Run 和产物保持不变。

        入参：
            run_id：源 Run；catalog_hash：重跑时捕获的新数据身份；actor：操作者。
        返回值：
            返回新 run_id 和新 task_id。
        异常：
            KeyError：源 Run 不存在时抛出；配置或数据库错误与 add_run 一致。
        """
        source = self.get_run(run_id)
        return self.add_run(
            source.experiment_id,
            source.config,
            catalog_hash,
            actor=actor,
            tags=(f"rerun-of:{run_id}",),
        )

    def get_experiment(self, experiment_id: str) -> ExperimentAggregate:
        """读取实验及其按创建顺序排列的全部 Run。

        入参：
            experiment_id：实验标识。
        返回值：
            返回实验定义、标签和稳定排序 Run 的聚合快照。
        异常：
            KeyError：实验不存在时抛出；冻结 JSON 损坏时抛出模型校验异常。
        """
        with Session(self._engine) as session:
            row = self._experiment_row(session, experiment_id)
            run_rows = session.scalars(
                select(RunORM)
                .where(RunORM.experiment_id == experiment_id)
                .order_by(RunORM.created_at, RunORM.id)
            ).all()
            tags = tuple(
                session.scalars(
                    select(ExperimentTagORM.tag)
                    .where(ExperimentTagORM.experiment_id == experiment_id)
                    .order_by(ExperimentTagORM.tag)
                ).all()
            )
            record = ExperimentRecord(
                id=row.id,
                definition=ExperimentDefinition.model_validate_json(
                    row.definition_json
                ),
                baseline_run_id=row.baseline_run_id,
                created_at=datetime.fromisoformat(row.created_at),
            )
            runs = tuple(self._run(session, item) for item in run_rows)
        return ExperimentAggregate(
            experiment=record,
            runs=runs,
            tags=tags,
        )

    def get_run(self, run_id: str) -> RunRecord:
        """读取一个 Run 快照。

        入参：
            run_id：Run 标识。
        返回值：
            返回冻结配置、状态、目录身份和发布结果快照。
        异常：
            KeyError：Run 不存在时抛出；配置 JSON 损坏时抛出模型校验异常。
        """
        with Session(self._engine) as session:
            row = session.get(RunORM, run_id)
            if row is None:
                raise KeyError(f"run does not exist: {run_id}")
            return self._run(session, row)

    def list_experiments(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[ExperimentRecord, ...]:
        """按创建时间倒序分页读取实验。

        入参：
            limit、offset：分页大小与偏移量。
        返回值：
            返回按创建时间和标识倒序排列的实验元组。
        异常：
            数据库读取失败或定义 JSON 损坏时传播对应异常。
        """
        with Session(self._engine) as session:
            rows = session.scalars(
                select(ExperimentORM)
                .order_by(ExperimentORM.created_at.desc(), ExperimentORM.id.desc())
                .limit(limit)
                .offset(offset)
            ).all()
        return tuple(
            ExperimentRecord(
                id=row.id,
                definition=ExperimentDefinition.model_validate_json(
                    row.definition_json
                ),
                baseline_run_id=row.baseline_run_id,
                created_at=datetime.fromisoformat(row.created_at),
            )
            for row in rows
        )

    def transition(
        self,
        run_id: str,
        expected: RunStatus,
        target: RunStatus,
        *,
        stage: RunStage,
        error: dict[str, JsonValue] | None = None,
        artifact_dir: str | None = None,
        manifest_hash: str | None = None,
    ) -> None:
        """以 CAS 迁移 Run 状态并绑定终态发布结果。

        入参：
            run_id、expected、target、stage：Run 身份和状态迁移；error、
            artifact_dir、manifest_hash：可选终态发布字段。
        返回值：
            唯一记录迁移成功后返回 None。
        异常：
            ValueError：Run 不存在或当前状态不等于 expected 时抛出。
        """
        now = datetime.now(UTC).isoformat()
        values: dict[str, object] = {"status": target.value, "stage": stage.value}
        if target is RunStatus.RUNNING:
            values["started_at"] = now
        if target in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}:
            values["completed_at"] = now
        if error is not None:
            values["error_json"] = canonical_json_bytes(error).decode("utf-8")
        if artifact_dir is not None:
            values["artifact_dir"] = artifact_dir
        if manifest_hash is not None:
            values["manifest_hash"] = manifest_hash
        with Session(self._engine) as session, session.begin():
            changed = cast(
                CursorResult[object],
                session.execute(
                    update(RunORM)
                    .where(RunORM.id == run_id, RunORM.status == expected.value)
                    .values(**values)
                ),
            ).rowcount
            if changed != 1:
                raise ValueError("RUN_STATE_CONFLICT")

    def update_stage(self, run_id: str, stage: RunStage) -> None:
        """在 RUNNING 状态以 CAS 更新当前阶段。

        入参：
            run_id：Run 标识；stage：即将执行或刚完成的阶段。
        返回值：
            唯一运行中记录更新成功后返回 None。
        异常：
            ValueError：Run 不存在或不处于 RUNNING 时抛出。
        """
        with Session(self._engine) as session, session.begin():
            changed = cast(
                CursorResult[object],
                session.execute(
                    update(RunORM)
                    .where(
                        RunORM.id == run_id, RunORM.status == RunStatus.RUNNING.value
                    )
                    .values(stage=stage.value)
                ),
            ).rowcount
            if changed != 1:
                raise ValueError("RUN_STATE_CONFLICT")

    def mark(self, run_id: str, mark: ResearchMark, *, actor: str = "user") -> None:
        """审计修改 Run 标记，并维护实验的精确 baseline 指针。

        入参：
            run_id：Run 标识；mark：新研究标记；actor：操作者。
        返回值：
            Run 标记和实验 baseline 指针在同一事务提交后返回 None。
        异常：
            KeyError：Run 或所属实验不存在时抛出；数据库失败时事务回滚。
        """
        now = datetime.now(UTC)
        with Session(self._engine) as session, session.begin():
            row = session.get(RunORM, run_id)
            if row is None:
                raise KeyError(f"run does not exist: {run_id}")
            experiment = self._experiment_row(session, row.experiment_id)
            if mark is ResearchMark.BASELINE:
                if (
                    experiment.baseline_run_id is not None
                    and experiment.baseline_run_id != run_id
                ):
                    session.execute(
                        update(RunORM)
                        .where(RunORM.id == experiment.baseline_run_id)
                        .values(research_mark=ResearchMark.UNREVIEWED.value)
                    )
                experiment.baseline_run_id = run_id
            elif experiment.baseline_run_id == run_id:
                experiment.baseline_run_id = None
            row.research_mark = mark.value
            self._audit(
                session,
                run_id,
                row.task_id,
                "RUN_RESEARCH_MARK_CHANGED",
                actor,
                {"mark": mark.value},
                now,
            )

    def register_outputs(
        self,
        run_id: str,
        metrics: dict[str, tuple[float, str | None, float | None, float | None]],
        artifacts: tuple[dict[str, JsonValue], ...],
    ) -> None:
        """登记已从最终目录复核的指标和 Manifest 产物。

        入参：
            run_id：Run 标识；metrics：指标及单位、显著性；artifacts：可信
            Manifest 中的产物记录。
        返回值：
            全部指标和产物原子登记后返回 None。
        异常：
            数据库约束拒绝不存在的 Run、重复指标或重复产物类型时抛出。
        """
        now = datetime.now(UTC).isoformat()
        with Session(self._engine) as session, session.begin():
            for name, values in sorted(metrics.items()):
                value, unit, p_value, adjusted = values
                session.add(
                    RunMetricORM(
                        run_id=run_id,
                        name=name,
                        value=value,
                        unit=unit,
                        p_value=p_value,
                        adjusted_p_value=adjusted,
                        created_at=now,
                    )
                )
            for item in sorted(
                artifacts, key=lambda value: cast(str, value["artifact_type"])
            ):
                session.add(
                    RunArtifactORM(
                        run_id=run_id,
                        artifact_type=cast(str, item["artifact_type"]),
                        relative_path=cast(str, item["relative_path"]),
                        content_hash=cast(str, item["content_hash"]),
                        byte_count=cast(int, item["byte_count"]),
                        row_count=cast(int | None, item.get("row_count")),
                        schema_json=json.dumps(item.get("schema"), sort_keys=True)
                        if item.get("schema") is not None
                        else None,
                        created_at=now,
                    )
                )

    def discard_outputs(self, run_id: str) -> None:
        """事务删除失败或取消 Run 的全部输出登记。

        入参：run_id：尚未成功提交的 Run 标识。返回值：删除完成后无返回。
        异常：Run 不存在或已经成功时抛出 ``ValueError``；数据库错误继续传播。
        """
        with Session(self._engine) as session, session.begin():
            row = session.get(RunORM, run_id)
            if row is None:
                raise ValueError("run does not exist")
            if row.status == RunStatus.SUCCEEDED.value:
                raise ValueError("cannot discard outputs of a succeeded Run")
            session.execute(delete(RunMetricORM).where(RunMetricORM.run_id == run_id))
            session.execute(
                delete(RunArtifactORM).where(RunArtifactORM.run_id == run_id)
            )

    def delete_run(self, run_id: str, *, actor: str = "system") -> None:
        """事务删除一个终态 Run，并保留其独立任务与审计历史。

        入参：run_id：待删除 Run；actor：审计操作者。
        返回值：Run、标签、指标和产物登记级联删除后无返回。
        异常：KeyError：Run 不存在；ValueError：Run 尚未进入终态。
        """
        now = datetime.now(UTC)
        with Session(self._engine) as session, session.begin():
            row = session.get(RunORM, run_id)
            if row is None:
                raise KeyError(f"run does not exist: {run_id}")
            if row.status not in _TERMINAL_RUN_STATUSES:
                raise ValueError("active Run cannot be deleted")
            task = session.get(TaskORM, row.task_id) if row.task_id is not None else None
            if task is not None and task.status not in _TERMINAL_TASK_STATUSES:
                raise ValueError("Run task is still active")
            experiment = self._experiment_row(session, row.experiment_id)
            if experiment.baseline_run_id == run_id:
                experiment.baseline_run_id = None
            self._audit(
                session,
                run_id,
                task.id if task is not None else None,
                "RUN_DELETED",
                actor,
                {"experiment_id": row.experiment_id, "status": row.status},
                now,
            )
            session.execute(
                update(TaskORM)
                .where(
                    TaskORM.subject_kind == "EXPERIMENT_RUN",
                    TaskORM.subject_id == run_id,
                )
                .values(subject_kind=None, subject_id=None, idempotency_key=None)
            )
            session.execute(delete(RunORM).where(RunORM.id == run_id))

    def delete_experiment(
        self, experiment_id: str, *, actor: str = "system"
    ) -> None:
        """事务删除实验及其全部终态 Run，并保留任务与审计历史。

        入参：experiment_id：待删除实验；actor：审计操作者。
        返回值：实验聚合级联删除后无返回。
        异常：KeyError：实验不存在；ValueError：任一 Run 尚未进入终态。
        """
        now = datetime.now(UTC)
        with Session(self._engine) as session, session.begin():
            self._experiment_row(session, experiment_id)
            runs = session.scalars(
                select(RunORM)
                .where(RunORM.experiment_id == experiment_id)
                .order_by(RunORM.created_at, RunORM.id)
            ).all()
            if any(row.status not in _TERMINAL_RUN_STATUSES for row in runs):
                raise ValueError("Experiment with active Runs cannot be deleted")
            run_ids = [row.id for row in runs]
            active_task = (
                session.scalar(
                    select(TaskORM.id)
                    .where(
                        TaskORM.subject_kind == "EXPERIMENT_RUN",
                        TaskORM.subject_id.in_(run_ids),
                        TaskORM.status.not_in(_TERMINAL_TASK_STATUSES),
                    )
                    .limit(1)
                )
                if run_ids
                else None
            )
            if active_task is not None:
                raise ValueError("Experiment has active Run tasks")
            audit_run_ids: list[JsonValue] = [run_id for run_id in run_ids]
            session.add(
                AuditEventORM(
                    run_id=None,
                    subject_kind="EXPERIMENT",
                    subject_id=experiment_id,
                    task_id=None,
                    event_type="EXPERIMENT_DELETED",
                    actor=actor,
                    details_json=canonical_json_bytes(
                        {
                            "experiment_id": experiment_id,
                            "run_ids": audit_run_ids,
                        }
                    ).decode("utf-8"),
                    created_at=now.isoformat(),
                )
            )
            if run_ids:
                session.execute(
                    update(TaskORM)
                    .where(
                        TaskORM.subject_kind == "EXPERIMENT_RUN",
                        TaskORM.subject_id.in_(run_ids),
                    )
                    .values(subject_kind=None, subject_id=None, idempotency_key=None)
                )
            session.execute(
                delete(ExperimentORM).where(ExperimentORM.id == experiment_id)
            )

    @staticmethod
    def _insert_run_task(
        session: Session,
        experiment_id: str,
        run_id: str,
        task_id: str,
        definition: ExperimentDefinition,
        config: RunConfig,
        catalog_hash: str,
        instant: datetime,
    ) -> None:
        definition.validate_run(config)
        config_json = ExperimentRunRegistry._model_json(config)
        stamp = instant.isoformat()
        progress = TaskProgress(stage="queued", completed=0, total=0, message="")
        session.add(
            TaskORM(
                id=task_id,
                subject_kind="EXPERIMENT_RUN",
                subject_id=run_id,
                task_type="EXPERIMENT_RUN",
                payload_json=canonical_json_bytes({"run_id": run_id}).decode("utf-8"),
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
                idempotency_key=f"experiment-run:{run_id}",
                worker_id=None,
                locked_at=None,
                error_json=None,
                result_json=None,
            )
        )
        session.flush()
        session.add(
            RunORM(
                id=run_id,
                experiment_id=experiment_id,
                task_id=task_id,
                config_json=config_json,
                config_hash=ExperimentRunRegistry._hash(config_json),
                catalog_hash=catalog_hash,
                status=RunStatus.QUEUED.value,
                stage=RunStage.VALIDATE.value,
                research_mark=ResearchMark.UNREVIEWED.value,
                uses_test_region=definition.uses_test_region(config),
                artifact_dir=None,
                manifest_hash=None,
                error_json=None,
                created_at=stamp,
                started_at=None,
                completed_at=None,
            )
        )
        session.flush()

    @staticmethod
    def _audit(
        session: Session,
        run_id: str,
        task_id: str | None,
        event: str,
        actor: str,
        details: dict[str, JsonValue],
        now: datetime,
    ) -> None:
        session.add(
            AuditEventORM(
                run_id=run_id,
                subject_kind="EXPERIMENT_RUN",
                subject_id=run_id,
                task_id=task_id,
                event_type=event,
                actor=actor,
                details_json=canonical_json_bytes(details).decode("utf-8"),
                created_at=now.isoformat(),
            )
        )

    @staticmethod
    def _experiment_row(session: Session, experiment_id: str) -> ExperimentORM:
        row = session.get(ExperimentORM, experiment_id)
        if row is None:
            raise KeyError(f"experiment does not exist: {experiment_id}")
        return row

    @staticmethod
    def _run(session: Session, row: RunORM) -> RunRecord:
        from pydantic import TypeAdapter

        from quant_research.experiments.models import RunConfig as RunConfigType

        parsed: RunConfig = TypeAdapter(RunConfigType).validate_json(row.config_json)
        tags = tuple(
            session.scalars(
                select(RunTagORM.tag)
                .where(RunTagORM.run_id == row.id)
                .order_by(RunTagORM.tag)
            ).all()
        )
        metrics = tuple(
            RunMetricRecord(
                name=item.name,
                value=item.value,
                unit=item.unit,
                p_value=item.p_value,
                adjusted_p_value=item.adjusted_p_value,
            )
            for item in session.scalars(
                select(RunMetricORM)
                .where(RunMetricORM.run_id == row.id)
                .order_by(RunMetricORM.name)
            ).all()
        )
        artifacts = tuple(
            RunArtifactRecord(
                artifact_type=item.artifact_type,
                relative_path=item.relative_path,
                content_hash=item.content_hash,
                byte_count=item.byte_count,
                row_count=item.row_count,
                schema=json.loads(item.schema_json) if item.schema_json else None,
            )
            for item in session.scalars(
                select(RunArtifactORM)
                .where(RunArtifactORM.run_id == row.id)
                .order_by(RunArtifactORM.artifact_type)
            ).all()
        )
        return RunRecord(
            id=row.id,
            experiment_id=row.experiment_id,
            config=parsed,
            config_hash=row.config_hash,
            catalog_hash=row.catalog_hash,
            status=RunStatus(row.status),
            stage=RunStage(row.stage),
            research_mark=ResearchMark(row.research_mark),
            uses_test_region=row.uses_test_region,
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
            tags=tags,
            metrics=metrics,
            artifacts=artifacts,
        )

    @staticmethod
    def _model_json(model: object) -> str:
        from pydantic import BaseModel

        if not isinstance(model, BaseModel):
            raise TypeError("model must be a Pydantic model")
        return canonical_json_bytes(model.model_dump(mode="json")).decode("utf-8")

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _now(value: datetime | None) -> datetime:
        result = value or datetime.now(UTC)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return result.astimezone(UTC)

    @staticmethod
    def _id() -> str:
        value = (
            (time.time_ns() // 1_000_000) & ((1 << 48) - 1)
        ) << 80 | secrets.randbits(80)
        chars = []
        for _ in range(26):
            chars.append(_CROCKFORD[value & 31])
            value >>= 5
        return "".join(reversed(chars))


__all__ = ["ExperimentAggregate", "ExperimentRunRegistry"]
