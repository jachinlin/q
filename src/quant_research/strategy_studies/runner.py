"""执行单一策略研究的四阶段状态机。"""

from __future__ import annotations

from typing import Protocol

from quant_research.data.contracts import JsonValue
from quant_research.strategy_studies.models import (
    STRATEGY_STUDY_STAGES,
    StrategyStudyRecord,
    StrategyStudyStage,
    StrategyStudyStatus,
)
from quant_research.strategy_studies.progress import StrategyStudyProgressReporter
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskStatus,
)


class CatalogGuard(Protocol):
    """校验目录身份。入参：实现依赖。返回值：守卫实例。异常：身份漂移时由实现抛出。"""

    def assert_unchanged(self, catalog_hash: str) -> None:
        """比较目录身份。入参：捕获哈希。返回值：无。异常：目录变化时抛出异常。"""
        ...


class StrategyStudyRegistry(Protocol):
    """定义研究持久化端口。入参：实现依赖。返回值：端口实例。异常：实现保留持久化异常。"""

    def get(self, study_id: str) -> StrategyStudyRecord:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：不存在时抛出键错误。"""
        ...

    def update_stage(self, study_id: str, stage: StrategyStudyStage) -> None:
        """更新阶段。入参：研究 ID 和阶段。返回值：无。异常：状态冲突时抛出值错误。"""
        ...

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
    ) -> None:
        """提交状态迁移。入参：研究 ID、前后状态及证据。返回值：无。异常：冲突时抛出值错误。"""
        ...

    def discard_outputs(self, study_id: str) -> None:
        """清理输出。入参：研究 ID。返回值：无。异常：持久化失败时传播。"""
        ...


class StrategyStudyExecutionSession(Protocol):
    """保存阶段共享状态。入参：会话依赖。返回值：会话实例。异常：构造异常由实现定义。"""

    def execute(
        self,
        stage: StrategyStudyStage,
        progress: StrategyStudyProgressReporter,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """执行阶段。入参：阶段、进度和取消端口。返回值：阶段结果。异常：执行失败时传播。"""
        ...

    def abort(self) -> None:
        """撤销输出。入参：无。返回值：无。异常：清理失败时传播。"""
        ...


class StrategyStudyExecutor(Protocol):
    """创建隔离执行会话。入参：执行依赖。返回值：执行器实例。异常：依赖非法时由实现抛出。"""

    def create(self, study: StrategyStudyRecord) -> StrategyStudyExecutionSession:
        """创建会话。入参：冻结研究。返回值：任务会话。异常：配置非法时传播。"""
        ...


class StrategyStudyHandler:
    """执行四阶段任务。入参：登记簿、目录守卫和执行器。返回值：处理器实例。异常：依赖非法时传播。"""

    task_type = "STRATEGY_STUDY"

    def __init__(
        self,
        registry: StrategyStudyRegistry,
        catalog: CatalogGuard,
        executor: StrategyStudyExecutor,
    ) -> None:
        self._registry = registry
        self._catalog = catalog
        self._executor = executor

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """执行一个研究。入参：任务、进度和取消端口。返回值：任务结果。异常：阶段失败时登记失败并传播。"""

        if task.subject_kind != "STRATEGY_STUDY" or task.subject_id is None:
            raise ValueError("STRATEGY_STUDY task must bind one study")
        study = self._registry.get(task.subject_id)
        self._registry.transition(
            study.id,
            StrategyStudyStatus.QUEUED,
            StrategyStudyStatus.RUNNING,
            stage=StrategyStudyStage.VALIDATE,
        )
        result: dict[str, JsonValue] = {}
        session: StrategyStudyExecutionSession | None = None
        reporter = StrategyStudyProgressReporter(progress)
        try:
            session = self._executor.create(study)
            for stage in STRATEGY_STUDY_STAGES:
                self._catalog.assert_unchanged(study.catalog_hash)
                if cancellation.is_cancelled():
                    session.abort()
                    self._registry.transition(
                        study.id,
                        StrategyStudyStatus.RUNNING,
                        StrategyStudyStatus.CANCELLED,
                        stage=stage,
                    )
                    return TaskOutcome(status=TaskStatus.CANCELLED)
                self._registry.update_stage(study.id, stage)
                reporter.stage_started(stage)
                stage_result = session.execute(stage, reporter, cancellation)
                if stage_result:
                    result = stage_result
                self._catalog.assert_unchanged(study.catalog_hash)
                if cancellation.is_cancelled():
                    session.abort()
                    self._registry.transition(
                        study.id,
                        StrategyStudyStatus.RUNNING,
                        StrategyStudyStatus.CANCELLED,
                        stage=stage,
                    )
                    return TaskOutcome(status=TaskStatus.CANCELLED)
                evidence: dict[str, JsonValue] = {}
                if stage is StrategyStudyStage.PUBLISH:
                    manifest_hash = stage_result.get("manifest_hash")
                    if isinstance(manifest_hash, str):
                        evidence["manifest_hash"] = manifest_hash
                reporter.stage_completed(stage, evidence)
            if not isinstance(result.get("artifact_dir"), str) or not isinstance(
                result.get("manifest_hash"), str
            ):
                raise TypeError("PUBLISH did not return verified artifact evidence")
            self._registry.transition(
                study.id,
                StrategyStudyStatus.RUNNING,
                StrategyStudyStatus.SUCCEEDED,
                stage=StrategyStudyStage.PUBLISH,
                artifact_dir=str(result["artifact_dir"]),
                manifest_hash=str(result["manifest_hash"]),
            )
            return TaskOutcome(
                status=TaskStatus.SUCCEEDED,
                result={"strategy_study_id": study.id, **result},
            )
        except Exception as error:
            if session is not None:
                session.abort()
            current = self._registry.get(study.id)
            if cancellation.is_cancelled():
                self._registry.transition(
                    study.id,
                    StrategyStudyStatus.RUNNING,
                    StrategyStudyStatus.CANCELLED,
                    stage=current.stage,
                )
                return TaskOutcome(status=TaskStatus.CANCELLED)
            self._registry.transition(
                study.id,
                StrategyStudyStatus.RUNNING,
                StrategyStudyStatus.FAILED,
                stage=current.stage,
                error={
                    "code": "STRATEGY_STUDY_STAGE_FAILED",
                    "error_type": type(error).__name__,
                    "substage": reporter.current_substage,
                },
            )
            raise


__all__ = [
    "StrategyStudyExecutionSession",
    "StrategyStudyExecutor",
    "StrategyStudyHandler",
    "StrategyStudyRegistry",
]
