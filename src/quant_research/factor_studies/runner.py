"""执行独立因子研究固定阶段图和状态机。"""

from __future__ import annotations

from typing import Protocol

from quant_research.data.contracts import JsonValue
from quant_research.factor_studies.models import (
    FACTOR_STUDY_STAGES,
    FactorStudyRecord,
    FactorStudyStage,
    FactorStudyStatus,
)
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)


class CatalogGuard(Protocol):
    """校验目录身份。入参：实现实例。返回值：守卫端口。异常：实现不满足协议时类型检查失败。"""

    def assert_unchanged(self, catalog_hash: str) -> None:
        """比较目录身份。入参：提交时哈希。返回值：无。异常：身份漂移时抛出领域错误。"""
        ...


class FactorStudyRunRegistry(Protocol):
    """定义执行仓储端口。入参：实现实例。返回值：仓储端口。异常：实现不满足协议时类型检查失败。"""

    def get(self, study_id: str) -> FactorStudyRecord:
        """读取研究。入参：研究 ID。返回值：研究快照。异常：研究不存在时抛出。"""
        ...

    def update_stage(self, study_id: str, stage: FactorStudyStage) -> None:
        """更新阶段。入参：研究 ID 和阶段。返回值：无。异常：状态冲突时抛出。"""
        ...

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
    ) -> None:
        """迁移研究状态。入参：研究、前后状态和终态证据。返回值：无。异常：CAS 冲突时抛出。"""
        ...


class FactorStudyExecutionSession(Protocol):
    """保存执行会话。入参：实现实例。返回值：会话端口。异常：实现不满足协议时类型检查失败。"""

    def execute(
        self,
        stage: FactorStudyStage,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """执行阶段。入参：阶段、进度和取消令牌。返回值：阶段证据。异常：计算或发布失败时抛出。"""
        ...

    def abort(self) -> None:
        """中止会话。入参：无。返回值：无。异常：清理失败时由实现抛出。"""
        ...


class FactorStudyExecutor(Protocol):
    """创建执行会话。入参：实现实例。返回值：执行器端口。异常：实现不满足协议时类型检查失败。"""

    def create(self, study: FactorStudyRecord) -> FactorStudyExecutionSession:
        """创建会话。入参：冻结研究。返回值：隔离会话。异常：依赖不可用时抛出。"""
        ...


class FactorStudyHandler:
    """执行研究任务。入参：仓储、目录守卫和执行器。返回值：处理器实例。异常：依赖非法时抛出。"""

    task_type = "FACTOR_STUDY"

    def __init__(
        self,
        registry: FactorStudyRunRegistry,
        catalog: CatalogGuard,
        executor: FactorStudyExecutor,
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
        """执行研究。入参：任务、进度和取消令牌。返回值：任务结果。异常：阶段失败时收敛状态后抛出。"""
        if task.subject_kind != "FACTOR_STUDY" or task.subject_id is None:
            raise ValueError("FACTOR_STUDY task must bind one study")
        study = self._registry.get(task.subject_id)
        self._registry.transition(
            study.id,
            FactorStudyStatus.QUEUED,
            FactorStudyStatus.RUNNING,
            stage=FactorStudyStage.VALIDATE,
        )
        session = self._executor.create(study)
        result: dict[str, JsonValue] = {}
        try:
            for index, stage in enumerate(FACTOR_STUDY_STAGES):
                self._catalog.assert_unchanged(study.catalog_hash)
                if cancellation.is_cancelled():
                    session.abort()
                    self._registry.transition(
                        study.id,
                        FactorStudyStatus.RUNNING,
                        FactorStudyStatus.CANCELLED,
                        stage=stage,
                    )
                    return TaskOutcome(status=TaskStatus.CANCELLED)
                self._registry.update_stage(study.id, stage)
                progress.update(
                    TaskProgress(
                        stage=stage.value,
                        completed=index,
                        total=len(FACTOR_STUDY_STAGES),
                        message=f"{stage.value.lower()} started",
                    )
                )
                stage_result = session.execute(stage, progress, cancellation)
                if stage_result:
                    result = stage_result
                self._catalog.assert_unchanged(study.catalog_hash)
            artifact_dir = result.get("artifact_dir")
            manifest_hash = result.get("manifest_hash")
            if not isinstance(artifact_dir, str) or not isinstance(
                manifest_hash, str
            ):
                raise TypeError("PUBLISH did not return verified artifact evidence")
            self._registry.transition(
                study.id,
                FactorStudyStatus.RUNNING,
                FactorStudyStatus.SUCCEEDED,
                stage=FactorStudyStage.PUBLISH,
                artifact_dir=artifact_dir,
                manifest_hash=manifest_hash,
            )
            return TaskOutcome(
                status=TaskStatus.SUCCEEDED,
                result={"factor_study_id": study.id, **result},
            )
        except Exception as error:
            session.abort()
            current = self._registry.get(study.id)
            if cancellation.is_cancelled():
                self._registry.transition(
                    study.id,
                    FactorStudyStatus.RUNNING,
                    FactorStudyStatus.CANCELLED,
                    stage=current.stage,
                )
                return TaskOutcome(status=TaskStatus.CANCELLED)
            self._registry.transition(
                study.id,
                FactorStudyStatus.RUNNING,
                FactorStudyStatus.FAILED,
                stage=current.stage,
                error={
                    "code": "FACTOR_STUDY_STAGE_FAILED",
                    "error_type": type(error).__name__,
                },
            )
            raise


__all__ = ["FactorStudyExecutor", "FactorStudyHandler"]
