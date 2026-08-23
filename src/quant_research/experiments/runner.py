"""实现两种实验共享的唯一 EXPERIMENT_RUN Worker 处理器。"""

from __future__ import annotations

from typing import Protocol

from quant_research.data.contracts import JsonValue
from quant_research.experiments.models import (
    FACTOR_STAGES,
    STRATEGY_STAGES,
    ExperimentKind,
    RunRecord,
    RunStage,
    RunStatus,
)
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)


class CatalogGuard(Protocol):
    """校验运行期间 Canonical 目录身份未变化。

    入参：提交时捕获的目录哈希。返回值：无。异常：目录变化时抛出数据漂移异常。
    """

    def assert_unchanged(self, catalog_hash: str) -> None:
        """比较当前目录与 Run 捕获身份。

        入参：预期目录哈希。返回值：无。异常：哈希不一致时抛出数据漂移异常。
        """
        ...


class RunRegistry(Protocol):
    """定义 Worker 推进 Run 所需的最小持久化端口。

    入参：Run ID、状态、阶段和可选终态信息。返回值：Run 快照或无。异常：CAS 冲突和记录缺失由实现方报告。
    """

    def get_run(self, run_id: str) -> RunRecord:
        """读取 Run 快照。

        入参：Run ID。返回值：当前 Run 记录。异常：Run 不存在时抛出对应异常。
        """
        ...

    def update_stage(self, run_id: str, stage: RunStage) -> None:
        """更新运行阶段但不改变生命周期状态。

        入参：Run ID 和目标阶段。返回值：无。异常：Run 不在运行态时抛出状态冲突。
        """
        ...

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
        """以 CAS 提交 Run 状态迁移。

        入参：Run ID、预期和目标状态、阶段及可选终态字段。返回值：无。异常：状态不符或证据非法时抛出冲突。
        """
        ...

    def discard_outputs(self, run_id: str) -> None:
        """事务删除未能进入成功终态的指标和产物登记。

        入参：Run ID。返回值：删除完成后无返回。异常：数据库清理失败时传播。
        """
        ...


class RunExecutionSession(Protocol):
    """保存一个任务内各阶段共享的短生命周期计算状态。

    入参：阶段、进度端口和取消令牌。返回值：阶段结果。异常：阶段失败时保留原异常。
    """

    def execute(
        self,
        stage: RunStage,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """执行当前阶段并保留后续阶段所需的内存状态。

        入参：阶段、进度端口和取消令牌。返回值：JSON 安全阶段结果。
        异常：阶段次序、输入、计算或发布失败时抛出对应异常。
        """
        ...

    def abort(self) -> None:
        """撤销本会话已发布但未成功提交的输出。

        入参：无。返回值：清理完成后无返回。异常：文件或登记清理失败时传播。
        """
        ...


class RunExecutor(Protocol):
    """为冻结 Run 创建隔离的阶段执行会话。

    入参：Run。返回值：任务专属执行会话。异常：Run kind 不匹配时抛出类型错误。
    """

    def create(self, run: RunRecord) -> RunExecutionSession:
        """创建任务专属会话。

        入参：冻结 Run。返回值：隔离的阶段执行会话。异常：配置类型错误时抛出。
        """
        ...


StrategyRunExecutor = RunExecutor
FactorRunExecutor = RunExecutor


class ExperimentRunHandler:
    """根据 Experiment kind 执行固定阶段图和 Run CAS 状态机。

    入参：构造时注入 Run 仓储、目录守卫和两种执行器。返回值：``run`` 返回任务终态。异常：状态冲突、漂移或阶段执行失败时抛出对应异常。
    """

    task_type = "EXPERIMENT_RUN"

    def __init__(
        self,
        registry: RunRegistry,
        catalog: CatalogGuard,
        strategy: StrategyRunExecutor,
        factor: FactorRunExecutor,
    ) -> None:
        self._registry = registry
        self._catalog = catalog
        self._strategy = strategy
        self._factor = factor

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """执行一个 Run，并在取消、漂移和失败时提交对应 Run 终态。

        入参：已认领任务、持久化进度端口和取消令牌。返回值：与 Run 终态一致的任务结果。异常：目录漂移或阶段失败时先登记 FAILED 再重抛。
        """
        if task.subject_kind != "EXPERIMENT_RUN" or task.subject_id is None:
            raise ValueError("EXPERIMENT_RUN task must bind one Run")
        run = self._registry.get_run(task.subject_id)
        self._registry.transition(
            run.id, RunStatus.QUEUED, RunStatus.RUNNING, stage=RunStage.VALIDATE
        )
        stages = (
            STRATEGY_STAGES
            if run.config.kind is ExperimentKind.STRATEGY_BACKTEST
            else FACTOR_STAGES
        )
        result: dict[str, JsonValue] = {}
        session: RunExecutionSession | None = None
        try:
            session = (
                self._strategy.create(run)
                if run.config.kind is ExperimentKind.STRATEGY_BACKTEST
                else self._factor.create(run)
            )
            for index, stage in enumerate(stages):
                self._catalog.assert_unchanged(run.catalog_hash)
                if cancellation.is_cancelled():
                    session.abort()
                    self._registry.transition(
                        run.id, RunStatus.RUNNING, RunStatus.CANCELLED, stage=stage
                    )
                    return TaskOutcome(status=TaskStatus.CANCELLED)
                self._registry.update_stage(run.id, stage)
                progress.update(
                    TaskProgress(
                        stage=stage.value,
                        completed=index,
                        total=len(stages),
                        message=f"{stage.value.lower()} started",
                    )
                )
                stage_result = session.execute(stage, progress, cancellation)
                if stage_result:
                    result = stage_result
                if cancellation.is_cancelled():
                    session.abort()
                    self._registry.transition(
                        run.id, RunStatus.RUNNING, RunStatus.CANCELLED, stage=stage
                    )
                    return TaskOutcome(status=TaskStatus.CANCELLED)
                self._catalog.assert_unchanged(run.catalog_hash)
            if not isinstance(result.get("artifact_dir"), str) or not isinstance(
                result.get("manifest_hash"), str
            ):
                raise TypeError("PERSIST did not return verified artifact evidence")
            artifact_dir = str(result.get("artifact_dir"))
            manifest_hash = str(result.get("manifest_hash"))
            self._registry.transition(
                run.id,
                RunStatus.RUNNING,
                RunStatus.SUCCEEDED,
                stage=RunStage.PERSIST,
                artifact_dir=artifact_dir,
                manifest_hash=manifest_hash,
            )
            return TaskOutcome(
                status=TaskStatus.SUCCEEDED, result={"run_id": run.id, **result}
            )
        except Exception as error:
            if session is not None:
                session.abort()
            if cancellation.is_cancelled():
                self._registry.transition(
                    run.id,
                    RunStatus.RUNNING,
                    RunStatus.CANCELLED,
                    stage=self._registry.get_run(run.id).stage,
                )
                return TaskOutcome(status=TaskStatus.CANCELLED)
            self._registry.transition(
                run.id,
                RunStatus.RUNNING,
                RunStatus.FAILED,
                stage=self._registry.get_run(run.id).stage,
                error={
                    "code": "EXPERIMENT_STAGE_FAILED",
                    "error_type": type(error).__name__,
                },
            )
            raise


__all__ = [
    "ExperimentRunHandler",
    "FactorRunExecutor",
    "RunExecutionSession",
    "RunExecutor",
    "StrategyRunExecutor",
]
