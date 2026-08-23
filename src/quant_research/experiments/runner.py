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


class StrategyRunExecutor(Protocol):
    """执行冻结策略配置并返回发布登记信息。

    入参：Run、进度端口和取消令牌。返回值：产物目录与 Manifest 身份。异常：回测或发布失败时保留原异常。
    """

    def execute(
        self,
        run: RunRecord,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """执行完整策略回测链。

        入参：冻结 Run、进度端口和取消令牌。返回值：JSON 安全发布结果。异常：输入、撮合或发布失败时抛出对应异常。
        """
        ...


class FactorRunExecutor(Protocol):
    """执行冻结因子研究配置并返回发布登记信息。

    入参：Run、进度端口和取消令牌。返回值：因子分析产物登记信息。异常：计算或发布失败时保留原异常。
    """

    def execute(
        self,
        run: RunRecord,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """执行因子计算和统计分析链。

        入参：冻结 Run、进度端口和取消令牌。返回值：JSON 安全发布结果。异常：数据、统计或发布失败时抛出对应异常。
        """
        ...


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
        try:
            for index, stage in enumerate(stages):
                self._catalog.assert_unchanged(run.catalog_hash)
                if cancellation.is_cancelled():
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
                if stage in {RunStage.STRATEGY_RUN, RunStage.ANALYZE_FACTORS}:
                    result = (
                        self._strategy
                        if stage is RunStage.STRATEGY_RUN
                        else self._factor
                    ).execute(run, progress, cancellation)
                self._catalog.assert_unchanged(run.catalog_hash)
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


__all__ = ["ExperimentRunHandler", "FactorRunExecutor", "StrategyRunExecutor"]
