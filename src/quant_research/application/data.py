"""提供数据更新与质量运行后台任务的应用用例。"""

from __future__ import annotations

from collections.abc import Mapping

from quant_research.data.contracts import JsonValue
from quant_research.data.pipeline.publish import (
    DataPipeline,
    DataPipelineCancelled,
    DataUpdatePlan,
    PipelineObserver,
)
from quant_research.domain.enums import DatasetKind
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)


class _DataUpdateObserver(PipelineObserver):
    """把数据流水线观察事件桥接为持久化任务进度。"""

    def __init__(self, progress: ProgressSink, cancellation: CancellationToken) -> None:
        self._progress = progress
        self._cancellation = cancellation
        self.results: dict[str, dict[str, JsonValue]] = {}

    def stage_started(self, stage: str, total: int) -> None:
        self._progress.update(
            TaskProgress(
                stage=stage,
                completed=0,
                total=total,
                message=f"{stage.lower()} started",
                context={},
            )
        )

    def dataset_completed(
        self,
        stage: str,
        dataset: DatasetKind,
        completed: int,
        total: int,
        details: Mapping[str, JsonValue],
    ) -> None:
        name = dataset.value
        self.results.setdefault(name, {})[stage.lower()] = dict(details)
        self._progress.update(
            TaskProgress(
                stage=stage,
                completed=completed,
                total=total,
                message=f"{stage.lower()} completed for {name}",
                context={"dataset": name, **dict(details)},
            )
        )

    def boundary(
        self,
        stage: str,
        dataset: DatasetKind,
        kind: str,
        details: Mapping[str, JsonValue],
    ) -> None:
        """在请求或分区安全边界发布细粒度进度。"""
        self._progress.update(
            TaskProgress(
                stage=stage,
                completed=0,
                total=0,
                message=f"{kind} completed for {dataset.value}",
                context={
                    "dataset": dataset.value,
                    "boundary": kind,
                    **dict(details),
                },
            )
        )

    def is_cancelled(self) -> bool:
        return self._cancellation.is_cancelled()


class DataUpdateHandler:
    """处理一个已认领的应用用例任务并持久化结果。

    入参：
        pipeline：按 LOCALIZE、CURATE、VALIDATE 顺序执行的数据流水线。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    task_type = "DATA_UPDATE"

    def __init__(self, pipeline: DataPipeline) -> None:
        self._pipeline = pipeline

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """执行应用用例。

        入参：
            task：Worker 已认领并带所有权围栏的任务快照。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行应用用例后的运行（``TaskOutcome``）。
        异常：
            无。
        """
        plan = DataUpdatePlan.from_payload(task.payload)
        observer = _DataUpdateObserver(progress, cancellation)
        try:
            result = self._pipeline.execute_update_plan(plan, observer=observer)
        except DataPipelineCancelled:
            return TaskOutcome(status=TaskStatus.CANCELLED)
        progress.update(
            TaskProgress(
                stage="COMPLETE",
                completed=1,
                total=1,
                message="data update completed",
                context={"data_hash": result.data_hash},
            )
        )
        return TaskOutcome(
            status=TaskStatus.SUCCEEDED,
            result={
                "run_id": result.run_id,
                "quality_run_id": str(result.quality_run_id),
                "data_hash": result.data_hash,
                "datasets": observer.results,
            },
        )


class DataValidationHandler:
    """执行独立的全目录或单数据集后台质量运行。

    入参：
        pipeline：提供 Canonical 质量校验和质量运行登记能力的数据流水线。
    返回值：
        构造可由 Worker 按 ``DATA_VALIDATION`` 分派的任务处理器。
    异常：
        构造阶段不抛出额外异常；任务载荷非法或质量运行失败时按原契约传播。
    """

    task_type = "DATA_VALIDATION"

    def __init__(self, pipeline: DataPipeline) -> None:
        self._pipeline = pipeline

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """执行任务载荷指定的质量运行并返回登记身份。

        入参：
            task：仅接受严格的 ``ALL`` 或单一 ``DATASET`` 载荷。
            progress：持久化质量运行开始和完成状态的进度端口。
            cancellation：在数据集与规则边界提供协作取消状态。
        返回值：
            取消时返回 ``CANCELLED``；完成时返回质量运行 ID 和范围。
        异常：
            ``TypeError``、``ValueError``：任务类型、范围或数据集载荷不满足契约时抛出。
            ``QuantError``：全目录质量运行发现阻断问题或数据发生漂移时传播。
        """
        dataset = self._dataset(task)
        scope = "ALL" if dataset is None else "DATASET"
        context: dict[str, JsonValue] = {"scope": scope}
        if dataset is not None:
            context["dataset"] = dataset.value
        progress.update(
            TaskProgress(
                stage="VALIDATE",
                completed=0,
                total=1,
                message="data validation started",
                context=context,
            )
        )

        def heartbeat() -> None:
            if cancellation.is_cancelled():
                raise DataPipelineCancelled("data validation cancellation requested")

        try:
            quality_run_id = self._pipeline.validate(
                dataset,
                heartbeat=heartbeat,
            )
        except DataPipelineCancelled:
            return TaskOutcome(status=TaskStatus.CANCELLED)
        result = {**context, "quality_run_id": str(quality_run_id)}
        progress.update(
            TaskProgress(
                stage="COMPLETE",
                completed=1,
                total=1,
                message="data validation completed",
                context=result,
            )
        )
        return TaskOutcome(status=TaskStatus.SUCCEEDED, result=result)

    @staticmethod
    def _dataset(task: ClaimedTask) -> DatasetKind | None:
        """严格解析质量任务范围并返回可选数据集。"""
        if task.task_type != DataValidationHandler.task_type:
            raise ValueError("data validation task type is invalid")
        payload = task.payload
        if payload == {"scope": "ALL"}:
            return None
        if set(payload) != {"scope", "dataset"} or payload.get("scope") != "DATASET":
            raise ValueError("DATA_VALIDATION payload fields are invalid")
        raw_dataset = payload.get("dataset")
        if not isinstance(raw_dataset, str):
            raise TypeError("DATA_VALIDATION dataset must be a dataset name")
        try:
            return DatasetKind(raw_dataset)
        except ValueError as error:
            raise ValueError("DATA_VALIDATION dataset is unsupported") from error
