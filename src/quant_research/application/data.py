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
        self._stage_total = 0
        self._dataset_index = 0
        self.results: dict[str, dict[str, JsonValue]] = {}

    def stage_started(self, stage: str, total: int) -> None:
        self._stage_total = total
        self._dataset_index = 0
        self._progress.update(
            TaskProgress(
                stage=stage,
                completed=0,
                total=total,
                message=f"开始 {stage}，共 {total} 个数据集",
                context={"dataset_total": total},
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
        self._dataset_index = completed
        self.results.setdefault(name, {})[stage.lower()] = dict(details)
        self._progress.update(
            TaskProgress(
                stage=stage,
                completed=completed,
                total=total,
                message=f"{stage} 已完成 {name}（{completed}/{total}）",
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
        values = dict(details)
        if kind == "dataset":
            raw_index = values.get("dataset_index", self._dataset_index + 1)
            raw_total = values.get("dataset_total", self._stage_total)
            completed = int(raw_index) - 1 if isinstance(raw_index, int) else 0
            total = int(raw_total) if isinstance(raw_total, int) else self._stage_total
            self._dataset_index = completed + 1
            message = f"{stage} 正在处理 {dataset.value}（{completed + 1}/{total}）"
        else:
            completed, total = self._item_counts(kind, values)
            message = self._activity_message(stage, dataset, kind, values)
        self._progress.update(
            TaskProgress(
                stage=stage,
                completed=completed,
                total=total,
                message=message,
                context={
                    "dataset": dataset.value,
                    "boundary": kind,
                    "dataset_index": self._dataset_index,
                    "dataset_total": self._stage_total,
                    **values,
                },
            )
        )

    @staticmethod
    def _item_counts(kind: str, details: Mapping[str, JsonValue]) -> tuple[int, int]:
        """从不同活动详情中提取当前完成量和总量。"""
        keys = {
            "raw_request": ("completed_requests", "request_total"),
            "request_plan": ("completed_requests", "request_total"),
            "raw_input": ("raw_index", "raw_total"),
            "canonical_partition": ("partition_index", "partition_total"),
            "canonical_dataset": ("dataset_index", "dataset_total"),
        }
        completed_key, total_key = keys.get(kind, ("completed", "total"))
        raw_completed = details.get(completed_key, 0)
        raw_total = details.get(total_key, 0)
        completed = raw_completed if isinstance(raw_completed, int) else 0
        total = raw_total if isinstance(raw_total, int) else 0
        if details.get("status") == "STARTED" and kind in {
            "raw_input",
            "canonical_partition",
            "canonical_dataset",
        }:
            completed = max(0, completed - 1)
        return min(completed, total), total

    @staticmethod
    def _activity_message(
        stage: str,
        dataset: DatasetKind,
        kind: str,
        details: Mapping[str, JsonValue],
    ) -> str:
        """把结构化进度压缩为适合 Dashboard 展示的短消息。"""
        status = details.get("status")
        if kind == "raw_request":
            endpoint = details.get("endpoint", "unknown")
            request = details.get("request")
            scope = _DataUpdateObserver._request_scope(request)
            verb = "正在下载" if status == "STARTED" else "已完成下载"
            return f"{verb} {dataset.value} / {endpoint}{scope}"
        if kind == "request_plan":
            return (
                f"{dataset.value} 请求计划就绪："
                f"待下载 {details.get('pending_requests', 0)} / "
                f"共 {details.get('request_total', 0)}"
            )
        if kind == "raw_input":
            verb = "正在清洗" if status == "STARTED" else "已清洗"
            return f"{verb} {dataset.value} Raw {details.get('raw_index', 0)}/{details.get('raw_total', 0)}"
        if kind == "canonical_partition":
            verb = "正在构建" if status == "STARTED" else "已构建"
            return f"{verb} {dataset.value} / {details.get('partition_key', 'unknown')}"
        if kind == "canonical_dataset":
            return f"{stage} 已载入 {dataset.value}，准备执行质量规则"
        return f"{stage} 正在处理 {dataset.value} / {kind}"

    @staticmethod
    def _request_scope(request: JsonValue | None) -> str:
        """提取交易日、市场或行业等可读请求切片，不拼接敏感配置。"""
        if not isinstance(request, dict):
            return ""
        keys = (
            "trade_date",
            "period",
            "start_date",
            "end_date",
            "market",
            "list_status",
            "exchange",
            "l1_code",
            "is_new",
            "ts_code",
        )
        parts = [f"{key}={request[key]}" for key in keys if key in request]
        return " · " + ", ".join(parts) if parts else ""

    def is_cancelled(self) -> bool:
        return self._cancellation.is_cancelled()


class DataBootstrapHandler:
    """执行首次 Canonical 基线初始化后台任务。

    入参：
        pipeline：提供可恢复 ``bootstrap`` 流水线的数据服务。
    返回值：
        构造仅处理 ``DATA_BOOTSTRAP`` 严格载荷的 Worker 处理器。
    异常：
        载荷不是唯一正整数 ``years`` 时抛出 ``TypeError`` 或 ``ValueError``；
        初始化状态冲突和流水线失败保持原错误语义。
    """

    task_type = "DATA_BOOTSTRAP"

    def __init__(self, pipeline: DataPipeline) -> None:
        self._pipeline = pipeline

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """按冻结年数完成首次采集、清洗和全目录校验。

        入参：
            task：载荷必须严格等于 ``{"years": 正整数}`` 的已认领任务。
            progress：接收三个流水线阶段和完成事件的进度端口。
            cancellation：在安全边界提供协作取消状态。
        返回值：
            完成时返回数据目录身份；取消时返回 ``CANCELLED``。
        异常：
            任务类型或载荷非法时抛出类型或值错误；数据流水线异常按原语义传播。
        """
        if task.task_type != self.task_type:
            raise ValueError("data bootstrap handler requires DATA_BOOTSTRAP task")
        if set(task.payload) != {"years"}:
            raise ValueError("data bootstrap payload must contain only years")
        years = task.payload["years"]
        if type(years) is not int:
            raise TypeError("data bootstrap years must be an integer")
        if years <= 0:
            raise ValueError("data bootstrap years must be positive")
        observer = _DataUpdateObserver(progress, cancellation)
        try:
            result = self._pipeline.bootstrap(years=years, observer=observer)
        except DataPipelineCancelled:
            return TaskOutcome(status=TaskStatus.CANCELLED)
        progress.update(
            TaskProgress(
                stage="COMPLETE",
                completed=1,
                total=1,
                message="data bootstrap completed",
                context={"data_hash": result.data_hash, "years": years},
            )
        )
        return TaskOutcome(
            status=TaskStatus.SUCCEEDED,
            result={
                "run_id": result.run_id,
                "quality_run_id": str(result.quality_run_id),
                "data_hash": result.data_hash,
                "years": years,
                "datasets": observer.results,
            },
        )


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
            observer = _DataUpdateObserver(progress, cancellation)
            quality_run_id = self._pipeline.validate(
                dataset,
                heartbeat=heartbeat,
                observer=observer,
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
