"""提供数据任务与通用后台任务的 Dashboard 写用例。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Protocol

from quant_research.application.experiments import ExperimentService
from quant_research.data.contracts import JsonValue
from quant_research.data.pipeline.publish import DataUpdatePlan
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.tasks.models import TaskRecord, TaskStatus


class OperationalTaskQueue(Protocol):
    """定义数据任务和运行中心所需的队列端口。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        priority: int,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        actor: str = "system",
        request_id: str | None = None,
        subject_kind: str | None = None,
        subject_id: str | None = None,
    ) -> str:
        """定义 enqueue 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...

    def get(self, task_id: str) -> TaskRecord:
        """定义 get 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...

    def request_cancel(
        self,
        task_id: str,
        actor: str = "system",
        *,
        request_id: str | None = None,
        strict: bool = False,
    ) -> TaskStatus:
        """定义 request_cancel 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...

    def delete(
        self,
        task_id: str,
        actor: str = "system",
        *,
        request_id: str | None = None,
    ) -> None:
        """定义 delete 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...

    def retry(
        self,
        task_id: str,
        actor: str = "system",
        *,
        available_at: datetime | None = None,
        request_id: str | None = None,
    ) -> str:
        """定义 retry 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


class DataUpdatePlanningPort(Protocol):
    """定义数据更新计划预览与冻结端口。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def plan(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
    ) -> DataUpdatePlan:
        """定义 plan 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


class OperationalCommandService:
    """执行数据更新、质量任务和通用任务控制。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        queue: OperationalTaskQueue,
        planner: DataUpdatePlanningPort,
        experiments: ExperimentService,
    ) -> None:
        self._queue = queue
        self._planner = planner
        self._experiments = experiments

    def preview_data_update(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
    ) -> dict[str, JsonValue]:
        """生成无写入的数据更新计划。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._planner.plan(start=start, end=end, datasets=datasets).to_payload()

    def enqueue_data_update(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
        expected_plan_hash: str,
        request_id: str,
    ) -> dict[str, JsonValue]:
        """复核计划身份后入队冻结的数据更新任务。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        plan = self._planner.plan(start=start, end=end, datasets=datasets)
        if plan.plan_hash != expected_plan_hash:
            raise QuantError(
                ErrorDetail(
                    code="DATA_UPDATE_PLAN_STALE",
                    severity=Severity.WARNING,
                    message="data update plan changed after preview",
                    context={"current_plan_hash": plan.plan_hash},
                    remediation="refresh the update plan preview and confirm it again",
                    retryable=True,
                )
            )
        if not plan.dataset_windows:
            raise QuantError(
                ErrorDetail(
                    code="DATA_UPDATE_NOT_REQUIRED",
                    severity=Severity.INFO,
                    message="selected datasets do not require an update",
                    context={
                        "skipped_datasets": [
                            item.dataset.value for item in plan.skipped_datasets
                        ]
                    },
                    remediation=(
                        "wait until the financial disclosure deadline has passed"
                    ),
                    retryable=False,
                )
            )
        task_id = self._queue.enqueue(
            "DATA_UPDATE",
            plan.to_payload(),
            0,
            idempotency_key="dashboard-data-update-" + plan.plan_hash[:24],
            actor="dashboard",
            request_id=request_id,
        )
        task = self._queue.get(task_id)
        return {
            "task_id": task.id,
            "request_id": request_id,
            "status": task.status.value,
            "plan_hash": plan.plan_hash,
        }

    def enqueue_data_validation(
        self,
        *,
        dataset: DatasetKind | None,
        request_id: str,
    ) -> dict[str, object]:
        """入队全目录门禁或单数据集诊断。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        scope = "ALL" if dataset is None else "DATASET"
        payload: dict[str, JsonValue] = {"scope": scope}
        key = "dashboard-data-validation-all"
        if dataset is not None:
            payload["dataset"] = dataset.value
            key = f"dashboard-data-validation-{dataset.value}"
        task_id = self._queue.enqueue(
            "DATA_VALIDATION",
            payload,
            0,
            idempotency_key=key,
            actor="dashboard",
            request_id=request_id,
        )
        task = self._queue.get(task_id)
        result: dict[str, object] = {
            "task_id": task.id,
            "request_id": request_id,
            "status": task.status.value,
            "scope": scope,
        }
        if dataset is not None:
            result["dataset"] = dataset.value
        return result

    def cancel_task(self, task_id: str, *, request_id: str) -> dict[str, object]:
        """严格请求取消目标任务。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        self._queue.request_cancel(task_id, actor="dashboard", request_id=request_id)
        task = self._queue.get(task_id)
        return {"task_id": task.id, "status": task.status.value}

    def retry_task(
        self,
        task_id: str,
        *,
        confirm_orphaned: bool,
        request_id: str,
    ) -> dict[str, JsonValue]:
        """数据任务创建新任务；研究任务创建全新 execution。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        task = self._queue.get(task_id)
        if task.status is TaskStatus.ORPHANED and not confirm_orphaned:
            raise ValueError("orphaned task retry requires explicit confirmation")
        if task.subject_kind == "EXPERIMENT_RUN" and task.subject_id is not None:
            aggregate = self._experiments.rerun(task.subject_id, actor="dashboard")
            newest = aggregate.runs[-1]
            return {
                "experiment_id": aggregate.experiment.id,
                "run_id": newest.id,
                "task_id": newest.task_id,
            }
        if task.task_type == "DATA_UPDATE":
            try:
                DataUpdatePlan.from_payload(task.payload)
            except (TypeError, ValueError) as error:
                raise QuantError(
                    ErrorDetail(
                        code="DATA_UPDATE_LEGACY_PLAN",
                        severity=Severity.WARNING,
                        message="data update task has no frozen dataset windows",
                        context={"task_id": task.id},
                        remediation="create a new update task from the data center",
                        retryable=False,
                    )
                ) from error
        if task.task_type not in {"DATA_UPDATE", "DATA_VALIDATION"}:
            raise ValueError("task type cannot be retried by the target platform")
        retried = self._queue.retry(task_id, actor="dashboard", request_id=request_id)
        return {"task_id": retried}

    def delete_task(self, task_id: str, *, request_id: str) -> dict[str, object]:
        """删除一个终态任务记录。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        self._queue.delete(task_id, actor="dashboard", request_id=request_id)
        return {"task_id": task_id, "status": "DELETED"}
