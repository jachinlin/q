"""提供任务与handlers相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from quant_research.tasks.models import ClaimedTask, TaskOutcome, TaskProgress

STANDARD_TASK_TYPES = frozenset(
    {
        "DATA_UPDATE",
        "DATA_VALIDATION",
        "FACTOR_COMPUTE",
        "BACKTEST",
        "REPORT",
    }
)


class ProgressSink(Protocol):
    """定义 ``ProgressSink`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    Persist the latest bounded task progress.
    """

    def update(self, progress: TaskProgress) -> None:
        """更新处理状态或进度。

        入参：
            progress：当前尝试已完成量、总量和阶段说明。
        返回值：
            无。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class CancellationToken(Protocol):
    """定义 ``CancellationToken`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    Expose cooperative cancellation at handler-defined batch boundaries.
    """

    def is_cancelled(self) -> bool:
        """判断``cancelled``。

        入参：
            无。
        返回值：
            返回是否``cancelled``。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class TaskHandler(Protocol):
    """定义 ``TaskHandler`` 的依赖端口与实现契约。

    入参：
        task_type：任务类型。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    Run one claimed task without owning queue transactions.
    """

    task_type: str

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """执行完整处理流程。

        入参：
            task：Worker 已认领并带所有权围栏的任务快照。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行持久化任务后的运行（``TaskOutcome``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class HandlerRegistry:
    """登记并按稳定身份查询持久化任务定义。

    入参：
        handlers：按任务类型分派且不得重复登记的处理器集合。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Register one unambiguous handler for each dispatched task type.
    """

    def __init__(self, handlers: Iterable[TaskHandler] = ()) -> None:
        self._handlers: dict[str, TaskHandler] = {}
        for handler in handlers:
            self.register(handler)

    def register(self, handler: TaskHandler) -> None:
        """登记持久化任务。

        入参：
            handler：任务处理器。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        task_type = handler.task_type
        if not isinstance(task_type, str):
            raise TypeError("handler task_type must be a string")
        if not task_type.strip():
            raise ValueError("handler task_type must not be empty")
        if task_type in self._handlers:
            raise ValueError(f"handler already registered for {task_type}")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler | None:
        """读取并返回约定对象。

        入参：
            task_type：任务类型。
        返回值：
            返回读取持久化任务后的``get``（``TaskHandler | None``）。
        异常：
            无。
        """
        return self._handlers.get(task_type)
