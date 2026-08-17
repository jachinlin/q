"""定义任务队列与执行过程共享的领域异常。"""

from quant_research.domain.errors import QuantError


class TaskQueueError(QuantError):
    """表示可机器识别的任务队列失败。

    入参：由 ``QuantError`` 定义。返回值：构造异常对象。异常：无。
    """


class TaskQueueNotFound(TaskQueueError):
    """表示请求的任务或尝试标识不存在。

    入参：由父类定义。返回值：构造异常对象。异常：无。
    """


class TaskQueueConflict(TaskQueueError):
    """表示队列状态、所有权或幂等前置条件冲突。

    入参：由父类定义。返回值：构造异常对象。异常：无。
    """


class TaskQueueBusy(TaskQueueError):
    """表示 SQLite 未能在限定重试内取得队列写锁。

    入参：由父类定义。返回值：构造异常对象。异常：无。
    """


TaskNotFound = TaskQueueNotFound
TaskConflict = TaskQueueConflict

__all__ = [
    "TaskConflict",
    "TaskNotFound",
    "TaskQueueBusy",
    "TaskQueueConflict",
    "TaskQueueError",
    "TaskQueueNotFound",
]
