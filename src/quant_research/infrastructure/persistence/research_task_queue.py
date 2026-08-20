"""为研究任务提供通用 subject_kind/subject_id 关联。"""

from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import Engine

from quant_research.data.contracts import JsonValue
from quant_research.infrastructure.persistence.task_queue import TaskQueue


class ResearchTaskQueue:
    """复用现有可靠队列，并原子补充通用研究对象关联。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, engine: Engine, queue: TaskQueue) -> None:
        if not isinstance(engine, Engine) or not isinstance(queue, TaskQueue):
            raise TypeError("research task queue requires Engine and TaskQueue")
        self._queue = queue

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        *,
        subject_kind: str,
        subject_id: str,
        priority: int = 0,
        idempotency_key: str,
        actor: str = "system",
        request_id: str | None = None,
    ) -> str:
        """幂等入队并把任务关联到研究族执行或运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not subject_kind or not subject_id:
            raise ValueError("research task subject must not be empty")
        return self._queue.enqueue(
            task_type,
            payload,
            priority,
            idempotency_key=idempotency_key,
            actor=actor,
            request_id=request_id,
            subject_kind=subject_kind,
            subject_id=subject_id,
        )
