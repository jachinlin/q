"""Stable handler-facing protocols for durable background tasks."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from quant_core.tasks.models import ClaimedTask, TaskOutcome, TaskProgress

STANDARD_TASK_TYPES = frozenset(
    {
        "DATA_UPDATE",
        "FACTOR_COMPUTE",
        "BACKTEST",
        "REPORT",
    }
)


class ProgressSink(Protocol):
    """Persist the latest bounded task progress."""

    def update(self, progress: TaskProgress) -> None: ...


class CancellationToken(Protocol):
    """Expose cooperative cancellation at handler-defined batch boundaries."""

    def is_cancelled(self) -> bool: ...


class TaskHandler(Protocol):
    """Run one claimed task without owning queue transactions."""

    task_type: str

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome: ...


class HandlerRegistry:
    """Register one unambiguous handler for each dispatched task type."""

    def __init__(self, handlers: Iterable[TaskHandler] = ()) -> None:
        self._handlers: dict[str, TaskHandler] = {}
        for handler in handlers:
            self.register(handler)

    def register(self, handler: TaskHandler) -> None:
        task_type = handler.task_type
        if not isinstance(task_type, str):
            raise TypeError("handler task_type must be a string")
        if not task_type.strip():
            raise ValueError("handler task_type must not be empty")
        if task_type in self._handlers:
            raise ValueError(f"handler already registered for {task_type}")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler | None:
        return self._handlers.get(task_type)
