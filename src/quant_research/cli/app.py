"""提供python-module-conventions与命令行相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import json
import os
import signal
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Never, Protocol

import typer
from typer import _click

from quant_research.application.experiments import (
    ExperimentClient,
    ExperimentInspection,
)
from quant_research.application.worker import WorkerRunResult
from quant_research.data.pipelines.dataset import DatasetCurateResult, LocalizeResult
from quant_research.data.pipelines.publish import DataPipeline, PipelineResult
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.logging import redact_context, sensitive_environment_values
from quant_research.tasks.models import TaskAttemptRecord, TaskRecord, TaskStatus


class TaskCommands(Protocol):
    """定义 ``TaskCommands`` 的依赖端口与实现契约。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def list(self, *, status: str | None, limit: int, offset: int) -> object:
        """列出符合条件的记录。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...

    def cancel(self, task_id: str) -> object:
        """请求取消目标任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...

    def retry(self, task_id: str) -> object:
        """重新提交可重试任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...


class ExperimentCommands(Protocol):
    """定义 ``ExperimentCommands`` 的依赖端口与实现契约。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def submit(self, config: str) -> object:
        """提交并登记约定任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...

    def show(self, experiment_id: str) -> object:
        """读取旧实验详情；仅供待移除实现内部完成切换。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...


class ResearchCommands(Protocol):
    """定义目标研究中心 CLI 的读写契约。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def validate(self, config: str) -> object:
        """定义 validate 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def submit(self, config: str) -> object:
        """定义 submit 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def list(self) -> object:
        """定义 list 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def show(self, family_id: str) -> object:
        """定义 show 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def rerun(self, family_id: str) -> object:
        """定义 rerun 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def components(self) -> object:
        """定义 components 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


class WorkerCommands(Protocol):
    """定义 ``WorkerCommands`` 的依赖端口与实现契约。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def once(self) -> object:
        """处理命令行中的``once``。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...

    def run(self) -> object:
        """执行完整处理流程。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ...


class _TaskQueuePort(Protocol):
    """约束 CLI 任务命令需要的队列操作。"""

    def list(
        self,
        *,
        status: TaskStatus | None = None,
        task_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[TaskRecord, ...]:
        """定义 list 端口操作。

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
        actor: str,
        *,
        request_id: str | None = None,
        strict: bool = False,
    ) -> None:
        """定义 request_cancel 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def clone_for_retry(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
    ) -> tuple[str | None, str]:
        """定义 clone_for_retry 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class _WorkerPort(Protocol):
    """约束 CLI 控制 Worker 所需的运行端口。"""

    @property
    def last_result(self) -> WorkerRunResult | None:
        """定义 last_result 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def run_once(self) -> bool:
        """定义 run_once 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def run_forever(self) -> None:
        """定义 run_forever 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def request_shutdown(self) -> None:
        """定义 request_shutdown 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class LocalTaskCommands:
    """表示命令行流程中的``local``任务``commands``及其业务不变量。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, queue: _TaskQueuePort) -> None:
        if queue is None:
            raise TypeError("queue must be supplied")
        self._queue = queue

    def list(
        self,
        *,
        status: str | None,
        limit: int,
        offset: int,
    ) -> Mapping[str, object]:
        """列出符合条件的记录。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        parsed_status: TaskStatus | None = None
        if status is not None:
            try:
                parsed_status = TaskStatus(status)
            except ValueError:
                _CliSupport._raise_argument_error(
                    "TASK_STATUS_INVALID",
                    "status must be a known task status",
                    {"status": status},
                )
        if type(limit) is not int or not 1 <= limit <= 500:
            _CliSupport._raise_argument_error(
                "TASK_LIST_LIMIT_INVALID",
                "limit must be an integer from 1 through 500",
                {"limit": limit},
            )
        if type(offset) is not int or offset < 0:
            _CliSupport._raise_argument_error(
                "TASK_LIST_OFFSET_INVALID",
                "offset must be a nonnegative integer",
                {"offset": offset},
            )
        return {
            "tasks": [
                _CliSupport._task_summary(record)
                for record in self._queue.list(
                    status=parsed_status,
                    limit=limit,
                    offset=offset,
                )
            ],
            "limit": limit,
            "offset": offset,
        }

    def cancel(self, task_id: str) -> Mapping[str, object]:
        """请求取消目标任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        self._queue.request_cancel(task_id, "cli", strict=True)
        record = self._queue.get(task_id)
        return {
            "task_id": record.id,
            "task_status": record.status.value,
        }

    def retry(self, task_id: str) -> Mapping[str, object]:
        """重新提交可重试任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        original = self._queue.get(task_id)
        new_experiment_id, new_task_id = self._queue.clone_for_retry(
            original.id,
            actor="cli",
        )
        return {
            "task_id": original.id,
            "experiment_id": original.experiment_id,
            "new_task_id": new_task_id,
            "new_experiment_id": new_experiment_id,
        }


class LocalExperimentCommands:
    """表示命令行流程中的``local``实验``commands``及其业务不变量。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, client: ExperimentClient) -> None:
        if not isinstance(client, ExperimentClient):
            raise TypeError("client must be an ExperimentClient")
        self._client = client

    def submit(self, config: str) -> Mapping[str, object]:
        """提交并登记约定任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        experiment, task = self._client.create_and_submit_from_yaml(
            config,
            actor="cli",
        )
        return {
            "experiment_id": experiment.id,
            "experiment_status": experiment.status.value,
            "task_id": task.id,
            "task_status": task.status.value,
        }

    def show(self, experiment_id: str) -> Mapping[str, object]:
        """读取并展示命令行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return _CliSupport._experiment_inspection(self._client.inspect(experiment_id))


class LocalWorkerCommands:
    """表示命令行流程中的``local``Worker``commands``及其业务不变量。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        worker: _WorkerPort,
        *,
        queue: _TaskQueuePort,
    ) -> None:
        if worker is None or queue is None:
            raise TypeError("worker and queue must be supplied")
        self._worker = worker
        self._queue = queue

    def once(self) -> Mapping[str, object]:
        """处理命令行中的``once``。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not self._worker.run_once():
            return {"worked": False}
        result = self._worker.last_result
        if result is None:
            raise RuntimeError("worker completed without a durable run result")
        task = self._queue.get(result.task_id)
        return {
            "worked": True,
            "task_id": task.id,
            "task_status": task.status.value,
            "subject_kind": task.subject_kind,
            "subject_id": task.subject_id,
        }

    def run(self) -> Mapping[str, object]:
        """执行完整处理流程。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        restorers: list[Callable[[], None]] = []

        def request_shutdown(_number: int, _frame: object) -> None:
            self._worker.request_shutdown()

        signal_numbers: list[int] = [signal.SIGINT, signal.SIGTERM]
        break_signal = getattr(signal, "SIGBREAK", None)
        if isinstance(break_signal, int):
            signal_numbers.append(break_signal)
        try:
            for number in signal_numbers:
                previous = signal.signal(number, request_shutdown)

                def restore(
                    signal_number: int = number,
                    previous_handler: signal._HANDLER = previous,
                ) -> None:
                    signal.signal(signal_number, previous_handler)

                restorers.append(restore)
        except (OSError, RuntimeError, ValueError) as error:
            for restore_handler in reversed(restorers):
                restore_handler()
            raise QuantError(
                ErrorDetail(
                    code="WORKER_SIGNAL_UNAVAILABLE",
                    severity=Severity.FATAL,
                    message="worker shutdown signal handlers are unavailable",
                    context={"error_type": type(error).__name__},
                    remediation="run the worker from the process main thread",
                    retryable=False,
                )
            ) from error
        try:
            self._worker.run_forever()
            return {"stopped": True}
        finally:
            for restore_handler in reversed(restorers):
                restore_handler()


@dataclass(slots=True)
class ApplicationServices:
    """集中持有 CLI 使用的数据、任务、实验和 Worker 服务及关闭回调。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    pipeline: DataPipeline
    task_commands: TaskCommands | None = None
    experiment_commands: ExperimentCommands | None = None
    worker_commands: WorkerCommands | None = None
    research_commands: ResearchCommands | None = None
    close_callback: Callable[[], None] | None = field(default=None, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def close(self) -> None:
        """关闭并释放持有的资源。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if self._closed:
            return
        self._closed = True
        if self.close_callback is not None:
            self.close_callback()


def create_app(
    services_factory: Callable[[], ApplicationServices],
) -> typer.Typer:
    """创建并返回约定对象；该函数作为稳定公开 API保留在模块级。

该函数作为模块级确定性辅助或框架入口保留。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    application = typer.Typer(no_args_is_help=True)
    data = typer.Typer(no_args_is_help=True)
    tasks = typer.Typer(no_args_is_help=True)
    research = typer.Typer(no_args_is_help=True)
    components = typer.Typer(no_args_is_help=True)
    worker = typer.Typer(no_args_is_help=True)
    application.add_typer(data, name="data")
    application.add_typer(tasks, name="tasks")
    application.add_typer(research, name="research")
    application.add_typer(components, name="components")
    application.add_typer(worker, name="worker")

    @application.command("dashboard")
    def run_dashboard(port: int = typer.Option(8000, min=1, max=65535)) -> None:
        """Run the local-only FastAPI dashboard."""
        import uvicorn

        uvicorn.run(
            "quant_research.bootstrap.dashboard:create_dashboard_app",
            factory=True,
            host="127.0.0.1",
            port=port,
            log_level="info",
        )

    from quant_research.cli.data import _DataCommands
    from quant_research.cli.research import _ResearchCommands
    from quant_research.cli.runtime import _RuntimeCommands
    from quant_research.cli.tasks import _TaskCommands
    from quant_research.cli.worker import _WorkerCommands

    _RuntimeCommands.register(application)
    _DataCommands.register(data, services_factory)
    _TaskCommands.register(tasks, services_factory)
    _ResearchCommands.register(research, components, services_factory)
    _WorkerCommands.register(worker, services_factory)
    return application


class _CliSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _task_commands(services: ApplicationServices) -> TaskCommands:
        commands = services.task_commands
        if commands is None:
            _CliSupport._raise_service_unavailable("tasks")
        return commands

    @staticmethod
    def _experiment_commands(services: ApplicationServices) -> ExperimentCommands:
        commands = services.experiment_commands
        if commands is None:
            _CliSupport._raise_service_unavailable("experiments")
        return commands

    @staticmethod
    def _research_commands(services: ApplicationServices) -> ResearchCommands:
        commands = services.research_commands
        if commands is None:
            _CliSupport._raise_service_unavailable("research")
        return commands

    @staticmethod
    def _worker_commands(services: ApplicationServices) -> WorkerCommands:
        commands = services.worker_commands
        if commands is None:
            _CliSupport._raise_service_unavailable("worker")
        return commands

    @staticmethod
    def _raise_service_unavailable(service: str) -> Never:
        raise QuantError(
            ErrorDetail(
                code="CLI_SERVICE_UNAVAILABLE",
                severity=Severity.FATAL,
                message="command service is unavailable",
                context={"service": service},
                remediation="use the production application composition root",
                retryable=False,
            )
        )

    @staticmethod
    def _raise_argument_error(
        code: str, message: str, context: Mapping[str, object]
    ) -> Never:
        raise QuantError(
            ErrorDetail(
                code=code,
                severity=Severity.SEVERE,
                message=message,
                context=context,
                remediation="correct the command arguments and retry",
                retryable=False,
            )
        )

    @staticmethod
    def _task_summary(record: TaskRecord) -> Mapping[str, object]:
        return {
            "task_id": record.id,
            "subject_kind": record.subject_kind,
            "subject_id": record.subject_id,
            "task_type": record.task_type,
            "status": record.status.value,
            "priority": record.priority,
            "progress": dict(record.progress),
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
            "heartbeat_at": (
                record.heartbeat_at.isoformat()
                if record.heartbeat_at is not None
                else None
            ),
            "completed_at": (
                record.completed_at.isoformat()
                if record.completed_at is not None
                else None
            ),
            "error": _CliSupport._error_summary(record.error),
        }

    @staticmethod
    def _attempt_summary(record: TaskAttemptRecord) -> Mapping[str, object]:
        return {
            "attempt_id": record.id,
            "attempt_no": record.attempt_no,
            "status": record.status.value,
            "worker_id": record.worker_id,
            "started_at": record.started_at.isoformat(),
            "heartbeat_at": (
                record.heartbeat_at.isoformat()
                if record.heartbeat_at is not None
                else None
            ),
            "completed_at": (
                record.completed_at.isoformat()
                if record.completed_at is not None
                else None
            ),
            "log_available": record.log_path is not None,
            "progress": dict(record.progress),
            "error": _CliSupport._error_summary(record.error),
        }

    @staticmethod
    def _error_summary(
        error: Mapping[str, object] | None,
    ) -> Mapping[str, object] | None:
        if error is None:
            return None
        return {
            "code": error.get("code"),
            "retryable": error.get("retryable"),
        }

    @staticmethod
    def _experiment_inspection(value: ExperimentInspection) -> Mapping[str, object]:
        summary = value.summary
        task = value.task
        return {
            "experiment": {
                "experiment_id": summary.id,
                "status": summary.status.value,
                "strategy_id": summary.strategy_id,
                "data_hash": summary.data_hash,
                "config_hash": summary.config_hash,
                "fingerprint": summary.fingerprint,
            },
            "task": _CliSupport._task_summary(task) if task is not None else None,
            "attempts": [
                _CliSupport._attempt_summary(attempt) for attempt in value.attempts
            ],
            "last_progress": dict(task.progress) if task is not None else None,
            "error": _CliSupport._error_summary(task.error)
            if task is not None
            else None,
            "result": {
                "metrics": [
                    {"name": metric.name, "value": metric.value, "unit": metric.unit}
                    for metric in summary.metrics
                ],
                "artifacts": [
                    {
                        "name": artifact.name,
                        "artifact_type": artifact.artifact_type,
                        "content_hash": artifact.content_hash,
                        "schema": artifact.metadata.get("schema"),
                        "row_count": artifact.metadata.get("row_count"),
                        "size_bytes": artifact.metadata.get("size_bytes"),
                    }
                    for artifact in summary.artifacts
                ],
            },
        }

    @staticmethod
    def _invoke(
        operation: Callable[[ApplicationServices], object],
        services_factory: Callable[[], ApplicationServices],
        *,
        add_status: bool = True,
    ) -> None:
        result = _CliSupport._call_with_services(
            operation,
            services_factory,
            unexpected=lambda error: QuantError(
                ErrorDetail(
                    code="DATA_PIPELINE_UNEXPECTED",
                    severity=Severity.FATAL,
                    message=str(error),
                    context={"error_type": type(error).__name__},
                    remediation="inspect local logs and pipeline checkpoints",
                    retryable=False,
                )
            ),
        )
        payload = _CliSupport._result_payload(result)
        if add_status and "status" not in payload:
            payload["status"] = "SUCCEEDED"
        typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    @staticmethod
    def _invoke_command(
        operation: Callable[[ApplicationServices], object],
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        result = _CliSupport._call_with_services(
            operation,
            services_factory,
            unexpected=lambda error: QuantError(
                ErrorDetail(
                    code="CLI_UNEXPECTED",
                    severity=Severity.FATAL,
                    message="command failed unexpectedly",
                    context={"error_type": type(error).__name__},
                    remediation=("inspect controlled logs using the request identity"),
                    retryable=False,
                )
            ),
        )
        payload = _CliSupport._result_payload(result)
        if "status" not in payload:
            payload["status"] = "SUCCEEDED"
        typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    @staticmethod
    def _call_with_services(
        operation: Callable[[ApplicationServices], object],
        services_factory: Callable[[], ApplicationServices],
        *,
        unexpected: Callable[[Exception], QuantError],
    ) -> object:
        services: ApplicationServices | None = None
        result: object = None
        failure: QuantError | None = None
        try:
            services = services_factory()
            result = operation(services)
        except QuantError as error:
            failure = error
        except Exception as error:  # noqa: BLE001 - stable CLI process boundary.
            failure = unexpected(error)
        finally:
            if services is not None:
                try:
                    services.close()
                except Exception as error:  # noqa: BLE001 - close is also structured.
                    if failure is None:
                        failure = _CliSupport._service_close_error(error)
        if failure is not None:
            _CliSupport._emit_error(failure)
        return result

    @staticmethod
    def _service_close_error(error: Exception) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="CLI_SERVICE_CLOSE_FAILED",
                severity=Severity.FATAL,
                message="command services failed to close",
                context={"error_type": type(error).__name__},
                remediation="inspect controlled logs before running another command",
                retryable=False,
            )
        )

    @staticmethod
    def _result_payload(result: object) -> dict[str, object]:
        if isinstance(result, Mapping):
            return dict(result)
        if isinstance(result, PipelineResult):
            return {
                "run_id": result.run_id,
                "quality_run_id": str(result.quality_run_id),
                "data_hash": result.data_hash,
            }
        raise TypeError("command returned an unsupported result")

    @staticmethod
    def _emit_error(error: QuantError) -> Never:
        _CliSupport._write_error(error)
        raise typer.Exit(code=2)

    @staticmethod
    def _write_error(error: QuantError) -> None:
        redacted = redact_context(
            {
                "message": error.detail.message,
                "context": error.detail.context,
                "remediation": error.detail.remediation,
            },
            sensitive_values=sensitive_environment_values(os.environ),
        )
        message = redacted.get("message")
        context = redacted.get("context")
        remediation = redacted.get("remediation")
        if (
            not isinstance(message, str)
            or not isinstance(context, dict)
            or not isinstance(remediation, str)
        ):
            message = "error details unavailable after safe redaction"
            context = {"redaction": "[REDACTION_FAILED]"}
            remediation = "inspect controlled logs using the request identity"
        payload = {
            "error": {
                "code": error.detail.code,
                "severity": error.detail.severity.value,
                "message": message,
                "context": context,
                "remediation": remediation,
                "retryable": error.detail.retryable,
            }
        }
        typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True), err=True)

    @staticmethod
    def _parse_cli_date(value: str, field: str) -> date:
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            _CliSupport._emit_error(
                QuantError(
                    ErrorDetail(
                        code="DATA_PIPELINE_ARGUMENT",
                        severity=Severity.SEVERE,
                        message=f"{field} must be YYYY-MM-DD",
                        context={"field": field},
                        remediation="provide an ISO calendar date",
                        retryable=False,
                    )
                )
            )
        return parsed

    @staticmethod
    def _dataset_arg(value: str) -> DatasetKind:
        try:
            dataset = DatasetKind(value)
        except ValueError:
            _CliSupport._raise_argument_error(
                "DATASET_UNSUPPORTED",
                "dataset is not in the catalog",
                {"dataset": value},
            )
        return dataset

    @staticmethod
    def _date_pair(
        start: str | None, end: str | None
    ) -> tuple[date | None, date | None]:
        if (start is None) != (end is None):
            _CliSupport._raise_argument_error(
                "DATA_PIPELINE_ARGUMENT",
                "--from and --to must be supplied together",
                {},
            )
        return (
            _CliSupport._parse_cli_date(start, "from") if start is not None else None,
            _CliSupport._parse_cli_date(end, "to") if end is not None else None,
        )

    @staticmethod
    def _localize_payload(result: LocalizeResult) -> dict[str, object]:
        return {
            "dataset": result.dataset.value,
            "fetched": result.fetched,
            "skipped": result.skipped,
            "raw_partitions": result.raw_partitions,
        }

    @staticmethod
    def _curate_payload(result: DatasetCurateResult) -> dict[str, object]:
        return {
            "dataset": result.dataset.value,
            "content_hash": result.content_hash,
            "partitions": result.partitions,
            "rows": result.rows,
            "rebuilt_partitions": result.rebuilt_partitions,
            "reused_partitions": result.reused_partitions,
            "raw_inputs_read": result.raw_inputs_read,
        }


def run(application: typer.Typer) -> int:
    """执行命令行；该函数作为稳定公开 API 或框架入口保留在模块级。

该函数作为模块级确定性辅助或框架入口保留。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    command = typer.main.get_command(application)
    try:
        result = command.main(prog_name="quant", standalone_mode=False)
    except _click.ClickException as error:
        _CliSupport._write_error(
            QuantError(
                ErrorDetail(
                    code="CLI_ARGUMENT_INVALID",
                    severity=Severity.SEVERE,
                    message="command arguments are invalid",
                    context={"error_type": type(error).__name__},
                    remediation="use --help and correct the command arguments",
                    retryable=False,
                )
            )
        )
        return 2
    if result is None:
        return 0
    return int(result)
