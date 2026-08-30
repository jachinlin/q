"""提供python-module-conventions与命令行相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import json
import os
import signal
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Never, Protocol, cast

import typer
from typer import _click

from quant_research.application.experiments import ExperimentService
from quant_research.application.factor_studies import FactorStudyService
from quant_research.application.worker import WorkerRunResult
from quant_research.data.pipeline.dataset import DatasetCurateResult, LocalizeResult
from quant_research.data.pipeline.publish import DataPipeline, PipelineResult
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
    """定义策略命令端口。入参：实现实例。返回值：命令端口。异常：实现不满足协议时类型检查失败。"""

    def validate(self, config: str) -> object:
        """校验策略配置。入参：受信配置路径。返回值：规范结果。异常：配置非法时抛出。"""
        ...

    def submit(self, config: str) -> object:
        """提交策略实验。入参：受信配置路径。返回值：创建结果。异常：门禁或事务失败时抛出。"""
        ...

    def show(self, experiment_id: str) -> object:
        """读取策略实验。入参：实验 ID。返回值：实验聚合。异常：实验不存在时抛出。"""
        ...

    def run(self, experiment_id: str, config: str) -> object:
        """创建策略运行。入参：实验 ID 和配置路径。返回值：运行结果。异常：配置或实验非法时抛出。"""
        ...

    def rerun(self, run_id: str) -> object:
        """重跑策略。入参：运行 ID。返回值：新运行结果。异常：运行不可重跑时抛出。"""
        ...

    def list(self) -> object:
        """列出策略实验。入参：无。返回值：有序聚合。异常：仓储不可用时抛出。"""
        ...


class FactorStudyCommands(Protocol):
    """定义研究命令端口。入参：实现实例。返回值：命令端口。异常：实现不满足协议时类型检查失败。"""

    def validate(self, config: str) -> object:
        """校验研究配置。入参：受信配置路径。返回值：规范结果。异常：配置非法时抛出。"""
        ...

    def submit(self, config: str) -> object:
        """提交研究。入参：受信配置路径。返回值：研究快照。异常：门禁或事务失败时抛出。"""
        ...

    def show(self, study_id: str) -> object:
        """读取研究。入参：研究 ID。返回值：研究快照。异常：研究不存在时抛出。"""
        ...

    def list(self) -> object:
        """列出研究。入参：无。返回值：有序快照。异常：仓储不可用时抛出。"""
        ...


class StrategyCommands(Protocol):
    """定义策略目录 CLI 的只读契约。

    入参：
        目录查询不接收用户配置。
    返回值：
        实现方返回稳定排序的策略描述。
    异常：
        策略注册存在重复标识时由组合根在构建阶段抛出。
    """

    def list(self) -> object:
        """列出稳定排序的已注册策略。

        入参：
            无。
        返回值：
            返回可 JSON 序列化的策略目录。
        异常：
            目录不可用时由实现方抛出受控异常。
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
        *,
        actor: str = "system",
        request_id: str | None = None,
    ) -> TaskStatus:
        """定义 request_cancel 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...

    def retry(
        self,
        task_id: str,
        *,
        actor: str = "system",
        request_id: str | None = None,
    ) -> str:
        """定义非实验任务 retry 端口操作。

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

    def __init__(
        self,
        queue: _TaskQueuePort,
        factor_studies: FactorStudyService | None = None,
    ) -> None:
        if queue is None:
            raise TypeError("queue must be supplied")
        self._queue = queue
        self._factor_studies = factor_studies

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
        self._queue.request_cancel(task_id, actor="cli")
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
        if original.subject_kind == "FACTOR_STUDY":
            if self._factor_studies is None:
                raise ValueError("factor study retry service is not configured")
            if original.subject_id is None:
                raise ValueError("factor study task has no subject identity")
            self._factor_studies.retry(original.subject_id, actor="cli")
            new_task_id = original.id
        else:
            new_task_id = self._queue.retry(original.id, actor="cli")
        return {
            "task_id": original.id,
            "new_task_id": new_task_id,
        }


class LocalExperimentCommands:
    """从受信配置目录调用统一实验应用服务。

    入参：
        service：实验应用服务；config_root：允许读取 YAML 的配置根目录。
    返回值：
        创建不接受任意文件路径的本地实验命令适配器。
    异常：
        构造不读取文件；命令执行时的路径和领域错误由各方法说明。
    """

    def __init__(self, service: ExperimentService, config_root: Path) -> None:
        self._service = service
        self._config_root = config_root.resolve()

    def validate(self, config: str) -> Mapping[str, object]:
        """校验实验定义并返回规范化配置。

        入参：
            config：受信配置根内的 YAML 文件名。
        返回值：
            返回规范化定义和确定性配置哈希。
        异常：
            ValueError：路径越界、文件不存在或严格 Schema 校验失败时抛出。
        """
        resolved = self._service.validate_experiment(self._read(config))
        return {
            "definition": resolved.definition.model_dump(mode="json"),
            "config_hash": resolved.config_hash,
        }

    def submit(self, config: str) -> Mapping[str, object]:
        """创建实验及首个已入队 Run。

        入参：
            config：受信实验 YAML 文件名。
        返回值：
            返回新实验、首个 Run 和标签。
        异常：
            ValueError：配置非法时抛出；持久化失败时事务回滚并传播异常。
        """
        return self._aggregate(self._service.submit(self._read(config), actor="cli"))

    def show(self, experiment_id: str) -> Mapping[str, object]:
        """读取实验定义及其全部 Run。

        入参：
            experiment_id：实验标识。
        返回值：
            返回实验、全部 Run 和标签的 JSON 映射。
        异常：
            KeyError：实验不存在时抛出。
        """
        return self._aggregate(self._service.show(experiment_id))

    def run(self, experiment_id: str, config: str) -> Mapping[str, object]:
        """在指定实验下创建一个显式 Run。

        入参：
            experiment_id：实验标识；config：受信 Run YAML 文件名。
        返回值：
            返回加入新 Run 后的实验聚合。
        异常：
            KeyError：实验不存在时抛出；ValueError：Run 配置违反协议时抛出。
        """
        return self._aggregate(
            self._service.add_run(experiment_id, self._read(config), actor="cli")
        )

    def rerun(self, run_id: str) -> Mapping[str, object]:
        """从指定 Run 的冻结配置创建新 Run。

        入参：
            run_id：源 Run 标识。
        返回值：
            返回包含新 Run 的实验聚合。
        异常：
            KeyError：源 Run 不存在时抛出。
        """
        return self._aggregate(self._service.rerun(run_id, actor="cli"))

    def list(self) -> Mapping[str, object]:
        """列出最近创建的实验。

        入参：
            无。
        返回值：
            返回最近实验的 JSON 列表。
        异常：
            实验存储读取失败时传播持久化异常。
        """
        return {
            "experiments": [
                item.model_dump(mode="json") for item in self._service.list()
            ]
        }

    def _read(self, value: str) -> str:
        candidate = Path(value).resolve()
        if not candidate.is_relative_to(self._config_root) or not candidate.is_file():
            raise ValueError("experiment config must be a file inside configs")
        return candidate.read_text(encoding="utf-8")

    @staticmethod
    def _aggregate(value: Any) -> Mapping[str, object]:
        experiment = value.experiment
        runs = value.runs
        tags = value.tags
        return {
            "experiment": experiment.model_dump(mode="json"),
            "runs": [item.model_dump(mode="json") for item in runs],
            "tags": list(tags),
        }


class LocalFactorStudyCommands:
    """实现研究命令。入参：研究服务和配置根。返回值：本机命令实例。异常：依赖或路径非法时抛出。"""

    def __init__(self, service: FactorStudyService, config_root: Path) -> None:
        self._service = service
        self._config_root = config_root.resolve()

    def validate(self, config: str) -> Mapping[str, object]:
        """校验研究配置。入参：配置路径。返回值：规范定义和哈希。异常：路径或配置非法时抛出。"""
        resolved = self._service.validate(self._read(config))
        return {
            "definition": resolved.definition.model_dump(mode="json"),
            "config_hash": resolved.config_hash,
        }

    def submit(self, config: str) -> Mapping[str, object]:
        """提交研究。入参：配置路径。返回值：已排队快照。异常：路径、门禁或事务非法时抛出。"""
        return cast(
            Mapping[str, object],
            self._service.submit(self._read(config), actor="cli").model_dump(
                mode="json"
            ),
        )

    def show(self, study_id: str) -> Mapping[str, object]:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：研究不存在时抛出。"""
        return cast(
            Mapping[str, object],
            self._service.show(study_id).model_dump(mode="json"),
        )

    def list(self) -> Mapping[str, object]:
        """列出研究。入参：无。返回值：有序快照。异常：仓储不可用时抛出。"""
        return {
            "factor_studies": [
                item.model_dump(mode="json") for item in self._service.list()
            ]
        }

    def _read(self, value: str) -> str:
        candidate = Path(value).resolve()
        if not candidate.is_relative_to(self._config_root) or not candidate.is_file():
            raise ValueError("factor study config must be a file inside configs")
        return candidate.read_text(encoding="utf-8")


class LocalStrategyCommands:
    """列出组合根登记的策略标识。

    入参：
        strategy_ids：已完成唯一性校验的策略标识元组。
    返回值：
        创建只读策略目录命令适配器。
    异常：
        构造过程不访问外部依赖。
    """

    def __init__(self, strategy_ids: tuple[str, ...]) -> None:
        self._strategy_ids = strategy_ids

    def list(self) -> Mapping[str, object]:
        """返回稳定排序的策略目录。

        入参：
            无。
        返回值：
            返回带 strategies 数组的 JSON 映射。
        异常：
            该只读操作不主动抛出异常。
        """
        return {"strategies": list(self._strategy_ids)}


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
    factor_study_commands: FactorStudyCommands | None = None
    strategy_commands: StrategyCommands | None = None
    worker_commands: WorkerCommands | None = None
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
    experiments = typer.Typer(no_args_is_help=True)
    factor_studies = typer.Typer(no_args_is_help=True)
    strategies = typer.Typer(no_args_is_help=True)
    worker = typer.Typer(no_args_is_help=True)
    application.add_typer(data, name="data")
    application.add_typer(tasks, name="tasks")
    application.add_typer(experiments, name="experiments")
    application.add_typer(factor_studies, name="factor-studies")
    application.add_typer(strategies, name="strategies")
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
    from quant_research.cli.experiments import _ExperimentCommands
    from quant_research.cli.factor_studies import _FactorStudyCommands
    from quant_research.cli.runtime import _RuntimeCommands
    from quant_research.cli.strategies import _StrategyCommands
    from quant_research.cli.tasks import _TaskCommands
    from quant_research.cli.worker import _WorkerCommands

    _RuntimeCommands.register(application)
    _DataCommands.register(data, services_factory)
    _TaskCommands.register(tasks, services_factory)
    _ExperimentCommands.register(experiments, services_factory)
    _FactorStudyCommands.register(factor_studies, services_factory)
    _StrategyCommands.register(strategies, services_factory)
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
    def _strategy_commands(services: ApplicationServices) -> StrategyCommands:
        commands = services.strategy_commands
        if commands is None:
            _CliSupport._raise_service_unavailable("strategies")
        return commands

    @staticmethod
    def _factor_study_commands(
        services: ApplicationServices,
    ) -> FactorStudyCommands:
        commands = services.factor_study_commands
        if commands is None:
            _CliSupport._raise_service_unavailable("factor-studies")
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
        result = command.main(prog_name="qlab", standalone_mode=False)
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
