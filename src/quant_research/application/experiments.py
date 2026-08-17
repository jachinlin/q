"""提供 client 模块的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Protocol, cast

import polars as pl

from quant_research.backtest.engine import StrategyRef
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.experiments.config import (
    ExperimentCatalog,
    ResolvedExperimentConfig,
    resolve_experiment_yaml,
    resolve_experiment_yaml_text,
)
from quant_research.experiments.fingerprint import (
    ExperimentFingerprintInput,
    compute_fingerprint,
)
from quant_research.experiments.models import (
    ExperimentRecord,
    ExperimentSpec,
    ExperimentStatus,
)
from quant_research.experiments.query import (
    ExperimentDetail,
    ExperimentSummary,
)
from quant_research.experiments.verification import validate_registered_publication
from quant_research.tasks.models import TaskAttemptRecord, TaskRecord, TaskStatus

_TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.ORPHANED,
    }
)


class _ExperimentQuery(Protocol):
    def get(self, experiment_id: str) -> ExperimentDetail: ...

    def inspection_summary(
        self,
        experiment_id: str,
        *,
        metric_limit: int = 100,
        artifact_limit: int = 100,
    ) -> ExperimentSummary: ...


class _ExperimentRegistry(Protocol):
    def create(
        self,
        config: ExperimentSpec,
        fingerprint: str,
        *,
        actor: str = "system",
        request_id: str | None = None,
        now: datetime | None = None,
    ) -> str: ...


class _TaskQueue(Protocol):
    def create_experiment_and_submit(
        self,
        spec: ExperimentSpec,
        *,
        priority: int = 0,
        actor: str = "cli",
        request_id: str | None = None,
    ) -> tuple[str, str]: ...

    def submit_backtest(
        self,
        experiment_id: str,
        config_hash: str,
        *,
        priority: int = 0,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> str: ...

    def get(self, task_id: str) -> TaskRecord: ...

    def list_for_experiment(
        self, experiment_id: str, *, limit: int = 100
    ) -> tuple[TaskRecord, ...]: ...

    def list_attempts(
        self, task_id: str, *, limit: int = 100
    ) -> tuple[TaskAttemptRecord, ...]: ...


@dataclass(frozen=True, slots=True)
class ExperimentInspection:
    """表示应用用例流程中的实验``inspection``及其业务不变量。

    入参：
        summary：供控制面展示的有界实验摘要，不包含 ORM 实例。
        task：Worker 已认领并带所有权围栏的任务快照。
        attempts：参与本次处理的``attempts``；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    summary: ExperimentSummary
    task: TaskRecord | None
    attempts: tuple[TaskAttemptRecord, ...]


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    """记录一次应用用例操作的结果、业务指标和审计身份。

    入参：
        _query：查询条件。
        _experiment_id：用于持久化关联和日志追踪的实验标识。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    _query: _ExperimentQuery
    _experiment_id: str

    def metrics(self) -> dict[str, JsonValue]:
        """处理应用用例中的指标集合。

        入参：
            无。
        返回值：
            返回实验登记的标量指标映射。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        try:
            payload = json.loads(self._registered_bytes("metrics.json"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                "registered metrics.json must be valid UTF-8 JSON"
            ) from error
        if not isinstance(payload, dict):
            raise TypeError("registered metrics.json must be a JSON object")
        canonical_json_bytes(cast(JsonValue, payload))
        return cast(dict[str, JsonValue], payload)

    def nav(self) -> pl.DataFrame:
        """处理应用用例中的净值序列。

        入参：
            无。
        返回值：
            返回实验成功产物中的按交易日净值序列。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        try:
            return pl.read_parquet(BytesIO(self._registered_bytes("nav.parquet")))
        except (OSError, pl.exceptions.PolarsError) as error:
            raise ValueError("registered nav.parquet cannot be read") from error

    def _registered_bytes(self, name: str) -> bytes:
        detail = self._query.get(self._experiment_id)
        if detail.record.status is not ExperimentStatus.SUCCEEDED:
            raise ValueError("experiment result requires SUCCEEDED status")
        publication = validate_registered_publication(detail)
        registered = {artifact.name: artifact for artifact in detail.artifacts}
        artifact = registered.get(name)
        entry = publication.entries.get(name)
        if artifact is None or entry is None:
            raise ValueError(f"successful experiment has no registered {name}")
        expected = (publication.artifact_dir / entry.path).resolve()
        if Path(artifact.path).resolve() != expected:
            raise ValueError(f"registered {name} path changed after verification")
        return _ClientSupport._read_registered_file(
            expected, entry.size_bytes, entry.sha256
        )


class ExperimentClient:
    """创建、提交并检查持久化实验，但不在当前进程启动 Worker。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        query：查询条件。
        queue：持久化任务状态、认领和重试的任务队列。
        config_root：所有派生路径必须位于其中的配置可信根目录。
        catalog：数据目录。
        strategies：参与本次处理的策略集合；调用方不得依赖未声明的顺序。
        rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
        environment_factory：由组合根注入、用于隔离外部副作用的运行环境``factory``端口。
        clock：用于产生可复现 UTC 时间戳的可注入时钟。
        sleeper：由组合根注入、用于隔离外部副作用的``sleeper``端口。
        monotonic：由组合根注入、用于隔离外部副作用的``monotonic``端口。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    def __init__(
        self,
        *,
        registry: _ExperimentRegistry,
        query: _ExperimentQuery,
        queue: _TaskQueue,
        config_root: Path,
        catalog: ExperimentCatalog,
        strategies: Mapping[StrategyRef, object],
        rulebook: MarketRuleBook,
        environment_factory: Callable[[], Mapping[str, JsonValue]],
        clock: Callable[[], datetime],
        sleeper: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        if registry is None or query is None or queue is None:
            raise TypeError("registry, query, and queue must be supplied")
        if not isinstance(config_root, Path):
            raise TypeError("config_root must be a Path")
        if not isinstance(strategies, Mapping):
            raise TypeError("strategies must be a mapping")
        if not callable(environment_factory) or not callable(clock):
            raise TypeError("environment_factory and clock must be callable")
        self._registry = registry
        self._query = query
        self._queue = queue
        self._config_root = config_root
        self._catalog = catalog
        self._strategies = dict(strategies)
        self._rulebook = rulebook
        self._environment_factory = environment_factory
        self._clock = clock
        self._sleeper = sleeper or time.sleep
        self._monotonic = monotonic or time.monotonic

    def create_from_yaml(
        self,
        path: str | Path,
        *,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> ExperimentRecord:
        """创建``from``YAML 文本。

        入参：
            path：经可信根边界校验后使用的路径。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回创建``from``YAML 文本后的``from``YAML 文本（``ExperimentRecord``）。
        异常：
            路径越出可信根、文件缺失或完整性校验失败时传播对应文件异常。
        """
        spec = self.prepare_from_yaml(path)
        experiment_id = self._registry.create(
            spec,
            spec.fingerprint,
            actor=actor,
            request_id=request_id,
            now=spec.created_at,
        )
        return self._query.get(experiment_id).record

    def prepare_from_yaml(self, path: str | Path) -> ExperimentSpec:
        """将受信目录内的 YAML 文件解析为尚未持久化的实验规格。

        入参：
            path：经可信根边界校验后使用的路径。
        返回值：
            返回``from``YAML 文本（``ExperimentSpec``）。
        异常：
            路径越出可信根、文件缺失或完整性校验失败时传播对应文件异常。
        """
        config_path = _ClientSupport._config_path(path, self._config_root)
        resolved = resolve_experiment_yaml(
            config_path,
            config_root=self._config_root,
            catalog=self._catalog,
            strategies=self._strategies,
            rulebook=self._rulebook,
        )
        return self._prepare_resolved(resolved)

    def prepare_from_yaml_text(self, config_yaml: str) -> ExperimentSpec:
        """将内存中的安全 YAML 文本解析为尚未持久化的实验规格。

        入参：
            config_yaml：用户提交的实验 YAML 原文；仅从受信配置根或内存文本解析。
        返回值：
            返回``from``YAML 文本``text``（``ExperimentSpec``）。
        异常：
            无。
        """
        resolved = resolve_experiment_yaml_text(
            config_yaml,
            catalog=self._catalog,
            strategies=self._strategies,
            rulebook=self._rulebook,
        )
        return self._prepare_resolved(resolved)

    def _prepare_resolved(self, resolved: ResolvedExperimentConfig) -> ExperimentSpec:
        environment = _ClientSupport._environment(self._environment_factory())
        mapping = resolved.mapping
        strategy_id = cast(str, mapping["strategy_id"])
        rulebook_hash = cast(str, mapping["rulebook_hash"])
        source_hash = cast(str, environment["source_hash"])
        lockfile_hash = cast(str, environment["lockfile_hash"])
        fingerprint = compute_fingerprint(
            ExperimentFingerprintInput(
                strategy_id=strategy_id,
                resolved_config=mapping,
                data_hash=resolved.data_hash,
                source_hash=source_hash,
                lockfile_hash=lockfile_hash,
                rulebook_hash=rulebook_hash,
            )
        )
        config_hash = hashlib.sha256(canonical_json_bytes(mapping)).hexdigest()
        created_at = self._clock()
        spec = ExperimentSpec(
            strategy_id=strategy_id,
            config=mapping,
            config_hash=config_hash,
            data_hash=resolved.data_hash,
            source_tree_hash=cast(str | None, environment["source_tree_hash"]),
            git_commit_hash=cast(str | None, environment["git_commit"]),
            lockfile_hash=lockfile_hash,
            rulebook_hash=rulebook_hash,
            fingerprint=fingerprint,
            created_at=created_at,
        )
        return spec

    def create_and_submit_from_yaml(
        self,
        path: str | Path,
        *,
        priority: int = 0,
        actor: str = "cli",
        request_id: str | None = None,
    ) -> tuple[ExperimentRecord, TaskRecord]:
        """解析受信 YAML 文件，并原子创建实验及其后台任务。

        入参：
            path：经可信根边界校验后使用的路径。
            priority：任务在同一可运行集合中的调度优先级。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回创建并``submit``来源YAML 文本后的并``submit``来源YAML 文本（``tuple[ExperimentRecord, TaskRecord]``）。
        异常：
            路径越出可信根、文件缺失或完整性校验失败时传播对应文件异常。
        """
        spec = self.prepare_from_yaml(path)
        experiment_id, task_id = self._queue.create_experiment_and_submit(
            spec,
            priority=priority,
            actor=actor,
            request_id=request_id,
        )
        return self._query.get(experiment_id).record, self._queue.get(task_id)

    def create_and_submit_from_yaml_text(
        self,
        config_yaml: str,
        *,
        priority: int = 0,
        actor: str = "dashboard",
        request_id: str | None = None,
    ) -> tuple[ExperimentRecord, TaskRecord]:
        """解析 YAML 文本，并原子创建实验及其后台任务。

        入参：
            config_yaml：用户提交的实验 YAML 原文；仅从受信配置根或内存文本解析。
            priority：任务在同一可运行集合中的调度优先级。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回创建并``submit``来源YAML 文本文本后的并``submit``来源YAML 文本文本（``tuple[ExperimentRecord, TaskRecord]``）。
        异常：
            无。
        """
        spec = self.prepare_from_yaml_text(config_yaml)
        experiment_id, task_id = self._queue.create_experiment_and_submit(
            spec,
            priority=priority,
            actor=actor,
            request_id=request_id,
        )
        return self._query.get(experiment_id).record, self._queue.get(task_id)

    def submit(
        self,
        experiment_id: str,
        *,
        priority: int = 0,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> TaskRecord:
        """提交应用用例。

        入参：
            experiment_id：持久化实验的 UUID 标识。
            priority：任务在同一可运行集合中的调度优先级。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回``submit``（``TaskRecord``）。
        异常：
            无。
        """
        experiment = self._query.get(experiment_id).record
        task_id = self._queue.submit_backtest(
            experiment.id,
            experiment.config_hash,
            priority=priority,
            actor=actor,
            request_id=request_id,
        )
        return self._queue.get(task_id)

    def wait(
        self,
        task_id: str,
        *,
        poll_seconds: float = 2.0,
        timeout_seconds: float | None = None,
    ) -> TaskRecord:
        """轮询等待应用用例。

        入参：
            task_id：持久化任务的 UUID 标识。
            poll_seconds：轮询``seconds``。
            timeout_seconds：超时时间``seconds``。
        返回值：
            返回``wait``（``TaskRecord``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TimeoutError``。
        """
        poll = _ClientSupport._positive_seconds(poll_seconds, "poll_seconds")
        timeout = (
            _ClientSupport._positive_seconds(timeout_seconds, "timeout_seconds")
            if timeout_seconds is not None
            else None
        )
        deadline = self._monotonic() + timeout if timeout is not None else None
        while True:
            task = self._queue.get(task_id)
            if task.status in _TERMINAL_TASK_STATUSES:
                return task
            if deadline is None:
                self._sleeper(poll)
                continue
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError(f"task did not finish before timeout: {task_id}")
            self._sleeper(min(poll, remaining))

    def result(self, experiment_id: str) -> ExperimentResult:
        """处理应用用例中的结果。

        入参：
            experiment_id：持久化实验的 UUID 标识。
        返回值：
            返回结果（``ExperimentResult``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        detail = self._query.get(experiment_id)
        if detail.record.status is not ExperimentStatus.SUCCEEDED:
            raise ValueError("experiment result requires SUCCEEDED status")
        validate_registered_publication(detail)
        return ExperimentResult(self._query, experiment_id)

    def inspect(self, experiment_id: str) -> ExperimentInspection:
        """处理应用用例中的``inspect``。

        入参：
            experiment_id：持久化实验的 UUID 标识。
        返回值：
            返回``inspect``（``ExperimentInspection``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        summary = self._query.inspection_summary(
            experiment_id,
            metric_limit=100,
            artifact_limit=100,
        )
        tasks = self._queue.list_for_experiment(experiment_id, limit=2)
        if len(tasks) > 1:
            raise ValueError("experiment has more than one canonical task")
        task = tasks[0] if tasks else None
        attempts = (
            self._queue.list_attempts(task.id, limit=100) if task is not None else ()
        )
        return ExperimentInspection(summary=summary, task=task, attempts=attempts)


class _ClientSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _positive_seconds(value: float, label: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{label} must be a finite positive number")
        return float(value)

    @staticmethod
    def _config_path(value: str | Path, config_root: Path) -> Path:
        if not isinstance(value, (str, Path)):
            raise TypeError("experiment config path must be text or a Path")
        candidate = Path(value)
        if candidate.is_absolute():
            return candidate
        if candidate.parts and candidate.parts[0] == config_root.name:
            return config_root.parent / candidate
        return config_root / candidate

    @staticmethod
    def _environment(value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        if not isinstance(value, Mapping):
            raise TypeError("environment_factory must return a mapping")
        encoded = canonical_json_bytes(value)
        plain = json.loads(encoded)
        expected = {
            "source_identity_mode",
            "source_hash",
            "git_commit",
            "source_tree_hash",
            "working_tree_dirty",
            "lockfile_path",
            "lockfile_hash",
            "python_version",
        }
        if not isinstance(plain, dict) or set(plain) != expected:
            raise ValueError("environment_factory returned invalid fields")
        return cast(dict[str, JsonValue], plain)

    @staticmethod
    def _read_registered_file(path: Path, size_bytes: int, sha256: str) -> bytes:
        """Read one regular file without following a swapped filesystem identity."""
        try:
            before = path.stat(follow_symlinks=False)
        except OSError as error:
            raise ValueError("registered artifact cannot be inspected") from error
        if not stat.S_ISREG(before.st_mode) or _ClientSupport._is_reparse(before):
            raise ValueError("registered artifact must be a plain regular file")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise ValueError("registered artifact cannot be opened safely") from error
        try:
            opened = os.fstat(descriptor)
            if _ClientSupport._file_identity(opened) != _ClientSupport._file_identity(
                before
            ):
                raise ValueError("registered artifact changed before open")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if _ClientSupport._file_identity(after) != _ClientSupport._file_identity(
            opened
        ):
            raise ValueError("registered artifact changed while being read")
        payload = b"".join(chunks)
        if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != sha256:
            raise ValueError("registered artifact size or hash changed")
        return payload

    @staticmethod
    def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
            int(getattr(value, "st_file_attributes", 0)),
        )

    @staticmethod
    def _is_reparse(value: os.stat_result) -> bool:
        return bool(int(getattr(value, "st_file_attributes", 0)) & 0x400)


__all__ = [
    "ExperimentClient",
    "ExperimentResult",
    "validate_registered_publication",
]
