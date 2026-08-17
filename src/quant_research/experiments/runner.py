"""提供实验与实验编排相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import html
import json
import math
import os
import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import UUID

import yaml

from quant_research.analytics.materialize import materialize_analytics
from quant_research.backtest.artifacts import (
    ExperimentArtifactPublication,
    publish_experiment_artifacts,
    validate_experiment_artifacts,
)
from quant_research.backtest.engine import BacktestCancelled, BacktestResult
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.experiments.adapters import FactorValueSource
from quant_research.experiments.models import ExperimentRecord, ExperimentStatus
from quant_research.experiments.query import ExperimentDetail
from quant_research.experiments.verification import validate_registered_publication
from quant_research.factors.base import (
    FactorArtifact,
    canonical_factor_ref,
    validate_sha256,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import LogContext, TaskLogManager
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)

_ENVIRONMENT_FIELDS = {
    "source_identity_mode",
    "source_hash",
    "git_commit",
    "source_tree_hash",
    "working_tree_dirty",
    "lockfile_path",
    "lockfile_hash",
    "python_version",
}


class ExperimentStage(StrEnum):
    """定义 ``ExperimentStage`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    VALIDATE = "VALIDATE"
    UNIVERSE = "UNIVERSE"
    FACTOR_COMPUTE = "FACTOR_COMPUTE"
    BACKTEST = "BACKTEST"
    ANALYTICS = "ANALYTICS"
    ARTIFACT_VERIFY = "ARTIFACT_VERIFY"
    REGISTER = "REGISTER"


EXPERIMENT_STAGES = tuple(ExperimentStage)


@dataclass(frozen=True, slots=True)
class ExperimentUniverseResult:
    """记录一次实验操作的结果、业务指标和审计身份。

    入参：
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        signal_dates：已纳入股票池哈希的实际调仓信号日。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Stable identity of the universe prepared for one experiment runtime.
    """

    universe_hash: str
    signal_dates: tuple[date, ...] = ()

    def __post_init__(self) -> None:
        validate_sha256(self.universe_hash, "universe_hash")
        if not isinstance(self.signal_dates, tuple) or any(
            not isinstance(value, date) for value in self.signal_dates
        ):
            raise TypeError("signal_dates must be a tuple of dates")
        if self.signal_dates != tuple(sorted(self.signal_dates)) or len(
            set(self.signal_dates)
        ) != len(self.signal_dates):
            raise ValueError("signal_dates must be strictly ascending and unique")


@dataclass(frozen=True, slots=True)
class ExperimentFactorResult:
    """记录一次实验操作的结果、业务指标和审计身份。

    入参：
        artifacts：参与本次处理的产物集合；调用方不得依赖未声明的顺序。
        factor_source：因子数据来源。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Verified factor results for one experiment run.
    """

    artifacts: Mapping[str, FactorArtifact]
    factor_source: FactorValueSource | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.artifacts, Mapping):
            raise TypeError("artifacts must be a mapping")
        artifacts = dict(self.artifacts)
        for reference, artifact in artifacts.items():
            if (
                canonical_factor_ref(reference) != reference
                or not isinstance(artifact, FactorArtifact)
                or artifact.factor_ref != reference
            ):
                raise ValueError("factor artifacts have an invalid identity")
        if self.factor_source is not None and not callable(
            getattr(self.factor_source, "values", None)
        ):
            raise TypeError("factor_source must provide values() or be None")
        if self.factor_source is not None:
            source_universe = getattr(self.factor_source, "universe_hash", None)
            if not isinstance(source_universe, str):
                raise TypeError("factor_source must bind a universe hash")
            validate_sha256(source_universe, "factor source universe_hash")
        if artifacts and self.factor_source is not None:
            raise ValueError("factor result cannot mix artifacts and a factor source")
        object.__setattr__(self, "artifacts", MappingProxyType(artifacts))

    def close(self) -> None:
        """关闭并释放持有的资源。

        入参：
            无。
        返回值：
            无。
        异常：
            无。
        Release resources owned by an optional bounded factor source.
        """
        close = getattr(self.factor_source, "close", None)
        if callable(close):
            close()


class _RunnerSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _cleanup_preserving_primary(
        cleanup: Callable[[], None], primary: BaseException, *, resource: str
    ) -> None:
        """Run cleanup without replacing an authoritative in-flight failure."""
        try:
            cleanup()
        except BaseException as cleanup_error:  # noqa: BLE001
            primary.add_note(
                f"{resource} cleanup failed with {type(cleanup_error).__name__}"
            )

    @staticmethod
    def _validate_environment_identity(
        experiment: ExperimentRecord, environment: Mapping[str, JsonValue]
    ) -> None:
        if environment.get("lockfile_hash") != experiment.lockfile_hash:
            raise ValueError("environment lockfile identity does not match experiment")
        if (
            experiment.source_tree_hash is not None
            and environment.get("source_tree_hash") != experiment.source_tree_hash
        ):
            raise ValueError(
                "environment source tree identity does not match experiment"
            )
        if (
            experiment.git_commit_hash is not None
            and environment.get("git_commit") != experiment.git_commit_hash
        ):
            raise ValueError("environment Git identity does not match experiment")

    @staticmethod
    def _write_exclusive(path: Path, payload: bytes) -> None:
        if not isinstance(payload, bytes):
            raise TypeError("artifact payload must be bytes")
        if path.exists():
            if not path.is_file() or path.read_bytes() != payload:
                raise ValueError(
                    f"experiment layer file conflicts with retry: {path.name}"
                )
            return
        try:
            with path.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            if not path.is_file() or path.read_bytes() != payload:
                raise ValueError(
                    f"experiment layer file conflicts with retry: {path.name}"
                ) from None

    @staticmethod
    def _finite_metrics(path: Path) -> dict[str, float]:
        try:
            payload = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("metrics.json must be valid UTF-8 JSON") from error
        if not isinstance(payload, dict):
            raise TypeError("metrics.json must be a JSON object")
        return {
            name: float(value)
            for name, value in payload.items()
            if isinstance(name, str)
            and not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
        }

    @staticmethod
    def _failure_detail(stage: ExperimentStage, error: BaseException) -> ErrorDetail:
        if isinstance(error, QuantError):
            original = error.detail
            context = dict(original.context)
            context["stage"] = stage.value
            return ErrorDetail(
                code=original.code,
                severity=original.severity,
                message=original.message,
                context=context,
                remediation=original.remediation,
                retryable=original.retryable,
            )
        return ErrorDetail(
            code="EXPERIMENT_RUN_FAILED",
            severity=Severity.SEVERE,
            message="experiment stage failed",
            context={"stage": stage.value, "error_code": type(error).__name__},
            remediation="inspect the experiment stage inputs and diagnostics",
            retryable=False,
        )

    @staticmethod
    def _cancel_detail(stage: ExperimentStage) -> ErrorDetail:
        return ErrorDetail(
            code="EXPERIMENT_CANCELLED",
            severity=Severity.SEVERE,
            message="experiment was cancelled at a cooperative boundary",
            context={"stage": stage.value},
            remediation="create a new task to run the experiment again",
            retryable=False,
        )

    @staticmethod
    def _failure_outcome(detail: ErrorDetail) -> TaskOutcome:
        stage = detail.context.get("stage")
        context: dict[str, JsonValue] = {}
        if isinstance(stage, str):
            context["stage"] = stage
        return TaskOutcome(
            status=TaskStatus.FAILED,
            error={
                "code": detail.code,
                "retryable": detail.retryable,
                "context": context,
            },
        )

    @staticmethod
    def _register_incomplete() -> QuantError:
        return QuantError(
            ErrorDetail(
                code="EXPERIMENT_REGISTER_INCOMPLETE",
                severity=Severity.SEVERE,
                message="runner returned before experiment registration completed",
                context={"stage": ExperimentStage.REGISTER.value},
                remediation="inspect the registry transaction and retry safely",
                retryable=False,
            )
        )

    @staticmethod
    def _experiment_uuid(value: str) -> UUID:
        try:
            parsed = UUID(value)
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError("experiment ID must be a canonical UUID") from error
        if str(parsed) != value:
            raise ValueError("experiment ID must be a canonical UUID")
        return parsed


@dataclass(frozen=True, slots=True)
class ExperimentRunResult:
    """记录一次实验操作的结果、业务指标和审计身份。

    入参：
        publication：不可变发布物。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    The final validated publication registered for the experiment.
    """

    publication: ExperimentArtifactPublication

    def __post_init__(self) -> None:
        if not isinstance(self.publication, ExperimentArtifactPublication):
            raise TypeError("publication must be ExperimentArtifactPublication")


class ExperimentStageFailure(RuntimeError):
    """表示 ``ExperimentStageFailure`` 对应的领域异常。

    入参：
        stage：执行阶段。
        error：需要处理或传播的异常，类型为 ``BaseException``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Retain the authoritative stage while preserving the original exception.
    """

    def __init__(self, stage: ExperimentStage, error: BaseException) -> None:
        if not isinstance(stage, ExperimentStage):
            raise TypeError("stage must be an ExperimentStage")
        if not isinstance(error, BaseException):
            raise TypeError("error must be an exception")
        self.stage = stage
        self.error = error
        super().__init__(f"experiment failed at {stage.value}: {type(error).__name__}")


class ExperimentRunCancelled(RuntimeError):
    """表示 ``ExperimentRunCancelled`` 对应的领域异常。

    入参：
        stage：执行阶段。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Cooperative cancellation observed before an irreversible stage boundary.
    """

    def __init__(self, stage: ExperimentStage) -> None:
        if not isinstance(stage, ExperimentStage):
            raise TypeError("stage must be an ExperimentStage")
        self.stage = stage
        super().__init__(f"experiment cancelled at {stage.value}")


class BacktestProgressSink(Protocol):
    """定义 ``BacktestProgressSink`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``BacktestProgressSink`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def update(self, completed: int, total: int, trade_date: date) -> None:
        """更新处理状态或进度。

        入参：
            completed：完成。
            total：总量。
            trade_date：目标交易日期，类型为 ``date``。
        返回值：
            无。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def event(self, event: str, context: Mapping[str, object]) -> None:
        """记录回测恢复边界的结构化事件。

        入参：
            event：稳定事件名称。
            context：事件业务上下文。
        返回值：
            无。
        异常：
            由具体日志实现按契约抛出。
        """
        ...


class CancellationToken(Protocol):
    """定义 ``CancellationToken`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``CancellationToken`` 实例。
    异常：
        由具体实现按接口契约定义。
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


class TaskProgressSink(Protocol):
    """定义 ``TaskProgressSink`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``TaskProgressSink`` 实例。
    异常：
        由具体实现按接口契约定义。
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


class PreparedExperimentRuntime(Protocol):
    """定义 ``PreparedExperimentRuntime`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    One current-data runtime prepared for an immutable experiment.
    """

    def validate(self) -> None:
        """校验实验。

        入参：
            无。
        返回值：
            无。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def assert_current_data(self, stage: str) -> None:
        """处理实验中的``assert``当前值数据。

        入参：
            stage：执行阶段。
        返回值：
            无。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def build_universe(self) -> ExperimentUniverseResult:
        """构建股票池。

        入参：
            无。
        返回值：
            返回构建股票池后的股票池（``ExperimentUniverseResult``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def compute_factors(
        self, universe: ExperimentUniverseResult
    ) -> ExperimentFactorResult:
        """计算因子集合。

        入参：
            universe：股票池。
        返回值：
            返回计算因子集合后的因子集合（``ExperimentFactorResult``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def backtest(
        self,
        universe: ExperimentUniverseResult,
        factors: ExperimentFactorResult,
        progress: BacktestProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        """处理实验中的回测。

        入参：
            universe：股票池。
            factors：因子集合。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回回测（``BacktestResult``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class _ExperimentQuery(Protocol):
    def get(self, experiment_id: str) -> ExperimentDetail: ...


class _ExperimentRegistry(Protocol):
    def transition(
        self,
        experiment_id: str,
        expected: ExperimentStatus,
        target: ExperimentStatus,
        reason: ErrorDetail | None = None,
        *,
        actor: str = "system",
        request_id: str | None = None,
    ) -> None: ...

    def register_success(
        self,
        experiment_id: str,
        manifest: ExperimentArtifactPublication,
        metrics: Mapping[str, float],
        *,
        actor: str = "system",
        request_id: str | None = None,
    ) -> None: ...


class _ArtifactFinalizer(Protocol):
    def validate_environment(self, experiment: ExperimentRecord) -> None: ...

    def finalize(
        self,
        experiment: ExperimentRecord,
        backtest: BacktestResult,
    ) -> ExperimentArtifactPublication: ...


class _ExperimentRunner(Protocol):
    def run(
        self,
        experiment_id: str,
        progress: TaskProgressSink,
        cancellation: CancellationToken,
    ) -> ExperimentRunResult: ...

    def verify_success(self, experiment_id: str) -> ExperimentArtifactPublication: ...


class _Publisher(Protocol):
    def __call__(
        self,
        staging_dir: Path,
        artifact_root: Path,
        experiment_id: UUID,
        *,
        resolved_config: Mapping[str, JsonValue],
    ) -> ExperimentArtifactPublication: ...


class _TaskLogMaterializer(Protocol):
    def __call__(self, experiment_id: str, staging_dir: Path) -> Path: ...


class ExperimentTaskLogMaterializer:
    """从活跃任务尝试解析日志绑定并固化其不可变成功前缀。

    入参：
        queue：持久化任务状态、认领和重试的任务队列。
        manager：``manager``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Resolve the active attempt from durable state and freeze its bound log.
    """

    def __init__(self, queue: TaskQueue, manager: TaskLogManager) -> None:
        if not isinstance(queue, TaskQueue):
            raise TypeError("queue must be a TaskQueue")
        if not isinstance(manager, TaskLogManager):
            raise TypeError("manager must be a TaskLogManager")
        self._queue = queue
        self._manager = manager

    def __call__(self, experiment_id: str, staging_dir: Path) -> Path:
        """以可调用对象形式执行公开协议。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            staging_dir：发布前写入文件的同文件系统暂存目录。
        返回值：
            返回``call``（``Path``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        tasks = self._queue.list_for_experiment(experiment_id, limit=2)
        if len(tasks) != 1:
            raise ValueError("experiment must have one canonical task log source")
        task = tasks[0]
        active_attempts = [
            attempt
            for attempt in self._queue.list_attempts(task.id, limit=100)
            if attempt.status in {TaskStatus.RUNNING, TaskStatus.CANCEL_REQUESTED}
        ]
        if len(active_attempts) != 1:
            raise ValueError("experiment must have one active attempt log owner")
        attempt = active_attempts[0]
        if attempt.worker_id is None:
            raise ValueError("active attempt worker identity is unavailable")
        context = LogContext(
            request_id=attempt.id,
            experiment_id=experiment_id,
            task_id=task.id,
            attempt_id=attempt.id,
            worker_id=attempt.worker_id,
        )
        if attempt.log_path is None:
            return self._manager.materialize_unavailable(
                context,
                staging_dir,
                stage=ExperimentStage.ARTIFACT_VERIFY.value,
            )
        if attempt.log_path != str(self._manager.diagnostic_path(context)):
            raise ValueError("active attempt log binding changed")
        return self._manager.seal_and_materialize(
            context,
            staging_dir,
            stage=ExperimentStage.ARTIFACT_VERIFY.value,
            sealed_through="ARTIFACT_VERIFY/pre-register",
        )


class ExperimentArtifactFinalizer:
    """补齐实验层产物后调用原子发布器并复核最终目录。

    入参：
        artifact_root：不可变实验产物的可信根目录。
        environment：参与本次处理的运行环境；调用方不得依赖未声明的顺序。
        publisher：``publisher``。
        task_log_materializer：任务日志产物固化器。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Add the experiment layer and invoke the existing atomic publisher.
    """

    def __init__(
        self,
        *,
        artifact_root: Path,
        environment: Mapping[str, JsonValue],
        publisher: _Publisher = publish_experiment_artifacts,
        task_log_materializer: _TaskLogMaterializer | None = None,
    ) -> None:
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        if not isinstance(environment, Mapping):
            raise TypeError("environment must be a mapping")
        environment_bytes = canonical_json_bytes(environment)
        plain_environment = json.loads(environment_bytes)
        if (
            not isinstance(plain_environment, dict)
            or set(plain_environment) != _ENVIRONMENT_FIELDS
        ):
            raise ValueError("environment has invalid fields")
        if not callable(publisher):
            raise TypeError("publisher must be callable")
        if task_log_materializer is not None and not callable(task_log_materializer):
            raise TypeError("task_log_materializer must be callable or None")
        self._artifact_root = artifact_root
        self._environment = cast(dict[str, JsonValue], plain_environment)
        self._environment_bytes = environment_bytes
        self._publisher = publisher
        self._task_log_materializer = task_log_materializer

    def recover(
        self, experiment: ExperimentRecord
    ) -> ExperimentArtifactPublication | None:
        """恢复并复核实验。

        入参：
            experiment：实验。
        返回值：
            返回``recover``（``ExperimentArtifactPublication | None``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        Return a fully validated publication left after a pre-register crash.
        """
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        final_dir = self._artifact_root / f"experiment_id={experiment.id}"
        if not os.path.lexists(final_dir):
            return None
        _RunnerSupport._validate_environment_identity(experiment, self._environment)
        return validate_experiment_artifacts(
            final_dir,
            resolved_config=experiment.config,
        )

    def validate_environment(self, experiment: ExperimentRecord) -> None:
        """校验当前 Worker 启动环境与实验绑定身份完全一致。

        入参：
            experiment：即将由当前 Worker 执行的不可变实验记录。
        返回值：
            无。
        异常：
            Worker 的源码或锁文件身份与实验不一致时抛出 ``ValueError``。

        该校验在 ``VALIDATE`` 阶段执行，使长期运行的旧 Worker 在昂贵计算前
            立即失败；发布阶段仍会重复校验以关闭检查与使用之间的窗口。
        """
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        _RunnerSupport._validate_environment_identity(experiment, self._environment)

    def finalize(
        self,
        experiment: ExperimentRecord,
        backtest: BacktestResult,
    ) -> ExperimentArtifactPublication:
        """处理实验中的``finalize``。

        入参：
            experiment：实验。
            backtest：回测。
        返回值：
            返回``finalize``（``ExperimentArtifactPublication``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        if not isinstance(backtest, BacktestResult):
            raise TypeError("backtest must be a BacktestResult")
        experiment_uuid = _RunnerSupport._experiment_uuid(experiment.id)
        if (
            backtest.experiment_id != experiment_uuid
            or backtest.artifact_dir.name != f"experiment_id={experiment.id}"
        ):
            raise ValueError("backtest artifact identity does not match experiment")
        self.validate_environment(experiment)
        final_dir = self._artifact_root / f"experiment_id={experiment.id}"
        if final_dir.exists():
            return validate_experiment_artifacts(
                final_dir, resolved_config=experiment.config
            )
        staging = backtest.artifact_dir
        _RunnerSupport._write_exclusive(
            staging / "resolved_config.yaml",
            yaml.safe_dump(
                experiment.config,
                allow_unicode=True,
                sort_keys=True,
            ).encode("utf-8"),
        )
        _RunnerSupport._write_exclusive(
            staging / "environment.json", self._environment_bytes
        )
        report = (
            '<!doctype html><html><head><meta charset="utf-8"></head>'
            f"<body><h1>Experiment {html.escape(experiment.id)}</h1></body></html>\n"
        ).encode()
        _RunnerSupport._write_exclusive(staging / "report.html", report)
        if self._task_log_materializer is None:
            stage_log = "".join(f"{stage.value}\n" for stage in EXPERIMENT_STAGES[:-1])
            _RunnerSupport._write_exclusive(
                staging / "run.log", stage_log.encode("utf-8")
            )
        else:
            materialized = self._task_log_materializer(experiment.id, staging)
            if materialized != staging / "run.log":
                raise ValueError("task log materializer returned an invalid path")
        publication = self._publisher(
            staging,
            self._artifact_root,
            experiment_uuid,
            resolved_config=experiment.config,
        )
        if not isinstance(publication, ExperimentArtifactPublication):
            raise TypeError("publisher must return ExperimentArtifactPublication")
        return publication

    def cleanup_intermediate(self, experiment_id: str) -> None:
        """在 REGISTER 已确认成功后尽力清理中间回测副本。

        入参：
            experiment_id：已成功登记的实验 UUID 字符串。
        返回值：
            无。
        异常：
            ValueError：实验标识不是有效 UUID 时抛出。
        """
        experiment_uuid = _RunnerSupport._experiment_uuid(experiment_id)
        intermediate = (
            self._artifact_root
            / ".experiment-staging"
            / f"experiment_id={experiment_uuid}"
        )
        if not intermediate.exists() or intermediate.is_symlink():
            return
        try:
            shutil.rmtree(intermediate)
        except OSError:
            pass


class _BacktestTaskProgress:
    def __init__(self, progress: TaskProgressSink) -> None:
        self._progress = progress

    def update(self, completed: int, total: int, trade_date: date) -> None:
        if (
            type(completed) is not int
            or type(total) is not int
            or type(trade_date) is not date
        ):
            raise TypeError("backtest progress values have invalid types")
        if total <= 0 or completed < 0 or completed > total:
            raise ValueError("backtest progress values are out of bounds")
        self._progress.update(
            TaskProgress(
                stage=ExperimentStage.BACKTEST.value,
                completed=3,
                total=len(EXPERIMENT_STAGES),
                message=(
                    f"session {completed}/{total} completed at {trade_date.isoformat()}"
                ),
            )
        )

    def event(self, event: str, context: Mapping[str, object]) -> None:
        if not isinstance(event, str) or not event:
            raise ValueError("event must be a nonempty string")
        if not isinstance(context, Mapping):
            raise TypeError("context must be a mapping")
        emit = getattr(self._progress, "event", None)
        if callable(emit):
            emit(event, context)


class ExperimentRunner:
    """按固定阶段顺序编排一次实验运行。

    入参：
        query：查询条件。
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        返回完成字段规范化和不变量校验的对象。
        artifact_finalizer：产物``finalizer``。
        analytics_materializer：由组合根注入、用于隔离外部副作用的分析产物固化器端口。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Execute the seven fixed stages with publication before registration.
    """

    def __init__(
        self,
        *,
        query: _ExperimentQuery,
        registry: _ExperimentRegistry,
        runtime_factory: Callable[[ExperimentRecord], PreparedExperimentRuntime],
        artifact_finalizer: _ArtifactFinalizer,
        analytics_materializer: Callable[[Path], object] = materialize_analytics,
    ) -> None:
        if query is None or registry is None:
            raise TypeError("query and registry must be supplied")
        if not callable(runtime_factory) or not callable(analytics_materializer):
            raise TypeError(
                "runtime_factory and analytics_materializer must be callable"
            )
        if not callable(getattr(artifact_finalizer, "finalize", None)) or not callable(
            getattr(artifact_finalizer, "validate_environment", None)
        ):
            raise TypeError(
                "artifact_finalizer must provide finalize() and validate_environment()"
            )
        self._query = query
        self._registry = registry
        self._runtime_factory = runtime_factory
        self._analytics = analytics_materializer
        self._finalizer = artifact_finalizer

    def run(
        self,
        experiment_id: str,
        progress: TaskProgressSink,
        cancellation: CancellationToken,
    ) -> ExperimentRunResult:
        """执行完整处理流程。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行实验后的运行（``ExperimentRunResult``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentStageFailure``、``TypeError``、``ValueError``。
        """
        if not isinstance(experiment_id, str) or not experiment_id:
            raise ValueError("experiment_id must be a nonempty string")
        if not callable(getattr(progress, "update", None)) or not callable(
            getattr(cancellation, "is_cancelled", None)
        ):
            raise TypeError("progress and cancellation ports are required")
        experiment = self._query.get(experiment_id).record
        if experiment.status is not ExperimentStatus.RUNNING:
            raise ValueError("experiment runner requires RUNNING status")

        recovered = self._recover_publication(experiment)
        if recovered is not None:
            self._emit_progress_event(
                progress,
                "experiment.publication_recovered",
                {"artifact_dir": str(recovered.artifact_dir)},
            )
            self._execute(
                ExperimentStage.REGISTER,
                6,
                progress,
                cancellation,
                lambda: self._register_publication(experiment, recovered),
                check_cancellation=False,
            )
            return ExperimentRunResult(recovered)

        runtime = cast(
            PreparedExperimentRuntime,
            self._execute(
                ExperimentStage.VALIDATE,
                0,
                progress,
                cancellation,
                lambda: self._validated_runtime(experiment),
            ),
        )
        universe = cast(
            ExperimentUniverseResult,
            self._execute(
                ExperimentStage.UNIVERSE,
                1,
                progress,
                cancellation,
                lambda: self._with_data_guard(
                    runtime, ExperimentStage.UNIVERSE, runtime.build_universe
                ),
            ),
        )
        factors = cast(
            ExperimentFactorResult,
            self._execute(
                ExperimentStage.FACTOR_COMPUTE,
                2,
                progress,
                cancellation,
                lambda: self._with_data_guard(
                    runtime,
                    ExperimentStage.FACTOR_COMPUTE,
                    lambda: runtime.compute_factors(universe),
                ),
                result_cleanup=lambda result: cast(
                    ExperimentFactorResult, result
                ).close(),
            ),
        )
        try:
            backtest = cast(
                BacktestResult,
                self._execute(
                    ExperimentStage.BACKTEST,
                    3,
                    progress,
                    cancellation,
                    lambda: self._with_data_guard(
                        runtime,
                        ExperimentStage.BACKTEST,
                        lambda: runtime.backtest(
                            universe,
                            factors,
                            _BacktestTaskProgress(progress),
                            cancellation,
                        ),
                    ),
                ),
            )
        except BaseException as error:
            _RunnerSupport._cleanup_preserving_primary(
                factors.close, error, resource="factor source"
            )
            raise
        try:
            factors.close()
        except Exception as error:
            raise ExperimentStageFailure(ExperimentStage.BACKTEST, error) from error
        self._execute(
            ExperimentStage.ANALYTICS,
            4,
            progress,
            cancellation,
            lambda: self._with_data_guard(
                runtime,
                ExperimentStage.ANALYTICS,
                lambda: self._analytics(backtest.artifact_dir),
            ),
        )
        publication = cast(
            ExperimentArtifactPublication,
            self._execute(
                ExperimentStage.ARTIFACT_VERIFY,
                5,
                progress,
                cancellation,
                lambda: self._with_data_guard(
                    runtime,
                    ExperimentStage.ARTIFACT_VERIFY,
                    lambda: self._finalizer.finalize(experiment, backtest),
                ),
            ),
        )

        self._execute(
            ExperimentStage.REGISTER,
            6,
            progress,
            cancellation,
            lambda: self._with_data_guard(
                runtime,
                ExperimentStage.REGISTER,
                lambda: self._register_publication(experiment, publication),
            ),
            check_cancellation=False,
        )
        return ExperimentRunResult(publication)

    @staticmethod
    def _emit_progress_event(
        progress: TaskProgressSink,
        event: str,
        context: Mapping[str, object],
    ) -> None:
        emit = getattr(progress, "event", None)
        if callable(emit):
            emit(event, context)

    def verify_success(self, experiment_id: str) -> ExperimentArtifactPublication:
        """处理实验中的验证成功发布。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
        返回值：
            返回``success``（``ExperimentArtifactPublication``）。
        异常：
            无。
        """
        detail = self._query.get(experiment_id)
        return validate_registered_publication(detail)

    def _validated_runtime(
        self, experiment: ExperimentRecord
    ) -> PreparedExperimentRuntime:
        self._finalizer.validate_environment(experiment)
        runtime = self._runtime_factory(experiment)
        required = (
            "validate",
            "assert_current_data",
            "build_universe",
            "compute_factors",
            "backtest",
        )
        if any(not callable(getattr(runtime, name, None)) for name in required):
            raise TypeError("runtime_factory returned an invalid prepared runtime")
        runtime.validate()
        return runtime

    @staticmethod
    def _with_data_guard(
        runtime: PreparedExperimentRuntime,
        stage: ExperimentStage,
        operation: Callable[[], object],
    ) -> object:
        runtime.assert_current_data(stage.value)
        result = operation()
        runtime.assert_current_data(stage.value)
        return result

    def _recover_publication(
        self, experiment: ExperimentRecord
    ) -> ExperimentArtifactPublication | None:
        recover = getattr(self._finalizer, "recover", None)
        if not callable(recover):
            return None
        try:
            recovered = recover(experiment)
        except Exception as error:
            raise ExperimentStageFailure(
                ExperimentStage.ARTIFACT_VERIFY, error
            ) from error
        if recovered is not None and not isinstance(
            recovered, ExperimentArtifactPublication
        ):
            raise ExperimentStageFailure(
                ExperimentStage.ARTIFACT_VERIFY,
                TypeError("artifact recovery returned an invalid publication"),
            )
        return recovered

    def _register_publication(
        self,
        experiment: ExperimentRecord,
        publication: ExperimentArtifactPublication,
    ) -> None:
        self._registry.register_success(
            experiment.id,
            publication,
            _RunnerSupport._finite_metrics(publication.artifact_dir / "metrics.json"),
        )
        if (
            self._query.get(experiment.id).record.status
            is not ExperimentStatus.SUCCEEDED
        ):
            raise _RunnerSupport._register_incomplete()
        cleanup = getattr(self._finalizer, "cleanup_intermediate", None)
        if callable(cleanup):
            cleanup(experiment.id)

    @staticmethod
    def _execute(
        stage: ExperimentStage,
        completed_before: int,
        progress: TaskProgressSink,
        cancellation: CancellationToken,
        operation: Callable[[], object],
        *,
        check_cancellation: bool = True,
        result_cleanup: Callable[[object], None] | None = None,
    ) -> object:
        if check_cancellation and cancellation.is_cancelled():
            raise ExperimentRunCancelled(stage)
        missing_result = object()
        result: object = missing_result

        def cleanup_result(primary: BaseException) -> None:
            if result is missing_result or result_cleanup is None:
                return
            _RunnerSupport._cleanup_preserving_primary(
                lambda: result_cleanup(result),
                primary,
                resource=f"{stage.value} result",
            )

        try:
            progress.update(
                TaskProgress(
                    stage=stage.value,
                    completed=completed_before,
                    total=len(EXPERIMENT_STAGES),
                    message=f"entering {stage.value}",
                )
            )
            result = operation()
            progress.update(
                TaskProgress(
                    stage=stage.value,
                    completed=completed_before + 1,
                    total=len(EXPERIMENT_STAGES),
                    message=f"completed {stage.value}",
                )
            )
        except ExperimentRunCancelled as error:
            cleanup_result(error)
            raise
        except BacktestCancelled as error:
            cancelled = ExperimentRunCancelled(stage)
            cleanup_result(cancelled)
            raise cancelled from error
        except Exception as error:
            cleanup_result(error)
            raise ExperimentStageFailure(stage, error) from error
        except BaseException as error:
            cleanup_result(error)
            raise
        return result


class ExperimentBacktestHandler:
    """处理一个已认领的实验任务并持久化结果。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        query：查询条件。
        runner：运行器。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Synchronize the experiment lifecycle around one claimed BACKTEST task.
    """

    task_type = "BACKTEST"

    def __init__(
        self,
        *,
        registry: _ExperimentRegistry,
        query: _ExperimentQuery,
        runner: _ExperimentRunner,
    ) -> None:
        if registry is None or query is None or runner is None:
            raise TypeError("registry, query, and runner must be supplied")
        self._registry = registry
        self._query = query
        self._runner = runner

    def run(
        self,
        task: ClaimedTask,
        progress: TaskProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """执行完整处理流程。

        入参：
            task：Worker 已认领并带所有权围栏的任务快照。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行实验后的运行（``TaskOutcome``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentStageFailure``。
        """
        experiment = self._validate_task(task)
        actor = task.worker_id
        request_id = task.attempt_id
        if experiment.status is ExperimentStatus.SUCCEEDED:
            try:
                self._runner.verify_success(experiment.id)
            # A corrupt publication may surface through any filesystem or parser
            # exception; recovery must convert it to a durable task outcome.
            except Exception as error:
                detail = _RunnerSupport._failure_detail(
                    ExperimentStage.ARTIFACT_VERIFY, error
                )
                raise QuantError(detail) from error
            return TaskOutcome(status=TaskStatus.SUCCEEDED)
        if cancellation.is_cancelled():
            if experiment.status in {ExperimentStatus.QUEUED, ExperimentStatus.RUNNING}:
                reason = _RunnerSupport._cancel_detail(ExperimentStage.VALIDATE)
                self._registry.transition(
                    experiment.id,
                    experiment.status,
                    ExperimentStatus.CANCELLED,
                    reason,
                    actor=actor,
                    request_id=request_id,
                )
            return TaskOutcome(status=TaskStatus.CANCELLED)
        if experiment.status is ExperimentStatus.QUEUED:
            self._registry.transition(
                experiment.id,
                ExperimentStatus.QUEUED,
                ExperimentStatus.RUNNING,
                actor=actor,
                request_id=request_id,
            )
        elif experiment.status is not ExperimentStatus.RUNNING:
            return _RunnerSupport._failure_outcome(
                ErrorDetail(
                    code="EXPERIMENT_NOT_RUNNABLE",
                    severity=Severity.SEVERE,
                    message="experiment is not queued, running, or succeeded",
                    context={"status": experiment.status.value},
                    remediation="submit a CREATED experiment or inspect its terminal state",
                    retryable=False,
                )
            )
        try:
            self._runner.run(experiment.id, progress, cancellation)
            if (
                self._query.get(experiment.id).record.status
                is not ExperimentStatus.SUCCEEDED
            ):
                raise ExperimentStageFailure(
                    ExperimentStage.REGISTER, _RunnerSupport._register_incomplete()
                )
        except ExperimentRunCancelled as error:
            current = self._query.get(experiment.id).record.status
            if current is ExperimentStatus.RUNNING:
                self._registry.transition(
                    experiment.id,
                    ExperimentStatus.RUNNING,
                    ExperimentStatus.CANCELLED,
                    _RunnerSupport._cancel_detail(error.stage),
                    actor=actor,
                    request_id=request_id,
                )
            return TaskOutcome(status=TaskStatus.CANCELLED)
        except ExperimentStageFailure as error:
            detail = _RunnerSupport._failure_detail(error.stage, error.error)
            current = self._query.get(experiment.id).record.status
            if current is ExperimentStatus.RUNNING:
                self._registry.transition(
                    experiment.id,
                    ExperimentStatus.RUNNING,
                    ExperimentStatus.FAILED,
                    detail,
                    actor=actor,
                    request_id=request_id,
                )
            raise QuantError(detail) from error
        # The task boundary must persist unexpected runtime failures rather than
        # letting the worker abandon a RUNNING experiment.
        except Exception as error:
            detail = _RunnerSupport._failure_detail(ExperimentStage.VALIDATE, error)
            current = self._query.get(experiment.id).record.status
            if current is ExperimentStatus.RUNNING:
                self._registry.transition(
                    experiment.id,
                    ExperimentStatus.RUNNING,
                    ExperimentStatus.FAILED,
                    detail,
                    actor=actor,
                    request_id=request_id,
                )
            raise QuantError(detail) from error
        return TaskOutcome(status=TaskStatus.SUCCEEDED)

    def _validate_task(self, task: ClaimedTask) -> ExperimentRecord:
        if not isinstance(task, ClaimedTask) or task.task_type != self.task_type:
            raise TypeError("handler requires a claimed BACKTEST task")
        if task.experiment_id is None:
            raise ValueError("BACKTEST task must reference an experiment")
        if set(task.payload) != {"experiment_id", "config_hash"}:
            raise ValueError(
                "BACKTEST payload must contain experiment_id and config_hash"
            )
        if task.payload["experiment_id"] != task.experiment_id:
            raise ValueError("BACKTEST payload experiment identity does not match task")
        experiment = self._query.get(task.experiment_id).record
        if task.payload["config_hash"] != experiment.config_hash:
            raise ValueError("BACKTEST payload config hash does not match experiment")
        return experiment


__all__ = [
    "EXPERIMENT_STAGES",
    "BacktestProgressSink",
    "CancellationToken",
    "ExperimentArtifactFinalizer",
    "ExperimentBacktestHandler",
    "ExperimentFactorResult",
    "ExperimentRunCancelled",
    "ExperimentRunResult",
    "ExperimentRunner",
    "ExperimentStage",
    "ExperimentStageFailure",
    "ExperimentUniverseResult",
    "PreparedExperimentRuntime",
    "TaskProgressSink",
]
