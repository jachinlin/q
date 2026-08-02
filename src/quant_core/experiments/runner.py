"""Fixed-stage experiment orchestration and BACKTEST task handling."""

from __future__ import annotations

import html
import json
import math
import os
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import UUID

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import yaml

from quant_core.analytics.materialize import materialize_analytics
from quant_core.backtest.artifacts import (
    FACTOR_METRICS_SCHEMA,
    ExperimentArtifactPublication,
    publish_experiment_artifacts,
    validate_experiment_artifacts,
)
from quant_core.backtest.engine import BacktestCancelled, BacktestResult
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail, QuantError
from quant_core.experiments.models import ExperimentRecord, ExperimentStatus
from quant_core.experiments.query import ExperimentDetail
from quant_core.experiments.verification import validate_registered_publication
from quant_core.factors.base import (
    FactorArtifact,
    canonical_factor_ref,
    validate_sha256,
)
from quant_core.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)

_ENVIRONMENT_FIELDS = {
    "schema_version",
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
    """Stable identity of the universe prepared for one experiment runtime."""

    universe_hash: str

    def __post_init__(self) -> None:
        validate_sha256(self.universe_hash, "universe_hash")


@dataclass(frozen=True, slots=True)
class ExperimentFactorResult:
    """Verified factor artifacts plus the exact experiment metrics table."""

    artifacts: Mapping[str, FactorArtifact]
    metrics: pa.Table

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
        if not isinstance(self.metrics, pa.Table):
            raise TypeError("metrics must be a pyarrow Table")
        metrics = self.metrics.combine_chunks()
        if metrics.schema != FACTOR_METRICS_SCHEMA:
            raise ValueError("factor metrics must use FACTOR_METRICS_SCHEMA")
        object.__setattr__(self, "artifacts", MappingProxyType(artifacts))
        object.__setattr__(self, "metrics", metrics)


@dataclass(frozen=True, slots=True)
class ExperimentRunResult:
    """The final validated publication registered for the experiment."""

    publication: ExperimentArtifactPublication

    def __post_init__(self) -> None:
        if not isinstance(self.publication, ExperimentArtifactPublication):
            raise TypeError("publication must be ExperimentArtifactPublication")


class ExperimentStageFailure(RuntimeError):
    """Retain the authoritative stage while preserving the original exception."""

    def __init__(self, stage: ExperimentStage, error: BaseException) -> None:
        if not isinstance(stage, ExperimentStage):
            raise TypeError("stage must be an ExperimentStage")
        if not isinstance(error, BaseException):
            raise TypeError("error must be an exception")
        self.stage = stage
        self.error = error
        super().__init__(f"experiment failed at {stage.value}: {type(error).__name__}")


class ExperimentRunCancelled(RuntimeError):
    """Cooperative cancellation observed before an irreversible stage boundary."""

    def __init__(self, stage: ExperimentStage) -> None:
        if not isinstance(stage, ExperimentStage):
            raise TypeError("stage must be an ExperimentStage")
        self.stage = stage
        super().__init__(f"experiment cancelled at {stage.value}")


class BacktestProgressSink(Protocol):
    def update(self, completed: int, total: int, trade_date: date) -> None: ...


class CancellationToken(Protocol):
    def is_cancelled(self) -> bool: ...


class TaskProgressSink(Protocol):
    def update(self, progress: TaskProgress) -> None: ...


class PreparedExperimentRuntime(Protocol):
    """One concrete snapshot runtime prepared for an immutable experiment."""

    def validate(self) -> None: ...

    def build_universe(self) -> ExperimentUniverseResult: ...

    def compute_factors(
        self, universe: ExperimentUniverseResult
    ) -> ExperimentFactorResult: ...

    def backtest(
        self,
        universe: ExperimentUniverseResult,
        factors: ExperimentFactorResult,
        progress: BacktestProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult: ...


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
    def finalize(
        self,
        experiment: ExperimentRecord,
        factors: ExperimentFactorResult,
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


class ExperimentArtifactFinalizer:
    """Add the experiment layer and invoke the existing atomic publisher."""

    def __init__(
        self,
        *,
        artifact_root: Path,
        environment: Mapping[str, JsonValue],
        publisher: _Publisher = publish_experiment_artifacts,
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
        self._artifact_root = artifact_root
        self._environment = cast(dict[str, JsonValue], plain_environment)
        self._environment_bytes = environment_bytes
        self._publisher = publisher

    def recover(
        self, experiment: ExperimentRecord
    ) -> ExperimentArtifactPublication | None:
        """Return a fully validated publication left after a pre-register crash."""
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        final_dir = self._artifact_root / f"experiment_id={experiment.id}"
        if not os.path.lexists(final_dir):
            return None
        _validate_environment_identity(experiment, self._environment)
        return validate_experiment_artifacts(
            final_dir,
            resolved_config=experiment.config,
        )

    def finalize(
        self,
        experiment: ExperimentRecord,
        factors: ExperimentFactorResult,
        backtest: BacktestResult,
    ) -> ExperimentArtifactPublication:
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        if not isinstance(factors, ExperimentFactorResult):
            raise TypeError("factors must be an ExperimentFactorResult")
        if not isinstance(backtest, BacktestResult):
            raise TypeError("backtest must be a BacktestResult")
        experiment_uuid = _experiment_uuid(experiment.id)
        if (
            backtest.experiment_id != experiment_uuid
            or backtest.artifact_dir.name != f"experiment_id={experiment.id}"
        ):
            raise ValueError("backtest artifact identity does not match experiment")
        _validate_environment_identity(experiment, self._environment)
        final_dir = self._artifact_root / f"experiment_id={experiment.id}"
        if final_dir.exists():
            return validate_experiment_artifacts(
                final_dir, resolved_config=experiment.config
            )
        staging = backtest.artifact_dir
        _write_exclusive(
            staging / "resolved_config.yaml",
            yaml.safe_dump(
                experiment.config,
                allow_unicode=True,
                sort_keys=True,
            ).encode("utf-8"),
        )
        _write_exclusive(staging / "environment.json", self._environment_bytes)
        _write_factor_metrics(staging / "factor_metrics.parquet", factors.metrics)
        report = (
            '<!doctype html><html><head><meta charset="utf-8"></head>'
            f"<body><h1>Experiment {html.escape(experiment.id)}</h1></body></html>\n"
        ).encode()
        _write_exclusive(staging / "report.html", report)
        stage_log = "".join(f"{stage.value}\n" for stage in EXPERIMENT_STAGES[:-1])
        _write_exclusive(staging / "run.log", stage_log.encode("utf-8"))
        publication = self._publisher(
            staging,
            self._artifact_root,
            experiment_uuid,
            resolved_config=experiment.config,
        )
        if not isinstance(publication, ExperimentArtifactPublication):
            raise TypeError("publisher must return ExperimentArtifactPublication")
        return publication


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


class ExperimentRunner:
    """Execute the seven fixed stages with publication before registration."""

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
        if not callable(getattr(artifact_finalizer, "finalize", None)):
            raise TypeError("artifact_finalizer must provide finalize()")
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
                runtime.build_universe,
            ),
        )
        factors = cast(
            ExperimentFactorResult,
            self._execute(
                ExperimentStage.FACTOR_COMPUTE,
                2,
                progress,
                cancellation,
                lambda: runtime.compute_factors(universe),
            ),
        )
        backtest = cast(
            BacktestResult,
            self._execute(
                ExperimentStage.BACKTEST,
                3,
                progress,
                cancellation,
                lambda: runtime.backtest(
                    universe,
                    factors,
                    _BacktestTaskProgress(progress),
                    cancellation,
                ),
            ),
        )
        self._execute(
            ExperimentStage.ANALYTICS,
            4,
            progress,
            cancellation,
            lambda: self._analytics(backtest.artifact_dir),
        )
        publication = cast(
            ExperimentArtifactPublication,
            self._execute(
                ExperimentStage.ARTIFACT_VERIFY,
                5,
                progress,
                cancellation,
                lambda: self._finalizer.finalize(experiment, factors, backtest),
            ),
        )

        self._execute(
            ExperimentStage.REGISTER,
            6,
            progress,
            cancellation,
            lambda: self._register_publication(experiment, publication),
            check_cancellation=False,
        )
        return ExperimentRunResult(publication)

    def verify_success(self, experiment_id: str) -> ExperimentArtifactPublication:
        detail = self._query.get(experiment_id)
        return validate_registered_publication(detail)

    def _validated_runtime(
        self, experiment: ExperimentRecord
    ) -> PreparedExperimentRuntime:
        runtime = self._runtime_factory(experiment)
        required = ("validate", "build_universe", "compute_factors", "backtest")
        if any(not callable(getattr(runtime, name, None)) for name in required):
            raise TypeError("runtime_factory returned an invalid prepared runtime")
        runtime.validate()
        return runtime

    def _recover_publication(
        self, experiment: ExperimentRecord
    ) -> ExperimentArtifactPublication | None:
        recover = getattr(self._finalizer, "recover", None)
        if not callable(recover):
            return None
        try:
            recovered = recover(experiment)
        except Exception as error:
            raise ExperimentStageFailure(ExperimentStage.ARTIFACT_VERIFY, error) from error
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
            _finite_metrics(publication.artifact_dir / "metrics.json"),
        )
        if (
            self._query.get(experiment.id).record.status
            is not ExperimentStatus.SUCCEEDED
        ):
            raise _register_incomplete()

    @staticmethod
    def _execute(
        stage: ExperimentStage,
        completed_before: int,
        progress: TaskProgressSink,
        cancellation: CancellationToken,
        operation: Callable[[], object],
        *,
        check_cancellation: bool = True,
    ) -> object:
        if check_cancellation and cancellation.is_cancelled():
            raise ExperimentRunCancelled(stage)
        progress.update(
            TaskProgress(
                stage=stage.value,
                completed=completed_before,
                total=len(EXPERIMENT_STAGES),
                message=f"entering {stage.value}",
            )
        )
        try:
            result = operation()
        except ExperimentRunCancelled:
            raise
        except BacktestCancelled as error:
            raise ExperimentRunCancelled(stage) from error
        except Exception as error:
            raise ExperimentStageFailure(stage, error) from error
        progress.update(
            TaskProgress(
                stage=stage.value,
                completed=completed_before + 1,
                total=len(EXPERIMENT_STAGES),
                message=f"completed {stage.value}",
            )
        )
        return result


class ExperimentBacktestHandler:
    """Synchronize the experiment lifecycle around one claimed BACKTEST task."""

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
        experiment = self._validate_task(task)
        actor = task.worker_id
        request_id = task.attempt_id
        if experiment.status is ExperimentStatus.SUCCEEDED:
            try:
                self._runner.verify_success(experiment.id)
            # A corrupt publication may surface through any filesystem or parser
            # exception; recovery must convert it to a durable task outcome.
            except Exception as error:  # noqa: BLE001
                return _failure_outcome(
                    _failure_detail(ExperimentStage.ARTIFACT_VERIFY, error)
                )
            return TaskOutcome(status=TaskStatus.SUCCEEDED)
        if cancellation.is_cancelled():
            if experiment.status in {ExperimentStatus.QUEUED, ExperimentStatus.RUNNING}:
                reason = _cancel_detail(ExperimentStage.VALIDATE)
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
            return _failure_outcome(
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
                    ExperimentStage.REGISTER, _register_incomplete()
                )
        except ExperimentRunCancelled as error:
            current = self._query.get(experiment.id).record.status
            if current is ExperimentStatus.RUNNING:
                self._registry.transition(
                    experiment.id,
                    ExperimentStatus.RUNNING,
                    ExperimentStatus.CANCELLED,
                    _cancel_detail(error.stage),
                    actor=actor,
                    request_id=request_id,
                )
            return TaskOutcome(status=TaskStatus.CANCELLED)
        except ExperimentStageFailure as error:
            detail = _failure_detail(error.stage, error.error)
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
            return _failure_outcome(detail)
        # The task boundary must persist unexpected runtime failures rather than
        # letting the worker abandon a RUNNING experiment.
        except Exception as error:  # noqa: BLE001
            detail = _failure_detail(ExperimentStage.VALIDATE, error)
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
            return _failure_outcome(detail)
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


def _validate_environment_identity(
    experiment: ExperimentRecord, environment: Mapping[str, JsonValue]
) -> None:
    if environment.get("lockfile_hash") != experiment.lockfile_hash:
        raise ValueError("environment lockfile identity does not match experiment")
    if (
        experiment.source_tree_hash is not None
        and environment.get("source_tree_hash") != experiment.source_tree_hash
    ):
        raise ValueError("environment source tree identity does not match experiment")
    if (
        experiment.git_commit_hash is not None
        and environment.get("git_commit") != experiment.git_commit_hash
    ):
        raise ValueError("environment Git identity does not match experiment")


def _write_exclusive(path: Path, payload: bytes) -> None:
    if not isinstance(payload, bytes):
        raise TypeError("artifact payload must be bytes")
    try:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as error:
        raise ValueError(
            f"experiment layer file already exists: {path.name}"
        ) from error


def _write_factor_metrics(path: Path, table: pa.Table) -> None:
    if table.schema != FACTOR_METRICS_SCHEMA:
        raise ValueError("factor metrics must use FACTOR_METRICS_SCHEMA")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=".factor-metrics-",
            suffix=".parquet",
            dir=path.parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
        pq.write_table(table, temporary, compression="zstd")
        if path.exists():
            raise ValueError(
                "experiment layer file already exists: factor_metrics.parquet"
            )
        temporary.replace(path)
        temporary = None
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


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


def _cancel_detail(stage: ExperimentStage) -> ErrorDetail:
    return ErrorDetail(
        code="EXPERIMENT_CANCELLED",
        severity=Severity.SEVERE,
        message="experiment was cancelled at a cooperative boundary",
        context={"stage": stage.value},
        remediation="create a new task to run the experiment again",
        retryable=False,
    )


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


def _experiment_uuid(value: str) -> UUID:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("experiment ID must be a canonical UUID") from error
    if str(parsed) != value:
        raise ValueError("experiment ID must be a canonical UUID")
    return parsed


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
