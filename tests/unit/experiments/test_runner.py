from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.artifacts import (
    FACTOR_METRICS_SCHEMA,
    ExperimentArtifactPublication,
)
from quant_core.backtest.engine import BacktestResult, StrategyRef
from quant_core.backtest.rulebook import AShareRuleBook
from quant_core.data.contracts import ProviderCapabilities
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.enums import DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    QualityRunId,
    SnapshotId,
)
from quant_core.errors import ErrorDetail, QuantError
from quant_core.experiments.config import (
    ExperimentCapabilityUnavailable,
    ExperimentConfigError,
    require_provider_capabilities,
    resolve_experiment_yaml,
)
from quant_core.experiments.models import (
    ExperimentRecord,
    ExperimentStatus,
    ResearchMark,
)
from quant_core.experiments.query import ExperimentDetail
from quant_core.experiments.runner import (
    EXPERIMENT_STAGES,
    ExperimentArtifactFinalizer,
    ExperimentBacktestHandler,
    ExperimentFactorResult,
    ExperimentRunCancelled,
    ExperimentRunner,
    ExperimentStage,
    ExperimentStageFailure,
    ExperimentUniverseResult,
)
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetVersionRecord,
    SnapshotRecord,
)
from quant_core.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)

_SNAPSHOT_ID = SnapshotId.parse("00000000-0000-0000-0000-000000000601")
_DAILY_VERSION_ID = DatasetVersionId.parse("00000000-0000-0000-0000-000000000602")
_QUALITY_RUN_ID = QualityRunId.parse("00000000-0000-0000-0000-000000000603")
_STOCK_REF = StrategyRef("stock_multifactor", "1.0.0")


class _Catalog:
    def __init__(
        self,
        snapshot: SnapshotRecord,
        daily_version: DatasetVersionRecord,
    ) -> None:
        self.snapshot = snapshot
        self.daily_version = daily_version

    def latest_snapshot(self) -> SnapshotRecord | None:
        return self.snapshot

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord:
        if identifier != self.snapshot.id:
            raise KeyError(str(identifier))
        return self.snapshot

    def get_dataset_version(self, identifier: DatasetVersionId) -> DatasetVersionRecord:
        if identifier != self.daily_version.id:
            raise KeyError(str(identifier))
        return self.daily_version


def _catalog(tmp_path: Path) -> _Catalog:
    now = datetime(2026, 8, 2, tzinfo=UTC)
    partition = tmp_path / "daily.parquet"
    partition.write_bytes(b"fixture identity only")
    daily = DatasetVersionRecord(
        id=_DAILY_VERSION_ID,
        dataset=DatasetKind.DAILY_BAR,
        fingerprint="1" * 64,
        source="offline-complete-fixture",
        status=SnapshotStatus.PUBLISHED.value,
        partitions=(
            DatasetPartitionRecord(
                content_hash="2" * 64,
                path=partition,
                schema_fingerprint="3" * 64,
                row_count=6,
            ),
        ),
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 31),
        created_run_id="fixture-run",
        created_at=now,
    )
    manifest = tmp_path / "snapshot-manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    snapshot = SnapshotRecord(
        id=_SNAPSHOT_ID,
        publication_fingerprint="4" * 64,
        as_of=now,
        status=SnapshotStatus.PUBLISHED,
        manifest_path=manifest,
        manifest_hash="5" * 64,
        quality_run_id=_QUALITY_RUN_ID,
        dataset_versions={DatasetKind.DAILY_BAR.value: _DAILY_VERSION_ID},
        created_at=now,
        published_at=now,
    )
    return _Catalog(snapshot, daily)


def _rulebook() -> AShareRuleBook:
    root = Path(__file__).resolve().parents[3]
    return AShareRuleBook.load(root / "configs" / "rules" / "a_share_v1.yaml")


def _write_config(root: Path, body: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / "experiment.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _minimal_yaml(**overrides: str) -> str:
    values = {
        "strategy_id": "stock_multifactor",
        "strategy_version": "1.0.0",
        "snapshot_id": "latest",
        "start_date": "snapshot_start",
        "end_date": "snapshot_end",
        "rulebook_version": "a-share-v1",
    }
    values.update(overrides)
    return "\n".join(
        (
            "schema_version: 1",
            f"strategy_id: {values['strategy_id']}",
            f"strategy_version: {values['strategy_version']}",
            f"snapshot_id: {values['snapshot_id']}",
            f"start_date: {values['start_date']}",
            f"end_date: {values['end_date']}",
            "benchmark: SSE:000300",
            "initial_cash_fen: 100000000",
            f"rulebook_version: {values['rulebook_version']}",
            "strategy_config: {}",
        )
    )


def test_resolves_snapshot_selectors_and_all_defaults_to_concrete_json(
    tmp_path: Path,
) -> None:
    config_root = tmp_path / "configs"
    path = _write_config(config_root, _minimal_yaml())

    resolved = resolve_experiment_yaml(
        path,
        config_root=config_root,
        catalog=_catalog(tmp_path),
        strategies={_STOCK_REF: object()},
        rulebook=_rulebook(),
    )

    assert resolved.snapshot_manifest_hash == "5" * 64
    assert resolved.mapping == {
        "benchmark": "SSE:000300",
        "end_date": "2024-01-31",
        "execution": {
            "max_volume_participation": 0.1,
            "reference_price": "OPEN",
            "slippage_bps": 5.0,
        },
        "initial_cash_fen": 100000000,
        "rulebook_version": "a-share-v1",
        "schema_version": 1,
        "snapshot_id": "00000000-0000-0000-0000-000000000601",
        "start_date": "2024-01-02",
        "strategy_config": {},
        "strategy_id": "stock_multifactor",
        "strategy_version": "1.0.0",
        "universe": {
            "allowed_boards": ["CHINEXT", "MAIN", "STAR"],
            "exclude_st": True,
            "exclude_suspended": True,
            "min_avg_amount_20d": None,
            "min_listing_days": 120,
        },
    }
    assert "latest" not in repr(resolved.mapping)
    assert "snapshot_start" not in repr(resolved.mapping)
    assert "snapshot_end" not in repr(resolved.mapping)


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ("unknown_top_level: true\n", "unknown_top_level"),
        ("execution:\n  surprise: true\n", "surprise"),
        ("universe:\n  surprise: true\n", "surprise"),
    ],
)
def test_rejects_unknown_keys_at_every_envelope_level(
    tmp_path: Path, change: str, match: str
) -> None:
    config_root = tmp_path / "configs"
    path = _write_config(config_root, _minimal_yaml() + "\n" + change)

    with pytest.raises(ExperimentConfigError, match=match):
        resolve_experiment_yaml(
            path,
            config_root=config_root,
            catalog=_catalog(tmp_path),
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )


def test_rejects_unsafe_yaml_non_mapping_and_paths_outside_config_root(
    tmp_path: Path,
) -> None:
    config_root = tmp_path / "configs"
    unsafe = _write_config(
        config_root,
        "!!python/object/apply:os.system ['echo unsafe']\n",
    )
    with pytest.raises(ExperimentConfigError, match="safe YAML"):
        resolve_experiment_yaml(
            unsafe,
            config_root=config_root,
            catalog=_catalog(tmp_path),
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )

    unsafe.write_text("- not\n- a mapping\n", encoding="utf-8")
    with pytest.raises(ExperimentConfigError, match="mapping"):
        resolve_experiment_yaml(
            unsafe,
            config_root=config_root,
            catalog=_catalog(tmp_path),
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )

    outside = _write_config(tmp_path / "outside", _minimal_yaml())
    with pytest.raises(ExperimentConfigError, match="inside config_root"):
        resolve_experiment_yaml(
            outside,
            config_root=config_root,
            catalog=_catalog(tmp_path),
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )


def test_rejects_symlink_or_reparse_config_path(tmp_path: Path) -> None:
    config_root = tmp_path / "configs"
    target = _write_config(config_root, _minimal_yaml())
    link = config_root / "linked.yaml"
    try:
        os.symlink(target, link)
    except OSError as error:
        pytest.skip(f"symlink creation unavailable: {error}")

    with pytest.raises(ExperimentConfigError, match="link or reparse"):
        resolve_experiment_yaml(
            link,
            config_root=config_root,
            catalog=_catalog(tmp_path),
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (_minimal_yaml(strategy_version="9.9.9"), "unknown strategy or version"),
        (_minimal_yaml(rulebook_version="other-v1"), "rulebook version"),
        (
            _minimal_yaml(snapshot_id="00000000-0000-0000-0000-000000000699"),
            "snapshot does not exist",
        ),
        (
            _minimal_yaml(start_date="2023-12-31", end_date="2024-01-31"),
            "outside snapshot daily-bar coverage",
        ),
    ],
)
def test_fails_closed_on_unknown_identity_or_out_of_coverage_range(
    tmp_path: Path, body: str, match: str
) -> None:
    config_root = tmp_path / "configs"
    path = _write_config(config_root, body)

    with pytest.raises(ExperimentConfigError, match=match):
        resolve_experiment_yaml(
            path,
            config_root=config_root,
            catalog=_catalog(tmp_path),
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )


@pytest.mark.parametrize(
    "snapshot_change",
    [
        {"status": SnapshotStatus.DRAFT},
        {"published_at": None},
    ],
)
def test_rejects_snapshot_that_is_not_fully_published(
    tmp_path: Path, snapshot_change: dict[str, object]
) -> None:
    catalog = _catalog(tmp_path)
    catalog.snapshot = replace(catalog.snapshot, **snapshot_change)
    config_root = tmp_path / "configs"
    path = _write_config(config_root, _minimal_yaml())

    with pytest.raises(ExperimentConfigError, match="published"):
        resolve_experiment_yaml(
            path,
            config_root=config_root,
            catalog=catalog,
            strategies={_STOCK_REF: object()},
            rulebook=_rulebook(),
        )


def test_baostock_capability_preflight_fails_before_any_artifact_callback() -> None:
    writes = 0

    def would_write_artifact() -> None:
        nonlocal writes
        writes += 1

    with pytest.raises(ExperimentCapabilityUnavailable) as caught:
        require_provider_capabilities(
            BAOSTOCK_CAPABILITIES,
            (
                "daily_bars",
                "trade_calendar",
                "corporate_actions",
                "pit_total_shares",
                "pit_industry_classification",
            ),
            provider="baostock",
            stage="VALIDATE",
        )
        would_write_artifact()

    assert caught.value.detail.code == "EXPERIMENT_PROVIDER_CAPABILITY_UNAVAILABLE"
    assert caught.value.detail.retryable is False
    assert caught.value.missing == (
        "corporate_actions",
        "pit_total_shares",
        "pit_industry_classification",
    )
    assert writes == 0


def test_complete_offline_profile_passes_the_same_capability_preflight() -> None:
    require_provider_capabilities(
        ProviderCapabilities.complete(),
        (
            "daily_bars",
            "trade_calendar",
            "corporate_actions",
            "pit_total_shares",
            "pit_industry_classification",
        ),
        provider="offline-complete-fixture",
        stage="VALIDATE",
    )


_RUN_ID = "00000000-0000-0000-0000-000000000611"
_CONFIG = {"schema_version": 1, "strategy_id": "stock_multifactor"}
_CONFIG_HASH = hashlib.sha256(
    json.dumps(_CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
_UNIVERSE_HASH = hashlib.sha256(b"runner-universe").hexdigest()


def _record(status: ExperimentStatus = ExperimentStatus.RUNNING) -> ExperimentRecord:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return ExperimentRecord(
        id=_RUN_ID,
        strategy_id="stock_multifactor",
        strategy_version="1.0.0",
        config=_CONFIG,
        config_hash=_CONFIG_HASH,
        snapshot_id=str(_SNAPSHOT_ID),
        snapshot_manifest_hash="5" * 64,
        source_tree_hash="6" * 64,
        git_commit_hash=None,
        lockfile_hash="7" * 64,
        rulebook_version="a-share-v1",
        fingerprint="8" * 64,
        status=status,
        research_mark=ResearchMark.UNREVIEWED,
        created_at=now,
        queued_at=now,
        started_at=now if status is not ExperimentStatus.QUEUED else None,
        completed_at=(
            now
            if status in {ExperimentStatus.SUCCEEDED, ExperimentStatus.FAILED}
            else None
        ),
    )


class _Query:
    def __init__(self, record: ExperimentRecord) -> None:
        self.record = record

    def get(self, experiment_id: str) -> ExperimentDetail:
        assert experiment_id == self.record.id
        return ExperimentDetail(self.record, (), (), (), None, ())


class _Registry:
    def __init__(self, query: _Query, events: list[str]) -> None:
        self.query = query
        self.events = events
        self.transitions: list[
            tuple[ExperimentStatus, ExperimentStatus, ErrorDetail | None]
        ] = []
        self.metrics: dict[str, float] | None = None

    def transition(
        self,
        experiment_id: str,
        expected: ExperimentStatus,
        target: ExperimentStatus,
        reason: ErrorDetail | None = None,
        **kwargs: object,
    ) -> None:
        del kwargs
        assert experiment_id == self.query.record.id
        assert self.query.record.status is expected
        self.transitions.append((expected, target, reason))
        self.query.record = self.query.record.model_copy(update={"status": target})

    def register_success(
        self,
        experiment_id: str,
        publication: ExperimentArtifactPublication,
        metrics: dict[str, float],
        **kwargs: object,
    ) -> None:
        del publication, kwargs
        assert experiment_id == self.query.record.id
        assert self.query.record.status is ExperimentStatus.RUNNING
        self.events.append("REGISTER")
        self.metrics = metrics
        self.query.record = self.query.record.model_copy(
            update={"status": ExperimentStatus.SUCCEEDED}
        )


class _Runtime:
    def __init__(
        self,
        events: list[str],
        artifact_dir: Path,
        *,
        fail_at: ExperimentStage | None = None,
    ) -> None:
        self.events = events
        self.artifact_dir = artifact_dir
        self.fail_at = fail_at

    def _enter(self, stage: ExperimentStage) -> None:
        self.events.append(stage.value)
        if self.fail_at is stage:
            raise RuntimeError(f"failed at {stage.value}")

    def validate(self) -> None:
        self._enter(ExperimentStage.VALIDATE)

    def build_universe(self) -> ExperimentUniverseResult:
        self._enter(ExperimentStage.UNIVERSE)
        return ExperimentUniverseResult(_UNIVERSE_HASH)

    def compute_factors(
        self, universe: ExperimentUniverseResult
    ) -> ExperimentFactorResult:
        assert universe.universe_hash == _UNIVERSE_HASH
        self._enter(ExperimentStage.FACTOR_COMPUTE)
        return ExperimentFactorResult(
            {}, pa.Table.from_pylist([], schema=FACTOR_METRICS_SCHEMA)
        )

    def backtest(
        self,
        universe: ExperimentUniverseResult,
        factors: ExperimentFactorResult,
        progress: Any,
        cancellation: Any,
    ) -> BacktestResult:
        del factors, cancellation
        assert universe.universe_hash == _UNIVERSE_HASH
        self._enter(ExperimentStage.BACKTEST)
        progress.update(1, 2, date(2024, 1, 30))
        progress.update(2, 2, date(2024, 1, 31))
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        manifest = self.artifact_dir / "manifest.json"
        manifest.write_text("{}", encoding="utf-8")
        return BacktestResult(
            UUID(_RUN_ID),
            self.artifact_dir,
            manifest,
            2,
            AccountSnapshot(date(2024, 1, 31), 100_000, (), 0, 100_000),
        )


class _Finalizer:
    def __init__(
        self,
        events: list[str],
        *,
        fail: bool = False,
    ) -> None:
        self.events = events
        self.fail = fail

    def finalize(
        self,
        experiment: ExperimentRecord,
        factors: ExperimentFactorResult,
        backtest: BacktestResult,
    ) -> ExperimentArtifactPublication:
        del factors
        self.events.append(ExperimentStage.ARTIFACT_VERIFY.value)
        if self.fail:
            raise ValueError("artifact validation failed")
        return ExperimentArtifactPublication(
            backtest.artifact_dir,
            backtest.manifest_path,
            {},
            {"experiment_id": experiment.id},
        )


class _Progress:
    def __init__(self) -> None:
        self.calls: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        self.calls.append(progress)


class _NeverCancelled:
    def is_cancelled(self) -> bool:
        return False


class _CancelOnCheck:
    def __init__(self, check: int) -> None:
        self.check = check
        self.calls = 0

    def is_cancelled(self) -> bool:
        self.calls += 1
        return self.calls >= self.check


def _runner(
    tmp_path: Path,
    *,
    fail_at: ExperimentStage | None = None,
    finalizer_fail: bool = False,
) -> tuple[ExperimentRunner, _Query, _Registry, list[str]]:
    events: list[str] = []
    query = _Query(_record())
    registry = _Registry(query, events)
    runtime = _Runtime(
        events,
        tmp_path / ".raw" / f"experiment_id={_RUN_ID}",
        fail_at=fail_at,
    )

    def analytics(path: Path) -> None:
        assert path == runtime.artifact_dir
        events.append(ExperimentStage.ANALYTICS.value)
        if fail_at is ExperimentStage.ANALYTICS:
            raise RuntimeError("failed at ANALYTICS")
        path.joinpath("metrics.json").write_text(
            '{"alpha":1.5,"missing":null,"metrics_version":"1.0.0"}',
            encoding="utf-8",
        )

    runner = ExperimentRunner(
        query=query,
        registry=registry,
        runtime_factory=lambda experiment: runtime,
        analytics_materializer=analytics,
        artifact_finalizer=_Finalizer(events, fail=finalizer_fail),
    )
    return runner, query, registry, events


def test_runner_executes_fixed_stages_and_registers_only_after_artifact_verify(
    tmp_path: Path,
) -> None:
    runner, query, registry, events = _runner(tmp_path)
    progress = _Progress()

    result = runner.run(_RUN_ID, progress, _NeverCancelled())

    assert events == [stage.value for stage in EXPERIMENT_STAGES]
    assert events[-1] == ExperimentStage.REGISTER.value
    assert events.index(ExperimentStage.ARTIFACT_VERIFY.value) < events.index(
        ExperimentStage.REGISTER.value
    )
    assert [item.completed for item in progress.calls] == [
        0,
        1,
        1,
        2,
        2,
        3,
        3,
        3,
        3,
        4,
        4,
        5,
        5,
        6,
        6,
        7,
    ]
    assert all(item.total == 7 for item in progress.calls)
    assert registry.metrics == {"alpha": 1.5}
    assert query.record.status is ExperimentStatus.SUCCEEDED
    assert result.publication.artifact_dir.name == f"experiment_id={_RUN_ID}"


@pytest.mark.parametrize(
    "stage",
    [
        ExperimentStage.VALIDATE,
        ExperimentStage.UNIVERSE,
        ExperimentStage.FACTOR_COMPUTE,
        ExperimentStage.BACKTEST,
        ExperimentStage.ANALYTICS,
    ],
)
def test_runner_stops_at_the_first_failed_stage_and_never_registers(
    tmp_path: Path, stage: ExperimentStage
) -> None:
    runner, query, registry, events = _runner(tmp_path, fail_at=stage)

    with pytest.raises(ExperimentStageFailure) as caught:
        runner.run(_RUN_ID, _Progress(), _NeverCancelled())

    assert caught.value.stage is stage
    assert events[-1] == stage.value
    assert ExperimentStage.REGISTER.value not in events
    assert registry.metrics is None
    assert query.record.status is ExperimentStatus.RUNNING


def test_artifact_verify_failure_cannot_be_skipped_or_registered(
    tmp_path: Path,
) -> None:
    runner, query, registry, events = _runner(tmp_path, finalizer_fail=True)

    with pytest.raises(ExperimentStageFailure) as caught:
        runner.run(_RUN_ID, _Progress(), _NeverCancelled())

    assert caught.value.stage is ExperimentStage.ARTIFACT_VERIFY
    assert events[-1] == ExperimentStage.ARTIFACT_VERIFY.value
    assert ExperimentStage.REGISTER.value not in events
    assert registry.metrics is None
    assert query.record.status is ExperimentStatus.RUNNING


def test_register_failure_is_reported_as_the_last_stage(tmp_path: Path) -> None:
    runner, query, registry, events = _runner(tmp_path)

    def fail_register(*args: object, **kwargs: object) -> None:
        del args, kwargs
        events.append(ExperimentStage.REGISTER.value)
        raise ValueError("registration failed")

    registry.register_success = fail_register  # type: ignore[method-assign]
    with pytest.raises(ExperimentStageFailure) as caught:
        runner.run(_RUN_ID, _Progress(), _NeverCancelled())

    assert caught.value.stage is ExperimentStage.REGISTER
    assert events[-1] == ExperimentStage.REGISTER.value
    assert query.record.status is ExperimentStatus.RUNNING


def test_cancellation_at_artifact_boundary_prevents_publish_and_register(
    tmp_path: Path,
) -> None:
    runner, query, registry, events = _runner(tmp_path)

    with pytest.raises(ExperimentRunCancelled) as caught:
        runner.run(_RUN_ID, _Progress(), _CancelOnCheck(6))

    assert caught.value.stage is ExperimentStage.ARTIFACT_VERIFY
    assert events == [
        ExperimentStage.VALIDATE.value,
        ExperimentStage.UNIVERSE.value,
        ExperimentStage.FACTOR_COMPUTE.value,
        ExperimentStage.BACKTEST.value,
        ExperimentStage.ANALYTICS.value,
    ]
    assert registry.metrics is None
    assert query.record.status is ExperimentStatus.RUNNING


def _task() -> ClaimedTask:
    return ClaimedTask(
        id="task-1",
        attempt_id="attempt-1",
        attempt_no=1,
        experiment_id=_RUN_ID,
        task_type="BACKTEST",
        payload={"experiment_id": _RUN_ID, "config_hash": _CONFIG_HASH},
        priority=0,
        worker_id="worker-1",
        progress=TaskProgress(stage="QUEUED", completed=0, total=7, message=""),
        claimed_at=datetime(2026, 8, 3, tzinfo=UTC),
    )


class _HandlerRunner:
    def __init__(
        self,
        query: _Query,
        *,
        error: BaseException | None = None,
        leave_running: bool = False,
    ) -> None:
        self.query = query
        self.error = error
        self.leave_running = leave_running
        self.run_calls = 0
        self.verify_calls = 0

    def run(self, experiment_id: str, progress: Any, cancellation: Any) -> object:
        del progress, cancellation
        assert experiment_id == _RUN_ID
        self.run_calls += 1
        if self.error is not None:
            raise self.error
        if not self.leave_running:
            self.query.record = self.query.record.model_copy(
                update={"status": ExperimentStatus.SUCCEEDED}
            )
        return object()

    def verify_success(self, experiment_id: str) -> object:
        assert experiment_id == _RUN_ID
        self.verify_calls += 1
        return object()


def test_handler_transitions_queued_to_running_and_returns_only_after_success() -> None:
    query = _Query(_record(ExperimentStatus.QUEUED))
    registry = _Registry(query, [])
    runner = _HandlerRunner(query)
    handler = ExperimentBacktestHandler(registry=registry, query=query, runner=runner)

    outcome = handler.run(_task(), _Progress(), _NeverCancelled())

    assert outcome == TaskOutcome(status=TaskStatus.SUCCEEDED)
    assert registry.transitions == [
        (ExperimentStatus.QUEUED, ExperimentStatus.RUNNING, None)
    ]
    assert runner.run_calls == 1
    assert query.record.status is ExperimentStatus.SUCCEEDED


def test_handler_failure_records_stage_and_transitions_experiment_failed() -> None:
    query = _Query(_record(ExperimentStatus.QUEUED))
    registry = _Registry(query, [])
    detail = ErrorDetail(
        code="FACTOR_FAILED",
        severity=Severity.SEVERE,
        message="factor failure",
        context={"dataset": "daily_bar"},
        remediation="inspect factor inputs",
        retryable=False,
    )
    runner = _HandlerRunner(
        query,
        error=ExperimentStageFailure(
            ExperimentStage.FACTOR_COMPUTE, QuantError(detail)
        ),
    )
    handler = ExperimentBacktestHandler(registry=registry, query=query, runner=runner)

    outcome = handler.run(_task(), _Progress(), _NeverCancelled())

    assert outcome.status is TaskStatus.FAILED
    assert outcome.error == {
        "code": "FACTOR_FAILED",
        "retryable": False,
        "context": {"stage": "FACTOR_COMPUTE"},
    }
    assert registry.transitions[-1][0:2] == (
        ExperimentStatus.RUNNING,
        ExperimentStatus.FAILED,
    )
    reason = registry.transitions[-1][2]
    assert reason is not None and reason.context["stage"] == "FACTOR_COMPUTE"
    assert query.record.status is ExperimentStatus.FAILED


def test_handler_cancellation_transitions_experiment_cancelled() -> None:
    query = _Query(_record(ExperimentStatus.QUEUED))
    registry = _Registry(query, [])
    runner = _HandlerRunner(
        query,
        error=ExperimentRunCancelled(ExperimentStage.BACKTEST),
    )
    handler = ExperimentBacktestHandler(registry=registry, query=query, runner=runner)

    outcome = handler.run(_task(), _Progress(), _NeverCancelled())

    assert outcome == TaskOutcome(status=TaskStatus.CANCELLED)
    assert registry.transitions[-1][0:2] == (
        ExperimentStatus.RUNNING,
        ExperimentStatus.CANCELLED,
    )
    assert query.record.status is ExperimentStatus.CANCELLED


def test_handler_already_succeeded_recovery_only_deep_verifies_artifacts() -> None:
    query = _Query(_record(ExperimentStatus.SUCCEEDED))
    registry = _Registry(query, [])
    runner = _HandlerRunner(query)
    handler = ExperimentBacktestHandler(registry=registry, query=query, runner=runner)

    outcome = handler.run(_task(), _Progress(), _NeverCancelled())

    assert outcome == TaskOutcome(status=TaskStatus.SUCCEEDED)
    assert runner.verify_calls == 1
    assert runner.run_calls == 0
    assert registry.transitions == []


def test_handler_rejects_runner_return_before_register_and_marks_failed() -> None:
    query = _Query(_record(ExperimentStatus.QUEUED))
    registry = _Registry(query, [])
    runner = _HandlerRunner(query, leave_running=True)
    handler = ExperimentBacktestHandler(registry=registry, query=query, runner=runner)

    outcome = handler.run(_task(), _Progress(), _NeverCancelled())

    assert outcome.status is TaskStatus.FAILED
    assert outcome.error is not None
    assert outcome.error["code"] == "EXPERIMENT_REGISTER_INCOMPLETE"
    assert query.record.status is ExperimentStatus.FAILED


def _environment() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source_identity_mode": "source_tree",
        "source_hash": "9" * 64,
        "git_commit": None,
        "source_tree_hash": "6" * 64,
        "working_tree_dirty": False,
        "lockfile_path": "uv.lock",
        "lockfile_hash": "7" * 64,
        "python_version": "3.12.0",
    }


def test_artifact_finalizer_writes_exact_layer_then_calls_existing_publisher(
    tmp_path: Path,
) -> None:
    raw = tmp_path / ".raw" / f"experiment_id={_RUN_ID}"
    raw.mkdir(parents=True)
    manifest = raw / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    backtest = BacktestResult(
        UUID(_RUN_ID),
        raw,
        manifest,
        1,
        AccountSnapshot(date(2024, 1, 31), 100_000, (), 0, 100_000),
    )
    factors = ExperimentFactorResult(
        {}, pa.Table.from_pylist([], schema=FACTOR_METRICS_SCHEMA)
    )
    published = tmp_path / "published" / f"experiment_id={_RUN_ID}"
    calls: list[tuple[Path, Path, UUID, dict[str, object]]] = []

    def publisher(
        staging_dir: Path,
        artifact_root: Path,
        experiment_id: UUID,
        *,
        resolved_config: dict[str, object],
    ) -> ExperimentArtifactPublication:
        calls.append((staging_dir, artifact_root, experiment_id, resolved_config))
        assert (
            yaml.safe_load((staging_dir / "resolved_config.yaml").read_text())
            == _CONFIG
        )
        assert (
            json.loads((staging_dir / "environment.json").read_bytes())
            == _environment()
        )
        assert (
            pq.read_schema(staging_dir / "factor_metrics.parquet")
            == FACTOR_METRICS_SCHEMA
        )
        assert "<html" in (staging_dir / "report.html").read_text(encoding="utf-8")
        assert (staging_dir / "run.log").read_text(encoding="utf-8").splitlines() == [
            stage.value for stage in EXPERIMENT_STAGES[:-1]
        ]
        return ExperimentArtifactPublication(
            published, published / "manifest.json", {}, {"experiment_id": _RUN_ID}
        )

    finalizer = ExperimentArtifactFinalizer(
        artifact_root=tmp_path / "published",
        environment=_environment(),
        publisher=publisher,
    )

    publication = finalizer.finalize(_record(), factors, backtest)

    assert publication.artifact_dir == published
    assert calls == [(raw, tmp_path / "published", UUID(_RUN_ID), _CONFIG)]
