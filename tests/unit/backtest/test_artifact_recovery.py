"""中间回测 bundle 的原子发布、深度校验与恢复测试。"""

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest

from quant_research.analytics.materialize import (
    materialize_analytics,
    validate_published_analytics,
)
from quant_research.backtest import (
    AccountSnapshot,
    BacktestArtifactWriter,
    BacktestResult,
    ExecutionBatch,
    ExecutionConfig,
    ExecutionPrice,
    ExecutionReason,
    FeeBreakdown,
    FillResult,
    ManifestContext,
    PositionSnapshot,
    validate_backtest_artifacts,
)
from quant_research.backtest.artifacts import validate_experiment_artifacts
from quant_research.data.contracts import canonical_json_bytes
from quant_research.domain.identifiers import InstrumentId
from quant_research.experiments.models import (
    ExperimentRecord,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.experiments.runner import ExperimentArtifactFinalizer
from quant_research.logging import LogContext, TaskLogManager
from quant_research.portfolio import (
    OrderIntent,
    OrderSide,
    TargetPortfolio,
    TargetPosition,
)

_EXPERIMENT_ID = UUID("00000000-0000-0000-0000-000000000091")
_TRADE_DATE = date(2026, 7, 30)
_INSTRUMENT = InstrumentId.parse("600001.SH")


def _context(
    *, data_hash: str = "a" * 64, industry_input: dict[str, object] | None = None
) -> ManifestContext:
    return ManifestContext(
        experiment_id=_EXPERIMENT_ID,
        data_hash=data_hash,
        strategy_id="recovery-test",
        start_date=_TRADE_DATE,
        end_date=_TRADE_DATE,
        benchmark=InstrumentId.parse("000001.SH"),
        initial_cash_fen=1_000_000,
        rulebook_hash="b" * 64,
        execution_config=ExecutionConfig(ExecutionPrice.CLOSE, 0.0, 1.0),
        industry_input=industry_input,  # type: ignore[arg-type]
    )


def _publish(root: Path) -> Path:
    writer = BacktestArtifactWriter(root, _EXPERIMENT_ID)
    writer.append_target(
        TargetPortfolio(
            date(2026, 7, 29),
            _TRADE_DATE,
            (TargetPosition(_INSTRUMENT, 0.5, 1.0, "SELECTED"),),
            0.5,
        )
    )
    intent = OrderIntent(_INSTRUMENT, OrderSide.BUY, 500, "RECOVERY")
    writer.append_execution(
        ExecutionBatch(
            _TRADE_DATE,
            (
                FillResult(
                    intent,
                    _TRADE_DATE,
                    500,
                    10.0,
                    500_000,
                    500,
                    0,
                    10.0,
                    500_000,
                    1,
                    FeeBreakdown(0, 0, 0, 0),
                    ExecutionReason.FILLED,
                ),
            ),
            500_000,
        )
    )
    writer.append_snapshot(
        AccountSnapshot(
            trade_date=_TRADE_DATE,
            cash_fen=500_000,
            positions=(PositionSnapshot(_INSTRUMENT, 500, 500, 400_000, 500_000),),
            total_market_value_fen=500_000,
            nav_fen=1_000_000,
        ),
        benchmark_close=100.0,
    )
    writer.close()
    writer.validate((_TRADE_DATE,), _context())
    return writer.publish().parent


def test_published_backtest_bundle_contains_manifest_and_recovers_final_snapshot(
    tmp_path: Path,
) -> None:
    artifact_dir = _publish(tmp_path)

    recovery = validate_backtest_artifacts(artifact_dir, context=_context())

    assert recovery.manifest_path == artifact_dir / "manifest.json"
    assert recovery.sessions_completed == 1
    assert recovery.final_snapshot.trade_date == _TRADE_DATE
    assert recovery.final_snapshot.nav_fen == 1_000_000
    assert recovery.final_snapshot.positions[0].instrument_id == _INSTRUMENT
    assert "industry" not in pl.read_parquet(artifact_dir / "targets.parquet").columns
    fills = pl.read_parquet(artifact_dir / "fills.parquet")
    assert fills["reference_price"].to_list() == [10.0]
    assert fills["requested_reference_value_fen"].to_list() == [500_000]
    manifest = json.loads((artifact_dir / "manifest.json").read_text(encoding="utf-8"))
    assert "industry_classification_mode" not in manifest

    materialize_analytics(artifact_dir)
    published_manifest = (artifact_dir / "manifest.json").read_bytes()
    materialize_analytics(artifact_dir)
    assert (artifact_dir / "manifest.json").read_bytes() == published_manifest
    validate_published_analytics(artifact_dir, expected_experiment_id=_EXPERIMENT_ID)
    analytics_manifest = json.loads(published_manifest)
    assert "execution_summary.parquet" in analytics_manifest["analytics"]["artifacts"]
    assert {
        "active_nav",
        "active_running_peak_nav",
        "active_drawdown",
    }.issubset(pl.read_parquet(artifact_dir / "drawdown.parquet").columns)
    quality = json.loads(
        (artifact_dir / "quality_disclosure.json").read_text(encoding="utf-8")
    )
    assert quality["risk_free_rate_annual"] == 0.0
    assert "industry_classification_mode" not in quality
    assert "industry" not in quality["unavailable_dimensions"]
    assert "INDUSTRY_CLASSIFICATION_NOT_PIT" not in quality["warnings"]
    assert "CORPORATE_ACTIONS_NOT_APPLIED" in quality["warnings"]


def test_unavailable_run_log_placeholder_passes_final_manifest_validation(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    staging_root = artifact_root / ".experiment-staging"
    artifact_dir = _publish(staging_root)
    recovery = validate_backtest_artifacts(artifact_dir, context=_context())
    materialize_analytics(artifact_dir)
    config = {"strategy_id": "recovery-test"}
    experiment_id = str(_EXPERIMENT_ID)
    experiment = ExperimentRecord(
        id=experiment_id,
        strategy_id="recovery-test",
        config=config,
        config_hash=hashlib.sha256(canonical_json_bytes(config)).hexdigest(),
        data_hash="a" * 64,
        source_tree_hash=None,
        git_commit_hash="b" * 40,
        lockfile_hash="c" * 64,
        rulebook_hash="b" * 64,
        fingerprint="e" * 64,
        status=ExperimentStatus.RUNNING,
        research_mark=ResearchMark.UNREVIEWED,
        created_at=datetime(2026, 8, 15, tzinfo=UTC),
        queued_at=datetime(2026, 8, 15, tzinfo=UTC),
        started_at=datetime(2026, 8, 15, tzinfo=UTC),
        completed_at=None,
    )
    manager = TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=artifact_root,
    )
    context = LogContext(
        experiment_id=experiment_id,
        task_id="00000000-0000-0000-0000-000000000092",
        attempt_id="00000000-0000-0000-0000-000000000093",
        worker_id="worker-7",
    )
    git_commit = "b" * 40
    source_hash = hashlib.sha256(
        canonical_json_bytes(
            {
                "mode": "git_commit",
                "git_commit": git_commit,
                "source_tree_hash": None,
            }
        )
    ).hexdigest()

    def materialize_log(_experiment_id: str, staging_dir: Path) -> Path:
        return manager.materialize_unavailable(
            context,
            staging_dir,
            stage="ARTIFACT_VERIFY",
        )

    finalizer = ExperimentArtifactFinalizer(
        artifact_root=artifact_root,
        environment={
            "source_identity_mode": "git_commit",
            "source_hash": source_hash,
            "git_commit": git_commit,
            "source_tree_hash": None,
            "working_tree_dirty": False,
            "lockfile_path": "uv.lock",
            "lockfile_hash": "c" * 64,
            "python_version": "3.12.0",
        },
        task_log_materializer=materialize_log,
    )
    backtest = BacktestResult(
        experiment_id=_EXPERIMENT_ID,
        artifact_dir=artifact_dir,
        manifest_path=recovery.manifest_path,
        sessions_completed=recovery.sessions_completed,
        final_snapshot=recovery.final_snapshot,
    )

    publication = finalizer.finalize(experiment, backtest)
    validated = validate_experiment_artifacts(
        publication.artifact_dir,
        resolved_config=config,
    )

    run_log = validated.artifact_dir / "run.log"
    record = json.loads(run_log.read_text(encoding="utf-8"))
    assert record["event"] == "task.log_unavailable"
    assert validated.entries["run.log"].sha256 == hashlib.sha256(
        run_log.read_bytes()
    ).hexdigest()


def test_recovery_rejects_bundle_from_different_experiment_identity(
    tmp_path: Path,
) -> None:
    artifact_dir = _publish(tmp_path)

    with pytest.raises(ValueError, match="identity"):
        validate_backtest_artifacts(artifact_dir, context=_context(data_hash="c" * 64))


def test_manifest_records_explicit_reconstructed_industry_input() -> None:
    industry_input = {
        "dataset": "industry_classification",
        "taxonomy": "证监会行业分类",
        "unclassified_policy": "EXCLUDE",
        "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
    }

    manifest = _context(industry_input=industry_input).build_manifest({}, 1)

    assert manifest["industry_input"] == industry_input


def test_recovery_rejects_removed_industry_manifest_field(tmp_path: Path) -> None:
    artifact_dir = _publish(tmp_path)
    manifest_path = artifact_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["industry_classification_mode"] = "LATEST_NON_PIT"
    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="manifest identity"):
        validate_backtest_artifacts(artifact_dir, context=_context())


def test_recovery_rejects_removed_industry_target_column(tmp_path: Path) -> None:
    artifact_dir = _publish(tmp_path)
    targets_path = artifact_dir / "targets.parquet"
    pl.read_parquet(targets_path).with_columns(
        pl.lit("BANK").alias("industry")
    ).write_parquet(targets_path)

    with pytest.raises(ValueError, match="schema mismatch for targets.parquet"):
        validate_backtest_artifacts(artifact_dir, context=_context())


def test_recovery_preserves_and_rejects_tampered_bundle(tmp_path: Path) -> None:
    artifact_dir = _publish(tmp_path)
    nav_path = artifact_dir / "nav.parquet"
    original_size = nav_path.stat().st_size
    with nav_path.open("ab") as stream:
        stream.write(b"tampered")

    with pytest.raises(ValueError):
        validate_backtest_artifacts(artifact_dir, context=_context())

    assert artifact_dir.is_dir()
    assert nav_path.stat().st_size == original_size + len(b"tampered")
