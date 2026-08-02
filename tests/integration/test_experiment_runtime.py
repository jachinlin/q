"""Production experiment runtime composition and exact identity acceptance."""

from __future__ import annotations

import hashlib
import importlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from quant_core.backtest.rulebook import AShareRuleBook
from quant_core.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_core.data.quality.models import QualityRunSpec
from quant_core.data.repository import SnapshotResearchRepository
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.data.snapshots import SnapshotPublisher
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import InstrumentId, QualityRunId, SnapshotId
from quant_core.errors import QuantError
from quant_core.experiments import build_default_experiment_worker
from quant_core.experiments.config import ExperimentCapabilityUnavailable
from quant_core.experiments.fingerprint import (
    ExperimentFingerprintInput,
    compute_fingerprint,
)
from quant_core.experiments.models import (
    ExperimentRecord,
    ExperimentSpec,
    ExperimentStatus,
    ResearchMark,
)
from quant_core.experiments.query import ExperimentQuery
from quant_core.experiments.registry import ExperimentRegistry
from quant_core.factors import FactorRegistry
from quant_core.factors.builtin import register_etf_factors, register_stock_factors
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    DatasetPartitionSpec,
    DatasetVersionSpec,
    MetadataRepository,
    SnapshotRecord,
)
from quant_core.strategies import EtfRotationConfig, MultifactorConfig
from quant_core.tasks.models import TaskStatus
from quant_core.tasks.queue import TaskQueue
from quant_core.tasks.worker import Worker


class _UnusedResearchData:
    """Registration-only dependency; a real compute would fail immediately."""

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"factor registration unexpectedly read {name}")


class _OneSnapshotCatalog:
    def __init__(self, snapshot: SnapshotRecord) -> None:
        self.snapshot = snapshot

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord:
        if identifier != self.snapshot.id:
            raise KeyError(identifier)
        return self.snapshot


def _audit(day: date) -> dict[str, object]:
    available = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    return {
        "source": "offline-complete-fixture",
        "source_version": "fixture-v1",
        "available_at": available,
        "availability_source": "fixture",
        "pit_usable": True,
        "ingested_at": datetime(2024, 7, 1, tzinfo=UTC),
    }


def _register_dataset(
    catalog: MetadataRepository,
    root: Path,
    dataset: DatasetKind,
    rows: list[dict[str, object]],
    *,
    coverage: tuple[date, date] | None = None,
) -> object:
    definition = CANONICAL_SCHEMAS[dataset]
    path = root / "curated" / f"{dataset.value}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=definition.columns, strict=False).write_parquet(path)
    table = pq.read_table(path)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    partition = DatasetPartitionSpec(
        content_hash=hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest(),
        path=path,
        schema_fingerprint=hashlib.sha256(
            table.schema.serialize().to_pybytes()
        ).hexdigest(),
        row_count=table.num_rows,
    )
    dated = dataset in {
        DatasetKind.TRADE_CALENDAR,
        DatasetKind.DAILY_BAR,
        DatasetKind.SECURITY_STATUS,
    }
    start_date, end_date = coverage or (date(2023, 12, 25), date(2024, 7, 1))
    return catalog.register_dataset_version(
        DatasetVersionSpec(
            dataset=dataset,
            source="offline-complete-fixture",
            partitions=(partition,),
            start_date=start_date if dated else None,
            end_date=end_date if dated else None,
            created_run_id="offline-complete-fixture",
        )
    )


def _published_offline_snapshot(
    catalog: MetadataRepository,
    root: Path,
    *,
    excluded: frozenset[DatasetKind] = frozenset(),
    data_end: date = date(2024, 7, 1),
    bars_end: date | None = None,
    coverage: dict[DatasetKind, tuple[date, date]] | None = None,
) -> SnapshotRecord:
    instrument = "SSE:510300"
    sessions: list[date] = []
    day = date(2023, 12, 25)
    while day <= data_end:
        if day.weekday() < 5:
            sessions.append(day)
        day += timedelta(days=1)
    instruments = [
        {
            "instrument_id": instrument,
            "exchange": "SSE",
            "board": "MAIN",
            "name": "Synthetic ETF",
            "instrument_type": "ETF",
            "listing_status": "LISTED",
            "list_date": date(2012, 1, 1),
            "delist_date": None,
            **_audit(date(2023, 12, 25)),
        }
    ]
    calendar = [
        {"trade_date": session, "is_trading_day": True, **_audit(session)}
        for session in sessions
    ]
    bars: list[dict[str, object]] = []
    statuses: list[dict[str, object]] = []
    previous = 3.0
    for ordinal, session in enumerate(sessions):
        close = 3.0 + ordinal * 0.01
        if bars_end is None or session <= bars_end:
            bars.append(
                {
                    "instrument_id": instrument,
                    "trade_date": session,
                    "open": close,
                    "high": close + 0.01,
                    "low": close - 0.01,
                    "close": close,
                    "preclose": previous,
                    "volume": 1_000_000,
                    "amount": close * 1_000_000,
                    "adjustment_flag": "none",
                    "turnover": 1.0,
                    "pct_change": (close / previous - 1.0) * 100.0,
                    "pe_ttm": 10.0,
                    "pb_mrq": 1.0,
                    "ps_ttm": 2.0,
                    "pcf_ncf_ttm": 3.0,
                    **_audit(session),
                }
            )
        statuses.append(
            {
                "instrument_id": instrument,
                "trade_date": session,
                "is_listed": True,
                "is_suspended": False,
                "is_risk_warning": False,
                "board": "MAIN",
                "price_limit_rule_id": "main",
                "tradable_reason": "normal",
                **_audit(session),
            }
        )
        previous = close
    rows_by_dataset = {
        DatasetKind.INSTRUMENT: instruments,
        DatasetKind.TRADE_CALENDAR: calendar,
        DatasetKind.DAILY_BAR: bars,
        DatasetKind.SECURITY_STATUS: statuses,
        DatasetKind.CORPORATE_ACTION: [],
    }
    coverage_by_dataset = coverage or {}
    records = [
        _register_dataset(
            catalog,
            root,
            dataset,
            rows,
            coverage=coverage_by_dataset.get(dataset),
        )
        for dataset, rows in rows_by_dataset.items()
        if dataset not in excluded
    ]
    versions = {record.dataset.value: record.id for record in records}  # type: ignore[attr-defined]
    quality = catalog.register_quality_run(
        QualityRunSpec(
            dataset_versions=versions,
            started_at=datetime(2024, 7, 1, tzinfo=UTC),
            completed_at=datetime(2024, 7, 1, 0, 1, tzinfo=UTC),
            issues=(),
        )
    )
    snapshot_id = SnapshotPublisher(
        catalog,
        root / "snapshots",
        clock=lambda: datetime(2024, 7, 1, tzinfo=UTC),
    ).publish(versions, quality.id)
    return catalog.get_snapshot(snapshot_id)


def _runtime_for_snapshot(
    catalog: MetadataRepository,
    root: Path,
    snapshot: SnapshotRecord,
    *,
    start: date = date(2024, 6, 27),
    end: date = date(2024, 6, 28),
) -> tuple[object, Path, Path]:
    config = _resolved_etf_config(snapshot.id)
    config["start_date"] = start.isoformat()
    config["end_date"] = end.isoformat()
    now = datetime(2024, 7, 1, tzinfo=UTC)
    experiment = ExperimentRecord(
        id="00000000-0000-0000-0000-000000000771",
        strategy_id="etf_rotation",
        strategy_version="1.0.0",
        config=config,
        config_hash=hashlib.sha256(canonical_json_bytes(config)).hexdigest(),
        snapshot_id=str(snapshot.id),
        snapshot_manifest_hash=snapshot.manifest_hash,
        source_tree_hash="3" * 64,
        git_commit_hash=None,
        lockfile_hash="4" * 64,
        rulebook_version="a-share-v1",
        fingerprint="5" * 64,
        status=ExperimentStatus.RUNNING,
        research_mark=ResearchMark.UNREVIEWED,
        created_at=now,
        queued_at=now,
        started_at=now,
        completed_at=None,
    )
    feature_root = root / "features"
    artifact_root = root / "artifacts"
    module = importlib.import_module("quant_core.experiments.runtime")
    runtime = module.ExperimentRuntimeFactory(
        catalog=catalog,
        repository=SnapshotResearchRepository(catalog),
        capabilities=ProviderCapabilities.complete(),
        provider="offline-complete-fixture",
        feature_root=feature_root,
        artifact_root=artifact_root,
        snapshot_root=root / "snapshots",
        rulebook=AShareRuleBook.load(Path("configs/rules/a_share_v1.yaml")),
        enrichment=None,
    )(experiment)
    return runtime, feature_root, artifact_root


def _assert_validate_rejects_without_writes(
    runtime: object,
    feature_root: Path,
    artifact_root: Path,
    *,
    cause_code: str,
    dataset: DatasetKind | None = None,
) -> None:
    with pytest.raises(QuantError) as caught:
        runtime.validate()  # type: ignore[attr-defined]

    assert caught.value.detail.code == "EXPERIMENT_SNAPSHOT_INVALID"
    assert caught.value.detail.context["stage"] == "VALIDATE"
    assert caught.value.detail.context["cause_code"] == cause_code
    if dataset is not None:
        assert caught.value.detail.context["dataset"] == dataset.value
    assert not feature_root.exists()
    assert not artifact_root.exists()


def test_validate_rejects_tampered_snapshot_manifest_bytes_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(catalog, tmp_path)
    snapshot.manifest_path.write_bytes(b'{"tampered":true}')
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAP_MANIFEST_MISMATCH",
    )


def test_validate_rejects_nonexact_snapshot_manifest_schema_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(catalog, tmp_path)
    manifest = json.loads(snapshot.manifest_path.read_bytes())
    manifest["unexpected"] = "field"
    payload = canonical_json_bytes(manifest)
    manifest_hash = hashlib.sha256(payload).hexdigest()
    snapshot.manifest_path.write_bytes(payload)
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER snapshot_published_no_update"))
        connection.execute(
            text("UPDATE snapshot SET manifest_hash = :manifest_hash WHERE id = :id"),
            {"manifest_hash": manifest_hash, "id": str(snapshot.id)},
        )
    rebound = catalog.get_snapshot(snapshot.id)
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, rebound
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAP_MANIFEST_MISMATCH",
    )


def test_validate_rejects_manifest_catalog_dataset_mapping_mismatch_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(catalog, tmp_path)
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER snapshot_dataset_published_no_update"))
        connection.execute(text("DROP TRIGGER quality_run_dataset_completed_no_update"))
        connection.execute(
            text(
                "UPDATE snapshot_dataset SET dataset_version_id = :replacement "
                "WHERE snapshot_id = :snapshot_id AND dataset = :dataset"
            ),
            {
                "replacement": str(
                    snapshot.dataset_versions[DatasetKind.CORPORATE_ACTION.value]
                ),
                "snapshot_id": str(snapshot.id),
                "dataset": DatasetKind.SECURITY_STATUS.value,
            },
        )
        connection.execute(
            text(
                "UPDATE quality_run_dataset SET dataset_version_id = :replacement "
                "WHERE quality_run_id = :quality_run_id AND dataset = :dataset"
            ),
            {
                "replacement": str(
                    snapshot.dataset_versions[DatasetKind.CORPORATE_ACTION.value]
                ),
                "quality_run_id": str(snapshot.quality_run_id),
                "dataset": DatasetKind.SECURITY_STATUS.value,
            },
        )
    rebound = catalog.get_snapshot(snapshot.id)
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, rebound
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAP_MANIFEST_MISMATCH",
    )


def test_validate_rejects_snapshot_quality_scope_mismatch_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(catalog, tmp_path)
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER quality_run_dataset_completed_no_delete"))
        connection.execute(
            text(
                "DELETE FROM quality_run_dataset "
                "WHERE quality_run_id = :quality_run_id AND dataset = :dataset"
            ),
            {
                "quality_run_id": str(snapshot.quality_run_id),
                "dataset": DatasetKind.CORPORATE_ACTION.value,
            },
        )
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAP_QUALITY_SCOPE_MISMATCH",
    )


@pytest.mark.parametrize(
    "missing",
    [
        DatasetKind.DAILY_BAR,
        DatasetKind.TRADE_CALENDAR,
        DatasetKind.INSTRUMENT,
    ],
)
def test_validate_rejects_each_foundation_dataset_without_writes(
    tmp_path: Path, missing: DatasetKind
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(
        catalog, tmp_path, excluded=frozenset({missing})
    )
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAPSHOT_DATASET_MISSING",
        dataset=missing,
    )


def test_validate_rejects_capability_derived_corporate_action_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(
        catalog,
        tmp_path,
        excluded=frozenset({DatasetKind.CORPORATE_ACTION}),
    )
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAPSHOT_DATASET_MISSING",
        dataset=DatasetKind.CORPORATE_ACTION,
    )


@pytest.mark.parametrize(
    ("dataset", "coverage"),
    [
        (DatasetKind.DAILY_BAR, (date(2024, 6, 28), date(2024, 7, 1))),
        (DatasetKind.TRADE_CALENDAR, (date(2023, 12, 25), date(2024, 6, 27))),
    ],
)
def test_validate_rejects_experiment_range_outside_dataset_coverage_without_writes(
    tmp_path: Path,
    dataset: DatasetKind,
    coverage: tuple[date, date],
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(
        catalog, tmp_path, coverage={dataset: coverage}
    )
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAPSHOT_DATE_COVERAGE_INVALID",
        dataset=dataset,
    )


def test_validate_rejects_missing_actual_benchmark_bars_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(
        catalog, tmp_path, bars_end=date(2024, 6, 27)
    )
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAPSHOT_DATE_COVERAGE_INVALID",
        dataset=DatasetKind.DAILY_BAR,
    )


def test_validate_rejects_missing_next_trading_session_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(
        catalog, tmp_path, data_end=date(2024, 6, 28)
    )
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAPSHOT_NEXT_SESSION_MISSING",
        dataset=DatasetKind.TRADE_CALENDAR,
    )


def test_validate_rejects_tampered_required_dataset_partition_without_writes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(catalog, tmp_path)
    daily = catalog.get_dataset_version(
        snapshot.dataset_versions[DatasetKind.DAILY_BAR.value]
    )
    daily.partitions[0].path.write_bytes(b"tampered parquet bytes")
    runtime, feature_root, artifact_root = _runtime_for_snapshot(
        catalog, tmp_path, snapshot
    )

    _assert_validate_rejects_without_writes(
        runtime,
        feature_root,
        artifact_root,
        cause_code="SNAPSHOT_DATASET_INVALID",
        dataset=DatasetKind.DAILY_BAR,
    )


def test_default_worker_is_shipped_as_a_local_concrete_composition_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module("quant_core.experiments.runtime")
    monkeypatch.setenv("QUANT_DATA_ROOT", str(tmp_path / "runtime-data"))

    worker = build_default_experiment_worker(worker_id="runtime-test")

    assert isinstance(worker, Worker)
    assert callable(module.ExperimentRuntimeFactory)


def test_shipped_strategy_market_refs_exactly_resolve_real_factor_registry() -> None:
    instrument = InstrumentId.parse("SSE:600000")
    provider = _UnusedResearchData()
    registry = FactorRegistry()
    register_stock_factors(
        registry,
        provider,  # type: ignore[arg-type]
        provider,  # type: ignore[arg-type]
        (instrument,),
        price_service=provider,  # type: ignore[arg-type]
    )
    register_etf_factors(
        registry,
        provider,  # type: ignore[arg-type]
        (instrument,),
    )
    etf_refs = {
        "return_20d_v1@2.1.0": 0.2,
        "return_60d_v1@2.1.0": 0.3,
        "return_120d_v1@2.1.0": 0.5,
    }
    etf = EtfRotationConfig.from_mapping(
        {
            "etf_pool": [instrument.canonical()],
            "return_factor_weights": etf_refs,
            "trend_factor_ref": "trend_120d_v1@2.1.0",
            "volatility_factor_ref": "volatility_60d_v1@2.1.0",
            "volatility_penalty": 0.5,
            "top_n": 1,
        }
    )
    stock_refs = {
        "earnings_yield_ttm_v1@1.0.0": {"category": "VALUE", "direction": 1},
        "book_to_price_mrq_v1@1.0.0": {"category": "VALUE", "direction": 1},
        "roe_avg_pit_v1@1.0.0": {"category": "QUALITY", "direction": 1},
        "cfo_to_np_pit_v1@1.0.0": {"category": "QUALITY", "direction": 1},
        "momentum_120_20_v1@2.1.0": {"category": "MOMENTUM", "direction": 1},
        "volatility_60d_v1@2.1.0": {"category": "RISK", "direction": -1},
        "downside_volatility_60d_v1@2.1.0": {
            "category": "RISK",
            "direction": -1,
        },
        "max_drawdown_120d_v1@2.1.0": {"category": "RISK", "direction": -1},
    }
    stock = MultifactorConfig.from_mapping(
        {
            "factor_definitions": stock_refs,
            "constraints": {
                "max_position_weight": 0.1,
                "max_industry_weight": 0.3,
                "min_positions": 1,
                "max_positions": 1,
                "min_adv_amount": 0.0,
                "max_turnover": 1.0,
            },
        }
    )

    expected = {
        *etf.return_factor_weights,
        etf.trend_factor_ref,
        etf.volatility_factor_ref,
        *stock.factor_definitions,
    }
    assert {registry.resolve(reference) for reference in expected} == expected


def test_complete_offline_snapshot_runs_public_worker_chain_to_registration(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    catalog = MetadataRepository(engine)
    snapshot = _published_offline_snapshot(catalog, tmp_path)
    config = _resolved_etf_config(snapshot.id)
    config_hash = hashlib.sha256(canonical_json_bytes(config)).hexdigest()
    source_hash = hashlib.sha256(
        canonical_json_bytes(
            {
                "git_commit": None,
                "mode": "source_tree",
                "schema": "quant.source-identity.v1",
                "source_tree_hash": "3" * 64,
            }
        )
    ).hexdigest()
    fingerprint = compute_fingerprint(
        ExperimentFingerprintInput(
            strategy_id="etf_rotation",
            strategy_version="1.0.0",
            resolved_config=config,
            snapshot_manifest_hash=snapshot.manifest_hash,
            source_hash=source_hash,
            lockfile_hash="4" * 64,
            rulebook_version="a-share-v1",
        )
    )
    spec = ExperimentSpec(
        strategy_id="etf_rotation",
        strategy_version="1.0.0",
        config=config,
        config_hash=config_hash,
        snapshot_id=str(snapshot.id),
        snapshot_manifest_hash=snapshot.manifest_hash,
        source_tree_hash="3" * 64,
        git_commit_hash=None,
        lockfile_hash="4" * 64,
        rulebook_version="a-share-v1",
        fingerprint=fingerprint,
        created_at=datetime(2024, 7, 1, tzinfo=UTC),
    )
    experiment_id = ExperimentRegistry(engine).create(spec, spec.fingerprint)
    queue = TaskQueue(engine)
    task_id = queue.submit_backtest(experiment_id, config_hash)
    module = importlib.import_module("quant_core.experiments.runtime")
    runtime_factory = module.ExperimentRuntimeFactory(
        catalog=catalog,
        repository=SnapshotResearchRepository(catalog),
        capabilities=ProviderCapabilities.complete(),
        provider="offline-complete-fixture",
        feature_root=tmp_path / "features",
        artifact_root=tmp_path / "artifacts",
        snapshot_root=tmp_path / "snapshots",
        rulebook=AShareRuleBook.load(Path("configs/rules/a_share_v1.yaml")),
        enrichment=None,
    )
    worker = module.build_experiment_worker(
        engine=engine,
        worker_id="offline-complete-worker",
        runtime_factory=runtime_factory,
        artifact_root=tmp_path / "artifacts",
        environment={
            "schema_version": 1,
            "source_identity_mode": "source_tree",
            "source_hash": source_hash,
            "git_commit": None,
            "source_tree_hash": "3" * 64,
            "working_tree_dirty": False,
            "lockfile_path": "uv.lock",
            "lockfile_hash": "4" * 64,
            "python_version": "3.12.0",
        },
    )

    assert worker.run_once()
    detail = ExperimentQuery(engine).get(experiment_id)
    task = queue.get(task_id)
    assert task.status is TaskStatus.SUCCEEDED, (
        task.progress,
        task.error,
        detail.audit,
    )
    assert detail.record.status is ExperimentStatus.SUCCEEDED
    assert {artifact.name for artifact in detail.artifacts} >= {
        "manifest.json",
        "metrics.json",
        "nav.parquet",
        "factor_metrics.parquet",
    }


def test_baostock_runtime_fails_validate_before_cache_or_artifact_creation(
    tmp_path: Path,
) -> None:
    module = importlib.import_module("quant_core.experiments.runtime")
    snapshot_id = SnapshotId.parse("00000000-0000-0000-0000-000000000761")
    now = datetime(2026, 8, 3, tzinfo=UTC)
    snapshot = SnapshotRecord(
        id=snapshot_id,
        publication_fingerprint="1" * 64,
        as_of=now,
        status=SnapshotStatus.PUBLISHED,
        manifest_path=tmp_path / "snapshot.json",
        manifest_hash="2" * 64,
        quality_run_id=QualityRunId.new(),
        dataset_versions={},
        created_at=now,
        published_at=now,
    )
    config = _resolved_etf_config(snapshot_id)
    experiment = ExperimentRecord(
        id="00000000-0000-0000-0000-000000000762",
        strategy_id="etf_rotation",
        strategy_version="1.0.0",
        config=config,
        config_hash=hashlib.sha256(canonical_json_bytes(config)).hexdigest(),
        snapshot_id=str(snapshot_id),
        snapshot_manifest_hash=snapshot.manifest_hash,
        source_tree_hash="3" * 64,
        git_commit_hash=None,
        lockfile_hash="4" * 64,
        rulebook_version="a-share-v1",
        fingerprint="5" * 64,
        status=ExperimentStatus.RUNNING,
        research_mark=ResearchMark.UNREVIEWED,
        created_at=now,
        queued_at=now,
        started_at=now,
        completed_at=None,
    )
    feature_root = tmp_path / "features"
    artifact_root = tmp_path / "artifacts"
    runtime = module.ExperimentRuntimeFactory(
        catalog=_OneSnapshotCatalog(snapshot),
        repository=_UnusedResearchData(),
        capabilities=BAOSTOCK_CAPABILITIES,
        provider="baostock",
        feature_root=feature_root,
        artifact_root=artifact_root,
        snapshot_root=tmp_path,
        rulebook=AShareRuleBook.load(Path("configs/rules/a_share_v1.yaml")),
        enrichment=None,
    )(experiment)

    with pytest.raises(ExperimentCapabilityUnavailable) as caught:
        runtime.validate()

    assert caught.value.missing == ("corporate_actions",)
    assert not feature_root.exists()
    assert not artifact_root.exists()


def _resolved_etf_config(snapshot_id: SnapshotId) -> dict[str, JsonValue]:
    return cast(
        dict[str, JsonValue],
        {
            "benchmark": "SSE:510300",
            "end_date": "2024-06-28",
            "execution": {
                "max_volume_participation": 1.0,
                "reference_price": "CLOSE",
                "slippage_bps": 0.0,
            },
            "initial_cash_fen": 200_000,
            "rulebook_version": "a-share-v1",
            "schema_version": 1,
            "snapshot_id": str(snapshot_id),
            "start_date": "2024-06-27",
            "strategy_config": {
                "etf_pool": ["SSE:510300"],
                "return_factor_weights": {
                    "return_20d_v1@2.1.0": 0.2,
                    "return_60d_v1@2.1.0": 0.3,
                    "return_120d_v1@2.1.0": 0.5,
                },
                "trend_factor_ref": "trend_120d_v1@2.1.0",
                "volatility_factor_ref": "volatility_60d_v1@2.1.0",
                "volatility_penalty": 0.5,
                "top_n": 1,
            },
            "strategy_id": "etf_rotation",
            "strategy_version": "1.0.0",
            "universe": {
                "allowed_boards": ["MAIN", "CHINEXT", "STAR"],
                "exclude_st": True,
                "exclude_suspended": True,
                "min_avg_amount_20d": None,
                "min_listing_days": 120,
            },
        },
    )
