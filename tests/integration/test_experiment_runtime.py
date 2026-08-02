"""Production experiment runtime composition and exact identity acceptance."""

from __future__ import annotations

import hashlib
import importlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
    return catalog.register_dataset_version(
        DatasetVersionSpec(
            dataset=dataset,
            source="offline-complete-fixture",
            partitions=(partition,),
            start_date=date(2023, 12, 25) if dated else None,
            end_date=date(2024, 7, 1) if dated else None,
            created_run_id="offline-complete-fixture",
        )
    )


def _published_offline_snapshot(
    catalog: MetadataRepository, root: Path
) -> SnapshotRecord:
    instrument = "SSE:510300"
    sessions: list[date] = []
    day = date(2023, 12, 25)
    while day <= date(2024, 7, 1):
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
    records = [
        _register_dataset(catalog, root, DatasetKind.INSTRUMENT, instruments),
        _register_dataset(catalog, root, DatasetKind.TRADE_CALENDAR, calendar),
        _register_dataset(catalog, root, DatasetKind.DAILY_BAR, bars),
        _register_dataset(catalog, root, DatasetKind.SECURITY_STATUS, statuses),
        _register_dataset(catalog, root, DatasetKind.CORPORATE_ACTION, []),
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
    assert task.status is TaskStatus.SUCCEEDED, (task.progress, task.error, detail.audit)
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
