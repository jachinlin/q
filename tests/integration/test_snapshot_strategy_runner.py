"""Offline synthetic snapshot-to-strategy integration evidence.

This fixture proves the software composition with a complete declared provider. It
is intentionally not BaoStock production acceptance evidence.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from quant_core.backtest.engine import BacktestRequest
from quant_core.backtest.models import ExecutionConfig, ExecutionPrice
from quant_core.backtest.rulebook import AShareRuleBook
from quant_core.data.contracts import ProviderCapabilities
from quant_core.data.repository import SnapshotResearchRepository
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    InstrumentId,
    QualityRunId,
    SnapshotId,
)
from quant_core.experiments.adapters import SnapshotStrategyRunner
from quant_core.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    FactorArtifact,
    factor_table_content_hash,
)
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetVersionRecord,
    SnapshotRecord,
)
from quant_core.portfolio import PortfolioConstructor, RebalancePlanner
from quant_core.strategies.etf_rotation import EtfRotationConfig, EtfRotationStrategy
from quant_core.universe.rules import UniverseRules

_SNAPSHOT = SnapshotId(UUID("00000000-0000-0000-0000-000000000071"))
_EXPERIMENT = UUID("00000000-0000-0000-0000-000000000072")
_BENCHMARK = InstrumentId.parse("SSE:000300")
_ETF = InstrumentId.parse("SSE:510300")
_SIGNAL = date(2024, 1, 31)
_EXECUTE = date(2024, 2, 2)
_POST_END = date(2024, 2, 5)
_UNIVERSE_HASH = hashlib.sha256(b"offline-synthetic-etf-pool").hexdigest()
_FACTOR_VALUES = {
    "return_20d_v1@1.0.0": 0.03,
    "return_60d_v1@1.0.0": 0.08,
    "return_120d_v1@1.0.0": 0.12,
    "trend_120d_v1@1.0.0": 1.0,
    "volatility_60d_v1@1.0.0": 0.2,
}


class _Catalog:
    def __init__(
        self,
        snapshot: SnapshotRecord,
        versions: dict[DatasetVersionId, DatasetVersionRecord],
    ) -> None:
        self.snapshot = snapshot
        self.versions = versions

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord:
        if identifier != self.snapshot.id:
            raise KeyError(identifier)
        return self.snapshot

    def get_dataset_version(self, identifier: DatasetVersionId) -> DatasetVersionRecord:
        return self.versions[identifier]


class _Progress:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, date]] = []

    def update(self, completed: int, total: int, trade_date: date) -> None:
        self.calls.append((completed, total, trade_date))


class _NeverCancelled:
    def is_cancelled(self) -> bool:
        return False


def _audit(available_at: datetime) -> dict[str, object]:
    return {
        "source": "offline-synthetic",
        "source_version": "fixture-v1",
        "available_at": available_at,
        "availability_source": "fixture",
        "pit_usable": True,
        "ingested_at": datetime(2024, 2, 6, tzinfo=UTC),
    }


def _dataset(
    tmp_path: Path,
    dataset: DatasetKind,
    rows: list[dict[str, object]],
) -> DatasetVersionRecord:
    definition = CANONICAL_SCHEMAS[dataset]
    path = tmp_path / f"{dataset.value}.parquet"
    pl.DataFrame(rows, schema=definition.columns, strict=False).write_parquet(path)
    table = pq.read_table(path)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return DatasetVersionRecord(
        id=DatasetVersionId.new(),
        dataset=dataset,
        fingerprint=hashlib.sha256(dataset.value.encode()).hexdigest(),
        source="offline-synthetic",
        status="PUBLISHED",
        partitions=(
            DatasetPartitionRecord(
                content_hash=hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest(),
                path=path,
                schema_fingerprint=hashlib.sha256(
                    table.schema.serialize().to_pybytes()
                ).hexdigest(),
                row_count=table.num_rows,
            ),
        ),
        start_date=_SIGNAL if dataset is DatasetKind.DAILY_BAR else None,
        end_date=_EXECUTE if dataset is DatasetKind.DAILY_BAR else None,
        created_run_id="offline-synthetic-fixture",
        created_at=datetime(2024, 2, 6, tzinfo=UTC),
    )


def _repository(tmp_path: Path) -> SnapshotResearchRepository:
    instruments = [
        {
            "instrument_id": instrument.canonical(),
            "exchange": "SSE",
            "board": "MAIN",
            "name": name,
            "instrument_type": instrument_type,
            "listing_status": "LISTED",
            "list_date": date(2012, 1, 1),
            "delist_date": None,
            **_audit(datetime(2024, 1, 1, tzinfo=UTC)),
        }
        for instrument, name, instrument_type in (
            (_BENCHMARK, "Synthetic benchmark", "INDEX"),
            (_ETF, "Synthetic ETF", "ETF"),
        )
    ]
    calendar = [
        {
            "trade_date": day,
            "is_trading_day": is_open,
            **_audit(datetime.combine(day, datetime.min.time(), tzinfo=UTC)),
        }
        for day, is_open in (
            (_SIGNAL, True),
            (date(2024, 2, 1), False),
            (_EXECUTE, True),
            (date(2024, 2, 3), False),
            (date(2024, 2, 4), False),
            (_POST_END, True),
        )
    ]
    bars: list[dict[str, object]] = []
    statuses: list[dict[str, object]] = []
    for day in (_SIGNAL, _EXECUTE):
        for instrument, close in ((_BENCHMARK, 3.0), (_ETF, 4.0)):
            available = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
            bars.append(
                {
                    "instrument_id": instrument.canonical(),
                    "trade_date": day,
                    "open": close,
                    "high": close + 0.1,
                    "low": close - 0.1,
                    "close": close,
                    "preclose": close,
                    "volume": 100_000,
                    "amount": close * 100_000,
                    "adjustment_flag": "none",
                    "turnover": 1.0,
                    "pct_change": 0.0,
                    "pe_ttm": 10.0,
                    "pb_mrq": 1.0,
                    "ps_ttm": 2.0,
                    "pcf_ncf_ttm": 3.0,
                    **_audit(available),
                }
            )
            statuses.append(
                {
                    "instrument_id": instrument.canonical(),
                    "trade_date": day,
                    "is_listed": True,
                    "is_suspended": False,
                    "is_risk_warning": False,
                    "board": "MAIN",
                    "price_limit_rule_id": "main",
                    "tradable_reason": "normal",
                    **_audit(available),
                }
            )
    versions = {
        record.id: record
        for record in (
            _dataset(tmp_path, DatasetKind.INSTRUMENT, instruments),
            _dataset(tmp_path, DatasetKind.TRADE_CALENDAR, calendar),
            _dataset(tmp_path, DatasetKind.DAILY_BAR, bars),
            _dataset(tmp_path, DatasetKind.SECURITY_STATUS, statuses),
            _dataset(tmp_path, DatasetKind.CORPORATE_ACTION, []),
        )
    }
    manifest = tmp_path / "snapshot-manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    snapshot = SnapshotRecord(
        id=_SNAPSHOT,
        publication_fingerprint="a" * 64,
        as_of=datetime(2024, 2, 6, tzinfo=UTC),
        status=SnapshotStatus.PUBLISHED,
        manifest_path=manifest,
        manifest_hash=hashlib.sha256(b"{}").hexdigest(),
        quality_run_id=QualityRunId.new(),
        dataset_versions={
            record.dataset.value: record.id for record in versions.values()
        },
        created_at=datetime(2024, 2, 6, tzinfo=UTC),
        published_at=datetime(2024, 2, 6, tzinfo=UTC),
    )
    return SnapshotResearchRepository(_Catalog(snapshot, versions))


def _factor_artifacts() -> dict[str, FactorArtifact]:
    artifacts: dict[str, FactorArtifact] = {}
    for factor_ref, value in _FACTOR_VALUES.items():
        factor_id, version = factor_ref.split("@")
        frame = pl.DataFrame(
            [
                {
                    "trade_date": _SIGNAL,
                    "instrument_id": _ETF.canonical(),
                    "factor_id": factor_id,
                    "factor_version": version,
                    "value": value,
                    "available_at": datetime(2024, 1, 31, 7, tzinfo=UTC),
                    "is_valid": True,
                }
            ],
            schema=FACTOR_OUTPUT_SCHEMA,
        )
        table = frame.to_arrow()
        artifacts[factor_ref] = FactorArtifact(
            factor_ref=factor_ref,
            cache_key=hashlib.sha256((factor_ref + "-cache").encode()).hexdigest(),
            content_hash=factor_table_content_hash(table),
            row_count=1,
            snapshot_id=_SNAPSHOT,
            universe_hash=_UNIVERSE_HASH,
            start=_SIGNAL,
            end=_EXECUTE,
            table=table,
        )
    return artifacts


def test_offline_synthetic_complete_snapshot_runs_strategy_through_real_engine(
    tmp_path: Path,
) -> None:
    """This is software integration evidence, not real BaoStock acceptance."""
    repository = _repository(tmp_path)
    strategy = EtfRotationStrategy(
        EtfRotationConfig.from_mapping(
            {
                "etf_pool": [_ETF.canonical()],
                "return_factor_weights": {
                    "return_20d_v1@1.0.0": 0.2,
                    "return_60d_v1@1.0.0": 0.3,
                    "return_120d_v1@1.0.0": 0.5,
                },
                "volatility_penalty": 0.1,
                "top_n": 1,
            }
        )
    )
    source_root = Path(__file__).resolve().parents[2]
    rulebook = AShareRuleBook.load(source_root / "configs/rules/a_share_v1.yaml")
    runner = SnapshotStrategyRunner(
        repository=repository,
        snapshot_id=_SNAPSHOT,
        capabilities=ProviderCapabilities.complete(),
        provider="offline-synthetic-complete",
        benchmark=_BENCHMARK,
        factor_artifacts=_factor_artifacts(),
        universe_hash=_UNIVERSE_HASH,
        universe_rules=UniverseRules(),
        enrichment=None,
        strategies={strategy.ref: strategy},
        stock_strategy_refs=frozenset(),
        rulebook=rulebook,
        portfolio_constructor=PortfolioConstructor(),
        rebalance_planner=RebalancePlanner(),
        artifact_root=tmp_path / "backtests",
    )
    request = BacktestRequest(
        _EXPERIMENT,
        _SNAPSHOT.value,
        strategy.ref,
        _SIGNAL,
        _EXECUTE,
        _BENCHMARK,
        1_000_000,
        "a-share-v1",
        ExecutionConfig(ExecutionPrice.CLOSE, 0.0, 1.0),
    )
    progress = _Progress()

    result = runner.run(request, progress, _NeverCancelled())

    targets = pq.read_table(result.artifact_dir / "targets.parquet").to_pylist()
    fills = pq.read_table(result.artifact_dir / "fills.parquet").to_pylist()
    assert result.sessions_completed == 2
    assert progress.calls == [(1, 2, _SIGNAL), (2, 2, _EXECUTE)]
    assert [
        (row["instrument_id"], row["execute_date"])
        for row in targets
        if row["instrument_id"] is not None
    ] == [(_ETF.canonical(), _EXECUTE)]
    assert [(row["instrument_id"], row["trade_date"]) for row in fills] == [
        (_ETF.canonical(), _EXECUTE)
    ]
    assert result.manifest_path.is_file()
