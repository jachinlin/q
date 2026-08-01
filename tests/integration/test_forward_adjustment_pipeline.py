"""End-to-end BaoStock forward-adjustment acceptance coverage."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from quant_core.data.adjustments import AdjustmentMode, PriceAdjustmentService
from quant_core.data.contracts import RawBatch
from quant_core.data.mappers.baostock import BaoStockMapper
from quant_core.data.partitions import RawPartitionStore
from quant_core.data.pipelines.curate import CuratedPartitionStore
from quant_core.data.pipelines.publish import DataPipeline
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.repository import SnapshotResearchRepository
from quant_core.data.snapshots import SnapshotPublisher
from quant_core.data.sources.baostock import DAILY_BAR_FIELDS
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import FactorContext, FactorEngine, FactorRegistry, FeatureCache
from quant_core.factors.base import (
    FactorArtifact,
    FactorSpec,
    factor_table_content_hash,
    thaw_json,
)
from quant_core.factors.builtin import register_etf_factors
from quant_core.factors.cache import build_cache_key
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import MetadataRepository

_ID = InstrumentId.parse("SSE:600000")
_DATES = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5))


class _FixedCalendar:
    def __init__(self, days: Sequence[date]) -> None:
        self._days = tuple(days)

    def bootstrap_window(self, years: int) -> tuple[date, date]:
        assert years == 20
        return self._days[0], self._days[-1]

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        return start, end

    def update_window(self, watermark: date, overlap_days: int) -> tuple[date, date]:
        raise AssertionError("this acceptance fixture only bootstraps")


class _FixtureBaoStockSource:
    provider = "baostock"

    def __init__(
        self,
        days: Sequence[date],
        closes: Sequence[float],
        precloses: Sequence[float],
    ) -> None:
        self.days = tuple(days)
        self.closes = tuple(closes)
        self.precloses = tuple(precloses)
        self.fetch_calls = 0

    def login(self) -> None:
        pass

    def close(self) -> None:
        pass

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        self.fetch_calls += 1
        retrieved_at = datetime.combine(
            self.days[-1] + timedelta(days=1), datetime.min.time(), UTC
        )
        yield RawBatch(
            provider=self.provider,
            dataset="instruments",
            request={"scope": "fixture"},
            retrieved_at=retrieved_at,
            schema=("code", "code_name", "ipoDate", "outDate", "type", "status"),
            rows=(
                {
                    "code": "sh.600000",
                    "code_name": "fixture",
                    "ipoDate": "1999-11-10",
                    "outDate": "",
                    "type": "1",
                    "status": "1",
                },
            ),
        )
        yield RawBatch(
            provider=self.provider,
            dataset="trade_calendar",
            request={"start": start.isoformat(), "end": end.isoformat()},
            retrieved_at=retrieved_at,
            schema=("calendar_date", "is_trading_day"),
            rows=tuple(
                {"calendar_date": day.isoformat(), "is_trading_day": "1"}
                for day in self.days
            ),
        )
        yield RawBatch(
            provider=self.provider,
            dataset="daily_bars",
            request={"api": "query_daily_history_k_AStock", "scope": "fixture"},
            retrieved_at=retrieved_at,
            schema=DAILY_BAR_FIELDS,
            rows=tuple(
                {
                    "date": day.isoformat(),
                    "code": "sh.600000",
                    "open": str(close),
                    "high": str(close),
                    "low": str(close),
                    "close": str(close),
                    "preclose": str(preclose),
                    "volume": "100",
                    "amount": "1000.0",
                    "adjustflag": "3",
                    "turn": "0.1",
                    "tradestatus": "1",
                    "pctChg": "0.0",
                    "peTTM": "10.0",
                    "pbMRQ": "1.0",
                    "psTTM": "1.0",
                    "pcfNcfTTM": "1.0",
                    "isST": "0",
                }
                for day, close, preclose in zip(
                    self.days, self.closes, self.precloses, strict=True
                )
            ),
        )


def _publish_fixture_snapshot(
    tmp_path: Path,
    days: Sequence[date] = _DATES,
    closes: Sequence[float] = (10.0, 12.0, 8.4, 9.0),
    precloses: Sequence[float] = (0.0, 10.0, 8.0, 8.4),
) -> tuple[SnapshotResearchRepository, object, _FixtureBaoStockSource]:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    metadata = MetadataRepository(create_sqlite_engine(database))
    snapshot_root = tmp_path / "data" / "snapshots"
    source = _FixtureBaoStockSource(days, closes, precloses)
    pipeline = DataPipeline(
        source=source,
        mapper=BaoStockMapper(),
        calendar=_FixedCalendar(days),
        raw_store=RawPartitionStore(tmp_path / "data" / "raw"),
        curated_store=CuratedPartitionStore(tmp_path / "data" / "curated"),
        repository=metadata,
        quality_runner=QualityRunner(),
        snapshot_publisher=SnapshotPublisher(
            metadata, snapshot_root, clock=lambda: datetime(2024, 1, 6, tzinfo=UTC)
        ),
        clock=lambda: datetime(2024, 1, 6, tzinfo=UTC),
    )
    result = pipeline.bootstrap()
    return SnapshotResearchRepository(metadata), result, source


def test_baostock_raw_to_snapshot_forward_adjustment_needs_no_corporate_actions(
    tmp_path: Path,
) -> None:
    """Removing the FORWARD branch would make this snapshot read require actions."""
    repository, result, _ = _publish_fixture_snapshot(tmp_path)

    # Baseline characterization: the full Raw -> Canonical -> Snapshot path already
    # exposes FORWARD without a CORPORATE_ACTION dataset.  The literal values below
    # protect that cross-boundary contract rather than a mapper implementation detail.
    assert "corporate_action" not in result.dataset_versions
    frame = (
        PriceAdjustmentService(repository)
        .bars(
            result.snapshot_id,
            [_ID],
            _DATES[0],
            _DATES[-1],
            AdjustmentMode.FORWARD,
            _DATES[-1],
        )
        .collect()
    )

    assert frame["close"].to_list() == pytest.approx([20.0 / 3.0, 8.0, 8.4, 9.0])
    assert frame["preclose"].to_list() == pytest.approx([0.0, 20.0 / 3.0, 8.0, 8.4])
    assert frame["adjustment_mode"].unique().to_list() == ["FORWARD"]


class _CountingPriceService:
    def __init__(self, delegate: PriceAdjustmentService) -> None:
        self._delegate = delegate
        self.calls = 0

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame:
        self.calls += 1
        return self._delegate.bars(snapshot_id, instruments, start, end, mode, as_of)


def _observed_weekdays(count: int) -> list[date]:
    days: list[date] = []
    current = date(2024, 1, 2)
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _artifact_signal_hash(artifact: FactorArtifact, signal_day: date) -> str:
    frame = artifact.lazy_frame().filter(pl.col("trade_date") == signal_day).collect()
    return factor_table_content_hash(frame.to_arrow())


def test_etf_forward_factor_cache_and_future_jump_keep_prior_signal_rows_stable(
    tmp_path: Path,
) -> None:
    """A later split-like jump must not rewrite earlier signal observations."""
    days = _observed_weekdays(122)
    closes = [100.0 + index for index in range(122)]
    precloses = [0.0, *(closes[index - 1] for index in range(1, 122))]
    precloses[-1] = closes[-2] * 0.7
    closes[-1] = closes[-2] * 0.72
    repository, result, source = _publish_fixture_snapshot(
        tmp_path, days, closes, precloses
    )
    prices = _CountingPriceService(PriceAdjustmentService(repository))
    registry = FactorRegistry()
    register_etf_factors(registry, prices, [_ID])
    cache = FeatureCache(tmp_path / "features")
    engine = FactorEngine(registry, cache)
    requested = (
        "return_20d_v1",
        "return_60d_v1",
        "return_120d_v1",
        "trend_120d_v1",
        "volatility_60d_v1",
    )
    original_ctx = FactorContext(result.snapshot_id, "f" * 64, days[-2], days[-2])

    first = engine.compute(requested, original_ctx)
    cache_state = _cache_state(cache.root)
    calls_after_first = prices.calls
    second = engine.compute(requested, original_ctx)

    assert source.fetch_calls == 1
    assert prices.calls == calls_after_first
    assert _cache_state(cache.root) == cache_state
    assert {key: value.content_hash for key, value in second.items()} == {
        key: value.content_hash for key, value in first.items()
    }
    assert all(
        json.loads((cache.root / artifact.cache_key / "manifest.json").read_text())[
            "parameters"
        ]["adjustment_mode"]
        == "FORWARD"
        for artifact in first.values()
    )

    spec = registry.spec("return_20d_v1")
    backward_spec = FactorSpec(
        spec.factor_id,
        spec.version,
        spec.frequency,
        spec.lookback_sessions,
        spec.dependencies,
        spec.direction,
        {**thaw_json(spec.parameters), "adjustment_mode": "BACKWARD"},
    )
    assert (
        build_cache_key(
            backward_spec,
            original_ctx,
            registry.code_hash("return_20d_v1"),
            {},
        )
        != first["return_20d_v1@1.0.0"].cache_key
    )

    original_hashes = {
        key: _artifact_signal_hash(artifact, days[-2])
        for key, artifact in first.items()
    }
    extended = engine.compute(
        requested,
        FactorContext(result.snapshot_id, "f" * 64, days[-2], days[-1]),
    )

    assert {
        key: _artifact_signal_hash(artifact, days[-2])
        for key, artifact in extended.items()
    } == original_hashes


def _cache_state(root: Path) -> dict[str, tuple[int, int, int]]:
    return {
        str(path.relative_to(root)): (
            path.stat().st_ino,
            path.stat().st_mtime_ns,
            path.stat().st_size,
        )
        for path in root.rglob("*")
        if path.is_file()
    }
