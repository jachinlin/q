"""市场全景口径与目录身份测试。"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.domain.identifiers import IndexId, InstrumentId, QualityRunId
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DataCatalogState,
)


def _sessions() -> tuple[date, ...]:
    result: list[date] = []
    current = date(2026, 1, 2)
    while len(result) < 21:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return tuple(result)


class _Catalog:
    def __init__(self, sessions: tuple[date, ...]) -> None:
        now = datetime(2026, 2, 1, tzinfo=UTC)
        self.state = DataCatalogState(
            catalog_hash="a" * 64,
            validated_catalog_hash="a" * 64,
            quality_run_id=QualityRunId.new(),
            updated_at=now,
            validated_at=now,
        )
        self.records = tuple(
            CanonicalDatasetRecord(
                dataset=dataset,
                content_hash=dataset.value.ljust(64, "0")[:64],
                source="test",
                partitions=(),
                start_date=sessions[0],
                end_date=sessions[-1],
                updated_at=now,
            )
            for dataset in (
                DatasetKind.STOCK_DAILY_BAR,
                DatasetKind.STOCK_DAILY_BASIC,
                DatasetKind.STOCK_SUSPENSION,
                DatasetKind.STOCK_RISK_WARNING,
                DatasetKind.INDEX_DAILY_BAR,
            )
        )

    def require_validated_catalog(self) -> DataCatalogState:
        return self.state

    def list_canonical_datasets(self) -> tuple[CanonicalDatasetRecord, ...]:
        return self.records


class _DriftingCatalog(_Catalog):
    def __init__(self, sessions: tuple[date, ...]) -> None:
        super().__init__(sessions)
        self.calls = 0

    def require_validated_catalog(self) -> DataCatalogState:
        self.calls += 1
        identity = ("a", "b", "b", "c")[min(self.calls - 1, 3)] * 64
        return replace(
            self.state,
            catalog_hash=identity,
            validated_catalog_hash=identity,
        )


class _Repository:
    def __init__(self, sessions: tuple[date, ...], catalog: _Catalog) -> None:
        self.sessions = sessions
        self._catalog = catalog
        selected = sessions[-1]
        self.stock_ids = (
            "600000.SH",
            "600001.SH",
            "300001.SZ",
            "688001.SH",
            "600002.SH",
            "600003.SH",
            "600004.SH",
        )
        boards = ("MAIN", "MAIN", "CHINEXT", "STAR", "MAIN", "MAIN", "MAIN")
        listings = (
            date(2020, 1, 2),
            date(2020, 1, 2),
            date(2020, 1, 2),
            sessions[-3],
            date(2020, 1, 2),
            date(2020, 1, 2),
            date(2020, 1, 2),
        )
        instruments = [
            {
                "instrument_id": identifier,
                "instrument_type": "STOCK",
                "name": f"股票{index}",
                "board": board,
                "list_date": listing,
            }
            for index, (identifier, board, listing) in enumerate(
                zip(self.stock_ids, boards, listings, strict=True)
            )
        ]
        self.instrument_frame = pl.DataFrame(instruments)

        statuses: list[dict[str, object]] = []
        bars: list[dict[str, object]] = []
        for session in sessions:
            for identifier, board in zip(self.stock_ids, boards, strict=True):
                is_st = identifier == "600001.SH"
                is_suspended = identifier == "300001.SZ" and session == selected
                statuses.append(
                    {
                        "instrument_id": identifier,
                        "trade_date": session,
                        "is_listed": True,
                        "is_suspended": is_suspended,
                        "is_st": is_st,
                        "board": board,
                    }
                )
                if identifier == "600002.SH" and session == selected:
                    continue
                bar = {
                    "instrument_id": identifier,
                    "trade_date": session,
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "preclose": 10.0,
                    "amount": 100_000_000.0,
                    "pct_change": 0.0,
                }
                if session == selected:
                    overrides = {
                        "600000.SH": (10.0, 11.0, 10.0, 11.0, 0.10),
                        "600001.SH": (10.5, 10.5, 10.5, 10.5, 0.05),
                        "688001.SH": (10.0, 12.0, 10.0, 12.0, 0.20),
                        "600003.SH": (10.0, 10.0, 9.0, 9.0, -0.10),
                        "600004.SH": (10.0, 11.0, 10.0, 10.5, 0.05),
                    }
                    if identifier in overrides:
                        open_price, high, low, close, pct = overrides[identifier]
                        bar.update(
                            open=open_price,
                            high=high,
                            low=low,
                            close=close,
                            pct_change=pct,
                        )
                bars.append(bar)
        self.status_frame = pl.DataFrame(statuses)
        self.bar_frame = pl.DataFrame(bars)
        self.basic_frame = pl.DataFrame(
            {
                "instrument_id": list(self.stock_ids),
                "trade_date": [selected] * len(self.stock_ids),
                "pe_ttm": [10.0, -2.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                "pb": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
                "ps_ttm": [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
                "turnover_rate": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07],
            }
        )
        self.industry_frame = pl.DataFrame(
            {
                "instrument_id": list(self.stock_ids[:-1]),
                "level1_code": ["金融", "金融", "科技", "科技", "工业", "工业"],
                "level1_name": ["金融", "金融", "科技", "科技", "工业", "工业"],
            }
        )
        index_rows: list[dict[str, object]] = []
        for identifier in (
            "399317.SZ",
            "000016.SH",
            "000300.SH",
            "000905.SH",
            "000852.SH",
        ):
            for offset, session in enumerate(sessions):
                close = 100.0 + offset
                index_rows.append(
                    {
                        "index_id": identifier,
                        "trade_date": session,
                        "high": close + 1.0,
                        "low": close - 1.0,
                        "close": close,
                        "preclose": close - 1.0,
                        "pct_change": 0.01,
                    }
                )
        self.index_frame = pl.DataFrame(index_rows)

    def catalog(self) -> _Catalog:
        return self._catalog

    def stocks(self) -> pl.LazyFrame:
        return self.instrument_frame.lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        return pl.DataFrame(
            {
                "trade_date": [item for item in self.sessions if start <= item <= end],
                "is_trading_day": [True]
                * sum(start <= item <= end for item in self.sessions),
            }
        ).lazy()

    def stock_bars(
        self, instruments: tuple[InstrumentId, ...], start: date, end: date
    ) -> pl.LazyFrame:
        identifiers = [item.canonical() for item in instruments]
        return self.bar_frame.filter(
            pl.col("instrument_id").is_in(identifiers)
            & pl.col("trade_date").is_between(start, end)
        ).lazy()

    def index_bars(
        self, indexes: tuple[IndexId, ...], start: date, end: date
    ) -> pl.LazyFrame:
        identifiers = [item.canonical() for item in indexes]
        return self.index_frame.filter(
            pl.col("index_id").is_in(identifiers)
            & pl.col("trade_date").is_between(start, end)
        ).lazy()

    def stock_suspensions(
        self,
        start: date,
        end: date,
        instruments: tuple[InstrumentId, ...] | None = None,
    ) -> pl.LazyFrame:
        return self.status_frame.filter(
            pl.col("trade_date").is_between(start, end) & pl.col("is_suspended")
        ).select("instrument_id", "trade_date").lazy()

    def stock_risk_warnings(
        self,
        start: date,
        end: date,
        instruments: tuple[InstrumentId, ...] | None = None,
    ) -> pl.LazyFrame:
        return self.status_frame.filter(
            pl.col("trade_date").is_between(start, end) & pl.col("is_st")
        ).select("instrument_id", "trade_date").lazy()

    def stock_daily_basics(
        self, instruments: tuple[InstrumentId, ...], start: date, end: date
    ) -> pl.LazyFrame:
        return self.basic_frame.filter(
            pl.col("trade_date").is_between(start, end)
        ).lazy()

    def industry_memberships_on_dates(
        self,
        instruments: tuple[InstrumentId, ...] | None,
        dates: tuple[date, ...],
    ) -> pl.LazyFrame:
        return self.industry_frame.with_columns(pl.lit(dates[0]).alias("query_date")).lazy()


def _service() -> tuple[MarketReviewService, _Repository]:
    sessions = _sessions()
    catalog = _Catalog(sessions)
    repository = _Repository(sessions, catalog)
    rulebook = AShareRuleBook.load(
        Path(__file__).resolve().parents[3] / "configs" / "rules" / "a_share.yaml"
    )
    return (
        MarketReviewService(cast(ResearchDataRepository, repository), rulebook),
        repository,
    )


def test_market_review_computes_full_cross_section_and_limit_coverage() -> None:
    service, _ = _service()
    dates = service.available_dates()
    result = service.review(None, exclude_st=False)

    assert dates.latest_trade_date == _sessions()[-1]
    assert result.data_quality.expected_count == 7
    assert result.data_quality.priced_count == 5
    assert result.data_quality.suspended_count == 1
    assert result.data_quality.missing_bar_count == 1
    assert result.breadth.up_count == 4
    assert result.breadth.down_count == 1
    assert result.breadth.median_return == 0.05
    assert result.liquidity.amount == 600_000_000.0
    assert result.sentiment.limit_up_count == 2
    assert result.sentiment.limit_down_count == 1
    assert result.sentiment.broken_limit_up_count == 1
    assert result.sentiment.one_price_limit_up_count == 1
    assert result.sentiment.unresolved_count == 1
    assert result.industries.available is True
    assert result.industries.coverage_rate == 6 / 7
    assert result.valuation.metrics[0].valid_count == 4
    assert tuple(item.index_id for item in result.indexes) == (
        "399317.SZ",
        "000016.SH",
        "000300.SH",
        "000905.SH",
        "000852.SH",
    )
    assert result.indexes[0].return_20d == pytest.approx(0.2)


def test_market_review_keeps_existing_indexes_when_cni_a_history_is_missing() -> None:
    service, repository = _service()
    repository.index_frame = repository.index_frame.filter(
        pl.col("index_id") != "399317.SZ"
    )

    result = service.review(_sessions()[-1], exclude_st=False)

    cni_a_share = result.indexes[0]
    assert cni_a_share.index_id == "399317.SZ"
    assert cni_a_share.name == "国证A指"
    assert cni_a_share.daily_return is None
    assert cni_a_share.return_5d is None
    assert cni_a_share.return_20d is None
    assert cni_a_share.series == ()
    assert all(item.daily_return == pytest.approx(0.01) for item in result.indexes[1:])


def test_market_review_excludes_st_consistently_and_caches_by_scope() -> None:
    service, _ = _service()
    included = service.review(_sessions()[-1], exclude_st=False)
    excluded = service.review(_sessions()[-1], exclude_st=True)

    assert included is service.review(_sessions()[-1], exclude_st=False)
    assert excluded.exclude_st is True
    assert excluded.data_quality.expected_count == 6
    assert excluded.data_quality.st_count == 0
    assert excluded.breadth.up_count == 3
    assert excluded.sentiment.one_price_limit_up_count == 0
    assert excluded.valuation.turnover_valid_count == 4


def test_market_review_ignores_non_tradable_supplier_test_codes() -> None:
    service, repository = _service()
    repository.instrument_frame = pl.concat(
        (
            repository.instrument_frame,
            pl.DataFrame(
                {
                    "instrument_id": ["T00018.SH"],
                    "instrument_type": ["STOCK"],
                    "name": ["供应商测试证券"],
                    "board": ["MAIN"],
                    "list_date": [date(2020, 1, 2)],
                }
            ),
        )
    )

    result = service.review(_sessions()[-1], exclude_st=False)

    assert result.data_quality.expected_count == 7
    assert all(
        item.instrument_id != "T00018.SH" for item in result.sentiment.events
    )


def test_market_review_rejects_non_session_and_never_falls_back_industry() -> None:
    service, repository = _service()
    repository.industry_frame = pl.DataFrame(
        schema={
            "instrument_id": pl.String,
            "taxonomy": pl.String,
            "industry_code": pl.String,
            "industry_name": pl.String,
            "is_classified": pl.Boolean,
        }
    )

    result = service.review(_sessions()[-1], exclude_st=False)

    assert result.industries.available is False
    assert result.industries.items == ()
    try:
        service.review(date(2026, 1, 3), exclude_st=False)
    except ValueError as error:
        assert "not an available" in str(error)
    else:
        raise AssertionError("non-session date must be rejected")


def test_market_review_distinguishes_snapshot_with_zero_selected_coverage() -> None:
    service, repository = _service()
    repository.industry_frame = pl.DataFrame(
        {
            "instrument_id": ["601999.SH"],
            "level1_code": ["C39"],
            "level1_name": ["C39"],
        }
    )

    result = service.review(None, exclude_st=False)

    assert result.industries.available is True
    assert result.industries.coverage_rate == 0.0
    assert result.industries.items == ()
    assert result.industries.unavailable_reason is None


def test_market_review_tombstones_do_not_inherit_old_industry() -> None:
    service, repository = _service()
    repository.industry_frame = repository.industry_frame.with_columns(
        pl.lit(None, dtype=pl.String).alias("level1_code"),
        pl.lit(None, dtype=pl.String).alias("level1_name"),
    )

    result = service.review(None, exclude_st=False)

    assert result.industries.available is True
    assert result.industries.coverage_rate == 0.0
    assert result.industries.items == ()


def test_market_review_rejects_repeated_catalog_drift() -> None:
    sessions = _sessions()
    catalog = _DriftingCatalog(sessions)
    repository = _Repository(sessions, catalog)
    service = MarketReviewService(
        cast(ResearchDataRepository, repository),
        AShareRuleBook.load(
            Path(__file__).resolve().parents[3] / "configs" / "rules" / "a_share.yaml"
        ),
    )

    with pytest.raises(QuantError, match="catalog changed"):
        service.review(sessions[-1], exclude_st=False)
