"""验证停牌日空成交额在股票池市场证据中的处理语义。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import cast

import polars as pl
import pytest

import quant_research.experiments.adapters as adapters_module
from quant_research.data.contracts import ProviderCapabilities
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.identifiers import InstrumentId
from quant_research.experiments.adapters import CanonicalStrategyData, _AdaptersSupport
from quant_research.universe.rules import UniverseRules


class _MarketEvidenceRepository:
    """提供包含一个空成交额交易日的二十日研究窗口。"""

    def __init__(self, *, suspended: bool) -> None:
        first = date(2025, 5, 12)
        candidates = (first + timedelta(days=offset) for offset in range(30))
        self.sessions = tuple(day for day in candidates if day.weekday() < 5)[:20]
        self.suspended = suspended

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        del start, end
        return pl.DataFrame(
            {
                "trade_date": self.sessions,
                "is_trading_day": [True] * len(self.sessions),
            }
        ).lazy()

    def bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        del instruments, start, end
        available = [
            datetime(day.year, day.month, day.day, tzinfo=UTC) for day in self.sessions
        ]
        return pl.DataFrame(
            {
                "instrument_id": ["600000.SH"] * len(self.sessions),
                "trade_date": self.sessions,
                "amount": [100.0] * (len(self.sessions) - 1) + [None],
                "available_at": available,
                "pit_usable": [True] * len(self.sessions),
            }
        ).lazy()

    def security_status_range(
        self,
        start: date,
        end: date,
        instruments: tuple[InstrumentId, ...] | None = None,
    ) -> pl.LazyFrame:
        del start, end, instruments
        return pl.DataFrame(
            {
                "instrument_id": ["600000.SH"],
                "trade_date": [self.sessions[-1]],
                "is_suspended": [self.suspended],
            }
        ).lazy()


def test_suspended_null_amount_counts_as_zero_in_adv() -> None:
    """确认停牌的空成交额应以零参与二十日平均成交额。"""
    repository = _MarketEvidenceRepository(suspended=True)

    result = _AdaptersSupport._universe_market_evidence(
        cast(ResearchDataRepository, repository),
        (InstrumentId.parse("600000.SH"),),
        repository.sessions[-1],
    )

    assert result == {"600000.SH": {"adv_amount": 95.0}}


def test_active_null_amount_remains_fail_closed() -> None:
    """非停牌证券的空成交额必须继续失败关闭。"""
    repository = _MarketEvidenceRepository(suspended=False)

    with pytest.raises(ValueError, match="market evidence is nonfinite"):
        _AdaptersSupport._universe_market_evidence(
            cast(ResearchDataRepository, repository),
            (InstrumentId.parse("600000.SH"),),
            repository.sessions[-1],
        )


class _NoIndustryRepository:
    def __getattr__(self, name: str) -> object:
        if "industry" in name:
            raise AssertionError(f"strategy attempted an industry read: {name}")
        raise AttributeError(name)


class _BoundFactorSource:
    data_hash = "a" * 64
    universe_hash = "b" * 64

    def values(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        raise AssertionError((signal_date, instruments, factor_refs))

    def close(self) -> None:
        return None


class _SingleStockUniverseBuilder:
    def __init__(self, repository: object) -> None:
        assert isinstance(repository, _NoIndustryRepository)

    def build(self, as_of: date, rules: UniverseRules) -> pl.DataFrame:
        assert isinstance(rules, UniverseRules)
        return pl.DataFrame(
            {
                "instrument_id": ["600000.SH"],
                "as_of": [as_of],
                "eligible": [True],
                "reason_codes": [[]],
            },
            schema={
                "instrument_id": pl.String,
                "as_of": pl.Date,
                "eligible": pl.Boolean,
                "reason_codes": pl.List(pl.String),
            },
        )


def test_strategy_universe_never_reads_industry_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_date = date(2026, 6, 9)
    repository = _NoIndustryRepository()
    monkeypatch.setattr(adapters_module, "UniverseBuilder", _SingleStockUniverseBuilder)
    monkeypatch.setattr(
        _AdaptersSupport,
        "_universe_market_evidence",
        staticmethod(
            lambda _bound_repository, _instruments, _day: {
                "600000.SH": {"adv_amount": 100.0}
            }
        ),
    )
    data = CanonicalStrategyData(
        repository=cast(ResearchDataRepository, repository),
        data_hash="a" * 64,
        factor_source=_BoundFactorSource(),
        universe_hash="b" * 64,
        universe_signal_dates=(signal_date,),
        universe_rules=UniverseRules(),
        capabilities=ProviderCapabilities.complete(),
        provider="test",
    )

    result = data.stock_universe(signal_date)

    assert result.columns == [
        "instrument_id",
        "as_of",
        "eligible",
        "reason_codes",
        "adv_amount",
    ]


class _IndustryRepository:
    def industry_classifications_as_of(
        self, instruments: object, as_of: date
    ) -> pl.LazyFrame:
        assert instruments is not None
        return pl.DataFrame(
            {
                "instrument_id": ["600000.SH", "600001.SH"],
                "taxonomy": ["证监会行业分类", "其他分类"],
                "industry_code": [None, "X"],
                "industry_name": [None, "X"],
                "is_classified": [False, True],
            }
        ).lazy()


def test_explicit_strategy_industry_read_uses_signal_date_and_keeps_tombstone() -> None:
    signal_date = date(2026, 6, 9)
    data = CanonicalStrategyData(
        repository=cast(ResearchDataRepository, _IndustryRepository()),
        data_hash="a" * 64,
        factor_source=_BoundFactorSource(),
        universe_hash="b" * 64,
        universe_signal_dates=(signal_date,),
        universe_rules=UniverseRules(),
        capabilities=ProviderCapabilities.complete(),
        provider="test",
    )

    result = data.industry_classifications(
        signal_date,
        (InstrumentId.parse("600000.SH"),),
        "证监会行业分类",
    )

    assert result.select("instrument_id", "industry_code", "is_classified").rows() == [
        ("600000.SH", None, False)
    ]
