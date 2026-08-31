"""策略研究逐日撮合行情范围的可选性能证据。"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from typing import Any, cast

import polars as pl
import pytest

from quant_research.bootstrap.worker import CanonicalStrategyStudyData
from quant_research.domain.identifiers import InstrumentId
from tests.performance._process_memory import process_peak_rss_bytes

pytestmark = pytest.mark.performance

_MAX_SECONDS = 20.0
_SESSION_COUNT = 1_212
_UNIVERSE_SIZE = 5_891
_MARKET_SCOPE_SIZE = 20


class _Repository:
    """返回内存交易日历和请求范围内的固定行情。"""

    def __init__(self, sessions: tuple[date, ...]) -> None:
        self._sessions = sessions
        self.calendar_calls = 0
        self.requested_sizes: list[int] = []

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回覆盖请求范围的交易日。"""
        self.calendar_calls += 1
        values = [item for item in self._sessions if start <= item <= end]
        return pl.DataFrame(
            {"trade_date": values, "is_trading_day": [True] * len(values)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def stock_bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """仅构造请求证券的单日行情。"""
        assert start == end
        self.requested_sizes.append(len(instruments))
        size = len(instruments)
        return pl.DataFrame(
            {
                "instrument_id": [item.canonical() for item in instruments],
                "trade_date": [start] * size,
                "open": [10.0] * size,
                "high": [10.1] * size,
                "low": [9.9] * size,
                "close": [10.0] * size,
                "preclose": [10.0] * size,
                "volume": [10_000] * size,
            },
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "preclose": pl.Float64,
                "volume": pl.Int64,
            },
        ).lazy()

    def stock_suspensions(self, *_args: object) -> pl.LazyFrame:
        """返回空停牌事件。"""
        return _empty_events()

    def stock_risk_warnings(self, *_args: object) -> pl.LazyFrame:
        """返回空风险警示事件。"""
        return _empty_events()


def test_market_scope_stays_bounded_over_five_year_full_catalog() -> None:
    """五年逐日行情处理量必须由持仓规模而非全市场规模决定。"""
    sessions = _sessions()
    identifiers = tuple(
        InstrumentId.parse(f"{600_000 + index:06d}.SH")
        for index in range(_UNIVERSE_SIZE)
    )
    repository = _Repository(sessions)
    source = object.__new__(CanonicalStrategyStudyData)
    source._repository = cast(Any, repository)
    source._metadata = {
        item.canonical(): {
            "instrument_id": item.canonical(),
            "instrument_type": "STOCK",
            "board": "MAIN",
            "list_date": date(2010, 1, 4),
            "delist_date": None,
        }
        for item in identifiers
    }
    source._stock_ids = identifiers
    source._fund_ids = ()
    source._no_limit_through = None
    scope = identifiers[:_MARKET_SCOPE_SIZE]

    started = time.perf_counter()
    source.calendar(sessions[0], sessions[-1], include_next_session=False)
    for trade_date in sessions:
        market = source.market_slice(trade_date, scope).market
        assert market.bars.height == _MARKET_SCOPE_SIZE
    elapsed = time.perf_counter() - started
    evidence = {
        "sessions": len(sessions),
        "universe_size": len(identifiers),
        "market_scope_size": _MARKET_SCOPE_SIZE,
        "calendar_calls": repository.calendar_calls,
        "max_requested_size": max(repository.requested_sizes),
        "elapsed_seconds": elapsed,
        "process_peak_rss_bytes": process_peak_rss_bytes(),
    }
    print(f"strategy_market_scope_performance={json.dumps(evidence, sort_keys=True)}")

    assert repository.calendar_calls == 1, evidence
    assert max(repository.requested_sizes) == _MARKET_SCOPE_SIZE, evidence
    assert elapsed <= _MAX_SECONDS, evidence


def _sessions() -> tuple[date, ...]:
    current = date(2020, 1, 2)
    output: list[date] = []
    while len(output) < _SESSION_COUNT:
        if current.weekday() < 5:
            output.append(current)
        current += timedelta(days=1)
    return tuple(output)


def _empty_events() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={"instrument_id": pl.String, "trade_date": pl.Date}
    ).lazy()
