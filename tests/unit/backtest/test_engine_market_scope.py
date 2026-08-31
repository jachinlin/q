"""验证回测引擎只读取撮合和估值需要的日行情范围。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import polars as pl

from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.engine import (
    BacktestEngine,
    BacktestRequest,
    BacktestResult,
    BoundMarketSlice,
)
from quant_research.backtest.models import ExecutionConfig, ExecutionPrice, MarketSlice
from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.domain.identifiers import IndexId, InstrumentId
from quant_research.portfolio.rebalance import OrderIntent, OrderSide
from quant_research.strategies.base import DecisionContext, StrategySpec

_INSTRUMENT = InstrumentId.parse("600000.SH")
_CATALOG_HASH = "a" * 64


class _MarketData:
    """记录引擎请求范围并返回可切换的停牌或缺失行情。"""

    def __init__(self, *, missing_execution_bar: bool = False) -> None:
        self.scopes: list[tuple[str, ...]] = []
        self._missing_execution_bar = missing_execution_bar

    def calendar(
        self, start: date, end: date, *, include_next_session: bool
    ) -> TradingCalendar:
        """返回三个回测交易日和一个附加交易日。"""
        assert start == date(2024, 1, 2)
        assert end == date(2024, 1, 4)
        assert include_next_session is True
        return TradingCalendar(
            start,
            date(2024, 1, 5),
            (
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ),
        )

    def market_slice(
        self, trade_date: date, instruments: Sequence[InstrumentId]
    ) -> BoundMarketSlice:
        """按请求范围返回有效、缺失或停牌占位行情。"""
        scope = tuple(item.canonical() for item in instruments)
        self.scopes.append(scope)
        if not scope or (
            self._missing_execution_bar and trade_date == date(2024, 1, 3)
        ):
            frame = pl.DataFrame(schema=_market_schema())
        elif trade_date == date(2024, 1, 4):
            frame = _market_frame(trade_date, suspended=True)
        else:
            frame = _market_frame(trade_date, suspended=False)
        return BoundMarketSlice(MarketSlice(trade_date, frame))

    def benchmark_close(self, benchmark: IndexId, trade_date: date) -> float:
        """返回固定基准收盘价。"""
        assert benchmark == IndexId.parse("000300.SH")
        del trade_date
        return 100.0


class _BoundDecisionData:
    """只暴露测试策略需要的绑定日期。"""

    def __init__(self, signal_date: date) -> None:
        self.signal_date = signal_date


class _DecisionDataFactory:
    """创建固定日期绑定视图。"""

    def bind(self, signal_date: date) -> _BoundDecisionData:
        """返回绑定信号日。"""
        return _BoundDecisionData(signal_date)


class _BuyOnceStrategy:
    """首日生成一笔买单，随后保持持仓。"""

    @property
    def spec(self) -> StrategySpec:
        """返回最小测试策略身份。"""
        return StrategySpec("scope_test", "DAILY", (), (), {})

    def warmup(self, ctx: DecisionContext) -> None:
        """测试策略无需预热。"""
        del ctx

    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]:
        """仅在首个信号日生成买单。"""
        if ctx.signal_date == date(2024, 1, 2):
            return (OrderIntent(_INSTRUMENT, OrderSide.BUY, 100, "TEST_BUY"),)
        return ()


class _Guard:
    """断言研究目录身份保持不变。"""

    def assert_unchanged(self, catalog_hash: str) -> None:
        """校验固定测试哈希。"""
        assert catalog_hash == _CATALOG_HASH


class _Progress:
    """忽略测试进度。"""

    def update(self, completed: int, total: int, trade_date: date) -> None:
        """接收合法进度。"""
        assert 1 <= completed <= total == 3
        assert isinstance(trade_date, date)


class _Cancellation:
    """测试运行永不取消。"""

    def is_cancelled(self) -> bool:
        """返回未取消。"""
        return False


def test_engine_scopes_market_to_pending_orders_then_positions() -> None:
    """空仓首日不读行情，新买入和停牌持仓都只读取目标证券。"""
    market = _MarketData()

    result = _run(market)

    assert market.scopes == [(), ("600000.SH",), ("600000.SH",)]
    assert result.tables["fills"].select(
        "trade_date", "instrument_id", "reason_code"
    ).to_dicts() == [
        {
            "trade_date": date(2024, 1, 3),
            "instrument_id": "600000.SH",
            "reason_code": "FILLED",
        }
    ]
    holdings = result.tables["holdings"].filter(
        pl.col("instrument_id") == "600000.SH"
    )
    assert holdings["trade_date"].to_list() == [date(2024, 1, 3), date(2024, 1, 4)]
    assert holdings["market_value_fen"].to_list() == [100_000, 100_000]


def test_missing_scoped_market_row_remains_no_market_data_rejection() -> None:
    """范围内证券缺少行情时保持既有 NO_MARKET_DATA 拒绝语义。"""
    market = _MarketData(missing_execution_bar=True)

    result = _run(market)

    assert market.scopes == [(), ("600000.SH",), ()]
    assert result.tables["fills"].select(
        "trade_date", "instrument_id", "reason_code"
    ).to_dicts() == [
        {
            "trade_date": date(2024, 1, 3),
            "instrument_id": "600000.SH",
            "reason_code": "NO_MARKET_DATA",
        }
    ]
    assert result.tables["holdings"].is_empty()


def _run(market: _MarketData) -> BacktestResult:
    rulebook = AShareRuleBook.load(
        Path(__file__).resolve().parents[3] / "configs" / "rules" / "a_share.yaml"
    )
    return BacktestEngine(
        market,
        _DecisionDataFactory(),  # type: ignore[arg-type]
        rulebook,
        _Guard(),
    ).run(
        BacktestRequest(
            "scope-test-study",
            _CATALOG_HASH,
            date(2024, 1, 2),
            date(2024, 1, 4),
            IndexId.parse("000300.SH"),
            1_000_000,
            rulebook.content_hash,
            ExecutionConfig(ExecutionPrice.OPEN, 0.0, 1.0),
        ),
        _BuyOnceStrategy(),
        _Progress(),
        _Cancellation(),
    )


def _market_frame(trade_date: date, *, suspended: bool) -> pl.DataFrame:
    values: dict[str, list[object]] = {
        "instrument_id": ["600000.SH"],
        "open": [None if suspended else 10.0],
        "high": [None if suspended else 10.0],
        "low": [None if suspended else 10.0],
        "close": [None if suspended else 10.0],
        "preclose": [None if suspended else 10.0],
        "volume": [None if suspended else 10_000],
        "is_suspended": [suspended],
        "security_status": ["NORMAL"],
        "instrument_type": ["STOCK"],
        "board": ["MAIN"],
    }
    del trade_date
    return pl.DataFrame(values, schema=_market_schema())


def _market_schema() -> pl.Schema:
    return pl.Schema(
        {
            "instrument_id": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "volume": pl.Int64,
            "is_suspended": pl.Boolean,
            "security_status": pl.String,
            "instrument_type": pl.String,
            "board": pl.String,
        }
    )
