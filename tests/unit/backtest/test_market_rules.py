"""回测市场切片、交易画像、执行精度与账户交收的契约测试。"""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from quant_research.backtest import (
    AccountView,
    AShareRuleBook,
    ExecutionBatch,
    ExecutionConfig,
    ExecutionModel,
    ExecutionPrice,
    ExecutionReason,
    FeeBreakdown,
    FillResult,
    MarketSlice,
    PortfolioAccount,
    SecurityStatus,
    Side,
    SimulatedFill,
    TradingCalendar,
)
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio import OrderIntent, OrderSide

_RULES = Path(__file__).resolve().parents[3] / "configs" / "rules" / "a_share.yaml"


def test_rulebook_exposes_pretrade_commission_from_single_source() -> None:
    """确认事前成本与事后成交共享规则文件中的佣金配置。"""
    rulebook = AShareRuleBook.load(_RULES)
    assert rulebook.commission_bps == 3.0
    assert rulebook.commission_minimum_fen == 500
_ETF_T1 = InstrumentId.parse("510050.SH")
_ETF_T0 = InstrumentId.parse("513100.SH")
_STAR_ETF = InstrumentId.parse("588000.SH")
_STAR_STOCK = InstrumentId.parse("688001.SH")
_MISSING_ETF = InstrumentId.parse("512999.SH")
_DAY_ONE = date(2026, 7, 30)
_DAY_TWO = date(2026, 7, 31)


def _market(
    instrument: InstrumentId,
    *,
    suspended: bool,
    price: float | None,
    instrument_type: str = "ETF",
    board: str = "MAIN",
) -> MarketSlice:
    values = {
        "instrument_id": [instrument.canonical()],
        "open": [price],
        "high": [price],
        "low": [price],
        "close": [price],
        "preclose": [price],
        "volume": [None if price is None else 10_000],
        "is_suspended": [suspended],
        "security_status": ["NORMAL"],
        "instrument_type": [instrument_type],
        "board": [board],
    }
    schema = {
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
    return MarketSlice(_DAY_ONE, pl.DataFrame(values, schema=schema))


def _buy_fill(settlement_sessions: int) -> FillResult:
    intent = OrderIntent(_ETF_T1, OrderSide.BUY, 100, "TEST")
    return FillResult(
        intent=intent,
        trade_date=_DAY_ONE,
        requested_quantity=100,
        reference_price=3.527,
        requested_reference_value_fen=35_270,
        filled_quantity=100,
        unfilled_quantity=0,
        price=3.527,
        gross_value_fen=35_270,
        settlement_sessions=settlement_sessions,
        fees=FeeBreakdown(500, 0, 0, 500),
        reason_code=ExecutionReason.FILLED,
    )


def test_suspended_market_slice_accepts_null_prices_and_rejects_before_price_read() -> (
    None
):
    market = _market(_ETF_T1, suspended=True, price=None)
    intent = OrderIntent(_ETF_T1, OrderSide.BUY, 100, "TEST")

    result = ExecutionModel().execute(
        [intent],
        market,
        AccountView(1_000_000, {}),
        AShareRuleBook.load(_RULES),
        ExecutionConfig(ExecutionPrice.OPEN, 0.0, 1.0),
    )

    assert result.results[0].reason_code is ExecutionReason.SUSPENDED
    assert result.results[0].reference_price is None
    assert result.results[0].requested_reference_value_fen is None
    assert result.ending_cash_fen == 1_000_000


def test_suspended_market_slice_accepts_valuation_price_with_null_volume() -> None:
    """停牌日可保留估值价格和空成交量，执行仍须在读取容量前拒绝。"""
    priced = _market(_ETF_T1, suspended=True, price=3.527)
    market = MarketSlice(
        _DAY_ONE,
        priced.bars.with_columns(pl.lit(None, dtype=pl.Int64).alias("volume")),
    )
    intent = OrderIntent(_ETF_T1, OrderSide.BUY, 100, "TEST")

    result = ExecutionModel().execute(
        [intent],
        market,
        AccountView(1_000_000, {}),
        AShareRuleBook.load(_RULES),
        ExecutionConfig(ExecutionPrice.OPEN, 0.0, 1.0),
    )

    assert result.results[0].reason_code is ExecutionReason.SUSPENDED


def test_etf_execution_retains_mill_price_and_waives_stock_taxes() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    intent = OrderIntent(_ETF_T1, OrderSide.BUY, 100, "TEST")

    batch = ExecutionModel().execute(
        [intent],
        _market(_ETF_T1, suspended=False, price=3.527),
        AccountView(1_000_000, {}),
        rulebook,
        ExecutionConfig(ExecutionPrice.OPEN, 0.0, 1.0),
    )

    fill = batch.results[0]
    assert isinstance(fill, FillResult)
    assert fill.reference_price == 3.527
    assert fill.requested_reference_value_fen == 35_270
    assert fill.price == 3.527
    assert fill.gross_value_fen == 35_270
    assert fill.fees.as_tuple() == (500, 0, 0, 500)
    assert batch.ending_cash_fen == 964_230


def test_execution_quality_uses_raw_reference_price_before_slippage() -> None:
    """成交质量参考价必须是滑点前原始价，请求金额按完整 ETF 精度取整。"""
    batch = ExecutionModel().execute(
        [OrderIntent(_ETF_T1, OrderSide.BUY, 100, "TEST")],
        _market(_ETF_T1, suspended=False, price=3.527),
        AccountView(1_000_000, {}),
        AShareRuleBook.load(_RULES),
        ExecutionConfig(ExecutionPrice.OPEN, 10.0, 1.0),
    )

    fill = batch.results[0]
    assert isinstance(fill, FillResult)
    assert fill.reference_price == 3.527
    assert fill.requested_reference_value_fen == 35_270
    assert fill.price == 3.531
    assert fill.gross_value_fen == 35_310


def test_etf_profiles_are_exact_and_encode_settlement_and_price_limit_groups() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    t1 = rulebook.trading_profile(_ETF_T1, "ETF", Board.MAIN, _DAY_ONE)
    t0 = rulebook.trading_profile(_ETF_T0, "ETF", Board.MAIN, _DAY_ONE)
    twenty_percent = rulebook.trading_profile(_STAR_ETF, "ETF", Board.STAR, _DAY_ONE)
    ten_percent_band = rulebook.price_limits(t1, _DAY_ONE, 3.527, SecurityStatus.NORMAL)
    twenty_percent_band = rulebook.price_limits(
        twenty_percent, _DAY_ONE, 1.0, SecurityStatus.NORMAL
    )

    assert (t1.settlement_sessions, t0.settlement_sessions) == (1, 0)
    assert ten_percent_band is not None
    assert (ten_percent_band.upper, ten_percent_band.lower) == (3.88, 3.174)
    assert twenty_percent_band is not None
    assert twenty_percent_band.upper == 1.2
    with pytest.raises(ValueError, match="ETF"):
        rulebook.trading_profile(_MISSING_ETF, "ETF", Board.MAIN, _DAY_ONE)


def test_etf_fee_profile_has_no_stamp_duty_or_transfer_fee_on_sell() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    profile = rulebook.trading_profile(_ETF_T1, "ETF", Board.MAIN, _DAY_ONE)
    fees = rulebook.fees(
        SimulatedFill(_ETF_T1, _DAY_ONE, Side.SELL, 100, 3.527), profile
    )

    assert fees.stamp_duty_cents == 0
    assert fees.transfer_fee_cents == 0


def test_star_profile_uses_200_share_minimum_then_one_share_increments() -> None:
    profile = AShareRuleBook.load(_RULES).trading_profile(
        _STAR_STOCK, "STOCK", Board.STAR, _DAY_ONE
    )

    assert not profile.is_quantity_valid(Side.BUY, 199)
    assert profile.is_quantity_valid(Side.BUY, 200)
    assert profile.is_quantity_valid(Side.BUY, 201)
    assert profile.normalize_quantity(Side.SELL, 199, position_quantity=199) == 199
    assert profile.normalize_quantity(Side.SELL, 198, position_quantity=199) == 0


@pytest.mark.parametrize(
    ("settlement_sessions", "day_one_sellable"), [(0, 100), (1, 0)]
)
def test_account_uses_profile_settlement_sessions(
    settlement_sessions: int, day_one_sellable: int
) -> None:
    calendar = TradingCalendar(_DAY_ONE, _DAY_TWO, (_DAY_ONE, _DAY_TWO))
    account = PortfolioAccount(1_000_000, calendar)
    account.begin_session(_DAY_ONE)
    account.apply(ExecutionBatch(_DAY_ONE, (_buy_fill(settlement_sessions),), 964_230))

    snapshot = account.mark_to_market(_DAY_ONE, {_ETF_T1: 3.527})

    assert snapshot.positions[0].sellable_quantity == day_one_sellable


def test_account_carries_last_valid_mark_over_suspended_session() -> None:
    calendar = TradingCalendar(_DAY_ONE, _DAY_TWO, (_DAY_ONE, _DAY_TWO))
    account = PortfolioAccount(1_000_000, calendar)
    account.begin_session(_DAY_ONE)
    account.apply(ExecutionBatch(_DAY_ONE, (_buy_fill(1),), 964_230))
    first = account.mark_to_market(_DAY_ONE, {_ETF_T1: 3.527})
    account.begin_session(_DAY_TWO)

    second = account.mark_to_market(_DAY_TWO, {})

    assert second.positions[0].market_value_fen == first.positions[0].market_value_fen
    assert second.positions[0].sellable_quantity == 100


def test_account_fails_closed_when_holding_has_no_current_or_historical_mark() -> None:
    calendar = TradingCalendar(_DAY_ONE, _DAY_TWO, (_DAY_ONE, _DAY_TWO))
    account = PortfolioAccount(1_000_000, calendar)
    account.begin_session(_DAY_ONE)
    account.apply(ExecutionBatch(_DAY_ONE, (_buy_fill(1),), 964_230))

    with pytest.raises(ValueError, match="missing close"):
        account.mark_to_market(_DAY_ONE, {})
