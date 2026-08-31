"""回测市场切片、交易画像、执行精度与账户交收的契约测试。"""

from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from quant_research.backtest import (
    AccountView,
    AShareRuleBook,
    CorporateAction,
    CorporateActionInstrumentType,
    CorporateActionType,
    ExecutionBatch,
    ExecutionConfig,
    ExecutionModel,
    ExecutionPrice,
    ExecutionReason,
    FeeBreakdown,
    FillResult,
    InstrumentTradingProfile,
    MappedCorporateAction,
    MarketSlice,
    PortfolioAccount,
    PriceLimitParameters,
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
_MAIN_STOCK = InstrumentId.parse("000001.SZ")
_DAY_ONE = date(2026, 7, 30)
_DAY_TWO = date(2026, 7, 31)


def test_price_limit_parameters_preserve_exact_integer_rounding_contract() -> None:
    """批量参数必须与 Decimal HALF_UP 规则使用同一精确比例和价格单位。"""
    rulebook = AShareRuleBook.load(_RULES)
    profile = rulebook.trading_profile(
        _MAIN_STOCK, "STOCK", Board.MAIN, _DAY_ONE
    )

    parameters = rulebook.price_limit_parameters(
        profile, _DAY_ONE, SecurityStatus.NORMAL
    )

    assert parameters == PriceLimitParameters(1, 10, 100, 1)
    assert rulebook.price_limits(
        profile, _DAY_ONE, 0.95, SecurityStatus.NORMAL
    ).upper == 1.05
    five_cent_profile = InstrumentTradingProfile(
        profile_id="FIVE_CENT",
        instrument_type="STOCK",
        price_tick=Decimal("0.05"),
        buy_minimum=100,
        buy_increment=100,
        sell_minimum=100,
        sell_increment=100,
        allow_full_odd_lot_sell=True,
        settlement_sessions=1,
        price_limit_group="MAIN",
        fee_group="STOCK",
    )
    assert rulebook.price_limit_parameters(
        five_cent_profile, _DAY_ONE, SecurityStatus.NORMAL
    ) == PriceLimitParameters(1, 10, 100, 5)
    assert rulebook.price_limits(
        five_cent_profile, _DAY_ONE, 1.02, SecurityStatus.NORMAL
    ).upper == 1.1


def _market(
    instrument: InstrumentId,
    *,
    suspended: bool,
    price: float | None,
    instrument_type: str = "ETF",
    board: str = "MAIN",
    security_status: str = "NORMAL",
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
        "security_status": [security_status],
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


def test_market_slice_accepts_every_rulebook_status_and_supported_board() -> None:
    """行情切片必须接受交易规则已定义的无涨跌停状态和北交所板块。"""
    market = _market(
        InstrumentId.parse("920001.BJ"),
        suspended=False,
        price=10.0,
        instrument_type="STOCK",
        board="BSE",
        security_status="NO_LIMIT",
    )

    assert market.bars.item(0, "security_status") == SecurityStatus.NO_LIMIT.value
    assert market.bars.item(0, "board") == Board.BSE.value


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


def test_account_accrues_pays_and_distributes_stock_at_explicit_phases() -> None:
    """登记日、除权日、支付日和上市日必须形成互不重复的账户状态。"""
    day_three = date(2026, 8, 3)
    day_four = date(2026, 8, 4)
    calendar = TradingCalendar(
        _DAY_ONE, day_four, (_DAY_ONE, _DAY_TWO, day_three, day_four)
    )
    account = PortfolioAccount(1_000_000, calendar)
    cash = MappedCorporateAction(
        CorporateAction(
            "cash-event", _ETF_T1, CorporateActionInstrumentType.FUND,
            CorporateActionType.CASH_DIVIDEND, 2, date(2026, 7, 27),
            date(2026, 7, 28), _DAY_ONE, _DAY_TWO, day_three, None,
            Decimal("0.12345"), Decimal(0),
        ),
        _DAY_ONE, _DAY_TWO, day_three, None,
    )
    stock = MappedCorporateAction(
        CorporateAction(
            "stock-event", _ETF_T1, CorporateActionInstrumentType.STOCK,
            CorporateActionType.STOCK_DISTRIBUTION, 3, date(2026, 7, 27),
            date(2026, 7, 28), _DAY_ONE, _DAY_TWO, None, day_three,
            Decimal(0), Decimal("0.15"),
        ),
        _DAY_ONE, _DAY_TWO, None, day_three,
    )
    actions = (cash, stock)
    account.begin_session(_DAY_ONE)
    account.apply(ExecutionBatch(_DAY_ONE, (_buy_fill(0),), 964_230))
    account.mark_to_market(_DAY_ONE, {_ETF_T1: 3.527})
    account.lock_corporate_actions_after_close(actions)

    account.begin_session(_DAY_TWO)
    account.apply_corporate_actions_before_open(actions)
    account.apply_corporate_actions_before_open(actions)
    open_view = account.execution_view()
    ex_snapshot = account.mark_to_market(_DAY_TWO, {_ETF_T1: 3.067})

    assert open_view.cash_fen == 964_230
    assert open_view.total_quantities[_ETF_T1] == 115
    assert open_view.sellable_quantities[_ETF_T1] == 100
    assert ex_snapshot.dividend_receivable_fen == 1_235
    assert ex_snapshot.positions[0].cost_basis_fen == 35_770

    account.begin_session(day_three)
    account.apply_corporate_actions_before_open(actions)
    paid = account.mark_to_market(day_three, {_ETF_T1: 3.067})

    assert paid.cash_fen == 965_465
    assert paid.dividend_receivable_fen == 0
    assert paid.positions[0].sellable_quantity == 115
    event_types = [event.event_type.value for event in account.ledger]
    assert event_types.count("DIVIDEND_ACCRUAL") == 1
    assert event_types.count("DIVIDEND_PAYMENT") == 1
    assert event_types.count("STOCK_DISTRIBUTION") == 1


def test_account_applies_fund_split_before_open_without_changing_cost() -> None:
    """基金拆分使用复权因子倍率增加份额，并使新增份额当日可卖。"""
    calendar = TradingCalendar(_DAY_ONE, _DAY_TWO, (_DAY_ONE, _DAY_TWO))
    account = PortfolioAccount(1_000_000, calendar)
    account.begin_session(_DAY_ONE)
    account.apply(ExecutionBatch(_DAY_ONE, (_buy_fill(0),), 964_230))
    account.mark_to_market(_DAY_ONE, {_ETF_T1: 3.527})
    split = MappedCorporateAction(
        CorporateAction(
            "split-event", _ETF_T1, CorporateActionInstrumentType.FUND,
            CorporateActionType.FUND_SPLIT, 0, None, None, None, _DAY_TWO,
            None, None, Decimal(0), Decimal(5), Decimal(1), Decimal("5.002"),
        ),
        None, _DAY_TWO, None, None,
    )

    account.begin_session(_DAY_TWO)
    account.apply_corporate_actions_before_open((split,))
    view = account.execution_view()
    snapshot = account.mark_to_market(_DAY_TWO, {_ETF_T1: 0.7054})

    assert view.total_quantities[_ETF_T1] == 500
    assert view.sellable_quantities[_ETF_T1] == 500
    assert snapshot.positions[0].cost_basis_fen == 35_770
    assert snapshot.positions[0].market_value_fen == 35_270
    assert account.dividend_records[0].distributed_quantity == 400
    assert account.ledger[-1].event_type.value == "FUND_SPLIT"
