"""Public contract tests for deterministic A-share daily execution."""

from datetime import date

import polars as pl
import pytest

from quant_core.backtest.execution import ExecutionModel
from quant_core.backtest.models import (
    AccountView,
    ExecutionConfig,
    ExecutionPrice,
    ExecutionReason,
    FillResult,
    MarketSlice,
    RejectResult,
)
from quant_core.backtest.rulebook import AShareRuleBook, FeeBreakdown, SimulatedFill
from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio.rebalance import OrderIntent, OrderSide

_DAY = date(2024, 1, 2)
_A = InstrumentId.parse("SSE:600001")
_B = InstrumentId.parse("SSE:600002")
_RULEBOOK = AShareRuleBook.load(
    __import__("pathlib").Path("configs/rules/a_share_v1.yaml")
)


class _NoBandMinimumFeeRuleBook:
    """Minimal real-value rulebook fixture for the negative sell proceeds edge."""

    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int:
        return 100

    def price_limits(
        self,
        instrument: InstrumentId,
        trade_date: date,
        prev_close: float,
        status: object,
    ) -> None:
        return None

    def fees(self, fill: SimulatedFill) -> FeeBreakdown:
        fee = 500 if fill.instrument == _A else 0
        return FeeBreakdown(fee, 0, 0, fee)


def _intent(
    instrument: InstrumentId, side: OrderSide, quantity: int = 100
) -> OrderIntent:
    return OrderIntent(instrument, side, quantity, "TEST")


def _market(*rows: dict[str, object]) -> MarketSlice:
    default = {
        "instrument_id": _A.canonical(),
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.0,
        "preclose": 10.0,
        "volume": 10_000,
        "is_suspended": False,
        "security_status": "NORMAL",
    }
    return MarketSlice(
        _DAY,
        pl.DataFrame([{**default, **row} for row in rows] or [default]),
    )


def _config(
    *,
    reference: ExecutionPrice = ExecutionPrice.OPEN,
    bps: float = 0.0,
    rate: float = 1.0,
) -> ExecutionConfig:
    return ExecutionConfig(reference, bps, rate)


def _execute(
    intents: list[OrderIntent],
    market: MarketSlice | None = None,
    cash: int = 1_000_000,
    sellable: dict[InstrumentId, int] | None = None,
    config: ExecutionConfig | None = None,
):
    return ExecutionModel().execute(
        intents,
        market or _market(),
        AccountView(cash, sellable or {}),
        _RULEBOOK,
        config or _config(),
    )


@pytest.mark.parametrize(
    ("intent", "row", "cash", "sellable", "rate", "reason"),
    [
        (
            _intent(_A, OrderSide.BUY),
            {"is_suspended": True},
            1_000_000,
            {},
            1.0,
            ExecutionReason.SUSPENDED,
        ),
        (
            _intent(_A, OrderSide.BUY),
            {"low": 11.0, "high": 11.0, "open": 11.0, "close": 11.0},
            1_000_000,
            {},
            1.0,
            ExecutionReason.LIMIT_UP_BUY_BLOCKED,
        ),
        (
            _intent(_A, OrderSide.SELL),
            {"low": 9.0, "high": 9.0, "open": 9.0, "close": 9.0},
            1_000_000,
            {_A: 100},
            1.0,
            ExecutionReason.LIMIT_DOWN_SELL_BLOCKED,
        ),
        (
            _intent(_A, OrderSide.BUY, 50),
            {},
            1_000_000,
            {},
            1.0,
            ExecutionReason.ODD_LOT,
        ),
        (
            _intent(_A, OrderSide.SELL),
            {},
            1_000_000,
            {},
            1.0,
            ExecutionReason.INSUFFICIENT_SELLABLE,
        ),
        (
            _intent(_A, OrderSide.BUY),
            {"volume": 99},
            1_000_000,
            {},
            1.0,
            ExecutionReason.VOLUME_CAP,
        ),
        (
            _intent(_B, OrderSide.BUY),
            None,
            1_000_000,
            {},
            1.0,
            ExecutionReason.NO_MARKET_DATA,
        ),
    ],
)
def test_execution_rejects_in_priority_order(
    intent: OrderIntent,
    row: dict[str, object] | None,
    cash: int,
    sellable: dict[InstrumentId, int],
    rate: float,
    reason: ExecutionReason,
) -> None:
    base = _market().bars.row(0, named=True)
    market = _market({**base, **row}) if row is not None else _market()
    result = _execute([intent], market, cash, sellable, _config(rate=rate)).results[0]

    assert isinstance(result, RejectResult)
    assert result.reason_code is reason


def test_execution_fills_and_preserves_input_order_with_sell_cash_before_buy() -> None:
    result = _execute(
        [_intent(_A, OrderSide.SELL), _intent(_B, OrderSide.BUY)],
        _market(
            {
                "instrument_id": _A.canonical(),
                "open": 10.0,
                "high": 10.5,
                "low": 9.5,
                "close": 10.0,
                "preclose": 10.0,
                "volume": 10_000,
                "is_suspended": False,
                "security_status": "NORMAL",
            },
            {
                "instrument_id": _B.canonical(),
                "open": 9.0,
                "high": 10.5,
                "low": 9.0,
                "close": 9.0,
                "preclose": 10.0,
                "volume": 10_000,
                "is_suspended": False,
                "security_status": "NORMAL",
            },
        ),
        cash=0,
        sellable={_A: 100},
    )

    assert [type(item) for item in result.results] == [FillResult, FillResult]
    assert [item.intent.side for item in result.results] == [
        OrderSide.SELL,
        OrderSide.BUY,
    ]
    assert result.ending_cash_fen >= 0


def test_execution_applies_directional_slippage_and_lot_rounded_volume_capacity() -> (
    None
):
    buy = _execute(
        [_intent(_A, OrderSide.BUY, 500)],
        _market({"volume": 550}),
        config=_config(bps=100, rate=0.5),
    ).results[0]
    sell = _execute(
        [_intent(_A, OrderSide.SELL, 100)],
        _market(),
        sellable={_A: 100},
        config=_config(bps=100),
    ).results[0]

    assert isinstance(buy, FillResult)
    assert (buy.price, buy.filled_quantity, buy.unfilled_quantity, buy.reason_code) == (
        10.10,
        200,
        300,
        ExecutionReason.VOLUME_CAP,
    )
    assert isinstance(sell, FillResult)
    assert sell.price == 9.90


def test_execution_limits_buy_by_cash_after_slippage_and_fees() -> None:
    result = _execute(
        [_intent(_A, OrderSide.BUY, 200)], cash=150_000, config=_config(bps=100)
    ).results[0]

    assert isinstance(result, FillResult)
    assert (result.filled_quantity, result.unfilled_quantity, result.reason_code) == (
        100,
        100,
        ExecutionReason.INSUFFICIENT_CASH,
    )


def test_execution_rejects_sell_when_minimum_fee_exceeds_cash_and_proceeds() -> None:
    low_price_market = _market(
        {
            "instrument_id": _A.canonical(),
            "open": 0.01,
            "high": 0.01,
            "low": 0.01,
            "close": 0.01,
            "preclose": 0.01,
        },
        {
            "instrument_id": _B.canonical(),
            "open": 0.01,
            "high": 0.01,
            "low": 0.01,
            "close": 0.01,
            "preclose": 0.01,
        },
    )
    batch = ExecutionModel().execute(
        [_intent(_A, OrderSide.SELL), _intent(_B, OrderSide.SELL)],
        low_price_market,
        AccountView(0, {_A: 100, _B: 100}),
        _NoBandMinimumFeeRuleBook(),
        _config(),
    )

    assert [type(result) for result in batch.results] == [RejectResult, FillResult]
    assert batch.results[0].reason_code is ExecutionReason.INSUFFICIENT_CASH
    assert batch.results[1].reason_code is ExecutionReason.FILLED
    assert batch.ending_cash_fen == 100


def test_execution_uses_preclose_rulebook_band_not_intraday_range_alone() -> None:
    result = _execute(
        [_intent(_A, OrderSide.BUY)],
        _market(
            {"preclose": 10.0, "low": 11.0, "high": 12.0, "open": 11.0, "close": 11.0}
        ),
    ).results[0]

    assert isinstance(result, RejectResult)
    assert result.reason_code is ExecutionReason.LIMIT_UP_BUY_BLOCKED


def test_execution_fails_closed_for_invalid_market_duplicate_intents_and_keeps_empty_batch() -> (
    None
):
    with pytest.raises(ValueError, match="market"):
        _execute([_intent(_A, OrderSide.BUY)], _market({"volume": -1}))
    with pytest.raises(ValueError, match="unique"):
        _execute([_intent(_A, OrderSide.BUY), _intent(_A, OrderSide.SELL)])

    batch = _execute([], cash=123)
    assert batch.results == ()
    assert batch.ending_cash_fen == 123
