"""Public contract tests for deterministic rebalance order planning."""

from datetime import date
from decimal import Decimal
from typing import Any, cast

import pytest

from quant_research.backtest import InstrumentTradingProfile
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio import (
    OrderSide,
    RebalancePlanner,
    TargetPortfolio,
    TargetPosition,
)

_A = InstrumentId.parse("600001.SH")
_B = InstrumentId.parse("600002.SH")
_C = InstrumentId.parse("000001.SZ")


def _profile() -> InstrumentTradingProfile:
    return InstrumentTradingProfile(
        profile_id="TEST_STOCK",
        instrument_type="STOCK",
        price_tick=Decimal("0.01"),
        buy_minimum=100,
        buy_increment=100,
        sell_minimum=100,
        sell_increment=100,
        allow_full_odd_lot_sell=True,
        settlement_sessions=1,
        price_limit_group="STOCK_MAIN",
        fee_group="STOCK",
    )


def _target(*positions: TargetPosition) -> TargetPortfolio:
    return TargetPortfolio(
        signal_date=date(2026, 7, 30),
        execute_date=date(2026, 7, 31),
        positions=positions,
        cash_weight=1.0 - sum(position.target_weight for position in positions),
    )


def _position(instrument: InstrumentId, weight: float) -> TargetPosition:
    return TargetPosition(instrument, weight, 1.0, "SELECTED")


def test_rebalance_sells_before_buys_with_lot_rounded_quantities() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_B, 0.6), _position(_C, 0.4)),
        {_A: 250, _B: 100},
        cash_fen=0,
        execution_prices={_A: 10.0, _B: 10.0, _C: 10.0},
        trading_profiles={_A: _profile(), _B: _profile(), _C: _profile()},
    )

    assert [
        (intent.instrument_id, intent.side, intent.quantity)
        for intent in result.intents
    ] == [
        (_A, OrderSide.SELL, 250),
        (_B, OrderSide.BUY, 100),
        (_C, OrderSide.BUY, 100),
    ]
    assert result.projected_cash_fen == 50_000
    assert all(intent.reason_code == "TARGET_REBALANCE" for intent in result.intents)


def test_rebalance_omits_zero_orders_and_preserves_existing_odd_lots() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_A, 1.0)),
        {_A: 50},
        cash_fen=0,
        execution_prices={_A: 10.0},
        trading_profiles={_A: _profile()},
    )

    assert result.intents == ()
    assert result.projected_cash_fen == 0


def test_rebalance_scales_buys_to_available_cash_and_keeps_residual_cash() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_A, 1.0)),
        {},
        cash_fen=10_500,
        execution_prices={_A: 1.0},
        trading_profiles={_A: _profile()},
    )

    assert [
        (intent.instrument_id, intent.side, intent.quantity)
        for intent in result.intents
    ] == [(_A, OrderSide.BUY, 100)]
    assert result.projected_cash_fen == 500


def test_rebalance_preserves_sub_cent_unit_price_until_quantity_multiplication() -> (
    None
):
    result = RebalancePlanner().plan(
        _target(_position(_A, 1.0)),
        {},
        cash_fen=10_000,
        execution_prices={_A: 0.001},
        trading_profiles={_A: _profile()},
    )

    assert [(intent.side, intent.quantity) for intent in result.intents] == [
        (OrderSide.BUY, 100_000)
    ]
    assert result.projected_cash_fen == 0


def test_rebalance_nets_each_instrument_to_at_most_one_intent() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_A, 0.5), _position(_B, 0.5)),
        {_A: 1_000, _B: 0},
        cash_fen=0,
        execution_prices={_A: 10.0, _B: 10.0},
        trading_profiles={_A: _profile(), _B: _profile()},
    )

    assert [
        (intent.instrument_id, intent.side, intent.quantity)
        for intent in result.intents
    ] == [
        (_A, OrderSide.SELL, 500),
        (_B, OrderSide.BUY, 500),
    ]


@pytest.mark.parametrize(
    ("positions", "cash_fen", "prices", "profiles", "message"),
    [
        ({_A: 100}, 0, {}, {_A: _profile()}, "price"),
        (
            {_A: 100},
            0,
            {_A: 10.0},
            {_A: cast(Any, 0)},
            "trading profile",
        ),
        ({_A: -1}, 0, {_A: 10.0}, {_A: _profile()}, "current_positions"),
    ],
)
def test_rebalance_fails_closed_for_invalid_execution_inputs(
    positions: dict[InstrumentId, int],
    cash_fen: int,
    prices: dict[InstrumentId, float],
    profiles: dict[InstrumentId, InstrumentTradingProfile],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        RebalancePlanner().plan(
            _target(_position(_A, 1.0)), positions, cash_fen, prices, profiles
        )
