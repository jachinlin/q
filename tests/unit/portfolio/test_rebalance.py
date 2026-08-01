"""Public contract tests for deterministic rebalance order planning."""

from datetime import date

import pytest

from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio import (
    OrderSide,
    RebalancePlanner,
    TargetPortfolio,
    TargetPosition,
)

_A = InstrumentId.parse("SSE:600001")
_B = InstrumentId.parse("SSE:600002")
_C = InstrumentId.parse("SZSE:000001")


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
        lot_sizes={_A: 100, _B: 100, _C: 100},
    )

    assert [
        (intent.instrument_id, intent.side, intent.quantity)
        for intent in result.intents
    ] == [
        (_A, OrderSide.SELL, 200),
        (_B, OrderSide.BUY, 100),
        (_C, OrderSide.BUY, 100),
    ]
    assert result.projected_cash_fen == 0
    assert all(intent.reason_code == "TARGET_REBALANCE" for intent in result.intents)


def test_rebalance_omits_zero_orders_and_preserves_existing_odd_lots() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_A, 1.0)),
        {_A: 50},
        cash_fen=0,
        execution_prices={_A: 10.0},
        lot_sizes={_A: 100},
    )

    assert result.intents == ()
    assert result.projected_cash_fen == 0


def test_rebalance_scales_buys_to_available_cash_and_keeps_residual_cash() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_A, 1.0)),
        {},
        cash_fen=10_500,
        execution_prices={_A: 1.0},
        lot_sizes={_A: 100},
    )

    assert [
        (intent.instrument_id, intent.side, intent.quantity)
        for intent in result.intents
    ] == [(_A, OrderSide.BUY, 100)]
    assert result.projected_cash_fen == 500


def test_rebalance_rejects_positive_prices_that_round_to_zero_fen() -> None:
    with pytest.raises(ValueError, match="price"):
        RebalancePlanner().plan(
            _target(_position(_A, 1.0)),
            {},
            cash_fen=10_000,
            execution_prices={_A: 0.001},
            lot_sizes={_A: 100},
        )


def test_rebalance_nets_each_instrument_to_at_most_one_intent() -> None:
    result = RebalancePlanner().plan(
        _target(_position(_A, 0.5), _position(_B, 0.5)),
        {_A: 1_000, _B: 0},
        cash_fen=0,
        execution_prices={_A: 10.0, _B: 10.0},
        lot_sizes={_A: 100, _B: 100},
    )

    assert [
        (intent.instrument_id, intent.side, intent.quantity)
        for intent in result.intents
    ] == [
        (_A, OrderSide.SELL, 500),
        (_B, OrderSide.BUY, 500),
    ]


@pytest.mark.parametrize(
    ("positions", "cash_fen", "prices", "lots", "message"),
    [
        ({_A: 100}, 0, {}, {_A: 100}, "price"),
        ({_A: 100}, 0, {_A: 10.0}, {_A: 0}, "lot"),
        ({_A: -1}, 0, {_A: 10.0}, {_A: 100}, "current_positions"),
    ],
)
def test_rebalance_fails_closed_for_invalid_execution_inputs(
    positions: dict[InstrumentId, int],
    cash_fen: int,
    prices: dict[InstrumentId, float],
    lots: dict[InstrumentId, int],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        RebalancePlanner().plan(
            _target(_position(_A, 1.0)), positions, cash_fen, prices, lots
        )
