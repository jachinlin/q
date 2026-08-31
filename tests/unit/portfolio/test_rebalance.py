"""验证策略侧目标权重到整数订单的确定性转换。"""

from __future__ import annotations

from datetime import date

import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.rebalance import RebalancePlanner
from quant_research.strategies.base import (
    AccountView,
    OrderIntent,
    OrderSide,
    TargetWeights,
)

_A = InstrumentId.parse("600001.SH")
_B = InstrumentId.parse("600002.SH")
_C = InstrumentId.parse("000001.SZ")


def _targets(weights: dict[InstrumentId, float]) -> TargetWeights:
    return TargetWeights(date(2026, 8, 20), date(2026, 8, 21), weights)


def test_rebalance_sells_before_buys_and_rounds_targets_to_lots() -> None:
    account = AccountView(
        cash_fen=0,
        positions={_A: 250, _B: 100},
        sellable={_A: 250, _B: 100},
        equity_fen=250_000,
    )

    result = RebalancePlanner().plan(
        _targets({_B: 0.6, _C: 0.4}),
        account,
        {_A: 10.0, _B: 10.0, _C: 10.0},
    )

    assert result == (
        OrderIntent(_A, OrderSide.SELL, 250, "TARGET_REBALANCE"),
        OrderIntent(_C, OrderSide.BUY, 100, "TARGET_REBALANCE"),
    )


def test_rebalance_caps_sales_at_sellable_quantity() -> None:
    account = AccountView(
        cash_fen=0,
        positions={_A: 250},
        sellable={_A: 100},
        equity_fen=250_000,
    )

    result = RebalancePlanner().plan(_targets({}), account, {_A: 10.0})

    assert result == (OrderIntent(_A, OrderSide.SELL, 100, "TARGET_REBALANCE"),)


def test_rebalance_skips_target_without_reference_price() -> None:
    account = AccountView(
        cash_fen=250_000,
        positions={},
        sellable={},
        equity_fen=250_000,
    )

    result = RebalancePlanner().plan(
        _targets({_A: 0.5, _B: 0.5}),
        account,
        {_A: 10.0},
    )

    assert result == (OrderIntent(_A, OrderSide.BUY, 100, "TARGET_REBALANCE"),)


@pytest.mark.parametrize("price", [0.0, -1.0, float("nan")])
def test_rebalance_rejects_invalid_reference_price(price: float) -> None:
    account = AccountView(100_000, {_A: 100}, {_A: 100}, 100_000)

    with pytest.raises(ValueError, match="reference price"):
        RebalancePlanner().plan(_targets({_A: 1.0}), account, {_A: price})


def test_rebalance_rejects_invalid_lot_size() -> None:
    account = AccountView(100_000, {}, {}, 100_000)
    planner = RebalancePlanner(lambda _instrument, _side: 0)

    with pytest.raises(ValueError, match="lot size"):
        planner.plan(_targets({_A: 1.0}), account, {_A: 10.0})
