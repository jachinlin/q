"""Deterministic sell-first, lot-rounded rebalance planning."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from math import floor, isfinite

from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio.constructor import TargetPortfolio


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class OrderIntent:
    instrument_id: InstrumentId
    side: OrderSide
    quantity: int
    reason_code: str


@dataclass(frozen=True, slots=True)
class RebalancePlan:
    intents: tuple[OrderIntent, ...]
    projected_cash_fen: int


class RebalancePlanner:
    """Produce non-executing order intentions from a target portfolio."""

    def plan(
        self,
        target: TargetPortfolio,
        current_positions: Mapping[InstrumentId, int],
        cash_fen: int,
        execution_prices: Mapping[InstrumentId, float],
        lot_sizes: Mapping[InstrumentId, int],
    ) -> RebalancePlan:
        _validate_cash(cash_fen)
        _validate_positions(current_positions)
        target_weights = _target_weights(target)
        identifiers = current_positions.keys() | target_weights.keys()
        prices_fen = {
            instrument_id: _price_fen(instrument_id, execution_prices)
            for instrument_id in identifiers
        }
        lots = {
            instrument_id: _lot_size(instrument_id, lot_sizes)
            for instrument_id in identifiers
        }
        total_equity_fen = cash_fen + sum(
            quantity * prices_fen[instrument_id]
            for instrument_id, quantity in current_positions.items()
        )
        target_quantities = {
            instrument_id: _target_quantity(
                total_equity_fen,
                target_weights.get(instrument_id, 0.0),
                prices_fen[instrument_id],
                lots[instrument_id],
            )
            for instrument_id in identifiers
        }
        cash_after_sales = cash_fen
        intents: list[OrderIntent] = []
        for instrument_id in sorted(identifiers, key=InstrumentId.canonical):
            current_quantity = current_positions.get(instrument_id, 0)
            sell_quantity = _round_down_lot(
                max(current_quantity - target_quantities[instrument_id], 0),
                lots[instrument_id],
            )
            if sell_quantity > 0:
                intents.append(
                    OrderIntent(
                        instrument_id, OrderSide.SELL, sell_quantity, "TARGET_REBALANCE"
                    )
                )
                cash_after_sales += sell_quantity * prices_fen[instrument_id]
        projected_cash = cash_after_sales
        for position in target.positions:
            instrument_id = position.instrument_id
            desired_quantity = _round_down_lot(
                max(
                    target_quantities[instrument_id]
                    - current_positions.get(instrument_id, 0),
                    0,
                ),
                lots[instrument_id],
            )
            affordable_quantity = _round_down_lot(
                projected_cash // prices_fen[instrument_id], lots[instrument_id]
            )
            buy_quantity = min(desired_quantity, affordable_quantity)
            if buy_quantity > 0:
                intents.append(
                    OrderIntent(
                        instrument_id, OrderSide.BUY, buy_quantity, "TARGET_REBALANCE"
                    )
                )
                projected_cash -= buy_quantity * prices_fen[instrument_id]
        return RebalancePlan(tuple(intents), projected_cash)


def _validate_cash(cash_fen: int) -> None:
    if not isinstance(cash_fen, int) or isinstance(cash_fen, bool) or cash_fen < 0:
        raise ValueError("cash_fen must be a nonnegative integer")


def _validate_positions(current_positions: Mapping[InstrumentId, int]) -> None:
    for instrument_id, quantity in current_positions.items():
        if not isinstance(instrument_id, InstrumentId):
            raise TypeError("current_positions keys must be InstrumentId")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity < 0:
            raise ValueError("current_positions must contain nonnegative integers")


def _target_weights(target: TargetPortfolio) -> dict[InstrumentId, float]:
    weights: dict[InstrumentId, float] = {}
    for position in target.positions:
        if position.instrument_id in weights:
            raise ValueError("target positions must be unique")
        if not isfinite(position.target_weight) or position.target_weight < 0:
            raise ValueError("target weights must be finite and nonnegative")
        weights[position.instrument_id] = position.target_weight
    return weights


def _price_fen(
    instrument_id: InstrumentId, execution_prices: Mapping[InstrumentId, float]
) -> int:
    try:
        price = execution_prices[instrument_id]
    except KeyError as error:
        raise ValueError(f"missing price for {instrument_id.canonical()}") from error
    if (
        not isinstance(price, (float, int))
        or isinstance(price, bool)
        or not isfinite(price)
        or price <= 0
    ):
        raise ValueError("price must be finite and positive")
    price_fen = int(
        (Decimal(str(price)) * Decimal(100)).quantize(Decimal(1), ROUND_HALF_UP)
    )
    if price_fen <= 0:
        raise ValueError("price must round to at least one fen")
    return price_fen


def _lot_size(
    instrument_id: InstrumentId, lot_sizes: Mapping[InstrumentId, int]
) -> int:
    try:
        lot_size = lot_sizes[instrument_id]
    except KeyError as error:
        raise ValueError(f"missing lot for {instrument_id.canonical()}") from error
    if not isinstance(lot_size, int) or isinstance(lot_size, bool) or lot_size <= 0:
        raise ValueError("lot size must be a positive integer")
    return lot_size


def _target_quantity(
    total_equity_fen: int, target_weight: float, price_fen: int, lot_size: int
) -> int:
    target_value_fen = floor(total_equity_fen * target_weight)
    return _round_down_lot(target_value_fen // price_fen, lot_size)


def _round_down_lot(quantity: int, lot_size: int) -> int:
    return quantity // lot_size * lot_size
