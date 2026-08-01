"""Vectorized market preparation and deterministic daily A-share execution."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

import polars as pl

from quant_core.backtest.models import (
    AccountView,
    ExecutionBatch,
    ExecutionConfig,
    ExecutionPrice,
    ExecutionReason,
    FillResult,
    MarketSlice,
    RejectResult,
)
from quant_core.backtest.rulebook import (
    FeeBreakdown,
    MarketRuleBook,
    PriceBand,
    SecurityStatus,
    Side,
    SimulatedFill,
)
from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio.rebalance import OrderIntent, OrderSide

_CENT = Decimal(100)
_TICK = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class ExecutionModel:
    """Execute a stable sequence of daily order intents against one market slice."""

    def execute(
        self,
        intents: Sequence[OrderIntent],
        market: MarketSlice,
        account: AccountView,
        rulebook: MarketRuleBook,
        config: ExecutionConfig,
    ) -> ExecutionBatch:
        ordered = _validate_intents(intents)
        if not isinstance(market, MarketSlice):
            raise TypeError("market must be a MarketSlice")
        if not isinstance(account, AccountView):
            raise TypeError("account must be an AccountView")
        if not isinstance(config, ExecutionConfig):
            raise TypeError("config must be an ExecutionConfig")
        if not ordered:
            return ExecutionBatch(market.trade_date, (), account.cash_fen)
        prepared = _prepare_market(ordered, market, config)
        cash = account.cash_fen
        results: list[FillResult | RejectResult] = []
        for row in prepared.iter_rows(named=True):
            intent = ordered[row["intent_index"]]
            result, cash = _execute_one(
                intent, row, market, cash, account, rulebook, config
            )
            results.append(result)
        return ExecutionBatch(market.trade_date, tuple(results), cash)


def _prepare_market(
    intents: tuple[OrderIntent, ...], market: MarketSlice, config: ExecutionConfig
) -> pl.DataFrame:
    intent_frame = pl.DataFrame(
        {
            "intent_index": list(range(len(intents))),
            "instrument_id": [intent.instrument_id.canonical() for intent in intents],
        },
        schema={"intent_index": pl.Int64, "instrument_id": pl.String},
    )
    return (
        intent_frame.join(market.bars, on="instrument_id", how="left")
        .with_columns(
            (pl.col("volume") * config.max_volume_participation)
            .floor()
            .cast(pl.Int64)
            .alias("raw_capacity")
        )
        .select("intent_index", *market.bars.columns, "raw_capacity")
        .sort("intent_index")
    )


def _execute_one(
    intent: OrderIntent,
    row: dict[str, object],
    market: MarketSlice,
    cash: int,
    account: AccountView,
    rulebook: MarketRuleBook,
    config: ExecutionConfig,
) -> tuple[FillResult | RejectResult, int]:
    if row["open"] is None:
        return _reject(intent, market, ExecutionReason.NO_MARKET_DATA), cash
    if row["is_suspended"] is True:
        return _reject(intent, market, ExecutionReason.SUSPENDED), cash
    lot = rulebook.lot_size(intent.instrument_id, market.trade_date)
    if type(lot) is not int or lot <= 0:
        raise ValueError("rulebook lot_size must be a positive integer")
    if intent.quantity % lot != 0:
        return _reject(intent, market, ExecutionReason.ODD_LOT), cash
    status = SecurityStatus(_string(row, "security_status"))
    band = rulebook.price_limits(
        intent.instrument_id, market.trade_date, _float(row, "preclose"), status
    )
    if band is not None:
        if intent.side is OrderSide.BUY and _float(row, "low") >= band.upper:
            return _reject(intent, market, ExecutionReason.LIMIT_UP_BUY_BLOCKED), cash
        if intent.side is OrderSide.SELL and _float(row, "high") <= band.lower:
            return _reject(
                intent, market, ExecutionReason.LIMIT_DOWN_SELL_BLOCKED
            ), cash
    sellable = account.sellable_quantities.get(intent.instrument_id, 0)
    if intent.side is OrderSide.SELL and sellable == 0:
        return _reject(intent, market, ExecutionReason.INSUFFICIENT_SELLABLE), cash
    capacity = _int(row, "raw_capacity") // lot * lot
    if capacity == 0:
        return _reject(intent, market, ExecutionReason.VOLUME_CAP), cash
    price, price_fen = _execution_price(row, intent.side, config, band)
    candidate = min(intent.quantity, capacity)
    if intent.side is OrderSide.SELL:
        candidate = min(candidate, sellable)
    candidate = candidate // lot * lot
    if candidate == 0:
        reason = (
            ExecutionReason.INSUFFICIENT_SELLABLE
            if intent.side is OrderSide.SELL
            else ExecutionReason.VOLUME_CAP
        )
        return _reject(intent, market, reason), cash
    filled = candidate
    fees = _fees(rulebook, intent, market, filled, price)
    if intent.side is OrderSide.BUY:
        filled = _affordable_quantity(
            cash, candidate, lot, price, price_fen, rulebook, intent, market
        )
        if filled == 0:
            return _reject(intent, market, ExecutionReason.INSUFFICIENT_CASH), cash
        fees = _fees(rulebook, intent, market, filled, price)
    gross = filled * price_fen
    if intent.side is OrderSide.SELL and cash + gross < fees.total_cents:
        return _reject(intent, market, ExecutionReason.INSUFFICIENT_CASH), cash
    unfilled = intent.quantity - filled
    reason = _fill_reason(intent, filled, candidate, capacity, sellable)
    result = FillResult(
        intent,
        market.trade_date,
        intent.quantity,
        filled,
        unfilled,
        price,
        gross,
        fees,
        reason,
    )
    if intent.side is OrderSide.BUY:
        return result, cash - gross - fees.total_cents
    return result, cash + gross - fees.total_cents


def _execution_price(
    row: dict[str, object],
    side: OrderSide,
    config: ExecutionConfig,
    band: PriceBand | None,
) -> tuple[float, int]:
    reference_key = "open" if config.reference_price is ExecutionPrice.OPEN else "close"
    reference = Decimal(str(_float(row, reference_key)))
    direction = Decimal(1) if side is OrderSide.BUY else Decimal(-1)
    price = reference * (
        Decimal(1) + direction * Decimal(str(config.slippage_bps)) / Decimal(10_000)
    )
    price = price.quantize(_TICK, rounding=ROUND_HALF_UP)
    if band is not None:
        lower = Decimal(str(band.lower))
        upper = Decimal(str(band.upper))
        price = min(max(price, lower), upper)
    if not price.is_finite() or price <= 0:
        raise ValueError("execution price must be finite and positive")
    price_fen = int((price * _CENT).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    if price_fen <= 0:
        raise ValueError("execution price must round to positive fen")
    return float(price), price_fen


def _fees(
    rulebook: MarketRuleBook,
    intent: OrderIntent,
    market: MarketSlice,
    quantity: int,
    price: float,
) -> FeeBreakdown:
    side = Side.BUY if intent.side is OrderSide.BUY else Side.SELL
    fees = rulebook.fees(
        SimulatedFill(intent.instrument_id, market.trade_date, side, quantity, price)
    )
    if not isinstance(fees, FeeBreakdown):
        raise TypeError("rulebook fees must return FeeBreakdown")
    if any(type(value) is not int or value < 0 for value in fees.as_tuple()):
        raise ValueError("rulebook fees must be nonnegative integer cents")
    if fees.total_cents != sum(fees.as_tuple()[:3]):
        raise ValueError("rulebook fee total is invalid")
    return fees


def _affordable_quantity(
    cash: int,
    candidate: int,
    lot: int,
    price: float,
    price_fen: int,
    rulebook: MarketRuleBook,
    intent: OrderIntent,
    market: MarketSlice,
) -> int:
    low, high = 0, candidate // lot
    while low < high:
        middle = (low + high + 1) // 2
        quantity = middle * lot
        cost = (
            quantity * price_fen
            + _fees(rulebook, intent, market, quantity, price).total_cents
        )
        if cost <= cash:
            low = middle
        else:
            high = middle - 1
    return low * lot


def _fill_reason(
    intent: OrderIntent, filled: int, candidate: int, capacity: int, sellable: int
) -> ExecutionReason:
    if filled == intent.quantity:
        return ExecutionReason.FILLED
    if intent.side is OrderSide.BUY:
        return (
            ExecutionReason.INSUFFICIENT_CASH
            if filled < candidate
            else ExecutionReason.VOLUME_CAP
        )
    return (
        ExecutionReason.INSUFFICIENT_SELLABLE
        if sellable < capacity
        else ExecutionReason.VOLUME_CAP
    )


def _reject(
    intent: OrderIntent, market: MarketSlice, reason: ExecutionReason
) -> RejectResult:
    return RejectResult(intent, market.trade_date, intent.quantity, reason)


def _float(row: dict[str, object], name: str) -> float:
    value = row[name]
    if not isinstance(value, float):
        raise TypeError(f"prepared market {name} must be a float")
    return value


def _int(row: dict[str, object], name: str) -> int:
    value = row[name]
    if type(value) is not int:
        raise ValueError(f"prepared market {name} must be an integer")
    return value


def _string(row: dict[str, object], name: str) -> str:
    value = row[name]
    if not isinstance(value, str):
        raise TypeError(f"prepared market {name} must be a string")
    return value


def _validate_intents(intents: Sequence[OrderIntent]) -> tuple[OrderIntent, ...]:
    if not isinstance(intents, Sequence) or isinstance(intents, (str, bytes)):
        raise TypeError("intents must be a sequence")
    ordered = tuple(intents)
    seen = set()
    for intent in ordered:
        if not isinstance(intent, OrderIntent):
            raise TypeError("intents must contain OrderIntent")
        if not isinstance(intent.instrument_id, InstrumentId):
            raise TypeError("intent instrument_id must be an InstrumentId")
        if (
            not isinstance(intent.side, OrderSide)
            or type(intent.quantity) is not int
            or intent.quantity <= 0
        ):
            raise ValueError("intent quantity and side are invalid")
        if intent.instrument_id in seen:
            raise ValueError("intents must be unique by instrument")
        seen.add(intent.instrument_id)
    return ordered
