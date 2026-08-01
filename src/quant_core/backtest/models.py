"""Immutable public models for daily backtest execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from math import isfinite
from types import MappingProxyType

import polars as pl

from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio.rebalance import OrderIntent, OrderSide


class ExecutionReason(StrEnum):
    FILLED = "FILLED"
    SUSPENDED = "SUSPENDED"
    LIMIT_UP_BUY_BLOCKED = "LIMIT_UP_BUY_BLOCKED"
    LIMIT_DOWN_SELL_BLOCKED = "LIMIT_DOWN_SELL_BLOCKED"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    INSUFFICIENT_SELLABLE = "INSUFFICIENT_SELLABLE"
    VOLUME_CAP = "VOLUME_CAP"
    ODD_LOT = "ODD_LOT"
    NO_MARKET_DATA = "NO_MARKET_DATA"


class ExecutionPrice(StrEnum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    reference_price: ExecutionPrice
    slippage_bps: float
    max_volume_participation: float

    def __post_init__(self) -> None:
        if not isinstance(self.reference_price, ExecutionPrice):
            raise TypeError("reference_price must be an ExecutionPrice")
        _finite_nonnegative(self.slippage_bps, "slippage_bps")
        participation = _finite_nonnegative(
            self.max_volume_participation, "max_volume_participation"
        )
        if participation <= 0 or participation > 1:
            raise ValueError("max_volume_participation must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class MarketSlice:
    trade_date: date
    bars: pl.DataFrame

    def __post_init__(self) -> None:
        _trade_date(self.trade_date)
        if not isinstance(self.bars, pl.DataFrame):
            raise TypeError("bars must be a polars DataFrame")
        _validate_bars(self.bars)


@dataclass(frozen=True, slots=True)
class AccountView:
    cash_fen: int
    sellable_quantities: Mapping[InstrumentId, int]

    def __post_init__(self) -> None:
        _nonnegative_int(self.cash_fen, "cash_fen")
        if not isinstance(self.sellable_quantities, Mapping):
            raise TypeError("sellable_quantities must be a mapping")
        quantities = dict(self.sellable_quantities)
        for instrument, quantity in quantities.items():
            if not isinstance(instrument, InstrumentId):
                raise TypeError("sellable_quantities keys must be InstrumentId")
            _nonnegative_int(quantity, "sellable quantity")
        object.__setattr__(self, "sellable_quantities", MappingProxyType(quantities))


@dataclass(frozen=True, slots=True)
class FillResult:
    intent: OrderIntent
    trade_date: date
    requested_quantity: int
    filled_quantity: int
    unfilled_quantity: int
    price: float
    gross_value_fen: int
    fees: FeeBreakdown
    reason_code: ExecutionReason

    def __post_init__(self) -> None:
        _intent(self.intent)
        _trade_date(self.trade_date)
        _positive_int(self.requested_quantity, "requested_quantity")
        _positive_int(self.filled_quantity, "filled_quantity")
        _nonnegative_int(self.unfilled_quantity, "unfilled_quantity")
        if self.filled_quantity + self.unfilled_quantity != self.requested_quantity:
            raise ValueError("fill quantities must equal requested_quantity")
        _finite_positive(self.price, "price")
        _nonnegative_int(self.gross_value_fen, "gross_value_fen")
        _fees(self.fees)
        if not isinstance(self.reason_code, ExecutionReason):
            raise TypeError("reason_code must be an ExecutionReason")


@dataclass(frozen=True, slots=True)
class RejectResult:
    intent: OrderIntent
    trade_date: date
    requested_quantity: int
    reason_code: ExecutionReason
    detail: str | None = None

    def __post_init__(self) -> None:
        _intent(self.intent)
        _trade_date(self.trade_date)
        _positive_int(self.requested_quantity, "requested_quantity")
        if not isinstance(self.reason_code, ExecutionReason):
            raise TypeError("reason_code must be an ExecutionReason")
        if self.reason_code is ExecutionReason.FILLED:
            raise ValueError("reject reason_code cannot be FILLED")
        if self.detail is not None and not isinstance(self.detail, str):
            raise TypeError("detail must be a string or None")


ExecutionResult = FillResult | RejectResult


@dataclass(frozen=True, slots=True)
class ExecutionBatch:
    trade_date: date
    results: tuple[ExecutionResult, ...]
    ending_cash_fen: int

    def __post_init__(self) -> None:
        _trade_date(self.trade_date)
        if not isinstance(self.results, tuple):
            raise TypeError("results must be a tuple")
        for result in self.results:
            if not isinstance(result, (FillResult, RejectResult)):
                raise TypeError("results must contain execution results")
            if result.trade_date != self.trade_date:
                raise ValueError("result trade_date must match batch")
        _nonnegative_int(self.ending_cash_fen, "ending_cash_fen")


def _validate_bars(bars: pl.DataFrame) -> None:
    expected = {
        "instrument_id": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "preclose": pl.Float64,
        "volume": pl.Int64,
        "is_suspended": pl.Boolean,
        "security_status": pl.String,
    }
    missing = set(expected).difference(bars.columns)
    if missing:
        raise ValueError("market bars missing required columns")
    for name, dtype in expected.items():
        if bars.schema[name] != dtype:
            raise ValueError(f"market bars column {name} has invalid type")
    if any(value != 0 for row in bars.null_count().iter_rows() for value in row):
        raise ValueError("market bars cannot contain nulls")
    seen: set[str] = set()
    for row in bars.select(list(expected)).iter_rows(named=True):
        identifier = row["instrument_id"]
        status = row["security_status"]
        if not isinstance(identifier, str):
            raise TypeError("market bars instrument_id must be canonical")
        try:
            InstrumentId.parse(identifier)
        except (TypeError, ValueError) as error:
            raise ValueError("market bars instrument_id must be canonical") from error
        if identifier in seen:
            raise ValueError("market bars instrument_id must be unique")
        seen.add(identifier)
        if not isinstance(status, str) or status not in {"NORMAL", "ST"}:
            raise ValueError("market bars security_status is invalid")
        for column in ("open", "high", "low", "close", "preclose"):
            value = row[column]
            if not isinstance(value, float) or not isfinite(value) or value <= 0:
                raise ValueError("market bars OHLC values must be finite positive")
        if row["low"] > row["open"] or row["low"] > row["close"]:
            raise ValueError("market bars low invariant is invalid")
        if row["open"] > row["high"] or row["close"] > row["high"]:
            raise ValueError("market bars high invariant is invalid")
        if not isinstance(row["volume"], int) or row["volume"] < 0:
            raise ValueError("market bars volume must be nonnegative")


def _intent(value: OrderIntent) -> None:
    if not isinstance(value, OrderIntent):
        raise TypeError("intent must be an OrderIntent")
    if not isinstance(value.instrument_id, InstrumentId):
        raise TypeError("intent instrument_id must be an InstrumentId")
    if not isinstance(value.side, OrderSide):
        raise TypeError("intent side must be an OrderSide")
    _positive_int(value.quantity, "intent quantity")
    if not isinstance(value.reason_code, str):
        raise TypeError("intent reason_code must be a string")


def _fees(value: FeeBreakdown) -> None:
    if not isinstance(value, FeeBreakdown):
        raise TypeError("fees must be a FeeBreakdown")
    for component in value.as_tuple():
        _nonnegative_int(component, "fee")
    if value.total_cents != sum(value.as_tuple()[:3]):
        raise ValueError("fee total must equal fee components")


def _trade_date(value: object) -> None:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError("trade_date must be a date")


def _positive_int(value: object, name: str) -> None:
    _nonnegative_int(value, name)
    if value == 0:
        raise ValueError(f"{name} must be positive")


def _nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


def _finite_nonnegative(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be finite and nonnegative")
    result = float(value)
    if not isfinite(result) or result < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return result


def _finite_positive(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be finite and positive")
    if not isfinite(float(value)) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")
