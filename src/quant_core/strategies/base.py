"""Snapshot-bound strategy contracts and the Task 5 target-generation adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from math import isfinite
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

import polars as pl

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.engine import StrategyRef
from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import canonical_factor_ref, is_available_on_signal_day
from quant_core.portfolio.constructor import (
    PortfolioConstructor,
    TargetPortfolio,
)

_FACTOR_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "factor_ref": pl.String,
    "value": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
    "is_valid": pl.Boolean,
}
_UNIVERSE_SCHEMA = {
    "instrument_id": pl.String,
    "as_of": pl.Date,
    "eligible": pl.Boolean,
    "reason_codes": pl.List(pl.String),
    "industry": pl.String,
    "adv_amount": pl.Float64,
    "log_market_cap": pl.Float64,
}
_EPSILON = 1e-10


class RebalanceFrequency(StrEnum):
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: str
    message: str
    field: str | None = None

    def __post_init__(self) -> None:
        for value, name in ((self.code, "code"), (self.message, "message")):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonempty string")
        if self.field is not None and (
            not isinstance(self.field, str) or not self.field.strip()
        ):
            raise ValueError("field must be a nonempty string or None")


class StrategyValidationError(ValueError):
    issues: tuple[ValidationIssue, ...]

    def __init__(
        self, issues: tuple[ValidationIssue, ...] | list[ValidationIssue]
    ) -> None:
        items = tuple(issues)
        if not items or any(not isinstance(item, ValidationIssue) for item in items):
            raise ValueError("issues must be a nonempty tuple of ValidationIssue")
        self.issues = items
        super().__init__("; ".join(f"{item.code}: {item.message}" for item in items))


class StrategyData(Protocol):
    def factor_values(
        self,
        snapshot_id: UUID,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame: ...

    def stock_universe(self, snapshot_id: UUID, signal_date: date) -> pl.DataFrame: ...


@dataclass(frozen=True, slots=True)
class StrategyContext:
    snapshot_id: UUID
    signal_date: date
    execute_date: date
    sessions: tuple[date, ...]
    data: StrategyData
    portfolio_constructor: PortfolioConstructor

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_id, UUID):
            raise TypeError("snapshot_id must be a UUID")
        _require_date(self.signal_date, "signal_date")
        _require_date(self.execute_date, "execute_date")
        if self.execute_date <= self.signal_date:
            raise ValueError("execute_date must be strictly after signal_date")
        if not isinstance(self.sessions, tuple) or not self.sessions:
            raise ValueError("sessions must be a nonempty tuple")
        if any(type(item) is not date for item in self.sessions):
            raise TypeError("sessions must contain dates")
        if tuple(sorted(self.sessions)) != self.sessions or len(
            set(self.sessions)
        ) != len(self.sessions):
            raise ValueError("sessions must be strictly ascending and unique")
        if (
            self.signal_date not in self.sessions
            or self.execute_date not in self.sessions
        ):
            raise ValueError("sessions must include signal_date and execute_date")
        if self.data is None:
            raise TypeError("data must be supplied")
        if not isinstance(self.portfolio_constructor, PortfolioConstructor):
            raise TypeError("portfolio_constructor must be a PortfolioConstructor")


@dataclass(frozen=True, slots=True)
class PortfolioPosition:
    instrument_id: InstrumentId
    quantity: int
    market_value_fen: int
    current_weight: float

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_id, InstrumentId):
            raise TypeError("instrument_id must be an InstrumentId")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        if type(self.market_value_fen) is not int or self.market_value_fen <= 0:
            raise ValueError("market_value_fen must be a positive integer")
        _require_weight(self.current_weight, "current_weight")


@dataclass(frozen=True, slots=True)
class PortfolioState:
    trade_date: date
    cash_fen: int
    nav_fen: int
    total_market_value_fen: int
    positions: tuple[PortfolioPosition, ...]
    cash_weight: float

    def __post_init__(self) -> None:
        _require_date(self.trade_date, "trade_date")
        for value, name in (
            (self.cash_fen, "cash_fen"),
            (self.nav_fen, "nav_fen"),
            (self.total_market_value_fen, "total_market_value_fen"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if (
            self.nav_fen <= 0
            or self.nav_fen != self.cash_fen + self.total_market_value_fen
        ):
            raise ValueError("nav_fen must equal positive cash plus market value")
        if not isinstance(self.positions, tuple) or any(
            not isinstance(position, PortfolioPosition) for position in self.positions
        ):
            raise TypeError("positions must be a tuple of PortfolioPosition")
        canonical = tuple(
            position.instrument_id.canonical() for position in self.positions
        )
        if canonical != tuple(sorted(canonical)) or len(set(canonical)) != len(
            canonical
        ):
            raise ValueError("positions must be unique and canonical-ID sorted")
        if self.total_market_value_fen != sum(
            position.market_value_fen for position in self.positions
        ):
            raise ValueError("total_market_value_fen must equal positions")
        _require_weight(self.cash_weight, "cash_weight")
        if abs(self.cash_weight - self.cash_fen / self.nav_fen) > _EPSILON:
            raise ValueError("cash_weight must equal cash/nav")
        if (
            abs(
                self.cash_weight
                + sum(position.current_weight for position in self.positions)
                - 1.0
            )
            > _EPSILON
        ):
            raise ValueError("cash and position weights must sum to one")
        for position in self.positions:
            if (
                abs(position.current_weight - position.market_value_fen / self.nav_fen)
                > _EPSILON
            ):
                raise ValueError("position weight must equal market_value/nav")

    @classmethod
    def from_account_snapshot(cls, snapshot: AccountSnapshot) -> PortfolioState:
        if not isinstance(snapshot, AccountSnapshot):
            raise TypeError("snapshot must be an AccountSnapshot")
        if snapshot.nav_fen <= 0:
            raise ValueError("account snapshot nav_fen must be positive")
        positions = tuple(
            PortfolioPosition(
                item.instrument_id,
                item.total_quantity,
                item.market_value_fen,
                item.market_value_fen / snapshot.nav_fen,
            )
            for item in snapshot.positions
        )
        return cls(
            snapshot.trade_date,
            snapshot.cash_fen,
            snapshot.nav_fen,
            snapshot.total_market_value_fen,
            positions,
            snapshot.cash_fen / snapshot.nav_fen,
        )


class Strategy(Protocol):
    strategy_id: str
    version: str

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]: ...

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool: ...

    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio: ...


def is_rebalance_boundary(ctx: StrategyContext, frequency: RebalanceFrequency) -> bool:
    """Evaluate the close-to-next-session boundary without calendar assumptions."""
    if not isinstance(frequency, RebalanceFrequency):
        raise TypeError("frequency must be a RebalanceFrequency")
    signal_index = ctx.sessions.index(ctx.signal_date)
    if (
        signal_index + 1 >= len(ctx.sessions)
        or ctx.sessions[signal_index + 1] != ctx.execute_date
    ):
        raise ValueError(
            "execute_date must be the next actual session after signal_date"
        )
    if frequency is RebalanceFrequency.DAILY:
        return True
    if frequency is RebalanceFrequency.WEEKLY:
        return ctx.execute_date.isocalendar()[:2] != ctx.signal_date.isocalendar()[:2]
    return (ctx.execute_date.year, ctx.execute_date.month) != (
        ctx.signal_date.year,
        ctx.signal_date.month,
    )


def validated_factor_values(
    frame: pl.DataFrame,
    *,
    signal_date: date,
    instruments: tuple[InstrumentId, ...] | None,
    factor_refs: tuple[str, ...],
) -> pl.DataFrame:
    """Fail closed on a data-port response that is not the requested PIT long table."""
    _require_date(signal_date, "signal_date")
    if not isinstance(frame, pl.DataFrame) or not _matches_factor_schema(frame):
        raise ValueError("factor_values has an invalid schema")
    refs = tuple(canonical_factor_ref(value) for value in factor_refs)
    if len(set(refs)) != len(refs):
        raise ValueError("factor_refs must be unique")
    requested_ids = (
        None if instruments is None else {item.canonical() for item in instruments}
    )
    seen: set[tuple[date, str, str]] = set()
    for row in frame.iter_rows(named=True):
        trade_date = row["trade_date"]
        instrument = row["instrument_id"]
        factor_ref = row["factor_ref"]
        if trade_date != signal_date:
            raise ValueError("factor_values trade_date must equal signal_date")
        _canonical_instrument(instrument)
        if requested_ids is not None and instrument not in requested_ids:
            raise ValueError(
                "factor_values includes instrument outside requested scope"
            )
        if factor_ref not in refs:
            raise ValueError("factor_values includes unknown factor_ref")
        key = (trade_date, instrument, factor_ref)
        if key in seen:
            raise ValueError("factor_values must have unique primary keys")
        seen.add(key)
        value, available_at, valid = row["value"], row["available_at"], row["is_valid"]
        if valid:
            if not isinstance(value, float) or not isfinite(value):
                raise ValueError("valid factor value must be finite")
            if not is_available_on_signal_day(available_at, signal_date):
                raise ValueError("valid factor value is not available on signal date")
    return frame


def validated_stock_universe(frame: pl.DataFrame, *, signal_date: date) -> pl.DataFrame:
    _require_date(signal_date, "signal_date")
    if not isinstance(frame, pl.DataFrame) or frame.schema != _UNIVERSE_SCHEMA:
        raise ValueError("stock_universe has an invalid schema")
    identifiers = frame["instrument_id"].to_list()
    if any(not isinstance(item, str) for item in identifiers):
        raise ValueError("stock_universe instrument_id must be nonnull")
    if identifiers != sorted(identifiers) or len(set(identifiers)) != len(identifiers):
        raise ValueError("stock_universe must be canonical-ID sorted and unique")
    for row in frame.iter_rows(named=True):
        _canonical_instrument(row["instrument_id"])
        if row["as_of"] != signal_date:
            raise ValueError("stock_universe as_of must equal signal_date")
    return frame


class StrategyTargetAdapter:
    """Resolve an exact strategy ref into the existing Task 5 target port."""

    def __init__(
        self,
        registry: Mapping[StrategyRef, Strategy],
        context_provider: Callable[[UUID, date, date], StrategyContext],
    ) -> None:
        if not isinstance(registry, Mapping) or not callable(context_provider):
            raise TypeError("registry and context_provider are required")
        registered = dict(registry)
        for ref, strategy in registered.items():
            if not isinstance(ref, StrategyRef) or (
                strategy.strategy_id != ref.strategy_id
                or strategy.version != ref.version
            ):
                raise ValueError("registry entries must match exact StrategyRef")
        self._registry = MappingProxyType(registered)
        self._context_provider = context_provider

    def generate_target(
        self,
        strategy: StrategyRef,
        snapshot_id: UUID,
        signal_date: date,
        execute_date: date,
        current: AccountSnapshot,
    ) -> TargetPortfolio | None:
        if not isinstance(strategy, StrategyRef):
            raise TypeError("strategy must be a StrategyRef")
        try:
            resolved = self._registry[strategy]
        except KeyError as error:
            raise ValueError("unknown strategy or version") from error
        ctx = self._context_provider(snapshot_id, signal_date, execute_date)
        if not isinstance(ctx, StrategyContext) or (
            ctx.snapshot_id != snapshot_id
            or ctx.signal_date != signal_date
            or ctx.execute_date != execute_date
        ):
            raise ValueError("context provider returned mismatched strategy context")
        issues = resolved.validate(ctx)
        if not isinstance(issues, list) or any(
            not isinstance(item, ValidationIssue) for item in issues
        ):
            raise StrategyValidationError(
                (
                    ValidationIssue(
                        "INVALID_VALIDATION_RESULT", "strategy returned invalid issues"
                    ),
                )
            )
        if issues:
            raise StrategyValidationError(issues)
        if not resolved.should_rebalance(ctx, signal_date):
            return None
        target = resolved.generate_targets(
            ctx, signal_date, PortfolioState.from_account_snapshot(current)
        )
        if not isinstance(target, TargetPortfolio) or (
            target.signal_date != signal_date or target.execute_date != execute_date
        ):
            raise ValueError("strategy target dates must match context")
        return target


def _require_date(value: object, name: str) -> None:
    if type(value) is not date:
        raise TypeError(f"{name} must be a date")


def _require_weight(value: object, name: str) -> None:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not isfinite(value)
    ):
        raise ValueError(f"{name} must be a finite weight")
    if value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be in [0, 1]")


def _canonical_instrument(value: object) -> None:
    if not isinstance(value, str):
        raise TypeError("instrument_id must be a canonical string")
    try:
        InstrumentId.parse(value)
    except (TypeError, ValueError) as error:
        raise ValueError("instrument_id must be canonical") from error


def _matches_factor_schema(frame: pl.DataFrame) -> bool:
    expected = dict(_FACTOR_SCHEMA)
    if "invalid_reason" in frame.columns:
        expected["invalid_reason"] = pl.String
    return frame.schema == expected
