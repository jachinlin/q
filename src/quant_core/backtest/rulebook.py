"""Explicit, versioned historical A-share market rules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from math import isfinite
from pathlib import Path
from typing import Any, Protocol

import yaml

from quant_core.backtest.calendar import TradingCalendar
from quant_core.domain.enums import Board, Exchange
from quant_core.domain.identifiers import InstrumentId

_RULE_START = date(2005, 1, 24)
_CENT = Decimal(100)
_PRICE_TICK = Decimal("0.01")


class SecurityStatus(StrEnum):
    """Risk-warning states relevant to daily price-limit rules."""

    NORMAL = "NORMAL"
    ST = "ST"


class Side(StrEnum):
    """Fill direction."""

    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class PriceBand:
    """Daily upper and lower limit prices in yuan."""

    upper: float
    lower: float


@dataclass(frozen=True, slots=True)
class FeeBreakdown:
    """Fee components, all expressed as integer cents."""

    commission_cents: int
    stamp_duty_cents: int
    transfer_fee_cents: int
    total_cents: int

    def as_tuple(self) -> tuple[int, int, int, int]:
        return (
            self.commission_cents,
            self.stamp_duty_cents,
            self.transfer_fee_cents,
            self.total_cents,
        )


@dataclass(frozen=True, slots=True)
class SimulatedFill:
    """The public fill input required to calculate historical transaction fees."""

    instrument: InstrumentId
    trade_date: date
    side: Side
    quantity: int
    price: float

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentId):
            raise TypeError("instrument must be an InstrumentId")
        if not isinstance(self.trade_date, date) or isinstance(
            self.trade_date, datetime
        ):
            raise TypeError("trade_date must be a date")
        if not isinstance(self.side, Side):
            raise TypeError("side must be a Side")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        _decimal_price(self.price)


class MarketRuleBook(Protocol):
    """Historical market constraints selected only by an explicit config version."""

    @property
    def version(self) -> str: ...

    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int: ...

    def earliest_sell_date(self, buy_date: date, instrument: InstrumentId) -> date: ...

    def price_limits(
        self,
        instrument: InstrumentId,
        trade_date: date,
        prev_close: float,
        status: SecurityStatus,
    ) -> PriceBand | None: ...

    def fees(self, fill: SimulatedFill) -> FeeBreakdown: ...


@dataclass(frozen=True, slots=True)
class _IntervalRule:
    start: date
    end: date | None
    value: Decimal
    basis: str | None = None


class AShareRuleBook:
    """A validated rulebook loaded only from an explicit local YAML file.

    IPO, relisting and delisting-first-day no-limit exceptions require listing
    lifecycle inputs which this task's public method signature does not accept.
    They are intentionally not inferred here; unmatched dates fail closed.
    """

    def __init__(
        self,
        version: str,
        price_limits: dict[tuple[Board, SecurityStatus], tuple[_IntervalRule, ...]],
        stamp_duty: dict[Side, tuple[_IntervalRule, ...]],
        transfer_fee: dict[Exchange, tuple[_IntervalRule, ...]],
        commission_rate: Decimal,
        commission_minimum_cents: int,
        calendar: TradingCalendar | None,
    ) -> None:
        self._version = version
        self._price_limits = price_limits
        self._stamp_duty = stamp_duty
        self._transfer_fee = transfer_fee
        self._commission_rate = commission_rate
        self._commission_minimum_cents = commission_minimum_cents
        self._calendar = calendar

    @property
    def version(self) -> str:
        return self._version

    @classmethod
    def load(
        cls, config_path: Path, *, calendar: TradingCalendar | None = None
    ) -> AShareRuleBook:
        """Load a local, fully-covered versioned YAML rulebook."""
        if not isinstance(config_path, Path):
            raise TypeError("config_path must be an explicit Path")
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        except OSError as error:
            raise ValueError("rulebook config cannot be read") from error
        if not isinstance(loaded, dict):
            raise ValueError("rulebook config must be a mapping")  # noqa: TRY004
        version = loaded.get("version")
        if not isinstance(version, str) or not version.strip():
            raise ValueError("rulebook version must be nonempty")
        commission = _mapping(loaded.get("commission"), "commission")
        commission_rate = _decimal(commission.get("rate"), "commission rate")
        minimum = commission.get("minimum_cents")
        if type(minimum) is not int or minimum < 0:
            raise ValueError("commission minimum_cents must be a nonnegative integer")
        price_limits = _price_limit_rules(loaded.get("price_limits"))
        stamp_duty = _fee_rules(loaded.get("stamp_duty"), Side, "stamp duty")
        transfer_fee = _fee_rules(
            loaded.get("transfer_fee"), Exchange, "transfer fee", require_basis=True
        )
        if calendar is not None and not isinstance(calendar, TradingCalendar):
            raise TypeError("calendar must be a TradingCalendar")
        return cls(
            version.strip(),
            price_limits,
            stamp_duty,
            transfer_fee,
            commission_rate,
            minimum,
            calendar,
        )

    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int:
        _validate_instrument_and_date(instrument, trade_date)
        return 200 if _board(instrument) is Board.STAR else 100

    def earliest_sell_date(self, buy_date: date, instrument: InstrumentId) -> date:
        _validate_instrument_and_date(instrument, buy_date)
        if self._calendar is None:
            raise ValueError("earliest sell date requires an explicit trading calendar")
        return self._calendar.next_session(buy_date)

    def price_limits(
        self,
        instrument: InstrumentId,
        trade_date: date,
        prev_close: float,
        status: SecurityStatus,
    ) -> PriceBand | None:
        _validate_instrument_and_date(instrument, trade_date)
        if not isinstance(status, SecurityStatus):
            raise TypeError("status must be a SecurityStatus")
        close = _decimal_price(prev_close)
        rate = _matching_rate(
            self._price_limits[(_board(instrument), status)], trade_date
        )
        upper = (close * (Decimal(1) + rate)).quantize(
            _PRICE_TICK, rounding=ROUND_HALF_UP
        )
        lower = (close * (Decimal(1) - rate)).quantize(
            _PRICE_TICK, rounding=ROUND_HALF_UP
        )
        return PriceBand(float(upper), float(lower))

    def fees(self, fill: SimulatedFill) -> FeeBreakdown:
        amount = _decimal_price(fill.price) * fill.quantity
        commission = max(
            _cents(amount * self._commission_rate), self._commission_minimum_cents
        )
        stamp = _cents(
            amount * _matching_rate(self._stamp_duty[fill.side], fill.trade_date)
        )
        transfer_rule = _matching_rule(
            self._transfer_fee[fill.instrument.exchange], fill.trade_date
        )
        transfer_base = amount
        if transfer_rule.basis == "face_value":
            transfer_base = Decimal(fill.quantity)
        transfer = _cents(transfer_base * transfer_rule.value)
        return FeeBreakdown(commission, stamp, transfer, commission + stamp + transfer)


def _price_limit_rules(
    value: object,
) -> dict[tuple[Board, SecurityStatus], tuple[_IntervalRule, ...]]:
    entries = _list(value, "price_limits")
    grouped: dict[tuple[Board, SecurityStatus], list[_IntervalRule]] = {}
    for entry in entries:
        row = _mapping(entry, "price limit rule")
        board = _enum(Board, row.get("board"), "board")
        status = _enum(SecurityStatus, row.get("status"), "status")
        grouped.setdefault((board, status), []).append(_interval(row, "rate"))
    expected = {(board, status) for board in Board for status in SecurityStatus}
    if set(grouped) != expected:
        raise ValueError("price limits must cover every board and status")
    return {key: _validate_intervals(key, rules) for key, rules in grouped.items()}


def _fee_rules(
    value: object,
    enum_type: type[Side | Exchange],
    name: str,
    *,
    require_basis: bool = False,
) -> dict[Any, tuple[_IntervalRule, ...]]:
    entries = _list(value, name)
    grouped: dict[Any, list[_IntervalRule]] = {}
    for entry in entries:
        row = _mapping(entry, f"{name} rule")
        key = _enum(
            enum_type,
            row.get("side") if enum_type is Side else row.get("exchange"),
            name,
        )
        rule = _interval(row, "rate")
        if require_basis:
            basis = row.get("basis")
            if basis not in {"turnover", "face_value"}:
                raise ValueError("transfer fee basis must be turnover or face_value")
            rule = _IntervalRule(rule.start, rule.end, rule.value, basis)
        grouped.setdefault(key, []).append(rule)
    if set(grouped) != set(enum_type):
        raise ValueError(f"{name} must cover every dimension")
    return {key: _validate_intervals(key, rules) for key, rules in grouped.items()}


def _interval(row: dict[str, object], value_name: str) -> _IntervalRule:
    start = _date(row.get("start"), "rule start")
    end_raw = row.get("end")
    end = None if end_raw is None else _date(end_raw, "rule end")
    if end is not None and end < start:
        raise ValueError("rule date interval must be ordered")
    return _IntervalRule(start, end, _decimal(row.get(value_name), value_name))


def _validate_intervals(
    key: object, rules: list[_IntervalRule]
) -> tuple[_IntervalRule, ...]:
    ordered = sorted(rules, key=lambda rule: rule.start)
    if not ordered or ordered[0].start != _RULE_START:
        raise ValueError(
            f"rule intervals for {key} must start at {_RULE_START.isoformat()}"
        )
    for index, rule in enumerate(ordered):
        if rule.end is None:
            if index != len(ordered) - 1:
                raise ValueError(f"rule intervals for {key} overlap")
            continue
        if index == len(ordered) - 1:
            raise ValueError(f"rule intervals for {key} must have an open end")
        next_start = ordered[index + 1].start
        if next_start <= rule.end:
            raise ValueError(f"rule intervals for {key} overlap")
        if (next_start - rule.end).days != 1:
            raise ValueError(f"rule intervals for {key} have a gap")
    return tuple(ordered)


def _matching_rate(rules: tuple[_IntervalRule, ...], trade_date: date) -> Decimal:
    return _matching_rule(rules, trade_date).value


def _matching_rule(rules: tuple[_IntervalRule, ...], trade_date: date) -> _IntervalRule:
    for rule in rules:
        if rule.start <= trade_date and (rule.end is None or trade_date <= rule.end):
            return rule
    raise ValueError("no configured rule matches trade date")


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be a mapping")
    return value


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")  # noqa: TRY004
    return value


def _enum(enum_type: type[Any], value: object, name: str) -> Any:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a valid enum value")  # noqa: TRY004
    try:
        return enum_type(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a valid enum value") from error


def _date(value: object, name: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{name} must be a date")  # noqa: TRY004
    return value


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{name} must be a finite nonnegative decimal")  # noqa: TRY004
    try:
        result = Decimal(str(value))
    except Exception as error:
        raise ValueError(f"{name} must be a finite nonnegative decimal") from error
    if not result.is_finite() or result < 0:
        raise ValueError(f"{name} must be a finite nonnegative decimal")
    return result


def _decimal_price(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError("price must be finite positive")  # noqa: TRY004
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("price must be finite positive")
    result = Decimal(str(value))
    if not result.is_finite() or result <= 0:
        raise ValueError("price must be finite positive")
    return result


def _cents(yuan: Decimal) -> int:
    return int((yuan * _CENT).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _board(instrument: InstrumentId) -> Board:
    if instrument.exchange is Exchange.SSE and instrument.symbol.startswith(
        ("688", "689")
    ):
        return Board.STAR
    if instrument.exchange is Exchange.SZSE and instrument.symbol.startswith(
        ("300", "301")
    ):
        return Board.CHINEXT
    return Board.MAIN


def _validate_instrument_and_date(instrument: InstrumentId, trade_date: date) -> None:
    if not isinstance(instrument, InstrumentId):
        raise TypeError("instrument must be an InstrumentId")
    if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
        raise TypeError("trade_date must be a date")
