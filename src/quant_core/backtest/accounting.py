"""Immutable-ledger portfolio accounting with T+1 settlement and corporate actions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from enum import StrEnum
from math import isfinite

from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.models import ExecutionBatch, ExecutionReason, FillResult
from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio.rebalance import OrderSide

_CENT = Decimal(100)
_ONE = Decimal(1)


class CorporateActionType(StrEnum):
    CASH_DIVIDEND = "CASH_DIVIDEND"
    BONUS_SHARES = "BONUS_SHARES"


class LedgerEventType(StrEnum):
    OPENING_CASH = "OPENING_CASH"
    BUY = "BUY"
    SELL = "SELL"
    CASH_DIVIDEND = "CASH_DIVIDEND"
    BONUS_SHARES = "BONUS_SHARES"


@dataclass(frozen=True, slots=True)
class CorporateAction:
    event_id: str
    action_type: CorporateActionType
    instrument_id: InstrumentId
    record_date: date
    effective_date: date
    cash_per_share_yuan: Decimal = Decimal(0)
    share_ratio: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        _identifier(self.event_id, "event_id")
        if not isinstance(self.action_type, CorporateActionType):
            raise TypeError("action_type must be a CorporateActionType")
        _instrument(self.instrument_id)
        _date(self.record_date, "record_date")
        _date(self.effective_date, "effective_date")
        if self.record_date > self.effective_date:
            raise ValueError("record_date must not follow effective_date")
        cash = _nonnegative_decimal(self.cash_per_share_yuan, "cash_per_share_yuan")
        ratio = _nonnegative_decimal(self.share_ratio, "share_ratio")
        if self.action_type is CorporateActionType.CASH_DIVIDEND:
            if cash <= 0 or ratio != 0:
                raise ValueError("cash dividend requires positive cash and zero ratio")
        elif ratio <= 0 or cash != 0:
            raise ValueError("bonus shares requires positive ratio and zero cash")


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    event_id: str
    event_type: LedgerEventType
    trade_date: date
    instrument_id: InstrumentId | None
    cash_delta_fen: int
    quantity_delta: int
    cost_basis_delta_fen: int
    gross_value_fen: int
    fees_fen: int
    source_id: str

    def __post_init__(self) -> None:
        _identifier(self.event_id, "event_id")
        if not isinstance(self.event_type, LedgerEventType):
            raise TypeError("event_type must be a LedgerEventType")
        _date(self.trade_date, "trade_date")
        if self.instrument_id is not None:
            _instrument(self.instrument_id)
        for value, name in (
            (self.cash_delta_fen, "cash_delta_fen"),
            (self.quantity_delta, "quantity_delta"),
            (self.cost_basis_delta_fen, "cost_basis_delta_fen"),
            (self.gross_value_fen, "gross_value_fen"),
            (self.fees_fen, "fees_fen"),
        ):
            _integer(value, name)
        _identifier(self.source_id, "source_id")
        _validate_ledger_shape(self)


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    instrument_id: InstrumentId
    total_quantity: int
    sellable_quantity: int
    cost_basis_fen: int
    market_value_fen: int

    def __post_init__(self) -> None:
        _instrument(self.instrument_id)
        _nonnegative_int(self.total_quantity, "total_quantity")
        _nonnegative_int(self.sellable_quantity, "sellable_quantity")
        _nonnegative_int(self.cost_basis_fen, "cost_basis_fen")
        _nonnegative_int(self.market_value_fen, "market_value_fen")
        if self.total_quantity == 0 and any(
            value != 0
            for value in (
                self.sellable_quantity,
                self.cost_basis_fen,
                self.market_value_fen,
            )
        ):
            raise ValueError("zero quantity position must have zero balances")
        if self.sellable_quantity > self.total_quantity:
            raise ValueError("sellable_quantity must not exceed total_quantity")
        if self.total_quantity > 0 and self.market_value_fen == 0:
            raise ValueError("market_value_fen must be positive for a position")


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    trade_date: date
    cash_fen: int
    positions: tuple[PositionSnapshot, ...]
    total_market_value_fen: int
    nav_fen: int

    def __post_init__(self) -> None:
        _date(self.trade_date, "trade_date")
        _nonnegative_int(self.cash_fen, "cash_fen")
        if not isinstance(self.positions, tuple):
            raise TypeError("positions must be a tuple")
        if any(
            not isinstance(position, PositionSnapshot) for position in self.positions
        ):
            raise TypeError("positions must contain PositionSnapshot")
        canonical = tuple(
            position.instrument_id.canonical() for position in self.positions
        )
        if canonical != tuple(sorted(canonical)) or len(set(canonical)) != len(
            canonical
        ):
            raise ValueError("positions must be unique and canonical-ID sorted")
        _nonnegative_int(self.total_market_value_fen, "total_market_value_fen")
        _nonnegative_int(self.nav_fen, "nav_fen")
        if self.total_market_value_fen != sum(
            position.market_value_fen for position in self.positions
        ):
            raise ValueError("total_market_value_fen must equal positions")
        if self.nav_fen != self.cash_fen + self.total_market_value_fen:
            raise ValueError("nav_fen must equal cash plus market value")


@dataclass(slots=True)
class _Lot:
    buy_date: date
    sellable_date: date
    quantity: int
    cost_basis_fen: int

    def copy(self) -> _Lot:
        return _Lot(
            self.buy_date, self.sellable_date, self.quantity, self.cost_basis_fen
        )


class PortfolioAccount:
    """Apply daily execution batches atomically and expose ledger-derived snapshots."""

    def __init__(self, initial_cash_fen: int, calendar: TradingCalendar) -> None:
        _nonnegative_int(initial_cash_fen, "initial_cash_fen")
        if not isinstance(calendar, TradingCalendar):
            raise TypeError("calendar must be a TradingCalendar")
        self._calendar = calendar
        self._cash_fen = initial_cash_fen
        self._lots: dict[InstrumentId, list[_Lot]] = {}
        opening = LedgerEvent(
            "account:opening-cash",
            LedgerEventType.OPENING_CASH,
            calendar.start,
            None,
            initial_cash_fen,
            0,
            0,
            0,
            0,
            "account:init",
        )
        self._ledger = [opening]
        self._ledger_event_ids = {opening.event_id}
        self._ledger_source_ids = {opening.source_id}
        self._processed_action_ids: set[str] = set()
        self._record_quantities: dict[date, dict[InstrumentId, int]] = {}
        self._last_session: date | None = None
        self._phase = "idle"
        self._last_snapshot: AccountSnapshot | None = None

    @property
    def ledger(self) -> tuple[LedgerEvent, ...]:
        return tuple(self._ledger)

    @property
    def last_snapshot(self) -> AccountSnapshot | None:
        return self._last_snapshot

    def begin_session(
        self, trade_date: date, actions: Sequence[CorporateAction]
    ) -> None:
        _date(trade_date, "trade_date")
        if self._phase not in {"idle", "marked"}:
            raise ValueError(
                "begin_session requires the preceding session to be marked"
            )
        if self._last_session is not None and trade_date <= self._last_session:
            raise ValueError("begin_session dates must be strictly increasing")
        if not _is_session(self._calendar, trade_date):
            raise ValueError("begin_session trade_date must be a loaded session")
        action_items = _actions(actions)
        if len({action.event_id for action in action_items}) != len(action_items):
            raise ValueError("corporate action event_id values must be unique per call")

        cash = self._cash_fen
        lots = _copy_lots(self._lots)
        ledger = list(self._ledger)
        event_ids = set(self._ledger_event_ids)
        source_ids = set(self._ledger_source_ids)
        processed = set(self._processed_action_ids)
        for action in action_items:
            if action.event_id in processed:
                continue
            if action.effective_date != trade_date:
                raise ValueError("corporate action effective_date must match session")
            try:
                entitled = self._record_quantities[action.record_date].get(
                    action.instrument_id, 0
                )
            except KeyError as error:
                raise ValueError(
                    "corporate action requires record-date evidence"
                ) from error
            if action.action_type is CorporateActionType.CASH_DIVIDEND:
                amount = _rounded_fen(Decimal(entitled) * action.cash_per_share_yuan)
                cash += amount
                event_id = f"corporate-action:{action.event_id}"
                ledger_event = LedgerEvent(
                    event_id,
                    LedgerEventType.CASH_DIVIDEND,
                    trade_date,
                    action.instrument_id,
                    amount,
                    0,
                    0,
                    0,
                    0,
                    event_id,
                )
                _reserve_ledger_identity(ledger_event, event_ids, source_ids)
                ledger.append(ledger_event)
            else:
                quantity = int(
                    (Decimal(entitled) * action.share_ratio).to_integral_value(
                        rounding=ROUND_FLOOR
                    )
                )
                if quantity:
                    lots.setdefault(action.instrument_id, []).append(
                        _Lot(trade_date, trade_date, quantity, 0)
                    )
                event_id = f"corporate-action:{action.event_id}"
                ledger_event = LedgerEvent(
                    event_id,
                    LedgerEventType.BONUS_SHARES,
                    trade_date,
                    action.instrument_id,
                    0,
                    quantity,
                    0,
                    0,
                    0,
                    event_id,
                )
                _reserve_ledger_identity(ledger_event, event_ids, source_ids)
                ledger.append(ledger_event)
            processed.add(action.event_id)
        _unlock_lots(lots, trade_date)
        self._cash_fen = cash
        self._lots = lots
        self._ledger = ledger
        self._ledger_event_ids = event_ids
        self._ledger_source_ids = source_ids
        self._processed_action_ids = processed
        self._last_session = trade_date
        self._phase = "open"
        self._last_snapshot = None

    def apply(self, execution: ExecutionBatch) -> None:
        if self._phase != "open":
            raise ValueError("apply requires an open begin_session")
        if not isinstance(execution, ExecutionBatch):
            raise TypeError("execution must be an ExecutionBatch")
        if execution.trade_date != self._last_session:
            raise ValueError("execution trade_date must match current session")

        cash = self._cash_fen
        lots = _copy_lots(self._lots)
        additions: list[LedgerEvent] = []
        event_ids = set(self._ledger_event_ids)
        source_ids = set(self._ledger_source_ids)
        for index, result in enumerate(execution.results):
            if not isinstance(result, FillResult):
                continue
            _validate_fill(result)
            event_id = f"execution:{execution.trade_date.isoformat()}:{index}"
            source_id = (
                f"{event_id}:{result.intent.instrument_id.canonical()}:"
                f"{result.intent.side.value}"
            )
            charges = result.gross_value_fen + result.fees.total_cents
            if result.intent.side is OrderSide.BUY:
                if charges > cash:
                    raise ValueError("buy would make cash negative")
                sellable_date = self._calendar.next_session(result.trade_date)
                lots.setdefault(result.intent.instrument_id, []).append(
                    _Lot(
                        result.trade_date,
                        sellable_date,
                        result.filled_quantity,
                        charges,
                    )
                )
                cash -= charges
                ledger_event = LedgerEvent(
                    event_id,
                    LedgerEventType.BUY,
                    result.trade_date,
                    result.intent.instrument_id,
                    -charges,
                    result.filled_quantity,
                    charges,
                    result.gross_value_fen,
                    result.fees.total_cents,
                    source_id,
                )
                _reserve_ledger_identity(ledger_event, event_ids, source_ids)
                additions.append(ledger_event)
            else:
                consumed = _consume_lots_for_date(
                    lots,
                    result.intent.instrument_id,
                    result.filled_quantity,
                    self._last_session,
                )
                if consumed is None:
                    raise ValueError("sell exceeds sellable quantity")
                proceeds = result.gross_value_fen - result.fees.total_cents
                if cash + proceeds < 0:
                    raise ValueError("sell would make cash negative")
                cash += proceeds
                ledger_event = LedgerEvent(
                    event_id,
                    LedgerEventType.SELL,
                    result.trade_date,
                    result.intent.instrument_id,
                    proceeds,
                    -result.filled_quantity,
                    -consumed,
                    result.gross_value_fen,
                    result.fees.total_cents,
                    source_id,
                )
                _reserve_ledger_identity(ledger_event, event_ids, source_ids)
                additions.append(ledger_event)
        if cash != execution.ending_cash_fen:
            raise ValueError("execution ending cash does not match accounting")
        self._cash_fen = cash
        self._lots = lots
        self._ledger.extend(additions)
        self._ledger_event_ids = event_ids
        self._ledger_source_ids = source_ids
        self._phase = "applied"

    def mark_to_market(
        self, trade_date: date, closes: Mapping[InstrumentId, float]
    ) -> AccountSnapshot:
        if self._phase not in {"open", "applied"}:
            raise ValueError("mark_to_market requires an open session and may run once")
        _date(trade_date, "trade_date")
        if trade_date != self._last_session:
            raise ValueError("mark_to_market trade_date must match current session")
        prices = _prices(closes)
        cash, quantities, costs = _reduce_ledger(self._ledger)
        if cash != self._cash_fen:
            raise RuntimeError("ledger cash does not match account cash")
        lot_quantities, lot_costs, sellable = _lot_totals(self._lots, trade_date)
        if quantities != lot_quantities or costs != lot_costs:
            raise RuntimeError("ledger positions do not match lot state")
        positions: list[PositionSnapshot] = []
        for instrument_id in sorted(quantities, key=InstrumentId.canonical):
            quantity = quantities[instrument_id]
            if quantity == 0:
                continue
            try:
                close_fen = prices[instrument_id]
            except KeyError as error:
                raise ValueError(
                    f"missing close for {instrument_id.canonical()}"
                ) from error
            positions.append(
                PositionSnapshot(
                    instrument_id,
                    quantity,
                    sellable.get(instrument_id, 0),
                    costs[instrument_id],
                    quantity * close_fen,
                )
            )
        market_value = sum(position.market_value_fen for position in positions)
        snapshot = AccountSnapshot(
            trade_date, cash, tuple(positions), market_value, cash + market_value
        )
        self._record_quantities[trade_date] = dict(quantities)
        self._last_snapshot = snapshot
        self._phase = "marked"
        return snapshot


def _validate_ledger_shape(event: LedgerEvent) -> None:
    if event.event_type is LedgerEventType.OPENING_CASH:
        if (
            event.instrument_id is not None
            or event.cash_delta_fen < 0
            or any(
                value != 0
                for value in (
                    event.quantity_delta,
                    event.cost_basis_delta_fen,
                    event.gross_value_fen,
                    event.fees_fen,
                )
            )
        ):
            raise ValueError("opening cash ledger event is invalid")
        return
    if event.instrument_id is None:
        raise ValueError("non-cash ledger event requires instrument_id")
    if event.gross_value_fen < 0 or event.fees_fen < 0:
        raise ValueError("gross_value_fen and fees_fen must be nonnegative")
    if event.event_type is LedgerEventType.BUY:
        if (
            event.quantity_delta <= 0
            or event.gross_value_fen <= 0
            or event.cost_basis_delta_fen != event.gross_value_fen + event.fees_fen
            or event.cash_delta_fen != -event.cost_basis_delta_fen
        ):
            raise ValueError("buy ledger event is invalid")
    elif event.event_type is LedgerEventType.SELL:
        if (
            event.quantity_delta >= 0
            or event.gross_value_fen <= 0
            or event.cost_basis_delta_fen > 0
            or event.cash_delta_fen != event.gross_value_fen - event.fees_fen
        ):
            raise ValueError("sell ledger event is invalid")
    elif event.event_type is LedgerEventType.CASH_DIVIDEND:
        if event.cash_delta_fen < 0 or any(
            value != 0
            for value in (
                event.quantity_delta,
                event.cost_basis_delta_fen,
                event.gross_value_fen,
                event.fees_fen,
            )
        ):
            raise ValueError("cash dividend ledger event is invalid")
    elif event.event_type is LedgerEventType.BONUS_SHARES and (
        event.quantity_delta < 0
        or any(
            value != 0
            for value in (
                event.cash_delta_fen,
                event.cost_basis_delta_fen,
                event.gross_value_fen,
                event.fees_fen,
            )
        )
    ):
        raise ValueError("bonus shares ledger event is invalid")


def _actions(actions: Sequence[CorporateAction]) -> tuple[CorporateAction, ...]:
    if not isinstance(actions, Sequence) or isinstance(actions, (str, bytes)):
        raise TypeError("actions must be a sequence")
    items = tuple(actions)
    if any(not isinstance(action, CorporateAction) for action in items):
        raise TypeError("actions must contain CorporateAction")
    return items


def _copy_lots(
    lots: Mapping[InstrumentId, list[_Lot]],
) -> dict[InstrumentId, list[_Lot]]:
    return {
        instrument: [lot.copy() for lot in values]
        for instrument, values in lots.items()
    }


def _unlock_lots(lots: Mapping[InstrumentId, list[_Lot]], trade_date: date) -> None:
    # Sellability is date-derived; retaining the sellable date avoids mutable flags.
    for values in lots.values():
        values.sort(key=lambda lot: (lot.buy_date, lot.sellable_date))


def _consume_lots_for_date(
    lots: dict[InstrumentId, list[_Lot]],
    instrument: InstrumentId,
    quantity: int,
    trade_date: date,
) -> int | None:
    candidates = [
        lot for lot in lots.get(instrument, ()) if lot.sellable_date <= trade_date
    ]
    if sum(lot.quantity for lot in candidates) < quantity:
        return None
    remaining = quantity
    consumed = 0
    for lot in candidates:
        take = min(remaining, lot.quantity)
        if take == lot.quantity:
            cost = lot.cost_basis_fen
        else:
            cost = int(
                (
                    Decimal(lot.cost_basis_fen) * Decimal(take) / Decimal(lot.quantity)
                ).quantize(_ONE, rounding=ROUND_HALF_UP)
            )
        lot.quantity -= take
        lot.cost_basis_fen -= cost
        consumed += cost
        remaining -= take
        if remaining == 0:
            break
    lots[instrument] = [lot for lot in lots[instrument] if lot.quantity]
    return consumed


def _lot_totals(
    lots: Mapping[InstrumentId, list[_Lot]], trade_date: date
) -> tuple[dict[InstrumentId, int], dict[InstrumentId, int], dict[InstrumentId, int]]:
    quantities: dict[InstrumentId, int] = {}
    costs: dict[InstrumentId, int] = {}
    sellable: dict[InstrumentId, int] = {}
    for instrument, values in lots.items():
        quantity = sum(lot.quantity for lot in values)
        cost = sum(lot.cost_basis_fen for lot in values)
        sellable_quantity = sum(
            lot.quantity for lot in values if lot.sellable_date <= trade_date
        )
        if quantity:
            quantities[instrument] = quantity
            costs[instrument] = cost
            sellable[instrument] = sellable_quantity
    return quantities, costs, sellable


def _reduce_ledger(
    ledger: Sequence[LedgerEvent],
) -> tuple[int, dict[InstrumentId, int], dict[InstrumentId, int]]:
    cash = 0
    quantities: dict[InstrumentId, int] = {}
    costs: dict[InstrumentId, int] = {}
    for event in ledger:
        cash += event.cash_delta_fen
        if event.instrument_id is not None:
            instrument = event.instrument_id
            quantities[instrument] = (
                quantities.get(instrument, 0) + event.quantity_delta
            )
            costs[instrument] = costs.get(instrument, 0) + event.cost_basis_delta_fen
            if quantities[instrument] < 0 or costs[instrument] < 0:
                raise RuntimeError("ledger reduction became negative")
    return (
        cash,
        {
            instrument: quantity
            for instrument, quantity in quantities.items()
            if quantity
        },
        {
            instrument: cost
            for instrument, cost in costs.items()
            if cost or quantities[instrument]
        },
    )


def _validate_fill(result: FillResult) -> None:
    if result.requested_quantity != result.intent.quantity:
        raise ValueError("fill requested quantity is inconsistent with intent")
    if result.filled_quantity + result.unfilled_quantity != result.intent.quantity:
        raise ValueError("fill quantities are inconsistent with intent")
    expected_gross = _rounded_fen(Decimal(str(result.price))) * result.filled_quantity
    if result.gross_value_fen != expected_gross:
        raise ValueError("fill gross value is inconsistent with price and quantity")
    if result.intent.side not in {OrderSide.BUY, OrderSide.SELL}:
        raise ValueError("fill side is invalid")
    if (
        result.filled_quantity == result.intent.quantity
        and result.unfilled_quantity == 0
    ):
        if result.reason_code is not ExecutionReason.FILLED:
            raise ValueError("fill reason is inconsistent with complete fill")
        return
    if result.unfilled_quantity <= 0 or result.reason_code not in {
        ExecutionReason.INSUFFICIENT_CASH,
        ExecutionReason.INSUFFICIENT_SELLABLE,
        ExecutionReason.VOLUME_CAP,
    }:
        raise ValueError("fill reason is inconsistent with partial fill")


def _reserve_ledger_identity(
    event: LedgerEvent, event_ids: set[str], source_ids: set[str]
) -> None:
    if event.event_id in event_ids or event.source_id in source_ids:
        raise ValueError("ledger event_id and source_id must be unique")
    event_ids.add(event.event_id)
    source_ids.add(event.source_id)


def _prices(closes: Mapping[InstrumentId, float]) -> dict[InstrumentId, int]:
    if not isinstance(closes, Mapping):
        raise TypeError("closes must be a mapping")
    return {
        instrument: _close_fen(instrument, close)
        for instrument, close in closes.items()
    }


def _close_fen(instrument: object, close: object) -> int:
    _instrument(instrument)
    if isinstance(close, bool) or not isinstance(close, (int, float, Decimal)):
        raise TypeError("close must be finite and positive")
    if isinstance(close, float) and not isfinite(close):
        raise ValueError("close must be finite and positive")
    value = Decimal(str(close))
    if not value.is_finite() or value <= 0:
        raise ValueError("close must be finite and positive")
    fen = _rounded_fen(value)
    if fen <= 0:
        raise ValueError("close must round to positive fen")
    return fen


def _rounded_fen(yuan: Decimal) -> int:
    return int((yuan * _CENT).quantize(_ONE, rounding=ROUND_HALF_UP))


def _is_session(calendar: TradingCalendar, trade_date: date) -> bool:
    return trade_date in calendar.sessions(trade_date, trade_date)


def _identifier(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be nonempty")


def _instrument(value: object) -> None:
    if not isinstance(value, InstrumentId):
        raise TypeError("instrument_id must be an InstrumentId")


def _date(value: object, name: str) -> None:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f"{name} must be a date")


def _integer(value: object, name: str) -> None:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")


def _nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be a finite nonnegative Decimal")
    return value
