"""提供回测与账户核算相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from math import isfinite
from types import MappingProxyType

from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.models import ExecutionBatch, ExecutionReason, FillResult
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.rebalance import OrderSide

_CENT = Decimal(100)
_ONE = Decimal(1)


class LedgerEventType(StrEnum):
    """定义 ``LedgerEventType`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    OPENING_CASH = "OPENING_CASH"
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    """记录一次现金、持仓、成本或费用变化的不可变账户流水。

    入参：
        event_id：用于持久化关联和日志追踪的事件标识。
        event_type：事件类型。
        trade_date：目标交易日期，类型为 ``date``。
        instrument_id：目标证券标识，类型为 ``InstrumentId | None``。
        cash_delta_fen：``cash``变动额分币金额。
        quantity_delta：数量变动额。
        cost_basis_delta_fen：成本成本基础变动额分币金额。
        gross_value_fen：成交价格乘成交数量后取整到分的成交总额。
        fees_fen：费用分币金额。
        source_id：用于持久化关联和日志追踪的数据来源标识。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

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
        _AccountingSupport._identifier(self.event_id, "event_id")
        if not isinstance(self.event_type, LedgerEventType):
            raise TypeError("event_type must be a LedgerEventType")
        _AccountingSupport._date(self.trade_date, "trade_date")
        if self.instrument_id is not None:
            _AccountingSupport._instrument(self.instrument_id)
        for value, name in (
            (self.cash_delta_fen, "cash_delta_fen"),
            (self.quantity_delta, "quantity_delta"),
            (self.cost_basis_delta_fen, "cost_basis_delta_fen"),
            (self.gross_value_fen, "gross_value_fen"),
            (self.fees_fen, "fees_fen"),
        ):
            _AccountingSupport._integer(value, name)
        _AccountingSupport._identifier(self.source_id, "source_id")
        _AccountingSupport._validate_ledger_shape(self)


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    """记录某交易日单只证券的数量、可卖数量和成本基础。

    入参：
        instrument_id：目标证券标识，类型为 ``InstrumentId``。
        total_quantity：总量数量。
        sellable_quantity：可卖数量。
        cost_basis_fen：成本成本基础分币金额。
        market_value_fen：市场数据值分币金额。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    instrument_id: InstrumentId
    total_quantity: int
    sellable_quantity: int
    cost_basis_fen: int
    market_value_fen: int

    def __post_init__(self) -> None:
        _AccountingSupport._instrument(self.instrument_id)
        _AccountingSupport._nonnegative_int(self.total_quantity, "total_quantity")
        _AccountingSupport._nonnegative_int(self.sellable_quantity, "sellable_quantity")
        _AccountingSupport._nonnegative_int(self.cost_basis_fen, "cost_basis_fen")
        _AccountingSupport._nonnegative_int(self.market_value_fen, "market_value_fen")
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
    """记录某交易日收盘后的现金、持仓市值、总资产和持仓集合。

    入参：
        trade_date：目标交易日期，类型为 ``date``。
        cash_fen：账户可用现金，采用整数分避免浮点货币误差。
        positions：参与本次处理的持仓集合；调用方不得依赖未声明的顺序。
        total_market_value_fen：总量市场数据值分币金额。
        nav_fen：净值序列分币金额。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    trade_date: date
    cash_fen: int
    positions: tuple[PositionSnapshot, ...]
    total_market_value_fen: int
    nav_fen: int

    def __post_init__(self) -> None:
        _AccountingSupport._date(self.trade_date, "trade_date")
        _AccountingSupport._nonnegative_int(self.cash_fen, "cash_fen")
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
        _AccountingSupport._nonnegative_int(
            self.total_market_value_fen, "total_market_value_fen"
        )
        _AccountingSupport._nonnegative_int(self.nav_fen, "nav_fen")
        if self.total_market_value_fen != sum(
            position.market_value_fen for position in self.positions
        ):
            raise ValueError("total_market_value_fen must equal positions")
        if self.nav_fen != self.cash_fen + self.total_market_value_fen:
            raise ValueError("nav_fen must equal cash plus market value")


@dataclass(frozen=True, slots=True)
class AccountExecutionView:
    """向撮合层暴露当日可用现金、总数量和可卖数量的只读快照。

    入参：
        cash_fen：账户可用现金，采用整数分避免浮点货币误差。
        total_quantities：参与本次处理的总量``quantities``；调用方不得依赖未声明的顺序。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Immutable balances available to the execution layer in an open session.
    """

    cash_fen: int
    total_quantities: Mapping[InstrumentId, int]
    sellable_quantities: Mapping[InstrumentId, int]

    def __post_init__(self) -> None:
        _AccountingSupport._nonnegative_int(self.cash_fen, "cash_fen")
        totals = _AccountingSupport._quantity_mapping(
            self.total_quantities, "total_quantities"
        )
        sellable = _AccountingSupport._quantity_mapping(
            self.sellable_quantities, "sellable_quantities"
        )
        if any(
            quantity > totals.get(instrument, 0)
            for instrument, quantity in sellable.items()
        ):
            raise ValueError("sellable quantities must not exceed total quantities")
        object.__setattr__(self, "total_quantities", MappingProxyType(totals))
        object.__setattr__(self, "sellable_quantities", MappingProxyType(sellable))


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
    """以账户流水为事实来源，原子应用成交并生成逐日估值快照。

    入参：
        initial_cash_fen：初始``cash``分币金额。
        calendar：交易日历。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Apply daily execution batches atomically and expose ledger-derived snapshots.
    """

    def __init__(self, initial_cash_fen: int, calendar: TradingCalendar) -> None:
        _AccountingSupport._nonnegative_int(initial_cash_fen, "initial_cash_fen")
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
        self._last_session: date | None = None
        self._phase = "idle"
        self._last_snapshot: AccountSnapshot | None = None
        self._last_mark_prices: dict[InstrumentId, Decimal] = {}

    @property
    def ledger(self) -> tuple[LedgerEvent, ...]:
        """处理回测中的账户流水。

        入参：
            无。
        返回值：
            返回按事件序号排序的不可变账户流水。
        异常：
            无。
        """
        return tuple(self._ledger)

    @property
    def last_snapshot(self) -> AccountSnapshot | None:
        """处理回测中的``last``账户快照。

        入参：
            无。
        返回值：
            返回最近一次完成估值的账户快照。
        异常：
            无。
        """
        return self._last_snapshot

    def begin_session(self, trade_date: date) -> None:
        """处理回测中的``begin``交易会话。

        入参：
            trade_date：目标交易日期，类型为 ``date``。
        返回值：
            无。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        _AccountingSupport._date(trade_date, "trade_date")
        if self._phase not in {"idle", "marked"}:
            raise ValueError(
                "begin_session requires the preceding session to be marked"
            )
        if self._last_session is not None and trade_date <= self._last_session:
            raise ValueError("begin_session dates must be strictly increasing")
        if not _AccountingSupport._is_session(self._calendar, trade_date):
            raise ValueError("begin_session trade_date must be a loaded session")
        lots = _AccountingSupport._copy_lots(self._lots)
        _AccountingSupport._unlock_lots(lots, trade_date)
        self._lots = lots
        self._last_session = trade_date
        self._phase = "open"
        self._last_snapshot = None

    def apply(self, execution: ExecutionBatch) -> None:
        """原子应用回测。

        入参：
            execution：成交执行。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if self._phase != "open":
            raise ValueError("apply requires an open begin_session")
        if not isinstance(execution, ExecutionBatch):
            raise TypeError("execution must be an ExecutionBatch")
        if execution.trade_date != self._last_session:
            raise ValueError("execution trade_date must match current session")

        cash = self._cash_fen
        lots = _AccountingSupport._copy_lots(self._lots)
        additions: list[LedgerEvent] = []
        event_ids = set(self._ledger_event_ids)
        source_ids = set(self._ledger_source_ids)
        for index, result in enumerate(execution.results):
            if not isinstance(result, FillResult):
                continue
            _AccountingSupport._validate_fill(result)
            event_id = f"execution:{execution.trade_date.isoformat()}:{index}"
            source_id = (
                f"{event_id}:{result.intent.instrument_id.canonical()}:"
                f"{result.intent.side.value}"
            )
            charges = result.gross_value_fen + result.fees.total_cents
            if result.intent.side is OrderSide.BUY:
                if charges > cash:
                    raise ValueError("buy would make cash negative")
                sellable_date = _AccountingSupport._settlement_date(
                    self._calendar,
                    result.trade_date,
                    result.settlement_sessions,
                )
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
                _AccountingSupport._reserve_ledger_identity(
                    ledger_event, event_ids, source_ids
                )
                additions.append(ledger_event)
            else:
                consumed = _AccountingSupport._consume_lots_for_date(
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
                _AccountingSupport._reserve_ledger_identity(
                    ledger_event, event_ids, source_ids
                )
                additions.append(ledger_event)
        if cash != execution.ending_cash_fen:
            raise ValueError("execution ending cash does not match accounting")
        self._cash_fen = cash
        self._lots = lots
        self._ledger.extend(additions)
        self._ledger_event_ids = event_ids
        self._ledger_source_ids = source_ids
        self._phase = "applied"

    def execution_view(self) -> AccountExecutionView:
        """处理回测中的成交执行只读视图。

        入参：
            无。
        返回值：
            返回只读视图（``AccountExecutionView``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Return ledger-owned balances after ``begin_session`` and before apply.
        """
        if self._phase != "open" or self._last_session is None:
            raise ValueError("execution_view requires an open begin_session")
        quantities, _, sellable = _AccountingSupport._lot_totals(
            self._lots, self._last_session
        )
        return AccountExecutionView(self._cash_fen, quantities, sellable)

    def mark_to_market(
        self, trade_date: date, closes: Mapping[InstrumentId, float]
    ) -> AccountSnapshot:
        """标记``to``市场数据。

        入参：
            trade_date：目标交易日期，类型为 ``date``。
            closes：参与本次处理的``closes``；调用方不得依赖未声明的顺序。
        返回值：
            返回``to``市场数据（``AccountSnapshot``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``RuntimeError``、``ValueError``。
        """
        if self._phase not in {"open", "applied"}:
            raise ValueError("mark_to_market requires an open session and may run once")
        _AccountingSupport._date(trade_date, "trade_date")
        if trade_date != self._last_session:
            raise ValueError("mark_to_market trade_date must match current session")
        prices = dict(self._last_mark_prices)
        prices.update(_AccountingSupport._prices(closes))
        cash, quantities, costs = _AccountingSupport._reduce_ledger(self._ledger)
        if cash != self._cash_fen:
            raise RuntimeError("ledger cash does not match account cash")
        lot_quantities, lot_costs, sellable = _AccountingSupport._lot_totals(
            self._lots, trade_date
        )
        if quantities != lot_quantities or costs != lot_costs:
            raise RuntimeError("ledger positions do not match lot state")
        positions: list[PositionSnapshot] = []
        for instrument_id in sorted(quantities, key=InstrumentId.canonical):
            quantity = quantities[instrument_id]
            if quantity == 0:
                continue
            try:
                close = prices[instrument_id]
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
                    _AccountingSupport._rounded_fen(close * quantity),
                )
            )
        market_value = sum(position.market_value_fen for position in positions)
        snapshot = AccountSnapshot(
            trade_date, cash, tuple(positions), market_value, cash + market_value
        )
        self._last_snapshot = snapshot
        self._last_mark_prices = prices
        self._phase = "marked"
        return snapshot


class _AccountingSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
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
        elif event.event_type is LedgerEventType.SELL and (
            event.quantity_delta >= 0
            or event.gross_value_fen <= 0
            or event.cost_basis_delta_fen > 0
            or event.cash_delta_fen != event.gross_value_fen - event.fees_fen
        ):
            raise ValueError("sell ledger event is invalid")

    @staticmethod
    def _copy_lots(
        lots: Mapping[InstrumentId, list[_Lot]],
    ) -> dict[InstrumentId, list[_Lot]]:
        return {
            instrument: [lot.copy() for lot in values]
            for instrument, values in lots.items()
        }

    @staticmethod
    def _unlock_lots(lots: Mapping[InstrumentId, list[_Lot]], trade_date: date) -> None:
        # Sellability is date-derived; retaining the sellable date avoids mutable flags.
        for values in lots.values():
            values.sort(key=lambda lot: (lot.buy_date, lot.sellable_date))

    @staticmethod
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
                        Decimal(lot.cost_basis_fen)
                        * Decimal(take)
                        / Decimal(lot.quantity)
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

    @staticmethod
    def _lot_totals(
        lots: Mapping[InstrumentId, list[_Lot]], trade_date: date
    ) -> tuple[
        dict[InstrumentId, int], dict[InstrumentId, int], dict[InstrumentId, int]
    ]:
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

    @staticmethod
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
                costs[instrument] = (
                    costs.get(instrument, 0) + event.cost_basis_delta_fen
                )
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

    @staticmethod
    def _validate_fill(result: FillResult) -> None:
        if result.requested_quantity != result.intent.quantity:
            raise ValueError("fill requested quantity is inconsistent with intent")
        if result.filled_quantity + result.unfilled_quantity != result.intent.quantity:
            raise ValueError("fill quantities are inconsistent with intent")
        expected_gross = _AccountingSupport._rounded_fen(
            Decimal(str(result.price)) * result.filled_quantity
        )
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
        partial_reasons = (
            {ExecutionReason.INSUFFICIENT_CASH, ExecutionReason.VOLUME_CAP}
            if result.intent.side is OrderSide.BUY
            else {ExecutionReason.INSUFFICIENT_SELLABLE, ExecutionReason.VOLUME_CAP}
        )
        if result.unfilled_quantity <= 0 or result.reason_code not in partial_reasons:
            raise ValueError("fill reason is inconsistent with partial fill")

    @staticmethod
    def _reserve_ledger_identity(
        event: LedgerEvent, event_ids: set[str], source_ids: set[str]
    ) -> None:
        if event.event_id in event_ids or event.source_id in source_ids:
            raise ValueError("ledger event_id and source_id must be unique")
        event_ids.add(event.event_id)
        source_ids.add(event.source_id)

    @staticmethod
    def _settlement_date(
        calendar: TradingCalendar, buy_date: date, sessions: int
    ) -> date:
        if not isinstance(calendar, TradingCalendar):
            raise TypeError("calendar must be a TradingCalendar")
        _AccountingSupport._date(buy_date, "buy_date")
        if type(sessions) is not int or sessions < 0:
            raise ValueError("settlement sessions must be nonnegative")
        result = buy_date
        for _ in range(sessions):
            result = calendar.next_session(result)
        return result

    @staticmethod
    def _prices(closes: Mapping[InstrumentId, float]) -> dict[InstrumentId, Decimal]:
        if not isinstance(closes, Mapping):
            raise TypeError("closes must be a mapping")
        return {
            instrument: _AccountingSupport._close_price(instrument, close)
            for instrument, close in closes.items()
        }

    @staticmethod
    def _quantity_mapping(
        values: Mapping[InstrumentId, int], name: str
    ) -> dict[InstrumentId, int]:
        if not isinstance(values, Mapping):
            raise TypeError(f"{name} must be a mapping")
        result = dict(values)
        for instrument, quantity in result.items():
            _AccountingSupport._instrument(instrument)
            _AccountingSupport._nonnegative_int(quantity, name)
        return result

    @staticmethod
    def _close_price(instrument: object, close: object) -> Decimal:
        _AccountingSupport._instrument(instrument)
        if isinstance(close, bool) or not isinstance(close, (int, float, Decimal)):
            raise TypeError("close must be finite and positive")
        if isinstance(close, float) and not isfinite(close):
            raise ValueError("close must be finite and positive")
        value = Decimal(str(close))
        if not value.is_finite() or value <= 0:
            raise ValueError("close must be finite and positive")
        return value

    @staticmethod
    def _rounded_fen(yuan: Decimal) -> int:
        return int((yuan * _CENT).quantize(_ONE, rounding=ROUND_HALF_UP))

    @staticmethod
    def _is_session(calendar: TradingCalendar, trade_date: date) -> bool:
        return trade_date in calendar.sessions(trade_date, trade_date)

    @staticmethod
    def _identifier(value: object, name: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be nonempty")

    @staticmethod
    def _instrument(value: object) -> None:
        if not isinstance(value, InstrumentId):
            raise TypeError("instrument_id must be an InstrumentId")

    @staticmethod
    def _date(value: object, name: str) -> None:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"{name} must be a date")

    @staticmethod
    def _integer(value: object, name: str) -> None:
        if type(value) is not int:
            raise ValueError(f"{name} must be an integer")

    @staticmethod
    def _nonnegative_int(value: object, name: str) -> None:
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")

    @staticmethod
    def _nonnegative_decimal(value: object, name: str) -> Decimal:
        if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
            raise ValueError(f"{name} must be a finite nonnegative Decimal")
        return value
