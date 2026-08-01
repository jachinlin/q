"""Golden ledger: purchases, FIFO sale, record-date actions, and idempotency."""

from datetime import date
from decimal import Decimal

import polars as pl

from quant_core.backtest.accounting import (
    CorporateAction,
    CorporateActionType,
    LedgerEventType,
    PortfolioAccount,
)
from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.models import ExecutionBatch, ExecutionReason, FillResult
from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.portfolio.rebalance import OrderIntent, OrderSide

_A = InstrumentId.parse("SSE:600001")
_D2, _D3, _D4, _D5 = (date(2024, 1, day) for day in range(2, 6))


class _Repository:
    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        return pl.DataFrame(
            {"trade_date": [_D2, _D3, _D4, _D5], "is_trading_day": [True] * 4}
        ).lazy()


def _fill(
    day: date, side: OrderSide, quantity: int, gross: int, fee: int
) -> FillResult:
    return FillResult(
        OrderIntent(_A, side, quantity, "GOLDEN"),
        day,
        quantity,
        quantity,
        0,
        gross / quantity / 100,
        gross,
        FeeBreakdown(fee, 0, 0, fee),
        ExecutionReason.FILLED,
    )


def test_golden_ledger_for_t_plus_one_fifo_and_corporate_actions() -> None:
    calendar = TradingCalendar.load(
        _Repository(),
        SnapshotId.parse("00000000-0000-0000-0000-000000000105"),
        _D2,
        _D5,
    )
    account = PortfolioAccount(1_000_000, calendar)

    account.begin_session(_D2, ())
    account.apply(
        ExecutionBatch(_D2, (_fill(_D2, OrderSide.BUY, 100, 100_000, 500),), 899_500)
    )
    first = account.mark_to_market(_D2, {_A: 10.0})
    assert (
        first.cash_fen,
        first.positions[0].total_quantity,
        first.positions[0].sellable_quantity,
        first.positions[0].cost_basis_fen,
        first.nav_fen,
    ) == (899_500, 100, 0, 100_500, 999_500)

    account.begin_session(_D3, ())
    account.apply(
        ExecutionBatch(_D3, (_fill(_D3, OrderSide.SELL, 40, 44_000, 500),), 943_000)
    )
    second = account.mark_to_market(_D3, {_A: 11.0})
    assert (
        second.cash_fen,
        second.positions[0].total_quantity,
        second.positions[0].sellable_quantity,
        second.positions[0].cost_basis_fen,
        second.nav_fen,
    ) == (943_000, 60, 60, 60_300, 1_009_000)

    dividend = CorporateAction(
        "dividend", CorporateActionType.CASH_DIVIDEND, _A, _D3, _D4, Decimal("0.10")
    )
    bonus = CorporateAction(
        "bonus",
        CorporateActionType.BONUS_SHARES,
        _A,
        _D3,
        _D4,
        Decimal(0),
        Decimal("0.10"),
    )
    account.begin_session(_D4, (dividend, bonus))
    third = account.mark_to_market(_D4, {_A: 10.0})
    assert (
        third.cash_fen,
        third.positions[0].total_quantity,
        third.positions[0].sellable_quantity,
        third.positions[0].cost_basis_fen,
        third.total_market_value_fen,
        third.nav_fen,
    ) == (943_600, 66, 66, 60_300, 66_000, 1_009_600)

    assert [
        (
            event.event_type,
            event.cash_delta_fen,
            event.quantity_delta,
            event.cost_basis_delta_fen,
        )
        for event in account.ledger
    ] == [
        (LedgerEventType.OPENING_CASH, 1_000_000, 0, 0),
        (LedgerEventType.BUY, -100_500, 100, 100_500),
        (LedgerEventType.SELL, 43_500, -40, -40_200),
        (LedgerEventType.CASH_DIVIDEND, 600, 0, 0),
        (LedgerEventType.BONUS_SHARES, 0, 6, 0),
    ]

    before = account.ledger
    account.begin_session(_D5, (dividend, bonus))
    account.mark_to_market(_D5, {_A: 10.0})
    assert account.ledger == before
