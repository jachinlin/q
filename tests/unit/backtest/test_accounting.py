"""Behavioral contracts for precise, T+1 portfolio accounting."""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from quant_core.backtest.accounting import (
    AccountSnapshot,
    CorporateAction,
    CorporateActionType,
    LedgerEvent,
    LedgerEventType,
    PortfolioAccount,
)
from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.models import ExecutionBatch, ExecutionReason, FillResult
from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.portfolio.rebalance import OrderIntent, OrderSide

_A = InstrumentId.parse("SSE:600001")
_B = InstrumentId.parse("SSE:600002")
_D2 = date(2024, 1, 2)
_D3 = date(2024, 1, 3)
_D4 = date(2024, 1, 4)
_D5 = date(2024, 1, 5)


class _CalendarRepository:
    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        return pl.DataFrame(
            {"trade_date": [_D2, _D3, _D4, _D5], "is_trading_day": [True] * 4}
        ).lazy()


def _calendar() -> TradingCalendar:
    return TradingCalendar.load(
        _CalendarRepository(),
        SnapshotId.parse("00000000-0000-0000-0000-000000000104"),
        _D2,
        _D5,
    )


def _fill(
    side: OrderSide,
    quantity: int,
    gross: int,
    fee: int = 0,
    *,
    instrument: InstrumentId = _A,
    trade_date: date = _D2,
) -> FillResult:
    return FillResult(
        OrderIntent(instrument, side, quantity, "TEST"),
        trade_date,
        quantity,
        quantity,
        0,
        gross / quantity / 100,
        gross,
        FeeBreakdown(fee, 0, 0, fee),
        ExecutionReason.FILLED,
    )


def _batch(
    *fills: FillResult, ending_cash: int, trade_date: date = _D2
) -> ExecutionBatch:
    return ExecutionBatch(trade_date, fills, ending_cash)


def test_t_plus_one_unlocks_only_on_the_next_actual_session() -> None:
    account = PortfolioAccount(1_000_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 100, 100_000), ending_cash=900_000))
    day_two = account.mark_to_market(_D2, {_A: 10.0})

    account.begin_session(_D3, ())
    day_three = account.mark_to_market(_D3, {_A: 10.0})

    assert (
        day_two.positions[0].total_quantity,
        day_two.positions[0].sellable_quantity,
    ) == (
        100,
        0,
    )
    assert (
        day_three.positions[0].total_quantity,
        day_three.positions[0].sellable_quantity,
    ) == (100, 100)


def test_apply_rejects_bad_ending_cash_atomically() -> None:
    account = PortfolioAccount(1_000_000, _calendar())
    account.begin_session(_D2, ())

    with pytest.raises(ValueError, match="ending cash"):
        account.apply(_batch(_fill(OrderSide.BUY, 100, 100_000), ending_cash=900_001))

    snapshot = account.mark_to_market(_D2, {})
    assert (snapshot.cash_fen, snapshot.positions, len(account.ledger)) == (
        1_000_000,
        (),
        1,
    )


def test_sell_fails_closed_when_it_exceeds_sellable_quantity() -> None:
    account = PortfolioAccount(1_000_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 100, 100_000), ending_cash=900_000))
    account.mark_to_market(_D2, {_A: 10.0})
    account.begin_session(_D3, ())

    with pytest.raises(ValueError, match="sellable"):
        account.apply(
            _batch(
                _fill(OrderSide.SELL, 101, 101_000, trade_date=_D3),
                ending_cash=1_001_000,
                trade_date=_D3,
            )
        )

    snapshot = account.mark_to_market(_D3, {_A: 10.0})
    assert (snapshot.cash_fen, snapshot.positions[0].total_quantity) == (900_000, 100)


def test_fifo_partial_sale_rounds_half_up_and_final_sale_consumes_residual_cost() -> (
    None
):
    account = PortfolioAccount(10_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 3, 300, 2), ending_cash=9_698))
    account.mark_to_market(_D2, {_A: 1.0})
    account.begin_session(_D3, ())
    account.apply(
        _batch(
            _fill(OrderSide.SELL, 1, 100, trade_date=_D3),
            ending_cash=9_798,
            trade_date=_D3,
        )
    )
    one_left = account.mark_to_market(_D3, {_A: 1.0})
    account.begin_session(_D4, ())
    account.apply(
        _batch(
            _fill(OrderSide.SELL, 2, 200, trade_date=_D4),
            ending_cash=9_998,
            trade_date=_D4,
        )
    )
    cleared = account.mark_to_market(_D4, {})

    assert one_left.positions[0].cost_basis_fen == 201
    assert cleared.positions == ()
    assert sum(event.cost_basis_delta_fen for event in account.ledger) == 0


def test_record_date_entitlements_are_idempotent_and_require_evidence() -> None:
    account = PortfolioAccount(1_000_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 100, 100_000), ending_cash=900_000))
    account.mark_to_market(_D2, {_A: 10.0})
    account.begin_session(_D3, ())
    account.mark_to_market(_D3, {_A: 10.0})
    dividend = CorporateAction(
        "div-1", CorporateActionType.CASH_DIVIDEND, _A, _D3, _D4, Decimal("0.10")
    )
    bonus = CorporateAction(
        "bonus-1",
        CorporateActionType.BONUS_SHARES,
        _A,
        _D3,
        _D4,
        Decimal(0),
        Decimal("0.10"),
    )

    account.begin_session(_D4, (dividend, bonus))
    snapshot = account.mark_to_market(_D4, {_A: 10.0})
    before = account.ledger
    account.begin_session(_D5, (dividend, bonus))
    repeated = account.mark_to_market(_D5, {_A: 10.0})

    assert (
        snapshot.cash_fen,
        snapshot.positions[0].total_quantity,
        snapshot.positions[0].sellable_quantity,
    ) == (901_000, 110, 110)
    assert (
        repeated.cash_fen,
        repeated.positions[0].total_quantity,
        account.ledger,
    ) == (901_000, 110, before)

    missing = PortfolioAccount(1, _calendar())
    with pytest.raises(ValueError, match="record-date"):
        missing.begin_session(_D4, (dividend,))


def test_state_machine_price_validation_and_ledger_derived_snapshot() -> None:
    account = PortfolioAccount(1_000, _calendar())
    with pytest.raises(ValueError, match="begin"):
        account.apply(_batch(ending_cash=1_000))
    account.begin_session(_D2, ())
    with pytest.raises(ValueError, match="positive fen"):
        account.mark_to_market(_D2, {_A: 0.001})
    snapshot = account.mark_to_market(_D2, {})
    assert isinstance(snapshot, AccountSnapshot)
    assert snapshot.nav_fen == sum(event.cash_delta_fen for event in account.ledger)
    with pytest.raises(ValueError, match="mark"):
        account.mark_to_market(_D2, {})


def test_immutable_models_reject_invalid_direct_construction() -> None:
    with pytest.raises(ValueError, match="nonempty"):
        CorporateAction("", CorporateActionType.CASH_DIVIDEND, _A, _D2, _D2, Decimal(1))
    with pytest.raises(ValueError, match="cash"):
        CorporateAction(
            "x", CorporateActionType.CASH_DIVIDEND, _A, _D2, _D2, Decimal(0)
        )
    with pytest.raises(ValueError, match="integer"):
        LedgerEvent("x", LedgerEventType.BUY, _D2, _A, 1.5, 1, 1, 1, 0, "x")
