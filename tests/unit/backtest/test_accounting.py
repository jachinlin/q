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
    PositionSnapshot,
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
_D8 = date(2024, 1, 8)


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


def _calendar_for(*sessions: date) -> TradingCalendar:
    class Repository:
        def trade_calendar(
            self, snapshot_id: SnapshotId, start: date, end: date
        ) -> pl.LazyFrame:
            return pl.DataFrame(
                {"trade_date": sessions, "is_trading_day": [True] * len(sessions)}
            ).lazy()

    return TradingCalendar.load(
        Repository(),
        SnapshotId.parse("00000000-0000-0000-0000-000000000106"),
        sessions[0],
        sessions[-1],
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


def _partial_fill(
    *,
    requested: int,
    filled: int,
    unfilled: int,
    reason: ExecutionReason,
    side: OrderSide = OrderSide.BUY,
    trade_date: date = _D2,
) -> FillResult:
    return FillResult(
        OrderIntent(_A, side, requested, "TEST"),
        trade_date,
        requested,
        filled,
        unfilled,
        0.01,
        filled,
        FeeBreakdown(0, 0, 0, 0),
        reason,
    )


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


def test_t_plus_one_unlocks_after_weekend_on_next_loaded_session() -> None:
    friday = date(2024, 2, 9)
    tuesday = date(2024, 2, 20)
    account = PortfolioAccount(1_000, _calendar_for(friday, tuesday))

    account.begin_session(friday, ())
    account.apply(
        _batch(
            _fill(OrderSide.BUY, 1, 100, trade_date=friday),
            ending_cash=900,
            trade_date=friday,
        )
    )
    account.mark_to_market(friday, {_A: 1.0})
    account.begin_session(tuesday, ())
    snapshot = account.mark_to_market(tuesday, {_A: 1.0})

    assert snapshot.positions[0].sellable_quantity == 1


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


def test_apply_rejects_semantically_inconsistent_fill_results() -> None:
    account = PortfolioAccount(1_000, _calendar())
    account.begin_session(_D2, ())

    inconsistent = _partial_fill(
        requested=100,
        filled=50,
        unfilled=50,
        reason=ExecutionReason.FILLED,
    )
    with pytest.raises(ValueError, match="fill"):
        account.apply(_batch(inconsistent, ending_cash=950))

    mismatched_requested = FillResult(
        OrderIntent(_A, OrderSide.BUY, 100, "TEST"),
        _D2,
        50,
        50,
        0,
        0.01,
        50,
        FeeBreakdown(0, 0, 0, 0),
        ExecutionReason.FILLED,
    )
    with pytest.raises(ValueError, match="requested quantity"):
        account.apply(_batch(mismatched_requested, ending_cash=950))

    hard_reject = _partial_fill(
        requested=100,
        filled=50,
        unfilled=50,
        reason=ExecutionReason.SUSPENDED,
    )
    with pytest.raises(ValueError, match="fill"):
        account.apply(_batch(hard_reject, ending_cash=950))


def test_apply_rejects_cross_side_partial_fill_reasons() -> None:
    buy_account = PortfolioAccount(1_000, _calendar())
    buy_account.begin_session(_D2, ())
    buy_with_sell_reason = _partial_fill(
        requested=100,
        filled=50,
        unfilled=50,
        reason=ExecutionReason.INSUFFICIENT_SELLABLE,
    )
    with pytest.raises(ValueError, match="partial fill"):
        buy_account.apply(_batch(buy_with_sell_reason, ending_cash=950))

    sell_account = PortfolioAccount(1_000, _calendar())
    sell_account.begin_session(_D2, ())
    sell_with_buy_reason = _partial_fill(
        requested=100,
        filled=50,
        unfilled=50,
        reason=ExecutionReason.INSUFFICIENT_CASH,
        side=OrderSide.SELL,
    )
    with pytest.raises(ValueError, match="partial fill"):
        sell_account.apply(_batch(sell_with_buy_reason, ending_cash=1_050))


def test_apply_preserves_multifill_cash_identity_and_rolls_back_second_failure() -> (
    None
):
    account = PortfolioAccount(1_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(
        _batch(
            _fill(OrderSide.BUY, 1, 100, instrument=_A),
            _fill(OrderSide.BUY, 1, 200, 10, instrument=_B),
            ending_cash=690,
        )
    )
    snapshot = account.mark_to_market(_D2, {_A: 1.0, _B: 2.0})
    assert snapshot.cash_fen == 690

    account.begin_session(_D3, ())
    with pytest.raises(ValueError, match="negative"):
        account.apply(
            _batch(
                _fill(OrderSide.BUY, 1, 100, trade_date=_D3),
                _fill(OrderSide.BUY, 1, 700, trade_date=_D3),
                ending_cash=0,
                trade_date=_D3,
            )
        )
    unchanged = account.mark_to_market(_D3, {_A: 1.0, _B: 2.0})
    assert (unchanged.cash_fen, len(account.ledger)) == (690, 3)


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


def test_corporate_action_batch_rolls_back_when_later_action_lacks_record_evidence() -> (
    None
):
    account = PortfolioAccount(1_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 1, 100), ending_cash=900))
    account.mark_to_market(_D2, {_A: 1.0})
    account.begin_session(_D3, ())
    account.mark_to_market(_D3, {_A: 1.0})
    valid = CorporateAction(
        "valid", CorporateActionType.CASH_DIVIDEND, _A, _D3, _D4, Decimal(1)
    )
    invalid = CorporateAction(
        "invalid", CorporateActionType.CASH_DIVIDEND, _A, _D4, _D4, Decimal(1)
    )

    with pytest.raises(ValueError, match="record-date"):
        account.begin_session(_D4, (valid, invalid))

    assert [(event.event_type, event.cash_delta_fen) for event in account.ledger] == [
        (LedgerEventType.OPENING_CASH, 1_000),
        (LedgerEventType.BUY, -100),
    ]


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


def test_state_machine_rejects_non_session_dates_wrong_batch_date_and_missing_close() -> (
    None
):
    account = PortfolioAccount(1_000, _calendar())
    with pytest.raises(ValueError, match="session"):
        account.begin_session(date(2024, 1, 6), ())
    account.begin_session(_D2, ())
    with pytest.raises(ValueError, match="trade_date"):
        account.apply(_batch(ending_cash=1_000, trade_date=_D3))
    account.apply(_batch(_fill(OrderSide.BUY, 1, 100), ending_cash=900))
    with pytest.raises(ValueError, match="missing close"):
        account.mark_to_market(_D2, {})
    snapshot = account.mark_to_market(_D2, {_A: 1.0})
    assert snapshot.cash_fen == 900


def test_state_machine_rejects_repeat_apply_wrong_mark_date_and_repeat_begin() -> None:
    account = PortfolioAccount(1_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 1, 100), ending_cash=900))
    ledger_after_apply = account.ledger
    with pytest.raises(ValueError, match="apply"):
        account.apply(_batch(ending_cash=900))
    with pytest.raises(ValueError, match="trade_date"):
        account.mark_to_market(_D3, {_A: 1.0})
    marked = account.mark_to_market(_D2, {_A: 1.0})
    with pytest.raises(ValueError, match="strictly increasing"):
        account.begin_session(_D2, ())
    account.begin_session(_D3, ())

    assert (marked.cash_fen, account.ledger) == (900, ledger_after_apply)


def test_account_snapshot_direct_construction_rejects_inconsistent_balances() -> None:
    position = PositionSnapshot(_A, 1, 0, 0, 100)

    with pytest.raises(ValueError, match="cash_fen"):
        AccountSnapshot(_D2, -1, (), 0, 0)
    with pytest.raises(ValueError, match="positions"):
        AccountSnapshot(_D2, 0, (), 1, 1)
    with pytest.raises(ValueError, match="total_market_value"):
        AccountSnapshot(_D2, 0, (position,), 99, 99)
    with pytest.raises(ValueError, match="nav_fen"):
        AccountSnapshot(_D2, 0, (position,), 100, 99)


def test_ledger_identity_uses_stable_namespaces_and_unique_source_ids() -> None:
    account = PortfolioAccount(1_000, _calendar())
    account.begin_session(_D2, ())
    account.apply(_batch(_fill(OrderSide.BUY, 1, 100), ending_cash=900))
    account.mark_to_market(_D2, {_A: 1.0})
    account.begin_session(_D3, ())
    account.mark_to_market(_D3, {_A: 1.0})
    dividend = CorporateAction(
        "cash", CorporateActionType.CASH_DIVIDEND, _A, _D3, _D4, Decimal(1)
    )
    account.begin_session(_D4, (dividend,))

    events = account.ledger
    assert events[0].event_id == "account:opening-cash"
    assert events[0].source_id == "account:init"
    assert events[1].event_id == "execution:2024-01-02:0"
    assert events[1].source_id == "execution:2024-01-02:0:SSE:600001:BUY"
    assert events[2].event_id == events[2].source_id == "corporate-action:cash"
    assert len({event.event_id for event in events}) == len(events)
    assert len({event.source_id for event in events}) == len(events)


def test_immutable_models_reject_invalid_direct_construction() -> None:
    with pytest.raises(ValueError, match="nonempty"):
        CorporateAction("", CorporateActionType.CASH_DIVIDEND, _A, _D2, _D2, Decimal(1))
    with pytest.raises(ValueError, match="cash"):
        CorporateAction(
            "x", CorporateActionType.CASH_DIVIDEND, _A, _D2, _D2, Decimal(0)
        )
    with pytest.raises(ValueError, match="integer"):
        LedgerEvent("x", LedgerEventType.BUY, _D2, _A, 1.5, 1, 1, 1, 0, "x")
    with pytest.raises(ValueError, match="zero quantity"):
        PositionSnapshot(_A, 0, 0, 1, 0)
    with pytest.raises(ValueError, match="market_value"):
        PositionSnapshot(_A, 1, 0, 0, 0)
    with pytest.raises(ValueError, match="buy"):
        LedgerEvent("x", LedgerEventType.BUY, _D2, _A, 0, 1, 0, 0, 0, "source")
    with pytest.raises(ValueError, match="sell"):
        LedgerEvent("x", LedgerEventType.SELL, _D2, _A, 1, 1, 0, 1, 0, "source")
