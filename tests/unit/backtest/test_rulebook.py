"""Versioned A-share rulebook behavior and historical boundaries."""

from datetime import date
from pathlib import Path

import pytest

from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.rulebook import (
    AShareRuleBook,
    SecurityStatus,
    Side,
    SimulatedFill,
)
from quant_core.domain.identifiers import InstrumentId, SnapshotId

_RULES = Path("configs/rules/a_share_v1.yaml")
_MAIN = InstrumentId.parse("SSE:600000")
_CHINEXT = InstrumentId.parse("SZSE:300001")
_STAR = InstrumentId.parse("SSE:688001")
_SZSE_MAIN = InstrumentId.parse("SZSE:000001")
_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000102")


def test_rulebook_uses_versioned_explicit_config_and_board_lot_sizes() -> None:
    rulebook = AShareRuleBook.load(_RULES)

    assert rulebook.version == "a-share-v1"
    assert rulebook.lot_size(_MAIN, date(2024, 1, 2)) == 100
    assert rulebook.lot_size(_CHINEXT, date(2024, 1, 2)) == 100
    assert rulebook.lot_size(_STAR, date(2024, 1, 2)) == 200


@pytest.mark.parametrize(
    ("instrument", "day", "status", "upper", "lower"),
    [
        (_MAIN, date(2024, 1, 2), SecurityStatus.NORMAL, 11.00, 9.00),
        (_CHINEXT, date(2020, 8, 23), SecurityStatus.NORMAL, 11.00, 9.00),
        (_CHINEXT, date(2020, 8, 24), SecurityStatus.NORMAL, 12.00, 8.00),
        (_CHINEXT, date(2020, 8, 23), SecurityStatus.ST, 10.50, 9.50),
        (_CHINEXT, date(2020, 8, 24), SecurityStatus.ST, 12.00, 8.00),
        (_STAR, date(2024, 1, 2), SecurityStatus.ST, 12.00, 8.00),
        (_MAIN, date(2026, 7, 5), SecurityStatus.ST, 10.50, 9.50),
        (_MAIN, date(2026, 7, 6), SecurityStatus.ST, 11.00, 9.00),
    ],
)
def test_price_limits_follow_board_status_and_date_boundaries(
    instrument: InstrumentId,
    day: date,
    status: SecurityStatus,
    upper: float,
    lower: float,
) -> None:
    band = AShareRuleBook.load(_RULES).price_limits(instrument, day, 10.00, status)

    assert band is not None
    assert band.upper == upper
    assert band.lower == lower


def test_price_limits_reject_nonfinite_or_nonpositive_previous_close() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    for invalid in (0.0, -1.0, float("inf"), float("nan"), True):
        with pytest.raises(ValueError, match="finite positive"):
            rulebook.price_limits(
                _MAIN, date(2024, 1, 2), invalid, SecurityStatus.NORMAL
            )  # type: ignore[arg-type]


def test_t_plus_one_sell_date_uses_next_actual_session() -> None:
    calendar = TradingCalendar(
        _SNAPSHOT,
        date(2024, 2, 9),
        date(2024, 2, 19),
        (date(2024, 2, 9), date(2024, 2, 19)),
    )
    rulebook = AShareRuleBook.load(_RULES, calendar=calendar)

    assert rulebook.earliest_sell_date(date(2024, 2, 9), _MAIN) == date(2024, 2, 19)


def test_fees_match_explicit_current_and_historical_constants() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    buy = _fill(_MAIN, date(2023, 8, 28), Side.BUY, 10_000, 10.00)
    sell = _fill(_MAIN, date(2023, 8, 28), Side.SELL, 10_000, 10.00)
    old_sse_buy = _fill(_MAIN, date(2010, 1, 4), Side.BUY, 10_000, 10.00)

    assert rulebook.fees(buy).as_tuple() == (3_000, 0, 100, 3_100)
    assert rulebook.fees(sell).as_tuple() == (3_000, 5_000, 100, 8_100)
    assert rulebook.fees(old_sse_buy).as_tuple() == (3_000, 0, 500, 3_500)


def test_fees_apply_minimum_commission_and_historical_stamp_and_transfer_boundaries() -> (
    None
):
    rulebook = AShareRuleBook.load(_RULES)

    small = rulebook.fees(_fill(_MAIN, date(2023, 8, 28), Side.BUY, 10, 10.00))
    stamp_2007 = rulebook.fees(_fill(_MAIN, date(2007, 5, 30), Side.BUY, 10_000, 10.00))
    stamp_2008 = rulebook.fees(_fill(_MAIN, date(2008, 9, 19), Side.BUY, 10_000, 10.00))
    sse_2012 = rulebook.fees(_fill(_MAIN, date(2012, 6, 1), Side.BUY, 10_000, 10.00))
    szse_2012 = rulebook.fees(
        _fill(_SZSE_MAIN, date(2012, 6, 1), Side.BUY, 10_000, 10.00)
    )
    transfer_2015 = rulebook.fees(
        _fill(_MAIN, date(2015, 8, 1), Side.BUY, 10_000, 10.00)
    )

    assert small.as_tuple() == (500, 0, 0, 500)
    assert stamp_2007.stamp_duty_cents == 30_000
    assert stamp_2008.stamp_duty_cents == 0
    assert sse_2012.transfer_fee_cents == 375
    assert szse_2012.transfer_fee_cents == 255
    assert transfer_2015.transfer_fee_cents == 200


def test_rulebook_rejects_overlapping_gapped_and_unmatched_rule_intervals(
    tmp_path: Path,
) -> None:
    base = _RULES.read_text(encoding="utf-8")
    overlapping = tmp_path / "overlap.yaml"
    overlapping.write_text(
        base.replace("end: 2026-07-05", "end: 2026-07-06", 1), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="overlap"):
        AShareRuleBook.load(overlapping)

    gapped = tmp_path / "gap.yaml"
    gapped.write_text(
        base.replace("start: 2026-07-06", "start: 2026-07-07", 1), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="gap"):
        AShareRuleBook.load(gapped)

    with pytest.raises(ValueError, match="no configured rule"):
        AShareRuleBook.load(_RULES).price_limits(
            _MAIN, date(2005, 1, 23), 10.00, SecurityStatus.NORMAL
        )


def test_rulebook_rejects_out_of_order_rule_intervals(tmp_path: Path) -> None:
    base = _RULES.read_text(encoding="utf-8")
    earlier = (
        "  - {board: MAIN, status: ST, start: 2005-01-24, "
        'end: 2026-07-05, rate: "0.05"}'
    )
    later = '  - {board: MAIN, status: ST, start: 2026-07-06, end: null, rate: "0.10"}'
    unordered = tmp_path / "unordered.yaml"
    unordered.write_text(
        base.replace(earlier + "\n" + later, later + "\n" + earlier), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="strictly increasing"):
        AShareRuleBook.load(unordered)


def _fill(
    instrument: InstrumentId, day: date, side: Side, quantity: int, price: float
) -> SimulatedFill:
    return SimulatedFill(
        instrument=instrument,
        trade_date=day,
        side=side,
        quantity=quantity,
        price=price,
    )
