"""Behavioral coverage for point-in-time price adjustment reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_core.data.adjustments import AdjustmentMode, PriceAdjustmentService
from quant_core.data.repository import SnapshotResearchRepository
from quant_core.domain.enums import DatasetKind
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from tests.fixtures.point_in_time import (
    FixtureSnapshotRepository,
    _write_dataset,
    point_in_time_fixture,
)

_INSTRUMENT = InstrumentId.parse("SSE:600000")
_DAYS = [date(2024, 1, day) for day in range(2, 7)]


@dataclass(frozen=True, slots=True)
class AdjustmentFixture:
    """One snapshot containing five raw bars and supplied corporate actions."""

    repository: FixtureSnapshotRepository
    snapshot_id: SnapshotId


def test_backward_adjustment_uses_only_actions_known_at_as_of(
    tmp_path: Path,
) -> None:
    """Dropping the PIT cutoff would adjust the first three days before day four."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="dividend",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
                share_ratio=0.1,
            ),
            _action_row(
                action_type="unusable",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=1.0,
                pit_usable=False,
            ),
        ],
    )
    service = PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))

    before_announcement = service.bars(
        fixture.snapshot_id,
        [_INSTRUMENT],
        _DAYS[0],
        _DAYS[2],
        AdjustmentMode.BACKWARD,
        _DAYS[2],
    ).collect()
    after_event = service.bars(
        fixture.snapshot_id,
        [_INSTRUMENT],
        _DAYS[0],
        _DAYS[-1],
        AdjustmentMode.BACKWARD,
        _DAYS[-1],
    ).collect()

    assert before_announcement["open"].to_list() == [10.0, 12.0, 16.0]
    assert before_announcement["volume"].to_list() == [100, 110, 120]
    assert before_announcement["adjustment_mode"].to_list() == ["BACKWARD"] * 3
    assert before_announcement["adjustment_as_of"].to_list() == [_DAYS[2]] * 3

    price_factor = 15.0 / 18.7
    assert after_event["open"].to_list()[:3] == pytest.approx(
        [10.0 * price_factor, 12.0 * price_factor, 16.0 * price_factor]
    )
    assert after_event["high"].to_list()[:3] == pytest.approx(
        [12.0 * price_factor, 14.0 * price_factor, 18.0 * price_factor]
    )
    assert after_event["low"].to_list()[:3] == pytest.approx(
        [9.0 * price_factor, 11.0 * price_factor, 15.0 * price_factor]
    )
    assert after_event["close"].to_list()[:3] == pytest.approx(
        [11.0 * price_factor, 13.0 * price_factor, 17.0 * price_factor]
    )
    assert after_event["preclose"].to_list()[:3] == pytest.approx(
        [10.0 * price_factor, 11.0 * price_factor, 15.0 * price_factor]
    )
    assert after_event["volume"].to_list()[:3] == pytest.approx(
        [100 / price_factor, 110 / price_factor, 120 / price_factor]
    )
    assert after_event["amount"].to_list() == [1100.0, 1430.0, 2040.0, 2310.0, 3100.0]
    assert after_event["open"].to_list()[3:] == [18.0, 25.0]
    assert after_event["volume"].to_list()[3:] == [130, 140]
    assert after_event["trade_date"].to_list() == _DAYS


def test_raw_mode_preserves_raw_ohlcv_values(tmp_path: Path) -> None:
    """Changing raw price or volume values would break consumers requesting RAW mode."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="dividend",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
                share_ratio=0.1,
            )
        ],
    )

    result = (
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))
        .bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.RAW,
            _DAYS[-1],
        )
        .collect()
    )

    assert result.select(
        "open", "high", "low", "close", "preclose", "volume"
    ).rows() == [
        (10.0, 12.0, 9.0, 11.0, 10.0, 100),
        (12.0, 14.0, 11.0, 13.0, 11.0, 110),
        (16.0, 18.0, 15.0, 17.0, 15.0, 120),
        (18.0, 20.0, 17.0, 19.0, 17.0, 130),
        (25.0, 27.0, 24.0, 26.0, 24.0, 140),
    ]
    assert result["adjustment_mode"].to_list() == ["RAW"] * 5
    assert result["adjustment_as_of"].to_list() == [_DAYS[-1]] * 5


def test_backward_adjustment_composes_multiple_events_in_ex_date_order(
    tmp_path: Path,
) -> None:
    """Omitting either event would leave the first day's hand-derived factor wrong."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="later",
                ex_date=_DAYS[4],
                available_at=datetime(2024, 1, 6, tzinfo=UTC),
                cash_per_share=3.0,
            ),
            _action_row(
                action_type="earlier",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
                share_ratio=0.1,
            ),
        ],
    )

    result = (
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))
        .bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.BACKWARD,
            _DAYS[-1],
        )
        .collect()
    )

    assert result["open"].to_list() == pytest.approx(
        [
            10.0 * (15.0 / 18.7) * (21.0 / 24.0),
            12.0 * (15.0 / 18.7) * (21.0 / 24.0),
            16.0 * (15.0 / 18.7) * (21.0 / 24.0),
            18.0 * (21.0 / 24.0),
            25.0,
        ]
    )
    assert result["volume"].to_list() == pytest.approx(
        [
            100.0 / ((15.0 / 18.7) * (21.0 / 24.0)),
            110.0 / ((15.0 / 18.7) * (21.0 / 24.0)),
            120.0 / ((15.0 / 18.7) * (21.0 / 24.0)),
            130.0 / (21.0 / 24.0),
            140.0,
        ]
    )


def test_backward_adjustment_rejects_nonpositive_event_factor(tmp_path: Path) -> None:
    """Allowing a cash distribution larger than preclose would fabricate negative prices."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="invalid-factor",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=20.0,
            )
        ],
    )

    with pytest.raises(ValueError, match="corporate action adjustment factor"):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.BACKWARD,
            _DAYS[-1],
        )


def test_backward_adjustment_rejects_rights_price_without_share_ratio(
    tmp_path: Path,
) -> None:
    """Using a rights price without issued shares would silently discard its economics."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="incomplete-rights",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                rights_price=8.0,
            )
        ],
    )

    with pytest.raises(
        ValueError, match="rights_price requires a positive share_ratio"
    ):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.BACKWARD,
            _DAYS[-1],
        )


def test_backward_adjustment_rejects_duplicate_action_primary_key(
    tmp_path: Path,
) -> None:
    """Silently double-counting duplicate canonical action keys would corrupt factors."""
    duplicate = _action_row(
        action_type="dividend",
        ex_date=_DAYS[3],
        available_at=datetime(2024, 1, 5, tzinfo=UTC),
        cash_per_share=2.0,
    )
    fixture = _adjustment_fixture(tmp_path, [duplicate, duplicate])

    with pytest.raises(ValueError, match="duplicate corporate action primary key"):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.BACKWARD,
            _DAYS[-1],
        )


@pytest.mark.parametrize(
    ("start", "end", "as_of", "message"),
    [
        (_DAYS[2], _DAYS[1], _DAYS[2], "start must not follow end"),
        (_DAYS[0], _DAYS[-1], _DAYS[-2], "as_of must not precede end"),
    ],
)
def test_adjustment_request_rejects_invalid_time_bounds(
    tmp_path: Path, start: date, end: date, as_of: date, message: str
) -> None:
    """Permitting invalid request bounds would make the PIT horizon ambiguous."""
    fixture = _adjustment_fixture(tmp_path, [])

    with pytest.raises(ValueError, match=message):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            start,
            end,
            AdjustmentMode.RAW,
            as_of,
        )


def _adjustment_fixture(
    tmp_path: Path, actions: list[dict[str, object]]
) -> AdjustmentFixture:
    base = point_in_time_fixture(tmp_path)
    bars = _write_dataset(
        tmp_path,
        "adjustment-bars",
        DatasetKind.DAILY_BAR,
        [_bar_row(day, values) for day, values in zip(_DAYS, _BAR_VALUES, strict=True)],
    )
    corporate_actions = _write_dataset(
        tmp_path,
        "adjustment-actions",
        DatasetKind.CORPORATE_ACTION,
        actions,
    )
    with_bars = base.repository.bind_dataset(
        base.late_snapshot_id, DatasetKind.DAILY_BAR, bars
    )
    snapshot_id = base.repository.bind_dataset(
        with_bars, DatasetKind.CORPORATE_ACTION, corporate_actions
    )
    return AdjustmentFixture(base.repository, snapshot_id)


_BAR_VALUES = [
    (10.0, 12.0, 9.0, 11.0, 10.0, 100, 1100.0),
    (12.0, 14.0, 11.0, 13.0, 11.0, 110, 1430.0),
    (16.0, 18.0, 15.0, 17.0, 15.0, 120, 2040.0),
    (18.0, 20.0, 17.0, 19.0, 17.0, 130, 2310.0),
    (25.0, 27.0, 24.0, 26.0, 24.0, 140, 3100.0),
]


def _bar_row(
    day: date, values: tuple[float, float, float, float, float, int, float]
) -> dict[str, object]:
    open_, high, low, close, preclose, volume, amount = values
    return {
        "instrument_id": _INSTRUMENT.canonical(),
        "trade_date": day,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "preclose": preclose,
        "volume": volume,
        "amount": amount,
        "adjustment_flag": "none",
        "turnover": 1.0,
        "pct_change": 0.1,
        "pe_ttm": 10.0,
        "pb_mrq": 1.0,
        "ps_ttm": 2.0,
        "pcf_ncf_ttm": 3.0,
        **_audit(datetime(2024, 1, 6, tzinfo=UTC)),
    }


def _action_row(
    *,
    action_type: str,
    ex_date: date,
    available_at: datetime | None,
    cash_per_share: float | None = None,
    share_ratio: float | None = None,
    rights_price: float | None = None,
    pit_usable: bool = True,
) -> dict[str, object]:
    return {
        "instrument_id": _INSTRUMENT.canonical(),
        "action_type": action_type,
        "record_date": ex_date,
        "ex_date": ex_date,
        "pay_date": ex_date,
        "cash_per_share": cash_per_share,
        "share_ratio": share_ratio,
        "rights_price": rights_price,
        **_audit(available_at, pit_usable=pit_usable),
    }


def _audit(
    available_at: datetime | None, *, pit_usable: bool = True
) -> dict[str, object]:
    return {
        "source": "fixture",
        "source_version": "v1",
        "available_at": available_at,
        "availability_source": "announcement",
        "pit_usable": pit_usable,
        "ingested_at": datetime(2024, 1, 6, tzinfo=UTC),
    }
