"""Behavioral coverage for point-in-time price adjustment reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from quant_core.data.adjustments import (
    ADJUSTMENT_EVENT_COMPONENTS_DTYPE,
    AdjustmentMode,
    PriceAdjustmentService,
)
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
    assert after_event["volume"].to_list()[:3] == [125, 137, 150]
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
    assert result["volume"].to_list() == [142, 157, 171, 149, 140]


def test_backward_adjustment_aggregates_same_day_share_ratios_before_factoring(
    tmp_path: Path,
) -> None:
    """Multiplying two same-day split factors uses one preclose twice and is wrong."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="bonus-a",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                share_ratio=0.1,
            ),
            _action_row(
                action_type="bonus-b",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                share_ratio=0.2,
            ),
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])

    assert result["open"].to_list()[:3] == pytest.approx(
        [10.0 / 1.3, 12.0 / 1.3, 16.0 / 1.3]
    )


def test_backward_adjustment_aggregates_same_day_cash_and_bonus(
    tmp_path: Path,
) -> None:
    """The cash and bonus components of one ex-date must share one daily factor."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="cash",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
            ),
            _action_row(
                action_type="bonus",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                share_ratio=0.1,
            ),
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])

    assert result["open"].to_list()[:3] == pytest.approx(
        [10.0 * 15.0 / 18.7, 12.0 * 15.0 / 18.7, 16.0 * 15.0 / 18.7]
    )


def test_backward_adjustment_rounds_volume_half_up_and_keeps_int64(
    tmp_path: Path,
) -> None:
    """Float volume output would break the canonical daily-bar volume contract."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="cash-and-bonus",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
                share_ratio=0.1,
            )
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])

    assert result.schema["volume"] == pl.Int64
    assert result["volume"].to_list()[:3] == [125, 137, 150]


def test_backward_adjustment_rejects_int64_volume_overflow(tmp_path: Path) -> None:
    """A reciprocal factor must not silently wrap a canonical Int64 volume."""
    overflow_rows = [
        (*_BAR_VALUES[0][:5], 2**63 - 1, _BAR_VALUES[0][-1]),
        *_BAR_VALUES[1:],
    ]
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="bonus",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                share_ratio=0.1,
            )
        ],
        bar_values=overflow_rows,
    )

    with pytest.raises(ValueError, match="adjusted volume exceeds Int64 range"):
        _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])


def test_backward_adjustment_uses_ex_date_after_requested_end(tmp_path: Path) -> None:
    """Only loading through end would omit a known later ex-date factor."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="later-than-end",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
                share_ratio=0.1,
            )
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[2], _DAYS[3])

    assert result["trade_date"].to_list() == _DAYS[:3]
    assert result["open"].to_list() == pytest.approx(
        [10.0 * 15.0 / 18.7, 12.0 * 15.0 / 18.7, 16.0 * 15.0 / 18.7]
    )
    assert result["adjustment_as_of"].to_list() == [_DAYS[3]] * 3


def test_backward_adjustment_ignores_future_duplicate_actions_before_key_check(
    tmp_path: Path,
) -> None:
    """A duplicate action after as_of must not corrupt the current usable set."""
    future = _action_row(
        action_type="future-duplicate",
        ex_date=_DAYS[-1],
        available_at=datetime(2024, 1, 4, tzinfo=UTC),
        cash_per_share=2.0,
    )
    fixture = _adjustment_fixture(tmp_path, [future, future])

    result = _backward_bars(fixture, _DAYS[0], _DAYS[2], _DAYS[3])

    assert result["open"].to_list() == [10.0, 12.0, 16.0]
    assert result["adjustment_mode"].to_list() == ["BACKWARD"] * 3


def test_backward_adjustment_empty_actions_preserves_schema_order_and_metadata(
    tmp_path: Path,
) -> None:
    """An empty action dataset must still return canonical raw bars plus metadata."""
    fixture = _adjustment_fixture(tmp_path, [])

    result = _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])

    assert result["trade_date"].to_list() == _DAYS
    assert result.schema["volume"] == pl.Int64
    assert result.schema["adjustment_mode"] == pl.String
    assert result.schema["adjustment_as_of"] == pl.Date
    assert result.schema["adjustment_event_components"] == (
        ADJUSTMENT_EVENT_COMPONENTS_DTYPE
    )
    assert result["adjustment_mode"].to_list() == ["BACKWARD"] * len(_DAYS)
    assert result["adjustment_as_of"].to_list() == [_DAYS[-1]] * len(_DAYS)
    assert result["adjustment_event_components"].to_list() == [[] for _ in _DAYS]


def test_backward_adjustment_exposes_exact_event_factor_and_availability(
    tmp_path: Path,
) -> None:
    """Without event-level lineage a factor cannot reconstruct an earlier PIT anchor."""
    available_at = datetime(2024, 1, 5, 18, tzinfo=UTC)
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="cash-and-bonus",
                ex_date=_DAYS[3],
                available_at=available_at,
                cash_per_share=2.0,
                share_ratio=0.1,
            )
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])

    event_factor = 15.0 / 18.7
    assert result["adjustment_factor"].to_list() == pytest.approx(
        [event_factor, event_factor, event_factor, 1.0, 1.0]
    )
    assert result["adjustment_event_factor"].to_list() == pytest.approx(
        [1.0, 1.0, 1.0, event_factor, 1.0]
    )
    assert result["adjustment_event_available_at"].to_list() == [
        None,
        None,
        None,
        available_at,
        None,
    ]


def test_backward_metadata_excludes_event_after_requested_rows(tmp_path: Path) -> None:
    """A future ex-date may scale rows globally but must not masquerade as a used event."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="after-output",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                cash_per_share=2.0,
                share_ratio=0.1,
            )
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[2], _DAYS[3])

    assert result["adjustment_factor"].to_list() == pytest.approx([15.0 / 18.7] * 3)
    assert result["adjustment_event_factor"].to_list() == [1.0] * 3
    assert result["adjustment_event_available_at"].to_list() == [None] * 3


def test_backward_metadata_preserves_same_day_component_availability(
    tmp_path: Path,
) -> None:
    """Collapsing component timestamps would hide the cash action known first."""
    cash_available = datetime(2024, 1, 4, 8, tzinfo=UTC)
    share_available = datetime(2024, 1, 5, 8, tzinfo=UTC)
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="cash",
                ex_date=_DAYS[3],
                available_at=cash_available,
                cash_per_share=2.0,
            ),
            _action_row(
                action_type="bonus",
                ex_date=_DAYS[3],
                available_at=share_available,
                share_ratio=0.1,
            ),
        ],
    )

    result = _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])

    assert result["adjustment_event_components"].to_list()[3] == [
        {
            "action_type": "bonus",
            "cash_per_share": 0.0,
            "share_ratio": 0.1,
            "available_at": share_available,
        },
        {
            "action_type": "cash",
            "cash_per_share": 2.0,
            "share_ratio": 0.0,
            "available_at": cash_available,
        },
    ]


def test_backward_adjustment_empty_bars_preserves_schema_and_metadata(
    tmp_path: Path,
) -> None:
    """Empty instrument results must retain typed metadata rather than a schema-less frame."""
    fixture = _adjustment_fixture(tmp_path, [])

    result = (
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))
        .bars(
            fixture.snapshot_id,
            [InstrumentId.parse("SZSE:000001")],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.BACKWARD,
            _DAYS[-1],
        )
        .collect()
    )

    assert result.is_empty()
    assert result.schema["volume"] == pl.Int64
    assert result.schema["adjustment_mode"] == pl.String
    assert result.schema["adjustment_as_of"] == pl.Date


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


def test_backward_adjustment_rejects_rights_price_without_independent_ratio(
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

    with pytest.raises(ValueError, match="rights_price is unsupported"):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.BACKWARD,
            _DAYS[-1],
        )


def test_backward_adjustment_rejects_any_rights_price_without_rights_ratio(
    tmp_path: Path,
) -> None:
    """Canonical share_ratio cannot safely stand in for a missing rights ratio."""
    fixture = _adjustment_fixture(
        tmp_path,
        [
            _action_row(
                action_type="rights",
                ex_date=_DAYS[3],
                available_at=datetime(2024, 1, 5, tzinfo=UTC),
                share_ratio=0.1,
                rights_price=8.0,
            )
        ],
    )

    with pytest.raises(ValueError, match="rights_price is unsupported"):
        _backward_bars(fixture, _DAYS[0], _DAYS[-1], _DAYS[-1])


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
    tmp_path: Path,
    actions: list[dict[str, object]],
    *,
    bar_values: list[tuple[float, float, float, float, float, int, float]]
    | None = None,
) -> AdjustmentFixture:
    base = point_in_time_fixture(tmp_path)
    bars = _write_dataset(
        tmp_path,
        "adjustment-bars",
        DatasetKind.DAILY_BAR,
        [
            _bar_row(day, values)
            for day, values in zip(_DAYS, bar_values or _BAR_VALUES, strict=True)
        ],
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


def _backward_bars(fixture: AdjustmentFixture, start: date, end: date, as_of: date):
    return (
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))
        .bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            start,
            end,
            AdjustmentMode.BACKWARD,
            as_of,
        )
        .collect()
    )


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
