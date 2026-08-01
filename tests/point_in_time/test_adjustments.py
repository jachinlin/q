"""Behavioral coverage for point-in-time price adjustment reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from inspect import getsource
from math import exp, log
from pathlib import Path
from random import Random

import polars as pl
import pytest

from quant_core.data.adjustments import (
    ADJUSTMENT_EVENT_COMPONENTS_DTYPE,
    FORWARD_LOG_RETURN_COLUMN,
    AdjustmentMode,
    PriceAdjustmentService,
    _forward_adjust,
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


def test_raw_and_backward_modes_do_not_expose_forward_log_returns(
    tmp_path: Path,
) -> None:
    """The row-local return contract belongs only to FORWARD research reads."""
    fixture = _adjustment_fixture(tmp_path, [])
    service = PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))

    for mode in (AdjustmentMode.RAW, AdjustmentMode.BACKWARD):
        result = service.bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            mode,
            _DAYS[-1],
        ).collect()

        assert FORWARD_LOG_RETURN_COLUMN not in result.columns


def test_forward_adjustment_uses_baostock_preclose_without_action_dataset(
    tmp_path: Path,
) -> None:
    """Forward prices must follow raw preclose gaps without corporate-action lineage."""
    raw_values = [
        (9.0, 11.0, 8.0, 10.0, 0.0, 100, 1000.0),
        (11.0, 13.0, 10.0, 12.0, 10.0, 110, 1320.0),
        (8.0, 9.0, 7.0, 8.4, 8.0, 120, 1008.0),
        (8.5, 9.5, 8.0, 9.0, 8.4, 130, 1170.0),
    ]
    base = point_in_time_fixture(tmp_path)
    bars = _write_dataset(
        tmp_path,
        "forward-bars",
        DatasetKind.DAILY_BAR,
        [
            _bar_row(day, values)
            for day, values in zip(_DAYS[:4], raw_values, strict=True)
        ],
    )
    snapshot_id = base.repository.bind_dataset(
        base.late_snapshot_id, DatasetKind.DAILY_BAR, bars
    )

    result = (
        PriceAdjustmentService(SnapshotResearchRepository(base.repository))
        .bars(
            snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[2],
            AdjustmentMode.FORWARD,
            _DAYS[3],
        )
        .collect()
    )

    expected_factors = [2.0 / 3.0, 2.0 / 3.0, 1.0]
    assert result["adjustment_factor"].to_list() == pytest.approx(expected_factors)
    assert result["close"].to_list() == pytest.approx([20.0 / 3.0, 8.0, 8.4])
    assert result["preclose"].to_list() == pytest.approx([0.0, 20.0 / 3.0, 8.0])
    assert result["close"].to_list()[:2] == pytest.approx(
        result["preclose"].to_list()[1:]
    )
    assert result["volume"].to_list() == [100, 110, 120]
    assert result["amount"].to_list() == [1000.0, 1320.0, 1008.0]
    assert result["adjustment_mode"].unique().to_list() == ["FORWARD"]
    assert result.schema["adjustment_event_components"] == (
        ADJUSTMENT_EVENT_COMPONENTS_DTYPE
    )
    assert result["adjustment_event_factor"].to_list() == [1.0, 1.0, 1.0]
    assert result["adjustment_event_available_at"].to_list() == [None, None, None]
    assert result["adjustment_event_components"].to_list() == [[], [], []]
    assert result.schema["forward_return_index"] == pl.Float64
    assert result["forward_return_index"].to_list() == pytest.approx([10.0, 12.0, 12.6])
    assert result.schema[FORWARD_LOG_RETURN_COLUMN] == pl.Float64
    assert result[FORWARD_LOG_RETURN_COLUMN].to_list() == [
        None,
        log(12.0) - log(10.0),
        log(8.4) - log(8.0),
    ]


def test_forward_adjustment_sorts_and_isolates_instruments() -> None:
    """Cross-instrument ordering must not let one close set another's factor."""
    result, factors = _forward_adjust(
        _forward_frame(
            [
                ("SSE:600001", _DAYS[1], 30.0, 10.0),
                ("SSE:600000", _DAYS[1], 12.0, 8.0),
                ("SSE:600001", _DAYS[0], 20.0, 0.0),
                ("SSE:600000", _DAYS[0], 10.0, None),
            ]
        ),
        _DAYS[1],
    )

    assert result.select("instrument_id", "trade_date").rows() == [
        ("SSE:600000", _DAYS[0]),
        ("SSE:600000", _DAYS[1]),
        ("SSE:600001", _DAYS[0]),
        ("SSE:600001", _DAYS[1]),
    ]
    assert factors == pytest.approx([0.8, 1.0, 0.5, 1.0])


def test_forward_adjustment_uses_original_reverse_recurrence_order() -> None:
    """A total-minus-prefix rewrite must not change the deterministic recurrence."""
    start = date(2020, 1, 1)
    log_gaps = [700.0 if index % 2 == 0 else -699.999999 for index in range(200)]
    frame = _forward_frame(
        [
            ("SSE:600000", start, 1.0, 0.0),
            *[
                (
                    "SSE:600000",
                    start + timedelta(days=index),
                    1.0,
                    exp(log_gap),
                )
                for index, log_gap in enumerate(log_gaps, start=1)
            ],
        ]
    )

    _, factors = _forward_adjust(frame, start + timedelta(days=200))

    assert factors == _trusted_reverse_forward_factors(frame)
    assert factors[-1] == 1.0


def test_forward_adjustment_anchor_is_exact_one_for_every_instrument() -> None:
    """Every independently grouped anchor is the unscaled raw observation exactly."""
    start = date(2020, 1, 1)
    random = Random(20260801)
    rows: list[tuple[str, date, float | None, float | None]] = []
    for instrument_id, count in (("SSE:600000", 500), ("SSE:600001", 137)):
        rows.append((instrument_id, start, 1.0, 0.0))
        rows.extend(
            (
                instrument_id,
                start + timedelta(days=index),
                1.0,
                random.uniform(0.95, 1.05),
            )
            for index in range(1, count)
        )

    result, factors = _forward_adjust(
        _forward_frame(list(reversed(rows))), start + timedelta(days=499)
    )

    anchors = (
        result.with_columns(pl.Series("factor", factors))
        .group_by("instrument_id", maintain_order=True)
        .agg(pl.col("factor").last())
    )
    assert anchors["factor"].to_list() == [1.0, 1.0]


def test_forward_adjustment_accepts_ipo_preclose_and_missing_sessions() -> None:
    """Only non-initial observed rows require a positive preclose."""
    result, factors = _forward_adjust(
        _forward_frame(
            [
                ("SSE:600000", _DAYS[0], 10.0, None),
                ("SSE:600000", _DAYS[2], 11.0, 9.0),
            ]
        ),
        _DAYS[2],
    )

    assert result["trade_date"].to_list() == [_DAYS[0], _DAYS[2]]
    assert factors == pytest.approx([0.9, 1.0])


def test_forward_adjustment_empty_bars_preserves_schema() -> None:
    """Empty reads need the same price columns for metadata construction."""
    frame = _forward_frame([])

    result, factors = _forward_adjust(frame, _DAYS[-1])

    assert result.drop("forward_return_index", FORWARD_LOG_RETURN_COLUMN).schema == (
        frame.schema
    )
    assert result.schema["forward_return_index"] == pl.Float64
    assert result.schema[FORWARD_LOG_RETURN_COLUMN] == pl.Float64
    assert result.is_empty()
    assert factors == []


@pytest.mark.parametrize(
    "invalid_close", [None, 0.0, -0.0, -1.0, float("nan"), float("inf"), -float("inf")]
)
def test_forward_adjustment_rejects_invalid_anchor_close(
    invalid_close: float | None,
) -> None:
    """The anchor close is output data even though no later ratio consumes it."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 10.0, 0.0),
            ("SSE:600000", _DAYS[1], invalid_close, 10.0),
        ]
    )

    with pytest.raises(ValueError, match="close must be finite and positive"):
        _forward_adjust(frame, _DAYS[1])


@pytest.mark.parametrize(
    "invalid_preclose", [-1.0, float("nan"), float("inf"), -float("inf")]
)
def test_forward_adjustment_rejects_invalid_first_preclose(
    invalid_preclose: float,
) -> None:
    """Only null and zero relax the first observed preclose constraint."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 10.0, invalid_preclose),
            ("SSE:600000", _DAYS[1], 11.0, 10.0),
        ]
    )

    with pytest.raises(ValueError, match="first.*preclose|preclose.*first"):
        _forward_adjust(frame, _DAYS[1])


@pytest.mark.parametrize("first_preclose", [None, 0.0, -0.0, 9.0])
def test_forward_adjustment_accepts_valid_first_preclose(
    first_preclose: float | None,
) -> None:
    """IPO null/zero and an ordinary positive first preclose are all canonical."""
    result, _ = _forward_adjust(
        _forward_frame(
            [
                ("SSE:600000", _DAYS[0], 10.0, first_preclose),
                ("SSE:600000", _DAYS[1], 11.0, 10.0),
            ]
        ),
        _DAYS[1],
    )

    assert result["preclose"].item(0) == first_preclose
    expected_log_return = (
        None if not first_preclose else log(10.0) - log(first_preclose)
    )
    assert result[FORWARD_LOG_RETURN_COLUMN].item(0) == expected_log_return


@pytest.mark.parametrize("column", ["open", "high", "low"])
@pytest.mark.parametrize(
    "invalid_value", [0.0, -0.0, -1.0, float("nan"), float("inf"), -float("inf")]
)
def test_forward_adjustment_rejects_invalid_nonnull_ohlc(
    column: str, invalid_value: float
) -> None:
    """Every non-null output price input must be finite and positive."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 10.0, 0.0),
            ("SSE:600000", _DAYS[1], 11.0, 10.0),
        ]
    ).with_columns(pl.lit(invalid_value, dtype=pl.Float64).alias(column))

    with pytest.raises(ValueError, match=rf"{column} must be finite and positive"):
        _forward_adjust(frame, _DAYS[1])


@pytest.mark.parametrize("column", ["open", "high", "low"])
def test_forward_adjustment_preserves_null_optional_ohlc(column: str) -> None:
    """Canonical nullable OHLC values remain null instead of being fabricated."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 10.0, 0.0),
            ("SSE:600000", _DAYS[1], 11.0, 10.0),
        ]
    ).with_columns(pl.lit(None, dtype=pl.Float64).alias(column))

    result, _ = _forward_adjust(frame, _DAYS[1])

    assert result[column].to_list() == [None, None]


def test_forward_adjustment_rejects_adjusted_price_overflow(tmp_path: Path) -> None:
    """A finite raw price times a finite factor must not escape as infinity."""
    overflowing = [
        (1e308, 1e308, 1.0, 1.0, 0.0, 100, 100.0),
        (10.0, 10.0, 10.0, 10.0, 10.0, 100, 1000.0),
        (10.0, 10.0, 10.0, 10.0, 10.0, 100, 1000.0),
        (10.0, 10.0, 10.0, 10.0, 10.0, 100, 1000.0),
        (10.0, 10.0, 10.0, 10.0, 10.0, 100, 1000.0),
    ]
    fixture = _adjustment_fixture(tmp_path, [], bar_values=overflowing)

    with pytest.raises(ValueError, match="adjusted open must be finite and positive"):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.FORWARD,
            _DAYS[-1],
        )


def test_forward_adjustment_rejects_adjusted_price_underflow_to_zero(
    tmp_path: Path,
) -> None:
    """A positive raw price must not silently become a zero adjusted price."""
    smallest_positive = float.fromhex("0x0.0000000000001p-1022")
    underflowing = [
        (smallest_positive, 1.0, smallest_positive, 1.0, 0.0, 100, 100.0),
        (1.0, 1.0, 1.0, 1.0, 0.5, 100, 100.0),
        (1.0, 1.0, 1.0, 1.0, 1.0, 100, 100.0),
        (1.0, 1.0, 1.0, 1.0, 1.0, 100, 100.0),
        (1.0, 1.0, 1.0, 1.0, 1.0, 100, 100.0),
    ]
    fixture = _adjustment_fixture(tmp_path, [], bar_values=underflowing)

    with pytest.raises(ValueError, match="adjusted open must be finite and positive"):
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository)).bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.FORWARD,
            _DAYS[-1],
        )


def test_forward_adjustment_rejects_forward_return_index_overflow() -> None:
    """A finite factor cannot make an infinite chained return index acceptable."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 1.0, 0.0),
            ("SSE:600000", _DAYS[1], 1e308, 1e-308),
        ]
    )

    with pytest.raises(ValueError, match="forward return index"):
        _forward_adjust(frame, _DAYS[1])


def test_forward_adjustment_rejects_forward_return_index_underflow_to_zero() -> None:
    """A positive raw return ratio must not silently collapse the index to zero."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 1.0, 0.0),
            (
                "SSE:600000",
                _DAYS[1],
                float.fromhex("0x0.0000000000001p-1022"),
                1e308,
            ),
        ]
    )

    with pytest.raises(ValueError, match="forward return index"):
        _forward_adjust(frame, _DAYS[1])


@pytest.mark.parametrize(
    ("close", "preclose", "expected_log_return"),
    [
        (
            float.fromhex("0x0.0000000000001p-1022"),
            1e308,
            -1453.6362805635472,
        ),
        (
            1e308,
            float.fromhex("0x0.0000000000001p-1022"),
            1453.6362805635472,
        ),
    ],
)
def test_forward_service_accepts_finite_extreme_log_difference(
    tmp_path: Path,
    close: float,
    preclose: float,
    expected_log_return: float,
) -> None:
    """A ratio may under/overflow even when its endpoint log difference is finite."""
    bars = [
        (close, close, close, close, preclose, 100, 100.0),
        *[
            (close, close, close, close, close, 100, 100.0)
            for _ in range(len(_DAYS) - 1)
        ],
    ]
    fixture = _adjustment_fixture(tmp_path, [], bar_values=bars)

    result = (
        PriceAdjustmentService(SnapshotResearchRepository(fixture.repository))
        .bars(
            fixture.snapshot_id,
            [_INSTRUMENT],
            _DAYS[0],
            _DAYS[-1],
            AdjustmentMode.FORWARD,
            _DAYS[-1],
        )
        .collect()
    )

    assert result["close"].to_list() == [close] * len(_DAYS)
    assert result["adjustment_factor"].to_list() == [1.0] * len(_DAYS)
    assert result[FORWARD_LOG_RETURN_COLUMN].to_list() == [
        expected_log_return,
        0.0,
        0.0,
        0.0,
        0.0,
    ]


def test_forward_log_return_matches_python_log_difference_exactly() -> None:
    """Constructing an intermediate ratio changes an ordinary return's bytes."""
    frame = _forward_frame(
        [
            ("SSE:600000", _DAYS[0], 12.0, 10.0),
        ]
    )

    result, _ = _forward_adjust(frame, _DAYS[0])

    expected = log(12.0) - log(10.0)
    ratio_derived = log(12.0 / 10.0)
    assert result[FORWARD_LOG_RETURN_COLUMN].item() == expected
    assert ratio_derived != expected


def test_forward_log_return_is_not_reconstructed_from_return_index() -> None:
    """Cumulative-index division introduces a different rounding path."""
    start = date(2024, 1, 1)
    close_preclose = [
        (417.2032526727958, 0.0),
        (11.159000287611292, 63.693106442399056),
        (825.3813027444894, 649.1346331055908),
        (299.3412153443932, 973.2343656088159),
        (369.0432777989872, 142.6340933148375),
        (194.46768769602917, 559.5773370292396),
        (566.4421605601324, 897.9650208542995),
        (162.52613610543884, 264.49172948091484),
        (125.14261739924663, 210.79839988840118),
        (433.503331741906, 718.0093727295953),
        (562.516409587767, 794.5188785368044),
    ]
    frame = _forward_frame(
        [
            ("SSE:600000", start + timedelta(days=index), close, preclose)
            for index, (close, preclose) in enumerate(close_preclose)
        ]
    )

    result, _ = _forward_adjust(frame, start + timedelta(days=10))

    expected = log(562.516409587767) - log(794.5188785368044)
    index_derived = log(
        result["forward_return_index"].item(10) / result["forward_return_index"].item(9)
    )
    assert result[FORWARD_LOG_RETURN_COLUMN].item(10) == expected
    assert index_derived != expected


def test_forward_log_return_is_byte_stable_across_request_starts() -> None:
    """Overlapping raw rows must have identical returns across request starts."""
    start = date(2024, 1, 7)
    frame = _forward_frame(
        [
            ("SSE:600001", start + timedelta(days=3), 203.0, 0.7 * 201.0),
            ("SSE:600000", start, 102.87106291432033, 0.0),
            ("SSE:600001", start + timedelta(days=1), 199.0, 140.0),
            (
                "SSE:600000",
                start + timedelta(days=1),
                101.65705717852148,
                72.00974404002423,
            ),
            ("SSE:600001", start, 200.0, None),
            (
                "SSE:600000",
                start + timedelta(days=3),
                103.0,
                0.7 * 101.51431209232835,
            ),
            ("SSE:600001", start + timedelta(days=2), 201.0, 199.0),
            (
                "SSE:600000",
                start + timedelta(days=2),
                101.51431209232835,
                101.65705717852148,
            ),
        ]
    )
    later_start = start + timedelta(days=1)

    full, _ = _forward_adjust(frame, start + timedelta(days=3))
    later, _ = _forward_adjust(
        frame.filter(pl.col("trade_date") >= later_start),
        start + timedelta(days=3),
    )
    overlap = full.filter(pl.col("trade_date") >= later_start)

    assert later.schema[FORWARD_LOG_RETURN_COLUMN] == pl.Float64
    assert later[FORWARD_LOG_RETURN_COLUMN].null_count() == 0
    assert (
        overlap[FORWARD_LOG_RETURN_COLUMN].to_numpy().tobytes()
        == later[FORWARD_LOG_RETURN_COLUMN].to_numpy().tobytes()
    )
    assert later[FORWARD_LOG_RETURN_COLUMN].to_list() == [
        log(101.65705717852148) - log(72.00974404002423),
        log(101.51431209232835) - log(101.65705717852148),
        log(103.0) - log(0.7 * 101.51431209232835),
        log(199.0) - log(140.0),
        log(201.0) - log(199.0),
        log(203.0) - log(0.7 * 201.0),
    ]


def test_forward_return_index_is_byte_stable_after_nonbinary_future_jumps() -> None:
    """A 0.7 future jump must not change any earlier research-price bytes."""
    prefix = _forward_frame(
        [
            ("SSE:600001", _DAYS[2], 204.0, 202.0),
            ("SSE:600000", _DAYS[0], 100.0, 0.0),
            ("SSE:600001", _DAYS[0], 200.0, None),
            ("SSE:600000", _DAYS[2], 102.0, 101.0),
            ("SSE:600001", _DAYS[1], 202.0, 200.0),
            ("SSE:600000", _DAYS[1], 101.0, 100.0),
        ]
    )
    future = _forward_frame(
        [
            ("SSE:600000", _DAYS[3], 103.0, 0.7 * 102.0),
            ("SSE:600001", _DAYS[3], 205.0, 0.5 * 204.0),
        ]
    )

    short, _ = _forward_adjust(prefix, _DAYS[2])
    extended, _ = _forward_adjust(pl.concat([prefix, future]), _DAYS[2])

    assert short.schema["forward_return_index"] == pl.Float64
    assert short["forward_return_index"].to_list() == pytest.approx(
        [100.0, 101.0, 102.0, 200.0, 202.0, 204.0]
    )
    assert (
        short["forward_return_index"].to_numpy().tobytes()
        == extended["forward_return_index"].to_numpy().tobytes()
    )
    assert short[FORWARD_LOG_RETURN_COLUMN].item(0) is None
    assert extended[FORWARD_LOG_RETURN_COLUMN].item(0) is None
    assert (
        short[FORWARD_LOG_RETURN_COLUMN].slice(1).to_numpy().tobytes()
        == extended[FORWARD_LOG_RETURN_COLUMN].slice(1).to_numpy().tobytes()
    )


def test_forward_adjustment_uses_vectorized_path_at_research_scale() -> None:
    """Research-scale adjustment must not materialize row dictionaries in Python."""
    source = getsource(_forward_adjust)
    assert "partition_by" not in source
    assert "to_dicts" not in source
    sessions = 250
    row_count = 50_000
    frame = (
        pl.DataFrame({"_i": pl.arange(0, row_count, eager=True)})
        .with_columns(
            (pl.col("_i") // sessions).cast(pl.String).alias("instrument_id"),
            (
                pl.lit(date(2020, 1, 1)) + pl.duration(days=pl.col("_i") % sessions)
            ).alias("trade_date"),
            (100.0 + (pl.col("_i") % sessions).cast(pl.Float64) * 0.01).alias("close"),
            pl.when((pl.col("_i") % sessions) == 0)
            .then(0.0)
            .otherwise(100.0 + ((pl.col("_i") % sessions) - 1).cast(pl.Float64) * 0.01)
            .alias("preclose"),
        )
        .with_columns(
            pl.col("close").alias("open"),
            pl.col("close").alias("high"),
            pl.col("close").alias("low"),
        )
        .select(
            "instrument_id",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "preclose",
        )
    )

    result, factors = _forward_adjust(frame, date(2030, 12, 31))

    assert result.height == row_count
    assert len(factors) == row_count
    assert result["forward_return_index"].is_finite().all()
    assert result[FORWARD_LOG_RETURN_COLUMN].drop_nulls().is_finite().all()


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[0], 11.0, 10.0),
            ],
            "duplicate daily bar key",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, None),
            ],
            "preclose must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 0.0),
            ],
            "preclose must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, -0.0),
            ],
            "preclose must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, -1.0),
            ],
            "preclose must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, float("nan")),
            ],
            "preclose must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 10.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, float("inf")),
            ],
            "preclose must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], None, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 10.0),
            ],
            "close must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 0.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 10.0),
            ],
            "close must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], -0.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 10.0),
            ],
            "close must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], -1.0, 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 10.0),
            ],
            "close must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], float("nan"), 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 10.0),
            ],
            "close must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], float("inf"), 0.0),
                ("SSE:600000", _DAYS[1], 12.0, 10.0),
            ],
            "close must be finite and positive",
        ),
        (
            [
                ("SSE:600000", _DAYS[0], 1e-308, 0.0),
                ("SSE:600000", _DAYS[1], 1.0, 1e308),
            ],
            "forward adjustment factor must be finite and positive",
        ),
    ],
)
def test_forward_adjustment_rejects_invalid_factor_inputs(
    rows: list[tuple[str, date, float | None, float | None]], message: str
) -> None:
    """Invalid observed gap inputs must fail rather than fabricate a factor."""
    with pytest.raises(ValueError, match=message):
        _forward_adjust(_forward_frame(rows), _DAYS[-1])


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
            AdjustmentMode.FORWARD,
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


def _forward_frame(
    rows: list[tuple[str, date, float | None, float | None]],
) -> pl.DataFrame:
    return (
        pl.DataFrame(
            rows,
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "close": pl.Float64,
                "preclose": pl.Float64,
            },
            orient="row",
        )
        .with_columns(
            pl.col("close").alias("open"),
            pl.col("close").alias("high"),
            pl.col("close").alias("low"),
        )
        .select(
            "instrument_id",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "preclose",
        )
    )


def _trusted_reverse_forward_factors(frame: pl.DataFrame) -> list[float]:
    """Mirror the original last-to-first scalar recurrence as a test oracle."""
    expected: list[float] = []
    ordered = frame.sort("instrument_id", "trade_date")
    for instrument in ordered.partition_by("instrument_id", maintain_order=True):
        rows = instrument.select("close", "preclose").rows()
        factors = [1.0] * len(rows)
        log_factor = 0.0
        for index in range(len(rows) - 1, 0, -1):
            previous_close = rows[index - 1][0]
            current_preclose = rows[index][1]
            assert previous_close is not None
            assert current_preclose is not None
            log_factor += log(current_preclose) - log(previous_close)
            factors[index - 1] = exp(log_factor)
        expected.extend(factors)
    return expected


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
