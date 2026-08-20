"""使用字面量行情验证双均线和 ETF Allocation 信号。"""

from datetime import UTC, date, datetime, timedelta

import polars as pl

from quant_research.signals.builtin import (
    DualMovingAverageSignal,
    EtfRotationAllocationSignal,
)
from quant_research.signals.models import ArtifactIdentity, Direction


def _identity(start: date, end: date) -> ArtifactIdentity:
    return ArtifactIdentity("run", "component", "a" * 64, "b" * 64, None, start, end)


def test_dual_ma_marks_first_complete_window_and_crossing() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=index) for index in range(6)]
    frame = pl.DataFrame(
        {
            "trade_date": dates,
            "instrument_id": ["510300.SH"] * 6,
            "close": [3.0, 2.0, 1.0, 2.0, 4.0, 5.0],
            "available_at": [datetime.combine(item, datetime.min.time(), UTC) for item in dates],
        },
        schema_overrides={"trade_date": pl.Date, "available_at": pl.Datetime("us", "UTC")},
    )
    artifact = DualMovingAverageSignal(short_window=2, long_window=3).compute(
        _identity(dates[0], dates[-1]), frame, dates
    )
    assert [item.is_valid for item in artifact.rows] == [False, False, True, True, True, True]
    assert [item.direction for item in artifact.rows[2:]] == [Direction.FLAT, Direction.FLAT, Direction.LONG, Direction.LONG]
    assert artifact.rows[4].state_changed is True


def test_etf_rotation_emits_normalized_allocation() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=index) for index in range(5)]
    rows: list[dict[str, object]] = []
    for instrument, prices in (("510300.SH", [1.0, 1.1, 1.2, 1.3, 1.4]), ("510050.SH", [1.0, 1.0, 1.0, 1.0, 1.0])):
        rows.extend(
            {
                "trade_date": day,
                "instrument_id": instrument,
                "close": price,
                "available_at": datetime.combine(day, datetime.min.time(), UTC),
            }
            for day, price in zip(dates, prices, strict=True)
        )
    frame = pl.DataFrame(rows).with_columns(pl.col("trade_date").cast(pl.Date), pl.col("available_at").cast(pl.Datetime("us", "UTC")))
    artifact = EtfRotationAllocationSignal(
        return_weights={1: 1.0}, trend_window=1, volatility_window=2, volatility_penalty=0.0, top_n=1
    ).compute(_identity(dates[0], dates[-1]), frame, (dates[-1],))
    exposures = {item.instrument_id: item.desired_exposure for item in artifact.rows}
    assert exposures == {"510050.SH": 0.0, "510300.SH": 1.0}
