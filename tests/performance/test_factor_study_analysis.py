"""Opt-in evidence for columnar factor-study labels and quantile assignment."""

from __future__ import annotations

import json
import time
from datetime import date, timedelta

import polars as pl
import pytest

from quant_research.factor_studies.analysis import build_future_returns
from quant_research.factors.analysis import assign_quantiles
from tests.performance._process_memory import process_peak_rss_bytes

pytestmark = pytest.mark.performance

_PARTITION_SIZE = 100


def test_twenty_year_partition_labels_and_quantiles_record_evidence() -> None:
    """A maximum instrument partition stays in native columnar execution."""
    current = date(2006, 1, 2)
    end = date(2025, 12, 31)
    session_values: list[date] = []
    while current <= end:
        if current.weekday() < 5:
            session_values.append(current)
        current += timedelta(days=1)
    sessions = tuple(session_values)
    instruments = pl.DataFrame(
        {
            "instrument_id": [
                f"{600_000 + index:06d}.SH" for index in range(_PARTITION_SIZE)
            ],
            "_instrument_rank": [float(index) for index in range(_PARTITION_SIZE)],
        },
        schema={"instrument_id": pl.String, "_instrument_rank": pl.Float64},
    )
    session_frame = pl.DataFrame(
        {
            "signal_date": sessions,
            "_session_rank": [float(index) for index in range(len(sessions))],
        },
        schema={"signal_date": pl.Date, "_session_rank": pl.Float64},
    )
    scope = session_frame.join(instruments, how="cross")
    eligible = scope.select(
        "signal_date", "instrument_id", pl.lit(True).alias("eligible")
    )
    bars = scope.select(
        "instrument_id",
        pl.col("signal_date").alias("trade_date"),
        (10.0 + pl.col("_instrument_rank") / 100.0).alias("open"),
        (
            10.0
            + pl.col("_instrument_rank") / 100.0
            + pl.col("_session_rank") / 100_000.0
        ).alias("close"),
    )
    factors = scope.select(
        "signal_date",
        "instrument_id",
        pl.col("_instrument_rank").alias("value"),
        pl.lit(True).alias("is_valid"),
    )

    labels_started = time.perf_counter()
    labels = build_future_returns(bars, sessions, eligible, (1, 5, 20))
    label_seconds = time.perf_counter() - labels_started
    quantiles_started = time.perf_counter()
    assigned = assign_quantiles(factors, 5)
    quantile_seconds = time.perf_counter() - quantiles_started

    expected_rows = len(sessions) * _PARTITION_SIZE
    assert all(frame.height == expected_rows for frame in labels.values())
    assert assigned.height == expected_rows
    assert assigned.group_by("signal_date", "quantile").len()["len"].min() == 20
    evidence = {
        "workload": "SYNTHETIC_MAX_FACTOR_STUDY_PARTITION",
        "sessions": len(sessions),
        "instruments": _PARTITION_SIZE,
        "rows": expected_rows,
        "peak_memory_bytes": process_peak_rss_bytes(),
        "stage_seconds": {
            "build_future_returns": label_seconds,
            "assign_quantiles": quantile_seconds,
        },
    }
    print(f"factor_study_performance={json.dumps(evidence, sort_keys=True)}")
