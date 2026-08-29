"""Opt-in evidence for columnar factor-study labels and quantile assignment."""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from quant_research.factor_studies.analysis import (
    DIRECTION_ADJUSTED,
    EXECUTABLE_FORWARD_RETURN,
    INDUSTRY_MARKET_CAP_NEUTRALIZED,
    INDUSTRY_NEUTRALIZED,
    THEORETICAL_FORWARD_RETURN,
    analyze,
    build_future_returns,
)
from quant_research.factor_studies.streaming import (
    FactorStudyTemporaryStore,
    StreamingStudyAnalyzer,
    TemporaryEvidence,
)
from quant_research.factors.analysis import assign_quantiles
from quant_research.factors.transforms import neutralize_industry_market_cap
from tests.performance._process_memory import process_peak_rss_bytes

pytestmark = pytest.mark.performance

_PARTITION_SIZE = 100
_FULL_SESSION_COUNT = 1_215
_FULL_INSTRUMENT_COUNT = 5_891
_FULL_FACTOR_INSTRUMENT_COUNT = 3_878
_BASELINE_ANALYSIS_SECONDS = 351.15476090001175
_MAX_ANALYSIS_SECONDS = 120.0
_BASELINE_ANALYSIS_PEAK_RSS_BYTES = int(8.5 * 1024**3)


class _Cancellation:
    """为独立性能进程提供永不取消的任务端口。"""

    def is_cancelled(self) -> bool:
        """返回固定未取消状态。"""
        return False


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
    executable_state = scope.select(
        "instrument_id",
        pl.col("signal_date").alias("trade_date"),
        pl.lit(True).alias("is_listed"),
        pl.lit(False).alias("is_suspended"),
        pl.lit(False).alias("entry_limit_up"),
    )

    labels_started = time.perf_counter()
    labels = build_future_returns(
        bars,
        sessions,
        eligible,
        (1, 5, 20),
        executable_state,
    )
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


def test_full_scale_factor_study_statistics_record_evidence() -> None:
    """五年全市场统计负载保持在约定耗时和内存上限内。"""
    sessions: list[date] = []
    current = date(2018, 1, 2)
    while len(sessions) < _FULL_SESSION_COUNT:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    session_frame = pl.DataFrame(
        {
            "signal_date": sessions,
            "_session_rank": range(_FULL_SESSION_COUNT),
        },
        schema={"signal_date": pl.Date, "_session_rank": pl.Int32},
    )
    instruments = pl.DataFrame(
        {
            "instrument_id": [
                f"{index:06d}.SZ" for index in range(_FULL_INSTRUMENT_COUNT)
            ],
            "_instrument_rank": range(_FULL_INSTRUMENT_COUNT),
        },
        schema={"instrument_id": pl.String, "_instrument_rank": pl.Int32},
    )
    scope = session_frame.join(instruments, how="cross")
    eligible = scope.select(
        "signal_date", "instrument_id", pl.lit(True).alias("eligible")
    )
    factor_base = scope.filter(
        pl.col("_instrument_rank") < _FULL_FACTOR_INSTRUMENT_COUNT
    ).select(
        "signal_date",
        "instrument_id",
        pl.lit("log_total_market_cap").alias("factor_id"),
        (
            pl.col("_instrument_rank").cast(pl.Float64)
            + pl.col("_session_rank").cast(pl.Float64) / 1_000_000.0
        ).alias("value"),
        pl.lit(True).alias("is_valid"),
    )
    factors = pl.concat(
        [
            factor_base.with_columns(
                pl.lit(variant).alias("signal_variant")
            )
            for variant in (DIRECTION_ADJUSTED, INDUSTRY_NEUTRALIZED)
        ]
    )
    return_base = scope.select(
        "signal_date",
        "instrument_id",
        (pl.col("signal_date") + pl.duration(days=1)).alias("return_start"),
        (pl.col("signal_date") + pl.duration(days=20)).alias("return_end"),
        (
            pl.col("_instrument_rank").cast(pl.Float64) / 100_000.0
            + pl.col("_session_rank").cast(pl.Float64) / 10_000_000.0
        ).alias("future_return"),
        pl.lit(True).alias("is_valid"),
        pl.lit(None, dtype=pl.String).alias("invalid_reason"),
    )
    future = {
        (horizon, label_kind): return_base.with_columns(
            pl.lit(horizon, dtype=pl.Int64).alias("horizon"),
            pl.lit(label_kind).alias("label_kind"),
        )
        for horizon in (1, 5, 20)
        for label_kind in (
            THEORETICAL_FORWARD_RETURN,
            EXECUTABLE_FORWARD_RETURN,
        )
    }

    started = time.perf_counter()
    outputs = analyze(
        factors,
        eligible,
        future,
        quantiles=5,
        cost_bps_scenarios=(5, 10, 20),
    )
    analysis_seconds = time.perf_counter() - started
    peak_rss_bytes = process_peak_rss_bytes()
    evidence = {
        "workload": "SYNTHETIC_FULL_FACTOR_STUDY_STATISTICS",
        "sessions": _FULL_SESSION_COUNT,
        "instruments": _FULL_INSTRUMENT_COUNT,
        "factor_rows": factors.height,
        "label_rows": sum(frame.height for frame in future.values()),
        "analysis_seconds": analysis_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "output_row_counts": {
            name: frame.height for name, frame in sorted(outputs.items())
        },
    }
    print(
        "factor_study_statistics_performance="
        f"{json.dumps(evidence, sort_keys=True)}"
    )

    assert outputs["summary"].height == 12
    assert outputs["ic"].height == 14_580
    assert outputs["quantile_returns"].height == 72_900
    assert analysis_seconds <= _MAX_ANALYSIS_SECONDS
    assert analysis_seconds <= _BASELINE_ANALYSIS_SECONDS * 0.4
    assert peak_rss_bytes <= _BASELINE_ANALYSIS_PEAK_RSS_BYTES


@pytest.mark.parametrize("factor_count", [1, 5])
def test_full_scale_streaming_factor_study_records_bounded_evidence(
    tmp_path: Path,
    factor_count: int,
) -> None:
    """单因子与五因子流式统计保持统一内存上限并记录临时磁盘峰值。"""
    sessions: list[date] = []
    current = date(2018, 1, 2)
    while len(sessions) < _FULL_SESSION_COUNT:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    session_frame = pl.DataFrame(
        {
            "signal_date": sessions,
            "_session_rank": range(_FULL_SESSION_COUNT),
        },
        schema={"signal_date": pl.Date, "_session_rank": pl.Int32},
    )
    instruments = pl.DataFrame(
        {
            "instrument_id": [
                f"{index:06d}.SZ" for index in range(_FULL_INSTRUMENT_COUNT)
            ],
            "_instrument_rank": range(_FULL_INSTRUMENT_COUNT),
        },
        schema={"instrument_id": pl.String, "_instrument_rank": pl.Int32},
    )
    scope = session_frame.join(instruments, how="cross")
    eligible = scope.select(
        "signal_date", "instrument_id", pl.lit(True).alias("eligible")
    )
    return_base = scope.select(
        "signal_date",
        "instrument_id",
        (pl.col("signal_date") + pl.duration(days=1)).alias("return_start"),
        (pl.col("signal_date") + pl.duration(days=20)).alias("return_end"),
        (
            pl.col("_instrument_rank").cast(pl.Float64) / 100_000.0
            + pl.col("_session_rank").cast(pl.Float64) / 10_000_000.0
        ).alias("future_return"),
    )
    wide = return_base.select(
        "signal_date",
        "instrument_id",
        "return_start",
        "return_end",
        pl.col("future_return").alias("theoretical_future_return"),
        pl.lit(True).alias("theoretical_is_valid"),
        pl.lit(None, dtype=pl.String).alias("theoretical_invalid_reason"),
        pl.col("future_return").alias("executable_future_return"),
        pl.lit(True).alias("executable_is_valid"),
        pl.lit(None, dtype=pl.String).alias("executable_invalid_reason"),
    )
    study_id = f"01M14STREAMINGPERF{factor_count:08d}"
    pipeline_started = time.perf_counter()
    with FactorStudyTemporaryStore(tmp_path, study_id) as temporary:
        signal_files = {}
        signal_started = time.perf_counter()
        for factor_index in range(factor_count):
            factor_ref = f"stream_factor_{factor_index}"
            factor = scope.select(
                "signal_date",
                "instrument_id",
                pl.lit(factor_ref).alias("factor_id"),
                (
                    pl.col("_instrument_rank").cast(pl.Float64)
                    + pl.col("_session_rank").cast(pl.Float64)
                    * (factor_index + 1)
                    / 1_000_000.0
                ).alias("value"),
                pl.lit(True).alias("is_valid"),
                pl.lit(None, dtype=pl.String).alias("invalid_reason"),
                (pl.col("_instrument_rank") % 31)
                .cast(pl.String)
                .str.pad_start(2, "0")
                .alias("_neutralization_industry"),
                ((pl.col("_instrument_rank") + 1).cast(pl.Float64) * 1_000_000.0)
                .alias("_neutralization_market_cap"),
            )
            direction_adjusted = factor.select(
                "signal_date",
                "instrument_id",
                "factor_id",
                "value",
                "is_valid",
                "invalid_reason",
                pl.lit(DIRECTION_ADJUSTED).alias("signal_variant"),
            )
            neutralized = neutralize_industry_market_cap(
                factor,
                "value",
                "_neutralization_market_cap",
                "_neutralization_industry",
                ("signal_date", "factor_id"),
            ).select(
                "signal_date",
                "instrument_id",
                "factor_id",
                "value",
                "is_valid",
                "invalid_reason",
                pl.lit(INDUSTRY_MARKET_CAP_NEUTRALIZED).alias("signal_variant"),
            )
            signal_files[(DIRECTION_ADJUSTED, factor_ref)] = temporary.write(
                "signal", direction_adjusted
            )
            signal_files[(
                INDUSTRY_MARKET_CAP_NEUTRALIZED,
                factor_ref,
            )] = temporary.write("signal", neutralized)
            del factor, direction_adjusted, neutralized
        signal_seconds = time.perf_counter() - signal_started
        label_files = {
            horizon: temporary.write("label", wide)
            for horizon in (1, 5, 20)
        }
        analysis_started = time.perf_counter()
        analyzer = StreamingStudyAnalyzer(
            quantiles=5,
            cost_bps_scenarios=(5, 10, 20),
            cancellation=_Cancellation(),
            temporary=temporary,
        )
        outputs = analyzer.run(signal_files, eligible, label_files)
        analysis_seconds = time.perf_counter() - analysis_started
        temporary_peak_bytes = TemporaryEvidence.byte_count(
            temporary.directory
        )
        pipeline_seconds = time.perf_counter() - pipeline_started
        peak_rss_bytes = process_peak_rss_bytes()

    evidence = {
        "workload": "SYNTHETIC_FULL_FACTOR_STUDY_STREAMING",
        "sessions": _FULL_SESSION_COUNT,
        "instruments": _FULL_INSTRUMENT_COUNT,
        "factor_count": factor_count,
        "analysis_seconds": analysis_seconds,
        "pipeline_seconds": pipeline_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "temporary_peak_bytes": temporary_peak_bytes,
        "signal_generation_seconds": signal_seconds,
        "stage_seconds": analyzer.performance_evidence,
        "output_row_counts": {
            name: frame.height for name, frame in sorted(outputs.items())
        },
    }
    print(
        "factor_study_streaming_performance="
        f"{json.dumps(evidence, sort_keys=True)}"
    )
    assert outputs["summary"].height == factor_count * 2 * 6
    assert outputs["ic"].height == factor_count * 2 * 6 * _FULL_SESSION_COUNT
    assert peak_rss_bytes <= int(8.5 * 1024**3)
    assert signal_seconds <= (15.0 if factor_count == 1 else 60.0)
    assert analysis_seconds <= (60.0 if factor_count == 1 else 300.0)
    assert pipeline_seconds <= (120.0 if factor_count == 1 else 300.0)
