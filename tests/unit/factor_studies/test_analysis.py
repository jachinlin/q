from __future__ import annotations

import hashlib
from datetime import date, timedelta
from io import BytesIO

import polars as pl
import pytest

from quant_research.factor_studies.analysis import (
    DIRECTION_ADJUSTED,
    EXECUTABLE_FORWARD_RETURN,
    INDUSTRY_NEUTRALIZED,
    THEORETICAL_FORWARD_RETURN,
    HacMeanAnalyzer,
    analyze,
    build_future_returns,
)


def executable_state(
    bars: pl.DataFrame, *, suspended: bool = False, limit_up: bool = False
) -> pl.DataFrame:
    """返回与行情日期对齐的可执行标签状态。"""
    return bars.select("instrument_id", "trade_date").with_columns(
        pl.lit(True).alias("is_listed"),
        pl.lit(suspended).alias("is_suspended"),
        pl.lit(limit_up).alias("entry_limit_up"),
    )


def study_labels(
    values: dict[int, pl.DataFrame],
) -> dict[tuple[int, str], pl.DataFrame]:
    """把测试收益表适配为理论标签完整契约。"""
    return {
        (horizon, THEORETICAL_FORWARD_RETURN): frame.with_columns(
            pl.lit(horizon).alias("horizon"),
            pl.lit(THEORETICAL_FORWARD_RETURN).alias("label_kind"),
            pl.col("future_return").is_not_null().alias("is_valid"),
            pl.when(pl.col("future_return").is_null())
            .then(pl.lit("MISSING_EXIT_PRICE"))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("invalid_reason"),
        )
        for horizon, frame in values.items()
    }


def run_analysis(
    factors: pl.DataFrame,
    eligible: pl.DataFrame,
    labels: dict[int, pl.DataFrame],
    *,
    quantiles: int = 5,
) -> dict[str, pl.DataFrame]:
    """使用固定成本运行统计内核。"""
    return analyze(
        factors,
        eligible,
        study_labels(labels),
        quantiles=quantiles,
        cost_bps_scenarios=(5, 10, 20),
    )


def test_coverage_schema_accepts_reason_after_inference_window() -> None:
    days = [date(2026, 1, 1) + timedelta(days=index) for index in range(101)]
    instruments = ["000001.SZ", "000002.SZ"]
    eligible = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "eligible": True,
            }
            for day in days
            for instrument_id in instruments
        ]
    )
    factors = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "factor_id": factor_id,
                "value": float(instrument_index),
                "is_valid": factor_id == "all_valid" or instrument_index == 0,
            }
            for factor_id in ("all_valid", "later_invalid")
            for day in days
            for instrument_index, instrument_id in enumerate(instruments)
        ]
    )
    future = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "return_start": day + timedelta(days=1),
                "return_end": day + timedelta(days=1),
                "future_return": float(instrument_index) / 100.0,
            }
            for day in days
            for instrument_index, instrument_id in enumerate(instruments)
        ]
    )

    coverage = analyze(
        factors,
        eligible,
        study_labels({1: future}),
        quantiles=2,
        cost_bps_scenarios=(5,),
        minimum=2,
    )["coverage"]

    assert coverage.schema["quality_reason"] == pl.String
    assert coverage.filter(pl.col("factor_ref") == "all_valid")[
        "quality_reason"
    ].null_count() == 101
    assert set(
        coverage.filter(pl.col("factor_ref") == "later_invalid")[
            "quality_reason"
        ].to_list()
    ) == {"INSUFFICIENT_CROSS_SECTION"}


def test_future_return_uses_next_open_and_horizon_close() -> None:
    sessions = tuple(date(2026, 1, 5) + timedelta(days=index) for index in range(4))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"] * 4,
            "trade_date": list(sessions),
            "open": [9.0, 10.0, 11.0, 12.0],
            "close": [9.5, 11.0, 12.0, 13.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )

    result = build_future_returns(
        bars, sessions, eligible, (1, 2), executable_state(bars)
    )

    assert result[(1, THEORETICAL_FORWARD_RETURN)]["future_return"].item() == pytest.approx(0.1)
    assert result[(2, THEORETICAL_FORWARD_RETURN)]["future_return"].item() == pytest.approx(0.2)
    assert result[(1, THEORETICAL_FORWARD_RETURN)].select("return_start", "return_end").row(0) == (
        sessions[1],
        sessions[1],
    )


def test_future_return_keeps_incomplete_window_null() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"],
            "trade_date": [sessions[0]],
            "open": [10.0],
            "close": [10.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )
    assert (
        build_future_returns(bars, sessions, eligible, (1,), executable_state(bars))[(1, THEORETICAL_FORWARD_RETURN)]["future_return"].item()
        is None
    )


def test_future_return_rejects_suspended_next_session() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ", "000001.SZ"],
            "trade_date": list(sessions),
            "open": [9.0, 10.0],
            "close": [9.5, 11.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )
    tradability = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"],
            "trade_date": [sessions[1]],
            "is_listed": [True],
            "is_suspended": [True],
            "entry_limit_up": [False],
        }
    )

    result = build_future_returns(bars, sessions, eligible, (1,), tradability)

    assert result[(1, EXECUTABLE_FORWARD_RETURN)]["future_return"].item() is None
    assert result[(1, THEORETICAL_FORWARD_RETURN)]["future_return"].item() == pytest.approx(0.1)


def test_future_return_rejects_missing_next_session_status() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ", "000001.SZ"],
            "trade_date": list(sessions),
            "open": [9.0, 10.0],
            "close": [9.5, 11.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )
    empty_status = pl.DataFrame(
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "is_listed": pl.Boolean,
            "is_suspended": pl.Boolean,
            "entry_limit_up": pl.Boolean,
        }
    )

    result = build_future_returns(bars, sessions, eligible, (1,), empty_status)

    assert result[(1, EXECUTABLE_FORWARD_RETURN)]["future_return"].item() is None


def test_executable_label_reason_priority_does_not_contaminate_theoretical() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    instruments = ["000001.SZ", "000002.SZ", "000003.SZ"]
    bars = pl.DataFrame(
        {
            "instrument_id": instruments * 2,
            "trade_date": [sessions[0]] * 3 + [sessions[1]] * 3,
            "open": [9.0, 9.0, 9.0, 10.0, 10.0, 10.0],
            "close": [9.5, 9.5, 9.5, 11.0, 11.0, 11.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]] * 3,
            "instrument_id": instruments,
            "eligible": [True] * 3,
        }
    )
    state = pl.DataFrame(
        {
            "instrument_id": instruments,
            "trade_date": [sessions[1]] * 3,
            "is_listed": [False, True, True],
            "is_suspended": [True, True, False],
            "entry_limit_up": [True, True, True],
        }
    )

    result = build_future_returns(bars, sessions, eligible, (1,), state)

    assert result[(1, EXECUTABLE_FORWARD_RETURN)]["invalid_reason"].to_list() == [
        "NOT_LISTED_AT_ENTRY",
        "ENTRY_SUSPENDED",
        "ENTRY_LIMIT_UP",
    ]
    theoretical = result[(1, THEORETICAL_FORWARD_RETURN)]
    assert theoretical["is_valid"].to_list() == [True, True, True]
    assert theoretical["future_return"].to_list() == pytest.approx([0.1] * 3)


def test_future_returns_vectorize_shuffled_multi_instrument_scope() -> None:
    sessions = tuple(date(2026, 1, 5) + timedelta(days=index) for index in range(3))
    bars = pl.DataFrame(
        {
            "instrument_id": [
                "000002.SZ",
                "000001.SZ",
                "000002.SZ",
                "000001.SZ",
                "000002.SZ",
                "000001.SZ",
            ],
            "trade_date": [
                sessions[2],
                sessions[1],
                sessions[0],
                sessions[2],
                sessions[1],
                sessions[0],
            ],
            "open": [22.0, 10.0, 18.0, 11.0, 20.0, 9.0],
            "close": [24.0, 11.0, 19.0, 12.0, 21.0, 9.5],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0], sessions[0], sessions[0]],
            "instrument_id": ["000002.SZ", "000003.SZ", "000001.SZ"],
            "eligible": [True, False, True],
        }
    )

    result = build_future_returns(
        bars, sessions, eligible, (2,), executable_state(bars)
    )[(2, THEORETICAL_FORWARD_RETURN)]

    assert result.select("instrument_id", "return_start", "return_end").rows() == [
        ("000001.SZ", sessions[1], sessions[2]),
        ("000002.SZ", sessions[1], sessions[2]),
    ]
    assert result["future_return"].to_list() == pytest.approx([0.2, 0.2])


def test_analysis_produces_rank_quantiles_and_long_short_without_compounding() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "factor_id": ["momentum_120_20"] * 30,
            "value": [float(index) for index in range(30)],
            "is_valid": [True] * 30,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )
    returns = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 30,
            "return_end": [date(2026, 1, 6)] * 30,
            "future_return": [index / 100.0 for index in range(30)],
        }
    )

    result = run_analysis(factors, eligible, {1: returns})

    assert result["ic"].select("pearson_ic", "rank_ic").row(0) == pytest.approx(
        (1.0, 1.0)
    )
    groups = result["quantile_returns"].select("quantile", "count", "mean_return")
    assert groups["count"].to_list() == [6, 6, 6, 6, 6]
    assert result["long_short_returns"]["long_short_return"].item() == pytest.approx(
        0.24
    )


def test_analysis_invalidates_horizon_with_too_few_future_pairs() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "factor_id": ["roe"] * 30,
            "value": [float(index) for index in range(30)],
            "is_valid": [True] * 30,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )
    future = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 30,
            "return_end": [date(2026, 1, 6)] * 30,
            "future_return": [
                index / 100.0 if index < 29 else None for index in range(30)
            ],
        }
    )

    result = run_analysis(factors, eligible, {1: future})

    assert result["ic"].select(
        "pearson_ic", "rank_ic", "is_valid", "invalid_reason"
    ).row(0) == (
        None,
        None,
        False,
        "INSUFFICIENT_FORWARD_PAIRS",
    )
    assert result["quantile_returns"]["mean_return"].null_count() == 5
    assert result["long_short_returns"]["is_valid"].item() is False
    assert result["monotonicity"].select(
        "is_valid", "invalid_reason"
    ).row(0) == (False, "INSUFFICIENT_VALID_QUANTILES")


def test_analysis_emits_stable_one_five_twenty_day_ic_decay() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    values = [float(index) for index in range(30)]
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "factor_id": ["momentum_120_20"] * 30,
            "value": values,
            "is_valid": [True] * 30,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )

    def future(horizon: int, returns: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "signal_date": [day] * 30,
                "instrument_id": instruments,
                "return_start": [day + timedelta(days=1)] * 30,
                "return_end": [day + timedelta(days=horizon)] * 30,
                "future_return": returns,
            }
        )

    labels = {
        1: future(1, values),
        5: future(5, [value**2 for value in values]),
        20: future(20, [-value for value in values]),
    }
    first = run_analysis(factors, eligible, labels)
    second = run_analysis(
        factors,
        eligible,
        {20: labels[20], 1: labels[1], 5: labels[5]},
    )

    decay = first["summary"].select("horizon", "pearson_ic_mean", "rank_ic_mean")
    assert decay["horizon"].to_list() == [1, 5, 20]
    assert decay["pearson_ic_mean"].to_list() == pytest.approx(
        [1.0, 0.9662730800150235, -1.0]
    )
    assert decay["rank_ic_mean"].to_list() == pytest.approx([1.0, 1.0, -1.0])

    def output_hash(outputs: dict[str, pl.DataFrame]) -> str:
        digest = hashlib.sha256()
        for name in sorted(outputs):
            buffer = BytesIO()
            outputs[name].write_parquet(buffer, compression="zstd")
            digest.update(name.encode("utf-8"))
            digest.update(buffer.getvalue())
        return digest.hexdigest()

    assert output_hash(first) == output_hash(second)


def test_vectorized_analysis_is_deterministic_for_shuffled_full_matrix() -> None:
    """多因子、双信号和六标签输入乱序后仍生成相同可信表。"""
    days = [date(2026, 2, 2) + timedelta(days=index) for index in range(4)]
    instruments = [f"{index:06d}.SZ" for index in range(8)]
    factor_rows: list[dict[str, object]] = []
    eligible_rows: list[dict[str, object]] = []
    return_rows: list[dict[str, object]] = []
    for day_index, signal_date in enumerate(days):
        for instrument_index, instrument_id in enumerate(instruments):
            eligible_rows.append(
                {
                    "signal_date": signal_date,
                    "instrument_id": instrument_id,
                    "eligible": not (
                        day_index == 2 and instrument_id == instruments[-1]
                    ),
                }
            )
            return_rows.append(
                {
                    "signal_date": signal_date,
                    "instrument_id": instrument_id,
                    "return_start": signal_date + timedelta(days=1),
                    "return_end": signal_date + timedelta(days=20),
                    "future_return": (
                        None
                        if day_index == 1 and instrument_index == 0
                        else (instrument_index - day_index) / 100.0
                    ),
                }
            )
            for factor_ref in ("factor_a", "factor_b"):
                for variant in (DIRECTION_ADJUSTED, INDUSTRY_NEUTRALIZED):
                    factor_rows.append(
                        {
                            "signal_date": signal_date,
                            "instrument_id": instrument_id,
                            "factor_id": factor_ref,
                            "signal_variant": variant,
                            "value": float(
                                (
                                    instrument_index
                                    if factor_ref == "factor_a"
                                    else 7 - instrument_index
                                )
                                // 2
                            ),
                            "is_valid": not (
                                day_index == 3 and instrument_index >= 6
                            ),
                        }
                    )
    factors = pl.DataFrame(factor_rows)
    eligible = pl.DataFrame(eligible_rows)
    base_returns = pl.DataFrame(return_rows)
    labels = {
        (horizon, label_kind): base_returns.with_columns(
            pl.lit(horizon).alias("horizon"),
            pl.lit(label_kind).alias("label_kind"),
            pl.col("future_return").is_not_null().alias("is_valid"),
            pl.when(pl.col("future_return").is_null())
            .then(pl.lit("MISSING_EXIT_PRICE"))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("invalid_reason"),
        )
        for horizon in (1, 5, 20)
        for label_kind in (
            THEORETICAL_FORWARD_RETURN,
            EXECUTABLE_FORWARD_RETURN,
        )
    }

    first = analyze(
        factors,
        eligible,
        labels,
        quantiles=5,
        cost_bps_scenarios=(5, 10, 20),
        minimum=3,
    )
    second = analyze(
        factors.sample(fraction=1.0, shuffle=True, seed=11),
        eligible.sample(fraction=1.0, shuffle=True, seed=12),
        {
            key: frame.sample(fraction=1.0, shuffle=True, seed=13 + index)
            for index, (key, frame) in enumerate(reversed(tuple(labels.items())))
        },
        quantiles=5,
        cost_bps_scenarios=(5, 10, 20),
        minimum=3,
    )

    def output_hash(outputs: dict[str, pl.DataFrame]) -> str:
        digest = hashlib.sha256()
        for name in sorted(outputs):
            buffer = BytesIO()
            outputs[name].write_parquet(buffer, compression="zstd")
            digest.update(name.encode("utf-8"))
            digest.update(buffer.getvalue())
        return digest.hexdigest()

    assert output_hash(first) == output_hash(second)
    assert first["summary"].height == 24
    assert first["ic"].height == 96
    assert first["quantile_returns"].height == 480


def test_analysis_invalidates_factor_correlation_with_too_few_common_pairs() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(31)]
    rows = []
    for factor_ref in ("roe", "book_to_price_mrq"):
        for index, instrument_id in enumerate(instruments):
            rows.append(
                {
                    "signal_date": day,
                    "instrument_id": instrument_id,
                    "factor_id": factor_ref,
                    "value": float(index),
                    "is_valid": index != (30 if factor_ref == "roe" else 0),
                }
            )
    factors = pl.DataFrame(rows)
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 31,
            "instrument_id": instruments,
            "eligible": [True] * 31,
        }
    )
    future = pl.DataFrame(
        {
            "signal_date": [day] * 31,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 31,
            "return_end": [date(2026, 1, 6)] * 31,
            "future_return": [index / 100.0 for index in range(31)],
        }
    )

    result = run_analysis(factors, eligible, {1: future})
    cross = result["correlation"].filter(
        (pl.col("factor_x") == "roe") & (pl.col("factor_y") == "book_to_price_mrq")
    )

    assert cross.select("rank_correlation", "is_valid").row(0) == (None, False)


def test_analysis_correlation_retains_factors_without_any_valid_value() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factors = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "factor_id": factor_ref,
                "value": float(index),
                "is_valid": factor_ref == "valid_factor",
            }
            for factor_ref in ("invalid_factor", "valid_factor")
            for index, instrument_id in enumerate(instruments)
        ]
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )
    future = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 30,
            "return_end": [date(2026, 1, 6)] * 30,
            "future_return": [index / 100.0 for index in range(30)],
        }
    )

    correlation = run_analysis(factors, eligible, {1: future})["correlation"]

    assert correlation.select("factor_x", "factor_y").rows() == [
        ("invalid_factor", "invalid_factor"),
        ("invalid_factor", "valid_factor"),
        ("valid_factor", "invalid_factor"),
        ("valid_factor", "valid_factor"),
    ]
    invalid = correlation.filter(
        (pl.col("factor_x") == "invalid_factor")
        | (pl.col("factor_y") == "invalid_factor")
    )
    assert invalid.select(
        "date_count",
        "pair_count",
        "pearson_correlation",
        "rank_correlation",
        "is_valid",
    ).rows() == [(0, 0, None, None, False)] * 3


def test_hac_uses_horizon_overlap_lag_and_literal_mean() -> None:
    summary = HacMeanAnalyzer.summarize([0.01, 0.02, 0.03, 0.04], 3)

    assert summary.mean == pytest.approx(0.025)
    assert summary.lag == 2
    assert summary.standard_error == pytest.approx(0.005951190357119042)
    assert summary.t_stat == pytest.approx(4.20084025208403)
    assert summary.p_value == pytest.approx(2.6592618550786602e-05)
    assert summary.ci_lower == pytest.approx(0.013335881234904616)
    assert summary.ci_upper == pytest.approx(0.036664118765095385)
    assert summary.invalid_reason is None


def test_hac_preserves_invalid_signal_session_in_lag_pairs() -> None:
    summary = HacMeanAnalyzer.summarize([2.0, None, 0.0], 2)

    assert summary.mean == pytest.approx(1.0)
    assert summary.valid_count == 2
    assert summary.lag == 1
    assert summary.standard_error == pytest.approx(0.7071067811865476)
    assert summary.t_stat == pytest.approx(1.414213562373095)
    assert summary.p_value == pytest.approx(0.1572992070502852)


def test_hac_preserves_leading_trailing_and_multiple_session_gaps() -> None:
    """首尾和连续缺口不得把真实相隔两个会话的样本压缩为相邻样本。"""
    summary = HacMeanAnalyzer.summarize([None, 2.0, None, 0.0, None], 3)

    assert summary.mean == pytest.approx(1.0)
    assert summary.valid_count == 2
    assert summary.lag == 2
    assert summary.standard_error == pytest.approx(0.5773502691896257)


def test_hac_zero_one_observation_and_horizon_one_boundaries() -> None:
    """零/单样本原因码和 horizon=1 的零滞后语义必须稳定。"""
    empty = HacMeanAnalyzer.summarize([None, None], 2)
    single = HacMeanAnalyzer.summarize([None, 2.0, None], 5)
    no_lag = HacMeanAnalyzer.summarize([2.0, None, 0.0], 1)

    assert (empty.valid_count, empty.lag, empty.invalid_reason) == (
        0,
        1,
        "NO_VALID_OBSERVATIONS",
    )
    assert (single.mean, single.valid_count, single.lag, single.invalid_reason) == (
        2.0,
        1,
        2,
        "INSUFFICIENT_OBSERVATIONS",
    )
    assert no_lag.lag == 0
    assert no_lag.standard_error == pytest.approx(0.7071067811865476)


def test_analysis_hac_left_aligns_invalid_ic_to_complete_signal_dates() -> None:
    days = [date(2026, 1, 5) + timedelta(days=index) for index in range(3)]
    instruments = ["000001.SZ", "000002.SZ", "000003.SZ"]
    factors = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "factor_id": "gap_factor",
                "value": float(index),
                "is_valid": True,
            }
            for day in days
            for index, instrument_id in enumerate(instruments)
        ]
    )
    eligible = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "eligible": True,
            }
            for day in days
            for instrument_id in instruments
        ]
    )
    future = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "return_start": day + timedelta(days=1),
                "return_end": day + timedelta(days=2),
                "future_return": (
                    None
                    if day_index == 1 and index == 2
                    else float(index if day_index < 2 else 2 - index)
                ),
            }
            for day_index, day in enumerate(days)
            for index, instrument_id in enumerate(instruments)
        ]
    )

    summary = analyze(
        factors,
        eligible,
        study_labels({2: future}),
        quantiles=2,
        cost_bps_scenarios=(5,),
        minimum=3,
    )["summary"].row(0, named=True)

    assert summary["rank_ic_hac_mean"] == pytest.approx(0.0)
    assert summary["rank_ic_hac_valid_count"] == 2
    assert summary["rank_ic_hac_hac_lag"] == 1
    assert summary["rank_ic_hac_hac_standard_error"] == pytest.approx(
        0.7071067811865476
    )


def test_cost_hac_left_aligns_invalid_days_and_turnover_boundary() -> None:
    days = [date(2026, 1, 5) + timedelta(days=index) for index in range(4)]
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factors = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "factor_id": "cost_gap_factor",
                "value": float(index),
                "is_valid": True,
            }
            for day in days
            for index, instrument_id in enumerate(instruments)
        ]
    )
    eligible = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "eligible": True,
            }
            for day in days
            for instrument_id in instruments
        ]
    )
    future = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "return_start": day + timedelta(days=1),
                "return_end": day + timedelta(days=2),
                "future_return": (
                    None
                    if day_index == 2 and index == 29
                    else (
                        float(min(index // 6, 4 - index // 6))
                        if day_index == 3
                        else float(index) / 12.0
                    )
                ),
            }
            for day_index, day in enumerate(days)
            for index, instrument_id in enumerate(instruments)
        ]
    )

    costs = analyze(
        factors,
        eligible,
        study_labels({2: future}),
        quantiles=5,
        cost_bps_scenarios=(5,),
        minimum=30,
    )["cost_scenarios"].row(0, named=True)

    assert costs["aligned_date_count"] == 2
    assert costs["net_spread_mean"] == pytest.approx(1.0)
    assert costs["net_spread_valid_count"] == 2
    assert costs["net_spread_hac_lag"] == 1
    assert costs["net_spread_hac_standard_error"] == pytest.approx(
        0.7071067811865476
    )


def test_monotonicity_turnover_and_cost_use_literal_oracles() -> None:
    days = [date(2026, 1, 5) + timedelta(days=index) for index in range(3)]
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factor_rows: list[dict[str, object]] = []
    return_rows: list[dict[str, object]] = []
    eligible_rows: list[dict[str, object]] = []
    for day_index, day in enumerate(days):
        values = list(range(30)) if day_index == 0 else list(reversed(range(30)))
        for instrument_index, instrument_id in enumerate(instruments):
            value = float(values[instrument_index])
            factor_rows.append(
                {
                    "signal_date": day,
                    "instrument_id": instrument_id,
                    "factor_id": "literal_factor",
                    "value": value,
                    "is_valid": True,
                }
            )
            return_rows.append(
                {
                    "signal_date": day,
                    "instrument_id": instrument_id,
                    "return_start": day + timedelta(days=1),
                    "return_end": day + timedelta(days=1),
                    "future_return": value / 100.0,
                }
            )
            eligible_rows.append(
                {
                    "signal_date": day,
                    "instrument_id": instrument_id,
                    "eligible": True,
                }
            )

    result = analyze(
        pl.DataFrame(factor_rows),
        pl.DataFrame(eligible_rows),
        study_labels({1: pl.DataFrame(return_rows)}),
        quantiles=5,
        cost_bps_scenarios=(5, 10, 20),
    )

    monotonicity = result["monotonicity"]
    assert monotonicity["quantile_rank_correlation"].to_list() == pytest.approx(
        [1.0, 1.0, 1.0]
    )
    assert monotonicity["adjacent_inversion_count"].to_list() == [0, 0, 0]
    assert monotonicity["terminal_spread"].to_list() == pytest.approx([0.24] * 3)
    turnover = result["turnover"]
    assert turnover["turnover_is_valid"].to_list() == [False, True, True]
    assert turnover["rank_autocorrelation"].to_list()[1:] == pytest.approx(
        [-1.0, 1.0]
    )
    assert turnover["total_turnover"].to_list()[1:] == pytest.approx([2.0, 0.0])
    costs = result["cost_scenarios"]
    assert costs["break_even_cost_bps"].to_list() == pytest.approx([2400.0] * 3)
    assert costs.filter(pl.col("cost_bps") == 5)["net_spread_mean"].item() == pytest.approx(
        0.2395
    )
    assert "stability" not in result
