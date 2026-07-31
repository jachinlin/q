"""Pure cross-sectional factor diagnostics with strict future alignment."""

from datetime import date, timedelta

import polars as pl
import pytest

from quant_core.factors.analysis import (
    assign_quantiles,
    coverage_by_date,
    factor_correlation_matrix,
    long_short_returns,
    quantile_future_returns,
    spearman_rank_ic,
)


def test_coverage_uses_explicit_eligible_universe_denominator() -> None:
    factors = _factors([(0, "A", 1.0, True), (0, "B", None, False)])
    universe = pl.DataFrame(
        {
            "signal_date": [_day(0)] * 3,
            "instrument_id": ["A", "B", "C"],
            "eligible": [True, True, True],
        }
    )
    result = coverage_by_date(factors, universe)
    assert result.rows() == [(_day(0), 3, 1, pytest.approx(1 / 3))]


def test_rank_ic_is_daily_spearman_and_rejects_nonfuture_windows() -> None:
    factors = _factors(
        [
            (0, "A", 1.0, True),
            (0, "B", 2.0, True),
            (1, "A", 100.0, True),
            (1, "B", 0.0, True),
        ]
    )
    future = _future([(0, "A", 0.4), (0, "B", 0.1), (1, "A", 0.2), (1, "B", 0.3)])
    result = spearman_rank_ic(factors, future)
    assert result["rank_ic"].to_list() == pytest.approx([-1.0, -1.0])
    assert result["pair_count"].to_list() == [2, 2]
    bad = future.with_columns(pl.col("signal_date").alias("return_start"))
    with pytest.raises(ValueError, match="strictly after"):
        spearman_rank_ic(factors, bad)


def test_constant_or_single_pair_rank_ic_is_invalid_not_zero() -> None:
    factors = _factors([(0, "A", 1.0, True), (0, "B", 1.0, True), (1, "A", 2.0, True)])
    result = spearman_rank_ic(
        factors, _future([(0, "A", 0.1), (0, "B", 0.2), (1, "A", 0.3)])
    )
    assert result["rank_ic"].to_list() == [None, None]
    assert result["is_valid"].to_list() == [False, False]


def test_quantiles_stably_break_ties_and_allow_empty_groups() -> None:
    factors = _factors([(0, "C", 1.0, True), (0, "A", 1.0, True), (0, "B", 1.0, True)])
    assigned = assign_quantiles(factors, 5)
    assert assigned.select("instrument_id", "quantile").rows() == [
        ("A", 1),
        ("B", 2),
        ("C", 4),
    ]
    with pytest.raises(ValueError, match="at least 2"):
        assign_quantiles(factors, 1)


def test_quantile_and_long_short_returns_align_exact_signal_keys() -> None:
    factors = _factors(
        [
            (0, "A", 1.0, True),
            (0, "B", 2.0, True),
            (0, "C", 3.0, True),
            (0, "D", 4.0, True),
        ]
    )
    future = _future([(0, "A", 0.01), (0, "B", 0.02), (0, "C", 0.03), (0, "D", 0.04)])
    groups = quantile_future_returns(factors, future, 2)
    assert groups.select("quantile", "count", "mean_return").rows() == [
        (1, 2, pytest.approx(0.015)),
        (2, 2, pytest.approx(0.035)),
    ]
    assert long_short_returns(groups)["long_short_return"].item() == pytest.approx(0.02)


def test_factor_correlations_pair_same_security_within_each_date() -> None:
    first = _factors(
        [
            (0, "A", 1.0, True),
            (0, "B", 2.0, True),
            (1, "A", 10.0, True),
            (1, "B", 20.0, True),
        ]
    ).with_columns(pl.lit("f1").alias("factor_id"))
    second = _factors(
        [
            (0, "A", 2.0, True),
            (0, "B", 4.0, True),
            (1, "A", 30.0, True),
            (1, "B", 15.0, True),
        ]
    ).with_columns(pl.lit("f2").alias("factor_id"))
    result = factor_correlation_matrix(pl.concat([first, second]))
    assert result.select("factor_x", "factor_y").rows() == [
        ("f1", "f1"),
        ("f1", "f2"),
        ("f2", "f1"),
        ("f2", "f2"),
    ]
    cross = result.filter((pl.col("factor_x") == "f1") & (pl.col("factor_y") == "f2"))
    assert cross["correlation"].item() == pytest.approx(0.0)
    assert cross["pair_count"].item() == 4


def _day(offset: int) -> date:
    return date(2024, 1, 2) + timedelta(days=offset)


def _factors(rows: list[tuple[int, str, float | None, bool]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "signal_date": [_day(r[0]) for r in rows],
            "instrument_id": [r[1] for r in rows],
            "value": [r[2] for r in rows],
            "is_valid": [r[3] for r in rows],
        }
    )


def _future(rows: list[tuple[int, str, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "signal_date": [_day(r[0]) for r in rows],
            "instrument_id": [r[1] for r in rows],
            "return_start": [_day(r[0]) + timedelta(days=1) for r in rows],
            "return_end": [_day(r[0]) + timedelta(days=2) for r in rows],
            "future_return": [r[2] for r in rows],
        }
    )
