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


def test_null_future_window_boundaries_never_participate_in_diagnostics() -> None:
    factors = _factors([(0, "A", 1.0, True), (0, "B", 2.0, True)])
    future = pl.DataFrame(
        {
            "signal_date": [_day(0), _day(0)],
            "instrument_id": ["A", "B"],
            "return_start": [_day(1), None],
            "return_end": [_day(2), _day(2)],
            "future_return": [0.1, 0.9],
        },
        schema={
            "signal_date": pl.Date,
            "instrument_id": pl.String,
            "return_start": pl.Date,
            "return_end": pl.Date,
            "future_return": pl.Float64,
        },
    )

    rank_ic = spearman_rank_ic(factors, future)
    quantiles = quantile_future_returns(factors, future, 2)

    assert rank_ic.select("pair_count", "rank_ic", "is_valid").row(0) == (
        1,
        None,
        False,
    )
    assert quantiles["count"].sum() == 1


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
    assert assigned.select(
        "instrument_id", "quantile", "bucket_count", "is_empty"
    ).rows() == [
        ("A", 1, 1, False),
        ("B", 2, 1, False),
        (None, 3, 0, True),
        ("C", 4, 1, False),
        (None, 5, 0, True),
    ]
    with pytest.raises(ValueError, match="at least 2"):
        assign_quantiles(factors, 1)


def test_small_cross_section_retains_quantile_domain_and_invalidates_long_short() -> (
    None
):
    """Deleting empty-domain rows or terminal validation would hide n < Q failure."""
    factors = _factors([(0, "A", 1.0, True), (0, "B", 2.0, True), (0, "C", 3.0, True)])
    future = _future([(0, "A", 0.01), (0, "B", 0.02), (0, "C", 0.03)])

    assignments = assign_quantiles(factors, 5)
    quantiles = quantile_future_returns(factors, future, 5)
    long_short = long_short_returns(quantiles)

    assert assignments["quantile"].to_list() == [1, 2, 3, 4, 5]
    assert assignments.filter(pl.col("is_empty")).select(
        "instrument_id", "value", "quantile", "bucket_count"
    ).rows() == [(None, None, 3, 0), (None, None, 5, 0)]
    assert quantiles.select("quantile", "count", "mean_return", "is_empty").rows() == [
        (1, 1, pytest.approx(0.01), False),
        (2, 1, pytest.approx(0.02), False),
        (3, 0, None, True),
        (4, 1, pytest.approx(0.03), False),
        (5, 0, None, True),
    ]
    assert long_short.row(0) == (
        _day(0),
        None,
        False,
        "MISSING_TERMINAL_QUANTILE_OBSERVATIONS",
    )


def test_all_invalid_cross_section_still_exposes_every_empty_quantile_bucket() -> None:
    """Iterating only valid rows would erase the n=0<Q diagnostic domain."""
    factors = _factors([(0, "A", None, False)])

    assignments = assign_quantiles(factors, 3)

    assert assignments.select(
        "quantile", "instrument_id", "bucket_count", "is_empty"
    ).rows() == [
        (1, None, 0, True),
        (2, None, 0, True),
        (3, None, 0, True),
    ]


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


@pytest.mark.parametrize(
    "operation", ["coverage", "rank_ic", "assign", "quantile_returns"]
)
def test_single_factor_diagnostics_reject_duplicate_signal_keys(operation: str) -> None:
    factors = _factors([(0, "A", 1.0, True), (0, "A", 2.0, True)])
    future = _future([(0, "A", 0.1)])
    universe = pl.DataFrame(
        {"signal_date": [_day(0)], "instrument_id": ["A"], "eligible": [True]}
    )

    with pytest.raises(ValueError, match="duplicate factors key"):
        if operation == "coverage":
            coverage_by_date(factors, universe)
        elif operation == "rank_ic":
            spearman_rank_ic(factors, future)
        elif operation == "assign":
            assign_quantiles(factors, 2)
        else:
            quantile_future_returns(factors, future, 2)


def test_correlation_rejects_duplicate_factor_keys() -> None:
    factors = _factors([(0, "A", 1.0, True), (0, "A", 2.0, False)]).with_columns(
        pl.lit("f1").alias("factor_id")
    )

    with pytest.raises(ValueError, match="duplicate factor correlation key"):
        factor_correlation_matrix(factors)


@pytest.mark.parametrize("operation", ["rank_ic", "quantile_returns"])
def test_future_return_duplicates_are_rejected_before_null_boundary_filtering(
    operation: str,
) -> None:
    factors = _factors([(0, "A", 1.0, True)])
    future = pl.DataFrame(
        {
            "signal_date": [_day(0), _day(0)],
            "instrument_id": ["A", "A"],
            "return_start": [_day(1), None],
            "return_end": [_day(2), _day(2)],
            "future_return": [0.1, 0.2],
        },
        schema={
            "signal_date": pl.Date,
            "instrument_id": pl.String,
            "return_start": pl.Date,
            "return_end": pl.Date,
            "future_return": pl.Float64,
        },
    )

    with pytest.raises(ValueError, match="duplicate future returns key"):
        if operation == "rank_ic":
            spearman_rank_ic(factors, future)
        else:
            quantile_future_returns(factors, future, 2)


def test_correlation_diagonal_is_invalid_for_only_single_or_constant_daily_sections() -> (
    None
):
    factors = _factors(
        [(0, "A", 1.0, True), (1, "A", 2.0, True), (1, "B", 2.0, True)]
    ).with_columns(pl.lit("f1").alias("factor_id"))

    result = factor_correlation_matrix(factors)

    assert result.select("pair_count", "correlation", "is_valid").row(0) == (
        3,
        None,
        False,
    )


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
