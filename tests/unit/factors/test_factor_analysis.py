"""Pure cross-sectional factor diagnostics with strict future alignment."""

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from quant_research.factors.analysis import (
    InformationCoefficientAnalyzer,
    assign_quantiles,
    coverage_by_date,
    factor_correlation_matrix,
    factor_rank_correlation_matrix,
    long_short_returns,
    quantile_future_returns,
    spearman_rank_ic,
)


def test_information_coefficients_share_alignment_but_keep_distinct_semantics() -> None:
    factors = _factors(
        [
            (0, "A", 1.0, True),
            (0, "B", 2.0, True),
            (0, "C", 3.0, True),
            (0, "D", 100.0, True),
        ]
    )
    future = _future([(0, "A", 1.0), (0, "B", 2.0), (0, "C", 4.0), (0, "D", 5.0)])

    result = InformationCoefficientAnalyzer(
        rolling_window=20, rolling_min_valid=10
    ).daily(factors, future, minimum_cross_section=4)

    assert result.select("pearson_ic", "rank_ic").row(0) == pytest.approx(
        (0.7413718364364857, 1.0)
    )


def test_rank_ic_uses_average_ranks_for_ties() -> None:
    factors = _factors(
        [
            (0, "A", 1.0, True),
            (0, "B", 1.0, True),
            (0, "C", 2.0, True),
            (0, "D", 3.0, True),
        ]
    )
    future = _future([(0, "A", 1.0), (0, "B", 2.0), (0, "C", 3.0), (0, "D", 4.0)])

    result = InformationCoefficientAnalyzer(
        rolling_window=20, rolling_min_valid=10
    ).daily(factors, future, minimum_cross_section=4)

    assert result["rank_ic"].item() == pytest.approx(0.9486832980505138)


def test_ic_summary_uses_sample_std_linear_quantiles_and_stable_streaks() -> None:
    days = [_day(index) for index in range(7)]
    daily = pl.DataFrame(
        {
            "signal_date": days,
            "rank_ic": [-1.0, -0.5, 0.0, 0.5, 1.0, None, -0.25],
            "is_valid": [True, True, True, True, True, False, True],
        },
        schema={
            "signal_date": pl.Date,
            "rank_ic": pl.Float64,
            "is_valid": pl.Boolean,
        },
    )

    summary = InformationCoefficientAnalyzer(
        rolling_window=20, rolling_min_valid=10
    ).summarize(daily, "rank_ic")

    assert summary.mean == pytest.approx(-1.0 / 24.0)
    assert summary.sample_std == pytest.approx(0.7144345083117604)
    assert summary.p05 == pytest.approx(-0.875)
    assert summary.p25 == pytest.approx(-0.4375)
    assert summary.p50 == pytest.approx(-0.125)
    assert summary.p75 == pytest.approx(0.375)
    assert summary.p95 == pytest.approx(0.875)
    assert summary.positive_rate == pytest.approx(2 / 6)
    assert (
        summary.max_positive_streak,
        summary.positive_streak_start,
        summary.positive_streak_end,
    ) == (2, days[3], days[4])
    assert (
        summary.max_negative_streak,
        summary.negative_streak_start,
        summary.negative_streak_end,
    ) == (2, days[0], days[1])


def test_ic_rolling_window_counts_signal_days_and_cumulative_carries_invalid_days() -> (
    None
):
    factor_rows: list[tuple[int, str, float | None, bool]] = []
    return_rows: list[tuple[int, str, float]] = []
    future_rows: list[dict[str, object]] = []
    for offset in range(21):
        factor_rows.extend(((offset, "A", 1.0, True), (offset, "B", 2.0, True)))
        if 10 <= offset < 20:
            return_rows.extend(((offset, "A", 0.1), (offset, "B", 0.2)))
        else:
            for instrument in ("A", "B"):
                future_rows.append(
                    {
                        "signal_date": _day(offset),
                        "instrument_id": instrument,
                        "return_start": _day(offset) + timedelta(days=1),
                        "return_end": _day(offset) + timedelta(days=2),
                        "future_return": None,
                    }
                )
    future = pl.concat(
        [
            _future(return_rows),
            pl.DataFrame(
                future_rows,
                schema={
                    "signal_date": pl.Date,
                    "instrument_id": pl.String,
                    "return_start": pl.Date,
                    "return_end": pl.Date,
                    "future_return": pl.Float64,
                },
            ),
        ]
    ).sort("signal_date", "instrument_id")

    result = InformationCoefficientAnalyzer(
        rolling_window=20, rolling_min_valid=10
    ).daily(_factors(factor_rows), future, minimum_cross_section=2)

    assert result["pearson_ic_cumulative_sum"].item(0) is None
    assert result.select(
        "rolling_valid_count",
        "pearson_ic_rolling_mean",
        "rank_ic_rolling_mean",
        "pearson_ic_cumulative_sum",
    ).row(19) == pytest.approx((10, 1.0, 1.0, 10.0))
    assert result.select(
        "rolling_valid_count",
        "pearson_ic_rolling_mean",
        "pearson_ic_cumulative_sum",
        "is_valid",
    ).row(20) == pytest.approx((10, 1.0, 10.0, False))


@pytest.mark.parametrize(
    ("factors", "future", "minimum", "reason"),
    [
        (
            [(0, "A", 1.0, True)],
            [(0, "A", 0.1)],
            2,
            "INSUFFICIENT_CROSS_SECTION",
        ),
        (
            [(0, "A", 1.0, True), (0, "B", 2.0, True)],
            [(0, "A", 0.1)],
            2,
            "INSUFFICIENT_FORWARD_PAIRS",
        ),
        (
            [(0, "A", 1.0, True), (0, "B", 1.0, True)],
            [(0, "A", 0.1), (0, "B", 0.2)],
            2,
            "ZERO_FACTOR_VARIANCE",
        ),
        (
            [(0, "A", 1.0, True), (0, "B", 2.0, True)],
            [(0, "A", 0.1), (0, "B", 0.1)],
            2,
            "ZERO_RETURN_VARIANCE",
        ),
    ],
)
def test_ic_invalid_reason_precedence(
    factors: list[tuple[int, str, float | None, bool]],
    future: list[tuple[int, str, float]],
    minimum: int,
    reason: str,
) -> None:
    result = InformationCoefficientAnalyzer(
        rolling_window=20, rolling_min_valid=10
    ).daily(_factors(factors), _future(future), minimum_cross_section=minimum)

    assert result.select("is_valid", "invalid_reason").row(0) == (False, reason)


def test_ic_excludes_nan_and_infinity_before_reason_precedence() -> None:
    analyzer = InformationCoefficientAnalyzer(rolling_window=20, rolling_min_valid=10)
    nonfinite_factor = analyzer.daily(
        _factors(
            [
                (0, "A", 1.0, True),
                (0, "B", 2.0, True),
                (0, "C", float("inf"), True),
                (0, "D", float("nan"), True),
            ]
        ),
        _future([(0, "A", 0.1), (0, "B", 0.2), (0, "C", 0.3), (0, "D", 0.4)]),
        minimum_cross_section=3,
    )
    nonfinite_return = analyzer.daily(
        _factors([(0, "A", 1.0, True), (0, "B", 2.0, True), (0, "C", 3.0, True)]),
        _future([(0, "A", 0.1), (0, "B", 0.2), (0, "C", float("inf"))]),
        minimum_cross_section=3,
    )

    assert nonfinite_factor.select("factor_valid_count", "invalid_reason").row(0) == (
        2,
        "INSUFFICIENT_CROSS_SECTION",
    )
    assert nonfinite_return.select("sample_count", "invalid_reason").row(0) == (
        2,
        "INSUFFICIENT_FORWARD_PAIRS",
    )


def test_ic_reports_nonfinite_correlation_after_finite_alignment() -> None:
    analyzer = InformationCoefficientAnalyzer(rolling_window=20, rolling_min_valid=10)
    with np.errstate(over="ignore", invalid="ignore"):
        result = analyzer.daily(
            _factors(
                [
                    (0, "A", -1e308, True),
                    (0, "B", 0.0, True),
                    (0, "C", 1e308, True),
                ]
            ),
            _future([(0, "A", -1e308), (0, "B", 0.0), (0, "C", 1e308)]),
            minimum_cross_section=3,
        )

    assert result.select("pearson_ic", "rank_ic", "invalid_reason").row(0) == (
        None,
        None,
        "NONFINITE_IC",
    )


def test_ic_streak_summary_has_empty_boundaries_when_sign_is_absent() -> None:
    analyzer = InformationCoefficientAnalyzer(rolling_window=20, rolling_min_valid=10)
    no_positive = pl.DataFrame(
        {
            "signal_date": [_day(0), _day(1)],
            "rank_ic": [-0.2, 0.0],
            "is_valid": [True, True],
        }
    )
    no_negative = pl.DataFrame(
        {
            "signal_date": [_day(0), _day(1)],
            "rank_ic": [0.0, 0.2],
            "is_valid": [True, True],
        }
    )

    negative_summary = analyzer.summarize(no_positive, "rank_ic")
    positive_summary = analyzer.summarize(no_negative, "rank_ic")

    assert (
        negative_summary.max_positive_streak,
        negative_summary.positive_streak_start,
        negative_summary.positive_streak_end,
    ) == (0, None, None)
    assert (
        positive_summary.max_negative_streak,
        positive_summary.negative_streak_start,
        positive_summary.negative_streak_end,
    ) == (0, None, None)


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


def test_vectorized_daily_ic_and_quantiles_match_naive_reference() -> None:
    """批量分区实现与逐日逐分位参考算法保持相同统计语义。"""
    days = [_day(offset) for offset in range(3)]
    instruments = tuple("ABCDEFGH")
    factor_rows: list[dict[str, object]] = []
    return_rows: list[dict[str, object]] = []
    for day_index, signal_date in enumerate(days):
        for instrument_index, instrument_id in enumerate(instruments):
            value: float | None = float((instrument_index + day_index) // 2)
            is_valid = True
            if instrument_id == "G":
                value = float("inf")
            elif instrument_id == "H":
                value = None
                is_valid = False
            factor_rows.append(
                {
                    "signal_date": signal_date,
                    "instrument_id": instrument_id,
                    "value": value,
                    "is_valid": is_valid,
                }
            )
            future_return: float | None = (
                float(instrument_index - day_index) / 100.0
            )
            return_start = signal_date + timedelta(days=1)
            if day_index == 1 and instrument_id == "F":
                future_return = None
            elif instrument_id == "G":
                future_return = float("inf")
            elif instrument_id == "H":
                return_start = None
            return_rows.append(
                {
                    "signal_date": signal_date,
                    "instrument_id": instrument_id,
                    "return_start": return_start,
                    "return_end": signal_date + timedelta(days=2),
                    "future_return": future_return,
                }
            )
    factors = pl.DataFrame(factor_rows)
    future = pl.DataFrame(
        return_rows,
        schema={
            "signal_date": pl.Date,
            "instrument_id": pl.String,
            "return_start": pl.Date,
            "return_end": pl.Date,
            "future_return": pl.Float64,
        },
    )

    daily = InformationCoefficientAnalyzer(
        rolling_window=2, rolling_min_valid=2
    ).daily(factors, future, minimum_cross_section=4)
    valid_factors = factors.filter(
        pl.col("is_valid")
        & pl.col("value").is_not_null()
        & pl.col("value").is_finite()
    )
    valid_returns = future.filter(
        pl.col("return_start").is_not_null()
        & pl.col("return_end").is_not_null()
        & pl.col("future_return").is_not_null()
        & pl.col("future_return").is_finite()
    )
    expected_ic: list[tuple[int, int, float, float]] = []
    for signal_date in days:
        factor_group = valid_factors.filter(
            pl.col("signal_date") == signal_date
        )
        paired = factor_group.join(
            valid_returns.filter(pl.col("signal_date") == signal_date),
            on=["signal_date", "instrument_id"],
            how="inner",
        )
        left = paired["value"].to_numpy()
        right = paired["future_return"].to_numpy()

        def average_ranks(values: np.ndarray) -> np.ndarray:
            order = np.argsort(values, kind="stable")
            ranks = np.empty(len(values), dtype=float)
            index = 0
            while index < len(values):
                stop = index + 1
                while (
                    stop < len(values)
                    and values[order[stop]] == values[order[index]]
                ):
                    stop += 1
                ranks[order[index:stop]] = (index + stop - 1) / 2.0
                index = stop
            return ranks

        expected_ic.append(
            (
                factor_group.height,
                paired.height,
                float(np.corrcoef(left, right)[0, 1]),
                float(
                    np.corrcoef(
                        average_ranks(left), average_ranks(right)
                    )[0, 1]
                ),
            )
        )
    assert daily.select("factor_valid_count", "sample_count").rows() == [
        row[:2] for row in expected_ic
    ]
    assert daily["pearson_ic"].to_list() == pytest.approx(
        [row[2] for row in expected_ic], rel=1e-12, abs=1e-12
    )
    assert daily["rank_ic"].to_list() == pytest.approx(
        [row[3] for row in expected_ic], rel=1e-12, abs=1e-12
    )

    assigned = assign_quantiles(factors, 5)
    quantiles = quantile_future_returns(factors, future, 5)
    joined = assigned.join(
        valid_returns.select(
            "signal_date", "instrument_id", "future_return"
        ),
        on=["signal_date", "instrument_id"],
        how="left",
    )
    expected_quantiles: list[tuple[object, ...]] = []
    for signal_date in days:
        day_rows = joined.filter(pl.col("signal_date") == signal_date)
        for quantile in range(1, 6):
            bucket = day_rows.filter(pl.col("quantile") == quantile)
            values = bucket["future_return"].drop_nulls().to_list()
            factor_values = bucket["value"].drop_nulls().to_list()
            expected_quantiles.append(
                (
                    signal_date,
                    quantile,
                    len(values),
                    sum(values) / len(values) if values else None,
                    min(factor_values) if factor_values else None,
                    max(factor_values) if factor_values else None,
                    not values,
                )
            )
    actual_quantiles = quantiles.select(
        "signal_date",
        "quantile",
        "count",
        "mean_return",
        "factor_lower_bound",
        "factor_upper_bound",
        "is_empty",
    ).rows()
    for actual, expected in zip(
        actual_quantiles, expected_quantiles, strict=True
    ):
        assert actual[:3] == expected[:3]
        assert actual[3:6] == pytest.approx(
            expected[3:6], rel=1e-12, abs=1e-12
        )
        assert actual[6] is expected[6]


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


def test_factor_correlations_keep_all_invalid_factor_and_empty_rank_schema() -> None:
    """相关矩阵域必须来自全部输入因子，空输入也须保持固定 Schema。"""
    invalid = _factors([(0, "A", 1.0, False)]).with_columns(
        pl.lit("invalid_factor").alias("factor_id")
    )

    plain = factor_correlation_matrix(invalid)
    ranked = factor_rank_correlation_matrix(invalid)
    empty = factor_rank_correlation_matrix(invalid.head(0))

    assert plain.row(0) == (
        "invalid_factor",
        "invalid_factor",
        0,
        None,
        False,
    )
    assert ranked.row(0) == (
        "invalid_factor",
        "invalid_factor",
        0,
        0,
        None,
        None,
        False,
    )
    assert empty.is_empty()
    assert empty.schema == ranked.schema


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
