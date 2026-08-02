"""Hand-checked behavioral tests for backtest performance analytics."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import polars as pl
import pytest

from quant_core.analytics.performance import calculate_performance

NAV_SCHEMA = {
    "trade_date": pl.Date,
    "cash_fen": pl.Int64,
    "market_value_fen": pl.Int64,
    "nav_fen": pl.Int64,
    "benchmark_close": pl.Float64,
}
FILLS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int32,
    "instrument_id": pl.String,
    "side": pl.String,
    "requested_quantity": pl.Int64,
    "filled_quantity": pl.Int64,
    "unfilled_quantity": pl.Int64,
    "price": pl.Float64,
    "gross_value_fen": pl.Int64,
    "reason_code": pl.String,
    "detail": pl.String,
}
COSTS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int32,
    "instrument_id": pl.String,
    "commission_fen": pl.Int64,
    "stamp_tax_fen": pl.Int64,
    "transfer_fee_fen": pl.Int64,
    "total_fees_fen": pl.Int64,
}


def _nav() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [
                date(2024, 1, 30),
                date(2024, 1, 31),
                date(2024, 2, 1),
                date(2024, 2, 2),
            ],
            "cash_fen": [10_000, 11_000, 9_900, 11_000],
            "market_value_fen": [0, 0, 0, 0],
            "nav_fen": [10_000, 11_000, 9_900, 11_000],
            "benchmark_close": [100.0, 105.0, 100.0, 102.0],
        },
        schema=NAV_SCHEMA,
    )


def _fills() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 31), date(2024, 2, 1)],
            "result_index": [0, 0],
            "instrument_id": ["SSE:600001", "SSE:600002"],
            "side": ["BUY", "BUY"],
            "requested_quantity": [1, 5],
            "filled_quantity": [1, 0],
            "unfilled_quantity": [0, 5],
            "price": [10.0, None],
            "gross_value_fen": [1_000, 0],
            "reason_code": ["FILLED", "NO_PRICE"],
            "detail": [None, "halted"],
        },
        schema=FILLS_SCHEMA,
    )


def _costs() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 31)],
            "result_index": [0],
            "instrument_id": ["SSE:600001"],
            "commission_fen": [10],
            "stamp_tax_fen": [0],
            "transfer_fee_fen": [0],
            "total_fees_fen": [10],
        },
        schema=COSTS_SCHEMA,
    )


def _empty_fills() -> pl.DataFrame:
    return pl.DataFrame(schema=FILLS_SCHEMA)


def _empty_costs() -> pl.DataFrame:
    return pl.DataFrame(schema=COSTS_SCHEMA)


def _period_nav(
    dates: list[date], nav_values: list[int], benchmark_values: list[float]
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": dates,
            "cash_fen": nav_values,
            "market_value_fen": [0] * len(dates),
            "nav_fen": nav_values,
            "benchmark_close": benchmark_values,
        },
        schema=NAV_SCHEMA,
    )


def test_calculate_performance_matches_hand_checked_metrics_and_periods() -> None:
    """A wrong metric formula, sample convention, or period boundary breaks this."""
    result = calculate_performance(_nav(), _fills(), _costs(), sessions_per_year=3)

    assert result.metrics_version == "1.0.0"
    expected_metrics = {
        "observations": 4,
        "cumulative_return": 0.1,
        "annualized_return": 0.1,
        "annualized_volatility": 0.20578065752724595,
        "sharpe_ratio": 0.5399492471560391,
        "sortino_ratio": 1.1111111111111116,
        "max_drawdown": -0.1,
        "calmar_ratio": 1.0,
        "one_way_turnover": 0.0477326968973747,
        "fee_rate": 0.0009546539379474941,
        "failed_fill_rate": 0.5,
        "benchmark_cumulative_return": 0.02,
        "relative_cumulative_return": 0.08,
        "information_ratio": 0.6932683130554919,
    }
    for name, expected in expected_metrics.items():
        assert result.metrics[name] == pytest.approx(expected)
    assert result.metrics["start_date"] == "2024-01-30"
    assert result.metrics["end_date"] == "2024-02-02"
    assert result.metrics["max_drawdown_peak_date"] == "2024-01-31"
    assert result.metrics["max_drawdown_trough_date"] == "2024-02-01"
    assert result.metrics["max_drawdown_recovery_date"] == "2024-02-02"
    assert result.nav["portfolio_daily_return"].to_list() == pytest.approx(
        [0.0, 0.1, -0.1, 0.11111111111111116]
    )
    expected_drawdown = {
        "nav": [1.0, 1.1, 0.99, 1.1],
        "benchmark_nav": [1.0, 1.05, 1.0, 1.02],
        "portfolio_daily_return": [0.0, 0.1, -0.1, 0.11111111111111116],
        "benchmark_daily_return": [0.0, 0.05, -0.04761904761904767, 0.02],
        "running_peak_nav": [1.0, 1.1, 1.1, 1.1],
        "drawdown": [0.0, 0.0, -0.1, 0.0],
    }
    for name, expected in expected_drawdown.items():
        assert result.drawdown[name].to_list() == pytest.approx(expected)
    assert result.drawdown["trade_date"].to_list() == _nav()["trade_date"].to_list()
    assert result.monthly_returns.to_dicts() == [
        {
            "year": 2024,
            "month": 1,
            "period_start": date(2024, 1, 30),
            "period_end": date(2024, 1, 31),
            "portfolio_return": pytest.approx(0.1),
            "benchmark_return": pytest.approx(0.05),
            "relative_return": pytest.approx(0.05),
        },
        {
            "year": 2024,
            "month": 2,
            "period_start": date(2024, 2, 1),
            "period_end": date(2024, 2, 2),
            "portfolio_return": pytest.approx(0.0),
            "benchmark_return": pytest.approx(-0.02857142857142858),
            "relative_return": pytest.approx(0.02857142857142858),
        },
    ]
    assert result.annual_returns.to_dicts() == [
        {
            "year": 2024,
            "period_start": date(2024, 1, 30),
            "period_end": date(2024, 2, 2),
            "portfolio_return": pytest.approx(0.1),
            "benchmark_return": pytest.approx(0.02),
            "relative_return": pytest.approx(0.08),
        }
    ]
    assert result.undefined_metrics == {}


def test_annual_returns_include_the_first_session_move_across_years() -> None:
    """Using the new year's first NAV as baseline drops its cross-year move."""
    nav = _period_nav(
        [
            date(2023, 12, 28),
            date(2023, 12, 29),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ],
        [10_000, 11_000, 9_900, 12_100],
        [100.0, 105.0, 100.0, 110.0],
    )

    result = calculate_performance(nav, _empty_fills(), _empty_costs())

    assert result.annual_returns.to_dicts() == [
        {
            "year": 2023,
            "period_start": date(2023, 12, 28),
            "period_end": date(2023, 12, 29),
            "portfolio_return": pytest.approx(0.1),
            "benchmark_return": pytest.approx(0.05),
            "relative_return": pytest.approx(0.05),
        },
        {
            "year": 2024,
            "period_start": date(2024, 1, 2),
            "period_end": date(2024, 1, 3),
            "portfolio_return": pytest.approx(0.1),
            "benchmark_return": pytest.approx(0.04761904761904767),
            "relative_return": pytest.approx(0.052380952380952334),
        },
    ]


def test_period_returns_compound_to_full_cumulative_return() -> None:
    """Dropping any boundary-session return breaks period-to-total compounding."""
    nav = _period_nav(
        [
            date(2023, 12, 28),
            date(2023, 12, 29),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ],
        [10_000, 11_000, 9_900, 12_100],
        [100.0, 105.0, 100.0, 110.0],
    )

    result = calculate_performance(nav, _empty_fills(), _empty_costs())

    assert result.metrics["cumulative_return"] == pytest.approx(0.21)
    for periods in (result.monthly_returns, result.annual_returns):
        compounded = 1.0
        for period_return in periods["portfolio_return"].to_list():
            compounded *= 1.0 + period_return
        assert compounded - 1.0 == pytest.approx(0.21)


def test_zero_volatility_discloses_undefined_ratios_without_nonfinite_values() -> None:
    """Zero denominators must become null with stable reasons, never NaN/Infinity."""
    nav = pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "cash_fen": [10_000, 10_000, 10_000],
            "market_value_fen": [0, 0, 0],
            "nav_fen": [10_000, 10_000, 10_000],
            "benchmark_close": [100.0, 100.0, 100.0],
        },
        schema=NAV_SCHEMA,
    )

    result = calculate_performance(nav, _empty_fills(), _empty_costs())

    assert result.metrics["annualized_volatility"] == 0.0
    assert result.metrics["sharpe_ratio"] is None
    assert result.metrics["sortino_ratio"] is None
    assert result.metrics["calmar_ratio"] is None
    assert result.metrics["information_ratio"] is None
    assert result.metrics["failed_fill_rate"] == 0.0
    assert result.undefined_metrics == {
        "calmar_ratio": "ZERO_MAX_DRAWDOWN",
        "information_ratio": "ZERO_ACTIVE_VOLATILITY",
        "sharpe_ratio": "ZERO_VOLATILITY",
        "sortino_ratio": "ZERO_DOWNSIDE_DEVIATION",
    }


def test_all_negative_returns_have_a_finite_sortino_ratio() -> None:
    """Using only negative observations incorrectly can divide by zero or return NaN."""
    nav = pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "cash_fen": [10_000, 9_000, 8_100],
            "market_value_fen": [0, 0, 0],
            "nav_fen": [10_000, 9_000, 8_100],
            "benchmark_close": [100.0, 100.0, 100.0],
        },
        schema=NAV_SCHEMA,
    )

    result = calculate_performance(
        nav, _empty_fills(), _empty_costs(), sessions_per_year=2
    )

    assert result.metrics["sortino_ratio"] == pytest.approx(-(2**0.5))


def test_unrecovered_drawdown_is_null_and_disclosed() -> None:
    """Inventing a recovery date hides a still-open drawdown."""
    nav = _nav().slice(0, 3)

    result = calculate_performance(nav, _fills(), _costs(), sessions_per_year=3)

    assert result.metrics["max_drawdown_recovery_date"] is None
    assert result.undefined_metrics["max_drawdown_recovery_date"] == (
        "DRAWDOWN_NOT_RECOVERED"
    )


def test_single_nav_row_has_exactly_defined_and_undefined_metrics() -> None:
    """A one-row run has no return sample and must not fabricate annualization."""
    result = calculate_performance(_nav().slice(0, 1), _empty_fills(), _empty_costs())

    assert result.metrics["observations"] == 1
    assert result.metrics["cumulative_return"] == 0.0
    assert result.metrics["annualized_return"] is None
    assert result.metrics["annualized_volatility"] == 0.0
    assert result.metrics["sharpe_ratio"] is None
    assert result.metrics["sortino_ratio"] is None
    assert result.metrics["information_ratio"] is None
    assert result.undefined_metrics["annualized_return"] == (
        "INSUFFICIENT_OBSERVATIONS"
    )


NavMutation = Callable[[pl.DataFrame], pl.DataFrame]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        pytest.param(
            lambda frame: frame.with_columns(pl.col("nav_fen").cast(pl.Int32)),
            "nav schema",
            id="wrong-schema",
        ),
        pytest.param(lambda frame: frame.reverse(), "sorted", id="out-of-order"),
        pytest.param(
            lambda frame: pl.concat([frame, frame.slice(0, 1)]).sort("trade_date"),
            "unique",
            id="duplicate-key",
        ),
        pytest.param(
            lambda frame: frame.with_columns(
                pl.when(pl.col("trade_date") == date(2024, 2, 1))
                .then(float("nan"))
                .otherwise(pl.col("benchmark_close"))
                .alias("benchmark_close")
            ),
            "finite",
            id="nonfinite-benchmark",
        ),
        pytest.param(
            lambda frame: frame.with_columns(
                pl.when(pl.col("trade_date") == date(2024, 2, 1))
                .then(0)
                .otherwise(pl.col("nav_fen"))
                .alias("nav_fen"),
                pl.when(pl.col("trade_date") == date(2024, 2, 1))
                .then(0)
                .otherwise(pl.col("cash_fen"))
                .alias("cash_fen"),
            ),
            "positive",
            id="zero-nav",
        ),
    ],
)
def test_nav_invariants_fail_closed(mutate: NavMutation, message: str) -> None:
    """Each malformed NAV invariant must be rejected before metric calculation."""
    with pytest.raises(ValueError, match=message):
        calculate_performance(mutate(_nav()), _fills(), _costs())


def test_negative_cost_fails_closed() -> None:
    """Negative fee source fields cannot become an apparently favorable fee rate."""
    costs = _costs().with_columns(
        pl.lit(-1, dtype=pl.Int64).alias("commission_fen"),
        pl.lit(-1, dtype=pl.Int64).alias("total_fees_fen"),
    )

    with pytest.raises(ValueError, match="nonnegative"):
        calculate_performance(_nav(), _fills(), costs)


def test_cross_table_execution_identity_mismatch_fails_closed() -> None:
    """A cost row must never be charged to a different fill identity."""
    costs = _costs().with_columns(
        pl.lit("SSE:600009", dtype=pl.String).alias("instrument_id")
    )

    with pytest.raises(ValueError, match="identity"):
        calculate_performance(_nav(), _fills(), costs)


@pytest.mark.parametrize(
    ("fills", "costs", "message"),
    [
        pytest.param(
            _fills().with_columns(pl.col("result_index").cast(pl.Int64)),
            _costs(),
            "fills schema",
            id="fills-schema",
        ),
        pytest.param(
            _fills().reverse(),
            _costs(),
            "fills rows must be canonically sorted",
            id="fills-order",
        ),
        pytest.param(
            pl.concat([_fills(), _fills().slice(0, 1)]).sort(
                ["trade_date", "result_index"]
            ),
            _costs(),
            "fills primary key must be unique",
            id="fills-primary-key",
        ),
        pytest.param(
            _fills().with_columns(
                pl.when(pl.col("filled_quantity") > 0)
                .then(float("inf"))
                .otherwise(pl.col("price"))
                .alias("price")
            ),
            _costs(),
            "fill price must be finite",
            id="fills-finite",
        ),
        pytest.param(
            _fills(),
            _costs().with_columns(pl.col("result_index").cast(pl.Int64)),
            "costs schema",
            id="costs-schema",
        ),
        pytest.param(
            _fills(),
            pl.concat([_costs(), _costs()]),
            "costs primary key must be unique",
            id="costs-primary-key",
        ),
    ],
)
def test_execution_table_schema_order_key_and_finite_invariants_fail_closed(
    fills: pl.DataFrame, costs: pl.DataFrame, message: str
) -> None:
    """Each canonical execution-table invariant is independently enforced."""
    with pytest.raises(ValueError, match=message):
        calculate_performance(_nav(), fills, costs)
