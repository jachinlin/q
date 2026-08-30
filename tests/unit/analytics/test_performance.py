"""Hand-checked behavioral tests for backtest performance analytics."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta

import polars as pl
import pytest

from quant_research.analytics.performance import calculate_performance

NAV_SCHEMA = {
    "trade_date": pl.Date,
    "cash_fen": pl.Int64,
    "long_market_value_fen": pl.Int64,
    "short_market_value_fen": pl.Int64,
    "accrued_fees_fen": pl.Int64,
    "margin_used_fen": pl.Int64,
    "equity_fen": pl.Int64,
    "benchmark_close": pl.Float64,
}
HOLDINGS_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "total_quantity": pl.Int64,
    "sellable_quantity": pl.Int64,
    "cost_basis_fen": pl.Int64,
    "market_value_fen": pl.Int64,
}
FILLS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int64,
    "instrument_id": pl.String,
    "side": pl.String,
    "requested_quantity": pl.Int64,
    "filled_quantity": pl.Int64,
    "unfilled_quantity": pl.Int64,
    "reference_price": pl.Float64,
    "price": pl.Float64,
    "gross_value_fen": pl.Int64,
    "reason_code": pl.String,
}
COSTS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int64,
    "instrument_id": pl.String,
    "rule_fees_fen": pl.Int64,
    "slippage_fen": pl.Int64,
    "total_cost_fen": pl.Int64,
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
            "long_market_value_fen": [0, 0, 0, 0],
            "short_market_value_fen": [0, 0, 0, 0],
            "accrued_fees_fen": [0, 0, 0, 0],
            "margin_used_fen": [0, 0, 0, 0],
            "equity_fen": [10_000, 11_000, 9_900, 11_000],
            "benchmark_close": [100.0, 105.0, 100.0, 102.0],
        },
        schema=NAV_SCHEMA,
    )


def _fills() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 31), date(2024, 2, 1)],
            "result_index": [0, 0],
            "instrument_id": ["600001.SH", "600002.SH"],
            "side": ["BUY", "BUY"],
            "requested_quantity": [1, 5],
            "reference_price": [10.0, None],
            "filled_quantity": [1, 0],
            "unfilled_quantity": [0, 5],
            "price": [10.0, None],
            "gross_value_fen": [1_000, 0],
            "reason_code": ["FILLED", "NO_MARKET_DATA"],
        },
        schema=FILLS_SCHEMA,
    )


def _costs() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 31)],
            "result_index": [0],
            "instrument_id": ["600001.SH"],
            "rule_fees_fen": [10],
            "slippage_fen": [0],
            "total_cost_fen": [10],
        },
        schema=COSTS_SCHEMA,
    )


def _empty_fills() -> pl.DataFrame:
    return pl.DataFrame(schema=FILLS_SCHEMA)


def _empty_holdings() -> pl.DataFrame:
    return pl.DataFrame(schema=HOLDINGS_SCHEMA)


def _empty_costs() -> pl.DataFrame:
    return pl.DataFrame(schema=COSTS_SCHEMA)


def _period_nav(
    dates: list[date], nav_values: list[int], benchmark_values: list[float]
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": dates,
            "cash_fen": nav_values,
            "long_market_value_fen": [0] * len(dates),
            "short_market_value_fen": [0] * len(dates),
            "accrued_fees_fen": [0] * len(dates),
            "margin_used_fen": [0] * len(dates),
            "equity_fen": nav_values,
            "benchmark_close": benchmark_values,
        },
        schema=NAV_SCHEMA,
    )


def test_calculate_performance_matches_hand_checked_metrics_and_periods() -> None:
    """A wrong metric formula, sample convention, or period boundary breaks this."""
    result = calculate_performance(
        _nav(), _empty_holdings(), _fills(), _costs(), sessions_per_year=3
    )

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
        "benchmark_annualized_return": 0.02,
        "geometric_excess_return": 1.1 / 1.02 - 1.0,
        "annualized_geometric_excess_return": 1.1 / 1.02 - 1.0,
        "tracking_error": 0.12798819311255116,
        "beta": 2.2307707810594835,
        "jensen_alpha": 0.0611843364873989,
        "active_max_drawdown": -0.055,
        "annualized_turnover": 0.05,
        "gross_cumulative_return": 0.101,
        "gross_annualized_return": 0.101,
        "cumulative_cost_drag": 0.001,
        "annualized_cost_drag": 0.001,
        "notional_fill_rate": 1.0,
        "priced_order_coverage_rate": 0.5,
        "average_cash_weight": 1.0,
        "max_position_weight": 0.0,
        "max_drawdown_duration_sessions": 1,
        "time_under_water_rate": 0.25,
        "benchmark_cumulative_return": 0.02,
        "relative_cumulative_return": 0.08,
        "information_ratio": 0.6932683130554919,
        "positive_month_rate": 0.5,
        "historical_daily_var_95_loss": 0.08,
        "historical_daily_expected_shortfall_95_loss": 0.1,
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
        "active_nav": [1.0, 1.1 / 1.05, 0.99, 1.1 / 1.02],
        "active_running_peak_nav": [1.0, 1.1 / 1.05, 1.1 / 1.05, 1.1 / 1.02],
        "active_drawdown": [0.0, 0.0, -0.055, 0.0],
    }
    for name, expected in expected_drawdown.items():
        assert result.drawdown[name].to_list() == pytest.approx(expected)
    assert result.drawdown["gross_nav"].to_list() == pytest.approx(
        [1.0, 1.101, 0.9909, 1.101]
    )
    assert result.drawdown["gross_cumulative_return"].to_list() == pytest.approx(
        [0.0, 0.101, -0.0091, 0.101]
    )
    assert result.drawdown["cumulative_cost_drag"].to_list() == pytest.approx(
        [0.0, 0.001, 0.0009, 0.001]
    )
    assert result.rolling_performance.is_empty()
    assert result.drawdown_episodes.to_dicts() == [
        {
            "episode_index": 1,
            "peak_date": date(2024, 1, 31),
            "trough_date": date(2024, 2, 1),
            "recovery_date": date(2024, 2, 2),
            "max_drawdown": pytest.approx(-0.1),
            "underwater_sessions": 1,
            "recovery_sessions": 1,
            "is_recovered": True,
        }
    ]
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
    assert result.execution_summary.to_dicts() == [
        {
            "side": "BUY",
            "reason_code": "FILLED",
            "order_count": 1,
            "requested_quantity": 1,
            "filled_quantity": 1,
            "unfilled_quantity": 0,
            "priced_requested_notional_fen": 1_000,
            "priced_filled_notional_fen": 1_000,
            "unpriced_order_count": 0,
        },
        {
            "side": "BUY",
            "reason_code": "NO_MARKET_DATA",
            "order_count": 1,
            "requested_quantity": 5,
            "filled_quantity": 0,
            "unfilled_quantity": 5,
            "priced_requested_notional_fen": 0,
            "priced_filled_notional_fen": 0,
            "unpriced_order_count": 1,
        },
    ]
    assert result.undefined_metrics == {}


def test_rolling_performance_uses_exactly_252_return_observations() -> None:
    """滚动表必须在第 252 个收益观察值结束时才产生首行。"""

    dates = [date(2024, 1, 1) + timedelta(days=index) for index in range(253)]
    nav = _period_nav(dates, [10_000] * 253, [100.0] * 253)

    result = calculate_performance(
        nav, _empty_holdings(), _empty_fills(), _empty_costs()
    )

    assert result.rolling_performance.to_dicts() == [
        {
            "trade_date": dates[-1],
            "window_sessions": 252,
            "annualized_return": 0.0,
            "benchmark_annualized_return": 0.0,
            "annualized_excess_return": 0.0,
            "annualized_volatility": 0.0,
            "sharpe_ratio": None,
            "max_drawdown": 0.0,
            "tracking_error": 0.0,
            "information_ratio": None,
            "beta": None,
        }
    ]
    assert result.drawdown_episodes.is_empty()


def test_all_unpriced_orders_disclose_null_notional_rate_and_zero_coverage() -> None:
    """全部停牌或无行情时只能定义覆盖率，不能伪造金额成交率。"""
    fills = (
        _fills()
        .slice(1, 1)
        .with_columns(pl.lit(0, dtype=pl.Int64).alias("result_index"))
    )

    result = calculate_performance(
        _nav(), _empty_holdings(), fills, _empty_costs(), sessions_per_year=3
    )

    assert result.metrics["notional_fill_rate"] is None
    assert result.metrics["priced_order_coverage_rate"] == 0.0
    assert result.undefined_metrics["notional_fill_rate"] == "NO_PRICEABLE_ORDERS"
    assert "priced_order_coverage_rate" not in result.undefined_metrics


def test_cash_and_position_weights_are_calculated_from_daily_holdings() -> None:
    """现金和最大持仓权重必须来自逐日账户快照而非目标权重。"""
    nav = _nav().with_columns(
        pl.Series("cash_fen", [10_000, 6_000, 4_900, 11_000], dtype=pl.Int64),
        pl.Series(
            "long_market_value_fen", [0, 5_000, 5_000, 0], dtype=pl.Int64
        ),
    )
    holdings = pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 31), date(2024, 2, 1)],
            "instrument_id": ["600001.SH", "600001.SH"],
            "total_quantity": [100, 100],
            "sellable_quantity": [100, 100],
            "cost_basis_fen": [5_000, 5_000],
            "market_value_fen": [5_000, 5_000],
        },
        schema=HOLDINGS_SCHEMA,
    )

    result = calculate_performance(
        nav, holdings, _fills(), _costs(), sessions_per_year=3
    )

    assert result.metrics["average_cash_weight"] == pytest.approx(
        (1.0 + 6_000 / 11_000 + 4_900 / 9_900 + 1.0) / 4
    )
    assert result.metrics["max_position_weight"] == pytest.approx(5_000 / 9_900)


def test_fee_on_first_nav_session_fails_closed() -> None:
    """首个净值会话已有费用时无法重建完整费用前路径，必须拒绝分析。"""
    first_fill = (
        _fills()
        .slice(0, 1)
        .with_columns(pl.lit(date(2024, 1, 30), dtype=pl.Date).alias("trade_date"))
    )
    first_cost = _costs().with_columns(
        pl.lit(date(2024, 1, 30), dtype=pl.Date).alias("trade_date")
    )

    with pytest.raises(ValueError, match="first NAV session"):
        calculate_performance(_nav(), _empty_holdings(), first_fill, first_cost)


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

    result = calculate_performance(
        nav, _empty_holdings(), _empty_fills(), _empty_costs()
    )

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

    result = calculate_performance(
        nav, _empty_holdings(), _empty_fills(), _empty_costs()
    )

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
            "long_market_value_fen": [0, 0, 0],
            "short_market_value_fen": [0, 0, 0],
            "accrued_fees_fen": [0, 0, 0],
            "margin_used_fen": [0, 0, 0],
            "equity_fen": [10_000, 10_000, 10_000],
            "benchmark_close": [100.0, 100.0, 100.0],
        },
        schema=NAV_SCHEMA,
    )

    result = calculate_performance(
        nav, _empty_holdings(), _empty_fills(), _empty_costs()
    )

    assert result.metrics["annualized_volatility"] == 0.0
    assert result.metrics["sharpe_ratio"] is None
    assert result.metrics["sortino_ratio"] is None
    assert result.metrics["calmar_ratio"] is None
    assert result.metrics["information_ratio"] is None
    assert result.metrics["failed_fill_rate"] is None
    assert result.undefined_metrics == {
        "beta": "ZERO_BENCHMARK_VARIANCE",
        "calmar_ratio": "ZERO_MAX_DRAWDOWN",
        "failed_fill_rate": "NO_ORDERS",
        "information_ratio": "ZERO_ACTIVE_VOLATILITY",
        "jensen_alpha": "ZERO_BENCHMARK_VARIANCE",
        "notional_fill_rate": "NO_ORDERS",
        "priced_order_coverage_rate": "NO_ORDERS",
        "sharpe_ratio": "ZERO_VOLATILITY",
        "sortino_ratio": "ZERO_DOWNSIDE_DEVIATION",
    }


def test_all_negative_returns_have_a_finite_sortino_ratio() -> None:
    """Using only negative observations incorrectly can divide by zero or return NaN."""
    nav = pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "cash_fen": [10_000, 9_000, 8_100],
            "long_market_value_fen": [0, 0, 0],
            "short_market_value_fen": [0, 0, 0],
            "accrued_fees_fen": [0, 0, 0],
            "margin_used_fen": [0, 0, 0],
            "equity_fen": [10_000, 9_000, 8_100],
            "benchmark_close": [100.0, 100.0, 100.0],
        },
        schema=NAV_SCHEMA,
    )

    result = calculate_performance(
        nav, _empty_holdings(), _empty_fills(), _empty_costs(), sessions_per_year=2
    )

    assert result.metrics["sortino_ratio"] == pytest.approx(-(2**0.5))


def test_unrecovered_drawdown_is_null_and_disclosed() -> None:
    """Inventing a recovery date hides a still-open drawdown."""
    nav = _nav().slice(0, 3)

    result = calculate_performance(
        nav, _empty_holdings(), _fills(), _costs(), sessions_per_year=3
    )

    assert result.metrics["max_drawdown_recovery_date"] is None
    assert result.undefined_metrics["max_drawdown_recovery_date"] == (
        "DRAWDOWN_NOT_RECOVERED"
    )
    assert result.drawdown_episodes.to_dicts() == [
        {
            "episode_index": 1,
            "peak_date": date(2024, 1, 31),
            "trough_date": date(2024, 2, 1),
            "recovery_date": None,
            "max_drawdown": pytest.approx(-0.1),
            "underwater_sessions": 1,
            "recovery_sessions": None,
            "is_recovered": False,
        }
    ]


def test_single_nav_row_has_exactly_defined_and_undefined_metrics() -> None:
    """A one-row run has no return sample and must not fabricate annualization."""
    result = calculate_performance(
        _nav().slice(0, 1),
        _empty_holdings(),
        _empty_fills(),
        _empty_costs(),
    )

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
            lambda frame: frame.with_columns(pl.col("equity_fen").cast(pl.Int32)),
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
                .otherwise(pl.col("equity_fen"))
                .alias("equity_fen"),
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
        calculate_performance(mutate(_nav()), _empty_holdings(), _fills(), _costs())


def test_negative_cost_fails_closed() -> None:
    """Negative fee source fields cannot become an apparently favorable fee rate."""
    costs = _costs().with_columns(
        pl.lit(-1, dtype=pl.Int64).alias("rule_fees_fen"),
        pl.lit(-1, dtype=pl.Int64).alias("total_cost_fen"),
    )

    with pytest.raises(ValueError, match="nonnegative"):
        calculate_performance(_nav(), _empty_holdings(), _fills(), costs)


def test_cross_table_execution_identity_mismatch_fails_closed() -> None:
    """A cost row must never be charged to a different fill identity."""
    costs = _costs().with_columns(
        pl.lit("600009.SH", dtype=pl.String).alias("instrument_id")
    )

    with pytest.raises(ValueError, match="identity"):
        calculate_performance(_nav(), _empty_holdings(), _fills(), costs)


@pytest.mark.parametrize(
    ("fills", "costs", "message"),
    [
        pytest.param(
            _fills().with_columns(pl.col("result_index").cast(pl.Int32)),
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
            _costs().with_columns(pl.col("result_index").cast(pl.Int32)),
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
        calculate_performance(_nav(), _empty_holdings(), fills, costs)
