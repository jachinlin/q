"""提供分析与绩效计算相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from math import exp, isfinite, log, sqrt

import numpy as np
import polars as pl
from numpy.typing import NDArray

_NAV_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "cash_fen": pl.Int64,
        "dividend_receivable_fen": pl.Int64,
        "long_market_value_fen": pl.Int64,
        "short_market_value_fen": pl.Int64,
        "accrued_fees_fen": pl.Int64,
        "margin_used_fen": pl.Int64,
        "equity_fen": pl.Int64,
        "benchmark_close": pl.Float64,
    }
)
_HOLDINGS_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "instrument_id": pl.String,
        "total_quantity": pl.Int64,
        "sellable_quantity": pl.Int64,
        "cost_basis_fen": pl.Int64,
        "market_value_fen": pl.Int64,
    }
)
_FILLS_SCHEMA = pl.Schema(
    {
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
)
_COSTS_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "result_index": pl.Int64,
        "instrument_id": pl.String,
        "rule_fees_fen": pl.Int64,
        "slippage_fen": pl.Int64,
        "total_cost_fen": pl.Int64,
    }
)

_MONTHLY_SCHEMA = pl.Schema(
    {
        "year": pl.Int32,
        "month": pl.Int8,
        "period_start": pl.Date,
        "period_end": pl.Date,
        "portfolio_return": pl.Float64,
        "benchmark_return": pl.Float64,
        "relative_return": pl.Float64,
    }
)
_ANNUAL_SCHEMA = pl.Schema(
    {
        "year": pl.Int32,
        "period_start": pl.Date,
        "period_end": pl.Date,
        "portfolio_return": pl.Float64,
        "benchmark_return": pl.Float64,
        "relative_return": pl.Float64,
    }
)
_DRAWDOWN_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "nav": pl.Float64,
        "benchmark_nav": pl.Float64,
        "gross_nav": pl.Float64,
        "gross_cumulative_return": pl.Float64,
        "cumulative_cost_drag": pl.Float64,
        "portfolio_daily_return": pl.Float64,
        "benchmark_daily_return": pl.Float64,
        "running_peak_nav": pl.Float64,
        "drawdown": pl.Float64,
        "active_nav": pl.Float64,
        "active_running_peak_nav": pl.Float64,
        "active_drawdown": pl.Float64,
    }
)
_ROLLING_PERFORMANCE_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "window_sessions": pl.Int64,
        "annualized_return": pl.Float64,
        "benchmark_annualized_return": pl.Float64,
        "annualized_excess_return": pl.Float64,
        "annualized_volatility": pl.Float64,
        "sharpe_ratio": pl.Float64,
        "max_drawdown": pl.Float64,
        "tracking_error": pl.Float64,
        "information_ratio": pl.Float64,
        "beta": pl.Float64,
    }
)
_DRAWDOWN_EPISODE_SCHEMA = pl.Schema(
    {
        "episode_index": pl.Int64,
        "peak_date": pl.Date,
        "trough_date": pl.Date,
        "recovery_date": pl.Date,
        "max_drawdown": pl.Float64,
        "underwater_sessions": pl.Int64,
        "recovery_sessions": pl.Int64,
        "is_recovered": pl.Boolean,
    }
)
_EXECUTION_SUMMARY_SCHEMA = pl.Schema(
    {
        "side": pl.String,
        "reason_code": pl.String,
        "order_count": pl.Int64,
        "requested_quantity": pl.Int64,
        "filled_quantity": pl.Int64,
        "unfilled_quantity": pl.Int64,
        "priced_requested_notional_fen": pl.Int64,
        "priced_filled_notional_fen": pl.Int64,
        "unpriced_order_count": pl.Int64,
    }
)


@dataclass(frozen=True, slots=True)
class PerformanceResult:
    """记录一次回测绩效与归因操作的结果、业务指标和审计身份。

    入参：
        metrics：参与本次处理的指标集合；调用方不得依赖未声明的顺序。
        nav：按交易日排序的账户净值序列，用于计算收益、回撤和归因。
        drawdown：回撤。
        rolling_performance：固定 252 个收益观察值的滚动绩效。
        drawdown_episodes：按时间排序的完整回撤事件。
        monthly_returns：月度收益序列。
        annual_returns：年度收益序列。
        execution_summary：按买卖方向和执行原因汇总的成交质量表。
        undefined_metrics：参与本次处理的未定义指标指标集合；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    metrics: Mapping[str, int | float | str | None]
    nav: pl.DataFrame
    drawdown: pl.DataFrame
    rolling_performance: pl.DataFrame
    drawdown_episodes: pl.DataFrame
    monthly_returns: pl.DataFrame
    annual_returns: pl.DataFrame
    execution_summary: pl.DataFrame
    undefined_metrics: Mapping[str, str]


def calculate_performance(
    nav: pl.DataFrame,
    holdings: pl.DataFrame,
    fills: pl.DataFrame,
    costs: pl.DataFrame,
    dividends: pl.DataFrame | None = None,
    *,
    sessions_per_year: int = 252,
) -> PerformanceResult:
    """计算绩效；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        nav：按交易日排序的账户净值序列，用于计算收益、回撤和归因。
        holdings：逐交易日、逐证券的收盘持仓快照。
        fills：回测撮合产生的逐笔成交及拒绝记录。
        costs：按交易日汇总的佣金、印花税和其他交易成本。
        dividends：分红送转与基金拆分审计明细；缺省表示无公司行动。
        sessions_per_year：将日频波动率和收益率年化时采用的年交易会话数。
    返回值：
        返回计算绩效后的绩效（``PerformanceResult``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Calculate metrics from validated canonical backtest tables.
    """
    _PerformanceSupport._validate_inputs(nav, holdings, fills, costs, sessions_per_year)
    dividend_rows = dividends if dividends is not None else pl.DataFrame()

    dates = tuple(nav["trade_date"].to_list())
    nav_values = np.asarray(nav["equity_fen"].to_list(), dtype=np.float64)
    benchmark_values = np.asarray(nav["benchmark_close"].to_list(), dtype=np.float64)
    normalized_nav = nav_values / nav_values[0]
    normalized_benchmark = benchmark_values / benchmark_values[0]
    portfolio_daily = _PerformanceSupport._daily_returns(nav_values)
    benchmark_daily = _PerformanceSupport._daily_returns(benchmark_values)
    sample = portfolio_daily[1:]
    benchmark_sample = benchmark_daily[1:]
    active_sample = sample - benchmark_sample
    running_peak = np.maximum.accumulate(normalized_nav)
    drawdowns = normalized_nav / running_peak - 1.0
    active_nav = normalized_nav / normalized_benchmark
    active_running_peak = np.maximum.accumulate(active_nav)
    active_drawdowns = active_nav / active_running_peak - 1.0
    monthly = _PerformanceSupport._period_returns(
        dates, nav_values, benchmark_values, include_month=True
    )
    annual = _PerformanceSupport._period_returns(
        dates, nav_values, benchmark_values, include_month=False
    )

    undefined: dict[str, str] = {}
    cumulative_return = float(normalized_nav[-1] - 1.0)
    benchmark_cumulative = float(normalized_benchmark[-1] - 1.0)
    annualized_return = _PerformanceSupport._annualized_return(
        normalized_nav[-1],
        len(nav_values),
        sessions_per_year,
        undefined,
        "annualized_return",
    )
    benchmark_annualized_return = _PerformanceSupport._annualized_return(
        normalized_benchmark[-1],
        len(nav_values),
        sessions_per_year,
        undefined,
        "benchmark_annualized_return",
    )
    annualized_geometric_excess_return = _PerformanceSupport._annualized_return(
        np.float64(active_nav[-1]),
        len(nav_values),
        sessions_per_year,
        undefined,
        "annualized_geometric_excess_return",
    )
    annualized_volatility = _PerformanceSupport._annualized_volatility(
        sample, sessions_per_year
    )
    sharpe_ratio = _PerformanceSupport._sharpe(sample, sessions_per_year, undefined)
    sortino_ratio = _PerformanceSupport._sortino(sample, sessions_per_year, undefined)
    (
        max_drawdown,
        peak_date,
        trough_date,
        recovery_date,
    ) = _PerformanceSupport._drawdown_metrics(
        dates, normalized_nav, running_peak, drawdowns, undefined
    )
    calmar_ratio = _PerformanceSupport._calmar(
        annualized_return, max_drawdown, undefined
    )
    information_ratio = _PerformanceSupport._information_ratio(
        active_sample, sessions_per_year, undefined
    )
    tracking_error = _PerformanceSupport._tracking_error(
        active_sample, sessions_per_year, undefined
    )
    beta, jensen_alpha = _PerformanceSupport._beta_alpha(
        sample, benchmark_sample, sessions_per_year, undefined
    )
    fees_by_date = _PerformanceSupport._fees_by_date(costs)
    if fees_by_date.get(dates[0], 0):
        raise ValueError("first NAV session must not contain fees")
    gross_daily = portfolio_daily.copy()
    for index in range(1, len(dates)):
        gross_daily[index] += fees_by_date.get(dates[index], 0) / nav_values[index - 1]
    gross_growth = np.cumprod(1.0 + gross_daily)
    gross_cumulative_return = float(gross_growth[-1] - 1.0)
    gross_annualized_return = _PerformanceSupport._annualized_return(
        gross_growth[-1],
        len(nav_values),
        sessions_per_year,
        undefined,
        "gross_annualized_return",
    )
    cumulative_cost_drag = gross_cumulative_return - cumulative_return
    annualized_cost_drag = (
        None
        if gross_annualized_return is None or annualized_return is None
        else gross_annualized_return - annualized_return
    )
    if annualized_cost_drag is None:
        undefined["annualized_cost_drag"] = "ANNUALIZED_RETURN_NOT_AVAILABLE"
    mean_nav = float(np.mean(nav_values))
    gross_value = sum(abs(value) for value in fills["gross_value_fen"].to_list())
    total_fees = sum(costs["total_cost_fen"].to_list())
    failed_fills = sum(
        unfilled > 0 or filled == 0
        for unfilled, filled in zip(
            fills["unfilled_quantity"].to_list(),
            fills["filled_quantity"].to_list(),
            strict=True,
        )
    )
    failed_fill_rate: float | None = None
    if fills.height:
        failed_fill_rate = float(failed_fills / fills.height)
    else:
        undefined["failed_fill_rate"] = "NO_ORDERS"
    annualized_turnover = _PerformanceSupport._annualized_turnover(
        dates, nav_values, fills, sessions_per_year, undefined
    )
    execution_summary = _PerformanceSupport._execution_summary(fills)
    notional_fill_rate, priced_order_coverage_rate = (
        _PerformanceSupport._execution_rates(fills, undefined)
    )
    average_cash_weight = float(
        np.mean(np.asarray(nav["cash_fen"].to_list(), dtype=np.float64) / nav_values)
    )
    average_receivable_weight = float(
        np.mean(
            np.asarray(
                nav["dividend_receivable_fen"].to_list(), dtype=np.float64
            )
            / nav_values
        )
    )
    nav_by_date = dict(zip(dates, nav_values, strict=True))
    max_position_weight = max(
        (
            row["market_value_fen"] / nav_by_date[row["trade_date"]]
            for row in holdings.iter_rows(named=True)
        ),
        default=0.0,
    )
    max_drawdown_duration = _PerformanceSupport._max_drawdown_duration(drawdowns)
    time_under_water_rate = float(np.count_nonzero(drawdowns < 0.0) / len(drawdowns))
    historical_var_95_loss, historical_expected_shortfall_95_loss = (
        _PerformanceSupport._historical_tail_losses(sample, undefined)
    )
    positive_month_rate = float(
        np.count_nonzero(
            np.asarray(monthly["portfolio_return"].to_list(), dtype=np.float64) > 0.0
        )
        / monthly.height
    )

    metrics: dict[str, int | float | str | None] = {
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "observations": len(nav_values),
        "cumulative_return": cumulative_return,
        "annualized_return": annualized_return,
        "benchmark_annualized_return": benchmark_annualized_return,
        "geometric_excess_return": float(
            normalized_nav[-1] / normalized_benchmark[-1] - 1.0
        ),
        "annualized_geometric_excess_return": annualized_geometric_excess_return,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "max_drawdown": max_drawdown,
        "max_drawdown_peak_date": peak_date.isoformat(),
        "max_drawdown_trough_date": trough_date.isoformat(),
        "max_drawdown_recovery_date": (
            recovery_date.isoformat() if recovery_date is not None else None
        ),
        "calmar_ratio": calmar_ratio,
        "tracking_error": tracking_error,
        "beta": beta,
        "jensen_alpha": jensen_alpha,
        "active_max_drawdown": float(np.min(active_drawdowns)),
        "one_way_turnover": float(gross_value / (2.0 * mean_nav)),
        "annualized_turnover": annualized_turnover,
        "fee_rate": float(total_fees / mean_nav),
        "gross_cumulative_return": gross_cumulative_return,
        "gross_annualized_return": gross_annualized_return,
        "cumulative_cost_drag": cumulative_cost_drag,
        "annualized_cost_drag": annualized_cost_drag,
        "failed_fill_rate": failed_fill_rate,
        "notional_fill_rate": notional_fill_rate,
        "priced_order_coverage_rate": priced_order_coverage_rate,
        "average_cash_weight": average_cash_weight,
        "average_receivable_weight": average_receivable_weight,
        "gross_dividend_cash_fen": (
            int(dividend_rows["gross_cash_fen"].sum())
            if "gross_cash_fen" in dividend_rows.columns
            else 0
        ),
        "stock_distribution_quantity": (
            int(
                dividend_rows.filter(
                    pl.col("action_type") == "STOCK_DISTRIBUTION"
                )["distributed_quantity"].sum()
            )
            if {"action_type", "distributed_quantity"}.issubset(
                dividend_rows.columns
            )
            else 0
        ),
        "fund_split_quantity": (
            int(
                dividend_rows.filter(pl.col("action_type") == "FUND_SPLIT")[
                    "distributed_quantity"
                ].sum()
            )
            if {"action_type", "distributed_quantity"}.issubset(
                dividend_rows.columns
            )
            else 0
        ),
        "discarded_fractional_stock_quantity": (
            float(dividend_rows["discarded_fractional_quantity"].sum())
            if "discarded_fractional_quantity" in dividend_rows.columns
            else 0.0
        ),
        "max_position_weight": max_position_weight,
        "max_drawdown_duration_sessions": max_drawdown_duration,
        "time_under_water_rate": time_under_water_rate,
        "benchmark_cumulative_return": benchmark_cumulative,
        "relative_cumulative_return": cumulative_return - benchmark_cumulative,
        "information_ratio": information_ratio,
        "positive_month_rate": positive_month_rate,
        "historical_daily_var_95_loss": historical_var_95_loss,
        "historical_daily_expected_shortfall_95_loss": (
            historical_expected_shortfall_95_loss
        ),
    }
    _PerformanceSupport._require_finite_metrics(metrics)

    drawdown = pl.DataFrame(
        {
            "trade_date": dates,
            "nav": normalized_nav,
            "benchmark_nav": normalized_benchmark,
            "gross_nav": gross_growth,
            "gross_cumulative_return": gross_growth - 1.0,
            "cumulative_cost_drag": gross_growth - normalized_nav,
            "portfolio_daily_return": portfolio_daily,
            "benchmark_daily_return": benchmark_daily,
            "running_peak_nav": running_peak,
            "drawdown": drawdowns,
            "active_nav": active_nav,
            "active_running_peak_nav": active_running_peak,
            "active_drawdown": active_drawdowns,
        },
        schema=_DRAWDOWN_SCHEMA,
    )
    rolling_performance = _PerformanceSupport._rolling_performance(
        dates,
        normalized_nav,
        normalized_benchmark,
        portfolio_daily,
        benchmark_daily,
        sessions_per_year,
    )
    drawdown_episodes = _PerformanceSupport._drawdown_episodes(dates, drawdowns)
    nav_output = nav.with_columns(
        pl.Series("nav", normalized_nav, dtype=pl.Float64),
        pl.Series("benchmark_nav", normalized_benchmark, dtype=pl.Float64),
        pl.Series("portfolio_daily_return", portfolio_daily, dtype=pl.Float64),
        pl.Series("benchmark_daily_return", benchmark_daily, dtype=pl.Float64),
    )
    return PerformanceResult(
        metrics,
        nav_output,
        drawdown,
        rolling_performance,
        drawdown_episodes,
        monthly,
        annual,
        execution_summary,
        dict(sorted(undefined.items())),
    )


class _PerformanceSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_inputs(
        nav: pl.DataFrame,
        holdings: pl.DataFrame,
        fills: pl.DataFrame,
        costs: pl.DataFrame,
        sessions_per_year: int,
    ) -> None:
        if not all(
            isinstance(frame, pl.DataFrame) for frame in (nav, holdings, fills, costs)
        ):
            raise TypeError("nav, holdings, fills, and costs must be Polars DataFrames")
        if nav.schema != _NAV_SCHEMA:
            raise ValueError("nav schema must match the canonical backtest schema")
        if holdings.schema != _HOLDINGS_SCHEMA:
            raise ValueError("holdings schema must match the canonical backtest schema")
        if fills.schema != _FILLS_SCHEMA:
            raise ValueError("fills schema must match the canonical backtest schema")
        if costs.schema != _COSTS_SCHEMA:
            raise ValueError("costs schema must match the canonical backtest schema")
        if type(sessions_per_year) is not int or sessions_per_year <= 0:
            raise ValueError("sessions_per_year must be a positive integer")
        if nav.is_empty():
            raise ValueError("nav must contain at least one row")
        _PerformanceSupport._require_non_null(nav, tuple(nav.columns), "nav")
        _PerformanceSupport._require_non_null(
            holdings, tuple(holdings.columns), "holdings"
        )
        _PerformanceSupport._require_non_null(
            fills,
            tuple(
                column
                for column in fills.columns
                if column not in {"price", "detail"}
                and column not in {"reference_price", "requested_reference_value_fen"}
            ),
            "fills",
        )
        _PerformanceSupport._require_non_null(costs, tuple(costs.columns), "costs")
        _PerformanceSupport._require_canonical(nav, ("trade_date",), "nav")
        _PerformanceSupport._require_canonical(
            holdings, ("trade_date", "instrument_id"), "holdings"
        )
        _PerformanceSupport._require_canonical(
            fills, ("trade_date", "result_index"), "fills"
        )
        _PerformanceSupport._require_canonical(
            costs, ("trade_date", "result_index"), "costs"
        )

        nav_rows = nav.select(
            "trade_date",
            "cash_fen",
            "dividend_receivable_fen",
            "long_market_value_fen",
            "short_market_value_fen",
            "accrued_fees_fen",
            "margin_used_fen",
            "equity_fen",
            "benchmark_close",
        ).iter_rows(named=True)
        nav_dates: set[date] = set()
        market_value_by_date: dict[date, int] = {}
        for row in nav_rows:
            trade_date = row["trade_date"]
            cash = row["cash_fen"]
            receivable = row["dividend_receivable_fen"]
            market_value = row["long_market_value_fen"]
            short_market_value = row["short_market_value_fen"]
            accrued_fees = row["accrued_fees_fen"]
            margin_used = row["margin_used_fen"]
            nav_fen = row["equity_fen"]
            benchmark = row["benchmark_close"]
            nav_dates.add(trade_date)
            market_value_by_date[trade_date] = market_value
            if any(
                value < 0
                for value in (
                    cash,
                    receivable,
                    market_value,
                    short_market_value,
                    accrued_fees,
                    margin_used,
                )
            ):
                raise ValueError("nav monetary values must be nonnegative")
            if nav_fen != (
                cash
                + receivable
                + market_value
                - short_market_value
                - accrued_fees
            ):
                raise ValueError("nav equity identity is invalid")
            if nav_fen <= 0:
                raise ValueError("nav_fen must be positive")
            if not isfinite(benchmark):
                raise ValueError("benchmark_close must be finite")
            if benchmark <= 0:
                raise ValueError("benchmark_close must be positive")

        holdings_value_by_date = {trade_date: 0 for trade_date in nav_dates}
        for row in holdings.iter_rows(named=True):
            trade_date = row["trade_date"]
            if trade_date not in nav_dates:
                raise ValueError("holding identity must reference a NAV trade date")
            if row["total_quantity"] <= 0:
                raise ValueError("holding quantity must be positive")
            if not 0 <= row["sellable_quantity"] <= row["total_quantity"]:
                raise ValueError("holding sellable quantity is invalid")
            if row["cost_basis_fen"] < 0 or row["market_value_fen"] < 0:
                raise ValueError("holding monetary values are invalid")
            holdings_value_by_date[trade_date] += row["market_value_fen"]
        if holdings_value_by_date != market_value_by_date:
            raise ValueError("holdings market values must equal NAV")

        fill_by_key: dict[tuple[date, int], tuple[str, int]] = {}
        for row in fills.iter_rows(named=True):
            key = (row["trade_date"], row["result_index"])
            if row["trade_date"] not in nav_dates:
                raise ValueError("fill identity must reference a NAV trade date")
            if (
                row["requested_quantity"] < 0
                or row["filled_quantity"] < 0
                or row["unfilled_quantity"] < 0
                or row["gross_value_fen"] < 0
            ):
                raise ValueError("fill quantities and gross value must be nonnegative")
            if (
                row["filled_quantity"] + row["unfilled_quantity"]
                != row["requested_quantity"]
            ):
                raise ValueError("fill quantity identity is invalid")
            reference_price = row["reference_price"]
            if reference_price is not None:
                if (
                    not isinstance(reference_price, float)
                    or not isfinite(reference_price)
                    or reference_price <= 0
                ):
                    raise ValueError("fill reference price is invalid")
            elif row["reason_code"] not in {"SUSPENDED", "NO_MARKET_DATA"}:
                raise ValueError("only unpriceable rejects may omit reference values")
            price = row["price"]
            if price is not None and (not isfinite(price) or price <= 0):
                raise ValueError("fill price must be finite and positive when present")
            fill_by_key[key] = (row["instrument_id"], row["filled_quantity"])

        cost_keys: set[tuple[date, int]] = set()
        for row in costs.iter_rows(named=True):
            key = (row["trade_date"], row["result_index"])
            components = (row["rule_fees_fen"], row["slippage_fen"])
            if any(value < 0 for value in (*components, row["total_cost_fen"])):
                raise ValueError("cost fields must be nonnegative")
            if sum(components) != row["total_cost_fen"]:
                raise ValueError("cost component identity is invalid")
            fill = fill_by_key.get(key)
            if fill is None or fill[0] != row["instrument_id"] or fill[1] <= 0:
                raise ValueError("cost identity must match a filled execution")
            cost_keys.add(key)
        expected_cost_keys = {
            key for key, (_, quantity) in fill_by_key.items() if quantity > 0
        }
        if cost_keys != expected_cost_keys:
            raise ValueError("cost identity must match every filled execution")

    @staticmethod
    def _require_non_null(
        frame: pl.DataFrame, columns: tuple[str, ...], label: str
    ) -> None:
        if any(frame[column].null_count() for column in columns):
            raise ValueError(f"{label} required fields must be non-null")

    @staticmethod
    def _require_canonical(
        frame: pl.DataFrame, key_columns: tuple[str, ...], label: str
    ) -> None:
        keys = list(frame.select(key_columns).iter_rows())
        if len(keys) != len(set(keys)):
            raise ValueError(f"{label} primary key must be unique")
        if keys != sorted(keys):
            raise ValueError(f"{label} rows must be canonically sorted")

    @staticmethod
    def _daily_returns(values: NDArray[np.float64]) -> NDArray[np.float64]:
        result = np.zeros(len(values), dtype=np.float64)
        if len(values) > 1:
            result[1:] = values[1:] / values[:-1] - 1.0
        return result

    @staticmethod
    def _historical_tail_losses(
        sample: NDArray[np.float64], undefined: dict[str, str]
    ) -> tuple[float | None, float | None]:
        """计算历史法 95% 单日 VaR 与 Expected Shortfall 损失。"""

        if not len(sample):
            undefined["historical_daily_var_95_loss"] = "NO_RETURN_OBSERVATIONS"
            undefined["historical_daily_expected_shortfall_95_loss"] = (
                "NO_RETURN_OBSERVATIONS"
            )
            return None, None
        cutoff = float(np.quantile(sample, 0.05, method="linear"))
        tail = sample[sample <= cutoff]
        return max(0.0, -cutoff), max(0.0, -float(np.mean(tail)))

    @staticmethod
    def _rolling_performance(
        dates: tuple[date, ...],
        normalized_nav: NDArray[np.float64],
        normalized_benchmark: NDArray[np.float64],
        portfolio_daily: NDArray[np.float64],
        benchmark_daily: NDArray[np.float64],
        sessions_per_year: int,
        *,
        window_sessions: int = 252,
    ) -> pl.DataFrame:
        """生成固定收益观察窗口的滚动绩效表。"""

        rows: list[dict[str, object]] = []
        for end in range(window_sessions, len(dates)):
            start = end - window_sessions
            portfolio_sample = portfolio_daily[start + 1 : end + 1]
            benchmark_sample = benchmark_daily[start + 1 : end + 1]
            active_sample = portfolio_sample - benchmark_sample
            portfolio_ratio = float(normalized_nav[end] / normalized_nav[start])
            benchmark_ratio = float(
                normalized_benchmark[end] / normalized_benchmark[start]
            )
            annualized_return = _PerformanceSupport._annualized_growth_ratio(
                portfolio_ratio, window_sessions, sessions_per_year
            )
            benchmark_annualized_return = (
                _PerformanceSupport._annualized_growth_ratio(
                    benchmark_ratio, window_sessions, sessions_per_year
                )
            )
            volatility = float(np.std(portfolio_sample, ddof=1))
            active_volatility = float(np.std(active_sample, ddof=1))
            benchmark_variance = float(np.var(benchmark_sample, ddof=1))
            window_nav = normalized_nav[start : end + 1]
            window_drawdown = window_nav / np.maximum.accumulate(window_nav) - 1.0
            rows.append(
                {
                    "trade_date": dates[end],
                    "window_sessions": window_sessions,
                    "annualized_return": annualized_return,
                    "benchmark_annualized_return": benchmark_annualized_return,
                    "annualized_excess_return": (
                        _PerformanceSupport._annualized_growth_ratio(
                            portfolio_ratio / benchmark_ratio,
                            window_sessions,
                            sessions_per_year,
                        )
                    ),
                    "annualized_volatility": volatility
                    * sqrt(sessions_per_year),
                    "sharpe_ratio": (
                        float(np.mean(portfolio_sample))
                        / volatility
                        * sqrt(sessions_per_year)
                        if volatility > 0.0
                        else None
                    ),
                    "max_drawdown": float(np.min(window_drawdown)),
                    "tracking_error": active_volatility
                    * sqrt(sessions_per_year),
                    "information_ratio": (
                        float(np.mean(active_sample))
                        / active_volatility
                        * sqrt(sessions_per_year)
                        if active_volatility > 0.0
                        else None
                    ),
                    "beta": (
                        float(
                            np.cov(
                                portfolio_sample, benchmark_sample, ddof=1
                            )[0, 1]
                        )
                        / benchmark_variance
                        if benchmark_variance > 0.0
                        else None
                    ),
                }
            )
        return pl.DataFrame(rows, schema=_ROLLING_PERFORMANCE_SCHEMA)

    @staticmethod
    def _annualized_growth_ratio(
        terminal_ratio: float, observations: int, sessions_per_year: int
    ) -> float:
        """按正增长比例和观察数计算确定性的年化收益。"""

        return exp(log(terminal_ratio) * sessions_per_year / observations) - 1.0

    @staticmethod
    def _drawdown_episodes(
        dates: tuple[date, ...], drawdowns: NDArray[np.float64]
    ) -> pl.DataFrame:
        """从完整回撤序列提取按时间排序的独立潜水事件。"""

        rows: list[dict[str, object]] = []
        start: int | None = None
        trough: int | None = None
        for index, value in enumerate(drawdowns):
            if value < 0.0 and start is None:
                start = index
                trough = index
            elif value < 0.0 and trough is not None:
                if value < drawdowns[trough]:
                    trough = index
            elif value >= 0.0 and start is not None and trough is not None:
                rows.append(
                    _PerformanceSupport._drawdown_episode_row(
                        len(rows) + 1,
                        dates,
                        drawdowns,
                        start,
                        trough,
                        index,
                    )
                )
                start = None
                trough = None
        if start is not None and trough is not None:
            rows.append(
                _PerformanceSupport._drawdown_episode_row(
                    len(rows) + 1,
                    dates,
                    drawdowns,
                    start,
                    trough,
                    None,
                )
            )
        return pl.DataFrame(rows, schema=_DRAWDOWN_EPISODE_SCHEMA)

    @staticmethod
    def _drawdown_episode_row(
        episode_index: int,
        dates: tuple[date, ...],
        drawdowns: NDArray[np.float64],
        start: int,
        trough: int,
        recovery: int | None,
    ) -> dict[str, object]:
        peak = max(0, start - 1)
        return {
            "episode_index": episode_index,
            "peak_date": dates[peak],
            "trough_date": dates[trough],
            "recovery_date": dates[recovery] if recovery is not None else None,
            "max_drawdown": float(drawdowns[trough]),
            "underwater_sessions": (
                recovery - start if recovery is not None else len(dates) - start
            ),
            "recovery_sessions": (
                recovery - trough if recovery is not None else None
            ),
            "is_recovered": recovery is not None,
        }

    @staticmethod
    def _reference_value_fen(price: float, quantity: int) -> int:
        return int(
            (Decimal(str(price)) * quantity * Decimal(100)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )

    @staticmethod
    def _fees_by_date(costs: pl.DataFrame) -> dict[date, int]:
        totals: dict[date, int] = {}
        for trade_date, total_fees in costs.select(
            "trade_date", "total_cost_fen"
        ).iter_rows():
            totals[trade_date] = totals.get(trade_date, 0) + total_fees
        return totals

    @staticmethod
    def _annualized_turnover(
        dates: tuple[date, ...],
        nav_values: NDArray[np.float64],
        fills: pl.DataFrame,
        sessions_per_year: int,
        undefined: dict[str, str],
    ) -> float | None:
        if len(dates) < 2:
            undefined["annualized_turnover"] = "INSUFFICIENT_OBSERVATIONS"
            return None
        gross_by_date: dict[date, int] = {}
        for trade_date, gross_value in fills.select(
            "trade_date", "gross_value_fen"
        ).iter_rows():
            gross_by_date[trade_date] = gross_by_date.get(trade_date, 0) + gross_value
        total = 0.0
        for index, trade_date in enumerate(dates):
            denominator = nav_values[index - 1] if index else nav_values[0]
            total += gross_by_date.get(trade_date, 0) / (2.0 * denominator)
        return total * sessions_per_year / (len(dates) - 1)

    @staticmethod
    def _execution_summary(fills: pl.DataFrame) -> pl.DataFrame:
        grouped: dict[tuple[str, str], dict[str, int]] = {}
        for row in fills.iter_rows(named=True):
            key = (row["side"], row["reason_code"])
            values = grouped.setdefault(
                key,
                {
                    "order_count": 0,
                    "requested_quantity": 0,
                    "filled_quantity": 0,
                    "unfilled_quantity": 0,
                    "priced_requested_notional_fen": 0,
                    "priced_filled_notional_fen": 0,
                    "unpriced_order_count": 0,
                },
            )
            values["order_count"] += 1
            values["requested_quantity"] += row["requested_quantity"]
            values["filled_quantity"] += row["filled_quantity"]
            values["unfilled_quantity"] += row["unfilled_quantity"]
            reference = row["reference_price"]
            if reference is None:
                values["unpriced_order_count"] += 1
            else:
                values["priced_requested_notional_fen"] += (
                    _PerformanceSupport._reference_value_fen(
                        reference, row["requested_quantity"]
                    )
                )
                values["priced_filled_notional_fen"] += (
                    _PerformanceSupport._reference_value_fen(
                        reference, row["filled_quantity"]
                    )
                    if row["filled_quantity"]
                    else 0
                )
        rows = [
            {"side": side, "reason_code": reason, **values}
            for (side, reason), values in sorted(grouped.items())
        ]
        return pl.DataFrame(rows, schema=_EXECUTION_SUMMARY_SCHEMA)

    @staticmethod
    def _execution_rates(
        fills: pl.DataFrame, undefined: dict[str, str]
    ) -> tuple[float | None, float | None]:
        if fills.is_empty():
            undefined["notional_fill_rate"] = "NO_ORDERS"
            undefined["priced_order_coverage_rate"] = "NO_ORDERS"
            return None, None
        priced = fills.filter(pl.col("reference_price").is_not_null())
        coverage = priced.height / fills.height
        if priced.is_empty():
            undefined["notional_fill_rate"] = "NO_PRICEABLE_ORDERS"
            return None, coverage
        requested = sum(
            _PerformanceSupport._reference_value_fen(reference, quantity)
            for reference, quantity in priced.select(
                "reference_price", "requested_quantity"
            ).iter_rows()
        )
        filled_value = sum(
            _PerformanceSupport._reference_value_fen(reference, quantity)
            for reference, quantity in priced.select(
                "reference_price", "filled_quantity"
            ).iter_rows()
        )
        return filled_value / requested, coverage

    @staticmethod
    def _max_drawdown_duration(drawdowns: NDArray[np.float64]) -> int:
        longest = 0
        current = 0
        for value in drawdowns:
            if value < 0.0:
                current += 1
                longest = max(longest, current)
            else:
                current = 0
        return longest

    @staticmethod
    def _annualized_return(
        terminal_ratio: np.float64,
        observations: int,
        sessions_per_year: int,
        undefined: dict[str, str],
        metric_name: str,
    ) -> float | None:
        if observations < 2:
            undefined[metric_name] = "INSUFFICIENT_OBSERVATIONS"
            return None
        exponent = sessions_per_year / (observations - 1)
        try:
            result = exp(log(float(terminal_ratio)) * exponent) - 1.0
        except OverflowError:
            result = float("inf")
        if not isfinite(result):
            undefined[metric_name] = "NONFINITE_ANNUALIZED_RETURN"
            return None
        return result

    @staticmethod
    def _annualized_volatility(
        sample: NDArray[np.float64], sessions_per_year: int
    ) -> float:
        if len(sample) < 2:
            return 0.0
        volatility = float(np.std(sample, ddof=1))
        if volatility == 0.0:
            return 0.0
        return volatility * sqrt(sessions_per_year)

    @staticmethod
    def _sharpe(
        sample: NDArray[np.float64],
        sessions_per_year: int,
        undefined: dict[str, str],
    ) -> float | None:
        if len(sample) < 2:
            undefined["sharpe_ratio"] = "ZERO_VOLATILITY"
            return None
        volatility = float(np.std(sample, ddof=1))
        if volatility == 0.0:
            undefined["sharpe_ratio"] = "ZERO_VOLATILITY"
            return None
        return float(np.mean(sample)) / volatility * sqrt(sessions_per_year)

    @staticmethod
    def _sortino(
        sample: NDArray[np.float64],
        sessions_per_year: int,
        undefined: dict[str, str],
    ) -> float | None:
        if not len(sample):
            undefined["sortino_ratio"] = "ZERO_DOWNSIDE_DEVIATION"
            return None
        downside = float(sqrt(float(np.mean(np.minimum(sample, 0.0) ** 2))))
        if downside == 0.0:
            undefined["sortino_ratio"] = "ZERO_DOWNSIDE_DEVIATION"
            return None
        return float(np.mean(sample)) / downside * sqrt(sessions_per_year)

    @staticmethod
    def _drawdown_metrics(
        dates: tuple[date, ...],
        normalized_nav: NDArray[np.float64],
        running_peak: NDArray[np.float64],
        drawdowns: NDArray[np.float64],
        undefined: dict[str, str],
    ) -> tuple[float, date, date, date | None]:
        trough_index = int(np.argmin(drawdowns))
        peak_nav = running_peak[trough_index]
        peak_index = int(
            np.flatnonzero(normalized_nav[: trough_index + 1] == peak_nav)[0]
        )
        max_drawdown = float(drawdowns[trough_index])
        if max_drawdown == 0.0:
            return (
                max_drawdown,
                dates[peak_index],
                dates[trough_index],
                dates[trough_index],
            )
        recovered = np.flatnonzero(normalized_nav[trough_index + 1 :] >= peak_nav)
        if len(recovered):
            recovery_index = trough_index + 1 + int(recovered[0])
            recovery_date: date | None = dates[recovery_index]
        else:
            recovery_date = None
            undefined["max_drawdown_recovery_date"] = "DRAWDOWN_NOT_RECOVERED"
        return (
            max_drawdown,
            dates[peak_index],
            dates[trough_index],
            recovery_date,
        )

    @staticmethod
    def _calmar(
        annualized_return: float | None,
        max_drawdown: float,
        undefined: dict[str, str],
    ) -> float | None:
        if max_drawdown == 0.0:
            undefined["calmar_ratio"] = "ZERO_MAX_DRAWDOWN"
            return None
        if annualized_return is None:
            undefined["calmar_ratio"] = "ANNUALIZED_RETURN_NOT_AVAILABLE"
            return None
        return annualized_return / abs(max_drawdown)

    @staticmethod
    def _information_ratio(
        active_sample: NDArray[np.float64],
        sessions_per_year: int,
        undefined: dict[str, str],
    ) -> float | None:
        if len(active_sample) < 2:
            undefined["information_ratio"] = "ZERO_ACTIVE_VOLATILITY"
            return None
        volatility = float(np.std(active_sample, ddof=1))
        if volatility == 0.0:
            undefined["information_ratio"] = "ZERO_ACTIVE_VOLATILITY"
            return None
        return float(np.mean(active_sample)) / volatility * sqrt(sessions_per_year)

    @staticmethod
    def _tracking_error(
        active_sample: NDArray[np.float64],
        sessions_per_year: int,
        undefined: dict[str, str],
    ) -> float | None:
        if len(active_sample) < 2:
            undefined["tracking_error"] = "INSUFFICIENT_OBSERVATIONS"
            return None
        volatility = float(np.std(active_sample, ddof=1))
        if volatility == 0.0:
            return 0.0
        return volatility * sqrt(sessions_per_year)

    @staticmethod
    def _beta_alpha(
        portfolio_sample: NDArray[np.float64],
        benchmark_sample: NDArray[np.float64],
        sessions_per_year: int,
        undefined: dict[str, str],
    ) -> tuple[float | None, float | None]:
        if len(portfolio_sample) < 2:
            undefined["beta"] = "INSUFFICIENT_OBSERVATIONS"
            undefined["jensen_alpha"] = "INSUFFICIENT_OBSERVATIONS"
            return None, None
        benchmark_variance = float(np.var(benchmark_sample, ddof=1))
        if benchmark_variance == 0.0:
            undefined["beta"] = "ZERO_BENCHMARK_VARIANCE"
            undefined["jensen_alpha"] = "ZERO_BENCHMARK_VARIANCE"
            return None, None
        covariance = float(np.cov(portfolio_sample, benchmark_sample, ddof=1)[0, 1])
        beta = covariance / benchmark_variance
        daily_alpha = float(np.mean(portfolio_sample)) - beta * float(
            np.mean(benchmark_sample)
        )
        return beta, daily_alpha * sessions_per_year

    @staticmethod
    def _period_returns(
        dates: tuple[date, ...],
        nav_values: NDArray[np.float64],
        benchmark_values: NDArray[np.float64],
        *,
        include_month: bool,
    ) -> pl.DataFrame:
        grouped: dict[tuple[int, ...], list[int]] = {}
        for index, trade_date in enumerate(dates):
            key: tuple[int, ...] = (
                (trade_date.year, trade_date.month)
                if include_month
                else (trade_date.year,)
            )
            grouped.setdefault(key, []).append(index)
        rows: list[dict[str, object]] = []
        for key, indexes in grouped.items():
            first = indexes[0]
            last = indexes[-1]
            baseline = first if first == 0 else first - 1
            portfolio_return = float(nav_values[last] / nav_values[baseline] - 1.0)
            benchmark_return = float(
                benchmark_values[last] / benchmark_values[baseline] - 1.0
            )
            row: dict[str, object] = {
                "year": key[0],
                "period_start": dates[first],
                "period_end": dates[last],
                "portfolio_return": portfolio_return,
                "benchmark_return": benchmark_return,
                "relative_return": portfolio_return - benchmark_return,
            }
            if include_month:
                row["month"] = key[-1]
            rows.append(row)
        schema = _MONTHLY_SCHEMA if include_month else _ANNUAL_SCHEMA
        return pl.DataFrame(rows, schema=schema)

    @staticmethod
    def _require_finite_metrics(
        metrics: Mapping[str, int | float | str | None],
    ) -> None:
        for name, value in metrics.items():
            if isinstance(value, float) and not isfinite(value):
                raise ValueError(f"calculated metric {name} is nonfinite")
