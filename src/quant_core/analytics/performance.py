"""Pure performance calculations for canonical backtest tables."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from math import exp, isfinite, log, sqrt

import numpy as np
import polars as pl
from numpy.typing import NDArray

METRICS_VERSION = "1.0.0"

_NAV_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "cash_fen": pl.Int64,
        "market_value_fen": pl.Int64,
        "nav_fen": pl.Int64,
        "benchmark_close": pl.Float64,
    }
)
_FILLS_SCHEMA = pl.Schema(
    {
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
)
_COSTS_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "result_index": pl.Int32,
        "instrument_id": pl.String,
        "commission_fen": pl.Int64,
        "stamp_tax_fen": pl.Int64,
        "transfer_fee_fen": pl.Int64,
        "total_fees_fen": pl.Int64,
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
        "portfolio_daily_return": pl.Float64,
        "benchmark_daily_return": pl.Float64,
        "running_peak_nav": pl.Float64,
        "drawdown": pl.Float64,
    }
)


@dataclass(frozen=True, slots=True)
class PerformanceResult:
    metrics_version: str
    metrics: Mapping[str, int | float | str | None]
    nav: pl.DataFrame
    drawdown: pl.DataFrame
    monthly_returns: pl.DataFrame
    annual_returns: pl.DataFrame
    undefined_metrics: Mapping[str, str]


def calculate_performance(
    nav: pl.DataFrame,
    fills: pl.DataFrame,
    costs: pl.DataFrame,
    *,
    sessions_per_year: int = 252,
) -> PerformanceResult:
    """Calculate metrics from validated canonical backtest tables."""
    _validate_inputs(nav, fills, costs, sessions_per_year)

    dates = tuple(nav["trade_date"].to_list())
    nav_values = np.asarray(nav["nav_fen"].to_list(), dtype=np.float64)
    benchmark_values = np.asarray(nav["benchmark_close"].to_list(), dtype=np.float64)
    normalized_nav = nav_values / nav_values[0]
    normalized_benchmark = benchmark_values / benchmark_values[0]
    portfolio_daily = _daily_returns(nav_values)
    benchmark_daily = _daily_returns(benchmark_values)
    sample = portfolio_daily[1:]
    benchmark_sample = benchmark_daily[1:]
    running_peak = np.maximum.accumulate(normalized_nav)
    drawdowns = normalized_nav / running_peak - 1.0

    undefined: dict[str, str] = {}
    cumulative_return = float(normalized_nav[-1] - 1.0)
    benchmark_cumulative = float(normalized_benchmark[-1] - 1.0)
    annualized_return = _annualized_return(
        normalized_nav[-1], len(nav_values), sessions_per_year, undefined
    )
    annualized_volatility = _annualized_volatility(sample, sessions_per_year)
    sharpe_ratio = _sharpe(sample, sessions_per_year, undefined)
    sortino_ratio = _sortino(sample, sessions_per_year, undefined)
    (
        max_drawdown,
        peak_date,
        trough_date,
        recovery_date,
    ) = _drawdown_metrics(dates, normalized_nav, running_peak, drawdowns, undefined)
    calmar_ratio = _calmar(annualized_return, max_drawdown, undefined)
    information_ratio = _information_ratio(
        sample - benchmark_sample, sessions_per_year, undefined
    )
    mean_nav = float(np.mean(nav_values))
    gross_value = sum(abs(value) for value in fills["gross_value_fen"].to_list())
    total_fees = sum(costs["total_fees_fen"].to_list())
    failed_fills = sum(
        unfilled > 0 or filled == 0
        for unfilled, filled in zip(
            fills["unfilled_quantity"].to_list(),
            fills["filled_quantity"].to_list(),
            strict=True,
        )
    )

    metrics: dict[str, int | float | str | None] = {
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "observations": len(nav_values),
        "cumulative_return": cumulative_return,
        "annualized_return": annualized_return,
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
        "one_way_turnover": float(gross_value / (2.0 * mean_nav)),
        "fee_rate": float(total_fees / mean_nav),
        "failed_fill_rate": (
            float(failed_fills / fills.height) if fills.height else 0.0
        ),
        "benchmark_cumulative_return": benchmark_cumulative,
        "relative_cumulative_return": cumulative_return - benchmark_cumulative,
        "information_ratio": information_ratio,
    }
    _require_finite_metrics(metrics)

    drawdown = pl.DataFrame(
        {
            "trade_date": dates,
            "nav": normalized_nav,
            "benchmark_nav": normalized_benchmark,
            "portfolio_daily_return": portfolio_daily,
            "benchmark_daily_return": benchmark_daily,
            "running_peak_nav": running_peak,
            "drawdown": drawdowns,
        },
        schema=_DRAWDOWN_SCHEMA,
    )
    nav_output = nav.with_columns(
        pl.Series("nav", normalized_nav, dtype=pl.Float64),
        pl.Series("benchmark_nav", normalized_benchmark, dtype=pl.Float64),
        pl.Series("portfolio_daily_return", portfolio_daily, dtype=pl.Float64),
        pl.Series("benchmark_daily_return", benchmark_daily, dtype=pl.Float64),
    )
    monthly = _period_returns(dates, nav_values, benchmark_values, include_month=True)
    annual = _period_returns(dates, nav_values, benchmark_values, include_month=False)
    return PerformanceResult(
        METRICS_VERSION,
        metrics,
        nav_output,
        drawdown,
        monthly,
        annual,
        dict(sorted(undefined.items())),
    )


def _validate_inputs(
    nav: pl.DataFrame,
    fills: pl.DataFrame,
    costs: pl.DataFrame,
    sessions_per_year: int,
) -> None:
    if not all(isinstance(frame, pl.DataFrame) for frame in (nav, fills, costs)):
        raise TypeError("nav, fills, and costs must be Polars DataFrames")
    if nav.schema != _NAV_SCHEMA:
        raise ValueError("nav schema must match the canonical backtest schema")
    if fills.schema != _FILLS_SCHEMA:
        raise ValueError("fills schema must match the canonical backtest schema")
    if costs.schema != _COSTS_SCHEMA:
        raise ValueError("costs schema must match the canonical backtest schema")
    if type(sessions_per_year) is not int or sessions_per_year <= 0:
        raise ValueError("sessions_per_year must be a positive integer")
    if nav.is_empty():
        raise ValueError("nav must contain at least one row")
    _require_non_null(nav, tuple(nav.columns), "nav")
    _require_non_null(
        fills,
        tuple(column for column in fills.columns if column not in {"price", "detail"}),
        "fills",
    )
    _require_non_null(costs, tuple(costs.columns), "costs")
    _require_canonical(nav, ("trade_date",), "nav")
    _require_canonical(fills, ("trade_date", "result_index"), "fills")
    _require_canonical(costs, ("trade_date", "result_index"), "costs")

    nav_rows = nav.select(
        "trade_date", "cash_fen", "market_value_fen", "nav_fen", "benchmark_close"
    ).iter_rows(named=True)
    nav_dates: set[date] = set()
    for row in nav_rows:
        trade_date = row["trade_date"]
        cash = row["cash_fen"]
        market_value = row["market_value_fen"]
        nav_fen = row["nav_fen"]
        benchmark = row["benchmark_close"]
        nav_dates.add(trade_date)
        if cash < 0 or market_value < 0:
            raise ValueError("nav cash and market value must be nonnegative")
        if nav_fen != cash + market_value:
            raise ValueError("nav identity must equal cash plus market value")
        if nav_fen <= 0:
            raise ValueError("nav_fen must be positive")
        if not isfinite(benchmark):
            raise ValueError("benchmark_close must be finite")
        if benchmark <= 0:
            raise ValueError("benchmark_close must be positive")

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
        price = row["price"]
        if price is not None and (not isfinite(price) or price <= 0):
            raise ValueError("fill price must be finite and positive when present")
        fill_by_key[key] = (row["instrument_id"], row["filled_quantity"])

    cost_keys: set[tuple[date, int]] = set()
    for row in costs.iter_rows(named=True):
        key = (row["trade_date"], row["result_index"])
        components = (
            row["commission_fen"],
            row["stamp_tax_fen"],
            row["transfer_fee_fen"],
        )
        if any(value < 0 for value in (*components, row["total_fees_fen"])):
            raise ValueError("cost fields must be nonnegative")
        if sum(components) != row["total_fees_fen"]:
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


def _require_non_null(
    frame: pl.DataFrame, columns: tuple[str, ...], label: str
) -> None:
    if any(frame[column].null_count() for column in columns):
        raise ValueError(f"{label} required fields must be non-null")


def _require_canonical(
    frame: pl.DataFrame, key_columns: tuple[str, ...], label: str
) -> None:
    keys = list(frame.select(key_columns).iter_rows())
    if len(keys) != len(set(keys)):
        raise ValueError(f"{label} primary key must be unique")
    if keys != sorted(keys):
        raise ValueError(f"{label} rows must be canonically sorted")


def _daily_returns(values: NDArray[np.float64]) -> NDArray[np.float64]:
    result = np.zeros(len(values), dtype=np.float64)
    if len(values) > 1:
        result[1:] = values[1:] / values[:-1] - 1.0
    return result


def _annualized_return(
    terminal_ratio: np.float64,
    observations: int,
    sessions_per_year: int,
    undefined: dict[str, str],
) -> float | None:
    if observations < 2:
        undefined["annualized_return"] = "INSUFFICIENT_OBSERVATIONS"
        return None
    exponent = sessions_per_year / (observations - 1)
    try:
        result = exp(log(float(terminal_ratio)) * exponent) - 1.0
    except OverflowError:
        result = float("inf")
    if not isfinite(result):
        undefined["annualized_return"] = "NONFINITE_ANNUALIZED_RETURN"
        return None
    return result


def _annualized_volatility(
    sample: NDArray[np.float64], sessions_per_year: int
) -> float:
    if len(sample) < 2:
        return 0.0
    volatility = float(np.std(sample, ddof=1))
    if volatility == 0.0:
        return 0.0
    return volatility * sqrt(sessions_per_year)


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


def _drawdown_metrics(
    dates: tuple[date, ...],
    normalized_nav: NDArray[np.float64],
    running_peak: NDArray[np.float64],
    drawdowns: NDArray[np.float64],
    undefined: dict[str, str],
) -> tuple[float, date, date, date | None]:
    trough_index = int(np.argmin(drawdowns))
    peak_nav = running_peak[trough_index]
    peak_index = int(np.flatnonzero(normalized_nav[: trough_index + 1] == peak_nav)[0])
    max_drawdown = float(drawdowns[trough_index])
    if max_drawdown == 0.0:
        return max_drawdown, dates[peak_index], dates[trough_index], dates[trough_index]
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
            (trade_date.year, trade_date.month) if include_month else (trade_date.year,)
        )
        grouped.setdefault(key, []).append(index)
    rows: list[dict[str, object]] = []
    for key, indexes in grouped.items():
        first = indexes[0]
        last = indexes[-1]
        portfolio_return = float(nav_values[last] / nav_values[first] - 1.0)
        benchmark_return = float(benchmark_values[last] / benchmark_values[first] - 1.0)
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


def _require_finite_metrics(
    metrics: Mapping[str, int | float | str | None],
) -> None:
    for name, value in metrics.items():
        if isinstance(value, float) and not isfinite(value):
            raise ValueError(f"calculated metric {name} is nonfinite")
