"""定义策略 Run 内存表与最终 Parquet 的唯一 Schema。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import polars as pl

RUN_PARQUET_SCHEMAS: dict[str, dict[str, object]] = {
    "signals": {
        "signal_date": pl.Date,
        "instrument_id": pl.String,
        "signal": pl.String,
        "value": pl.Float64,
        "state_changed": pl.Boolean,
        "invalid_reason": pl.String,
    },
    "orders": {
        "signal_date": pl.Date,
        "execute_date": pl.Date,
        "order_index": pl.Int64,
        "instrument_id": pl.String,
        "side": pl.String,
        "quantity": pl.Int64,
        "reason": pl.String,
    },
    "fills": {
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
    },
    "holdings": {
        "trade_date": pl.Date,
        "instrument_id": pl.String,
        "total_quantity": pl.Int64,
        "sellable_quantity": pl.Int64,
        "cost_basis_fen": pl.Int64,
        "market_value_fen": pl.Int64,
    },
    "costs": {
        "trade_date": pl.Date,
        "result_index": pl.Int64,
        "instrument_id": pl.String,
        "rule_fees_fen": pl.Int64,
        "slippage_fen": pl.Int64,
        "total_cost_fen": pl.Int64,
    },
    "nav": {
        "trade_date": pl.Date,
        "cash_fen": pl.Int64,
        "long_market_value_fen": pl.Int64,
        "short_market_value_fen": pl.Int64,
        "accrued_fees_fen": pl.Int64,
        "margin_used_fen": pl.Int64,
        "equity_fen": pl.Int64,
        "benchmark_close": pl.Float64,
    },
    "performance": {
        "trade_date": pl.Date,
        "return": pl.Float64,
        "benchmark_return": pl.Float64,
        "cumulative_return": pl.Float64,
        "benchmark_cumulative_return": pl.Float64,
        "active_return": pl.Float64,
        "nav": pl.Float64,
        "benchmark_nav": pl.Float64,
        "gross_nav": pl.Float64,
        "gross_cumulative_return": pl.Float64,
        "cumulative_cost_drag": pl.Float64,
        "drawdown": pl.Float64,
        "active_drawdown": pl.Float64,
    },
    "rolling_performance": {
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
    },
    "drawdown_episodes": {
        "episode_index": pl.Int64,
        "peak_date": pl.Date,
        "trough_date": pl.Date,
        "recovery_date": pl.Date,
        "max_drawdown": pl.Float64,
        "underwater_sessions": pl.Int64,
        "recovery_sessions": pl.Int64,
        "is_recovered": pl.Boolean,
    },
    "monthly_returns": {
        "year": pl.Int32,
        "month": pl.Int8,
        "period_start": pl.Date,
        "period_end": pl.Date,
        "portfolio_return": pl.Float64,
        "benchmark_return": pl.Float64,
        "relative_return": pl.Float64,
    },
    "annual_returns": {
        "year": pl.Int32,
        "period_start": pl.Date,
        "period_end": pl.Date,
        "portfolio_return": pl.Float64,
        "benchmark_return": pl.Float64,
        "relative_return": pl.Float64,
    },
    "execution_summary": {
        "side": pl.String,
        "reason_code": pl.String,
        "order_count": pl.Int64,
        "requested_quantity": pl.Int64,
        "filled_quantity": pl.Int64,
        "unfilled_quantity": pl.Int64,
        "priced_requested_notional_fen": pl.Int64,
        "priced_filled_notional_fen": pl.Int64,
        "unpriced_order_count": pl.Int64,
    },
    "exposure_summary": {
        "trade_date": pl.Date,
        "dimension": pl.String,
        "key": pl.String,
        "weight": pl.Float64,
    },
    "attribution": {
        "trade_date": pl.Date,
        "dimension": pl.String,
        "key": pl.String,
        "pnl_fen": pl.Int64,
        "contribution_return": pl.Float64,
    },
}
RUN_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "signals": ("signal_date", "instrument_id"),
    "orders": ("signal_date", "order_index"),
    "fills": ("trade_date", "result_index"),
    "holdings": ("trade_date", "instrument_id"),
    "costs": ("trade_date", "result_index"),
    "nav": ("trade_date",),
    "performance": ("trade_date",),
    "rolling_performance": ("trade_date",),
    "drawdown_episodes": ("episode_index",),
    "monthly_returns": ("year", "month"),
    "annual_returns": ("year",),
    "execution_summary": ("side", "reason_code"),
    "exposure_summary": ("trade_date", "dimension", "key"),
    "attribution": ("trade_date", "dimension", "key"),
}
_BACKTEST_TABLES = ("signals", "orders", "fills", "holdings", "costs", "nav")


class RunTableSchema:
    """按唯一 Run Schema 规范化并验证内存表。

    入参：类仅提供无状态静态方法。返回值：规范化的固定 DataFrame 映射。
    异常：表名、字段、类型、主键或排序不符合契约时抛出对应异常。
    """

    @staticmethod
    def canonical_tables(
        tables: Mapping[str, Sequence[Mapping[str, object]] | pl.DataFrame],
    ) -> dict[str, pl.DataFrame]:
        """规范化全部回测和分析表。

        入参：任意固定表映射。返回值：包含全部固定表的规范化 DataFrame。
        异常：缺列、类型或唯一键不符合契约时抛出 ``ValueError``。
        """
        return RunTableSchema._canonical(tables, tuple(RUN_PARQUET_SCHEMAS))

    @staticmethod
    def canonical_backtest_tables(
        tables: Mapping[str, Sequence[Mapping[str, object]] | pl.DataFrame],
    ) -> dict[str, pl.DataFrame]:
        """规范化回测引擎负责的六张原始表。

        入参：回测内存行。返回值：六张固定 Schema DataFrame。
        异常：缺列、类型或唯一键不符合契约时抛出 ``ValueError``。
        """
        return RunTableSchema._canonical(tables, _BACKTEST_TABLES)

    @staticmethod
    def normalize(frame: pl.DataFrame, name: str) -> pl.DataFrame:
        """规范化一张已知 Run 表。

        入参：DataFrame 和产物名。返回值：固定列序与排序的表。
        异常：产物名未知或缺列时抛出 ``KeyError``/``ValueError``。
        """
        schema = RUN_PARQUET_SCHEMAS[name]
        missing = set(schema) - set(frame.columns)
        if missing:
            raise ValueError(f"artifact columns missing: {min(missing)}")
        return (
            frame.select(tuple(schema))
            .cast(cast(Any, schema))
            .sort(RUN_PRIMARY_KEYS[name])
        )

    @staticmethod
    def _canonical(
        tables: Mapping[str, Sequence[Mapping[str, object]] | pl.DataFrame],
        names: tuple[str, ...],
    ) -> dict[str, pl.DataFrame]:
        result: dict[str, pl.DataFrame] = {}
        for name in names:
            schema = RUN_PARQUET_SCHEMAS[name]
            value = tables.get(name, ())
            frame = (
                value
                if isinstance(value, pl.DataFrame)
                else pl.DataFrame(value, schema=cast(Any, schema))
            )
            normalized = RunTableSchema.normalize(frame, name)
            if normalized.select(
                pl.struct(RUN_PRIMARY_KEYS[name]).is_duplicated().any()
            ).item():
                raise ValueError("artifact primary key is not unique")
            result[name] = normalized
        return result


__all__ = ["RUN_PARQUET_SCHEMAS", "RUN_PRIMARY_KEYS", "RunTableSchema"]
