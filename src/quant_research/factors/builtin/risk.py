"""提供内置实现与风险因子相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Sequence
from math import expm1, fsum, isfinite, sqrt

import numpy as np
import polars as pl

from quant_research.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FactorSpec
from quant_research.factors.builtin.momentum import (
    AdjustedBarService,
    MarketBarsCache,
    _MarketFactor,
)

_PRICE_BASIS = "baostock_forward_log_return"
_LOG_RETURN_FORMULA = "log_close_minus_log_preclose"
_PATH_CONSTRUCTION = "window_forward_cumsum"
_ANNUALIZATION_SCALE = sqrt(252.0)


class Volatility60dFactor(_MarketFactor):
    """计算最近 60 个日对数收益的年化样本波动率。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        market_bars：市场数据行情。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Annualized sample volatility of the latest 60 daily log returns.
    """

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        *,
        market_bars: MarketBarsCache | None = None,
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="volatility_60d",
                frequency="daily",
                lookback_sessions=60,
                dependencies=(),
                direction=-1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "annualization_sessions": 252,
                    "ddof": 1,
                    "formula": "std(forward_log_return[1:61],ddof=1)*sqrt(252)",
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_basis": "exchange_sessions",
                    "suspension_return_policy": "explicit_placeholder_zero",
                    "missing_session_policy": "invalidate_window",
                    "window_prices": 61,
                    "window_returns": 60,
                },
            ),
            required_prices=61,
            evaluator=_RiskSupport._volatility_value,
            native_evaluator=_RiskSupport._volatility_expression,
            market_bars=market_bars,
        )


class DownsideVolatility60dFactor(_MarketFactor):
    """计算最近 60 日负对数收益的年化均方根下行波动率。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        market_bars：市场数据行情。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Annualized root-mean-square of negative log returns.
    """

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        *,
        market_bars: MarketBarsCache | None = None,
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="downside_volatility_60d",
                frequency="daily",
                lookback_sessions=60,
                dependencies=(),
                direction=-1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "annualization_sessions": 252,
                    "eligible_for_alpha": True,
                    "formula": (
                        "sqrt(mean(min(forward_log_return[1:61],0)^2))*sqrt(252)"
                    ),
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_basis": "exchange_sessions",
                    "suspension_return_policy": "explicit_placeholder_zero",
                    "missing_session_policy": "invalidate_window",
                    "window_prices": 61,
                },
            ),
            required_prices=61,
            evaluator=_RiskSupport._downside_volatility_value,
            native_evaluator=_RiskSupport._downside_volatility_expression,
            market_bars=market_bars,
        )


class MaxDrawdown120dFactor(_MarketFactor):
    """计算最近 120 个复权收盘价的最大峰谷回撤。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        market_bars：市场数据行情。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Largest peak-to-later-close loss in the latest 120 prices.
    """

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        *,
        market_bars: MarketBarsCache | None = None,
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="max_drawdown_120d",
                frequency="daily",
                lookback_sessions=119,
                dependencies=(),
                direction=-1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "eligible_for_alpha": True,
                    "formula": ("max(1-exp(relative_log_path-running_peak_log))"),
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_basis": "exchange_sessions",
                    "suspension_return_policy": "explicit_placeholder_zero",
                    "missing_session_policy": "invalidate_window",
                    "window_prices": 120,
                },
            ),
            required_prices=120,
            evaluator=_RiskSupport._max_drawdown_value,
            batch_evaluator=_RiskSupport._max_drawdown_batch,
            market_bars=market_bars,
        )


class _RiskSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _volatility_value(
        relative_log_path: Sequence[float], log_returns: Sequence[float]
    ) -> float | None:
        del relative_log_path
        return _RiskSupport._annualized_scaled_rms(
            log_returns,
            center=True,
            denominator=len(log_returns) - 1,
        )

    @staticmethod
    def _volatility_expression(log_returns: pl.Expr) -> pl.Expr:
        """Evaluate sample volatility with one native grouped rolling kernel."""
        finite = (
            log_returns.is_not_null()
            & log_returns.is_not_nan()
            & log_returns.is_finite()
        )
        finite_count = (
            finite.cast(pl.UInt32).rolling_sum(60, min_samples=60).over("instrument_id")
        )
        clean = pl.when(finite).then(log_returns).otherwise(0.0)
        rolling_std = clean.rolling_std(60, min_samples=60, ddof=1).over(
            "instrument_id"
        )
        return (
            pl.when(finite_count == 60)
            .then(rolling_std * _ANNUALIZATION_SCALE)
            .otherwise(pl.lit(None, dtype=pl.Float64))
        )

    @staticmethod
    def _downside_volatility_value(
        relative_log_path: Sequence[float], log_returns: Sequence[float]
    ) -> float | None:
        del relative_log_path
        return _RiskSupport._annualized_scaled_rms(
            [min(value, 0.0) for value in log_returns],
            center=False,
            denominator=len(log_returns),
        )

    @staticmethod
    def _downside_volatility_expression(log_returns: pl.Expr) -> pl.Expr:
        finite = (
            log_returns.is_not_null()
            & log_returns.is_not_nan()
            & log_returns.is_finite()
        )
        finite_count = (
            finite.cast(pl.UInt32).rolling_sum(60, min_samples=60).over("instrument_id")
        )
        downside = (
            pl.when(finite & (log_returns < 0.0)).then(log_returns).otherwise(0.0)
        )
        mean_square = (
            (downside * downside).rolling_mean(60, min_samples=60).over("instrument_id")
        )
        return (
            pl.when(finite_count == 60)
            .then(mean_square.sqrt() * _ANNUALIZATION_SCALE)
            .otherwise(pl.lit(None, dtype=pl.Float64))
        )

    @staticmethod
    def _annualized_scaled_rms(
        values: Sequence[float], *, center: bool, denominator: int
    ) -> float | None:
        if (
            not values
            or denominator <= 0
            or any(not isfinite(value) for value in values)
        ):
            return None
        scale = max(abs(value) for value in values)
        if scale == 0.0:
            return 0.0
        try:
            normalized = [value / scale for value in values]
            mean = fsum(normalized) / len(normalized) if center else 0.0
            normalized_square_sum = fsum((value - mean) ** 2 for value in normalized)
            result = (
                scale * sqrt(normalized_square_sum / denominator) * _ANNUALIZATION_SCALE
            )
        except OverflowError:
            return None
        return result if isfinite(result) else None

    @staticmethod
    def _max_drawdown_value(
        relative_log_path: Sequence[float], log_returns: Sequence[float]
    ) -> float | None:
        del log_returns
        peak = relative_log_path[0]
        drawdown = 0.0
        for log_price in relative_log_path:
            peak = max(peak, log_price)
            drawdown = max(drawdown, -expm1(log_price - peak))
        return drawdown if isfinite(drawdown) else None

    @staticmethod
    def _max_drawdown_batch(frame: pl.DataFrame) -> pl.Series:
        output = np.full(frame.height, np.nan, dtype=np.float64)
        offset = 0
        window_returns = 119
        chunk_size = 2048
        for group in frame.partition_by("instrument_id", maintain_order=True):
            returns = (
                group[FORWARD_LOG_RETURN_COLUMN]
                .fill_null(float("nan"))
                .to_numpy()
                .astype(np.float64, copy=False)
            )
            if len(returns) >= window_returns + 1:
                windows = np.lib.stride_tricks.sliding_window_view(
                    returns, window_returns
                )[1:]
                values = np.full(len(windows), np.nan, dtype=np.float64)
                for first in range(0, len(windows), chunk_size):
                    block = windows[first : first + chunk_size]
                    finite = np.isfinite(block).all(axis=1)
                    paths = np.empty((len(block), window_returns + 1), dtype=np.float64)
                    paths[:, 0] = 0.0
                    np.cumsum(block, axis=1, out=paths[:, 1:])
                    peaks = np.maximum.accumulate(paths, axis=1)
                    with np.errstate(invalid="ignore", over="ignore"):
                        drawdowns = np.max(-np.expm1(paths - peaks), axis=1)
                    drawdowns[~finite | ~np.isfinite(drawdowns)] = np.nan
                    values[first : first + len(block)] = drawdowns
                output[offset + window_returns : offset + len(returns)] = values
            offset += len(returns)
        return pl.Series("_factor_value", output, dtype=pl.Float64)
