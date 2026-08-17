"""提供内置实现与动量因子相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from math import expm1, isfinite
from threading import Lock
from typing import Protocol

import numpy as np
import polars as pl

from quant_research.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec

_RETURN_WINDOWS = frozenset({20, 60, 120})
_HISTORY_CALENDAR_MULTIPLIER = 3
_PRICE_BASIS = "baostock_forward_log_return"
_LOG_RETURN_FORMULA = "log_close_minus_log_preclose"
_PATH_CONSTRUCTION = "window_forward_cumsum"


class AdjustedBarService(Protocol):
    """定义内置市场因子所需的最小会话收益边界。

    入参：
        无。
    返回值：
        构造并返回 ``AdjustedBarService`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """读取包含精确回看会话、停牌补零且缺行保空的对数收益。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
            lookback_sessions：回看窗口交易会话集合。
        返回值：
            返回收益序列（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...


class MarketBarsCache:
    """在同一运行范围内为多个市场因子共享一份不可变复权行情读取。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        max_lookback_sessions：限制资源使用、数量或等待时间的上限回看窗口交易会话集合。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``RuntimeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Share one immutable adjusted market-bar read across sibling factors.
    """

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        *,
        max_lookback_sessions: int,
    ) -> None:
        if type(max_lookback_sessions) is not int or max_lookback_sessions < 0:
            raise ValueError("max_lookback_sessions must be a nonnegative integer")
        self._price_service = price_service
        self._instruments = _MomentumSupport._canonical_instrument_scope(instruments)
        self._max_lookback_sessions = max_lookback_sessions
        self._ctx: FactorContext | None = None
        self._bars: pl.DataFrame | None = None
        self._lock = Lock()

    def matches(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        *,
        max_lookback_sessions: int,
    ) -> bool:
        """处理因子计算中的``matches``。

        入参：
            price_service：价格服务。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            max_lookback_sessions：限制资源使用、数量或等待时间的上限回看窗口交易会话集合。
        返回值：
            返回是否处理因子计算中的``matches``。
        异常：
            无。
        Return whether this cache can serve the same pooling boundary.
        """
        return (
            self._price_service is price_service
            and tuple(instrument.canonical() for instrument in self._instruments)
            == tuple(instrument.canonical() for instrument in instruments)
            and self._max_lookback_sessions == max_lookback_sessions
        )

    def load(self, ctx: FactorContext) -> pl.DataFrame:
        """加载并校验约定资源。

        入参：
            ctx：本次计算的上下文，类型为 ``FactorContext``。
        返回值：
            返回加载并校验因子计算后的``load``（``pl.DataFrame``）。
        异常：
            输入或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Return the one normalized price frame for the active factor context.
        """
        with self._lock:
            if self._ctx == ctx and self._bars is not None:
                return self._bars
            bars = _MomentumSupport._load_log_returns(
                self._price_service,
                self._instruments,
                ctx,
                self._max_lookback_sessions,
            )
            self._ctx = ctx
            self._bars = bars
            return bars


class _MarketFactor:
    """Shared deterministic window execution over row-level log returns."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        spec: FactorSpec,
        required_prices: int,
        evaluator: Callable[[Sequence[float], Sequence[float]], float | None],
        native_evaluator: Callable[[pl.Expr], pl.Expr] | None = None,
        batch_evaluator: Callable[[pl.DataFrame], pl.Series] | None = None,
        market_bars: MarketBarsCache | None = None,
    ) -> None:
        self._price_service = price_service
        self._instruments = _MomentumSupport._canonical_instrument_scope(instruments)
        self._spec = spec
        self._required_prices = required_prices
        self._evaluator = evaluator
        self._native_evaluator = native_evaluator
        self._batch_evaluator = batch_evaluator
        self._market_bars = market_bars

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """Compute rows inside ``ctx`` using request-stable row log returns."""
        if self._market_bars is None:
            normalized = _MomentumSupport._load_log_returns(
                self._price_service,
                self._instruments,
                ctx,
                self.spec.lookback_sessions,
            )
        else:
            normalized = self._market_bars.load(ctx)
        if normalized.is_empty():
            return _MomentumSupport._empty_factor_output().lazy()

        if self._native_evaluator is not None:
            return self._compute_native(normalized, ctx)
        if self._batch_evaluator is not None:
            return self._compute_batch(normalized, ctx)

        output_dates: list[date] = []
        output_instruments: list[str] = []
        output_values: list[float | None] = []
        output_available_at: list[datetime | None] = []
        for instrument_frame in normalized.partition_by(
            "instrument_id", maintain_order=True
        ):
            trade_dates = instrument_frame["trade_date"].to_list()
            instrument_ids = instrument_frame["instrument_id"].to_list()
            log_returns = instrument_frame[FORWARD_LOG_RETURN_COLUMN].to_list()
            availability = instrument_frame["available_at"].to_list()
            for index, trade_date in enumerate(trade_dates):
                if not isinstance(trade_date, date):
                    raise TypeError("adjusted bar trade_date must be a date")
                if trade_date < ctx.start or trade_date > ctx.end:
                    continue
                start_index = index - self._required_prices + 1
                window_start = max(0, start_index)
                window_availability = availability[window_start : index + 1]
                available_at = _MomentumSupport._latest_available_at(
                    window_availability
                )
                value: float | None = None
                if (
                    start_index >= 0
                    and len(window_availability) == self._required_prices
                ):
                    log_window = _MomentumSupport._relative_log_window(
                        log_returns[window_start : index + 1]
                    )
                    if log_window is not None and available_at is not None:
                        relative_log_path, window_returns = log_window
                        value = self._evaluator(relative_log_path, window_returns)
                        if value is not None and not isfinite(value):
                            value = None
                instrument_id = instrument_ids[index]
                if not isinstance(instrument_id, str):
                    raise TypeError("adjusted bar instrument_id must be a string")
                output_dates.append(trade_date)
                output_instruments.append(instrument_id)
                output_values.append(value)
                output_available_at.append(available_at)
        count = len(output_dates)
        return (
            pl.DataFrame(
                {
                    "trade_date": output_dates,
                    "instrument_id": output_instruments,
                    "factor_id": [self.spec.factor_id] * count,
                    "value": output_values,
                    "available_at": output_available_at,
                    "is_valid": [value is not None for value in output_values],
                },
                schema=FACTOR_OUTPUT_SCHEMA,
            )
            .sort("trade_date", "instrument_id")
            .lazy()
        )

    def _compute_native(
        self, normalized: pl.DataFrame, ctx: FactorContext
    ) -> pl.LazyFrame:
        """Build one native grouped window plan without Python data iteration."""
        native_evaluator = self._native_evaluator
        if native_evaluator is None:
            raise RuntimeError("native market factor evaluator is unavailable")
        group = "instrument_id"
        row_number = pl.int_range(1, pl.len() + 1, dtype=pl.UInt32).over(group)
        observed_prices = row_number.clip(upper_bound=self._required_prices)
        availability_count = (
            pl.col("available_at")
            .is_not_null()
            .cast(pl.UInt32)
            .rolling_sum(self._required_prices, min_samples=1)
            .over(group)
        )
        latest_availability = (
            pl.col("available_at")
            .rolling_max(self._required_prices, min_samples=1)
            .over(group)
        )
        available_at = (
            pl.when(availability_count == observed_prices)
            .then(latest_availability)
            .otherwise(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
        )
        raw_value = native_evaluator(pl.col(FORWARD_LOG_RETURN_COLUMN))
        valid = (
            (row_number >= self._required_prices)
            & available_at.is_not_null()
            & raw_value.is_not_null()
            & raw_value.is_finite()
        )
        return (
            normalized.lazy()
            .with_columns(
                available_at.alias("_factor_available_at"),
                raw_value.alias("_factor_value"),
                valid.alias("_factor_valid"),
            )
            .filter(pl.col("trade_date").is_between(ctx.start, ctx.end, closed="both"))
            .select(
                pl.col("trade_date"),
                pl.col("instrument_id"),
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(pl.col("_factor_valid"))
                .then(pl.col("_factor_value"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("value"),
                pl.col("_factor_available_at").alias("available_at"),
                pl.col("_factor_valid").alias("is_valid"),
            )
            .sort("trade_date", "instrument_id")
        )

    def _compute_batch(
        self, normalized: pl.DataFrame, ctx: FactorContext
    ) -> pl.LazyFrame:
        """Compute bounded vectorized blocks without per-signal Python slicing."""
        batch_evaluator = self._batch_evaluator
        if batch_evaluator is None:
            raise RuntimeError("batch market factor evaluator is unavailable")
        values = batch_evaluator(normalized)
        if len(values) != normalized.height:
            raise ValueError("batch market factor output length differs")
        group = "instrument_id"
        row_number = pl.int_range(1, pl.len() + 1, dtype=pl.UInt32).over(group)
        observed_prices = row_number.clip(upper_bound=self._required_prices)
        availability_count = (
            pl.col("available_at")
            .is_not_null()
            .cast(pl.UInt32)
            .rolling_sum(self._required_prices, min_samples=1)
            .over(group)
        )
        latest_availability = (
            pl.col("available_at")
            .rolling_max(self._required_prices, min_samples=1)
            .over(group)
        )
        available_at = (
            pl.when(availability_count == observed_prices)
            .then(latest_availability)
            .otherwise(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
        )
        return (
            normalized.with_columns(
                values.alias("_factor_value"),
                row_number.alias("_row_number"),
                availability_count.alias("_availability_count"),
                available_at.alias("_factor_available_at"),
            )
            .lazy()
            .filter(pl.col("trade_date").is_between(ctx.start, ctx.end, closed="both"))
            .with_columns(
                (
                    (pl.col("_row_number") >= self._required_prices)
                    & (pl.col("_availability_count") == self._required_prices)
                    & pl.col("_factor_value").is_not_null()
                    & pl.col("_factor_value").is_finite()
                ).alias("_factor_valid")
            )
            .select(
                pl.col("trade_date"),
                pl.col("instrument_id"),
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(pl.col("_factor_valid"))
                .then(pl.col("_factor_value"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("value"),
                pl.col("_factor_available_at").alias("available_at"),
                pl.col("_factor_valid").alias("is_valid"),
            )
            .sort("trade_date", "instrument_id")
        )


class _MomentumSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _load_log_returns(
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        ctx: FactorContext,
        lookback_sessions: int,
    ) -> pl.DataFrame:
        bars = price_service.log_returns(
            instruments,
            ctx.start,
            ctx.end,
            lookback_sessions=lookback_sessions,
        ).collect()
        _MomentumSupport._validate_adjusted_bars(bars)
        normalized = bars.sort("instrument_id", "trade_date")
        if normalized.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate adjusted bar key")
        return normalized

    @staticmethod
    def _return_value(
        relative_log_path: Sequence[float], log_returns: Sequence[float]
    ) -> float | None:
        del log_returns
        return _MomentumSupport._finite_expm1(relative_log_path[-1])

    @staticmethod
    def _return_expression(window: int) -> Callable[[pl.Expr], pl.Expr]:
        def evaluate(log_returns: pl.Expr) -> pl.Expr:
            total = log_returns.rolling_sum(window, min_samples=window).over(
                "instrument_id"
            )
            return total.map_batches(
                np.expm1,
                return_dtype=pl.Float64,
                is_elementwise=True,
            )

        return evaluate

    @staticmethod
    def _trend_expression(log_returns: pl.Expr) -> pl.Expr:
        window_returns = 119
        weights = [
            index * (120 - index) / 2.0 for index in range(1, window_returns + 1)
        ]
        denominator = 120.0 * (120.0**2 - 1.0) / 12.0
        finite_return = (
            log_returns.is_not_null()
            & log_returns.is_not_nan()
            & log_returns.is_finite()
        )
        finite_count = (
            finite_return.cast(pl.UInt32)
            .rolling_sum(window_returns, min_samples=window_returns)
            .over("instrument_id")
        )
        clean_return = pl.when(finite_return).then(log_returns).otherwise(0.0)
        weighted = (
            clean_return.rolling_sum(
                window_returns,
                weights=weights,
                min_samples=window_returns,
            ).over("instrument_id")
            / denominator
        )
        return (
            pl.when(finite_count == window_returns)
            .then(weighted)
            .otherwise(pl.lit(None, dtype=pl.Float64))
        )

    @staticmethod
    def _momentum_120_20_value(
        relative_log_path: Sequence[float], log_returns: Sequence[float]
    ) -> float | None:
        del log_returns
        return _MomentumSupport._finite_expm1(relative_log_path[-21])

    @staticmethod
    def _momentum_120_20_expression(log_returns: pl.Expr) -> pl.Expr:
        shifted = log_returns.shift(20)
        finite = shifted.is_not_null() & shifted.is_not_nan() & shifted.is_finite()
        finite_count = (
            finite.cast(pl.UInt32)
            .rolling_sum(100, min_samples=100)
            .over("instrument_id")
        )
        total = (
            pl.when(finite)
            .then(shifted)
            .otherwise(0.0)
            .rolling_sum(100, min_samples=100)
            .over("instrument_id")
        )
        result = total.map_batches(
            np.expm1,
            return_dtype=pl.Float64,
            is_elementwise=True,
        )
        return (
            pl.when(finite_count == 100)
            .then(result)
            .otherwise(pl.lit(None, dtype=pl.Float64))
        )

    @staticmethod
    def _trend_value(
        relative_log_path: Sequence[float], log_returns: Sequence[float]
    ) -> float | None:
        del log_returns
        count = len(relative_log_path)
        mean_x = (count - 1) / 2.0
        mean_y = sum(relative_log_path) / count
        if not isfinite(mean_y):
            return None
        denominator = sum((index - mean_x) ** 2 for index in range(count))
        numerator = sum(
            (index - mean_x) * (value - mean_y)
            for index, value in enumerate(relative_log_path)
        )
        slope = numerator / denominator
        return slope if isfinite(slope) else None

    @staticmethod
    def _canonical_instrument_scope(
        instruments: Sequence[InstrumentId],
    ) -> tuple[InstrumentId, ...]:
        scope = tuple(instruments)
        if any(not isinstance(instrument, InstrumentId) for instrument in scope):
            raise TypeError("instruments must contain InstrumentId values")
        canonical = [instrument.canonical() for instrument in scope]
        if len(set(canonical)) != len(canonical):
            raise ValueError("instrument scope contains duplicates")
        return scope

    @staticmethod
    def _expanded_history_start(start: date, lookback_sessions: int) -> date:
        days = max(
            lookback_sessions * _HISTORY_CALENDAR_MULTIPLIER, lookback_sessions + 14
        )
        try:
            return start - timedelta(days=days)
        except OverflowError:
            return date.min

    @staticmethod
    def _needs_full_history(
        frame: pl.DataFrame, ctx: FactorContext, required_prices: int
    ) -> bool:
        insufficient = (
            frame.lazy()
            .with_columns(
                pl.int_range(1, pl.len() + 1, dtype=pl.UInt32)
                .over("instrument_id")
                .alias("_observed_prices")
            )
            .filter(pl.col("trade_date").is_between(ctx.start, ctx.end, closed="both"))
            .group_by("instrument_id")
            .agg(pl.col("_observed_prices").first())
            .select((pl.col("_observed_prices") < required_prices).any())
            .collect()
        )
        return bool(insufficient.item()) if insufficient.height else False

    @staticmethod
    def _validate_adjusted_bars(frame: pl.DataFrame) -> None:
        required = {
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "available_at": pl.Datetime("us", "UTC"),
            FORWARD_LOG_RETURN_COLUMN: pl.Float64,
        }
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"adjusted bars missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if frame.schema[column] != dtype:
                raise TypeError(f"adjusted bar {column} must have dtype {dtype}")

    @staticmethod
    def _finite_log_return(value: object) -> float | None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
        ):
            return None
        return float(value)

    @staticmethod
    def _relative_log_window(
        window_log_returns: Sequence[object],
    ) -> tuple[list[float], list[float]] | None:
        relative_log_path = [0.0]
        log_returns: list[float] = []
        cumulative = 0.0
        for raw_value in window_log_returns[1:]:
            value = _MomentumSupport._finite_log_return(raw_value)
            if value is None:
                return None
            log_returns.append(value)
            cumulative += value
            if not isfinite(cumulative):
                return None
            relative_log_path.append(cumulative)
        return relative_log_path, log_returns

    @staticmethod
    def _finite_expm1(value: float) -> float | None:
        try:
            result = expm1(value)
        except OverflowError:
            return None
        return result if isfinite(result) else None

    @staticmethod
    def _latest_available_at(
        values: Sequence[object],
    ) -> datetime | None:
        timestamps: list[datetime] = []
        for value in values:
            if value is None:
                return None
            if not isinstance(value, datetime):
                raise TypeError("adjusted bar available_at must be a datetime")
            timestamps.append(value)
        if not timestamps:
            return None
        return max(timestamps)

    @staticmethod
    def _empty_factor_output() -> pl.DataFrame:
        return pl.DataFrame(schema=FACTOR_OUTPUT_SCHEMA)


class ReturnFactor(_MarketFactor):
    """计算注册期限内 ETF 收盘到收盘的复权收益率。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        window：窗口。
        market_bars：市场数据行情。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Close-to-close return over one of the registered ETF horizons.
    """

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        window: int,
        *,
        market_bars: MarketBarsCache | None = None,
    ) -> None:
        if type(window) is not int or window not in _RETURN_WINDOWS:
            raise ValueError("window must be one of 20, 60, 120")
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id=f"return_{window}d",
                frequency="daily",
                lookback_sessions=window,
                dependencies=(),
                direction=1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "formula": ("expm1(forward_cumsum(forward_log_return[1:n+1])[-1])"),
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_basis": "exchange_sessions",
                    "suspension_return_policy": "explicit_placeholder_zero",
                    "missing_session_policy": "invalidate_window",
                    "window_sessions": window,
                },
            ),
            required_prices=window + 1,
            evaluator=_MomentumSupport._return_value,
            native_evaluator=_MomentumSupport._return_expression(window),
            market_bars=market_bars,
        )


class Trend120dFactor(_MarketFactor):
    """计算最近 120 个复权对数收盘价的尺度无关 OLS 趋势斜率。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        market_bars：市场数据行情。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Scale-invariant OLS slope of the latest 120 adjusted log closes.
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
                factor_id="trend_120d",
                frequency="daily",
                lookback_sessions=120,
                dependencies=(),
                direction=1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "formula": (
                        "ols_slope([0,forward_cumsum("
                        "forward_log_return[1:120])],x=0..119)"
                    ),
                    "include_intercept": True,
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
            evaluator=_MomentumSupport._trend_value,
            native_evaluator=_MomentumSupport._trend_expression,
            market_bars=market_bars,
        )


class Momentum12020Factor(_MarketFactor):
    """计算跳过最近 20 日的 120 日至 20 日复权动量收益。

    入参：
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        market_bars：市场数据行情。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Adjusted return from t-120 through t-20, skipping recent sessions.
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
                factor_id="momentum_120_20",
                frequency="daily",
                lookback_sessions=120,
                dependencies=(),
                direction=1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "eligible_for_alpha": True,
                    "formula": ("expm1(forward_cumsum(forward_log_return[1:101])[-1])"),
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_basis": "exchange_sessions",
                    "suspension_return_policy": "explicit_placeholder_zero",
                    "missing_session_policy": "invalidate_window",
                    "skip_recent_sessions": 20,
                    "window_prices": 121,
                },
            ),
            required_prices=121,
            evaluator=_MomentumSupport._momentum_120_20_value,
            native_evaluator=_MomentumSupport._momentum_120_20_expression,
            market_bars=market_bars,
        )
