"""提供基于当前 Canonical 数据、满足时点约束的价格复权服务。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from enum import StrEnum
from math import isfinite
from typing import Protocol, cast

import polars as pl

from quant_research.domain.identifiers import InstrumentId

FORWARD_LOG_RETURN_COLUMN = "forward_log_return"
FORWARD_RETURN_INDEX_COLUMN = "forward_return_index"
FACTOR_LOG_RETURN_SCHEMA = pl.Schema(
    [
        ("trade_date", pl.Date),
        ("instrument_id", pl.String),
        (FORWARD_LOG_RETURN_COLUMN, pl.Float64),
        ("available_at", pl.Datetime("us", "UTC")),
    ]
)
ADJUSTMENT_EVENT_COMPONENTS_DTYPE = pl.List(
    pl.Struct(
        {
            "action_type": pl.String,
            "cash_per_share": pl.Float64,
            "share_ratio": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        }
    )
)


class AdjustmentMode(StrEnum):
    """枚举研究行情支持的价格表示模式。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``AdjustmentMode`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    RAW = "RAW"
    FORWARD = "FORWARD"


class _PriceDataReader(Protocol):
    """约束私有复权引擎所需的最小研究数据读取能力。"""

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame: ...


class _PriceAdjustmentEngine:
    """在 Repository 内部生成前复权行情和会话对数收益。"""

    def __init__(
        self,
        repository: _PriceDataReader,
        bar_reader: Callable[
            [Sequence[InstrumentId], date, date], pl.LazyFrame
        ],
        factor_reader: Callable[
            [Sequence[InstrumentId], date, date], pl.LazyFrame
        ],
    ) -> None:
        self._repository = repository
        self._bar_reader = bar_reader
        self._factor_reader = factor_reader

    def adjusted_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """返回以 ``end`` 为信息截止日的前复权行情。"""
        _PriceAdjustmentSupport._validate_range(start, end)
        raw = self._bar_reader(instruments, start, end).collect()
        factors = self._factor_reader(instruments, start, end).collect()
        adjusted, price_factors = _PriceAdjustmentSupport._factor_adjust(
            self._without_untraded_placeholders(raw), factors, end
        )
        if adjusted.is_empty():
            return _PriceAdjustmentSupport._with_metadata(adjusted, end).lazy()
        _PriceAdjustmentSupport._validate_forward_prices(adjusted, adjusted=True)
        return _PriceAdjustmentSupport._with_metadata(
            adjusted,
            end,
            adjustment_factors=price_factors,
        ).lazy()

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """返回按交易会话补齐的前复权对数收益。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
            lookback_sessions：回看窗口交易会话集合。
        返回值：
            返回收益序列（``pl.LazyFrame``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        _PriceAdjustmentSupport._validate_range(start, end)
        if type(lookback_sessions) is not int or lookback_sessions < 0:
            raise ValueError("lookback_sessions must be a nonnegative integer")
        scope = tuple(instruments)
        if any(not isinstance(instrument, InstrumentId) for instrument in scope):
            raise TypeError("instruments must contain InstrumentId values")
        instrument_ids = [instrument.canonical() for instrument in scope]
        if len(set(instrument_ids)) != len(instrument_ids):
            raise ValueError("instrument scope contains duplicates")
        if not instrument_ids:
            return pl.DataFrame(schema=FACTOR_LOG_RETURN_SCHEMA).lazy()

        sessions = _PriceAdjustmentSupport._factor_sessions(
            self._repository.trade_calendar(date.min, end).collect(),
            start,
            end,
            lookback_sessions,
        )
        if not sessions:
            return pl.DataFrame(schema=FACTOR_LOG_RETURN_SCHEMA).lazy()
        history_start = sessions[0]
        raw = self._bar_reader(scope, history_start, end).collect()
        factors = self._factor_reader(scope, history_start, end).collect()
        _PriceAdjustmentSupport._validate_session_bar_keys(raw)
        observed = raw.filter(pl.col("trade_date") <= end)
        traded = self._without_untraded_placeholders(raw)
        adjusted, _ = _PriceAdjustmentSupport._factor_adjust(traded, factors, end)
        return _PriceAdjustmentSupport._session_complete_returns(
            instrument_ids,
            sessions,
            observed,
            adjusted,
        ).lazy()

    @staticmethod
    def _without_untraded_placeholders(frame: pl.DataFrame) -> pl.DataFrame:
        """在价格链计算前移除供应商生成的合法停牌占位行。"""
        if not {"volume", "amount"}.issubset(frame.columns):
            raise ValueError("daily bars are missing trading activity columns")
        return frame.filter(~_PriceAdjustmentSupport._is_untraded_placeholder())


class _PriceAdjustmentSupport:
    """集中承载价格复权服务内部、无独立公开语义的计算逻辑。"""

    @staticmethod
    def _validate_range(start: date, end: date) -> None:
        if start > end:
            raise ValueError("start must not follow end")

    @staticmethod
    def _factor_sessions(
        calendar: pl.DataFrame,
        start: date,
        end: date,
        lookback_sessions: int,
    ) -> tuple[date, ...]:
        required = {"trade_date": pl.Date, "is_trading_day": pl.Boolean}
        missing = sorted(set(required) - set(calendar.columns))
        if missing:
            raise ValueError(f"trade calendar missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if calendar.schema[column] != dtype:
                raise TypeError(f"trade calendar {column} must have dtype {dtype}")
        if calendar["trade_date"].is_duplicated().any():
            raise ValueError("duplicate trade calendar date")
        trading = calendar.filter(
            pl.col("is_trading_day") & (pl.col("trade_date") <= end)
        ).sort("trade_date")
        all_sessions = cast(list[date], trading["trade_date"].to_list())
        output_start = next(
            (index for index, session in enumerate(all_sessions) if session >= start),
            len(all_sessions),
        )
        first = max(0, output_start - lookback_sessions)
        return tuple(all_sessions[first:])

    @staticmethod
    def _validate_session_bar_keys(frame: pl.DataFrame) -> None:
        required = {
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "volume": pl.Int64,
            "amount": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        }
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"daily bars missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if frame.schema[column] != dtype:
                raise TypeError(f"daily bar {column} must have dtype {dtype}")
        _PriceAdjustmentSupport._validate_unique_bar_keys(frame)

    @staticmethod
    def _session_complete_returns(
        instrument_ids: Sequence[str],
        sessions: Sequence[date],
        raw: pl.DataFrame,
        adjusted: pl.DataFrame,
    ) -> pl.DataFrame:
        grid = pl.DataFrame(
            {"instrument_id": instrument_ids}, schema={"instrument_id": pl.String}
        ).join(
            pl.DataFrame({"trade_date": sessions}, schema={"trade_date": pl.Date}),
            how="cross",
        )
        observations = raw.select(
            "instrument_id",
            "trade_date",
            pl.lit(True).alias("_has_observation"),
            _PriceAdjustmentSupport._is_untraded_placeholder().alias(
                "_is_suspended"
            ),
            pl.col("available_at").alias("_raw_available_at"),
        )
        returns = adjusted.select(
            "instrument_id",
            "trade_date",
            pl.col(FORWARD_LOG_RETURN_COLUMN).alias("_traded_log_return"),
        )
        return (
            grid.join(
                observations,
                on=["instrument_id", "trade_date"],
                how="left",
                validate="1:1",
            )
            .join(
                returns,
                on=["instrument_id", "trade_date"],
                how="left",
                validate="1:1",
            )
            .select(
                "trade_date",
                "instrument_id",
                pl.when(pl.col("_has_observation").fill_null(False))
                .then(
                    pl.when(pl.col("_is_suspended"))
                    .then(0.0)
                    .otherwise(pl.col("_traded_log_return"))
                )
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias(FORWARD_LOG_RETURN_COLUMN),
                pl.when(pl.col("_has_observation").fill_null(False))
                .then(pl.col("_raw_available_at"))
                .otherwise(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
                .alias("available_at"),
            )
            .cast(FACTOR_LOG_RETURN_SCHEMA)
            .sort("instrument_id", "trade_date")
        )

    @staticmethod
    def _is_untraded_placeholder() -> pl.Expr:
        """返回与 Canonical 行情质量规则一致的零成交占位条件。"""
        return (pl.col("volume").fill_null(0) == 0) & (
            pl.col("amount").fill_null(0.0) == 0.0
        )

    @staticmethod
    def _required_positive(value: object, field: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{field} must be finite and positive")
        return float(value)

    @staticmethod
    def _validate_unique_bar_keys(frame: pl.DataFrame) -> None:
        if frame.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate daily bar key")

    @staticmethod
    def _factor_adjust(
        frame: pl.DataFrame, factors: pl.DataFrame, end: date
    ) -> tuple[pl.DataFrame, list[float]]:
        """按 Tushare 复权因子生成以截止日归一化的前复权价格。"""
        ordered = frame.sort("instrument_id", "trade_date")
        _PriceAdjustmentSupport._validate_unique_bar_keys(ordered)
        _PriceAdjustmentSupport._validate_forward_prices(ordered)
        required = {"instrument_id", "trade_date", "adjustment_factor"}
        missing = sorted(required - set(factors.columns))
        if missing:
            raise ValueError(f"adjustment factors missing columns: {', '.join(missing)}")
        if factors.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate adjustment factor key")
        if ordered.is_empty():
            return (
                ordered.with_columns(
                    pl.Series(FORWARD_LOG_RETURN_COLUMN, [], dtype=pl.Float64),
                    pl.Series(FORWARD_RETURN_INDEX_COLUMN, [], dtype=pl.Float64),
                ),
                [],
            )
        normalized_factors = factors.sort("instrument_id", "trade_date").with_columns(
            pl.col("adjustment_factor")
            .last()
            .over("instrument_id")
            .alias("_end_factor"),
        )
        factor = pl.col("adjustment_factor")
        if _PriceAdjustmentSupport._has_any(
            normalized_factors,
            factor.is_null() | ~factor.is_finite() | (factor <= 0),
        ):
            raise ValueError("adjustment factor must be finite and positive")
        calculated = (
            ordered.join_asof(
                normalized_factors.select(
                    "instrument_id",
                    "trade_date",
                    "adjustment_factor",
                    "_end_factor",
                ),
                on="trade_date",
                by="instrument_id",
                strategy="backward",
                check_sortedness=False,
            )
            .join_asof(
                normalized_factors.select(
                    "instrument_id",
                    "trade_date",
                    pl.col("adjustment_factor").alias("_previous_factor"),
                ),
                on="trade_date",
                by="instrument_id",
                strategy="backward",
                allow_exact_matches=False,
                check_sortedness=False,
            )
            .with_columns(
                pl.col("_previous_factor").fill_null(pl.col("adjustment_factor"))
            )
        )
        invalid = pl.col("adjustment_factor").is_null() | ~pl.col(
            "adjustment_factor"
        ).is_finite() | (pl.col("adjustment_factor") <= 0)
        if _PriceAdjustmentSupport._has_any(calculated, invalid):
            raise ValueError("adjustment factor must be finite and positive")
        calculated = calculated.with_columns(
            *[
                (
                    pl.col(column)
                    * pl.col("adjustment_factor")
                    / pl.col("_end_factor")
                ).alias(column)
                for column in ("open", "high", "low", "close")
            ],
            (
                pl.col("preclose")
                * pl.col("_previous_factor")
                / pl.col("_end_factor")
            ).alias("preclose"),
            (pl.col("adjustment_factor") / pl.col("_end_factor")).alias(
                "_price_factor"
            ),
            (pl.col("adjustment_factor") / pl.col("_previous_factor")).alias(
                "_event_factor"
            ),
        ).with_columns(
            (pl.col("close").log() - pl.col("preclose").log())
            .cast(pl.Float64)
            .alias(FORWARD_LOG_RETURN_COLUMN),
            (pl.col("close") / pl.col("preclose") - 1.0)
            .cast(pl.Float64)
            .alias("pct_change"),
            (pl.col("close") - pl.col("preclose")).cast(pl.Float64).alias("change"),
            pl.col("close").alias(FORWARD_RETURN_INDEX_COLUMN),
        )
        filtered = calculated.filter(pl.col("trade_date") <= end)
        price_factors = cast(list[float], filtered["_price_factor"].to_list())
        return (
            filtered.drop(
                "_end_factor", "_previous_factor", "_price_factor", "_event_factor"
            ),
            price_factors,
        )

    @staticmethod
    def _has_any(frame: pl.DataFrame, expression: pl.Expr) -> bool:
        return bool(frame.select(expression.any()).item())

    @staticmethod
    def _validate_forward_prices(
        frame: pl.DataFrame, *, adjusted: bool = False
    ) -> None:
        prefix = "adjusted " if adjusted else ""
        close = pl.col("close")
        if _PriceAdjustmentSupport._has_any(
            frame, close.is_null() | ~close.is_finite() | (close <= 0)
        ):
            raise ValueError(f"{prefix}close must be finite and positive")

        first = pl.col("instrument_id").is_first_distinct()
        preclose = pl.col("preclose")
        valid_first_preclose = preclose.is_null() | (
            preclose.is_finite() & (preclose >= 0)
        )
        invalid_later_preclose = (
            preclose.is_null() | ~preclose.is_finite() | (preclose <= 0)
        )
        if _PriceAdjustmentSupport._has_any(
            frame,
            pl.when(first)
            .then(~valid_first_preclose)
            .otherwise(invalid_later_preclose),
        ):
            if adjusted:
                raise ValueError(
                    "adjusted preclose must be null or nonnegative on the first "
                    "session and finite and positive thereafter"
                )
            raise ValueError(
                "first preclose must be null, zero, or finite and positive; "
                "later preclose must be finite and positive"
            )

        for column in ("open", "high", "low"):
            value = pl.col(column)
            if _PriceAdjustmentSupport._has_any(
                frame,
                value.is_not_null() & (~value.is_finite() | (value <= 0)),
            ):
                raise ValueError(
                    f"{prefix}{column} must be finite and positive when non-null"
                )

    @staticmethod
    def _with_metadata(
        frame: pl.DataFrame,
        as_of: date,
        *,
        adjustment_factors: Sequence[float] | None = None,
    ) -> pl.DataFrame:
        factors = (
            list(adjustment_factors)
            if adjustment_factors is not None
            else [1.0] * frame.height
        )
        if len(factors) != frame.height:
            raise ValueError("adjustment factor count must match bar rows")
        normalized = frame.with_columns(
            pl.Series("adjustment_factor", factors, dtype=pl.Float64)
        ).sort("instrument_id", "trade_date")
        return normalized.with_columns(
            pl.lit(AdjustmentMode.FORWARD.value, dtype=pl.String).alias(
                "adjustment_mode"
            ),
            pl.lit(as_of, dtype=pl.Date).alias("adjustment_as_of"),
            pl.lit(1.0, dtype=pl.Float64).alias("adjustment_event_factor"),
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(
                "adjustment_event_available_at"
            ),
            pl.lit([], dtype=ADJUSTMENT_EVENT_COMPONENTS_DTYPE).alias(
                "adjustment_event_components"
            ),
        )
