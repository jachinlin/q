"""提供基于 Daily Basic 历史窗口的换手率 Alpha 因子。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

import polars as pl

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec
from quant_research.factors.builtin._stock_common import BarRepository, canonical_scope

_WINDOW = 20


class Turnover20dFactor:
    """计算最近 20 个 Daily Basic 观测的自由流通换手率均值。

    入参：
        repository：提供 Daily Basic 历史的研究仓储。
        instruments：参与计算的规范证券集合。
    返回值：
        20 日自由流通换手率 Alpha 因子实现。
    异常：
        输入字段、类型或唯一键违反契约时抛出 ``TypeError``、``ValueError``。
    """

    def __init__(
        self, repository: BarRepository, instruments: Sequence[InstrumentId]
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._spec = FactorSpec(
            factor_id="turnover_20d",
            frequency="daily",
            lookback_sessions=_WINDOW - 1,
            dependencies=(),
            direction=-1,
            parameters={
                "source_field": "turnover_rate_free_float",
                "formula": "mean(turnover_rate_free_float)",
                "window_observations": _WINDOW,
                "window_basis": "observed_daily_basic_rows",
                "value_domain": "nonnegative_finite",
                "full_window_required": True,
                "direction": -1,
                "eligible_for_alpha": True,
            },
        )

    @property
    def spec(self) -> FactorSpec:
        """返回换手率因子的不可变规格。

        入参：
            无。
        返回值：
            因子窗口、方向、源字段和有效值约束。
        异常：
            无。
        """
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """计算给定上下文内的 20 个观测滚动换手率。

        入参：
            ctx：因子运行的精确 PIT 上下文。
        返回值：
            符合标准因子输出 Schema 的惰性数据表。
        异常：
            Daily Basic 历史违反输入契约时传播相应异常。
        """
        from quant_research.factors.builtin.momentum import _MomentumSupport

        history_start = _MomentumSupport._expanded_history_start(ctx.start, _WINDOW)
        frame = self._load(ctx, history_start)
        if history_start != date.min and self._needs_full_history(frame, ctx):
            frame = self._load(ctx, date.min)
        turnover = pl.col("turnover_rate_free_float")
        acceptable = (
            turnover.is_not_null() & turnover.is_finite() & (turnover >= 0.0)
        ).fill_null(False)
        acceptable_count = (
            acceptable.cast(pl.UInt32)
            .rolling_sum(_WINDOW, min_samples=_WINDOW)
            .over("instrument_id")
        )
        availability_count = (
            pl.col("available_at")
            .is_not_null()
            .cast(pl.UInt32)
            .rolling_sum(_WINDOW, min_samples=_WINDOW)
            .over("instrument_id")
        )
        mean_turnover = (
            pl.when(acceptable)
            .then(turnover)
            .otherwise(0.0)
            .rolling_mean(_WINDOW, min_samples=_WINDOW)
            .over("instrument_id")
        )
        latest_availability = (
            pl.col("available_at")
            .rolling_max(_WINDOW, min_samples=_WINDOW)
            .over("instrument_id")
        )
        return (
            frame.lazy()
            .with_columns(
                acceptable_count.alias("_acceptable_count"),
                availability_count.alias("_availability_count"),
                mean_turnover.alias("_mean_turnover"),
                latest_availability.alias("_latest_availability"),
            )
            .with_columns(
                (
                    (pl.col("_acceptable_count") == _WINDOW)
                    & (pl.col("_availability_count") == _WINDOW)
                    & pl.col("_mean_turnover").is_not_null()
                    & pl.col("_mean_turnover").is_finite()
                    & pl.col("_latest_availability").is_not_null()
                    & (
                        pl.col("_latest_availability")
                        .dt.convert_time_zone("Asia/Shanghai")
                        .dt.date()
                        <= pl.col("trade_date")
                    )
                )
                .fill_null(False)
                .alias("_factor_valid")
            )
            .filter(pl.col("trade_date").is_between(ctx.start, ctx.end, closed="both"))
            .select(
                "trade_date",
                "instrument_id",
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(pl.col("_factor_valid"))
                .then(pl.col("_mean_turnover"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("value"),
                pl.when(pl.col("_availability_count") == _WINDOW)
                .then(pl.col("_latest_availability"))
                .otherwise(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
                .alias("available_at"),
                pl.col("_factor_valid").alias("is_valid"),
            )
            .cast(FACTOR_OUTPUT_SCHEMA)
            .sort("trade_date", "instrument_id")
        )

    def _load(self, ctx: FactorContext, start: date) -> pl.DataFrame:
        frame = self._repository.stock_daily_basics(
            self._instruments, start, ctx.end
        ).collect()
        required = {
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "turnover_rate_free_float": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        }
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"turnover data missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if frame.schema[column] != dtype:
                raise TypeError(f"turnover data {column} must have dtype {dtype}")
        if frame.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate turnover data key")
        for value in frame["available_at"].to_list():
            if value is not None and (
                not isinstance(value, datetime) or value.tzinfo is None
            ):
                raise TypeError("turnover available_at must be timezone-aware")
        return frame.sort("instrument_id", "trade_date")

    @staticmethod
    def _needs_full_history(frame: pl.DataFrame, ctx: FactorContext) -> bool:
        in_scope = (
            frame.lazy()
            .with_columns(
                pl.int_range(1, pl.len() + 1, dtype=pl.UInt32)
                .over("instrument_id")
                .alias("_observed")
            )
            .filter(pl.col("trade_date").is_between(ctx.start, ctx.end, closed="both"))
            .group_by("instrument_id")
            .agg(pl.col("_observed").first())
            .select((pl.col("_observed") < _WINDOW).any())
            .collect()
        )
        return bool(in_scope.item()) if in_scope.height else False
