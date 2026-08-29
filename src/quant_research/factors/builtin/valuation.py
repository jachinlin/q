"""提供内置实现与估值因子相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Sequence
from threading import Lock

import polars as pl

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec
from quant_research.factors.builtin._stock_common import (
    BarRepository,
    canonical_scope,
)


class DailyBasicsCache:
    """在同一证券分区内共享不可变的每日估值输入。

    入参：
        repository：提供持久化访问的仓储，类型为 ``BarRepository``。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def __init__(
        self, repository: BarRepository, instruments: Sequence[InstrumentId]
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._ctx: FactorContext | None = None
        self._frame: pl.DataFrame | None = None
        self._lock = Lock()

    def load(self, ctx: FactorContext) -> pl.DataFrame:
        """读取或复用当前上下文的已校验 Daily Basics。

        入参：
            ctx：本次计算的上下文，类型为 ``FactorContext``。
        返回值：
            返回加载并校验因子计算后的``load``（``pl.DataFrame``）。
        异常：
            无。
        """
        with self._lock:
            if self._ctx == ctx and self._frame is not None:
                return self._frame
            frame = self._repository.stock_daily_basics(
                self._instruments, ctx.start, ctx.end
            ).collect()
            self._validate(frame)
            normalized = frame.sort("trade_date", "instrument_id")
            self._ctx = ctx
            self._frame = normalized
            return normalized

    @staticmethod
    def _validate(frame: pl.DataFrame) -> None:
        required = {
            "trade_date": pl.Date,
            "instrument_id": pl.String,
            "pe_ttm": pl.Float64,
            "pb": pl.Float64,
            "ps_ttm": pl.Float64,
            "dividend_yield_ttm": pl.Float64,
            "total_market_value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        }
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"valuation bars missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if frame.schema[column] != dtype:
                raise TypeError(f"valuation bar {column} must have dtype {dtype}")
        if frame.select(
            pl.struct("trade_date", "instrument_id").is_duplicated().any()
        ).item():
            raise ValueError("duplicate valuation bar key")


class _ReciprocalMultipleFactor:
    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        factor_id: str,
        field: str,
        daily_basics: DailyBasicsCache | None = None,
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._field = field
        self._daily_basics = daily_basics
        self._spec = FactorSpec(
            factor_id,
            "daily",
            0,
            (),
            1,
            {
                "source_field": field,
                "formula": f"1/{field}",
                "signed_denominator": True,
                "invalid_denominator": "zero_or_nonfinite",
                "value_domain": "signed_finite",
                "direction": 1,
                "eligible_for_alpha": True,
            },
        )

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        frame = (
            self._daily_basics.load(ctx)
            if self._daily_basics is not None
            else DailyBasicsCache(self._repository, self._instruments).load(ctx)
        )
        denominator = pl.col(self._field)
        known_on_day = pl.col("available_at").is_not_null() & (
            pl.col("available_at").dt.convert_time_zone("Asia/Shanghai").dt.date()
            <= pl.col("trade_date")
        )
        valid = (
            denominator.is_not_null()
            & denominator.is_finite()
            & (denominator != 0.0)
            & known_on_day
        ).fill_null(False)
        return (
            frame.lazy()
            .select(
                "trade_date",
                "instrument_id",
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(valid)
                .then(1.0 / denominator)
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("value"),
                "available_at",
                valid.alias("is_valid"),
            )
            .cast(FACTOR_OUTPUT_SCHEMA)
            .sort("trade_date", "instrument_id")
        )


class LogTotalMarketCapFactor:
    """以 PIT 可见总市值的自然对数计算股票市值因子。

    入参：
        repository：提供持久化访问的仓储，类型为 ``BarRepository``。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        daily_basics：日频 ``basics`` 共享缓存。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        daily_basics: DailyBasicsCache | None = None,
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._daily_basics = daily_basics
        self._spec = FactorSpec(
            factor_id="log_total_market_cap",
            frequency="daily",
            lookback_sessions=0,
            dependencies=(),
            direction=-1,
            parameters={
                "source_field": "total_market_value",
                "formula": "ln(total_market_value)",
                "positive_input_required": True,
                "eligible_for_alpha": True,
            },
        )

    @property
    def spec(self) -> FactorSpec:
        """返回市值因子的不可变计算规格。

        入参：无。
        返回值：因子频率、方向、公式和输入约束。
        异常：无。
        """
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """计算给定上下文内的对数总市值因子。

        入参：
            ctx：包含数据身份、股票池身份和计算日期区间的上下文。
        返回值：
            返回符合标准因子输出 Schema 的惰性数据表。
        异常：
            Daily Basics 输入违反字段、类型或唯一键契约时传播相应异常。
        """
        frame = (
            self._daily_basics.load(ctx)
            if self._daily_basics is not None
            else DailyBasicsCache(self._repository, self._instruments).load(ctx)
        )
        market_value = pl.col("total_market_value")
        known_on_day = pl.col("available_at").is_not_null() & (
            pl.col("available_at").dt.convert_time_zone("Asia/Shanghai").dt.date()
            <= pl.col("trade_date")
        )
        valid = (
            market_value.is_not_null()
            & market_value.is_finite()
            & (market_value > 0.0)
            & known_on_day
        ).fill_null(False)
        return (
            frame.lazy()
            .select(
                "trade_date",
                "instrument_id",
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(valid)
                .then(market_value.log())
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("value"),
                "available_at",
                valid.alias("is_valid"),
            )
            .cast(FACTOR_OUTPUT_SCHEMA)
            .sort("trade_date", "instrument_id")
        )


class EarningsYieldFactor(_ReciprocalMultipleFactor):
    """以 PIT 可见净利润和总市值计算股票盈利收益率。

    入参：
        repository：提供持久化访问的仓储，类型为 ``BarRepository``。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        daily_basics：日频``basics``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        daily_basics: DailyBasicsCache | None = None,
    ) -> None:
        super().__init__(
            repository,
            instruments,
            factor_id="earnings_yield_ttm",
            field="pe_ttm",
            daily_basics=daily_basics,
        )


class BookToPriceFactor(_ReciprocalMultipleFactor):
    """以 PIT 可见净资产和总市值计算股票账面市值比。

    入参：
        repository：提供持久化访问的仓储，类型为 ``BarRepository``。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        daily_basics：日频``basics``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        daily_basics: DailyBasicsCache | None = None,
    ) -> None:
        super().__init__(
            repository,
            instruments,
            factor_id="book_to_price_mrq",
            field="pb",
            daily_basics=daily_basics,
        )


class SalesYieldFactor(_ReciprocalMultipleFactor):
    """以 PIT 可见 TTM 市销率倒数计算销售收益率。

    入参：
        repository：提供 Daily Basic 数据的研究仓储。
        instruments：参与计算的规范证券集合。
        daily_basics：可选的共享 Daily Basic 缓存。
    返回值：
        完成销售收益率规格绑定的因子。
    异常：
        输入范围或共享缓存违反契约时传播相应异常。
    """

    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        daily_basics: DailyBasicsCache | None = None,
    ) -> None:
        super().__init__(
            repository,
            instruments,
            factor_id="sales_yield",
            field="ps_ttm",
            daily_basics=daily_basics,
        )
        self._spec = FactorSpec(
            factor_id="sales_yield",
            frequency="daily",
            lookback_sessions=0,
            dependencies=(),
            direction=1,
            parameters={
                "source_field": "ps_ttm",
                "formula": "1/ps_ttm",
                "measurement": "ttm",
                "signed_denominator": True,
                "invalid_denominator": "zero_or_nonfinite",
                "value_domain": "signed_finite",
                "direction": 1,
                "eligible_for_alpha": True,
            },
        )


class DividendYieldFactor:
    """直接使用 PIT 可见的 TTM 股息率计算股息收益率因子。

    入参：
        repository：提供 Daily Basic 数据的研究仓储。
        instruments：参与计算的规范证券集合。
        daily_basics：可选的共享 Daily Basic 缓存。
    返回值：
        完成股息收益率规格绑定的因子。
    异常：
        输入范围或共享缓存违反契约时传播相应异常。
    """

    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        daily_basics: DailyBasicsCache | None = None,
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._daily_basics = daily_basics
        self._spec = FactorSpec(
            factor_id="dividend_yield",
            frequency="daily",
            lookback_sessions=0,
            dependencies=(),
            direction=1,
            parameters={
                "source_field": "dividend_yield_ttm",
                "formula": "dividend_yield_ttm",
                "measurement": "ttm",
                "value_domain": "nonnegative_finite",
                "direction": 1,
                "eligible_for_alpha": True,
            },
        )

    @property
    def spec(self) -> FactorSpec:
        """返回股息收益率因子的不可变规格。

        入参：
            无。
        返回值：
            因子口径、方向、源字段和有效值约束。
        异常：
            无。
        """
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """计算给定上下文内的 TTM 股息收益率。

        入参：
            ctx：因子运行的精确 PIT 上下文。
        返回值：
            符合标准因子输出 Schema 的惰性数据表。
        异常：
            Daily Basics 输入违反缓存契约时传播相应异常。
        """
        frame = (
            self._daily_basics.load(ctx)
            if self._daily_basics is not None
            else DailyBasicsCache(self._repository, self._instruments).load(ctx)
        )
        value = pl.col("dividend_yield_ttm")
        known_on_day = pl.col("available_at").is_not_null() & (
            pl.col("available_at").dt.convert_time_zone("Asia/Shanghai").dt.date()
            <= pl.col("trade_date")
        )
        valid = (
            value.is_not_null()
            & value.is_finite()
            & (value >= 0.0)
            & known_on_day
        ).fill_null(False)
        return (
            frame.lazy()
            .select(
                "trade_date",
                "instrument_id",
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(valid)
                .then(value)
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("value"),
                "available_at",
                valid.alias("is_valid"),
            )
            .cast(FACTOR_OUTPUT_SCHEMA)
            .sort("trade_date", "instrument_id")
        )
