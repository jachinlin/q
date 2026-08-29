"""提供基于 PIT 财务指标历史的内置质量与成长因子。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from threading import Lock
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import polars as pl

from quant_research.data.canonical.schemas import PolarsDataType
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec
from quant_research.factors.builtin._stock_common import (
    TradeCalendarProvider,
    canonical_scope,
    trading_signal_dates,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_FINANCIAL_STALENESS_CALENDAR_DAYS = 190


class FinancialProvider(TradeCalendarProvider, Protocol):
    """定义 PIT 财务因子所需的交易日历与完整修订历史端口。

    入参：
        无。
    返回值：
        由具体实现提供符合协议的数据端口。
    异常：
        由具体实现按接口契约定义。
    """

    def stock_financial_indicators(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取截止观察日可见且未折叠的财务指标修订历史。

        入参：
            as_of：PIT 查询截止日。
            instruments：参与计算的规范证券集合。
        返回值：
            未折叠修订历史的惰性数据表。
        异常：
            由具体数据端口按其契约定义。
        """
        ...


class FinancialIndicatorsCache:
    """按 ``FactorContext`` 共享交易日历和 PIT 财务修订历史。

    入参：
        provider：财务指标与交易日历数据端口。
        instruments：参与计算的规范证券集合。
        fields：同批因子需要的 Canonical 财务指标字段。
    返回值：
        可供多个财务因子共享的上下文缓存。
    异常：
        字段集合为空或重复时抛出 ``ValueError``。
    """

    def __init__(
        self,
        provider: FinancialProvider,
        instruments: Sequence[InstrumentId],
        fields: Sequence[str],
    ) -> None:
        if not fields or len(set(fields)) != len(fields):
            raise ValueError("financial metric fields must be nonempty and unique")
        self._provider = provider
        self._instruments = canonical_scope(instruments)
        self._fields = tuple(fields)
        self._ctx: FactorContext | None = None
        self._frame: pl.DataFrame | None = None
        self._lock = Lock()

    def load(self, ctx: FactorContext) -> pl.DataFrame:
        """读取并对齐当前上下文，重复调用复用同一不可变结果。

        入参：
            ctx：因子运行的精确 PIT 上下文。
        返回值：
            信号日、证券和最新可见财务指标组成的确定性数据表。
        异常：
            输入历史违反字段、类型或修订唯一性契约时抛出异常。
        """
        with self._lock:
            if self._ctx == ctx and self._frame is not None:
                return self._frame
            signal_dates = trading_signal_dates(self._provider, ctx.start, ctx.end)
            grid = _PitFinancialSupport.signal_grid(signal_dates, self._instruments)
            if grid.is_empty():
                aligned = _PitFinancialSupport.empty_aligned(self._fields)
            else:
                history = self._provider.stock_financial_indicators(
                    ctx.end, self._instruments
                ).collect()
                transitions = _PitFinancialSupport.transitions(history, self._fields)
                if transitions.is_empty():
                    aligned = grid.with_columns(
                        *(
                            pl.lit(None, dtype=pl.Float64).alias(field)
                            for field in self._fields
                        ),
                        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(
                            "_active_available_at"
                        ),
                    )
                else:
                    aligned = grid.sort("instrument_id", "_signal_at").join_asof(
                        transitions.sort("instrument_id", "_event_at"),
                        left_on="_signal_at",
                        right_on="_event_at",
                        by="instrument_id",
                        strategy="backward",
                        check_sortedness=False,
                    )
            normalized = aligned.sort("trade_date", "instrument_id")
            self._ctx = ctx
            self._frame = normalized
            return normalized


class FinancialMetricFactor:
    """从共享 PIT 对齐结果计算一个最新可见财务指标因子。

    入参：
        provider：财务指标数据端口。
        instruments：参与计算的规范证券集合。
        factor_id：简洁、唯一的因子标识。
        field：Canonical 财务指标源字段。
        direction：因子方向。
        value_domain：有效值约束。
        measurement：财务指标口径。
        cache：可选的共享 PIT 财务缓存。
    返回值：
        完成规格绑定的财务指标因子。
    异常：
        有效值约束不受支持时抛出 ``ValueError``。
    """

    def __init__(
        self,
        provider: FinancialProvider,
        instruments: Sequence[InstrumentId],
        *,
        factor_id: str,
        field: str,
        direction: int,
        value_domain: str,
        measurement: str,
        cache: FinancialIndicatorsCache | None = None,
    ) -> None:
        if value_domain not in {"signed_finite", "nonnegative_finite"}:
            raise ValueError("unsupported financial factor value domain")
        self._provider = provider
        self._instruments = canonical_scope(instruments)
        self._field = field
        self._value_domain = value_domain
        self._cache = cache or FinancialIndicatorsCache(
            provider, self._instruments, (field,)
        )
        self._spec = FactorSpec(
            factor_id,
            "daily",
            0,
            (),
            direction,
            {
                "source_field": field,
                "measurement": measurement,
                "selection": "latest_report_period_as_of_signal",
                "revision_policy": "latest_visible_revision",
                "staleness_age_basis": "active_record_available_at_shanghai_date",
                "staleness_calendar_days": _FINANCIAL_STALENESS_CALENDAR_DAYS,
                "value_domain": value_domain,
                "invalid_latest_record_fallback": False,
                "direction": direction,
                "eligible_for_alpha": True,
                "required_capabilities": ["financials_with_announcement_date"],
            },
        )

    @property
    def spec(self) -> FactorSpec:
        """返回财务指标因子的不可变规格。

        入参：
            无。
        返回值：
            因子频率、方向、源字段、PIT 选择和有效值约束。
        异常：
            无。
        """
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """按信号日计算最新可见且不超过 190 日的财务指标。

        入参：
            ctx：因子运行的精确 PIT 上下文。
        返回值：
            符合标准因子输出 Schema 的惰性数据表。
        异常：
            输入历史违反共享缓存契约时传播相应异常。
        """
        aligned = self._cache.load(ctx)
        active_date = (
            pl.col("_active_available_at")
            .dt.convert_time_zone("Asia/Shanghai")
            .dt.date()
        )
        age = (pl.col("trade_date") - active_date).dt.total_days()
        value = pl.col(self._field)
        valid = (
            value.is_not_null()
            & value.is_finite()
            & pl.col("_active_available_at").is_not_null()
            & age.is_between(
                0, _FINANCIAL_STALENESS_CALENDAR_DAYS, closed="both"
            )
        )
        if self._value_domain == "nonnegative_finite":
            valid &= value >= 0.0
        valid = valid.fill_null(False)
        return (
            aligned.lazy()
            .select(
                "trade_date",
                "instrument_id",
                pl.lit(self.spec.factor_id, dtype=pl.String).alias("factor_id"),
                pl.when(valid)
                .then(value)
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("value"),
                pl.col("_active_available_at").alias("available_at"),
                valid.alias("is_valid"),
            )
            .cast(FACTOR_OUTPUT_SCHEMA)
            .sort("trade_date", "instrument_id")
        )


class RoeFactor(FinancialMetricFactor):
    """计算最新可见、具有 190 日时效上限的 ROE 因子。

    入参：
        provider：财务指标数据端口。
        instruments：参与计算的规范证券集合。
        cache：可选的共享 PIT 财务缓存。
    返回值：
        完成 ROE 规格绑定的财务指标因子。
    异常：
        共享缓存或输入范围违反契约时传播相应异常。
    """

    def __init__(
        self,
        provider: FinancialProvider,
        instruments: Sequence[InstrumentId],
        *,
        cache: FinancialIndicatorsCache | None = None,
    ) -> None:
        super().__init__(
            provider,
            instruments,
            factor_id="roe",
            field="roe",
            direction=1,
            value_domain="signed_finite",
            measurement="point_in_time",
            cache=cache,
        )


class _PitFinancialSupport:
    """构建财务修订事件转换和信号日网格。"""

    @staticmethod
    def transitions(frame: pl.DataFrame, fields: Sequence[str]) -> pl.DataFrame:
        _PitFinancialSupport._validate_history(frame, fields)
        schema_fields: list[tuple[str, PolarsDataType]] = [
            ("instrument_id", pl.String),
            ("_event_at", pl.Datetime("us", "UTC")),
            *((field, pl.Float64) for field in fields),
            ("_active_available_at", pl.Datetime("us", "UTC")),
        ]
        schema = pl.Schema(schema_fields)
        if frame.is_empty():
            return pl.DataFrame(schema=schema)
        rows = frame.sort(
            "instrument_id", "available_at", "revision", "report_period"
        ).to_dicts()
        transitions: list[dict[str, object]] = []
        index = 0
        while index < len(rows):
            instrument = cast(str, rows[index]["instrument_id"])
            active_by_period: dict[date, dict[str, object]] = {}
            active_identity: tuple[date, int, datetime] | None = None
            while index < len(rows) and rows[index]["instrument_id"] == instrument:
                event_at = cast(datetime, rows[index]["available_at"])
                while (
                    index < len(rows)
                    and rows[index]["instrument_id"] == instrument
                    and rows[index]["available_at"] == event_at
                ):
                    event = rows[index]
                    active_by_period[cast(date, event["report_period"])] = event
                    index += 1
                latest_period = max(active_by_period)
                active = active_by_period[latest_period]
                identity = (
                    latest_period,
                    cast(int, active["revision"]),
                    cast(datetime, active["available_at"]),
                )
                if identity != active_identity:
                    transition: dict[str, object] = {
                        "instrument_id": instrument,
                        "_event_at": event_at,
                        "_active_available_at": cast(
                            datetime, active["available_at"]
                        ),
                    }
                    transition.update({field: active[field] for field in fields})
                    transitions.append(transition)
                    active_identity = identity
        return pl.DataFrame(transitions, schema=schema).sort(
            "instrument_id", "_event_at"
        )

    @staticmethod
    def signal_grid(
        signal_dates: Sequence[date], instruments: Sequence[InstrumentId]
    ) -> pl.DataFrame:
        signal_schema: dict[str, PolarsDataType] = {
            "trade_date": pl.Date,
            "_signal_at": pl.Datetime("us", "UTC"),
        }
        signals = pl.DataFrame(
            {
                "trade_date": signal_dates,
                "_signal_at": [
                    datetime.combine(day, time.max, tzinfo=_SHANGHAI).astimezone(UTC)
                    for day in signal_dates
                ],
            },
            schema=pl.Schema(signal_schema),
        )
        scope = pl.DataFrame(
            {"instrument_id": [item.canonical() for item in instruments]},
            schema={"instrument_id": pl.String},
        )
        return scope.join(signals, how="cross")

    @staticmethod
    def empty_aligned(fields: Sequence[str]) -> pl.DataFrame:
        """返回无信号日或无证券时的稳定内部 Schema。"""
        schema_fields: list[tuple[str, PolarsDataType]] = [
            ("instrument_id", pl.String),
            ("trade_date", pl.Date),
            ("_signal_at", pl.Datetime("us", "UTC")),
            *((field, pl.Float64) for field in fields),
            ("_active_available_at", pl.Datetime("us", "UTC")),
        ]
        return pl.DataFrame(schema=pl.Schema(schema_fields))

    @staticmethod
    def _validate_history(frame: pl.DataFrame, fields: Sequence[str]) -> None:
        required: dict[str, PolarsDataType] = {
            "instrument_id": pl.String,
            "report_period": pl.Date,
            "revision": pl.Int64,
            "available_at": pl.Datetime("us", "UTC"),
            **{field: pl.Float64 for field in fields},
        }
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"financial data missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if frame.schema[column] != dtype:
                raise TypeError(f"financial data {column} must have dtype {dtype}")
        if frame.select(
            pl.struct("instrument_id", "report_period", "revision")
            .is_duplicated()
            .any()
        ).item():
            raise ValueError("duplicate financial revision key")
        for value in frame["available_at"].to_list():
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TypeError("financial available_at must be timezone-aware")
        for field in fields:
            for value in frame[field].to_list():
                if value is not None and not isinstance(value, float):
                    raise TypeError(f"financial data {field} must contain Float64 values")
