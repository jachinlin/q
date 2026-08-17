"""提供内置实现与质量因子相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import polars as pl

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec
from quant_research.factors.builtin._stock_common import (
    TradeCalendarProvider,
    canonical_scope,
    trading_signal_dates,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ROE_STALENESS_CALENDAR_DAYS = 190


class FinancialProvider(TradeCalendarProvider, Protocol):
    """定义 PIT 财务因子所需的日历与完整修订历史边界。

    入参：
        无。
    返回值：
        构造并返回 ``FinancialProvider`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def financial_history(
        self,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取截止研究时点可见且未折叠的全部财务修订。

        入参：
            field_ids：参与本次处理的字段``ids``；调用方不得依赖未声明的顺序。
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回``history``（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...


class RoePitFactor:
    """计算每个信号日已知、具有时效上限的最新杜邦 ROE。

    入参：
        provider：数据供应商。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    fields = ("dupont_roe",)

    def __init__(
        self, provider: FinancialProvider, instruments: Sequence[InstrumentId]
    ) -> None:
        self._provider = provider
        self._instruments = canonical_scope(instruments)
        self._spec = FactorSpec(
            "roe_pit",
            "daily",
            0,
            (),
            1,
            {
                "source_metric": "dupont_roe",
                "selection": "latest_report_period_as_of_signal",
                "staleness_age_basis": "active_record_available_at_shanghai_date",
                "staleness_calendar_days": _ROE_STALENESS_CALENDAR_DAYS,
                "eligible_for_alpha": True,
                "required_capabilities": ["financials_with_announcement_date"],
            },
        )

    @property
    def spec(self) -> FactorSpec:
        """处理因子计算中的不可变规格。

        入参：
            无。
        返回值：
            返回不可变规格（``FactorSpec``）。
        异常：
            无。
        """
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """通过一次财务历史读取计算完整日度 PIT ROE。

        入参：
            ctx：本次计算的上下文，类型为 ``FactorContext``。
        返回值：
            返回计算因子计算后的``compute``（``pl.LazyFrame``）。
        异常：
            无。
        """
        signal_dates = trading_signal_dates(self._provider, ctx.start, ctx.end)
        if not signal_dates or not self._instruments:
            return pl.DataFrame(schema=FACTOR_OUTPUT_SCHEMA).lazy()
        history = self._provider.financial_history(
            self.fields, ctx.end, self._instruments
        ).collect()
        transitions = _PitFinancialSupport.transitions(history)
        grid = _PitFinancialSupport.signal_grid(signal_dates, self._instruments)
        if transitions.is_empty():
            aligned = grid.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("_active_value"),
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
        active_date = (
            pl.col("_active_available_at")
            .dt.convert_time_zone("Asia/Shanghai")
            .dt.date()
        )
        age = (pl.col("trade_date") - active_date).dt.total_days()
        value = pl.col("_active_value")
        valid = (
            value.is_not_null()
            & value.is_finite()
            & pl.col("_active_available_at").is_not_null()
            & age.is_between(0, _ROE_STALENESS_CALENDAR_DAYS, closed="both")
        ).fill_null(False)
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


class _PitFinancialSupport:
    """Build event transitions and signal grids for PIT financial factors."""

    @staticmethod
    def transitions(frame: pl.DataFrame) -> pl.DataFrame:
        _PitFinancialSupport._validate_history(frame)
        schema = pl.Schema(
            [
                ("instrument_id", pl.String),
                ("_event_at", pl.Datetime("us", "UTC")),
                ("_active_value", pl.Float64),
                ("_active_available_at", pl.Datetime("us", "UTC")),
            ]
        )
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
                    raw_value = active["value"]
                    value = (
                        float(raw_value)
                        if isinstance(raw_value, (int, float))
                        and not isinstance(raw_value, bool)
                        else float("nan")
                    )
                    transitions.append(
                        {
                            "instrument_id": instrument,
                            "_event_at": event_at,
                            "_active_value": value,
                            "_active_available_at": cast(
                                datetime, active["available_at"]
                            ),
                        }
                    )
                    active_identity = identity
        return pl.DataFrame(transitions, schema=schema).sort(
            "instrument_id", "_event_at"
        )

    @staticmethod
    def signal_grid(
        signal_dates: Sequence[date], instruments: Sequence[InstrumentId]
    ) -> pl.DataFrame:
        signals = pl.DataFrame(
            {
                "trade_date": signal_dates,
                "_signal_at": [
                    datetime.combine(day, time.max, tzinfo=_SHANGHAI).astimezone(UTC)
                    for day in signal_dates
                ],
            },
            schema=pl.Schema(
                [
                    ("trade_date", pl.Date),
                    ("_signal_at", pl.Datetime("us", "UTC")),
                ]
            ),
        )
        scope = pl.DataFrame(
            {"instrument_id": [instrument.canonical() for instrument in instruments]},
            schema={"instrument_id": pl.String},
        )
        return scope.join(signals, how="cross")

    @staticmethod
    def _validate_history(frame: pl.DataFrame) -> None:
        required = {
            "instrument_id": pl.String,
            "report_period": pl.Date,
            "metric": pl.String,
            "value": pl.Float64,
            "revision": pl.Int64,
            "available_at": pl.Datetime("us", "UTC"),
        }
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"financial data missing columns: {', '.join(missing)}")
        for column, dtype in required.items():
            if frame.schema[column] != dtype:
                raise TypeError(f"financial data {column} must have dtype {dtype}")
        if frame.select(
            pl.struct("instrument_id", "report_period", "metric", "revision")
            .is_duplicated()
            .any()
        ).item():
            raise ValueError("duplicate financial revision key")
        if frame.filter(pl.col("metric") != "dupont_roe").height:
            raise ValueError("financial history contains an unexpected metric")
        for value in frame["available_at"].to_list():
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TypeError("financial available_at must be timezone-aware")
        for value in frame["value"].to_list():
            if value is not None and not isinstance(value, float):
                raise TypeError("financial value must be a Float64 value")
