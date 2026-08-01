"""Point-in-time financial quality factors."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from math import isfinite
from typing import Protocol

import polars as pl

from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import FactorContext, FactorSpec
from quant_core.factors.builtin._stock_common import (
    TradeCalendarProvider,
    canonical_scope,
    output_frame,
    trading_signal_dates,
)

_VERSION = "1.0.0"
type FinancialHistory = dict[date, dict[str, tuple[float, datetime]]]


class FinancialProvider(TradeCalendarProvider, Protocol):
    def financials_as_of(
        self,
        snapshot_id: SnapshotId,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame: ...


class _PitFinancialFactor:
    fields: tuple[str, ...]

    def __init__(
        self,
        provider: FinancialProvider,
        instruments: Sequence[InstrumentId],
        spec: FactorSpec,
    ) -> None:
        self._provider = provider
        self._instruments = canonical_scope(instruments)
        self._spec = spec

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        rows = []
        for signal_date in trading_signal_dates(
            self._provider, ctx.snapshot_id, ctx.start, ctx.end
        ):
            frame = self._provider.financials_as_of(
                ctx.snapshot_id, self.fields, signal_date, self._instruments
            ).collect()
            latest = _latest_by_report(frame)
            for instrument in self._instruments:
                value, available = self._evaluate(
                    latest.get(instrument.canonical(), {})
                )
                rows.append((signal_date, instrument.canonical(), value, available))
        return output_frame(self.spec, rows)

    def _evaluate(
        self, values: FinancialHistory
    ) -> tuple[float | None, datetime | None]:
        raise NotImplementedError


class RoeAvgPitFactor(_PitFinancialFactor):
    fields = ("roe_avg",)

    def __init__(
        self, provider: FinancialProvider, instruments: Sequence[InstrumentId]
    ) -> None:
        super().__init__(
            provider,
            instruments,
            FactorSpec(
                "roe_avg_pit_v1",
                _VERSION,
                "daily",
                0,
                (),
                1,
                {
                    "source_metric": "roe_avg",
                    "selection": "latest_report_period_as_of_signal",
                    "eligible_for_alpha": True,
                    "required_capabilities": ["financials_with_announcement_date"],
                },
            ),
        )

    def _evaluate(
        self, values: FinancialHistory
    ) -> tuple[float | None, datetime | None]:
        periods = [period for period, metrics in values.items() if "roe_avg" in metrics]
        if not periods:
            return None, None
        item = values[max(periods)]["roe_avg"]
        if not isfinite(item[0]):
            return None, None
        return item


class CfoToNetProfitFactor(_PitFinancialFactor):
    fields = ("operating_cash_flow", "net_profit")

    def __init__(
        self,
        provider: FinancialProvider,
        instruments: Sequence[InstrumentId],
        min_abs_net_profit: float = 1e-12,
    ) -> None:
        if (
            isinstance(min_abs_net_profit, bool)
            or not isinstance(min_abs_net_profit, (int, float))
            or not isfinite(min_abs_net_profit)
            or min_abs_net_profit <= 0
        ):
            raise ValueError("min_abs_net_profit must be finite positive")
        self._minimum = float(min_abs_net_profit)
        super().__init__(
            provider,
            instruments,
            FactorSpec(
                "cfo_to_np_pit_v1",
                _VERSION,
                "daily",
                0,
                (),
                1,
                {
                    "source_metrics": list(self.fields),
                    "same_report_period": True,
                    "min_abs_net_profit": self._minimum,
                    "eligible_for_alpha": True,
                    "required_capabilities": ["financials_with_announcement_date"],
                },
            ),
        )

    def _evaluate(
        self, values: FinancialHistory
    ) -> tuple[float | None, datetime | None]:
        periods = [
            period
            for period, metrics in values.items()
            if all(metric in metrics for metric in self.fields)
        ]
        if not periods:
            return None, None
        metrics = values[max(periods)]
        cfo, profit = metrics["operating_cash_flow"], metrics["net_profit"]
        if (
            not isfinite(cfo[0])
            or not isfinite(profit[0])
            or abs(profit[0]) < self._minimum
        ):
            return None, None
        return cfo[0] / profit[0], max(cfo[1], profit[1])


def _latest_by_report(
    frame: pl.DataFrame,
) -> dict[str, FinancialHistory]:
    required = {"instrument_id", "report_period", "metric", "value", "available_at"}
    if not required.issubset(frame.columns):
        raise ValueError("financial data missing required columns")
    if frame.select(
        pl.struct("instrument_id", "report_period", "metric").is_duplicated().any()
    ).item():
        raise ValueError("duplicate financial metric key")
    result: dict[str, FinancialHistory] = {}
    for row in frame.to_dicts():
        value = row["value"]
        period = row["report_period"]
        available_at = row["available_at"]
        if (
            isinstance(period, date)
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and isinstance(available_at, datetime)
            and available_at.tzinfo is not None
        ):
            result.setdefault(row["instrument_id"], {}).setdefault(period, {})[
                row["metric"]
            ] = (float(value), available_at)
    return result
