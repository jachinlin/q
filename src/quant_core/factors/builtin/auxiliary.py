"""Auditable auxiliary factor fields that are ineligible for alpha composition."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from math import isfinite, log
from typing import Protocol

import polars as pl

from quant_core.data.adjustments import AdjustmentMode
from quant_core.data.contracts import JsonValue
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import (
    FactorContext,
    FactorSpec,
    is_available_on_signal_day,
)
from quant_core.factors.builtin._stock_common import (
    BarRepository,
    TradeCalendarProvider,
    _known_availability,
    canonical_scope,
    output_frame,
    trading_signal_dates,
)

_VERSION = "1.0.0"


class PitValueProvider(Protocol):
    """Return numeric values explicitly keyed by snapshot, signal date and security."""

    def values_as_of(
        self,
        snapshot_id: SnapshotId,
        as_of: date,
        instruments: Sequence[InstrumentId],
    ) -> pl.LazyFrame: ...


class RawBarService(Protocol):
    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame: ...


def _aux_spec(
    factor_id: str, lookback: int, parameters: dict[str, JsonValue]
) -> FactorSpec:
    return FactorSpec(
        factor_id,
        _VERSION,
        "daily",
        lookback,
        (),
        1,
        {**parameters, "role": "auxiliary", "eligible_for_alpha": False},
    )


def assert_alpha_eligible(specs: Sequence[FactorSpec]) -> None:
    """Fail closed before auxiliary identities can enter an alpha composite."""
    rejected = [
        spec.canonical_ref
        for spec in specs
        if spec.parameters.get("eligible_for_alpha") is False
    ]
    if rejected:
        raise ValueError(
            f"auxiliary factors are not eligible for alpha: {', '.join(rejected)}"
        )


class AvgAmount20dFactor:
    def __init__(
        self, repository: BarRepository, instruments: Sequence[InstrumentId]
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._spec = _aux_spec(
            "avg_amount_20d_v1", 19, {"source_field": "amount", "window_sessions": 20}
        )

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        from quant_core.factors.builtin.momentum import _expanded_history_start

        history_start = _expanded_history_start(ctx.start, 20)
        frame = self._load(ctx, history_start)
        if history_start != date.min and _needs_amount_full_history(frame, ctx, 20):
            frame = self._load(ctx, date.min)
        rows = []
        for group in frame.partition_by("instrument_id", maintain_order=True):
            data = group.to_dicts()
            for index, row in enumerate(data):
                day = row["trade_date"]
                if not ctx.start <= day <= ctx.end:
                    continue
                window = data[max(0, index - 19) : index + 1]
                amounts = [item["amount"] for item in window]
                availability = [item["available_at"] for item in window]
                valid = (
                    len(window) == 20
                    and all(_nonnegative(item) for item in amounts)
                    and all(_known_availability(item) for item in availability)
                )
                value = sum(float(item) for item in amounts) / 20.0 if valid else None
                available = max(availability) if valid else None
                rows.append((day, row["instrument_id"], value, available))
        return output_frame(self.spec, rows)

    def _load(self, ctx: FactorContext, start: date) -> pl.DataFrame:
        frame = self._repository.bars(
            ctx.snapshot_id, self._instruments, start, ctx.end
        ).collect()
        required = {"instrument_id", "trade_date", "amount", "available_at"}
        if not required.issubset(frame.columns):
            raise ValueError("amount bars missing required columns")
        if frame.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate amount bar key")
        return frame.sort("instrument_id", "trade_date")


class LogMarketCapFactor:
    def __init__(
        self,
        raw_service: BarRepository,
        instruments: Sequence[InstrumentId],
        shares_provider: PitValueProvider | None = None,
        *,
        calendar_provider: TradeCalendarProvider,
    ) -> None:
        self._service = raw_service
        self._instruments = canonical_scope(instruments)
        self._provider = shares_provider
        self._calendar = calendar_provider
        self._spec = _aux_spec(
            "log_market_cap_v1",
            0,
            {
                "formula": "log(raw_close*total_shares)",
                "capability": "pit_total_shares_provider",
                "missing_reason": "CAPABILITY_OR_DATA_UNAVAILABLE",
                "required_capabilities": ["pit_total_shares"],
            },
        )

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        bars = self._service.bars(
            ctx.snapshot_id, self._instruments, ctx.start, ctx.end
        ).collect()
        required = {"instrument_id", "trade_date", "close", "available_at"}
        if not required.issubset(bars.columns):
            raise ValueError("raw bars missing required columns")
        if bars.select(
            pl.struct("trade_date", "instrument_id").is_duplicated().any()
        ).item():
            raise ValueError("duplicate raw market-cap bar key")
        by_key = {
            (row["trade_date"], row["instrument_id"]): row for row in bars.to_dicts()
        }
        rows = []
        for day in trading_signal_dates(
            self._calendar, ctx.snapshot_id, ctx.start, ctx.end
        ):
            supplied = _provider_values(
                self._provider, ctx.snapshot_id, day, self._instruments
            )
            for instrument in self._instruments:
                identity = instrument.canonical()
                bar = by_key.get((day, identity))
                item = supplied.get(identity)
                value = None
                available = bar["available_at"] if bar else None
                if (
                    bar is not None
                    and item is not None
                    and _positive(bar["close"])
                    and _positive(item[0])
                    and _known_availability(bar["available_at"])
                ):
                    cap = float(bar["close"]) * item[0]
                    value = log(cap) if isfinite(cap) and cap > 0 else None
                    available = max(bar["available_at"], item[1])
                rows.append((day, identity, value, available))
        return output_frame(self.spec, rows)


class IndustryCodePitFactor:
    def __init__(
        self,
        instruments: Sequence[InstrumentId],
        provider: PitValueProvider | None = None,
        *,
        calendar_provider: TradeCalendarProvider,
    ) -> None:
        self._instruments = canonical_scope(instruments)
        self._provider = provider
        self._calendar = calendar_provider
        self._spec = _aux_spec(
            "industry_code_pit_v1",
            0,
            {
                "taxonomy": "provider_numeric_code",
                "capability": "pit_industry_provider",
                "missing_reason": "CAPABILITY_OR_DATA_UNAVAILABLE",
                "required_capabilities": ["pit_industry_classification"],
            },
        )

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        rows = []
        for day in trading_signal_dates(
            self._calendar, ctx.snapshot_id, ctx.start, ctx.end
        ):
            supplied = _provider_values(
                self._provider,
                ctx.snapshot_id,
                day,
                self._instruments,
                require_positive=False,
            )
            for instrument in self._instruments:
                item = supplied.get(instrument.canonical())
                rows.append(
                    (
                        day,
                        instrument.canonical(),
                        item[0] if item else None,
                        item[1] if item else None,
                    )
                )
        return output_frame(self.spec, rows)


def _provider_values(
    provider: PitValueProvider | None,
    snapshot_id: SnapshotId,
    day: date,
    instruments: Sequence[InstrumentId],
    *,
    require_positive: bool = True,
) -> dict[str, tuple[float, datetime]]:
    if provider is None:
        return {}
    frame = provider.values_as_of(snapshot_id, day, instruments).collect()
    required = {"instrument_id", "value", "available_at"}
    if not required.issubset(frame.columns):
        raise ValueError("PIT provider output missing required columns")
    if frame["instrument_id"].is_duplicated().any():
        raise ValueError("duplicate PIT provider instrument key")
    result = {}
    for row in frame.to_dicts():
        valid_value = (
            _positive(row["value"])
            if require_positive
            else _finite_number(row["value"])
        )
        if valid_value and is_available_on_signal_day(row["available_at"], day):
            result[row["instrument_id"]] = (float(row["value"]), row["available_at"])
    return result


def _needs_amount_full_history(
    frame: pl.DataFrame, ctx: FactorContext, required_observations: int
) -> bool:
    for group in frame.partition_by("instrument_id", maintain_order=True):
        dates = group["trade_date"].to_list()
        for index, trade_date in enumerate(dates):
            if ctx.start <= trade_date <= ctx.end:
                if index + 1 < required_observations:
                    return True
                break
    return False


def _positive(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(value)
        and value > 0
    )


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(value)
    )


def _nonnegative(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(value)
        and value >= 0
    )
