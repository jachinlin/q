"""Backward-adjusted ETF return and log-price trend factors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from math import isclose, isfinite, log
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import polars as pl

from quant_core.data.adjustments import AdjustmentMode
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec

_VERSION = "1.0.0"
_RETURN_WINDOWS = frozenset({20, 60, 120})
_HISTORY_CALENDAR_MULTIPLIER = 3
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class AdjustedBarService(Protocol):
    """Minimum price boundary required by the built-in market factors."""

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame: ...


class _MarketFactor:
    """Shared deterministic window execution over adjusted observed sessions."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        spec: FactorSpec,
        required_prices: int,
        evaluator: Callable[[Sequence[float]], float | None],
    ) -> None:
        self._price_service = price_service
        self._instruments = _canonical_instrument_scope(instruments)
        self._spec = spec
        self._required_prices = required_prices
        self._evaluator = evaluator

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """Compute rows inside ``ctx`` using only backward-adjusted prior data."""
        history_start = _expanded_history_start(ctx.start, self.spec.lookback_sessions)
        normalized = self._load_bars(ctx, history_start)
        if history_start != date.min and _needs_full_history(
            normalized, ctx, self._required_prices
        ):
            normalized = self._load_bars(ctx, date.min)
        if normalized.is_empty():
            return _empty_factor_output().lazy()

        output: list[dict[str, object]] = []
        for instrument_frame in normalized.partition_by(
            "instrument_id", maintain_order=True
        ):
            rows = instrument_frame.select(
                "trade_date",
                "instrument_id",
                "close",
                "available_at",
                "adjustment_factor",
                "adjustment_event_factor",
                "adjustment_event_available_at",
            ).to_dicts()
            for index, row in enumerate(rows):
                trade_date = row["trade_date"]
                if not isinstance(trade_date, date):
                    raise TypeError("adjusted bar trade_date must be a date")
                if trade_date < ctx.start or trade_date > ctx.end:
                    continue
                start_index = index - self._required_prices + 1
                window = rows[max(0, start_index) : index + 1]
                bar_available_at = _latest_available_at(window)
                available_at = bar_available_at
                value: float | None = None
                if start_index >= 0 and len(window) == self._required_prices:
                    closes, action_available_at = _point_in_time_closes(
                        window, trade_date
                    )
                    available_at = _max_available_at(
                        bar_available_at, action_available_at
                    )
                    if closes is not None and available_at is not None:
                        value = self._evaluator(closes)
                        if value is not None and not isfinite(value):
                            value = None
                output.append(
                    {
                        "trade_date": trade_date,
                        "instrument_id": row["instrument_id"],
                        "factor_id": self.spec.factor_id,
                        "factor_version": self.spec.version,
                        "value": value,
                        "available_at": available_at,
                        "is_valid": value is not None,
                    }
                )
        return (
            pl.DataFrame(output, schema=FACTOR_OUTPUT_SCHEMA)
            .sort("trade_date", "instrument_id")
            .lazy()
        )

    def _load_bars(self, ctx: FactorContext, start: date) -> pl.DataFrame:
        bars = self._price_service.bars(
            ctx.snapshot_id,
            self._instruments,
            start,
            ctx.end,
            AdjustmentMode.BACKWARD,
            ctx.end,
        ).collect()
        _validate_adjusted_bars(bars)
        normalized = bars.sort("instrument_id", "trade_date")
        if normalized.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate adjusted bar key")
        return normalized


class ReturnFactor(_MarketFactor):
    """Close-to-close return over one of the registered ETF horizons."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        window: int,
    ) -> None:
        if type(window) is not int or window not in _RETURN_WINDOWS:
            raise ValueError("window must be one of 20, 60, 120")
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id=f"return_{window}d_v1",
                version=_VERSION,
                frequency="daily",
                lookback_sessions=window,
                dependencies=(),
                direction=1,
                parameters={
                    "adjustment_mode": AdjustmentMode.BACKWARD.value,
                    "formula": "close[t]/close[t-n]-1",
                    "price_field": "close",
                    "window_sessions": window,
                },
            ),
            required_prices=window + 1,
            evaluator=_return_value,
        )


class Trend120dFactor(_MarketFactor):
    """Normalized OLS slope of the latest 120 adjusted log closes."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="trend_120d_v1",
                version=_VERSION,
                frequency="daily",
                lookback_sessions=120,
                dependencies=(),
                direction=1,
                parameters={
                    "adjustment_mode": AdjustmentMode.BACKWARD.value,
                    "formula": "ols_slope(log(close),x=0..119)/mean(log(close))",
                    "include_intercept": True,
                    "price_field": "close",
                    "window_prices": 120,
                },
            ),
            required_prices=120,
            evaluator=_trend_value,
        )


def _return_value(closes: Sequence[float]) -> float | None:
    value = closes[-1] / closes[0] - 1.0
    return value if isfinite(value) else None


def _trend_value(closes: Sequence[float]) -> float | None:
    log_prices = [log(value) for value in closes]
    count = len(log_prices)
    mean_x = (count - 1) / 2.0
    mean_y = sum(log_prices) / count
    scale = max(1.0, *(abs(value) for value in log_prices))
    if not isfinite(mean_y) or isclose(mean_y, 0.0, rel_tol=0.0, abs_tol=1e-15 * scale):
        return None
    denominator = sum((index - mean_x) ** 2 for index in range(count))
    numerator = sum(
        (index - mean_x) * (value - mean_y) for index, value in enumerate(log_prices)
    )
    slope = numerator / denominator
    normalized = slope / mean_y
    return normalized if isfinite(normalized) else None


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


def _expanded_history_start(start: date, lookback_sessions: int) -> date:
    days = max(lookback_sessions * _HISTORY_CALENDAR_MULTIPLIER, lookback_sessions + 14)
    try:
        return start - timedelta(days=days)
    except OverflowError:
        return date.min


def _needs_full_history(
    frame: pl.DataFrame, ctx: FactorContext, required_prices: int
) -> bool:
    for instrument_frame in frame.partition_by("instrument_id", maintain_order=True):
        dates = instrument_frame["trade_date"].to_list()
        for index, trade_date in enumerate(dates):
            if ctx.start <= trade_date <= ctx.end:
                if index + 1 < required_prices:
                    return True
                break
    return False


def _validate_adjusted_bars(frame: pl.DataFrame) -> None:
    required = {
        "instrument_id": pl.String,
        "trade_date": pl.Date,
        "close": pl.Float64,
        "available_at": pl.Datetime("us", "UTC"),
        "adjustment_factor": pl.Float64,
        "adjustment_event_factor": pl.Float64,
        "adjustment_event_available_at": pl.Datetime("us", "UTC"),
    }
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"adjusted bars missing columns: {', '.join(missing)}")
    for column, dtype in required.items():
        if frame.schema[column] != dtype:
            raise TypeError(f"adjusted bar {column} must have dtype {dtype}")


def _valid_positive_prices(values: Sequence[object]) -> bool:
    return all(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and isfinite(value)
        and value > 0
        for value in values
    )


def _point_in_time_closes(
    window: Sequence[dict[str, object]], signal_date: date
) -> tuple[list[float] | None, datetime | None]:
    global_closes = [row["close"] for row in window]
    global_factors = [row["adjustment_factor"] for row in window]
    event_factors = [row["adjustment_event_factor"] for row in window]
    if not (
        _valid_positive_prices(global_closes)
        and _valid_positive_prices(global_factors)
        and _valid_positive_prices(event_factors)
    ):
        return None, None

    cutoff = datetime.combine(signal_date, time.max, _SHANGHAI).astimezone(UTC)
    known_events: list[tuple[int, float, datetime]] = []
    for index, row in enumerate(window):
        factor = float(cast(int | float, row["adjustment_event_factor"]))
        available_at = row["adjustment_event_available_at"]
        if factor == 1.0:
            continue
        if not isinstance(available_at, datetime) or available_at.tzinfo is None:
            return None, None
        if available_at <= cutoff:
            known_events.append((index, factor, available_at))

    closes: list[float] = []
    for index, (adjusted, global_factor) in enumerate(
        zip(global_closes, global_factors, strict=True)
    ):
        raw_close = float(cast(int | float, adjusted)) / float(
            cast(int | float, global_factor)
        )
        local_factor = 1.0
        for event_index, event_factor, _ in known_events:
            if index < event_index:
                local_factor *= event_factor
        local_close = raw_close * local_factor
        if not isfinite(local_close) or local_close <= 0:
            return None, None
        closes.append(local_close)

    used_available = [
        available_at for event_index, _, available_at in known_events if event_index > 0
    ]
    return closes, max(used_available, default=None)


def _max_available_at(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return None
    return max(left, right) if right is not None else left


def _latest_available_at(
    window: Sequence[dict[str, object]],
) -> datetime | None:
    values: list[datetime] = []
    for row in window:
        value = row["available_at"]
        if value is None:
            return None
        if not isinstance(value, datetime):
            raise TypeError("adjusted bar available_at must be a datetime")
        values.append(value)
    if not values:
        return None
    return max(values)


def _empty_factor_output() -> pl.DataFrame:
    return pl.DataFrame(schema=FACTOR_OUTPUT_SCHEMA)
