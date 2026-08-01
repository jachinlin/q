"""Row-log-return ETF return and log-price trend factors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from math import expm1, isfinite
from typing import Protocol

import polars as pl

from quant_core.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec

_VERSION = "2.0.0"
_RETURN_WINDOWS = frozenset({20, 60, 120})
_HISTORY_CALENDAR_MULTIPLIER = 3
_PRICE_BASIS = "baostock_forward_log_return_v1"
_PATH_CONSTRUCTION = "window_forward_cumsum_v1"


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
    """Shared deterministic window execution over row-level log returns."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        spec: FactorSpec,
        required_prices: int,
        evaluator: Callable[[Sequence[float], Sequence[float]], float | None],
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
        """Compute rows inside ``ctx`` using request-stable row log returns."""
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
                FORWARD_LOG_RETURN_COLUMN,
                "available_at",
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
                    log_window = _relative_log_window(window)
                    if log_window is not None and available_at is not None:
                        relative_log_path, log_returns = log_window
                        value = self._evaluator(relative_log_path, log_returns)
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
            AdjustmentMode.FORWARD,
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
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "formula": ("expm1(forward_cumsum(forward_log_return[1:n+1])[-1])"),
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_sessions": window,
                },
            ),
            required_prices=window + 1,
            evaluator=_return_value,
        )


class Trend120dFactor(_MarketFactor):
    """Scale-invariant OLS slope of the latest 120 adjusted log closes."""

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
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "formula": (
                        "ols_slope([0,forward_cumsum("
                        "forward_log_return[1:120])],x=0..119)"
                    ),
                    "include_intercept": True,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_prices": 120,
                },
            ),
            required_prices=120,
            evaluator=_trend_value,
        )


class Momentum12020Factor(_MarketFactor):
    """Adjusted return from t-120 through t-20, skipping recent sessions."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="momentum_120_20_v1",
                version=_VERSION,
                frequency="daily",
                lookback_sessions=120,
                dependencies=(),
                direction=1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "eligible_for_alpha": True,
                    "formula": ("expm1(forward_cumsum(forward_log_return[1:101])[-1])"),
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "skip_recent_sessions": 20,
                    "window_prices": 121,
                },
            ),
            required_prices=121,
            evaluator=_momentum_120_20_value,
        )


def _return_value(
    relative_log_path: Sequence[float], log_returns: Sequence[float]
) -> float | None:
    del log_returns
    return _finite_expm1(relative_log_path[-1])


def _momentum_120_20_value(
    relative_log_path: Sequence[float], log_returns: Sequence[float]
) -> float | None:
    del log_returns
    return _finite_expm1(relative_log_path[-21])


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
        "available_at": pl.Datetime("us", "UTC"),
        FORWARD_LOG_RETURN_COLUMN: pl.Float64,
    }
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"adjusted bars missing columns: {', '.join(missing)}")
    for column, dtype in required.items():
        if frame.schema[column] != dtype:
            raise TypeError(f"adjusted bar {column} must have dtype {dtype}")


def _finite_log_return(value: object) -> float | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
    ):
        return None
    return float(value)


def _relative_log_window(
    window: Sequence[dict[str, object]],
) -> tuple[list[float], list[float]] | None:
    relative_log_path = [0.0]
    log_returns: list[float] = []
    cumulative = 0.0
    for row in window[1:]:
        value = _finite_log_return(row[FORWARD_LOG_RETURN_COLUMN])
        if value is None:
            return None
        log_returns.append(value)
        cumulative += value
        if not isfinite(cumulative):
            return None
        relative_log_path.append(cumulative)
    return relative_log_path, log_returns


def _finite_expm1(value: float) -> float | None:
    try:
        result = expm1(value)
    except OverflowError:
        return None
    return result if isfinite(result) else None


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
