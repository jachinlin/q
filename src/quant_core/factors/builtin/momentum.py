"""Row-log-return ETF return and log-price trend factors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Future
from datetime import date, datetime, timedelta
from math import expm1, isfinite
from threading import Lock, get_ident
from typing import Protocol

import polars as pl

from quant_core.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorSpec

_VERSION = "2.1.0"
_RETURN_WINDOWS = frozenset({20, 60, 120})
_HISTORY_CALENDAR_MULTIPLIER = 3
_PRICE_BASIS = "baostock_forward_log_return_v2"
_LOG_RETURN_FORMULA = "log_close_minus_log_preclose_v2"
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


class MarketBarsCache:
    """Share one immutable adjusted market-bar read across sibling factors."""

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
        self._instruments = _canonical_instrument_scope(instruments)
        self._max_lookback_sessions = max_lookback_sessions
        self._ctx: FactorContext | None = None
        self._bars: pl.DataFrame | None = None
        self._inflight: dict[FactorContext, tuple[Future[pl.DataFrame], int]] = {}
        self._waiting_for_owner: dict[int, tuple[int, Future[pl.DataFrame]]] = {}
        self._lock = Lock()

    def matches(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        *,
        max_lookback_sessions: int,
    ) -> bool:
        """Return whether this cache can serve the same pooling boundary."""
        return (
            self._price_service is price_service
            and tuple(instrument.canonical() for instrument in self._instruments)
            == tuple(instrument.canonical() for instrument in instruments)
            and self._max_lookback_sessions == max_lookback_sessions
        )

    def load(self, ctx: FactorContext) -> pl.DataFrame:
        """Return the one normalized price frame for the active factor context."""
        owner_thread_id = get_ident()
        with self._lock:
            if self._ctx == ctx and self._bars is not None:
                return self._bars
            active = self._inflight.get(ctx)
            if active is None:
                future: Future[pl.DataFrame] = Future()
                self._inflight[ctx] = (future, owner_thread_id)
                owns_load = True
            else:
                future, active_owner_thread_id = active
                if active_owner_thread_id == owner_thread_id:
                    raise RuntimeError(
                        "recursive market bar load for the same factor context"
                    )
                if self._would_create_wait_cycle(
                    owner_thread_id, active_owner_thread_id
                ):
                    raise RuntimeError("cyclic market bar cache wait detected")
                wait_edge = (active_owner_thread_id, future)
                self._waiting_for_owner[owner_thread_id] = wait_edge
                owns_load = False

        if not owns_load:
            try:
                return future.result()
            finally:
                with self._lock:
                    if self._waiting_for_owner.get(owner_thread_id) == wait_edge:
                        del self._waiting_for_owner[owner_thread_id]

        try:
            history_start = _expanded_history_start(
                ctx.start, self._max_lookback_sessions
            )
            bars = _load_adjusted_bars(
                self._price_service,
                self._instruments,
                ctx,
                history_start,
            )
            if history_start != date.min and _needs_full_history(
                bars, ctx, self._max_lookback_sessions + 1
            ):
                bars = _load_adjusted_bars(
                    self._price_service,
                    self._instruments,
                    ctx,
                    date.min,
                )
        except BaseException as error:
            with self._lock:
                if self._inflight.get(ctx) == (future, owner_thread_id):
                    del self._inflight[ctx]
            future.set_exception(error)
            raise

        with self._lock:
            self._ctx = ctx
            self._bars = bars
            if self._inflight.get(ctx) == (future, owner_thread_id):
                del self._inflight[ctx]
        future.set_result(bars)
        return bars

    def _would_create_wait_cycle(self, waiter: int, owner: int) -> bool:
        current = owner
        visited: set[int] = set()
        while current not in visited:
            if current == waiter:
                return True
            visited.add(current)
            wait_edge = self._waiting_for_owner.get(current)
            if wait_edge is None:
                return False
            waiting_for, future = wait_edge
            if future.done():
                return False
            current = waiting_for
        return False


class _MarketFactor:
    """Shared deterministic window execution over row-level log returns."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
        spec: FactorSpec,
        required_prices: int,
        evaluator: Callable[[Sequence[float], Sequence[float]], float | None],
        market_bars: MarketBarsCache | None = None,
    ) -> None:
        self._price_service = price_service
        self._instruments = _canonical_instrument_scope(instruments)
        self._spec = spec
        self._required_prices = required_prices
        self._evaluator = evaluator
        self._market_bars = market_bars

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """Compute rows inside ``ctx`` using request-stable row log returns."""
        if self._market_bars is None:
            history_start = _expanded_history_start(
                ctx.start, self.spec.lookback_sessions
            )
            normalized = self._load_bars(ctx, history_start)
            if history_start != date.min and _needs_full_history(
                normalized, ctx, self._required_prices
            ):
                normalized = self._load_bars(ctx, date.min)
        else:
            normalized = self._market_bars.load(ctx)
        if normalized.is_empty():
            return _empty_factor_output().lazy()

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
                available_at = _latest_available_at(window_availability)
                value: float | None = None
                if (
                    start_index >= 0
                    and len(window_availability) == self._required_prices
                ):
                    log_window = _relative_log_window(
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
                    "factor_version": [self.spec.version] * count,
                    "value": output_values,
                    "available_at": output_available_at,
                    "is_valid": [value is not None for value in output_values],
                },
                schema=FACTOR_OUTPUT_SCHEMA,
            )
            .sort("trade_date", "instrument_id")
            .lazy()
        )

    def _load_bars(self, ctx: FactorContext, start: date) -> pl.DataFrame:
        return _load_adjusted_bars(self._price_service, self._instruments, ctx, start)


def _load_adjusted_bars(
    price_service: AdjustedBarService,
    instruments: Sequence[InstrumentId],
    ctx: FactorContext,
    start: date,
) -> pl.DataFrame:
    bars = price_service.bars(
        ctx.snapshot_id,
        instruments,
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
        *,
        market_bars: MarketBarsCache | None = None,
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
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_sessions": window,
                },
            ),
            required_prices=window + 1,
            evaluator=_return_value,
            market_bars=market_bars,
        )


class Trend120dFactor(_MarketFactor):
    """Scale-invariant OLS slope of the latest 120 adjusted log closes."""

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
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_prices": 120,
                },
            ),
            required_prices=120,
            evaluator=_trend_value,
            market_bars=market_bars,
        )


class Momentum12020Factor(_MarketFactor):
    """Adjusted return from t-120 through t-20, skipping recent sessions."""

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
                    "log_return_formula": _LOG_RETURN_FORMULA,
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "skip_recent_sessions": 20,
                    "window_prices": 121,
                },
            ),
            required_prices=121,
            evaluator=_momentum_120_20_value,
            market_bars=market_bars,
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
    window_log_returns: Sequence[object],
) -> tuple[list[float], list[float]] | None:
    relative_log_path = [0.0]
    log_returns: list[float] = []
    cumulative = 0.0
    for raw_value in window_log_returns[1:]:
        value = _finite_log_return(raw_value)
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


def _empty_factor_output() -> pl.DataFrame:
    return pl.DataFrame(schema=FACTOR_OUTPUT_SCHEMA)
