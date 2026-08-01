"""Point-in-time-safe price adjustments over immutable snapshot data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from math import isfinite
from typing import cast

import polars as pl

from quant_core.data.repository import ResearchDataRepository
from quant_core.domain.identifiers import InstrumentId, SnapshotId

_INT64_MAX = 2**63 - 1
FORWARD_LOG_RETURN_COLUMN = "forward_log_return"
FORWARD_RETURN_INDEX_COLUMN = "forward_return_index"
_FORWARD_PRICE_COLUMNS = ("open", "high", "low", "close", "preclose")
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
    """The supported research price representations."""

    RAW = "RAW"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"


@dataclass(frozen=True, slots=True)
class _DailyAction:
    instrument_id: str
    action_type: str
    ex_date: date
    cash_per_share: float
    share_ratio: float
    available_at: datetime


@dataclass(frozen=True, slots=True)
class _AdjustmentEvent:
    instrument_id: str
    ex_date: date
    factor: float
    available_at: datetime
    components: tuple[_DailyAction, ...]


class PriceAdjustmentService:
    """Serve raw, backward-adjusted, or forward-adjusted snapshot bars."""

    def __init__(self, repository: ResearchDataRepository) -> None:
        self._repository = repository

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame:
        """Return stable, snapshot-bound bars with explicit adjustment metadata.

        The aggregate of all events on ex-date ``d`` contributes
        ``(P_pre - C) / (P_pre * (1 + R))`` to every earlier price, where
        ``C`` is total cash per old share and ``R`` is total new shares per old
        share. Volume uses the reciprocal; amount is intentionally untouched.
        """
        _validate_request(start, end, as_of)
        if mode is AdjustmentMode.RAW:
            raw = self._repository.bars(snapshot_id, instruments, start, end).collect()
            return _with_metadata(raw, mode, as_of).lazy()

        if mode is AdjustmentMode.FORWARD:
            raw = self._repository.bars(
                snapshot_id, instruments, start, as_of
            ).collect()
            adjusted, price_factors = _forward_adjust(raw, end)
            if adjusted.is_empty():
                return _with_metadata(adjusted, mode, as_of).lazy()
            factor_column = pl.Series("_price_factor", price_factors, dtype=pl.Float64)
            adjusted_prices = [
                (pl.col(column) * factor_column).alias(column)
                for column in _FORWARD_PRICE_COLUMNS
            ]
            result = adjusted.with_columns(adjusted_prices)
            _validate_forward_prices(result, adjusted=True)
            return _with_metadata(
                result,
                mode,
                as_of,
                adjustment_factors=price_factors,
            ).lazy()

        actions = self._repository.corporate_actions_as_of(
            snapshot_id, instruments, as_of
        ).collect()
        _validate_action_keys(actions)
        applicable = _daily_actions(_applicable_actions(actions, start, as_of))
        action_end = max((action.ex_date for action in applicable), default=end)
        raw = self._repository.bars(
            snapshot_id, instruments, start, max(end, action_end)
        ).collect()
        factors = _backward_factors(raw, applicable)
        result = raw.filter(pl.col("trade_date") <= end).sort(
            "instrument_id", "trade_date"
        )
        if result.is_empty():
            return _with_metadata(result, mode, as_of, events=factors).lazy()

        price_factors = [
            _combined_factor(factors, row["instrument_id"], row["trade_date"])
            for row in result.select("instrument_id", "trade_date").to_dicts()
        ]
        factor_column = pl.Series("_price_factor", price_factors, dtype=pl.Float64)
        adjusted_prices = [
            (pl.col(column) * factor_column).alias(column)
            for column in ("open", "high", "low", "close", "preclose")
        ]
        result = result.with_columns(adjusted_prices).with_columns(
            _adjusted_volumes(result["volume"], price_factors)
        )
        return _with_metadata(
            result,
            mode,
            as_of,
            adjustment_factors=price_factors,
            events=factors,
        ).lazy()


def _validate_request(start: date, end: date, as_of: date) -> None:
    if start > end:
        raise ValueError("start must not follow end")
    if as_of < end:
        raise ValueError("as_of must not precede end")


def _required_positive(value: object, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field} must be finite and positive")
    return float(value)


def _validate_unique_bar_keys(frame: pl.DataFrame) -> None:
    if frame.select(
        pl.struct("instrument_id", "trade_date").is_duplicated().any()
    ).item():
        raise ValueError("duplicate daily bar key")


def _has_any(frame: pl.DataFrame, expression: pl.Expr) -> bool:
    return bool(frame.select(expression.any()).item())


def _validate_forward_prices(frame: pl.DataFrame, *, adjusted: bool = False) -> None:
    prefix = "adjusted " if adjusted else ""
    close = pl.col("close")
    if _has_any(frame, close.is_null() | ~close.is_finite() | (close <= 0)):
        raise ValueError(f"{prefix}close must be finite and positive")

    first = pl.col("instrument_id").is_first_distinct()
    preclose = pl.col("preclose")
    valid_first_preclose = preclose.is_null() | (preclose.is_finite() & (preclose >= 0))
    invalid_later_preclose = (
        preclose.is_null() | ~preclose.is_finite() | (preclose <= 0)
    )
    if _has_any(
        frame,
        pl.when(first).then(~valid_first_preclose).otherwise(invalid_later_preclose),
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
        if _has_any(
            frame,
            value.is_not_null() & (~value.is_finite() | (value <= 0)),
        ):
            raise ValueError(
                f"{prefix}{column} must be finite and positive when non-null"
            )


def _forward_adjust(frame: pl.DataFrame, end: date) -> tuple[pl.DataFrame, list[float]]:
    ordered = frame.sort("instrument_id", "trade_date")
    _validate_unique_bar_keys(ordered)
    _validate_forward_prices(ordered)
    if ordered.is_empty():
        return (
            ordered.with_columns(
                pl.Series(FORWARD_LOG_RETURN_COLUMN, [], dtype=pl.Float64),
                pl.Series(FORWARD_RETURN_INDEX_COLUMN, [], dtype=pl.Float64),
            ),
            [],
        )

    first = pl.col("instrument_id").is_first_distinct()
    calculated = ordered.with_columns(
        pl.when(first)
        .then(0.0)
        .otherwise(
            pl.col("preclose").log()
            - pl.col("close").shift(1).over("instrument_id").log()
        )
        .alias("_forward_log_ratio"),
        pl.when(first)
        .then(pl.col("close"))
        .otherwise(pl.col("close") / pl.col("preclose"))
        .alias("_forward_return_ratio"),
        pl.when(pl.col("preclose").is_null() | (pl.col("preclose") == 0))
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("close").log() - pl.col("preclose").log())
        .cast(pl.Float64)
        .alias(FORWARD_LOG_RETURN_COLUMN),
    ).with_columns(
        pl.col("_forward_log_ratio")
        .reverse()
        .cum_sum()
        .reverse()
        .shift(-1)
        .fill_null(0.0)
        .over("instrument_id")
        .exp()
        .alias("_forward_factor"),
        pl.col("_forward_return_ratio")
        .cum_prod()
        .over("instrument_id")
        .cast(pl.Float64)
        .alias(FORWARD_RETURN_INDEX_COLUMN),
    )
    factor = pl.col("_forward_factor")
    if _has_any(calculated, ~factor.is_finite() | (factor <= 0)):
        raise ValueError("forward adjustment factor must be finite and positive")
    return_index = pl.col(FORWARD_RETURN_INDEX_COLUMN)
    if _has_any(calculated, ~return_index.is_finite() | (return_index <= 0)):
        raise ValueError("forward return index must be finite and positive")
    log_return = pl.col(FORWARD_LOG_RETURN_COLUMN)
    if _has_any(calculated, log_return.is_not_null() & ~log_return.is_finite()):
        raise ValueError("forward log return must be finite when non-null")

    filtered = calculated.filter(pl.col("trade_date") <= end)
    filtered_factors = cast(list[float], filtered["_forward_factor"].to_list())
    filtered = filtered.drop(
        "_forward_log_ratio", "_forward_return_ratio", "_forward_factor"
    )
    return filtered, filtered_factors


def _with_metadata(
    frame: pl.DataFrame,
    mode: AdjustmentMode,
    as_of: date,
    *,
    adjustment_factors: Sequence[float] | None = None,
    events: dict[str, list[_AdjustmentEvent]] | None = None,
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
    event_factors, event_available, event_components = _event_metadata(
        normalized, events or {}
    )
    return normalized.with_columns(
        pl.lit(mode.value, dtype=pl.String).alias("adjustment_mode"),
        pl.lit(as_of, dtype=pl.Date).alias("adjustment_as_of"),
        pl.Series("adjustment_event_factor", event_factors, dtype=pl.Float64),
        pl.Series(
            "adjustment_event_available_at",
            event_available,
            dtype=pl.Datetime("us", "UTC"),
        ),
        pl.Series(
            "adjustment_event_components",
            event_components,
            dtype=ADJUSTMENT_EVENT_COMPONENTS_DTYPE,
        ),
    )


def _validate_action_keys(actions: pl.DataFrame) -> None:
    key_columns = ("instrument_id", "action_type", "record_date", "ex_date", "pay_date")
    if actions.is_duplicated().any():
        raise ValueError("duplicate corporate action primary key")
    if actions.select(pl.struct(*key_columns).is_duplicated().any()).item():
        raise ValueError("duplicate corporate action primary key")


def _applicable_actions(
    actions: pl.DataFrame, start: date, as_of: date
) -> list[dict[str, object]]:
    rows = actions.filter(
        pl.col("ex_date").is_not_null()
        & (pl.col("ex_date") > start)
        & (pl.col("ex_date") <= as_of)
    ).sort("instrument_id", "ex_date", "action_type", "record_date", "pay_date")
    return rows.to_dicts()


def _backward_factors(
    bars: pl.DataFrame, actions: Sequence[_DailyAction]
) -> dict[str, list[_AdjustmentEvent]]:
    factors: dict[str, list[_AdjustmentEvent]] = defaultdict(list)
    grouped: dict[tuple[str, date], list[_DailyAction]] = defaultdict(list)
    for action in actions:
        grouped[(action.instrument_id, action.ex_date)].append(action)
    for (instrument_id, ex_date), components_list in grouped.items():
        preclose_rows = bars.filter(
            (pl.col("instrument_id") == instrument_id)
            & (pl.col("trade_date") == ex_date)
        ).select("preclose")
        if preclose_rows.height != 1:
            raise ValueError("corporate action ex_date has no matching bar")
        preclose = preclose_rows.item()
        if (
            not isinstance(preclose, (int, float))
            or not isfinite(preclose)
            or preclose <= 0
        ):
            raise ValueError("corporate action requires a positive ex-date preclose")
        components = tuple(components_list)
        factor = _event_factor(
            sum(component.cash_per_share for component in components),
            sum(component.share_ratio for component in components),
            float(preclose),
        )
        factors[instrument_id].append(
            _AdjustmentEvent(
                instrument_id,
                ex_date,
                factor,
                max(component.available_at for component in components),
                components,
            )
        )
    return factors


def _daily_actions(actions: Sequence[dict[str, object]]) -> list[_DailyAction]:
    daily_actions: list[_DailyAction] = []
    for action in actions:
        instrument = _required_string(action, "instrument_id")
        action_type = _required_string(action, "action_type")
        ex_date = _required_date(action, "ex_date")
        cash = _nonnegative_number(action, "cash_per_share")
        share_ratio = _nonnegative_number(action, "share_ratio")
        rights_price = _nonnegative_number(action, "rights_price")
        available_at = _required_datetime(action, "available_at")
        if rights_price != 0:
            raise ValueError(
                "corporate action rights_price is unsupported without an independent rights ratio"
            )
        daily_actions.append(
            _DailyAction(
                instrument,
                action_type,
                ex_date,
                cash,
                share_ratio,
                available_at,
            )
        )
    return daily_actions


def _event_factor(cash_per_share: float, share_ratio: float, preclose: float) -> float:
    numerator = preclose - cash_per_share
    denominator = preclose * (1.0 + share_ratio)
    factor = numerator / denominator
    if not isfinite(factor) or factor <= 0:
        raise ValueError("corporate action adjustment factor must be positive")
    return factor


def _nonnegative_number(action: dict[str, object], column: str) -> float:
    value = action[column]
    if value is None:
        return 0.0
    if not isinstance(value, (int, float)) or not isfinite(value) or value < 0:
        raise ValueError(
            f"corporate action {column} must be a finite nonnegative number"
        )
    return float(value)


def _required_string(action: dict[str, object], column: str) -> str:
    value = action[column]
    if not isinstance(value, str):
        raise TypeError(f"corporate action {column} must be present")
    return value


def _required_date(action: dict[str, object], column: str) -> date:
    value = action[column]
    if not isinstance(value, date):
        raise TypeError(f"corporate action {column} must be present")
    return value


def _required_datetime(action: dict[str, object], column: str) -> datetime:
    value = action[column]
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise TypeError(f"corporate action {column} must be a timezone-aware datetime")
    return value


def _combined_factor(
    factors: dict[str, list[_AdjustmentEvent]],
    instrument: object,
    trade_date: object,
) -> float:
    if not isinstance(instrument, str) or not isinstance(trade_date, date):
        raise TypeError("daily bars must have canonical instrument and trade dates")
    factor = 1.0
    for event in factors[instrument]:
        if trade_date < event.ex_date:
            factor *= event.factor
    if not isfinite(factor) or factor <= 0:
        raise ValueError("combined corporate action adjustment factor must be positive")
    return factor


def _event_metadata(
    frame: pl.DataFrame, events: dict[str, list[_AdjustmentEvent]]
) -> tuple[
    list[float],
    list[datetime | None],
    list[list[dict[str, object]]],
]:
    by_key = {
        (event.instrument_id, event.ex_date): event
        for instrument_events in events.values()
        for event in instrument_events
        if event.factor != 1.0
    }
    event_factors: list[float] = []
    event_available: list[datetime | None] = []
    event_components: list[list[dict[str, object]]] = []
    for row in frame.select("instrument_id", "trade_date").to_dicts():
        event = by_key.get((row["instrument_id"], row["trade_date"]))
        event_factors.append(event.factor if event is not None else 1.0)
        event_available.append(event.available_at if event is not None else None)
        event_components.append(
            [
                {
                    "action_type": component.action_type,
                    "cash_per_share": component.cash_per_share,
                    "share_ratio": component.share_ratio,
                    "available_at": component.available_at,
                }
                for component in event.components
            ]
            if event is not None
            else []
        )
    return event_factors, event_available, event_components


def _adjusted_volumes(volumes: pl.Series, factors: Sequence[float]) -> pl.Series:
    adjusted: list[int | None] = []
    for volume, factor in zip(volumes.to_list(), factors, strict=True):
        if volume is None:
            adjusted.append(None)
            continue
        if isinstance(volume, bool) or not isinstance(volume, int):
            raise TypeError("daily bar volume must be an Int64 value")
        if volume < 0:
            raise ValueError("daily bar volume must be nonnegative")
        try:
            rounded = (Decimal(volume) / Decimal(str(factor))).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        except (InvalidOperation, ValueError) as error:
            raise ValueError("adjusted volume must be finite") from error
        if rounded > _INT64_MAX:
            raise ValueError("adjusted volume exceeds Int64 range")
        adjusted.append(int(rounded))
    return pl.Series("volume", adjusted, dtype=pl.Int64)
