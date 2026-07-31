"""Point-in-time-safe price adjustments over immutable snapshot data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from math import isfinite

import polars as pl

from quant_core.data.repository import ResearchDataRepository
from quant_core.domain.identifiers import InstrumentId, SnapshotId

_INT64_MAX = 2**63 - 1


class AdjustmentMode(StrEnum):
    """The supported research price representations."""

    RAW = "RAW"
    BACKWARD = "BACKWARD"


@dataclass(frozen=True, slots=True)
class _DailyAction:
    instrument_id: str
    ex_date: date
    cash_per_share: float
    share_ratio: float


class PriceAdjustmentService:
    """Serve raw or backward-adjusted bars from one immutable snapshot."""

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
            return _with_metadata(result, mode, as_of).lazy()

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
        return _with_metadata(result, mode, as_of).lazy()


def _validate_request(start: date, end: date, as_of: date) -> None:
    if start > end:
        raise ValueError("start must not follow end")
    if as_of < end:
        raise ValueError("as_of must not precede end")


def _with_metadata(
    frame: pl.DataFrame, mode: AdjustmentMode, as_of: date
) -> pl.DataFrame:
    return frame.sort("instrument_id", "trade_date").with_columns(
        pl.lit(mode.value, dtype=pl.String).alias("adjustment_mode"),
        pl.lit(as_of, dtype=pl.Date).alias("adjustment_as_of"),
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
) -> dict[str, list[tuple[date, float]]]:
    factors: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for action in actions:
        preclose_rows = bars.filter(
            (pl.col("instrument_id") == action.instrument_id)
            & (pl.col("trade_date") == action.ex_date)
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
        factor = _event_factor(action, float(preclose))
        factors[action.instrument_id].append((action.ex_date, factor))
    return factors


def _daily_actions(actions: Sequence[dict[str, object]]) -> list[_DailyAction]:
    aggregated: dict[tuple[str, date], _DailyAction] = {}
    for action in actions:
        instrument = _required_string(action, "instrument_id")
        ex_date = _required_date(action, "ex_date")
        cash = _nonnegative_number(action, "cash_per_share")
        share_ratio = _nonnegative_number(action, "share_ratio")
        rights_price = _nonnegative_number(action, "rights_price")
        if rights_price != 0:
            raise ValueError(
                "corporate action rights_price is unsupported without an independent rights ratio"
            )
        key = (instrument, ex_date)
        existing = aggregated.get(key)
        if existing is None:
            aggregated[key] = _DailyAction(instrument, ex_date, cash, share_ratio)
        else:
            aggregated[key] = _DailyAction(
                instrument,
                ex_date,
                existing.cash_per_share + cash,
                existing.share_ratio + share_ratio,
            )
    return list(aggregated.values())


def _event_factor(action: _DailyAction, preclose: float) -> float:
    numerator = preclose - action.cash_per_share
    denominator = preclose * (1.0 + action.share_ratio)
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


def _combined_factor(
    factors: dict[str, list[tuple[date, float]]], instrument: object, trade_date: object
) -> float:
    if not isinstance(instrument, str) or not isinstance(trade_date, date):
        raise TypeError("daily bars must have canonical instrument and trade dates")
    factor = 1.0
    for ex_date, event_factor in factors[instrument]:
        if trade_date < ex_date:
            factor *= event_factor
    if not isfinite(factor) or factor <= 0:
        raise ValueError("combined corporate action adjustment factor must be positive")
    return factor


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
