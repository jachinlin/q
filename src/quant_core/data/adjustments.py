"""Point-in-time-safe price adjustments over immutable snapshot data."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import date
from enum import StrEnum
from math import isfinite

import polars as pl

from quant_core.data.repository import ResearchDataRepository
from quant_core.domain.identifiers import InstrumentId, SnapshotId


class AdjustmentMode(StrEnum):
    """The supported research price representations."""

    RAW = "RAW"
    BACKWARD = "BACKWARD"


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

        An event on ex-date ``d`` contributes
        ``(P_pre - C + R * Q) / (P_pre * (1 + R))`` to every earlier price,
        where ``C`` is cash per old share, ``R`` is new shares per old share,
        and ``Q`` is their subscription price.  Volume uses the reciprocal;
        amount is intentionally left untouched.
        """
        _validate_request(start, end, as_of)
        if mode is AdjustmentMode.RAW:
            raw = self._repository.bars(snapshot_id, instruments, start, end).collect()
            return _with_metadata(raw, mode, as_of).lazy()

        actions = self._repository.corporate_actions_as_of(
            snapshot_id, instruments, as_of
        ).collect()
        _validate_action_keys(actions)
        applicable = _applicable_actions(actions, start, as_of)
        action_end = max(
            (_required_date(row, "ex_date") for row in applicable), default=end
        )
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
            (pl.col("volume").cast(pl.Float64) / factor_column).alias("volume")
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
    bars: pl.DataFrame, actions: Sequence[dict[str, object]]
) -> dict[str, list[tuple[date, float]]]:
    factors: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for action in actions:
        instrument = _required_string(action, "instrument_id")
        ex_date = _required_date(action, "ex_date")
        preclose_rows = bars.filter(
            (pl.col("instrument_id") == instrument) & (pl.col("trade_date") == ex_date)
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
        factors[instrument].append((ex_date, factor))
    return factors


def _event_factor(action: dict[str, object], preclose: float) -> float:
    cash = _nonnegative_number(action, "cash_per_share")
    share_ratio = _nonnegative_number(action, "share_ratio")
    rights_price = _nonnegative_number(action, "rights_price")
    if rights_price > 0 and share_ratio == 0:
        raise ValueError(
            "corporate action rights_price requires a positive share_ratio"
        )
    numerator = preclose - cash + share_ratio * rights_price
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


def _combined_factor(
    factors: dict[str, list[tuple[date, float]]], instrument: object, trade_date: object
) -> float:
    if not isinstance(instrument, str) or not isinstance(trade_date, date):
        raise TypeError("daily bars must have canonical instrument and trade dates")
    factor = 1.0
    for ex_date, event_factor in factors[instrument]:
        if trade_date < ex_date:
            factor *= event_factor
    return factor
