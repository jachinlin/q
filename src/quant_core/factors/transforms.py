"""Deterministic cross-sectional transforms for factor observations."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from typing import cast

import numpy as np
import polars as pl

MIN_CROSS_SECTION_SIZE = 3

_MISSING_VALUE = "MISSING_VALUE"
_NONFINITE_VALUE = "NONFINITE_VALUE"
_INPUT_INVALID = "INPUT_INVALID"
_INSUFFICIENT_CROSS_SECTION = "INSUFFICIENT_CROSS_SECTION"
_ZERO_VARIANCE = "ZERO_VARIANCE"
_MISSING_INDUSTRY = "MISSING_INDUSTRY"
_MISSING_SIZE = "MISSING_SIZE"
_NONFINITE_SIZE = "NONFINITE_SIZE"
_INVALID_WEIGHT = "INVALID_WEIGHT"
_SINGLE_MEMBER_INDUSTRY = "SINGLE_MEMBER_INDUSTRY"
_RANK_DEFICIENT_DESIGN = "RANK_DEFICIENT_DESIGN"
_AUDIT_COLUMNS = frozenset({"is_valid", "invalid_reason"})

type _GroupKey = tuple[object, ...]


def winsorize_mad(
    frame: pl.DataFrame,
    value_col: str,
    group_cols: Sequence[str],
    n_mad: float = 3.0,
) -> pl.DataFrame:
    """Clip finite valid values by each group's median absolute deviation."""
    _validate_frame(frame)
    groups = _validate_columns(frame, value_col, group_cols)
    if type(n_mad) not in {int, float} or not math.isfinite(n_mad) or n_mad <= 0:
        raise ValueError("n_mad must be a positive finite number")
    state = _initial_state(frame, value_col)
    grouped = _candidate_groups(frame, groups, state.valid)
    for indices in grouped.values():
        if len(indices) < MIN_CROSS_SECTION_SIZE:
            _invalidate(state, indices, _INSUFFICIENT_CROSS_SECTION)
            continue
        values = [state.values[index] for index in indices]
        scale = max(abs(value) for value in values)
        if scale == 0.0:
            continue
        scaled_values = [value / scale for value in values]
        median = _median(scaled_values)
        mad = _median([abs(value - median) for value in scaled_values])
        if mad == 0.0:
            continue
        lower = median - float(n_mad) * mad
        upper = median + float(n_mad) * mad
        for index in indices:
            bounded = min(max(state.values[index] / scale, lower), upper)
            state.values[index] = bounded * scale
    return _result(frame, value_col, state)


def neutralize_wls(
    frame: pl.DataFrame,
    value_col: str,
    industry_col: str,
    size_col: str,
) -> pl.DataFrame:
    """Return WLS residuals against industry dummies and log market cap."""
    _validate_frame(frame)
    _validate_columns(frame, value_col, (industry_col, size_col))
    if frame.schema[industry_col] != pl.String:
        raise TypeError("industry column must have String dtype")
    _require_numeric_column(frame, size_col)
    state = _initial_state(frame, value_col)
    industries = frame[industry_col].to_list()
    sizes = frame[size_col].to_list()
    candidates = [index for index, valid in enumerate(state.valid) if valid]
    active: list[int] = []
    for index in candidates:
        industry = industries[index]
        size = sizes[index]
        if industry is None:
            _invalidate(state, [index], _MISSING_INDUSTRY)
            continue
        if size is None:
            _invalidate(state, [index], _MISSING_SIZE)
            continue
        numeric_size = float(cast(int | float, size))
        if not math.isfinite(numeric_size):
            _invalidate(state, [index], _NONFINITE_SIZE)
            continue
        active.append(index)

    if not active:
        return _result(frame, value_col, state)
    ordered = active
    log_market_caps = np.asarray(
        [float(cast(int | float, sizes[index])) for index in ordered],
        dtype=np.float64,
    )
    with np.errstate(over="ignore", under="ignore", invalid="ignore"):
        raw_weights = np.exp(log_market_caps - np.max(log_market_caps))
    total_weight = float(raw_weights.sum())
    if (
        not math.isfinite(total_weight)
        or total_weight <= 0.0
        or not np.all(np.isfinite(raw_weights))
        or np.any(raw_weights <= 0.0)
    ):
        _invalidate(state, active, _INVALID_WEIGHT)
        return _result(frame, value_col, state)
    normalized_weights = raw_weights / total_weight
    if not np.all(np.isfinite(normalized_weights)) or np.any(normalized_weights <= 0.0):
        _invalidate(state, active, _INVALID_WEIGHT)
        return _result(frame, value_col, state)
    industry_counts: dict[str, int] = defaultdict(int)
    for index in active:
        industry_counts[cast(str, industries[index])] += 1
    if any(count == 1 for count in industry_counts.values()):
        _invalidate(state, active, _SINGLE_MEMBER_INDUSTRY)
        return _result(frame, value_col, state)

    industry_levels = sorted(industry_counts)
    parameter_count = len(industry_levels) + 1
    if len(active) < MIN_CROSS_SECTION_SIZE or len(active) <= parameter_count:
        _invalidate(state, active, _INSUFFICIENT_CROSS_SECTION)
        return _result(frame, value_col, state)

    baseline = industry_levels[0]
    design = np.column_stack(
        (
            np.ones(len(ordered), dtype=np.float64),
            *(
                np.asarray(
                    [industries[index] == level for index in ordered], dtype=np.float64
                )
                for level in industry_levels
                if level != baseline
            ),
            log_market_caps,
        )
    )
    response = np.asarray([state.values[index] for index in ordered], dtype=np.float64)
    root_weights = np.sqrt(normalized_weights)
    weighted_design = design * root_weights[:, None]
    rcond = np.finfo(np.float64).eps * max(weighted_design.shape)
    beta, _, rank, _ = np.linalg.lstsq(
        weighted_design, response * root_weights, rcond=rcond
    )
    if rank != parameter_count:
        _invalidate(state, active, _RANK_DEFICIENT_DESIGN)
        return _result(frame, value_col, state)
    residuals = response - design @ beta
    if not np.all(np.isfinite(residuals)):
        _invalidate(state, active, _RANK_DEFICIENT_DESIGN)
        return _result(frame, value_col, state)
    for index, residual in zip(ordered, residuals, strict=True):
        state.values[index] = float(residual)
    return _result(frame, value_col, state)


def zscore(
    frame: pl.DataFrame, value_col: str, group_cols: Sequence[str]
) -> pl.DataFrame:
    """Standardize finite valid values within each group using population variance."""
    _validate_frame(frame)
    groups = _validate_columns(frame, value_col, group_cols)
    state = _initial_state(frame, value_col)
    grouped = _candidate_groups(frame, groups, state.valid)
    for indices in grouped.values():
        if len(indices) < MIN_CROSS_SECTION_SIZE:
            _invalidate(state, indices, _INSUFFICIENT_CROSS_SECTION)
            continue
        values = [state.values[index] for index in indices]
        scale = max(abs(value) for value in values)
        if scale == 0.0:
            _invalidate(state, indices, _ZERO_VARIANCE)
            continue
        scaled_values = [value / scale for value in values]
        mean = math.fsum(scaled_values) / len(scaled_values)
        variance = math.fsum((value - mean) ** 2 for value in scaled_values) / len(
            scaled_values
        )
        if not math.isfinite(variance) or variance <= 0.0:
            _invalidate(state, indices, _ZERO_VARIANCE)
            continue
        deviation = math.sqrt(variance)
        for index in indices:
            state.values[index] = (state.values[index] / scale - mean) / deviation
    return _result(frame, value_col, state)


class _TransformState:
    def __init__(
        self, values: list[float], valid: list[bool], reasons: list[str | None]
    ) -> None:
        self.values = values
        self.valid = valid
        self.reasons = reasons


def _validate_frame(frame: pl.DataFrame) -> None:
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("frame must be a polars DataFrame")


def _validate_columns(
    frame: pl.DataFrame, value_col: str, other_cols: Sequence[str]
) -> tuple[str, ...]:
    if not isinstance(value_col, str) or not value_col:
        raise ValueError("value column must be a nonempty string")
    if isinstance(other_cols, str) or not isinstance(other_cols, Sequence):
        raise TypeError("column names must be a sequence")
    names = (value_col, *other_cols)
    if any(not isinstance(name, str) or not name for name in names):
        raise ValueError("column names must be nonempty strings")
    if len(set(names)) != len(names):
        raise ValueError("duplicate column reference")
    if value_col in _AUDIT_COLUMNS:
        raise ValueError("value column must not use a reserved audit column")
    if any(name in _AUDIT_COLUMNS for name in other_cols):
        raise ValueError("semantic columns must not use reserved audit columns")
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise ValueError(f"column does not exist: {missing[0]}")
    _require_numeric_column(frame, value_col)
    return tuple(other_cols)


def _require_numeric_column(frame: pl.DataFrame, name: str) -> None:
    dtype = frame.schema[name]
    if dtype == pl.Boolean or not dtype.is_numeric():
        raise TypeError(f"{name} must have a numeric dtype")


def _initial_state(frame: pl.DataFrame, value_col: str) -> _TransformState:
    if "is_valid" in frame.columns and frame.schema["is_valid"] != pl.Boolean:
        raise TypeError("is_valid must have Boolean dtype")
    if (
        "invalid_reason" in frame.columns
        and frame.schema["invalid_reason"] != pl.String
    ):
        raise TypeError("invalid_reason must have String dtype")
    source_values = frame[value_col].to_list()
    source_validity = (
        frame["is_valid"].to_list()
        if "is_valid" in frame.columns
        else [True] * frame.height
    )
    source_reasons = (
        frame["invalid_reason"].to_list()
        if "invalid_reason" in frame.columns
        else [None] * frame.height
    )
    values: list[float] = [0.0] * frame.height
    valid: list[bool] = [False] * frame.height
    reasons: list[str | None] = [None] * frame.height
    for index, source_value in enumerate(source_values):
        source_reason = source_reasons[index]
        if source_validity[index] is not True:
            reasons[index] = (
                cast(str, source_reason)
                if source_reason is not None
                else _INPUT_INVALID
            )
            continue
        if source_value is None:
            reasons[index] = _MISSING_VALUE
            continue
        numeric_value = float(cast(int | float, source_value))
        if not math.isfinite(numeric_value):
            reasons[index] = _NONFINITE_VALUE
            continue
        values[index] = numeric_value
        valid[index] = True
    return _TransformState(values, valid, reasons)


def _candidate_groups(
    frame: pl.DataFrame, group_cols: tuple[str, ...], valid: list[bool]
) -> dict[_GroupKey, list[int]]:
    grouped: dict[_GroupKey, list[int]] = defaultdict(list)
    keys = frame.select(group_cols).rows() if group_cols else [()] * frame.height
    for index, key in enumerate(keys):
        if valid[index]:
            grouped[cast(_GroupKey, key)].append(index)
    return grouped


def _invalidate(state: _TransformState, indices: Sequence[int], reason: str) -> None:
    for index in indices:
        if state.valid[index]:
            state.values[index] = 0.0
            state.valid[index] = False
            state.reasons[index] = reason


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _result(
    frame: pl.DataFrame, value_col: str, state: _TransformState
) -> pl.DataFrame:
    values = [
        value if valid else None
        for value, valid in zip(state.values, state.valid, strict=True)
    ]
    return frame.with_columns(
        pl.Series(value_col, values, dtype=pl.Float64),
        pl.Series("is_valid", state.valid, dtype=pl.Boolean),
        pl.Series("invalid_reason", state.reasons, dtype=pl.String),
    )
