"""提供因子与因子变换相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from typing import cast

import polars as pl

MIN_CROSS_SECTION_SIZE = 3

_MISSING_VALUE = "MISSING_VALUE"
_NONFINITE_VALUE = "NONFINITE_VALUE"
_INPUT_INVALID = "INPUT_INVALID"
_INSUFFICIENT_CROSS_SECTION = "INSUFFICIENT_CROSS_SECTION"
_ZERO_VARIANCE = "ZERO_VARIANCE"
_MISSING_INDUSTRY = "MISSING_INDUSTRY"
_SINGLE_MEMBER_INDUSTRY = "SINGLE_MEMBER_INDUSTRY"
_AUDIT_COLUMNS = frozenset({"is_valid", "invalid_reason"})

type _GroupKey = tuple[object, ...]


def winsorize_mad(
    frame: pl.DataFrame,
    value_col: str,
    group_cols: Sequence[str],
    n_mad: float = 3.0,
) -> pl.DataFrame:
    """处理因子计算中的缩尾MAD；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        frame：待处理的数据帧，类型为 ``pl.DataFrame``。
        value_col：值列名。
        返回MAD（``pl.DataFrame``）。
        n_mad：倍数MAD。
    返回值：
        返回``mad``（``pl.DataFrame``）。
    异常：
        无。
    Clip finite valid values by each group's median absolute deviation.
    """
    _TransformsSupport._validate_frame(frame)
    groups = _TransformsSupport._validate_columns(frame, value_col, group_cols)
    _TransformsSupport._validate_group_dtypes(frame, groups)
    multiplier = _TransformsSupport._validated_mad_multiplier(n_mad)
    state = _TransformsSupport._initial_state(frame, value_col)
    grouped = _TransformsSupport._candidate_groups(frame, groups, state.valid)
    for indices in grouped.values():
        if len(indices) < MIN_CROSS_SECTION_SIZE:
            _TransformsSupport._invalidate(state, indices, _INSUFFICIENT_CROSS_SECTION)
            continue
        values = [state.values[index] for index in indices]
        scale = max(abs(value) for value in values)
        if scale == 0.0:
            continue
        scaled_values = [value / scale for value in values]
        median = _TransformsSupport._median(scaled_values)
        mad = _TransformsSupport._median(
            [abs(value - median) for value in scaled_values]
        )
        if mad == 0.0:
            continue
        lower = median - multiplier * mad
        upper = median + multiplier * mad
        for index in indices:
            bounded = min(max(state.values[index] / scale, lower), upper)
            state.values[index] = bounded * scale
    return _TransformsSupport._result(frame, value_col, state)


def neutralize_industry(
    frame: pl.DataFrame,
    value_col: str,
    industry_col: str,
    group_cols: Sequence[str] = (),
) -> pl.DataFrame:
    """按给定截面和行业执行等权组内去均值；该函数作为稳定公开 API 保留在模块级。

    入参：
        frame：待处理的数据帧。
        value_col：需要中性化的数值列。
        industry_col：PIT 对齐后的行业代码列。
        group_cols：定义独立截面的列，例如信号日和因子 ID。
    返回值：
        保留原顺序并更新数值、有效标记和无效原因的数据帧。
    异常：
        列不存在、类型非法或使用保留审计列时抛出 ``TypeError`` 或 ``ValueError``。
    """
    _TransformsSupport._validate_frame(frame)
    if isinstance(group_cols, str) or not isinstance(group_cols, Sequence):
        raise TypeError("group columns must be a sequence")
    groups = _TransformsSupport._validate_columns(
        frame, value_col, (*group_cols, industry_col)
    )
    grouping = groups[:-1]
    if frame.schema[industry_col] != pl.String:
        raise ValueError("industry column must have String dtype")
    _TransformsSupport._validate_group_dtypes(frame, grouping)
    if "is_valid" in frame.columns and frame.schema["is_valid"] != pl.Boolean:
        raise TypeError("is_valid must have Boolean dtype")
    if (
        "invalid_reason" in frame.columns
        and frame.schema["invalid_reason"] != pl.String
    ):
        raise TypeError("invalid_reason must have String dtype")

    source_valid = (
        pl.col("is_valid").fill_null(False)
        if "is_valid" in frame.columns
        else pl.lit(True)
    )
    source_reason = (
        pl.col("invalid_reason")
        if "invalid_reason" in frame.columns
        else pl.lit(None, dtype=pl.String)
    )
    numeric = pl.col(value_col).cast(pl.Float64)
    value_present = numeric.is_not_null()
    value_finite = numeric.is_finite().fill_null(False)
    industry_present = (
        pl.col(industry_col).is_not_null()
        & (pl.col(industry_col).str.len_chars() > 0)
    ).fill_null(False)
    active = source_valid & value_present & value_finite & industry_present
    partition = [*grouping, industry_col]
    count = active.cast(pl.Int64).sum().over(partition)
    mean = pl.when(active).then(numeric).otherwise(None).mean().over(partition)
    valid = active & (count >= 2)
    reason = (
        pl.when(~source_valid)
        .then(source_reason.fill_null(_INPUT_INVALID))
        .when(~value_present)
        .then(pl.lit(_MISSING_VALUE))
        .when(~value_finite)
        .then(pl.lit(_NONFINITE_VALUE))
        .when(~industry_present)
        .then(pl.lit(_MISSING_INDUSTRY))
        .when(count < 2)
        .then(pl.lit(_SINGLE_MEMBER_INDUSTRY))
        .otherwise(pl.lit(None, dtype=pl.String))
    )
    return frame.with_columns(
        pl.when(valid)
        .then(numeric - mean)
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias(value_col),
        valid.alias("is_valid"),
        reason.alias("invalid_reason"),
    )


class _TransformsSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_frame(frame: pl.DataFrame) -> None:
        if not isinstance(frame, pl.DataFrame):
            raise TypeError("frame must be a polars DataFrame")

    @staticmethod
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
        _TransformsSupport._require_numeric_column(frame, value_col)
        return tuple(other_cols)

    @staticmethod
    def _require_numeric_column(frame: pl.DataFrame, name: str) -> None:
        dtype = frame.schema[name]
        if dtype == pl.Boolean or not dtype.is_numeric():
            raise TypeError(f"{name} must have a numeric dtype")

    @staticmethod
    def _validate_group_dtypes(frame: pl.DataFrame, group_cols: Sequence[str]) -> None:
        """Reject nested/object group keys before Python grouping can raise TypeError."""
        for name in group_cols:
            dtype = frame.schema[name]
            if dtype.is_nested() or dtype.is_object():
                raise ValueError(f"unsupported group dtype: {name}")

    @staticmethod
    def _validated_mad_multiplier(n_mad: float) -> float:
        if type(n_mad) not in {int, float}:
            raise ValueError("n_mad must be a positive finite number")
        try:
            multiplier = float(n_mad)
        except OverflowError as error:
            raise ValueError("n_mad must be a positive finite number") from error
        if not math.isfinite(multiplier) or multiplier <= 0.0:
            raise ValueError("n_mad must be a positive finite number")
        return multiplier

    @staticmethod
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

    @staticmethod
    def _candidate_groups(
        frame: pl.DataFrame, group_cols: tuple[str, ...], valid: list[bool]
    ) -> dict[_GroupKey, list[int]]:
        grouped: dict[_GroupKey, list[int]] = defaultdict(list)
        keys = frame.select(group_cols).rows() if group_cols else [()] * frame.height
        for index, key in enumerate(keys):
            if valid[index]:
                grouped[cast(_GroupKey, key)].append(index)
        return grouped

    @staticmethod
    def _invalidate(
        state: _TransformState, indices: Sequence[int], reason: str
    ) -> None:
        for index in indices:
            if state.valid[index]:
                state.values[index] = 0.0
                state.valid[index] = False
                state.reasons[index] = reason

    @staticmethod
    def _median(values: Sequence[float]) -> float:
        ordered = sorted(values)
        midpoint = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[midpoint]
        return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0

    @staticmethod
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


def zscore(
    frame: pl.DataFrame, value_col: str, group_cols: Sequence[str]
) -> pl.DataFrame:
    """处理因子计算中的``zscore``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        frame：待处理的数据帧，类型为 ``pl.DataFrame``。
        value_col：值列名。
        返回``zscore``（``pl.DataFrame``）。
    返回值：
        返回``zscore``（``pl.DataFrame``）。
    异常：
        无。
    Standardize finite valid values within each group using population variance.
    """
    _TransformsSupport._validate_frame(frame)
    groups = _TransformsSupport._validate_columns(frame, value_col, group_cols)
    _TransformsSupport._validate_group_dtypes(frame, groups)
    state = _TransformsSupport._initial_state(frame, value_col)
    grouped = _TransformsSupport._candidate_groups(frame, groups, state.valid)
    for indices in grouped.values():
        if len(indices) < MIN_CROSS_SECTION_SIZE:
            _TransformsSupport._invalidate(state, indices, _INSUFFICIENT_CROSS_SECTION)
            continue
        values = [state.values[index] for index in indices]
        scale = max(abs(value) for value in values)
        if scale == 0.0:
            _TransformsSupport._invalidate(state, indices, _ZERO_VARIANCE)
            continue
        scaled_values = [value / scale for value in values]
        mean = math.fsum(scaled_values) / len(scaled_values)
        variance = math.fsum((value - mean) ** 2 for value in scaled_values) / len(
            scaled_values
        )
        if not math.isfinite(variance) or variance <= 0.0:
            _TransformsSupport._invalidate(state, indices, _ZERO_VARIANCE)
            continue
        deviation = math.sqrt(variance)
        for index in indices:
            state.values[index] = (state.values[index] / scale - mean) / deviation
    return _TransformsSupport._result(frame, value_col, state)


class _TransformState:
    def __init__(
        self, values: list[float], valid: list[bool], reasons: list[str | None]
    ) -> None:
        self.values = values
        self.valid = valid
        self.reasons = reasons
