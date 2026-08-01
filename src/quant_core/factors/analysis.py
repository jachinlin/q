"""Pure, deterministic point-in-time factor diagnostics."""

from __future__ import annotations

from math import isfinite

import numpy as np
import polars as pl


def coverage_by_date(
    factors: pl.DataFrame, eligible_universe: pl.DataFrame
) -> pl.DataFrame:
    """Count valid factor observations against an explicit eligible denominator."""
    _require(factors, {"signal_date", "instrument_id", "value", "is_valid"}, "factors")
    _require(
        eligible_universe, {"signal_date", "instrument_id", "eligible"}, "universe"
    )
    _unique(factors, "factors")
    _unique(eligible_universe, "universe")
    joined = eligible_universe.filter(pl.col("eligible")).join(
        _valid_factors(factors)
        .select("signal_date", "instrument_id")
        .with_columns(pl.lit(True).alias("_valid")),
        on=["signal_date", "instrument_id"],
        how="left",
    )
    return (
        joined.group_by("signal_date")
        .agg(
            pl.len().alias("eligible_count"),
            pl.col("_valid").fill_null(False).sum().cast(pl.Int64).alias("valid_count"),
        )
        .with_columns(
            (pl.col("valid_count") / pl.col("eligible_count")).alias("coverage")
        )
        .sort("signal_date")
    )


def spearman_rank_ic(
    factors: pl.DataFrame, future_returns: pl.DataFrame
) -> pl.DataFrame:
    """Compute one valid-pair Spearman correlation per signal date."""
    _unique(factors, "factors")
    pairs = _aligned_pairs(factors, future_returns)
    rows = []
    dates = sorted(set(factors["signal_date"].to_list()))
    for day in dates:
        group = pairs.filter(pl.col("signal_date") == day)
        count = group.height
        correlation = (
            _correlation(
                _ranks(group["value"].to_numpy()),
                _ranks(group["future_return"].to_numpy()),
            )
            if count >= 2
            else None
        )
        rows.append((day, count, correlation, correlation is not None))
    return pl.DataFrame(
        rows,
        schema={
            "signal_date": pl.Date,
            "pair_count": pl.Int64,
            "rank_ic": pl.Float64,
            "is_valid": pl.Boolean,
        },
        orient="row",
    )


def assign_quantiles(factors: pl.DataFrame, quantiles: int) -> pl.DataFrame:
    """Assign 1=lowest through Q=highest with stable instrument tie-breaking."""
    if type(quantiles) is not int or quantiles < 2:
        raise ValueError("quantiles must be an integer of at least 2")
    _unique(factors, "factors")
    valid = _valid_factors(factors)
    rows = []
    for group in valid.partition_by("signal_date", maintain_order=False):
        ordered = group.sort("value", "instrument_id")
        count = ordered.height
        for index, row in enumerate(ordered.to_dicts()):
            rows.append(
                (
                    row["signal_date"],
                    row["instrument_id"],
                    row["value"],
                    index * quantiles // count + 1,
                    quantiles,
                )
            )
    return pl.DataFrame(
        rows,
        schema={
            "signal_date": pl.Date,
            "instrument_id": pl.String,
            "value": pl.Float64,
            "quantile": pl.Int64,
            "quantiles": pl.Int64,
        },
        orient="row",
    ).sort("signal_date", "instrument_id")


def quantile_future_returns(
    factors: pl.DataFrame, future_returns: pl.DataFrame, quantiles: int
) -> pl.DataFrame:
    """Average strictly future returns for every date/quantile, including empty groups."""
    assigned = assign_quantiles(factors, quantiles)
    _validate_future(future_returns)
    joined = assigned.join(
        _valid_returns(future_returns), on=["signal_date", "instrument_id"], how="left"
    )
    rows = []
    for day in sorted(set(factors["signal_date"].to_list())):
        day_rows = joined.filter(pl.col("signal_date") == day)
        for quantile in range(1, quantiles + 1):
            values = (
                day_rows.filter(pl.col("quantile") == quantile)["future_return"]
                .drop_nulls()
                .to_list()
            )
            rows.append(
                (
                    day,
                    quantile,
                    len(values),
                    sum(values) / len(values) if values else None,
                    quantiles,
                )
            )
    return pl.DataFrame(
        rows,
        schema={
            "signal_date": pl.Date,
            "quantile": pl.Int64,
            "count": pl.Int64,
            "mean_return": pl.Float64,
            "quantiles": pl.Int64,
        },
        orient="row",
    ).sort("signal_date", "quantile")


def long_short_returns(quantile_returns: pl.DataFrame) -> pl.DataFrame:
    """Return the fixed Q-minus-1 portfolio return for each signal date."""
    _require(
        quantile_returns,
        {"signal_date", "quantile", "mean_return", "quantiles"},
        "quantile returns",
    )
    rows = []
    for group in quantile_returns.partition_by("signal_date", maintain_order=False):
        day = group["signal_date"].item(0)
        q = group["quantiles"].item(0)
        low = group.filter(pl.col("quantile") == 1)["mean_return"].item()
        high = group.filter(pl.col("quantile") == q)["mean_return"].item()
        value = high - low if high is not None and low is not None else None
        rows.append((day, value, value is not None))
    return pl.DataFrame(
        rows,
        schema={
            "signal_date": pl.Date,
            "long_short_return": pl.Float64,
            "is_valid": pl.Boolean,
        },
        orient="row",
    ).sort("signal_date")


def factor_correlation_matrix(factors: pl.DataFrame) -> pl.DataFrame:
    """Average same-date, same-security Pearson factor correlations."""
    _require(
        factors,
        {"signal_date", "instrument_id", "factor_id", "value", "is_valid"},
        "factors",
    )
    if factors.select(
        pl.struct("signal_date", "instrument_id", "factor_id").is_duplicated().any()
    ).item():
        raise ValueError("duplicate factor correlation key")
    valid = _valid_factors(factors)
    ids = sorted(set(valid["factor_id"].to_list()))
    rows = []
    for left in ids:
        for right in ids:
            daily: list[float] = []
            left_frame = valid.filter(pl.col("factor_id") == left).select(
                "signal_date", "instrument_id", pl.col("value").alias("left")
            )
            right_frame = valid.filter(pl.col("factor_id") == right).select(
                "signal_date", "instrument_id", pl.col("value").alias("right")
            )
            paired = left_frame.join(
                right_frame, on=["signal_date", "instrument_id"], how="inner"
            )
            pair_count = paired.height
            for group in paired.partition_by("signal_date", maintain_order=False):
                corr = (
                    _correlation(group["left"].to_numpy(), group["right"].to_numpy())
                    if group.height >= 2
                    else None
                )
                if corr is not None:
                    daily.append(corr)
            correlation = sum(daily) / len(daily) if daily else None
            rows.append((left, right, pair_count, correlation, correlation is not None))
    return pl.DataFrame(
        rows,
        schema={
            "factor_x": pl.String,
            "factor_y": pl.String,
            "pair_count": pl.Int64,
            "correlation": pl.Float64,
            "is_valid": pl.Boolean,
        },
        orient="row",
    )


def _aligned_pairs(factors: pl.DataFrame, future_returns: pl.DataFrame) -> pl.DataFrame:
    valid = _valid_factors(factors)
    _validate_future(future_returns)
    return valid.join(
        _valid_returns(future_returns), on=["signal_date", "instrument_id"], how="inner"
    )


def _valid_factors(frame: pl.DataFrame) -> pl.DataFrame:
    _require(frame, {"signal_date", "instrument_id", "value", "is_valid"}, "factors")
    return frame.filter(
        pl.col("is_valid") & pl.col("value").is_not_null() & pl.col("value").is_finite()
    )


def _valid_returns(frame: pl.DataFrame) -> pl.DataFrame:
    return _return_rows_with_boundaries(frame).filter(
        pl.col("future_return").is_not_null() & pl.col("future_return").is_finite()
    )


def _validate_future(frame: pl.DataFrame) -> None:
    _require(
        frame,
        {"signal_date", "instrument_id", "return_start", "return_end", "future_return"},
        "future returns",
    )
    _unique(frame, "future returns")
    bounded = _return_rows_with_boundaries(frame)
    if bounded.filter(
        (pl.col("return_start") <= pl.col("signal_date"))
        | (pl.col("return_end") < pl.col("return_start"))
    ).height:
        raise ValueError("future return window must be strictly after signal date")


def _return_rows_with_boundaries(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(
        pl.col("signal_date").is_not_null()
        & pl.col("instrument_id").is_not_null()
        & pl.col("return_start").is_not_null()
        & pl.col("return_end").is_not_null()
    )


def _unique(frame: pl.DataFrame, name: str) -> None:
    if frame.select(
        pl.struct("signal_date", "instrument_id").is_duplicated().any()
    ).item():
        raise ValueError(f"duplicate {name} key")


def _require(frame: pl.DataFrame, columns: set[str], name: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise ValueError(f"{name} missing columns: {', '.join(missing)}")


def _ranks(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="stable")
    ranks = np.empty(len(values), dtype=np.float64)
    index = 0
    while index < len(values):
        stop = index + 1
        while stop < len(values) and values[order[stop]] == values[order[index]]:
            stop += 1
        ranks[order[index:stop]] = (index + stop - 1) / 2.0
        index = stop
    return ranks


def _correlation(left: np.ndarray, right: np.ndarray) -> float | None:
    if len(left) < 2 or np.ptp(left) == 0 or np.ptp(right) == 0:
        return None
    result = float(np.corrcoef(left, right)[0, 1])
    return result if isfinite(result) else None
