"""基于时点安全输入组装独立因子研究诊断结果。"""

from __future__ import annotations

from datetime import date
from typing import cast

import polars as pl

from quant_research.factor_studies.models import (
    DIRECTION_ADJUSTED,
    IC_QUANTILE_PROBABILITIES,
    IC_ROLLING_MIN_VALID,
    IC_ROLLING_WINDOW,
    MIN_CROSS_SECTION,
)
from quant_research.factors.analysis import (
    InformationCoefficientAnalyzer,
    factor_rank_correlation_matrix,
    long_short_returns,
    quantile_future_returns,
)


def build_future_returns(
    bars: pl.DataFrame,
    sessions: tuple[date, ...],
    eligible: pl.DataFrame,
    horizons: tuple[int, ...],
    tradability: pl.DataFrame | None = None,
) -> dict[int, pl.DataFrame]:
    """构造 T+1 开盘至 T+h 收盘的未来收益；该函数作为稳定公开 API保留在模块级。

    入参：
        bars：包含证券、交易日和 OHLCV 字段的市场行情表。
        sessions：参与本次处理的交易会话集合；调用方不得依赖未声明的顺序。
        eligible：准入证券。
        horizons：参与本次处理的收益期限集合；调用方不得依赖未声明的顺序。
        tradability：可交易性。
    返回值：
        返回构建未来收益收益序列后的未来收益收益序列（``dict[int, pl.DataFrame]``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """
    required = {"instrument_id", "trade_date", "open", "close"}
    if not required.issubset(bars.columns):
        raise ValueError("adjusted bars are missing future-return columns")
    price = {
        (cast(str, row["instrument_id"]), cast(date, row["trade_date"])): (
            row["open"],
            row["close"],
        )
        for row in bars.select(required).iter_rows(named=True)
    }
    position = {day: index for index, day in enumerate(sessions)}
    tradable_keys: set[tuple[str, date]] | None = None
    if tradability is not None:
        required_status = {"instrument_id", "trade_date", "is_listed", "is_suspended"}
        if not required_status.issubset(tradability.columns):
            raise ValueError("tradability data is missing required columns")
        tradable_keys = {
            (cast(str, row["instrument_id"]), cast(date, row["trade_date"]))
            for row in tradability.select(required_status).iter_rows(named=True)
            if bool(row["is_listed"]) and not bool(row["is_suspended"])
        }
    eligible_rows = eligible.filter(pl.col("eligible")).select(
        "signal_date", "instrument_id"
    )
    output: dict[int, pl.DataFrame] = {}
    for horizon in horizons:
        rows: list[tuple[date, str, date | None, date | None, float | None]] = []
        for signal_day, instrument_id in eligible_rows.iter_rows():
            index = position.get(signal_day)
            start_index = None if index is None else index + 1
            end_index = None if index is None else index + horizon
            if start_index is None or end_index is None or end_index >= len(sessions):
                rows.append((signal_day, instrument_id, None, None, None))
                continue
            start_day, end_day = sessions[start_index], sessions[end_index]
            start = price.get((instrument_id, start_day))
            end = price.get((instrument_id, end_day))
            open_price = None if start is None else start[0]
            close_price = None if end is None else end[1]
            valid = (
                isinstance(open_price, (int, float))
                and isinstance(close_price, (int, float))
                and open_price > 0
                and close_price > 0
                and (
                    tradable_keys is None or (instrument_id, start_day) in tradable_keys
                )
            )
            value: float | None = None
            if valid:
                assert isinstance(open_price, (int, float))
                assert isinstance(close_price, (int, float))
                value = float(close_price / open_price - 1.0)
            rows.append((signal_day, instrument_id, start_day, end_day, value))
        output[horizon] = pl.DataFrame(
            rows,
            schema={
                "signal_date": pl.Date,
                "instrument_id": pl.String,
                "return_start": pl.Date,
                "return_end": pl.Date,
                "future_return": pl.Float64,
            },
            orient="row",
        ).sort("signal_date", "instrument_id")
    return output


def analyze(
    factors: pl.DataFrame,
    eligible: pl.DataFrame,
    future_returns: dict[int, pl.DataFrame],
    *,
    quantiles: int,
    minimum: int = MIN_CROSS_SECTION,
    ic_rolling_window: int = IC_ROLLING_WINDOW,
    ic_rolling_min_valid: int = IC_ROLLING_MIN_VALID,
    ic_quantile_probabilities: tuple[float, ...] = IC_QUANTILE_PROBABILITIES,
) -> dict[str, pl.DataFrame]:
    """计算每个因子和未来窗口的完整 IC 与分层研究诊断；该函数作为稳定公开 API保留在模块级。

    入参：
        factors：因子集合。
        eligible：准入证券。
        future_returns：未来收益收益序列。
        quantiles：分位组数。
        minimum：最小值。
        ic_rolling_window：IC滚动窗口。
        ic_rolling_min_valid：IC滚动下限有效样本。
        ic_quantile_probabilities：参与本次处理的``ic``分位组``probabilities``；调用方不得依赖未声明的顺序。
    返回值：
        返回``analyze``（``dict[str, pl.DataFrame]``）。
    异常：
        无。
    """

    def mean(values: list[float]) -> float | None:
        return None if not values else sum(values) / len(values)

    ic_analyzer = InformationCoefficientAnalyzer(
        rolling_window=ic_rolling_window,
        rolling_min_valid=ic_rolling_min_valid,
        quantile_probabilities=ic_quantile_probabilities,
    )
    if "signal_variant" not in factors.columns:
        factors = factors.with_columns(
            pl.lit(DIRECTION_ADJUSTED).alias("signal_variant")
        )
    refs = sorted(set(factors["factor_id"].to_list()))
    variants = sorted(set(factors["signal_variant"].to_list()))
    coverage_rows: list[dict[str, object]] = []
    ic_frames: list[pl.DataFrame] = []
    quantile_frames: list[pl.DataFrame] = []
    long_short_frames: list[pl.DataFrame] = []
    summary_rows: list[dict[str, object]] = []
    correlation_frames: list[pl.DataFrame] = []
    denominator = eligible.filter(pl.col("eligible")).group_by("signal_date").len()
    for variant in variants:
        variant_factors = factors.filter(pl.col("signal_variant") == variant)
        masked_frames: list[pl.DataFrame] = []
        for ref in refs:
            frame = variant_factors.filter(pl.col("factor_id") == ref)
            counts = frame.filter(pl.col("is_valid")).group_by("signal_date").len()
            masked = (
                frame.join(
                    counts.rename({"len": "valid_count"}),
                    on="signal_date",
                    how="left",
                )
                .with_columns(
                    (
                        pl.col("is_valid")
                        & (pl.col("valid_count").fill_null(0) >= minimum)
                    ).alias("is_valid")
                )
                .drop("valid_count")
            )
            masked_frames.append(masked)
            joined_counts = denominator.join(
                counts, on="signal_date", how="left"
            ).with_columns(pl.col("len_right").fill_null(0).alias("valid_count"))
            for row in joined_counts.iter_rows(named=True):
                total = int(row["len"])
                valid_count = int(row["valid_count"])
                coverage_rows.append(
                    {
                        "signal_variant": variant,
                        "factor_ref": ref,
                        "signal_date": row["signal_date"],
                        "eligible_count": total,
                        "valid_count": valid_count,
                        "coverage": valid_count / total if total else None,
                        "is_valid": valid_count >= minimum,
                        "quality_reason": None
                        if valid_count >= minimum
                        else "INSUFFICIENT_CROSS_SECTION",
                    }
                )
            for horizon, returns in future_returns.items():
                ic = ic_analyzer.daily(
                    frame,
                    returns,
                    minimum_cross_section=minimum,
                ).with_columns(
                    pl.lit(variant).alias("signal_variant"),
                    pl.lit(ref).alias("factor_ref"),
                    pl.lit(horizon).alias("horizon"),
                )
                quantile = quantile_future_returns(masked, returns, quantiles)
                paired_counts = quantile.group_by("signal_date").agg(
                    pl.col("count").sum().alias("paired_count")
                )
                quantile = (
                    quantile.join(paired_counts, on="signal_date", how="left")
                    .with_columns(
                        (
                            pl.col("is_empty") | (pl.col("paired_count") < minimum)
                        ).alias("is_empty"),
                        pl.when(pl.col("paired_count") >= minimum)
                        .then(pl.col("mean_return"))
                        .otherwise(pl.lit(None, dtype=pl.Float64))
                        .alias("mean_return"),
                        pl.lit(variant).alias("signal_variant"),
                        pl.lit(ref).alias("factor_ref"),
                        pl.lit(horizon).alias("horizon"),
                    )
                    .drop("paired_count")
                )
                long_short = long_short_returns(quantile).with_columns(
                    pl.lit(variant).alias("signal_variant"),
                    pl.lit(ref).alias("factor_ref"),
                    pl.lit(horizon).alias("horizon"),
                )
                ic_frames.append(ic)
                quantile_frames.append(quantile)
                long_short_frames.append(long_short)
                valid_ls = long_short.filter(pl.col("is_valid"))[
                    "long_short_return"
                ].drop_nulls()
                pearson_summary = ic_analyzer.summarize(ic, "pearson_ic")
                rank_summary = ic_analyzer.summarize(ic, "rank_ic")
                summary_rows.append(
                    {
                        "signal_variant": variant,
                        "factor_ref": ref,
                        "horizon": horizon,
                        **pearson_summary.columns("pearson_ic"),
                        **rank_summary.columns("rank_ic"),
                        "long_short_mean": mean(
                            cast(list[float], valid_ls.to_list())
                        ),
                    }
                )
        correlation_frames.append(
            factor_rank_correlation_matrix(
                pl.concat(masked_frames), minimum_pairs=minimum
            ).with_columns(pl.lit(variant).alias("signal_variant"))
        )
    return {
        "summary": pl.DataFrame(summary_rows).sort(
            "signal_variant", "factor_ref", "horizon"
        ),
        "coverage": pl.DataFrame(coverage_rows).sort(
            "signal_variant", "factor_ref", "signal_date"
        ),
        "ic": pl.concat(ic_frames).sort(
            "signal_variant", "factor_ref", "horizon", "signal_date"
        ),
        "quantile_returns": pl.concat(quantile_frames).sort(
            "signal_variant", "factor_ref", "horizon", "signal_date", "quantile"
        ),
        "long_short_returns": pl.concat(long_short_frames).sort(
            "signal_variant", "factor_ref", "horizon", "signal_date"
        ),
        "correlation": pl.concat(correlation_frames).sort(
            "signal_variant", "factor_x", "factor_y"
        ),
    }
