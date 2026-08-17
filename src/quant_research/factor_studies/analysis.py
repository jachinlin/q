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
    session_frame = pl.DataFrame(
        {"signal_date": pl.Series(sessions, dtype=pl.Date)}
    )
    eligible_rows = eligible.filter(pl.col("eligible")).select(
        "signal_date", "instrument_id"
    )
    entry_prices = bars.select(
        "instrument_id",
        pl.col("trade_date").alias("return_start"),
        pl.col("open").cast(pl.Float64, strict=False).alias("_entry_open"),
    )
    exit_prices = bars.select(
        "instrument_id",
        pl.col("trade_date").alias("return_end"),
        pl.col("close").cast(pl.Float64, strict=False).alias("_exit_close"),
    )
    tradable_entries: pl.DataFrame | None = None
    if tradability is not None:
        required_status = {"instrument_id", "trade_date", "is_listed", "is_suspended"}
        if not required_status.issubset(tradability.columns):
            raise ValueError("tradability data is missing required columns")
        tradable_entries = (
            tradability.filter(
                pl.col("is_listed").fill_null(False)
                & ~pl.col("is_suspended").fill_null(True)
            )
            .select(
                "instrument_id",
                pl.col("trade_date").alias("return_start"),
                pl.lit(True).alias("_entry_tradable"),
            )
            .unique(["instrument_id", "return_start"])
        )
    output: dict[int, pl.DataFrame] = {}
    for horizon in horizons:
        boundaries = session_frame.with_columns(
            pl.col("signal_date").shift(-1).alias("return_start"),
            pl.col("signal_date").shift(-horizon).alias("return_end"),
        )
        joined = (
            eligible_rows.join(boundaries, on="signal_date", how="left")
            .join(entry_prices, on=["instrument_id", "return_start"], how="left")
            .join(exit_prices, on=["instrument_id", "return_end"], how="left")
        )
        if tradable_entries is not None:
            joined = joined.join(
                tradable_entries,
                on=["instrument_id", "return_start"],
                how="left",
            )
            entry_tradable = pl.col("_entry_tradable").fill_null(False)
        else:
            entry_tradable = pl.lit(True)
        raw_return = pl.col("_exit_close") / pl.col("_entry_open") - 1.0
        valid = (
            pl.col("return_start").is_not_null()
            & pl.col("return_end").is_not_null()
            & pl.col("_entry_open").is_not_null()
            & pl.col("_entry_open").is_finite()
            & (pl.col("_entry_open") > 0.0)
            & pl.col("_exit_close").is_not_null()
            & pl.col("_exit_close").is_finite()
            & (pl.col("_exit_close") > 0.0)
            & raw_return.is_finite()
            & entry_tradable
        ).fill_null(False)
        output[horizon] = (
            joined.select(
                "signal_date",
                "instrument_id",
                "return_start",
                "return_end",
                pl.when(valid)
                .then(raw_return)
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("future_return"),
            )
            .sort("signal_date", "instrument_id")
        )
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
