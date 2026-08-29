"""基于时点安全输入组装统一因子研究诊断结果。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from math import erfc, isfinite, sqrt
from typing import cast

import numpy as np
import polars as pl

from quant_research.factors.analysis import (
    InformationCoefficientAnalyzer,
    _AnalysisSupport,
    assign_quantiles,
    factor_rank_correlation_matrix,
    long_short_returns,
)

DIRECTION_ADJUSTED = "DIRECTION_ADJUSTED"
INDUSTRY_NEUTRALIZED = "INDUSTRY_NEUTRALIZED"
MARKET_CAP_NEUTRALIZED = "MARKET_CAP_NEUTRALIZED"
INDUSTRY_MARKET_CAP_NEUTRALIZED = "INDUSTRY_MARKET_CAP_NEUTRALIZED"
THEORETICAL_FORWARD_RETURN = "THEORETICAL_FORWARD_RETURN"
EXECUTABLE_FORWARD_RETURN = "EXECUTABLE_FORWARD_RETURN"
LABEL_KINDS = (THEORETICAL_FORWARD_RETURN, EXECUTABLE_FORWARD_RETURN)
IC_QUANTILE_PROBABILITIES = (0.05, 0.25, 0.5, 0.75, 0.95)
IC_ROLLING_MIN_VALID = 10
IC_ROLLING_WINDOW = 20
MIN_CROSS_SECTION = 30
_NORMAL_95 = 1.959963984540054

_COVERAGE_SCHEMA = {
    "signal_variant": pl.String,
    "factor_ref": pl.String,
    "signal_date": pl.Date,
    "eligible_count": pl.Int64,
    "valid_count": pl.Int64,
    "coverage": pl.Float64,
    "is_valid": pl.Boolean,
    "quality_reason": pl.String,
}

_INDUSTRY_COVERAGE_SCHEMA = {
    "signal_date": pl.Date,
    "taxonomy": pl.String,
    "unclassified_policy": pl.String,
    "eligible_count": pl.Int64,
    "classified_count": pl.Int64,
    "tombstone_count": pl.Int64,
    "missing_state_count": pl.Int64,
    "usable_count": pl.Int64,
    "classified_coverage": pl.Float64,
    "usable_coverage": pl.Float64,
}


@dataclass(frozen=True, slots=True)
class HacMetricSummary:
    """保存均值的 Newey-West/HAC 推断结果。

    入参：
        各字段分别记录均值、有效样本数、滞后阶数、标准误、检验统计量、
        双侧 p 值、置信区间和无效原因。
    返回值：
        返回冻结的 HAC 推断值对象。
    异常：
        无；字段一致性由生成该对象的分析器保证。
    """

    mean: float | None
    valid_count: int
    lag: int
    standard_error: float | None
    t_stat: float | None
    p_value: float | None
    ci_lower: float | None
    ci_upper: float | None
    invalid_reason: str | None

    def columns(self, prefix: str) -> dict[str, object]:
        """返回适合写入 Parquet 摘要的稳定列集合。

        入参：
            prefix：输出列的指标前缀。
        返回值：
            返回包含 HAC 推断各字段的稳定映射。
        异常：
            ValueError：前缀为空时抛出。
        """
        if not prefix:
            raise ValueError("HAC summary prefix must be nonempty")
        return {
            f"{prefix}_mean": self.mean,
            f"{prefix}_valid_count": self.valid_count,
            f"{prefix}_hac_lag": self.lag,
            f"{prefix}_hac_standard_error": self.standard_error,
            f"{prefix}_hac_t_stat": self.t_stat,
            f"{prefix}_hac_p_value": self.p_value,
            f"{prefix}_hac_ci_lower": self.ci_lower,
            f"{prefix}_hac_ci_upper": self.ci_upper,
            f"{prefix}_hac_invalid_reason": self.invalid_reason,
        }


class HacMeanAnalyzer:
    """以 Bartlett kernel 计算均值的异方差和自相关稳健推断。

    入参：
        无；该无状态分析器通过静态方法接收样本。
    返回值：
        返回 HacMeanAnalyzer 类型并提供确定性推断入口。
    异常：
        无；不可推断的样本通过结果原因码表达。
    """

    @staticmethod
    def summarize(
        values: list[float | None], horizon: int
    ) -> HacMetricSummary:
        """按完整信号会话轴和固定持有期滞后输出 HAC 均值推断。

        入参：
            values：按连续信号会话排序的观测值，无效会话以 ``None`` 占位。
            horizon：收益持有期，用于确定固定滞后阶数。
        返回值：
            返回包含均值、HAC 标准误和正态近似推断的冻结对象。
        异常：
            无；样本不足或长期方差非正时返回明确原因码。
        """
        samples = np.asarray(
            [
                value
                if value is not None and isfinite(value)
                else float("nan")
                for value in values
            ],
            dtype=float,
        )
        finite_mask = np.isfinite(samples)
        finite = samples[finite_mask]
        count = len(finite)
        lag = min(max(horizon - 1, 0), max(len(samples) - 1, 0))
        if count == 0:
            return HacMetricSummary(
                None,
                0,
                lag,
                None,
                None,
                None,
                None,
                None,
                "NO_VALID_OBSERVATIONS",
            )
        mean = float(np.mean(finite))
        if count < 2:
            return HacMetricSummary(
                mean,
                count,
                lag,
                None,
                None,
                None,
                None,
                None,
                "INSUFFICIENT_OBSERVATIONS",
            )
        centered = np.where(finite_mask, samples - mean, 0.0)
        long_run_variance = float(np.dot(centered, centered) / count)
        for offset in range(1, lag + 1):
            pair_mask = finite_mask[offset:] & finite_mask[:-offset]
            covariance = float(
                np.dot(
                    centered[offset:][pair_mask],
                    centered[:-offset][pair_mask],
                )
                / count
            )
            long_run_variance += (
                2.0 * (1.0 - offset / (lag + 1.0)) * covariance
            )
        if not isfinite(long_run_variance) or long_run_variance <= 0.0:
            return HacMetricSummary(
                mean,
                count,
                lag,
                None,
                None,
                None,
                None,
                None,
                "NONPOSITIVE_LONG_RUN_VARIANCE",
            )
        standard_error = sqrt(long_run_variance / count)
        t_stat = mean / standard_error
        p_value = min(1.0, max(0.0, erfc(abs(t_stat) / sqrt(2.0))))
        return HacMetricSummary(
            mean,
            count,
            lag,
            standard_error,
            t_stat,
            p_value,
            mean - _NORMAL_95 * standard_error,
            mean + _NORMAL_95 * standard_error,
            None,
        )


def build_future_returns(
    bars: pl.DataFrame,
    sessions: tuple[date, ...],
    eligible: pl.DataFrame,
    horizons: tuple[int, ...],
    executable_state: pl.DataFrame,
) -> dict[tuple[int, str], pl.DataFrame]:
    """构造理论和可执行 T+1 开盘至 T+h 收盘未来收益。

    该函数作为因子研究稳定模块级公开入口保留，统一生成不可静默丢样本的双标签表。

    入参：
        bars：前复权开盘价和收盘价。
        sessions：冻结交易日序列。
        eligible：逐日股票池资格。
        horizons：严格升序且唯一的正持有期。
        executable_state：未复权行情和证券状态生成的入场可执行性。
    返回值：
        返回以持有期和标签种类为键的未来收益表。
    异常：
        ValueError：输入缺列或持有期不满足约束时抛出。
    """
    required = {"instrument_id", "trade_date", "open", "close"}
    if not required.issubset(bars.columns):
        raise ValueError("adjusted bars are missing future-return columns")
    state_required = {
        "instrument_id",
        "trade_date",
        "is_listed",
        "is_suspended",
        "entry_limit_up",
    }
    if not state_required.issubset(executable_state.columns):
        raise ValueError("executable state is missing required columns")
    if tuple(sorted(set(horizons))) != horizons or any(
        value <= 0 for value in horizons
    ):
        raise ValueError(
            "horizons must be unique positive values in ascending order"
        )
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
    entry_state = executable_state.select(
        "instrument_id",
        pl.col("trade_date").alias("return_start"),
        pl.col("is_listed").alias("_entry_listed"),
        pl.col("is_suspended").alias("_entry_suspended"),
        "entry_limit_up",
    )
    exit_state = executable_state.select(
        "instrument_id",
        pl.col("trade_date").alias("return_end"),
        pl.col("is_listed").alias("_exit_listed"),
    )
    output: dict[tuple[int, str], pl.DataFrame] = {}
    for horizon in horizons:
        boundaries = session_frame.with_columns(
            pl.col("signal_date").shift(-1).alias("return_start"),
            pl.col("signal_date").shift(-horizon).alias("return_end"),
        )
        base = (
            eligible_rows.join(boundaries, on="signal_date", how="left")
            .join(
                entry_prices,
                on=["instrument_id", "return_start"],
                how="left",
            )
            .join(
                exit_prices,
                on=["instrument_id", "return_end"],
                how="left",
            )
            .join(
                entry_state,
                on=["instrument_id", "return_start"],
                how="left",
            )
            .join(
                exit_state,
                on=["instrument_id", "return_end"],
                how="left",
            )
        )
        raw_return = pl.col("_exit_close") / pl.col("_entry_open") - 1.0
        for label_kind in LABEL_KINDS:
            executable = label_kind == EXECUTABLE_FORWARD_RETURN
            invalid_reason = (
                pl.when(
                    pl.col("return_start").is_null()
                    | pl.col("return_end").is_null()
                )
                .then(pl.lit("INCOMPLETE_FORWARD_WINDOW"))
                .when(executable & ~pl.col("_entry_listed").fill_null(False))
                .then(pl.lit("NOT_LISTED_AT_ENTRY"))
                .when(executable & pl.col("_entry_suspended").fill_null(True))
                .then(pl.lit("ENTRY_SUSPENDED"))
                .when(executable & pl.col("entry_limit_up").fill_null(False))
                .then(pl.lit("ENTRY_LIMIT_UP"))
                .when(
                    pl.col("_entry_open").is_null()
                    | (pl.col("_entry_open") <= 0.0)
                )
                .then(pl.lit("MISSING_ENTRY_PRICE"))
                .when(
                    pl.col("_exit_close").is_null()
                    & ~pl.col("_exit_listed").fill_null(True)
                )
                .then(pl.lit("DELISTED_WITHOUT_EXIT_PRICE"))
                .when(
                    pl.col("_exit_close").is_null()
                    | (pl.col("_exit_close") <= 0.0)
                )
                .then(pl.lit("MISSING_EXIT_PRICE"))
                .when(~raw_return.is_finite().fill_null(False))
                .then(pl.lit("NONFINITE_RETURN"))
                .otherwise(pl.lit(None, dtype=pl.String))
            )
            output[(horizon, label_kind)] = (
                base.with_columns(invalid_reason.alias("invalid_reason"))
                .select(
                    "signal_date",
                    "instrument_id",
                    pl.lit(horizon, dtype=pl.Int64).alias("horizon"),
                    pl.lit(label_kind).alias("label_kind"),
                    "return_start",
                    "return_end",
                    pl.when(pl.col("invalid_reason").is_null())
                    .then(raw_return)
                    .otherwise(pl.lit(None, dtype=pl.Float64))
                    .cast(pl.Float64)
                    .alias("future_return"),
                    pl.col("invalid_reason").is_null().alias("is_valid"),
                    "invalid_reason",
                )
                .sort("signal_date", "instrument_id")
            )
    return output


def analyze(
    factors: pl.DataFrame,
    eligible: pl.DataFrame,
    future_returns: Mapping[tuple[int, str], pl.DataFrame],
    *,
    quantiles: int,
    cost_bps_scenarios: tuple[int, ...],
    minimum: int = MIN_CROSS_SECTION,
    ic_rolling_window: int = IC_ROLLING_WINDOW,
    ic_rolling_min_valid: int = IC_ROLLING_MIN_VALID,
    ic_quantile_probabilities: tuple[float, ...] = IC_QUANTILE_PROBABILITIES,
) -> dict[str, pl.DataFrame]:
    """计算覆盖、IC、分层、换手和成本代理完整诊断。

    该函数作为因子研究稳定模块级公开入口保留，集中发布固定 Schema 的可信产物。

    入参：
        factors：方向调整并可选行业中性化的因子值。
        eligible：逐日股票池资格。
        future_returns：按持有期和标签隔离的未来收益。
        quantiles：分位组数量。
        cost_bps_scenarios：严格升序且唯一的成本基点情景。
        minimum：计算日度 IC 的最小截面数。
        ic_rolling_window：IC 滚动窗口。
        ic_rolling_min_valid：IC 滚动窗口最少有效样本数。
        ic_quantile_probabilities：IC 描述统计分位点。
    返回值：
        返回覆盖、标签质量、IC、分层、换手和成本等固定产物表。
    异常：
        ValueError：输入 Schema 或配置违反研究契约时抛出。
    """
    analyzer = _StudyAnalyzer(
        quantiles=quantiles,
        minimum=minimum,
        cost_bps_scenarios=cost_bps_scenarios,
        ic_analyzer=InformationCoefficientAnalyzer(
            rolling_window=ic_rolling_window,
            rolling_min_valid=ic_rolling_min_valid,
            quantile_probabilities=ic_quantile_probabilities,
        ),
    )
    return analyzer.run(factors, eligible, future_returns)


class _StudyAnalyzer:
    """承载一次因子研究的确定性多表分析。"""

    def __init__(
        self,
        *,
        quantiles: int,
        minimum: int,
        cost_bps_scenarios: tuple[int, ...],
        ic_analyzer: InformationCoefficientAnalyzer,
    ) -> None:
        self._quantiles = quantiles
        self._minimum = minimum
        self._cost_bps_scenarios = cost_bps_scenarios
        self._ic_analyzer = ic_analyzer

    def run(
        self,
        factors: pl.DataFrame,
        eligible: pl.DataFrame,
        future_returns: Mapping[tuple[int, str], pl.DataFrame],
    ) -> dict[str, pl.DataFrame]:
        if "signal_variant" not in factors.columns:
            factors = factors.with_columns(
                pl.lit(DIRECTION_ADJUSTED).alias("signal_variant")
            )
        denominator = (
            eligible.filter(pl.col("eligible"))
            .group_by("signal_date")
            .len()
            .rename({"len": "eligible_count"})
        )
        signal_dates = (
            eligible.select("signal_date").unique().sort("signal_date")
        )
        coverage_rows: list[dict[str, object]] = []
        summary_rows: list[dict[str, object]] = []
        ic_frames: list[pl.DataFrame] = []
        quantile_frames: list[pl.DataFrame] = []
        long_short_frames: list[pl.DataFrame] = []
        monotonicity_frames: list[pl.DataFrame] = []
        turnover_frames: list[pl.DataFrame] = []
        cost_rows: list[dict[str, object]] = []
        correlation_frames: list[pl.DataFrame] = []
        refs = sorted(set(cast(list[str], factors["factor_id"].to_list())))
        variants = sorted(
            set(cast(list[str], factors["signal_variant"].to_list()))
        )
        ordered_returns = tuple(sorted(future_returns.items()))
        for _, returns in ordered_returns:
            _AnalysisSupport._validate_future(returns)
        for variant in variants:
            variant_factors = factors.filter(
                pl.col("signal_variant") == variant
            )
            correlation_frames.append(
                factor_rank_correlation_matrix(
                    variant_factors, minimum_pairs=self._minimum
                ).with_columns(pl.lit(variant).alias("signal_variant"))
            )
            prepared: list[
                tuple[
                    str,
                    pl.DataFrame,
                    dict[date, int],
                    pl.DataFrame,
                    pl.DataFrame,
                    pl.DataFrame,
                ]
            ] = []
            for factor_ref in refs:
                frame = variant_factors.filter(
                    pl.col("factor_id") == factor_ref
                )
                if frame.is_empty():
                    continue
                counts = (
                    frame.filter(pl.col("is_valid"))
                    .group_by("signal_date")
                    .len()
                    .rename({"len": "valid_count"})
                )
                joined_counts = denominator.join(
                    counts, on="signal_date", how="left"
                ).with_columns(pl.col("valid_count").fill_null(0))
                for row in joined_counts.iter_rows(named=True):
                    total = int(row["eligible_count"])
                    valid_count = int(row["valid_count"])
                    coverage_rows.append(
                        {
                            "signal_variant": variant,
                            "factor_ref": factor_ref,
                            "signal_date": row["signal_date"],
                            "eligible_count": total,
                            "valid_count": valid_count,
                            "coverage": valid_count / total if total else None,
                            "is_valid": valid_count >= self._minimum,
                            "quality_reason": (
                                None
                                if valid_count >= self._minimum
                                else "INSUFFICIENT_CROSS_SECTION"
                            ),
                        }
                    )
                valid_factors = _AnalysisSupport._valid_factors(frame)
                factor_counts = {
                    cast(date, signal_date): int(count)
                    for signal_date, count in valid_factors.group_by(
                        "signal_date"
                    )
                    .len()
                    .iter_rows()
                }
                removed_dates = counts.filter(
                    pl.col("valid_count") < self._minimum
                ).select("signal_date")
                additional_valid_factors = valid_factors.join(
                    removed_dates,
                    on="signal_date",
                    how="inner",
                ).select("signal_date", "instrument_id", "value")
                del valid_factors
                masked = self._minimum_mask(frame)
                assigned = assign_quantiles(masked, self._quantiles)
                del masked
                turnover = self._turnover(assigned).with_columns(
                    pl.lit(variant).alias("signal_variant"),
                    pl.lit(factor_ref).alias("factor_ref"),
                )
                turnover_frames.append(turnover)
                prepared.append(
                    (
                        factor_ref,
                        frame.select("signal_date").unique(),
                        factor_counts,
                        additional_valid_factors,
                        assigned,
                        turnover,
                    )
                )
            for (horizon, label_kind), returns in ordered_returns:
                valid_returns = _AnalysisSupport._valid_returns(returns)
                label_counts = (
                    returns.filter(pl.col("is_valid"))
                    .group_by("signal_date")
                    .len()
                    .rename({"len": "label_valid_count"})
                )
                for (
                    factor_ref,
                    factor_dates,
                    factor_counts,
                    additional_valid_factors,
                    assigned,
                    turnover,
                ) in prepared:
                    ic = self._ic(
                        factor_dates,
                        factor_counts,
                        additional_valid_factors,
                        assigned,
                        valid_returns,
                        label_counts,
                        denominator,
                    ).with_columns(
                        pl.lit(variant).alias("signal_variant"),
                        pl.lit(factor_ref).alias("factor_ref"),
                        pl.lit(horizon).alias("horizon"),
                        pl.lit(label_kind).alias("label_kind"),
                    )
                    quantile = _AnalysisSupport._quantile_returns_from_assigned(
                        assigned,
                        valid_returns,
                        self._quantiles,
                    )
                    paired = quantile.group_by("signal_date").agg(
                        pl.col("count").sum().alias("paired_count")
                    )
                    quantile = (
                        quantile.join(paired, on="signal_date", how="left")
                        .with_columns(
                            (
                                pl.col("is_empty")
                                | (pl.col("paired_count") < self._minimum)
                            ).alias("is_empty"),
                            pl.when(pl.col("paired_count") >= self._minimum)
                            .then(pl.col("mean_return"))
                            .otherwise(None)
                            .alias("mean_return"),
                            pl.lit(variant).alias("signal_variant"),
                            pl.lit(factor_ref).alias("factor_ref"),
                            pl.lit(horizon).alias("horizon"),
                            pl.lit(label_kind).alias("label_kind"),
                        )
                        .drop("paired_count")
                    )
                    long_short = long_short_returns(quantile).with_columns(
                        pl.lit(variant).alias("signal_variant"),
                        pl.lit(factor_ref).alias("factor_ref"),
                        pl.lit(horizon).alias("horizon"),
                        pl.lit(label_kind).alias("label_kind"),
                    )
                    monotonicity = self._monotonicity(quantile).with_columns(
                        pl.lit(variant).alias("signal_variant"),
                        pl.lit(factor_ref).alias("factor_ref"),
                        pl.lit(horizon).alias("horizon"),
                        pl.lit(label_kind).alias("label_kind"),
                    )
                    costs, break_even = self._costs(
                        long_short,
                        turnover,
                        signal_dates,
                        horizon,
                        variant,
                        factor_ref,
                        label_kind,
                    )
                    cost_rows.extend(costs)
                    summary_rows.append(
                        self._summary(
                            ic,
                            long_short,
                            monotonicity,
                            turnover,
                            signal_dates,
                            horizon,
                            variant,
                            factor_ref,
                            label_kind,
                            break_even,
                        )
                    )
                    ic_frames.append(ic)
                    quantile_frames.append(quantile)
                    long_short_frames.append(long_short)
                    monotonicity_frames.append(monotonicity)
        return {
            "summary": pl.DataFrame(summary_rows).sort(
                "signal_variant", "label_kind", "factor_ref", "horizon"
            ),
            "coverage": pl.DataFrame(
                coverage_rows, schema=_COVERAGE_SCHEMA
            ).sort(
                "signal_variant", "factor_ref", "signal_date"
            ),
            "label_quality": self._label_quality(future_returns),
            "industry_coverage": pl.DataFrame(
                schema=_INDUSTRY_COVERAGE_SCHEMA
            ),
            "ic": pl.concat(ic_frames).sort(
                "signal_variant",
                "label_kind",
                "factor_ref",
                "horizon",
                "signal_date",
            ),
            "quantile_returns": pl.concat(quantile_frames).sort(
                "signal_variant",
                "label_kind",
                "factor_ref",
                "horizon",
                "signal_date",
                "quantile",
            ),
            "long_short_returns": pl.concat(long_short_frames).sort(
                "signal_variant",
                "label_kind",
                "factor_ref",
                "horizon",
                "signal_date",
            ),
            "monotonicity": pl.concat(monotonicity_frames).sort(
                "signal_variant",
                "label_kind",
                "factor_ref",
                "horizon",
                "signal_date",
            ),
            "turnover": pl.concat(turnover_frames).sort(
                "signal_variant", "factor_ref", "signal_date"
            ),
            "cost_scenarios": pl.DataFrame(cost_rows).sort(
                "signal_variant",
                "label_kind",
                "factor_ref",
                "horizon",
                "cost_bps",
            ),
            "correlation": pl.concat(correlation_frames).sort(
                "signal_variant", "factor_x", "factor_y"
            ),
        }

    def _minimum_mask(self, frame: pl.DataFrame) -> pl.DataFrame:
        counts = (
            frame.filter(pl.col("is_valid"))
            .group_by("signal_date")
            .len()
            .rename({"len": "_valid_count"})
        )
        return (
            frame.join(counts, on="signal_date", how="left")
            .with_columns(
                (
                    pl.col("is_valid")
                    & (pl.col("_valid_count").fill_null(0) >= self._minimum)
                ).alias("is_valid")
            )
            .drop("_valid_count")
        )

    def _ic(
        self,
        factor_dates: pl.DataFrame,
        factor_counts: dict[date, int],
        additional_valid_factors: pl.DataFrame,
        assigned: pl.DataFrame,
        valid_returns: pl.DataFrame,
        label_counts: pl.DataFrame,
        denominator: pl.DataFrame,
        prepared_pairs: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        daily = self._ic_analyzer._daily_from_valid_inputs(
            factor_dates,
            assigned,
            valid_returns,
            minimum_cross_section=self._minimum,
            factor_counts_override=factor_counts,
            additional_valid_factors=additional_valid_factors,
            valid_factors_sorted=True,
            prepared_pairs=prepared_pairs,
        )
        return (
            daily.join(denominator, on="signal_date", how="left")
            .join(label_counts, on="signal_date", how="left")
            .with_columns(
                pl.col("label_valid_count").fill_null(0),
                pl.when(pl.col("eligible_count") > 0)
                .then(pl.col("sample_count") / pl.col("eligible_count"))
                .otherwise(None)
                .alias("pair_coverage"),
            )
        )

    def _summary(
        self,
        ic: pl.DataFrame,
        long_short: pl.DataFrame,
        monotonicity: pl.DataFrame,
        turnover: pl.DataFrame,
        signal_dates: pl.DataFrame,
        horizon: int,
        variant: str,
        factor_ref: str,
        label_kind: str,
        break_even: float | None,
    ) -> dict[str, object]:
        pearson = self._ic_analyzer.summarize(ic, "pearson_ic")
        rank = self._ic_analyzer.summarize(ic, "rank_ic")
        valid_ls = cast(
            list[float],
            long_short.filter(pl.col("is_valid"))[
                "long_short_return"
            ].drop_nulls().to_list(),
        )
        valid_mono = cast(
            list[float],
            monotonicity.filter(pl.col("is_valid"))[
                "quantile_rank_correlation"
            ].drop_nulls().to_list(),
        )
        valid_turnover = turnover.filter(pl.col("turnover_is_valid"))
        return {
            "signal_variant": variant,
            "label_kind": label_kind,
            "factor_ref": factor_ref,
            "horizon": horizon,
            **pearson.columns("pearson_ic"),
            **rank.columns("rank_ic"),
            **HacMeanAnalyzer.summarize(
                self._hac_axis_values(
                    signal_dates, ic, "pearson_ic", "is_valid"
                ),
                horizon,
            ).columns("pearson_ic_hac"),
            **HacMeanAnalyzer.summarize(
                self._hac_axis_values(
                    signal_dates, ic, "rank_ic", "is_valid"
                ),
                horizon,
            ).columns("rank_ic_hac"),
            **HacMeanAnalyzer.summarize(
                self._hac_axis_values(
                    signal_dates,
                    long_short,
                    "long_short_return",
                    "is_valid",
                ),
                horizon,
            ).columns("long_short"),
            "long_short_positive_rate": self._positive_rate(valid_ls),
            "monotonicity_mean": self._mean(valid_mono),
            "monotonic_day_rate": self._positive_rate(valid_mono),
            "rank_autocorrelation_mean": self._mean(
                cast(
                    list[float],
                    turnover.filter(pl.col("rank_is_valid"))[
                        "rank_autocorrelation"
                    ].drop_nulls().to_list(),
                )
            ),
            "high_quantile_turnover_mean": (
                cast(float | None, valid_turnover["high_quantile_turnover"].mean())
                if not valid_turnover.is_empty()
                else None
            ),
            "low_quantile_turnover_mean": (
                cast(float | None, valid_turnover["low_quantile_turnover"].mean())
                if not valid_turnover.is_empty()
                else None
            ),
            "total_turnover_mean": (
                cast(float | None, valid_turnover["total_turnover"].mean())
                if not valid_turnover.is_empty()
                else None
            ),
            "break_even_cost_bps": break_even,
        }

    def _monotonicity(self, quantile: pl.DataFrame) -> pl.DataFrame:
        rows: list[tuple[object, ...]] = []
        for group in quantile.partition_by(
            "signal_date", maintain_order=False
        ):
            day = group["signal_date"].item(0)
            valid = group.filter(pl.col("mean_return").is_not_null()).sort(
                "quantile"
            )
            if valid.height < 3:
                rows.append(
                    (
                        day,
                        valid.height,
                        None,
                        None,
                        None,
                        None,
                        False,
                        "INSUFFICIENT_VALID_QUANTILES",
                    )
                )
                continue
            x = np.asarray(valid["quantile"].to_list(), dtype=float)
            y = np.asarray(valid["mean_return"].to_list(), dtype=float)
            rank_correlation = float(
                np.corrcoef(x, self._average_ranks(y))[0, 1]
            )
            centered_x = x - np.mean(x)
            slope = float(
                np.dot(centered_x, y - np.mean(y))
                / np.dot(centered_x, centered_x)
            )
            inversions = int(np.sum(np.diff(y) < 0.0))
            terminal = float(y[-1] - y[0])
            rows.append(
                (
                    day,
                    len(y),
                    rank_correlation,
                    slope,
                    inversions,
                    terminal,
                    True,
                    None,
                )
            )
        return pl.DataFrame(
            rows,
            schema={
                "signal_date": pl.Date,
                "valid_quantile_count": pl.Int64,
                "quantile_rank_correlation": pl.Float64,
                "trend_slope": pl.Float64,
                "adjacent_inversion_count": pl.Int64,
                "terminal_spread": pl.Float64,
                "is_valid": pl.Boolean,
                "invalid_reason": pl.String,
            },
            orient="row",
        ).sort("signal_date")

    def _turnover(self, assigned: pl.DataFrame) -> pl.DataFrame:
        grouped = assigned.group_by("signal_date", maintain_order=True).len()
        rows: list[tuple[object, ...]] = []
        previous: pl.DataFrame | None = None
        offset = 0
        for signal_date, count in grouped.iter_rows():
            day = cast(date, signal_date)
            size = int(count)
            current = assigned.slice(offset, size).filter(
                pl.col("instrument_id").is_not_null()
            )
            if previous is None:
                rows.append(
                    (
                        day,
                        None,
                        None,
                        None,
                        None,
                        False,
                        False,
                        "NO_PREVIOUS_SIGNAL_DATE",
                    )
                )
                previous = current
                offset += size
                continue
            rank = self._rank_autocorrelation(previous, current)
            low = self._leg_turnover(previous, current, 1)
            high = self._leg_turnover(previous, current, self._quantiles)
            turnover_valid = low is not None and high is not None
            total_turnover = (
                low + high if low is not None and high is not None else None
            )
            rows.append(
                (
                    day,
                    rank,
                    low,
                    high,
                    total_turnover,
                    rank is not None,
                    turnover_valid,
                    (
                        None
                        if turnover_valid
                        else "MISSING_TERMINAL_QUANTILE"
                    ),
                )
            )
            previous = current
            offset += size
        return pl.DataFrame(
            rows,
            schema={
                "signal_date": pl.Date,
                "rank_autocorrelation": pl.Float64,
                "low_quantile_turnover": pl.Float64,
                "high_quantile_turnover": pl.Float64,
                "total_turnover": pl.Float64,
                "rank_is_valid": pl.Boolean,
                "turnover_is_valid": pl.Boolean,
                "invalid_reason": pl.String,
            },
            orient="row",
        ).sort("signal_date")

    def _costs(
        self,
        long_short: pl.DataFrame,
        turnover: pl.DataFrame,
        signal_dates: pl.DataFrame,
        horizon: int,
        variant: str,
        factor_ref: str,
        label_kind: str,
    ) -> tuple[list[dict[str, object]], float | None]:
        aligned = (
            signal_dates.join(
                long_short.filter(pl.col("is_valid")).select(
                    "signal_date", "long_short_return"
                ),
                on="signal_date",
                how="left",
            )
            .join(
                turnover.filter(pl.col("turnover_is_valid")).select(
                    "signal_date", "total_turnover"
                ),
                on="signal_date",
                how="left",
            )
            .sort("signal_date")
        )
        valid_aligned = aligned.filter(
            pl.col("long_short_return").is_not_null()
            & pl.col("long_short_return").is_finite()
            & pl.col("total_turnover").is_not_null()
            & pl.col("total_turnover").is_finite()
        )
        gross = cast(list[float], valid_aligned["long_short_return"].to_list())
        turnover_values = cast(
            list[float], valid_aligned["total_turnover"].to_list()
        )
        gross_sum, turnover_sum = sum(gross), sum(turnover_values)
        break_even = (
            10_000.0 * gross_sum / turnover_sum
            if gross_sum > 0.0 and turnover_sum > 0.0
            else None
        )
        rows: list[dict[str, object]] = []
        for bps in self._cost_bps_scenarios:
            net = cast(
                list[float | None],
                aligned.select(
                    pl.when(
                        pl.col("long_short_return").is_not_null()
                        & pl.col("long_short_return").is_finite()
                        & pl.col("total_turnover").is_not_null()
                        & pl.col("total_turnover").is_finite()
                    )
                    .then(
                        pl.col("long_short_return")
                        - pl.col("total_turnover") * bps / 10_000.0
                    )
                    .otherwise(None)
                    .alias("net_spread")
                )["net_spread"].to_list(),
            )
            summary = HacMeanAnalyzer.summarize(net, horizon)
            rows.append(
                {
                    "signal_variant": variant,
                    "label_kind": label_kind,
                    "factor_ref": factor_ref,
                    "horizon": horizon,
                    "cost_bps": bps,
                    "aligned_date_count": len(gross),
                    "gross_spread_mean": self._mean(gross),
                    "mean_total_turnover": self._mean(turnover_values),
                    "estimated_cost_drag_mean": (
                        None
                        if not turnover_values
                        else cast(float, self._mean(turnover_values))
                        * bps
                        / 10_000.0
                    ),
                    **summary.columns("net_spread"),
                    "break_even_cost_bps": break_even,
                    "break_even_invalid_reason": (
                        None
                        if break_even is not None
                        else (
                            "NONPOSITIVE_GROSS_SPREAD"
                            if gross_sum <= 0.0
                            else "ZERO_TOTAL_TURNOVER"
                        )
                    ),
                }
            )
        return rows, break_even

    @staticmethod
    def _hac_axis_values(
        signal_dates: pl.DataFrame,
        frame: pl.DataFrame,
        value_column: str,
        valid_column: str,
    ) -> list[float | None]:
        """把有效指标左对齐到完整信号会话轴并保留无效日占位。

        入参：完整日期轴、指标表、数值列和有效性列。
        返回值：返回按信号会话排序且以空值保留缺口的数值序列。
        异常：输入缺列或日期键重复时传播 Polars 对应异常。
        """
        return cast(
            list[float | None],
            signal_dates.join(
                frame.filter(
                    pl.col(valid_column)
                    & pl.col(value_column).is_not_null()
                    & pl.col(value_column).is_finite()
                ).select("signal_date", value_column),
                on="signal_date",
                how="left",
            ).sort("signal_date")[value_column].to_list(),
        )

    def _label_quality(
        self,
        future_returns: Mapping[tuple[int, str], pl.DataFrame],
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for (horizon, label_kind), frame in sorted(future_returns.items()):
            prepared = frame.select(
                "signal_date",
                pl.col("invalid_reason").fill_null("VALID").alias("reason"),
            )
            totals = prepared.group_by("signal_date").len().rename(
                {"len": "eligible_count"}
            ).with_columns(pl.col("eligible_count").cast(pl.Int64))
            frames.append(
                prepared.group_by("signal_date", "reason")
                .len()
                .rename({"len": "count"})
                .with_columns(pl.col("count").cast(pl.Int64))
                .join(totals, on="signal_date", how="left")
                .with_columns(
                    pl.lit(label_kind).alias("label_kind"),
                    pl.lit(horizon, dtype=pl.Int64).alias("horizon"),
                    (pl.col("count") / pl.col("eligible_count")).alias(
                        "rate"
                    ),
                )
                .select(
                    "label_kind",
                    "horizon",
                    "signal_date",
                    "reason",
                    "count",
                    "eligible_count",
                    "rate",
                )
            )
        return pl.concat(frames).sort(
            "label_kind", "horizon", "signal_date", "reason"
        )

    @staticmethod
    def _rank_autocorrelation(
        previous: pl.DataFrame, current: pl.DataFrame
    ) -> float | None:
        paired = previous.select(
            "instrument_id", pl.col("value").alias("previous")
        ).join(
            current.select(
                "instrument_id", pl.col("value").alias("current")
            ),
            on="instrument_id",
            how="inner",
        )
        if paired.height < 2:
            return None
        value = float(
            np.corrcoef(
                _StudyAnalyzer._average_ranks(
                    paired["previous"].to_numpy()
                ),
                _StudyAnalyzer._average_ranks(paired["current"].to_numpy()),
            )[0, 1]
        )
        return value if isfinite(value) else None

    @staticmethod
    def _leg_turnover(
        previous: pl.DataFrame, current: pl.DataFrame, quantile: int
    ) -> float | None:
        previous_ids = cast(
            list[str],
            previous.filter(pl.col("quantile") == quantile)[
                "instrument_id"
            ].to_list(),
        )
        current_ids = cast(
            list[str],
            current.filter(pl.col("quantile") == quantile)[
                "instrument_id"
            ].to_list(),
        )
        if not previous_ids or not current_ids:
            return None
        previous_weights = {
            item: 1.0 / len(previous_ids) for item in previous_ids
        }
        current_weights = {
            item: 1.0 / len(current_ids) for item in current_ids
        }
        return 0.5 * sum(
            abs(
                current_weights.get(item, 0.0)
                - previous_weights.get(item, 0.0)
            )
            for item in set(previous_weights) | set(current_weights)
        )

    @staticmethod
    def _average_ranks(values: np.ndarray) -> np.ndarray:
        return _AnalysisSupport._ranks(values)

    @staticmethod
    def _mean(values: list[float]) -> float | None:
        return None if not values else float(np.mean(values))

    @staticmethod
    def _positive_rate(values: list[float]) -> float | None:
        return (
            None
            if not values
            else sum(value > 0.0 for value in values) / len(values)
        )


__all__ = [
    "DIRECTION_ADJUSTED",
    "EXECUTABLE_FORWARD_RETURN",
    "INDUSTRY_MARKET_CAP_NEUTRALIZED",
    "INDUSTRY_NEUTRALIZED",
    "LABEL_KINDS",
    "MARKET_CAP_NEUTRALIZED",
    "THEORETICAL_FORWARD_RETURN",
    "HacMeanAnalyzer",
    "analyze",
    "build_future_returns",
]
