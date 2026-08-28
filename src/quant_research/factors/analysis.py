"""提供因子与统计分析相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import cast

import numpy as np
import polars as pl


@dataclass(frozen=True, slots=True)
class IcMetricSummary:
    """保存一种 IC 口径的确定性描述统计。

    入参：
        mean：均值。
        sample_std：样本标准差。
        icir_unannualized：ICIR未年化。
        positive_rate：正值比例。
        p05：``p05``。
        p25：``p25``。
        p50：``p50``。
        p75：``p75``。
        p95：``p95``。
        valid_date_count：有效样本日期``count``。
        返回完成字段规范化和不变量校验的对象。
        positive_streak_start：正值连续区间开始日期。
        positive_streak_end：正值连续区间结束日期。
        返回完成字段规范化和不变量校验的对象。
        negative_streak_start：负值连续区间开始日期。
        negative_streak_end：负值连续区间结束日期。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    mean: float | None
    sample_std: float | None
    icir_unannualized: float | None
    positive_rate: float | None
    p05: float | None
    p25: float | None
    p50: float | None
    p75: float | None
    p95: float | None
    valid_date_count: int
    max_positive_streak: int
    positive_streak_start: date | None
    positive_streak_end: date | None
    max_negative_streak: int
    negative_streak_start: date | None
    negative_streak_end: date | None

    def columns(self, prefix: str) -> dict[str, object]:
        """使用指定前缀返回可写入研究摘要表的扁平列。

        入参：
            prefix：``prefix``。
        返回值：
            返回``columns``（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not isinstance(prefix, str) or not prefix:
            raise ValueError("IC summary prefix must be a nonempty string")
        return {
            f"{prefix}_mean": self.mean,
            f"{prefix}_sample_std": self.sample_std,
            f"{prefix}ir_unannualized": self.icir_unannualized,
            f"{prefix}_positive_rate": self.positive_rate,
            f"{prefix}_p05": self.p05,
            f"{prefix}_p25": self.p25,
            f"{prefix}_p50": self.p50,
            f"{prefix}_p75": self.p75,
            f"{prefix}_p95": self.p95,
            f"{prefix}_valid_date_count": self.valid_date_count,
            f"{prefix}_max_positive_streak": self.max_positive_streak,
            f"{prefix}_positive_streak_start": self.positive_streak_start,
            f"{prefix}_positive_streak_end": self.positive_streak_end,
            f"{prefix}_max_negative_streak": self.max_negative_streak,
            f"{prefix}_negative_streak_start": self.negative_streak_start,
            f"{prefix}_negative_streak_end": self.negative_streak_end,
        }


class InformationCoefficientAnalyzer:
    """统一计算日度 Pearson/Rank IC 及其时间序列诊断。

    入参：
        rolling_window：滚动窗口。
        rolling_min_valid：滚动下限有效样本。
        quantile_probabilities：参与本次处理的分位组``probabilities``；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def __init__(
        self,
        *,
        rolling_window: int,
        rolling_min_valid: int,
        quantile_probabilities: tuple[float, ...] = (0.05, 0.25, 0.5, 0.75, 0.95),
    ) -> None:
        """校验并保存滚动窗口、最小观察数及汇总分位点。"""
        if type(rolling_window) is not int or rolling_window < 2:
            raise ValueError("rolling_window must be an integer of at least 2")
        if (
            type(rolling_min_valid) is not int
            or rolling_min_valid < 2
            or rolling_min_valid > rolling_window
        ):
            raise ValueError("rolling_min_valid must be from 2 through rolling_window")
        if (
            len(quantile_probabilities) != 5
            or tuple(sorted(quantile_probabilities)) != quantile_probabilities
            or any(value < 0.0 or value > 1.0 for value in quantile_probabilities)
        ):
            raise ValueError(
                "quantile_probabilities must be five ordered probabilities"
            )
        self._rolling_window = rolling_window
        self._rolling_min_valid = rolling_min_valid
        self._quantile_probabilities = quantile_probabilities

    def daily(
        self,
        factors: pl.DataFrame,
        future_returns: pl.DataFrame,
        *,
        minimum_cross_section: int,
    ) -> pl.DataFrame:
        """按信号日计算两类 IC，并补充滚动、累计和无效原因。

        入参：
            factors：因子集合。
            future_returns：未来收益收益序列。
            返回日频（``pl.DataFrame``）。
        返回值：
            返回日频（``pl.DataFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if type(minimum_cross_section) is not int or minimum_cross_section < 2:
            raise ValueError("minimum_cross_section must be an integer of at least 2")
        _AnalysisSupport._unique(factors, "factors")
        _AnalysisSupport._validate_future(future_returns)
        valid_factors = _AnalysisSupport._valid_factors(factors)
        return self._daily_from_valid_inputs(
            factors,
            valid_factors,
            _AnalysisSupport._valid_returns(future_returns),
            minimum_cross_section=minimum_cross_section,
        )

    def _daily_from_valid_inputs(
        self,
        factors: pl.DataFrame,
        valid_factors: pl.DataFrame,
        valid_returns: pl.DataFrame,
        *,
        minimum_cross_section: int,
        factor_counts_override: dict[date, int] | None = None,
        additional_valid_factors: pl.DataFrame | None = None,
        valid_factors_sorted: bool = False,
    ) -> pl.DataFrame:
        """从已校验且已过滤的输入按连续日期分区计算 IC。"""
        prepared_factors = valid_factors.filter(
            pl.col("instrument_id").is_not_null()
        ).select("signal_date", "instrument_id", "value")
        if not valid_factors_sorted:
            prepared_factors = prepared_factors.sort(
                "signal_date", "instrument_id"
            )
        pairs = prepared_factors.join(
            valid_returns,
            on=["signal_date", "instrument_id"],
            how="inner",
            maintain_order="left",
        )
        if (
            additional_valid_factors is not None
            and not additional_valid_factors.is_empty()
        ):
            pairs = pl.concat(
                [
                    pairs,
                    additional_valid_factors.select(
                    "signal_date", "instrument_id", "value"
                    ).join(
                        valid_returns,
                        on=["signal_date", "instrument_id"],
                        how="inner",
                        maintain_order="left",
                    ),
                ]
            ).sort("signal_date", "instrument_id")
        factor_counts = factor_counts_override or {
            cast(date, signal_date): int(count)
            for signal_date, count in valid_factors.filter(
                pl.col("instrument_id").is_not_null()
            )
            .group_by("signal_date")
            .len()
            .iter_rows()
        }
        pair_statistics: dict[
            date, tuple[int, float | None, float | None, str | None]
        ] = {}
        offset = 0
        for signal_date, count in (
            pairs.group_by("signal_date", maintain_order=True)
            .len()
            .iter_rows()
        ):
            day = cast(date, signal_date)
            size = int(count)
            factor_count = factor_counts.get(day, 0)
            pearson_ic: float | None = None
            rank_ic: float | None = None
            invalid_reason: str | None = None
            if factor_count < minimum_cross_section:
                invalid_reason = "INSUFFICIENT_CROSS_SECTION"
            elif size < minimum_cross_section:
                invalid_reason = "INSUFFICIENT_FORWARD_PAIRS"
            else:
                group = pairs.slice(offset, size)
                factor_values = group["value"].to_numpy()
                return_values = group["future_return"].to_numpy()
                if np.ptp(factor_values) == 0:
                    invalid_reason = "ZERO_FACTOR_VARIANCE"
                elif np.ptp(return_values) == 0:
                    invalid_reason = "ZERO_RETURN_VARIANCE"
                else:
                    pearson_ic = _AnalysisSupport._correlation(
                        factor_values, return_values
                    )
                    rank_ic = _AnalysisSupport._correlation(
                        _AnalysisSupport._ranks(factor_values),
                        _AnalysisSupport._ranks(return_values),
                    )
                    if pearson_ic is None or rank_ic is None:
                        pearson_ic = None
                        rank_ic = None
                        invalid_reason = "NONFINITE_IC"
            pair_statistics[day] = (
                size,
                pearson_ic,
                rank_ic,
                invalid_reason,
            )
            offset += size
        rows: list[
            tuple[date, int, int, float | None, float | None, bool, str | None]
        ] = []
        dates = sorted(set(cast(list[date], factors["signal_date"].to_list())))
        for day in dates:
            factor_count = factor_counts.get(day, 0)
            statistics = pair_statistics.get(day)
            if statistics is None:
                sample_count = 0
                pearson_ic = None
                rank_ic = None
                invalid_reason = (
                    "INSUFFICIENT_CROSS_SECTION"
                    if factor_count < minimum_cross_section
                    else "INSUFFICIENT_FORWARD_PAIRS"
                )
            else:
                sample_count, pearson_ic, rank_ic, invalid_reason = statistics
            rows.append(
                (
                    day,
                    factor_count,
                    sample_count,
                    pearson_ic,
                    rank_ic,
                    invalid_reason is None,
                    invalid_reason,
                )
            )
        daily = pl.DataFrame(
            rows,
            schema={
                "signal_date": pl.Date,
                "factor_valid_count": pl.Int64,
                "sample_count": pl.Int64,
                "pearson_ic": pl.Float64,
                "rank_ic": pl.Float64,
                "is_valid": pl.Boolean,
                "invalid_reason": pl.String,
            },
            orient="row",
        ).sort("signal_date")
        return self._time_series_columns(daily)

    def summarize(self, daily: pl.DataFrame, column: str) -> IcMetricSummary:
        """汇总一种日度 IC，并按信号日识别最长正负连续区间。

        入参：
            daily：日频。
            column：列。
        返回值：
            返回``summarize``（``IcMetricSummary``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if column not in {"pearson_ic", "rank_ic"}:
            raise ValueError("IC summary column must be pearson_ic or rank_ic")
        _AnalysisSupport._require(
            daily, {"signal_date", column, "is_valid"}, "daily IC"
        )
        ordered = daily.sort("signal_date")
        values = [
            float(value)
            for value in ordered.filter(pl.col("is_valid"))[column]
            .drop_nulls()
            .to_list()
            if isinstance(value, (int, float)) and isfinite(value)
        ]
        mean = None if not values else float(np.mean(values))
        sample_std = None if len(values) < 2 else float(np.std(values, ddof=1))
        quantiles = (
            (None, None, None, None, None)
            if not values
            else tuple(
                float(value)
                for value in np.quantile(
                    np.asarray(values, dtype=np.float64),
                    self._quantile_probabilities,
                    method="linear",
                )
            )
        )
        positive = self._longest_streak(ordered, column, positive=True)
        negative = self._longest_streak(ordered, column, positive=False)
        return IcMetricSummary(
            mean=mean,
            sample_std=sample_std,
            icir_unannualized=(
                None
                if mean is None or sample_std is None or sample_std == 0.0
                else mean / sample_std
            ),
            positive_rate=(
                None
                if not values
                else sum(value > 0.0 for value in values) / len(values)
            ),
            p05=quantiles[0],
            p25=quantiles[1],
            p50=quantiles[2],
            p75=quantiles[3],
            p95=quantiles[4],
            valid_date_count=len(values),
            max_positive_streak=positive[0],
            positive_streak_start=positive[1],
            positive_streak_end=positive[2],
            max_negative_streak=negative[0],
            negative_streak_start=negative[1],
            negative_streak_end=negative[2],
        )

    def _time_series_columns(self, daily: pl.DataFrame) -> pl.DataFrame:
        window_pearson: list[float | None] = []
        window_rank: list[float | None] = []
        rolling_counts: list[int] = []
        rolling_pearson: list[float | None] = []
        rolling_rank: list[float | None] = []
        cumulative_pearson: list[float | None] = []
        cumulative_rank: list[float | None] = []
        pearson_total: float | None = None
        rank_total: float | None = None
        for row in daily.iter_rows(named=True):
            valid = row["is_valid"] is True
            pearson = float(row["pearson_ic"]) if valid else None
            rank = float(row["rank_ic"]) if valid else None
            window_pearson.append(pearson)
            window_rank.append(rank)
            window_pearson = window_pearson[-self._rolling_window :]
            window_rank = window_rank[-self._rolling_window :]
            valid_pearson = [value for value in window_pearson if value is not None]
            valid_rank = [value for value in window_rank if value is not None]
            rolling_counts.append(len(valid_pearson))
            enough = len(valid_pearson) >= self._rolling_min_valid
            rolling_pearson.append(float(np.mean(valid_pearson)) if enough else None)
            rolling_rank.append(float(np.mean(valid_rank)) if enough else None)
            if pearson is not None and rank is not None:
                pearson_total = (pearson_total or 0.0) + pearson
                rank_total = (rank_total or 0.0) + rank
            cumulative_pearson.append(pearson_total)
            cumulative_rank.append(rank_total)
        return daily.with_columns(
            pl.lit(self._rolling_window, dtype=pl.Int64).alias("rolling_window"),
            pl.Series("rolling_valid_count", rolling_counts, dtype=pl.Int64),
            pl.Series("pearson_ic_rolling_mean", rolling_pearson, dtype=pl.Float64),
            pl.Series("rank_ic_rolling_mean", rolling_rank, dtype=pl.Float64),
            pl.Series(
                "pearson_ic_cumulative_sum", cumulative_pearson, dtype=pl.Float64
            ),
            pl.Series("rank_ic_cumulative_sum", cumulative_rank, dtype=pl.Float64),
        )

    @staticmethod
    def _longest_streak(
        daily: pl.DataFrame, column: str, *, positive: bool
    ) -> tuple[int, date | None, date | None]:
        best_length = 0
        best_start: date | None = None
        best_end: date | None = None
        current_length = 0
        current_start: date | None = None
        for row in daily.select("signal_date", column, "is_valid").iter_rows(
            named=True
        ):
            value = row[column]
            matches = (
                row["is_valid"] is True
                and isinstance(value, (int, float))
                and ((value > 0.0) if positive else (value < 0.0))
            )
            if matches:
                if current_length == 0:
                    current_start = row["signal_date"]
                current_length += 1
                if current_length > best_length:
                    best_length = current_length
                    best_start = current_start
                    best_end = row["signal_date"]
            else:
                current_length = 0
                current_start = None
        return best_length, best_start, best_end


def coverage_by_date(
    factors: pl.DataFrame, eligible_universe: pl.DataFrame
) -> pl.DataFrame:
    """处理因子计算中的覆盖率``by``日期；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        factors：因子集合。
        eligible_universe：准入证券股票池。
    返回值：
        返回``by``日期（``pl.DataFrame``）。
    异常：
        无。
    Count valid factor observations against an explicit eligible denominator.
    """
    _AnalysisSupport._require(
        factors, {"signal_date", "instrument_id", "value", "is_valid"}, "factors"
    )
    _AnalysisSupport._require(
        eligible_universe, {"signal_date", "instrument_id", "eligible"}, "universe"
    )
    _AnalysisSupport._unique(factors, "factors")
    _AnalysisSupport._unique(eligible_universe, "universe")
    joined = eligible_universe.filter(pl.col("eligible")).join(
        _AnalysisSupport._valid_factors(factors)
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
    """计算每个信号日的有效样本 Spearman Rank IC；该函数作为稳定公开 API保留在模块级。

    入参：
        factors：因子集合。
        future_returns：未来收益收益序列。
    返回值：
        返回秩IC（``pl.DataFrame``）。
    异常：
        无。
    """
    return (
        InformationCoefficientAnalyzer(
            rolling_window=2,
            rolling_min_valid=2,
        )
        .daily(factors, future_returns, minimum_cross_section=2)
        .select(
            "signal_date",
            pl.col("sample_count").alias("pair_count"),
            "rank_ic",
            "is_valid",
        )
    )


def assign_quantiles(factors: pl.DataFrame, quantiles: int) -> pl.DataFrame:
    """处理因子计算中的``assign``分位组数；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        factors：因子集合。
        quantiles：分位组数。
    返回值：
        返回分位组数（``pl.DataFrame``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Assign 1=lowest through Q=highest and retain empty bucket diagnostics.
    """
    if type(quantiles) is not int or quantiles < 2:
        raise ValueError("quantiles must be an integer of at least 2")
    _AnalysisSupport._unique(factors, "factors")
    ordered = _AnalysisSupport._valid_factors(factors).sort(
        "signal_date", "value", "instrument_id"
    )
    assigned = ordered.with_columns(
        pl.int_range(0, pl.len(), dtype=pl.Int64)
        .over("signal_date")
        .alias("_row_index"),
        pl.len().over("signal_date").cast(pl.Int64).alias("_row_count"),
    ).select(
        "signal_date",
        "instrument_id",
        pl.col("value").cast(pl.Float64),
        ((pl.col("_row_index") * quantiles) // pl.col("_row_count") + 1)
        .cast(pl.Int64)
        .alias("quantile"),
        pl.lit(quantiles, dtype=pl.Int64).alias("quantiles"),
        pl.lit(1, dtype=pl.Int64).alias("bucket_count"),
        pl.lit(False).alias("is_empty"),
    )
    domain = (
        factors.select("signal_date")
        .unique()
        .join(
            pl.DataFrame(
                {"quantile": pl.Series(range(1, quantiles + 1), dtype=pl.Int64)}
            ),
            how="cross",
        )
    )
    empty = domain.join(
        assigned.select("signal_date", "quantile").unique(),
        on=["signal_date", "quantile"],
        how="anti",
    ).select(
        "signal_date",
        pl.lit(None, dtype=pl.String).alias("instrument_id"),
        pl.lit(None, dtype=pl.Float64).alias("value"),
        "quantile",
        pl.lit(quantiles, dtype=pl.Int64).alias("quantiles"),
        pl.lit(0, dtype=pl.Int64).alias("bucket_count"),
        pl.lit(True).alias("is_empty"),
    )
    return pl.concat([assigned, empty]).sort("signal_date", "quantile", "instrument_id")


def quantile_future_returns(
    factors: pl.DataFrame, future_returns: pl.DataFrame, quantiles: int
) -> pl.DataFrame:
    """处理因子计算中的分位组未来收益收益序列；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        factors：因子集合。
        future_returns：未来收益收益序列。
        quantiles：分位组数。
    返回值：
        返回未来收益收益序列（``pl.DataFrame``）。
    异常：
        无。
    Average strictly future returns for every date/quantile, including empty groups.
    """
    assigned = assign_quantiles(factors, quantiles)
    _AnalysisSupport._validate_future(future_returns)
    return _AnalysisSupport._quantile_returns_from_assigned(
        assigned,
        _AnalysisSupport._valid_returns(future_returns),
        quantiles,
    )


def long_short_returns(quantile_returns: pl.DataFrame) -> pl.DataFrame:
    """处理因子计算中的``long``空头收益序列；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        quantile_returns：分位组收益序列。
    返回值：
        返回``short``收益序列（``pl.DataFrame``）。
    异常：
        无。
    Return the fixed Q-minus-1 portfolio return for each signal date.
    """
    _AnalysisSupport._require(
        quantile_returns,
        {"signal_date", "quantile", "count", "mean_return", "quantiles"},
        "quantile returns",
    )
    rows: list[tuple[object, float | None, bool, str | None]] = []
    for group in quantile_returns.partition_by("signal_date", maintain_order=False):
        day = group["signal_date"].item(0)
        q = group["quantiles"].item(0)
        domain = set(group["quantile"].to_list())
        if domain != set(range(1, q + 1)):
            rows.append((day, None, False, "INCOMPLETE_QUANTILE_DOMAIN"))
            continue
        low = group.filter(pl.col("quantile") == 1)
        high = group.filter(pl.col("quantile") == q)
        low_value, high_value = low["mean_return"].item(), high["mean_return"].item()
        if (
            low["count"].item() == 0
            or high["count"].item() == 0
            or not isinstance(low_value, (int, float))
            or not isinstance(high_value, (int, float))
            or not isfinite(low_value)
            or not isfinite(high_value)
        ):
            rows.append((day, None, False, "MISSING_TERMINAL_QUANTILE_OBSERVATIONS"))
            continue
        value: float | None = float(high_value) - float(low_value)
        rows.append((day, value, True, None))
    return pl.DataFrame(
        rows,
        schema={
            "signal_date": pl.Date,
            "long_short_return": pl.Float64,
            "is_valid": pl.Boolean,
            "invalid_reason": pl.String,
        },
        orient="row",
    ).sort("signal_date")


def factor_correlation_matrix(factors: pl.DataFrame) -> pl.DataFrame:
    """读取因子相关性``matrix``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        factors：因子集合。
    返回值：
        返回相关性``matrix``（``pl.DataFrame``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Average same-date, same-security Pearson factor correlations.
    """
    _AnalysisSupport._require(
        factors,
        {"signal_date", "instrument_id", "factor_id", "value", "is_valid"},
        "factors",
    )
    if factors.select(
        pl.struct("signal_date", "instrument_id", "factor_id").is_duplicated().any()
    ).item():
        raise ValueError("duplicate factor correlation key")
    valid = _AnalysisSupport._valid_factors(factors)
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
                    _AnalysisSupport._correlation(
                        group["left"].to_numpy(), group["right"].to_numpy()
                    )
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


def factor_rank_correlation_matrix(
    factors: pl.DataFrame, minimum_pairs: int = 2
) -> pl.DataFrame:
    """计算同日同证券对齐后的因子 Spearman 相关矩阵；该函数作为稳定公开 API保留在模块级。

    入参：
        factors：因子集合。
        minimum_pairs：判定输入或结果有效所需达到的最小值``pairs``。
    返回值：
        返回``rank``相关性``matrix``（``pl.DataFrame``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """
    if minimum_pairs < 2:
        raise ValueError("minimum_pairs must be at least 2")
    _AnalysisSupport._require(
        factors,
        {"signal_date", "instrument_id", "factor_id", "value", "is_valid"},
        "factors",
    )
    if factors.select(
        pl.struct("signal_date", "instrument_id", "factor_id").is_duplicated().any()
    ).item():
        raise ValueError("duplicate factor correlation key")
    valid = _AnalysisSupport._valid_factors(factors)
    ids = sorted(set(valid["factor_id"].to_list()))
    rows = []
    for left in ids:
        for right in ids:
            daily_rank: list[float] = []
            daily_pearson: list[float] = []
            pair_count = 0
            left_frame = valid.filter(pl.col("factor_id") == left).select(
                "signal_date", "instrument_id", pl.col("value").alias("left")
            )
            right_frame = valid.filter(pl.col("factor_id") == right).select(
                "signal_date", "instrument_id", pl.col("value").alias("right")
            )
            paired = left_frame.join(
                right_frame, on=["signal_date", "instrument_id"], how="inner"
            )
            for group in paired.partition_by("signal_date", maintain_order=False):
                if group.height < minimum_pairs:
                    continue
                pearson = _AnalysisSupport._correlation(
                    group["left"].to_numpy(), group["right"].to_numpy()
                )
                rank = _AnalysisSupport._correlation(
                    _AnalysisSupport._ranks(group["left"].to_numpy()),
                    _AnalysisSupport._ranks(group["right"].to_numpy()),
                )
                if pearson is not None and rank is not None:
                    daily_pearson.append(pearson)
                    daily_rank.append(rank)
                    pair_count += group.height
            pearson_value = (
                sum(daily_pearson) / len(daily_pearson) if daily_pearson else None
            )
            rank_value = sum(daily_rank) / len(daily_rank) if daily_rank else None
            rows.append(
                (
                    left,
                    right,
                    len(daily_rank),
                    pair_count,
                    pearson_value,
                    rank_value,
                    rank_value is not None,
                )
            )
    return pl.DataFrame(
        rows,
        schema={
            "factor_x": pl.String,
            "factor_y": pl.String,
            "date_count": pl.Int64,
            "pair_count": pl.Int64,
            "pearson_correlation": pl.Float64,
            "rank_correlation": pl.Float64,
            "is_valid": pl.Boolean,
        },
        orient="row",
    ).sort("factor_x", "factor_y")


class _AnalysisSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _aligned_pairs(
        factors: pl.DataFrame, future_returns: pl.DataFrame
    ) -> pl.DataFrame:
        valid = _AnalysisSupport._valid_factors(factors)
        _AnalysisSupport._validate_future(future_returns)
        return valid.join(
            _AnalysisSupport._valid_returns(future_returns),
            on=["signal_date", "instrument_id"],
            how="inner",
        )

    @staticmethod
    def _valid_factors(frame: pl.DataFrame) -> pl.DataFrame:
        _AnalysisSupport._require(
            frame, {"signal_date", "instrument_id", "value", "is_valid"}, "factors"
        )
        return frame.filter(
            pl.col("is_valid")
            & pl.col("value").is_not_null()
            & pl.col("value").is_finite()
        )

    @staticmethod
    def _valid_returns(frame: pl.DataFrame) -> pl.DataFrame:
        return _AnalysisSupport._return_rows_with_boundaries(frame).filter(
            pl.col("future_return").is_not_null() & pl.col("future_return").is_finite()
        )

    @staticmethod
    def _quantile_returns_from_assigned(
        assigned: pl.DataFrame,
        valid_returns: pl.DataFrame,
        quantiles: int,
    ) -> pl.DataFrame:
        """从已分配分位和已过滤收益批量聚合分层收益。"""
        joined = assigned.join(
            valid_returns.select(
                "signal_date", "instrument_id", "future_return"
            ),
            on=["signal_date", "instrument_id"],
            how="left",
        )
        return (
            joined.group_by("signal_date", "quantile")
            .agg(
                pl.col("future_return").count().cast(pl.Int64).alias("count"),
                pl.col("future_return").mean().alias("mean_return"),
                pl.col("value").min().alias("factor_lower_bound"),
                pl.col("value").max().alias("factor_upper_bound"),
            )
            .with_columns(
                pl.lit(quantiles, dtype=pl.Int64).alias("quantiles"),
                (pl.col("count") == 0).alias("is_empty"),
            )
            .select(
                "signal_date",
                "quantile",
                "count",
                "mean_return",
                "factor_lower_bound",
                "factor_upper_bound",
                "quantiles",
                "is_empty",
            )
            .sort("signal_date", "quantile")
        )

    @staticmethod
    def _validate_future(frame: pl.DataFrame) -> None:
        _AnalysisSupport._require(
            frame,
            {
                "signal_date",
                "instrument_id",
                "return_start",
                "return_end",
                "future_return",
            },
            "future returns",
        )
        _AnalysisSupport._unique(frame, "future returns")
        bounded = _AnalysisSupport._return_rows_with_boundaries(frame)
        if bounded.filter(
            (pl.col("return_start") <= pl.col("signal_date"))
            | (pl.col("return_end") < pl.col("return_start"))
        ).height:
            raise ValueError("future return window must be strictly after signal date")

    @staticmethod
    def _return_rows_with_boundaries(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.filter(
            pl.col("signal_date").is_not_null()
            & pl.col("instrument_id").is_not_null()
            & pl.col("return_start").is_not_null()
            & pl.col("return_end").is_not_null()
        )

    @staticmethod
    def _unique(frame: pl.DataFrame, name: str) -> None:
        if frame.select(
            pl.struct("signal_date", "instrument_id").is_duplicated().any()
        ).item():
            raise ValueError(f"duplicate {name} key")

    @staticmethod
    def _require(frame: pl.DataFrame, columns: set[str], name: str) -> None:
        missing = sorted(columns - set(frame.columns))
        if missing:
            raise ValueError(f"{name} missing columns: {', '.join(missing)}")

    @staticmethod
    def _ranks(values: np.ndarray) -> np.ndarray:
        order = np.argsort(values, kind="stable")
        ranks = np.empty(len(values), dtype=np.float64)
        if len(values) == 0:
            return ranks
        ordered = values[order]
        starts = np.concatenate(
            (
                np.asarray([0], dtype=np.int64),
                np.flatnonzero(ordered[1:] != ordered[:-1]) + 1,
            )
        )
        stops = np.concatenate(
            (starts[1:], np.asarray([len(values)], dtype=np.int64))
        )
        ranks[order] = np.repeat(
            (starts + stops - 1).astype(np.float64) / 2.0,
            stops - starts,
        )
        return ranks

    @staticmethod
    def _correlation(left: np.ndarray, right: np.ndarray) -> float | None:
        if len(left) < 2 or np.ptp(left) == 0 or np.ptp(right) == 0:
            return None
        result = float(np.corrcoef(left, right)[0, 1])
        return result if isfinite(result) else None
