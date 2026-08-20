"""实现批量统计风险估计和不可变决策日风险切片。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import sqrt

import numpy as np
import polars as pl


@dataclass(frozen=True, slots=True)
class RiskSlice:
    """表示一个决策日上按证券稳定排序的风险状态。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    signal_date: date
    instruments: tuple[str, ...]
    annualized_volatility: tuple[float, ...]
    covariance: tuple[tuple[float, ...], ...]
    liquidity_amount: tuple[float, ...]

    def __post_init__(self) -> None:
        size = len(self.instruments)
        if not size or self.instruments != tuple(sorted(set(self.instruments))):
            raise ValueError("risk instruments must be unique and sorted")
        if len(self.annualized_volatility) != size or len(self.liquidity_amount) != size:
            raise ValueError("risk vectors must align with instruments")
        if len(self.covariance) != size or any(len(row) != size for row in self.covariance):
            raise ValueError("risk covariance must be square")


@dataclass(frozen=True, slots=True)
class RiskArtifact:
    """保存研究区间内全部决策日风险切片。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    slices: tuple[RiskSlice, ...]

    def __post_init__(self) -> None:
        dates = tuple(item.signal_date for item in self.slices)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("risk slices must have unique sorted dates")


class StatisticalRiskEstimator:
    """使用回看收益和对角收缩生成稳定协方差及流动性估计。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, *, lookback: int = 60, shrinkage: float = 0.2) -> None:
        if lookback < 2 or not 0.0 <= shrinkage <= 1.0:
            raise ValueError("invalid statistical risk parameters")
        self._lookback = lookback
        self._shrinkage = shrinkage

    def estimate(self, observations: pl.DataFrame, decision_dates: tuple[date, ...]) -> RiskArtifact:
        """从收益与成交额长表估计每个决策日的年化风险。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        required = {"trade_date", "instrument_id", "log_return", "amount"}
        if not required.issubset(observations.columns):
            raise ValueError("risk observations lack required columns")
        slices: list[RiskSlice] = []
        for decision_date in sorted(set(decision_dates)):
            history = observations.filter(pl.col("trade_date") <= decision_date).sort("trade_date").tail(self._lookback * observations["instrument_id"].n_unique())
            pivot = history.pivot(on="instrument_id", index="trade_date", values="log_return", aggregate_function="first").sort("trade_date")
            instruments = tuple(sorted(column for column in pivot.columns if column != "trade_date"))
            if not instruments:
                continue
            matrix = pivot.select(instruments).to_numpy().astype(float)
            matrix = np.nan_to_num(matrix, nan=0.0)
            sample = np.cov(matrix, rowvar=False, ddof=1)
            sample = np.atleast_2d(sample)
            diagonal = np.diag(np.diag(sample))
            covariance = (1.0 - self._shrinkage) * sample + self._shrinkage * diagonal
            volatility = tuple(float(sqrt(max(covariance[index, index], 0.0) * 252.0)) for index in range(len(instruments)))
            liquidity_rows = history.group_by("instrument_id").agg(pl.col("amount").mean()).to_dicts()
            liquidity_map = {str(row["instrument_id"]): float(row["amount"] or 0.0) for row in liquidity_rows}
            slices.append(RiskSlice(decision_date, instruments, volatility, tuple(tuple(float(value) for value in row) for row in covariance), tuple(liquidity_map.get(item, 0.0) for item in instruments)))
        return RiskArtifact(tuple(slices))
