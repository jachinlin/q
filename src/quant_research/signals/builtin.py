"""实现三种参考策略所需的批量信号模型。"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from math import isfinite
from typing import cast

import polars as pl

from quant_research.signals.models import (
    AllocationSignalArtifact,
    AllocationSignalRow,
    ArtifactIdentity,
    CrossSectionalScoreArtifact,
    CrossSectionalScoreRow,
    Direction,
    DirectionalSignalArtifact,
    DirectionalSignalRow,
)


class CrossSectionalMultifactorSignal:
    """把已完成 PIT 校验的长表因子转换为横截面综合分。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, signal_id: str, weights: Mapping[str, float]) -> None:
        if not signal_id or not weights:
            raise ValueError("signal_id and factor weights must not be empty")
        if any(not isfinite(value) for value in weights.values()):
            raise ValueError("factor weights must be finite")
        total = sum(abs(value) for value in weights.values())
        if total <= 0.0:
            raise ValueError("factor weights must contain non-zero exposure")
        self._signal_id = signal_id
        self._weights = dict(
            sorted((key, value / total) for key, value in weights.items())
        )

    def compute(
        self,
        identity: ArtifactIdentity,
        factors: pl.DataFrame,
        decision_dates: Sequence[date],
    ) -> CrossSectionalScoreArtifact:
        """按决策日标准化因子并生成稳定横截面评分。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        required = {
            "signal_date",
            "instrument_id",
            "factor_id",
            "value",
            "available_at",
            "is_valid",
        }
        if not required.issubset(factors.columns):
            raise ValueError("factor frame lacks required signal columns")
        filtered = factors.filter(
            pl.col("signal_date").is_in(tuple(sorted(set(decision_dates))))
            & pl.col("factor_id").is_in(tuple(self._weights))
        )
        valid = filtered.filter(pl.col("is_valid") & pl.col("value").is_finite())
        normalized = valid.with_columns(
            mean=pl.col("value").mean().over("signal_date", "factor_id"),
            std=pl.col("value").std(ddof=0).over("signal_date", "factor_id"),
        ).with_columns(
            pl.when(pl.col("std") > 0.0)
            .then((pl.col("value") - pl.col("mean")) / pl.col("std"))
            .otherwise(None)
            .alias("zscore")
        )
        weights = pl.DataFrame(
            {"factor_id": tuple(self._weights), "weight": tuple(self._weights.values())}
        )
        scores = (
            normalized.join(weights, on="factor_id", how="inner")
            .group_by("signal_date", "instrument_id")
            .agg(
                (pl.col("zscore") * pl.col("weight")).sum().alias("score"),
                pl.col("zscore").is_not_null().sum().alias("valid_count"),
                pl.col("available_at").max().alias("available_at"),
            )
            .with_columns(pl.lit(len(self._weights)).alias("required_count"))
            .sort("signal_date", "instrument_id")
        )
        rows: list[CrossSectionalScoreRow] = []
        for row in scores.iter_rows(named=True):
            complete = cast(int, row["valid_count"]) == cast(int, row["required_count"])
            raw_score = row["score"]
            score = (
                float(cast(float, raw_score))
                if complete and raw_score is not None
                else None
            )
            rows.append(
                CrossSectionalScoreRow(
                    signal_date=cast(date, row["signal_date"]),
                    instrument_id=cast(str, row["instrument_id"]),
                    signal_id=self._signal_id,
                    score=score,
                    confidence=1.0
                    if complete
                    else cast(int, row["valid_count"]) / len(self._weights),
                    available_at=cast(datetime, row["available_at"]),
                    is_valid=complete,
                    invalid_reason=None if complete else "INCOMPLETE_FACTOR_SET",
                )
            )
        return CrossSectionalScoreArtifact(identity, tuple(rows))


class DualMovingAverageSignal:
    """从前复权收盘价批量生成 LONG/FLAT 双均线信号。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self, *, short_window: int, long_window: int, signal_id: str = "dual_ma"
    ) -> None:
        if type(short_window) is not int or type(long_window) is not int:
            raise TypeError("moving-average windows must be integers")
        if short_window <= 0 or long_window <= short_window:
            raise ValueError("moving-average windows require 0 < short < long")
        self._short = short_window
        self._long = long_window
        self._signal_id = signal_id

    def compute(
        self,
        identity: ArtifactIdentity,
        adjusted_bars: pl.DataFrame,
        decision_dates: Sequence[date],
    ) -> DirectionalSignalArtifact:
        """使用完整窗口计算信号，并显式保留窗口不足样本。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        required = {"trade_date", "instrument_id", "close", "available_at"}
        if not required.issubset(adjusted_bars.columns):
            raise ValueError("adjusted bars lack required columns")
        frame = (
            adjusted_bars.sort("instrument_id", "trade_date")
            .with_columns(
                pl.col("close")
                .rolling_mean(self._short, min_samples=self._short)
                .over("instrument_id")
                .alias("short_ma"),
                pl.col("close")
                .rolling_mean(self._long, min_samples=self._long)
                .over("instrument_id")
                .alias("long_ma"),
            )
            .with_columns((pl.col("short_ma") > pl.col("long_ma")).alias("is_long"))
            .with_columns(
                (pl.col("is_long") != pl.col("is_long").shift(1).over("instrument_id"))
                .fill_null(False)
                .alias("state_changed")
            )
            .filter(pl.col("trade_date").is_in(tuple(sorted(set(decision_dates)))))
            .sort("trade_date", "instrument_id")
        )
        rows: list[DirectionalSignalRow] = []
        for row in frame.iter_rows(named=True):
            valid = row["short_ma"] is not None and row["long_ma"] is not None
            direction = (
                Direction.LONG
                if valid and row["is_long"] is True
                else Direction.FLAT
                if valid
                else None
            )
            rows.append(
                DirectionalSignalRow(
                    signal_date=cast(date, row["trade_date"]),
                    instrument_id=cast(str, row["instrument_id"]),
                    signal_id=self._signal_id,
                    direction=direction,
                    strength=1.0 if direction is Direction.LONG else 0.0,
                    state_changed=bool(row["state_changed"]) if valid else False,
                    available_at=cast(datetime, row["available_at"]),
                    is_valid=valid,
                    invalid_reason=None if valid else "INSUFFICIENT_MA_WINDOW",
                )
            )
        return DirectionalSignalArtifact(identity, tuple(rows))


class EtfRotationAllocationSignal:
    """按多窗口收益、趋势和波动率生成 ETF 目标暴露。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        *,
        return_weights: Mapping[int, float],
        trend_window: int,
        volatility_window: int,
        volatility_penalty: float,
        top_n: int,
        signal_id: str = "etf_rotation",
    ) -> None:
        if not return_weights or any(window <= 0 for window in return_weights):
            raise ValueError("return windows must be positive")
        if abs(sum(return_weights.values()) - 1.0) > 1e-10:
            raise ValueError("return weights must sum to one")
        if trend_window <= 0 or volatility_window <= 1 or top_n <= 0:
            raise ValueError("rotation windows and top_n must be positive")
        if not isfinite(volatility_penalty) or volatility_penalty < 0.0:
            raise ValueError("volatility_penalty must be finite and non-negative")
        self._weights = dict(sorted(return_weights.items()))
        self._trend = trend_window
        self._volatility = volatility_window
        self._penalty = volatility_penalty
        self._top_n = top_n
        self._signal_id = signal_id

    def compute(
        self,
        identity: ArtifactIdentity,
        adjusted_bars: pl.DataFrame,
        decision_dates: Sequence[date],
    ) -> AllocationSignalArtifact:
        """计算排名，趋势不合格或窗口不足的 ETF 暴露为零或无效。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        frame = adjusted_bars.sort("instrument_id", "trade_date")
        expressions: list[pl.Expr] = []
        for window in self._weights:
            expressions.append(
                (
                    pl.col("close")
                    / pl.col("close").shift(window).over("instrument_id")
                    - 1.0
                ).alias(f"return_{window}")
            )
        frame = frame.with_columns(
            *expressions,
            (
                pl.col("close")
                / pl.col("close").shift(self._trend).over("instrument_id")
                - 1.0
            ).alias("trend"),
            pl.col("close")
            .log()
            .diff()
            .over("instrument_id")
            .rolling_std(self._volatility, min_samples=self._volatility)
            .over("instrument_id")
            .alias("volatility"),
        ).filter(pl.col("trade_date").is_in(tuple(sorted(set(decision_dates)))))
        rows: list[AllocationSignalRow] = []
        for signal_date in sorted(set(decision_dates)):
            day = frame.filter(pl.col("trade_date") == signal_date).sort(
                "instrument_id"
            )
            scored: list[tuple[str, float, datetime]] = []
            invalid: list[tuple[str, datetime]] = []
            for row in day.iter_rows(named=True):
                values = [row[f"return_{window}"] for window in self._weights]
                available = cast(datetime, row["available_at"])
                instrument = cast(str, row["instrument_id"])
                if (
                    any(
                        value is None or not isfinite(cast(float, value))
                        for value in values
                    )
                    or row["trend"] is None
                    or row["volatility"] is None
                ):
                    invalid.append((instrument, available))
                    continue
                score = sum(
                    self._weights[window] * cast(float, row[f"return_{window}"])
                    for window in self._weights
                ) - self._penalty * cast(float, row["volatility"])
                if cast(float, row["trend"]) > 0.0:
                    scored.append((instrument, score, available))
                else:
                    scored.append((instrument, float("-inf"), available))
            ranked = [
                item
                for item in sorted(scored, key=lambda item: (-item[1], item[0]))
                if isfinite(item[1])
            ][: self._top_n]
            winners = {item[0] for item in ranked}
            weight = 1.0 / len(winners) if winners else 0.0
            for instrument, _, available in scored:
                rows.append(
                    AllocationSignalRow(
                        signal_date,
                        instrument,
                        self._signal_id,
                        weight if instrument in winners else 0.0,
                        available,
                        True,
                        None,
                    )
                )
            for instrument, available in invalid:
                rows.append(
                    AllocationSignalRow(
                        signal_date,
                        instrument,
                        self._signal_id,
                        None,
                        available,
                        False,
                        "INSUFFICIENT_ROTATION_WINDOW",
                    )
                )
        rows.sort(key=lambda row: (row.signal_date, row.instrument_id, row.signal_id))
        return AllocationSignalArtifact(identity, tuple(rows))


def signal_component_hash(component_id: str, config: Mapping[str, object]) -> str:
    """根据组件 ID 和规范配置生成独立组件哈希。

    该函数作为模块级确定性辅助或框架入口保留。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    payload = repr((component_id, tuple(sorted(config.items())))).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
