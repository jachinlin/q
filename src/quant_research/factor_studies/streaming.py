"""以任务内 Parquet 边界执行内存有界的因子研究分析。"""

from __future__ import annotations

import shutil
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Self, cast

import numpy as np
import polars as pl

from quant_research.factor_studies.analysis import (
    _COVERAGE_SCHEMA,
    _INDUSTRY_COVERAGE_SCHEMA,
    EXECUTABLE_FORWARD_RETURN,
    IC_QUANTILE_PROBABILITIES,
    IC_ROLLING_MIN_VALID,
    IC_ROLLING_WINDOW,
    LABEL_KINDS,
    MIN_CROSS_SECTION,
    THEORETICAL_FORWARD_RETURN,
    _StudyAnalyzer,
)
from quant_research.factors.analysis import (
    InformationCoefficientAnalyzer,
    _AnalysisSupport,
    assign_quantiles,
    long_short_returns,
)
from quant_research.tasks.handlers import CancellationToken


@dataclass(frozen=True, slots=True)
class SpilledFrame:
    """描述一个仅在当前研究任务内有效的临时 Parquet。

    入参：path 为可信临时文件路径，row_count 为落盘行数。
    返回值：返回冻结的临时表描述对象。
    异常：字段不变量由创建该对象的临时存储保证。
    """

    path: Path
    row_count: int


@dataclass(frozen=True, slots=True)
class _PreparedSignal:
    """保存一次分位分配后可跨全部标签复用的信号状态。"""

    variant: str
    factor_ref: str
    assigned: SpilledFrame
    factor_dates: pl.DataFrame
    factor_counts: dict[date, int]
    additional_valid_factors: pl.DataFrame
    coverage: pl.DataFrame
    turnover: pl.DataFrame


class FactorStudyTemporaryStore:
    """管理可信数据根下单次因子研究的临时 Parquet 生命周期。

    入参：
        data_root：应用数据根。
        study_id：内部生成的研究标识，只参与受控目录选择。
    返回值：
        返回可作为上下文管理器使用的临时存储。
    异常：
        ValueError：研究标识或解析后的目录越过可信边界时抛出。
        OSError：目录或 Parquet 写入失败时传播。
    """

    def __init__(self, data_root: Path, study_id: str) -> None:
        if not study_id or any(character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in study_id):
            raise ValueError("factor study temporary id is invalid")
        self._root = (data_root.resolve() / "tmp" / "factor-studies").resolve()
        self._directory = (self._root / study_id).resolve()
        if not self._directory.is_relative_to(self._root):
            raise ValueError("factor study temporary directory escapes trusted root")
        self._counter = 0

    @property
    def directory(self) -> Path:
        """返回经边界校验的当前研究临时目录。

        入参：无。
        返回值：返回当前研究的可信临时目录。
        异常：无；目录边界已在初始化时校验。
        """
        return self._directory

    def __enter__(self) -> Self:
        """清理同研究重试残留并创建空临时目录。

        入参：无。
        返回值：返回已激活的当前临时存储。
        异常：目录创建或旧目录清理失败时传播 ``OSError``。
        """
        self.cleanup()
        self._directory.mkdir(parents=True, exist_ok=False)
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        """无论终态为何均清理当前研究临时目录。

        入参：上下文管理器传入的异常类型、值和回溯对象。
        返回值：清理后无返回，不抑制原异常。
        异常：临时目录清理失败时传播 ``OSError``。
        """
        self.cleanup()

    def write(self, category: str, frame: pl.DataFrame) -> SpilledFrame:
        """以内部序号写入一张确定性排序后的临时表。

        入参：
            category：代码声明的短类别，只用于可诊断文件前缀。
            frame：待落盘数据。
        返回值：
            返回内部路径和行数，不暴露用户可控文件名。
        异常：
            ValueError：类别不安全或存储尚未进入时抛出。
            OSError：写入失败时传播。
        """
        if not category or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789-"
            for character in category
        ):
            raise ValueError("factor study temporary category is invalid")
        if not self._directory.is_dir():
            raise ValueError("factor study temporary store is not active")
        self._counter += 1
        path = self._directory / f"{self._counter:05d}-{category}.parquet"
        row_group_size: int | None = None
        if category == "signal" and "signal_date" in frame.columns:
            date_count = frame["signal_date"].n_unique()
            if date_count > 0:
                rows_per_date = max(1, (frame.height + date_count - 1) // date_count)
                row_group_size = rows_per_date * 20
        frame.write_parquet(
            path,
            compression="zstd",
            statistics=True,
            row_group_size=row_group_size,
        )
        return SpilledFrame(path=path, row_count=frame.height)

    def remove(self, spilled: SpilledFrame) -> None:
        """删除一个已消费且仍位于当前可信目录内的临时表。

        入参：spilled 为当前存储生成的临时表描述。
        返回值：删除完成后无返回。
        异常：文件越过任务目录时抛出 ``ValueError``，删除失败时传播 ``OSError``。
        """
        path = spilled.path.resolve()
        if not path.is_relative_to(self._directory):
            raise ValueError("factor study temporary file escapes task directory")
        path.unlink(missing_ok=True)

    def cleanup(self) -> None:
        """清理同一研究的可信临时目录；不存在时保持幂等。

        入参：无。
        返回值：清理完成后无返回。
        异常：目录越过可信根时抛出 ``ValueError``，删除失败时传播 ``OSError``。
        """
        if not self._directory.is_relative_to(self._root):
            raise ValueError("factor study temporary directory escapes trusted root")
        if self._directory.exists():
            shutil.rmtree(self._directory)


class StreamingForwardReturnBuilder:
    """逐期限构建同时包含理论与可执行口径的紧凑标签表。

    入参：行情、交易日、股票池和入场可执行状态。
    返回值：返回可逐期限构建宽标签的有状态构建器。
    异常：输入缺少稳定契约列时抛出 ``ValueError``。
    """

    _COMMON_COLUMNS = (
        "signal_date",
        "instrument_id",
        "return_start",
        "return_end",
    )

    def __init__(
        self,
        bars: pl.DataFrame,
        sessions: tuple[date, ...],
        eligible: pl.DataFrame,
        executable_state: pl.DataFrame,
    ) -> None:
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
        self._sessions = sessions
        self._eligible = eligible.filter(pl.col("eligible")).select(
            "signal_date", "instrument_id"
        )
        self._entry_prices = bars.select(
            "instrument_id",
            pl.col("trade_date").alias("return_start"),
            pl.col("open").cast(pl.Float64, strict=False).alias("_entry_open"),
        )
        self._exit_prices = bars.select(
            "instrument_id",
            pl.col("trade_date").alias("return_end"),
            pl.col("close").cast(pl.Float64, strict=False).alias("_exit_close"),
        )
        self._entry_state = executable_state.select(
            "instrument_id",
            pl.col("trade_date").alias("return_start"),
            pl.col("is_listed").alias("_entry_listed"),
            pl.col("is_suspended").alias("_entry_suspended"),
            "entry_limit_up",
        )
        self._exit_state = executable_state.select(
            "instrument_id",
            pl.col("trade_date").alias("return_end"),
            pl.col("is_listed").alias("_exit_listed"),
        )

    def build(self, horizon: int) -> pl.DataFrame:
        """构建一个期限的宽标签；两个口径共享边界和价格列。

        入参：horizon 为正持有期交易日数。
        返回值：返回稳定排序的双口径紧凑宽表。
        异常：持有期不是正整数时抛出 ``ValueError``。
        """
        if type(horizon) is not int or horizon <= 0:
            raise ValueError("horizon must be a positive integer")
        session_frame = pl.DataFrame(
            {"signal_date": pl.Series(self._sessions, dtype=pl.Date)}
        )
        boundaries = session_frame.with_columns(
            pl.col("signal_date").shift(-1).alias("return_start"),
            pl.col("signal_date").shift(-horizon).alias("return_end"),
        )
        base = (
            self._eligible.join(boundaries, on="signal_date", how="left")
            .join(self._entry_prices, on=["instrument_id", "return_start"], how="left")
            .join(self._exit_prices, on=["instrument_id", "return_end"], how="left")
            .join(self._entry_state, on=["instrument_id", "return_start"], how="left")
            .join(self._exit_state, on=["instrument_id", "return_end"], how="left")
        )
        raw_return = pl.col("_exit_close") / pl.col("_entry_open") - 1.0
        common_reason = (
            pl.when(pl.col("return_start").is_null() | pl.col("return_end").is_null())
            .then(pl.lit("INCOMPLETE_FORWARD_WINDOW"))
            .when(pl.col("_entry_open").is_null() | (pl.col("_entry_open") <= 0.0))
            .then(pl.lit("MISSING_ENTRY_PRICE"))
            .when(pl.col("_exit_close").is_null() & ~pl.col("_exit_listed").fill_null(True))
            .then(pl.lit("DELISTED_WITHOUT_EXIT_PRICE"))
            .when(pl.col("_exit_close").is_null() | (pl.col("_exit_close") <= 0.0))
            .then(pl.lit("MISSING_EXIT_PRICE"))
            .when(~raw_return.is_finite().fill_null(False))
            .then(pl.lit("NONFINITE_RETURN"))
            .otherwise(pl.lit(None, dtype=pl.String))
        )
        executable_reason = (
            pl.when(pl.col("return_start").is_null() | pl.col("return_end").is_null())
            .then(pl.lit("INCOMPLETE_FORWARD_WINDOW"))
            .when(~pl.col("_entry_listed").fill_null(False))
            .then(pl.lit("NOT_LISTED_AT_ENTRY"))
            .when(pl.col("_entry_suspended").fill_null(True))
            .then(pl.lit("ENTRY_SUSPENDED"))
            .when(pl.col("entry_limit_up").fill_null(False))
            .then(pl.lit("ENTRY_LIMIT_UP"))
            .otherwise(common_reason)
        )
        return (
            base.with_columns(
                common_reason.alias("theoretical_invalid_reason"),
                executable_reason.alias("executable_invalid_reason"),
            )
            .select(
                *self._COMMON_COLUMNS,
                pl.when(pl.col("theoretical_invalid_reason").is_null())
                .then(raw_return)
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("theoretical_future_return"),
                pl.col("theoretical_invalid_reason").is_null().alias(
                    "theoretical_is_valid"
                ),
                "theoretical_invalid_reason",
                pl.when(pl.col("executable_invalid_reason").is_null())
                .then(raw_return)
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .cast(pl.Float64)
                .alias("executable_future_return"),
                pl.col("executable_invalid_reason").is_null().alias(
                    "executable_is_valid"
                ),
                "executable_invalid_reason",
            )
            .sort("signal_date", "instrument_id")
        )

    @staticmethod
    def project(frame: pl.DataFrame, horizon: int, label_kind: str) -> pl.DataFrame:
        """将宽标签投影回稳定的最终标签输入 Schema。

        入参：frame 为宽标签，horizon 为期限，label_kind 为标签种类。
        返回值：返回与公开内存入口一致的长标签表。
        异常：标签种类不受支持时抛出 ``ValueError``。
        """
        if label_kind not in LABEL_KINDS:
            raise ValueError("unsupported factor study label kind")
        prefix = (
            "theoretical"
            if label_kind == THEORETICAL_FORWARD_RETURN
            else "executable"
        )
        return frame.select(
            "signal_date",
            "instrument_id",
            pl.lit(horizon, dtype=pl.Int64).alias("horizon"),
            pl.lit(label_kind).alias("label_kind"),
            "return_start",
            "return_end",
            pl.col(f"{prefix}_future_return").alias("future_return"),
            pl.col(f"{prefix}_is_valid").alias("is_valid"),
            pl.col(f"{prefix}_invalid_reason").alias("invalid_reason"),
        )


class StreamingStudyAnalyzer:
    """从任务临时表顺序装载分析单元并合并稳定的小型产物。

    入参：分位数、成本情景、取消端口、临时存储及最小截面数。
    返回值：返回可执行一次流式统计的分析器。
    异常：配置或依赖违反统计契约时抛出 ``ValueError`` 或 ``RuntimeError``。
    """

    _CONCAT_TABLES = (
        "summary",
        "coverage",
        "label_quality",
        "ic",
        "quantile_returns",
        "long_short_returns",
        "monotonicity",
        "turnover",
        "cost_scenarios",
    )

    def __init__(
        self,
        *,
        quantiles: int,
        cost_bps_scenarios: tuple[int, ...],
        cancellation: CancellationToken,
        temporary: FactorStudyTemporaryStore,
        minimum: int = MIN_CROSS_SECTION,
    ) -> None:
        self._quantiles = quantiles
        self._cost_bps_scenarios = cost_bps_scenarios
        self._minimum = minimum
        self._cancellation = cancellation
        self._temporary = temporary
        self._ic_analyzer = InformationCoefficientAnalyzer(
            rolling_window=IC_ROLLING_WINDOW,
            rolling_min_valid=IC_ROLLING_MIN_VALID,
            quantile_probabilities=IC_QUANTILE_PROBABILITIES,
        )
        self._analyzer = _StudyAnalyzer(
            quantiles=quantiles,
            minimum=minimum,
            cost_bps_scenarios=cost_bps_scenarios,
            ic_analyzer=self._ic_analyzer,
        )
        self._performance_evidence: dict[str, float] = {}

    @property
    def performance_evidence(self) -> Mapping[str, float]:
        """返回最近一次流式分析各内部段的只读耗时证据。

        入参：无。
        返回值：返回相关性、信号准备和分析单元的秒数映射。
        异常：无；尚未运行时返回空映射。
        """
        return dict(self._performance_evidence)

    def run(
        self,
        signal_files: Mapping[tuple[str, str], SpilledFrame],
        eligible: pl.DataFrame,
        label_files: Mapping[int, SpilledFrame],
    ) -> dict[str, pl.DataFrame]:
        """按期限、标签、信号版本和因子顺序执行内存有界分析。

        入参：signal_files 为信号临时表，eligible 为股票池，label_files 为宽标签表。
        返回值：返回固定 11 张最终研究产物表。
        异常：取消时抛出 ``RuntimeError``，临时文件或统计契约无效时传播对应异常。
        """
        collected: dict[str, list[pl.DataFrame]] = {
            name: [] for name in self._CONCAT_TABLES
        }
        aliases = self._equivalent_signal_aliases(signal_files)
        correlation_files = dict(signal_files)
        correlation_variant_aliases: dict[str, str] = {}
        for variant in sorted({item[0] for item in signal_files}):
            keys = [key for key in signal_files if key[0] == variant]
            source_variants = {
                aliases[key][0] for key in keys if key in aliases
            }
            if len(keys) > 0 and len(source_variants) == 1 and all(
                key in aliases for key in keys
            ):
                source_variant = next(iter(source_variants))
                correlation_variant_aliases[variant] = source_variant
                for key in keys:
                    correlation_files.pop(key)
        correlation_started = time.perf_counter()
        correlation = self._correlations(correlation_files)
        if correlation_variant_aliases:
            correlation = pl.concat(
                [
                    correlation,
                    *[
                        correlation.filter(
                            pl.col("signal_variant") == source_variant
                        ).with_columns(
                            pl.lit(variant).alias("signal_variant")
                        )
                        for variant, source_variant in sorted(
                            correlation_variant_aliases.items()
                        )
                    ],
                ]
            )
        correlation_seconds = time.perf_counter() - correlation_started
        denominator = (
            eligible.filter(pl.col("eligible"))
            .group_by("signal_date")
            .len()
            .rename({"len": "eligible_count"})
        )
        prepare_started = time.perf_counter()
        prepared_by_key = {
            key: self._prepare_signal(*key, signal_files[key], denominator)
            for key in sorted(signal_files)
            if key not in aliases
        }
        prepared: list[_PreparedSignal] = []
        for key in sorted(signal_files):
            source = prepared_by_key[aliases.get(key, key)]
            variant, factor_ref = key
            prepared.append(
                source
                if key == (source.variant, source.factor_ref)
                else _PreparedSignal(
                    variant=variant,
                    factor_ref=factor_ref,
                    assigned=source.assigned,
                    factor_dates=source.factor_dates,
                    factor_counts=source.factor_counts,
                    additional_valid_factors=source.additional_valid_factors,
                    coverage=source.coverage.with_columns(
                        pl.lit(variant).alias("signal_variant"),
                        pl.lit(factor_ref).alias("factor_ref"),
                    ),
                    turnover=source.turnover.with_columns(
                        pl.lit(variant).alias("signal_variant"),
                        pl.lit(factor_ref).alias("factor_ref"),
                    ),
                )
            )
        collected["coverage"] = [item.coverage for item in prepared]
        collected["turnover"] = [item.turnover for item in prepared]
        prepare_seconds = time.perf_counter() - prepare_started
        units_started = time.perf_counter()
        for horizon, label_file in sorted(label_files.items()):
            self._check_cancelled()
            wide = pl.read_parquet(label_file.path)
            labels_equivalent = bool(
                wide.select(
                    (
                        pl.col("theoretical_future_return")
                        .eq_missing(pl.col("executable_future_return"))
                        .all()
                        & pl.col("theoretical_is_valid")
                        .eq_missing(pl.col("executable_is_valid"))
                        .all()
                        & pl.col("theoretical_invalid_reason")
                        .eq_missing(pl.col("executable_invalid_reason"))
                        .all()
                    ).alias("equivalent")
                ).item()
            )
            projected: list[
                tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]
            ] = []
            for label_kind in LABEL_KINDS:
                returns = StreamingForwardReturnBuilder.project(
                    wide, horizon, label_kind
                )
                collected["label_quality"].append(
                    self._analyzer._label_quality({(horizon, label_kind): returns})
                )
                valid_returns = _AnalysisSupport._valid_returns(returns)
                label_counts = (
                    returns.filter(pl.col("is_valid"))
                    .group_by("signal_date")
                    .len()
                    .rename({"len": "label_valid_count"})
                )
                projected.append(
                    (label_kind, returns, valid_returns, label_counts)
                )
            signal_unit_cache: dict[
                tuple[Path, str], dict[str, pl.DataFrame]
            ] = {}
            for item in prepared:
                assigned: pl.DataFrame | None = None
                joined_wide: pl.DataFrame | None = None
                reusable_unit: dict[str, pl.DataFrame] | None = None
                for (
                    label_kind,
                    _returns,
                    valid_returns,
                    label_counts,
                ) in projected:
                    self._check_cancelled()
                    cache_key = (item.assigned.path, label_kind)
                    cached = signal_unit_cache.get(cache_key)
                    if cached is not None:
                        unit = {
                            name: frame.with_columns(
                                pl.lit(item.variant).alias("signal_variant"),
                                pl.lit(item.factor_ref).alias("factor_ref"),
                            )
                            for name, frame in cached.items()
                        }
                        for name in (
                            "summary",
                            "ic",
                            "quantile_returns",
                            "long_short_returns",
                            "monotonicity",
                            "cost_scenarios",
                        ):
                            collected[name].append(unit[name])
                        if (
                            labels_equivalent
                            and label_kind == THEORETICAL_FORWARD_RETURN
                        ):
                            reusable_unit = unit
                        del unit
                        continue
                    if (
                        labels_equivalent
                        and label_kind == EXECUTABLE_FORWARD_RETURN
                        and reusable_unit is not None
                    ):
                        unit = {
                            name: frame.with_columns(
                                pl.lit(label_kind).alias("label_kind")
                            )
                            for name, frame in reusable_unit.items()
                        }
                        for name in (
                            "summary",
                            "ic",
                            "quantile_returns",
                            "long_short_returns",
                            "monotonicity",
                            "cost_scenarios",
                        ):
                            collected[name].append(unit[name])
                        signal_unit_cache[cache_key] = unit
                        del unit
                        continue
                    if assigned is None:
                        assigned = pl.read_parquet(item.assigned.path)
                        joined_wide = assigned.join(
                            wide.select(
                                "signal_date",
                                "instrument_id",
                                "theoretical_future_return",
                                "executable_future_return",
                            ),
                            on=["signal_date", "instrument_id"],
                            how="left",
                        )
                    if joined_wide is None:
                        raise RuntimeError("factor study signal join is unavailable")
                    prefix = (
                        "theoretical"
                        if label_kind == THEORETICAL_FORWARD_RETURN
                        else "executable"
                    )
                    joined = joined_wide.select(
                        *assigned.columns,
                        pl.col(f"{prefix}_future_return").alias(
                            "future_return"
                        ),
                    )
                    unit = self._analyze_unit(
                        item,
                        horizon,
                        label_kind,
                        valid_returns,
                        label_counts,
                        denominator,
                        assigned,
                        joined,
                    )
                    for name in (
                        "summary",
                        "ic",
                        "quantile_returns",
                        "long_short_returns",
                        "monotonicity",
                        "cost_scenarios",
                    ):
                        collected[name].append(unit[name])
                    if (
                        labels_equivalent
                        and label_kind == THEORETICAL_FORWARD_RETURN
                    ):
                        reusable_unit = unit
                    signal_unit_cache[cache_key] = unit
                    del unit
                del assigned, joined_wide
            del wide, projected
        self._performance_evidence = {
            "correlation_seconds": correlation_seconds,
            "prepare_signals_seconds": prepare_seconds,
            "analysis_units_seconds": time.perf_counter() - units_started,
        }
        output = {
            name: pl.concat(frames, how="vertical_relaxed")
            for name, frames in collected.items()
        }
        output["correlation"] = correlation
        output["industry_coverage"] = pl.DataFrame(
            schema=_INDUSTRY_COVERAGE_SCHEMA
        )
        return self._sort_outputs(output)

    @staticmethod
    def _equivalent_signal_aliases(
        signal_files: Mapping[tuple[str, str], SpilledFrame],
    ) -> dict[tuple[str, str], tuple[str, str]]:
        """以逐行完全比较识别同一因子的等价信号版本。"""
        aliases: dict[tuple[str, str], tuple[str, str]] = {}
        factors = sorted({factor_ref for _, factor_ref in signal_files})
        columns = [
            "signal_date",
            "instrument_id",
            "factor_id",
            "value",
            "is_valid",
            "invalid_reason",
        ]
        for factor_ref in factors:
            keys = sorted(
                key for key in signal_files if key[1] == factor_ref
            )
            if len(keys) < 2:
                continue
            source_key = keys[0]
            source = pl.read_parquet(
                signal_files[source_key].path, columns=columns
            )
            for key in keys[1:]:
                candidate = pl.read_parquet(
                    signal_files[key].path, columns=columns
                )
                if source.equals(candidate, null_equal=True):
                    aliases[key] = source_key
                del candidate
            del source
        return aliases

    def _prepare_signal(
        self,
        variant: str,
        factor_ref: str,
        signal_file: SpilledFrame,
        denominator: pl.DataFrame,
    ) -> _PreparedSignal:
        """只执行一次有效样本过滤、最小截面屏蔽和分位分配。"""
        self._check_cancelled()
        frame = pl.read_parquet(signal_file.path)
        counts = (
            frame.filter(pl.col("is_valid"))
            .group_by("signal_date")
            .len()
            .rename({"len": "valid_count"})
        )
        coverage_rows: list[dict[str, object]] = []
        for row in denominator.join(
            counts, on="signal_date", how="left"
        ).with_columns(pl.col("valid_count").fill_null(0)).iter_rows(named=True):
            total = int(cast(int, row["eligible_count"]))
            valid_count = int(cast(int, row["valid_count"]))
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
            for signal_date, count in valid_factors.group_by("signal_date")
            .len()
            .iter_rows()
        }
        removed_dates = counts.filter(
            pl.col("valid_count") < self._minimum
        ).select("signal_date")
        additional = valid_factors.join(
            removed_dates, on="signal_date", how="inner"
        ).select("signal_date", "instrument_id", "value")
        assigned = assign_quantiles(
            self._analyzer._minimum_mask(frame), self._quantiles
        )
        spilled = self._temporary.write("assigned", assigned)
        turnover = self._turnover(assigned).with_columns(
            pl.lit(variant).alias("signal_variant"),
            pl.lit(factor_ref).alias("factor_ref"),
        )
        factor_dates = frame.select("signal_date").unique()
        del frame, valid_factors, assigned
        return _PreparedSignal(
            variant=variant,
            factor_ref=factor_ref,
            assigned=spilled,
            factor_dates=factor_dates,
            factor_counts=factor_counts,
            additional_valid_factors=additional,
            coverage=pl.DataFrame(coverage_rows, schema=_COVERAGE_SCHEMA).sort(
                "signal_variant", "factor_ref", "signal_date"
            ),
            turnover=turnover,
        )

    def _turnover(self, assigned: pl.DataFrame) -> pl.DataFrame:
        """以全日期向量化配对计算秩自相关和等权双边换手。"""
        dates = assigned.select("signal_date").unique().sort("signal_date")
        date_values = cast(list[date], dates["signal_date"].to_list())
        date_map = dates.with_columns(
            pl.Series(
                "_previous_signal_date",
                [None, *date_values[:-1]],
                dtype=pl.Date,
            )
        )
        valid = assigned.filter(
            pl.col("instrument_id").is_not_null()
        ).select("signal_date", "instrument_id", "value", "quantile")
        previous_values = valid.select(
            pl.col("signal_date").alias("_previous_signal_date"),
            "instrument_id",
            pl.col("value").alias("_previous_value"),
        )
        paired = (
            valid.join(date_map, on="signal_date", how="left")
            .join(
                previous_values,
                on=["_previous_signal_date", "instrument_id"],
                how="inner",
            )
            .with_columns(
                pl.col("value")
                .rank(method="average")
                .over("signal_date")
                .alias("_current_rank"),
                pl.col("_previous_value")
                .rank(method="average")
                .over("signal_date")
                .alias("_previous_rank"),
            )
        )
        rank = (
            paired.group_by("signal_date")
            .agg(
                pl.corr("_previous_rank", "_current_rank").alias(
                    "rank_autocorrelation"
                )
            )
            .with_columns(
                pl.when(pl.col("rank_autocorrelation").is_finite())
                .then(pl.col("rank_autocorrelation"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("rank_autocorrelation")
            )
        )
        low = self._leg_turnover_by_date(valid, date_map, 1).rename(
            {"_turnover": "low_quantile_turnover"}
        )
        high = self._leg_turnover_by_date(
            valid, date_map, self._quantiles
        ).rename({"_turnover": "high_quantile_turnover"})
        return (
            date_map.join(rank, on="signal_date", how="left")
            .join(low, on="signal_date", how="left")
            .join(high, on="signal_date", how="left")
            .with_columns(
                (
                    pl.col("low_quantile_turnover")
                    + pl.col("high_quantile_turnover")
                ).alias("total_turnover"),
                pl.col("rank_autocorrelation")
                .is_not_null()
                .alias("rank_is_valid"),
                (
                    pl.col("low_quantile_turnover").is_not_null()
                    & pl.col("high_quantile_turnover").is_not_null()
                ).alias("turnover_is_valid"),
            )
            .with_columns(
                pl.when(pl.col("_previous_signal_date").is_null())
                .then(pl.lit("NO_PREVIOUS_SIGNAL_DATE"))
                .when(pl.col("turnover_is_valid"))
                .then(pl.lit(None, dtype=pl.String))
                .otherwise(pl.lit("MISSING_TERMINAL_QUANTILE"))
                .alias("invalid_reason")
            )
            .select(
                "signal_date",
                "rank_autocorrelation",
                "low_quantile_turnover",
                "high_quantile_turnover",
                "total_turnover",
                "rank_is_valid",
                "turnover_is_valid",
                "invalid_reason",
            )
            .sort("signal_date")
        )

    @staticmethod
    def _leg_turnover_by_date(
        valid: pl.DataFrame,
        date_map: pl.DataFrame,
        quantile: int,
    ) -> pl.DataFrame:
        """按相邻日期终端分位成员交集计算等权单边换手。"""
        members = valid.filter(pl.col("quantile") == quantile).select(
            "signal_date", "instrument_id"
        )
        counts = members.group_by("signal_date").len().rename(
            {"len": "_current_count"}
        )
        previous_counts = counts.select(
            pl.col("signal_date").alias("_previous_signal_date"),
            pl.col("_current_count").alias("_previous_count"),
        )
        previous_members = members.select(
            pl.col("signal_date").alias("_previous_signal_date"),
            "instrument_id",
        )
        overlap = (
            members.join(date_map, on="signal_date", how="left")
            .join(
                previous_members,
                on=["_previous_signal_date", "instrument_id"],
                how="inner",
            )
            .group_by("signal_date")
            .len()
            .rename({"len": "_overlap"})
        )
        return (
            counts.join(date_map, on="signal_date", how="left")
            .join(previous_counts, on="_previous_signal_date", how="left")
            .join(overlap, on="signal_date", how="left")
            .with_columns(pl.col("_overlap").fill_null(0))
            .with_columns(
                (
                    0.5
                    * (
                        pl.col("_overlap")
                        * (
                            1.0 / pl.col("_current_count")
                            - 1.0 / pl.col("_previous_count")
                        ).abs()
                        + (pl.col("_previous_count") - pl.col("_overlap"))
                        / pl.col("_previous_count")
                        + (pl.col("_current_count") - pl.col("_overlap"))
                        / pl.col("_current_count")
                    )
                ).alias("_turnover")
            )
            .select("signal_date", "_turnover")
        )

    def _analyze_unit(
        self,
        item: _PreparedSignal,
        horizon: int,
        label_kind: str,
        valid_returns: pl.DataFrame,
        label_counts: pl.DataFrame,
        denominator: pl.DataFrame,
        assigned: pl.DataFrame,
        joined: pl.DataFrame,
    ) -> dict[str, pl.DataFrame]:
        """一次连接一个已分配信号与标签并生成该单元全部统计。"""
        pairs = joined.filter(
            pl.col("instrument_id").is_not_null()
            & pl.col("future_return").is_not_null()
            & pl.col("future_return").is_finite()
        ).select("signal_date", "instrument_id", "value", "future_return")
        if not item.additional_valid_factors.is_empty():
            pairs = pl.concat(
                [
                    pairs,
                    item.additional_valid_factors.join(
                        valid_returns.select(
                            "signal_date", "instrument_id", "future_return"
                        ),
                        on=["signal_date", "instrument_id"],
                        how="inner",
                    ),
                ]
            ).sort("signal_date", "instrument_id")
        ic = self._analyzer._ic(
            item.factor_dates,
            item.factor_counts,
            pl.DataFrame(
                schema={
                    "signal_date": pl.Date,
                    "instrument_id": pl.String,
                    "value": pl.Float64,
                }
            ),
            assigned,
            valid_returns,
            label_counts,
            denominator,
            prepared_pairs=pairs,
        ).with_columns(
            pl.lit(item.variant).alias("signal_variant"),
            pl.lit(item.factor_ref).alias("factor_ref"),
            pl.lit(horizon).alias("horizon"),
            pl.lit(label_kind).alias("label_kind"),
        )
        quantile = _AnalysisSupport._quantile_returns_from_joined(
            joined, self._quantiles
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
                pl.lit(item.variant).alias("signal_variant"),
                pl.lit(item.factor_ref).alias("factor_ref"),
                pl.lit(horizon).alias("horizon"),
                pl.lit(label_kind).alias("label_kind"),
            )
            .drop("paired_count")
        )
        long_short = long_short_returns(quantile).with_columns(
            pl.lit(item.variant).alias("signal_variant"),
            pl.lit(item.factor_ref).alias("factor_ref"),
            pl.lit(horizon).alias("horizon"),
            pl.lit(label_kind).alias("label_kind"),
        )
        monotonicity = self._analyzer._monotonicity(quantile).with_columns(
            pl.lit(item.variant).alias("signal_variant"),
            pl.lit(item.factor_ref).alias("factor_ref"),
            pl.lit(horizon).alias("horizon"),
            pl.lit(label_kind).alias("label_kind"),
        )
        costs, break_even = self._analyzer._costs(
            long_short,
            item.turnover,
            horizon,
            item.variant,
            item.factor_ref,
            label_kind,
        )
        summary = self._analyzer._summary(
            ic,
            long_short,
            monotonicity,
            item.turnover,
            horizon,
            item.variant,
            item.factor_ref,
            label_kind,
            break_even,
        )
        return {
            "summary": pl.DataFrame([summary]),
            "ic": ic,
            "quantile_returns": quantile,
            "long_short_returns": long_short,
            "monotonicity": monotonicity,
            "cost_scenarios": pl.DataFrame(costs),
        }

    def _correlations(
        self, signal_files: Mapping[tuple[str, str], SpilledFrame]
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        variants = sorted({variant for variant, _ in signal_files})
        for variant in variants:
            self._check_cancelled()
            paths = [
                (factor_ref, signal_files[(item_variant, factor_ref)].path)
                for item_variant, factor_ref in sorted(signal_files)
                if item_variant == variant
            ]
            active_refs: list[str] = []
            long_frames: list[pl.DataFrame] = []
            for factor_ref, path in paths:
                self._check_cancelled()
                factor_values = (
                    pl.read_parquet(
                        path,
                        columns=[
                            "signal_date",
                            "instrument_id",
                            "value",
                            "is_valid",
                        ],
                    )
                    .filter(
                        pl.col("is_valid")
                        & pl.col("value").is_not_null()
                        & pl.col("value").is_finite()
                    )
                    .select(
                        "signal_date",
                        "instrument_id",
                        pl.lit(factor_ref).alias("_factor_ref"),
                        "value",
                    )
                )
                if factor_values.is_empty():
                    continue
                active_refs.append(factor_ref)
                long_frames.append(factor_values)
            if not long_frames:
                continue
            aligned = (
                pl.concat(long_frames)
                .pivot(
                    on="_factor_ref",
                    index=["signal_date", "instrument_id"],
                    values="value",
                )
                .sort("signal_date", "instrument_id")
            )
            del long_frames
            totals: dict[tuple[str, str], list[float]] = {
                (left, right): [0.0, 0.0, 0.0, 0.0]
                for left in active_refs
                for right in active_refs
            }
            offset = 0
            grouped = aligned.group_by(
                "signal_date", maintain_order=True
            ).len()
            for day_index, (_, count) in enumerate(grouped.iter_rows()):
                if day_index % 20 == 0:
                    self._check_cancelled()
                size = int(count)
                group = aligned.slice(offset, size)
                offset += size
                if group.is_empty():
                    continue
                arrays = {
                    factor_ref: (
                        group[factor_ref].to_numpy()
                        if factor_ref in group.columns
                        else np.full(group.height, np.nan, dtype=np.float64)
                    )
                    for factor_ref in active_refs
                }
                rank_cache: dict[tuple[str, bytes], np.ndarray] = {}
                for left in active_refs:
                    left_values = arrays[left]
                    for right in active_refs:
                        right_values = arrays[right]
                        mask = np.isfinite(left_values) & np.isfinite(right_values)
                        pair_count = int(np.sum(mask))
                        if pair_count < self._minimum:
                            continue
                        paired_left = left_values[mask]
                        paired_right = right_values[mask]
                        pearson = _AnalysisSupport._correlation(
                            paired_left, paired_right
                        )
                        mask_key = mask.tobytes()
                        left_key = (left, mask_key)
                        right_key = (right, mask_key)
                        left_ranks = rank_cache.get(left_key)
                        if left_ranks is None:
                            left_ranks = _AnalysisSupport._ranks(paired_left)
                            rank_cache[left_key] = left_ranks
                        right_ranks = rank_cache.get(right_key)
                        if right_ranks is None:
                            right_ranks = _AnalysisSupport._ranks(paired_right)
                            rank_cache[right_key] = right_ranks
                        rank = _AnalysisSupport._correlation(
                            left_ranks, right_ranks
                        )
                        if pearson is None or rank is None:
                            continue
                        aggregate = totals[(left, right)]
                        aggregate[0] += 1
                        aggregate[1] += pair_count
                        aggregate[2] += pearson
                        aggregate[3] += rank
            del aligned
            rows: list[tuple[object, ...]] = []
            for (left, right), aggregate in sorted(totals.items()):
                date_count = int(aggregate[0])
                rows.append(
                    (
                        left,
                        right,
                        date_count,
                        int(aggregate[1]),
                        aggregate[2] / date_count if date_count else None,
                        aggregate[3] / date_count if date_count else None,
                        date_count > 0,
                        variant,
                    )
                )
            frames.append(
                pl.DataFrame(
                    rows,
                    schema={
                        "factor_x": pl.String,
                        "factor_y": pl.String,
                        "date_count": pl.Int64,
                        "pair_count": pl.Int64,
                        "pearson_correlation": pl.Float64,
                        "rank_correlation": pl.Float64,
                        "is_valid": pl.Boolean,
                        "signal_variant": pl.String,
                    },
                    orient="row",
                )
            )
        return pl.concat(frames)

    @staticmethod
    def _sort_outputs(
        output: dict[str, pl.DataFrame]
    ) -> dict[str, pl.DataFrame]:
        keys = {
            "summary": ["signal_variant", "label_kind", "factor_ref", "horizon"],
            "coverage": ["signal_variant", "factor_ref", "signal_date"],
            "label_quality": ["label_kind", "horizon", "signal_date", "reason"],
            "ic": ["signal_variant", "label_kind", "factor_ref", "horizon", "signal_date"],
            "quantile_returns": ["signal_variant", "label_kind", "factor_ref", "horizon", "signal_date", "quantile"],
            "long_short_returns": ["signal_variant", "label_kind", "factor_ref", "horizon", "signal_date"],
            "monotonicity": ["signal_variant", "label_kind", "factor_ref", "horizon", "signal_date"],
            "turnover": ["signal_variant", "factor_ref", "signal_date"],
            "cost_scenarios": ["signal_variant", "label_kind", "factor_ref", "horizon", "cost_bps"],
            "correlation": ["signal_variant", "factor_x", "factor_y"],
        }
        for name, columns in keys.items():
            output[name] = output[name].sort(*columns)
        return output

    def _check_cancelled(self) -> None:
        if self._cancellation.is_cancelled():
            raise RuntimeError("factor study cancelled")


class TemporaryEvidence:
    """计算临时目录磁盘占用的测试与诊断证据。

    入参：无；通过静态方法接收可信临时目录。
    返回值：返回无状态的临时证据工具类型。
    异常：目录读取失败时传播 ``OSError``。
    """

    @staticmethod
    def byte_count(directory: Path) -> int:
        """返回可信临时目录当前全部普通文件字节数。

        入参：directory 为当前研究的可信临时目录。
        返回值：返回全部 Parquet 文件的总字节数。
        异常：目录扫描或文件属性读取失败时传播 ``OSError``。
        """
        total = 0
        for path in sorted(directory.glob("*.parquet")):
            size = path.stat().st_size
            total += size
        return total


__all__ = [
    "FactorStudyTemporaryStore",
    "SpilledFrame",
    "StreamingForwardReturnBuilder",
    "StreamingStudyAnalyzer",
    "TemporaryEvidence",
]
