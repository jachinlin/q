"""使用现有 Canonical Repository 执行三个参考策略的研究链。"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import date, timedelta
from math import erfc, sqrt
from typing import cast

import numpy as np
import polars as pl

from quant_research.application.research_platform import ResearchRunResult
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.costs import LiquidityImpactCostModel
from quant_research.data.contracts import JsonValue
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.execution import AShareExecutionSimulator
from quant_research.experiments.research import (
    FamilyExecutionRecord,
    ResearchFamilyRecord,
    ResearchMetricRecord,
    ResearchPhase,
    ResearchRunRecord,
    ResearchStage,
    ResearchVariantRecord,
)
from quant_research.experiments.research_artifacts import ResearchArtifactPublisher
from quant_research.portfolio.constructor import TargetPortfolio
from quant_research.portfolio.research import (
    AllocationProjector,
    AlphaRiskCostOptimizer,
    DirectionalExposureMapper,
)
from quant_research.research_protocols import ResearchConfigResolver, ResearchMode
from quant_research.research_protocols.models import ResearchFamilyConfig
from quant_research.risk import StatisticalRiskEstimator
from quant_research.signals.builtin import (
    CrossSectionalMultifactorSignal,
    DualMovingAverageSignal,
    EtfRotationAllocationSignal,
)
from quant_research.signals.models import (
    AllocationSignalArtifact,
    ArtifactIdentity,
    CrossSectionalScoreArtifact,
    DirectionalSignalArtifact,
)
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import TaskProgress
from quant_research.universe import UniverseBuilder, UniverseRules


class CanonicalResearchRuntime:
    """只通过注入的研究数据契约读取已验证 Canonical 数据。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        repository: ResearchDataRepository,
        publisher: ResearchArtifactPublisher,
        rulebook: MarketRuleBook,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._execution = AShareExecutionSimulator(repository, rulebook)
        self._resolver = ResearchConfigResolver()

    def execute(
        self,
        family: ResearchFamilyRecord,
        execution: FamilyExecutionRecord,
        variant: ResearchVariantRecord,
        run: ResearchRunRecord,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> ResearchRunResult:
        """执行固定阶段，并按研究深度显式跳过不适用阶段。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        config = self._resolver.resolve_normalized(variant.config).config
        periods = (
            (("TRAIN", config.research_protocol.train), ("VALIDATION", config.research_protocol.validation))
            if run.phase is ResearchPhase.TRAIN_VALIDATION
            else (("TEST", config.research_protocol.test),)
        )
        stage_status: dict[str, JsonValue] = {}
        all_frames: dict[str, list[pl.DataFrame]] = {}
        metrics: list[ResearchMetricRecord] = []
        total = len(periods) * 5
        completed = 0
        self._require_catalog(execution.catalog_hash, "VALIDATE")
        stage_status[ResearchStage.VALIDATE.value] = "SUCCEEDED"
        for split, period in periods:
            if cancellation.is_cancelled():
                raise RuntimeError("research run cancelled at split boundary")
            progress.update(TaskProgress(stage="UNIVERSE", completed=completed, total=total, message=f"构建 {split} 股票池"))
            instruments, decisions, bars, universe = self._inputs(
                config, period.start, period.end
            )
            universe = universe.with_columns(pl.lit(split).alias("split"))
            self._append(all_frames, "universe.parquet", universe)
            completed += 1
            stage_status[ResearchStage.UNIVERSE.value] = "SUCCEEDED"
            progress.update(TaskProgress(stage="RESEARCH_COMPUTE", completed=completed, total=total, message=f"计算 {split} 信号与风险"))
            signal = self._signal(
                config,
                family,
                execution,
                variant,
                run,
                instruments,
                decisions,
                bars,
                universe,
                period.start,
                period.end,
            )
            signal_frame = self._signal_frame(signal).with_columns(pl.lit(split).alias("split"))
            self._append(all_frames, "signals/signals.parquet", signal_frame)
            completed += 1
            stage_status[ResearchStage.RESEARCH_COMPUTE.value] = "SUCCEEDED"
            progress.update(TaskProgress(stage="SIMULATE", completed=completed, total=total, message=f"构建 {split} 组合"))
            targets, returns, fills = self._simulate(
                config,
                signal,
                bars,
                decisions,
                period.start,
                period.end,
            )
            if config.research_mode is ResearchMode.SIGNAL_STUDY:
                stage_status[ResearchStage.SIMULATE.value] = "SKIPPED:SIGNAL_STUDY"
            else:
                self._append(all_frames, "target_portfolios.parquet", targets.with_columns(pl.lit(split).alias("split")))
                if config.research_mode is ResearchMode.BACKTEST_EXPERIMENT:
                    self._append(all_frames, "fills.parquet", fills.with_columns(pl.lit(split).alias("split")))
                else:
                    stage_status[ResearchStage.SIMULATE.value] = "SUCCEEDED:THEORETICAL_PORTFOLIO"
            completed += 1
            progress.update(TaskProgress(stage="ANALYTICS", completed=completed, total=total, message=f"分析 {split} 结果"))
            split_metrics, nav = self._analytics(run.id, split, signal_frame, returns)
            metrics.extend(split_metrics)
            self._append(all_frames, "analytics/nav.parquet", nav.with_columns(pl.lit(split).alias("split")))
            completed += 1
            stage_status[ResearchStage.ANALYTICS.value] = "SUCCEEDED"
            self._require_catalog(execution.catalog_hash, f"{split}:ARTIFACT_VERIFY")
            completed += 1
        stage_status[ResearchStage.ARTIFACT_VERIFY.value] = "SUCCEEDED"
        frames = {name: pl.concat(items, how="diagonal_relaxed") for name, items in all_frames.items()}
        identity: dict[str, JsonValue] = {
            "family_id": family.id,
            "execution_id": execution.id,
            "run_id": run.id,
            "variant_id": variant.id,
            "phase": run.phase.value,
            "catalog_hash": execution.catalog_hash,
            "composition_hash": variant.composition_hash,
        }
        manifest_path, manifest_hash = self._publisher.publish(
            family_id=family.id,
            execution_id=execution.id,
            run_id=run.id,
            frames=frames,
            documents={
                "strategy_definition.json": variant.config,
                "research_protocol.json": cast(Mapping[str, JsonValue], variant.config["research_protocol"]),
            },
            identity=identity,
        )
        stage_status[ResearchStage.REGISTER.value] = "SUCCEEDED"
        progress.update(TaskProgress(stage="REGISTER", completed=total, total=total, message="产物已验证并登记"))
        return ResearchRunResult(str(manifest_path), manifest_hash, tuple(metrics), stage_status)

    def _inputs(
        self, config: ResearchFamilyConfig, start: date, end: date
    ) -> tuple[
        tuple[InstrumentId, ...],
        tuple[date, ...],
        pl.DataFrame,
        pl.DataFrame,
    ]:
        universe_config = config.universe
        calendar = self._repository.trade_calendar(start, end).collect().filter(pl.col("is_trading_day"))
        dates = tuple(cast(list[date], calendar["trade_date"].to_list()))
        schedule = config.decision_schedule
        decisions = self._schedule(dates, cast(str, schedule.get("frequency", "DAILY")))
        if universe_config.get("component") == "fixed_instruments":
            raw = universe_config.get("instruments")
            if not isinstance(raw, list):
                raise ValueError("fixed_instruments requires instruments")
            instruments = tuple(
                sorted(
                    (InstrumentId.parse(cast(str, item)) for item in raw),
                    key=lambda item: item.canonical(),
                )
            )
            universe = pl.DataFrame(
                {
                    "signal_date": [day for day in decisions for _ in instruments],
                    "instrument_id": [
                        item.canonical() for _ in decisions for item in instruments
                    ],
                    "eligible": [True] * (len(decisions) * len(instruments)),
                    "reason_codes": [[] for _ in range(len(decisions) * len(instruments))],
                },
                schema_overrides={
                    "signal_date": pl.Date,
                    "reason_codes": pl.List(pl.String),
                },
            )
        else:
            allowed_raw = universe_config.get("allowed_boards", ["MAIN", "CHINEXT", "STAR"])
            if not isinstance(allowed_raw, list):
                raise ValueError("allowed_boards must be a list")
            rules = UniverseRules(
                min_listing_days=cast(int, universe_config.get("min_listing_days", 120)),
                allowed_boards=frozenset(Board(cast(str, item)) for item in allowed_raw),
                exclude_st=cast(bool, universe_config.get("exclude_st", True)),
                exclude_suspended=cast(bool, universe_config.get("exclude_suspended", True)),
                min_avg_amount_20d=(
                    None
                    if universe_config.get("min_avg_amount_20d") is None
                    else float(cast(int | float, universe_config["min_avg_amount_20d"]))
                ),
            )
            snapshots = tuple(
                UniverseBuilder(self._repository)
                .build(day, rules)
                .rename({"as_of": "signal_date"})
                for day in decisions
            )
            universe = (
                pl.concat(snapshots)
                if snapshots
                else pl.DataFrame(
                    schema={
                        "instrument_id": pl.String,
                        "signal_date": pl.Date,
                        "eligible": pl.Boolean,
                        "reason_codes": pl.List(pl.String),
                    }
                )
            ).sort("signal_date", "instrument_id")
            eligible_ids = universe.filter(pl.col("eligible"))["instrument_id"].unique().to_list()
            instruments = tuple(
                sorted(
                    (InstrumentId.parse(value) for value in eligible_ids),
                    key=lambda item: item.canonical(),
                )
            )
        if not instruments:
            raise ValueError("research universe contains no instruments")
        lookback_start = start - timedelta(days=500)
        bars = self._repository.adjusted_bars(instruments, lookback_start, end).collect().sort("instrument_id", "trade_date")
        return instruments, decisions, bars, universe

    def _signal(
        self,
        config: ResearchFamilyConfig,
        family: ResearchFamilyRecord,
        execution: FamilyExecutionRecord,
        variant: ResearchVariantRecord,
        run: ResearchRunRecord,
        instruments: tuple[InstrumentId, ...],
        decisions: tuple[date, ...],
        bars: pl.DataFrame,
        universe: pl.DataFrame,
        start: date,
        end: date,
    ) -> CrossSectionalScoreArtifact | DirectionalSignalArtifact | AllocationSignalArtifact:
        signal_config = config.signal
        component = cast(str, signal_config["component"])
        universe_hash = hashlib.sha256(universe.write_json().encode("utf-8")).hexdigest()
        identity = ArtifactIdentity(run.id, component, hashlib.sha256(component.encode()).hexdigest(), execution.catalog_hash, universe_hash, start, end)
        if family.strategy_id == "dual_ma_trend":
            return DualMovingAverageSignal(short_window=cast(int, signal_config["short_window_sessions"]), long_window=cast(int, signal_config["long_window_sessions"])).compute(identity, bars, decisions)
        if family.strategy_id == "etf_rotation":
            raw_weights = cast(dict[str, JsonValue], signal_config["return_weights"])
            etf_weights = {int(key): float(cast(int | float, value)) for key, value in raw_weights.items()}
            return EtfRotationAllocationSignal(return_weights=etf_weights, trend_window=cast(int, signal_config["trend_window_sessions"]), volatility_window=cast(int, signal_config["volatility_window_sessions"]), volatility_penalty=float(cast(int | float, signal_config["volatility_penalty"])), top_n=cast(int, signal_config["top_n"])).compute(identity, bars, decisions)
        basics = self._repository.daily_basics(instruments, start, end).collect()
        feature = (
            bars.with_columns((pl.col("close") / pl.col("close").shift(120).over("instrument_id") - 1.0).alias("momentum"))
            .filter(pl.col("trade_date").is_in(decisions))
            .join(basics.select("instrument_id", "trade_date", "pe_ttm", "pb_mrq"), on=("instrument_id", "trade_date"), how="left")
            .with_columns(
                pl.when((pl.col("pe_ttm") > 0) & (pl.col("pb_mrq") > 0)).then(1.0 / pl.col("pe_ttm") + 1.0 / pl.col("pb_mrq")).otherwise(None).alias("value")
            )
        )
        factors = pl.concat(
            [
                feature.select(pl.col("trade_date").alias("signal_date"), "instrument_id", pl.lit(name).alias("factor_id"), pl.col(name).alias("value"), "available_at").with_columns(pl.col("value").is_not_null().alias("is_valid"))
                for name in ("value", "momentum")
            ]
        ).join(
            universe.filter(pl.col("eligible")).select(
                "signal_date", "instrument_id"
            ),
            on=("signal_date", "instrument_id"),
            how="inner",
        )
        raw_weights = cast(dict[str, JsonValue], signal_config["factor_weights"])
        factor_weights = {key: float(cast(int | float, value)) for key, value in raw_weights.items()}
        return CrossSectionalMultifactorSignal("stock_multifactor", factor_weights).compute(identity, factors, decisions)

    def _simulate(
        self,
        config: ResearchFamilyConfig,
        signal: CrossSectionalScoreArtifact | DirectionalSignalArtifact | AllocationSignalArtifact,
        bars: pl.DataFrame,
        decisions: tuple[date, ...],
        start: date,
        end: date,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        mode = config.research_mode
        if mode is ResearchMode.SIGNAL_STUDY:
            return pl.DataFrame(), self._signal_proxy_returns(signal, bars), pl.DataFrame()
        portfolio_config = config.portfolio
        dates = sorted(set(cast(list[date], bars["trade_date"].to_list())))
        next_date = {dates[index]: dates[index + 1] for index in range(len(dates) - 1)}
        target_rows: list[dict[str, object]] = []
        target_objects: list[TargetPortfolio] = []
        current: dict[str, float] = {}
        for decision in decisions:
            execute = next_date.get(decision)
            if execute is None:
                continue
            if isinstance(signal, DirectionalSignalArtifact):
                directional_rows = tuple(item for item in signal.rows if item.signal_date == decision and item.is_valid)
                if not directional_rows:
                    continue
                target = DirectionalExposureMapper(long_weight=float(cast(int | float, portfolio_config.get("long_weight", 1.0)))).construct(directional_rows[0], execute)
            elif isinstance(signal, AllocationSignalArtifact):
                allocation_rows = tuple(item for item in signal.rows if item.signal_date == decision)
                target = AllocationProjector(max_position_weight=float(cast(int | float, portfolio_config.get("max_position_weight", 1.0)))).construct(allocation_rows, execute)
            else:
                score_rows = tuple(item for item in signal.rows if item.signal_date == decision)
                if not score_rows:
                    continue
                instrument_ids = tuple(sorted(item.instrument_id for item in score_rows))
                day_bars = bars.filter(pl.col("instrument_id").is_in(instrument_ids)).with_columns(pl.col("close").log().diff().over("instrument_id").alias("log_return")).rename({"trade_date": "trade_date"})
                risk = StatisticalRiskEstimator(lookback=60).estimate(day_bars.select("trade_date", "instrument_id", "log_return", "amount"), (decision,)).slices[0]
                liquidity = dict(zip(risk.instruments, risk.liquidity_amount, strict=True))
                costs = LiquidityImpactCostModel(fixed_bps=2.0, impact_bps=10.0, max_participation=0.1).build(decision, liquidity, float(config.initial_cash_fen) / 100.0)
                target = AlphaRiskCostOptimizer(min_positions=min(cast(int, portfolio_config.get("min_positions", 2)), len(score_rows)), max_positions=min(cast(int, portfolio_config.get("max_positions", 50)), len(score_rows)), max_position_weight=float(cast(int | float, portfolio_config.get("max_position_weight", 0.1))), max_turnover=float(cast(int | float, portfolio_config.get("max_turnover", 0.5))), risk_aversion=float(cast(int | float, portfolio_config.get("risk_aversion", 0.1))), cost_aversion=float(cast(int | float, portfolio_config.get("cost_aversion", 1.0)))).construct(signal_date=decision, execute_date=execute, signals=score_rows, risk=risk, costs=costs, current_weights=current).target
            current = {item.instrument_id.canonical(): item.target_weight for item in target.positions}
            target_objects.append(target)
            target_rows.extend({"signal_date": decision, "execute_date": execute, "instrument_id": item.instrument_id.canonical(), "target_weight": item.target_weight, "cash_weight": target.cash_weight, "reason_code": item.reason_code} for item in target.positions)
        targets = pl.from_dicts(target_rows) if target_rows else pl.DataFrame(schema={"signal_date": pl.Date, "execute_date": pl.Date, "instrument_id": pl.String, "target_weight": pl.Float64, "cash_weight": pl.Float64, "reason_code": pl.String})
        if mode is ResearchMode.BACKTEST_EXPERIMENT:
            execution_config = config.execution
            result = self._execution.run(
                target_objects,
                start=start,
                end=end,
                initial_cash_fen=config.initial_cash_fen,
                reference_price=cast(str, execution_config.get("reference_price", "OPEN")),
                slippage_bps=float(cast(int | float, execution_config.get("slippage_bps", 0.0))),
                max_volume_participation=float(
                    cast(
                        int | float,
                        execution_config.get("max_volume_participation", 0.1),
                    )
                ),
            )
            return targets, result.returns, result.fills
        returns, fills = self._target_returns(targets, bars, realized=False)
        return targets, returns, fills

    @staticmethod
    def _signal_frame(
        signal: CrossSectionalScoreArtifact
        | DirectionalSignalArtifact
        | AllocationSignalArtifact,
    ) -> pl.DataFrame:
        rows = [asdict(item) for item in signal.rows]
        return pl.from_dicts(rows) if rows else pl.DataFrame()

    @staticmethod
    def _signal_proxy_returns(
        signal: CrossSectionalScoreArtifact
        | DirectionalSignalArtifact
        | AllocationSignalArtifact,
        bars: pl.DataFrame,
    ) -> pl.DataFrame:
        signal_frame = CanonicalResearchRuntime._signal_frame(signal)
        if signal_frame.is_empty():
            return pl.DataFrame(schema={"trade_date": pl.Date, "return": pl.Float64})
        if "score" in signal_frame.columns:
            scored = signal_frame.with_columns(pl.col("score").fill_null(0.0).alias("exposure"))
        elif "desired_exposure" in signal_frame.columns:
            scored = signal_frame.with_columns(pl.col("desired_exposure").fill_null(0.0).alias("exposure"))
        else:
            scored = signal_frame.with_columns(pl.col("strength").fill_null(0.0).alias("exposure"))
        del bars
        return scored.group_by("signal_date").agg(pl.col("exposure").mean().alias("return")).rename({"signal_date": "trade_date"})

    @staticmethod
    def _target_returns(targets: pl.DataFrame, bars: pl.DataFrame, *, realized: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
        if targets.is_empty():
            empty = pl.DataFrame(schema={"trade_date": pl.Date, "return": pl.Float64})
            return empty, pl.DataFrame()
        prices = bars.select("trade_date", "instrument_id", "open", "close")
        joined = targets.join(prices, left_on=("execute_date", "instrument_id"), right_on=("trade_date", "instrument_id"), how="left").with_columns((pl.col("close") / pl.col("open") - 1.0).fill_null(0.0).alias("asset_return"))
        cost = 0.0007 if realized else 0.0
        returns = joined.group_by("execute_date").agg(((pl.col("target_weight") * pl.col("asset_return")).sum() - cost * pl.col("target_weight").sum()).alias("return")).rename({"execute_date": "trade_date"}).sort("trade_date")
        fills = joined.select(pl.col("execute_date").alias("trade_date"), "instrument_id", "target_weight", pl.col("open").alias("fill_price"), pl.lit(cost).alias("realized_cost_rate"))
        return returns, fills

    @staticmethod
    def _analytics(run_id: str, split: str, signal_frame: pl.DataFrame, returns: pl.DataFrame) -> tuple[tuple[ResearchMetricRecord, ...], pl.DataFrame]:
        values = np.asarray(returns["return"].to_list() if "return" in returns.columns else [], dtype=float)
        values = values[np.isfinite(values)]
        cumulative = float(np.prod(1.0 + values) - 1.0) if values.size else 0.0
        volatility = float(values.std(ddof=1)) if values.size > 1 else 0.0
        sharpe = float(values.mean() / volatility * sqrt(252.0)) if volatility > 0.0 else 0.0
        nav_values = np.cumprod(1.0 + values) if values.size else np.asarray([], dtype=float)
        peaks = np.maximum.accumulate(nav_values) if values.size else nav_values
        drawdowns = nav_values / peaks - 1.0 if values.size else nav_values
        max_drawdown = float(drawdowns.min()) if values.size else 0.0
        annualized = float((1.0 + cumulative) ** (252.0 / values.size) - 1.0) if values.size and cumulative > -1.0 else 0.0
        calmar = annualized / abs(max_drawdown) if max_drawdown < 0.0 else 0.0
        statistic = abs(float(values.mean())) / (volatility / sqrt(values.size)) if values.size > 1 and volatility > 0.0 else 0.0
        p_value = erfc(statistic / sqrt(2.0)) if statistic else 1.0
        metrics = tuple(ResearchMetricRecord(run_id=run_id, split=split, category="PERFORMANCE", name=name, value=value, unit=None, p_value=p_value if name == "sharpe" else None, adjusted_p_value=None) for name, value in (("cumulative_return", cumulative), ("sharpe", sharpe), ("max_drawdown", max_drawdown), ("calmar", calmar), ("signal_coverage", float(signal_frame.height))))
        nav = pl.DataFrame({"trade_date": returns["trade_date"] if "trade_date" in returns.columns else [], "nav": nav_values.tolist()})
        return metrics, nav

    def _require_catalog(self, expected: str, stage: str) -> None:
        state = self._repository.catalog().require_validated_catalog()
        if state.catalog_hash != expected:
            raise ValueError(f"canonical catalog drift at {stage}: expected {expected}, got {state.catalog_hash}")

    @staticmethod
    def _schedule(dates: Sequence[date], frequency: str) -> tuple[date, ...]:
        if frequency == "DAILY":
            return tuple(dates)
        buckets: dict[tuple[int, int], date] = {}
        for item in dates:
            key = (item.year, item.month) if frequency == "MONTHLY" else (item.isocalendar().year, item.isocalendar().week)
            buckets[key] = item
        return tuple(sorted(buckets.values()))

    @staticmethod
    def _append(target: dict[str, list[pl.DataFrame]], name: str, frame: pl.DataFrame) -> None:
        target.setdefault(name, []).append(frame)
