"""装配以 Canonical Repository 为唯一数据入口的研究 Worker。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import polars as pl
from sqlalchemy import Engine

from quant_research.analytics.attribution import (
    AttributionResult,
    calculate_attribution,
)
from quant_research.analytics.performance import (
    PerformanceResult,
    calculate_performance,
)
from quant_research.application.worker import Worker
from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.engine import (
    BacktestEngine,
    BacktestRequest,
    BacktestResult,
    BoundMarketSlice,
)
from quant_research.backtest.models import ExecutionConfig, ExecutionPrice, MarketSlice
from quant_research.backtest.rulebook import AShareRuleBook, SecurityStatus
from quant_research.backtest.study_artifacts import StrategyStudyArtifactPublisher
from quant_research.config import Settings
from quant_research.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.domain.enums import Board, DatasetKind, MultipleTestingMethod
from quant_research.domain.identifiers import IndexId, InstrumentId
from quant_research.factor_studies.analysis import (
    DIRECTION_ADJUSTED,
    EXECUTABLE_FORWARD_RETURN,
    INDUSTRY_MARKET_CAP_NEUTRALIZED,
    INDUSTRY_NEUTRALIZED,
    LABEL_KINDS,
    MARKET_CAP_NEUTRALIZED,
    THEORETICAL_FORWARD_RETURN,
)
from quant_research.factor_studies.models import (
    FactorStudyDefinition,
    FactorStudyRecord,
    FactorStudyStage,
    FactorStudyStatus,
    IndustryUnclassifiedPolicy,
)
from quant_research.factor_studies.progress import FactorStudyProgressReporter
from quant_research.factor_studies.runner import FactorStudyHandler
from quant_research.factor_studies.statistics import MultipleTestingCorrector
from quant_research.factor_studies.streaming import (
    FactorStudyTemporaryStore,
    SpilledFrame,
    StreamingForwardReturnBuilder,
    StreamingStudyAnalyzer,
)
from quant_research.factors import (
    FactorArtifact,
    FactorContext,
    FactorEngine,
    FactorExecutionDescriptor,
    FactorRegistry,
)
from quant_research.factors.builtin import register_stock_factors
from quant_research.factors.transforms import (
    neutralize_industry,
    neutralize_industry_market_cap,
    neutralize_market_cap,
)
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.factor_studies import (
    FactorStudyRegistry,
)
from quant_research.infrastructure.persistence.strategy_studies import (
    StrategyStudyRegistry,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import TaskLogManager
from quant_research.strategies.base import DecisionData, Strategy
from quant_research.strategies.registry import StrategyRegistry
from quant_research.strategy_studies.models import (
    StrategyStudyRecord,
    StrategyStudyStage,
    StrategyStudyStatus,
)
from quant_research.strategy_studies.runner import StrategyStudyHandler
from quant_research.tasks.handlers import CancellationToken, ProgressSink, TaskHandler
from quant_research.tasks.models import TaskProgress, TaskStatus
from quant_research.universe.builder import UniverseBatchEvaluator
from quant_research.universe.rules import UniverseRules

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CN_STOCK_STANDARD_RULES = UniverseRules()

_MARKET_SCHEMA = {
    "instrument_id": pl.String,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "preclose": pl.Float64,
    "volume": pl.Int64,
    "is_suspended": pl.Boolean,
    "security_status": pl.String,
    "instrument_type": pl.String,
    "board": pl.String,
}
_FACTOR_ARTIFACT_KEYS: dict[str, tuple[str, ...]] = {
    "summary": ("signal_variant", "label_kind", "factor_ref", "horizon"),
    "coverage": ("signal_variant", "factor_ref", "signal_date"),
    "label_quality": ("label_kind", "horizon", "signal_date", "reason"),
    "industry_coverage": ("signal_date", "taxonomy", "unclassified_policy"),
    "ic": (
        "signal_variant",
        "label_kind",
        "factor_ref",
        "horizon",
        "signal_date",
    ),
    "quantile_returns": (
        "signal_variant",
        "label_kind",
        "factor_ref",
        "horizon",
        "signal_date",
        "quantile",
    ),
    "long_short_returns": (
        "signal_variant",
        "label_kind",
        "factor_ref",
        "horizon",
        "signal_date",
    ),
    "monotonicity": (
        "signal_variant",
        "label_kind",
        "factor_ref",
        "horizon",
        "signal_date",
    ),
    "turnover": ("signal_variant", "factor_ref", "signal_date"),
    "cost_scenarios": (
        "signal_variant",
        "label_kind",
        "factor_ref",
        "horizon",
        "cost_bps",
    ),
    "correlation": ("signal_variant", "factor_x", "factor_y"),
}
_ACTIVE_STRATEGY_STUDY_STATUSES = frozenset(
    {StrategyStudyStatus.QUEUED, StrategyStudyStatus.RUNNING}
)
_ACTIVE_FACTOR_STUDY_STATUSES = frozenset(
    {FactorStudyStatus.QUEUED, FactorStudyStatus.RUNNING}
)
_ORPHAN_STALE_AFTER = timedelta(seconds=60)
_ORPHAN_PAGE_SIZE = 200


class CanonicalCatalogGuard:
    """在阶段和交易日边界校验当前目录仍等于提交身份。

    入参：
        repository：唯一只读 CanonicalResearchRepository。
    返回值：
        创建引用 Repository 目录门禁的身份守卫。
    异常：
        构造不读取目录；校验时的门禁与漂移错误由方法说明。
    """

    def __init__(self, repository: CanonicalResearchRepository) -> None:
        self._catalog = repository.catalog()

    def assert_unchanged(self, catalog_hash: str) -> None:
        """目录未验证或哈希漂移时立即失败。

        入参：
            catalog_hash：Run 提交时捕获的 SHA-256 目录身份。
        返回值：
            当前身份一致时返回 None。
        异常：
            ValueError：当前目录身份与 Run 身份不一致时抛出；门禁关闭时传播
            Repository 的目录异常。
        """
        if self._catalog.require_validated_catalog().catalog_hash != catalog_hash:
            raise ValueError("EXPERIMENT_DATA_DRIFT")


class _StockPriceService:
    """把股票专用研究接口适配为内置股票因子的收益端口。"""

    def __init__(self, repository: CanonicalResearchRepository) -> None:
        self._repository = repository

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """读取股票前复权对数收益。"""
        return self._repository.stock_log_returns(
            instruments, start, end, lookback_sessions=lookback_sessions
        )


class CanonicalStrategyStudyData:
    """将只读 Canonical Repository 适配为回测行情和 PIT 决策工厂。

    入参：
        repository：唯一 Canonical 数据读取入口；catalog_hash：研究冻结数据身份；
        strategy：可选策略规格，用于限定证券类型和显式标的池。
    返回值：
        创建可提供交易日历、逐日行情和信号日数据的策略研究数据源。
    异常：
        证券目录不可读、策略标的未登记或因子注册冲突时在构造阶段抛出。
    """

    def __init__(
        self,
        repository: CanonicalResearchRepository,
        catalog_hash: str,
        *,
        strategy: Strategy | None = None,
    ) -> None:
        self._repository = repository
        self._catalog_hash = catalog_hash
        stocks = repository.stocks().collect().with_columns(
            pl.lit("STOCK").alias("instrument_type")
        )
        funds = repository.funds().collect().with_columns(
            pl.lit("FUND").alias("instrument_type"),
            pl.lit("MAIN").alias("board"),
        )
        if strategy is not None:
            dependencies = set(strategy.spec.data_dependencies)
            if DatasetKind.STOCK_DAILY_BAR not in dependencies:
                stocks = stocks.head(0)
            if DatasetKind.FUND_DAILY_BAR not in dependencies:
                funds = funds.head(0)
            explicit = self._explicit_market_instruments(strategy)
            if explicit is not None:
                allowed = [item.canonical() for item in explicit]
                stocks = stocks.filter(pl.col("instrument_id").is_in(allowed))
                funds = funds.filter(pl.col("instrument_id").is_in(allowed))
                known = set(stocks["instrument_id"].to_list()) | set(
                    funds["instrument_id"].to_list()
                )
                missing = sorted(set(allowed) - known)
                if missing:
                    raise ValueError(
                        f"strategy market instrument is not registered: {missing[0]}"
                    )
        self._instruments = pl.concat(
            [stocks, funds], how="diagonal_relaxed"
        ).sort("instrument_id")
        self._metadata = {
            cast(str, row["instrument_id"]): row for row in self._instruments.to_dicts()
        }
        self._stock_ids = tuple(
            InstrumentId.parse(identifier)
            for identifier, row in sorted(self._metadata.items())
            if row.get("instrument_type") == "STOCK"
        )
        self._fund_ids = tuple(
            InstrumentId.parse(identifier)
            for identifier, row in sorted(self._metadata.items())
            if row.get("instrument_type") == "FUND"
        )
        registry = FactorRegistry()
        if self._stock_ids:
            register_stock_factors(
                registry,
                repository,
                repository,
                self._stock_ids,
                price_service=_StockPriceService(repository),
            )
        self._factor_engine = FactorEngine(
            registry, capabilities=ProviderCapabilities.complete()
        )

    @staticmethod
    def _explicit_market_instruments(
        strategy: Strategy,
    ) -> tuple[InstrumentId, ...] | None:
        spec = strategy.spec
        if spec.strategy_id == "dual_ma_trend":
            raw = spec.parameters.get("instrument_id")
            if not isinstance(raw, str):
                raise TypeError("dual_ma_trend instrument_id must be a string")
            return (InstrumentId.parse(raw),)
        if spec.strategy_id != "etf_rotation":
            return None
        pipeline = spec.parameters.get("pipeline")
        if not isinstance(pipeline, Mapping):
            raise TypeError("etf_rotation pipeline must be a mapping")
        alpha = pipeline.get("alpha")
        if not isinstance(alpha, Mapping):
            raise TypeError("etf_rotation alpha must be a mapping")
        params = alpha.get("params")
        if not isinstance(params, Mapping):
            raise TypeError("etf_rotation alpha params must be a mapping")
        pool = params.get("etf_pool")
        if (
            not isinstance(pool, list)
            or not pool
            or any(not isinstance(item, str) for item in pool)
        ):
            raise TypeError("etf_rotation etf_pool must be a nonempty string list")
        return tuple(InstrumentId.parse(cast(str, item)) for item in pool)

    def calendar(
        self, start: date, end: date, *, include_next_session: bool
    ) -> TradingCalendar:
        """从 Canonical 交易日历加载指定区间。

        入参：
            start、end：闭区间日期；include_next_session：是否追加区间后的首个交易日。
        返回值：
            返回由权威 Canonical 日历构建的 TradingCalendar。
        异常：
            ValueError：请求下一交易日但覆盖范围内不存在时抛出。
        """
        if include_next_session:
            sessions = self._sessions(end + timedelta(days=1), end + timedelta(days=14))
            if not sessions:
                raise ValueError("no later trading session in canonical coverage")
            end = sessions[0]
        return TradingCalendar.load(self._repository, start, end)

    def market_slice(self, trade_date: date) -> BoundMarketSlice:
        """连接未复权行情、证券元数据和当日交易状态。

        入参：
            trade_date：待撮合和估值的交易日。
        返回值：
            返回按证券标识排序、严格绑定该日的未复权行情切片。
        异常：
            Canonical 数据缺失、Schema 不一致或 PIT 查询失败时传播数据异常。
        """
        bar_frames = []
        if self._stock_ids:
            bar_frames.append(
                self._repository.stock_bars(
                    self._stock_ids, trade_date, trade_date
                ).collect()
            )
        if self._fund_ids:
            bar_frames.append(
                self._repository.fund_bars(
                    self._fund_ids, trade_date, trade_date
                ).collect()
            )
        bars = (
            pl.concat(bar_frames, how="diagonal_relaxed")
            if bar_frames
            else pl.DataFrame()
        )
        suspended = set(
            self._repository.stock_suspensions(
                trade_date, trade_date, self._stock_ids
            ).collect()["instrument_id"].to_list()
        )
        warned = set(
            self._repository.stock_risk_warnings(
                trade_date, trade_date, self._stock_ids
            ).collect()["instrument_id"].to_list()
        )
        listing_dates = [
            cast(date, row["list_date"])
            for row in self._metadata.values()
            if row.get("instrument_type") == "STOCK"
            and isinstance(row.get("list_date"), date)
            and row["list_date"] <= trade_date
        ]
        trading_sessions = (
            self._sessions(min(listing_dates), trade_date) if listing_dates else ()
        )
        rows: list[dict[str, object]] = []
        for row in bars.to_dicts():
            identifier = cast(str, row["instrument_id"])
            metadata = self._metadata.get(identifier)
            if metadata is None:
                continue
            listing = metadata.get("list_date")
            delisting = metadata.get("delist_date")
            if isinstance(listing, date) and trade_date < listing:
                continue
            if isinstance(delisting, date) and trade_date > delisting:
                continue
            board = str(metadata.get("board") or "MAIN")
            if board not in {"MAIN", "CHINEXT", "STAR", "BSE"}:
                board = "MAIN"
            raw_volume = row.get("volume")
            no_limit = (
                metadata.get("instrument_type") == "STOCK"
                and isinstance(listing, date)
                and sum(listing <= session <= trade_date for session in trading_sessions)
                <= 5
            )
            rows.append(
                {
                    "instrument_id": identifier,
                    **{
                        name: row.get(name)
                        for name in ("open", "high", "low", "close", "preclose")
                    },
                    "volume": int(raw_volume) if raw_volume is not None else None,
                    "is_suspended": identifier in suspended,
                    "security_status": (
                        SecurityStatus.NO_LIMIT.value
                        if no_limit
                        else SecurityStatus.ST.value
                        if identifier in warned
                        else SecurityStatus.NORMAL.value
                    ),
                    "instrument_type": str(metadata.get("instrument_type") or "STOCK"),
                    "board": board,
                }
            )
        frame = pl.DataFrame(rows, schema=_MARKET_SCHEMA, strict=False).sort(
            "instrument_id"
        )
        return BoundMarketSlice(MarketSlice(trade_date, frame))

    def benchmark_close(self, benchmark: IndexId, trade_date: date) -> float:
        """读取基准。入参：指数和交易日。返回值：收盘价。异常：缺失或非法时抛出。"""
        frame = self._repository.index_bars(
            (benchmark,), trade_date, trade_date
        ).collect()
        if frame.height != 1 or frame["close"][0] is None:
            raise ValueError("benchmark close is missing from index bars")
        close = float(frame["close"][0])
        if not isfinite(close) or close <= 0:
            raise ValueError("benchmark close must be finite and positive")
        return close

    def bind(self, signal_date: date) -> DecisionData:
        """创建无法接受其他 as-of 日期的决策数据视图。

        入参：
            signal_date：策略当次决策唯一可见日期。
        返回值：
            返回查询方法不暴露日期参数的 DecisionData。
        异常：
            构造不读取数据；具体读取错误由绑定视图方法抛出。
        """
        return _BoundDecisionData(self, signal_date)

    def _sessions(self, start: date, end: date) -> tuple[date, ...]:
        frame = self._repository.trade_calendar(start, end).collect()
        return tuple(
            sorted(
                cast(date, row[0])
                for row in frame.filter(pl.col("is_trading_day"))
                .select("trade_date")
                .iter_rows()
            )
        )

    def _lookback_start(self, signal_date: date, count: int) -> date:
        start = signal_date - timedelta(days=max(60, count * 3))
        sessions = self._sessions(start, signal_date)
        return sessions[-count] if len(sessions) >= count else start

    def universe(self, signal_date: date) -> pl.DataFrame:
        """输出当日可交易股票池及稳定排除原因。

        入参：
            signal_date：股票池可见信息截止日。
        返回值：
            返回按证券排序的 eligible 标记和原因代码表。
        异常：
            证券状态无法按 PIT 读取时传播 Canonical 数据异常。
        """
        suspended = set(
            self._repository.stock_suspensions(
                signal_date, signal_date, self._stock_ids
            ).collect()["instrument_id"].to_list()
        )
        warned = set(
            self._repository.stock_risk_warnings(
                signal_date, signal_date, self._stock_ids
            ).collect()["instrument_id"].to_list()
        )
        rows: list[dict[str, object]] = []
        for instrument in self._stock_ids:
            reasons: list[str] = []
            if instrument.canonical() in warned:
                reasons.append("RISK_WARNING")
            if instrument.canonical() in suspended:
                reasons.append("SUSPENDED")
            rows.append(
                {
                    "instrument_id": instrument.canonical(),
                    "as_of": signal_date,
                    "eligible": not reasons,
                    "reason_codes": reasons,
                }
            )
        return pl.DataFrame(rows).sort("instrument_id")

    def factor_values(
        self,
        signal_date: date,
        factor_ids: Sequence[str],
        instruments: Sequence[InstrumentId],
    ) -> pl.LazyFrame:
        """使用现有 FactorEngine 在当前 Run 内即时计算因子。

        入参：
            signal_date：唯一计算日；factor_ids：因子标识；instruments：证券范围。
        返回值：
            返回按日期、证券和因子稳定排序的惰性结果。
        异常：
            ValueError：因子未知或不满足数据能力时抛出；计算失败时传播因子异常。
        """
        requested = tuple(sorted(set(factor_ids)))
        universe_ids = tuple(sorted({item.canonical() for item in instruments}))
        universe_hash = hashlib.sha256(
            canonical_json_bytes(list(universe_ids))
        ).hexdigest()
        artifacts = self._factor_engine.compute(
            requested,
            FactorContext(self._catalog_hash, universe_hash, signal_date, signal_date),
        )
        frames = [artifacts[item].lazy_frame().collect() for item in requested]
        if not frames:
            return pl.DataFrame().lazy()
        return (
            pl.concat(frames)
            .filter(pl.col("instrument_id").is_in(list(universe_ids)))
            .sort("trade_date", "instrument_id", "factor_id")
            .lazy()
        )

    def asset_type(self, instruments: Sequence[InstrumentId]) -> str:
        """判定资产类型。入参：证券集合。返回值：股票或基金。异常：混合或未知时抛出。"""
        identifiers = {item.canonical() for item in instruments}
        if not identifiers:
            raise ValueError("instrument scope must not be empty")
        stock_ids = {item.canonical() for item in self._stock_ids}
        fund_ids = {item.canonical() for item in self._fund_ids}
        if identifiers <= stock_ids:
            return "STOCK"
        if identifiers <= fund_ids:
            return "FUND"
        raise ValueError("instrument scope must contain only stocks or only funds")


class _BoundDecisionData:
    """绑定一个 signal_date 并封闭所有显式日期参数。"""

    def __init__(self, source: CanonicalStrategyStudyData, signal_date: date) -> None:
        self._source, self._repository, self._signal_date = (
            source,
            source._repository,
            signal_date,
        )

    @property
    def signal_date(self) -> date:
        """返回唯一 PIT 截止日。"""
        return self._signal_date

    def bars(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的未复权窗口。"""
        start = self._source._lookback_start(self._signal_date, lookback_sessions)
        if self._source.asset_type(instruments) == "STOCK":
            return self._repository.stock_bars(instruments, start, self._signal_date)
        return self._repository.fund_bars(instruments, start, self._signal_date)

    def adjusted_bars(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的前复权窗口。"""
        start = self._source._lookback_start(self._signal_date, lookback_sessions)
        if self._source.asset_type(instruments) == "STOCK":
            return self._repository.adjusted_stock_bars(
                instruments, start, self._signal_date
            )
        return self._repository.adjusted_fund_bars(
            instruments, start, self._signal_date
        )

    def log_returns(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的对数收益窗口。"""
        if self._source.asset_type(instruments) == "STOCK":
            return self._repository.stock_log_returns(
                instruments,
                self._signal_date,
                self._signal_date,
                lookback_sessions=lookback_sessions,
            )
        return self._repository.fund_log_returns(
            instruments,
            self._signal_date,
            self._signal_date,
            lookback_sessions=lookback_sessions,
        )

    def daily_basics(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的每日基础指标窗口。"""
        if self._source.asset_type(instruments) != "STOCK":
            raise ValueError("daily basics are available only for stocks")
        return self._repository.stock_daily_basics(
            instruments,
            self._source._lookback_start(self._signal_date, lookback_sessions),
            self._signal_date,
        )

    def factor_values(
        self, factor_ids: Sequence[str], instruments: Sequence[InstrumentId]
    ) -> pl.LazyFrame:
        """即时计算所请求因子。"""
        return self._source.factor_values(self._signal_date, factor_ids, instruments)

    def industry(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame:
        """读取信号日可见的行业状态。"""
        return (
            self._repository.industry_memberships_on_dates(
                instruments, (self._signal_date,)
            )
            .select(
                "instrument_id",
                pl.col("level1_code").alias("industry_code"),
                pl.lit("SW2021").alias("taxonomy"),
                pl.lit(True).alias("is_classified"),
            )
            .sort("instrument_id")
        )

    def security_status(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame:
        """读取信号日交易状态。"""
        if self._source.asset_type(instruments) != "STOCK":
            raise ValueError("security status is available only for stocks")
        identifiers = [item.canonical() for item in instruments]
        suspended = self._repository.stock_suspensions(
            self._signal_date, self._signal_date, instruments
        ).select("instrument_id").unique()
        warned = self._repository.stock_risk_warnings(
            self._signal_date, self._signal_date, instruments
        ).select("instrument_id").unique()
        return (
            pl.DataFrame({"instrument_id": identifiers})
            .lazy()
            .join(suspended.with_columns(pl.lit(True).alias("is_suspended")), on="instrument_id", how="left")
            .join(warned.with_columns(pl.lit(True).alias("is_st")), on="instrument_id", how="left")
            .with_columns(
                pl.col("is_suspended").fill_null(False),
                pl.col("is_st").fill_null(False),
            )
            .sort("instrument_id")
        )

    def stock_universe(self) -> pl.LazyFrame:
        """返回信号日动态股票池。"""
        return self._source.universe(self._signal_date).lazy()


class _BacktestProgress:
    def __init__(self, sink: ProgressSink) -> None:
        self._sink = sink

    def update(self, completed: int, total: int, trade_date: date) -> None:
        """把交易日进度映射为任务进度。"""
        self._sink.update(
            TaskProgress(
                stage="BACKTEST",
                completed=completed,
                total=total,
                message=trade_date.isoformat(),
            )
        )


class StrategyStudyExecutor:
    """为策略研究创建隔离的分阶段执行会话。

    入参：
        repository、registry、strategies、rulebook、artifact_root：数据入口、研究
        登记簿、策略目录、唯一 A 股规则和不可变产物根。
    返回值：
        创建策略研究执行器。
    异常：
        构造不执行回测；运行期错误由会话方法说明。
    """

    def __init__(
        self,
        repository: CanonicalResearchRepository,
        registry: StrategyStudyRegistry,
        strategies: StrategyRegistry,
        rulebook: AShareRuleBook,
        artifact_root: Path,
    ) -> None:
        self._repository, self._registry, self._strategies = (
            repository,
            registry,
            strategies,
        )
        self._rulebook, self._artifact_root = rulebook, artifact_root

    def create(self, study: StrategyStudyRecord) -> _StrategyStudySession:
        """创建绑定一个冻结策略研究的阶段会话。

        入参：study：冻结策略研究。返回值：任务专属会话。
        异常：冻结配置不满足策略契约时抛出 ``TypeError``。
        """
        return _StrategyStudySession(
            study,
            self._repository,
            self._registry,
            self._strategies,
            self._rulebook,
            self._artifact_root,
        )


class _StrategyStudySession:
    """保存策略研究的校验、回测、分析和发布中间状态。"""

    def __init__(
        self,
        study: StrategyStudyRecord,
        repository: CanonicalResearchRepository,
        registry: StrategyStudyRegistry,
        strategies: StrategyRegistry,
        rulebook: AShareRuleBook,
        artifact_root: Path,
    ) -> None:
        self._study = study
        self._repository = repository
        self._registry = registry
        self._strategies = strategies
        self._rulebook = rulebook
        self._artifact_root = artifact_root
        self._strategy: Strategy | None = None
        self._source: CanonicalStrategyStudyData | None = None
        self._backtest: BacktestResult | None = None
        self._tables: dict[str, pl.DataFrame] | None = None
        self._performance: PerformanceResult | None = None
        self._attribution: AttributionResult | None = None
        self._published_dir: Path | None = None

    def execute(
        self,
        stage: StrategyStudyStage,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """按四阶段计算并在 PUBLISH 一次性发布。

        入参：stage、progress、cancellation：当前阶段、进度端口和取消端口。
        返回值：仅 PUBLISH 返回可信发布身份，其余阶段返回空映射。
        异常：阶段乱序、取消、回测、分析、发布或登记失败时抛出。
        """
        if stage is StrategyStudyStage.VALIDATE:
            self._validate()
            return {}
        if stage is StrategyStudyStage.BACKTEST:
            self._backtest_strategy(progress, cancellation)
            return {}
        if stage is StrategyStudyStage.ANALYTICS:
            self._analyze()
            return {}
        if stage is StrategyStudyStage.PUBLISH:
            return self._publish(cancellation)
        raise ValueError(f"unsupported strategy stage: {stage.value}")

    def abort(self) -> None:
        """撤销未成功提交的策略研究输出。

        入参：无。返回值：数据库登记和最终目录清理后无返回。
        异常：登记清理失败时传播，目录仍会尽力删除。
        """
        if self._published_dir is None:
            return
        try:
            self._registry.discard_outputs(self._study.id)
        finally:
            if self._published_dir.is_relative_to(self._artifact_root.resolve()):
                shutil.rmtree(self._published_dir, ignore_errors=True)
            self._published_dir = None

    def _validate(self) -> None:
        definition = self._study.definition
        frozen_hash = hashlib.sha256(
            canonical_json_bytes(definition.model_dump(mode="json"))
        ).hexdigest()
        if frozen_hash != self._study.config_hash:
            raise ValueError("strategy study frozen config hash mismatch")
        self._strategy = self._strategies.build(
            definition.strategy.strategy_id,
            cast(Mapping[str, JsonValue], definition.strategy.parameters),
        )
        self._source = CanonicalStrategyStudyData(
            self._repository,
            self._study.catalog_hash,
            strategy=self._strategy,
        )

    def _backtest_strategy(
        self, progress: ProgressSink, cancellation: CancellationToken
    ) -> None:
        definition = self._study.definition
        if self._strategy is None or self._source is None:
            raise RuntimeError("strategy study must be validated before backtest")
        self._backtest = BacktestEngine(
            self._source,
            self._source,
            self._rulebook,
            CanonicalCatalogGuard(self._repository),
        ).run(
            BacktestRequest(
                self._study.id,
                self._study.catalog_hash,
                definition.start_date,
                definition.end_date,
                definition.benchmark,
                definition.initial_cash_fen,
                self._rulebook.content_hash,
                ExecutionConfig(
                    ExecutionPrice(definition.execution.reference_price),
                    definition.execution.slippage_bps,
                    definition.execution.max_volume_participation,
                ),
            ),
            self._strategy,
            _BacktestProgress(progress),
            cancellation,
        )

    def _analyze(self) -> None:
        if self._backtest is None:
            raise RuntimeError("strategy analytics requires completed backtest")
        tables = StrategyStudyArtifactPublisher.canonical_tables(
            self._backtest.tables
        )
        performance = calculate_performance(
            tables["nav"], tables["holdings"], tables["fills"], tables["costs"]
        )
        attribution = calculate_attribution(
            tables["nav"], tables["holdings"], tables["fills"], tables["costs"]
        )
        drawdown = performance.drawdown
        tables.update(
            {
                "performance": drawdown.select(
                    pl.col("trade_date"),
                    pl.col("portfolio_daily_return").alias("return"),
                    pl.col("benchmark_daily_return").alias("benchmark_return"),
                    (pl.col("nav") - 1.0).alias("cumulative_return"),
                    (pl.col("benchmark_nav") - 1.0).alias(
                        "benchmark_cumulative_return"
                    ),
                    (
                        pl.col("portfolio_daily_return")
                        - pl.col("benchmark_daily_return")
                    ).alias("active_return"),
                    pl.col("nav"),
                    pl.col("benchmark_nav"),
                    pl.col("drawdown"),
                    pl.col("active_drawdown"),
                ),
                "monthly_returns": performance.monthly_returns,
                "annual_returns": performance.annual_returns,
                "execution_summary": performance.execution_summary,
                "exposure_summary": attribution.exposure_summary,
                "attribution": attribution.attribution,
            }
        )
        self._tables = StrategyStudyArtifactPublisher.canonical_tables(tables)
        self._performance = performance
        self._attribution = attribution

    def _publish(self, cancellation: CancellationToken) -> dict[str, JsonValue]:
        if (
            self._backtest is None
            or self._tables is None
            or self._performance is None
            or self._attribution is None
        ):
            raise RuntimeError("strategy publication requires completed analytics")
        if cancellation.is_cancelled():
            raise RuntimeError("strategy publication cancelled before publication")
        metrics = dict(self._performance.metrics)
        quality = cast(
            Mapping[str, JsonValue],
            {
                "calculation_mode": "CASH_EXACT",
                "risk_free_rate_annual": 0.0,
                "undefined_metrics": dict(self._performance.undefined_metrics),
                "unavailable_dimensions": {
                    "factor": "UNAVAILABLE",
                    "style": "UNAVAILABLE",
                },
                "attribution_method": "CASH_EXACT_SECURITY",
                "warnings": list(self._attribution.disclosures),
            },
        )
        directory, manifest_hash, artifacts = StrategyStudyArtifactPublisher(
            self._artifact_root, self._study.id
        ).publish(
            self._tables,
            config=self._backtest.config,
            metrics=cast(Mapping[str, JsonValue], metrics),
            quality_disclosure=quality,
            identities=self._backtest.identities,
        )
        self._published_dir = directory
        if cancellation.is_cancelled():
            self.abort()
            raise RuntimeError("strategy persistence cancelled after publication")
        registered_metrics = _StrategyStudySession._registered_metrics(metrics)
        try:
            self._registry.register_outputs(
                self._study.id, registered_metrics, artifacts
            )
        except BaseException:
            if directory.is_relative_to(self._artifact_root.resolve()):
                shutil.rmtree(directory, ignore_errors=True)
            self._published_dir = None
            raise
        return {
            "artifact_dir": str(directory),
            "manifest_hash": manifest_hash,
            "sessions_completed": self._backtest.sessions_completed,
        }

    @staticmethod
    def _registered_metrics(
        metrics: Mapping[str, int | float | str | None],
    ) -> dict[str, tuple[float, str | None]]:
        units = {
            "observations": "count",
            "max_drawdown_duration_sessions": "sessions",
        }
        ratio_names = {
            name
            for name in metrics
            if any(
                token in name
                for token in (
                    "return",
                    "volatility",
                    "drawdown",
                    "turnover",
                    "rate",
                    "weight",
                    "drag",
                    "alpha",
                    "error",
                )
            )
        }
        result: dict[str, tuple[float, str | None]] = {}
        for name, value in metrics.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            numeric = float(value)
            if not isfinite(numeric):
                continue
            result[name] = (
                numeric,
                units.get(name, "ratio" if name in ratio_names else None),
            )
        return result


class IndependentFactorStudyExecutor:
    """复用 FactorEngine 和统计内核执行独立因子研究。

    入参：
        repository：Canonical 数据入口；registry：Run 登记簿；artifact_root：
        不可变产物根。
    返回值：
        创建统一因子研究 Run 执行器。
    异常：
        构造不计算因子；运行期错误由 execute 方法说明。
    """

    def __init__(
        self,
        repository: CanonicalResearchRepository,
        registry: FactorStudyRegistry,
        rulebook: AShareRuleBook,
        artifact_root: Path,
    ) -> None:
        self._repository, self._registry, self._rulebook, self._artifact_root = (
            repository,
            registry,
            rulebook,
            artifact_root,
        )

    def create(self, study: FactorStudyRecord) -> _FactorStudySession:
        """创建绑定一个冻结因子研究的阶段会话。

        入参：study：冻结因子研究。返回值：任务专属会话。异常：无。
        """
        return _FactorStudySession(
            study,
            self._repository,
            self._registry,
            self._rulebook,
            self._artifact_root,
        )


class _CanonicalUniverseHasher:
    """按有序批次计算与完整 canonical JSON 成员列表相同的身份。"""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._row_count = 0
        self._instrument_ids: set[str] = set()
        self._finished = False

    @property
    def row_count(self) -> int:
        """返回已经纳入哈希的合格成员行数。"""
        return self._row_count

    @property
    def instrument_ids(self) -> tuple[InstrumentId, ...]:
        """返回至少一个交易日合格的稳定证券集合。"""
        return tuple(
            InstrumentId.parse(value) for value in sorted(self._instrument_ids)
        )

    def update(self, eligible: pl.DataFrame) -> None:
        """把一个已按日期和证券排序的合格成员批次追加到身份。"""
        if self._finished:
            raise ValueError("universe hasher is already finalized")
        membership = (
            eligible.filter(pl.col("eligible"))
            .select("signal_date", "instrument_id")
            .sort("signal_date", "instrument_id")
        )
        if membership.is_empty():
            return
        batch_ids = membership["instrument_id"].unique().sort().to_list()
        for value in batch_ids:
            InstrumentId.parse(cast(str, value))
        serialized = (
            membership.select(
                pl.concat_str(
                    pl.lit('{"instrument_id":"'),
                    pl.col("instrument_id"),
                    pl.lit('","signal_date":"'),
                    pl.col("signal_date").cast(pl.String),
                    pl.lit('"}'),
                ).alias("_canonical_member")
            )["_canonical_member"]
            .str.join(",")
            .item()
        )
        if not isinstance(serialized, str):
            raise TypeError("universe member serialization must be a string")
        if self._row_count:
            self._digest.update(b",")
        self._digest.update(serialized.encode("utf-8"))
        self._row_count += membership.height
        self._instrument_ids.update(cast(str, value) for value in batch_ids)

    def finish(self) -> str:
        """封闭列表并返回 SHA-256；同一实例只允许完成一次。"""
        if self._finished:
            raise ValueError("universe hasher is already finalized")
        self._digest.update(b"]")
        self._finished = True
        return self._digest.hexdigest()


class _FactorStudySession:
    """保存独立因子研究的分析表和待发布指标。"""

    def __init__(
        self,
        study: FactorStudyRecord,
        repository: CanonicalResearchRepository,
        registry: FactorStudyRegistry,
        rulebook: AShareRuleBook,
        artifact_root: Path,
    ) -> None:
        self._study = study
        self._repository = repository
        self._registry = registry
        self._rulebook = rulebook
        self._artifact_root = artifact_root
        self._tables: dict[str, pl.DataFrame] | None = None
        self._metrics: dict[
            str, tuple[float, str | None, float | None, float | None]
        ] | None = None
        self._published_dir: Path | None = None
        self._analysis_identity: dict[str, JsonValue] | None = None

    def execute(
        self,
        stage: FactorStudyStage,
        progress: FactorStudyProgressReporter,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        """在 ANALYZE_FACTORS 计算并在 PERSIST 发布。

        入参：stage、progress、cancellation：当前阶段、进度端口和取消端口。
        返回值：仅 PERSIST 返回可信发布身份，其余阶段返回空映射。
        异常：阶段次序、取消、计算、发布或登记失败时抛出。
        """
        if stage in {FactorStudyStage.VALIDATE, FactorStudyStage.PREPARE_INPUTS}:
            return {}
        if stage is FactorStudyStage.ANALYZE_FACTORS:
            self._analyze(progress, cancellation)
            return {}
        if stage is FactorStudyStage.PUBLISH:
            return self._persist(progress, cancellation)
        raise ValueError(f"unsupported factor stage: {stage.value}")

    def abort(self) -> None:
        """撤销未成功提交的因子 Run 输出。

        入参：无。返回值：数据库登记和最终目录清理后无返回。
        异常：登记清理失败时传播，目录仍会尽力删除。
        """
        if self._published_dir is None:
            return
        try:
            self._registry.discard_outputs(self._study.id)
        finally:
            if self._published_dir.is_relative_to(self._artifact_root.resolve()):
                shutil.rmtree(self._published_dir, ignore_errors=True)
            self._published_dir = None

    @staticmethod
    def _analysis_factor_frame(
        artifacts: Mapping[str, FactorArtifact],
        factor_ids: Sequence[str],
        directions: Mapping[str, int],
        eligible: pl.DataFrame,
    ) -> pl.DataFrame:
        """将标准因子产物方向调整并限制在每日 PIT 股票池内。"""
        direction_frame = pl.DataFrame(
            {
                "factor_id": sorted(directions),
                "_direction": [directions[item] for item in sorted(directions)],
            }
        )
        return (
            pl.concat(
                [artifacts[factor_id].lazy_frame().collect() for factor_id in factor_ids]
            )
            .rename({"trade_date": "signal_date"})
            .join(direction_frame, on="factor_id", how="inner")
            .join(
                eligible.filter(pl.col("eligible")).select(
                    "signal_date", "instrument_id"
                ),
                on=["signal_date", "instrument_id"],
                how="inner",
            )
            .with_columns(
                (pl.col("value") * pl.col("_direction")).alias("value"),
                pl.lit(DIRECTION_ADJUSTED).alias("signal_variant"),
                pl.lit(None, dtype=pl.String).alias("invalid_reason"),
            )
            .drop("_direction")
            .sort("signal_date", "instrument_id", "factor_id")
        )

    def _signal_variants(
        self,
        factor_frame: pl.DataFrame,
        eligible: pl.DataFrame,
        universe_ids: tuple[InstrumentId, ...],
        sessions: tuple[date, ...],
        config: FactorStudyDefinition,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """按显式 PIT 行业和市值配置生成唯一中性版本及行业覆盖证据。"""
        industry = config.industry
        market_cap = config.market_cap
        coverage = pl.DataFrame(
            schema={
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
        )
        if industry is None and market_cap is None:
            return factor_frame, coverage

        joined = factor_frame
        helper_columns: list[str] = []
        if industry is not None:
            state = (
                self._repository.industry_memberships_on_dates(
                    universe_ids, sessions
                )
                .collect()
                .select(
                    pl.col("query_date").alias("signal_date"),
                    "instrument_id",
                    pl.col("level1_code").alias("industry_code"),
                    pl.lit(True).alias("is_classified"),
                )
                .with_columns(pl.lit(True).alias("_state_present"))
            )
            aligned = (
                eligible.filter(pl.col("eligible"))
                .select("signal_date", "instrument_id")
                .join(state, on=["signal_date", "instrument_id"], how="left")
                .with_columns(
                    pl.col("_state_present").fill_null(False),
                    (
                        pl.col("is_classified").fill_null(False)
                        & pl.col("industry_code").is_not_null()
                        & (pl.col("industry_code").str.len_chars() > 0)
                    ).alias("_classified"),
                )
            )
            if industry.unclassified_policy is IndustryUnclassifiedPolicy.UNCLASSIFIED:
                aligned = aligned.with_columns(
                    pl.when(pl.col("_classified"))
                    .then(pl.col("industry_code"))
                    .otherwise(pl.lit("__UNCLASSIFIED__"))
                    .alias("_neutralization_industry")
                )
            else:
                aligned = aligned.with_columns(
                    pl.when(pl.col("_classified"))
                    .then(pl.col("industry_code"))
                    .otherwise(pl.lit(None, dtype=pl.String))
                    .alias("_neutralization_industry")
                )
            coverage = (
                aligned.group_by("signal_date")
                .agg(
                    pl.len().alias("eligible_count"),
                    pl.col("_classified").sum().cast(pl.Int64).alias("classified_count"),
                    (pl.col("_state_present") & ~pl.col("_classified"))
                    .sum().cast(pl.Int64).alias("tombstone_count"),
                    (~pl.col("_state_present")).sum().cast(pl.Int64).alias("missing_state_count"),
                    pl.col("_neutralization_industry").is_not_null().sum().cast(pl.Int64).alias("usable_count"),
                )
                .with_columns(
                    pl.lit(industry.taxonomy).alias("taxonomy"),
                    pl.lit(industry.unclassified_policy.value).alias("unclassified_policy"),
                    (pl.col("classified_count") / pl.col("eligible_count")).alias("classified_coverage"),
                    (pl.col("usable_count") / pl.col("eligible_count")).alias("usable_coverage"),
                )
                .select(*coverage.columns)
                .sort("signal_date")
            )
            joined = joined.join(
                aligned.select("signal_date", "instrument_id", "_neutralization_industry"),
                on=["signal_date", "instrument_id"],
                how="left",
            )
            helper_columns.append("_neutralization_industry")

        if market_cap is not None:
            cutoffs = pl.DataFrame(
                {
                    "signal_date": sessions,
                    "_pit_cutoff": [
                        datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(UTC)
                        for value in sessions
                    ],
                },
                schema={
                    "signal_date": pl.Date,
                    "_pit_cutoff": pl.Datetime("us", "UTC"),
                },
            )
            basics = self._repository.stock_daily_basics(
                universe_ids, sessions[0], sessions[-1]
            ).collect()
            visible = (
                basics.rename({"trade_date": "signal_date"})
                .join(cutoffs, on="signal_date", how="inner")
                .filter(
                    pl.col("pit_usable")
                    & pl.col("available_at").is_not_null()
                    & (pl.col("available_at") <= pl.col("_pit_cutoff"))
                )
                .select(
                    "signal_date",
                    "instrument_id",
                    pl.col("total_market_value").alias("_neutralization_market_cap"),
                )
                .sort("signal_date", "instrument_id")
            )
            if visible.select("signal_date", "instrument_id").is_duplicated().any():
                raise ValueError("factor study market-cap input has duplicate keys")
            joined = joined.join(
                visible,
                on=["signal_date", "instrument_id"],
                how="left",
            )
            helper_columns.append("_neutralization_market_cap")

        if industry is not None and market_cap is not None:
            neutralized = neutralize_industry_market_cap(
                joined,
                "value",
                "_neutralization_market_cap",
                "_neutralization_industry",
                ("signal_date", "factor_id"),
            ).with_columns(
                pl.lit(INDUSTRY_MARKET_CAP_NEUTRALIZED).alias("signal_variant")
            )
        elif industry is not None:
            neutralized = neutralize_industry(
                joined,
                "value",
                "_neutralization_industry",
                ("signal_date", "factor_id"),
            ).with_columns(pl.lit(INDUSTRY_NEUTRALIZED).alias("signal_variant"))
        else:
            neutralized = neutralize_market_cap(
                joined,
                "value",
                "_neutralization_market_cap",
                ("signal_date", "factor_id"),
            ).with_columns(pl.lit(MARKET_CAP_NEUTRALIZED).alias("signal_variant"))
        neutralized = neutralized.drop(helper_columns)
        return (
            pl.concat([factor_frame, neutralized], how="diagonal_relaxed").sort(
                "signal_variant", "signal_date", "instrument_id", "factor_id"
            ),
            coverage,
        )

    def _neutralization_analysis_inputs(
        self,
        eligible: pl.DataFrame,
        universe_ids: tuple[InstrumentId, ...],
        sessions: tuple[date, ...],
        config: FactorStudyDefinition,
    ) -> tuple[pl.DataFrame | None, pl.DataFrame]:
        """一次读取 PIT 行业与市值并返回可跨因子复用的对齐表和覆盖证据。"""
        industry = config.industry
        market_cap = config.market_cap
        empty = pl.DataFrame(
            schema={
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
        )
        if industry is None and market_cap is None:
            return None, empty
        aligned = eligible.filter(pl.col("eligible")).select(
            "signal_date", "instrument_id"
        )
        helper_columns: list[str] = []
        coverage = empty
        if industry is not None:
            state = (
                self._repository.industry_memberships_on_dates(universe_ids, sessions)
                .collect()
                .select(
                    pl.col("query_date").alias("signal_date"),
                    "instrument_id",
                    pl.col("level1_code").alias("industry_code"),
                    pl.lit(True).alias("is_classified"),
                )
                .with_columns(pl.lit(True).alias("_state_present"))
            )
            aligned = aligned.join(
                state, on=["signal_date", "instrument_id"], how="left"
            ).with_columns(
                pl.col("_state_present").fill_null(False),
                (
                    pl.col("is_classified").fill_null(False)
                    & pl.col("industry_code").is_not_null()
                    & (pl.col("industry_code").str.len_chars() > 0)
                ).alias("_classified"),
            )
            if (
                industry.unclassified_policy
                is IndustryUnclassifiedPolicy.UNCLASSIFIED
            ):
                aligned = aligned.with_columns(
                    pl.when(pl.col("_classified"))
                    .then(pl.col("industry_code"))
                    .otherwise(pl.lit("__UNCLASSIFIED__"))
                    .alias("_neutralization_industry")
                )
            else:
                aligned = aligned.with_columns(
                    pl.when(pl.col("_classified"))
                    .then(pl.col("industry_code"))
                    .otherwise(pl.lit(None, dtype=pl.String))
                    .alias("_neutralization_industry")
                )
            coverage = (
                aligned.group_by("signal_date")
                .agg(
                    pl.len().cast(pl.Int64).alias("eligible_count"),
                    pl.col("_classified")
                    .sum()
                    .cast(pl.Int64)
                    .alias("classified_count"),
                    (pl.col("_state_present") & ~pl.col("_classified"))
                    .sum()
                    .cast(pl.Int64)
                    .alias("tombstone_count"),
                    (~pl.col("_state_present"))
                    .sum()
                    .cast(pl.Int64)
                    .alias("missing_state_count"),
                    pl.col("_neutralization_industry")
                    .is_not_null()
                    .sum()
                    .cast(pl.Int64)
                    .alias("usable_count"),
                )
                .with_columns(
                    pl.lit(industry.taxonomy).alias("taxonomy"),
                    pl.lit(industry.unclassified_policy.value).alias(
                        "unclassified_policy"
                    ),
                    (pl.col("classified_count") / pl.col("eligible_count")).alias(
                        "classified_coverage"
                    ),
                    (pl.col("usable_count") / pl.col("eligible_count")).alias(
                        "usable_coverage"
                    ),
                )
                .select(*empty.columns)
                .sort("signal_date")
            )
            helper_columns.append("_neutralization_industry")

        if market_cap is not None:
            cutoffs = pl.DataFrame(
                {
                    "signal_date": sessions,
                    "_pit_cutoff": [
                        datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(
                            UTC
                        )
                        for value in sessions
                    ],
                },
                schema={
                    "signal_date": pl.Date,
                    "_pit_cutoff": pl.Datetime("us", "UTC"),
                },
            )
            visible = (
                self._repository.stock_daily_basics(
                    universe_ids, sessions[0], sessions[-1]
                )
                .collect()
                .rename({"trade_date": "signal_date"})
                .join(cutoffs, on="signal_date", how="inner")
                .filter(
                    pl.col("pit_usable")
                    & pl.col("available_at").is_not_null()
                    & (pl.col("available_at") <= pl.col("_pit_cutoff"))
                )
                .select(
                    "signal_date",
                    "instrument_id",
                    pl.col("total_market_value").alias(
                        "_neutralization_market_cap"
                    ),
                )
                .sort("signal_date", "instrument_id")
            )
            if visible.select("signal_date", "instrument_id").is_duplicated().any():
                raise ValueError("factor study market-cap input has duplicate keys")
            aligned = aligned.join(
                visible, on=["signal_date", "instrument_id"], how="left"
            )
            helper_columns.append("_neutralization_market_cap")

        return aligned.select("signal_date", "instrument_id", *helper_columns), coverage

    @staticmethod
    def _neutralized_factor_frame(
        factor_frame: pl.DataFrame,
        alignment: pl.DataFrame,
        config: FactorStudyDefinition,
    ) -> pl.DataFrame:
        """对单个方向统一因子应用已对齐的行业和市值暴露。"""
        joined = factor_frame.join(
            alignment,
            on=["signal_date", "instrument_id"],
            how="left",
        )
        if config.industry is not None and config.market_cap is not None:
            neutralized = neutralize_industry_market_cap(
                joined,
                "value",
                "_neutralization_market_cap",
                "_neutralization_industry",
                ("signal_date", "factor_id"),
            ).with_columns(
                pl.lit(INDUSTRY_MARKET_CAP_NEUTRALIZED).alias("signal_variant")
            )
            helper_columns: tuple[str, ...] = (
                "_neutralization_industry",
                "_neutralization_market_cap",
            )
        elif config.industry is not None:
            neutralized = neutralize_industry(
                joined,
                "value",
                "_neutralization_industry",
                ("signal_date", "factor_id"),
            ).with_columns(pl.lit(INDUSTRY_NEUTRALIZED).alias("signal_variant"))
            helper_columns = ("_neutralization_industry",)
        elif config.market_cap is not None:
            neutralized = neutralize_market_cap(
                joined,
                "value",
                "_neutralization_market_cap",
                ("signal_date", "factor_id"),
            ).with_columns(pl.lit(MARKET_CAP_NEUTRALIZED).alias("signal_variant"))
            helper_columns = ("_neutralization_market_cap",)
        else:
            raise ValueError("factor study neutralization is not configured")
        return neutralized.drop(helper_columns).sort(
            "signal_date", "instrument_id", "factor_id"
        )

    def _stock_metadata_and_sessions(
        self,
        universe_ids: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """加载股票元数据及带全局序号的研究区间交易日。"""
        identifiers = sorted({item.canonical() for item in universe_ids})
        requested = pl.DataFrame(
            {"instrument_id": identifiers},
            schema={"instrument_id": pl.String},
        )
        catalog = self._repository.stocks().collect()
        required = {"instrument_id", "list_date", "delist_date", "board"}
        if not required.issubset(catalog.schema):
            raise ValueError("factor study stock metadata schema is invalid")
        catalog = catalog.select(*sorted(required)).filter(
            pl.col("instrument_id").is_in(identifiers)
        )
        if catalog["instrument_id"].is_duplicated().any():
            raise ValueError("factor study stock metadata has duplicate keys")
        metadata = requested.join(
            catalog.with_columns(pl.lit(True).alias("metadata_present")),
            on="instrument_id",
            how="left",
        ).with_columns(pl.col("metadata_present").fill_null(False))
        listing_dates = [
            value
            for value in metadata["list_date"].drop_nulls().to_list()
            if isinstance(value, date) and value <= end
        ]
        calendar_start = min(listing_dates) if listing_dates else start
        calendar = self._repository.trade_calendar(calendar_start, end).collect()
        if not {"trade_date", "is_trading_day"}.issubset(calendar.schema):
            raise ValueError("factor study trade calendar schema is invalid")
        if calendar["trade_date"].is_duplicated().any():
            raise ValueError("factor study trade calendar has duplicate keys")
        trading = (
            calendar.filter(
                pl.col("is_trading_day")
                & pl.col("trade_date").is_between(calendar_start, end)
            )
            .select("trade_date")
            .sort("trade_date")
            .with_row_index("session_ordinal", offset=1)
            .with_columns(pl.col("session_ordinal").cast(pl.Int64))
        )
        trading_dates = cast(list[date], trading["trade_date"].to_list())
        listing_ordinals: list[int | None] = []
        for value in metadata["list_date"].to_list():
            if not isinstance(value, date):
                listing_ordinals.append(None)
                continue
            index = bisect_left(trading_dates, value)
            listing_ordinals.append(index + 1 if index < len(trading_dates) else None)
        metadata = metadata.with_columns(
            pl.Series("listing_ordinal", listing_ordinals, dtype=pl.Int64)
        ).sort("instrument_id")
        return metadata, trading.filter(pl.col("trade_date").is_between(start, end))

    def _executable_state(
        self,
        universe_ids: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.DataFrame:
        """使用未复权行情和规则簿向量化判定可执行标签入场涨停。"""
        if not universe_ids:
            return pl.DataFrame(
                schema={
                    "instrument_id": pl.String,
                    "trade_date": pl.Date,
                    "is_listed": pl.Boolean,
                    "is_suspended": pl.Boolean,
                    "entry_limit_up": pl.Boolean,
                }
            )
        metadata, sessions = self._stock_metadata_and_sessions(
            universe_ids, start, end
        )
        metadata = metadata.with_columns(
            pl.lit("STOCK").alias("instrument_type")
        )
        raw_bars = self._repository.stock_bars(
            universe_ids, start, end
        ).collect().select(
            "instrument_id", "trade_date", "low", "preclose"
        )
        suspensions = self._repository.stock_suspensions(
            start, end, universe_ids
        ).collect().select("instrument_id", "trade_date").unique().with_columns(
            pl.lit(True).alias("is_suspended")
        )
        warnings = self._repository.stock_risk_warnings(
            start, end, universe_ids
        ).collect().select("instrument_id", "trade_date").unique().with_columns(
            pl.lit(True).alias("is_st")
        )
        base = (
            metadata.join(sessions, how="cross")
            .join(raw_bars, on=["instrument_id", "trade_date"], how="left")
            .join(suspensions, on=["instrument_id", "trade_date"], how="left")
            .join(warnings, on=["instrument_id", "trade_date"], how="left")
            .with_columns(
                (
                    (pl.col("list_date") <= pl.col("trade_date"))
                    & (
                        pl.col("delist_date").is_null()
                        | (pl.col("delist_date") > pl.col("trade_date"))
                    )
                ).fill_null(False).alias("is_listed"),
                pl.col("is_suspended").fill_null(False),
                pl.col("is_st").fill_null(False),
                pl.when(pl.col("board").is_in(["MAIN", "CHINEXT", "STAR", "BSE"]))
                .then(pl.col("board"))
                .otherwise(pl.lit("MAIN"))
                .alias("board")
            )
            .sort("instrument_id", "trade_date")
            .with_columns(
                (
                    pl.col("session_ordinal")
                    - pl.col("listing_ordinal")
                    + 1
                )
                .alias("_listing_session")
            )
        )
        if base.is_empty():
            return pl.DataFrame(
                schema={
                    "instrument_id": pl.String,
                    "trade_date": pl.Date,
                    "is_listed": pl.Boolean,
                    "is_suspended": pl.Boolean,
                    "entry_limit_up": pl.Boolean,
                }
            )
        parameter_rows: list[dict[str, object]] = []
        groups = base.select(
            "trade_date", "instrument_type", "board", "is_st", "instrument_id"
        ).unique(["trade_date", "instrument_type", "board", "is_st"]).sort(
            "trade_date", "instrument_type", "board", "is_st"
        )
        for row in groups.iter_rows(named=True):
            trade_date = cast(date, row["trade_date"])
            profile = self._rulebook.trading_profile(
                InstrumentId.parse(cast(str, row["instrument_id"])),
                cast(str, row["instrument_type"]),
                Board(cast(str, row["board"])),
                trade_date,
            )
            limit = self._rulebook.price_limit_parameters(
                profile,
                trade_date,
                SecurityStatus.ST if row["is_st"] is True else SecurityStatus.NORMAL,
            )
            parameter_rows.append(
                {
                    "trade_date": trade_date,
                    "instrument_type": row["instrument_type"],
                    "board": row["board"],
                    "is_st": row["is_st"],
                    "_limit_rate_numerator": limit.rate_numerator,
                    "_limit_rate_denominator": limit.rate_denominator,
                    "_price_scale": limit.price_scale,
                    "_tick_units": limit.tick_units,
                }
            )
        parameters = pl.DataFrame(parameter_rows)
        preclose_units = (
            pl.col("preclose") * pl.col("_price_scale")
        ).round(0).cast(pl.Int64, strict=False)
        low_units = (
            pl.col("low") * pl.col("_price_scale")
        ).round(0).cast(pl.Int64, strict=False)
        upper_numerator = preclose_units * (
            pl.col("_limit_rate_denominator")
            + pl.col("_limit_rate_numerator")
        )
        upper_denominator = (
            pl.col("_limit_rate_denominator") * pl.col("_tick_units")
        )
        upper_units = (
            (
                upper_numerator * 2
                + upper_denominator
            )
            // (upper_denominator * 2)
            * pl.col("_tick_units")
        )
        return (
            base.join(
                parameters,
                on=["trade_date", "instrument_type", "board", "is_st"],
                how="left",
            )
            .select(
                "instrument_id",
                "trade_date",
                "is_listed",
                "is_suspended",
                (
                    (pl.col("_listing_session") > 5)
                    &
                    pl.col("low").is_not_null()
                    & pl.col("preclose").is_not_null()
                    & (low_units >= upper_units)
                )
                .fill_null(False)
                .alias("entry_limit_up"),
            )
            .sort("trade_date", "instrument_id")
        )

    @staticmethod
    def _universe_batch_ends(item_total: int) -> tuple[int, ...]:
        """返回首项及最早跨越每个 5% 桶的确定性批次终点。"""
        if item_total <= 0:
            raise ValueError("universe item total must be positive")
        return tuple(
            sorted(
                {
                    1,
                    *(
                        (item_total * bucket + 19) // 20
                        for bucket in range(1, 21)
                    ),
                }
            )
        )

    @staticmethod
    def _pit_status_keys(
        events: pl.DataFrame,
        cutoffs: pl.DataFrame,
    ) -> pl.DataFrame:
        """按每个交易日上海日终过滤并去重股票状态键。"""
        required = {"instrument_id", "trade_date", "available_at", "pit_usable"}
        if not required.issubset(events.schema):
            raise ValueError("factor study stock status schema is invalid")
        return (
            events.join(cutoffs, on="trade_date", how="inner")
            .filter(
                pl.col("pit_usable")
                & pl.col("available_at").is_not_null()
                & (pl.col("available_at") <= pl.col("_pit_cutoff"))
            )
            .select(
                pl.col("trade_date").alias("signal_date"),
                "instrument_id",
            )
            .unique()
            .sort("signal_date", "instrument_id")
        )

    @staticmethod
    def _universe_batch(
        instruments: pl.DataFrame,
        signal_sessions: pl.DataFrame,
        suspended: pl.DataFrame,
        warned: pl.DataFrame,
    ) -> pl.DataFrame:
        """按 CN_STOCK_STANDARD 向量化生成连续日期批次资格表。"""
        return UniverseBatchEvaluator(_CN_STOCK_STANDARD_RULES).evaluate(
            instruments, signal_sessions, suspended, warned
        )

    def _build_factor_study_universe(
        self,
        stock_ids: Sequence[InstrumentId],
        sessions: Sequence[date],
        progress: FactorStudyProgressReporter,
        cancellation: CancellationToken,
    ) -> tuple[pl.DataFrame, tuple[InstrumentId, ...], str]:
        """批量读取 PIT 状态并按确定性日期批次构建因子研究股票池。"""
        if not sessions:
            raise ValueError("factor study has no trading sessions")
        if cancellation.is_cancelled():
            raise RuntimeError("factor study cancelled")
        first_session, last_session = sessions[0], sessions[-1]
        suspended_events = self._repository.stock_suspensions(
            first_session, last_session, stock_ids
        ).collect()
        if cancellation.is_cancelled():
            raise RuntimeError("factor study cancelled")
        warned_events = self._repository.stock_risk_warnings(
            first_session, last_session, stock_ids
        ).collect()
        cutoffs = pl.DataFrame(
            {
                "trade_date": sessions,
                "_pit_cutoff": [
                    datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(
                        UTC
                    )
                    for value in sessions
                ],
            },
            schema={
                "trade_date": pl.Date,
                "_pit_cutoff": pl.Datetime("us", "UTC"),
            },
        )
        suspended = self._pit_status_keys(suspended_events, cutoffs)
        warned = self._pit_status_keys(warned_events, cutoffs)
        instruments, study_sessions = self._stock_metadata_and_sessions(
            stock_ids, first_session, last_session
        )
        expected_sessions = list(sessions)
        if study_sessions["trade_date"].to_list() != expected_sessions:
            raise ValueError("factor study sessions do not match canonical calendar")
        frames: list[pl.DataFrame] = []
        hasher = _CanonicalUniverseHasher()
        previous = 0
        for batch_end in self._universe_batch_ends(len(sessions)):
            if cancellation.is_cancelled():
                raise RuntimeError("factor study cancelled")
            batch = self._universe_batch(
                instruments,
                study_sessions.slice(previous, batch_end - previous),
                suspended,
                warned,
            )
            hasher.update(batch)
            frames.append(batch.select("signal_date", "instrument_id", "eligible"))
            signal_date = sessions[batch_end - 1]
            progress.substage_progress(
                "BUILD_UNIVERSE",
                f"正在准备 PIT 股票池（{batch_end}/{len(sessions)}）",
                item_completed=batch_end,
                item_total=len(sessions),
                evidence={"signal_date": signal_date.isoformat()},
            )
            previous = batch_end
        if cancellation.is_cancelled():
            raise RuntimeError("factor study cancelled")
        eligible = pl.concat(frames)
        universe_hash = hasher.finish()
        universe_ids = hasher.instrument_ids
        progress.substage_completed(
            "BUILD_UNIVERSE",
            "PIT 股票池准备完成",
            {
                "session_count": len(sessions),
                "eligible_row_count": hasher.row_count,
                "instrument_count": len(universe_ids),
                "batch_count": len(frames),
                "suspension_row_count": suspended.height,
                "risk_warning_row_count": warned.height,
                "universe_hash": universe_hash,
            },
        )
        return eligible, universe_ids, universe_hash

    def _streaming_analysis_tables(
        self,
        *,
        source: CanonicalStrategyStudyData,
        eligible: pl.DataFrame,
        universe_ids: tuple[InstrumentId, ...],
        universe_hash: str,
        sessions: tuple[date, ...],
        progress: FactorStudyProgressReporter,
        cancellation: CancellationToken,
        temporary: FactorStudyTemporaryStore,
    ) -> tuple[dict[str, pl.DataFrame], FactorExecutionDescriptor, pl.DataFrame]:
        """逐因子和逐期限落盘，并以单分析单元峰值装配最终小表。"""
        config = self._study.definition
        progress.substage_started(
            "COMPUTE_FACTORS",
            "正在重新计算研究因子",
            {"requested_factor_count": len(config.factor_ids)},
        )
        descriptor = source._factor_engine.execution_descriptor(config.factor_ids)
        directions = {node.factor_ref: node.spec.direction for node in descriptor.plan}
        context = FactorContext(
            self._study.catalog_hash,
            universe_hash,
            config.start_date,
            config.end_date,
        )
        raw_files: dict[str, SpilledFrame] = {}
        factor_row_count = 0
        for factor_id in sorted(config.factor_ids):
            if cancellation.is_cancelled():
                raise RuntimeError("factor study cancelled")
            artifacts = source._factor_engine.compute((factor_id,), context)
            artifact = artifacts[factor_id]
            frame = artifact.lazy_frame().collect()
            raw_files[factor_id] = temporary.write("factor", frame)
            factor_row_count += frame.height
            del frame, artifact, artifacts
        progress.substage_completed(
            "COMPUTE_FACTORS",
            "研究因子计算完成",
            {
                "requested_factor_count": len(config.factor_ids),
                "execution_factor_count": len(descriptor.plan),
                "factor_row_count": factor_row_count,
                "factor_execution_descriptor_hash": descriptor.content_hash,
            },
        )
        progress.substage_started(
            "BUILD_SIGNALS",
            "正在构建方向统一与中性化信号",
            {
                "industry_enabled": config.industry is not None,
                "market_cap_enabled": config.market_cap is not None,
            },
        )
        neutralization_alignment, industry_coverage = (
            self._neutralization_analysis_inputs(
                eligible, universe_ids, sessions, config
            )
        )
        neutralized_variant = (
            INDUSTRY_MARKET_CAP_NEUTRALIZED
            if config.industry is not None and config.market_cap is not None
            else INDUSTRY_NEUTRALIZED
            if config.industry is not None
            else MARKET_CAP_NEUTRALIZED
            if config.market_cap is not None
            else None
        )
        signal_files: dict[tuple[str, str], SpilledFrame] = {}
        signal_row_count = 0
        for factor_id, raw_file in sorted(raw_files.items()):
            if cancellation.is_cancelled():
                raise RuntimeError("factor study cancelled")
            raw = pl.read_parquet(raw_file.path)
            base = (
                raw.rename({"trade_date": "signal_date"})
                .join(
                    eligible.filter(pl.col("eligible")).select(
                        "signal_date", "instrument_id"
                    ),
                    on=["signal_date", "instrument_id"],
                    how="inner",
                )
                .with_columns(
                    (pl.col("value") * directions[factor_id]).alias("value"),
                    pl.lit(DIRECTION_ADJUSTED).alias("signal_variant"),
                    pl.lit(None, dtype=pl.String).alias("invalid_reason"),
                )
                .sort("signal_date", "instrument_id", "factor_id")
            )
            signal_files[(DIRECTION_ADJUSTED, factor_id)] = temporary.write(
                "signal", base
            )
            signal_row_count += base.height
            if (
                neutralization_alignment is not None
                and neutralized_variant is not None
            ):
                neutralized = self._neutralized_factor_frame(
                    base, neutralization_alignment, config
                )
                signal_files[(neutralized_variant, factor_id)] = temporary.write(
                    "signal", neutralized
                )
                signal_row_count += neutralized.height
                del neutralized
            temporary.remove(raw_file)
            del raw, base
        del raw_files, neutralization_alignment
        progress.substage_completed(
            "BUILD_SIGNALS",
            "研究信号构建完成",
            {
                "signal_row_count": signal_row_count,
                "signal_variant_count": len(
                    {variant for variant, _ in signal_files}
                ),
                "signal_variants": cast(
                    list[JsonValue],
                    sorted({variant for variant, _ in signal_files}),
                ),
                "industry_enabled": config.industry is not None,
                "market_cap_enabled": config.market_cap is not None,
                "industry_coverage_row_count": len(industry_coverage),
            },
        )
        progress.substage_started(
            "LOAD_LABEL_INPUTS",
            "正在加载远期收益标签输入",
            {"horizon_count": len(config.horizons)},
        )
        horizon_tail = max(config.horizons)
        later = source._sessions(
            config.end_date + timedelta(days=1),
            config.end_date + timedelta(days=horizon_tail * 3 + 30),
        )
        all_sessions = sessions + later[:horizon_tail]
        bars = self._repository.adjusted_stock_bars(
            universe_ids, config.start_date, all_sessions[-1]
        ).collect()
        executable_state = self._executable_state(
            universe_ids, all_sessions[0], all_sessions[-1]
        )
        progress.substage_completed(
            "LOAD_LABEL_INPUTS",
            "远期收益标签输入加载完成",
            {
                "extended_session_count": len(all_sessions),
                "bar_row_count": len(bars),
                "executable_state_row_count": len(executable_state),
            },
        )
        progress.substage_started(
            "BUILD_FORWARD_RETURNS",
            "正在构建理论与可执行远期收益标签",
            {"horizons": list(config.horizons)},
        )
        label_builder = StreamingForwardReturnBuilder(
            bars, all_sessions, eligible, executable_state
        )
        label_files: dict[int, SpilledFrame] = {}
        label_row_count = 0
        for horizon in config.horizons:
            if cancellation.is_cancelled():
                raise RuntimeError("factor study cancelled")
            wide = label_builder.build(horizon)
            label_files[horizon] = temporary.write("label", wide)
            label_row_count += wide.height * len(LABEL_KINDS)
            del wide
        del label_builder, bars, executable_state
        progress.substage_completed(
            "BUILD_FORWARD_RETURNS",
            "远期收益标签构建完成",
            {
                "label_table_count": len(label_files) * len(LABEL_KINDS),
                "label_row_count": label_row_count,
            },
        )
        progress.substage_started(
            "ANALYZE_STATISTICS",
            "正在计算 IC、分层、换手和成本统计",
            {
                "quantiles": config.quantiles,
                "cost_scenario_count": len(config.cost_bps_scenarios),
            },
        )
        tables = StreamingStudyAnalyzer(
            quantiles=config.quantiles,
            cost_bps_scenarios=config.cost_bps_scenarios,
            cancellation=cancellation,
            temporary=temporary,
        ).run(signal_files, eligible, label_files)
        tables["industry_coverage"] = industry_coverage
        progress.substage_completed(
            "ANALYZE_STATISTICS",
            "因子统计分析完成",
            {
                "table_row_counts": {
                    name: len(frame) for name, frame in sorted(tables.items())
                }
            },
        )
        return tables, descriptor, industry_coverage

    def _analyze(
        self,
        progress: FactorStudyProgressReporter,
        cancellation: CancellationToken,
    ) -> None:
        config = self._study.definition
        source = CanonicalStrategyStudyData(
            self._repository, self._study.catalog_hash
        )
        progress.substage_started(
            "BUILD_UNIVERSE",
            "正在准备逐日 PIT 股票池",
            {
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
            },
        )
        sessions = source._sessions(config.start_date, config.end_date)
        if not sessions:
            raise ValueError("factor study has no trading sessions")
        eligible, universe_ids, universe_hash = self._build_factor_study_universe(
            source._stock_ids,
            sessions,
            progress,
            cancellation,
        )
        with FactorStudyTemporaryStore(
            self._artifact_root.parent, self._study.id
        ) as temporary:
            self._tables, descriptor, _ = self._streaming_analysis_tables(
                source=source,
                eligible=eligible,
                universe_ids=universe_ids,
                universe_hash=universe_hash,
                sessions=sessions,
                progress=progress,
                cancellation=cancellation,
                temporary=temporary,
            )
        progress.substage_started(
            "BUILD_METRICS",
            "正在整理研究指标与分析身份",
        )
        self._analysis_identity = {
            "universe_hash": universe_hash,
            "factor_execution_descriptor": descriptor.json_value(),
            "factor_execution_descriptor_hash": descriptor.content_hash,
            "rulebook_hash": self._rulebook.content_hash,
            "label_kinds": list(LABEL_KINDS),
            "label_definitions": {
                THEORETICAL_FORWARD_RETURN: (
                    "T+1 adjusted open to T+h adjusted close; endpoint prices required"
                ),
                EXECUTABLE_FORWARD_RETURN: (
                    "theoretical label plus listed, not suspended and not one-price "
                    "limit-up at T+1 entry"
                ),
            },
            "industry": cast(
                JsonValue,
                config.industry.model_dump(mode="json")
                if config.industry is not None
                else None,
            ),
            "hac_kernel": "BARTLETT",
            "hac_lag": "min(horizon-1, signal_date_count-1)",
            "turnover_formula": "0.5*sum(abs(weight_t-weight_t_minus_1)) per leg",
            "cost_formula": "gross_spread-total_turnover*bps/10000",
            "cost_bps_scenarios": list(
                config.cost_bps_scenarios
            ),
        }
        self._metrics = _FactorPublisher.metrics(self._tables, config.correction)
        progress.substage_completed(
            "BUILD_METRICS",
            "研究指标与分析身份整理完成",
            {"metric_count": len(self._metrics)},
        )

    def _persist(
        self,
        progress: FactorStudyProgressReporter,
        cancellation: CancellationToken,
    ) -> dict[str, JsonValue]:
        if (
            self._tables is None
            or self._metrics is None
            or self._analysis_identity is None
        ):
            raise RuntimeError("factor persistence requires completed analysis")
        if cancellation.is_cancelled():
            raise RuntimeError("factor persistence cancelled before publication")
        progress.substage_started(
            "PUBLISH_ARTIFACTS",
            "正在写入并复核因子研究产物",
            {"table_count": len(self._tables)},
        )
        directory, manifest_hash, artifacts = _FactorPublisher(
            self._artifact_root, self._study.id
        ).publish(
            self._tables,
            self._study,
            self._metrics,
            self._analysis_identity,
        )
        self._published_dir = directory
        progress.substage_completed(
            "PUBLISH_ARTIFACTS",
            "因子研究产物写入并复核完成",
            {
                "artifact_count": len(artifacts),
                "artifact_row_count": sum(
                    cast(int, item["row_count"])
                    for item in artifacts
                    if isinstance(item.get("row_count"), int)
                ),
                "artifact_byte_count": sum(
                    cast(int, item["byte_count"])
                    for item in artifacts
                    if isinstance(item.get("byte_count"), int)
                ),
                "manifest_hash": manifest_hash,
            },
        )
        if cancellation.is_cancelled():
            self.abort()
            raise RuntimeError("factor persistence cancelled after publication")
        progress.substage_started(
            "REGISTER_OUTPUTS",
            "正在登记因子研究指标与产物",
            {
                "metric_count": len(self._metrics),
                "artifact_count": len(artifacts),
            },
        )
        try:
            self._registry.register_outputs(self._study.id, self._metrics, artifacts)
        except BaseException:
            if directory.is_relative_to(self._artifact_root.resolve()):
                shutil.rmtree(directory, ignore_errors=True)
            self._published_dir = None
            raise
        progress.substage_completed(
            "REGISTER_OUTPUTS",
            "因子研究指标与产物登记完成",
            {
                "metric_count": len(self._metrics),
                "artifact_count": len(artifacts),
            },
        )
        return {"artifact_dir": str(directory), "manifest_hash": manifest_hash}


class _FactorPublisher:
    """以同文件系统 staging 原子发布因子研究表和 Manifest。"""

    def __init__(self, root: Path, factor_study_id: str) -> None:
        self._target = root.resolve() / "factor-studies" / factor_study_id

    def publish(
        self,
        tables: Mapping[str, pl.DataFrame],
        study: FactorStudyRecord,
        metrics: Mapping[str, tuple[float, str | None, float | None, float | None]],
        analysis_identity: Mapping[str, JsonValue],
    ) -> tuple[Path, str, tuple[dict[str, JsonValue], ...]]:
        """写入稳定排序 Parquet、配置和 Manifest，禁止覆盖。"""
        if self._target.exists():
            raise FileExistsError("Run artifact directory already exists")
        self._target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(
            tempfile.mkdtemp(prefix=f".{study.id}-", dir=self._target.parent)
        )
        try:
            entries: list[dict[str, JsonValue]] = []
            for name, frame in sorted(tables.items()):
                keys = _FACTOR_ARTIFACT_KEYS[name]
                frame = frame.sort(keys)
                if frame.select(pl.struct(keys).is_duplicated().any()).item():
                    raise ValueError("factor artifact primary key is not unique")
                path = staging / f"{name}.parquet"
                frame.write_parquet(path)
                entries.append(
                    self._entry(
                        path,
                        name,
                        len(frame),
                        {key: str(value) for key, value in frame.schema.items()},
                    )
                )
            config_path = staging / "config.json"
            config_path.write_bytes(
                canonical_json_bytes(study.definition.model_dump(mode="json"))
            )
            entries.append(self._entry(config_path, "config", None, None))
            metrics_path = staging / "metrics.json"
            metrics_path.write_bytes(
                canonical_json_bytes(
                    {
                        name: {
                            "value": values[0],
                            "unit": values[1],
                            "p_value": values[2],
                            "adjusted_p_value": values[3],
                        }
                        for name, values in sorted(metrics.items())
                    }
                )
            )
            entries.append(self._entry(metrics_path, "metrics", None, None))
            manifest: dict[str, JsonValue] = {
                "factor_study_id": study.id,
                "catalog_hash": study.catalog_hash,
                "analysis_identity": dict(analysis_identity),
                "artifacts": cast(list[JsonValue], entries),
            }
            manifest_path = staging / "manifest.json"
            manifest_path.write_bytes(canonical_json_bytes(manifest))
            manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            os.replace(staging, self._target)
            final_manifest = self._target / "manifest.json"
            if hashlib.sha256(final_manifest.read_bytes()).hexdigest() != manifest_hash:
                raise ValueError("factor manifest changed during publication")
            for entry in entries:
                final_path = self._target / cast(str, entry["relative_path"])
                content = final_path.read_bytes()
                if (
                    len(content) != entry["byte_count"]
                    or hashlib.sha256(content).hexdigest() != entry["content_hash"]
                ):
                    raise ValueError("factor artifact changed during publication")
                if final_path.suffix == ".parquet":
                    final_frame = pl.read_parquet(final_path)
                    if len(final_frame) != entry["row_count"]:
                        raise ValueError("factor artifact row count changed")
                    actual_schema = {
                        key: str(value) for key, value in final_frame.schema.items()
                    }
                    if actual_schema != entry["schema"]:
                        raise ValueError("factor artifact schema changed")
                    keys = tuple(cast(list[str], entry["sort_key"]))
                    if not final_frame.equals(final_frame.sort(keys)):
                        raise ValueError("factor artifact rows are not sorted")
                else:
                    canonical_json_bytes(
                        cast(JsonValue, json.loads(final_path.read_bytes()))
                    )
            return self._target, manifest_hash, tuple(entries)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            if self._target.exists():
                shutil.rmtree(self._target, ignore_errors=True)
            raise

    @staticmethod
    def _entry(
        path: Path,
        artifact_type: str,
        row_count: int | None,
        schema: Mapping[str, JsonValue] | None,
    ) -> dict[str, JsonValue]:
        encoded = path.read_bytes()
        return {
            "artifact_type": artifact_type,
            "relative_path": path.name,
            "content_hash": hashlib.sha256(encoded).hexdigest(),
            "byte_count": len(encoded),
            "row_count": row_count,
            "schema": schema,
            "primary_key": list(_FACTOR_ARTIFACT_KEYS[artifact_type])
            if row_count is not None
            else None,
            "sort_key": list(_FACTOR_ARTIFACT_KEYS[artifact_type])
            if row_count is not None
            else None,
        }

    @staticmethod
    def metrics(
        tables: Mapping[str, pl.DataFrame],
        correction: MultipleTestingMethod,
    ) -> dict[str, tuple[float, str | None, float | None, float | None]]:
        """登记健康度指标并分别校正 Rank IC 和多空 spread 假设族。"""
        result: dict[str, tuple[float, str | None, float | None, float | None]] = {}
        coverage, ic, summary = (
            tables.get("coverage"),
            tables.get("ic"),
            tables.get("summary"),
        )
        if (
            coverage is not None
            and not coverage.is_empty()
            and "coverage" in coverage.columns
        ):
            mean_coverage = cast(float | None, coverage["coverage"].drop_nulls().mean())
            if mean_coverage is not None:
                result["mean_factor_coverage"] = (
                    mean_coverage,
                    "ratio",
                    None,
                    None,
                )
        if ic is not None and not ic.is_empty():
            mean_pair_coverage = cast(
                float | None, ic["pair_coverage"].drop_nulls().mean()
            )
            if mean_pair_coverage is not None:
                result["mean_pair_coverage"] = (
                    mean_pair_coverage,
                    "ratio",
                    None,
                    None,
                )
        if summary is not None and not summary.is_empty():
            families: dict[str, list[tuple[str, float, float]]] = {
                "rank_ic": [],
                "long_short": [],
            }
            for row in summary.sort(
                "signal_variant", "label_kind", "factor_ref", "horizon"
            ).to_dicts():
                dimensions = (
                    str(row["signal_variant"]),
                    str(row["label_kind"]),
                    str(row["factor_ref"]),
                    str(row["horizon"]),
                )
                for family, value_column, p_column in (
                    ("rank_ic", "rank_ic_mean", "rank_ic_hac_p_value"),
                    (
                        "long_short",
                        "long_short_mean",
                        "long_short_hac_p_value",
                    ),
                ):
                    value, p_value = row.get(value_column), row.get(p_column)
                    if (
                        isinstance(value, (int, float))
                        and not isinstance(value, bool)
                        and isinstance(p_value, (int, float))
                        and not isinstance(p_value, bool)
                    ):
                        families[family].append(
                            (
                                "/".join((value_column, *dimensions)),
                                float(value),
                                float(p_value),
                            )
                        )
            significant_rank = 0
            for family, hypotheses in families.items():
                adjusted = MultipleTestingCorrector.adjust(
                    correction, tuple(item[2] for item in hypotheses)
                )
                for (name, value, p_value), adjusted_p_value in zip(
                    hypotheses, adjusted, strict=True
                ):
                    result[name] = (
                        value,
                        "ratio",
                        p_value,
                        adjusted_p_value,
                    )
                    if family == "rank_ic" and adjusted_p_value <= 0.05:
                        significant_rank += 1
            result["tested_rank_ic_count"] = (
                float(len(families["rank_ic"])),
                "count",
                None,
                None,
            )
            result["significant_rank_ic_count"] = (
                float(significant_rank),
                "count",
                None,
                None,
            )
        return result


class _WorkerRecovery:
    """回收失联任务，并将其仍活动的研究同步终结为失败。"""

    def __init__(
        self,
        queue: TaskQueue,
        registry: StrategyStudyRegistry,
        factor_studies: FactorStudyRegistry,
        stale_after: timedelta = _ORPHAN_STALE_AFTER,
    ) -> None:
        self._queue = queue
        self._registry = registry
        self._factor_studies = factor_studies
        self._stale_after = stale_after

    def __call__(self, now: datetime) -> int:
        """回收超时任务并收敛全部 ORPHANED 研究。"""
        recovered = self._queue.mark_orphans(now, self._stale_after)
        offset = 0
        while True:
            tasks = self._queue.list(
                status=TaskStatus.ORPHANED,
                subject_kind="STRATEGY_STUDY",
                limit=_ORPHAN_PAGE_SIZE,
                offset=offset,
            )
            for task in tasks:
                if task.subject_id is None:
                    continue
                try:
                    strategy_study = self._registry.get(task.subject_id)
                except KeyError:
                    continue
                if strategy_study.status not in _ACTIVE_STRATEGY_STUDY_STATUSES:
                    continue
                error: dict[str, JsonValue] = {
                    "code": "TASK_ORPHANED",
                    "message": "Strategy study heartbeat exceeded the stale threshold",
                    "task_id": task.id,
                }
                try:
                    self._registry.transition(
                        strategy_study.id,
                        strategy_study.status,
                        StrategyStudyStatus.FAILED,
                        stage=strategy_study.stage,
                        error=error,
                    )
                except ValueError:
                    current = self._registry.get(strategy_study.id)
                    if current.status in _ACTIVE_STRATEGY_STUDY_STATUSES:
                        raise
            if len(tasks) < _ORPHAN_PAGE_SIZE:
                break
            offset += len(tasks)
        offset = 0
        while True:
            studies = self._queue.list(
                status=TaskStatus.ORPHANED,
                subject_kind="FACTOR_STUDY",
                limit=_ORPHAN_PAGE_SIZE,
                offset=offset,
            )
            for task in studies:
                if task.subject_id is None:
                    continue
                try:
                    factor_study = self._factor_studies.get(task.subject_id)
                except KeyError:
                    continue
                if factor_study.status not in _ACTIVE_FACTOR_STUDY_STATUSES:
                    continue
                self._factor_studies.transition(
                    factor_study.id,
                    factor_study.status,
                    FactorStudyStatus.FAILED,
                    stage=factor_study.stage,
                    error={
                        "code": "TASK_ORPHANED",
                        "message": "Factor study heartbeat exceeded the stale threshold",
                        "task_id": task.id,
                    },
                )
            if len(studies) < _ORPHAN_PAGE_SIZE:
                break
            offset += len(studies)
        return recovered


def build_strategy_study_worker(
    *,
    engine: Engine,
    worker_id: str,
    repository: CanonicalResearchRepository,
    artifact_root: Path,
    rulebook: AShareRuleBook,
    extra_handlers: tuple[TaskHandler, ...] = (),
) -> Worker:
    """作为组合根模块级入口装配策略研究、因子研究和附加任务。

    入参：
        engine、worker_id、repository、artifact_root、rulebook：运行依赖；
        extra_handlers：数据任务等额外处理器。
    返回值：
        返回可处理 STRATEGY_STUDY 及附加任务类型的 Worker。
    异常：
        处理器类型重复、规则或日志根非法时在装配阶段抛出。
    """
    log_root = artifact_root.parent / "state" / "task-logs"
    queue, registry, factor_studies = (
        TaskQueue(engine, task_log_root=log_root),
        StrategyStudyRegistry(engine),
        FactorStudyRegistry(engine),
    )
    handler = StrategyStudyHandler(
        registry,
        CanonicalCatalogGuard(repository),
        StrategyStudyExecutor(
            repository,
            registry,
            StrategyRegistry.builtins(
                commission_bps=rulebook.commission_bps,
                commission_minimum_fen=rulebook.commission_minimum_fen,
            ),
            rulebook,
            artifact_root,
        ),
    )
    factor_handler = FactorStudyHandler(
        factor_studies,
        CanonicalCatalogGuard(repository),
        IndependentFactorStudyExecutor(
            repository, factor_studies, rulebook, artifact_root
        ),
    )
    logs = TaskLogManager(
        diagnostic_root=log_root, artifact_root=artifact_root, sensitive_values=()
    )
    return Worker(
        queue,
        worker_id=worker_id,
        handlers=(handler, factor_handler, *extra_handlers),
        orphan_recovery=_WorkerRecovery(queue, registry, factor_studies),
        task_logs=logs,
    )


def build_default_strategy_study_worker(
    *,
    worker_id: str,
    engine: Engine | None = None,
    extra_handlers: tuple[TaskHandler, ...] = (),
) -> Worker:
    """作为组合根模块级入口从本地设置装配默认研究 Worker。

    入参：
        worker_id：Worker 身份；engine：可选已有数据库引擎；extra_handlers：
        数据更新和质量任务处理器。
    返回值：
        返回连接本地 SQLite、Canonical Repository 和规则文件的 Worker。
    异常：
        设置、数据库迁移、数据根或交易规则无效时传播对应启动异常。
    """
    source_root, settings = Path(__file__).resolve().parents[3], Settings.load()
    upgrade_database(settings.state_db)
    service_engine = engine or create_sqlite_engine(settings.state_db)
    repository = CanonicalResearchRepository.from_sqlite(
        service_engine, trusted_curated_root=settings.curated_root
    )
    rulebook = AShareRuleBook.load(source_root / "configs" / "rules" / "a_share.yaml")
    return build_strategy_study_worker(
        engine=service_engine,
        worker_id=worker_id,
        repository=repository,
        artifact_root=settings.artifact_root,
        rulebook=rulebook,
        extra_handlers=extra_handlers,
    )


__all__ = [
    "CanonicalCatalogGuard",
    "CanonicalStrategyStudyData",
    "build_default_strategy_study_worker",
    "build_strategy_study_worker",
]
