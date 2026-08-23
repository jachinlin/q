"""装配以 Canonical Repository 为唯一数据入口的 Experiment Run Worker。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import polars as pl
from sqlalchemy import Engine

from quant_research.application.worker import Worker
from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.engine import (
    BacktestEngine,
    BacktestRequest,
    BoundMarketSlice,
)
from quant_research.backtest.models import ExecutionConfig, ExecutionPrice, MarketSlice
from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.config import Settings
from quant_research.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.domain.identifiers import InstrumentId
from quant_research.experiments.models import (
    FactorStudyRunConfig,
    MultipleTestingMethod,
    RunRecord,
    StrategyBacktestRunConfig,
)
from quant_research.experiments.runner import ExperimentRunHandler
from quant_research.experiments.statistics import MultipleTestingCorrector
from quant_research.factor_studies.analysis import analyze, build_future_returns
from quant_research.factors import FactorContext, FactorEngine, FactorRegistry
from quant_research.factors.builtin import register_stock_factors
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.experiment_runs import (
    ExperimentRunRegistry,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import TaskLogManager
from quant_research.strategies.base import DecisionData
from quant_research.strategies.registry import StrategyRegistry
from quant_research.tasks.handlers import CancellationToken, ProgressSink, TaskHandler
from quant_research.tasks.models import TaskProgress

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
    "summary": ("signal_variant", "factor_ref", "horizon"),
    "coverage": ("signal_variant", "factor_ref", "signal_date"),
    "ic": ("signal_variant", "factor_ref", "horizon", "signal_date"),
    "quantile_returns": (
        "signal_variant",
        "factor_ref",
        "horizon",
        "signal_date",
        "quantile",
    ),
    "long_short_returns": (
        "signal_variant",
        "factor_ref",
        "horizon",
        "signal_date",
    ),
    "correlation": ("signal_variant", "factor_x", "factor_y"),
}


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


class CanonicalRunData:
    """将只读 Canonical Repository 适配为回测行情和 PIT 决策工厂。

    入参：
        repository：唯一 Canonical 数据读取入口；catalog_hash：Run 冻结数据身份。
    返回值：
        创建可提供交易日历、逐日行情和信号日数据的 Run 数据源。
    异常：
        证券目录不可读或因子注册冲突时在构造阶段抛出。
    """

    def __init__(
        self, repository: CanonicalResearchRepository, catalog_hash: str
    ) -> None:
        self._repository = repository
        self._catalog_hash = catalog_hash
        self._instruments = repository.instruments().collect().sort("instrument_id")
        self._metadata = {
            cast(str, row["instrument_id"]): row for row in self._instruments.to_dicts()
        }
        self._stock_ids = tuple(
            InstrumentId.parse(identifier)
            for identifier, row in sorted(self._metadata.items())
            if row.get("instrument_type") == "STOCK"
        )
        registry = FactorRegistry()
        if self._stock_ids:
            register_stock_factors(
                registry,
                repository,
                repository,
                self._stock_ids,
                price_service=repository,
            )
        self._factor_engine = FactorEngine(
            registry, capabilities=ProviderCapabilities.complete()
        )

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
        identifiers = tuple(InstrumentId.parse(value) for value in self._metadata)
        bars = self._repository.bars(identifiers, trade_date, trade_date).collect()
        indexes = tuple(
            InstrumentId.parse(value)
            for value, row in sorted(self._metadata.items())
            if row.get("instrument_type") == "INDEX"
        )
        if indexes:
            index_bars = self._repository.index_bars(
                indexes, trade_date, trade_date
            ).collect()
            if not index_bars.is_empty():
                bars = pl.concat(
                    [bars, index_bars.rename({"index_id": "instrument_id"})],
                    how="diagonal_relaxed",
                )
        statuses = self._repository.security_status(trade_date).collect()
        status_map = {
            cast(str, row["instrument_id"]): row for row in statuses.to_dicts()
        }
        rows: list[dict[str, object]] = []
        for row in bars.to_dicts():
            identifier = cast(str, row["instrument_id"])
            metadata = self._metadata.get(identifier)
            if metadata is None:
                continue
            status = status_map.get(identifier, {})
            board = str(metadata.get("board") or "MAIN")
            if board not in {"MAIN", "CHINEXT", "STAR"}:
                board = "MAIN"
            raw_volume = row.get("volume")
            rows.append(
                {
                    "instrument_id": identifier,
                    **{
                        name: row.get(name)
                        for name in ("open", "high", "low", "close", "preclose")
                    },
                    "volume": int(raw_volume) if raw_volume is not None else None,
                    "is_suspended": bool(status.get("is_suspended", False)),
                    "security_status": "ST"
                    if status.get("is_st") is True
                    else "NORMAL",
                    "instrument_type": str(metadata.get("instrument_type") or "STOCK"),
                    "board": board,
                }
            )
        frame = pl.DataFrame(rows, schema=_MARKET_SCHEMA, strict=False).sort(
            "instrument_id"
        )
        return BoundMarketSlice(MarketSlice(trade_date, frame))

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
        statuses = self._repository.security_status(
            signal_date, self._stock_ids
        ).collect()
        status_map = {
            cast(str, row["instrument_id"]): row for row in statuses.to_dicts()
        }
        rows: list[dict[str, object]] = []
        for instrument in self._stock_ids:
            status = status_map.get(instrument.canonical())
            reasons: list[str] = []
            if status is None:
                reasons.append("STATUS_MISSING")
            elif status.get("is_st") is True:
                reasons.append("RISK_WARNING")
            elif status.get("is_suspended") is True:
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


class _BoundDecisionData:
    """绑定一个 signal_date 并封闭所有显式日期参数。"""

    def __init__(self, source: CanonicalRunData, signal_date: date) -> None:
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
        return self._repository.bars(
            instruments,
            self._source._lookback_start(self._signal_date, lookback_sessions),
            self._signal_date,
        )

    def adjusted_bars(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的前复权窗口。"""
        return self._repository.adjusted_bars(
            instruments,
            self._source._lookback_start(self._signal_date, lookback_sessions),
            self._signal_date,
        )

    def log_returns(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的对数收益窗口。"""
        return self._repository.log_returns(
            instruments,
            self._signal_date,
            self._signal_date,
            lookback_sessions=lookback_sessions,
        )

    def daily_basics(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取截至信号日的每日基础指标窗口。"""
        return self._repository.daily_basics(
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
        return self._repository.industry_classifications_as_of(
            instruments, self._signal_date
        )

    def security_status(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame:
        """读取信号日交易状态。"""
        return self._repository.security_status(self._signal_date, instruments)

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
                stage="STRATEGY_RUN",
                completed=completed,
                total=total,
                message=trade_date.isoformat(),
            )
        )


class StrategyRunExecutor:
    """构造策略并运行唯一订单驱动回测引擎。

    入参：
        repository、registry、strategies、rulebook、artifact_root：数据入口、Run
        登记簿、策略目录、唯一 A 股规则和不可变产物根。
    返回值：
        创建策略 Run 执行器。
    异常：
        构造不执行回测；运行期错误由 execute 方法说明。
    """

    def __init__(
        self,
        repository: CanonicalResearchRepository,
        registry: ExperimentRunRegistry,
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

    def execute(
        self, run: RunRecord, progress: ProgressSink, cancellation: CancellationToken
    ) -> dict[str, JsonValue]:
        """执行策略 Run，验证并登记固定产物。

        入参：
            run：冻结策略 Run；progress：任务进度端口；cancellation：取消端口。
        返回值：
            返回产物目录、Manifest 哈希和已完成交易日数。
        异常：
            TypeError：Run kind 错误时抛出；策略、数据漂移、回测或产物发布错误
            继续向 ExperimentRunHandler 传播。
        """
        config = run.config
        if not isinstance(config, StrategyBacktestRunConfig):
            raise TypeError("strategy executor requires STRATEGY_BACKTEST config")
        strategy = self._strategies.build(
            config.strategy.strategy_id,
            cast(Mapping[str, JsonValue], config.strategy.parameters),
        )
        source = CanonicalRunData(self._repository, run.catalog_hash)
        result = BacktestEngine(
            source,
            source,
            self._rulebook,
            CanonicalCatalogGuard(self._repository),
            artifact_root=self._artifact_root,
        ).run(
            BacktestRequest(
                run.experiment_id,
                run.id,
                run.catalog_hash,
                config.start_date,
                config.end_date,
                InstrumentId.parse(config.benchmark),
                config.initial_cash_fen,
                self._rulebook.content_hash,
                ExecutionConfig(
                    ExecutionPrice(config.execution.reference_price),
                    config.execution.slippage_bps,
                    config.execution.max_volume_participation,
                ),
            ),
            strategy,
            _BacktestProgress(progress),
            cancellation,
        )
        self._registry.register_outputs(
            run.id,
            {key: (value, None, None, None) for key, value in result.metrics.items()},
            result.artifacts,
        )
        return {
            "artifact_dir": str(result.artifact_dir),
            "manifest_hash": result.manifest_hash,
            "sessions_completed": result.sessions_completed,
        }


class FactorRunExecutor:
    """复用 FactorEngine 和因子分析统计内核执行统一因子 Run。

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
        registry: ExperimentRunRegistry,
        artifact_root: Path,
    ) -> None:
        self._repository, self._registry, self._artifact_root = (
            repository,
            registry,
            artifact_root,
        )

    def execute(
        self, run: RunRecord, progress: ProgressSink, cancellation: CancellationToken
    ) -> dict[str, JsonValue]:
        """计算因子、未来收益和诊断表并原子发布。

        入参：
            run：冻结因子 Run；progress：任务进度端口；cancellation：取消端口。
        返回值：
            返回已发布目录和 Manifest 哈希。
        异常：
            TypeError：Run kind 错误时抛出；ValueError：区间无交易日或未来收益
            覆盖不足时抛出；取消、因子和发布错误继续向任务边界传播。
        """
        config = run.config
        if not isinstance(config, FactorStudyRunConfig):
            raise TypeError("factor executor requires FACTOR_STUDY config")
        source = CanonicalRunData(self._repository, run.catalog_hash)
        sessions = source._sessions(config.start_date, config.end_date)
        if not sessions:
            raise ValueError("factor study has no trading sessions")
        eligible_frames: list[pl.DataFrame] = []
        for index, signal_date in enumerate(sessions):
            if cancellation.is_cancelled():
                raise RuntimeError("factor study cancelled")
            eligible_frames.append(
                source.universe(signal_date).rename({"as_of": "signal_date"})
            )
            progress.update(
                TaskProgress(
                    stage="ANALYZE_FACTORS",
                    completed=index + 1,
                    total=len(sessions),
                    message=signal_date.isoformat(),
                )
            )
        eligible = pl.concat(eligible_frames).sort("signal_date", "instrument_id")
        universe_ids = tuple(
            InstrumentId.parse(value)
            for value in sorted(
                set(eligible.filter(pl.col("eligible"))["instrument_id"].to_list())
            )
        )
        universe_hash = hashlib.sha256(
            canonical_json_bytes([item.canonical() for item in universe_ids])
        ).hexdigest()
        artifacts_by_factor = source._factor_engine.compute(
            config.factor_study.factor_ids,
            FactorContext(
                run.catalog_hash, universe_hash, config.start_date, config.end_date
            ),
        )
        factor_frame = pl.concat(
            [
                artifacts_by_factor[item].lazy_frame().collect()
                for item in config.factor_study.factor_ids
            ]
        )
        horizon_tail = max(config.factor_study.horizons)
        later = source._sessions(
            config.end_date + timedelta(days=1),
            config.end_date + timedelta(days=horizon_tail * 3 + 30),
        )
        all_sessions = sessions + later[:horizon_tail]
        bars = self._repository.adjusted_bars(
            universe_ids, config.start_date, all_sessions[-1]
        ).collect()
        future = build_future_returns(
            bars, all_sessions, eligible, config.factor_study.horizons
        )
        tables = analyze(
            factor_frame, eligible, future, quantiles=config.factor_study.quantiles
        )
        governance = self._registry.get_experiment(
            run.experiment_id
        ).experiment.definition.governance
        metrics = _FactorPublisher.metrics(tables, governance.correction)
        directory, manifest_hash, artifacts = _FactorPublisher(
            self._artifact_root, run.experiment_id, run.id
        ).publish(tables, run, metrics)
        self._registry.register_outputs(run.id, metrics, artifacts)
        return {"artifact_dir": str(directory), "manifest_hash": manifest_hash}


class _FactorPublisher:
    """以同文件系统 staging 原子发布因子研究表和 Manifest。"""

    def __init__(self, root: Path, experiment_id: str, run_id: str) -> None:
        self._target = root.resolve() / "experiments" / experiment_id / run_id

    def publish(
        self,
        tables: Mapping[str, pl.DataFrame],
        run: RunRecord,
        metrics: Mapping[str, tuple[float, str | None, float | None, float | None]],
    ) -> tuple[Path, str, tuple[dict[str, JsonValue], ...]]:
        """写入稳定排序 Parquet、配置和 Manifest，禁止覆盖。"""
        if self._target.exists():
            raise FileExistsError("Run artifact directory already exists")
        self._target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{run.id}-", dir=self._target.parent))
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
                canonical_json_bytes(run.config.model_dump(mode="json"))
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
                "experiment_id": run.experiment_id,
                "run_id": run.id,
                "catalog_hash": run.catalog_hash,
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
        """提取汇总指标并校正每个因子-期限 Rank IC 的 p-value。"""
        result: dict[str, tuple[float, str | None, float | None, float | None]] = {}
        coverage, summary = tables.get("coverage"), tables.get("summary")
        if (
            coverage is not None
            and not coverage.is_empty()
            and "coverage" in coverage.columns
        ):
            mean_coverage = cast(float | None, coverage["coverage"].drop_nulls().mean())
            result["mean_coverage"] = (mean_coverage or 0.0, None, None, None)
        if summary is not None and not summary.is_empty():
            for column in ("rank_ic_mean", "pearson_ic_mean", "long_short_mean"):
                if column in summary.columns:
                    mean_value = cast(float | None, summary[column].drop_nulls().mean())
                    result[column] = (mean_value or 0.0, None, None, None)
            hypotheses: list[tuple[str, float, float]] = []
            for row in summary.sort(
                "signal_variant", "factor_ref", "horizon"
            ).to_dicts():
                mean_value = row.get("rank_ic_mean")
                sample_std = row.get("rank_ic_sample_std")
                count = row.get("rank_ic_valid_date_count")
                if not isinstance(mean_value, (int, float)) or isinstance(
                    mean_value, bool
                ):
                    continue
                if not isinstance(sample_std, (int, float)) or isinstance(
                    sample_std, bool
                ):
                    continue
                if type(count) is not int or sample_std <= 0 or count < 2:
                    continue
                p_value = MultipleTestingCorrector.normal_mean_p_value(
                    float(mean_value), float(sample_std), count
                )
                name = "/".join(
                    (
                        "rank_ic_mean",
                        str(row["signal_variant"]),
                        str(row["factor_ref"]),
                        str(row["horizon"]),
                    )
                )
                hypotheses.append((name, float(mean_value), p_value))
            adjusted = MultipleTestingCorrector.adjust(
                correction, tuple(item[2] for item in hypotheses)
            )
            for (name, value, p_value), adjusted_p_value in zip(
                hypotheses, adjusted, strict=True
            ):
                result[name] = (value, None, p_value, adjusted_p_value)
        return result


def build_experiment_worker(
    *,
    engine: Engine,
    worker_id: str,
    repository: CanonicalResearchRepository,
    artifact_root: Path,
    rulebook: AShareRuleBook,
    extra_handlers: tuple[TaskHandler, ...] = (),
) -> Worker:
    """作为组合根模块级入口装配统一 Run handler、队列和任务日志。

    入参：
        engine、worker_id、repository、artifact_root、rulebook：运行依赖；
        extra_handlers：数据任务等额外处理器。
    返回值：
        返回可处理 EXPERIMENT_RUN 及附加任务类型的 Worker。
    异常：
        处理器类型重复、规则或日志根非法时在装配阶段抛出。
    """
    log_root = artifact_root.parent / "state" / "task-logs"
    queue, registry = (
        TaskQueue(engine, task_log_root=log_root),
        ExperimentRunRegistry(engine),
    )
    handler = ExperimentRunHandler(
        registry,
        CanonicalCatalogGuard(repository),
        StrategyRunExecutor(
            repository,
            registry,
            StrategyRegistry.builtins(
                commission_bps=rulebook.commission_bps,
                commission_minimum_fen=rulebook.commission_minimum_fen,
            ),
            rulebook,
            artifact_root,
        ),
        FactorRunExecutor(repository, registry, artifact_root),
    )
    logs = TaskLogManager(
        diagnostic_root=log_root, artifact_root=artifact_root, sensitive_values=()
    )
    return Worker(
        queue, worker_id=worker_id, handlers=(handler, *extra_handlers), task_logs=logs
    )


def build_default_experiment_worker(
    *,
    worker_id: str,
    engine: Engine | None = None,
    extra_handlers: tuple[TaskHandler, ...] = (),
) -> Worker:
    """作为组合根模块级入口从本地设置装配默认统一实验 Worker。

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
    return build_experiment_worker(
        engine=service_engine,
        worker_id=worker_id,
        repository=repository,
        artifact_root=settings.artifact_root,
        rulebook=rulebook,
        extra_handlers=extra_handlers,
    )


__all__ = [
    "CanonicalCatalogGuard",
    "CanonicalRunData",
    "build_default_experiment_worker",
    "build_experiment_worker",
]
