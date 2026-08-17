"""使用当前 Canonical 数据执行实验的本地组合根与具体运行时。"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import cast
from uuid import UUID, uuid4

import polars as pl
from sqlalchemy import Engine

from quant_research.application.factor_studies import FactorAnalysisHandler
from quant_research.application.worker import Worker
from quant_research.backtest.artifacts import (
    ManifestContext,
    validate_backtest_artifacts,
)
from quant_research.backtest.engine import BacktestRequest, BacktestResult, StrategyRef
from quant_research.backtest.models import (
    ExecutionConfig,
    ExecutionPrice,
)
from quant_research.backtest.rulebook import AShareRuleBook, MarketRuleBook
from quant_research.config import Settings
from quant_research.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_research.data.repository import (
    CanonicalDatasetMissing,
    CanonicalResearchRepository,
    ResearchDataRepository,
)
from quant_research.domain.enums import Board, DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import InstrumentId
from quant_research.experiments.adapters import (
    CanonicalBacktestMarketData,
    CanonicalStrategyRunner,
    PartitionedFactorValueSource,
)
from quant_research.experiments.config import require_provider_capabilities
from quant_research.experiments.fingerprint import capture_environment
from quant_research.experiments.models import ExperimentRecord
from quant_research.experiments.query import ExperimentQuery
from quant_research.experiments.registry import ExperimentRegistry
from quant_research.experiments.runner import (
    BacktestProgressSink,
    CancellationToken,
    ExperimentArtifactFinalizer,
    ExperimentBacktestHandler,
    ExperimentFactorResult,
    ExperimentRunner,
    ExperimentTaskLogMaterializer,
    ExperimentUniverseResult,
    PreparedExperimentRuntime,
    _RunnerSupport,
)
from quant_research.factors import (
    FactorContext,
    FactorEngine,
    FactorRegistry,
    PartitionedFactorEngine,
)
from quant_research.factors.builtin import register_etf_factors, register_stock_factors
from quant_research.infrastructure.baostock.client import BAOSTOCK_RESEARCH_CAPABILITIES
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.factor_studies import (
    FactorStudyRepository,
)
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import TaskLogManager
from quant_research.portfolio import PortfolioConstructor, RebalancePlanner
from quant_research.strategies import (
    EtfRotationConfig,
    EtfRotationStrategy,
    MultifactorConfig,
    MultifactorStrategy,
    Strategy,
    rebalance_signal_dates,
)
from quant_research.tasks.handlers import TaskHandler
from quant_research.universe.builder import UniverseBuilder
from quant_research.universe.rules import UniverseRules

_MARKET_CAPABILITIES = (
    "daily_bars",
    "trade_calendar",
    "instruments",
    "security_status",
)
_STOCK_CAPABILITIES = ("financials_with_announcement_date",)
OFFLINE_ETF_CAPABILITIES = ProviderCapabilities(
    daily_bars=True,
    trade_calendar=True,
    instruments=True,
    security_status=True,
    financials_with_announcement_date=False,
    adjustment_factors=True,
)
_STOCK_REF = StrategyRef("stock_multifactor")
_ETF_REF = StrategyRef("etf_rotation")
_MAX_EXPERIMENT_FACTOR_PARTITION_SIZE = 100
_CAPABILITY_DATASETS = MappingProxyType(
    {
        "daily_bars": DatasetKind.DAILY_BAR,
        "trade_calendar": DatasetKind.TRADE_CALENDAR,
        "instruments": DatasetKind.INSTRUMENT,
        "security_status": DatasetKind.SECURITY_STATUS,
        "financials_with_announcement_date": DatasetKind.FINANCIAL_OBSERVATION,
    }
)
type StrategyFactory = Callable[[Mapping[str, object]], Strategy]


class ExperimentDataDrift(QuantError):
    """表示 ``ExperimentDataDrift`` 对应的领域异常。

    入参：
        expected：``expected``。
        actual：实际值。
        stage：执行阶段。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    The canonical catalog differs from the hash captured at submission.
    """

    def __init__(self, expected: str, actual: str | None, *, stage: str) -> None:
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_DATA_DRIFT",
                severity=Severity.FATAL,
                message="current canonical data changed after experiment submission",
                context={"expected": expected, "actual": actual, "stage": stage},
                remediation="run validate-all and submit a new experiment",
                retryable=False,
            )
        )


def strategy_factories() -> Mapping[StrategyRef, StrategyFactory]:
    """处理运行时依赖装配中的策略``factories``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        无。
    返回值：
        返回``factories``（``Mapping[StrategyRef, StrategyFactory]``）。
    异常：
        无。
    """
    return MappingProxyType(
        {
            _STOCK_REF: lambda value: MultifactorStrategy(
                MultifactorConfig.from_mapping(value)
            ),
            _ETF_REF: lambda value: EtfRotationStrategy(
                EtfRotationConfig.from_mapping(value)
            ),
        }
    )


class ExperimentRuntimeFactory:
    """根据已登记实验组装绑定当前 Canonical 数据的运行时。

    入参：
        repository：提供研究数据及其只读 Canonical 目录的仓储。
        capabilities：当前数据源确实支持的数据集和字段能力。
        provider：数据供应商。
        artifact_root：不可变实验产物的可信根目录。
        rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
        factories：参与本次处理的``factories``；调用方不得依赖未声明的顺序。
        max_partition_size：限制资源使用、数量或等待时间的上限分区字节数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        capabilities: ProviderCapabilities,
        provider: str,
        artifact_root: Path,
        rulebook: MarketRuleBook,
        factories: Mapping[StrategyRef, StrategyFactory] | None = None,
        max_partition_size: int,
    ) -> None:
        """创建实验运行时工厂。

        参数:
            repository: 已封装可信 Curated 根目录及只读 Catalog 的研究数据仓库。
            capabilities: 当前供应商可以提供的研究能力集合。
            provider: 当前供应商配置标识。
            artifact_root: 实验产物根目录。
            rulebook: 回测使用的市场交易规则。
            factories: 可选的策略引用到构造函数映射；省略时使用内置注册表。
            max_partition_size: 实验产物单分区最大字节数。

        返回:
            ``None``。

        抛出:
            TypeError: 能力配置或产物根目录类型不正确。
            ValueError: 供应商标识或分区大小非法。
        """
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        self._repository = repository
        self._capabilities = capabilities
        self._provider = _RuntimeSupport._text(provider, "provider")
        self._artifact_root = artifact_root
        self._rulebook = rulebook
        self._factories = dict(factories or strategy_factories())
        self._max_partition_size = _RuntimeSupport._validated_max_partition_size(
            max_partition_size
        )

    def __call__(self, experiment: ExperimentRecord) -> PreparedExperimentRuntime:
        """为指定实验创建已解析策略和配置的运行时。

        入参：
            experiment：实验。
        返回值：
            返回``call``（``PreparedExperimentRuntime``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        config = _RuntimeSupport._runtime_config(experiment)
        reference = StrategyRef(experiment.strategy_id)
        factory = self._factories.get(reference)
        if factory is None:
            raise ValueError("persisted experiment strategy is not registered")
        strategy = factory(cast(Mapping[str, object], config["strategy_config"]))
        if strategy.strategy_id != reference.strategy_id:
            raise ValueError("strategy factory returned the wrong identity")
        return _ConcreteExperimentRuntime(
            experiment=experiment,
            config=config,
            repository=self._repository,
            capabilities=self._capabilities,
            provider=self._provider,
            artifact_root=self._artifact_root / ".experiment-staging",
            rulebook=self._rulebook,
            strategy=strategy,
            strategy_ref=reference,
            max_partition_size=self._max_partition_size,
        )


class _ConcreteExperimentRuntime:
    def __init__(
        self,
        *,
        experiment: ExperimentRecord,
        config: dict[str, JsonValue],
        repository: ResearchDataRepository,
        capabilities: ProviderCapabilities,
        provider: str,
        artifact_root: Path,
        rulebook: MarketRuleBook,
        strategy: Strategy,
        strategy_ref: StrategyRef,
        max_partition_size: int,
    ) -> None:
        self._experiment = experiment
        self._config = config
        self._repository = repository
        self._catalog = repository.catalog()
        self._capabilities = capabilities
        self._provider = provider
        self._artifact_root = artifact_root
        self._rulebook = rulebook
        self._strategy = strategy
        self._strategy_ref = strategy_ref
        self._max_partition_size = max_partition_size
        self._start = date.fromisoformat(cast(str, config["start_date"]))
        self._end = date.fromisoformat(cast(str, config["end_date"]))
        self._benchmark = InstrumentId.parse(cast(str, config["benchmark"]))
        self._rules = _RuntimeSupport._universe_rules(
            cast(Mapping[str, object], config["universe"])
        )
        self._execution = _RuntimeSupport._execution_config(
            cast(Mapping[str, object], config["execution"])
        )
        self._validated = False
        self._benchmark_is_index = False
        self._factor_refs: tuple[str, ...] = ()
        self._instruments: tuple[InstrumentId, ...] = ()

    def assert_current_data(self, stage: str) -> None:
        try:
            state = self._catalog.require_validated_catalog()
        except QuantError as error:
            raise ExperimentDataDrift(
                self._experiment.data_hash, None, stage=stage
            ) from error
        if state.catalog_hash != self._experiment.data_hash:
            raise ExperimentDataDrift(
                self._experiment.data_hash, state.catalog_hash, stage=stage
            )

    def validate(self) -> None:
        self.assert_current_data("VALIDATE")
        if self._rulebook.content_hash != self._experiment.rulebook_hash:
            raise ValueError("persisted rulebook hash is not an exact match")
        required_capabilities = set(_MARKET_CAPABILITIES)
        if self._strategy_ref == _STOCK_REF:
            required_capabilities.update(_STOCK_CAPABILITIES)
        require_provider_capabilities(
            self._capabilities,
            tuple(sorted(required_capabilities)),
            provider=self._provider,
            stage="VALIDATE",
        )
        if self._strategy_ref == _ETF_REF:
            strategy = cast(EtfRotationStrategy, self._strategy)
            self._instruments = strategy.config.etf_pool
            self._factor_refs = tuple(
                sorted(
                    {
                        *strategy.config.return_factor_weights,
                        strategy.config.trend_factor_ref,
                        strategy.config.volatility_factor_ref,
                    }
                )
            )
        else:
            self._factor_refs = tuple(
                cast(MultifactorStrategy, self._strategy).config.factor_definitions
            )
        instrument_frame = self._repository.instruments().collect()
        available = {
            InstrumentId.parse(value)
            for value in cast(list[str], instrument_frame["instrument_id"].to_list())
        }
        if self._strategy_ref == _ETF_REF:
            if not set(self._instruments).issubset(available):
                raise ValueError("ETF pool exceeds current instrument scope")
            metadata = {
                InstrumentId.parse(cast(str, row["instrument_id"])): row
                for row in instrument_frame.select(
                    "instrument_id", "instrument_type", "board"
                ).iter_rows(named=True)
            }
            for instrument in self._instruments:
                row = metadata[instrument]
                if row["instrument_type"] != "ETF":
                    raise ValueError("ETF pool contains a non-ETF instrument")
                try:
                    board = Board(cast(str, row["board"]))
                except ValueError as error:
                    raise ValueError("ETF pool contains an invalid board") from error
                self._rulebook.trading_profile(instrument, "ETF", board, self._start)
        else:
            self._instruments = tuple(sorted(available, key=InstrumentId.canonical))
        if self._benchmark not in available:
            raise ValueError("benchmark is absent from current instrument scope")
        self._benchmark_is_index = (
            _RuntimeSupport._instrument_type(instrument_frame, self._benchmark)
            == "INDEX"
        )
        registry = self._factor_registry(self._instruments[: self._max_partition_size])
        plan = registry.preflight(self._factor_refs, self._capabilities)
        required = set(self._required_datasets(registry, plan))
        if self._benchmark_is_index:
            required.add(DatasetKind.INDEX_BAR)
        records = self._required_dataset_records(required)
        max_lookback = max(
            (registry.spec(reference).lookback_sessions for reference in plan),
            default=0,
        )
        self._validate_coverage(records, max_lookback)
        CanonicalBacktestMarketData(
            repository=self._repository,
            benchmark=self._benchmark,
            capabilities=self._capabilities,
            provider=self._provider,
        ).preflight()
        self.assert_current_data("VALIDATE")
        self._validated = True

    def _factor_registry(self, instruments: tuple[InstrumentId, ...]) -> FactorRegistry:
        registry = FactorRegistry()
        if self._strategy_ref == _ETF_REF:
            register_etf_factors(registry, self._repository, instruments)
        else:
            register_stock_factors(
                registry,
                self._repository,
                self._repository,
                instruments,
                price_service=self._repository,
            )
        return registry

    def _partition_engine(self, instruments: tuple[InstrumentId, ...]) -> FactorEngine:
        return FactorEngine(
            self._factor_registry(instruments), capabilities=self._capabilities
        )

    def _required_datasets(
        self, registry: FactorRegistry, plan: Sequence[str]
    ) -> frozenset[DatasetKind]:
        capabilities = set(_MARKET_CAPABILITIES)
        datasets: set[DatasetKind] = set()
        if self._strategy_ref == _STOCK_REF:
            capabilities.update(_STOCK_CAPABILITIES)
        for reference in plan:
            spec = registry.spec(reference)
            datasets.update(spec.required_datasets)
            value = spec.parameters.get("required_capabilities", ())
            if isinstance(value, tuple):
                capabilities.update(cast(tuple[str, ...], value))
        datasets.update(
            dataset
            for capability, dataset in _CAPABILITY_DATASETS.items()
            if capability in capabilities
        )
        if "industry" in self._config:
            datasets.add(DatasetKind.INDUSTRY_CLASSIFICATION)
        return frozenset(datasets)

    def _required_dataset_records(
        self, required: Collection[DatasetKind]
    ) -> dict[DatasetKind, CanonicalDatasetRecord]:
        records: dict[DatasetKind, CanonicalDatasetRecord] = {}
        for dataset in sorted(required, key=lambda item: item.value):
            try:
                records[dataset] = self._catalog.get_canonical_dataset(dataset)
            except KeyError as error:
                raise CanonicalDatasetMissing(dataset) from error
        return records

    def _validate_coverage(
        self,
        records: Mapping[DatasetKind, CanonicalDatasetRecord],
        max_lookback: int,
    ) -> None:
        calendar_record = records[DatasetKind.TRADE_CALENDAR]
        daily_record = records[DatasetKind.DAILY_BAR]
        coverage_records = [calendar_record, daily_record]
        if self._benchmark_is_index:
            coverage_records.append(records[DatasetKind.INDEX_BAR])
        for record in coverage_records:
            if (
                record.start_date is None
                or record.end_date is None
                or self._start < record.start_date
                or self._end > record.end_date
            ):
                raise ValueError("canonical dataset does not cover experiment dates")
        assert calendar_record.start_date is not None
        assert calendar_record.end_date is not None
        sessions = _RuntimeSupport._trading_sessions(
            self._repository.trade_calendar(
                calendar_record.start_date, calendar_record.end_date
            ).collect()
        )
        requested = tuple(day for day in sessions if self._start <= day <= self._end)
        later = tuple(day for day in sessions if day > self._end)
        prior = (
            tuple(day for day in sessions if day < requested[0]) if requested else ()
        )
        if not requested or not later or len(prior) < max_lookback:
            raise ValueError("canonical data lacks experiment boundary sessions")
        history_start = prior[-max_lookback] if max_lookback else requested[0]
        if daily_record.start_date is None or history_start < daily_record.start_date:
            raise ValueError("canonical bars lack factor lookback coverage")
        identifier_column = "instrument_id"
        if self._benchmark_is_index:
            bars = self._repository.index_bars(
                (self._benchmark,), requested[0], later[0]
            ).collect()
            identifier_column = "index_id"
        else:
            bars = self._repository.bars(
                (self._benchmark,), requested[0], later[0]
            ).collect()
        if not _RuntimeSupport._complete_instrument_dates(
            bars,
            (self._benchmark,),
            (*requested, later[0]),
            identifier_column=identifier_column,
        ):
            raise ValueError("canonical bars lack complete benchmark coverage")

    def build_universe(self) -> ExperimentUniverseResult:
        self.assert_current_data("UNIVERSE")
        if not self._validated or not self._instruments:
            raise RuntimeError("runtime must be validated before universe construction")
        if self._strategy_ref == _STOCK_REF:
            sessions = _RuntimeSupport._trading_sessions(
                self._repository.trade_calendar(self._start, self._end).collect()
            )
            frequency = cast(MultifactorStrategy, self._strategy).config.frequency
            signal_dates = rebalance_signal_dates(sessions, frequency)
            digest_builder = hashlib.sha256()
            header = canonical_json_bytes(
                {
                    "data_hash": self._experiment.data_hash,
                    "end": self._end.isoformat(),
                    "start": self._start.isoformat(),
                    "strategy_id": self._strategy_ref.strategy_id,
                }
            )
            digest_builder.update(len(header).to_bytes(8, "big"))
            digest_builder.update(header)
            builder = UniverseBuilder(self._repository)
            for signal_date in signal_dates:
                frame = (
                    builder.build(signal_date, self._rules)
                    .select("as_of", "instrument_id", "eligible", "reason_codes")
                    .sort("instrument_id")
                )
                universe_rows = cast(
                    JsonValue,
                    frame.with_columns(
                        pl.col("as_of").dt.to_string("%Y-%m-%d")
                    ).to_dicts(),
                )
                payload = canonical_json_bytes(
                    {
                        "signal_date": signal_date.isoformat(),
                        "universe": universe_rows,
                    }
                )
                digest_builder.update(len(payload).to_bytes(8, "big"))
                digest_builder.update(payload)
            digest = digest_builder.hexdigest()
        else:
            signal_dates = ()
            digest = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "data_hash": self._experiment.data_hash,
                        "end": self._end.isoformat(),
                        "start": self._start.isoformat(),
                        "strategy_id": self._strategy_ref.strategy_id,
                        "universe": [item.canonical() for item in self._instruments],
                    }
                )
            ).hexdigest()
        self.assert_current_data("UNIVERSE")
        return ExperimentUniverseResult(digest, signal_dates)

    def compute_factors(
        self, universe: ExperimentUniverseResult
    ) -> ExperimentFactorResult:
        self.assert_current_data("FACTOR_COMPUTE")
        if not self._validated:
            raise RuntimeError("runtime must be validated before factor computation")
        engine = PartitionedFactorEngine(
            self._partition_engine,
            max_partition_size=self._max_partition_size,
        )
        result = engine.compute(
            self._factor_refs,
            self._instruments,
            FactorContext(
                self._experiment.data_hash,
                universe.universe_hash,
                self._start,
                self._end,
            ),
        )
        self.assert_current_data("FACTOR_COMPUTE")
        return ExperimentFactorResult(
            {},
            factor_source=PartitionedFactorValueSource(result),
        )

    def backtest(
        self,
        universe: ExperimentUniverseResult,
        factors: ExperimentFactorResult,
        progress: BacktestProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        self.assert_current_data("BACKTEST")
        experiment_uuid = UUID(self._experiment.id)
        request = BacktestRequest(
            experiment_id=experiment_uuid,
            data_hash=self._experiment.data_hash,
            strategy=self._strategy_ref,
            start_date=self._start,
            end_date=self._end,
            benchmark=self._benchmark,
            initial_cash_fen=cast(int, self._config["initial_cash_fen"]),
            rulebook_hash=self._rulebook.content_hash,
            execution_config=self._execution,
            industry_input=(
                cast(Mapping[str, JsonValue], self._config["industry"])
                if "industry" in self._config
                else None
            ),
        )
        recovery_dir = self._artifact_root / f"experiment_id={experiment_uuid}"
        if os.path.lexists(recovery_dir):
            if not (recovery_dir / "manifest.json").is_file():
                quarantine = recovery_dir.with_name(
                    f".incomplete-{recovery_dir.name}-{uuid4().hex}"
                )
                os.replace(recovery_dir, quarantine)
                progress.event(
                    "experiment.backtest_bundle_isolated",
                    {
                        "artifact_dir": str(recovery_dir),
                        "quarantine_dir": str(quarantine),
                        "reason": "manifest_missing",
                    },
                )
            else:
                recovered = validate_backtest_artifacts(
                    recovery_dir,
                    context=ManifestContext(
                        experiment_uuid,
                        request.data_hash,
                        request.strategy.strategy_id,
                        request.start_date,
                        request.end_date,
                        request.benchmark,
                        request.initial_cash_fen,
                        request.rulebook_hash,
                        request.execution_config,
                        request.industry_input,
                    ),
                )
                progress.event(
                    "experiment.backtest_bundle_recovered",
                    {
                        "artifact_dir": str(recovered.artifact_dir),
                        "sessions_completed": recovered.sessions_completed,
                    },
                )
                factors.close()
                return BacktestResult(
                    experiment_uuid,
                    recovered.artifact_dir,
                    recovered.manifest_path,
                    recovered.sessions_completed,
                    recovered.final_snapshot,
                )
        runner = CanonicalStrategyRunner(
            repository=self._repository,
            data_hash=self._experiment.data_hash,
            capabilities=self._capabilities,
            provider=self._provider,
            benchmark=self._benchmark,
            factor_artifacts=factors.artifacts
            if factors.factor_source is None
            else None,
            factor_source=factors.factor_source,
            universe_hash=universe.universe_hash,
            universe_signal_dates=universe.signal_dates,
            universe_rules=self._rules,
            strategies={self._strategy_ref: self._strategy},
            stock_strategy_refs=(
                frozenset({self._strategy_ref})
                if self._strategy_ref == _STOCK_REF
                else frozenset()
            ),
            rulebook=self._rulebook,
            portfolio_constructor=PortfolioConstructor(),
            rebalance_planner=RebalancePlanner(),
            artifact_root=self._artifact_root,
        )
        try:
            result = runner.run(
                request,
                progress,
                cancellation,
            )
            self.assert_current_data("BACKTEST")
        except BaseException as error:
            _RunnerSupport._cleanup_preserving_primary(
                factors.close, error, resource="factor source"
            )
            raise
        factors.close()
        return result


def build_experiment_worker(
    *,
    engine: Engine,
    worker_id: str,
    runtime_factory: ExperimentRuntimeFactory,
    artifact_root: Path,
    environment: Mapping[str, JsonValue],
    extra_handlers: tuple[TaskHandler, ...] = (),
) -> Worker:
    """构建实验Worker；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        engine：引擎。
        worker_id：当前 Worker 实例的稳定所有者标识。
        runtime_factory：运行时工厂。
        artifact_root：不可变实验产物的可信根目录。
        environment：参与本次处理的运行环境；调用方不得依赖未声明的顺序。
        extra_handlers：参与本次处理的``extra``任务处理器集合；调用方不得依赖未声明的顺序。
    返回值：
        返回构建实验Worker后的实验Worker（``Worker``）。
    异常：
        无。
    """
    query = ExperimentQuery(engine)
    registry = ExperimentRegistry(engine)
    diagnostic_root = artifact_root.parent / "state" / "task-logs"
    task_logs = TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=artifact_root,
        sensitive_values=(),
    )
    queue = TaskQueue(engine, task_log_root=diagnostic_root)
    runner = ExperimentRunner(
        query=query,
        registry=registry,
        runtime_factory=runtime_factory,
        artifact_finalizer=ExperimentArtifactFinalizer(
            artifact_root=artifact_root,
            environment=environment,
            task_log_materializer=ExperimentTaskLogMaterializer(queue, task_logs),
        ),
    )
    return Worker(
        queue,
        worker_id=worker_id,
        handlers=(
            ExperimentBacktestHandler(registry=registry, query=query, runner=runner),
            *extra_handlers,
        ),
        task_logs=task_logs,
    )


def build_default_experiment_worker(
    *,
    worker_id: str,
    engine: Engine | None = None,
    extra_handlers: tuple[TaskHandler, ...] = (),
) -> Worker:
    """构建``default``实验Worker；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        worker_id：当前 Worker 实例的稳定所有者标识。
        engine：引擎。
        extra_handlers：参与本次处理的``extra``任务处理器集合；调用方不得依赖未声明的顺序。
    返回值：
        返回构建``default``实验Worker后的``default``实验Worker（``Worker``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``QuantError``。
    """
    source_root = Path(__file__).resolve().parents[3]
    settings = Settings.load()
    upgrade_database(settings.state_db)
    service_engine = engine or create_sqlite_engine(settings.state_db)
    repository = CanonicalResearchRepository.from_sqlite(
        service_engine,
        trusted_curated_root=settings.curated_root,
    )
    rulebook = AShareRuleBook.load(source_root / "configs" / "rules" / "a_share.yaml")
    profile = os.environ.get("QUANT_WORKER_PROFILE", "baostock")
    if profile == "baostock":
        capabilities = BAOSTOCK_RESEARCH_CAPABILITIES
        provider = "baostock"
    elif profile == "offline-etf":
        capabilities = OFFLINE_ETF_CAPABILITIES
        provider = "offline-etf"
    else:
        raise QuantError(
            ErrorDetail(
                code="WORKER_PROFILE_INVALID",
                severity=Severity.FATAL,
                message="worker profile is not supported",
                context={"profile": profile},
                remediation="use baostock or offline-etf",
                retryable=False,
            )
        )
    environment = capture_environment(source_root, source_root / "uv.lock")
    runtime_factory = ExperimentRuntimeFactory(
        repository=repository,
        capabilities=capabilities,
        provider=provider,
        artifact_root=settings.artifact_root,
        rulebook=rulebook,
        max_partition_size=settings.max_partition_size,
    )
    factor_handlers: tuple[TaskHandler, ...] = ()
    if capabilities.financials_with_announcement_date:
        factor_handlers = (
            FactorAnalysisHandler(
                studies=FactorStudyRepository(service_engine),
                repository=repository,
                capabilities=capabilities,
                artifact_root=settings.artifact_root,
                environment=environment,
                max_partition_size=settings.max_partition_size,
            ),
        )
    return build_experiment_worker(
        engine=service_engine,
        worker_id=worker_id,
        runtime_factory=runtime_factory,
        artifact_root=settings.artifact_root,
        environment=environment,
        extra_handlers=(*factor_handlers, *extra_handlers),
    )


class _RuntimeSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _runtime_config(experiment: ExperimentRecord) -> dict[str, JsonValue]:
        config = experiment.config
        if (
            hashlib.sha256(canonical_json_bytes(config)).hexdigest()
            != experiment.config_hash
        ):
            raise ValueError("persisted resolved config hash changed")
        if (
            config.get("strategy_id") != experiment.strategy_id
            or config.get("rulebook_hash") != experiment.rulebook_hash
        ):
            raise ValueError("persisted resolved config identity changed")
        return dict(config)

    @staticmethod
    def _universe_rules(mapping: Mapping[str, object]) -> UniverseRules:
        return UniverseRules(
            min_listing_days=cast(int, mapping["min_listing_days"]),
            allowed_boards=frozenset(
                Board(cast(str, item))
                for item in cast(Sequence[object], mapping["allowed_boards"])
            ),
            exclude_st=cast(bool, mapping["exclude_st"]),
            exclude_suspended=cast(bool, mapping["exclude_suspended"]),
            min_avg_amount_20d=cast(float | None, mapping["min_avg_amount_20d"]),
        )

    @staticmethod
    def _execution_config(mapping: Mapping[str, object]) -> ExecutionConfig:
        return ExecutionConfig(
            ExecutionPrice(cast(str, mapping["reference_price"])),
            cast(float, mapping["slippage_bps"]),
            cast(float, mapping["max_volume_participation"]),
        )

    @staticmethod
    def _trading_sessions(frame: pl.DataFrame) -> tuple[date, ...]:
        if not {"trade_date", "is_trading_day"}.issubset(frame.columns):
            raise ValueError("trade calendar columns are incomplete")
        rows = frame.select("trade_date", "is_trading_day").iter_rows()
        observed: set[date] = set()
        sessions: list[date] = []
        for trade_date, is_trading_day in rows:
            if type(trade_date) is not date or type(is_trading_day) is not bool:
                raise TypeError("trade calendar values are invalid")
            if trade_date in observed:
                raise ValueError("trade calendar contains duplicate dates")
            observed.add(trade_date)
            if is_trading_day:
                sessions.append(trade_date)
        if sessions != sorted(sessions):
            raise ValueError("trade calendar sessions are not ordered")
        return tuple(sessions)

    @staticmethod
    def _instrument_type(frame: pl.DataFrame, instrument: InstrumentId) -> str:
        required = {"instrument_id", "instrument_type"}
        if not required.issubset(frame.columns):
            raise ValueError("instrument data lacks benchmark classification")
        matches = frame.filter(
            pl.col("instrument_id") == instrument.canonical()
        ).select("instrument_type")
        if matches.height != 1:
            raise ValueError("benchmark classification must be unique")
        value = matches.item()
        if not isinstance(value, str) or not value.strip():
            raise ValueError("benchmark instrument type is invalid")
        return value

    @staticmethod
    def _complete_instrument_dates(
        frame: pl.DataFrame,
        instruments: tuple[InstrumentId, ...],
        sessions: tuple[date, ...],
        *,
        identifier_column: str = "instrument_id",
    ) -> bool:
        if identifier_column not in {"instrument_id", "index_id"}:
            raise ValueError("unsupported market identifier column")
        if not {identifier_column, "trade_date"}.issubset(frame.columns):
            return False
        expected = {
            (instrument.canonical(), session)
            for instrument in instruments
            for session in sessions
        }
        actual = set(frame.select(identifier_column, "trade_date").iter_rows())
        return actual == expected

    @staticmethod
    def _text(value: str, label: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be a nonempty string")
        return value

    @staticmethod
    def _validated_max_partition_size(value: int) -> int:
        if (
            type(value) is not int
            or not 1 <= value <= _MAX_EXPERIMENT_FACTOR_PARTITION_SIZE
        ):
            raise ValueError(
                "max_partition_size must be an integer from 1 through "
                f"{_MAX_EXPERIMENT_FACTOR_PARTITION_SIZE}"
            )
        return value


__all__ = [
    "OFFLINE_ETF_CAPABILITIES",
    "ExperimentDataDrift",
    "ExperimentRuntimeFactory",
    "build_default_experiment_worker",
    "build_experiment_worker",
    "strategy_factories",
]
