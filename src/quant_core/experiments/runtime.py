"""Concrete local composition root for persisted experiment execution."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import UUID

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
from sqlalchemy import Engine

from quant_core.backtest.artifacts import FACTOR_METRICS_SCHEMA
from quant_core.backtest.engine import BacktestRequest, BacktestResult, StrategyRef
from quant_core.backtest.models import ExecutionConfig, ExecutionPrice
from quant_core.backtest.rulebook import AShareRuleBook, MarketRuleBook
from quant_core.data.adjustments import PriceAdjustmentService
from quant_core.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_core.data.repository import (
    ResearchDataRepository,
    SnapshotResearchRepository,
    verify_published_dataset,
)
from quant_core.data.snapshots import SnapshotPublisher
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.enums import Board, DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    InstrumentId,
    QualityRunId,
    SnapshotId,
)
from quant_core.errors import ErrorDetail, QuantError
from quant_core.experiments.adapters import (
    PitUniverseEnrichmentProvider,
    SnapshotBacktestMarketData,
    SnapshotStrategyRunner,
)
from quant_core.experiments.config import require_provider_capabilities
from quant_core.experiments.fingerprint import capture_environment
from quant_core.experiments.models import ExperimentRecord
from quant_core.experiments.query import ExperimentQuery
from quant_core.experiments.registry import ExperimentRegistry
from quant_core.experiments.runner import (
    BacktestProgressSink,
    CancellationToken,
    ExperimentArtifactFinalizer,
    ExperimentBacktestHandler,
    ExperimentFactorResult,
    ExperimentRunner,
    ExperimentUniverseResult,
    PreparedExperimentRuntime,
)
from quant_core.factors import FactorContext, FactorEngine, FactorRegistry, FeatureCache
from quant_core.factors.builtin import register_etf_factors, register_stock_factors
from quant_core.factors.builtin.auxiliary import PitValueProvider
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    DatasetVersionRecord,
    MetadataRepository,
    QualityRunRecord,
    SnapshotRecord,
)
from quant_core.portfolio import PortfolioConstructor, RebalancePlanner
from quant_core.settings import Settings
from quant_core.strategies import (
    EtfRotationConfig,
    EtfRotationStrategy,
    MultifactorConfig,
    MultifactorStrategy,
    Strategy,
)
from quant_core.tasks.queue import TaskQueue
from quant_core.tasks.worker import Worker
from quant_core.universe.builder import UniverseBuilder
from quant_core.universe.rules import UniverseRules

_MARKET_CAPABILITIES = (
    "daily_bars",
    "trade_calendar",
    "instruments",
    "security_status",
    "corporate_actions",
)
_STOCK_CAPABILITIES = (
    "financials_with_announcement_date",
    "pit_total_shares",
    "pit_industry_classification",
)
_STOCK_REF = StrategyRef("stock_multifactor", "1.0.0")
_ETF_REF = StrategyRef("etf_rotation", "1.0.0")
_CAPABILITY_DATASETS = MappingProxyType(
    {
        "daily_bars": DatasetKind.DAILY_BAR,
        "trade_calendar": DatasetKind.TRADE_CALENDAR,
        "instruments": DatasetKind.INSTRUMENT,
        "security_status": DatasetKind.SECURITY_STATUS,
        "financials_with_announcement_date": DatasetKind.FINANCIAL_OBSERVATION,
        "corporate_actions": DatasetKind.CORPORATE_ACTION,
    }
)
type StrategyFactory = Callable[[Mapping[str, object]], Strategy]


class RuntimeSnapshotCatalog(Protocol):
    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord: ...

    def get_dataset_version(
        self, identifier: DatasetVersionId
    ) -> DatasetVersionRecord: ...

    def get_quality_run(self, identifier: QualityRunId) -> QualityRunRecord: ...


class ExperimentSnapshotInvalid(QuantError):
    """A persisted experiment is not executable from its bound snapshot."""

    def __init__(
        self,
        snapshot_id: SnapshotId,
        cause_code: str,
        *,
        dataset: DatasetKind | None = None,
    ) -> None:
        context: dict[str, object] = {
            "cause_code": cause_code,
            "snapshot_id": str(snapshot_id),
            "stage": "VALIDATE",
        }
        if dataset is not None:
            context["dataset"] = dataset.value
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_SNAPSHOT_INVALID",
                severity=Severity.FATAL,
                message="experiment snapshot failed immutable VALIDATE checks",
                context=context,
                remediation=(
                    "select a published snapshot with intact required datasets and "
                    "complete experiment coverage"
                ),
                retryable=False,
            )
        )


def strategy_factories() -> Mapping[StrategyRef, StrategyFactory]:
    """Return exact shipped strategy constructors, never identity sentinels."""
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
    """Reconstruct a complete runtime from one persisted resolved experiment."""

    def __init__(
        self,
        *,
        catalog: RuntimeSnapshotCatalog,
        repository: ResearchDataRepository,
        capabilities: ProviderCapabilities,
        provider: str,
        feature_root: Path,
        artifact_root: Path,
        snapshot_root: Path,
        rulebook: MarketRuleBook,
        enrichment: PitUniverseEnrichmentProvider | None,
        shares_provider: PitValueProvider | None = None,
        industry_provider: PitValueProvider | None = None,
        factories: Mapping[StrategyRef, StrategyFactory] | None = None,
    ) -> None:
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        if not all(
            isinstance(path, Path)
            for path in (feature_root, artifact_root, snapshot_root)
        ):
            raise TypeError(
                "feature_root, artifact_root, and snapshot_root must be Paths"
            )
        self._catalog = catalog
        self._repository = repository
        self._capabilities = capabilities
        self._provider = _text(provider, "provider")
        self._feature_root = feature_root
        self._artifact_root = artifact_root
        self._rulebook = rulebook
        self._enrichment = enrichment
        self._shares = shares_provider
        self._industry = industry_provider
        self._factories = dict(factories or strategy_factories())
        self._snapshot_root = snapshot_root.resolve()

    def __call__(self, experiment: ExperimentRecord) -> PreparedExperimentRuntime:
        if not isinstance(experiment, ExperimentRecord):
            raise TypeError("experiment must be an ExperimentRecord")
        config = _runtime_config(experiment)
        reference = StrategyRef(experiment.strategy_id, experiment.strategy_version)
        factory = self._factories.get(reference)
        if factory is None:
            raise ValueError("persisted experiment strategy is not registered")
        strategy_config = cast(Mapping[str, object], config["strategy_config"])
        strategy = factory(strategy_config)
        if (
            strategy.strategy_id != reference.strategy_id
            or strategy.version != reference.version
        ):
            raise ValueError("strategy factory returned the wrong exact identity")
        snapshot = self._catalog.get_snapshot(
            SnapshotId.parse(cast(str, config["snapshot_id"]))
        )
        return _ConcreteExperimentRuntime(
            experiment=experiment,
            config=config,
            snapshot=snapshot,
            catalog=self._catalog,
            snapshot_root=self._snapshot_root,
            repository=self._repository,
            capabilities=self._capabilities,
            provider=self._provider,
            feature_root=self._feature_root,
            artifact_root=self._artifact_root / ".experiment-staging",
            rulebook=self._rulebook,
            enrichment=self._enrichment,
            shares_provider=self._shares,
            industry_provider=self._industry,
            strategy=strategy,
            strategy_ref=reference,
        )


class _ConcreteExperimentRuntime:
    def __init__(
        self,
        *,
        experiment: ExperimentRecord,
        config: dict[str, JsonValue],
        snapshot: SnapshotRecord,
        catalog: RuntimeSnapshotCatalog,
        snapshot_root: Path,
        repository: ResearchDataRepository,
        capabilities: ProviderCapabilities,
        provider: str,
        feature_root: Path,
        artifact_root: Path,
        rulebook: MarketRuleBook,
        enrichment: PitUniverseEnrichmentProvider | None,
        shares_provider: PitValueProvider | None,
        industry_provider: PitValueProvider | None,
        strategy: Strategy,
        strategy_ref: StrategyRef,
    ) -> None:
        self._experiment = experiment
        self._config = config
        self._snapshot = snapshot
        self._catalog = catalog
        self._snapshot_root = snapshot_root
        self._repository = repository
        self._capabilities = capabilities
        self._provider = provider
        self._feature_root = feature_root
        self._artifact_root = artifact_root
        self._rulebook = rulebook
        self._enrichment = enrichment
        self._shares = shares_provider
        self._industry = industry_provider
        self._strategy = strategy
        self._strategy_ref = strategy_ref
        self._snapshot_id = SnapshotId.parse(cast(str, config["snapshot_id"]))
        self._start = date.fromisoformat(cast(str, config["start_date"]))
        self._end = date.fromisoformat(cast(str, config["end_date"]))
        self._benchmark = InstrumentId.parse(cast(str, config["benchmark"]))
        self._rules = _universe_rules(cast(Mapping[str, object], config["universe"]))
        self._execution = _execution_config(
            cast(Mapping[str, object], config["execution"])
        )
        self._factor_engine: FactorEngine | None = None
        self._factor_refs: tuple[str, ...] = ()
        self._instruments: tuple[InstrumentId, ...] = ()

    def validate(self) -> None:
        if self._snapshot.id != self._snapshot_id:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id, "SNAPSHOT_IDENTITY_MISMATCH"
            )
        if (
            self._snapshot.status is not SnapshotStatus.PUBLISHED
            or self._snapshot.published_at is None
        ):
            raise ExperimentSnapshotInvalid(self._snapshot_id, "SNAP_NOT_PUBLISHED")
        if self._snapshot.manifest_hash != self._experiment.snapshot_manifest_hash:
            raise ExperimentSnapshotInvalid(self._snapshot_id, "SNAP_MANIFEST_MISMATCH")
        if self._rulebook.version != self._experiment.rulebook_version:
            raise ValueError("persisted rulebook version is not an exact match")
        require_provider_capabilities(
            self._capabilities,
            _MARKET_CAPABILITIES,
            provider=self._provider,
            stage="VALIDATE",
        )
        if self._strategy_ref == _STOCK_REF:
            require_provider_capabilities(
                self._capabilities,
                _STOCK_CAPABILITIES,
                provider=self._provider,
                stage="VALIDATE",
            )
            if (
                self._enrichment is None
                or self._shares is None
                or self._industry is None
            ):
                raise ValueError(
                    "stock runtime requires explicit PIT enrichment providers"
                )
        self._verify_snapshot_publication()
        registry = FactorRegistry()
        prices = PriceAdjustmentService(self._repository)
        if self._strategy_ref == _ETF_REF:
            config = cast(EtfRotationStrategy, self._strategy).config
            self._instruments = config.etf_pool
            register_etf_factors(registry, prices, self._instruments)
            self._factor_refs = tuple(
                sorted(
                    {
                        *config.return_factor_weights,
                        config.trend_factor_ref,
                        config.volatility_factor_ref,
                    }
                )
            )
        else:
            self._factor_refs = tuple(
                cast(MultifactorStrategy, self._strategy).config.factor_definitions
            )
        plan = (
            registry.preflight(self._factor_refs, self._capabilities)
            if self._strategy_ref == _ETF_REF
            else ()
        )
        required = self._required_datasets(registry, plan)
        versions = self._verify_required_datasets(required)
        try:
            instrument_frame = self._repository.instruments(self._snapshot_id).collect()
            values = sorted(
                set(cast(list[str], instrument_frame["instrument_id"].to_list()))
            )
            available = {InstrumentId.parse(value) for value in values}
        except (KeyError, OSError, TypeError, ValueError) as error:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATASET_INVALID",
                dataset=DatasetKind.INSTRUMENT,
            ) from error
        if self._strategy_ref == _ETF_REF:
            pool = cast(EtfRotationStrategy, self._strategy).config.etf_pool
            if not set(pool).issubset(available):
                raise ExperimentSnapshotInvalid(
                    self._snapshot_id,
                    "SNAPSHOT_INSTRUMENT_SCOPE_INVALID",
                    dataset=DatasetKind.INSTRUMENT,
                )
            self._instruments = pool
        else:
            self._instruments = tuple(sorted(available, key=InstrumentId.canonical))
        if self._benchmark not in available:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_INSTRUMENT_SCOPE_INVALID",
                dataset=DatasetKind.INSTRUMENT,
            )
        if self._strategy_ref == _STOCK_REF:
            register_stock_factors(
                registry,
                self._repository,
                self._repository,
                self._instruments,
                price_service=prices,
                shares_provider=self._shares,
                industry_provider=self._industry,
            )
            self._factor_refs = tuple(
                cast(MultifactorStrategy, self._strategy).config.factor_definitions
            )
            plan = registry.preflight(self._factor_refs, self._capabilities)
            required = self._required_datasets(registry, plan)
            missing_after_universe = required.difference(versions)
            if missing_after_universe:
                self._verify_required_datasets(missing_after_universe)
        max_lookback = max(
            (registry.spec(reference).lookback_sessions for reference in plan),
            default=0,
        )
        self._validate_coverage(versions, max_lookback)
        engine = FactorEngine(
            registry,
            FeatureCache(self._feature_root),
            capabilities=self._capabilities,
        )
        engine.execution_descriptor(self._factor_refs)
        SnapshotBacktestMarketData(
            repository=self._repository,
            snapshot_id=self._snapshot_id,
            benchmark=self._benchmark,
            capabilities=self._capabilities,
            provider=self._provider,
        ).preflight()
        self._factor_engine = engine

    def _verify_snapshot_publication(self) -> None:
        try:
            verified = SnapshotPublisher(
                cast(MetadataRepository, self._catalog), self._snapshot_root
            ).verify_published(
                self._snapshot_id,
                self._snapshot.dataset_versions,
                self._snapshot.quality_run_id,
            )
        except QuantError as error:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id, error.detail.code
            ) from error
        except (KeyError, OSError, TypeError, ValueError) as error:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id, "SNAP_MANIFEST_MISMATCH"
            ) from error
        if verified.manifest_hash != self._experiment.snapshot_manifest_hash:
            raise ExperimentSnapshotInvalid(self._snapshot_id, "SNAP_MANIFEST_MISMATCH")

    def _required_datasets(
        self, registry: FactorRegistry, plan: Sequence[str]
    ) -> frozenset[DatasetKind]:
        capabilities = set(_MARKET_CAPABILITIES)
        if self._strategy_ref == _STOCK_REF:
            capabilities.update(_STOCK_CAPABILITIES)
        for reference in plan:
            value = registry.spec(reference).parameters.get("required_capabilities", ())
            if isinstance(value, tuple):
                capabilities.update(cast(tuple[str, ...], value))
        return frozenset(
            dataset
            for capability, dataset in _CAPABILITY_DATASETS.items()
            if capability in capabilities
        )

    def _verify_required_datasets(
        self, required: Sequence[DatasetKind] | frozenset[DatasetKind]
    ) -> dict[DatasetKind, DatasetVersionRecord]:
        versions: dict[DatasetKind, DatasetVersionRecord] = {}
        for dataset in sorted(required, key=lambda item: item.value):
            try:
                versions[dataset] = verify_published_dataset(
                    self._catalog, self._snapshot_id, dataset
                )
            except QuantError as error:
                raise ExperimentSnapshotInvalid(
                    self._snapshot_id,
                    error.detail.code,
                    dataset=dataset,
                ) from error
            except (KeyError, OSError, TypeError, ValueError) as error:
                raise ExperimentSnapshotInvalid(
                    self._snapshot_id,
                    "SNAPSHOT_DATASET_INVALID",
                    dataset=dataset,
                ) from error
        return versions

    def _validate_coverage(
        self,
        versions: Mapping[DatasetKind, DatasetVersionRecord],
        max_lookback: int,
    ) -> None:
        for dataset in (DatasetKind.TRADE_CALENDAR, DatasetKind.DAILY_BAR):
            record = versions[dataset]
            if (
                record.start_date is None
                or record.end_date is None
                or self._start < record.start_date
                or self._end > record.end_date
            ):
                raise ExperimentSnapshotInvalid(
                    self._snapshot_id,
                    "SNAPSHOT_DATE_COVERAGE_INVALID",
                    dataset=dataset,
                )
        calendar_record = versions[DatasetKind.TRADE_CALENDAR]
        assert calendar_record.start_date is not None
        assert calendar_record.end_date is not None
        calendar = self._repository.trade_calendar(
            self._snapshot_id,
            calendar_record.start_date,
            calendar_record.end_date,
        ).collect()
        try:
            sessions = _trading_sessions(calendar)
        except (TypeError, ValueError) as error:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATE_COVERAGE_INVALID",
                dataset=DatasetKind.TRADE_CALENDAR,
            ) from error
        requested = tuple(day for day in sessions if self._start <= day <= self._end)
        if not requested:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATE_COVERAGE_INVALID",
                dataset=DatasetKind.TRADE_CALENDAR,
            )
        later = tuple(day for day in sessions if day > self._end)
        if not later:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_NEXT_SESSION_MISSING",
                dataset=DatasetKind.TRADE_CALENDAR,
            )
        next_session = later[0]
        status_record = versions[DatasetKind.SECURITY_STATUS]
        if (
            status_record.start_date is None
            or status_record.end_date is None
            or requested[0] < status_record.start_date
            or next_session > status_record.end_date
        ):
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATE_COVERAGE_INVALID",
                dataset=DatasetKind.SECURITY_STATUS,
            )
        prior = tuple(day for day in sessions if day < requested[0])
        if len(prior) < max_lookback:
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATE_COVERAGE_INVALID",
                dataset=DatasetKind.TRADE_CALENDAR,
            )
        history_start = prior[-max_lookback] if max_lookback else requested[0]
        daily_record = versions[DatasetKind.DAILY_BAR]
        if (
            daily_record.start_date is None
            or daily_record.end_date is None
            or history_start < daily_record.start_date
            or next_session > daily_record.end_date
        ):
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATE_COVERAGE_INVALID",
                dataset=DatasetKind.DAILY_BAR,
            )
        scope = tuple(
            sorted({*self._instruments, self._benchmark}, key=InstrumentId.canonical)
        )
        required_sessions = tuple(
            day for day in sessions if history_start <= day <= next_session
        )
        bars = self._repository.bars(
            self._snapshot_id, scope, history_start, next_session
        ).collect()
        if not _complete_instrument_dates(bars, scope, required_sessions):
            raise ExperimentSnapshotInvalid(
                self._snapshot_id,
                "SNAPSHOT_DATE_COVERAGE_INVALID",
                dataset=DatasetKind.DAILY_BAR,
            )
        for session in (*requested, next_session):
            statuses = self._repository.security_status(
                self._snapshot_id, session, scope
            ).collect()
            if not _complete_instrument_dates(statuses, scope, (session,)):
                raise ExperimentSnapshotInvalid(
                    self._snapshot_id,
                    "SNAPSHOT_DATE_COVERAGE_INVALID",
                    dataset=DatasetKind.SECURITY_STATUS,
                )

    def build_universe(self) -> ExperimentUniverseResult:
        if not self._instruments:
            raise RuntimeError("runtime must be validated before universe construction")
        if self._strategy_ref == _STOCK_REF:
            frame = UniverseBuilder(self._repository).build(
                self._snapshot_id, self._start, self._rules
            )
            universe_value: JsonValue = frame.to_dicts()  # type: ignore[assignment]
        else:
            universe_value = [item.canonical() for item in self._instruments]
        digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "end": self._end.isoformat(),
                    "snapshot_id": str(self._snapshot_id),
                    "start": self._start.isoformat(),
                    "universe": universe_value,
                }
            )
        ).hexdigest()
        return ExperimentUniverseResult(digest)

    def compute_factors(
        self, universe: ExperimentUniverseResult
    ) -> ExperimentFactorResult:
        engine = self._factor_engine
        if engine is None:
            raise RuntimeError("runtime must be validated before factor computation")
        artifacts = engine.compute(
            self._factor_refs,
            FactorContext(
                self._snapshot_id, universe.universe_hash, self._start, self._end
            ),
        )
        return ExperimentFactorResult(
            artifacts,
            pa.Table.from_pylist([], schema=FACTOR_METRICS_SCHEMA),
        )

    def backtest(
        self,
        universe: ExperimentUniverseResult,
        factors: ExperimentFactorResult,
        progress: BacktestProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        runner = SnapshotStrategyRunner(
            repository=self._repository,
            snapshot_id=self._snapshot_id,
            capabilities=self._capabilities,
            provider=self._provider,
            benchmark=self._benchmark,
            factor_artifacts=factors.artifacts,
            universe_hash=universe.universe_hash,
            universe_rules=self._rules,
            enrichment=self._enrichment,
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
        return runner.run(
            BacktestRequest(
                experiment_id=UUID(self._experiment.id),
                snapshot_id=self._snapshot_id.value,
                strategy=self._strategy_ref,
                start_date=self._start,
                end_date=self._end,
                benchmark=self._benchmark,
                initial_cash_fen=cast(int, self._config["initial_cash_fen"]),
                rulebook_version=self._rulebook.version,
                execution_config=self._execution,
            ),
            progress,
            cancellation,
        )


def build_experiment_worker(
    *,
    engine: Engine,
    worker_id: str,
    runtime_factory: ExperimentRuntimeFactory,
    artifact_root: Path,
    environment: Mapping[str, JsonValue],
) -> Worker:
    """Assemble the real queue/runner/handler/worker chain without starting it."""
    query = ExperimentQuery(engine)
    registry = ExperimentRegistry(engine)
    runner = ExperimentRunner(
        query=query,
        registry=registry,
        runtime_factory=runtime_factory,
        artifact_finalizer=ExperimentArtifactFinalizer(
            artifact_root=artifact_root,
            environment=environment,
        ),
    )
    return Worker(
        TaskQueue(engine),
        worker_id=worker_id,
        handlers=(
            ExperimentBacktestHandler(registry=registry, query=query, runner=runner),
        ),
    )


def build_default_experiment_worker(*, worker_id: str) -> Worker:
    """Build the local BaoStock-profile worker; do not start or network it."""
    source_root = Path(__file__).resolve().parents[3]
    data_root_text = os.environ.get("QUANT_DATA_ROOT")
    if not data_root_text:
        raise ValueError("QUANT_DATA_ROOT is required")
    config_path = Path(
        os.environ.get("QUANT_CONFIG", source_root / "configs" / "base.yaml")
    )
    settings = Settings.load(
        config_path,
        data_root=Path(data_root_text),
        source_root=source_root,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    catalog = MetadataRepository(engine)
    repository = SnapshotResearchRepository(catalog)
    rulebook = AShareRuleBook.load(
        source_root / "configs" / "rules" / "a_share_v1.yaml"
    )
    runtime_factory = ExperimentRuntimeFactory(
        catalog=catalog,
        repository=repository,
        capabilities=BAOSTOCK_CAPABILITIES,
        provider="baostock",
        feature_root=settings.feature_root,
        artifact_root=settings.artifact_root,
        rulebook=rulebook,
        enrichment=None,
        snapshot_root=settings.snapshot_root,
    )
    return build_experiment_worker(
        engine=engine,
        worker_id=worker_id,
        runtime_factory=runtime_factory,
        artifact_root=settings.artifact_root,
        environment=capture_environment(source_root, source_root / "uv.lock"),
    )


def _runtime_config(experiment: ExperimentRecord) -> dict[str, JsonValue]:
    config = experiment.config
    if (
        hashlib.sha256(canonical_json_bytes(config)).hexdigest()
        != experiment.config_hash
    ):
        raise ValueError("persisted resolved config hash changed")
    if (
        config.get("strategy_id") != experiment.strategy_id
        or config.get("strategy_version") != experiment.strategy_version
        or config.get("snapshot_id") != experiment.snapshot_id
        or config.get("rulebook_version") != experiment.rulebook_version
    ):
        raise ValueError("persisted resolved config identity changed")
    return dict(config)


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


def _execution_config(mapping: Mapping[str, object]) -> ExecutionConfig:
    return ExecutionConfig(
        ExecutionPrice(cast(str, mapping["reference_price"])),
        cast(float, mapping["slippage_bps"]),
        cast(float, mapping["max_volume_participation"]),
    )


def _trading_sessions(frame: pl.DataFrame) -> tuple[date, ...]:
    if not {"trade_date", "is_trading_day"}.issubset(frame.columns):
        raise ValueError("trade calendar columns are incomplete")
    observed: set[date] = set()
    sessions: list[date] = []
    for trade_date, is_trading_day in frame.select(
        "trade_date", "is_trading_day"
    ).iter_rows():
        if type(trade_date) is not date or type(is_trading_day) is not bool:
            raise TypeError("trade calendar values are invalid")
        if trade_date in observed:
            raise ValueError("trade calendar contains duplicate dates")
        observed.add(trade_date)
        if is_trading_day:
            sessions.append(trade_date)
    if sessions != sorted(sessions):
        raise ValueError("trade calendar dates are not ordered")
    return tuple(sessions)


def _complete_instrument_dates(
    frame: pl.DataFrame,
    instruments: Sequence[InstrumentId],
    sessions: Sequence[date],
) -> bool:
    if not {"instrument_id", "trade_date"}.issubset(frame.columns):
        return False
    actual = frame.select("instrument_id", "trade_date").rows()
    expected = {
        (instrument.canonical(), session)
        for instrument in instruments
        for session in sessions
    }
    return len(actual) == len(set(actual)) and set(actual) == expected


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{label} must be nonempty trimmed text")
    return value


__all__ = [
    "ExperimentRuntimeFactory",
    "ExperimentSnapshotInvalid",
    "build_default_experiment_worker",
    "build_experiment_worker",
    "strategy_factories",
]
