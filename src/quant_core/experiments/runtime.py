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
)
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.enums import Board, SnapshotStatus
from quant_core.domain.identifiers import InstrumentId, SnapshotId
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
from quant_core.persistence.repositories import MetadataRepository, SnapshotRecord
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
type StrategyFactory = Callable[[Mapping[str, object]], Strategy]


class RuntimeSnapshotCatalog(Protocol):
    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord: ...


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
        rulebook: MarketRuleBook,
        enrichment: PitUniverseEnrichmentProvider | None,
        shares_provider: PitValueProvider | None = None,
        industry_provider: PitValueProvider | None = None,
        factories: Mapping[StrategyRef, StrategyFactory] | None = None,
    ) -> None:
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        if not isinstance(feature_root, Path) or not isinstance(artifact_root, Path):
            raise TypeError("feature_root and artifact_root must be Paths")
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
        if strategy.strategy_id != reference.strategy_id or strategy.version != reference.version:
            raise ValueError("strategy factory returned the wrong exact identity")
        return _ConcreteExperimentRuntime(
            experiment=experiment,
            config=config,
            snapshot=self._catalog.get_snapshot(SnapshotId.parse(cast(str, config["snapshot_id"]))),
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
        self._execution = _execution_config(cast(Mapping[str, object], config["execution"]))
        self._factor_engine: FactorEngine | None = None
        self._factor_refs: tuple[str, ...] = ()
        self._instruments: tuple[InstrumentId, ...] = ()

    def validate(self) -> None:
        if (
            self._snapshot.id != self._snapshot_id
            or self._snapshot.status is not SnapshotStatus.PUBLISHED
            or self._snapshot.published_at is None
            or self._snapshot.manifest_hash != self._experiment.snapshot_manifest_hash
        ):
            raise ValueError("persisted snapshot identity is not a published exact match")
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
            if self._enrichment is None or self._shares is None or self._industry is None:
                raise ValueError("stock runtime requires explicit PIT enrichment providers")
        instrument_frame = self._repository.instruments(self._snapshot_id).collect()
        values = sorted(set(cast(list[str], instrument_frame["instrument_id"].to_list())))
        available = {InstrumentId.parse(value) for value in values}
        if self._strategy_ref == _ETF_REF:
            pool = cast(EtfRotationStrategy, self._strategy).config.etf_pool
            if not set(pool).issubset(available):
                raise ValueError("ETF pool is not contained in the bound snapshot")
            self._instruments = pool
        else:
            self._instruments = tuple(sorted(available, key=InstrumentId.canonical))
        if self._benchmark not in available:
            raise ValueError("benchmark is not contained in the bound snapshot")
        registry = FactorRegistry()
        prices = PriceAdjustmentService(self._repository)
        if self._strategy_ref == _ETF_REF:
            register_etf_factors(registry, prices, self._instruments)
            config = cast(EtfRotationStrategy, self._strategy).config
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
            FactorContext(self._snapshot_id, universe.universe_hash, self._start, self._end),
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
    config_path = Path(os.environ.get("QUANT_CONFIG", source_root / "configs" / "base.yaml"))
    settings = Settings.load(
        config_path,
        data_root=Path(data_root_text),
        source_root=source_root,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    catalog = MetadataRepository(engine)
    repository = SnapshotResearchRepository(catalog)
    rulebook = AShareRuleBook.load(source_root / "configs" / "rules" / "a_share_v1.yaml")
    runtime_factory = ExperimentRuntimeFactory(
        catalog=catalog,
        repository=repository,
        capabilities=BAOSTOCK_CAPABILITIES,
        provider="baostock",
        feature_root=settings.feature_root,
        artifact_root=settings.artifact_root,
        rulebook=rulebook,
        enrichment=None,
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
    if hashlib.sha256(canonical_json_bytes(config)).hexdigest() != experiment.config_hash:
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
        allowed_boards=frozenset(Board(cast(str, item)) for item in cast(Sequence[object], mapping["allowed_boards"])),
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


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{label} must be nonempty trimmed text")
    return value


__all__ = [
    "ExperimentRuntimeFactory",
    "build_default_experiment_worker",
    "build_experiment_worker",
    "strategy_factories",
]
