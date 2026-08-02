"""Snapshot-bound adapters composing research data into strategy backtests."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from math import isfinite, log
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import UUID
from zoneinfo import ZoneInfo

import polars as pl

from quant_core.backtest.accounting import (
    CorporateAction,
    CorporateActionType,
)
from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.engine import (
    BacktestEngine,
    BacktestRequest,
    BacktestResult,
    CancellationToken,
    ProgressSink,
    SnapshotMarketSlice,
    StrategyRef,
)
from quant_core.backtest.models import MarketSlice
from quant_core.backtest.rulebook import MarketRuleBook
from quant_core.data.contracts import ProviderCapabilities
from quant_core.data.repository import ResearchDataRepository
from quant_core.data.schemas import PolarsDataType
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.experiments.config import require_provider_capabilities
from quant_core.factors.base import (
    FactorArtifact,
    canonical_factor_ref,
    validate_sha256,
)
from quant_core.portfolio import PortfolioConstructor, RebalancePlanner
from quant_core.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyData,
    StrategyTargetAdapter,
    validated_factor_values,
    validated_stock_universe,
)
from quant_core.universe.builder import UniverseBuilder
from quant_core.universe.rules import UniverseRules

PIT_UNIVERSE_ENRICHMENT_SCHEMA = pl.Schema(
    {
        "instrument_id": pl.String,
        "as_of": pl.Date,
        "total_shares": pl.Float64,
        "industry": pl.String,
    }
)
_STRATEGY_UNIVERSE_COLUMNS: dict[str, PolarsDataType] = {
    "instrument_id": pl.String,
    "as_of": pl.Date,
    "eligible": pl.Boolean,
    "reason_codes": cast(pl.DataType, pl.List(pl.String)),
    "industry": pl.String,
    "adv_amount": pl.Float64,
    "log_market_cap": pl.Float64,
}
STRATEGY_UNIVERSE_SCHEMA = pl.Schema(_STRATEGY_UNIVERSE_COLUMNS)
_STRATEGY_FACTOR_COLUMNS: dict[str, PolarsDataType] = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "factor_ref": pl.String,
    "value": pl.Float64,
    "available_at": cast(pl.DataType, pl.Datetime("us", "UTC")),
    "is_valid": pl.Boolean,
}
_STRATEGY_FACTOR_SCHEMA = pl.Schema(_STRATEGY_FACTOR_COLUMNS)
_MARKET_SLICE_SCHEMA = pl.Schema(
    {
        "instrument_id": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "preclose": pl.Float64,
        "volume": pl.Int64,
        "is_suspended": pl.Boolean,
        "security_status": pl.String,
    }
)
_MARKET_CAPABILITIES = (
    "daily_bars",
    "trade_calendar",
    "instruments",
    "security_status",
    "corporate_actions",
)
_STOCK_CAPABILITIES = (
    "daily_bars",
    "trade_calendar",
    "instruments",
    "security_status",
    "pit_total_shares",
    "pit_industry_classification",
)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ONE_DAY = timedelta(days=1)


class PitUniverseEnrichmentProvider(Protocol):
    """Supply explicitly point-in-time shares and industry classifications."""

    def values(
        self,
        snapshot_id: SnapshotId,
        signal_date: date,
        instruments: tuple[InstrumentId, ...],
    ) -> pl.DataFrame: ...


class SnapshotBacktestMarketData:
    """Adapt canonical snapshot data to the backtest market-data boundary."""

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        snapshot_id: SnapshotId,
        benchmark: InstrumentId,
        capabilities: ProviderCapabilities,
        provider: str,
    ) -> None:
        if repository is None:
            raise TypeError("repository must be supplied")
        if not isinstance(snapshot_id, SnapshotId):
            raise TypeError("snapshot_id must be a SnapshotId")
        if not isinstance(benchmark, InstrumentId):
            raise TypeError("benchmark must be an InstrumentId")
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        self._repository = repository
        self._snapshot_id = snapshot_id
        self._benchmark = benchmark
        self._capabilities = capabilities
        self._provider = _nonempty_text(provider, "provider")

    def preflight(self) -> None:
        """Reject an incomplete full-backtest provider without reading or writing."""
        self._require_capabilities(_MARKET_CAPABILITIES, stage="VALIDATE")

    def calendar(
        self,
        snapshot_id: UUID,
        start: date,
        end: date,
        *,
        include_next_session: bool,
    ) -> TradingCalendar:
        bound = self._bound_snapshot(snapshot_id)
        _date_range(start, end)
        if type(include_next_session) is not bool:
            raise TypeError("include_next_session must be a bool")
        self._require_capabilities(("trade_calendar",), stage="VALIDATE")
        loaded_end = end
        if include_next_session:
            if end == date.max:
                raise ValueError("no later trading session can follow date.max")
            later = self._repository.trade_calendar(
                bound, end + _ONE_DAY, date.max
            ).collect()
            loaded_end = _first_later_session(later, end)
        calendar = TradingCalendar.load(self._repository, bound, start, loaded_end)
        if include_next_session and calendar.next_session(end) != loaded_end:
            raise ValueError("calendar did not load the first later trading session")
        return calendar

    def market_slice(self, snapshot_id: UUID, trade_date: date) -> SnapshotMarketSlice:
        bound = self._bound_snapshot(snapshot_id)
        _strict_date(trade_date, "trade_date")
        self._require_capabilities(
            ("daily_bars", "instruments", "security_status"),
            stage="BACKTEST",
        )
        instrument_frame = self._repository.instruments(bound).collect()
        identifiers = _instrument_scope(instrument_frame, "market slice instruments")
        bars = self._repository.bars(
            bound, identifiers, trade_date, trade_date
        ).collect()
        bar_rows = _market_bar_rows(bars, identifiers, trade_date)
        if self._benchmark.canonical() not in bar_rows:
            raise ValueError("market slice is missing benchmark")
        statuses = self._repository.security_status(
            bound, trade_date, identifiers
        ).collect()
        status_rows = _market_status_rows(statuses, set(bar_rows), trade_date)
        if set(status_rows) != set(bar_rows):
            raise ValueError("market slice status join is incomplete")
        output = pl.DataFrame(
            [
                {
                    "instrument_id": identifier,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "preclose": row["preclose"],
                    "volume": row["volume"],
                    "is_suspended": status_rows[identifier]["is_suspended"],
                    "security_status": (
                        "ST"
                        if status_rows[identifier]["is_risk_warning"] is True
                        else "NORMAL"
                    ),
                }
                for identifier, row in sorted(bar_rows.items())
            ],
            schema=_MARKET_SLICE_SCHEMA,
            strict=False,
        )
        try:
            market = MarketSlice(trade_date, output)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "market slice contains invalid or nonfinite canonical OHLC values"
            ) from error
        return SnapshotMarketSlice(bound.value, market)

    def corporate_actions(
        self, snapshot_id: UUID, trade_date: date
    ) -> tuple[CorporateAction, ...]:
        bound = self._bound_snapshot(snapshot_id)
        _strict_date(trade_date, "trade_date")
        self._require_capabilities(("corporate_actions",), stage="BACKTEST")
        frame = self._repository.corporate_actions_as_of(
            bound, None, trade_date
        ).collect()
        events: list[CorporateAction] = []
        for row in _corporate_action_rows(frame):
            events.extend(_mapped_actions(bound, row, trade_date))
        events.sort(
            key=lambda item: (
                item.instrument_id.canonical(),
                item.action_type.value,
                item.event_id,
            )
        )
        return tuple(events)

    def _bound_snapshot(self, value: UUID) -> SnapshotId:
        if not isinstance(value, UUID):
            raise TypeError("snapshot_id must be a UUID")
        if value != self._snapshot_id.value:
            raise ValueError("requested snapshot does not match bound snapshot")
        return self._snapshot_id

    def _require_capabilities(self, required: Sequence[str], *, stage: str) -> None:
        require_provider_capabilities(
            self._capabilities,
            required,
            provider=self._provider,
            stage=stage,
        )


class SnapshotStrategyData:
    """Serve strategy data only from the bound snapshot and verified artifacts."""

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        snapshot_id: SnapshotId,
        factor_artifacts: Mapping[str, FactorArtifact],
        universe_hash: str,
        universe_rules: UniverseRules,
        enrichment: PitUniverseEnrichmentProvider | None,
        capabilities: ProviderCapabilities,
        provider: str,
    ) -> None:
        if repository is None:
            raise TypeError("repository must be supplied")
        if not isinstance(snapshot_id, SnapshotId):
            raise TypeError("snapshot_id must be a SnapshotId")
        if not isinstance(factor_artifacts, Mapping):
            raise TypeError("factor_artifacts must be a mapping")
        artifacts = dict(factor_artifacts)
        for reference, artifact in artifacts.items():
            if (
                canonical_factor_ref(reference) != reference
                or not isinstance(artifact, FactorArtifact)
                or artifact.factor_ref != reference
            ):
                raise ValueError("factor artifact mapping has an invalid identity")
        validate_sha256(universe_hash, "universe_hash")
        if not isinstance(universe_rules, UniverseRules):
            raise TypeError("universe_rules must be UniverseRules")
        if enrichment is not None and not callable(getattr(enrichment, "values", None)):
            raise TypeError("enrichment must provide values() or be None")
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        self._repository = repository
        self._snapshot_id = snapshot_id
        self._artifacts = MappingProxyType(artifacts)
        self._universe_hash = universe_hash
        self._universe_rules = universe_rules
        self._enrichment = enrichment
        self._capabilities = capabilities
        self._provider = _nonempty_text(provider, "provider")

    def preflight(self, *, require_stock_universe: bool) -> None:
        if type(require_stock_universe) is not bool:
            raise TypeError("require_stock_universe must be a bool")
        if not require_stock_universe:
            return
        require_provider_capabilities(
            self._capabilities,
            _STOCK_CAPABILITIES,
            provider=self._provider,
            stage="VALIDATE",
        )
        if self._enrichment is None:
            raise ValueError("stock strategy requires an explicit PIT enrichment")

    def factor_values(
        self,
        snapshot_id: UUID,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        self._bound_snapshot(snapshot_id)
        _strict_date(signal_date, "signal_date")
        requested_instruments = _requested_instruments(instruments)
        if not isinstance(factor_refs, tuple):
            raise TypeError("factor_refs must be a tuple")
        references = tuple(canonical_factor_ref(item) for item in factor_refs)
        if len(set(references)) != len(references):
            raise ValueError("factor_refs must be unique")
        frames: list[pl.DataFrame] = []
        for reference in references:
            artifact = self._artifacts.get(reference)
            if artifact is None:
                raise ValueError(f"factor artifact is missing: {reference}")
            _validate_factor_artifact(
                artifact,
                reference=reference,
                snapshot_id=self._snapshot_id,
                universe_hash=self._universe_hash,
                signal_date=signal_date,
            )
            frame = artifact.lazy_frame().collect()
            factor_id, version = reference.split("@")
            if frame.filter(
                (pl.col("factor_id") != factor_id)
                | (pl.col("factor_version") != version)
            ).height:
                raise ValueError(
                    "factor artifact table identity does not match its ref"
                )
            selected = (
                frame.filter(pl.col("trade_date") == signal_date)
                .with_columns(pl.lit(reference, dtype=pl.String).alias("factor_ref"))
                .select(_STRATEGY_FACTOR_SCHEMA.names())
            )
            if requested_instruments is not None:
                selected = selected.filter(
                    pl.col("instrument_id").is_in(
                        [item.canonical() for item in requested_instruments]
                    )
                )
            frames.append(selected)
        output = (
            pl.concat(frames)
            .cast(_STRATEGY_FACTOR_SCHEMA)
            .sort("trade_date", "instrument_id", "factor_ref")
            if frames
            else pl.DataFrame(schema=_STRATEGY_FACTOR_SCHEMA)
        )
        return validated_factor_values(
            output,
            signal_date=signal_date,
            instruments=requested_instruments,
            factor_refs=references,
        )

    def stock_universe(self, snapshot_id: UUID, signal_date: date) -> pl.DataFrame:
        self._bound_snapshot(snapshot_id)
        _strict_date(signal_date, "signal_date")
        self.preflight(require_stock_universe=True)
        universe = UniverseBuilder(self._repository).build(
            self._snapshot_id, signal_date, self._universe_rules
        )
        identifiers = tuple(
            InstrumentId.parse(value) for value in universe["instrument_id"].to_list()
        )
        enrichment_provider = self._enrichment
        if enrichment_provider is None:
            raise ValueError("stock strategy requires an explicit PIT enrichment")
        enrichment = enrichment_provider.values(
            self._snapshot_id, signal_date, identifiers
        )
        enrichment_rows = _validated_enrichment(enrichment, identifiers, signal_date)
        market_rows = _universe_market_evidence(
            self._repository,
            self._snapshot_id,
            identifiers,
            signal_date,
        )
        output_rows: list[dict[str, object]] = []
        for row in universe.iter_rows(named=True):
            identifier = cast(str, row["instrument_id"])
            evidence = market_rows.get(identifier)
            extra = enrichment_rows[identifier]
            eligible = row["eligible"] is True
            industry = extra["industry"]
            shares = extra["total_shares"]
            if eligible:
                _eligible_enrichment(identifier, industry, shares, evidence)
            adv_amount: float | None = None
            log_market_cap: float | None = None
            if evidence is not None and _positive_finite(shares):
                adv_amount = evidence["adv_amount"]
                capitalization = evidence["close"] * cast(float, shares)
                if isfinite(capitalization) and capitalization > 0:
                    log_market_cap = log(capitalization)
            output_rows.append(
                {
                    "instrument_id": identifier,
                    "as_of": row["as_of"],
                    "eligible": row["eligible"],
                    "reason_codes": row["reason_codes"],
                    "industry": industry,
                    "adv_amount": adv_amount,
                    "log_market_cap": log_market_cap,
                }
            )
        output = pl.DataFrame(
            output_rows,
            schema=STRATEGY_UNIVERSE_SCHEMA,
            strict=False,
        ).sort("instrument_id")
        return validated_stock_universe(output, signal_date=signal_date)

    def _bound_snapshot(self, value: UUID) -> None:
        if not isinstance(value, UUID):
            raise TypeError("snapshot_id must be a UUID")
        if value != self._snapshot_id.value:
            raise ValueError("requested snapshot does not match bound snapshot")


class SnapshotStrategyContextProvider:
    """Build contexts only for two genuinely adjacent snapshot sessions."""

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        snapshot_id: SnapshotId,
        data: StrategyData,
        portfolio_constructor: PortfolioConstructor,
    ) -> None:
        if repository is None or data is None:
            raise TypeError("repository and data must be supplied")
        if not isinstance(snapshot_id, SnapshotId):
            raise TypeError("snapshot_id must be a SnapshotId")
        if not isinstance(portfolio_constructor, PortfolioConstructor):
            raise TypeError("portfolio_constructor must be a PortfolioConstructor")
        self._repository = repository
        self._snapshot_id = snapshot_id
        self._data = data
        self._portfolio_constructor = portfolio_constructor

    def __call__(
        self, snapshot_id: UUID, signal_date: date, execute_date: date
    ) -> StrategyContext:
        if not isinstance(snapshot_id, UUID):
            raise TypeError("snapshot_id must be a UUID")
        if snapshot_id != self._snapshot_id.value:
            raise ValueError("context snapshot does not match bound snapshot")
        _date_range(signal_date, execute_date)
        if signal_date == execute_date:
            raise ValueError("execute_date must be the next actual session")
        calendar = TradingCalendar.load(
            self._repository,
            self._snapshot_id,
            signal_date,
            execute_date,
        )
        sessions = calendar.sessions(signal_date, execute_date)
        try:
            adjacent = calendar.next_session(signal_date)
        except ValueError as error:
            raise ValueError("execute_date must be the next actual session") from error
        if adjacent != execute_date or signal_date not in sessions:
            raise ValueError("execute_date must be the next actual session")
        return StrategyContext(
            snapshot_id,
            signal_date,
            execute_date,
            sessions,
            self._data,
            self._portfolio_constructor,
        )


class SnapshotStrategyRunner:
    """Compose concrete snapshot adapters, strategy targets, and BacktestEngine."""

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        snapshot_id: SnapshotId,
        capabilities: ProviderCapabilities,
        provider: str,
        benchmark: InstrumentId,
        factor_artifacts: Mapping[str, FactorArtifact],
        universe_hash: str,
        universe_rules: UniverseRules,
        enrichment: PitUniverseEnrichmentProvider | None,
        strategies: Mapping[StrategyRef, Strategy],
        stock_strategy_refs: frozenset[StrategyRef],
        rulebook: MarketRuleBook,
        portfolio_constructor: PortfolioConstructor,
        rebalance_planner: RebalancePlanner,
        artifact_root: Path,
    ) -> None:
        if not isinstance(strategies, Mapping):
            raise TypeError("strategies must be a mapping")
        registered = dict(strategies)
        for reference, strategy in registered.items():
            if (
                not isinstance(reference, StrategyRef)
                or strategy.strategy_id != reference.strategy_id
                or strategy.version != reference.version
            ):
                raise ValueError("registry entries must match exact StrategyRef")
        if not isinstance(stock_strategy_refs, frozenset) or not all(
            isinstance(item, StrategyRef) for item in stock_strategy_refs
        ):
            raise TypeError("stock_strategy_refs must be a frozenset of StrategyRef")
        if not stock_strategy_refs.issubset(registered):
            raise ValueError("stock strategy refs must exist in the exact registry")
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        if not isinstance(rebalance_planner, RebalancePlanner):
            raise TypeError("rebalance_planner must be a RebalancePlanner")
        version = getattr(rulebook, "version", None)
        if not isinstance(version, str) or not version.strip():
            raise TypeError("rulebook must provide a nonempty version")
        self._snapshot_id = snapshot_id
        self._benchmark = benchmark
        self._strategies = MappingProxyType(registered)
        self._stock_strategy_refs = stock_strategy_refs
        self._rulebook_version = version
        self._market = SnapshotBacktestMarketData(
            repository=repository,
            snapshot_id=snapshot_id,
            benchmark=benchmark,
            capabilities=capabilities,
            provider=provider,
        )
        self._data = SnapshotStrategyData(
            repository=repository,
            snapshot_id=snapshot_id,
            factor_artifacts=factor_artifacts,
            universe_hash=universe_hash,
            universe_rules=universe_rules,
            enrichment=enrichment,
            capabilities=capabilities,
            provider=provider,
        )
        contexts = SnapshotStrategyContextProvider(
            repository=repository,
            snapshot_id=snapshot_id,
            data=self._data,
            portfolio_constructor=portfolio_constructor,
        )
        targets = StrategyTargetAdapter(self._strategies, contexts)
        self._engine = BacktestEngine(
            self._market,
            targets,
            rulebook,
            rebalance_planner,
            artifact_root=artifact_root,
        )

    def run(
        self,
        request: BacktestRequest,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        if not isinstance(request, BacktestRequest):
            raise TypeError("request must be a BacktestRequest")
        if request.snapshot_id != self._snapshot_id.value:
            raise ValueError("request snapshot does not match bound snapshot")
        if request.benchmark != self._benchmark:
            raise ValueError("request benchmark does not match bound benchmark")
        if request.rulebook_version != self._rulebook_version:
            raise ValueError("request rulebook version does not match bound rulebook")
        if request.strategy not in self._strategies:
            raise ValueError("unknown strategy or version")
        self._market.preflight()
        self._data.preflight(
            require_stock_universe=request.strategy in self._stock_strategy_refs
        )
        return self._engine.run(request, progress, cancellation)


def _first_later_session(frame: pl.DataFrame, after: date) -> date:
    required = {"trade_date", "is_trading_day"}
    if not required.issubset(frame.columns):
        raise ValueError("trade calendar is missing required columns")
    seen: set[date] = set()
    sessions: list[date] = []
    for trade_date, is_open in frame.select("trade_date", "is_trading_day").iter_rows():
        if type(trade_date) is not date or type(is_open) is not bool:
            raise ValueError("trade calendar has invalid values")
        if trade_date <= after:
            raise ValueError("trade calendar returned an out-of-scope later row")
        if trade_date in seen:
            raise ValueError("trade calendar contains duplicate trade_date")
        seen.add(trade_date)
        if is_open:
            sessions.append(trade_date)
    if not sessions:
        raise ValueError("calendar has no later trading session")
    return min(sessions)


def _instrument_scope(frame: pl.DataFrame, label: str) -> tuple[InstrumentId, ...]:
    if "instrument_id" not in frame.columns:
        raise ValueError(f"{label} is missing instrument_id")
    values = frame["instrument_id"].to_list()
    if len(values) != len(set(values)):
        raise ValueError(f"{label} contains duplicate instrument_id")
    try:
        instruments = tuple(InstrumentId.parse(value) for value in values)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} contains a noncanonical instrument") from error
    canonical = [item.canonical() for item in instruments]
    if canonical != sorted(canonical):
        raise ValueError(f"{label} must be canonical-ID sorted")
    return instruments


def _market_bar_rows(
    frame: pl.DataFrame,
    requested: tuple[InstrumentId, ...],
    trade_date: date,
) -> dict[str, dict[str, object]]:
    columns = {
        "instrument_id",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
    }
    if not columns.issubset(frame.columns):
        raise ValueError("market slice bars are missing required columns")
    allowed = {item.canonical() for item in requested}
    rows: dict[str, dict[str, object]] = {}
    for row in frame.select(*sorted(columns)).iter_rows(named=True):
        identifier = row["instrument_id"]
        try:
            canonical = InstrumentId.parse(cast(str, identifier)).canonical()
        except (TypeError, ValueError) as error:
            raise ValueError("market slice has a noncanonical instrument") from error
        if canonical != identifier or canonical not in allowed:
            raise ValueError("market slice contains an unexpected instrument")
        if row["trade_date"] != trade_date:
            raise ValueError("market slice bar date does not match request")
        if canonical in rows:
            raise ValueError("market slice contains duplicate bars")
        rows[canonical] = row
    return rows


def _market_status_rows(
    frame: pl.DataFrame,
    expected: set[str],
    trade_date: date,
) -> dict[str, dict[str, object]]:
    columns = {
        "instrument_id",
        "trade_date",
        "is_listed",
        "is_suspended",
        "is_risk_warning",
    }
    if not columns.issubset(frame.columns):
        raise ValueError("market slice statuses are missing required columns")
    rows: dict[str, dict[str, object]] = {}
    for row in frame.select(*sorted(columns)).iter_rows(named=True):
        identifier = row["instrument_id"]
        if not isinstance(identifier, str):
            raise TypeError("market slice status has an unexpected instrument")
        if identifier not in expected:
            continue
        try:
            canonical = InstrumentId.parse(identifier).canonical()
        except ValueError as error:
            raise ValueError(
                "market slice status has a noncanonical instrument"
            ) from error
        if canonical != identifier or row["trade_date"] != trade_date:
            raise ValueError("market slice status date or instrument is invalid")
        if identifier in rows:
            raise ValueError("market slice contains duplicate status rows")
        if any(
            type(row[name]) is not bool
            for name in ("is_listed", "is_suspended", "is_risk_warning")
        ):
            raise ValueError("market slice status flags must be booleans")
        rows[identifier] = row
    return rows


def _corporate_action_rows(frame: pl.DataFrame) -> list[dict[str, object]]:
    required = {
        "instrument_id",
        "action_type",
        "record_date",
        "ex_date",
        "pay_date",
        "cash_per_share",
        "share_ratio",
        "rights_price",
    }
    if not required.issubset(frame.columns):
        raise ValueError("corporate action rows are missing required fields")
    rows = frame.select(*sorted(required)).to_dicts()
    keys: set[tuple[object, ...]] = set()
    for row in rows:
        key = tuple(row[name] for name in sorted(required))
        if key in keys:
            raise ValueError("corporate action rows contain duplicate evidence")
        keys.add(key)
    return rows


def _mapped_actions(
    snapshot_id: SnapshotId,
    row: dict[str, object],
    trade_date: date,
) -> list[CorporateAction]:
    try:
        instrument = InstrumentId.parse(cast(str, row["instrument_id"]))
    except (TypeError, ValueError) as error:
        raise ValueError("corporate action instrument is not canonical") from error
    action_type = row["action_type"]
    record_date = row["record_date"]
    ex_date = row["ex_date"]
    pay_date = row["pay_date"]
    if type(record_date) is not date or type(ex_date) is not date:
        raise ValueError("corporate action record and ex dates are required")
    if pay_date is not None and type(pay_date) is not date:
        raise ValueError("corporate action pay date is invalid")
    rights = _nonnegative_decimal(row["rights_price"], allow_none=True)
    if rights != 0:
        raise ValueError("corporate action rights issues are unsupported")
    cash = _nonnegative_decimal(row["cash_per_share"], allow_none=True)
    ratio = _nonnegative_decimal(row["share_ratio"], allow_none=True)
    kinds: tuple[tuple[CorporateActionType, Decimal, Decimal], ...]
    if action_type == "CASH_DIVIDEND":
        if cash <= 0 or ratio != 0:
            raise ValueError("corporate action cash dividend fields are incomplete")
        kinds = ((CorporateActionType.CASH_DIVIDEND, cash, Decimal(0)),)
    elif action_type == "BONUS_SHARES":
        if ratio <= 0 or cash != 0:
            raise ValueError("corporate action bonus-share fields are incomplete")
        kinds = ((CorporateActionType.BONUS_SHARES, Decimal(0), ratio),)
    elif action_type == "DIVIDEND":
        if cash <= 0 and ratio <= 0:
            raise ValueError("corporate action dividend fields are incomplete")
        kinds = tuple(
            item
            for item in (
                (CorporateActionType.CASH_DIVIDEND, cash, Decimal(0))
                if cash > 0
                else None,
                (CorporateActionType.BONUS_SHARES, Decimal(0), ratio)
                if ratio > 0
                else None,
            )
            if item is not None
        )
    else:
        raise ValueError("corporate action type is unsupported")
    events: list[CorporateAction] = []
    for kind, cash_value, ratio_value in kinds:
        effective = pay_date if kind is CorporateActionType.CASH_DIVIDEND else ex_date
        if type(effective) is not date:
            raise ValueError("corporate action effective date is incomplete")
        event = CorporateAction(
            _corporate_action_id(
                snapshot_id,
                instrument,
                kind,
                record_date,
                effective,
                cash_value,
                ratio_value,
            ),
            kind,
            instrument,
            record_date,
            effective,
            cash_value,
            ratio_value,
        )
        if effective == trade_date:
            events.append(event)
    return events


def _corporate_action_id(
    snapshot_id: SnapshotId,
    instrument: InstrumentId,
    kind: CorporateActionType,
    record_date: date,
    effective_date: date,
    cash: Decimal,
    ratio: Decimal,
) -> str:
    identity = "|".join(
        (
            str(snapshot_id),
            instrument.canonical(),
            kind.value,
            record_date.isoformat(),
            effective_date.isoformat(),
            str(cash),
            str(ratio),
        )
    )
    return "corporate-action-" + hashlib.sha256(identity.encode()).hexdigest()


def _nonnegative_decimal(value: object, *, allow_none: bool) -> Decimal:
    if value is None and allow_none:
        return Decimal(0)
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError("corporate action amount must be finite and nonnegative")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(
            "corporate action amount must be finite and nonnegative"
        ) from error
    if not result.is_finite() or result < 0:
        raise ValueError("corporate action amount must be finite and nonnegative")
    return result


def _requested_instruments(
    instruments: tuple[InstrumentId, ...] | None,
) -> tuple[InstrumentId, ...] | None:
    if instruments is None:
        return None
    if not isinstance(instruments, tuple) or any(
        not isinstance(item, InstrumentId) for item in instruments
    ):
        raise TypeError("instruments must be a tuple of InstrumentId or None")
    canonical = tuple(item.canonical() for item in instruments)
    if len(set(canonical)) != len(canonical):
        raise ValueError("requested instruments must be unique")
    return instruments


def _validate_factor_artifact(
    artifact: FactorArtifact,
    *,
    reference: str,
    snapshot_id: SnapshotId,
    universe_hash: str,
    signal_date: date,
) -> None:
    if artifact.factor_ref != reference:
        raise ValueError("factor artifact reference does not match request")
    if artifact.snapshot_id != snapshot_id:
        raise ValueError("factor artifact snapshot does not match strategy snapshot")
    if artifact.universe_hash != universe_hash:
        raise ValueError("factor artifact universe does not match strategy universe")
    if signal_date < artifact.start or signal_date > artifact.end:
        raise ValueError("factor artifact does not cover requested date range")


def _validated_enrichment(
    frame: pl.DataFrame,
    instruments: tuple[InstrumentId, ...],
    signal_date: date,
) -> dict[str, dict[str, object]]:
    if (
        not isinstance(frame, pl.DataFrame)
        or frame.schema != PIT_UNIVERSE_ENRICHMENT_SCHEMA
    ):
        raise ValueError("PIT enrichment has an invalid schema")
    expected = {item.canonical() for item in instruments}
    rows: dict[str, dict[str, object]] = {}
    for row in frame.iter_rows(named=True):
        identifier = row["instrument_id"]
        if not isinstance(identifier, str):
            raise TypeError("PIT enrichment instrument_id is invalid")
        try:
            canonical = InstrumentId.parse(identifier).canonical()
        except ValueError as error:
            raise ValueError("PIT enrichment instrument_id is invalid") from error
        if canonical != identifier or identifier not in expected:
            raise ValueError("PIT enrichment contains an unexpected instrument")
        if row["as_of"] != signal_date:
            raise ValueError("PIT enrichment date does not match signal date")
        if identifier in rows:
            raise ValueError("PIT enrichment contains duplicate instruments")
        rows[identifier] = row
    if set(rows) != expected:
        raise ValueError("PIT enrichment scope is incomplete")
    return rows


def _universe_market_evidence(
    repository: ResearchDataRepository,
    snapshot_id: SnapshotId,
    instruments: tuple[InstrumentId, ...],
    signal_date: date,
) -> dict[str, dict[str, float]]:
    if not instruments:
        return {}
    coverage_start = date.min + _ONE_DAY
    calendar = TradingCalendar.load(
        repository, snapshot_id, coverage_start, signal_date
    )
    sessions = calendar.sessions(coverage_start, signal_date)[-20:]
    if not sessions or sessions[-1] != signal_date:
        raise ValueError("stock universe signal date is not an actual trading session")
    bars = repository.bars(snapshot_id, instruments, sessions[0], signal_date).collect()
    required = {
        "instrument_id",
        "trade_date",
        "close",
        "amount",
        "available_at",
        "pit_usable",
    }
    if not required.issubset(bars.columns):
        raise ValueError("stock universe market evidence has an invalid schema")
    close_utc = datetime.combine(signal_date, time.max, tzinfo=_SHANGHAI).astimezone(
        UTC
    )
    visible = bars.filter(
        pl.col("pit_usable")
        & pl.col("available_at").is_not_null()
        & (pl.col("available_at") <= close_utc)
    )
    expected_ids = {item.canonical() for item in instruments}
    expected_dates = set(sessions)
    seen: set[tuple[str, date]] = set()
    amounts: dict[str, list[float]] = {}
    closes: dict[str, float] = {}
    for row in visible.select(*sorted(required)).iter_rows(named=True):
        identifier = row["instrument_id"]
        trade_date = row["trade_date"]
        if not isinstance(identifier, str) or identifier not in expected_ids:
            raise ValueError("stock universe market evidence has unexpected scope")
        if type(trade_date) is not date or trade_date not in expected_dates:
            raise ValueError("stock universe market evidence has unexpected dates")
        key = (identifier, trade_date)
        if key in seen:
            raise ValueError("stock universe market evidence has duplicate bars")
        seen.add(key)
        amount = row["amount"]
        close = row["close"]
        if (
            not isinstance(amount, float)
            or not isfinite(amount)
            or amount < 0
            or not isinstance(close, float)
            or not isfinite(close)
            or close <= 0
        ):
            raise ValueError("stock universe market evidence is nonfinite")
        amounts.setdefault(identifier, []).append(amount)
        if trade_date == signal_date:
            closes[identifier] = close
    result: dict[str, dict[str, float]] = {}
    for identifier, values in amounts.items():
        close = closes.get(identifier)
        if close is not None and values:
            result[identifier] = {
                "adv_amount": sum(values) / len(values),
                "close": close,
            }
    return result


def _eligible_enrichment(
    identifier: str,
    industry: object,
    shares: object,
    evidence: dict[str, float] | None,
) -> None:
    if not isinstance(industry, str) or not industry.strip():
        raise ValueError(f"PIT enrichment industry is missing for {identifier}")
    if not _positive_finite(shares):
        raise ValueError(f"PIT enrichment total_shares is invalid for {identifier}")
    if evidence is None:
        raise ValueError(f"PIT enrichment market evidence is missing for {identifier}")


def _positive_finite(value: object) -> bool:
    return isinstance(value, float) and isfinite(value) and value > 0


def _date_range(start: object, end: object) -> None:
    _strict_date(start, "start")
    _strict_date(end, "end")
    if cast(date, start) > cast(date, end):
        raise ValueError("start must not follow end")


def _strict_date(value: object, name: str) -> None:
    if type(value) is not date:
        raise TypeError(f"{name} must be a date")


def _nonempty_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


__all__ = [
    "PIT_UNIVERSE_ENRICHMENT_SCHEMA",
    "STRATEGY_UNIVERSE_SCHEMA",
    "PitUniverseEnrichmentProvider",
    "SnapshotBacktestMarketData",
    "SnapshotStrategyContextProvider",
    "SnapshotStrategyData",
    "SnapshotStrategyRunner",
]
