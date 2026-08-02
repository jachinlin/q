"""Snapshot-bound daily backtest scheduling without look-ahead execution."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from pathlib import Path
from typing import Protocol
from uuid import UUID

from quant_core.backtest.accounting import (
    AccountSnapshot,
    CorporateAction,
    PortfolioAccount,
)
from quant_core.backtest.artifacts import BacktestArtifactWriter, ManifestContext
from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.execution import ExecutionModel
from quant_core.backtest.models import AccountView, ExecutionConfig, MarketSlice
from quant_core.backtest.rulebook import MarketRuleBook
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.portfolio.constructor import TargetPortfolio, validate_target_portfolio
from quant_core.portfolio.rebalance import RebalancePlanner

_EPSILON = 1e-10


@dataclass(frozen=True, slots=True)
class StrategyRef:
    strategy_id: str
    version: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.strategy_id, "strategy_id"),
            (self.version, "version"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonempty string")


@dataclass(frozen=True, slots=True)
class BacktestRequest:
    experiment_id: UUID
    snapshot_id: UUID
    strategy: StrategyRef
    start_date: date
    end_date: date
    benchmark: InstrumentId
    initial_cash_fen: int
    rulebook_version: str
    execution_config: ExecutionConfig

    def __post_init__(self) -> None:
        if not isinstance(self.experiment_id, UUID) or not isinstance(
            self.snapshot_id, UUID
        ):
            raise TypeError("experiment_id and snapshot_id must be UUID values")
        if not isinstance(self.strategy, StrategyRef):
            raise TypeError("strategy must be a StrategyRef")
        _date(self.start_date, "start_date")
        _date(self.end_date, "end_date")
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")
        if not isinstance(self.benchmark, InstrumentId):
            raise TypeError("benchmark must be an InstrumentId")
        if type(self.initial_cash_fen) is not int or self.initial_cash_fen <= 0:
            raise ValueError("initial_cash_fen must be a positive integer")
        if (
            not isinstance(self.rulebook_version, str)
            or not self.rulebook_version.strip()
        ):
            raise ValueError("rulebook_version must be a nonempty string")
        if not isinstance(self.execution_config, ExecutionConfig):
            raise TypeError("execution_config must be an ExecutionConfig")


@dataclass(frozen=True, slots=True)
class BacktestResult:
    experiment_id: UUID
    artifact_dir: Path
    manifest_path: Path
    sessions_completed: int
    final_snapshot: AccountSnapshot

    def __post_init__(self) -> None:
        if not isinstance(self.experiment_id, UUID):
            raise TypeError("experiment_id must be a UUID")
        if not isinstance(self.artifact_dir, Path) or not isinstance(
            self.manifest_path, Path
        ):
            raise TypeError("artifact_dir and manifest_path must be Path values")
        if self.manifest_path != self.artifact_dir / "manifest.json":
            raise ValueError("manifest_path must be artifact_dir/manifest.json")
        if type(self.sessions_completed) is not int or self.sessions_completed <= 0:
            raise ValueError("sessions_completed must be a positive integer")
        if not isinstance(self.final_snapshot, AccountSnapshot):
            raise TypeError("final_snapshot must be an AccountSnapshot")


@dataclass(frozen=True, slots=True)
class SnapshotMarketSlice:
    """A market slice bound to the immutable snapshot that supplied it."""

    snapshot_id: UUID
    market: MarketSlice

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_id, UUID):
            raise TypeError("snapshot_id must be a UUID")
        if not isinstance(self.market, MarketSlice):
            raise TypeError("market must be a MarketSlice")


class BacktestMarketData(Protocol):
    def calendar(
        self,
        snapshot_id: UUID,
        start: date,
        end: date,
        *,
        include_next_session: bool,
    ) -> TradingCalendar:
        """Return the range and, when requested, its first later session."""
        ...

    def market_slice(
        self, snapshot_id: UUID, trade_date: date
    ) -> SnapshotMarketSlice: ...

    def corporate_actions(
        self, snapshot_id: UUID, trade_date: date
    ) -> tuple[CorporateAction, ...]: ...


class TargetGenerationPort(Protocol):
    def generate_target(
        self,
        strategy: StrategyRef,
        snapshot_id: UUID,
        signal_date: date,
        execute_date: date,
        current: AccountSnapshot,
    ) -> TargetPortfolio | None: ...


class ProgressSink(Protocol):
    def update(self, completed: int, total: int, trade_date: date) -> None: ...


class CancellationToken(Protocol):
    def is_cancelled(self) -> bool: ...


class BacktestCancelled(RuntimeError):
    def __init__(self, staging_dir: Path, sessions_completed: int) -> None:
        self.staging_dir = staging_dir
        self.sessions_completed = sessions_completed
        super().__init__(f"backtest cancelled after {sessions_completed} sessions")


class BacktestEngine:
    """Coordinate injected data, execution, accounting, and streaming artifacts."""

    def __init__(
        self,
        market_data: BacktestMarketData,
        targets: TargetGenerationPort,
        rulebook: MarketRuleBook,
        rebalance_planner: RebalancePlanner,
        *,
        artifact_root: Path,
        execution_model: ExecutionModel | None = None,
        writer_factory: Callable[
            [Path, UUID], BacktestArtifactWriter
        ] = BacktestArtifactWriter,
    ) -> None:
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        if not isinstance(rebalance_planner, RebalancePlanner):
            raise TypeError("rebalance_planner must be a RebalancePlanner")
        if execution_model is not None and not isinstance(
            execution_model, ExecutionModel
        ):
            raise TypeError("execution_model must be an ExecutionModel")
        if not callable(writer_factory):
            raise TypeError("writer_factory must be callable")
        version = getattr(rulebook, "version", None)
        if not isinstance(version, str) or not version.strip():
            raise TypeError("rulebook must provide a nonempty version")
        self._market_data = market_data
        self._targets = targets
        self._rulebook = rulebook
        self._planner = rebalance_planner
        self._root = artifact_root
        self._execution = execution_model or ExecutionModel()
        self._writer_factory = writer_factory

    def run(
        self,
        request: BacktestRequest,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        if not isinstance(request, BacktestRequest):
            raise TypeError("request must be a BacktestRequest")
        if request.rulebook_version != self._rulebook.version:
            raise ValueError(
                "request rulebook_version does not match injected rulebook"
            )
        writer = self._writer_factory(self._root, request.experiment_id)
        completed = 0
        try:
            calendar = self._market_data.calendar(
                request.snapshot_id,
                request.start_date,
                request.end_date,
                include_next_session=True,
            )
            sessions = _validate_calendar(calendar, request)
            account = PortfolioAccount(request.initial_cash_fen, calendar)
            pending: TargetPortfolio | None = None
            final_snapshot: AccountSnapshot | None = None
            for index, trade_date in enumerate(sessions):
                if cancellation.is_cancelled():
                    raise BacktestCancelled(writer.staging_dir, completed)
                bound_market = self._market_data.market_slice(
                    request.snapshot_id, trade_date
                )
                market = _validate_bound_market(bound_market, request, trade_date)
                closes, benchmark_close = _validate_market(market, request, trade_date)
                actions = self._market_data.corporate_actions(
                    request.snapshot_id, trade_date
                )
                account.begin_session(trade_date, actions)
                view = account.execution_view()
                if pending is None:
                    execution = self._execution.execute(
                        (),
                        market,
                        AccountView(view.cash_fen, view.sellable_quantities),
                        self._rulebook,
                        request.execution_config,
                    )
                else:
                    execution_prices = _execution_prices(
                        market,
                        pending,
                        view.total_quantities,
                        request.execution_config,
                    )
                    lots = {
                        instrument: self._rulebook.lot_size(instrument, trade_date)
                        for instrument in execution_prices
                    }
                    plan = self._planner.plan(
                        pending,
                        view.total_quantities,
                        view.cash_fen,
                        execution_prices,
                        lots,
                    )
                    execution = self._execution.execute(
                        plan.intents,
                        market,
                        AccountView(view.cash_fen, view.sellable_quantities),
                        self._rulebook,
                        request.execution_config,
                    )
                    pending = None
                account.apply(execution)
                final_snapshot = account.mark_to_market(trade_date, closes)
                writer.append_execution(execution)
                writer.append_snapshot(final_snapshot, benchmark_close)
                if index + 1 < len(sessions):
                    next_session = sessions[index + 1]
                    generated = self._targets.generate_target(
                        request.strategy,
                        request.snapshot_id,
                        trade_date,
                        next_session,
                        final_snapshot,
                    )
                    if generated is not None:
                        validate_target_portfolio(generated, trade_date, next_session)
                        if pending is not None:
                            raise ValueError("duplicate pending target")
                        writer.append_target(generated)
                        pending = generated
                completed += 1
                progress.update(completed, len(sessions), trade_date)
            if final_snapshot is None:
                raise RuntimeError("no final snapshot was produced")
            writer.close()
            context = ManifestContext(
                request.experiment_id,
                request.snapshot_id,
                request.strategy.strategy_id,
                request.strategy.version,
                request.start_date,
                request.end_date,
                request.benchmark,
                request.initial_cash_fen,
                request.rulebook_version,
                request.execution_config,
            )
            writer.validate(sessions, context)
            manifest_path = writer.publish()
            return BacktestResult(
                request.experiment_id,
                writer.artifact_dir,
                manifest_path,
                completed,
                final_snapshot,
            )
        except BaseException as error:
            writer.abort(error)
            raise


def _validate_calendar(calendar: object, request: BacktestRequest) -> tuple[date, ...]:
    if not isinstance(calendar, TradingCalendar):
        raise TypeError("market data calendar must be a TradingCalendar")
    if calendar.snapshot_id != SnapshotId(request.snapshot_id):
        raise ValueError("calendar snapshot does not match request")
    if calendar.start > request.start_date or calendar.end < request.end_date:
        raise ValueError("calendar does not cover request range")
    try:
        calendar.next_session(request.end_date)
    except ValueError as error:
        raise ValueError(
            "calendar does not cover next trading session after request end"
        ) from error
    sessions = calendar.sessions(request.start_date, request.end_date)
    if not sessions:
        raise ValueError("backtest request contains no trading sessions")
    return sessions


def _validate_market(
    market: object, request: BacktestRequest, trade_date: date
) -> tuple[dict[InstrumentId, float], float]:
    if not isinstance(market, MarketSlice):
        raise TypeError("market data must return a MarketSlice")
    if market.trade_date != trade_date:
        raise ValueError("market slice trade_date does not match session")
    closes: dict[InstrumentId, float] = {}
    for row in market.bars.select("instrument_id", "close").iter_rows(named=True):
        raw = row["instrument_id"]
        close = row["close"]
        if not isinstance(raw, str) or not isinstance(close, float):
            raise TypeError("market slice has invalid close data")
        closes[InstrumentId.parse(raw)] = close
    try:
        benchmark = closes[request.benchmark]
    except KeyError as error:
        raise ValueError("market slice is missing benchmark") from error
    if not isfinite(benchmark) or benchmark <= 0:
        raise ValueError("benchmark close must be finite and positive")
    return closes, benchmark


def _validate_bound_market(
    bound_market: object, request: BacktestRequest, trade_date: date
) -> MarketSlice:
    if not isinstance(bound_market, SnapshotMarketSlice):
        raise TypeError("market data must return a SnapshotMarketSlice")
    if bound_market.snapshot_id != request.snapshot_id:
        raise ValueError("market slice snapshot does not match request")
    if bound_market.market.trade_date != trade_date:
        raise ValueError("market slice trade_date does not match session")
    return bound_market.market


def _execution_prices(
    market: MarketSlice,
    target: TargetPortfolio,
    current: Mapping[InstrumentId, int],
    config: ExecutionConfig,
) -> dict[InstrumentId, float]:
    key = "open" if config.reference_price.value == "OPEN" else "close"
    rows = {
        InstrumentId.parse(raw): price
        for raw, price in market.bars.select("instrument_id", key).iter_rows()
    }
    needed = set(current) | {position.instrument_id for position in target.positions}
    try:
        return {instrument: rows[instrument] for instrument in needed}
    except KeyError as error:
        raise ValueError("market slice is missing execution price") from error


def _date(value: object, name: str) -> None:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f"{name} must be a date")
