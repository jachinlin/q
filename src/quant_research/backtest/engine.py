"""提供回测与回测引擎相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

from quant_research.backtest.accounting import AccountSnapshot, PortfolioAccount
from quant_research.backtest.artifacts import BacktestArtifactWriter, ManifestContext
from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.execution import ExecutionModel
from quant_research.backtest.models import (
    AccountView,
    ExecutionConfig,
    MarketSlice,
)
from quant_research.backtest.rulebook import InstrumentTradingProfile, MarketRuleBook
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.constructor import (
    TargetPortfolio,
    validate_target_portfolio,
)
from quant_research.portfolio.rebalance import RebalancePlanner

_EPSILON = 1e-10


@dataclass(frozen=True, slots=True)
class StrategyRef:
    """表示回测流程中的策略``ref``及其业务不变量。

    入参：
        strategy_id：用于持久化关联和日志追踪的策略标识。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    strategy_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise ValueError("strategy_id must be a nonempty string")


@dataclass(frozen=True, slots=True)
class BacktestRequest:
    """定义一次回测操作在进入用例边界前必须校验的输入。

    入参：
        experiment_id：目标实验标识，类型为 ``UUID``。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        strategy：策略。
        start_date：查询或运行覆盖区间的首日（含）。
        end_date：查询或运行覆盖区间的末日（含）。
        benchmark：基准。
        initial_cash_fen：初始``cash``分币金额。
        rulebook_hash：唯一 A 股交易规则文件的内容身份。
        execution_config：成交执行配置。
        industry_input：显式行业依赖及其可见性语义；未启用时为空。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    experiment_id: UUID
    data_hash: str
    strategy: StrategyRef
    start_date: date
    end_date: date
    benchmark: InstrumentId
    initial_cash_fen: int
    rulebook_hash: str
    execution_config: ExecutionConfig
    industry_input: Mapping[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.experiment_id, UUID):
            raise TypeError("experiment_id must be a UUID")
        _EngineSupport._hash(self.data_hash, "data_hash")
        if not isinstance(self.strategy, StrategyRef):
            raise TypeError("strategy must be a StrategyRef")
        _EngineSupport._date(self.start_date, "start_date")
        _EngineSupport._date(self.end_date, "end_date")
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")
        if not isinstance(self.benchmark, InstrumentId):
            raise TypeError("benchmark must be an InstrumentId")
        if type(self.initial_cash_fen) is not int or self.initial_cash_fen <= 0:
            raise ValueError("initial_cash_fen must be a positive integer")
        _EngineSupport._hash(self.rulebook_hash, "rulebook_hash")
        if not isinstance(self.execution_config, ExecutionConfig):
            raise TypeError("execution_config must be an ExecutionConfig")
        if self.industry_input is not None:
            if not isinstance(self.industry_input, Mapping):
                raise TypeError("industry_input must be a mapping or None")
            canonical_json_bytes(self.industry_input)
            object.__setattr__(
                self, "industry_input", MappingProxyType(dict(self.industry_input))
            )


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """记录一次回测操作的结果、业务指标和审计身份。

    入参：
        experiment_id：目标实验标识，类型为 ``UUID``。
        artifact_dir：完成原子发布后的不可变产物目录。
        manifest_path：记录文件身份、Schema、行数和输入身份的清单路径。
        sessions_completed：交易会话集合完成。
        final_snapshot：``final``账户快照。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

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
class BoundMarketSlice:
    """表示回测流程中的``bound``市场数据``slice``及其业务不变量。

    入参：
        market：当前交易日经过 Schema 校验的市场切片。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    A validated market slice supplied for one trading date.
    """

    market: MarketSlice

    def __post_init__(self) -> None:
        if not isinstance(self.market, MarketSlice):
            raise TypeError("market must be a MarketSlice")


class BacktestMarketData(Protocol):
    """定义 ``BacktestMarketData`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``BacktestMarketData`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def calendar(
        self,
        start: date,
        end: date,
        *,
        include_next_session: bool,
    ) -> TradingCalendar:
        """处理回测中的交易日历。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
            include_next_session：控制是否启用包含范围``next``交易会话规则的布尔开关。
        返回值：
            返回交易日历（``TradingCalendar``）。
        异常：
            无。
        Return the range and, when requested, its first later session.
        """
        ...

    def market_slice(self, trade_date: date) -> BoundMarketSlice:
        """处理回测中的市场数据``slice``。

        入参：
            trade_date：目标交易日期，类型为 ``date``。
        返回值：
            返回``slice``（``BoundMarketSlice``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class TargetGenerationPort(Protocol):
    """定义 ``TargetGenerationPort`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``TargetGenerationPort`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def generate_target(
        self,
        strategy: StrategyRef,
        signal_date: date,
        execute_date: date,
        current: AccountSnapshot,
    ) -> TargetPortfolio | None:
        """生成目标组合。

        入参：
            strategy：策略。
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            execute_date：使用上一交易日信号生成委托并撮合的交易日。
            current：当前值。
        返回值：
            返回生成目标组合后的目标组合（``TargetPortfolio | None``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class ProgressSink(Protocol):
    """定义 ``ProgressSink`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``ProgressSink`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def update(self, completed: int, total: int, trade_date: date) -> None:
        """更新处理状态或进度。

        入参：
            completed：完成。
            total：总量。
            trade_date：目标交易日期，类型为 ``date``。
        返回值：
            无。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class CancellationToken(Protocol):
    """定义 ``CancellationToken`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``CancellationToken`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def is_cancelled(self) -> bool:
        """判断``cancelled``。

        入参：
            无。
        返回值：
            返回是否``cancelled``。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class BacktestCancelled(RuntimeError):
    """表示 ``BacktestCancelled`` 对应的领域异常。

    入参：
        staging_dir：发布前写入文件的同文件系统暂存目录。
        sessions_completed：交易会话集合完成。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    def __init__(self, staging_dir: Path, sessions_completed: int) -> None:
        self.staging_dir = staging_dir
        self.sessions_completed = sessions_completed
        super().__init__(f"backtest cancelled after {sessions_completed} sessions")


class BacktestEngine:
    """协调回测计算所需的输入、规则和输出校验。

    入参：
        market_data：市场数据数据。
        targets：``targets``。
        rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
        rebalance_planner：调仓``planner``。
        artifact_root：不可变实验产物的可信根目录。
        execution_model：成交执行``model``。
        writer_factory：由组合根注入、用于隔离外部副作用的产物写入器``factory``端口。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Coordinate injected data, execution, accounting, and streaming artifacts.
    """

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
        """执行完整处理流程。

        入参：
            request：请求。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行回测后的运行（``BacktestResult``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``BacktestCancelled``、``RuntimeError``、``TypeError``、``ValueError``。
        """
        if not isinstance(request, BacktestRequest):
            raise TypeError("request must be a BacktestRequest")
        writer = self._writer_factory(self._root, request.experiment_id)
        completed = 0
        try:
            calendar = self._market_data.calendar(
                request.start_date,
                request.end_date,
                include_next_session=True,
            )
            sessions = _EngineSupport._validate_calendar(calendar, request)
            account = PortfolioAccount(request.initial_cash_fen, calendar)
            pending: TargetPortfolio | None = None
            final_snapshot: AccountSnapshot | None = None
            last_closes: dict[InstrumentId, float] = {}
            for index, trade_date in enumerate(sessions):
                if cancellation.is_cancelled():
                    raise BacktestCancelled(writer.staging_dir, completed)
                bound_market = self._market_data.market_slice(trade_date)
                market = _EngineSupport._validate_bound_market(
                    bound_market, request, trade_date
                )
                closes, benchmark_close = _EngineSupport._validate_market(
                    market, request, trade_date
                )
                last_closes.update(closes)
                account.begin_session(trade_date)
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
                    execution_prices = _EngineSupport._execution_prices(
                        market,
                        pending,
                        view.total_quantities,
                        request.execution_config,
                        last_closes,
                    )
                    profiles = _EngineSupport._trading_profiles(
                        market,
                        execution_prices,
                        self._rulebook,
                        trade_date,
                    )
                    plan = self._planner.plan(
                        pending,
                        view.total_quantities,
                        view.cash_fen,
                        execution_prices,
                        profiles,
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
                request.data_hash,
                request.strategy.strategy_id,
                request.start_date,
                request.end_date,
                request.benchmark,
                request.initial_cash_fen,
                request.rulebook_hash,
                request.execution_config,
                request.industry_input,
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


class _EngineSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_calendar(
        calendar: object, request: BacktestRequest
    ) -> tuple[date, ...]:
        if not isinstance(calendar, TradingCalendar):
            raise TypeError("market data calendar must be a TradingCalendar")
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

    @staticmethod
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
            if not isinstance(raw, str):
                raise TypeError("market slice has invalid instrument data")
            if close is None:
                continue
            if not isinstance(close, float) or not isfinite(close) or close <= 0:
                raise TypeError("market slice has invalid close data")
            closes[InstrumentId.parse(raw)] = close
        try:
            benchmark = closes[request.benchmark]
        except KeyError as error:
            raise ValueError("market slice is missing benchmark") from error
        if not isfinite(benchmark) or benchmark <= 0:
            raise ValueError("benchmark close must be finite and positive")
        return closes, benchmark

    @staticmethod
    def _validate_bound_market(
        bound_market: object, request: BacktestRequest, trade_date: date
    ) -> MarketSlice:
        if not isinstance(bound_market, BoundMarketSlice):
            raise TypeError("market data must return a BoundMarketSlice")
        if bound_market.market.trade_date != trade_date:
            raise ValueError("market slice trade_date does not match session")
        return bound_market.market

    @staticmethod
    def _execution_prices(
        market: MarketSlice,
        target: TargetPortfolio,
        current: Mapping[InstrumentId, int],
        config: ExecutionConfig,
        last_closes: Mapping[InstrumentId, float],
    ) -> dict[InstrumentId, float]:
        key = "open" if config.reference_price.value == "OPEN" else "close"
        rows = {
            InstrumentId.parse(raw): price
            for raw, price in market.bars.select("instrument_id", key).iter_rows()
            if isinstance(price, float)
        }
        needed = set(current) | {
            position.instrument_id for position in target.positions
        }
        try:
            return {
                instrument: (
                    rows[instrument] if instrument in rows else last_closes[instrument]
                )
                for instrument in needed
            }
        except KeyError as error:
            raise ValueError("market slice is missing execution price") from error

    @staticmethod
    def _trading_profiles(
        market: MarketSlice,
        prices: Mapping[InstrumentId, float],
        rulebook: MarketRuleBook,
        trade_date: date,
    ) -> dict[InstrumentId, InstrumentTradingProfile]:
        rows = {
            InstrumentId.parse(raw): (instrument_type, board)
            for raw, instrument_type, board in market.bars.select(
                "instrument_id", "instrument_type", "board"
            ).iter_rows()
        }
        profiles: dict[InstrumentId, InstrumentTradingProfile] = {}
        for instrument in prices:
            try:
                instrument_type, raw_board = rows[instrument]
            except KeyError as error:
                raise ValueError("market slice is missing trading metadata") from error
            if not isinstance(instrument_type, str) or not isinstance(raw_board, str):
                raise TypeError("market slice has invalid trading metadata")
            profiles[instrument] = rulebook.trading_profile(
                instrument, instrument_type, Board(raw_board), trade_date
            )
        return profiles

    @staticmethod
    def _date(value: object, name: str) -> None:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"{name} must be a date")

    @staticmethod
    def _hash(value: object, name: str) -> None:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
