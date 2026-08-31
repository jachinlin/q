"""实现策略订单驱动、T 日决策与 T+1 撮合的唯一回测引擎。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Protocol

import polars as pl

from quant_research.backtest.accounting import AccountSnapshot, PortfolioAccount
from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.corporate_actions import (
    CorporateAction,
    CorporateActionCalendarMapper,
    CorporateActionType,
)
from quant_research.backtest.execution import ExecutionModel
from quant_research.backtest.models import AccountView as ExecutionAccountView
from quant_research.backtest.models import ExecutionConfig, FillResult, MarketSlice
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.backtest.run_schema import RunTableSchema
from quant_research.data.contracts import JsonValue
from quant_research.domain.identifiers import IndexId, InstrumentId
from quant_research.strategies.base import (
    AccountView,
    DecisionContext,
    DecisionData,
    OrderIntent,
    Strategy,
)


@dataclass(frozen=True, slots=True)
class BacktestRequest:
    """定义一次策略研究回测的身份、区间、资金、基准和撮合配置。

    入参：
        strategy_study_id：策略研究标识；catalog_hash、rulebook_hash：
        数据目录与交易规则的 SHA-256；其余字段定义区间、基准、资金和撮合参数。
    返回值：
        创建经过身份、日期和资金校验的回测请求。
    异常：
        ValueError：标识为空、哈希非法、日期倒置或初始资金非正时抛出。
    """

    strategy_study_id: str
    catalog_hash: str
    start_date: date
    end_date: date
    benchmark: IndexId
    initial_cash_fen: int
    rulebook_hash: str
    execution_config: ExecutionConfig

    def __post_init__(self) -> None:
        if not self.strategy_study_id:
            raise ValueError("strategy_study_id must be nonempty")
        for value, name in (
            (self.catalog_hash, "catalog_hash"),
            (self.rulebook_hash, "rulebook_hash"),
        ):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValueError(f"{name} must be a SHA-256 digest")
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")
        if self.initial_cash_fen <= 0:
            raise ValueError("initial_cash_fen must be positive")


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """记录尚未发布的规范回测表、冻结输入和最终账户状态。

    入参：
        各字段分别提供策略研究标识、回测表、配置、输入身份、已处理交易日数和
        最终账户快照。
    返回值：
        创建不可变的回测结果值对象。
    异常：
        不执行额外校验；字段类型错误由调用边界的类型校验负责。
    """

    strategy_study_id: str
    tables: Mapping[str, pl.DataFrame]
    config: Mapping[str, JsonValue]
    identities: Mapping[str, JsonValue]
    sessions_completed: int
    final_snapshot: AccountSnapshot


@dataclass(frozen=True, slots=True)
class BoundMarketSlice:
    """表示已绑定一个交易日的未复权撮合行情。

    入参：
        market：包含唯一交易日及证券行情的撮合切片。
    返回值：
        创建供回测引擎消费的日期绑定行情对象。
    异常：
        不执行额外校验；日期一致性由回测引擎检查。
    """

    market: MarketSlice


class BacktestMarketData(Protocol):
    """定义交易日历和逐日未复权行情读取端口。

    入参：
        实现方接收回测日期范围或单个交易日。
    返回值：
        实现方返回交易日历和绑定日期的行情切片。
    异常：
        数据缺失、PIT 违规或读取失败时由实现方给出明确异常。
    """

    def calendar(
        self, start: date, end: date, *, include_next_session: bool
    ) -> TradingCalendar:
        """读取指定闭区间的交易日历。

        入参：
            start、end：日历边界；include_next_session：是否附加下一交易日。
        返回值：
            返回稳定排序且无重复日期的交易日历。
        异常：
            ValueError：日期范围无效或权威日历缺失时抛出。
        """
        ...

    def market_slice(
        self,
        trade_date: date,
        instruments: Sequence[InstrumentId],
    ) -> BoundMarketSlice:
        """读取单个交易日的未复权撮合行情。

        入参：
            trade_date：待撮合和估值的交易日；instruments：当日持仓与待执行
            委托涉及的确定性证券范围。
        返回值：
            返回严格绑定该日期的行情切片。
        异常：
            ValueError：该日行情不存在或不完整时抛出。
        """
        ...

    def benchmark_closes(
        self,
        benchmark: IndexId,
        sessions: Sequence[date],
    ) -> Mapping[date, float]:
        """一次读取完整回测区间的基准收盘价。

        入参：benchmark：基准指数；sessions：稳定排序且无重复的回测交易日。
        返回值：覆盖全部交易日的不可变日期到收盘价映射。
        异常：日期范围、覆盖、唯一性或价格非法时抛出 ``ValueError``。
        """
        ...


class BacktestCorporateActionData(Protocol):
    """定义回测区间权益事件的一次性预载端口。

    入参：实现方接收研究自然日闭区间。返回值：按登记日 PIT 裁决的股票与基金
    实施事件。异常：数据缺失、PIT 违规或读取失败时由实现方抛出明确异常。
    """

    def corporate_actions(
        self, start: date, end: date
    ) -> tuple[CorporateAction, ...]:
        """一次读取研究区间全部分红送转事件。

        入参：研究开始与结束日。返回值：事件 ID 唯一且稳定排序的不可变序列。
        异常：范围、PIT 或 Canonical 读取不满足契约时抛出明确异常。
        """
        ...

class DecisionDataFactory(Protocol):
    """为单个信号日创建只允许 PIT 截断读取的决策数据。

    入参：
        实现方接收策略当前唯一信号日。
    返回值：
        实现方返回无法越过信号日读取数据的 DecisionData。
    异常：
        信号日无权威数据或 PIT 约束失败时由实现方抛出明确异常。
    """

    def bind(self, signal_date: date) -> DecisionData:
        """将策略数据读取能力绑定到唯一信号日。

        入参：
            signal_date：策略可见信息的截止日期。
        返回值：
            返回信号日固定且查询方法不接受额外日期的决策数据。
        异常：
            ValueError：信号日不能形成有效 PIT 视图时抛出。
        """
        ...


class CatalogGuard(Protocol):
    """在回测边界校验提交时捕获的数据目录身份。

    入参：
        实现方接收 Run 提交时冻结的 catalog_hash。
    返回值：
        校验成功时不返回业务数据。
    异常：
        RuntimeError：当前目录身份已漂移或目录门禁关闭时抛出。
    """

    def assert_unchanged(self, catalog_hash: str) -> None:
        """断言当前数据目录仍等于 Run 捕获的身份。

        入参：
            catalog_hash：提交时冻结的 SHA-256 目录身份。
        返回值：
            身份一致时返回 None。
        异常：
            RuntimeError：目录身份变化、不可用或未通过质量门禁时抛出。
        """
        ...


class ProgressSink(Protocol):
    """接收交易日粒度的持久化回测进度。

    入参：
        实现方接收已完成数量、总数量和最近完成交易日。
    返回值：
        进度写入成功时不返回业务数据。
    异常：
        持久化失败时由实现方保留存储异常语义。
    """

    def update(self, completed: int, total: int, trade_date: date) -> None:
        """持久化最近完成的交易日和总体进度。

        入参：
            completed、total：已完成数与总数；trade_date：最近完成日期。
        返回值：
            写入成功时返回 None。
        异常：
            ValueError：进度越界时可由实现方抛出；存储错误继续向上传播。
        """
        ...


class CancellationToken(Protocol):
    """提供无副作用的协作取消查询。

    入参：
        查询不接收参数，状态由任务存储提供。
    返回值：
        返回任务是否已收到取消请求。
    异常：
        取消状态无法读取时由实现方抛出存储异常。
    """

    def is_cancelled(self) -> bool:
        """查询当前 Run 是否应在最近边界取消。

        入参：
            无。
        返回值：
            已请求取消返回 True，否则返回 False。
        异常：
            任务状态无法可靠读取时抛出实现方定义的异常。
        """
        ...


class BacktestCancelled(RuntimeError):
    """表示回测在交易日边界响应了取消。

    入参：
        message：包含已完成交易日数量的取消说明。
    返回值：
        创建供策略研究处理器转换为 CANCELLED 状态的异常。
    异常：
        构造过程不主动抛出其他异常。
    """


class BacktestEngine:
    """按交易日先执行前日订单，再估值并调用唯一策略回调。

    入参：
        market_data、decision_data、rulebook、catalog_guard：数据与规则端口；
        artifact_root：不可变产物根；execution_model：可选撮合内核。
    返回值：
        创建可执行 T 日决策、T+1 撮合的回测引擎。
    异常：
        依赖构造本身不触发读取；运行期异常由 run 方法说明。
    """

    def __init__(
        self,
        market_data: BacktestMarketData,
        decision_data: DecisionDataFactory,
        corporate_action_data: BacktestCorporateActionData,
        rulebook: MarketRuleBook,
        catalog_guard: CatalogGuard,
        *,
        execution_model: ExecutionModel | None = None,
    ) -> None:
        self._market_data = market_data
        self._decision_data = decision_data
        self._corporate_action_data = corporate_action_data
        self._rulebook = rulebook
        self._catalog_guard = catalog_guard
        self._execution = execution_model or ExecutionModel()

    def run(
        self,
        request: BacktestRequest,
        strategy: Strategy,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        """执行订单驱动回测并返回供分析和原子发布的内存结果。

        入参：
            request：冻结的 Run 请求；strategy：订单策略；progress：进度端口；
            cancellation：取消查询端口。
        返回值：
            返回规范回测表、冻结输入身份和最终账户快照。
        异常：
            TypeError：请求类型错误时抛出；ValueError：日历、行情或策略输出
            不合法时抛出；BacktestCancelled：任务被协作取消时抛出；
            RuntimeError：数据身份漂移或账户状态非法时抛出。
        """
        if not isinstance(request, BacktestRequest):
            raise TypeError("request must be BacktestRequest")
        self._catalog_guard.assert_unchanged(request.catalog_hash)
        calendar = self._market_data.calendar(
            request.start_date, request.end_date, include_next_session=True
        )
        sessions = calendar.sessions(request.start_date, request.end_date)
        if not sessions:
            raise ValueError("backtest has no trading sessions")
        benchmark_closes = self._market_data.benchmark_closes(
            request.benchmark, sessions
        )
        corporate_actions = CorporateActionCalendarMapper.map(
            self._corporate_action_data.corporate_actions(
                request.start_date, request.end_date
            ),
            calendar,
        )
        account = PortfolioAccount(request.initial_cash_fen, calendar)
        pending: tuple[OrderIntent, ...] = ()
        tables: dict[str, list[dict[str, object]]] = {
            name: []
            for name in ("orders", "fills", "holdings", "costs", "nav", "dividends")
        }
        snapshots: list[AccountSnapshot] = []
        final: AccountSnapshot | None = None
        for index, trade_date in enumerate(sessions):
            if cancellation.is_cancelled():
                raise BacktestCancelled(f"backtest cancelled after {index} sessions")
            account.begin_session(trade_date)
            account.apply_corporate_actions_before_open(corporate_actions)
            execution_view = account.execution_view()
            market_instruments = tuple(
                sorted(
                    set(execution_view.total_quantities)
                    | {intent.instrument_id for intent in pending},
                    key=InstrumentId.canonical,
                )
            )
            bound = self._market_data.market_slice(trade_date, market_instruments)
            if bound.market.trade_date != trade_date:
                raise ValueError("market slice is bound to a different date")
            market = bound.market
            closes = self._closes(market)
            try:
                benchmark_close = benchmark_closes[trade_date]
            except KeyError as error:
                raise ValueError("benchmark close is missing for a session") from error
            execution = self._execution.execute(
                pending,
                market,
                ExecutionAccountView(
                    execution_view.cash_fen, execution_view.sellable_quantities
                ),
                self._rulebook,
                request.execution_config,
            )
            account.apply(execution)
            final = account.mark_to_market(trade_date, closes)
            account.lock_corporate_actions_after_close(corporate_actions)
            snapshots.append(final)
            self._append_execution(
                tables, execution, request.execution_config.slippage_bps
            )
            self._append_snapshot(tables, final, benchmark_close)
            pending = ()
            if index + 1 < len(sessions):
                next_date = sessions[index + 1]
                data = self._decision_data.bind(trade_date)
                if data.signal_date != trade_date:
                    raise ValueError("DecisionData is bound to a different signal_date")
                account_view = AccountView(
                    cash_fen=final.cash_fen,
                    positions={
                        item.instrument_id: item.total_quantity
                        for item in final.positions
                    },
                    sellable={
                        item.instrument_id: item.sellable_quantity
                        for item in final.positions
                    },
                    equity_fen=final.nav_fen,
                    available_margin_fen=0,
                    mark_prices={
                        item.instrument_id: (
                            item.market_value_fen / item.total_quantity / 100
                        )
                        for item in final.positions
                        if item.total_quantity > 0
                    },
                )
                context = DecisionContext(trade_date, next_date, data, account_view)
                if index == 0:
                    strategy.warmup(context)
                pending = tuple(strategy.on_event(context))
                self._append_orders(tables, context, pending)
            progress.update(index + 1, len(sessions), trade_date)
        if final is None:
            raise RuntimeError("backtest produced no account snapshot")
        self._append_dividends(tables, account)
        self._catalog_guard.assert_unchanged(request.catalog_hash)
        signals = self._signals(strategy)
        return BacktestResult(
            request.strategy_study_id,
            RunTableSchema.canonical_backtest_tables({**tables, "signals": signals}),
            self._config(request, strategy),
            {
                "strategy_study_id": request.strategy_study_id,
                "catalog_hash": request.catalog_hash,
                "rulebook_hash": request.rulebook_hash,
                "strategy_id": strategy.spec.strategy_id,
            },
            len(sessions),
            final,
        )

    @staticmethod
    def _closes(market: MarketSlice) -> dict[InstrumentId, float]:
        closes: dict[InstrumentId, float] = {}
        for identifier, close in market.bars.select(
            "instrument_id", "close"
        ).iter_rows():
            if close is not None and float(close) > 0:
                closes[InstrumentId.parse(identifier)] = float(close)
        return closes

    @staticmethod
    def _append_orders(
        tables: dict[str, list[dict[str, object]]],
        context: DecisionContext,
        orders: Sequence[OrderIntent],
    ) -> None:
        seen: set[InstrumentId] = set()
        for order_index, order in enumerate(orders):
            if order.instrument_id in seen:
                raise ValueError(
                    "strategy orders must be unique by instrument per decision"
                )
            seen.add(order.instrument_id)
            tables["orders"].append(
                {
                    "signal_date": context.signal_date,
                    "execute_date": context.execute_date,
                    "order_index": order_index,
                    "instrument_id": order.instrument_id.canonical(),
                    "side": order.side.value,
                    "quantity": order.quantity,
                    "reason": order.reason,
                }
            )

    @staticmethod
    def _append_execution(
        tables: dict[str, list[dict[str, object]]],
        execution: object,
        slippage_bps: float,
    ) -> None:
        from quant_research.backtest.models import ExecutionBatch

        if not isinstance(execution, ExecutionBatch):
            raise TypeError("execution must be ExecutionBatch")
        for result_index, result in enumerate(execution.results):
            filled = result.filled_quantity if isinstance(result, FillResult) else 0
            price = result.price if isinstance(result, FillResult) else None
            gross = result.gross_value_fen if isinstance(result, FillResult) else 0
            tables["fills"].append(
                {
                    "trade_date": execution.trade_date,
                    "result_index": result_index,
                    "instrument_id": result.intent.instrument_id.canonical(),
                    "side": result.intent.side.value,
                    "requested_quantity": result.requested_quantity,
                    "filled_quantity": filled,
                    "unfilled_quantity": result.requested_quantity - filled,
                    "reference_price": result.reference_price,
                    "price": price,
                    "gross_value_fen": gross,
                    "reason_code": result.reason_code.value,
                }
            )
            if isinstance(result, FillResult):
                slippage = round(
                    abs(result.price - result.reference_price)
                    * result.filled_quantity
                    * 100
                )
                fees = result.fees.total_cents
                tables["costs"].append(
                    {
                        "trade_date": execution.trade_date,
                        "result_index": result_index,
                        "instrument_id": result.intent.instrument_id.canonical(),
                        "rule_fees_fen": fees,
                        "slippage_fen": slippage,
                        "total_cost_fen": fees + slippage,
                    }
                )

    @staticmethod
    def _append_snapshot(
        tables: dict[str, list[dict[str, object]]],
        snapshot: AccountSnapshot,
        benchmark_close: float,
    ) -> None:
        for position in snapshot.positions:
            tables["holdings"].append(
                {
                    "trade_date": snapshot.trade_date,
                    "instrument_id": position.instrument_id.canonical(),
                    "total_quantity": position.total_quantity,
                    "sellable_quantity": position.sellable_quantity,
                    "cost_basis_fen": position.cost_basis_fen,
                    "market_value_fen": position.market_value_fen,
                }
            )
        tables["nav"].append(
            {
                "trade_date": snapshot.trade_date,
                "cash_fen": snapshot.cash_fen,
                "dividend_receivable_fen": snapshot.dividend_receivable_fen,
                "long_market_value_fen": snapshot.total_market_value_fen,
                "short_market_value_fen": 0,
                "accrued_fees_fen": 0,
                "margin_used_fen": 0,
                "equity_fen": snapshot.nav_fen,
                "benchmark_close": benchmark_close,
            }
        )

    @staticmethod
    def _append_dividends(
        tables: dict[str, list[dict[str, object]]], account: PortfolioAccount
    ) -> None:
        """把账户权益审计记录转换为固定 Run 表。

        入参：回测表与已完成账户。返回值：无。异常：账户记录字段非法时由
        ``RunTableSchema`` 在规范化阶段报告。
        """
        for record in account.dividend_records:
            action = record.action
            event = action.event
            tables["dividends"].append(
                {
                    "event_id": event.event_id,
                    "instrument_id": event.instrument_id.canonical(),
                    "instrument_type": event.instrument_type.value,
                    "action_type": event.action_type.value,
                    "source_revision": event.source_revision,
                    "announcement_date": event.announcement_date,
                    "implementation_announcement_date": (
                        event.implementation_announcement_date
                    ),
                    "record_date": event.record_date,
                    "mapped_record_date": action.mapped_record_date,
                    "ex_date": event.ex_date,
                    "mapped_ex_date": action.mapped_ex_date,
                    "pay_date": event.pay_date,
                    "mapped_pay_date": action.mapped_pay_date,
                    "stock_listing_date": event.stock_listing_date,
                    "mapped_stock_listing_date": action.mapped_stock_listing_date,
                    "entitlement_quantity": record.entitlement_quantity,
                    "cash_per_share_or_unit": float(event.cash_per_share_or_unit),
                    "gross_cash_fen": record.gross_cash_fen,
                    "receivable_fen": record.receivable_fen,
                    "paid_fen": record.paid_fen,
                    "stock_dividend_per_share": float(
                        event.stock_dividend_per_share
                    ),
                    "previous_adjustment_factor": (
                        float(event.previous_adjustment_factor)
                        if event.previous_adjustment_factor is not None
                        else None
                    ),
                    "adjustment_factor": (
                        float(event.adjustment_factor)
                        if event.adjustment_factor is not None
                        else None
                    ),
                    "raw_adjustment_factor_ratio": (
                        float(
                            event.adjustment_factor
                            / event.previous_adjustment_factor
                        )
                        if event.adjustment_factor is not None
                        and event.previous_adjustment_factor is not None
                        else None
                    ),
                    "split_inference_relative_error": (
                        float(
                            abs(
                                event.adjustment_factor
                                / event.previous_adjustment_factor
                                / event.stock_dividend_per_share
                                - 1
                            )
                        )
                        if event.action_type is CorporateActionType.FUND_SPLIT
                        and event.adjustment_factor is not None
                        and event.previous_adjustment_factor is not None
                        else None
                    ),
                    "split_inference_method": (
                        "ADJUSTMENT_FACTOR_NEAR_INTEGER_0.1_PERCENT"
                        if event.action_type is CorporateActionType.FUND_SPLIT
                        else None
                    ),
                    "distributed_quantity": record.distributed_quantity,
                    "discarded_fractional_quantity": float(
                        record.discarded_fractional_quantity
                    ),
                    "cash_tax_treatment": "PRE_TAX",
                    "stock_rounding_treatment": "FLOOR",
                }
            )

    @staticmethod
    def _signals(strategy: Strategy) -> pl.DataFrame | list[dict[str, object]]:
        getter = getattr(strategy, "signal_frame", None)
        if not callable(getter):
            return []
        frame = getter()
        if not isinstance(frame, pl.DataFrame) or frame.is_empty():
            return []
        return pl.DataFrame(
            {
                "signal_date": frame["signal_date"],
                "instrument_id": frame["instrument_id"],
                "signal": frame["state"]
                if "state" in frame.columns
                else pl.Series([strategy.spec.strategy_id] * len(frame)),
                "value": (frame["short_ma"] - frame["long_ma"])
                if {"short_ma", "long_ma"}.issubset(frame.columns)
                else frame["score"]
                if "score" in frame.columns
                else pl.Series([None] * len(frame), dtype=pl.Float64),
                "state_changed": frame["state_changed"]
                if "state_changed" in frame.columns
                else pl.Series([False] * len(frame)),
                "invalid_reason": frame["invalid_reason"]
                if "invalid_reason" in frame.columns
                else pl.Series([None] * len(frame), dtype=pl.String),
            }
        )

    @staticmethod
    def _config(request: BacktestRequest, strategy: Strategy) -> dict[str, JsonValue]:
        return {
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
            "benchmark": request.benchmark.canonical(),
            "initial_cash_fen": request.initial_cash_fen,
            "strategy": {
                "strategy_id": strategy.spec.strategy_id,
                "parameters": dict(strategy.spec.parameters),
            },
            "execution": {
                "reference_price": request.execution_config.reference_price.value,
                "slippage_bps": request.execution_config.slippage_bps,
                "max_volume_participation": request.execution_config.max_volume_participation,
            },
        }


__all__ = [
    "BacktestCancelled",
    "BacktestCorporateActionData",
    "BacktestEngine",
    "BacktestRequest",
    "BacktestResult",
    "BoundMarketSlice",
    "CancellationToken",
    "CatalogGuard",
    "DecisionDataFactory",
    "ProgressSink",
]
