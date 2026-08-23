"""定义策略插件、PIT 决策视图和订单输出的唯一公共契约。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from math import isfinite
from types import MappingProxyType
from typing import Protocol

import polars as pl

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId


class OrderSide(StrEnum):
    """表示策略可声明的订单方向；P3 只执行买入和卖出。

    入参：按枚举值构造时接收方向字符串。
    返回值：对应的订单方向枚举成员。
    异常：未知方向由 ``StrEnum`` 抛出 ``ValueError``。
    """

    BUY = "BUY"
    SELL = "SELL"
    SHORT_OPEN = "SHORT_OPEN"
    SHORT_COVER = "SHORT_COVER"


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """表示策略提交给回测引擎的整数股数订单意图。

    入参：证券标识、订单方向、正整数股数和可选原因码。
    返回值：冻结的订单意图值对象。
    异常：证券、方向、数量或原因类型非法时抛出类型或值错误。
    """

    instrument_id: InstrumentId
    side: OrderSide
    quantity: int
    reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_id, InstrumentId):
            raise TypeError("instrument_id must be an InstrumentId")
        if not isinstance(self.side, OrderSide):
            raise TypeError("side must be an OrderSide")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        if not isinstance(self.reason, str):
            raise TypeError("reason must be a string")


@dataclass(frozen=True, slots=True)
class AccountView:
    """向策略暴露不可修改的现金、头寸、可卖数量和权益。

    入参：分为单位的现金与权益、整数持仓、可卖数量和可用保证金。
    返回值：按证券稳定排序且不可修改的账户视图。
    异常：金额、证券键或数量不满足 P3 多头约束时抛出类型或值错误。
    """

    cash_fen: int
    positions: Mapping[InstrumentId, int]
    sellable: Mapping[InstrumentId, int]
    equity_fen: int
    available_margin_fen: int = 0

    def __post_init__(self) -> None:
        for value, field in (
            (self.cash_fen, "cash_fen"),
            (self.equity_fen, "equity_fen"),
            (self.available_margin_fen, "available_margin_fen"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a nonnegative integer")
        positions = self._quantities(self.positions, "positions")
        sellable = self._quantities(self.sellable, "sellable")
        if any(value > positions.get(key, 0) for key, value in sellable.items()):
            raise ValueError("sellable quantity must not exceed position")
        object.__setattr__(self, "positions", MappingProxyType(positions))
        object.__setattr__(self, "sellable", MappingProxyType(sellable))

    @staticmethod
    def _quantities(
        values: Mapping[InstrumentId, int], field: str
    ) -> dict[InstrumentId, int]:
        if not isinstance(values, Mapping):
            raise TypeError(f"{field} must be a mapping")
        result: dict[InstrumentId, int] = {}
        for key, value in values.items():
            if not isinstance(key, InstrumentId):
                raise TypeError(f"{field} keys must be InstrumentId")
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} values must be nonnegative integers")
            result[key] = value
        return dict(sorted(result.items(), key=lambda item: item[0].canonical()))


@dataclass(frozen=True, slots=True)
class TargetWeights:
    """表示权重型策略在一个决策日产生的目标组合。

    入参：信号日、下一执行日和证券到非负权重的映射。
    返回值：按证券稳定排序的冻结目标权重。
    异常：日期次序、证券键、权重范围或总权重非法时抛出错误。
    """

    signal_date: date
    execute_date: date
    weights: Mapping[InstrumentId, float]

    def __post_init__(self) -> None:
        if type(self.signal_date) is not date or type(self.execute_date) is not date:
            raise TypeError("signal_date and execute_date must be dates")
        if self.execute_date <= self.signal_date:
            raise ValueError("execute_date must follow signal_date")
        output: dict[InstrumentId, float] = {}
        for key, value in self.weights.items():
            if not isinstance(key, InstrumentId):
                raise TypeError("weight keys must be InstrumentId")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError("weights must be numeric")
            number = float(value)
            if not isfinite(number) or number < 0 or number > 1:
                raise ValueError("P3 weights must be finite values in [0, 1]")
            output[key] = number
        if sum(output.values()) > 1.0 + 1e-10:
            raise ValueError("target weights must sum to at most one")
        object.__setattr__(
            self,
            "weights",
            MappingProxyType(
                dict(sorted(output.items(), key=lambda item: item[0].canonical()))
            ),
        )


class DecisionData(Protocol):
    """定义绑定单一信号日、无法请求未来日期的只读数据视图。

    入参：各查询只接受证券、因子和回看长度，不接受 ``as_of`` 或结束日。
    返回值：PIT 截断后的 Polars ``LazyFrame``。
    异常：缺少数据或请求越界时由实现方抛出数据读取异常。
    """

    @property
    def signal_date(self) -> date:
        """读取该视图唯一信号日。

        入参：无。
        返回值：绑定的交易日。
        异常：无。
        """
        ...

    def bars(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取未复权日线。

        入参：证券序列和正回看交易日数。
        返回值：不晚于信号日的行情表。
        异常：请求非法或数据不可用时抛出对应异常。
        """
        ...

    def adjusted_bars(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取前复权日线。

        入参：证券序列和正回看交易日数。
        返回值：不晚于信号日的复权行情表。
        异常：请求非法或数据不可用时抛出对应异常。
        """
        ...

    def log_returns(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取对数收益率。

        入参：证券序列和正回看交易日数。
        返回值：不晚于信号日的收益率表。
        异常：请求非法或数据不足时抛出对应异常。
        """
        ...

    def daily_basics(
        self, instruments: Sequence[InstrumentId], lookback_sessions: int
    ) -> pl.LazyFrame:
        """读取日频估值与成交统计。

        入参：证券序列和正回看交易日数。
        返回值：PIT 可见的 ``daily_basic`` 数据。
        异常：请求非法或数据不可用时抛出对应异常。
        """
        ...

    def factor_values(
        self, factor_ids: Sequence[str], instruments: Sequence[InstrumentId]
    ) -> pl.LazyFrame:
        """在当前 Run 内计算并读取因子值。

        入参：因子 ID 和证券序列。
        返回值：绑定信号日和目录身份的因子长表。
        异常：因子未注册或输入不足时抛出对应异常。
        """
        ...

    def industry(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame:
        """读取信号日可见行业状态。

        入参：证券序列。
        返回值：PIT 行业分类表。
        异常：数据不可用时抛出对应异常。
        """
        ...

    def security_status(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame:
        """读取信号日证券状态。

        入参：证券序列。
        返回值：停牌、ST 等 PIT 状态表。
        异常：数据不可用时抛出对应异常。
        """
        ...

    def stock_universe(self) -> pl.LazyFrame:
        """读取信号日动态股票池。

        入参：无。
        返回值：含稳定纳入与排除证据的股票池表。
        异常：股票池输入不可用时抛出对应异常。
        """
        ...


@dataclass(frozen=True, slots=True)
class DecisionContext:
    """绑定信号日、下一执行日、PIT 数据视图和账户状态。

    入参：信号日、下一交易日、只读决策数据和账户视图。
    返回值：冻结的单日策略回调上下文。
    异常：日期次序或依赖对象非法时抛出类型或值错误。
    """

    signal_date: date
    execute_date: date
    data: DecisionData
    account: AccountView

    def __post_init__(self) -> None:
        if type(self.signal_date) is not date or type(self.execute_date) is not date:
            raise TypeError("signal_date and execute_date must be dates")
        if self.execute_date <= self.signal_date:
            raise ValueError("execute_date must follow signal_date")
        if self.data is None:
            raise TypeError("data must be supplied")
        if not isinstance(self.account, AccountView):
            raise TypeError("account must be an AccountView")


@dataclass(frozen=True, slots=True)
class StrategySpec:
    """声明策略标识、频率、数据依赖、因子依赖和冻结参数。

    入参：策略 ID、决策频率、去重后的依赖和 JSON 参数。
    返回值：不可修改的策略规格。
    异常：标识为空或依赖重复时抛出 ``ValueError``。
    """

    strategy_id: str
    frequency: str
    data_dependencies: tuple[DatasetKind, ...]
    factor_dependencies: tuple[str, ...]
    parameters: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        if not self.strategy_id or not self.frequency:
            raise ValueError("strategy_id and frequency must be nonempty")
        if len(set(self.data_dependencies)) != len(self.data_dependencies) or len(
            set(self.factor_dependencies)
        ) != len(self.factor_dependencies):
            raise ValueError("strategy dependencies must be unique")
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))


class Strategy(Protocol):
    """定义所有策略插件必须实现的稳定回调协议。

    入参：实现方接收绑定单日的 ``DecisionContext``。
    返回值：策略规格、预热结果和整数订单序列。
    异常：实现方保留配置、数据和计算异常语义。
    """

    @property
    def spec(self) -> StrategySpec:
        """读取冻结策略规格。

        入参：无。
        返回值：策略身份、依赖和参数。
        异常：无。
        """
        ...

    def warmup(self, ctx: DecisionContext) -> None:
        """在正式决策前建立确定性内部状态。

        入参：当前决策上下文。
        返回值：无。
        异常：预热数据不足时由策略实现抛出对应异常。
        """
        ...

    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]:
        """处理一个交易日事件并生成下一日订单。

        入参：当前决策上下文。
        返回值：整数股数订单序列。
        异常：策略配置或数据不满足契约时抛出对应异常。
        """
        ...


class RebalancePlanner(Protocol):
    """定义目标权重到整数订单的策略侧翻译端口。

    入参：目标权重、账户视图和信号日参考价。
    返回值：尚未执行的整数订单序列。
    异常：价格或申报规则非法时由实现方抛出对应异常。
    """

    def plan(
        self,
        targets: TargetWeights,
        account: AccountView,
        reference_prices: Mapping[InstrumentId, float],
    ) -> Sequence[OrderIntent]:
        """把目标持仓转换成订单差额。

        入参：目标权重、账户视图和参考价映射。
        返回值：确定性排序的订单序列。
        异常：输入不完整或非法时抛出对应异常。
        """
        ...


class WeightTargetStrategy:
    """为权重策略实现目标记忆、差额续单和统一订单翻译。

    入参：构造时接收再平衡端口和目标权重容差。
    返回值：每日回调返回待执行整数订单。
    异常：规划器缺失或容差越界时抛出类型或值错误。
    """

    def __init__(self, planner: RebalancePlanner, *, target_tolerance: float) -> None:
        if planner is None:
            raise TypeError("planner must be supplied")
        if not isfinite(target_tolerance) or not 0 <= target_tolerance <= 0.1:
            raise ValueError("target_tolerance must be in [0, 0.1]")
        self._planner = planner
        self._target_tolerance = target_tolerance
        self._active_weights: Mapping[InstrumentId, float] | None = None

    def warmup(self, ctx: DecisionContext) -> None:
        """执行默认空预热，具体策略可覆盖。

        入参：当前决策上下文。
        返回值：无。
        异常：默认实现不抛出异常。
        """
        del ctx

    def target_weights(self, ctx: DecisionContext) -> TargetWeights | None:
        """计算新目标；无状态变化时返回 ``None``。

        入参：当前决策上下文。
        返回值：新目标权重，或表示继续旧目标的 ``None``。
        异常：基类方法始终抛出 ``NotImplementedError``。
        """
        raise NotImplementedError

    def reference_prices(
        self, ctx: DecisionContext, instruments: Sequence[InstrumentId]
    ) -> Mapping[InstrumentId, float]:
        """返回信号日未复权收盘价，供目标权重转换成整数订单。

        入参：当前上下文和需要估值的证券序列。
        返回值：证券到正数收盘价的映射。
        异常：行情读取失败时保留数据端口异常。
        """
        latest = (
            ctx.data.bars(instruments, 1)
            .collect()
            .sort("trade_date", "instrument_id")
            .group_by("instrument_id")
            .agg(pl.col("close").last())
        )
        return {
            InstrumentId.parse(identifier): float(close)
            for identifier, close in latest.select("instrument_id", "close").iter_rows()
            if close is not None and float(close) > 0
        }

    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]:
        """记录新目标并在未达到容差时持续生成剩余差额订单。

        入参：绑定信号日与账户状态的决策上下文。
        返回值：下一交易日待执行的整数订单序列。
        异常：目标、价格或订单转换不合法时抛出对应异常。
        """
        proposed = self.target_weights(ctx)
        if proposed is not None:
            self._active_weights = proposed.weights
        if self._active_weights is None:
            return ()
        instruments = tuple(
            sorted(
                set(self._active_weights) | set(ctx.account.positions),
                key=InstrumentId.canonical,
            )
        )
        if not instruments:
            self._active_weights = None
            return ()
        prices = self.reference_prices(ctx, instruments)
        current = self._current_weights(ctx.account, prices)
        keys = set(current) | set(self._active_weights)
        if all(
            abs(current.get(key, 0.0) - self._active_weights.get(key, 0.0))
            <= self._target_tolerance
            for key in keys
        ):
            self._active_weights = None
            return ()
        return tuple(
            self._planner.plan(
                TargetWeights(ctx.signal_date, ctx.execute_date, self._active_weights),
                ctx.account,
                prices,
            )
        )

    @staticmethod
    def _current_weights(
        account: AccountView, prices: Mapping[InstrumentId, float]
    ) -> dict[InstrumentId, float]:
        if account.equity_fen <= 0:
            return {}
        return {
            key: quantity * prices[key] * 100 / account.equity_fen
            for key, quantity in account.positions.items()
            if quantity > 0 and key in prices
        }
