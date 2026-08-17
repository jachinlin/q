"""提供组合构建与再平衡相关的公开模型、协议与处理流程。"""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from math import floor, isfinite

from quant_research.backtest.rulebook import InstrumentTradingProfile, Side
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.constructor import TargetPortfolio


class OrderSide(StrEnum):
    """定义 ``OrderSide`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """表示由目标组合差异产生但尚未撮合的买入或卖出委托。

    入参：
        instrument_id：目标证券标识，类型为 ``InstrumentId``。
        side：买卖方向。
        quantity：数量。
        reason_code：说明成交、拒绝或排除原因的稳定机器码。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    instrument_id: InstrumentId
    side: OrderSide
    quantity: int
    reason_code: str


@dataclass(frozen=True, slots=True)
class RebalancePlan:
    """汇总调仓日生成的确定性委托意图序列。

    入参：
        intents：参与本次处理的委托意图集合；调用方不得依赖未声明的顺序。
        projected_cash_fen：预计``cash``分币金额。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    intents: tuple[OrderIntent, ...]
    projected_cash_fen: int


class RebalancePlanner:
    """表示目标组合流程中的调仓``planner``及其业务不变量。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Produce non-executing order intentions from a target portfolio.
    """

    def plan(
        self,
        target: TargetPortfolio,
        current_positions: Mapping[InstrumentId, int],
        cash_fen: int,
        execution_prices: Mapping[InstrumentId, float],
        trading_profiles: Mapping[InstrumentId, InstrumentTradingProfile],
    ) -> RebalancePlan:
        """生成不执行交易的目标组合。

        入参：
            target：目标组合。
            current_positions：参与本次处理的当前值持仓集合；调用方不得依赖未声明的顺序。
            cash_fen：账户可用现金，采用整数分避免浮点货币误差。
            execution_prices：参与本次处理的成交执行``prices``；调用方不得依赖未声明的顺序。
            trading_profiles：各证券在执行日生效的不可变交易画像。
        返回值：
            返回调仓计划（``RebalancePlan``）。
        异常：
            无。
        """
        _RebalanceSupport._validate_cash(cash_fen)
        _RebalanceSupport._validate_positions(current_positions)
        target_weights = _RebalanceSupport._target_weights(target)
        identifiers = current_positions.keys() | target_weights.keys()
        prices = {
            instrument_id: _RebalanceSupport._price(instrument_id, execution_prices)
            for instrument_id in identifiers
        }
        profiles = {
            instrument_id: _RebalanceSupport._profile(instrument_id, trading_profiles)
            for instrument_id in identifiers
        }
        total_equity_fen = cash_fen + sum(
            _RebalanceSupport._gross_value_fen(prices[instrument_id], quantity)
            for instrument_id, quantity in current_positions.items()
        )
        target_quantities = {
            instrument_id: _RebalanceSupport._target_quantity(
                total_equity_fen,
                target_weights.get(instrument_id, 0.0),
                prices[instrument_id],
            )
            for instrument_id in identifiers
        }
        cash_after_sales = cash_fen
        intents: list[OrderIntent] = []
        for instrument_id in sorted(identifiers, key=InstrumentId.canonical):
            current_quantity = current_positions.get(instrument_id, 0)
            sell_quantity = profiles[instrument_id].normalize_quantity(
                Side.SELL,
                max(current_quantity - target_quantities[instrument_id], 0),
                position_quantity=current_quantity,
            )
            if sell_quantity > 0:
                intents.append(
                    OrderIntent(
                        instrument_id, OrderSide.SELL, sell_quantity, "TARGET_REBALANCE"
                    )
                )
                cash_after_sales += _RebalanceSupport._gross_value_fen(
                    prices[instrument_id], sell_quantity
                )
        projected_cash = cash_after_sales
        for position in target.positions:
            instrument_id = position.instrument_id
            desired_quantity = profiles[instrument_id].normalize_quantity(
                Side.BUY,
                max(
                    target_quantities[instrument_id]
                    - current_positions.get(instrument_id, 0),
                    0,
                ),
            )
            affordable_quantity = profiles[instrument_id].normalize_quantity(
                Side.BUY,
                floor(Decimal(projected_cash) / (prices[instrument_id] * Decimal(100))),
            )
            buy_quantity = min(desired_quantity, affordable_quantity)
            if buy_quantity > 0:
                intents.append(
                    OrderIntent(
                        instrument_id, OrderSide.BUY, buy_quantity, "TARGET_REBALANCE"
                    )
                )
                projected_cash -= _RebalanceSupport._gross_value_fen(
                    prices[instrument_id], buy_quantity
                )
        return RebalancePlan(tuple(intents), projected_cash)


class _RebalanceSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_cash(cash_fen: int) -> None:
        if not isinstance(cash_fen, int) or isinstance(cash_fen, bool) or cash_fen < 0:
            raise ValueError("cash_fen must be a nonnegative integer")

    @staticmethod
    def _validate_positions(current_positions: Mapping[InstrumentId, int]) -> None:
        for instrument_id, quantity in current_positions.items():
            if not isinstance(instrument_id, InstrumentId):
                raise TypeError("current_positions keys must be InstrumentId")
            if (
                not isinstance(quantity, int)
                or isinstance(quantity, bool)
                or quantity < 0
            ):
                raise ValueError("current_positions must contain nonnegative integers")

    @staticmethod
    def _target_weights(target: TargetPortfolio) -> dict[InstrumentId, float]:
        weights: dict[InstrumentId, float] = {}
        for position in target.positions:
            if position.instrument_id in weights:
                raise ValueError("target positions must be unique")
            if not isfinite(position.target_weight) or position.target_weight < 0:
                raise ValueError("target weights must be finite and nonnegative")
            weights[position.instrument_id] = position.target_weight
        return weights

    @staticmethod
    def _price(
        instrument_id: InstrumentId, execution_prices: Mapping[InstrumentId, float]
    ) -> Decimal:
        try:
            price = execution_prices[instrument_id]
        except KeyError as error:
            raise ValueError(
                f"missing price for {instrument_id.canonical()}"
            ) from error
        if (
            not isinstance(price, (float, int))
            or isinstance(price, bool)
            or not isfinite(price)
            or price <= 0
        ):
            raise ValueError("price must be finite and positive")
        return Decimal(str(price))

    @staticmethod
    def _profile(
        instrument_id: InstrumentId,
        profiles: Mapping[InstrumentId, InstrumentTradingProfile],
    ) -> InstrumentTradingProfile:
        try:
            profile = profiles[instrument_id]
        except KeyError as error:
            raise ValueError(
                f"missing trading profile for {instrument_id.canonical()}"
            ) from error
        if not isinstance(profile, InstrumentTradingProfile):
            raise TypeError("trading profile mapping contains an invalid value")
        return profile

    @staticmethod
    def _target_quantity(
        total_equity_fen: int,
        target_weight: float,
        price: Decimal,
    ) -> int:
        target_value_fen = floor(total_equity_fen * target_weight)
        return floor(Decimal(target_value_fen) / (price * Decimal(100)))

    @staticmethod
    def _gross_value_fen(price: Decimal, quantity: int) -> int:
        if type(quantity) is not int or quantity < 0:
            raise ValueError("quantity must be a nonnegative integer")
        return int(
            (price * quantity * Decimal(100)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
