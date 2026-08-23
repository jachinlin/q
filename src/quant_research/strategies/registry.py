"""提供无接口层依赖的确定性策略插件注册表。"""

from collections.abc import Callable, Mapping

from quant_research.data.contracts import JsonValue
from quant_research.strategies.base import Strategy

StrategyFactory = Callable[[Mapping[str, JsonValue]], Strategy]


class StrategyRegistry:
    """登记策略工厂并按唯一 ``strategy_id`` 构造策略。

    入参：构造无需参数，登记时接收工厂和策略 ID。返回值：新策略实例或稳定 ID 列表。异常：重复、未知或工厂身份不符时抛出错误。
    """

    def __init__(self) -> None:
        self._factories: dict[str, StrategyFactory] = {}

    def register(self, factory: StrategyFactory, *, strategy_id: str) -> None:
        """登记一个策略工厂；重复标识立即失败。

        入参：可调用工厂和非空策略 ID。返回值：无。异常：输入非法或 ID 重复时抛出类型或值错误。
        """
        if not callable(factory):
            raise TypeError("factory must be callable")
        if not isinstance(strategy_id, str) or not strategy_id.strip():
            raise ValueError("strategy_id must be nonempty")
        if strategy_id in self._factories:
            raise ValueError(f"strategy already registered: {strategy_id}")
        self._factories[strategy_id] = factory

    def build(self, strategy_id: str, params: Mapping[str, JsonValue]) -> Strategy:
        """按标识和严格参数构造新策略实例。

        入参：策略 ID 和 JSON 参数映射。返回值：全新策略实例。异常：策略未知、参数非法或工厂返回错误 ID 时抛出值错误。
        """
        try:
            factory = self._factories[strategy_id]
        except KeyError as error:
            raise ValueError(f"unknown strategy: {strategy_id}") from error
        strategy = factory(params)
        if strategy.spec.strategy_id != strategy_id:
            raise ValueError("strategy factory returned a mismatched strategy_id")
        return strategy

    def validate(self, strategy_id: str, params: Mapping[str, JsonValue]) -> None:
        """校验标识和参数能够确定性构造已登记策略。

        入参：策略 ID 和 JSON 参数映射。返回值：配置有效时无返回值。异常：未知策略、未知字段或领域参数非法时抛出错误。
        """
        self.build(strategy_id, params)

    def strategy_ids(self) -> tuple[str, ...]:
        """返回稳定排序的全部策略标识。

        入参：无。返回值：策略 ID 元组。异常：无。
        """
        return tuple(sorted(self._factories))

    @classmethod
    def builtins(
        cls, *, commission_bps: float, commission_minimum_fen: int
    ) -> "StrategyRegistry":
        """创建登记股票多因子、ETF 轮动和双均线的内置注册表。

        入参：唯一交易规则解析出的佣金基点和最低佣金分值。
        返回值：已登记三个内置策略的注册表。
        异常：费率非法、内置 ID 重复或策略构造失败时抛出值错误。
        """
        from quant_research.portfolio.rebalance import RebalancePlanner
        from quant_research.strategies.dual_ma import DualMAConfig, DualMATrendStrategy
        from quant_research.strategies.etf_rotation import (
            EtfRotationConfig,
            EtfRotationStrategy,
        )
        from quant_research.strategies.multifactor import (
            MultifactorConfig,
            MultifactorStrategy,
        )

        registry = cls()
        registry.register(
            lambda value: MultifactorStrategy(
                MultifactorConfig.from_mapping(value),
                RebalancePlanner(),
                commission_bps=commission_bps,
                commission_minimum_fen=commission_minimum_fen,
            ),
            strategy_id="stock_multifactor",
        )
        registry.register(
            lambda value: EtfRotationStrategy(
                EtfRotationConfig.from_mapping(value),
                RebalancePlanner(),
                commission_bps=commission_bps,
                commission_minimum_fen=commission_minimum_fen,
            ),
            strategy_id="etf_rotation",
        )
        registry.register(
            lambda value: DualMATrendStrategy(
                DualMAConfig.from_mapping(value), RebalancePlanner()
            ),
            strategy_id="dual_ma_trend",
        )
        return registry
