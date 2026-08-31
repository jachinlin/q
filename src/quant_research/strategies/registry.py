"""提供无接口层依赖的确定性策略插件注册表与说明资源契约。"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import resources

from quant_research.data.contracts import JsonValue
from quant_research.strategies.base import Strategy

StrategyFactory = Callable[[Mapping[str, JsonValue]], Strategy]


@dataclass(frozen=True, slots=True)
class StrategyProfile:
    """描述策略在目录与 Dashboard 中展示的结构性说明。

    入参：策略 ID、展示名称、摘要和完整 Markdown。
    返回值：冻结且可确定性序列化的策略说明。
    异常：字段为空、Markdown 标题或摘要不满足约定时抛出值错误。
    """

    strategy_id: str
    display_name: str
    summary: str
    documentation_markdown: str

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.strategy_id, "strategy_id"),
            (self.display_name, "display_name"),
            (self.summary, "summary"),
            (self.documentation_markdown, "documentation_markdown"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be nonempty")

    @classmethod
    def from_package(cls, *, strategy_id: str, package: str) -> "StrategyProfile":
        """从可信策略包的 README 创建说明。

        入参：非空策略 ID 和可导入的策略包名。
        返回值：由一级标题、首段摘要和全文构造的冻结说明。
        异常：包或 README 不存在、编码错误、标题或摘要缺失时传播明确错误。
        """
        markdown = (
            resources.files(package)
            .joinpath("README.md")
            .read_text(encoding="utf-8")
            .replace("\r\n", "\n")
        )
        return cls.from_markdown(strategy_id=strategy_id, markdown=markdown)

    @classmethod
    def from_markdown(
        cls, *, strategy_id: str, markdown: str
    ) -> "StrategyProfile":
        """从统一模板 Markdown 创建策略说明。

        入参：策略 ID 与 UTF-8 解码后的 Markdown。返回值：解析出的冻结说明。异常：标题或首段摘要缺失时抛出值错误。
        """
        lines = markdown.splitlines()
        first = next(
            (index for index, line in enumerate(lines) if line.strip()), None
        )
        if first is None or not lines[first].startswith("# "):
            raise ValueError(f"strategy README must start with H1: {strategy_id}")
        display_name = lines[first][2:].strip()
        summary_lines: list[str] = []
        for line in lines[first + 1 :]:
            stripped = line.strip()
            if not stripped:
                if summary_lines:
                    break
                continue
            if stripped.startswith("#"):
                break
            summary_lines.append(stripped)
        summary = " ".join(summary_lines)
        if not display_name or not summary:
            raise ValueError(
                f"strategy README must provide title and summary: {strategy_id}"
            )
        return cls(
            strategy_id=strategy_id,
            display_name=display_name,
            summary=summary,
            documentation_markdown=markdown,
        )


class StrategyRegistry:
    """登记策略工厂并按唯一 ``strategy_id`` 构造策略。

    入参：构造无需参数，登记时接收工厂和策略 ID。返回值：新策略实例或稳定 ID 列表。异常：重复、未知或工厂身份不符时抛出错误。
    """

    def __init__(self) -> None:
        self._factories: dict[str, StrategyFactory] = {}
        self._profiles: dict[str, StrategyProfile] = {}

    def register(self, factory: StrategyFactory, *, profile: StrategyProfile) -> None:
        """登记一个策略工厂；重复标识立即失败。

        入参：可调用工厂和同一策略的结构性说明。返回值：无。异常：输入非法或 ID 重复时抛出类型或值错误。
        """
        if not callable(factory):
            raise TypeError("factory must be callable")
        if not isinstance(profile, StrategyProfile):
            raise TypeError("profile must be a StrategyProfile")
        strategy_id = profile.strategy_id
        if strategy_id in self._factories:
            raise ValueError(f"strategy already registered: {strategy_id}")
        self._factories[strategy_id] = factory
        self._profiles[strategy_id] = profile

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

    def profiles(self) -> tuple[StrategyProfile, ...]:
        """返回按策略 ID 稳定排序的全部结构性说明。

        入参：无。返回值：冻结说明元组。异常：无。
        """
        return tuple(self._profiles[item] for item in sorted(self._profiles))

    def profile(self, strategy_id: str) -> StrategyProfile:
        """读取一个已注册策略的结构性说明。

        入参：策略 ID。返回值：对应冻结说明。异常：策略未知时抛出值错误。
        """
        try:
            return self._profiles[strategy_id]
        except KeyError as error:
            raise ValueError(f"unknown strategy: {strategy_id}") from error

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
        from quant_research.strategies.dual_ma_trend import (
            DualMAConfig,
            DualMATrendStrategy,
        )
        from quant_research.strategies.etf_rotation import (
            EtfRotationConfig,
            EtfRotationStrategy,
        )
        from quant_research.strategies.stock_multifactor import (
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
            profile=StrategyProfile.from_package(
                strategy_id="stock_multifactor",
                package="quant_research.strategies.stock_multifactor",
            ),
        )
        registry.register(
            lambda value: EtfRotationStrategy(
                EtfRotationConfig.from_mapping(value),
                RebalancePlanner(),
                commission_bps=commission_bps,
                commission_minimum_fen=commission_minimum_fen,
            ),
            profile=StrategyProfile.from_package(
                strategy_id="etf_rotation",
                package="quant_research.strategies.etf_rotation",
            ),
        )
        registry.register(
            lambda value: DualMATrendStrategy(
                DualMAConfig.from_mapping(value), RebalancePlanner()
            ),
            profile=StrategyProfile.from_package(
                strategy_id="dual_ma_trend",
                package="quant_research.strategies.dual_ma_trend",
            ),
        )
        return registry


__all__ = ["StrategyFactory", "StrategyProfile", "StrategyRegistry"]
