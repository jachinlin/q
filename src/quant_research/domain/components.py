"""定义可替换研究组件的能力描述与组装校验。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_research.data.contracts import JsonValue, canonical_json_bytes


class SignalKind(StrEnum):
    """定义互不混用的策略信号种类。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    CROSS_SECTIONAL_SCORE = "CROSS_SECTIONAL_SCORE"
    DIRECTIONAL = "DIRECTIONAL"
    ALLOCATION = "ALLOCATION"


@dataclass(frozen=True, slots=True)
class ComponentDescriptor:
    """声明组件身份、数据要求、输入输出能力和配置 Schema。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    component_id: str
    component_hash: str
    input_capabilities: tuple[str, ...]
    output_capabilities: tuple[str, ...]
    required_datasets: tuple[str, ...]
    required_fields: tuple[str, ...]
    supported_signal_kinds: tuple[SignalKind, ...]
    supports_batch: bool
    determinism_contract: str
    config_schema: dict[str, JsonValue]

    def __post_init__(self) -> None:
        """校验能力描述可以稳定发布到 CLI 和 Dashboard。"""
        if not self.component_id:
            raise ValueError("component_id must not be empty")
        if len(self.component_hash) != 64:
            raise ValueError("component_hash must be a SHA-256 digest")
        if not self.supports_batch:
            raise ValueError("research components must support batch execution")
        if not self.determinism_contract:
            raise ValueError("determinism_contract must not be empty")
        canonical_json_bytes(self.config_schema)
        for values in (
            self.input_capabilities,
            self.output_capabilities,
            self.required_datasets,
            self.required_fields,
        ):
            if tuple(sorted(set(values))) != values:
                raise ValueError(
                    "component descriptor tuples must be unique and sorted"
                )


class CompositionValidator:
    """在运行前验证组件能力闭包和信号类型兼容性。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def validate(
        self,
        components: tuple[ComponentDescriptor, ...],
        *,
        signal_kind: SignalKind,
    ) -> None:
        """拒绝缺少上游能力或不支持信号类型的组件组合。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        available: set[str] = set()
        for component in components:
            missing = set(component.input_capabilities) - available
            if missing:
                raise ValueError(
                    f"component {component.component_id} lacks capabilities: "
                    + ", ".join(sorted(missing))
                )
            if (
                component.supported_signal_kinds
                and signal_kind not in component.supported_signal_kinds
            ):
                raise ValueError(
                    f"component {component.component_id} does not support {signal_kind.value}"
                )
            available.update(component.output_capabilities)
