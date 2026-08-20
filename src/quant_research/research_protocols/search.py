"""解析严格 YAML 并确定性展开有限候选空间。"""

from __future__ import annotations

import hashlib
import itertools
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import cast

import yaml

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.research_protocols.models import ResearchFamilyConfig

_MAX_YAML_BYTES = 1_048_576
_MAX_VARIANTS = 256


@dataclass(frozen=True, slots=True)
class ExpandedVariant:
    """表示从搜索空间解析出的一个不可变候选组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    variant_id: str
    composition_hash: str
    parameters: Mapping[str, JsonValue]
    config: Mapping[str, JsonValue]


@dataclass(frozen=True, slots=True)
class ResolvedResearchFamily:
    """返回规范研究定义及其确定性候选列表。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    config: ResearchFamilyConfig
    config_hash: str
    normalized: Mapping[str, JsonValue]
    variants: tuple[ExpandedVariant, ...]


class ResearchConfigResolver:
    """解析研究 YAML、校验配置并展开最多 256 个候选。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def resolve_yaml(self, text: str) -> ResolvedResearchFamily:
        """解析内存 YAML，不读取任意用户文件路径。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not isinstance(text, str):
            raise TypeError("research YAML must be text")
        payload = text.encode("utf-8")
        if len(payload) > _MAX_YAML_BYTES:
            raise ValueError("research YAML exceeds the size limit")
        loaded = yaml.safe_load(payload)
        if not isinstance(loaded, dict):
            raise TypeError("research YAML must contain a mapping")
        config = ResearchFamilyConfig.model_validate(loaded)
        return self._resolve_config(config)

    def resolve_normalized(
        self, mapping: Mapping[str, JsonValue]
    ) -> ResolvedResearchFamily:
        """从已登记的规范 JSON 恢复研究定义，用于 Worker 重启。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        config = ResearchFamilyConfig.model_validate_json(canonical_json_bytes(mapping))
        return self._resolve_config(config)

    def _resolve_config(
        self, config: ResearchFamilyConfig
    ) -> ResolvedResearchFamily:
        normalized = cast(dict[str, JsonValue], config.model_dump(mode="json"))
        config_hash = hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()
        variants = self._expand(normalized, config)
        return ResolvedResearchFamily(config, config_hash, normalized, variants)

    def _expand(
        self,
        normalized: dict[str, JsonValue],
        config: ResearchFamilyConfig,
    ) -> tuple[ExpandedVariant, ...]:
        paths = tuple(sorted(config.research_protocol.parameter_search_space))
        choices = tuple(
            config.research_protocol.parameter_search_space[path] for path in paths
        )
        trial_count = 1
        for values in choices:
            trial_count *= len(values)
        if trial_count > _MAX_VARIANTS:
            raise ValueError(
                f"parameter search expands to {trial_count} variants; maximum is {_MAX_VARIANTS}"
            )
        variants: list[ExpandedVariant] = []
        for values in itertools.product(*choices):
            candidate = deepcopy(normalized)
            parameters = dict(zip(paths, values, strict=True))
            for path, value in parameters.items():
                self._set_path(candidate, path, value)
            composition_hash = hashlib.sha256(
                canonical_json_bytes(candidate)
            ).hexdigest()
            variants.append(
                ExpandedVariant(
                    variant_id=f"variant-{composition_hash[:16]}",
                    composition_hash=composition_hash,
                    parameters=dict(sorted(parameters.items())),
                    config=candidate,
                )
            )
        return tuple(sorted(variants, key=lambda item: item.variant_id))

    @staticmethod
    def _set_path(root: dict[str, JsonValue], path: str, value: JsonValue) -> None:
        current: dict[str, JsonValue] = root
        parts = path.split(".")
        if parts[0] == "research_protocol":
            raise ValueError("parameter search cannot modify research_protocol")
        for part in parts[:-1]:
            nested = current.get(part)
            if not isinstance(nested, dict):
                raise TypeError(f"search path does not resolve to a mapping: {path}")
            current = cast(dict[str, JsonValue], nested)
        leaf = parts[-1]
        if leaf not in current:
            raise ValueError(f"search path does not exist: {path}")
        current[leaf] = value
