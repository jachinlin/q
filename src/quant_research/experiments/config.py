"""严格解析并规范化纯策略实验和派生 Run YAML。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.experiments.models import (
    ExperimentDefinition,
    StrategyBacktestRunConfig,
)


@dataclass(frozen=True, slots=True)
class ResolvedExperiment:
    """保存严格模型、规范 JSON 和内容哈希。

    入参：实验定义、规范字节和 SHA-256。返回值：冻结解析结果。异常：构造阶段不执行额外 I/O。
    """

    definition: ExperimentDefinition
    normalized: bytes
    config_hash: str


@dataclass(frozen=True, slots=True)
class ResolvedRun:
    """保存严格派生 Run 模型、规范 JSON 和内容哈希。

    入参：Run 配置、规范字节和 SHA-256。返回值：冻结解析结果。异常：构造阶段不执行额外 I/O。
    """

    config: StrategyBacktestRunConfig
    normalized: bytes
    config_hash: str


class ExperimentConfigParser:
    """从文本或受信文件解析无兼容分支的目标 Schema。

    入参：各解析方法接收 YAML 文本或明确文件路径。返回值：规范模型和内容哈希。异常：YAML 或严格 Schema 非法时抛出 ``ValueError``。
    """

    def parse_experiment(self, text: str) -> ResolvedExperiment:
        """解析实验 YAML，并返回稳定规范化结果。

        入参：完整实验 YAML 文本。返回值：``ResolvedExperiment``。异常：语法、字段或领域约束非法时抛出值错误。
        """
        raw = self._mapping(text)
        try:
            # YAML 是外部文本边界；枚举、列表和 ISO 日期在此处规范化为
            # 领域模型的严格、冻结表示。模型自身仍禁止额外字段和运行期赋值。
            model = ExperimentDefinition.model_validate(raw, strict=False)
        except ValidationError as error:
            raise ValueError(self._message(error)) from error
        normalized = canonical_json_bytes(model.model_dump(mode="json"))
        return ResolvedExperiment(
            model, normalized, hashlib.sha256(normalized).hexdigest()
        )

    def parse_run(self, text: str) -> ResolvedRun:
        """解析派生 Run YAML，并返回稳定规范化结果。

        入参：不含实验外层字段的 Run YAML 文本。返回值：``ResolvedRun``。异常：语法、判别 kind 或字段非法时抛出值错误。
        """
        raw = self._mapping(text)
        try:
            model = StrategyBacktestRunConfig.model_validate(raw, strict=False)
        except ValidationError as error:
            raise ValueError(self._message(error)) from error
        normalized = canonical_json_bytes(model.model_dump(mode="json"))
        return ResolvedRun(model, normalized, hashlib.sha256(normalized).hexdigest())

    def parse_experiment_file(self, path: Path) -> ResolvedExperiment:
        """读取明确文件并解析实验配置。

        入参：受调用方控制的 YAML 文件路径。返回值：规范实验结果。异常：文件读取或配置校验失败时抛出对应异常。
        """
        return self.parse_experiment(path.read_text(encoding="utf-8"))

    def parse_run_file(self, path: Path) -> ResolvedRun:
        """读取明确文件并解析派生 Run 配置。

        入参：受调用方控制的 Run YAML 文件路径。返回值：规范 Run 结果。异常：文件读取或配置校验失败时抛出对应异常。
        """
        return self.parse_run(path.read_text(encoding="utf-8"))

    @staticmethod
    def _mapping(text: str) -> dict[str, JsonValue]:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("YAML must not be empty")
        try:
            value = yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise ValueError("YAML is invalid") from error
        if not isinstance(value, dict) or any(
            not isinstance(key, str) for key in value
        ):
            raise ValueError("YAML root must be a string-keyed mapping")
        return value

    @staticmethod
    def _message(error: ValidationError) -> str:
        first = error.errors(include_url=False)[0]
        location = ".".join(str(value) for value in first["loc"])
        return f"{location}: {first['msg']}" if location else str(first["msg"])


__all__ = ["ExperimentConfigParser", "ResolvedExperiment", "ResolvedRun"]
