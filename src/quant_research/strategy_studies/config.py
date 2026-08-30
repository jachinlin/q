"""严格解析并规范化单一策略研究 YAML。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.strategy_studies.models import StrategyStudyDefinition


@dataclass(frozen=True, slots=True)
class ResolvedStrategyStudy:
    """保存解析结果。入参：定义、规范字节和哈希。返回值：不可变结果。异常：构造不主动抛出异常。"""

    definition: StrategyStudyDefinition
    normalized: bytes
    config_hash: str


class StrategyStudyConfigParser:
    """解析策略研究 YAML。入参：无构造参数。返回值：解析器实例。异常：构造不主动抛出异常。"""

    def parse(self, text: str) -> ResolvedStrategyStudy:
        """解析策略研究 YAML。入参：YAML 文本。返回值：规范结果。异常：语法或字段非法时抛出值错误。"""

        raw = self._mapping(text)
        try:
            model = StrategyStudyDefinition.model_validate(raw, strict=False)
        except ValidationError as error:
            raise ValueError(self._message(error)) from error
        normalized = canonical_json_bytes(model.model_dump(mode="json"))
        return ResolvedStrategyStudy(
            model, normalized, hashlib.sha256(normalized).hexdigest()
        )

    def parse_file(self, path: Path) -> ResolvedStrategyStudy:
        """读取并解析配置。入参：受信文件路径。返回值：规范结果。异常：读取或配置非法时传播异常。"""

        return self.parse(path.read_text(encoding="utf-8"))

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


__all__ = ["ResolvedStrategyStudy", "StrategyStudyConfigParser"]
