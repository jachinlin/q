"""严格解析独立因子研究 YAML 并生成确定性身份。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from quant_research.data.contracts import canonical_json_bytes
from quant_research.factor_studies.models import FactorStudyDefinition


@dataclass(frozen=True, slots=True)
class ResolvedFactorStudy:
    """保存解析结果。入参：定义、规范字节和哈希。返回值：不可变结果。异常：字段缺失时类型错误。"""

    definition: FactorStudyDefinition
    normalized: bytes
    config_hash: str


class FactorStudyConfigParser:
    """解析最终配置。入参：YAML 文本或受信路径。返回值：规范结果。异常：语法或字段非法时抛出值错误。"""

    def parse(self, text: str) -> ResolvedFactorStudy:
        """解析因子研究 YAML。

        入参：非空 YAML 文本。返回值：规范定义和哈希。异常：语法或 Schema 非法时抛出值错误。
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("YAML must not be empty")
        try:
            value = yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise ValueError(str(error)) from error
        if not isinstance(value, dict) or not all(
            isinstance(key, str) for key in value
        ):
            raise ValueError("YAML root must be a string-keyed mapping")
        try:
            definition = FactorStudyDefinition.model_validate(value, strict=False)
        except ValidationError as error:
            detail = error.errors(include_url=False)[0]
            path = ".".join(str(item) for item in detail["loc"])
            raise ValueError(f"{path}: {detail['msg']}") from error
        normalized = canonical_json_bytes(definition.model_dump(mode="json"))
        return ResolvedFactorStudy(
            definition=definition,
            normalized=normalized,
            config_hash=hashlib.sha256(normalized).hexdigest(),
        )

    def parse_file(self, path: Path) -> ResolvedFactorStudy:
        """读取并解析配置。入参：受信路径。返回值：规范结果。异常：读取失败或配置非法时抛出异常。"""
        return self.parse(path.read_text(encoding="utf-8"))


__all__ = ["FactorStudyConfigParser", "ResolvedFactorStudy"]
