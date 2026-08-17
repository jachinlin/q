"""提供内置实现与代码哈希相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from importlib import resources
from types import MappingProxyType

from quant_research.data.contracts import canonical_json_bytes
from quant_research.factors.base import FactorSpec, thaw_json


class _CodeHashSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _builtin_source_bytes() -> Mapping[str, bytes]:
        """Read all production sources that affect bundled factor outputs."""
        sources: dict[str, bytes] = {
            "quant_research/data/adjustments.py": resources.files("quant_research.data")
            .joinpath("adjustments.py")
            .read_bytes(),
            "quant_research/factors/base.py": resources.files("quant_research.factors")
            .joinpath("base.py")
            .read_bytes(),
        }
        package = resources.files("quant_research.factors.builtin")
        for resource in package.iterdir():
            if resource.is_file() and resource.name.endswith(".py"):
                sources[f"quant_research/factors/builtin/{resource.name}"] = (
                    resource.read_bytes()
                )
        return MappingProxyType(sources)

    @staticmethod
    def _hash_source_bundle(spec: FactorSpec, sources: Mapping[str, bytes]) -> str:
        """Hash a deterministic source bundle and the factor's logical contract."""
        digest = hashlib.sha256()
        for logical_name, payload in sorted(sources.items()):
            digest.update(logical_name.encode("utf-8"))
            digest.update(b"\0")
            digest.update(payload)
            digest.update(b"\0")
        digest.update(spec.canonical_ref.encode("utf-8"))
        digest.update(canonical_json_bytes(thaw_json(spec.parameters)))
        return digest.hexdigest()


def builtin_source_hash(spec: FactorSpec) -> str:
    """处理因子计算中的``builtin``数据来源哈希；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        spec：不可变规格。
    返回值：
        返回数据来源哈希（``str``）。
    异常：
        无。
    Return the audit identity for a bundled factor implementation.
    """
    return _CodeHashSupport._hash_source_bundle(
        spec, _CodeHashSupport._builtin_source_bytes()
    )
