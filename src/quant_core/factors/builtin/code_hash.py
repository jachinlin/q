"""Stable source-sensitive identities for bundled factor implementations."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from importlib import resources
from types import MappingProxyType

from quant_core.data.contracts import canonical_json_bytes
from quant_core.factors.base import FactorSpec, thaw_json


def _builtin_source_bytes() -> Mapping[str, bytes]:
    """Read all production sources that affect bundled factor outputs."""
    sources: dict[str, bytes] = {
        "quant_core/data/adjustments.py": resources.files("quant_core.data")
        .joinpath("adjustments.py")
        .read_bytes(),
        "quant_core/factors/base.py": resources.files("quant_core.factors")
        .joinpath("base.py")
        .read_bytes(),
    }
    package = resources.files("quant_core.factors.builtin")
    for resource in package.iterdir():
        if resource.is_file() and resource.name.endswith(".py"):
            sources[f"quant_core/factors/builtin/{resource.name}"] = (
                resource.read_bytes()
            )
    return MappingProxyType(sources)


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
    """Return the cache identity for a bundled factor implementation."""
    return _hash_source_bundle(spec, _builtin_source_bytes())
