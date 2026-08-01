"""Canonical factor execution identity for composite cache addressing."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.factors.base import (
    FactorSpec,
    canonical_factor_ref,
    thaw_json,
    validate_sha256,
)

_DESCRIPTOR_FIELDS = {"plan", "requested_refs"}
_NODE_FIELDS = {"code_hash", "factor_ref", "spec"}
_SPEC_FIELDS = {
    "dependencies",
    "direction",
    "factor_id",
    "frequency",
    "lookback_sessions",
    "parameters",
    "version",
}


@dataclass(frozen=True, slots=True)
class FactorExecutionNode:
    """One planned factor's complete logical and implementation identity."""

    spec: FactorSpec
    code_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.spec, FactorSpec):
            raise TypeError("execution node spec must be a FactorSpec")
        validate_sha256(self.code_hash, "factor execution code_hash")

    @property
    def factor_ref(self) -> str:
        return self.spec.canonical_ref

    def json_value(self) -> dict[str, JsonValue]:
        parameters = thaw_json(self.spec.parameters)
        if not isinstance(parameters, Mapping):
            raise TypeError("factor parameters must be a mapping")
        return {
            "code_hash": self.code_hash,
            "factor_ref": self.factor_ref,
            "spec": {
                "dependencies": list(self.spec.dependencies),
                "direction": self.spec.direction,
                "factor_id": self.spec.factor_id,
                "frequency": self.spec.frequency,
                "lookback_sessions": self.spec.lookback_sessions,
                "parameters": parameters,
                "version": self.spec.version,
            },
        }


@dataclass(frozen=True, slots=True)
class FactorExecutionDescriptor:
    """Ordered requested roots and their complete deterministic dependency DAG."""

    requested_refs: tuple[str, ...]
    plan: tuple[FactorExecutionNode, ...]

    def __post_init__(self) -> None:
        requested = tuple(canonical_factor_ref(item) for item in self.requested_refs)
        if requested != tuple(sorted(set(requested))):
            raise ValueError("execution requested refs must be unique and ordered")
        refs = tuple(node.factor_ref for node in self.plan)
        if len(set(refs)) != len(refs):
            raise ValueError("execution plan contains duplicate factors")
        positions = {factor_ref: index for index, factor_ref in enumerate(refs)}
        for index, node in enumerate(self.plan):
            for dependency in node.spec.dependencies:
                if dependency not in positions or positions[dependency] >= index:
                    raise ValueError(
                        "execution plan dependencies must precede their consumers"
                    )
        if not set(requested).issubset(positions):
            raise ValueError("execution plan does not contain every requested factor")
        reachable: set[str] = set()

        def visit(factor_ref: str) -> None:
            if factor_ref in reachable:
                return
            reachable.add(factor_ref)
            node = self.plan[positions[factor_ref]]
            for dependency in node.spec.dependencies:
                visit(dependency)

        for factor_ref in requested:
            visit(factor_ref)
        if reachable != set(refs):
            raise ValueError(
                "execution plan contains factors outside requested closure"
            )
        object.__setattr__(self, "requested_refs", requested)

    @property
    def content_hash(self) -> str:
        """Hash the complete canonical descriptor into a composite key input."""
        return hashlib.sha256(canonical_json_bytes(self.json_value())).hexdigest()

    def json_value(self) -> dict[str, JsonValue]:
        return {
            "plan": [node.json_value() for node in self.plan],
            "requested_refs": list(self.requested_refs),
        }

    @classmethod
    def from_json_value(cls, value: object) -> FactorExecutionDescriptor:
        """Parse an exact-field descriptor from a composite manifest."""
        if not isinstance(value, Mapping) or set(value) != _DESCRIPTOR_FIELDS:
            raise ValueError("execution descriptor fields are invalid")
        requested_refs = _string_tuple(value["requested_refs"], "requested_refs")
        raw_plan = value["plan"]
        if not isinstance(raw_plan, list):
            raise TypeError("execution descriptor plan must be a list")
        nodes: list[FactorExecutionNode] = []
        for raw_node in raw_plan:
            if not isinstance(raw_node, Mapping) or set(raw_node) != _NODE_FIELDS:
                raise ValueError("execution descriptor node fields are invalid")
            raw_spec = raw_node["spec"]
            if not isinstance(raw_spec, Mapping) or set(raw_spec) != _SPEC_FIELDS:
                raise ValueError("execution descriptor spec fields are invalid")
            dependencies = _string_tuple(raw_spec["dependencies"], "dependencies")
            parameters = raw_spec["parameters"]
            if not isinstance(parameters, Mapping):
                raise TypeError("execution descriptor parameters must be a mapping")
            spec = FactorSpec(
                factor_id=_string(raw_spec, "factor_id"),
                version=_string(raw_spec, "version"),
                frequency=_string(raw_spec, "frequency"),
                lookback_sessions=_integer(raw_spec, "lookback_sessions"),
                dependencies=dependencies,
                direction=_integer(raw_spec, "direction"),
                parameters=cast(Mapping[str, JsonValue], parameters),
            )
            if _string(raw_node, "factor_ref") != spec.canonical_ref:
                raise ValueError("execution descriptor factor_ref differs from spec")
            nodes.append(
                FactorExecutionNode(
                    spec=spec,
                    code_hash=validate_sha256(
                        _string(raw_node, "code_hash"),
                        "factor execution code_hash",
                    ),
                )
            )
        return cls(requested_refs=requested_refs, plan=tuple(nodes))


def _string(mapping: Mapping[str, object], field: str) -> str:
    value = mapping[field]
    if not isinstance(value, str):
        raise TypeError(f"execution descriptor {field} must be a string")
    return value


def _integer(mapping: Mapping[str, object], field: str) -> int:
    value = mapping[field]
    if type(value) is not int:
        raise TypeError(f"execution descriptor {field} must be an integer")
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TypeError(f"execution descriptor {field} must be a string list")
    return cast(tuple[str, ...], tuple(value))


__all__ = ["FactorExecutionDescriptor", "FactorExecutionNode"]
