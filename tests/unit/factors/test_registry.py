"""Tests for versioned factor contracts and deterministic dependency planning."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError
from datetime import date
from typing import cast

import polars as pl
import pytest

from quant_core.data.contracts import JsonValue
from quant_core.domain.identifiers import SnapshotId
from quant_core.factors import FactorContext, FactorRegistry, FactorSpec


class StubFactor:
    """A factor whose output is irrelevant to registry-only tests."""

    def __init__(self, spec: FactorSpec) -> None:
        self._spec = spec

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        raise AssertionError(f"registry must not compute {self.spec.factor_id}: {ctx}")


def make_spec(
    factor_id: str,
    *,
    version: str = "1.0.0",
    dependencies: tuple[str, ...] = (),
    parameters: dict[str, JsonValue] | None = None,
) -> FactorSpec:
    return FactorSpec(
        factor_id=factor_id,
        version=version,
        frequency="daily",
        lookback_sessions=20,
        dependencies=dependencies,
        direction=1,
        parameters={} if parameters is None else parameters,
    )


def code_hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def test_factor_spec_copies_nested_parameters_into_immutable_canonical_values() -> None:
    """Caller mutation must not change a registered factor's logical identity."""
    nested: dict[str, JsonValue] = {
        "window": 20,
        "winsorize": {"limits": [0.01, 0.99]},
    }

    spec = make_spec("momentum", parameters=nested)
    cast(dict[str, JsonValue], nested["winsorize"])["limits"] = [0.2, 0.8]

    assert spec.parameters == {
        "window": 20,
        "winsorize": {"limits": (0.01, 0.99)},
    }
    with pytest.raises(TypeError):
        cast(dict[str, JsonValue], spec.parameters)["window"] = 21
    with pytest.raises(FrozenInstanceError):
        spec.direction = -1  # type: ignore[misc]


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"factor_id": ""}, "factor_id"),
        ({"factor_id": "bad@id"}, "factor_id"),
        ({"version": ""}, "version"),
        ({"version": "bad@version"}, "version"),
        ({"frequency": ""}, "frequency"),
        ({"lookback_sessions": -1}, "lookback_sessions"),
        ({"lookback_sessions": True}, "lookback_sessions"),
        ({"direction": 0}, "direction"),
        ({"direction": True}, "direction"),
        ({"dependencies": ("price@1", "price@1")}, "duplicate"),
        ({"dependencies": ("price",)}, "factor_id@version"),
        ({"parameters": {"threshold": float("nan")}}, "serializable"),
        ({"parameters": {"threshold": float("inf")}}, "serializable"),
    ],
)
def test_factor_spec_rejects_ambiguous_or_non_reproducible_identity(
    changes: dict[str, object], expected: str
) -> None:
    """Malformed identity material must fail before it can enter a cache key."""
    values: dict[str, object] = {
        "factor_id": "momentum",
        "version": "1.0.0",
        "frequency": "daily",
        "lookback_sessions": 20,
        "dependencies": (),
        "direction": 1,
        "parameters": {},
    }
    values.update(changes)

    with pytest.raises((TypeError, ValueError), match=expected):
        FactorSpec(**values)  # type: ignore[arg-type]


def test_factor_context_is_normalized_immutable_and_bound_to_exact_scope() -> None:
    """A context cannot drift to latest or mutate after a cache lookup."""
    snapshot_id = SnapshotId.parse("12345678-1234-5678-9234-567812345678")

    ctx = FactorContext(
        snapshot_id=snapshot_id,
        universe_hash="a" * 64,
        start=date(2025, 1, 2),
        end=date(2025, 1, 31),
    )

    assert ctx.snapshot_id is snapshot_id
    assert ctx.universe_hash == "a" * 64
    assert (ctx.start, ctx.end) == (date(2025, 1, 2), date(2025, 1, 31))
    with pytest.raises(FrozenInstanceError):
        ctx.end = date(2025, 2, 1)  # type: ignore[misc]


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"snapshot_id": "latest"}, "SnapshotId"),
        ({"universe_hash": "A" * 64}, "universe_hash"),
        ({"universe_hash": "a" * 63}, "universe_hash"),
        ({"start": "2025-01-02"}, "start"),
        ({"end": "2025-01-31"}, "end"),
        (
            {"start": date(2025, 2, 1), "end": date(2025, 1, 31)},
            "start must not follow end",
        ),
    ],
)
def test_factor_context_rejects_non_immutable_or_invalid_scope(
    changes: dict[str, object], expected: str
) -> None:
    """Every context must identify one valid immutable PIT interval."""
    values: dict[str, object] = {
        "snapshot_id": SnapshotId.new(),
        "universe_hash": "a" * 64,
        "start": date(2025, 1, 2),
        "end": date(2025, 1, 31),
    }
    values.update(changes)

    with pytest.raises((TypeError, ValueError), match=expected):
        FactorContext(**values)  # type: ignore[arg-type]


def test_registry_rejects_duplicate_logical_identity() -> None:
    """Two implementations may not claim the same factor_id and version."""
    registry = FactorRegistry()
    registry.register(StubFactor(make_spec("momentum")), code_hash=code_hash("first"))

    with pytest.raises(ValueError, match=r"duplicate.*momentum@1\.0\.0"):
        registry.register(
            StubFactor(make_spec("momentum")), code_hash=code_hash("second")
        )


def test_registry_rejects_missing_dependency_with_canonical_reference() -> None:
    """A plan must fail closed when a declared version is absent."""
    registry = FactorRegistry()
    registry.register(
        StubFactor(make_spec("signal", dependencies=("price@2.0.0",))),
        code_hash=code_hash("signal"),
    )

    with pytest.raises(ValueError, match=r"missing dependency price@2\.0\.0"):
        registry.topological_order(("signal@1.0.0",))


def test_registry_reports_the_complete_dependency_cycle_path() -> None:
    """Cycle diagnostics must expose every edge needed to repair the graph."""
    registry = FactorRegistry()
    for factor_id, dependency in (
        ("alpha", "beta@1.0.0"),
        ("beta", "gamma@1.0.0"),
        ("gamma", "alpha@1.0.0"),
    ):
        registry.register(
            StubFactor(make_spec(factor_id, dependencies=(dependency,))),
            code_hash=code_hash(factor_id),
        )

    with pytest.raises(
        ValueError,
        match=(
            r"dependency cycle: alpha@1\.0\.0 -> beta@1\.0\.0 -> "
            r"gamma@1\.0\.0 -> alpha@1\.0\.0"
        ),
    ):
        registry.topological_order(("alpha@1.0.0",))


def test_topological_order_is_stable_across_registration_and_dependency_order() -> None:
    """Equivalent DAGs must schedule dependencies in canonical lexical order."""
    first = FactorRegistry()
    second = FactorRegistry()
    specs = (
        make_spec("quality"),
        make_spec("price"),
        make_spec("signal", dependencies=("quality@1.0.0", "price@1.0.0")),
    )
    for spec in specs:
        first.register(StubFactor(spec), code_hash=code_hash(spec.factor_id))
    for spec in reversed(specs):
        dependencies = tuple(reversed(spec.dependencies))
        reordered = make_spec(spec.factor_id, dependencies=dependencies)
        second.register(StubFactor(reordered), code_hash=code_hash(reordered.factor_id))

    expected = ("price@1.0.0", "quality@1.0.0", "signal@1.0.0")
    assert first.topological_order(("signal@1.0.0",)) == expected
    assert second.topological_order(("signal@1.0.0",)) == expected


def test_bare_factor_id_resolution_requires_exactly_one_registered_version() -> None:
    """Convenience resolution may never guess among multiple versions."""
    registry = FactorRegistry()
    registry.register(
        StubFactor(make_spec("momentum", version="1.0.0")),
        code_hash=code_hash("v1"),
    )

    assert registry.resolve("momentum") == "momentum@1.0.0"

    registry.register(
        StubFactor(make_spec("momentum", version="2.0.0")),
        code_hash=code_hash("v2"),
    )
    with pytest.raises(ValueError, match=r"ambiguous factor id momentum"):
        registry.resolve("momentum")


def test_registry_rejects_non_sha256_code_identity() -> None:
    """Process-local hashes and informal versions cannot identify implementation code."""
    registry = FactorRegistry()

    with pytest.raises(ValueError, match="code_hash"):
        registry.register(StubFactor(make_spec("momentum")), code_hash="v1")


def test_registry_snapshots_the_immutable_spec_at_registration() -> None:
    """A Factor provider cannot drift an already-registered logical identity."""
    registry = FactorRegistry()
    factor = StubFactor(make_spec("momentum"))
    registry.register(factor, code_hash=code_hash("momentum"))

    factor._spec = make_spec("reversal", version="2.0.0")

    assert registry.resolve("momentum") == "momentum@1.0.0"
    assert registry.spec("momentum@1.0.0").canonical_ref == "momentum@1.0.0"
