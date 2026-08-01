"""Factor registration, strict version resolution, and deterministic DAG planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast

from quant_core.data.contracts import ProviderCapabilities
from quant_core.factors.base import (
    Factor,
    FactorArtifact,
    FactorContext,
    FactorSpec,
    canonical_factor_ref,
    validate_sha256,
)
from quant_core.factors.cache import FeatureCache, build_cache_key


@dataclass(frozen=True, slots=True)
class _Registration:
    factor: Factor
    spec: FactorSpec
    code_hash: str


class FactorCapabilityUnavailable(ValueError):
    """A requested factor needs source inputs absent from the runtime profile."""

    def __init__(self, factor_ref: str, missing: tuple[str, ...]) -> None:
        self.factor_ref = factor_ref
        self.missing = missing
        super().__init__(
            f"factor {factor_ref} requires unavailable capabilities: {', '.join(missing)}"
        )


class FactorRegistry:
    """An in-memory catalog keyed only by canonical factor references."""

    def __init__(self) -> None:
        self._registrations: dict[str, _Registration] = {}

    def register(self, factor: Factor, *, code_hash: str) -> None:
        """Register one logical version with an auditable implementation digest."""
        if not hasattr(factor, "spec") or not isinstance(factor.spec, FactorSpec):
            raise TypeError("factor must expose a FactorSpec")
        spec = factor.spec
        canonical_ref = spec.canonical_ref
        validate_sha256(code_hash, "code_hash")
        if canonical_ref in self._registrations:
            raise ValueError(f"duplicate factor registration: {canonical_ref}")
        self._registrations[canonical_ref] = _Registration(factor, spec, code_hash)

    def resolve(self, reference: str) -> str:
        """Resolve an explicit ref or one strictly unambiguous bare factor ID."""
        if "@" in reference:
            canonical_ref = canonical_factor_ref(reference)
            if canonical_ref not in self._registrations:
                raise ValueError(f"unknown factor: {canonical_ref}")
            return canonical_ref
        matches = sorted(
            canonical_ref
            for canonical_ref, registration in self._registrations.items()
            if registration.spec.factor_id == reference
        )
        if not matches:
            raise ValueError(f"unknown factor id {reference}")
        if len(matches) != 1:
            raise ValueError(f"ambiguous factor id {reference}: {', '.join(matches)}")
        return matches[0]

    def factor(self, reference: str) -> Factor:
        """Return the implementation for an explicit or unambiguous reference."""
        return self._registrations[self.resolve(reference)].factor

    def code_hash(self, reference: str) -> str:
        """Return the registered implementation SHA-256 for a factor reference."""
        return self._registrations[self.resolve(reference)].code_hash

    def registered_references(self) -> tuple[str, ...]:
        """Return every registered canonical reference in deterministic order."""
        return tuple(sorted(self._registrations))

    def spec(self, reference: str) -> FactorSpec:
        """Return the immutable contract captured when a factor was registered."""
        return self._registrations[self.resolve(reference)].spec

    def topological_order(self, references: tuple[str, ...]) -> tuple[str, ...]:
        """Return all transitive dependencies before roots in stable lexical order."""
        roots = tuple(sorted({self.resolve(reference) for reference in references}))
        ordered: list[str] = []
        completed: set[str] = set()
        active: list[str] = []

        def visit(canonical_ref: str) -> None:
            if canonical_ref in completed:
                return
            if canonical_ref in active:
                cycle_start = active.index(canonical_ref)
                cycle = (*active[cycle_start:], canonical_ref)
                raise ValueError(f"dependency cycle: {' -> '.join(cycle)}")
            registration = self._registrations.get(canonical_ref)
            if registration is None:
                raise ValueError(f"missing dependency {canonical_ref}")
            active.append(canonical_ref)
            for dependency in sorted(registration.spec.dependencies):
                if dependency not in self._registrations:
                    raise ValueError(
                        f"missing dependency {dependency} required by {canonical_ref}"
                    )
                visit(dependency)
            active.pop()
            completed.add(canonical_ref)
            ordered.append(canonical_ref)

        for root in roots:
            visit(root)
        return tuple(ordered)

    def runnable_references(
        self, capabilities: ProviderCapabilities
    ) -> tuple[str, ...]:
        """Return registered factors whose declared source inputs are available."""
        return tuple(
            canonical_ref
            for canonical_ref in sorted(self._registrations)
            if not capabilities.missing(
                _required_capabilities(self.spec(canonical_ref))
            )
        )

    def preflight(
        self, references: Sequence[str], capabilities: ProviderCapabilities
    ) -> tuple[str, ...]:
        """Resolve a request and reject unavailable data requirements before compute."""
        plan = self.topological_order(tuple(references))
        for canonical_ref in plan:
            missing = capabilities.missing(
                _required_capabilities(self.spec(canonical_ref))
            )
            if missing:
                raise FactorCapabilityUnavailable(canonical_ref, missing)
        return plan


class FactorEngine:
    """Compute a dependency closure once and materialize verified cache artifacts."""

    def __init__(
        self,
        registry: FactorRegistry,
        cache: FeatureCache,
        *,
        capabilities: ProviderCapabilities,
    ) -> None:
        self._registry = registry
        self._cache = cache
        self._capabilities = capabilities

    def runnable_references(self) -> tuple[str, ...]:
        """List factors runnable under this engine's explicit capability profile."""
        return self._registry.runnable_references(self._capabilities)

    def compute(
        self, factor_ids: Sequence[str], ctx: FactorContext
    ) -> Mapping[str, FactorArtifact]:
        """Return requested canonical artifacts after stable dependency evaluation."""
        requested = tuple(self._registry.resolve(reference) for reference in factor_ids)
        if len(set(requested)) != len(requested):
            raise ValueError("factor request contains duplicate logical identities")
        plan = self._registry.preflight(requested, self._capabilities)
        computed: dict[str, FactorArtifact] = {}
        for canonical_ref in plan:
            factor = self._registry.factor(canonical_ref)
            spec = self._registry.spec(canonical_ref)
            dependency_hashes = {
                dependency: computed[dependency].content_hash
                for dependency in spec.dependencies
            }
            cache_key = build_cache_key(
                spec,
                ctx,
                self._registry.code_hash(canonical_ref),
                dependency_hashes,
            )
            artifact = self._cache.load(cache_key)
            if artifact is None:
                artifact = self._cache.publish(
                    cache_key,
                    factor.compute(ctx),
                    spec=spec,
                    ctx=ctx,
                    code_hash=self._registry.code_hash(canonical_ref),
                    dependency_hashes=dependency_hashes,
                )
            computed[canonical_ref] = artifact
        return MappingProxyType(
            {canonical_ref: computed[canonical_ref] for canonical_ref in requested}
        )


def _required_capabilities(spec: FactorSpec) -> tuple[str, ...]:
    value = spec.parameters.get("required_capabilities", ())
    if not isinstance(value, tuple):
        raise TypeError("required_capabilities must be a list of capability names")
    requirements = cast(tuple[object, ...], value)
    if not all(isinstance(item, str) for item in requirements):
        raise ValueError("required_capabilities must be a list of capability names")
    return cast(tuple[str, ...], requirements)
