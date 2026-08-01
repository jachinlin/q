"""Bounded deterministic factor execution with atomic composite manifests."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol, cast

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.storage import resolved_storage_root
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import (
    FactorArtifact,
    FactorContext,
    canonical_factor_ref,
    validate_sha256,
)
from quant_core.factors.cache import FeatureCache
from quant_core.factors.composite_cache import CompositeManifestCache
from quant_core.factors.execution import FactorExecutionDescriptor
from quant_core.factors.registry import FactorEngine

_FORMAT_VERSION = 2
_CONTENT_HASH_CONTRACT = "quant-core.ordered-partition-factor-artifacts.v2"
_PARTITION_CONTRACT = "quant-core.factor-partition-scope.v1"
_PARTITION_UNIVERSE_CONTRACT = "quant-core.factor-partition-universe.v1"
_MANIFEST_FIELDS = frozenset(
    {
        "composite_key",
        "content_hash",
        "content_hash_contract",
        "end",
        "execution_descriptor",
        "execution_descriptor_hash",
        "factor_refs",
        "format_version",
        "instrument_ids",
        "max_partition_size",
        "partition_size",
        "partitions",
        "snapshot_id",
        "start",
        "universe_hash",
    }
)
_PARTITION_FIELDS = {
    "artifacts",
    "index",
    "instrument_ids",
    "partition_id",
    "universe_hash",
}
_ARTIFACT_FIELDS = {
    "cache_key",
    "content_hash",
    "factor_ref",
    "row_count",
    "schema_fingerprint",
}


class PartitionEngineFactory(Protocol):
    """Build one ordinary engine bound to exactly one instrument partition."""

    def __call__(
        self, instruments: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        """Return an engine whose factors use ``instruments`` and ``cache``."""


@dataclass(frozen=True, slots=True)
class PartitionFactorArtifactRef:
    """Legacy-compatible identity of one bounded partition factor artifact."""

    factor_ref: str
    cache_key: str
    content_hash: str
    row_count: int
    schema_fingerprint: str

    def __post_init__(self) -> None:
        canonical_factor_ref(self.factor_ref)
        validate_sha256(self.cache_key, "partition artifact cache_key")
        validate_sha256(self.content_hash, "partition artifact content_hash")
        validate_sha256(self.schema_fingerprint, "partition schema_fingerprint")
        if type(self.row_count) is not int or self.row_count < 0:
            raise ValueError("partition artifact row_count must be nonnegative")


@dataclass(frozen=True, slots=True)
class CompositeFactorPartition:
    """Auditable scope and ordered artifact references for one partition."""

    index: int
    partition_id: str
    universe_hash: str
    instrument_ids: tuple[str, ...]
    artifacts: tuple[PartitionFactorArtifactRef, ...]

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0:
            raise ValueError("partition index must be nonnegative")
        validate_sha256(self.partition_id, "partition_id")
        validate_sha256(self.universe_hash, "partition universe_hash")
        canonical = tuple(
            InstrumentId.parse(value).canonical() for value in self.instrument_ids
        )
        if canonical != tuple(sorted(set(canonical))):
            raise ValueError("partition instruments must be unique and canonical")
        if tuple(item.factor_ref for item in self.artifacts) != tuple(
            sorted(item.factor_ref for item in self.artifacts)
        ):
            raise ValueError("partition factor references must be canonically ordered")

    @property
    def row_count(self) -> int:
        """Return all factor rows represented by this bounded partition."""
        return sum(item.row_count for item in self.artifacts)


@dataclass(frozen=True, slots=True)
class CompositeFactorArtifact:
    """Metadata-only binding of every ordered bounded factor artifact."""

    composite_key: str
    content_hash: str
    content_hash_contract: str
    execution_descriptor: FactorExecutionDescriptor
    snapshot_id: SnapshotId
    universe_hash: str
    start: date
    end: date
    factor_refs: tuple[str, ...]
    instrument_ids: tuple[str, ...]
    partition_size: int
    max_partition_size: int
    partitions: tuple[CompositeFactorPartition, ...]
    manifest_path: Path

    def __post_init__(self) -> None:
        validate_sha256(self.composite_key, "composite_key")
        validate_sha256(self.content_hash, "composite content_hash")
        if self.content_hash_contract != _CONTENT_HASH_CONTRACT:
            raise ValueError("unsupported composite content hash contract")
        if not isinstance(self.execution_descriptor, FactorExecutionDescriptor):
            raise TypeError("execution_descriptor must be a FactorExecutionDescriptor")
        if not isinstance(self.snapshot_id, SnapshotId):
            raise TypeError("snapshot_id must be a SnapshotId")
        validate_sha256(self.universe_hash, "composite universe_hash")
        if type(self.start) is not date or type(self.end) is not date:
            raise TypeError("composite dates must be dates")
        if self.start > self.end:
            raise ValueError("composite start must not follow end")
        canonical_refs = tuple(canonical_factor_ref(item) for item in self.factor_refs)
        if canonical_refs != tuple(sorted(set(canonical_refs))):
            raise ValueError("composite factor references must be unique and ordered")
        canonical_ids = tuple(
            InstrumentId.parse(item).canonical() for item in self.instrument_ids
        )
        if canonical_ids != tuple(sorted(set(canonical_ids))):
            raise ValueError("composite instruments must be unique and ordered")
        _validated_partition_size(self.partition_size, self.max_partition_size)
        if not isinstance(self.manifest_path, Path):
            raise TypeError("manifest_path must be a Path")

    @property
    def row_count(self) -> int:
        """Return all rows represented without retaining any partition tables."""
        return sum(item.row_count for item in self.partitions)

    @property
    def execution_descriptor_hash(self) -> str:
        return self.execution_descriptor.content_hash


class PartitionedFactorEngine:
    """Run ordinary factor engines over deterministic bounded partitions."""

    def __init__(
        self,
        root: Path,
        engine_factory: PartitionEngineFactory,
        *,
        max_partition_size: int,
    ) -> None:
        if not callable(engine_factory):
            raise TypeError("engine_factory must be callable")
        _validated_partition_size(max_partition_size, max_partition_size)
        storage_root = resolved_storage_root(root)
        storage_root.mkdir(parents=True, exist_ok=True)
        self._feature_cache = FeatureCache(storage_root / "artifacts")
        self._composite_cache = CompositeManifestCache(
            storage_root / "composites", manifest_fields=_MANIFEST_FIELDS
        )
        self._engine_factory = engine_factory
        self._max_partition_size = max_partition_size

    @property
    def max_partition_size(self) -> int:
        """Return the construction-time hard bound for every execution."""
        return self._max_partition_size

    def compute(
        self,
        factor_ids: Sequence[str],
        instruments: Sequence[InstrumentId],
        ctx: FactorContext,
        *,
        partition_size: int | None = None,
    ) -> CompositeFactorArtifact:
        """Compute, consume, and release each bounded partition in stable order."""
        if not isinstance(ctx, FactorContext):
            raise TypeError("ctx must be a FactorContext")
        factor_refs = _canonical_factor_refs(factor_ids)
        canonical_instruments = _canonical_instruments(instruments)
        size = self._max_partition_size if partition_size is None else partition_size
        _validated_partition_size(size, self._max_partition_size)
        instrument_ids = tuple(item.canonical() for item in canonical_instruments)
        first_scope = canonical_instruments[:size]
        first_engine = self._engine_factory(first_scope, self._feature_cache)
        execution_descriptor = first_engine.execution_descriptor(factor_refs)
        if execution_descriptor.requested_refs != factor_refs:
            raise ValueError("partition execution descriptor roots differ")
        composite_key = _composite_key(
            factor_refs,
            instrument_ids,
            ctx,
            size,
            self._max_partition_size,
            execution_descriptor.content_hash,
        )
        existing = self._load_composite(composite_key)
        if existing is not None:
            if existing.execution_descriptor != execution_descriptor:
                raise ValueError("composite execution descriptor differs")
            for offset in range(size, len(canonical_instruments), size):
                scope = canonical_instruments[offset : offset + size]
                engine = self._engine_factory(scope, self._feature_cache)
                self._validate_engine_descriptor(
                    engine, factor_refs, execution_descriptor
                )
                del engine
            del first_engine
            self._validate_artifact_references(existing)
            return existing

        partition_metadata: list[CompositeFactorPartition] = []
        for index, offset in enumerate(range(0, len(canonical_instruments), size)):
            partition_instruments = canonical_instruments[offset : offset + size]
            partition_ids = instrument_ids[offset : offset + size]
            partition_id = _partition_id(composite_key, index, partition_ids)
            universe_hash = _partition_universe_hash(ctx.universe_hash, partition_id)
            partition_ctx = FactorContext(
                ctx.snapshot_id, universe_hash, ctx.start, ctx.end
            )
            engine = (
                first_engine
                if index == 0
                else self._engine_factory(partition_instruments, self._feature_cache)
            )
            self._validate_engine_descriptor(engine, factor_refs, execution_descriptor)
            artifacts = engine.compute(factor_refs, partition_ctx)
            if tuple(artifacts) != factor_refs:
                raise ValueError(
                    "partition engine returned unexpected factor references"
                )
            artifact_refs: list[PartitionFactorArtifactRef] = []
            for factor_ref in factor_refs:
                artifact = artifacts[factor_ref]
                _validate_partition_artifact(
                    artifact, factor_ref, partition_ctx, partition_ids
                )
                artifact_refs.append(
                    PartitionFactorArtifactRef(
                        factor_ref=factor_ref,
                        cache_key=artifact.cache_key,
                        content_hash=artifact.content_hash,
                        row_count=artifact.row_count,
                        schema_fingerprint=_schema_fingerprint(artifact.table.schema),
                    )
                )
            partition_metadata.append(
                CompositeFactorPartition(
                    index=index,
                    partition_id=partition_id,
                    universe_hash=universe_hash,
                    instrument_ids=partition_ids,
                    artifacts=tuple(artifact_refs),
                )
            )
            del artifact, artifact_refs, artifacts, engine
        if not canonical_instruments:
            del first_engine

        manifest = _build_manifest(
            composite_key=composite_key,
            factor_refs=factor_refs,
            instrument_ids=instrument_ids,
            ctx=ctx,
            partition_size=size,
            max_partition_size=self._max_partition_size,
            execution_descriptor=execution_descriptor,
            partitions=tuple(partition_metadata),
        )
        loaded_manifest, manifest_path = self._composite_cache.publish(manifest)
        composite = _parse_manifest(loaded_manifest, composite_key, manifest_path)
        self._validate_artifact_references(composite)
        return composite

    @staticmethod
    def _validate_engine_descriptor(
        engine: FactorEngine,
        factor_refs: tuple[str, ...],
        expected: FactorExecutionDescriptor,
    ) -> None:
        actual = engine.execution_descriptor(factor_refs)
        if actual != expected or actual.content_hash != expected.content_hash:
            raise ValueError("partition execution descriptor differs across scopes")

    def _load_composite(self, composite_key: str) -> CompositeFactorArtifact | None:
        loaded = self._composite_cache.load(composite_key)
        if loaded is None:
            return None
        manifest, manifest_path = loaded
        try:
            return _parse_manifest(manifest, composite_key, manifest_path)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("composite manifest is invalid") from error

    def _validate_artifact_references(self, composite: CompositeFactorArtifact) -> None:
        for partition in composite.partitions:
            partition_ctx = FactorContext(
                composite.snapshot_id,
                partition.universe_hash,
                composite.start,
                composite.end,
            )
            for expected in partition.artifacts:
                artifact = self._feature_cache.load(expected.cache_key)
                if artifact is None:
                    raise ValueError("composite manifest references a missing artifact")
                _validate_partition_artifact(
                    artifact,
                    expected.factor_ref,
                    partition_ctx,
                    partition.instrument_ids,
                )
                if (
                    artifact.content_hash != expected.content_hash
                    or artifact.row_count != expected.row_count
                    or _schema_fingerprint(artifact.table.schema)
                    != expected.schema_fingerprint
                ):
                    raise ValueError("composite artifact metadata differs")
                del artifact


def _canonical_factor_refs(factor_ids: Sequence[str]) -> tuple[str, ...]:
    if isinstance(factor_ids, (str, bytes)) or not isinstance(factor_ids, Sequence):
        raise TypeError("factor_ids must be a sequence")
    normalized = tuple(canonical_factor_ref(item) for item in factor_ids)
    if len(set(normalized)) != len(normalized):
        raise ValueError("factor request contains duplicate logical identities")
    return tuple(sorted(normalized))


def _canonical_instruments(
    instruments: Sequence[InstrumentId],
) -> tuple[InstrumentId, ...]:
    if isinstance(instruments, (str, bytes)) or not isinstance(instruments, Sequence):
        raise TypeError("instruments must be a sequence")
    normalized: dict[str, InstrumentId] = {}
    for instrument in instruments:
        if not isinstance(instrument, InstrumentId):
            raise TypeError("instruments must contain InstrumentId values")
        canonical = instrument.canonical()
        if canonical in normalized:
            raise ValueError("instrument scope contains a duplicate identity")
        normalized[canonical] = instrument
    return tuple(normalized[key] for key in sorted(normalized))


def _validated_partition_size(value: int, maximum: int) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("partition size must be a positive integer")
    if type(maximum) is not int or maximum <= 0:
        raise ValueError("maximum partition size must be a positive integer")
    if value > maximum:
        raise ValueError("partition size exceeds configured maximum")
    return value


def _composite_key(
    factor_refs: tuple[str, ...],
    instrument_ids: tuple[str, ...],
    ctx: FactorContext,
    partition_size: int,
    max_partition_size: int,
    execution_descriptor_hash: str,
) -> str:
    validate_sha256(execution_descriptor_hash, "execution_descriptor_hash")
    payload: dict[str, JsonValue] = {
        "content_hash_contract": _CONTENT_HASH_CONTRACT,
        "end": ctx.end.isoformat(),
        "factor_refs": list(factor_refs),
        "format_version": _FORMAT_VERSION,
        "execution_descriptor_hash": execution_descriptor_hash,
        "instrument_ids": list(instrument_ids),
        "max_partition_size": max_partition_size,
        "partition_size": partition_size,
        "snapshot_id": str(ctx.snapshot_id),
        "start": ctx.start.isoformat(),
        "universe_hash": ctx.universe_hash,
    }
    return _sha256_json(payload)


def _partition_id(
    composite_key: str, index: int, instrument_ids: tuple[str, ...]
) -> str:
    return _sha256_json(
        {
            "composite_key": composite_key,
            "contract": _PARTITION_CONTRACT,
            "index": index,
            "instrument_ids": list(instrument_ids),
        }
    )


def _partition_universe_hash(parent_universe_hash: str, partition_id: str) -> str:
    return _sha256_json(
        {
            "contract": _PARTITION_UNIVERSE_CONTRACT,
            "parent_universe_hash": parent_universe_hash,
            "partition_id": partition_id,
        }
    )


def _validate_partition_artifact(
    artifact: FactorArtifact,
    factor_ref: str,
    ctx: FactorContext,
    instrument_ids: tuple[str, ...],
) -> None:
    if not isinstance(artifact, FactorArtifact):
        raise TypeError("partition engine values must be FactorArtifact instances")
    if (
        artifact.factor_ref != factor_ref
        or artifact.snapshot_id != ctx.snapshot_id
        or artifact.universe_hash != ctx.universe_hash
        or artifact.start != ctx.start
        or artifact.end != ctx.end
    ):
        raise ValueError("partition artifact PIT identity differs")
    frame = cast(pl.DataFrame, pl.from_arrow(artifact.table))
    if frame.filter(~pl.col("instrument_id").is_in(instrument_ids)).height:
        raise ValueError("partition artifact contains an out-of-scope instrument")


def _artifact_manifest(item: PartitionFactorArtifactRef) -> dict[str, JsonValue]:
    return {
        "cache_key": item.cache_key,
        "content_hash": item.content_hash,
        "factor_ref": item.factor_ref,
        "row_count": item.row_count,
        "schema_fingerprint": item.schema_fingerprint,
    }


def _partition_manifest(item: CompositeFactorPartition) -> dict[str, JsonValue]:
    return {
        "artifacts": [_artifact_manifest(artifact) for artifact in item.artifacts],
        "index": item.index,
        "instrument_ids": list(item.instrument_ids),
        "partition_id": item.partition_id,
        "universe_hash": item.universe_hash,
    }


def _build_manifest(
    *,
    composite_key: str,
    factor_refs: tuple[str, ...],
    instrument_ids: tuple[str, ...],
    ctx: FactorContext,
    partition_size: int,
    max_partition_size: int,
    execution_descriptor: FactorExecutionDescriptor,
    partitions: tuple[CompositeFactorPartition, ...],
) -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {
        "composite_key": composite_key,
        "content_hash_contract": _CONTENT_HASH_CONTRACT,
        "end": ctx.end.isoformat(),
        "execution_descriptor": execution_descriptor.json_value(),
        "execution_descriptor_hash": execution_descriptor.content_hash,
        "factor_refs": list(factor_refs),
        "format_version": _FORMAT_VERSION,
        "instrument_ids": list(instrument_ids),
        "max_partition_size": max_partition_size,
        "partition_size": partition_size,
        "partitions": [_partition_manifest(item) for item in partitions],
        "snapshot_id": str(ctx.snapshot_id),
        "start": ctx.start.isoformat(),
        "universe_hash": ctx.universe_hash,
    }
    payload["content_hash"] = _sha256_json(payload)
    return payload


def _parse_manifest(
    manifest: Mapping[str, object], expected_key: str, manifest_path: Path
) -> CompositeFactorArtifact:
    if manifest["format_version"] != _FORMAT_VERSION:
        raise ValueError("composite manifest version is unsupported")
    if manifest["content_hash_contract"] != _CONTENT_HASH_CONTRACT:
        raise ValueError("composite hash contract is unsupported")
    factor_refs = _string_tuple(manifest["factor_refs"], "factor_refs")
    factor_refs = _canonical_factor_refs(factor_refs)
    execution_descriptor = FactorExecutionDescriptor.from_json_value(
        manifest["execution_descriptor"]
    )
    execution_descriptor_hash = validate_sha256(
        _required_string(manifest, "execution_descriptor_hash"),
        "execution_descriptor_hash",
    )
    if execution_descriptor.content_hash != execution_descriptor_hash:
        raise ValueError("composite execution descriptor hash differs")
    if execution_descriptor.requested_refs != factor_refs:
        raise ValueError("composite execution descriptor roots differ")
    instrument_ids = _string_tuple(manifest["instrument_ids"], "instrument_ids")
    parsed_instruments = tuple(InstrumentId.parse(item) for item in instrument_ids)
    if tuple(item.canonical() for item in parsed_instruments) != instrument_ids:
        raise ValueError("composite instruments are not canonical")
    if instrument_ids != tuple(sorted(set(instrument_ids))):
        raise ValueError("composite instruments are not ordered and unique")
    snapshot_id = SnapshotId.parse(_required_string(manifest, "snapshot_id"))
    universe_hash = validate_sha256(
        _required_string(manifest, "universe_hash"), "composite universe_hash"
    )
    start = _parse_date(_required_string(manifest, "start"), "start")
    end = _parse_date(_required_string(manifest, "end"), "end")
    partition_size = _required_int(manifest, "partition_size")
    max_partition_size = _required_int(manifest, "max_partition_size")
    _validated_partition_size(partition_size, max_partition_size)
    ctx = FactorContext(snapshot_id, universe_hash, start, end)
    derived_key = _composite_key(
        factor_refs,
        instrument_ids,
        ctx,
        partition_size,
        max_partition_size,
        execution_descriptor_hash,
    )
    composite_key = _required_string(manifest, "composite_key")
    if composite_key != expected_key or composite_key != derived_key:
        raise ValueError("composite manifest key differs")

    raw_partitions = manifest["partitions"]
    if not isinstance(raw_partitions, list):
        raise TypeError("composite partitions must be a list")
    partitions: list[CompositeFactorPartition] = []
    expected_groups = tuple(
        instrument_ids[offset : offset + partition_size]
        for offset in range(0, len(instrument_ids), partition_size)
    )
    if len(raw_partitions) != len(expected_groups):
        raise ValueError("composite partition coverage differs")
    for index, raw_partition in enumerate(raw_partitions):
        if (
            not isinstance(raw_partition, Mapping)
            or set(raw_partition) != _PARTITION_FIELDS
        ):
            raise ValueError("composite partition fields are invalid")
        partition_ids = _string_tuple(
            raw_partition["instrument_ids"], "partition instrument_ids"
        )
        if partition_ids != expected_groups[index]:
            raise ValueError("composite partition ordering or coverage differs")
        partition_id = _required_string(raw_partition, "partition_id")
        if partition_id != _partition_id(composite_key, index, partition_ids):
            raise ValueError("composite partition identity differs")
        partition_universe = _required_string(raw_partition, "universe_hash")
        if partition_universe != _partition_universe_hash(universe_hash, partition_id):
            raise ValueError("composite partition universe differs")
        if _required_int(raw_partition, "index") != index:
            raise ValueError("composite partition index differs")
        raw_artifacts = raw_partition["artifacts"]
        if not isinstance(raw_artifacts, list) or len(raw_artifacts) != len(
            factor_refs
        ):
            raise ValueError("composite partition factor coverage differs")
        artifact_refs: list[PartitionFactorArtifactRef] = []
        for factor_ref, raw_artifact in zip(factor_refs, raw_artifacts, strict=True):
            if (
                not isinstance(raw_artifact, Mapping)
                or set(raw_artifact) != _ARTIFACT_FIELDS
            ):
                raise ValueError("composite artifact fields are invalid")
            if _required_string(raw_artifact, "factor_ref") != factor_ref:
                raise ValueError("composite factor ordering differs")
            artifact_refs.append(
                PartitionFactorArtifactRef(
                    factor_ref=factor_ref,
                    cache_key=_required_string(raw_artifact, "cache_key"),
                    content_hash=_required_string(raw_artifact, "content_hash"),
                    row_count=_required_int(raw_artifact, "row_count"),
                    schema_fingerprint=_required_string(
                        raw_artifact, "schema_fingerprint"
                    ),
                )
            )
        partitions.append(
            CompositeFactorPartition(
                index=index,
                partition_id=partition_id,
                universe_hash=partition_universe,
                instrument_ids=partition_ids,
                artifacts=tuple(artifact_refs),
            )
        )

    content_hash = _required_string(manifest, "content_hash")
    validate_sha256(content_hash, "composite content_hash")
    content_payload = dict(manifest)
    del content_payload["content_hash"]
    if content_hash != _sha256_json(cast(Mapping[str, JsonValue], content_payload)):
        raise ValueError("composite content hash differs")
    return CompositeFactorArtifact(
        composite_key=composite_key,
        content_hash=content_hash,
        content_hash_contract=_CONTENT_HASH_CONTRACT,
        execution_descriptor=execution_descriptor,
        snapshot_id=snapshot_id,
        universe_hash=universe_hash,
        start=start,
        end=end,
        factor_refs=factor_refs,
        instrument_ids=instrument_ids,
        partition_size=partition_size,
        max_partition_size=max_partition_size,
        partitions=tuple(partitions),
        manifest_path=manifest_path,
    )


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _sha256_json(payload: Mapping[str, JsonValue]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _required_string(mapping: Mapping[str, object], field: str) -> str:
    value = mapping[field]
    if not isinstance(value, str):
        raise TypeError(f"composite manifest {field} must be a string")
    return value


def _required_int(mapping: Mapping[str, object], field: str) -> int:
    value = mapping[field]
    if type(value) is not int:
        raise TypeError(f"composite manifest {field} must be an integer")
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TypeError(f"composite manifest {field} must be a string list")
    return cast(tuple[str, ...], tuple(value))


def _parse_date(value: str, field: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"composite manifest {field} is invalid") from error
    if parsed.isoformat() != value:
        raise ValueError(f"composite manifest {field} is invalid")
    return parsed


__all__ = [
    "CompositeFactorArtifact",
    "CompositeFactorPartition",
    "PartitionEngineFactory",
    "PartitionFactorArtifactRef",
    "PartitionedFactorEngine",
]
