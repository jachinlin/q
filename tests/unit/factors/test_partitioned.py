"""Deterministic bounded factor execution and composite-manifest contracts."""

from __future__ import annotations

import gc
import hashlib
import inspect
import json
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
import pytest

from quant_core.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_core.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import (
    FACTOR_OUTPUT_SCHEMA,
    CompositeFactorArtifact,
    FactorArtifact,
    FactorContext,
    FactorEngine,
    FactorExecutionDescriptor,
    FactorExecutionNode,
    FactorRegistry,
    FactorSpec,
    FeatureCache,
    PartitionedFactorEngine,
    build_cache_key,
)
from quant_core.factors.builtin import register_etf_factors

_FACTOR_REFS = tuple(f"factor_{index}@1.0.0" for index in range(5))
_MARKET_FACTOR_REFS = (
    "return_20d_v1@2.1.0",
    "return_60d_v1@2.1.0",
    "return_120d_v1@2.1.0",
    "trend_120d_v1@2.1.0",
    "volatility_60d_v1@2.1.0",
)
_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000091")


def _instruments(count: int) -> tuple[InstrumentId, ...]:
    return tuple(
        InstrumentId.parse(f"SSE:{600000 + index:06d}") for index in range(count)
    )


def _context() -> FactorContext:
    return FactorContext(_SNAPSHOT, "9" * 64, date(2025, 1, 2), date(2025, 1, 3))


def _table(instruments: Sequence[InstrumentId], factor_ref: str) -> pa.Table:
    factor_id, _, version = factor_ref.partition("@")
    frame = pl.DataFrame(
        {
            "trade_date": [date(2025, 1, 2)] * len(instruments),
            "instrument_id": [item.canonical() for item in instruments],
            "factor_id": [factor_id] * len(instruments),
            "factor_version": [version] * len(instruments),
            "value": [float(item.symbol) for item in instruments],
            "available_at": [datetime(2025, 1, 2, 8, tzinfo=UTC)] * len(instruments),
            "is_valid": [True] * len(instruments),
        },
        schema=FACTOR_OUTPUT_SCHEMA,
    )
    return frame.to_arrow()


class _FakeEngine:
    def __init__(
        self,
        instruments: tuple[InstrumentId, ...],
        cache: FeatureCache,
        state: dict[str, object],
    ) -> None:
        self._instruments = instruments
        self._cache = cache
        self._state = state

    def execution_descriptor(
        self, factor_ids: Sequence[str]
    ) -> FactorExecutionDescriptor:
        factor_refs = tuple(sorted(factor_ids))
        return FactorExecutionDescriptor(
            requested_refs=factor_refs,
            plan=tuple(
                FactorExecutionNode(
                    spec=FactorSpec(
                        factor_id=factor_ref.partition("@")[0],
                        version=factor_ref.partition("@")[2],
                        frequency="daily",
                        lookback_sessions=0,
                        dependencies=(),
                        direction=1,
                        parameters={},
                    ),
                    code_hash=hashlib.sha256(f"code:{factor_ref}".encode()).hexdigest(),
                )
                for factor_ref in factor_refs
            ),
        )

    def compute(
        self, factor_ids: Sequence[str], ctx: FactorContext
    ) -> Mapping[str, FactorArtifact]:
        calls = self._state.setdefault("calls", [])
        assert isinstance(calls, list)
        calls.append(tuple(item.canonical() for item in self._instruments))
        live = int(self._state.get("live", 0)) + len(factor_ids)
        self._state["live"] = live
        self._state["peak_live"] = max(int(self._state.get("peak_live", 0)), live)
        result: dict[str, FactorArtifact] = {}
        for factor_ref in factor_ids:
            factor_id, _, version = factor_ref.partition("@")
            spec = FactorSpec(factor_id, version, "daily", 0, (), 1, {})
            code_hash = hashlib.sha256(f"code:{factor_ref}".encode()).hexdigest()
            cache_key = build_cache_key(spec, ctx, code_hash, {})
            artifact = self._cache.load(cache_key)
            if artifact is None:
                table = _table(self._instruments, factor_ref)
                artifact = self._cache.publish(
                    cache_key,
                    pl.from_arrow(table).lazy(),
                    spec=spec,
                    ctx=ctx,
                    code_hash=code_hash,
                    dependency_hashes={},
                )
            result[factor_ref] = artifact
        return _ReleasingArtifacts(result, self._state)


class _ReleasingArtifacts(dict[str, FactorArtifact]):
    def __init__(self, values: dict[str, FactorArtifact], state: dict[str, object]):
        super().__init__(values)
        self._state = state

    def __del__(self) -> None:
        self._state["live"] = int(self._state.get("live", 0)) - len(self)


class _ConstantFactor:
    def __init__(
        self,
        instruments: tuple[InstrumentId, ...],
        *,
        value: float,
        state: dict[str, int],
        factor_id: str = "stable_factor",
        dependencies: tuple[str, ...] = (),
        parameters: Mapping[str, JsonValue] | None = None,
    ) -> None:
        self._instruments = instruments
        self._value = value
        self._state = state
        self._spec = FactorSpec(
            factor_id,
            "1.0.0",
            "daily",
            0,
            dependencies,
            1,
            {} if parameters is None else parameters,
        )

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        self._state["factor_computes"] = self._state.get("factor_computes", 0) + 1
        return pl.DataFrame(
            {
                "trade_date": [ctx.start] * len(self._instruments),
                "instrument_id": [item.canonical() for item in self._instruments],
                "factor_id": [self.spec.factor_id] * len(self._instruments),
                "factor_version": [self.spec.version] * len(self._instruments),
                "value": [self._value] * len(self._instruments),
                "available_at": [datetime(2025, 1, 2, 8, tzinfo=UTC)]
                * len(self._instruments),
                "is_valid": [True] * len(self._instruments),
            },
            schema=FACTOR_OUTPUT_SCHEMA,
        ).lazy()


def _constant_executor(
    root: Path,
    *,
    code_label: str,
    value: float,
    state: dict[str, int],
    maximum: int = 10,
    parameters: Mapping[str, JsonValue] | None = None,
) -> PartitionedFactorEngine:
    def factory(
        instruments: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        state["factory_calls"] = state.get("factory_calls", 0) + 1
        registry = FactorRegistry()
        registry.register(
            _ConstantFactor(
                instruments, value=value, state=state, parameters=parameters
            ),
            code_hash=hashlib.sha256(code_label.encode()).hexdigest(),
        )
        return FactorEngine(
            registry, cache, capabilities=ProviderCapabilities.complete()
        )

    return PartitionedFactorEngine(root, factory, max_partition_size=maximum)


def test_composite_identity_changes_with_same_ref_new_implementation(
    tmp_path: Path,
) -> None:
    """A composite hit must not hide a same-version implementation change."""
    instruments = _instruments(2)
    state_a: dict[str, int] = {}
    first = _constant_executor(
        tmp_path, code_label="implementation-a", value=1.0, state=state_a
    ).compute(("stable_factor@1.0.0",), instruments, _context())
    first_ref = first.partitions[0].artifacts[0]

    state_b: dict[str, int] = {}
    second = _constant_executor(
        tmp_path, code_label="implementation-b", value=2.0, state=state_b
    ).compute(("stable_factor@1.0.0",), instruments, _context())
    second_ref = second.partitions[0].artifacts[0]
    second_artifact = FeatureCache(tmp_path / "artifacts").load(second_ref.cache_key)

    assert state_b == {"factory_calls": 1, "factor_computes": 1}
    assert second.composite_key != first.composite_key
    assert second_ref.cache_key != first_ref.cache_key
    assert second_ref.content_hash != first_ref.content_hash
    assert second_artifact is not None
    assert second_artifact.table.column("value").to_pylist() == [2.0, 2.0]


def test_composite_identity_binds_full_factor_spec_parameters(tmp_path: Path) -> None:
    first = _constant_executor(
        tmp_path,
        code_label="same-code",
        value=1.0,
        state={},
        parameters={"window": 20, "nested": {"limits": [0.1, 0.9]}},
    ).compute(("stable_factor@1.0.0",), _instruments(2), _context())
    second = _constant_executor(
        tmp_path,
        code_label="same-code",
        value=2.0,
        state={},
        parameters={"window": 60, "nested": {"limits": [0.2, 0.8]}},
    ).compute(("stable_factor@1.0.0",), _instruments(2), _context())

    assert second.composite_key != first.composite_key
    assert (
        second.partitions[0].artifacts[0].cache_key
        != first.partitions[0].artifacts[0].cache_key
    )


def _dependency_executor(
    root: Path, *, dependency_code: str, state: dict[str, int]
) -> PartitionedFactorEngine:
    def factory(
        instruments: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        state["factory_calls"] = state.get("factory_calls", 0) + 1
        registry = FactorRegistry()
        registry.register(
            _ConstantFactor(
                instruments,
                value=1.0,
                state=state,
                factor_id="dependency_factor",
            ),
            code_hash=hashlib.sha256(dependency_code.encode()).hexdigest(),
        )
        registry.register(
            _ConstantFactor(
                instruments,
                value=3.0,
                state=state,
                factor_id="root_factor",
                dependencies=("dependency_factor@1.0.0",),
            ),
            code_hash=hashlib.sha256(b"root-code").hexdigest(),
        )
        return FactorEngine(
            registry, cache, capabilities=ProviderCapabilities.complete()
        )

    return PartitionedFactorEngine(root, factory, max_partition_size=10)


def test_composite_identity_binds_dependency_implementation_hash(
    tmp_path: Path,
) -> None:
    first = _dependency_executor(
        tmp_path, dependency_code="dependency-a", state={}
    ).compute(("root_factor@1.0.0",), _instruments(2), _context())
    second = _dependency_executor(
        tmp_path, dependency_code="dependency-b", state={}
    ).compute(("root_factor@1.0.0",), _instruments(2), _context())

    assert second.composite_key != first.composite_key
    assert second.execution_descriptor_hash != first.execution_descriptor_hash


def test_factor_request_order_canonicalizes_execution_descriptor_and_composite(
    tmp_path: Path,
) -> None:
    state: dict[str, int] = {}

    def factory(
        instruments: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        state["factory_calls"] = state.get("factory_calls", 0) + 1
        registry = FactorRegistry()
        for factor_id, value in (("alpha_factor", 1.0), ("beta_factor", 2.0)):
            registry.register(
                _ConstantFactor(
                    instruments,
                    value=value,
                    state=state,
                    factor_id=factor_id,
                ),
                code_hash=hashlib.sha256(factor_id.encode()).hexdigest(),
            )
        return FactorEngine(
            registry, cache, capabilities=ProviderCapabilities.complete()
        )

    executor = PartitionedFactorEngine(tmp_path, factory, max_partition_size=10)
    first = executor.compute(
        ("beta_factor@1.0.0", "alpha_factor@1.0.0"),
        _instruments(2),
        _context(),
    )
    second = executor.compute(
        ("alpha_factor@1.0.0", "beta_factor@1.0.0"),
        _instruments(2),
        _context(),
    )

    assert second == first
    assert first.factor_refs == ("alpha_factor@1.0.0", "beta_factor@1.0.0")
    assert first.execution_descriptor.requested_refs == first.factor_refs


def test_composite_hit_constructs_each_scope_but_never_computes_factor(
    tmp_path: Path,
) -> None:
    state: dict[str, int] = {}
    executor = _constant_executor(
        tmp_path, code_label="stable", value=1.0, state=state, maximum=2
    )
    first = executor.compute(("stable_factor@1.0.0",), _instruments(5), _context())
    assert state == {"factory_calls": 3, "factor_computes": 3}

    repeated = executor.compute(
        ("stable_factor@1.0.0",), tuple(reversed(_instruments(5))), _context()
    )

    assert repeated == first
    assert state == {"factory_calls": 6, "factor_computes": 3}


def test_partition_scope_descriptor_mismatch_fails_closed_on_hit(
    tmp_path: Path,
) -> None:
    instruments = _instruments(4)
    _constant_executor(
        tmp_path, code_label="stable", value=1.0, state={}, maximum=2
    ).compute(("stable_factor@1.0.0",), instruments, _context())
    state: dict[str, int] = {}

    def inconsistent_factory(
        scope: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        state["factory_calls"] = state.get("factory_calls", 0) + 1
        code_label = "stable" if scope[0] == instruments[0] else "changed"
        registry = FactorRegistry()
        registry.register(
            _ConstantFactor(scope, value=1.0, state=state),
            code_hash=hashlib.sha256(code_label.encode()).hexdigest(),
        )
        return FactorEngine(
            registry, cache, capabilities=ProviderCapabilities.complete()
        )

    with pytest.raises(ValueError, match="execution descriptor"):
        PartitionedFactorEngine(
            tmp_path, inconsistent_factory, max_partition_size=2
        ).compute(("stable_factor@1.0.0",), instruments, _context())

    assert state.get("factor_computes", 0) == 0
    assert state["factory_calls"] == 2


def _legacy_v1_composite_manifest() -> tuple[str, dict[str, JsonValue]]:
    ctx = _context()
    key_payload: dict[str, JsonValue] = {
        "content_hash_contract": "quant-core.ordered-partition-factor-artifacts.v1",
        "end": ctx.end.isoformat(),
        "factor_refs": ["stable_factor@1.0.0"],
        "format_version": 1,
        "instrument_ids": [],
        "max_partition_size": 10,
        "partition_size": 10,
        "snapshot_id": str(ctx.snapshot_id),
        "start": ctx.start.isoformat(),
        "universe_hash": ctx.universe_hash,
    }
    composite_key = hashlib.sha256(canonical_json_bytes(key_payload)).hexdigest()
    manifest = {
        **key_payload,
        "composite_key": composite_key,
        "partitions": [],
    }
    manifest["content_hash"] = hashlib.sha256(
        canonical_json_bytes(manifest)
    ).hexdigest()
    return composite_key, manifest


def test_v1_composite_manifest_is_not_reused_by_descriptor_contract(
    tmp_path: Path,
) -> None:
    v1_key, v1_manifest = _legacy_v1_composite_manifest()
    v1_entry = tmp_path / "composites" / v1_key
    v1_entry.mkdir(parents=True)
    (v1_entry / "manifest.json").write_bytes(canonical_json_bytes(v1_manifest))
    state: dict[str, int] = {}

    current = _constant_executor(
        tmp_path, code_label="current", value=1.0, state=state
    ).compute(("stable_factor@1.0.0",), (), _context())

    assert state == {"factory_calls": 1}
    assert current.composite_key != v1_key
    assert json.loads(current.manifest_path.read_bytes())["format_version"] == 2


def test_manifest_requires_uncorrupted_execution_descriptor(tmp_path: Path) -> None:
    executor = _constant_executor(tmp_path, code_label="stable", value=1.0, state={})
    artifact = executor.compute(("stable_factor@1.0.0",), _instruments(2), _context())
    original = artifact.manifest_path.read_bytes()
    manifest = json.loads(original)
    assert manifest["format_version"] == 2
    assert manifest["execution_descriptor_hash"] == artifact.execution_descriptor_hash

    for field in ("execution_descriptor", "execution_descriptor_hash"):
        corrupted = json.loads(original)
        corrupted.pop(field)
        artifact.manifest_path.write_bytes(canonical_json_bytes(corrupted))
        with pytest.raises(ValueError, match="composite manifest"):
            executor.compute(("stable_factor@1.0.0",), _instruments(2), _context())
        artifact.manifest_path.write_bytes(original)

    corrupted = json.loads(original)
    corrupted["execution_descriptor"]["plan"][0]["code_hash"] = "0" * 64
    artifact.manifest_path.write_bytes(canonical_json_bytes(corrupted))
    with pytest.raises(ValueError, match="composite manifest"):
        executor.compute(("stable_factor@1.0.0",), _instruments(2), _context())


def _executor(
    root: Path, state: dict[str, object], *, maximum: int = 31
) -> PartitionedFactorEngine:
    def factory(
        instruments: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        return _FakeEngine(instruments, cache, state)  # type: ignore[return-value]

    return PartitionedFactorEngine(root, factory, max_partition_size=maximum)


def test_public_partition_api_is_bounded_stable_and_releases_each_partition(
    tmp_path: Path,
) -> None:
    state: dict[str, object] = {}
    executor = _executor(tmp_path, state)
    instruments = _instruments(503)

    artifact = executor.compute(_FACTOR_REFS, tuple(reversed(instruments)), _context())
    gc.collect()

    scopes = [item for call in state["calls"] for item in call]  # type: ignore[union-attr]
    assert scopes == [item.canonical() for item in instruments]
    assert len(artifact.partitions) == 17
    assert max(len(item.instrument_ids) for item in artifact.partitions) <= 31
    assert int(state["peak_live"]) == 5
    assert int(state["live"]) == 0
    assert sum(item.row_count for item in artifact.partitions) == 503 * 5
    assert len({item.partition_id for item in artifact.partitions}) == 17

    repeated = executor.compute(_FACTOR_REFS, instruments, _context())
    assert repeated == artifact
    assert len(state["calls"]) == 17  # type: ignore[arg-type]


def test_partition_size_is_validated_and_part_of_composite_identity(
    tmp_path: Path,
) -> None:
    state: dict[str, object] = {}
    executor = _executor(tmp_path, state, maximum=20)
    instruments = _instruments(41)

    small = executor.compute(_FACTOR_REFS, instruments, _context(), partition_size=7)
    large = executor.compute(_FACTOR_REFS, instruments, _context(), partition_size=20)

    assert small.composite_key != large.composite_key
    assert small.content_hash != large.content_hash
    assert [len(item.instrument_ids) for item in small.partitions] == [7, 7, 7, 7, 7, 6]
    with pytest.raises(ValueError, match="positive"):
        executor.compute(_FACTOR_REFS, instruments, _context(), partition_size=0)
    with pytest.raises(ValueError, match="maximum"):
        executor.compute(_FACTOR_REFS, instruments, _context(), partition_size=21)


def test_product_cache_layout_stays_flat_under_a_long_windows_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / ("bounded-partition-root-" + "x" * 40)
    artifact = _executor(root, {}).compute(_FACTOR_REFS, _instruments(4), _context())

    for partition in artifact.partitions:
        assert partition.partition_id not in artifact.manifest_path.parts
        for reference in partition.artifacts:
            entry = root / "artifacts" / reference.cache_key
            assert entry.parent == root / "artifacts"
            assert (entry / "data.parquet").is_file()
    assert artifact.manifest_path.parent.parent == root / "composites"


def test_empty_single_and_multi_partition_manifests_round_trip(tmp_path: Path) -> None:
    executor = _executor(tmp_path, {})
    empty = executor.compute(_FACTOR_REFS, (), _context())
    single = executor.compute(_FACTOR_REFS, _instruments(3), _context())
    multi = executor.compute(_FACTOR_REFS, _instruments(33), _context())

    assert empty.partitions == ()
    assert len(single.partitions) == 1
    assert len(multi.partitions) == 2
    for artifact in (empty, single, multi):
        manifest = json.loads(artifact.manifest_path.read_text())
        assert manifest["format_version"] == 2
        assert manifest["content_hash_contract"] == artifact.content_hash_contract
        assert manifest["content_hash"] == artifact.content_hash
        assert [item["partition_id"] for item in manifest["partitions"]] == [
            item.partition_id for item in artifact.partitions
        ]


def test_composite_manifest_detects_missing_corrupt_reordered_and_duplicate_refs(
    tmp_path: Path,
) -> None:
    executor = _executor(tmp_path, {})
    artifact = executor.compute(_FACTOR_REFS, _instruments(33), _context())
    original = artifact.manifest_path.read_bytes()

    variants: list[dict[str, object]] = []
    for operation in ("missing", "corrupt", "reordered", "duplicate"):
        manifest = json.loads(original)
        partitions = manifest["partitions"]
        if operation == "missing":
            partitions[0]["artifacts"].pop()
        elif operation == "corrupt":
            partitions[0]["artifacts"][0]["content_hash"] = "0" * 64
        elif operation == "reordered":
            partitions.reverse()
        else:
            partitions.append(partitions[0])
        variants.append(manifest)

    for manifest in variants:
        artifact.manifest_path.write_bytes(
            canonical_json_bytes(cast(JsonValue, manifest))
        )
        with pytest.raises(ValueError, match="composite"):
            executor.compute(_FACTOR_REFS, _instruments(33), _context())
        artifact.manifest_path.write_bytes(original)


def test_missing_referenced_partition_artifact_is_detected(tmp_path: Path) -> None:
    executor = _executor(tmp_path, {})
    artifact = executor.compute(_FACTOR_REFS, _instruments(3), _context())
    reference = artifact.partitions[0].artifacts[0]
    (tmp_path / "artifacts" / reference.cache_key / "data.parquet").unlink()

    with pytest.raises((OSError, ValueError)):
        executor.compute(_FACTOR_REFS, _instruments(3), _context())


def test_mid_run_failure_has_no_visible_composite_and_retry_recovers(
    tmp_path: Path,
) -> None:
    failed_state: dict[str, object] = {}
    factories = 0

    def fail_on_third(
        instruments: tuple[InstrumentId, ...], cache: FeatureCache
    ) -> FactorEngine:
        nonlocal factories
        factories += 1
        if factories == 3:
            raise RuntimeError("injected partition failure")
        return _FakeEngine(instruments, cache, failed_state)  # type: ignore[return-value]

    failing = PartitionedFactorEngine(tmp_path, fail_on_third, max_partition_size=3)
    with pytest.raises(RuntimeError, match="injected"):
        failing.compute(_FACTOR_REFS, _instruments(10), _context())
    assert not [path for path in (tmp_path / "composites").iterdir() if path.is_dir()]

    recovered = _executor(tmp_path, {}, maximum=3).compute(
        _FACTOR_REFS, _instruments(10), _context()
    )
    assert len(recovered.partitions) == 4


def test_composite_conflict_and_atomic_rename_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = _executor(tmp_path / "conflict", {})
    artifact = executor.compute(_FACTOR_REFS, (), _context())
    conflicting = json.loads(artifact.manifest_path.read_bytes())
    conflicting["content_hash"] = "f" * 64
    with pytest.raises(ValueError, match="conflict"):
        executor._composite_cache.publish(conflicting)

    root = tmp_path / "rollback"
    rollback_executor = _executor(root, {})
    rename = Path.rename

    def fail_composite_rename(path: Path, target: Path) -> Path:
        if path.parent.name == "composites":
            raise OSError("injected composite rename failure")
        return rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_composite_rename)
    with pytest.raises(OSError, match="injected"):
        rollback_executor.compute(_FACTOR_REFS, (), _context())
    assert not [path for path in (root / "composites").iterdir() if path.is_dir()]


def test_atomic_composite_publish_is_idempotent_under_same_key_concurrency(
    tmp_path: Path,
) -> None:
    state: dict[str, object] = {}
    executor = _executor(tmp_path, state)

    def run() -> CompositeFactorArtifact:
        return executor.compute(_FACTOR_REFS, _instruments(33), _context())

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(lambda _index: run(), range(2))
    assert first == second
    assert not list((tmp_path / "composites").glob(".*.tmp"))


def test_partition_orchestration_has_no_python_row_or_full_artifact_crossing() -> None:
    source = inspect.getsource(PartitionedFactorEngine.compute)
    forbidden = (".rows(", ".to_dicts(", ".iter_rows(", ".to_pylist(")
    assert all(token not in source for token in forbidden)
    assert "list(artifacts" not in source


class _RecordingMarketBars:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls: list[tuple[str, ...]] = []

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame:
        del snapshot_id, as_of
        assert mode is AdjustmentMode.FORWARD
        canonical = tuple(item.canonical() for item in instruments)
        self.calls.append(canonical)
        return self._frame.lazy().filter(
            pl.col("instrument_id").is_in(canonical)
            & pl.col("trade_date").is_between(start, end, closed="both")
        )


def _market_bars(instruments: tuple[InstrumentId, ...], sessions: int) -> pl.DataFrame:
    days = pl.DataFrame({"_session": pl.int_range(0, sessions, eager=True)}).select(
        (pl.lit(date(2024, 1, 1)) + pl.duration(days=pl.col("_session")))
        .cast(pl.Date)
        .alias("trade_date"),
        pl.col("_session"),
    )
    identifiers = pl.DataFrame(
        {"instrument_id": [item.canonical() for item in instruments]}
    )
    return (
        identifiers.join(days, how="cross")
        .with_columns(
            (
                (pl.col("_session") % 17).cast(pl.Float64) * 0.00001
                + pl.col("instrument_id").str.slice(-3).cast(pl.Float64) * 1e-8
            ).alias(FORWARD_LOG_RETURN_COLUMN),
            pl.col("trade_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("available_at"),
        )
        .select(
            "instrument_id",
            "trade_date",
            "available_at",
            FORWARD_LOG_RETURN_COLUMN,
        )
        .sort("instrument_id", "trade_date")
    )


def test_five_market_factors_share_one_read_and_match_partition_oracle(
    tmp_path: Path,
) -> None:
    instruments = _instruments(503)
    bars = _market_bars(instruments, 122)
    service = _RecordingMarketBars(bars)

    def factory(scope: tuple[InstrumentId, ...], cache: FeatureCache) -> FactorEngine:
        registry = FactorRegistry()
        register_etf_factors(registry, service, scope)
        return FactorEngine(registry, cache, capabilities=BAOSTOCK_CAPABILITIES)

    ctx = FactorContext(
        _SNAPSHOT,
        "8" * 64,
        date(2024, 4, 30),
        date(2024, 5, 1),
    )
    formal = PartitionedFactorEngine(
        tmp_path / "formal", factory, max_partition_size=31
    ).compute(_MARKET_FACTOR_REFS, tuple(reversed(instruments)), ctx)

    assert len(service.calls) == len(formal.partitions) == 17
    assert [
        item for partition in formal.partitions for item in partition.instrument_ids
    ] == [item.canonical() for item in instruments]
    formal_cache = FeatureCache(tmp_path / "formal" / "artifacts")
    hit_call_count = len(service.calls)
    repeated = PartitionedFactorEngine(
        tmp_path / "formal", factory, max_partition_size=31
    ).compute(_MARKET_FACTOR_REFS, instruments, ctx)
    assert repeated == formal
    assert len(service.calls) == hit_call_count
    formal_calls = len(service.calls)
    for partition in formal.partitions:
        scope = tuple(InstrumentId.parse(item) for item in partition.instrument_ids)
        partition_ctx = FactorContext(
            ctx.snapshot_id,
            partition.universe_hash,
            ctx.start,
            ctx.end,
        )
        registry = FactorRegistry()
        register_etf_factors(registry, service, scope)
        oracle = FactorEngine(
            registry,
            FeatureCache(tmp_path / "o" / str(partition.index)),
            capabilities=BAOSTOCK_CAPABILITIES,
        ).compute(_MARKET_FACTOR_REFS, partition_ctx)
        for expected in partition.artifacts:
            actual = formal_cache.load(expected.cache_key)
            assert actual is not None
            assert actual.content_hash == oracle[expected.factor_ref].content_hash
            assert actual.table.equals(oracle[expected.factor_ref].table)
            result = cast(pl.DataFrame, pl.from_arrow(actual.table))
            assert result.get_column("instrument_id").n_unique() == len(scope)
        del oracle
    assert len(service.calls) - formal_calls == len(formal.partitions)


def test_legacy_factor_engine_compute_signature_is_unchanged() -> None:
    signature = inspect.signature(FactorEngine.compute)
    assert tuple(signature.parameters) == ("self", "factor_ids", "ctx")
