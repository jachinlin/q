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
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import (
    FACTOR_OUTPUT_SCHEMA,
    CompositeFactorArtifact,
    FactorArtifact,
    FactorContext,
    FactorEngine,
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
        assert manifest["format_version"] == 1
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
