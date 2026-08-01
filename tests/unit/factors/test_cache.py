"""Tests for content-addressed immutable feature caching."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, time
from inspect import getsource
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_core.data.contracts import ProviderCapabilities, canonical_json_bytes
from quant_core.domain.identifiers import SnapshotId
from quant_core.factors import (
    FACTOR_OUTPUT_SCHEMA,
    FactorArtifact,
    FactorContext,
    FactorEngine,
    FactorRegistry,
    FactorSpec,
    FeatureCache,
    build_cache_key,
)
from quant_core.factors.base import _factor_table_ipc_bytes, factor_table_content_hash


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def make_context(
    *,
    snapshot_id: SnapshotId | None = None,
    universe_hash: str | None = None,
    start: date = date(2025, 1, 2),
    end: date = date(2025, 1, 31),
) -> FactorContext:
    return FactorContext(
        snapshot_id=snapshot_id
        or SnapshotId.parse("12345678-1234-5678-9234-567812345678"),
        universe_hash=universe_hash or digest("universe"),
        start=start,
        end=end,
    )


def make_spec(
    *,
    factor_id: str = "momentum",
    version: str = "1.0.0",
    parameters: dict[str, object] | None = None,
    dependencies: tuple[str, ...] = (),
) -> FactorSpec:
    return FactorSpec(
        factor_id=factor_id,
        version=version,
        frequency="daily",
        lookback_sessions=20,
        dependencies=dependencies,
        direction=1,
        parameters=(
            {"window": 20, "winsorize": {"upper": 0.99}}
            if parameters is None
            else parameters
        ),  # type: ignore[arg-type]
    )


def make_frame(
    *,
    factor_id: str = "momentum",
    factor_version: str = "1.0.0",
    values: tuple[float | None, ...] = (0.2, 0.1),
    validity: tuple[bool, ...] = (True, True),
) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2025, 1, 3), date(2025, 1, 2)],
            "instrument_id": ["SSE:600001", "SSE:600000"],
            "factor_id": [factor_id, factor_id],
            "factor_version": [factor_version, factor_version],
            "value": values,
            "available_at": [
                datetime(2025, 1, 3, 8, tzinfo=UTC),
                datetime(2025, 1, 2, 8, tzinfo=UTC),
            ],
            "is_valid": validity,
        },
        schema=FACTOR_OUTPUT_SCHEMA,
    ).lazy()


def single_factor_frame(
    day: date,
    available_at: datetime | None,
    *,
    value: float | None = 0.1,
    is_valid: bool = True,
) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "trade_date": [day],
            "instrument_id": ["SSE:600000"],
            "factor_id": ["momentum"],
            "factor_version": ["1.0.0"],
            "value": [value],
            "available_at": [available_at],
            "is_valid": [is_valid],
        },
        schema=FACTOR_OUTPUT_SCHEMA,
    ).lazy()


def publish(cache: FeatureCache, frame: pl.LazyFrame | None = None) -> FactorArtifact:
    spec = make_spec(dependencies=("price@1.0.0",))
    ctx = make_context()
    dependencies = {"price@1.0.0": digest("price")}
    code_hash = digest("implementation")
    key = build_cache_key(spec, ctx, code_hash, dependencies)
    return cache.publish(
        key,
        make_frame() if frame is None else frame,
        spec=spec,
        ctx=ctx,
        code_hash=code_hash,
        dependency_hashes=dependencies,
    )


def large_factor_frame(row_count: int) -> pl.LazyFrame:
    """Build many unique keys without allocating Python row objects."""
    return (
        pl.DataFrame({"_row": pl.int_range(0, row_count, eager=True)})
        .select(
            pl.lit(date(2025, 1, 2), dtype=pl.Date).alias("trade_date"),
            pl.concat_str(
                pl.lit("SSE:"), pl.col("_row").cast(pl.String).str.zfill(8)
            ).alias("instrument_id"),
            pl.lit("momentum", dtype=pl.String).alias("factor_id"),
            pl.lit("1.0.0", dtype=pl.String).alias("factor_version"),
            (pl.col("_row") / row_count).cast(pl.Float64).alias("value"),
            pl.lit(
                datetime(2025, 1, 2, 8, tzinfo=UTC),
                dtype=pl.Datetime("us", "UTC"),
            ).alias("available_at"),
            pl.lit(True, dtype=pl.Boolean).alias("is_valid"),
        )
        .reverse()
        .lazy()
    )


def entry_paths(cache: FeatureCache, artifact: FactorArtifact) -> tuple[Path, Path]:
    entry = cache.root / artifact.cache_key
    return entry / "data.parquet", entry / "manifest.json"


def assert_only_persistent_guard_files(root: Path) -> None:
    remaining = list(root.iterdir())
    assert len(remaining) == 1
    assert remaining[0].is_file()
    assert remaining[0].name.endswith(".lock.guard")


def test_cache_key_is_canonical_across_mapping_order() -> None:
    """Equivalent parameter and dependency mappings must share one address."""
    first = make_spec(
        parameters={"window": 20, "winsorize": {"lower": 0.01, "upper": 0.99}},
        dependencies=("price@1.0.0", "quality@1.0.0"),
    )
    second = make_spec(
        parameters={"winsorize": {"upper": 0.99, "lower": 0.01}, "window": 20},
        dependencies=("quality@1.0.0", "price@1.0.0"),
    )
    ctx = make_context()
    first_dependencies = {"quality@1.0.0": digest("q"), "price@1.0.0": digest("p")}
    second_dependencies = {"price@1.0.0": digest("p"), "quality@1.0.0": digest("q")}

    first_key = build_cache_key(first, ctx, digest("code"), first_dependencies)
    second_key = build_cache_key(second, ctx, digest("code"), second_dependencies)

    assert first_key == second_key
    assert len(first_key) == 64
    assert first_key == first_key.lower()


@pytest.mark.parametrize(
    "case",
    [
        "factor_id",
        "version",
        "parameters",
        "code_hash",
        "dependencies",
        "snapshot_id",
        "universe_hash",
        "start",
        "end",
    ],
)
def test_cache_key_changes_when_any_reproducibility_input_changes(case: str) -> None:
    """No PIT scope, implementation, or upstream artifact may alias another entry."""
    spec = make_spec(dependencies=("price@1.0.0",))
    ctx = make_context()
    code_hash = digest("code")
    dependencies = {"price@1.0.0": digest("price")}
    baseline = build_cache_key(spec, ctx, code_hash, dependencies)

    if case == "factor_id":
        spec = make_spec(factor_id="reversal", dependencies=("price@1.0.0",))
    elif case == "version":
        spec = make_spec(version="2.0.0", dependencies=("price@1.0.0",))
    elif case == "parameters":
        spec = make_spec(parameters={"window": 21}, dependencies=("price@1.0.0",))
    elif case == "code_hash":
        code_hash = digest("different-code")
    elif case == "dependencies":
        dependencies = {"price@1.0.0": digest("different-price")}
    elif case == "snapshot_id":
        ctx = make_context(
            snapshot_id=SnapshotId.parse("22345678-1234-5678-9234-567812345678")
        )
    elif case == "universe_hash":
        ctx = make_context(universe_hash=digest("different-universe"))
    elif case == "start":
        ctx = make_context(start=date(2025, 1, 3))
    else:
        ctx = make_context(end=date(2025, 2, 3))

    assert build_cache_key(spec, ctx, code_hash, dependencies) != baseline


@pytest.mark.parametrize(
    "code_hash,dependencies",
    [("not-sha", {}), ("a" * 64, {"price": "b" * 64}), ("a" * 64, {"price@1": "bad"})],
)
def test_cache_key_rejects_unverifiable_hash_or_dependency_identity(
    code_hash: str, dependencies: dict[str, str]
) -> None:
    """Cache identity accepts only canonical refs and lowercase SHA-256 material."""
    with pytest.raises(ValueError, match="hash|factor_id@version"):
        build_cache_key(make_spec(), make_context(), code_hash, dependencies)


def test_publish_sorts_validates_and_round_trips_an_immutable_artifact(
    tmp_path: Path,
) -> None:
    """Only a fully verified exact-schema Parquet becomes visible in the cache."""
    cache = FeatureCache(tmp_path)

    artifact = publish(cache)
    loaded = cache.load(artifact.cache_key)

    assert loaded == artifact
    assert artifact.factor_ref == "momentum@1.0.0"
    assert artifact.snapshot_id == make_context().snapshot_id
    assert artifact.universe_hash == make_context().universe_hash
    assert artifact.row_count == 2
    data_path, manifest_path = entry_paths(cache, artifact)
    assert data_path.is_file()
    assert manifest_path.is_file()
    result = artifact.lazy_frame().collect()
    assert result.schema == FACTOR_OUTPUT_SCHEMA
    assert result.select("trade_date", "instrument_id").rows() == [
        (date(2025, 1, 2), "SSE:600000"),
        (date(2025, 1, 3), "SSE:600001"),
    ]
    assert sorted(path.name for path in data_path.parent.iterdir()) == [
        "data.parquet",
        "manifest.json",
    ]


def test_load_returns_owned_content_when_data_is_replaced_after_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A verified artifact owns read bytes and cannot later follow a replaced path."""
    from quant_core.factors import cache as cache_module

    cache = FeatureCache(tmp_path / "cache")
    published = publish(cache)
    data_path, _ = entry_paths(cache, published)
    read_validated = cache_module._read_validated_parquet
    replaced = False

    def read_then_replace(path: Path, **kwargs: object) -> object:
        nonlocal replaced
        table = read_validated(path, **kwargs)
        if Path(path) == data_path and not replaced:
            replacement = data_path.with_suffix(".replacement")
            replacement.write_bytes(b"corrupt parquet")
            replacement.replace(data_path)
            replaced = True
        return table

    monkeypatch.setattr(cache_module, "_read_validated_parquet", read_then_replace)

    artifact = cache.load(published.cache_key)

    assert artifact is not None
    assert (
        artifact.lazy_frame()
        .collect()
        .equals(
            make_frame()
            .collect()
            .sort("trade_date", "instrument_id", "factor_id", "factor_version")
        )
    )
    assert not hasattr(artifact, "data_path")
    assert not hasattr(artifact, "manifest_path")
    with pytest.raises(ValueError, match="Parquet"):
        cache.load(published.cache_key)


def test_artifact_owned_arrow_buffers_are_read_only(tmp_path: Path) -> None:
    """Public artifact content cannot be mutated through Arrow buffer views."""
    artifact = publish(FeatureCache(tmp_path))

    buffers = [
        buffer
        for column in artifact.table.columns
        for chunk in column.chunks
        for buffer in chunk.buffers()
        if buffer is not None
    ]

    assert buffers
    assert all(not buffer.is_mutable for buffer in buffers)


def test_load_returns_owned_content_when_manifest_is_replaced_after_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Manifest replacement cannot retarget an already materialized artifact."""
    cache = FeatureCache(tmp_path / "cache")
    published = publish(cache)
    _, manifest_path = entry_paths(cache, published)
    read_bytes = Path.read_bytes
    replaced = False

    def read_then_replace(path: Path) -> bytes:
        nonlocal replaced
        contents = read_bytes(path)
        if path == manifest_path and not replaced:
            replacement = manifest_path.with_suffix(".replacement")
            replacement.write_bytes(b"{}")
            replacement.replace(manifest_path)
            replaced = True
        return contents

    monkeypatch.setattr(Path, "read_bytes", read_then_replace)

    artifact = cache.load(published.cache_key)

    assert artifact is not None
    assert artifact.lazy_frame().collect().height == 2
    assert not hasattr(artifact, "data_path")
    assert not hasattr(artifact, "manifest_path")
    with pytest.raises(ValueError, match="manifest"):
        cache.load(published.cache_key)


@pytest.mark.parametrize("filename", ["data.parquet", "manifest.json"])
def test_load_rejects_cache_files_with_additional_hardlinks(
    tmp_path: Path, filename: str
) -> None:
    """A cache file with another filesystem name is not immutable provenance."""
    cache = FeatureCache(tmp_path / "cache")
    artifact = publish(cache)
    entry = cache.root / artifact.cache_key
    os.link(entry / filename, tmp_path / f"alias-{filename.replace('.', '-')}")

    with pytest.raises(ValueError, match="hard link"):
        cache.load(artifact.cache_key)


def test_load_hashes_the_table_returned_by_the_parquet_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A path race cannot validate one table while returning different content."""
    from quant_core.factors import cache as cache_module

    cache = FeatureCache(tmp_path)
    artifact = publish(cache)
    different = (
        make_frame(values=(0.3, 0.1))
        .collect()
        .sort("trade_date", "instrument_id", "factor_id", "factor_version")
        .to_arrow()
    )
    monkeypatch.setattr(
        cache_module,
        "_read_validated_parquet",
        lambda path, **kwargs: different,
    )

    with pytest.raises(ValueError, match="integrity metadata differs"):
        cache.load(artifact.cache_key)


def test_publish_canonicalizes_arrow_chunks_before_content_hashing(
    tmp_path: Path,
) -> None:
    """Physical chunk layout must not create a false post-Parquet integrity failure."""

    def part(day: int) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "trade_date": [date(2025, 1, day)],
                "instrument_id": [f"SSE:60000{day}"],
                "factor_id": ["momentum"],
                "factor_version": ["1.0.0"],
                "value": [float(day)],
                "available_at": [datetime(2025, 1, day, 8, tzinfo=UTC)],
                "is_valid": [True],
            },
            schema=FACTOR_OUTPUT_SCHEMA,
        )

    multi_chunk_frame = pl.concat([part(2), part(3)], rechunk=False)
    assert multi_chunk_frame["trade_date"].n_chunks() == 2

    artifact = publish(FeatureCache(tmp_path), multi_chunk_frame.lazy())

    assert artifact.row_count == 2


def test_factor_content_hash_normalizes_null_buffers_without_python_row_lists() -> None:
    """Research-size artifacts must not create one Python object per cell to hash."""
    assert "to_pylist" not in getsource(_factor_table_ipc_bytes)


def test_factor_content_hash_matches_the_legacy_generic_arrow_contract() -> None:
    """Nested and binary Arrow values keep the prior logical hash semantics."""
    dictionary_type = pa.dictionary(pa.int8(), pa.string())
    dictionary = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None, 0], type=pa.int8()), pa.array(["alpha", "beta"])
    )
    table = pa.table(
        {
            "large_text": pa.array(["x", None, "y", "z"], type=pa.large_string()),
            "binary": pa.array([b"x", None, b"y", b"z"], type=pa.binary()),
            "numbers": pa.array([[1, None], None, [2], []], type=pa.list_(pa.int32())),
            "record": pa.array(
                [
                    {"number": 1, "label": "x"},
                    None,
                    {"number": 2, "label": None},
                    {"number": None, "label": "z"},
                ],
                type=pa.struct([("number", pa.int32()), ("label", pa.string())]),
            ),
            "category": dictionary,
        },
        schema=pa.schema(
            [
                ("large_text", pa.large_string()),
                ("binary", pa.binary()),
                ("numbers", pa.list_(pa.int32())),
                ("record", pa.struct([("number", pa.int32()), ("label", pa.string())])),
                ("category", dictionary_type),
            ]
        ),
    )

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)
    assert factor_table_content_hash(table) == _legacy_generic_factor_hash(table)


def test_factor_content_hash_ignores_chunk_slice_offset_and_dictionary_encoding() -> (
    None
):
    """Equivalent Arrow logical values must not inherit physical layout identity."""
    dictionary_type = pa.dictionary(pa.int8(), pa.string())
    first = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, None, 0], type=pa.int8()), pa.array(["alpha", "beta"])
    )
    second = pa.DictionaryArray.from_arrays(
        pa.array([1, 0, None, 1], type=pa.int8()), pa.array(["beta", "alpha"])
    )
    expected = pa.table(
        {
            "value": pa.array(["alpha", "beta", None, "alpha"]),
            "category": first,
        },
        schema=pa.schema([("value", pa.string()), ("category", dictionary_type)]),
    )
    physical_variant = pa.table(
        {
            "value": pa.chunked_array(
                [
                    pa.array(["ignored", "alpha", "beta"])[1:],
                    pa.array([None, "alpha", "ignored"])[0:2],
                ]
            ),
            "category": pa.chunked_array([second.slice(0, 2), second.slice(2)]),
        },
        schema=expected.schema,
    )

    assert factor_table_content_hash(physical_variant) == factor_table_content_hash(
        expected
    )


def test_factor_content_hash_matches_legacy_nested_dictionary_bytes() -> None:
    """A dictionary child beneath a nullable struct remains generic Arrow content."""
    category = pa.dictionary(pa.int8(), pa.string())
    nested_type = pa.struct([("category", category), ("weight", pa.float64())])
    table = pa.table(
        {
            "nested": pa.array(
                [
                    {"category": "alpha", "weight": 1.0},
                    None,
                    {"category": None, "weight": 2.0},
                ],
                type=nested_type,
            )
        }
    )

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_ignores_dictionary_values_hidden_by_parent_nulls() -> None:
    """A hidden child category must not enter the canonical dictionary encoding."""
    category_type = pa.dictionary(pa.int8(), pa.string())
    category = pa.array(["hidden", "alpha"], type=category_type)
    nested = pa.StructArray.from_arrays(
        [category, pa.array([0.0, 1.0])],
        fields=[pa.field("category", category_type), pa.field("weight", pa.float64())],
        mask=pa.array([True, False]),
    )
    table = pa.table({"nested": nested})

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_ignores_fixed_list_dictionary_values_hidden_by_parent_nulls() -> (
    None
):
    """A nullable struct must hide categories stored in its fixed-list child."""
    category_type = pa.dictionary(pa.int8(), pa.string())
    fixed_type = pa.list_(category_type, 2)
    categories = pa.array(["hidden-a", "hidden-b", "alpha", "beta"], type=category_type)
    fixed = pa.FixedSizeListArray.from_arrays(categories, 2)
    nested = pa.StructArray.from_arrays(
        [fixed],
        fields=[pa.field("categories", fixed_type)],
        mask=pa.array([True, False]),
    )
    table = pa.table({"nested": nested})

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_propagates_ancestor_nulls_through_nested_fixed_lists() -> (
    None
):
    """A null struct must hide categories beneath every fixed-list level."""
    category_type = pa.dictionary(pa.int8(), pa.string())
    inner_type = pa.list_(category_type, 2)
    outer_type = pa.list_(inner_type, 2)
    categories = pa.array(["hidden-a", "hidden-b", "alpha", "beta"], type=category_type)
    inner = pa.FixedSizeListArray.from_arrays(categories, 2)
    outer = pa.FixedSizeListArray.from_arrays(inner, 2)
    nested = pa.StructArray.from_arrays(
        [outer],
        fields=[pa.field("categories", outer_type)],
        mask=pa.array([True]),
    )
    table = pa.table({"nested": nested})

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_matches_legacy_three_level_fixed_list_nulls() -> None:
    """Own fixed-list nulls and ancestor nulls retain distinct leaf encodings."""
    category_type = pa.dictionary(pa.int8(), pa.string())
    inner_type = pa.list_(category_type, 2)
    middle_type = pa.list_(inner_type, 2)
    outer_type = pa.list_(middle_type, 2)
    categories = pa.array(
        [f"category-{index}" for index in range(24)], type=category_type
    )
    inner = pa.FixedSizeListArray.from_arrays(categories, 2)
    middle = pa.FixedSizeListArray.from_arrays(inner, 2)
    outer = pa.FixedSizeListArray.from_arrays(
        middle, 2, mask=pa.array([False, True, False])
    )
    nested = pa.StructArray.from_arrays(
        [outer],
        fields=[pa.field("categories", outer_type)],
        mask=pa.array([True, False, False]),
    )
    table = pa.table({"nested": nested})

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_matches_legacy_chunked_mixed_nested_fixed_lists() -> None:
    """Nested fixed lists recurse through list, struct, map, slices, and chunks."""
    category_type = pa.dictionary(pa.int8(), pa.string())
    fixed_type = pa.list_(pa.list_(category_type, 2), 2)
    item_type = pa.struct(
        [
            pa.field("categories", fixed_type),
            pa.field("mapping", pa.map_(pa.string(), category_type)),
        ]
    )
    list_type = pa.list_(item_type)
    outer_type = pa.struct([pa.field("items", list_type)])

    def make_chunk(
        labels: list[str],
        outer_mask: list[bool] | None,
        fixed_mask: list[bool] | None,
    ) -> pa.StructArray:
        categories = pa.array(labels, type=category_type)
        inner = pa.FixedSizeListArray.from_arrays(categories, 2)
        fixed = pa.FixedSizeListArray.from_arrays(
            inner,
            2,
            mask=None if fixed_mask is None else pa.array(fixed_mask),
        )
        mapping = pa.array(
            [
                [("a", labels[0])],
                [("b", labels[4])],
                [("c", labels[8])],
            ],
            type=item_type[1].type,
        )
        items = pa.StructArray.from_arrays([fixed, mapping], fields=list(item_type))
        lists = pa.ListArray.from_arrays(pa.array([0, 1, 2, 3]), items)
        return pa.StructArray.from_arrays(
            [lists],
            fields=list(outer_type),
            mask=None if outer_mask is None else pa.array(outer_mask),
        )

    prefix = make_chunk(
        [
            "ancestor-a",
            "ancestor-b",
            "ancestor-c",
            "ancestor-d",
            "own-a",
            "own-b",
            "own-c",
            "own-d",
            "visible-a",
            "visible-b",
            "visible-c",
            "visible-d",
        ],
        [True, False, False],
        [False, True, False],
    )
    suffix = make_chunk(
        [
            "slice-a",
            "slice-b",
            "slice-c",
            "slice-d",
            "unused-a",
            "unused-b",
            "unused-c",
            "unused-d",
            "unused-e",
            "unused-f",
            "unused-g",
            "unused-h",
        ],
        None,
        None,
    )
    assert suffix.buffers()[0] is None
    chunked = pa.chunked_array(
        [prefix.slice(0, 3), suffix.slice(0, 1)], type=outer_type
    )
    table = pa.table({"nested": chunked})

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_propagates_parent_nulls_through_deep_chunked_fixed_lists() -> (
    None
):
    """Ancestor nulls survive deeper structs, sliced chunks, and absent null buffers."""
    category_type = pa.dictionary(pa.int8(), pa.string())
    fixed_type = pa.list_(category_type, 2)
    inner_type = pa.struct([pa.field("categories", fixed_type)])
    outer_type = pa.struct([pa.field("inner", inner_type)])

    def make_chunk(
        values: list[str],
        outer_mask: list[bool] | None,
        inner_mask: list[bool] | None,
    ) -> pa.StructArray:
        categories = pa.array(values, type=category_type)
        fixed = pa.FixedSizeListArray.from_arrays(categories, 2)
        inner = pa.StructArray.from_arrays(
            [fixed],
            fields=list(inner_type),
            mask=None if inner_mask is None else pa.array(inner_mask),
        )
        return pa.StructArray.from_arrays(
            [inner],
            fields=list(outer_type),
            mask=None if outer_mask is None else pa.array(outer_mask),
        )

    prefix = make_chunk(
        ["ignored-a", "ignored-b", "hidden-a", "hidden-b", "alpha", "beta"],
        [False, True, False],
        [False, False, False],
    )
    suffix = make_chunk(
        ["gamma", "delta", "ignored-c", "ignored-d"],
        None,
        None,
    )
    assert suffix.buffers()[0] is None
    assert suffix.field(0).buffers()[0] is None
    chunked = pa.chunked_array([prefix.slice(1), suffix.slice(0, 1)], type=outer_type)
    table = pa.table({"nested": chunked})

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_matches_legacy_fixed_list_and_map_bytes() -> None:
    """Fixed-size lists and maps retain exact pre-optimization IPC semantics."""
    fixed_type = pa.list_(pa.int32(), 2)
    map_type = pa.map_(pa.string(), pa.int32())
    table = pa.table(
        {
            "fixed": pa.array([[1, None], None, [2, 3]], type=fixed_type),
            "mapping": pa.array(
                [[("a", 1), ("b", None)], None, [("c", 3)]], type=map_type
            ),
        }
    )

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def test_factor_content_hash_matches_legacy_recursive_nested_bytes() -> None:
    """List/struct/dictionary/map combinations recurse without Python rows."""
    category = pa.dictionary(pa.int8(), pa.string())
    item = pa.struct(
        [
            ("category", category),
            ("attributes", pa.map_(pa.string(), pa.int32())),
        ]
    )
    table = pa.table(
        {
            "nested": pa.array(
                [
                    [
                        {"category": "alpha", "attributes": [("x", 1)]},
                        None,
                    ],
                    None,
                    [{"category": None, "attributes": [("y", None)]}],
                ],
                type=pa.list_(item),
            )
        }
    )

    assert _factor_table_ipc_bytes(table) == _legacy_generic_factor_bytes(table)


def _legacy_generic_factor_hash(table: pa.Table) -> str:
    """Reference the pre-performance generic logical Arrow canonicalization."""
    return hashlib.sha256(_legacy_generic_factor_bytes(table)).hexdigest()


def _legacy_generic_factor_bytes(table: pa.Table) -> bytes:
    """Return the exact prior IPC representation without using production code."""
    combined = table.combine_chunks()
    canonical = pa.table(
        [
            pa.array(combined.column(index).to_pylist(), type=field.type)
            for index, field in enumerate(combined.schema)
        ],
        schema=combined.schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, canonical.schema) as writer:
        writer.write_table(canonical)
    return sink.getvalue().to_pybytes()


def test_publish_manifest_is_canonical_and_binds_every_cache_key_input(
    tmp_path: Path,
) -> None:
    """The publication marker must audit the complete content address."""
    cache = FeatureCache(tmp_path)
    artifact = publish(cache)
    _, manifest_path = entry_paths(cache, artifact)

    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)

    assert b" " not in raw
    assert raw == json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert manifest["cache_key"] == artifact.cache_key
    assert manifest["factor_id"] == "momentum"
    assert manifest["factor_version"] == "1.0.0"
    assert manifest["parameters"] == {"window": 20, "winsorize": {"upper": 0.99}}
    assert manifest["code_hash"] == digest("implementation")
    assert manifest["dependency_hashes"] == {"price@1.0.0": digest("price")}
    assert manifest["snapshot_id"] == "12345678-1234-5678-9234-567812345678"
    assert manifest["universe_hash"] == digest("universe")
    assert manifest["start"] == "2025-01-02"
    assert manifest["end"] == "2025-01-31"
    assert manifest["data_path"] == "data.parquet"
    assert manifest["row_count"] == 2
    assert manifest["content_hash"] == artifact.content_hash


def test_republishing_same_key_with_different_content_fails_without_overwrite(
    tmp_path: Path,
) -> None:
    """A nondeterministic recomputation must conflict with the completed entry."""
    cache = FeatureCache(tmp_path)
    first = publish(cache)
    data_path, manifest_path = entry_paths(cache, first)
    original_data = data_path.read_bytes()
    original_manifest = manifest_path.read_bytes()

    with pytest.raises(ValueError, match="conflict"):
        publish(cache, make_frame(values=(0.3, 0.1)))

    assert data_path.read_bytes() == original_data
    assert manifest_path.read_bytes() == original_manifest


def test_concurrent_same_key_same_content_publishers_are_idempotent(
    tmp_path: Path,
) -> None:
    """The shared token lock serializes identical feature publishers safely."""
    cache = FeatureCache(tmp_path)
    barrier = threading.Barrier(2)

    def publish_after_barrier() -> FactorArtifact:
        barrier.wait(timeout=5)
        return publish(cache)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish_after_barrier) for _ in range(2)]
        artifacts = [future.result(timeout=10) for future in futures]

    assert artifacts[0] == artifacts[1]
    assert artifacts[0].lazy_frame().collect().height == 2
    assert sorted(path.name for path in cache.root.iterdir() if path.is_dir()) == [
        artifacts[0].cache_key
    ]


def test_concurrent_same_key_different_content_has_one_winner_and_one_conflict(
    tmp_path: Path,
) -> None:
    """Nondeterministic concurrent output cannot overwrite the winning artifact."""
    cache = FeatureCache(tmp_path)
    barrier = threading.Barrier(2)

    def publish_after_barrier(frame: pl.LazyFrame) -> FactorArtifact:
        barrier.wait(timeout=5)
        return publish(cache, frame)

    frames = [make_frame(values=(0.2, 0.1)), make_frame(values=(0.3, 0.1))]
    artifacts: list[FactorArtifact] = []
    conflicts: list[ValueError] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish_after_barrier, frame) for frame in frames]
        for future in futures:
            try:
                artifacts.append(future.result(timeout=10))
            except ValueError as error:
                conflicts.append(error)

    assert len(artifacts) == 1
    assert len(conflicts) == 1
    assert "conflict" in str(conflicts[0])
    loaded = cache.load(artifacts[0].cache_key)
    assert loaded == artifacts[0]


def test_publish_failure_never_leaves_a_visible_or_temporary_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed Parquet write cannot register partial cache state."""
    sink_parquet = pl.LazyFrame.sink_parquet

    def write_then_fail(frame: pl.LazyFrame, *args: object, **kwargs: object) -> None:
        sink_parquet(frame, *args, **kwargs)  # type: ignore[arg-type]
        raise OSError("injected feature write failure")

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", write_then_fail)

    with pytest.raises(OSError, match="injected feature write failure"):
        publish(FeatureCache(tmp_path))

    assert_only_persistent_guard_files(tmp_path)


def test_publish_streams_canonical_parquet_in_bounded_row_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publication must not convert the complete result through an eager Arrow table."""
    row_count = 65_537

    def forbidden_to_arrow(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("feature publication materialized a complete Arrow table")

    monkeypatch.setattr(pl.DataFrame, "to_arrow", forbidden_to_arrow)

    artifact = publish(FeatureCache(tmp_path), large_factor_frame(row_count))
    data_path, _ = entry_paths(FeatureCache(tmp_path), artifact)
    parquet = pq.ParquetFile(data_path)

    assert artifact.row_count == row_count
    assert parquet.num_row_groups == 2
    assert (
        max(
            parquet.metadata.row_group(index).num_rows
            for index in range(parquet.num_row_groups)
        )
        <= 65_536
    )


def test_load_uses_iter_batches_and_rejects_cross_batch_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Canonical primary keys must remain strictly increasing between batches."""
    from quant_core.factors import cache as cache_module

    row_count = 65_537
    artifact = publish(FeatureCache(tmp_path), large_factor_frame(row_count))
    data_path, _ = entry_paths(FeatureCache(tmp_path), artifact)
    table = pq.read_table(data_path)
    duplicate = pa.concat_tables(
        [table.slice(0, 65_536), table.slice(65_535, 1)],
        promote_options="none",
    )
    pq.write_table(duplicate, data_path, row_group_size=65_536)
    calls = 0
    parquet_file = cache_module.pq.ParquetFile

    class RecordingParquetFile:
        def __init__(self, path: Path) -> None:
            self._delegate = parquet_file(path)
            self.schema_arrow = self._delegate.schema_arrow

        def iter_batches(self, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            return self._delegate.iter_batches(**kwargs)

    monkeypatch.setattr(cache_module.pq, "ParquetFile", RecordingParquetFile)

    with pytest.raises(ValueError, match="Parquet"):
        FeatureCache(tmp_path).load(artifact.cache_key)
    assert calls == 1


def test_streamed_parquet_validation_rejects_schema_change_between_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every yielded batch must retain the file's exact factor schema."""
    from quant_core.factors import cache as cache_module

    artifact = publish(FeatureCache(tmp_path), large_factor_frame(65_537))
    parquet_file = cache_module.pq.ParquetFile

    class SchemaChangingParquetFile:
        def __init__(self, path: Path) -> None:
            self._delegate = parquet_file(path)
            self.schema_arrow = self._delegate.schema_arrow

        def iter_batches(self, **kwargs: object) -> object:
            for index, batch in enumerate(self._delegate.iter_batches(**kwargs)):
                if index == 1:
                    yield batch.rename_columns(
                        [*batch.schema.names[:-1], "changed_is_valid"]
                    )
                else:
                    yield batch

    monkeypatch.setattr(cache_module.pq, "ParquetFile", SchemaChangingParquetFile)

    with pytest.raises(ValueError, match="Parquet"):
        FeatureCache(tmp_path).load(artifact.cache_key)


def test_directory_flush_failure_before_rename_leaves_no_visible_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unflushed staging metadata cannot be promoted into the cache namespace."""
    from quant_core.factors import cache as cache_module

    def fail_directory_flush(path: Path) -> None:
        raise OSError(f"injected directory flush failure: {path.name}")

    monkeypatch.setattr(
        cache_module, "_fsync_directory", fail_directory_flush, raising=False
    )

    with pytest.raises(OSError, match="injected directory flush failure"):
        publish(FeatureCache(tmp_path))

    assert_only_persistent_guard_files(tmp_path)


def test_publish_recovers_stale_same_key_staging_after_dead_owner(
    tmp_path: Path,
) -> None:
    """A pre-rename crash leaves no permanent garbage or cache ambiguity."""
    cache = FeatureCache(tmp_path)
    spec = make_spec(dependencies=("price@1.0.0",))
    ctx = make_context()
    dependencies = {"price@1.0.0": digest("price")}
    key = build_cache_key(spec, ctx, digest("implementation"), dependencies)
    from quant_core.factors.cache import _compact_sha256

    stale = tmp_path / f".{_compact_sha256(key)}.{'0' * 16}.tmp"
    stale.mkdir()
    (stale / "data.parquet").write_bytes(b"interrupted")
    (stale / "manifest.json").write_bytes(b"interrupted")

    artifact = publish(cache)

    assert artifact.cache_key == key
    assert not stale.exists()
    assert sorted(path.name for path in tmp_path.iterdir() if path.is_dir()) == [key]


def test_retry_flushes_a_complete_entry_after_post_rename_flush_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retry makes a complete but not-yet-durable rename explicitly durable."""
    from quant_core.factors import cache as cache_module

    cache = FeatureCache(tmp_path)
    root_flush_attempts = 0

    def fail_first_root_flush(path: Path) -> None:
        nonlocal root_flush_attempts
        if path == cache.root:
            root_flush_attempts += 1
            if root_flush_attempts == 1:
                raise OSError("injected post-rename root flush failure")

    monkeypatch.setattr(cache_module, "_fsync_directory", fail_first_root_flush)

    with pytest.raises(OSError, match="post-rename root flush failure"):
        publish(cache)

    recovered = publish(cache)

    assert recovered.lazy_frame().collect().height == 2
    assert root_flush_attempts == 2


def test_publish_rejects_wrong_schema_without_registering_an_entry(
    tmp_path: Path,
) -> None:
    """A missing required column cannot become a cache hit."""
    malformed = make_frame().collect().drop("available_at").lazy()

    with pytest.raises(ValueError, match="schema"):
        publish(FeatureCache(tmp_path), malformed)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "frame,expected",
    [
        (
            pl.concat([make_frame(), make_frame()]),
            "duplicate",
        ),
        (make_frame(factor_id="reversal"), "factor_id"),
        (make_frame(factor_version="2.0.0"), "factor_version"),
        (make_frame(values=(math.inf, 0.1)), "finite"),
        (make_frame(values=(None, 0.1)), "finite"),
    ],
)
def test_publish_rejects_semantically_invalid_factor_rows(
    tmp_path: Path, frame: pl.LazyFrame, expected: str
) -> None:
    """Correct dtypes cannot hide invalid identity, keys, or valid values."""
    with pytest.raises(ValueError, match=expected):
        publish(FeatureCache(tmp_path), frame)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("day", "available_at", "expected"),
    [
        (
            date(2025, 1, 1),
            datetime(2025, 1, 1, 8, tzinfo=UTC),
            "range",
        ),
        (
            date(2025, 2, 1),
            datetime(2025, 2, 1, 8, tzinfo=UTC),
            "range",
        ),
        (
            date(2025, 1, 2),
            datetime(2025, 1, 2, 16, tzinfo=UTC),
            "available_at|future",
        ),
    ],
)
def test_publish_rejects_rows_outside_context_or_after_shanghai_day_end(
    tmp_path: Path,
    day: date,
    available_at: datetime,
    expected: str,
) -> None:
    """Context range and Shanghai-local PIT cutoff are mandatory cache inputs."""
    with pytest.raises(ValueError, match=expected):
        publish(FeatureCache(tmp_path), single_factor_frame(day, available_at))

    assert list(tmp_path.iterdir()) == []


def test_publish_accepts_availability_exactly_at_shanghai_day_end(
    tmp_path: Path,
) -> None:
    """The inclusive Shanghai end-of-day boundary remains a valid observation."""
    day = date(2025, 1, 2)
    boundary = datetime.combine(
        day, time.max, tzinfo=ZoneInfo("Asia/Shanghai")
    ).astimezone(UTC)

    artifact = publish(
        FeatureCache(tmp_path),
        single_factor_frame(day, boundary),
    )

    assert artifact.lazy_frame().collect()["is_valid"].item() is True


@pytest.mark.parametrize(
    "column,dtype",
    [
        ("trade_date", pl.Date),
        ("instrument_id", pl.String),
        ("factor_id", pl.String),
        ("factor_version", pl.String),
        ("available_at", pl.Datetime("us", "UTC")),
        ("is_valid", pl.Boolean),
    ],
)
def test_publish_rejects_null_identity_and_audit_fields(
    tmp_path: Path, column: str, dtype: pl.DataType
) -> None:
    """Null keys, constant identity, availability, or validity cannot be audited."""
    frame = (
        make_frame()
        .with_columns(pl.lit(None, dtype=dtype).alias(column))
        .select(FACTOR_OUTPUT_SCHEMA.names())
    )

    with pytest.raises(ValueError, match="null"):
        publish(FeatureCache(tmp_path), frame)

    assert list(tmp_path.iterdir()) == []


def test_publish_allows_null_values_only_when_marked_invalid(tmp_path: Path) -> None:
    """Unavailable observations remain representable without claiming a valid number."""
    frame = make_frame(values=(None, 0.1), validity=(False, True))

    artifact = publish(FeatureCache(tmp_path), frame)

    assert artifact.row_count == 2


@pytest.mark.parametrize("invalid_value", [42.0, math.nan])
def test_publish_rejects_unknown_availability_with_non_null_invalid_value(
    tmp_path: Path, invalid_value: float
) -> None:
    """Unknown availability is legal only for a null invalid observation."""
    frame = (
        make_frame(values=(invalid_value, 0.1), validity=(False, True))
        .with_columns(
            pl.when(pl.col("trade_date") == date(2025, 1, 3))
            .then(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
            .otherwise(pl.col("available_at"))
            .alias("available_at")
        )
        .select(FACTOR_OUTPUT_SCHEMA.names())
    )

    with pytest.raises(ValueError, match="available_at|null"):
        publish(FeatureCache(tmp_path), frame)

    assert list(tmp_path.iterdir()) == []


def test_invalid_null_with_unknown_availability_round_trips_through_manifest(
    tmp_path: Path,
) -> None:
    """Capability-missing rows remain valid cache content across publish and load."""
    frame = (
        make_frame(values=(None, 0.1), validity=(False, True))
        .with_columns(
            pl.when(pl.col("trade_date") == date(2025, 1, 3))
            .then(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
            .otherwise(pl.col("available_at"))
            .alias("available_at")
        )
        .select(FACTOR_OUTPUT_SCHEMA.names())
    )
    cache = FeatureCache(tmp_path)

    artifact = publish(cache, frame)
    loaded = cache.load(artifact.cache_key)

    assert loaded is not None
    assert loaded.content_hash == artifact.content_hash
    assert loaded.lazy_frame().collect().filter(~pl.col("is_valid")).row(0) == (
        date(2025, 1, 3),
        "SSE:600001",
        "momentum",
        "1.0.0",
        None,
        None,
        False,
    )


@pytest.mark.parametrize(
    ("day", "available_at", "expected"),
    [
        (
            date(2025, 1, 1),
            datetime(2025, 1, 1, 8, tzinfo=UTC),
            "range",
        ),
        (
            date(2025, 1, 2),
            datetime(2025, 1, 2, 16, tzinfo=UTC),
            "available_at|future",
        ),
    ],
)
def test_factor_artifact_direct_construction_enforces_scope_and_pit(
    day: date,
    available_at: datetime,
    expected: str,
) -> None:
    """Direct artifact construction cannot bypass cache publish validation."""
    table = single_factor_frame(day, available_at).collect().to_arrow()

    with pytest.raises(ValueError, match=expected):
        FactorArtifact(
            factor_ref="momentum@1.0.0",
            cache_key=digest("cache"),
            content_hash=factor_table_content_hash(table),
            row_count=table.num_rows,
            snapshot_id=make_context().snapshot_id,
            universe_hash=digest("universe"),
            start=date(2025, 1, 2),
            end=date(2025, 1, 31),
            table=table,
        )


@pytest.mark.parametrize(
    ("column", "dtype"),
    [("trade_date", pl.Date), ("is_valid", pl.Boolean)],
)
def test_factor_artifact_direct_construction_rejects_null_required_fields(
    column: str,
    dtype: pl.DataType,
) -> None:
    """Nullable Arrow schema fields cannot bypass required output identities."""
    table = (
        single_factor_frame(
            date(2025, 1, 2),
            datetime(2025, 1, 2, 8, tzinfo=UTC),
        )
        .with_columns(pl.lit(None, dtype=dtype).alias(column))
        .select(FACTOR_OUTPUT_SCHEMA.names())
        .collect()
        .to_arrow()
    )

    with pytest.raises(ValueError, match="identity and audit fields.*null"):
        FactorArtifact(
            factor_ref="momentum@1.0.0",
            cache_key=digest("cache"),
            content_hash=factor_table_content_hash(table),
            row_count=table.num_rows,
            snapshot_id=make_context().snapshot_id,
            universe_hash=digest("universe"),
            start=date(2025, 1, 2),
            end=date(2025, 1, 31),
            table=table,
        )


def test_factor_artifact_direct_construction_preserves_empty_table() -> None:
    """A zero-row exact-schema factor remains a legal artifact."""
    table = (
        single_factor_frame(
            date(2025, 1, 2),
            datetime(2025, 1, 2, 8, tzinfo=UTC),
        )
        .limit(0)
        .collect()
        .to_arrow()
    )

    artifact = FactorArtifact(
        factor_ref="momentum@1.0.0",
        cache_key=digest("cache"),
        content_hash=factor_table_content_hash(table),
        row_count=0,
        snapshot_id=make_context().snapshot_id,
        universe_hash=digest("universe"),
        start=date(2025, 1, 2),
        end=date(2025, 1, 31),
        table=table,
    )

    assert artifact.row_count == 0


@pytest.mark.parametrize(
    ("value", "available_at", "is_valid", "expected"),
    [
        (None, datetime(2025, 1, 2, 8, tzinfo=UTC), True, "finite"),
        (math.nan, datetime(2025, 1, 2, 8, tzinfo=UTC), True, "finite"),
        (1.0, None, True, "available_at|null"),
        (1.0, None, False, "available_at|null"),
    ],
)
def test_factor_artifact_direct_construction_enforces_valid_row_contract(
    value: float | None,
    available_at: datetime | None,
    is_valid: bool,
    expected: str,
) -> None:
    """Direct construction retains the cache's finite/known-availability contract."""
    table = (
        single_factor_frame(
            date(2025, 1, 2),
            available_at,
            value=value,
            is_valid=is_valid,
        )
        .collect()
        .to_arrow()
    )

    with pytest.raises(ValueError, match=expected):
        FactorArtifact(
            factor_ref="momentum@1.0.0",
            cache_key=digest("cache"),
            content_hash=factor_table_content_hash(table),
            row_count=table.num_rows,
            snapshot_id=make_context().snapshot_id,
            universe_hash=digest("universe"),
            start=date(2025, 1, 2),
            end=date(2025, 1, 31),
            table=table,
        )


def test_factor_artifact_direct_construction_preserves_capability_missing_row() -> None:
    """An invalid null row with unknown availability remains a legal artifact."""
    table = (
        single_factor_frame(
            date(2025, 1, 2),
            None,
            value=None,
            is_valid=False,
        )
        .collect()
        .to_arrow()
    )

    artifact = FactorArtifact(
        factor_ref="momentum@1.0.0",
        cache_key=digest("cache"),
        content_hash=factor_table_content_hash(table),
        row_count=table.num_rows,
        snapshot_id=make_context().snapshot_id,
        universe_hash=digest("universe"),
        start=date(2025, 1, 2),
        end=date(2025, 1, 31),
        table=table,
    )

    assert artifact.lazy_frame().collect().row(0)[4:] == (None, None, False)


@pytest.mark.parametrize(
    ("row_index", "out_of_range_day"),
    [(0, date(2025, 1, 1)), (2, date(2025, 2, 1))],
)
def test_load_uses_manifest_scope_across_parquet_batch_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    row_index: int,
    out_of_range_day: date,
) -> None:
    """The first or last streamed Parquet batch cannot escape manifest range."""
    from quant_core.factors import cache as cache_module

    monkeypatch.setattr(cache_module, "_PUBLISH_BATCH_ROWS", 2)
    cache = FeatureCache(tmp_path)
    artifact = publish(cache, large_factor_frame(3))
    data_path, manifest_path = entry_paths(cache, artifact)
    changed = (
        pl.read_parquet(data_path)
        .with_row_index("_row")
        .with_columns(
            pl.when(pl.col("_row") == row_index)
            .then(pl.lit(out_of_range_day, dtype=pl.Date))
            .otherwise(pl.col("trade_date"))
            .alias("trade_date")
        )
        .drop("_row")
    )
    pq.write_table(changed.to_arrow(), data_path, row_group_size=2)
    table = pq.read_table(data_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["content_hash"] = factor_table_content_hash(table)
    manifest["row_count"] = table.num_rows
    manifest["schema_fingerprint"] = hashlib.sha256(
        table.schema.serialize().to_pybytes()
    ).hexdigest()
    manifest_path.write_bytes(canonical_json_bytes(manifest))  # type: ignore[arg-type]
    assert pq.ParquetFile(data_path).num_row_groups == 2

    with pytest.raises(ValueError, match="range|Parquet"):
        cache.load(artifact.cache_key)


def test_load_revalidates_parquet_content_and_fails_closed(tmp_path: Path) -> None:
    """A manifest cannot make damaged Parquet appear to be a cache hit."""
    cache = FeatureCache(tmp_path)
    artifact = publish(cache)
    data_path, _ = entry_paths(cache, artifact)
    data_path.write_bytes(b"not parquet")

    with pytest.raises(ValueError, match="cache.*integrity|Parquet"):
        cache.load(artifact.cache_key)


def test_load_revalidates_manifest_path_and_cache_key(tmp_path: Path) -> None:
    """Moved or edited metadata cannot redirect a cache read."""
    cache = FeatureCache(tmp_path)
    artifact = publish(cache)
    _, manifest_path = entry_paths(cache, artifact)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["data_path"] = "../outside.parquet"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest|path"):
        cache.load(artifact.cache_key)


def _create_directory_link(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"New-Item -ItemType Junction -Path '{link}' -Target '{target}' | Out-Null",
            ],
            check=True,
            capture_output=True,
        )


def test_load_rejects_a_cache_entry_replaced_by_directory_link(tmp_path: Path) -> None:
    """A junction cannot redirect feature reads outside the controlled root."""
    cache = FeatureCache(tmp_path / "cache")
    artifact = publish(cache)
    entry = cache.root / artifact.cache_key
    outside = tmp_path / "outside"
    entry.rename(outside)
    _create_directory_link(entry, outside)

    with pytest.raises(ValueError, match="link|reparse"):
        cache.load(artifact.cache_key)


def test_load_revalidates_paths_after_a_read_time_directory_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A validate-then-open race cannot redirect a completed cache read."""
    from quant_core.factors import cache as cache_module

    cache = FeatureCache(tmp_path / "cache")
    artifact = publish(cache)
    entry = cache.root / artifact.cache_key
    outside = tmp_path / "outside"
    parked = tmp_path / "parked"
    shutil.copytree(entry, outside)
    read_validated = cache_module._read_validated_parquet
    swapped = False

    def read_then_swap(path: Path, **kwargs: object) -> object:
        nonlocal swapped
        table = read_validated(path, **kwargs)
        if not swapped:
            entry.rename(parked)
            _create_directory_link(entry, outside)
            swapped = True
        return table

    monkeypatch.setattr(cache_module, "_read_validated_parquet", read_then_swap)

    with pytest.raises(ValueError, match="link|reparse"):
        cache.load(artifact.cache_key)


class RecordingFactor:
    def __init__(self, spec: FactorSpec, calls: list[str]) -> None:
        self._spec = spec
        self._calls = calls

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        self._calls.append(f"{self.spec.factor_id}@{self.spec.version}")
        return make_frame(
            factor_id=self.spec.factor_id, factor_version=self.spec.version
        )


def test_engine_computes_dependencies_once_in_stable_order_then_hits_cache(
    tmp_path: Path,
) -> None:
    """A cache hit must skip factor code and preserve immutable files."""
    calls: list[str] = []
    registry = FactorRegistry()
    for spec in (
        make_spec(factor_id="quality", parameters={}),
        make_spec(factor_id="price", parameters={}),
        make_spec(
            factor_id="signal",
            parameters={},
            dependencies=("quality@1.0.0", "price@1.0.0"),
        ),
    ):
        registry.register(
            RecordingFactor(spec, calls), code_hash=digest(spec.factor_id)
        )
    engine = FactorEngine(
        registry, FeatureCache(tmp_path), capabilities=ProviderCapabilities.complete()
    )

    first = engine.compute(("signal@1.0.0",), make_context())
    mtimes = {
        path: path.stat().st_mtime_ns for path in tmp_path.rglob("*") if path.is_file()
    }
    second = engine.compute(("signal",), make_context())

    assert calls == ["price@1.0.0", "quality@1.0.0", "signal@1.0.0"]
    assert tuple(first) == ("signal@1.0.0",)
    assert second == first
    assert {path: path.stat().st_mtime_ns for path in mtimes} == mtimes


def test_engine_requires_an_explicit_capability_profile(tmp_path: Path) -> None:
    """Dropping the required profile would make production preflight optional again."""
    with pytest.raises(TypeError, match="capabilities"):
        FactorEngine(FactorRegistry(), FeatureCache(tmp_path))

    FactorEngine(
        FactorRegistry(),
        FeatureCache(tmp_path),
        capabilities=ProviderCapabilities.complete(),
    )


def test_runnable_references_require_supported_dependency_closure() -> None:
    """Checking only a root's metadata would mislabel signal -> quality runnable."""
    calls: list[str] = []
    registry = FactorRegistry()
    for spec in (
        make_spec(
            factor_id="quality",
            parameters={"required_capabilities": ["financials_with_announcement_date"]},
        ),
        make_spec(factor_id="price", parameters={}),
        make_spec(
            factor_id="signal",
            parameters={},
            dependencies=("quality@1.0.0", "price@1.0.0"),
        ),
    ):
        registry.register(
            RecordingFactor(spec, calls), code_hash=digest(spec.factor_id)
        )
    without_financials = replace(
        ProviderCapabilities.complete(), financials_with_announcement_date=False
    )

    assert registry.runnable_references(without_financials) == ("price@1.0.0",)
    assert registry.runnable_references(ProviderCapabilities.complete()) == (
        "price@1.0.0",
        "quality@1.0.0",
        "signal@1.0.0",
    )


def test_artifact_is_immutable(tmp_path: Path) -> None:
    """Consumers cannot retarget a verified artifact after cache lookup."""
    artifact = publish(FeatureCache(tmp_path))

    with pytest.raises(FrozenInstanceError):
        artifact.content_hash = digest("forged")  # type: ignore[misc]
