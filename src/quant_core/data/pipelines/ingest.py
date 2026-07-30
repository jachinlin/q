"""Raw-stage checkpoint serialization and integrity verification."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import JsonValue, PublishedPartition


def partition_to_json(partition: PublishedPartition) -> dict[str, JsonValue]:
    return {
        "provider": partition.provider,
        "dataset": partition.dataset,
        "request": dict(partition.request),
        "retrieved_at": partition.retrieved_at.isoformat(),
        "data_path": partition.data_path.resolve().as_posix(),
        "manifest_path": partition.manifest_path.resolve().as_posix(),
        "request_hash": partition.request_hash,
        "content_hash": partition.content_hash,
        "schema_fingerprint": partition.schema_fingerprint,
        "row_count": partition.row_count,
    }


def partition_from_json(value: Mapping[str, object]) -> PublishedPartition:
    partition = PublishedPartition(
        provider=str(value["provider"]),
        dataset=str(value["dataset"]),
        request=value["request"] if isinstance(value["request"], Mapping) else {},
        retrieved_at=datetime.fromisoformat(str(value["retrieved_at"])),
        data_path=Path(str(value["data_path"])),
        manifest_path=Path(str(value["manifest_path"])),
        request_hash=str(value["request_hash"]),
        content_hash=str(value["content_hash"]),
        schema_fingerprint=str(value["schema_fingerprint"]),
        row_count=int(str(value["row_count"])),
    )
    _verify(partition)
    return partition


def _verify(partition: PublishedPartition) -> None:
    manifest = json.loads(partition.manifest_path.read_text(encoding="utf-8"))
    table = pq.read_table(partition.data_path)
    expected = {
        "provider": partition.provider,
        "dataset": partition.dataset,
        "request_hash": partition.request_hash,
        "content_hash": partition.content_hash,
        "row_count": partition.row_count,
        "schema_fingerprint": partition.schema_fingerprint,
        "retrieved_at": partition.retrieved_at.isoformat(),
    }
    if manifest != expected:
        raise ValueError("raw checkpoint manifest does not match its stage output")
    if (
        _content_hash(table) != partition.content_hash
        or _schema_fingerprint(table.schema) != partition.schema_fingerprint
        or table.num_rows != partition.row_count
    ):
        raise ValueError("raw checkpoint data fails integrity checks")


def _content_hash(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
