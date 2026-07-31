"""Raw-stage checkpoint serialization and integrity verification."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import JsonValue, PublishedPartition
from quant_core.data.storage import resolved_storage_root, validate_storage_path


def partition_to_json(partition: PublishedPartition) -> dict[str, JsonValue]:
    run_component = partition.data_path.parent.name
    ingest_component = partition.data_path.parent.parent.name
    if not run_component.startswith("run_id=") or not ingest_component.startswith(
        "ingest_date="
    ):
        raise ValueError("raw partition path does not use the canonical layout")
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
        "run_id": run_component.removeprefix("run_id="),
        "ingest_date": ingest_component.removeprefix("ingest_date="),
    }


def partition_from_json(
    value: Mapping[str, object], raw_root: Path
) -> PublishedPartition:
    root = resolved_storage_root(raw_root)
    provider = str(value["provider"])
    dataset = str(value["dataset"])
    request = value["request"] if isinstance(value["request"], Mapping) else {}
    retrieved_at = datetime.fromisoformat(str(value["retrieved_at"]))
    request_hash = str(value["request_hash"])
    if (
        hashlib.sha256(
            json.dumps(
                request,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        != request_hash
    ):
        raise ValueError("raw checkpoint request hash is invalid")
    ingest_date = str(value["ingest_date"])
    run_id = str(value["run_id"])
    if retrieved_at.astimezone(UTC).date().isoformat() != ingest_date:
        raise ValueError("raw checkpoint ingest date is invalid")
    directory = (
        root
        / f"provider={provider}"
        / f"dataset={dataset}"
        / f"ingest_date={ingest_date}"
        / f"run_id={run_id}"
    )
    expected_data = directory / f"{request_hash}.parquet"
    expected_manifest = directory / f"{request_hash}.manifest.json"
    if (
        Path(str(value["data_path"])).absolute() != expected_data
        or Path(str(value["manifest_path"])).absolute() != expected_manifest
    ):
        raise ValueError("raw checkpoint path does not match its canonical layout")
    validate_storage_path(root, expected_data, require_file=True)
    validate_storage_path(root, expected_manifest, require_file=True)
    partition = PublishedPartition(
        provider=provider,
        dataset=dataset,
        request=request,
        retrieved_at=retrieved_at,
        data_path=expected_data,
        manifest_path=expected_manifest,
        request_hash=request_hash,
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
