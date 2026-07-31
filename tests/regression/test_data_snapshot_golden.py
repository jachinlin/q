"""Golden regression for canonical content published by the offline fixture."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

from quant_core.data.contracts import canonical_json_bytes
from quant_core.persistence.repositories import PipelineStageName
from tests.integration.test_data_pipeline import OfflineBaoStockSource, make_pipeline


def test_offline_snapshot_matches_reviewed_semantic_golden(tmp_path: Path) -> None:
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())
    result = pipeline.bootstrap()
    manifest = json.loads(
        repository.get_snapshot(result.snapshot_id).manifest_path.read_text(
            encoding="utf-8"
        )
    )
    semantic = {
        "format_version": manifest["format_version"],
        "status": manifest["status"],
        "datasets": {
            dataset: [
                {
                    "content_hash": partition["content_hash"],
                    "row_count": partition["row_count"],
                    "schema_fingerprint": partition["schema_fingerprint"],
                }
                for partition in value["partitions"]
            ]
            for dataset, value in sorted(manifest["datasets"].items())
        },
    }
    semantic_hash = hashlib.sha256(canonical_json_bytes(semantic)).hexdigest()
    raw_checkpoint = repository.get_pipeline_stage(
        result.run_id, PipelineStageName.INGEST_RAW
    )
    assert isinstance(raw_checkpoint.output, Mapping)
    raw_partitions = raw_checkpoint.output["partitions"]
    assert isinstance(raw_partitions, tuple)
    daily_partition = next(
        partition
        for partition in raw_partitions
        if isinstance(partition, Mapping) and partition["dataset"] == "daily_bars"
    )

    assert daily_partition["request"] == {
        "api": "query_daily_history_k_AStock",
        "scope": "ALL",
        "date": "2026-01-05",
        "frequency": "d",
        "catalog_instrument_count": 1,
        "catalog_instruments_sha256": (
            "4ce2319dba32d78a0d8f387c01dfda93b89ecfc1429822803da3709d75ec5e98"
        ),
        "response_instrument_count": 1,
        "response_instruments_sha256": (
            "0b65e0880c3c56b781009d8686a77a5473246a7a0fcb74ab6c7d6f5410ed7b6b"
        ),
    }

    assert semantic == {
        "format_version": 1,
        "status": "PUBLISHED",
        "datasets": {
            "daily_bar": [
                {
                    "content_hash": "8ef704e8a370435496f9cfee02ddf25ea5a71eec8572b03001e59361e8ebc32b",
                    "row_count": 1,
                    "schema_fingerprint": "4f88a3e7656118b465f367201267313600780ab400de7a7d8fe83cc62dfbac9d",
                }
            ],
            "instrument": [
                {
                    "content_hash": "ba96f3805e0af98f7f9d06838d2da986b1ea379e89c8733c0de5d55350e0b7bc",
                    "row_count": 1,
                    "schema_fingerprint": "44401acc4f6e516c3451490e7bd9c9834b72b9a15220c7d5e835702a14751e03",
                }
            ],
            "security_status": [
                {
                    "content_hash": "814008fd72f208028a8f0de30455bfe1f277e448ad9d98a7360a577f526d615f",
                    "row_count": 1,
                    "schema_fingerprint": "02e735b0f16037e00075435ee2dc04d4cb74bd227fbf7403716e2cfc8c2cec3a",
                }
            ],
            "trade_calendar": [
                {
                    "content_hash": "7a6e85f5642bd4493b23c0117c9c55c6acc5f3904f49fa822d9f3da1897c4a94",
                    "row_count": 1,
                    "schema_fingerprint": "259ad3b579400c2377497470c5163afa71d415475b5c0ac0fcc62fc43e57cee0",
                }
            ],
        },
    }
    assert (
        semantic_hash
        == "b04beb053b3569d20c739b946de735ce58d9dde361416783baaff26fa0ca900d"
    )
