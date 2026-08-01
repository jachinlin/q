"""Golden artifact regression for a fixed deterministic daily backtest."""

import hashlib
import json
from datetime import date
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quant_core.backtest.artifacts as artifacts_module
from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.artifacts import BacktestArtifactWriter
from quant_core.backtest.engine import BacktestEngine
from quant_core.portfolio import RebalancePlanner
from tests.integration.test_backtest_timeline import (
    _Data,
    _NeverCancelled,
    _Progress,
    _request,
    _RuleBook,
    _Targets,
)


def test_manifest_hashes_describe_closed_deterministic_artifacts(
    tmp_path: Path,
) -> None:
    result = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    ).run(_request(), _Progress(), _NeverCancelled())

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    artifacts = manifest["artifacts"]
    assert sorted(artifacts) == [
        "costs.parquet",
        "fills.parquet",
        "holdings.parquet",
        "nav.parquet",
        "targets.parquet",
    ]
    for name, entry in artifacts.items():
        artifact = result.artifact_dir / name
        assert entry["path"] == name
        assert entry["size_bytes"] == artifact.stat().st_size
        assert entry["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert pq.read_table(result.artifact_dir / "nav.parquet").num_rows == 3
    assert manifest["completed_sessions"] == 3
    assert pq.read_table(result.artifact_dir / "nav.parquet").to_pylist() == [
        {
            "trade_date": date(2024, 1, 5),
            "cash_fen": 200_000,
            "market_value_fen": 0,
            "nav_fen": 200_000,
            "benchmark_close": 3.0,
        },
        {
            "trade_date": date(2024, 1, 8),
            "cash_fen": 79_900,
            "market_value_fen": 120_000,
            "nav_fen": 199_900,
            "benchmark_close": 3.0,
        },
        {
            "trade_date": date(2024, 1, 9),
            "cash_fen": 189_800,
            "market_value_fen": 0,
            "nav_fen": 189_800,
            "benchmark_close": 3.0,
        },
    ]
    assert pq.read_table(result.artifact_dir / "holdings.parquet").to_pylist() == [
        {
            "trade_date": date(2024, 1, 8),
            "instrument_id": "SSE:600001",
            "total_quantity": 100,
            "sellable_quantity": 0,
            "cost_basis_fen": 120_100,
            "market_value_fen": 120_000,
        }
    ]
    assert pq.read_table(result.artifact_dir / "targets.parquet").to_pylist() == [
        {
            "signal_date": date(2024, 1, 5),
            "execute_date": date(2024, 1, 8),
            "position_index": 0,
            "instrument_id": "SSE:600001",
            "target_weight": 1.0,
            "score": 2.0,
            "reason_code": "TEST",
            "cash_weight": 0.0,
        },
        {
            "signal_date": date(2024, 1, 5),
            "execute_date": date(2024, 1, 8),
            "position_index": 1,
            "instrument_id": None,
            "target_weight": 0.0,
            "score": None,
            "reason_code": "CASH",
            "cash_weight": 0.0,
        },
        {
            "signal_date": date(2024, 1, 8),
            "execute_date": date(2024, 1, 9),
            "position_index": 0,
            "instrument_id": None,
            "target_weight": 1.0,
            "score": None,
            "reason_code": "CASH",
            "cash_weight": 1.0,
        },
    ]
    assert [
        (row["trade_date"], row["side"], row["gross_value_fen"])
        for row in pq.read_table(result.artifact_dir / "fills.parquet").to_pylist()
    ] == [
        (date(2024, 1, 8), "BUY", 120_000),
        (date(2024, 1, 9), "SELL", 110_000),
    ]
    assert [
        (row["trade_date"], row["total_fees_fen"])
        for row in pq.read_table(result.artifact_dir / "costs.parquet").to_pylist()
    ] == [
        (date(2024, 1, 8), 100),
        (date(2024, 1, 9), 100),
    ]
    assert {name: entry["row_count"] for name, entry in artifacts.items()} == {
        "nav.parquet": 3,
        "holdings.parquet": 1,
        "targets.parquet": 3,
        "fills.parquet": 2,
        "costs.parquet": 2,
    }


def test_artifact_validation_rejects_a_tampered_nav_identity(tmp_path: Path) -> None:
    writer = BacktestArtifactWriter(
        tmp_path, UUID("00000000-0000-0000-0000-000000000003")
    )
    writer.append_snapshot(AccountSnapshot(_request().start_date, 100, (), 0, 100), 1.0)
    writer.close()
    schema = pq.read_schema(writer.staging_dir / "nav.parquet")
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "trade_date": _request().start_date,
                    "cash_fen": 100,
                    "market_value_fen": 0,
                    "nav_fen": 99,
                    "benchmark_close": 1.0,
                }
            ],
            schema=schema,
        ),
        writer.staging_dir / "nav.parquet",
    )

    with pytest.raises(ValueError, match="nav"):
        writer.validate((_request().start_date,))


def test_artifact_writer_rejects_publish_before_validation(tmp_path: Path) -> None:
    writer = BacktestArtifactWriter(
        tmp_path, UUID("00000000-0000-0000-0000-000000000004")
    )
    writer.close()

    with pytest.raises(ValueError, match="VALIDATED"):
        writer.publish({})


def test_artifact_writer_rejects_manifest_entry_that_differs_from_validation(
    tmp_path: Path,
) -> None:
    writer = BacktestArtifactWriter(
        tmp_path, UUID("00000000-0000-0000-0000-000000000005")
    )
    writer.append_snapshot(AccountSnapshot(_request().start_date, 100, (), 0, 100), 1.0)
    writer.close()
    entries = writer.validate((_request().start_date,))
    manifest = {
        "schema_version": 1,
        "experiment_id": "00000000-0000-0000-0000-000000000005",
        "snapshot_id": "00000000-0000-0000-0000-000000000001",
        "strategy": {"strategy_id": "test", "version": "1"},
        "start_date": "2024-01-05",
        "end_date": "2024-01-05",
        "benchmark": "SSE:000001",
        "initial_cash_fen": 100,
        "rulebook_version": "test-v1",
        "execution_config": {
            "reference_price": "CLOSE",
            "slippage_bps": 0.0,
            "max_volume_participation": 1.0,
        },
        "completed_sessions": 1,
        "artifacts": {
            name: {
                "path": entry.path,
                "schema": entry.schema,
                "row_count": entry.row_count,
                "size_bytes": entry.size_bytes,
                "sha256": entry.sha256,
            }
            for name, entry in entries.items()
        },
    }
    manifest["artifacts"]["nav.parquet"]["sha256"] = "0" * 64

    with pytest.raises(ValueError, match="manifest"):
        writer.publish(manifest)


def test_partial_writer_construction_closes_created_writers_and_diagnoses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[object] = []

    class ObservedWriter:
        closed = False

        def close(self) -> None:
            self.closed = True

    def fail_second_writer(*args: object, **kwargs: object) -> ObservedWriter:
        del args, kwargs
        if len(created) == 1:
            raise OSError("second writer failure")
        writer = ObservedWriter()
        created.append(writer)
        return writer

    monkeypatch.setattr(artifacts_module.pq, "ParquetWriter", fail_second_writer)

    with pytest.raises(OSError, match="second writer failure"):
        BacktestArtifactWriter(tmp_path, UUID("00000000-0000-0000-0000-000000000006"))

    assert all(writer.closed for writer in created)
    assert list(tmp_path.glob(".staging-*/diagnostic.json"))
