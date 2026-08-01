"""Golden artifact regression for a fixed deterministic daily backtest."""

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
        writer.validate(1)
