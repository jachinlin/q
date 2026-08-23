"""验证当前 Run Schema 的唯一原子发布路径。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from quant_research.backtest.run_artifacts import RunArtifactPublisher


def test_publisher_writes_and_rechecks_the_complete_artifact_set(
    tmp_path: Path,
) -> None:
    tables = RunArtifactPublisher.canonical_tables(
        {
            "nav": pl.DataFrame(
                {
                    "trade_date": [date(2024, 1, 2)],
                    "cash_fen": [1_000_000],
                    "long_market_value_fen": [0],
                    "short_market_value_fen": [0],
                    "accrued_fees_fen": [0],
                    "margin_used_fen": [0],
                    "equity_fen": [1_000_000],
                    "benchmark_close": [100.0],
                }
            )
        }
    )
    publisher = RunArtifactPublisher(tmp_path, "experiment-1", "run-1")

    final, manifest_hash, entries = publisher.publish(
        tables,
        config={"kind": "STRATEGY_BACKTEST"},
        metrics={"cumulative_return": 0.0, "undefined_metric": None},
        quality_disclosure={
            "undefined_metrics": {"sharpe_ratio": "insufficient observations"}
        },
        identities={"catalog_hash": "a" * 64},
    )

    expected = {
        "signals",
        "orders",
        "fills",
        "holdings",
        "costs",
        "nav",
        "performance",
        "monthly_returns",
        "annual_returns",
        "execution_summary",
        "exposure_summary",
        "attribution",
        "config",
        "metrics",
        "quality_disclosure",
    }
    assert {entry["artifact_type"] for entry in entries} == expected
    assert len(manifest_hash) == 64
    assert final == tmp_path / "experiments" / "experiment-1" / "run-1"
    assert json.loads((final / "quality_disclosure.json").read_bytes()) == {
        "undefined_metrics": {"sharpe_ratio": "insufficient observations"}
    }
    manifest = json.loads((final / "manifest.json").read_bytes())
    assert manifest["identities"]["catalog_hash"] == "a" * 64
    assert pl.read_parquet(final / "nav.parquet")["equity_fen"].to_list() == [1_000_000]
