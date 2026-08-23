"""当前 Run Schema 的可选 20 年分析与发布性能证据。"""

from __future__ import annotations

import json
import time
import tracemalloc
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from quant_research.analytics.attribution import calculate_attribution
from quant_research.analytics.performance import calculate_performance
from quant_research.backtest.run_artifacts import RunArtifactPublisher
from quant_research.data.contracts import JsonValue
from tests.performance._process_memory import process_peak_rss_bytes

pytestmark = pytest.mark.performance

_MAX_SECONDS = 60 * 60
_UNIVERSE_SIZE = 5_000
_POSITION_COUNT = 100


def _sessions() -> tuple[date, ...]:
    current = date(2005, 12, 30)
    end = date(2025, 12, 31)
    sessions: list[date] = []
    while current <= end:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _raw_tables() -> tuple[dict[str, pl.DataFrame], int]:
    sessions = _sessions()
    instruments = tuple(f"{600_000 + index:06d}.SH" for index in range(_UNIVERSE_SIZE))
    nav_rows: list[dict[str, object]] = []
    holding_rows: list[dict[str, object]] = []
    for index, trade_date in enumerate(sessions):
        block = (index // 21) % (_UNIVERSE_SIZE // _POSITION_COUNT)
        selected = instruments[block * _POSITION_COUNT : (block + 1) * _POSITION_COUNT]
        value = 10_000 + index % 7
        market_value = value * len(selected)
        cash = 9_000_000
        nav_rows.append(
            {
                "trade_date": trade_date,
                "cash_fen": cash,
                "long_market_value_fen": market_value,
                "short_market_value_fen": 0,
                "accrued_fees_fen": 0,
                "margin_used_fen": 0,
                "equity_fen": cash + market_value,
                "benchmark_close": 100.0 + index * 0.01,
            }
        )
        holding_rows.extend(
            {
                "trade_date": trade_date,
                "instrument_id": instrument,
                "total_quantity": 100,
                "sellable_quantity": 100,
                "cost_basis_fen": 10_000,
                "market_value_fen": value,
            }
            for instrument in selected
        )
    return {
        "nav": pl.DataFrame(nav_rows),
        "holdings": pl.DataFrame(holding_rows),
    }, len(sessions)


def test_current_schema_analysis_and_atomic_publication_stay_within_budget(
    tmp_path: Path,
) -> None:
    """完整内存分析与唯一发布链必须保持在一小时预算内。"""
    tracemalloc.start()
    started = time.perf_counter()
    raw_tables, sessions = _raw_tables()
    normalized = RunArtifactPublisher.canonical_tables(raw_tables)

    analytics_started = time.perf_counter()
    performance = calculate_performance(
        normalized["nav"],
        normalized["holdings"],
        normalized["fills"],
        normalized["costs"],
    )
    attribution = calculate_attribution(
        normalized["nav"],
        normalized["holdings"],
        normalized["fills"],
        normalized["costs"],
    )
    drawdown = performance.drawdown
    normalized.update(
        {
            "performance": drawdown.select(
                pl.col("trade_date"),
                pl.col("portfolio_daily_return").alias("return"),
                pl.col("benchmark_daily_return").alias("benchmark_return"),
                (pl.col("nav") - 1.0).alias("cumulative_return"),
                (pl.col("benchmark_nav") - 1.0).alias("benchmark_cumulative_return"),
                (
                    pl.col("portfolio_daily_return") - pl.col("benchmark_daily_return")
                ).alias("active_return"),
                pl.col("nav"),
                pl.col("benchmark_nav"),
                pl.col("drawdown"),
                pl.col("active_drawdown"),
            ),
            "monthly_returns": performance.monthly_returns,
            "annual_returns": performance.annual_returns,
            "execution_summary": performance.execution_summary,
            "exposure_summary": attribution.exposure_summary,
            "attribution": attribution.attribution,
        }
    )
    analytics_seconds = time.perf_counter() - analytics_started

    publish_started = time.perf_counter()
    final, _, _ = RunArtifactPublisher(
        tmp_path, "synthetic-full-market", "run-current-schema"
    ).publish(
        RunArtifactPublisher.canonical_tables(normalized),
        config={"kind": "STRATEGY_BACKTEST"},
        metrics=cast(dict[str, JsonValue], dict(performance.metrics)),
        quality_disclosure={
            "undefined_metrics": dict(performance.undefined_metrics),
            "warnings": list(attribution.disclosures),
        },
        identities={"catalog_hash": "a" * 64},
    )
    publication_seconds = time.perf_counter() - publish_started
    total_seconds = time.perf_counter() - started
    _, python_peak_tracemalloc_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    process_peak_bytes = process_peak_rss_bytes()
    evidence = {
        "workload": "SYNTHETIC_NOT_RELEASE_ACCEPTANCE",
        "sessions": sessions,
        "universe_size": _UNIVERSE_SIZE,
        "positions_per_session": _POSITION_COUNT,
        "total_seconds": total_seconds,
        "process_peak_rss_bytes": process_peak_bytes,
        "python_peak_tracemalloc_bytes": python_peak_tracemalloc_bytes,
        "stages": {
            "analytics_seconds": analytics_seconds,
            "atomic_publication_seconds": publication_seconds,
        },
    }
    print(f"synthetic_performance={json.dumps(evidence, sort_keys=True)}")
    assert total_seconds <= _MAX_SECONDS, evidence
    assert (final / "manifest.json").is_file()
    assert (final / "quality_disclosure.json").is_file()
