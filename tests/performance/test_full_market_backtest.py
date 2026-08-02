"""Opt-in synthetic 20-year full-market analytics performance evidence."""

from __future__ import annotations

import json
import time
import tracemalloc
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from quant_core.analytics.materialize import materialize_analytics
from quant_core.backtest.accounting import AccountSnapshot, PositionSnapshot
from quant_core.backtest.artifacts import BacktestArtifactWriter, ManifestContext
from quant_core.backtest.models import ExecutionConfig, ExecutionPrice
from quant_core.domain.identifiers import InstrumentId

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


def _published_synthetic_artifact(root: Path) -> tuple[Path, int]:
    sessions = _sessions()
    experiment_id = UUID("00000000-0000-0000-0000-000000000071")
    snapshot_id = UUID("00000000-0000-0000-0000-000000000072")
    instruments = [
        InstrumentId.parse(f"SSE:{600_000 + index:06d}")
        for index in range(_UNIVERSE_SIZE)
    ]
    variants: dict[tuple[int, int], tuple[PositionSnapshot, ...]] = {}
    for block in range(_UNIVERSE_SIZE // _POSITION_COUNT):
        selected = instruments[block * _POSITION_COUNT : (block + 1) * _POSITION_COUNT]
        for phase in range(7):
            market_value = 10_000 + phase
            variants[(block, phase)] = tuple(
                PositionSnapshot(instrument, 100, 100, 10_000, market_value)
                for instrument in selected
            )

    writer = BacktestArtifactWriter(root, experiment_id)
    for index, trade_date in enumerate(sessions):
        positions = variants[
            ((index // 21) % (_UNIVERSE_SIZE // _POSITION_COUNT), index % 7)
        ]
        market_value = sum(position.market_value_fen for position in positions)
        cash_fen = 9_000_000
        writer.append_snapshot(
            AccountSnapshot(
                trade_date,
                cash_fen,
                positions,
                market_value,
                cash_fen + market_value,
            ),
            float(100.0 + index * 0.01),
        )
    writer.close()
    context = ManifestContext(
        experiment_id,
        snapshot_id,
        "synthetic-full-market",
        "1.0.0",
        sessions[0],
        sessions[-1],
        InstrumentId.parse("SSE:000001"),
        10_000_000,
        "synthetic-v1",
        ExecutionConfig(ExecutionPrice.CLOSE, 0.0, 1.0),
    )
    writer.validate(sessions, context)
    return writer.publish().parent, len(sessions)


def test_synthetic_twenty_year_full_market_analytics_completes_within_budget(
    tmp_path: Path,
) -> None:
    """The complete synthetic publication must stay below the 60-minute budget."""
    tracemalloc.start()
    started = time.perf_counter()
    raw_started = time.perf_counter()
    artifact_dir, sessions = _published_synthetic_artifact(tmp_path)
    raw_seconds = time.perf_counter() - raw_started
    analytics_started = time.perf_counter()
    materialize_analytics(artifact_dir)
    analytics_seconds = time.perf_counter() - analytics_started
    total_seconds = time.perf_counter() - started
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    stages = {
        "raw_publication_seconds": raw_seconds,
        "analytics_materialization_seconds": analytics_seconds,
    }
    slowest_stage = max(stages, key=stages.__getitem__)
    evidence = {
        "workload": "SYNTHETIC_NOT_RELEASE_ACCEPTANCE",
        "sessions": sessions,
        "universe_size": _UNIVERSE_SIZE,
        "positions_per_session": _POSITION_COUNT,
        "total_seconds": total_seconds,
        "peak_memory_bytes": peak_bytes,
        "stages": stages,
        "slowest_stage": slowest_stage,
    }
    print(f"synthetic_performance={json.dumps(evidence, sort_keys=True)}")
    assert total_seconds <= _MAX_SECONDS, (
        f"synthetic analytics exceeded {_MAX_SECONDS}s; "
        f"slowest_stage={slowest_stage}; evidence={evidence}"
    )
    assert (artifact_dir / "manifest.json").is_file()
    manifest = json.loads((artifact_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["analytics"]["metrics_version"] == "1.0.0"
