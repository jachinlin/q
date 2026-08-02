"""Opt-in real-snapshot analytics release acceptance workload."""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
import time
import tracemalloc
from datetime import date, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from quant_core.analytics.materialize import materialize_analytics
from quant_core.data.repository import SnapshotResearchRepository
from quant_core.domain.identifiers import SnapshotId
from quant_core.persistence.database import create_sqlite_engine
from quant_core.persistence.repositories import MetadataRepository

pytestmark = pytest.mark.acceptance

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MULTIFACTOR_LIMIT_SECONDS = 60 * 60
_ETF_LIMIT_SECONDS = 5 * 60
_STRATEGY_IDS = ("stock_multifactor", "etf_rotation")
_RAW_FILES = (
    "manifest.json",
    "nav.parquet",
    "holdings.parquet",
    "targets.parquet",
    "fills.parquet",
    "costs.parquet",
)


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"acceptance manifest is not an object: {path}")
    return payload


def _acceptance_artifacts(
    artifact_root: Path, snapshot_id: str, expected_end: date
) -> dict[str, tuple[Path, dict[str, Any]]]:
    candidates: dict[str, list[tuple[Path, dict[str, Any]]]] = {
        strategy_id: [] for strategy_id in _STRATEGY_IDS
    }
    for manifest_path in artifact_root.rglob("manifest.json"):
        manifest = _load_manifest(manifest_path)
        strategy = manifest.get("strategy")
        if manifest.get("snapshot_id") != snapshot_id or not isinstance(strategy, dict):
            continue
        strategy_id = strategy.get("strategy_id")
        if strategy_id not in candidates:
            continue
        if manifest.get("end_date") != expected_end.isoformat():
            continue
        start = date.fromisoformat(manifest["start_date"])
        try:
            twenty_year_boundary = expected_end.replace(year=expected_end.year - 20)
        except ValueError:
            twenty_year_boundary = expected_end.replace(
                year=expected_end.year - 20, day=28
            )
        if start <= twenty_year_boundary:
            candidates[strategy_id].append((manifest_path.parent, manifest))
    selected: dict[str, tuple[Path, dict[str, Any]]] = {}
    for strategy_id, matches in candidates.items():
        assert len(matches) == 1, (
            f"expected exactly one 20-year {strategy_id} artifact for snapshot "
            f"{snapshot_id} ending {expected_end}, found {len(matches)}"
        )
        selected[strategy_id] = matches[0]
    return selected


def _latest_complete_trading_day(
    repository: SnapshotResearchRepository, snapshot_id: SnapshotId, today: date
) -> date:
    calendar = repository.trade_calendar(
        snapshot_id, today - timedelta(days=31), today - timedelta(days=1)
    ).collect()
    complete = calendar.filter(pl.col("is_trading_day"))["trade_date"].to_list()
    assert complete, "snapshot has no complete trading day in the prior 31 days"
    return complete[-1]


def _clone_raw_artifact(source: Path, parent: Path) -> Path:
    destination = parent / source.name
    destination.mkdir(parents=True, exist_ok=False)
    for name in _RAW_FILES:
        source_path = source / name
        assert source_path.is_file(), f"acceptance raw artifact missing {source_path}"
        shutil.copy2(source_path, destination / name)
    return destination


def _physical_memory_bytes() -> int | None:
    if sys.platform == "win32":
        import ctypes

        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total_physical", ctypes.c_ulonglong),
                ("available_physical", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended_virtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return int(status.total_physical)
    return None


def _environment_payload(
    *,
    data_root: Path,
    snapshot_id: str,
    expected_end: date,
    artifacts: dict[str, tuple[Path, dict[str, Any]]],
    security_count: int,
    timings: dict[str, float],
    peak_memory_bytes: int,
) -> dict[str, object]:
    starts = [date.fromisoformat(item[1]["start_date"]) for item in artifacts.values()]
    return {
        "cpu": platform.processor() or platform.machine(),
        "logical_cores": os.cpu_count(),
        "physical_memory_bytes": _physical_memory_bytes(),
        "disk_type": "FIXED_OR_PLATFORM_DEFAULT",
        "python_version": platform.python_version(),
        "dependency_versions": {
            name: version(name) for name in ("duckdb", "numpy", "polars", "pyarrow")
        },
        "snapshot_id": snapshot_id,
        "data_root": str(data_root.resolve()),
        "date_range": {
            "start": min(starts).isoformat(),
            "end": expected_end.isoformat(),
        },
        "security_count": security_count,
        "factor_count": 10,
        "strategy_parameters": {
            strategy_id: {
                "strategy": manifest["strategy"],
                "execution_config": manifest["execution_config"],
            }
            for strategy_id, (_, manifest) in artifacts.items()
        },
        "stage_seconds": timings,
        "peak_memory_bytes": peak_memory_bytes,
    }


def test_real_twenty_year_snapshot_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real snapshot is mandatory; synthetic evidence cannot satisfy release acceptance."""
    raw_snapshot_id = os.environ.get("QUANT_ACCEPTANCE_SNAPSHOT_ID")
    assert raw_snapshot_id, (
        "QUANT_ACCEPTANCE_SNAPSHOT_ID is required for real 20-year release acceptance; "
        "synthetic performance evidence is not a substitute"
    )
    try:
        parsed_snapshot_id = UUID(raw_snapshot_id)
    except ValueError as error:
        pytest.fail(f"QUANT_ACCEPTANCE_SNAPSHOT_ID must be a UUID: {error}")
    raw_data_root = os.environ.get("QUANT_DATA_ROOT")
    assert raw_data_root, (
        "QUANT_DATA_ROOT is required to resolve the immutable acceptance snapshot"
    )
    data_root = Path(raw_data_root)
    state_db = data_root / "state" / "quant.db"
    artifact_root = data_root / "artifacts"
    assert state_db.is_file(), f"acceptance state database is missing: {state_db}"
    assert artifact_root.is_dir(), (
        f"acceptance artifact root is missing: {artifact_root}"
    )

    engine = create_sqlite_engine(state_db)
    try:
        catalog = MetadataRepository(engine)
        repository = SnapshotResearchRepository(catalog)
        snapshot_id = SnapshotId(parsed_snapshot_id)
        today = __import__("datetime").datetime.now(_SHANGHAI).date()
        expected_end = _latest_complete_trading_day(repository, snapshot_id, today)
        security_count = (
            repository.instruments(snapshot_id).select(pl.len()).collect().item()
        )
    finally:
        engine.dispose()
    artifacts = _acceptance_artifacts(artifact_root, raw_snapshot_id, expected_end)

    tracemalloc.start()
    timings: dict[str, float] = {}
    for strategy_id in _STRATEGY_IDS:
        clone_started = time.perf_counter()
        cloned = _clone_raw_artifact(artifacts[strategy_id][0], tmp_path / strategy_id)
        timings[f"{strategy_id}_clone"] = time.perf_counter() - clone_started
        analytics_started = time.perf_counter()
        materialize_analytics(cloned)
        timings[f"{strategy_id}_analytics"] = time.perf_counter() - analytics_started
    _, peak_memory_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    multifactor_seconds = sum(
        value for name, value in timings.items() if name.startswith("stock_multifactor")
    )
    etf_seconds = sum(
        value for name, value in timings.items() if name.startswith("etf_rotation")
    )
    slowest_stage = max(timings, key=timings.__getitem__)
    assert multifactor_seconds <= _MULTIFACTOR_LIMIT_SECONDS, (
        f"multifactor acceptance exceeded {_MULTIFACTOR_LIMIT_SECONDS}s; "
        f"slowest_stage={slowest_stage}; stages={timings}; "
        f"peak_memory_bytes={peak_memory_bytes}"
    )
    assert etf_seconds <= _ETF_LIMIT_SECONDS, (
        f"ETF acceptance exceeded {_ETF_LIMIT_SECONDS}s; "
        f"slowest_stage={slowest_stage}; stages={timings}; "
        f"peak_memory_bytes={peak_memory_bytes}"
    )
    evidence = _environment_payload(
        data_root=data_root,
        snapshot_id=raw_snapshot_id,
        expected_end=expected_end,
        artifacts=artifacts,
        security_count=security_count,
        timings=timings,
        peak_memory_bytes=peak_memory_bytes,
    )
    evidence_path = tmp_path / "acceptance_environment.json"
    evidence_path.write_text(
        json.dumps(
            evidence, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False
        ),
        encoding="utf-8",
    )
    print(f"acceptance_environment={evidence_path}")
    monkeypatch.setenv("QUANT_ACCEPTANCE_EVIDENCE_PATH", str(evidence_path))
