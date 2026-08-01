"""Reproducible partitioned benchmark for the five production market factors."""

from __future__ import annotations

import argparse
import ctypes
import gc
import hashlib
import json
import tempfile
import time
import tracemalloc
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import polars as pl

from quant_core.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import FactorContext, FactorEngine, FactorRegistry, FeatureCache
from quant_core.factors.builtin import register_etf_factors

_FACTOR_IDS = (
    "return_20d_v1",
    "return_60d_v1",
    "return_120d_v1",
    "trend_120d_v1",
    "volatility_60d_v1",
)


class _SyntheticBars:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls = 0

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame:
        del snapshot_id, as_of
        assert mode is AdjustmentMode.FORWARD
        self.calls += 1
        return self._frame.lazy().filter(
            pl.col("instrument_id").is_in(
                [instrument.canonical() for instrument in instruments]
            )
            & pl.col("trade_date").is_between(start, end, closed="both")
        )


def _bars(instruments: Sequence[InstrumentId], sessions: int) -> pl.DataFrame:
    days = pl.DataFrame({"_session": pl.int_range(0, sessions, eager=True)}).select(
        (pl.lit(date(2000, 1, 1)) + pl.duration(days=pl.col("_session")))
        .cast(pl.Date)
        .alias("trade_date"),
        pl.col("_session"),
    )
    identifiers = pl.DataFrame(
        {"instrument_id": [instrument.canonical() for instrument in instruments]}
    )
    return (
        identifiers.join(days, how="cross")
        .with_columns(
            (
                ((pl.col("_session") % 31) - 15).cast(pl.Float64) * 0.00001
                + pl.col("instrument_id").str.slice(-3).cast(pl.Float64) * 1e-8
            ).alias(FORWARD_LOG_RETURN_COLUMN),
            pl.col("trade_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("available_at"),
        )
        .select(
            "instrument_id",
            "trade_date",
            "available_at",
            FORWARD_LOG_RETURN_COLUMN,
        )
        .sort("instrument_id", "trade_date")
    )


def _peak_rss_bytes() -> int | None:
    if not hasattr(ctypes, "windll"):
        return None

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong),
            ("PageFaultCount", ctypes.c_ulong),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    process = kernel32.GetCurrentProcess()
    get_memory_info = kernel32.K32GetProcessMemoryInfo
    get_memory_info.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]
    get_memory_info.restype = ctypes.c_int
    ok = get_memory_info(process, ctypes.byref(counters), counters.cb)
    return int(counters.PeakWorkingSetSize) if ok else None


def run(
    total_instruments: int, sessions: int, partition_size: int
) -> dict[str, object]:
    if total_instruments <= 0 or sessions < 121 or partition_size <= 0:
        raise ValueError("instruments/partition must be positive and sessions >= 121")
    started = time.perf_counter()
    tracemalloc.start()
    output_rows = 0
    read_calls = 0
    partitions: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="quant-i3-benchmark-") as raw_root:
        root = Path(raw_root)
        for offset in range(0, total_instruments, partition_size):
            count = min(partition_size, total_instruments - offset)
            instruments = [
                InstrumentId.parse(f"SSE:{600000 + index:06d}")
                for index in range(offset, offset + count)
            ]
            frame = _bars(instruments, sessions)
            service = _SyntheticBars(frame)
            registry = FactorRegistry()
            register_etf_factors(registry, service, instruments)
            engine = FactorEngine(
                registry,
                FeatureCache(root / f"partition-{offset // partition_size:04d}"),
                capabilities=BAOSTOCK_CAPABILITIES,
            )
            first_signal = frame.get_column("trade_date")[120]
            last_signal = frame.get_column("trade_date")[-1]
            partition_started = time.perf_counter()
            ctx = FactorContext(
                SnapshotId.parse(f"00000000-0000-0000-0000-{offset + 1:012d}"),
                hashlib.sha256(f"{offset}:{count}".encode()).hexdigest(),
                first_signal,
                last_signal,
            )
            artifacts = engine.compute(_FACTOR_IDS, ctx)
            rows = sum(artifact.row_count for artifact in artifacts.values())
            output_rows += rows
            read_calls += service.calls
            partitions.append(
                {
                    "instruments": count,
                    "input_rows": frame.height,
                    "output_rows": rows,
                    "wall_seconds": time.perf_counter() - partition_started,
                }
            )
            del artifacts, engine, registry, service, frame
            gc.collect()
    _, python_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "total_instruments": total_instruments,
        "sessions": sessions,
        "input_rows": total_instruments * sessions,
        "output_rows": output_rows,
        "partition_size": partition_size,
        "max_partition_input_rows": partition_size * sessions,
        "publish_row_group_limit": 65_536,
        "market_bar_reads": read_calls,
        "wall_seconds": time.perf_counter() - started,
        "python_heap_peak_bytes": python_peak,
        "process_peak_rss_bytes": _peak_rss_bytes(),
        "partitions": partitions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instruments", type=int, default=1_000)
    parser.add_argument("--sessions", type=int, default=5_000)
    parser.add_argument("--partition-size", type=int, default=100)
    args = parser.parse_args()
    print(
        json.dumps(
            run(args.instruments, args.sessions, args.partition_size),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
