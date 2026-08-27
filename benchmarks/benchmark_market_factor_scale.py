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
from datetime import date, timedelta

import polars as pl

from quant_research.data.canonical.adjustments import FORWARD_LOG_RETURN_COLUMN
from quant_research.data.contracts import ProviderCapabilities
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors import (
    FactorContext,
    FactorEngine,
    FactorRegistry,
    PartitionedFactorEngine,
)
from quant_research.factors.builtin import register_etf_factors

_FACTOR_IDS = (
    "return_20d",
    "return_60d",
    "return_120d",
    "trend_120d",
    "volatility_60d",
)


class _SyntheticBars:
    def __init__(self, frame: pl.DataFrame, statistics: dict[str, int]) -> None:
        self._frame = frame
        self._statistics = statistics

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        del start, lookback_sessions
        self._statistics["market_bar_reads"] += 1
        return self._frame.lazy().filter(
            pl.col("instrument_id").is_in(
                [instrument.canonical() for instrument in instruments]
            )
            & (pl.col("trade_date") <= end)
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
                + pl.col("instrument_id").str.slice(0, 6).cast(pl.Float64) * 1e-8
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
    instruments = tuple(
        InstrumentId.parse(f"{600000 + index:06d}.SH")
        for index in range(total_instruments)
    )
    statistics = {"market_bar_reads": 0, "max_partition_input_rows": 0}
    with tempfile.TemporaryDirectory(prefix="quant-i3-benchmark-"):

        def engine_factory(scope: tuple[InstrumentId, ...]) -> FactorEngine:
            frame = _bars(scope, sessions)
            statistics["max_partition_input_rows"] = max(
                statistics["max_partition_input_rows"], frame.height
            )
            service = _SyntheticBars(frame, statistics)
            registry = FactorRegistry()
            register_etf_factors(registry, service, scope)
            return FactorEngine(
                registry,
                capabilities=ProviderCapabilities.complete(),
            )

        universe_hash = hashlib.sha256(
            "\n".join(item.canonical() for item in instruments).encode()
        ).hexdigest()
        first_signal = date(2000, 1, 1) + timedelta(days=120)
        last_signal = date(2000, 1, 1) + timedelta(days=sessions - 1)
        ctx = FactorContext(
            "0" * 64,
            universe_hash,
            first_signal,
            last_signal,
        )
        executor = PartitionedFactorEngine(
            engine_factory, max_partition_size=partition_size
        )
        composite = executor.compute(
            _FACTOR_IDS,
            tuple(reversed(instruments)),
            ctx,
            partition_size=partition_size,
        )
        partitions = [
            {
                "index": partition.index,
                "instruments": len(partition.instrument_ids),
                "output_rows": partition.row_count,
                "universe_hash": partition.universe_hash,
            }
            for partition in composite.partitions
        ]
        output_rows = composite.row_count
        max_partition_output_rows = max(
            (partition.row_count for partition in composite.partitions), default=0
        )
        max_factor_artifact_rows = max(
            (
                artifact.row_count
                for partition in composite.partitions
                for artifact in partition.artifacts.values()
            ),
            default=0,
        )
        del composite, executor
        gc.collect()
    _, python_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "total_instruments": total_instruments,
        "sessions": sessions,
        "input_rows": total_instruments * sessions,
        "output_rows": output_rows,
        "partition_size": partition_size,
        "partition_count": len(partitions),
        "max_partition_input_rows": statistics["max_partition_input_rows"],
        "max_partition_output_rows": max_partition_output_rows,
        "max_factor_artifact_rows": max_factor_artifact_rows,
        "publish_row_group_limit": 65_536,
        "market_bar_reads": statistics["market_bar_reads"],
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
