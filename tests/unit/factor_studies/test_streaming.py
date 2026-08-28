from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import quant_research.factor_studies.streaming as streaming_module
from quant_research.factor_studies.analysis import (
    DIRECTION_ADJUSTED,
    EXECUTABLE_FORWARD_RETURN,
    INDUSTRY_NEUTRALIZED,
    THEORETICAL_FORWARD_RETURN,
    analyze,
    build_future_returns,
)
from quant_research.factor_studies.streaming import (
    FactorStudyTemporaryStore,
    StreamingForwardReturnBuilder,
    StreamingStudyAnalyzer,
)


class _Cancellation:
    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled

    def is_cancelled(self) -> bool:
        return self.cancelled


def _inputs() -> tuple[
    tuple[date, ...], pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame
]:
    sessions = tuple(date(2026, 1, 5) + timedelta(days=index) for index in range(28))
    instruments = tuple(f"{index:06d}.SZ" for index in range(5))
    eligible = pl.DataFrame(
        [
            {"signal_date": day, "instrument_id": instrument_id, "eligible": True}
            for day in sessions[:-2]
            for instrument_id in instruments
        ]
    )
    bars = pl.DataFrame(
        [
            {
                "trade_date": day,
                "instrument_id": instrument_id,
                "open": 10.0 + day_index / 10.0 + instrument_index / 100.0,
                "close": 10.1 + day_index / 10.0 + instrument_index / 100.0,
            }
            for day_index, day in enumerate(sessions)
            for instrument_index, instrument_id in enumerate(instruments)
        ]
    )
    executable = bars.select("trade_date", "instrument_id").with_columns(
        pl.lit(True).alias("is_listed"),
        pl.lit(False).alias("is_suspended"),
        pl.lit(False).alias("entry_limit_up"),
    )
    factor_ids = ("alpha", "beta", "delta", "epsilon", "gamma")
    factors = pl.DataFrame(
        [
            {
                "signal_date": day,
                "instrument_id": instrument_id,
                "factor_id": factor_id,
                "value": float(
                    (instrument_index + day_index * (factor_index + 1)) % 7
                ),
                "available_at": None,
                "is_valid": not (
                    factor_id == "beta"
                    and instrument_index == 0
                    and day_index % 4 == 0
                ),
                "invalid_reason": None,
                "signal_variant": DIRECTION_ADJUSTED,
            }
            for factor_index, factor_id in enumerate(factor_ids)
            for day_index, day in enumerate(sessions[:-2])
            for instrument_index, instrument_id in enumerate(instruments)
        ]
    ).sample(fraction=1.0, shuffle=True, seed=19)
    return sessions, eligible, bars, executable, factors


def test_wide_labels_project_exact_public_contract() -> None:
    sessions, eligible, bars, executable, _ = _inputs()
    expected = build_future_returns(
        bars, sessions, eligible, (1, 2), executable
    )
    builder = StreamingForwardReturnBuilder(
        bars, sessions, eligible, executable
    )

    for horizon in (1, 2):
        wide = builder.build(horizon)
        for label_kind in (
            THEORETICAL_FORWARD_RETURN,
            EXECUTABLE_FORWARD_RETURN,
        ):
            assert_frame_equal(
                StreamingForwardReturnBuilder.project(
                    wide, horizon, label_kind
                ),
                expected[(horizon, label_kind)],
                check_row_order=True,
                check_column_order=True,
            )


def test_streaming_analysis_matches_in_memory_for_five_factors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions, eligible, bars, executable, factors = _inputs()
    study_factors = pl.concat(
        [
            factors,
            factors.with_columns(
                pl.lit(INDUSTRY_NEUTRALIZED).alias("signal_variant")
            ),
        ]
    )
    future = build_future_returns(bars, sessions, eligible, (1, 2), executable)
    expected = analyze(
        study_factors,
        eligible,
        future,
        quantiles=3,
        cost_bps_scenarios=(5, 10),
        minimum=3,
    )
    assignment_count = 0
    original_assign = streaming_module.assign_quantiles

    def counted_assign(frame: pl.DataFrame, quantiles: int) -> pl.DataFrame:
        nonlocal assignment_count
        assignment_count += 1
        return original_assign(frame, quantiles)

    monkeypatch.setattr(streaming_module, "assign_quantiles", counted_assign)
    with FactorStudyTemporaryStore(
        tmp_path, "01M14STREAMINGANALYSIS001"
    ) as temporary:
        signal_files = {}
        for variant in (DIRECTION_ADJUSTED, INDUSTRY_NEUTRALIZED):
            for factor_ref in ("alpha", "beta", "delta", "epsilon", "gamma"):
                signal_files[(variant, factor_ref)] = temporary.write(
                    "signal",
                    study_factors.filter(
                        (pl.col("factor_id") == factor_ref)
                        & (pl.col("signal_variant") == variant)
                    ).sort("signal_date", "instrument_id", "factor_id"),
                )
        builder = StreamingForwardReturnBuilder(
            bars, sessions, eligible, executable
        )
        label_files = {
            horizon: temporary.write("label", builder.build(horizon))
            for horizon in (1, 2)
        }
        actual = StreamingStudyAnalyzer(
            quantiles=3,
            cost_bps_scenarios=(5, 10),
            minimum=3,
            cancellation=_Cancellation(),
            temporary=temporary,
        ).run(signal_files, eligible, label_files)

    for name in expected:
        assert actual[name].schema == expected[name].schema
        assert_frame_equal(
            actual[name],
            expected[name],
            check_row_order=True,
            check_column_order=True,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
    assert actual["label_quality"].schema["count"] == pl.Int64
    assert actual["label_quality"].schema["eligible_count"] == pl.Int64
    assert assignment_count == 5


def test_temporary_store_cleans_success_failure_retry_and_rejects_paths(
    tmp_path: Path,
) -> None:
    store = FactorStudyTemporaryStore(tmp_path, "01M14STREAMINGCLEANUP001")
    with store:
        store.write("signal", pl.DataFrame({"value": [1]}))
        assert store.directory.is_dir()
    assert not store.directory.exists()

    store.directory.mkdir(parents=True)
    (store.directory / "stale.parquet").touch()
    with pytest.raises(RuntimeError, match="boom"), store:
        assert not (store.directory / "stale.parquet").exists()
        raise RuntimeError("boom")
    assert not store.directory.exists()

    with pytest.raises(ValueError, match="id is invalid"):
        FactorStudyTemporaryStore(tmp_path, "../outside")


def test_streaming_analysis_checks_cancellation_between_units(
    tmp_path: Path,
) -> None:
    sessions, eligible, bars, executable, factors = _inputs()
    cancellation = _Cancellation(cancelled=True)
    with FactorStudyTemporaryStore(
        tmp_path, "01M14STREAMINGCANCEL0001"
    ) as temporary:
        signal = temporary.write("signal", factors.filter(pl.col("factor_id") == "alpha"))
        label = temporary.write(
            "label",
            StreamingForwardReturnBuilder(
                bars, sessions, eligible, executable
            ).build(1),
        )
        with pytest.raises(RuntimeError, match="factor study cancelled"):
            StreamingStudyAnalyzer(
                quantiles=3,
                cost_bps_scenarios=(5,),
                minimum=3,
                cancellation=cancellation,
                temporary=temporary,
            ).run({(DIRECTION_ADJUSTED, "alpha"): signal}, eligible, {1: label})


def test_temporary_store_cleans_after_parquet_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FactorStudyTemporaryStore(tmp_path, "01M14STREAMINGDISKFAIL01")

    def fail_write(_frame: pl.DataFrame, _path: Path, **_options: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_write)
    with pytest.raises(OSError, match="disk full"), store:
        store.write("signal", pl.DataFrame({"value": [1]}))
    assert not store.directory.exists()


def test_streaming_keeps_distinct_executable_label_statistics(
    tmp_path: Path,
) -> None:
    sessions, eligible, bars, executable, factors = _inputs()
    executable = executable.with_row_index("_row").with_columns(
        (pl.col("_row") == 1).alias("is_suspended")
    ).drop("_row")
    factor = factors.filter(pl.col("factor_id") == "alpha")
    future = build_future_returns(bars, sessions, eligible, (1,), executable)
    expected = analyze(
        factor,
        eligible,
        future,
        quantiles=3,
        cost_bps_scenarios=(5,),
        minimum=3,
    )
    with FactorStudyTemporaryStore(
        tmp_path, "01M14STREAMINGDISTINCT001"
    ) as temporary:
        signal = temporary.write("signal", factor)
        label = temporary.write(
            "label",
            StreamingForwardReturnBuilder(
                bars, sessions, eligible, executable
            ).build(1),
        )
        actual = StreamingStudyAnalyzer(
            quantiles=3,
            cost_bps_scenarios=(5,),
            minimum=3,
            cancellation=_Cancellation(),
            temporary=temporary,
        ).run(
            {(DIRECTION_ADJUSTED, "alpha"): signal},
            eligible,
            {1: label},
        )

    for name in expected:
        assert_frame_equal(
            actual[name],
            expected[name],
            check_row_order=True,
            check_column_order=True,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
