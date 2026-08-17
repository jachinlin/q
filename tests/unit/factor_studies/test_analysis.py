from __future__ import annotations

import hashlib
from datetime import date, timedelta
from io import BytesIO

import polars as pl
import pytest

from quant_research.factor_studies.analysis import analyze, build_future_returns


def test_future_return_uses_next_open_and_horizon_close() -> None:
    sessions = tuple(date(2026, 1, 5) + timedelta(days=index) for index in range(4))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"] * 4,
            "trade_date": list(sessions),
            "open": [9.0, 10.0, 11.0, 12.0],
            "close": [9.5, 11.0, 12.0, 13.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )

    result = build_future_returns(bars, sessions, eligible, (1, 2))

    assert result[1]["future_return"].item() == pytest.approx(0.1)
    assert result[2]["future_return"].item() == pytest.approx(0.2)
    assert result[1].select("return_start", "return_end").row(0) == (
        sessions[1],
        sessions[1],
    )


def test_future_return_keeps_incomplete_window_null() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"],
            "trade_date": [sessions[0]],
            "open": [10.0],
            "close": [10.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )
    assert (
        build_future_returns(bars, sessions, eligible, (1,))[1]["future_return"].item()
        is None
    )


def test_future_return_rejects_suspended_next_session() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ", "000001.SZ"],
            "trade_date": list(sessions),
            "open": [9.0, 10.0],
            "close": [9.5, 11.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )
    tradability = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"],
            "trade_date": [sessions[1]],
            "is_listed": [True],
            "is_suspended": [True],
        }
    )

    result = build_future_returns(bars, sessions, eligible, (1,), tradability)

    assert result[1]["future_return"].item() is None


def test_future_return_rejects_missing_next_session_status() -> None:
    sessions = (date(2026, 1, 5), date(2026, 1, 6))
    bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ", "000001.SZ"],
            "trade_date": list(sessions),
            "open": [9.0, 10.0],
            "close": [9.5, 11.0],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0]],
            "instrument_id": ["000001.SZ"],
            "eligible": [True],
        }
    )
    empty_status = pl.DataFrame(
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "is_listed": pl.Boolean,
            "is_suspended": pl.Boolean,
        }
    )

    result = build_future_returns(bars, sessions, eligible, (1,), empty_status)

    assert result[1]["future_return"].item() is None


def test_future_returns_vectorize_shuffled_multi_instrument_scope() -> None:
    sessions = tuple(date(2026, 1, 5) + timedelta(days=index) for index in range(3))
    bars = pl.DataFrame(
        {
            "instrument_id": [
                "000002.SZ",
                "000001.SZ",
                "000002.SZ",
                "000001.SZ",
                "000002.SZ",
                "000001.SZ",
            ],
            "trade_date": [
                sessions[2],
                sessions[1],
                sessions[0],
                sessions[2],
                sessions[1],
                sessions[0],
            ],
            "open": [22.0, 10.0, 18.0, 11.0, 20.0, 9.0],
            "close": [24.0, 11.0, 19.0, 12.0, 21.0, 9.5],
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [sessions[0], sessions[0], sessions[0]],
            "instrument_id": ["000002.SZ", "000003.SZ", "000001.SZ"],
            "eligible": [True, False, True],
        }
    )

    result = build_future_returns(bars, sessions, eligible, (2,))[2]

    assert result.select("instrument_id", "return_start", "return_end").rows() == [
        ("000001.SZ", sessions[1], sessions[2]),
        ("000002.SZ", sessions[1], sessions[2]),
    ]
    assert result["future_return"].to_list() == pytest.approx([0.2, 0.2])


def test_analysis_produces_rank_quantiles_and_long_short_without_compounding() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "factor_id": ["momentum_120_20"] * 30,
            "value": [float(index) for index in range(30)],
            "is_valid": [True] * 30,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )
    returns = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 30,
            "return_end": [date(2026, 1, 6)] * 30,
            "future_return": [index / 100.0 for index in range(30)],
        }
    )

    result = analyze(factors, eligible, {1: returns}, quantiles=5)

    assert result["ic"].select("pearson_ic", "rank_ic").row(0) == pytest.approx(
        (1.0, 1.0)
    )
    groups = result["quantile_returns"].select("quantile", "count", "mean_return")
    assert groups["count"].to_list() == [6, 6, 6, 6, 6]
    assert result["long_short_returns"]["long_short_return"].item() == pytest.approx(
        0.24
    )


def test_analysis_invalidates_horizon_with_too_few_future_pairs() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "factor_id": ["roe_pit"] * 30,
            "value": [float(index) for index in range(30)],
            "is_valid": [True] * 30,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )
    future = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 30,
            "return_end": [date(2026, 1, 6)] * 30,
            "future_return": [
                index / 100.0 if index < 29 else None for index in range(30)
            ],
        }
    )

    result = analyze(factors, eligible, {1: future}, quantiles=5)

    assert result["ic"].select(
        "pearson_ic", "rank_ic", "is_valid", "invalid_reason"
    ).row(0) == (
        None,
        None,
        False,
        "INSUFFICIENT_FORWARD_PAIRS",
    )
    assert result["quantile_returns"]["mean_return"].null_count() == 5
    assert result["long_short_returns"]["is_valid"].item() is False


def test_analysis_emits_stable_one_five_twenty_day_ic_decay() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(30)]
    values = [float(index) for index in range(30)]
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "factor_id": ["momentum_120_20"] * 30,
            "value": values,
            "is_valid": [True] * 30,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 30,
            "instrument_id": instruments,
            "eligible": [True] * 30,
        }
    )

    def future(horizon: int, returns: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "signal_date": [day] * 30,
                "instrument_id": instruments,
                "return_start": [day + timedelta(days=1)] * 30,
                "return_end": [day + timedelta(days=horizon)] * 30,
                "future_return": returns,
            }
        )

    labels = {
        1: future(1, values),
        5: future(5, [value**2 for value in values]),
        20: future(20, [-value for value in values]),
    }
    first = analyze(factors, eligible, labels, quantiles=5)
    second = analyze(
        factors,
        eligible,
        {20: labels[20], 1: labels[1], 5: labels[5]},
        quantiles=5,
    )

    decay = first["summary"].select("horizon", "pearson_ic_mean", "rank_ic_mean")
    assert decay["horizon"].to_list() == [1, 5, 20]
    assert decay["pearson_ic_mean"].to_list() == pytest.approx(
        [1.0, 0.9662730800150235, -1.0]
    )
    assert decay["rank_ic_mean"].to_list() == pytest.approx([1.0, 1.0, -1.0])

    def output_hash(outputs: dict[str, pl.DataFrame]) -> str:
        digest = hashlib.sha256()
        for name in sorted(outputs):
            buffer = BytesIO()
            outputs[name].write_parquet(buffer, compression="zstd")
            digest.update(name.encode("utf-8"))
            digest.update(buffer.getvalue())
        return digest.hexdigest()

    assert output_hash(first) == output_hash(second)


def test_analysis_invalidates_factor_correlation_with_too_few_common_pairs() -> None:
    day = date(2026, 1, 5)
    instruments = [f"{index:06d}.SZ" for index in range(31)]
    rows = []
    for factor_ref in ("roe_pit", "book_to_price_mrq"):
        for index, instrument_id in enumerate(instruments):
            rows.append(
                {
                    "signal_date": day,
                    "instrument_id": instrument_id,
                    "factor_id": factor_ref,
                    "value": float(index),
                    "is_valid": index != (30 if factor_ref == "roe_pit" else 0),
                }
            )
    factors = pl.DataFrame(rows)
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 31,
            "instrument_id": instruments,
            "eligible": [True] * 31,
        }
    )
    future = pl.DataFrame(
        {
            "signal_date": [day] * 31,
            "instrument_id": instruments,
            "return_start": [date(2026, 1, 6)] * 31,
            "return_end": [date(2026, 1, 6)] * 31,
            "future_return": [index / 100.0 for index in range(31)],
        }
    )

    result = analyze(factors, eligible, {1: future}, quantiles=5)
    cross = result["correlation"].filter(
        (pl.col("factor_x") == "roe_pit") & (pl.col("factor_y") == "book_to_price_mrq")
    )

    assert cross.select("correlation", "is_valid").row(0) == (None, False)
