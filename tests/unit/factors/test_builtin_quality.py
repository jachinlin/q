from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext
from quant_research.factors.builtin.quality import (
    FinancialIndicatorsCache,
    FinancialMetricFactor,
    RoeFactor,
)

_FIELDS = (
    "debt_to_assets",
    "grossprofit_margin",
    "netprofit_yoy",
    "ocf_to_opincome",
    "roa",
    "roe",
    "tr_yoy",
)
_FINANCIAL_SCHEMA = {
    "instrument_id": pl.String,
    "report_period": pl.Date,
    "revision": pl.Int64,
    "available_at": pl.Datetime("us", "UTC"),
    **{field: pl.Float64 for field in _FIELDS},
}


class _Provider:
    def __init__(self, rows: list[dict[str, object]], signals: list[date]) -> None:
        self._history = pl.DataFrame(rows, schema=_FINANCIAL_SCHEMA)
        self._signals = signals
        self.history_calls = 0
        self.calendar_calls = 0

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        self.calendar_calls += 1
        days = [day for day in self._signals if start <= day <= end]
        return pl.DataFrame(
            {"trade_date": days, "is_trading_day": [True] * len(days)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def stock_financial_indicators(
        self,
        as_of: date,
        instruments: tuple[InstrumentId, ...] | None = None,
    ) -> pl.LazyFrame:
        self.history_calls += 1
        assert instruments is not None
        instrument_ids = [instrument.canonical() for instrument in instruments]
        return self._history.filter(
            pl.col("instrument_id").is_in(instrument_ids)
            & (pl.col("available_at").dt.date() <= as_of)
        ).lazy()


def _row(
    *,
    period: date,
    available: date,
    revision: int = 0,
    instrument: str = "000001.SZ",
    **values: float | None,
) -> dict[str, object]:
    return {
        "instrument_id": instrument,
        "report_period": period,
        "revision": revision,
        "available_at": datetime.combine(available, datetime.min.time(), tzinfo=UTC),
        **{field: values.get(field, 0.1) for field in _FIELDS},
    }


def _context(start: date, end: date) -> FactorContext:
    return FactorContext("a" * 64, "b" * 64, start, end)


def _factor(
    provider: _Provider,
    cache: FinancialIndicatorsCache,
    factor_id: str,
    field: str,
    direction: int = 1,
    value_domain: str = "signed_finite",
    measurement: str = "point_in_time",
) -> FinancialMetricFactor:
    instrument = InstrumentId.parse("000001.SZ")
    return FinancialMetricFactor(
        provider,
        (instrument,),
        factor_id=factor_id,
        field=field,
        direction=direction,
        value_domain=value_domain,
        measurement=measurement,
        cache=cache,
    )


def test_financial_factors_share_one_pit_history_and_use_literal_values() -> None:
    signal = date(2026, 4, 30)
    row = _row(
        period=date(2025, 12, 31),
        available=date(2026, 4, 29),
        roe=0.12,
        tr_yoy=-0.08,
        netprofit_yoy=0.23,
        roa=-0.01,
        grossprofit_margin=0.41,
        ocf_to_opincome=-0.32,
        debt_to_assets=0.55,
    )
    provider = _Provider([row], [signal])
    instrument = InstrumentId.parse("000001.SZ")
    cache = FinancialIndicatorsCache(provider, (instrument,), _FIELDS)
    factors = (
        RoeFactor(provider, (instrument,), cache=cache),
        _factor(provider, cache, "revenue_growth", "tr_yoy", measurement="year_over_year"),
        _factor(provider, cache, "profit_growth", "netprofit_yoy", measurement="year_over_year"),
        _factor(provider, cache, "roa", "roa"),
        _factor(provider, cache, "gross_margin", "grossprofit_margin"),
        _factor(provider, cache, "cash_quality", "ocf_to_opincome"),
        _factor(
            provider,
            cache,
            "leverage",
            "debt_to_assets",
            direction=-1,
            value_domain="nonnegative_finite",
        ),
    )

    results = [factor.compute(_context(signal, signal)).collect() for factor in factors]

    assert [result["value"].item() for result in results] == [
        0.12,
        -0.08,
        0.23,
        -0.01,
        0.41,
        -0.32,
        0.55,
    ]
    assert all(result.schema == FACTOR_OUTPUT_SCHEMA for result in results)
    assert [factor.spec.direction for factor in factors] == [1, 1, 1, 1, 1, 1, -1]
    assert provider.calendar_calls == 1
    assert provider.history_calls == 1
    assert factors[1].spec.parameters["measurement"] == "year_over_year"
    assert factors[-1].spec.parameters["value_domain"] == "nonnegative_finite"
    assert factors[-1].spec.parameters["direction"] == -1


def test_roe_uses_latest_report_and_revision_without_falling_back() -> None:
    signals = [date(2026, 4, 29), date(2026, 4, 30), date(2026, 5, 1)]
    provider = _Provider(
        [
            _row(period=date(2025, 9, 30), available=date(2026, 1, 1), roe=0.10),
            _row(period=date(2025, 12, 31), available=signals[0], roe=0.12),
            _row(
                period=date(2025, 9, 30),
                available=signals[1],
                revision=1,
                roe=0.90,
            ),
            _row(
                period=date(2025, 12, 31),
                available=signals[2],
                revision=1,
                roe=float("nan"),
            ),
        ],
        signals,
    )
    factor = RoeFactor(provider, (InstrumentId.parse("000001.SZ"),))

    result = factor.compute(_context(signals[0], signals[-1])).collect()

    assert result.select("trade_date", "value", "is_valid").rows() == [
        (signals[0], 0.12, True),
        (signals[1], 0.12, True),
        (signals[2], None, False),
    ]
    assert factor.spec.factor_id == "roe"
    assert factor.spec.parameters["invalid_latest_record_fallback"] is False


def test_financial_staleness_boundary_is_inclusive() -> None:
    available = date(2026, 1, 1)
    signals = [available + timedelta(days=190), available + timedelta(days=191)]
    provider = _Provider(
        [_row(period=date(2025, 12, 31), available=available, roe=0.12)], signals
    )
    factor = RoeFactor(provider, (InstrumentId.parse("000001.SZ"),))

    result = factor.compute(_context(signals[0], signals[-1])).collect()

    assert result["value"].to_list() == [0.12, None]
    assert result["is_valid"].to_list() == [True, False]
    assert factor.spec.parameters["staleness_calendar_days"] == 190


def test_financial_factor_rejects_availability_after_signal_day_end() -> None:
    signal = date(2026, 4, 30)
    row = _row(period=date(2025, 12, 31), available=signal, roe=0.12)
    row["available_at"] = datetime(2026, 4, 30, 16, 1, tzinfo=UTC)
    factor = RoeFactor(
        _Provider([row], [signal]),
        (InstrumentId.parse("000001.SZ"),),
    )

    result = factor.compute(_context(signal, signal)).collect()

    assert result.select("value", "available_at", "is_valid").rows() == [
        (None, None, False)
    ]


@pytest.mark.parametrize("value", [-0.01, float("nan"), float("inf"), None])
def test_leverage_rejects_invalid_values(value: float | None) -> None:
    signal = date(2026, 4, 30)
    provider = _Provider(
        [
            _row(
                period=date(2025, 12, 31),
                available=date(2026, 4, 29),
                debt_to_assets=value,
            )
        ],
        [signal],
    )
    instrument = InstrumentId.parse("000001.SZ")
    cache = FinancialIndicatorsCache(provider, (instrument,), _FIELDS)

    result = _factor(
        provider,
        cache,
        "leverage",
        "debt_to_assets",
        direction=-1,
        value_domain="nonnegative_finite",
    ).compute(_context(signal, signal)).collect()

    assert result.select("value", "is_valid").rows() == [(None, False)]


def test_financial_factor_rejects_duplicate_revision_keys() -> None:
    signal = date(2026, 4, 30)
    row = _row(period=date(2025, 12, 31), available=date(2026, 4, 29), roe=0.12)
    factor = RoeFactor(
        _Provider([row, dict(row)], [signal]),
        (InstrumentId.parse("000001.SZ"),),
    )

    with pytest.raises(ValueError, match="duplicate financial revision key"):
        factor.compute(_context(signal, signal))
