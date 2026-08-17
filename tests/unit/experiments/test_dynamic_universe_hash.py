"""动态股票池身份覆盖全部实际调仓信号日的测试。"""

from datetime import date
from types import SimpleNamespace
from typing import Any, ClassVar

import polars as pl
import pytest

import quant_research.bootstrap.worker as runtime_module
from quant_research.strategies import RebalanceFrequency, rebalance_signal_dates

_SESSIONS = (
    date(2026, 1, 29),
    date(2026, 1, 30),
    date(2026, 2, 2),
    date(2026, 2, 27),
    date(2026, 3, 2),
)


class _Catalog:
    def require_validated_catalog(self) -> object:
        return SimpleNamespace(catalog_hash="a" * 64)


class _Repository:
    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        del start, end
        return pl.DataFrame(
            {"trade_date": _SESSIONS, "is_trading_day": [True] * len(_SESSIONS)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()


class _UniverseBuilder:
    rows_by_date: ClassVar[dict[date, tuple[bool, tuple[str, ...]]]] = {}
    calls: ClassVar[list[date]] = []

    def __init__(self, repository: object) -> None:
        del repository

    def build(self, as_of: date, rules: object) -> pl.DataFrame:
        del rules
        self.calls.append(as_of)
        eligible, reasons = self.rows_by_date[as_of]
        return pl.DataFrame(
            {
                "instrument_id": ["600001.SH"],
                "as_of": [as_of],
                "eligible": [eligible],
                "reason_codes": [list(reasons)],
            },
            schema={
                "instrument_id": pl.String,
                "as_of": pl.Date,
                "eligible": pl.Boolean,
                "reason_codes": pl.List(pl.String),
            },
        )


def _prepared_runtime() -> Any:
    prepared = object.__new__(runtime_module._ConcreteExperimentRuntime)
    prepared._experiment = SimpleNamespace(data_hash="a" * 64)
    prepared._catalog = _Catalog()
    prepared._repository = _Repository()
    prepared._start = _SESSIONS[0]
    prepared._end = _SESSIONS[-1]
    prepared._strategy_ref = runtime_module._STOCK_REF
    prepared._strategy = SimpleNamespace(
        config=SimpleNamespace(frequency=RebalanceFrequency.MONTHLY)
    )
    prepared._rules = object()
    prepared._validated = True
    prepared._instruments = (object(),)
    return prepared


def test_universe_hash_reads_exact_actual_signal_dates_and_changes_with_later_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_dates = rebalance_signal_dates(_SESSIONS, RebalanceFrequency.MONTHLY)
    _UniverseBuilder.calls = []
    _UniverseBuilder.rows_by_date = {
        signal_date: (True, ()) for signal_date in signal_dates
    }
    monkeypatch.setattr(runtime_module, "UniverseBuilder", _UniverseBuilder)
    prepared = _prepared_runtime()

    first = prepared.build_universe()
    _UniverseBuilder.rows_by_date[signal_dates[-1]] = (False, ("SUSPENDED",))
    second = prepared.build_universe()

    assert first.signal_dates == signal_dates
    assert _UniverseBuilder.calls == [*signal_dates, *signal_dates]
    assert first.universe_hash != second.universe_hash
