"""Integration contracts for the daily backtest event loop."""

from datetime import date
from pathlib import Path
from uuid import UUID

import polars as pl
import pyarrow.parquet as pq

from quant_core.backtest.accounting import CorporateAction
from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.engine import BacktestEngine, BacktestRequest, StrategyRef
from quant_core.backtest.models import ExecutionConfig, ExecutionPrice, MarketSlice
from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.portfolio import RebalancePlanner, TargetPortfolio, TargetPosition

_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000001")
_EXPERIMENT = UUID("00000000-0000-0000-0000-000000000002")
_BENCHMARK = InstrumentId.parse("SSE:000001")
_STOCK = InstrumentId.parse("SSE:600001")
_FRIDAY = date(2024, 1, 5)
_MONDAY = date(2024, 1, 8)
_TUESDAY = date(2024, 1, 9)


class _RuleBook:
    version = "test-v1"

    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int:
        return 100

    def price_limits(self, *args: object) -> None:
        return None

    def fees(self, *args: object) -> FeeBreakdown:
        return FeeBreakdown(0, 0, 0, 0)


class _Data:
    def __init__(self) -> None:
        self.calendar_value = TradingCalendar(
            SnapshotId(_SNAPSHOT), _FRIDAY, _TUESDAY, (_FRIDAY, _MONDAY, _TUESDAY)
        )
        self.slices = {
            _FRIDAY: _slice(_FRIDAY, stock_close=10.0),
            _MONDAY: _slice(_MONDAY, stock_close=12.0),
            _TUESDAY: _slice(_TUESDAY, stock_close=11.0),
        }

    def calendar(self, snapshot_id: UUID, start: date, end: date) -> TradingCalendar:
        assert (snapshot_id, start, end) == (_SNAPSHOT, _FRIDAY, _TUESDAY)
        return self.calendar_value

    def market_slice(self, snapshot_id: UUID, trade_date: date) -> MarketSlice:
        assert snapshot_id == _SNAPSHOT
        return self.slices[trade_date]

    def corporate_actions(
        self, snapshot_id: UUID, trade_date: date
    ) -> tuple[CorporateAction, ...]:
        assert snapshot_id == _SNAPSHOT
        return ()


class _Targets:
    def __init__(self) -> None:
        self.calls: list[tuple[date, date]] = []

    def generate_target(
        self,
        strategy: StrategyRef,
        snapshot_id: UUID,
        signal_date: date,
        execute_date: date,
        current: object,
    ) -> TargetPortfolio | None:
        assert (strategy.strategy_id, snapshot_id) == ("timeline", _SNAPSHOT)
        self.calls.append((signal_date, execute_date))
        if signal_date != _FRIDAY:
            return None
        return TargetPortfolio(
            signal_date,
            execute_date,
            (TargetPosition(_STOCK, 1.0, 2.0, "TEST"),),
            0.0,
        )


class _Progress:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, date]] = []

    def update(self, completed: int, total: int, trade_date: date) -> None:
        self.calls.append((completed, total, trade_date))


class _NeverCancelled:
    def is_cancelled(self) -> bool:
        return False


def _slice(trade_date: date, *, stock_close: float) -> MarketSlice:
    rows = []
    for instrument, close in ((_BENCHMARK, 3.0), (_STOCK, stock_close)):
        rows.append(
            {
                "instrument_id": instrument.canonical(),
                "open": close - 1.0 if instrument == _STOCK else close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "preclose": close,
                "volume": 10_000,
                "is_suspended": False,
                "security_status": "NORMAL",
            }
        )
    return MarketSlice(trade_date, pl.DataFrame(rows))


def _request() -> BacktestRequest:
    return BacktestRequest(
        _EXPERIMENT,
        _SNAPSHOT,
        StrategyRef("timeline", "1"),
        _FRIDAY,
        _TUESDAY,
        _BENCHMARK,
        200_000,
        "test-v1",
        ExecutionConfig(ExecutionPrice.CLOSE, 0.0, 1.0),
    )


def test_engine_generates_after_close_and_executes_on_next_session(
    tmp_path: Path,
) -> None:
    data = _Data()
    targets = _Targets()
    progress = _Progress()
    result = BacktestEngine(
        data,
        targets,
        _RuleBook(),
        RebalancePlanner(),
        artifact_root=tmp_path,
    ).run(_request(), progress, _NeverCancelled())

    fills = pq.read_table(result.artifact_dir / "fills.parquet").to_pylist()
    targets_rows = pq.read_table(result.artifact_dir / "targets.parquet").to_pylist()
    nav = pq.read_table(result.artifact_dir / "nav.parquet").to_pylist()

    assert targets.calls == [(_FRIDAY, _MONDAY), (_MONDAY, _TUESDAY)]
    assert [(row["signal_date"], row["execute_date"]) for row in targets_rows] == [
        (_FRIDAY, _MONDAY),
        (_FRIDAY, _MONDAY),
    ]
    assert [(row["trade_date"], row["price"]) for row in fills] == [(_MONDAY, 12.0)]
    assert [(row["trade_date"], row["benchmark_close"]) for row in nav] == [
        (_FRIDAY, 3.0),
        (_MONDAY, 3.0),
        (_TUESDAY, 3.0),
    ]
    assert progress.calls == [
        (1, 3, _FRIDAY),
        (2, 3, _MONDAY),
        (3, 3, _TUESDAY),
    ]
    assert result.sessions_completed == 3
