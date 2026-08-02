"""Integration contracts for the daily backtest event loop."""

from dataclasses import replace
from datetime import date
from pathlib import Path
from uuid import UUID

import polars as pl
import pyarrow.parquet as pq
import pytest

import quant_core.backtest.engine as engine_module
from quant_core.backtest.accounting import CorporateAction
from quant_core.backtest.calendar import TradingCalendar
from quant_core.backtest.engine import (
    BacktestEngine,
    BacktestRequest,
    SnapshotMarketSlice,
    StrategyRef,
)
from quant_core.backtest.models import ExecutionConfig, ExecutionPrice, MarketSlice
from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.portfolio import RebalancePlanner, TargetPortfolio, TargetPosition

_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000001")
_WRONG_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000099")
_EXPERIMENT = UUID("00000000-0000-0000-0000-000000000002")
_BENCHMARK = InstrumentId.parse("SSE:000001")
_STOCK = InstrumentId.parse("SSE:600001")
_FRIDAY = date(2024, 1, 5)
_MONDAY = date(2024, 1, 8)
_TUESDAY = date(2024, 1, 9)
_NEXT_SESSION = date(2024, 1, 12)


class _RuleBook:
    version = "test-v1"

    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int:
        return 100

    def price_limits(self, *args: object) -> None:
        return None

    def fees(self, *args: object) -> FeeBreakdown:
        return FeeBreakdown(100, 0, 0, 100)


class _Data:
    def __init__(self) -> None:
        self.calendar_value = TradingCalendar(
            SnapshotId(_SNAPSHOT),
            _FRIDAY,
            _NEXT_SESSION,
            (_FRIDAY, _MONDAY, _TUESDAY, _NEXT_SESSION),
        )
        self.slices = {
            _FRIDAY: _slice(_FRIDAY, stock_close=10.0),
            _MONDAY: _slice(_MONDAY, stock_close=12.0),
            _TUESDAY: _slice(_TUESDAY, stock_close=11.0),
        }

    def calendar(
        self,
        snapshot_id: UUID,
        start: date,
        end: date,
        *,
        include_next_session: bool,
    ) -> TradingCalendar:
        assert (snapshot_id, start, end, include_next_session) == (
            _SNAPSHOT,
            _FRIDAY,
            _TUESDAY,
            True,
        )
        return self.calendar_value

    def market_slice(self, snapshot_id: UUID, trade_date: date) -> SnapshotMarketSlice:
        assert snapshot_id == _SNAPSHOT
        return SnapshotMarketSlice(_SNAPSHOT, self.slices[trade_date])

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
            return TargetPortfolio(signal_date, execute_date, (), 1.0)
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
        (_MONDAY, _TUESDAY),
    ]
    assert [(row["trade_date"], row["price"]) for row in fills] == [
        (_MONDAY, 12.0),
        (_TUESDAY, 11.0),
    ]
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


def test_end_date_buy_uses_requested_next_session_coverage_and_publishes(
    tmp_path: Path,
) -> None:
    """Omitting the post-end session makes a final-session BUY abort at T+1."""

    class CoverageData(_Data):
        def __init__(self) -> None:
            super().__init__()
            self.calendar_calls: list[tuple[UUID, date, date, bool]] = []

        def calendar(
            self,
            snapshot_id: UUID,
            start: date,
            end: date,
            *,
            include_next_session: bool = False,
        ) -> TradingCalendar:
            self.calendar_calls.append((snapshot_id, start, end, include_next_session))
            if include_next_session:
                return TradingCalendar(
                    SnapshotId(_SNAPSHOT),
                    start,
                    _NEXT_SESSION,
                    (_MONDAY, _TUESDAY, _NEXT_SESSION),
                )
            return TradingCalendar(
                SnapshotId(_SNAPSHOT), start, end, (_MONDAY, _TUESDAY)
            )

    class FinalSessionBuyTargets:
        def generate_target(
            self,
            strategy: StrategyRef,
            snapshot_id: UUID,
            signal_date: date,
            execute_date: date,
            current: object,
        ) -> TargetPortfolio:
            del strategy, snapshot_id, current
            return TargetPortfolio(
                signal_date,
                execute_date,
                (TargetPosition(_STOCK, 1.0, 2.0, "TEST"),),
                0.0,
            )

    data = CoverageData()
    request = replace(
        _request(),
        experiment_id=UUID("00000000-0000-0000-0000-000000000011"),
        start_date=_MONDAY,
    )

    result = BacktestEngine(
        data,
        FinalSessionBuyTargets(),
        _RuleBook(),
        RebalancePlanner(),
        artifact_root=tmp_path,
    ).run(request, _Progress(), _NeverCancelled())

    assert data.calendar_calls == [
        (_SNAPSHOT, _MONDAY, _TUESDAY, True),
    ]
    assert result.manifest_path.is_file()
    assert result.final_snapshot.trade_date == _TUESDAY
    assert result.final_snapshot.positions[0].sellable_quantity == 0


def test_engine_rejects_missing_post_end_session_before_market_loop(
    tmp_path: Path,
) -> None:
    """A provider ignoring next-session coverage must fail before simulating data."""

    class InsufficientCalendarData(_Data):
        def __init__(self) -> None:
            super().__init__()
            self.market_slice_calls = 0

        def calendar(
            self,
            snapshot_id: UUID,
            start: date,
            end: date,
            *,
            include_next_session: bool = False,
        ) -> TradingCalendar:
            del include_next_session
            return TradingCalendar(
                SnapshotId(snapshot_id), start, end, (_MONDAY, _TUESDAY)
            )

        def market_slice(
            self, snapshot_id: UUID, trade_date: date
        ) -> SnapshotMarketSlice:
            self.market_slice_calls += 1
            return super().market_slice(snapshot_id, trade_date)

    class NoneTargets:
        def generate_target(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    data = InsufficientCalendarData()
    request = replace(
        _request(),
        experiment_id=UUID("00000000-0000-0000-0000-000000000012"),
        start_date=_MONDAY,
    )

    with pytest.raises(ValueError, match="next trading session"):
        BacktestEngine(
            data,
            NoneTargets(),
            _RuleBook(),
            RebalancePlanner(),
            artifact_root=tmp_path,
        ).run(request, _Progress(), _NeverCancelled())

    assert data.market_slice_calls == 0


def test_engine_rejects_same_date_slice_from_another_snapshot(tmp_path: Path) -> None:
    class WrongSnapshotData(_Data):
        def market_slice(
            self, snapshot_id: UUID, trade_date: date
        ) -> SnapshotMarketSlice:
            return SnapshotMarketSlice(_WRONG_SNAPSHOT, self.slices[trade_date])

    with pytest.raises(ValueError, match="snapshot"):
        BacktestEngine(
            WrongSnapshotData(),
            _Targets(),
            _RuleBook(),
            RebalancePlanner(),
            artifact_root=tmp_path,
        ).run(_request(), _Progress(), _NeverCancelled())

    assert not list(tmp_path.glob("experiment_id=*/manifest.json"))
    assert list(tmp_path.glob(".staging-*/diagnostic.json"))


def test_engine_uses_next_session_open_when_configured(tmp_path: Path) -> None:
    request = replace(
        _request(),
        experiment_id=UUID("00000000-0000-0000-0000-000000000007"),
        execution_config=ExecutionConfig(ExecutionPrice.OPEN, 0.0, 1.0),
    )
    result = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    ).run(request, _Progress(), _NeverCancelled())

    fills = pq.read_table(result.artifact_dir / "fills.parquet").to_pylist()
    assert [(row["trade_date"], row["price"]) for row in fills] == [
        (_MONDAY, 11.0),
        (_TUESDAY, 10.0),
    ]


def test_engine_orders_begin_execution_mark_then_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[tuple[str, date]] = []
    begin = engine_module.PortfolioAccount.begin_session
    execute = engine_module.ExecutionModel.execute
    mark = engine_module.PortfolioAccount.mark_to_market

    def record_begin(self: object, trade_date: date, actions: object) -> None:
        events.append(("begin", trade_date))
        begin(self, trade_date, actions)

    def record_execute(self: object, *args: object, **kwargs: object):
        market = args[1]
        assert isinstance(market, MarketSlice)
        events.append(("execute", market.trade_date))
        return execute(self, *args, **kwargs)

    def record_mark(self: object, trade_date: date, closes: object):
        events.append(("mark", trade_date))
        return mark(self, trade_date, closes)

    class RecordingTargets(_Targets):
        def generate_target(
            self, *args: object, **kwargs: object
        ) -> TargetPortfolio | None:
            events.append(("target", args[2]))
            return super().generate_target(*args, **kwargs)

    monkeypatch.setattr(engine_module.PortfolioAccount, "begin_session", record_begin)
    monkeypatch.setattr(engine_module.ExecutionModel, "execute", record_execute)
    monkeypatch.setattr(engine_module.PortfolioAccount, "mark_to_market", record_mark)
    BacktestEngine(
        _Data(),
        RecordingTargets(),
        _RuleBook(),
        RebalancePlanner(),
        artifact_root=tmp_path,
    ).run(_request(), _Progress(), _NeverCancelled())

    assert events == [
        ("begin", _FRIDAY),
        ("execute", _FRIDAY),
        ("mark", _FRIDAY),
        ("target", _FRIDAY),
        ("begin", _MONDAY),
        ("execute", _MONDAY),
        ("mark", _MONDAY),
        ("target", _MONDAY),
        ("begin", _TUESDAY),
        ("execute", _TUESDAY),
        ("mark", _TUESDAY),
    ]


def test_manifest_retains_non_session_request_boundary(tmp_path: Path) -> None:
    request = replace(
        _request(),
        experiment_id=UUID("00000000-0000-0000-0000-000000000010"),
        start_date=date(2024, 1, 6),
    )

    class WeekendData(_Data):
        def calendar(
            self,
            snapshot_id: UUID,
            start: date,
            end: date,
            *,
            include_next_session: bool,
        ) -> TradingCalendar:
            assert (snapshot_id, start, end, include_next_session) == (
                _SNAPSHOT,
                date(2024, 1, 6),
                _TUESDAY,
                True,
            )
            return self.calendar_value

    result = BacktestEngine(
        WeekendData(),
        _Targets(),
        _RuleBook(),
        RebalancePlanner(),
        artifact_root=tmp_path,
    ).run(request, _Progress(), _NeverCancelled())

    manifest = __import__("json").loads(
        result.manifest_path.read_text(encoding="utf-8")
    )
    assert (
        result.sessions_completed,
        manifest["start_date"],
        manifest["end_date"],
    ) == (
        2,
        "2024-01-06",
        "2024-01-09",
    )


def test_engine_none_targets_writes_empty_rebalance_artifacts(tmp_path: Path) -> None:
    class NoneTargets:
        def generate_target(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    result = BacktestEngine(
        _Data(), NoneTargets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    ).run(_request(), _Progress(), _NeverCancelled())
    for name in ("targets.parquet", "fills.parquet", "costs.parquet"):
        assert pq.read_table(result.artifact_dir / name).num_rows == 0


def test_engine_cash_target_writes_cash_row_without_orders(tmp_path: Path) -> None:
    class CashTargets:
        def generate_target(
            self,
            strategy: object,
            snapshot: object,
            signal: date,
            execute: date,
            current: object,
        ) -> TargetPortfolio:
            del strategy, snapshot, current
            return TargetPortfolio(signal, execute, (), 1.0)

    result = BacktestEngine(
        _Data(), CashTargets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    ).run(_request(), _Progress(), _NeverCancelled())
    targets = pq.read_table(result.artifact_dir / "targets.parquet").to_pylist()
    assert len(targets) == 2 and all(row["instrument_id"] is None for row in targets)
    assert pq.read_table(result.artifact_dir / "fills.parquet").num_rows == 0
