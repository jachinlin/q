"""Contract tests for snapshot-bound experiment runtime adapters."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest

from quant_core.backtest.accounting import CorporateActionType
from quant_core.backtest.engine import BacktestRequest, StrategyRef
from quant_core.backtest.models import ExecutionConfig, ExecutionPrice
from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.data.contracts import ProviderCapabilities
from quant_core.data.sources.baostock import BAOSTOCK_CAPABILITIES
from quant_core.domain.enums import Board
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.experiments.adapters import (
    PIT_UNIVERSE_ENRICHMENT_SCHEMA,
    STRATEGY_UNIVERSE_SCHEMA,
    SnapshotBacktestMarketData,
    SnapshotStrategyContextProvider,
    SnapshotStrategyData,
    SnapshotStrategyRunner,
)
from quant_core.experiments.config import ExperimentCapabilityUnavailable
from quant_core.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    FactorArtifact,
    factor_table_content_hash,
)
from quant_core.portfolio import PortfolioConstructor, RebalancePlanner
from quant_core.strategies.base import StrategyContext, ValidationIssue
from quant_core.universe.rules import UniverseRules

_SNAPSHOT = SnapshotId(UUID("00000000-0000-0000-0000-000000000061"))
_OTHER_SNAPSHOT = SnapshotId(UUID("00000000-0000-0000-0000-000000000062"))
_EXPERIMENT = UUID("00000000-0000-0000-0000-000000000063")
_BENCHMARK = InstrumentId.parse("SSE:000300")
_STOCK = InstrumentId.parse("SSE:600000")
_SIGNAL = date(2024, 1, 31)
_NEXT = date(2024, 2, 2)
_AFTER_NEXT = date(2024, 2, 5)
_UNIVERSE_HASH = hashlib.sha256(b"unit-universe").hexdigest()
_FACTOR_REF = "alpha_v1@1.0.0"


class _Repository:
    """Adversarial in-memory implementation of the research-data protocol."""

    def __init__(self) -> None:
        calendar_days = [
            date(2024, 1, 1) + timedelta(days=index) for index in range(36)
        ]
        sessions = {
            day
            for day in calendar_days
            if day.weekday() < 5 and day != date(2024, 2, 1)
        }
        self.calendar_frame = pl.DataFrame(
            {
                "trade_date": calendar_days,
                "is_trading_day": [day in sessions for day in calendar_days],
            },
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        )
        self.instrument_frame = pl.DataFrame(
            [
                {
                    "instrument_id": _BENCHMARK.canonical(),
                    "instrument_type": "INDEX",
                    "list_date": date(2005, 1, 1),
                    "delist_date": None,
                },
                {
                    "instrument_id": _STOCK.canonical(),
                    "instrument_type": "STOCK",
                    "list_date": date(2005, 1, 1),
                    "delist_date": None,
                },
            ],
            schema={
                "instrument_id": pl.String,
                "instrument_type": pl.String,
                "list_date": pl.Date,
                "delist_date": pl.Date,
            },
        )
        bar_rows: list[dict[str, object]] = []
        for session in sorted(sessions):
            for instrument, close, amount in (
                (_BENCHMARK, 3.0, 3_000.0),
                (_STOCK, 10.0, 2_000.0),
            ):
                bar_rows.append(_bar_row(instrument, session, close, amount))
        self.bar_frame = pl.DataFrame(bar_rows)
        self.status_frame = pl.DataFrame(
            [
                _status_row(_BENCHMARK, _SIGNAL, risk=False),
                _status_row(_STOCK, _SIGNAL, risk=True),
            ]
        )
        self.action_frame = pl.DataFrame(
            [
                {
                    "instrument_id": _STOCK.canonical(),
                    "action_type": "DIVIDEND",
                    "record_date": date(2024, 1, 30),
                    "ex_date": _SIGNAL,
                    "pay_date": _NEXT,
                    "cash_per_share": 0.1,
                    "share_ratio": 0.2,
                    "rights_price": None,
                    "available_at": datetime(2024, 1, 30, 8, tzinfo=UTC),
                    "pit_usable": True,
                }
            ]
        )
        self.calls: list[tuple[str, SnapshotId]] = []

    def instruments(self, snapshot_id: SnapshotId) -> pl.LazyFrame:
        self.calls.append(("instruments", snapshot_id))
        return self.instrument_frame.lazy()

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        self.calls.append(("trade_calendar", snapshot_id))
        return self.calendar_frame.filter(
            pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        self.calls.append(("bars", snapshot_id))
        identifiers = [item.canonical() for item in instruments]
        return self.bar_frame.filter(
            pl.col("instrument_id").is_in(identifiers)
            & pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()

    def corporate_actions_as_of(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId] | None,
        as_of: date,
    ) -> pl.LazyFrame:
        self.calls.append(("corporate_actions", snapshot_id))
        frame = self.action_frame.filter(
            pl.col("pit_usable")
            & pl.col("available_at").is_not_null()
            & (pl.col("ex_date") <= as_of)
        )
        if instruments is not None:
            frame = frame.filter(
                pl.col("instrument_id").is_in(
                    [item.canonical() for item in instruments]
                )
            )
        return frame.lazy()

    def financials_as_of(self, *args: object, **kwargs: object) -> pl.LazyFrame:
        del args, kwargs
        raise AssertionError("financials are outside these adapter tests")

    def security_status(
        self,
        snapshot_id: SnapshotId,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        del as_of
        self.calls.append(("security_status", snapshot_id))
        frame = self.status_frame
        if instruments is not None:
            frame = frame.filter(
                pl.col("instrument_id").is_in(
                    [item.canonical() for item in instruments]
                )
            )
        return frame.lazy()


class _Enrichment:
    def __init__(self, frame: pl.DataFrame | None = None) -> None:
        self.frame = frame
        self.calls: list[tuple[SnapshotId, date, tuple[InstrumentId, ...]]] = []

    def values(
        self,
        snapshot_id: SnapshotId,
        signal_date: date,
        instruments: tuple[InstrumentId, ...],
    ) -> pl.DataFrame:
        self.calls.append((snapshot_id, signal_date, instruments))
        if self.frame is not None:
            return self.frame
        return pl.DataFrame(
            [
                {
                    "instrument_id": _BENCHMARK.canonical(),
                    "as_of": signal_date,
                    "total_shares": 2_000_000.0,
                    "industry": "BENCHMARK",
                },
                {
                    "instrument_id": _STOCK.canonical(),
                    "as_of": signal_date,
                    "total_shares": 1_000_000.0,
                    "industry": "BANK",
                },
            ],
            schema=PIT_UNIVERSE_ENRICHMENT_SCHEMA,
        )


class _RuleBook:
    version = "test-v1"

    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int:
        del instrument, trade_date
        return 100

    def earliest_sell_date(self, buy_date: date, instrument: InstrumentId) -> date:
        del instrument
        return buy_date + timedelta(days=1)

    def price_limits(self, *args: object) -> None:
        del args

    def fees(self, *args: object) -> FeeBreakdown:
        del args
        return FeeBreakdown(0, 0, 0, 0)


class _KnownStrategy:
    strategy_id = "known"
    version = "1.0.0"

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]:
        del ctx
        return []

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool:
        del ctx, rebalance_date
        return False

    def generate_targets(self, *args: object) -> object:
        del args
        raise AssertionError("unknown strategy must fail before target generation")


class _Progress:
    def update(self, completed: int, total: int, trade_date: date) -> None:
        del completed, total, trade_date


class _NeverCancelled:
    def is_cancelled(self) -> bool:
        return False


def _bar_row(
    instrument: InstrumentId, trade_date: date, close: float, amount: float
) -> dict[str, object]:
    return {
        "instrument_id": instrument.canonical(),
        "trade_date": trade_date,
        "open": close,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "preclose": close,
        "volume": 10_000,
        "amount": amount,
        "available_at": datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC),
        "pit_usable": True,
    }


def _status_row(
    instrument: InstrumentId, trade_date: date, *, risk: bool
) -> dict[str, object]:
    return {
        "instrument_id": instrument.canonical(),
        "trade_date": trade_date,
        "is_listed": True,
        "is_suspended": False,
        "is_risk_warning": risk,
        "board": "MAIN",
        "available_at": datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC),
        "pit_usable": True,
    }


def _market(repo: _Repository | None = None) -> SnapshotBacktestMarketData:
    return SnapshotBacktestMarketData(
        repository=repo or _Repository(),
        snapshot_id=_SNAPSHOT,
        benchmark=_BENCHMARK,
        capabilities=ProviderCapabilities.complete(),
        provider="offline-complete",
    )


def _rules() -> UniverseRules:
    return UniverseRules(
        min_listing_days=0,
        allowed_boards=frozenset({Board.MAIN}),
        exclude_st=False,
        exclude_suspended=True,
        min_avg_amount_20d=None,
    )


def _artifact(
    *,
    factor_ref: str = _FACTOR_REF,
    row_ref: str | None = None,
    snapshot_id: SnapshotId = _SNAPSHOT,
    universe_hash: str = _UNIVERSE_HASH,
    start: date = _SIGNAL,
    end: date = _NEXT,
) -> FactorArtifact:
    factor_id, version = (row_ref or factor_ref).split("@")
    frame = pl.DataFrame(
        [
            {
                "trade_date": _SIGNAL,
                "instrument_id": _STOCK.canonical(),
                "factor_id": factor_id,
                "factor_version": version,
                "value": 1.25,
                "available_at": datetime(2024, 1, 31, 7, tzinfo=UTC),
                "is_valid": True,
            }
        ],
        schema=FACTOR_OUTPUT_SCHEMA,
    )
    table = frame.to_arrow()
    return FactorArtifact(
        factor_ref=factor_ref,
        cache_key=hashlib.sha256((factor_ref + "-cache").encode()).hexdigest(),
        content_hash=factor_table_content_hash(table),
        row_count=1,
        snapshot_id=snapshot_id,
        universe_hash=universe_hash,
        start=start,
        end=end,
        table=table,
    )


def _strategy_data(
    *,
    repo: _Repository | None = None,
    artifacts: Mapping[str, FactorArtifact] | None = None,
    enrichment: _Enrichment | None = None,
    capabilities: ProviderCapabilities | None = None,
) -> SnapshotStrategyData:
    return SnapshotStrategyData(
        repository=repo or _Repository(),
        snapshot_id=_SNAPSHOT,
        factor_artifacts=artifacts or {_FACTOR_REF: _artifact()},
        universe_hash=_UNIVERSE_HASH,
        universe_rules=_rules(),
        enrichment=enrichment or _Enrichment(),
        capabilities=capabilities or ProviderCapabilities.complete(),
        provider="offline-complete",
    )


@pytest.mark.parametrize("method", ["calendar", "market_slice", "corporate_actions"])
def test_market_adapter_rejects_every_snapshot_mismatch(method: str) -> None:
    adapter = _market()
    if method == "calendar":
        call = lambda: adapter.calendar(
            _OTHER_SNAPSHOT.value, _SIGNAL, _NEXT, include_next_session=True
        )
    else:
        call = lambda: getattr(adapter, method)(_OTHER_SNAPSHOT.value, _SIGNAL)

    with pytest.raises(ValueError, match="snapshot"):
        call()


def test_calendar_loads_the_first_actual_later_session_without_weekday_guess() -> None:
    adapter = _market()

    calendar = adapter.calendar(
        _SNAPSHOT.value, _SIGNAL, _SIGNAL, include_next_session=True
    )

    assert calendar.sessions(_SIGNAL, _NEXT) == (_SIGNAL, _NEXT)
    assert calendar.next_session(_SIGNAL) == _NEXT
    assert calendar.end == _NEXT


def test_market_slice_joins_same_day_status_into_exact_sorted_schema() -> None:
    bound = _market().market_slice(_SNAPSHOT.value, _SIGNAL)

    assert bound.snapshot_id == _SNAPSHOT.value
    assert bound.market.bars.schema == pl.Schema(
        {
            "instrument_id": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "volume": pl.Int64,
            "is_suspended": pl.Boolean,
            "security_status": pl.String,
        }
    )
    assert bound.market.bars.select(
        "instrument_id", "is_suspended", "security_status"
    ).rows() == [
        (_BENCHMARK.canonical(), False, "NORMAL"),
        (_STOCK.canonical(), False, "ST"),
    ]


@pytest.mark.parametrize(
    "mutation",
    ["missing_status", "duplicate_status", "wrong_status_date", "missing_benchmark"],
)
def test_market_slice_fails_closed_on_status_or_benchmark_join_breaks(
    mutation: str,
) -> None:
    repo = _Repository()
    if mutation == "missing_status":
        repo.status_frame = repo.status_frame.filter(
            pl.col("instrument_id") != _STOCK.canonical()
        )
    elif mutation == "duplicate_status":
        repo.status_frame = pl.concat(
            [
                repo.status_frame,
                repo.status_frame.filter(pl.col("instrument_id") == _STOCK.canonical()),
            ]
        )
    elif mutation == "wrong_status_date":
        repo.status_frame = repo.status_frame.with_columns(
            pl.lit(_NEXT).cast(pl.Date).alias("trade_date")
        )
    else:
        repo.bar_frame = repo.bar_frame.filter(
            pl.col("instrument_id") != _BENCHMARK.canonical()
        )

    with pytest.raises(ValueError, match="market slice"):
        _market(repo).market_slice(_SNAPSHOT.value, _SIGNAL)


def test_market_slice_fails_closed_on_nonfinite_price() -> None:
    repo = _Repository()
    repo.bar_frame = repo.bar_frame.with_columns(
        pl.when(
            (pl.col("instrument_id") == _STOCK.canonical())
            & (pl.col("trade_date") == _SIGNAL)
        )
        .then(float("nan"))
        .otherwise(pl.col("close"))
        .alias("close")
    )

    with pytest.raises(ValueError, match="finite|OHLC"):
        _market(repo).market_slice(_SNAPSHOT.value, _SIGNAL)


def test_corporate_actions_split_dividend_into_stable_cash_and_bonus_events() -> None:
    adapter = _market()

    bonus = adapter.corporate_actions(_SNAPSHOT.value, _SIGNAL)
    cash = adapter.corporate_actions(_SNAPSHOT.value, _NEXT)

    assert [(item.action_type, item.effective_date) for item in bonus] == [
        (CorporateActionType.BONUS_SHARES, _SIGNAL)
    ]
    assert [(item.action_type, item.effective_date) for item in cash] == [
        (CorporateActionType.CASH_DIVIDEND, _NEXT)
    ]
    assert bonus[0].share_ratio == Decimal("0.2")
    assert cash[0].cash_per_share_yuan == Decimal("0.1")
    assert cash == adapter.corporate_actions(_SNAPSHOT.value, _NEXT)


@pytest.mark.parametrize("mutation", ["unknown_type", "rights", "incomplete"])
def test_corporate_actions_reject_unknown_rights_and_incomplete_rows(
    mutation: str,
) -> None:
    repo = _Repository()
    if mutation == "unknown_type":
        repo.action_frame = repo.action_frame.with_columns(
            pl.lit("MERGER").alias("action_type")
        )
    elif mutation == "rights":
        repo.action_frame = repo.action_frame.with_columns(
            pl.lit(3.5).alias("rights_price")
        )
    else:
        repo.action_frame = repo.action_frame.with_columns(
            pl.lit(None).cast(pl.Date).alias("record_date")
        )

    with pytest.raises(ValueError, match="corporate action"):
        _market(repo).corporate_actions(_SNAPSHOT.value, _SIGNAL)


def test_market_preflight_rejects_baostock_before_repository_reads() -> None:
    repo = _Repository()
    adapter = SnapshotBacktestMarketData(
        repository=repo,
        snapshot_id=_SNAPSHOT,
        benchmark=_BENCHMARK,
        capabilities=BAOSTOCK_CAPABILITIES,
        provider="baostock",
    )

    with pytest.raises(ExperimentCapabilityUnavailable) as caught:
        adapter.preflight()

    assert caught.value.missing == ("corporate_actions",)
    assert caught.value.detail.retryable is False
    assert repo.calls == []


def test_factor_values_verify_artifacts_and_emit_exact_strategy_schema() -> None:
    frame = _strategy_data().factor_values(
        _SNAPSHOT.value,
        _SIGNAL,
        (_STOCK,),
        (_FACTOR_REF,),
    )

    assert frame.schema == pl.Schema(
        {
            "trade_date": pl.Date,
            "instrument_id": pl.String,
            "factor_ref": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
            "is_valid": pl.Boolean,
        }
    )
    assert frame.rows() == [
        (
            _SIGNAL,
            _STOCK.canonical(),
            _FACTOR_REF,
            1.25,
            datetime(2024, 1, 31, 7, tzinfo=UTC),
            True,
        )
    ]


def test_factor_values_reject_call_and_artifact_snapshot_mismatch() -> None:
    data = _strategy_data()
    with pytest.raises(ValueError, match="snapshot"):
        data.factor_values(_OTHER_SNAPSHOT.value, _SIGNAL, None, (_FACTOR_REF,))

    mismatched = _strategy_data(
        artifacts={_FACTOR_REF: _artifact(snapshot_id=_OTHER_SNAPSHOT)}
    )
    with pytest.raises(ValueError, match="artifact snapshot"):
        mismatched.factor_values(_SNAPSHOT.value, _SIGNAL, None, (_FACTOR_REF,))


@pytest.mark.parametrize("mutation", ["identity", "universe", "range", "missing"])
def test_factor_values_fail_closed_on_artifact_identity_and_scope(
    mutation: str,
) -> None:
    artifact = _artifact()
    artifacts: Mapping[str, FactorArtifact] = {_FACTOR_REF: artifact}
    requested = (_FACTOR_REF,)
    if mutation == "identity":
        artifacts = {_FACTOR_REF: _artifact(row_ref="other_v1@1.0.0")}
    elif mutation == "universe":
        artifacts = {
            _FACTOR_REF: _artifact(
                universe_hash=hashlib.sha256(b"other-universe").hexdigest()
            )
        }
    elif mutation == "range":
        artifacts = {_FACTOR_REF: _artifact(start=_SIGNAL, end=_SIGNAL)}
        requested_date = _NEXT
        with pytest.raises(ValueError, match="date range"):
            _strategy_data(artifacts=artifacts).factor_values(
                _SNAPSHOT.value, requested_date, None, requested
            )
        return
    else:
        requested = ("missing_v1@1.0.0",)

    with pytest.raises(ValueError, match="factor artifact"):
        _strategy_data(artifacts=artifacts).factor_values(
            _SNAPSHOT.value, _SIGNAL, None, requested
        )


def test_stock_universe_joins_explicit_pit_enrichment_and_market_evidence() -> None:
    enrichment = _Enrichment()
    frame = _strategy_data(enrichment=enrichment).stock_universe(
        _SNAPSHOT.value, _SIGNAL
    )

    assert frame.schema == STRATEGY_UNIVERSE_SCHEMA
    stock = frame.filter(pl.col("instrument_id") == _STOCK.canonical()).row(
        0, named=True
    )
    assert stock == {
        "instrument_id": _STOCK.canonical(),
        "as_of": _SIGNAL,
        "eligible": True,
        "reason_codes": [],
        "industry": "BANK",
        "adv_amount": 2_000.0,
        "log_market_cap": math.log(10_000_000.0),
    }
    assert enrichment.calls == [(_SNAPSHOT, _SIGNAL, (_BENCHMARK, _STOCK))]


@pytest.mark.parametrize("mutation", ["wrong_date", "duplicate", "missing"])
def test_stock_universe_rejects_malformed_enrichment_scope(mutation: str) -> None:
    frame = _Enrichment().values(_SNAPSHOT, _SIGNAL, (_BENCHMARK, _STOCK))
    if mutation == "wrong_date":
        frame = frame.with_columns(pl.lit(_NEXT).cast(pl.Date).alias("as_of"))
    elif mutation == "duplicate":
        frame = pl.concat([frame, frame.slice(0, 1)])
    else:
        frame = frame.filter(pl.col("instrument_id") != _STOCK.canonical())

    with pytest.raises(ValueError, match="enrichment"):
        _strategy_data(enrichment=_Enrichment(frame)).stock_universe(
            _SNAPSHOT.value, _SIGNAL
        )


def test_stock_universe_rejects_baostock_pit_gaps_before_repository_reads() -> None:
    repo = _Repository()
    data = _strategy_data(repo=repo, capabilities=BAOSTOCK_CAPABILITIES)

    with pytest.raises(ExperimentCapabilityUnavailable) as caught:
        data.preflight(require_stock_universe=True)

    assert caught.value.missing == (
        "pit_total_shares",
        "pit_industry_classification",
    )
    assert repo.calls == []


def test_context_provider_requires_the_actual_adjacent_trading_session() -> None:
    data = _strategy_data()
    provider = SnapshotStrategyContextProvider(
        repository=_Repository(),
        snapshot_id=_SNAPSHOT,
        data=data,
        portfolio_constructor=PortfolioConstructor(),
    )

    context = provider(_SNAPSHOT.value, _SIGNAL, _NEXT)

    assert context.sessions == (_SIGNAL, _NEXT)
    with pytest.raises(ValueError, match="next actual session"):
        provider(_SNAPSHOT.value, _SIGNAL, _AFTER_NEXT)


def test_snapshot_strategy_runner_rejects_unknown_version_before_artifact_write(
    tmp_path: Path,
) -> None:
    known = _KnownStrategy()
    root = tmp_path / "artifacts"
    runner = SnapshotStrategyRunner(
        repository=_Repository(),
        snapshot_id=_SNAPSHOT,
        capabilities=ProviderCapabilities.complete(),
        provider="offline-complete",
        benchmark=_BENCHMARK,
        factor_artifacts={_FACTOR_REF: _artifact()},
        universe_hash=_UNIVERSE_HASH,
        universe_rules=_rules(),
        enrichment=_Enrichment(),
        strategies={StrategyRef("known", "1.0.0"): known},
        stock_strategy_refs=frozenset(),
        rulebook=_RuleBook(),
        portfolio_constructor=PortfolioConstructor(),
        rebalance_planner=RebalancePlanner(),
        artifact_root=root,
    )
    request = BacktestRequest(
        _EXPERIMENT,
        _SNAPSHOT.value,
        StrategyRef("known", "2.0.0"),
        _SIGNAL,
        _NEXT,
        _BENCHMARK,
        100_000,
        "test-v1",
        ExecutionConfig(ExecutionPrice.CLOSE, 0.0, 1.0),
    )

    with pytest.raises(ValueError, match="unknown strategy or version"):
        runner.run(request, _Progress(), _NeverCancelled())

    assert not root.exists()
