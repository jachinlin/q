"""验证因子研究股票池的批量 PIT 构建和稳定身份。"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from quant_research.bootstrap.worker import (
    _CanonicalUniverseHasher,
    _FactorStudySession,
)
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.identifiers import InstrumentId
from quant_research.factor_studies.models import FactorStudyStage
from quant_research.factor_studies.progress import FactorStudyProgressReporter
from quant_research.tasks.models import TaskProgress
from quant_research.universe import (
    UniverseBatchEvaluator,
    UniverseBuilder,
    UniverseRules,
)


class _ProgressSink:
    """收集测试中的任务进度。"""

    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        """保存一次不可变进度。"""
        self.values.append(progress)


class _Cancellation:
    """在指定检查次数后返回取消。"""

    def __init__(self, cancel_at: int | None = None) -> None:
        self._cancel_at = cancel_at
        self.calls = 0

    def is_cancelled(self) -> bool:
        """返回当前检查是否达到取消边界。"""
        self.calls += 1
        return self._cancel_at is not None and self.calls >= self._cancel_at


class _StatusRepository:
    """返回固定状态帧并记录整段读取参数。"""

    def __init__(
        self,
        suspensions: pl.DataFrame,
        warnings: pl.DataFrame,
        stocks: pl.DataFrame | None = None,
    ) -> None:
        self._suspensions = suspensions
        self._warnings = warnings
        self._stocks = (
            pl.DataFrame(
                {
                    "instrument_id": ["000001.SZ", "600000.SH"],
                    "list_date": [date(2025, 1, 1), date(2025, 1, 1)],
                    "delist_date": pl.Series([None, None], dtype=pl.Date),
                    "board": ["MAIN", "MAIN"],
                }
            )
            if stocks is None
            else stocks
        )
        self.suspension_calls: list[tuple[date, date, object]] = []
        self.warning_calls: list[tuple[date, date, object]] = []

    def stocks(self) -> pl.LazyFrame:
        """返回固定证券元数据。"""
        return self._stocks.lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回请求范围内的工作日交易日历。"""
        days: list[date] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                days.append(current)
            current += timedelta(days=1)
        return pl.DataFrame(
            {"trade_date": days, "is_trading_day": [True] * len(days)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def stock_suspensions(
        self,
        start: date,
        end: date,
        instruments: object,
    ) -> pl.LazyFrame:
        """返回完整停牌状态范围。"""
        self.suspension_calls.append((start, end, instruments))
        return self._suspensions.lazy()

    def stock_risk_warnings(
        self,
        start: date,
        end: date,
        instruments: object,
    ) -> pl.LazyFrame:
        """返回完整风险警示状态范围。"""
        self.warning_calls.append((start, end, instruments))
        return self._warnings.lazy()


class _ParityRepository:
    """同时支持逐日构建器与批量判定器的同源小样本仓储。"""

    def __init__(
        self,
        stocks: pl.DataFrame,
        suspensions: pl.DataFrame,
        warnings: pl.DataFrame,
    ) -> None:
        self._stocks = stocks
        self._suspensions = suspensions
        self._warnings = warnings
        valid = [
            value
            for value in stocks["list_date"].drop_nulls().to_list()
            if isinstance(value, date)
        ]
        self._calendar_origin = min(valid) if valid else date(2024, 1, 1)

    def stocks(self) -> pl.LazyFrame:
        """返回固定证券元数据。"""
        return self._stocks.lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回自最早证券历史起的工作日历。"""
        values: list[date] = []
        current = max(start, self._calendar_origin)
        while current <= end:
            if current.weekday() < 5:
                values.append(current)
            current += timedelta(days=1)
        return pl.DataFrame(
            {"trade_date": values, "is_trading_day": [True] * len(values)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def stock_suspensions(
        self, start: date, end: date, _: object
    ) -> pl.LazyFrame:
        """返回请求日期范围内的停牌状态。"""
        return self._suspensions.filter(
            pl.col("trade_date").is_between(start, end)
        ).lazy()

    def stock_risk_warnings(
        self, start: date, end: date, _: object
    ) -> pl.LazyFrame:
        """返回请求日期范围内的风险警示状态。"""
        return self._warnings.filter(
            pl.col("trade_date").is_between(start, end)
        ).lazy()

    def stock_bars(self, _: object, __: date, ___: date) -> pl.LazyFrame:
        """返回不启用流动性门槛时允许为空的固定 Schema。"""
        return pl.DataFrame(
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "amount": pl.Float64,
                "available_at": pl.Datetime("us", "UTC"),
                "pit_usable": pl.Boolean,
            }
        ).lazy()


def _events(
    rows: list[tuple[str, date, datetime, bool]],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "instrument_id": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "available_at": [row[2] for row in rows],
            "pit_usable": [row[3] for row in rows],
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "available_at": pl.Datetime("us", "UTC"),
            "pit_usable": pl.Boolean,
        },
    )


def _reporter() -> tuple[FactorStudyProgressReporter, _ProgressSink]:
    sink = _ProgressSink()
    reporter = FactorStudyProgressReporter(sink)
    reporter.stage_started(FactorStudyStage.ANALYZE_FACTORS)
    reporter.substage_started("BUILD_UNIVERSE", "开始")
    return reporter, sink


def _session(repository: _StatusRepository) -> _FactorStudySession:
    return _FactorStudySession(
        cast(Any, object()),
        cast(Any, repository),
        cast(Any, object()),
        cast(Any, object()),
        Path("."),
    )


def test_batch_universe_preserves_daily_pit_reasons_and_identity() -> None:
    """整段读取必须按每日截止过滤并保持逐日股票池和哈希。"""
    days = (date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7))
    first, second = InstrumentId.parse("000001.SZ"), InstrumentId.parse(
        "600000.SH"
    )
    repository = _StatusRepository(
        _events(
            [
                (first.canonical(), days[0], datetime(2026, 1, 5, 7, tzinfo=UTC), True),
                (first.canonical(), days[0], datetime(2026, 1, 5, 7, tzinfo=UTC), True),
                (second.canonical(), days[1], datetime(2026, 1, 6, 8, tzinfo=UTC), True),
                (second.canonical(), days[2], datetime(2026, 1, 7, 8, tzinfo=UTC), False),
            ]
        ),
        _events(
            [
                (first.canonical(), days[0], datetime(2026, 1, 5, 6, tzinfo=UTC), True),
                (first.canonical(), days[1], datetime(2026, 1, 7, 0, tzinfo=UTC), True),
            ]
        ),
    )
    reporter, sink = _reporter()

    eligible, universe_ids, universe_hash = _session(
        repository
    )._build_factor_study_universe(
        (second, first), days, reporter, _Cancellation()
    )

    assert repository.suspension_calls == [(days[0], days[-1], (second, first))]
    assert repository.warning_calls == [(days[0], days[-1], (second, first))]
    assert eligible.rows() == [
        (days[0], first.canonical(), False),
        (days[0], second.canonical(), True),
        (days[1], first.canonical(), True),
        (days[1], second.canonical(), False),
        (days[2], first.canonical(), True),
        (days[2], second.canonical(), True),
    ]
    assert universe_ids == (first, second)
    membership = [
        {"signal_date": value.isoformat(), "instrument_id": instrument}
        for value, instrument, is_eligible in eligible.iter_rows()
        if is_eligible
    ]
    assert universe_hash == hashlib.sha256(
        canonical_json_bytes(cast(list[JsonValue], membership))
    ).hexdigest()
    completed = sink.values[-1]
    assert completed.context["batch_count"] == 3
    assert completed.context["suspension_row_count"] == 2
    assert completed.context["risk_warning_row_count"] == 1
    assert completed.context["eligible_row_count"] == 4


def test_cn_stock_standard_applies_metadata_board_and_listing_boundaries() -> None:
    """标准股票池必须完整执行元数据、板块和 120 个上市交易日门槛。"""
    listing = date(2025, 1, 4)
    calendar_repository = _StatusRepository(_events([]), _events([]))
    trading_days = calendar_repository.trade_calendar(
        listing, date(2025, 12, 31)
    ).collect()["trade_date"].to_list()
    sessions = (trading_days[118], trading_days[119])
    stocks = pl.DataFrame(
        {
            "instrument_id": [
                "000001.SZ",
                "000002.SZ",
                "000003.SZ",
                "000004.SZ",
                "000005.SZ",
                "920001.BJ",
            ],
            "list_date": [
                date(2024, 1, 1),
                listing,
                sessions[-1] + timedelta(days=1),
                date(2024, 1, 1),
                None,
                date(2024, 1, 1),
            ],
            "delist_date": pl.Series(
                [None, None, None, sessions[0], None, None], dtype=pl.Date
            ),
            "board": ["MAIN", "MAIN", "MAIN", "MAIN", "MAIN", "BSE"],
        }
    )
    repository = _StatusRepository(_events([]), _events([]), stocks)
    identifiers = tuple(
        InstrumentId.parse(value)
        for value in [*stocks["instrument_id"].to_list(), "000006.SZ"]
    )
    reporter, _ = _reporter()
    study_session = _session(repository)
    metadata, study_sessions = study_session._stock_metadata_and_sessions(
        identifiers, sessions[0], sessions[-1]
    )

    eligible, universe_ids, _ = study_session._build_factor_study_universe(
        identifiers, sessions, reporter, _Cancellation()
    )

    first_day = {
        row["instrument_id"]: (row["eligible"], row["reason_codes"])
        for row in _FactorStudySession._universe_batch(
            metadata,
            study_sessions.head(1),
            pl.DataFrame(schema={"signal_date": pl.Date, "instrument_id": pl.String}),
            pl.DataFrame(schema={"signal_date": pl.Date, "instrument_id": pl.String}),
        ).to_dicts()
    }
    assert first_day == {
        "000001.SZ": (True, []),
        "000002.SZ": (False, ["INSUFFICIENT_LISTING_DAYS"]),
        "000003.SZ": (False, ["NOT_LISTED_YET"]),
        "000004.SZ": (False, ["DELISTED"]),
        "000005.SZ": (False, ["INSTRUMENT_HISTORY_MISSING"]),
        "000006.SZ": (
            False,
            ["INSTRUMENT_HISTORY_MISSING", "BOARD_NOT_ALLOWED"],
        ),
        "920001.BJ": (False, ["BOARD_NOT_ALLOWED"]),
    }
    boundary = eligible.filter(pl.col("instrument_id") == "000002.SZ")
    assert boundary["eligible"].to_list() == [False, True]
    assert tuple(item.canonical() for item in universe_ids) == (
        "000001.SZ",
        "000002.SZ",
    )


def test_batch_evaluator_matches_daily_universe_builder_reason_priority() -> None:
    """批量规则必须与逐日权威构建器的资格和完整原因顺序逐字一致。"""
    as_of = date(2026, 1, 5)
    identifiers = [
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
        "000004.SZ",
        "000005.SZ",
        "000006.SZ",
        "000007.SZ",
        "920001.BJ",
    ]
    stocks = pl.DataFrame(
        {
            "instrument_id": identifiers,
            "list_date": [
                date(2024, 1, 1),
                date(2025, 12, 1),
                date(2026, 1, 6),
                date(2024, 1, 1),
                None,
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 1, 1),
            ],
            "delist_date": pl.Series(
                [None, None, None, as_of, None, None, None, None],
                dtype=pl.Date,
            ),
            "board": ["MAIN"] * 7 + ["BSE"],
        }
    )
    suspensions = _events(
        [("000007.SZ", as_of, datetime(2026, 1, 5, 7, tzinfo=UTC), True)]
    )
    warnings = _events(
        [("000006.SZ", as_of, datetime(2026, 1, 5, 7, tzinfo=UTC), True)]
    )
    repository = _ParityRepository(stocks, suspensions, warnings)
    rules = UniverseRules()
    expected = UniverseBuilder(cast(Any, repository)).build(as_of, rules)
    study_session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, repository),
        cast(Any, object()),
        cast(Any, object()),
        Path("."),
    )
    instrument_ids = tuple(InstrumentId.parse(value) for value in identifiers)
    metadata, sessions = study_session._stock_metadata_and_sessions(
        instrument_ids, as_of, as_of
    )
    cutoffs = pl.DataFrame(
        {
            "trade_date": [as_of],
            "_pit_cutoff": [datetime(2026, 1, 5, 15, 59, tzinfo=UTC)],
        },
        schema={
            "trade_date": pl.Date,
            "_pit_cutoff": pl.Datetime("us", "UTC"),
        },
    )
    actual = UniverseBatchEvaluator(rules).evaluate(
        metadata,
        sessions,
        study_session._pit_status_keys(suspensions, cutoffs),
        study_session._pit_status_keys(warnings, cutoffs),
    ).rename({"signal_date": "as_of"}).select(
        "instrument_id", "as_of", "eligible", "reason_codes"
    )

    assert actual.rows() == expected.rows()


@pytest.mark.parametrize("item_total", [1, 2, 19, 20, 21, 1_215])
def test_batch_ends_match_exhaustive_five_percent_sampling(item_total: int) -> None:
    """向量化批次终点必须与逐项调用报告器时的采样点一致。"""
    reporter, sink = _reporter()
    for completed in range(1, item_total + 1):
        reporter.substage_progress(
            "BUILD_UNIVERSE",
            "处理中",
            item_completed=completed,
            item_total=item_total,
        )
    sampled = tuple(
        cast(int, value.context["item_completed"])
        for value in sink.values
        if value.context.get("substage_state") == "PROGRESS"
    )

    assert _FactorStudySession._universe_batch_ends(item_total) == sampled
    assert len(sampled) <= 21


def test_batch_universe_cancels_between_vectorized_chunks() -> None:
    """取消必须在首批完成后阻止后续批次和完成事件。"""
    sessions: tuple[date, ...] = tuple(
        day
        for offset in range(31)
        if (day := date(2026, 1, 1) + timedelta(days=offset)).weekday() < 5
    )[:21]
    repository = _StatusRepository(_events([]), _events([]))
    reporter, sink = _reporter()

    with pytest.raises(RuntimeError, match="factor study cancelled"):
        _session(repository)._build_factor_study_universe(
            (InstrumentId.parse("000001.SZ"),),
            sessions,
            reporter,
            _Cancellation(cancel_at=4),
        )

    sampled = [
        value
        for value in sink.values
        if value.context.get("substage_state") == "PROGRESS"
    ]
    assert [value.context["item_completed"] for value in sampled] == [1]
    assert not any(
        value.context.get("substage_state") == "COMPLETED"
        for value in sink.values
    )
    assert len(repository.suspension_calls) == 1
    assert len(repository.warning_calls) == 1


def test_streaming_universe_hash_matches_one_canonical_list() -> None:
    """分批哈希必须与旧实现的一次性 canonical JSON 完全相同。"""
    first = pl.DataFrame(
        {
            "signal_date": [date(2026, 1, 5), date(2026, 1, 5)],
            "instrument_id": ["000001.SZ", "600000.SH"],
            "eligible": [True, False],
            "reason_codes": [[], ["SUSPENDED"]],
        }
    )
    second = pl.DataFrame(
        {
            "signal_date": [date(2026, 1, 6), date(2026, 1, 6)],
            "instrument_id": ["000001.SZ", "600000.SH"],
            "eligible": [True, True],
            "reason_codes": [[], []],
        }
    )
    hasher = _CanonicalUniverseHasher()
    hasher.update(first)
    hasher.update(second)
    expected = [
        {"signal_date": "2026-01-05", "instrument_id": "000001.SZ"},
        {"signal_date": "2026-01-06", "instrument_id": "000001.SZ"},
        {"signal_date": "2026-01-06", "instrument_id": "600000.SH"},
    ]

    assert hasher.finish() == hashlib.sha256(
        canonical_json_bytes(cast(list[JsonValue], expected))
    ).hexdigest()
    assert hasher.row_count == 3
    assert tuple(item.canonical() for item in hasher.instrument_ids) == (
        "000001.SZ",
        "600000.SH",
    )
    with pytest.raises(ValueError, match="already finalized"):
        hasher.finish()
    with pytest.raises(ValueError, match="already finalized"):
        hasher.update(first)
