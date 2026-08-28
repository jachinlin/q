"""验证因子研究股票池的批量 PIT 构建和稳定身份。"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
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
    ) -> None:
        self._suspensions = suspensions
        self._warnings = warnings
        self.suspension_calls: list[tuple[date, date, object]] = []
        self.warning_calls: list[tuple[date, date, object]] = []

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
        (days[0], first.canonical(), False, ["RISK_WARNING", "SUSPENDED"]),
        (days[0], second.canonical(), True, []),
        (days[1], first.canonical(), True, []),
        (days[1], second.canonical(), False, ["SUSPENDED"]),
        (days[2], first.canonical(), True, []),
        (days[2], second.canonical(), True, []),
    ]
    assert universe_ids == (first, second)
    membership = [
        {"signal_date": value.isoformat(), "instrument_id": instrument}
        for value, instrument, is_eligible, _ in eligible.iter_rows()
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
    sessions = tuple(date(2026, 1, day) for day in range(1, 22))
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
