"""记录完整规模因子研究股票池批量构建的性能证据。"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from quant_research.bootstrap.worker import _FactorStudySession
from quant_research.domain.identifiers import InstrumentId
from quant_research.factor_studies.models import FactorStudyStage
from quant_research.factor_studies.progress import FactorStudyProgressReporter
from quant_research.tasks.models import TaskProgress
from tests.performance._process_memory import process_peak_rss_bytes

pytestmark = pytest.mark.performance

_SESSION_COUNT = 1_215
_INSTRUMENT_COUNT = 5_894
_MAX_BUILD_SECONDS = 8 * 60
_MAX_PEAK_RSS_BYTES = 1_536 * 1024 * 1024


class _EmptyStatusRepository:
    """提供空状态表并统计范围读取次数。"""

    def __init__(self) -> None:
        self.suspension_calls = 0
        self.warning_calls = 0
        self.metadata_calls = 0
        self.calendar_calls = 0
        self._empty = pl.DataFrame(
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "available_at": pl.Datetime("us", "UTC"),
                "pit_usable": pl.Boolean,
            }
        )

    def stocks(self) -> pl.LazyFrame:
        """返回全部已满足上市时长的主板股票。"""
        self.metadata_calls += 1
        return pl.DataFrame(
            {
                "instrument_id": [
                    f"{index + 1:06d}.SZ" for index in range(_INSTRUMENT_COUNT)
                ],
                "list_date": [date(2010, 1, 1)] * _INSTRUMENT_COUNT,
                "delist_date": pl.Series([None] * _INSTRUMENT_COUNT, dtype=pl.Date),
                "board": ["MAIN"] * _INSTRUMENT_COUNT,
            }
        ).lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回覆盖全局上市序号的工作日历。"""
        self.calendar_calls += 1
        values: list[date] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                values.append(current)
            current += timedelta(days=1)
        return pl.DataFrame(
            {"trade_date": values, "is_trading_day": [True] * len(values)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def stock_suspensions(self, *_: object) -> pl.LazyFrame:
        """返回空停牌状态。"""
        self.suspension_calls += 1
        return self._empty.lazy()

    def stock_risk_warnings(self, *_: object) -> pl.LazyFrame:
        """返回空风险警示状态。"""
        self.warning_calls += 1
        return self._empty.lazy()


class _ProgressSink:
    """收集性能运行中的进度事件。"""

    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        """保存一次进度。"""
        self.values.append(progress)


class _NeverCancelled:
    """提供始终继续的取消端口。"""

    def is_cancelled(self) -> bool:
        """返回未取消。"""
        return False


def _sessions() -> tuple[date, ...]:
    values: list[date] = []
    current = date(2018, 1, 1)
    while len(values) < _SESSION_COUNT:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return tuple(values)


def test_full_scale_factor_study_universe_meets_resource_budget() -> None:
    """完整五年股票池必须以两次状态读取满足耗时和内存预算。"""
    repository = _EmptyStatusRepository()
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, repository),
        cast(Any, object()),
        cast(Any, object()),
        Path("."),
    )
    sink = _ProgressSink()
    progress = FactorStudyProgressReporter(sink)
    progress.stage_started(FactorStudyStage.ANALYZE_FACTORS)
    progress.substage_started("BUILD_UNIVERSE", "开始")
    instruments = tuple(
        InstrumentId.parse(f"{index + 1:06d}.SZ")
        for index in range(_INSTRUMENT_COUNT)
    )

    started = time.perf_counter()
    eligible, universe_ids, universe_hash = session._build_factor_study_universe(
        instruments,
        _sessions(),
        progress,
        _NeverCancelled(),
    )
    elapsed = time.perf_counter() - started
    peak_rss = process_peak_rss_bytes()
    progress_events = [
        value
        for value in sink.values
        if value.context.get("substage_state") == "PROGRESS"
    ]
    evidence = {
        "batch_count": sink.values[-1].context["batch_count"],
        "elapsed_seconds": elapsed,
        "estimated_frame_bytes": eligible.estimated_size(),
        "instruments": _INSTRUMENT_COUNT,
        "peak_rss_bytes": peak_rss,
        "progress_events": len(progress_events),
        "rows": eligible.height,
        "sessions": _SESSION_COUNT,
        "status_queries": repository.suspension_calls + repository.warning_calls,
    }
    print(f"factor_study_universe_performance={json.dumps(evidence, sort_keys=True)}")

    assert eligible.height == _SESSION_COUNT * _INSTRUMENT_COUNT
    assert len(universe_ids) == _INSTRUMENT_COUNT
    assert len(universe_hash) == 64
    assert repository.suspension_calls == 1
    assert repository.warning_calls == 1
    assert repository.metadata_calls == 1
    assert repository.calendar_calls == 1
    assert len(progress_events) <= 21
    assert elapsed <= _MAX_BUILD_SECONDS
    assert peak_rss <= _MAX_PEAK_RSS_BYTES
