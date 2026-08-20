"""使用内存 Canonical 端口验证三策略和三种研究深度。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
import pytest
import yaml

from quant_research.application.canonical_research_runtime import (
    CanonicalResearchRuntime,
)
from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.data.repository import ResearchDataRepository
from quant_research.experiments.research import (
    FamilyExecutionRecord,
    ResearchFamilyRecord,
    ResearchMark,
    ResearchPhase,
    ResearchRunRecord,
    ResearchStage,
    ResearchStatus,
    ResearchVariantRecord,
)
from quant_research.experiments.research_artifacts import ResearchArtifactPublisher
from quant_research.research_protocols import ResearchConfigResolver, ResearchMode
from quant_research.tasks.models import TaskProgress

_HASH = "a" * 64
_NOW = datetime(2026, 8, 18, tzinfo=UTC)


class _CatalogState:
    catalog_hash = _HASH


class _Catalog:
    def require_validated_catalog(self) -> _CatalogState:
        return _CatalogState()


class _Progress:
    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        self.values.append(progress)


class _Cancellation:
    def is_cancelled(self) -> bool:
        return False


class _Repository:
    def __init__(self, strategy_id: str) -> None:
        self._catalog = _Catalog()
        if strategy_id == "stock_multifactor":
            self.ids = tuple(f"{600000 + index:06d}.SH" for index in range(25))
            instrument_type = "STOCK"
        elif strategy_id == "dual_ma_trend":
            self.ids = ("510300.SH",)
            instrument_type = "ETF"
        else:
            self.ids = ("510050.SH", "510300.SH", "513100.SH", "588000.SH")
            instrument_type = "ETF"
        self._instrument_type = instrument_type
        self._dates = self._business_dates(date(2021, 8, 1), date(2024, 6, 30))
        self._bars = self._build_bars()

    def catalog(self) -> _Catalog:
        return self._catalog

    def instruments(self) -> pl.LazyFrame:
        return pl.DataFrame(
            {
                "instrument_id": self.ids,
                "instrument_type": [self._instrument_type] * len(self.ids),
                "board": ["MAIN"] * len(self.ids),
                "list_date": [date(2020, 1, 2)] * len(self.ids),
                "delist_date": [None] * len(self.ids),
            }
        ).lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        dates = tuple(day for day in self._dates if start <= day <= end)
        return pl.DataFrame(
            {"trade_date": dates, "is_trading_day": [True] * len(dates)}
        ).lazy()

    def security_status(
        self, as_of: date, instruments: tuple[object, ...] | None = None
    ) -> pl.LazyFrame:
        identifiers = (
            list(self.ids)
            if instruments is None
            else [item.canonical() for item in instruments]
        )
        return pl.DataFrame(
            {
                "trade_date": [as_of] * len(identifiers),
                "instrument_id": identifiers,
                "is_listed": [True] * len(identifiers),
                "is_suspended": [False] * len(identifiers),
                "is_st": [False] * len(identifiers),
                "board": ["MAIN"] * len(identifiers),
                "pit_usable": [True] * len(identifiers),
                "available_at": [
                    datetime.combine(as_of, datetime.min.time(), tzinfo=UTC)
                ]
                * len(identifiers),
            }
        ).lazy()

    def adjusted_bars(
        self, instruments: tuple[object, ...], start: date, end: date
    ) -> pl.LazyFrame:
        identifiers = [item.canonical() for item in instruments]
        return self._bars.filter(
            pl.col("instrument_id").is_in(identifiers)
            & pl.col("trade_date").is_between(start, end)
        ).lazy()

    def bars(
        self, instruments: tuple[object, ...], start: date, end: date
    ) -> pl.LazyFrame:
        identifiers = [item.canonical() for item in instruments]
        return self._bars.filter(
            pl.col("instrument_id").is_in(identifiers)
            & pl.col("trade_date").is_between(start, end)
        ).lazy()

    def daily_basics(
        self, instruments: tuple[object, ...], start: date, end: date
    ) -> pl.LazyFrame:
        identifiers = [item.canonical() for item in instruments]
        rows = [
            {
                "trade_date": day,
                "instrument_id": identifier,
                "pe_ttm": 8.0 + index,
                "pb_mrq": 0.8 + index * 0.05,
                "available_at": datetime.combine(
                    day + timedelta(days=1), datetime.min.time(), tzinfo=UTC
                ),
            }
            for day in self._dates
            if start <= day <= end
            for index, identifier in enumerate(identifiers)
        ]
        return pl.from_dicts(rows).lazy()

    def security_status_range(
        self, start: date, end: date, instruments: tuple[object, ...]
    ) -> pl.LazyFrame:
        identifiers = [item.canonical() for item in instruments]
        rows = [
            {
                "trade_date": day,
                "instrument_id": identifier,
                "is_suspended": False,
                "is_st": False,
            }
            for day in self._dates
            if start <= day <= end
            for identifier in identifiers
        ]
        return pl.from_dicts(rows).lazy()

    def _build_bars(self) -> pl.DataFrame:
        rows: list[dict[str, object]] = []
        for index, identifier in enumerate(self.ids):
            previous = 10.0 + index
            for ordinal, day in enumerate(self._dates):
                trend = 0.00025 * (index + 1) + 0.001 * ((ordinal % 17) - 8) / 8
                close = previous * (1.0 + trend)
                opening = previous * (1.0 + trend * 0.35)
                rows.append(
                    {
                        "trade_date": day,
                        "instrument_id": identifier,
                        "open": opening,
                        "high": max(opening, close) * 1.01,
                        "low": min(opening, close) * 0.99,
                        "close": close,
                        "preclose": previous,
                        "volume": 5_000_000 + index * 50_000,
                        "amount": (5_000_000 + index * 50_000) * close,
                        "available_at": datetime.combine(
                            day, datetime.min.time(), tzinfo=UTC
                        ),
                        "pit_usable": True,
                    }
                )
                previous = close
        return pl.from_dicts(rows).sort("instrument_id", "trade_date")

    @staticmethod
    def _business_dates(start: date, end: date) -> tuple[date, ...]:
        current = start
        values: list[date] = []
        while current <= end:
            if current.weekday() < 5:
                values.append(current)
            current += timedelta(days=1)
        return tuple(values)


@pytest.mark.parametrize(
    "strategy_id",
    ("stock_multifactor", "dual_ma_trend", "etf_rotation"),
)
@pytest.mark.parametrize("mode", tuple(ResearchMode))
def test_runtime_executes_each_strategy_in_each_research_mode(
    tmp_path: Path,
    strategy_id: str,
    mode: ResearchMode,
) -> None:
    source_root = Path(__file__).parents[2]
    raw = yaml.safe_load(
        (source_root / "configs" / "research" / "examples" / f"{strategy_id}.yaml").read_text(
            encoding="utf-8"
        )
    )
    raw["research_mode"] = mode.value
    raw["research_protocol"]["train"] = {"start": "2023-01-02", "end": "2023-06-30"}
    raw["research_protocol"]["validation"] = {"start": "2023-07-03", "end": "2023-12-29"}
    raw["research_protocol"]["test"] = {"start": "2024-01-02", "end": "2024-06-28"}
    resolved = ResearchConfigResolver().resolve_normalized(raw)
    variant_config = resolved.variants[0].config
    family = ResearchFamilyRecord(
        id="family",
        name=resolved.config.name,
        hypothesis=resolved.config.hypothesis,
        strategy_id=strategy_id,
        research_mode=mode,
        config=resolved.normalized,
        config_hash=resolved.config_hash,
        mark=ResearchMark.UNREVIEWED,
        note=None,
        created_at=_NOW,
        archived_at=None,
    )
    execution = FamilyExecutionRecord(
        id="execution",
        family_id=family.id,
        catalog_hash=_HASH,
        source_hash=_HASH,
        lockfile_hash=_HASH,
        rulebook_hash=_HASH,
        environment_hash=_HASH,
        status=ResearchStatus.RUNNING,
        selected_variant_id=None,
        selection_reason=None,
        created_at=_NOW,
        started_at=_NOW,
        completed_at=None,
        error=None,
    )
    variant = ResearchVariantRecord(
        id="variant",
        execution_id=execution.id,
        ordinal=0,
        composition_hash=resolved.variants[0].composition_hash,
        parameters={},
        config=variant_config,
        rejection_reasons=(),
        created_at=_NOW,
    )
    run = ResearchRunRecord(
        id="run",
        execution_id=execution.id,
        variant_id=variant.id,
        phase=ResearchPhase.TEST,
        status=ResearchStatus.RUNNING,
        stage=ResearchStage.VALIDATE,
        stage_status={},
        manifest_path=None,
        manifest_hash=None,
        created_at=_NOW,
        started_at=_NOW,
        completed_at=None,
        error=None,
    )
    repository = cast(ResearchDataRepository, _Repository(strategy_id))
    runtime = CanonicalResearchRuntime(
        repository,
        ResearchArtifactPublisher(tmp_path),
        AShareRuleBook.load(source_root / "configs" / "rules" / "a_share.yaml"),
    )

    result = runtime.execute(
        family,
        execution,
        variant,
        run,
        _Progress(),
        _Cancellation(),
    )

    assert Path(result.manifest_path).is_file()
    assert len(result.manifest_hash) == 64
    assert {metric.split for metric in result.metrics} == {"TEST"}
    assert result.stage_status["REGISTER"] == "SUCCEEDED"
    if mode is ResearchMode.SIGNAL_STUDY:
        assert str(result.stage_status["SIMULATE"]).startswith("SKIPPED")
