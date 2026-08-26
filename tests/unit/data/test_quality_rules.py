from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.quality.models import QualityRuleStatus
from quant_research.data.quality.rules import (
    coverage_issues,
    daily_bar_value_issues,
    required_value_issues,
)
from quant_research.data.quality.runner import QualityRunner
from quant_research.domain.enums import DatasetKind, Severity


def _daily_basic(*, turnover: float | None) -> pl.DataFrame:
    now = datetime(2026, 8, 13, tzinfo=UTC)
    dataset = DatasetKind.STOCK_DAILY_BASIC
    row = dict.fromkeys(CANONICAL_SCHEMAS[dataset].columns.names()) | {
        "instrument_id": ["600000.SH"],
        "trade_date": [date(2026, 8, 13)],
        "turnover_rate": [turnover],
        "source": ["tushare"],
        "available_at": [now],
        "availability_source": ["trade_date"],
        "pit_usable": [True],
        "ingested_at": [now],
    }
    return pl.DataFrame(
        row,
        schema=CANONICAL_SCHEMAS[dataset].columns,
        strict=False,
    )


def _daily_bar(
    *,
    open_price: float | None = 10.0,
    high: float | None = 11.0,
    low: float | None = 9.0,
    close: float | None = 10.5,
    volume: int | None = 100,
    amount: float | None = 1_000.0,
) -> pl.DataFrame:
    now = datetime(2026, 8, 13, tzinfo=UTC)
    dataset = DatasetKind.STOCK_DAILY_BAR
    return pl.DataFrame(
        {
            "instrument_id": ["600000.SH"],
            "trade_date": [date(2026, 8, 13)],
            "open": [open_price],
            "high": [high],
            "low": [low],
            "close": [close],
            "preclose": [10.0],
            "change": [0.5],
            "volume": [volume],
            "amount": [amount],
            "after_hours_volume": [None],
            "after_hours_amount": [None],
            "pct_change": [0.05],
            "source": ["tushare"],
            "available_at": [now],
            "availability_source": ["trade_date"],
            "pit_usable": [True],
            "ingested_at": [now],
        },
        schema=CANONICAL_SCHEMAS[dataset].columns,
        strict=False,
    )


@pytest.mark.parametrize("column", ["open", "high", "low", "close"])
@pytest.mark.parametrize(
    "invalid", [0.0, -1.0, float("nan"), float("inf"), float("-inf")]
)
def test_daily_bar_requires_finite_positive_traded_prices(
    column: str, invalid: float
) -> None:
    frame = _daily_bar().with_columns(pl.lit(invalid).cast(pl.Float64).alias(column))

    issues = daily_bar_value_issues({DatasetKind.STOCK_DAILY_BAR: (frame,)})
    issue = next(item for item in issues if item.rule_id == "positive_finite_price")

    assert issue.dataset is DatasetKind.STOCK_DAILY_BAR
    assert issue.actual == 1
    assert issue.threshold == 0


def test_daily_bar_price_rule_ignores_untraded_placeholder() -> None:
    issues = daily_bar_value_issues(
        {
            DatasetKind.STOCK_DAILY_BAR: (
                _daily_bar(
                    open_price=0.0,
                    high=float("inf"),
                    low=-1.0,
                    close=float("nan"),
                    volume=None,
                    amount=None,
                ),
            )
        }
    )

    assert not any(item.rule_id == "positive_finite_price" for item in issues)


def test_quality_runner_fails_positive_finite_price_rule() -> None:
    evaluation = QualityRunner().evaluate(
        {DatasetKind.STOCK_DAILY_BAR: (_daily_bar(close=0.0),)}
    )
    result = next(
        item
        for item in evaluation.rule_results
        if item.rule_id == "positive_finite_price"
    )

    assert result.status is QualityRuleStatus.FAIL
    assert result.severity is Severity.SEVERE
    assert result.actual == 1
    assert result.threshold == 0


def test_daily_basic_allows_nullable_optional_values() -> None:
    issues = required_value_issues(
        {DatasetKind.STOCK_DAILY_BASIC: (_daily_basic(turnover=None),)}
    )

    assert issues == []


def test_daily_basic_requires_non_null_primary_key() -> None:
    frame = _daily_basic(turnover=None).with_columns(
        pl.lit(None, dtype=pl.String).alias("instrument_id")
    )
    (issue,) = required_value_issues({DatasetKind.STOCK_DAILY_BASIC: (frame,)})

    assert issue.rule_id == "required_value_null"
    assert issue.dataset is DatasetKind.STOCK_DAILY_BASIC
    assert issue.actual == 1


def test_daily_basic_requires_non_null_audit_evidence() -> None:
    frame = _daily_basic(turnover=None).with_columns(
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("available_at")
    )
    (issue,) = required_value_issues({DatasetKind.STOCK_DAILY_BASIC: (frame,)})

    assert issue.actual == 1


def test_daily_bar_coverage_ignores_calendar_dates_after_latest_bar() -> None:
    issues = coverage_issues(
        {
            DatasetKind.STOCK_DAILY_BAR: (
                pl.DataFrame(
                    {
                        "instrument_id": ["600000.SH", "600000.SH"],
                        "trade_date": [date(2026, 8, 13), date(2026, 8, 14)],
                    }
                ),
            ),
            DatasetKind.TRADE_CALENDAR: (
                pl.DataFrame(
                    {
                        "trade_date": [
                            date(2026, 8, 13),
                            date(2026, 8, 14),
                            date(2026, 8, 17),
                        ],
                        "is_trading_day": [True, True, True],
                    }
                ),
            ),
        }
    )

    assert issues == []


def test_daily_bar_coverage_reports_missing_date_before_latest_bar() -> None:
    (issue,) = coverage_issues(
        {
            DatasetKind.STOCK_DAILY_BAR: (
                pl.DataFrame(
                    {
                        "instrument_id": ["600000.SH", "600000.SH"],
                        "trade_date": [date(2026, 8, 12), date(2026, 8, 14)],
                    }
                ),
            ),
            DatasetKind.TRADE_CALENDAR: (
                pl.DataFrame(
                    {
                        "trade_date": [
                            date(2026, 8, 12),
                            date(2026, 8, 13),
                            date(2026, 8, 14),
                            date(2026, 8, 17),
                        ],
                        "is_trading_day": [True, True, True, True],
                    }
                ),
            ),
        }
    )

    assert issue.rule_id == "trading_day_coverage"
    assert issue.dataset is DatasetKind.STOCK_DAILY_BAR
    assert issue.actual == 1


def _trade_calendar(
    *, duplicate: bool = False, conforming: bool = True
) -> pl.DataFrame:
    now = datetime(2026, 8, 13, tzinfo=UTC)
    dates = [date(2026, 8, 13), date(2026, 8, 13)] if duplicate else [date(2026, 8, 13)]
    values: dict[str, object] = {
        "exchange": ["SSE"] * len(dates),
        "trade_date": dates,
        "is_trading_day": [True] * len(dates),
        "previous_trade_date": [date(2026, 8, 12)] * len(dates),
    }
    if conforming:
        values.update(
            {
                "source": ["test"] * len(dates),
                "available_at": [now] * len(dates),
                "availability_source": ["calendar"] * len(dates),
                "pit_usable": [True] * len(dates),
                "ingested_at": [now] * len(dates),
            }
        )
    if not conforming:
        return pl.DataFrame(values)
    return pl.DataFrame(
        values,
        schema=CANONICAL_SCHEMAS[DatasetKind.TRADE_CALENDAR].columns,
        strict=False,
    )


def test_quality_runner_records_every_applicable_rule_as_pass() -> None:
    evaluation = QualityRunner().evaluate(
        {DatasetKind.TRADE_CALENDAR: (_trade_calendar(),)}
    )

    assert evaluation.issues == ()
    assert evaluation.rule_results
    assert {item.status for item in evaluation.rule_results} == {QualityRuleStatus.PASS}
    assert tuple(item.rule_id for item in evaluation.rule_results) == (
        "required_dataset_missing",
        "required_dataset_empty",
        "canonical_schema",
        "trading_window_empty",
        "cross_partition_schema",
        "primary_key_duplicate",
        "required_value_null",
    )


def test_quality_runner_records_fail_and_schema_dependency_skips() -> None:
    duplicate = QualityRunner().evaluate(
        {DatasetKind.TRADE_CALENDAR: (_trade_calendar(duplicate=True),)}
    )
    malformed = QualityRunner().evaluate(
        {DatasetKind.TRADE_CALENDAR: (_trade_calendar(conforming=False),)}
    )

    duplicate_result = next(
        item
        for item in duplicate.rule_results
        if item.rule_id == "primary_key_duplicate"
    )
    assert duplicate_result.status is QualityRuleStatus.FAIL
    assert duplicate_result.actual == 1
    malformed_by_rule = {item.rule_id: item for item in malformed.rule_results}
    assert malformed_by_rule["canonical_schema"].status is QualityRuleStatus.FAIL
    assert malformed_by_rule["required_value_null"].status is QualityRuleStatus.SKIPPED
    assert "canonical_schema" in (
        malformed_by_rule["required_value_null"].skip_reason or ""
    )
