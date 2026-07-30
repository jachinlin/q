"""Foundation quality rules over vendor-neutral canonical frames."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import polars as pl

from quant_core.data.quality.models import QualityIssue
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind, Severity

type CanonicalPartitions = Mapping[DatasetKind, Sequence[pl.DataFrame]]

_LINEAGE_COLUMNS = (
    "source",
    "source_version",
    "availability_source",
    "pit_usable",
    "ingested_at",
)
_IMMEDIATE_AVAILABILITY = (*_LINEAGE_COLUMNS, "available_at")
_REQUIRED_COLUMNS: dict[DatasetKind, tuple[str, ...]] = {
    DatasetKind.INSTRUMENT: (
        "instrument_id",
        "exchange",
        "name",
        *_IMMEDIATE_AVAILABILITY,
    ),
    DatasetKind.TRADE_CALENDAR: (
        "trade_date",
        "is_trading_day",
        *_IMMEDIATE_AVAILABILITY,
    ),
    DatasetKind.DAILY_BAR: (
        "instrument_id",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        *_IMMEDIATE_AVAILABILITY,
    ),
    DatasetKind.SECURITY_STATUS: (
        "instrument_id",
        "trade_date",
        "is_listed",
        "is_suspended",
        *_IMMEDIATE_AVAILABILITY,
    ),
    DatasetKind.FINANCIAL_OBSERVATION: (
        "instrument_id",
        "report_period",
        "metric",
        "revision",
        *_LINEAGE_COLUMNS,
    ),
    DatasetKind.CORPORATE_ACTION: (
        "instrument_id",
        "action_type",
        *_LINEAGE_COLUMNS,
    ),
}


def cross_partition_schema_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        if len(partitions) < 2:
            continue
        expected = partitions[0].schema
        mismatches = sum(partition.schema != expected for partition in partitions[1:])
        if mismatches:
            issues.append(
                _issue(
                    "cross_partition_schema",
                    Severity.FATAL,
                    dataset,
                    actual=mismatches,
                    threshold=0,
                    message="canonical partitions use different schemas",
                    remediation="rebuild the dataset version from one canonical schema",
                )
            )
    return issues


def primary_key_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        frame = _compatible_frame(partitions)
        definition = CANONICAL_SCHEMAS.get(dataset)
        if (
            frame is None
            or definition is None
            or not set(definition.primary_key) <= set(frame.columns)
        ):
            continue
        duplicates = (
            frame.group_by(list(definition.primary_key))
            .len()
            .filter(pl.col("len") > 1)
            .height
        )
        if duplicates:
            issues.append(
                _issue(
                    "primary_key_duplicate",
                    Severity.FATAL,
                    dataset,
                    actual=duplicates,
                    threshold=0,
                    message="canonical primary key is duplicated",
                    remediation="deduplicate or correct the canonical mapping",
                )
            )
    return issues


def required_value_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        frame = _compatible_frame(partitions)
        required = _REQUIRED_COLUMNS.get(dataset, ())
        if frame is None or not required or not set(required) <= set(frame.columns):
            continue
        nulls = sum(frame.get_column(column).null_count() for column in required)
        if nulls:
            issues.append(
                _issue(
                    "required_value_null",
                    Severity.SEVERE,
                    dataset,
                    actual=nulls,
                    threshold=0,
                    message="required canonical values are null",
                    remediation="repair the source mapping or exclude invalid records",
                )
            )
    return issues


def daily_bar_value_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    frame = _compatible_frame(inputs.get(DatasetKind.DAILY_BAR, ()))
    if frame is None:
        return []
    issues: list[QualityIssue] = []
    ohlc_invalid = frame.filter(
        (pl.col("high") < pl.max_horizontal("open", "low", "close"))
        | (pl.col("low") > pl.min_horizontal("open", "high", "close"))
    ).height
    if ohlc_invalid:
        issues.append(
            _issue(
                "ohlc_relationship",
                Severity.SEVERE,
                DatasetKind.DAILY_BAR,
                actual=ohlc_invalid,
                threshold=0,
                message="OHLC price relationships are invalid",
                remediation="inspect price fields in the affected partitions",
            )
        )
    negative_volume = frame.filter(pl.col("volume") < 0).height
    if negative_volume:
        issues.append(
            _issue(
                "negative_volume",
                Severity.SEVERE,
                DatasetKind.DAILY_BAR,
                actual=negative_volume,
                threshold=0,
                message="daily volume is negative",
                remediation="correct the source volume conversion",
            )
        )
    return issues


def coverage_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    bars = _compatible_frame(inputs.get(DatasetKind.DAILY_BAR, ()))
    if bars is None:
        return []
    issues: list[QualityIssue] = []
    calendar = _compatible_frame(inputs.get(DatasetKind.TRADE_CALENDAR, ()))
    if calendar is not None:
        trading_dates = set(
            calendar.filter(pl.col("is_trading_day")).get_column("trade_date").to_list()
        )
        observed_dates = set(bars.get_column("trade_date").drop_nulls().to_list())
        missing_dates = trading_dates - observed_dates
        if missing_dates:
            issues.append(
                _issue(
                    "trading_day_coverage",
                    Severity.SEVERE,
                    DatasetKind.DAILY_BAR,
                    actual=len(missing_dates),
                    threshold=0,
                    message="daily bars contain dates absent from the trading calendar",
                    remediation="refresh or repair the trading calendar version",
                )
            )
    instruments = _compatible_frame(inputs.get(DatasetKind.INSTRUMENT, ()))
    if instruments is not None:
        known = set(instruments.get_column("instrument_id").drop_nulls().to_list())
        missing_instruments = (
            set(bars.get_column("instrument_id").drop_nulls().to_list()) - known
        )
        if missing_instruments:
            issues.append(
                _issue(
                    "instrument_coverage",
                    Severity.SEVERE,
                    DatasetKind.DAILY_BAR,
                    actual=len(missing_instruments),
                    threshold=0,
                    message="daily bars contain instruments absent from master data",
                    remediation="refresh instrument master data or repair code mapping",
                )
            )
    return issues


def financial_availability_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    frame = _compatible_frame(inputs.get(DatasetKind.FINANCIAL_OBSERVATION, ()))
    if frame is None:
        return []
    invalid = frame.filter(
        pl.col("pit_usable")
        & (
            pl.col("announced_at").is_null()
            | pl.col("available_at").is_null()
            | (
                pl.col("announced_at").is_not_null()
                & (pl.col("announced_at") > pl.col("available_at"))
            )
        )
    ).height
    if not invalid:
        return []
    return [
        _issue(
            "financial_availability",
            Severity.SEVERE,
            DatasetKind.FINANCIAL_OBSERVATION,
            actual=invalid,
            threshold=0,
            message="financial availability is missing or precedes its announcement",
            remediation="correct publication timing or mark the observation PIT unusable",
        )
    ]


def _compatible_frame(partitions: Sequence[pl.DataFrame]) -> pl.DataFrame | None:
    if not partitions:
        return None
    schema = partitions[0].schema
    compatible = [partition for partition in partitions if partition.schema == schema]
    return pl.concat(compatible) if len(compatible) > 1 else compatible[0]


def _issue(
    rule_id: str,
    severity: Severity,
    dataset: DatasetKind,
    *,
    actual: int,
    threshold: int,
    message: str,
    remediation: str,
) -> QualityIssue:
    return QualityIssue(
        rule_id=rule_id,
        severity=severity,
        dataset=dataset,
        scope={},
        actual=actual,
        threshold=threshold,
        message=message,
        remediation=remediation,
    )
