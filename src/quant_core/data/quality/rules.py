"""Foundation quality rules over vendor-neutral canonical frames."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import polars as pl

from quant_core.data.quality.models import QualityIssue
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind, Severity

type CanonicalFrame = pl.DataFrame | pl.LazyFrame
type CanonicalPartitions = Mapping[DatasetKind, Sequence[CanonicalFrame]]
QUALITY_RULE_SET_VERSION = "foundation-quality-rules-v2"

FOUNDATION_REQUIRED_DATASETS = frozenset(
    {
        DatasetKind.INSTRUMENT,
        DatasetKind.TRADE_CALENDAR,
        DatasetKind.DAILY_BAR,
        DatasetKind.SECURITY_STATUS,
    }
)

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


def required_dataset_issues(
    inputs: CanonicalPartitions,
    required: frozenset[DatasetKind] = FOUNDATION_REQUIRED_DATASETS,
) -> list[QualityIssue]:
    """Fail closed when a production foundation dataset or scope is empty."""
    issues: list[QualityIssue] = []
    for dataset in sorted(required, key=lambda item: item.value):
        partitions = inputs.get(dataset, ())
        if not partitions:
            issues.append(
                _issue(
                    "required_dataset_missing",
                    Severity.FATAL,
                    dataset,
                    actual=0,
                    threshold=1,
                    message="required foundation dataset is missing",
                    remediation="ingest and curate the required dataset before publishing",
                )
            )
            continue
        if _row_count(_compatible_frame(partitions)) == 0:
            issues.append(
                _issue(
                    "required_dataset_empty",
                    Severity.FATAL,
                    dataset,
                    actual=0,
                    threshold=1,
                    message="required foundation dataset is empty",
                    remediation="repair source coverage before publishing",
                )
            )
    calendar = _compatible_frame(inputs.get(DatasetKind.TRADE_CALENDAR, ()))
    if calendar is not None and (
        _row_count(calendar) == 0
        or not bool(
            calendar.select(pl.col("is_trading_day").fill_null(False).any())
            .collect()
            .item()
        )
    ):
        issues.append(
            _issue(
                "trading_window_empty",
                Severity.FATAL,
                DatasetKind.TRADE_CALENDAR,
                actual=0,
                threshold=1,
                message="resolved pipeline window contains no open trading day",
                remediation="choose a window containing at least one open trading day",
            )
        )
    return issues


def cross_partition_schema_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        if len(partitions) < 2:
            continue
        expected = _schema(partitions[0])
        mismatches = sum(_schema(partition) != expected for partition in partitions[1:])
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
            or not set(definition.primary_key) <= set(frame.collect_schema().names())
        ):
            continue
        duplicates = _count(
            frame.group_by(list(definition.primary_key)).len().filter(pl.col("len") > 1)
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
        if (
            frame is None
            or not required
            or not set(required) <= set(frame.collect_schema().names())
        ):
            continue
        nulls = sum(
            int(frame.select(pl.col(column).null_count()).collect().item())
            for column in required
        )
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
    ohlc_invalid = _count(
        frame.filter(
            (pl.col("high") < pl.max_horizontal("open", "low", "close"))
            | (pl.col("low") > pl.min_horizontal("open", "high", "close"))
        )
    )
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
    negative_volume = _count(frame.filter(pl.col("volume") < 0))
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
            calendar.filter(pl.col("is_trading_day"))
            .select("trade_date")
            .collect()
            .get_column("trade_date")
            .to_list()
        )
        observed_dates = set(
            bars.select(pl.col("trade_date").drop_nulls())
            .collect()
            .get_column("trade_date")
            .to_list()
        )
        missing_dates = trading_dates - observed_dates
        if missing_dates:
            issues.append(
                _issue(
                    "trading_day_coverage",
                    Severity.SEVERE,
                    DatasetKind.DAILY_BAR,
                    actual=len(missing_dates),
                    threshold=0,
                    message="open trading dates are missing from daily bars",
                    remediation="refresh or repair the daily bar version",
                )
            )
    instruments = _compatible_frame(inputs.get(DatasetKind.INSTRUMENT, ()))
    if instruments is not None:
        known = set(
            instruments.select(pl.col("instrument_id").drop_nulls())
            .collect()
            .get_column("instrument_id")
            .to_list()
        )
        missing_instruments = (
            set(
                bars.select(pl.col("instrument_id").drop_nulls())
                .collect()
                .get_column("instrument_id")
                .to_list()
            )
            - known
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
    invalid = _count(
        frame.filter(
            pl.col("pit_usable")
            & (
                pl.col("announced_at").is_null()
                | pl.col("available_at").is_null()
                | (
                    pl.col("announced_at").is_not_null()
                    & (pl.col("announced_at") > pl.col("available_at"))
                )
            )
        )
    )
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


def _compatible_frame(partitions: Sequence[CanonicalFrame]) -> pl.LazyFrame | None:
    if not partitions:
        return None
    schema = _schema(partitions[0])
    compatible = [
        _lazy(partition) for partition in partitions if _schema(partition) == schema
    ]
    return pl.concat(compatible) if len(compatible) > 1 else compatible[0]


def _lazy(frame: CanonicalFrame) -> pl.LazyFrame:
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def _schema(frame: CanonicalFrame) -> pl.Schema:
    return frame.schema if isinstance(frame, pl.DataFrame) else frame.collect_schema()


def _count(frame: pl.LazyFrame) -> int:
    return int(frame.select(pl.len()).collect().item())


def _row_count(frame: pl.LazyFrame | None) -> int:
    return 0 if frame is None else _count(frame)


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
