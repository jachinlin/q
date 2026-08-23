"""定义作用于供应商无关 Canonical 数据帧的基础质量规则。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time
from zoneinfo import ZoneInfo

import polars as pl

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.quality.models import QualityIssue
from quant_research.domain.enums import DatasetKind, Severity

type CanonicalFrame = pl.DataFrame | pl.LazyFrame
type CanonicalPartitions = Mapping[DatasetKind, Sequence[CanonicalFrame]]

FOUNDATION_REQUIRED_DATASETS = frozenset(
    {
        DatasetKind.INSTRUMENT,
        DatasetKind.TRADE_CALENDAR,
        DatasetKind.DAILY_BAR,
        DatasetKind.DAILY_BASIC,
        DatasetKind.SECURITY_STATUS,
        DatasetKind.FINANCIAL_OBSERVATION,
        DatasetKind.INDUSTRY_CLASSIFICATION,
        DatasetKind.INDEX_BAR,
    }
)

_LINEAGE_COLUMNS = (
    "source",
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
    DatasetKind.DAILY_BASIC: (
        "instrument_id",
        "trade_date",
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
    DatasetKind.INDUSTRY_CLASSIFICATION: (
        "as_of_date",
        "supplier_update_date",
        "instrument_id",
        "taxonomy",
        "is_classified",
        *_LINEAGE_COLUMNS,
    ),
    DatasetKind.INDEX_BAR: (
        "index_id",
        "trade_date",
        "close",
        *_IMMEDIATE_AVAILABILITY,
    ),
}


def required_dataset_issues(
    inputs: CanonicalPartitions,
    required: frozenset[DatasetKind] = FOUNDATION_REQUIRED_DATASETS,
) -> list[QualityIssue]:
    """检查生产基础数据集及作用域是否为空；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
        required：调用接口所需的同名参数，具体约束见类型标注。
    返回值：
        返回数据集质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    issues: list[QualityIssue] = []
    for dataset in sorted(required, key=lambda item: item.value):
        partitions = inputs.get(dataset, ())
        if not partitions:
            issues.append(
                _QualityRuleSupport._issue(
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
        if (
            _QualityRuleSupport._row_count(
                _QualityRuleSupport._compatible_frame(partitions)
            )
            == 0
        ):
            issues.append(
                _QualityRuleSupport._issue(
                    "required_dataset_empty",
                    Severity.FATAL,
                    dataset,
                    actual=0,
                    threshold=1,
                    message="required foundation dataset is empty",
                    remediation="repair source coverage before publishing",
                )
            )
    calendar = _QualityRuleSupport._compatible_frame(
        inputs.get(DatasetKind.TRADE_CALENDAR, ())
    )
    if calendar is not None and (
        _QualityRuleSupport._row_count(calendar) == 0
        or not bool(
            calendar.select(pl.col("is_trading_day").fill_null(False).any())
            .collect()
            .item()
        )
    ):
        issues.append(
            _QualityRuleSupport._issue(
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
    """检查同一数据集跨分区 Schema 是否一致；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回分区Schema质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        if len(partitions) < 2:
            continue
        expected = _QualityRuleSupport._schema(partitions[0])
        mismatches = sum(
            _QualityRuleSupport._schema(partition) != expected
            for partition in partitions[1:]
        )
        if mismatches:
            issues.append(
                _QualityRuleSupport._issue(
                    "cross_partition_schema",
                    Severity.FATAL,
                    dataset,
                    actual=mismatches,
                    threshold=0,
                    message="canonical partitions use different schemas",
                    remediation="rebuild the current dataset from one canonical schema",
                )
            )
    return issues


def canonical_schema_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查各分区是否精确匹配声明的 Canonical Schema；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回Schema质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        definition = CANONICAL_SCHEMAS.get(dataset)
        if definition is None:
            continue
        mismatches = sum(
            _QualityRuleSupport._schema(partition) != definition.columns
            for partition in partitions
        )
        if mismatches:
            issues.append(
                _QualityRuleSupport._issue(
                    "canonical_schema",
                    Severity.FATAL,
                    dataset,
                    actual=mismatches,
                    threshold=0,
                    message="canonical partitions do not match the declared schema",
                    remediation="re-curate the dataset with the declared canonical columns and types",
                )
            )
    return issues


def canonical_conforming_partitions(
    inputs: CanonicalPartitions,
) -> dict[DatasetKind, tuple[CanonicalFrame, ...]]:
    """筛选可安全执行依赖 Canonical 列名规则的分区；该筛选器是质量规则间共享的稳定入口；该函数作为稳定公开 API保留在模块级。

    入参：
        inputs：按数据集分组、等待执行质量规则的 Canonical 分区集合。
    返回值：
        返回``conforming``分区集合（``dict[DatasetKind, tuple[CanonicalFrame, ...]]``）。
    异常：
        无。
    """
    conforming: dict[DatasetKind, tuple[CanonicalFrame, ...]] = {}
    for dataset, partitions in inputs.items():
        definition = CANONICAL_SCHEMAS.get(dataset)
        if definition is None:
            continue
        matches = tuple(
            partition
            for partition in partitions
            if _QualityRuleSupport._schema(partition) == definition.columns
        )
        if matches:
            conforming[dataset] = matches
    return conforming


def primary_key_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查 Canonical 主键中的空值与重复值；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回``key``质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        frame = _QualityRuleSupport._compatible_frame(partitions)
        definition = CANONICAL_SCHEMAS.get(dataset)
        if (
            frame is None
            or definition is None
            or not set(definition.primary_key) <= set(frame.collect_schema().names())
        ):
            continue
        duplicates = _QualityRuleSupport._count(
            frame.group_by(list(definition.primary_key)).len().filter(pl.col("len") > 1)
        )
        if duplicates:
            issues.append(
                _QualityRuleSupport._issue(
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
    """检查研究所需关键字段是否缺失；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回值质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        frame = _QualityRuleSupport._compatible_frame(partitions)
        required = _REQUIRED_COLUMNS.get(dataset, ())
        if (
            frame is None
            or not required
            or not set(required) <= set(frame.collect_schema().names())
        ):
            continue
        if dataset == DatasetKind.DAILY_BAR:
            frame = _QualityRuleSupport._traded_bar_rows(frame)
        nulls = sum(
            int(frame.select(pl.col(column).null_count()).collect().item())
            for column in required
        )
        if dataset == DatasetKind.DAILY_BASIC:
            nulls += _QualityRuleSupport._required_daily_basic_turnover_nulls(
                inputs, frame
            )
        if nulls:
            issues.append(
                _QualityRuleSupport._issue(
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
    """检查日行情价格、成交量与成交额约束；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回行情值质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    frame = _QualityRuleSupport._compatible_frame(inputs.get(DatasetKind.DAILY_BAR, ()))
    if frame is None:
        return []
    issues: list[QualityIssue] = []
    traded = _QualityRuleSupport._traded_bar_rows(frame)
    close = pl.col("close")
    optional_price_invalid = pl.any_horizontal(
        pl.col(column).is_not_null()
        & (~pl.col(column).is_finite() | (pl.col(column) <= 0))
        for column in ("open", "high", "low")
    )
    invalid_price = _QualityRuleSupport._count(
        traded.filter(
            close.is_null() | ~close.is_finite() | (close <= 0) | optional_price_invalid
        )
    )
    if invalid_price:
        issues.append(
            _QualityRuleSupport._issue(
                "positive_finite_price",
                Severity.SEVERE,
                DatasetKind.DAILY_BAR,
                actual=invalid_price,
                threshold=0,
                message="traded daily-bar prices must be finite and positive",
                remediation="correct nonfinite or nonpositive OHLC source values",
            )
        )
    ohlc_invalid = _QualityRuleSupport._count(
        traded.filter(
            (pl.col("high") < pl.max_horizontal("open", "low", "close"))
            | (pl.col("low") > pl.min_horizontal("open", "high", "close"))
        )
    )
    if ohlc_invalid:
        issues.append(
            _QualityRuleSupport._issue(
                "ohlc_relationship",
                Severity.SEVERE,
                DatasetKind.DAILY_BAR,
                actual=ohlc_invalid,
                threshold=0,
                message="OHLC price relationships are invalid",
                remediation="inspect price fields in the affected partitions",
            )
        )
    negative_volume = _QualityRuleSupport._count(traded.filter(pl.col("volume") < 0))
    if negative_volume:
        issues.append(
            _QualityRuleSupport._issue(
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
    """检查交易日历、行情与状态数据的覆盖一致性。

    日线交易日覆盖仅校验至其最新可用日期；质量规则按稳定函数契约注册，
    因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    bars = _QualityRuleSupport._compatible_frame(inputs.get(DatasetKind.DAILY_BAR, ()))
    if bars is None:
        return []
    issues: list[QualityIssue] = []
    calendar = _QualityRuleSupport._compatible_frame(
        inputs.get(DatasetKind.TRADE_CALENDAR, ())
    )
    if calendar is not None:
        observed_dates = set(
            bars.select(pl.col("trade_date").drop_nulls())
            .collect()
            .get_column("trade_date")
            .to_list()
        )
        trading_dates = set(
            calendar.filter(pl.col("is_trading_day"))
            .select("trade_date")
            .collect()
            .get_column("trade_date")
            .to_list()
        )
        if observed_dates:
            coverage_end = max(observed_dates)
            trading_dates = {
                trade_date for trade_date in trading_dates if trade_date <= coverage_end
            }
        missing_dates = trading_dates - observed_dates
        if missing_dates:
            issues.append(
                _QualityRuleSupport._issue(
                    "trading_day_coverage",
                    Severity.SEVERE,
                    DatasetKind.DAILY_BAR,
                    actual=len(missing_dates),
                    threshold=0,
                    message="open trading dates are missing from daily bars",
                    remediation="refresh or repair the current daily bar data",
                )
            )
    instruments = _QualityRuleSupport._compatible_frame(
        inputs.get(DatasetKind.INSTRUMENT, ())
    )
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
                _QualityRuleSupport._issue(
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
    """检查财务观测的公告时间与 PIT 可用性；质量规则按稳定函数契约注册，因此保留为模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回``availability``质量问题（``list[QualityIssue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    frame = _QualityRuleSupport._compatible_frame(
        inputs.get(DatasetKind.FINANCIAL_OBSERVATION, ())
    )
    if frame is None:
        return []
    invalid = _QualityRuleSupport._count(
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
        _QualityRuleSupport._issue(
            "financial_availability",
            Severity.SEVERE,
            DatasetKind.FINANCIAL_OBSERVATION,
            actual=invalid,
            threshold=0,
            message="financial availability is missing or precedes its announcement",
            remediation="correct publication timing or mark the observation PIT unusable",
        )
    ]


def industry_state_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查行业事件状态与 as-of 证据；质量规则按稳定函数契约注册，故保留模块级入口。

    入参：
        inputs：质量规则使用的 Canonical 分区集合。
    返回值：
        返回行业日期、tombstone 或可用性证据问题。
    异常：
        Canonical 值无法按声明类型解释时传播对应异常。
    """
    frame = _QualityRuleSupport._compatible_frame(
        inputs.get(DatasetKind.INDUSTRY_CLASSIFICATION, ())
    )
    if frame is None:
        return []
    shanghai = ZoneInfo("Asia/Shanghai")
    invalid = 0
    for row in frame.collect().iter_rows(named=True):
        as_of_date = row["as_of_date"]
        supplier_update_date = row["supplier_update_date"]
        available_at = row["available_at"]
        classified = row["is_classified"]
        state_valid = (
            classified is True
            and row["industry_code"] is not None
            and row["industry_name"] is not None
        ) or (
            classified is False
            and row["industry_code"] is None
            and row["industry_name"] is None
        )
        expected_available_at = (
            None
            if as_of_date is None
            else datetime.combine(as_of_date, time.max, tzinfo=shanghai)
        )
        if (
            as_of_date is None
            or supplier_update_date is None
            or supplier_update_date > as_of_date
            or not state_valid
            or available_at is None
            or expected_available_at is None
            or available_at.astimezone(shanghai) != expected_available_at
            or row["availability_source"] != "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED"
            or row["pit_usable"] is not True
        ):
            invalid += 1
    if not invalid:
        return []
    return [
        _QualityRuleSupport._issue(
            "industry_state",
            Severity.FATAL,
            DatasetKind.INDUSTRY_CLASSIFICATION,
            actual=invalid,
            threshold=0,
            message="industry events contain invalid state or as-of evidence",
            remediation="repair industry Raw mapping and rebuild the affected year",
        )
    ]


class _QualityRuleSupport:
    """集中承载质量规则共享的内部表处理与问题构造逻辑。"""

    @staticmethod
    def _required_daily_basic_turnover_nulls(
        inputs: CanonicalPartitions,
        daily_basic: pl.LazyFrame,
    ) -> int:
        """Allow BaoStock's empty turnover only for confirmed suspended rows."""
        missing_turnover = daily_basic.filter(pl.col("turnover").is_null())
        status = _QualityRuleSupport._compatible_frame(
            inputs.get(DatasetKind.SECURITY_STATUS, ())
        )
        if status is None:
            return _QualityRuleSupport._count(missing_turnover)
        suspended_keys = (
            status.filter(pl.col("is_suspended").eq(True))
            .select("instrument_id", "trade_date")
            .unique()
        )
        return _QualityRuleSupport._count(
            missing_turnover.join(
                suspended_keys,
                on=["instrument_id", "trade_date"],
                how="anti",
            )
        )

    @staticmethod
    def _traded_bar_rows(frame: pl.LazyFrame) -> pl.LazyFrame:
        """Keep only traded daily-bar sessions.

        Untraded placeholder rows (suspension days) carry null ``volume`` and
        ``amount`` because BaoStock returns empty strings for them; those rows may
        keep null or stale OHLC values and are not subject to daily-bar value
        checks.  A row with only one of the two null is corrupt and stays flagged.
        """
        return frame.filter(~(pl.col("volume").is_null() & pl.col("amount").is_null()))

    @staticmethod
    def _compatible_frame(partitions: Sequence[CanonicalFrame]) -> pl.LazyFrame | None:
        if not partitions:
            return None
        schema = _QualityRuleSupport._schema(partitions[0])
        compatible = [
            _QualityRuleSupport._lazy(partition)
            for partition in partitions
            if _QualityRuleSupport._schema(partition) == schema
        ]
        return pl.concat(compatible) if len(compatible) > 1 else compatible[0]

    @staticmethod
    def _lazy(frame: CanonicalFrame) -> pl.LazyFrame:
        return frame.lazy() if isinstance(frame, pl.DataFrame) else frame

    @staticmethod
    def _schema(frame: CanonicalFrame) -> pl.Schema:
        return (
            frame.schema if isinstance(frame, pl.DataFrame) else frame.collect_schema()
        )

    @staticmethod
    def _count(frame: pl.LazyFrame) -> int:
        return int(frame.select(pl.len()).collect().item())

    @staticmethod
    def _row_count(frame: pl.LazyFrame | None) -> int:
        return 0 if frame is None else _QualityRuleSupport._count(frame)

    @staticmethod
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
