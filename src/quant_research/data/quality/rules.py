"""定义作用于 Tushare Canonical 数据帧的基础质量规则。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date

import polars as pl

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.quality.models import QualityIssue
from quant_research.domain.enums import DatasetKind, Severity

type CanonicalFrame = pl.DataFrame | pl.LazyFrame
type CanonicalPartitions = Mapping[DatasetKind, Sequence[CanonicalFrame]]

FOUNDATION_REQUIRED_DATASETS = frozenset(DatasetKind)
_AUDIT = ("source", "availability_source", "pit_usable", "ingested_at")
_BSE_FIRST_TRADING_DAY = date(2021, 11, 15)
_PCT_CHANGE_ABSOLUTE_TOLERANCE = 1e-3
_REQUIRED_COLUMNS: Mapping[DatasetKind, tuple[str, ...]] = {
    dataset: (*schema.primary_key, *_AUDIT)
    for dataset, schema in CANONICAL_SCHEMAS.items()
}
_BAR_DATASETS = (
    DatasetKind.STOCK_DAILY_BAR,
    DatasetKind.FUND_DAILY_BAR,
    DatasetKind.INDEX_DAILY_BAR,
)
_STATEMENT_DATASETS = (
    DatasetKind.STOCK_INCOME_STATEMENT,
    DatasetKind.STOCK_BALANCE_SHEET,
    DatasetKind.STOCK_CASH_FLOW_STATEMENT,
)
_DIVIDEND_DATASETS = (DatasetKind.STOCK_DIVIDEND, DatasetKind.FUND_DIVIDEND)
_INSTRUMENT_DATASETS = tuple(
    dataset
    for dataset, schema in CANONICAL_SCHEMAS.items()
    if "instrument_id" in schema.columns
)


def required_dataset_issues(
    inputs: CanonicalPartitions,
    required: frozenset[DatasetKind] = FOUNDATION_REQUIRED_DATASETS,
) -> list[QualityIssue]:
    """检查必需数据集和交易日历是否非空；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区和必需数据集。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset in sorted(required, key=lambda item: item.value):
        partitions = inputs.get(dataset, ())
        if not partitions:
            issues.append(
                _Support.issue(
                    "required_dataset_missing", Severity.FATAL, dataset, 0, 1
                )
            )
        elif _Support.rows(_Support.compatible(partitions)) == 0:
            issues.append(
                _Support.issue("required_dataset_empty", Severity.FATAL, dataset, 0, 1)
            )
    calendar = _Support.compatible(inputs.get(DatasetKind.TRADE_CALENDAR, ()))
    if calendar is not None and not bool(
        calendar.select(pl.col("is_trading_day").fill_null(False).any())
        .collect()
        .item()
    ):
        issues.append(
            _Support.issue(
                "trading_window_empty", Severity.FATAL, DatasetKind.TRADE_CALENDAR, 0, 1
            )
        )
    return issues


def canonical_schema_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查 Schema；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        expected = CANONICAL_SCHEMAS[dataset].columns
        mismatches = sum(_Support.schema(item) != expected for item in partitions)
        if mismatches:
            issues.append(
                _Support.issue(
                    "canonical_schema", Severity.FATAL, dataset, mismatches, 0
                )
            )
    return issues


def canonical_conforming_partitions(
    inputs: CanonicalPartitions,
) -> dict[DatasetKind, tuple[CanonicalFrame, ...]]:
    """筛选合规分区；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：合规分区。异常：帧错误按原类型传播。
    """
    return {
        dataset: tuple(
            item
            for item in partitions
            if _Support.schema(item) == CANONICAL_SCHEMAS[dataset].columns
        )
        for dataset, partitions in inputs.items()
    }


def cross_partition_schema_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查跨分区 Schema；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        if len(partitions) < 2:
            continue
        first = _Support.schema(partitions[0])
        count = sum(_Support.schema(item) != first for item in partitions[1:])
        if count:
            issues.append(
                _Support.issue(
                    "cross_partition_schema", Severity.FATAL, dataset, count, 0
                )
            )
    return issues


def primary_key_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查主键；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        frame = _Support.compatible(partitions)
        if frame is None:
            continue
        keys = CANONICAL_SCHEMAS[dataset].primary_key
        duplicates = int(
            frame.group_by(list(keys))
            .len()
            .filter(pl.col("len") > 1)
            .select(pl.len())
            .collect()
            .item()
        )
        if duplicates:
            issues.append(
                _Support.issue(
                    "primary_key_duplicate", Severity.FATAL, dataset, duplicates, 0
                )
            )
    return issues


def required_value_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查必填值；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset, partitions in inputs.items():
        frame = _Support.compatible(partitions)
        if frame is None:
            continue
        fields = _REQUIRED_COLUMNS[dataset]
        invalid = int(
            frame.filter(
                pl.any_horizontal(*(pl.col(name).is_null() for name in fields))
                | (pl.col("pit_usable") & pl.col("available_at").is_null())
            )
            .select(pl.len())
            .collect()
            .item()
        )
        if invalid:
            issues.append(
                _Support.issue(
                    "required_value_null", Severity.SEVERE, dataset, invalid, 0
                )
            )
    return issues


def instrument_identifier_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查场内证券代码；该函数作为质量规则框架入口保留在模块级。

    入参：Canonical 分区。返回值：代码不满足六位数字加交易所后缀时的质量问题。
    异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset in _INSTRUMENT_DATASETS:
        frame = _Support.compatible(inputs.get(dataset, ()))
        if frame is None:
            continue
        invalid = int(
            frame.filter(
                pl.col("instrument_id").is_not_null()
                & ~pl.col("instrument_id").str.contains(
                    r"^[0-9]{6}\.(?:SH|SZ|BJ)$"
                )
            )
            .select(pl.len())
            .collect()
            .item()
        )
        if invalid:
            issues.append(
                _Support.issue(
                    "instrument_identifier",
                    Severity.FATAL,
                    dataset,
                    invalid,
                    0,
                )
            )
    return issues


def daily_bar_value_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查行情值；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset in _BAR_DATASETS:
        frame = _Support.compatible(inputs.get(dataset, ()))
        if frame is None:
            continue
        traded = frame.filter(
            (pl.col("volume").fill_null(0) != 0)
            | (pl.col("amount").fill_null(0.0) != 0.0)
        )
        invalid_price = int(
            traded.filter(
                pl.any_horizontal(
                    *(
                        pl.col(name).is_null()
                        | ~pl.col(name).is_finite()
                        | (pl.col(name) <= 0)
                        for name in ("open", "high", "low", "close")
                    )
                )
            )
            .select(pl.len())
            .collect()
            .item()
        )
        ohlc_eligible = traded
        if dataset is DatasetKind.STOCK_DAILY_BAR:
            ohlc_eligible = ohlc_eligible.filter(
                ~(
                    pl.col("instrument_id").str.ends_with(".BJ")
                    & (pl.col("trade_date") < _BSE_FIRST_TRADING_DAY)
                )
            )
        invalid_ohlc = int(
            ohlc_eligible.filter(
                (pl.col("high") < pl.max_horizontal("open", "close"))
                | (pl.col("low") > pl.min_horizontal("open", "close"))
                | (pl.col("high") < pl.col("low"))
            )
            .select(pl.len())
            .collect()
            .item()
        )
        negative_volume = int(
            traded.filter(pl.col("volume") < 0).select(pl.len()).collect().item()
        )
        pct_change_eligible = traded.filter(
            pl.col("preclose").is_not_null()
            & pl.col("preclose").is_finite()
            & (pl.col("preclose") > 0)
            & pl.col("pct_change").is_not_null()
            & pl.col("pct_change").is_finite()
        )
        invalid_pct_change = int(
            pct_change_eligible.filter(
                (
                    pl.col("pct_change")
                    - (pl.col("close") / pl.col("preclose") - 1.0)
                ).abs()
                > _PCT_CHANGE_ABSOLUTE_TOLERANCE
            )
            .select(pl.len())
            .collect()
            .item()
        )
        for rule, count in (
            ("positive_finite_price", invalid_price),
            ("ohlc_relationship", invalid_ohlc),
            ("negative_volume", negative_volume),
            ("pct_change_cross_check", invalid_pct_change),
        ):
            if count:
                issues.append(_Support.issue(rule, Severity.SEVERE, dataset, count, 0))
    return issues


def coverage_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查交易日与 Master 覆盖；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    stock_bars = _Support.compatible(inputs.get(DatasetKind.STOCK_DAILY_BAR, ()))
    calendar = _Support.compatible(inputs.get(DatasetKind.TRADE_CALENDAR, ()))
    if stock_bars is not None and calendar is not None:
        latest = stock_bars.select(pl.col("trade_date").max()).collect().item()
        if latest is not None:
            observed = stock_bars.select("trade_date").unique()
            missing = int(
                calendar.filter(
                    pl.col("is_trading_day") & (pl.col("trade_date") <= latest)
                )
                .select("trade_date")
                .unique()
                .join(observed, on="trade_date", how="anti")
                .select(pl.len())
                .collect()
                .item()
            )
            if missing:
                issues.append(
                    _Support.issue(
                        "trading_day_coverage",
                        Severity.SEVERE,
                        DatasetKind.STOCK_DAILY_BAR,
                        missing,
                        0,
                    )
                )
    for bars, master in (
        (DatasetKind.STOCK_DAILY_BAR, DatasetKind.STOCK_MASTER),
        (DatasetKind.FUND_DAILY_BAR, DatasetKind.FUND_MASTER),
    ):
        bar_frame = _Support.compatible(inputs.get(bars, ()))
        master_frame = _Support.compatible(inputs.get(master, ()))
        if bar_frame is None or master_frame is None:
            continue
        unknown = int(
            bar_frame.select("instrument_id")
            .unique()
            .join(
                master_frame.select("instrument_id").unique(),
                on="instrument_id",
                how="anti",
            )
            .select(pl.len())
            .collect()
            .item()
        )
        if unknown:
            issues.append(
                _Support.issue("instrument_coverage", Severity.WARNING, bars, unknown, 0)
            )
    return issues


def financial_availability_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查财务可用性；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    for dataset in (DatasetKind.STOCK_FINANCIAL_INDICATOR, *_STATEMENT_DATASETS):
        frame = _Support.compatible(inputs.get(dataset, ()))
        if frame is None:
            continue
        invalid_filter = (
            pl.col("announcement_date").is_null()
            | (pl.col("announcement_date") < pl.col("report_period"))
            | pl.col("available_at").is_null()
            | ~pl.any_horizontal(
                *(
                    (pl.col("report_period").dt.month() == month)
                    & (pl.col("report_period").dt.day() == day)
                    for month, day in ((3, 31), (6, 30), (9, 30), (12, 31))
                )
            )
        )
        if dataset in _STATEMENT_DATASETS:
            invalid_filter = invalid_filter | (pl.col("report_type") != "1") | (
                pl.col("actual_announcement_date").is_not_null()
                & (pl.col("actual_announcement_date") < pl.col("report_period"))
            )
        invalid = int(
            frame.filter(pl.col("pit_usable") & invalid_filter)
            .select(pl.len())
            .collect()
            .item()
        )
        if invalid:
            issues.append(
                _Support.issue(
                    "financial_availability", Severity.SEVERE, dataset, invalid, 0
                )
            )
    return issues


def dividend_event_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查分红数值、日期与证券边界；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    issues: list[QualityIssue] = []
    nonnegative = {
        DatasetKind.STOCK_DIVIDEND: (
            "stock_dividend_per_share",
            "stock_bonus_rate_per_share",
            "stock_conversion_rate_per_share",
            "cash_dividend_after_tax_per_share",
            "cash_dividend_before_tax_per_share",
            "base_share_count",
        ),
        DatasetKind.FUND_DIVIDEND: (
            "cash_dividend_per_unit",
            "base_unit_count",
            "distribution_amount",
        ),
    }
    for dataset in _DIVIDEND_DATASETS:
        frame = _Support.compatible(inputs.get(dataset, ()))
        if frame is None:
            continue
        negative = pl.any_horizontal(
            *(
                pl.col(field).is_not_null() & (pl.col(field) < 0)
                for field in nonnegative[dataset]
            )
        )
        pay_date = pl.col("pay_date")
        date_conflict = pay_date.is_not_null() & pl.any_horizontal(
            *(
                pl.col(earlier).is_not_null() & (pay_date < pl.col(earlier))
                for earlier in (
                    "announcement_date",
                    "implementation_announcement_date",
                    "record_date",
                    "ex_date",
                )
            )
        )
        invalid_suffix = ~pl.col("instrument_id").str.contains(
            r"^\d{6}\.(?:SH|SZ|BJ)$"
        )
        invalid = int(
            frame.filter(negative | date_conflict | invalid_suffix)
            .select(pl.len())
            .collect()
            .item()
        )
        if invalid:
            issues.append(
                _Support.issue("dividend_event", Severity.SEVERE, dataset, invalid, 0)
            )
    return issues


def industry_state_issues(inputs: CanonicalPartitions) -> list[QualityIssue]:
    """检查行业状态；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：Canonical 分区。返回值：质量问题。异常：帧错误按原类型传播。
    """
    dataset = DatasetKind.INDUSTRY_MEMBERSHIP
    frame = _Support.compatible(inputs.get(dataset, ()))
    if frame is None:
        return []
    invalid = int(
        frame.filter(
            pl.col("in_date").is_null()
            | pl.col("in_available_at").is_null()
            | (
                pl.col("out_date").is_not_null()
                & (pl.col("out_date") < pl.col("in_date"))
            )
            | (pl.col("out_date").is_not_null() & pl.col("out_available_at").is_null())
        )
        .select(pl.len())
        .collect()
        .item()
    )
    return (
        []
        if not invalid
        else [_Support.issue("industry_state", Severity.FATAL, dataset, invalid, 0)]
    )


class _Support:
    """集中质量规则的帧操作和问题构造。"""

    @staticmethod
    def lazy(frame: CanonicalFrame) -> pl.LazyFrame:
        return frame.lazy() if isinstance(frame, pl.DataFrame) else frame

    @staticmethod
    def schema(frame: CanonicalFrame) -> pl.Schema:
        return (
            frame.schema if isinstance(frame, pl.DataFrame) else frame.collect_schema()
        )

    @classmethod
    def compatible(cls, partitions: Sequence[CanonicalFrame]) -> pl.LazyFrame | None:
        if not partitions:
            return None
        schema = cls.schema(partitions[0])
        frames = [cls.lazy(item) for item in partitions if cls.schema(item) == schema]
        return pl.concat(frames) if len(frames) > 1 else frames[0]

    @staticmethod
    def rows(frame: pl.LazyFrame | None) -> int:
        return 0 if frame is None else int(frame.select(pl.len()).collect().item())

    @staticmethod
    def issue(
        rule: str, severity: Severity, dataset: DatasetKind, actual: int, threshold: int
    ) -> QualityIssue:
        return QualityIssue(
            rule_id=rule,
            severity=severity,
            dataset=dataset,
            scope={},
            actual=actual,
            threshold=threshold,
            message=f"canonical quality rule failed: {rule}",
            remediation="repair the Tushare Raw input and rebuild the affected Canonical dataset",
        )
