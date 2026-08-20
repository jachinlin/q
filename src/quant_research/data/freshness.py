"""依据可执行目录和运营状态评估 Canonical 数据新鲜度。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo

from quant_research.data.catalog import DatasetCatalog, FreshnessBasis
from quant_research.data.sources.financials import FinancialDisclosureSchedule
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DatasetOperationalStateRecord,
)


class FreshnessStatus(StrEnum):
    """枚举数据中心展示的数据集新鲜度状态。

    入参：按枚举值构造。返回值：返回枚举成员。异常：非法值抛出 ValueError。
    """

    CURRENT = "CURRENT"
    STALE = "STALE"
    MISSING = "MISSING"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class DatasetFreshness:
    """保存一次数据集新鲜度判定的完整证据。

    入参：由字段声明给出。返回值：构造不可变证据。异常：非法类型按 Python 契约传播。
    """

    dataset: DatasetKind
    status: FreshnessStatus
    actual_watermark: date | None
    expected_watermark: date | None
    lag_days: int | None
    evaluated_at: datetime
    reason: str
    trigger_date: date | None = None
    update_required: bool | None = None


class FreshnessEvaluator:
    """使用目录策略、当前指针和阶段状态执行非阻断新鲜度判定。

    入参：
        catalog：Canonical 可执行目录。
        timezone：业务时区。
        completion_hour：当日数据视为完整的本地小时。
    返回值：
        构造并返回评估器。
    异常：
        ValueError：时间或输入状态不满足契约时抛出。
    """

    def __init__(
        self,
        catalog: DatasetCatalog,
        *,
        timezone: ZoneInfo,
        completion_hour: int = 18,
    ) -> None:
        if not 0 <= completion_hour <= 23:
            raise ValueError("completion_hour must be from 0 through 23")
        self._catalog = catalog
        self._timezone = timezone
        self._completion_hour = completion_hour

    def evaluate(
        self,
        *,
        canonical: tuple[CanonicalDatasetRecord, ...],
        operational: tuple[DatasetOperationalStateRecord, ...],
        evaluated_at: datetime,
        latest_complete_session: date | None,
    ) -> tuple[DatasetFreshness, ...]:
        """评估目录中的全部数据集并返回确定性结果。

        入参：
            canonical：当前 Canonical 数据集记录。
            operational：最近成功阶段状态。
            evaluated_at：本次评估时间。
            latest_complete_session：由已验证交易日历解析的最近完整交易日。
        返回值：
            按目录顺序排列的新鲜度记录。
        异常：
            ValueError：评估时间缺少时区时抛出。
        """
        if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        current = {item.dataset: item for item in canonical}
        states = {item.dataset: item for item in operational}
        cutoff = self._cutoff_date(evaluated_at)
        return tuple(
            self._evaluate_one(
                dataset,
                current.get(dataset),
                states.get(dataset),
                evaluated_at,
                cutoff,
                latest_complete_session,
            )
            for dataset in self._catalog
        )

    def _evaluate_one(
        self,
        dataset: DatasetKind,
        canonical: CanonicalDatasetRecord | None,
        operational: DatasetOperationalStateRecord | None,
        evaluated_at: datetime,
        cutoff: date,
        latest_session: date | None,
    ) -> DatasetFreshness:
        if canonical is None:
            return self._record(
                dataset,
                FreshnessStatus.MISSING,
                None,
                None,
                None,
                evaluated_at,
                "canonical dataset is missing",
            )
        policy = self._catalog[dataset].freshness
        if policy.basis is FreshnessBasis.CALENDAR_HORIZON:
            expected = cutoff + timedelta(days=policy.tolerance_days)
            return self._watermark_record(
                dataset, canonical.end_date, expected, evaluated_at
            )
        if policy.basis is FreshnessBasis.TRADING_SESSION:
            if latest_session is None:
                return self._record(
                    dataset,
                    FreshnessStatus.UNKNOWN,
                    canonical.end_date,
                    None,
                    None,
                    evaluated_at,
                    "validated trade calendar is unavailable",
                )
            return self._watermark_record(
                dataset, canonical.end_date, latest_session, evaluated_at
            )
        if policy.basis is FreshnessBasis.DISCLOSURE_DEADLINE:
            return self._disclosure_record(
                dataset,
                operational,
                evaluated_at,
            )
        if operational is None or operational.last_localized_at is None:
            return self._record(
                dataset,
                FreshnessStatus.UNKNOWN,
                None,
                latest_session,
                None,
                evaluated_at,
                "no successful localize evidence",
            )
        if policy.tolerance_days == 0:
            if latest_session is None:
                return self._record(
                    dataset,
                    FreshnessStatus.UNKNOWN,
                    None,
                    None,
                    None,
                    evaluated_at,
                    "validated trade calendar is unavailable",
                )
            actual = operational.localized_through
            if actual is None:
                return self._record(
                    dataset,
                    FreshnessStatus.UNKNOWN,
                    None,
                    latest_session,
                    None,
                    evaluated_at,
                    "localize target watermark is unavailable",
                )
            return self._watermark_record(dataset, actual, latest_session, evaluated_at)
        age = evaluated_at.astimezone(
            self._timezone
        ) - operational.last_localized_at.astimezone(self._timezone)
        lag = max(0, age.days - policy.tolerance_days)
        status = (
            FreshnessStatus.CURRENT
            if age <= timedelta(days=policy.tolerance_days)
            else FreshnessStatus.STALE
        )
        return self._record(
            dataset,
            status,
            operational.last_localized_at.date(),
            evaluated_at.astimezone(self._timezone).date(),
            lag,
            evaluated_at,
            "last successful localize is within cadence"
            if status is FreshnessStatus.CURRENT
            else "last successful localize exceeded cadence",
        )

    def _watermark_record(
        self,
        dataset: DatasetKind,
        actual: date | None,
        expected: date,
        evaluated_at: datetime,
    ) -> DatasetFreshness:
        if actual is None:
            return self._record(
                dataset,
                FreshnessStatus.UNKNOWN,
                None,
                expected,
                None,
                evaluated_at,
                "canonical watermark is unavailable",
            )
        lag = max(0, (expected - actual).days)
        status = (
            FreshnessStatus.CURRENT if actual >= expected else FreshnessStatus.STALE
        )
        return self._record(
            dataset,
            status,
            actual,
            expected,
            lag,
            evaluated_at,
            "watermark meets target"
            if status is FreshnessStatus.CURRENT
            else "watermark is behind target",
        )

    def _disclosure_record(
        self,
        dataset: DatasetKind,
        operational: DatasetOperationalStateRecord | None,
        evaluated_at: datetime,
    ) -> DatasetFreshness:
        """按最近已结束报告期的披露截止日评估财务数据，不使用内容水位。

        入参：
            dataset：财务观察数据集标识。
            operational：最近成功 LOCALIZE 的运营证据；尚无证据时为空。
            evaluated_at：带时区的评估时间。
        返回值：
            包含披露触发日和是否需要更新的确定性新鲜度记录。
        异常：
            日期计算异常按财务披露日程契约传播。
        """
        local_date = evaluated_at.astimezone(self._timezone).date()
        batch = FinancialDisclosureSchedule.latest_completed_batch(local_date)
        deadline = batch.disclosure_deadline
        if local_date <= deadline:
            return self._record(
                dataset,
                FreshnessStatus.CURRENT,
                None,
                None,
                None,
                evaluated_at,
                "latest completed quarter has not passed its disclosure deadline",
                trigger_date=deadline,
                update_required=False,
            )
        localized_date = (
            None
            if operational is None or operational.last_localized_at is None
            else operational.last_localized_at.astimezone(self._timezone).date()
        )
        refreshed = localized_date is not None and localized_date > deadline
        return self._record(
            dataset,
            FreshnessStatus.CURRENT if refreshed else FreshnessStatus.STALE,
            None,
            None,
            None if refreshed else (local_date - deadline).days,
            evaluated_at,
            "latest disclosure batch has been refreshed"
            if refreshed
            else "latest disclosure deadline has passed and refresh is required",
            trigger_date=deadline,
            update_required=not refreshed,
        )

    @staticmethod
    def _record(
        dataset: DatasetKind,
        status: FreshnessStatus,
        actual: date | None,
        expected: date | None,
        lag: int | None,
        evaluated_at: datetime,
        reason: str,
        *,
        trigger_date: date | None = None,
        update_required: bool | None = None,
    ) -> DatasetFreshness:
        return DatasetFreshness(
            dataset=dataset,
            status=status,
            actual_watermark=actual,
            expected_watermark=expected,
            lag_days=lag,
            evaluated_at=evaluated_at,
            reason=reason,
            trigger_date=trigger_date,
            update_required=update_required,
        )

    def _cutoff_date(self, evaluated_at: datetime) -> date:
        local = evaluated_at.astimezone(self._timezone)
        return (
            local.date()
            if local.hour >= self._completion_hour
            else local.date() - timedelta(days=1)
        )
