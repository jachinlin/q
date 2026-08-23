"""提供财务报告期与保守披露截止日的稳定计算规则。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


def financial_report_period_end(report_year: int, report_quarter: int) -> date:
    """计算财务报告年和季度对应的期末日；该日期规则是稳定公开 API，因此保留为模块级入口。

    入参：
        report_year：财务报告年度。
        report_quarter：财务报告季度，取值为 1 至 4。
    返回值：
        返回报告报告期结束日期（``date``）。
    异常：
        ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    if report_quarter == 1:
        return date(report_year, 3, 31)
    if report_quarter == 2:
        return date(report_year, 6, 30)
    if report_quarter == 3:
        return date(report_year, 9, 30)
    if report_quarter == 4:
        return date(report_year, 12, 31)
    raise ValueError("report_quarter must be between 1 and 4")


def financial_disclosure_deadline(report_year: int, report_quarter: int) -> date:
    """计算报告期的保守法定披露截止日；该日期规则是稳定公开 API，因此保留为模块级入口。

    入参：
        report_year：财务报告年度。
        report_quarter：财务报告季度，取值为 1 至 4。
    返回值：
        返回披露截止日（``date``）。
    异常：
        ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    if report_quarter == 1:
        return date(report_year, 4, 30)
    if report_quarter == 2:
        return date(report_year, 8, 31)
    if report_quarter == 3:
        return date(report_year, 10, 31)
    if report_quarter == 4:
        return date(report_year + 1, 4, 30)
    raise ValueError("report_quarter must be between 1 and 4")


def financial_request_is_eligible(
    report_year: int,
    report_quarter: int,
    *,
    cutoff: date,
) -> bool:
    """判断计划日是否已严格越过保守披露截止日；该日期规则是稳定公开 API，因此保留为模块级入口。

    入参：
        report_year：财务报告年度。
        report_quarter：财务报告季度，取值为 1 至 4。
        cutoff：计划执行日；截止日当天仍不允许请求。
    返回值：
        计划日晚于截止日时返回 ``True``，否则返回 ``False``。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    return financial_disclosure_deadline(report_year, report_quarter) < cutoff


@dataclass(frozen=True, slots=True)
class FinancialDisclosureBatch:
    """描述最近已结束报告期对应的同截止日财务刷新批次。

    入参：
        report_periods：按报告期末升序排列的 ``(年度, 季度)`` 集合。
        disclosure_deadline：该批次共同采用的保守披露截止日。
    返回值：
        构造不可变的季度披露批次。
    异常：
        ValueError：报告期为空、顺序不稳定或截止日不一致时抛出。
    """

    report_periods: tuple[tuple[int, int], ...]
    disclosure_deadline: date

    def __post_init__(self) -> None:
        if not self.report_periods:
            raise ValueError("financial disclosure batch must contain report periods")
        ordered = tuple(
            sorted(
                self.report_periods,
                key=lambda item: financial_report_period_end(item[0], item[1]),
            )
        )
        if ordered != self.report_periods:
            raise ValueError("financial report periods must use deterministic order")
        if any(
            financial_disclosure_deadline(year, quarter) != self.disclosure_deadline
            for year, quarter in ordered
        ):
            raise ValueError("financial report periods must share one deadline")

    @property
    def start(self) -> date:
        """返回批次内最早报告期末日。

        入参：无。
        返回值：最早报告期末日。
        异常：实例构造已保证批次非空，不主动抛出异常。
        """
        year, quarter = self.report_periods[0]
        return financial_report_period_end(year, quarter)

    @property
    def end(self) -> date:
        """返回批次内最晚报告期末日。

        入参：无。
        返回值：最晚报告期末日。
        异常：实例构造已保证批次非空，不主动抛出异常。
        """
        year, quarter = self.report_periods[-1]
        return financial_report_period_end(year, quarter)


class FinancialDisclosureSchedule:
    """按计划日解析最近已结束报告期及其季度披露触发条件。

    入参：无需构造参数。
    返回值：构造无状态的披露日程计算器。
    异常：日期不在 Python ``date`` 支持范围内时传播标准日期异常。
    """

    @staticmethod
    def latest_completed_batch(planning_date: date) -> FinancialDisclosureBatch:
        """解析计划日前最近已结束报告期对应的完整披露批次。

        入参：
            planning_date：上海业务日期；报告期末当天尚不视为报告期已结束。
        返回值：
            最近报告期及所有共享同一披露截止日的报告期批次。
        异常：
            ValueError：无法在相邻年度中解析到已结束报告期时抛出。
        """
        candidates = tuple(
            (year, quarter)
            for year in range(planning_date.year - 1, planning_date.year + 1)
            for quarter in range(1, 5)
            if financial_report_period_end(year, quarter) < planning_date
        )
        if not candidates:
            raise ValueError("no completed financial report period is available")
        latest = max(
            candidates,
            key=lambda item: financial_report_period_end(item[0], item[1]),
        )
        deadline = financial_disclosure_deadline(latest[0], latest[1])
        periods = tuple(
            sorted(
                (
                    item
                    for item in candidates
                    if financial_disclosure_deadline(item[0], item[1]) == deadline
                ),
                key=lambda item: financial_report_period_end(item[0], item[1]),
            )
        )
        return FinancialDisclosureBatch(periods, deadline)
