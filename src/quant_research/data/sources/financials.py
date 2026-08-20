"""提供财务报告期与保守披露截止日的稳定计算规则。"""

from __future__ import annotations

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
    """判断报告期是否已达到保守披露截止日；该日期规则是稳定公开 API，因此保留为模块级入口。

    入参：
        report_year：财务报告年度。
        report_quarter：财务报告季度，取值为 1 至 4。
        cutoff：调用接口所需的同名参数，具体约束见类型标注。
    返回值：
        返回是否处理Canonical 数据中的财务数据请求``is``准入证券。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    return financial_disclosure_deadline(report_year, report_quarter) <= cutoff
