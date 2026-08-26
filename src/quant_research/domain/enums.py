"""提供领域基础与enums相关的公开模型、协议与处理流程。"""

from enum import StrEnum


class Exchange(StrEnum):
    """定义 ``Exchange`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Supported mainland China stock exchanges.
    """

    SSE = "SSE"
    SZSE = "SZSE"
    BSE = "BSE"


class Board(StrEnum):
    """定义 ``Board`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Supported equity listing boards.
    """

    MAIN = "MAIN"
    CHINEXT = "CHINEXT"
    STAR = "STAR"
    BSE = "BSE"


class Severity(StrEnum):
    """定义 ``Severity`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Severity assigned to structured application errors.
    """

    INFO = "INFO"
    WARNING = "WARNING"
    SEVERE = "SEVERE"
    FATAL = "FATAL"


class DatasetKind(StrEnum):
    """定义 ``DatasetKind`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Kinds of vendor-neutral datasets managed by the platform.
    """

    STOCK_MASTER = "stock_master"
    FUND_MASTER = "fund_master"
    INDEX_MASTER = "index_master"
    TRADE_CALENDAR = "trade_calendar"
    STOCK_DAILY_BAR = "stock_daily_bar"
    STOCK_ADJUSTMENT_FACTOR = "stock_adjustment_factor"
    FUND_DAILY_BAR = "fund_daily_bar"
    FUND_ADJUSTMENT_FACTOR = "fund_adjustment_factor"
    INDEX_DAILY_BAR = "index_daily_bar"
    STOCK_DAILY_BASIC = "stock_daily_basic"
    STOCK_SUSPENSION = "stock_suspension"
    STOCK_RISK_WARNING = "stock_risk_warning"
    STOCK_FINANCIAL_INDICATOR = "stock_financial_indicator"
    INDUSTRY_CATALOG = "industry_catalog"
    INDUSTRY_MEMBERSHIP = "industry_membership"


class MultipleTestingMethod(StrEnum):
    """定义研究假设族采用的多重检验校正方法。

    入参：校正方法字符串。返回值：对应稳定枚举。异常：未知值抛出 ``ValueError``。
    """

    BONFERRONI = "BONFERRONI"
    BH_FDR = "BH_FDR"
