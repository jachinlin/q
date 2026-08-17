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

    INSTRUMENT = "instrument"
    TRADE_CALENDAR = "trade_calendar"
    DAILY_BAR = "daily_bar"
    DAILY_BASIC = "daily_basic"
    SECURITY_STATUS = "security_status"
    FINANCIAL_OBSERVATION = "financial_observation"
    INDUSTRY_CLASSIFICATION = "industry_classification"
    INDEX_BAR = "index_bar"
