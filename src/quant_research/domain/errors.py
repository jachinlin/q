"""提供python-module-conventions与统一错误相关的公开模型、协议与处理流程。"""

from collections.abc import Mapping
from dataclasses import dataclass

from quant_research.domain.enums import Severity


@dataclass(frozen=True, slots=True)
class ErrorDetail:
    """表示领域流程中的错误详情及其业务不变量。

    入参：
        code：跨 CLI 和 Dashboard 边界返回的稳定机器可读错误码。
        severity：质量问题或应用错误的严重程度。
        message：面向用户且已脱敏的错误或状态说明。
        context：本次调用的上下文，类型为 ``Mapping[str, object]``。
        remediation：调用者可执行的修复建议。
        retryable：控制是否启用``retryable``规则的布尔开关。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Machine-readable and actionable information about an application error.
    """

    code: str
    severity: Severity
    message: str
    context: Mapping[str, object]
    remediation: str
    retryable: bool


class QuantError(Exception):
    """表示 ``QuantError`` 对应的领域异常。

    入参：
        detail：供调用者诊断失败原因的可选安全文本。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Application exception that retains structured failure details.
    """

    def __init__(self, detail: ErrorDetail) -> None:
        self.detail = detail
        super().__init__(detail.message)
