"""Structured errors used across the quant research platform."""

from collections.abc import Mapping
from dataclasses import dataclass

from quant_core.domain.enums import Severity


@dataclass(frozen=True, slots=True)
class ErrorDetail:
    """Machine-readable and actionable information about an application error."""

    code: str
    severity: Severity
    message: str
    context: Mapping[str, object]
    remediation: str
    retryable: bool


class QuantError(Exception):
    """Application exception that retains structured failure details."""

    def __init__(self, detail: ErrorDetail) -> None:
        self.detail = detail
        super().__init__(detail.message)
