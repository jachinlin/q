"""Tests for structured quant errors."""

from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError


def test_quant_error_preserves_detail() -> None:
    """QuantError exposes the exact structured detail supplied to it."""
    detail = ErrorDetail(
        code="DATA_SCHEMA_MISMATCH",
        severity=Severity.SEVERE,
        message="schema mismatch",
        context={"dataset": "daily_bar"},
        remediation="inspect raw schema",
        retryable=False,
    )

    error = QuantError(detail)

    assert error.detail == detail
