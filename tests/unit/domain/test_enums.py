"""Tests for shared domain enumeration values."""

from quant_core.domain.enums import (
    Board,
    DatasetKind,
    Exchange,
    Severity,
    SnapshotStatus,
)


def test_domain_enums_use_the_specified_serialized_values() -> None:
    """Serialization preserves the controlled vocabulary used by downstream data."""
    assert [(member.name, member.value) for member in Exchange] == [
        ("SSE", "SSE"),
        ("SZSE", "SZSE"),
    ]
    assert [(member.name, member.value) for member in Board] == [
        ("MAIN", "MAIN"),
        ("CHINEXT", "CHINEXT"),
        ("STAR", "STAR"),
    ]
    assert [(member.name, member.value) for member in Severity] == [
        ("INFO", "INFO"),
        ("WARNING", "WARNING"),
        ("SEVERE", "SEVERE"),
        ("FATAL", "FATAL"),
    ]
    assert [(member.name, member.value) for member in DatasetKind] == [
        ("INSTRUMENT", "instrument"),
        ("TRADE_CALENDAR", "trade_calendar"),
        ("DAILY_BAR", "daily_bar"),
        ("SECURITY_STATUS", "security_status"),
        ("FINANCIAL_OBSERVATION", "financial_observation"),
        ("CORPORATE_ACTION", "corporate_action"),
        ("FACTOR_VALUE", "factor_value"),
    ]
    assert [(member.name, member.value) for member in SnapshotStatus] == [
        ("DRAFT", "DRAFT"),
        ("PUBLISHED", "PUBLISHED"),
    ]
