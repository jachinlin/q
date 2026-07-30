"""Enumerations shared across vendor-neutral domain models."""

from enum import StrEnum


class Exchange(StrEnum):
    """Supported mainland China stock exchanges."""

    SSE = "SSE"
    SZSE = "SZSE"


class Board(StrEnum):
    """Supported equity listing boards."""

    MAIN = "MAIN"
    CHINEXT = "CHINEXT"
    STAR = "STAR"


class Severity(StrEnum):
    """Severity assigned to structured application errors."""

    INFO = "INFO"
    WARNING = "WARNING"
    SEVERE = "SEVERE"
    FATAL = "FATAL"


class DatasetKind(StrEnum):
    """Kinds of vendor-neutral datasets managed by the platform."""

    INSTRUMENT = "instrument"
    TRADE_CALENDAR = "trade_calendar"
    DAILY_BAR = "daily_bar"
    SECURITY_STATUS = "security_status"
    FINANCIAL_OBSERVATION = "financial_observation"
    CORPORATE_ACTION = "corporate_action"
    FACTOR_VALUE = "factor_value"


class SnapshotStatus(StrEnum):
    """Publication state for a dataset snapshot."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
