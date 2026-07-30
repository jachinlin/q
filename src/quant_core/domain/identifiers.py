"""Vendor-neutral identifiers for domain entities."""

from dataclasses import dataclass
from typing import Self
from uuid import UUID, uuid4

from quant_core.domain.enums import Exchange


@dataclass(frozen=True, slots=True)
class InstrumentId:
    """A canonical exchange-qualified six-digit instrument identifier."""

    exchange: Exchange
    symbol: str

    def __post_init__(self) -> None:
        """Enforce identifier invariants for every construction path."""
        if not isinstance(self.exchange, Exchange):
            raise TypeError("exchange must be an Exchange")
        if (
            len(self.symbol) != 6
            or not self.symbol.isascii()
            or not self.symbol.isdecimal()
        ):
            raise ValueError("instrument symbol must be exactly six ASCII digits")

    @classmethod
    def parse(cls, value: str) -> "InstrumentId":
        """Parse a strict ``EXCHANGE:SYMBOL`` canonical identifier."""
        exchange_value, separator, symbol = value.partition(":")
        if separator != ":" or ":" in symbol:
            raise ValueError("instrument identifier must be EXCHANGE:SYMBOL")
        try:
            exchange = Exchange(exchange_value)
        except ValueError as error:
            raise ValueError(f"unsupported exchange: {exchange_value}") from error
        return cls(exchange=exchange, symbol=symbol)

    def canonical(self) -> str:
        """Return the identifier in canonical ``EXCHANGE:SYMBOL`` form."""
        return f"{self.exchange}:{self.symbol}"


@dataclass(frozen=True, slots=True)
class DatasetVersionId:
    """Stable UUID identity for one immutable canonical dataset version."""

    value: UUID

    def __post_init__(self) -> None:
        _require_uuid(self.value)

    @classmethod
    def parse(cls, value: str) -> Self:
        return cls(_parse_canonical_uuid(value))

    @classmethod
    def new(cls) -> Self:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class QualityRunId:
    """UUID identity for one persisted quality evaluation."""

    value: UUID

    def __post_init__(self) -> None:
        _require_uuid(self.value)

    @classmethod
    def parse(cls, value: str) -> Self:
        return cls(_parse_canonical_uuid(value))

    @classmethod
    def new(cls) -> Self:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class SnapshotId:
    """Stable UUID identity for one immutable published snapshot."""

    value: UUID

    def __post_init__(self) -> None:
        _require_uuid(self.value)

    @classmethod
    def parse(cls, value: str) -> Self:
        return cls(_parse_canonical_uuid(value))

    @classmethod
    def new(cls) -> Self:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


def _parse_canonical_uuid(value: str) -> UUID:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("identifier must be a canonical UUID") from error
    if str(parsed) != value:
        raise ValueError("identifier must be a canonical UUID")
    return parsed


def _require_uuid(value: object) -> None:
    if not isinstance(value, UUID):
        raise TypeError("identifier value must be a UUID")
