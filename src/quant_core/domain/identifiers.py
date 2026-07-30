"""Vendor-neutral identifiers for domain entities."""

from dataclasses import dataclass

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
