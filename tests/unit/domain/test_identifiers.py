"""Tests for vendor-neutral domain identifiers and enumerations."""

from dataclasses import FrozenInstanceError

import pytest

from quant_core.domain.enums import Exchange
from quant_core.domain.identifiers import InstrumentId


def test_instrument_id_round_trip() -> None:
    """A parsed canonical SSE identifier retains its exchange and symbol."""
    instrument = InstrumentId.parse("SSE:600000")

    assert instrument.canonical() == "SSE:600000"
    assert instrument.exchange is Exchange.SSE
    assert instrument.symbol == "600000"


def test_instrument_id_rejects_unknown_exchange() -> None:
    """An identifier from an unsupported exchange cannot enter the domain."""
    with pytest.raises(ValueError, match="unsupported exchange"):
        InstrumentId.parse("BSE:600000")


def test_instrument_id_rejects_non_six_digit_symbol() -> None:
    """A symbol that is not exactly six ASCII digits cannot enter the domain."""
    with pytest.raises(ValueError, match="six ASCII digits"):
        InstrumentId.parse("SSE:60000A")


def test_instrument_id_is_immutable() -> None:
    """A parsed identifier cannot be mutated after construction."""
    instrument = InstrumentId.parse("SZSE:000001")

    with pytest.raises(FrozenInstanceError):
        instrument.symbol = "000002"  # type: ignore[misc]


def test_instrument_id_rejects_invalid_direct_symbol() -> None:
    """Direct construction cannot bypass the six-digit symbol invariant."""
    with pytest.raises(ValueError, match="six ASCII digits"):
        InstrumentId(exchange=Exchange.SSE, symbol="60000A")
