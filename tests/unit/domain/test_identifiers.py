"""Tests for vendor-neutral domain identifiers and enumerations."""

from dataclasses import FrozenInstanceError

import pytest

from quant_core.domain.enums import Exchange
from quant_core.domain.identifiers import (
    DatasetVersionId,
    InstrumentId,
    QualityRunId,
    SnapshotId,
)


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


@pytest.mark.parametrize(
    "identifier_type",
    [DatasetVersionId, QualityRunId, SnapshotId],
)
def test_uuid_identifiers_round_trip_and_reject_noncanonical_values(
    identifier_type: type[DatasetVersionId] | type[QualityRunId] | type[SnapshotId],
) -> None:
    """Metadata IDs accept canonical UUID text without accepting arbitrary strings."""
    value = "12345678-1234-5678-9234-567812345678"

    assert str(identifier_type.parse(value)) == value
    with pytest.raises(ValueError, match="canonical UUID"):
        identifier_type.parse("not-a-uuid")


@pytest.mark.parametrize(
    "identifier_type",
    [DatasetVersionId, QualityRunId, SnapshotId],
)
def test_new_uuid_identifiers_are_distinct_and_immutable(
    identifier_type: type[DatasetVersionId] | type[QualityRunId] | type[SnapshotId],
) -> None:
    """New metadata IDs are real UUID values and cannot be mutated."""
    first = identifier_type.new()
    second = identifier_type.new()

    assert first != second
    assert identifier_type.parse(str(first)) == first
    with pytest.raises(FrozenInstanceError):
        first.value = str(second)  # type: ignore[misc]


@pytest.mark.parametrize(
    "identifier_type",
    [DatasetVersionId, QualityRunId, SnapshotId],
)
def test_uuid_identifier_direct_construction_cannot_bypass_validation(
    identifier_type: type[DatasetVersionId] | type[QualityRunId] | type[SnapshotId],
) -> None:
    """Direct construction rejects values that are not UUID objects."""
    with pytest.raises(TypeError, match="UUID"):
        identifier_type("not-a-uuid")  # type: ignore[arg-type]
