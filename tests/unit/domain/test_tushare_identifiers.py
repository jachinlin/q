from __future__ import annotations

import pytest

from quant_research.domain.identifiers import IndexId, InstrumentId
from quant_research.strategies.base import OrderIntent, OrderSide


@pytest.mark.parametrize("value", ("600000.SH", "000001.SZ", "920001.BJ"))
def test_instrument_id_accepts_supported_trading_venues(value: str) -> None:
    assert InstrumentId.parse(value).canonical() == value


def test_index_id_is_an_independent_domain_type() -> None:
    index = IndexId.parse("000300.SH")
    assert index.canonical() == "000300.SH"
    assert not isinstance(index, InstrumentId)


def test_order_rejects_index_id_at_runtime() -> None:
    with pytest.raises(TypeError, match="InstrumentId"):
        OrderIntent(IndexId.parse("000300.SH"), OrderSide.BUY, 100)  # type: ignore[arg-type]
