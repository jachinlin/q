from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from quant_research.backtest.rulebook import AShareRuleBook, SecurityStatus
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId

_RULES = Path(__file__).resolve().parents[3] / "configs" / "rules" / "a_share.yaml"


def test_bse_profile_uses_thirty_percent_price_limit() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    profile = rulebook.trading_profile(
        InstrumentId.parse("920001.BJ"), "STOCK", Board.BSE, date(2026, 8, 25)
    )
    band = rulebook.price_limits(
        profile, date(2026, 8, 25), 10.0, SecurityStatus.NORMAL
    )
    assert band is not None
    assert band.upper == pytest.approx(13.0)
    assert band.lower == pytest.approx(7.0)


def test_no_limit_status_skips_price_band() -> None:
    rulebook = AShareRuleBook.load(_RULES)
    profile = rulebook.trading_profile(
        InstrumentId.parse("600000.SH"), "STOCK", Board.MAIN, date(2026, 8, 25)
    )
    assert (
        rulebook.price_limits(
            profile, date(2026, 8, 25), 10.0, SecurityStatus.NO_LIMIT
        )
        is None
    )
