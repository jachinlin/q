from __future__ import annotations

from pathlib import Path

_SOURCE = Path(__file__).resolve().parents[2] / "src" / "quant_research"


def test_production_source_has_no_legacy_provider_or_per_instrument_api() -> None:
    forbidden = (
        "baostock",
        "pro_bar",
        "daily_vip",
        "query_profit_data",
        "query_dupont_data",
        "DatasetKind.INSTRUMENT",
        "DatasetKind.DAILY_BAR",
        "DatasetKind.SECURITY_STATUS",
    )
    violations: list[str] = []
    for path in sorted(_SOURCE.rglob("*.py")):
        text = path.read_text(encoding="utf-8").lower()
        for token in forbidden:
            if token.lower() in text:
                violations.append(f"{path.relative_to(_SOURCE)}: {token}")
    repository = (_SOURCE / "data" / "repository.py").read_text(encoding="utf-8")
    for token in ("def instruments(", "def bars(", "def adjusted_bars("):
        if token in repository:
            violations.append(f"data/repository.py: {token}")
    assert not violations, violations
