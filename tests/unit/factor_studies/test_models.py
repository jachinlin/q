"""验证独立 FactorStudy 最终 YAML 契约和确定性身份。"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from quant_research.domain.enums import MultipleTestingMethod
from quant_research.factor_studies.config import FactorStudyConfigParser


def test_checked_in_factor_study_uses_flat_final_contract() -> None:
    """示例必须严格解析为扁平、不可变定义。"""
    parser = FactorStudyConfigParser()
    path = Path("configs/factor_studies/examples/factor_study.yaml")

    first = parser.parse_file(path)
    second = parser.parse(path.read_text(encoding="utf-8"))

    assert first.config_hash == second.config_hash
    assert first.definition.correction is MultipleTestingMethod.BH_FDR
    assert first.definition.factor_ids == (
        "book_to_price_mrq",
        "momentum_120_20",
    )
    assert first.definition.horizons == (1, 5, 20)
    assert first.definition.cost_bps_scenarios == (5, 10, 20)
    assert first.definition.industry is not None
    assert first.definition.market_cap is not None
    assert first.definition.market_cap.exposure == "LOG_TOTAL_MARKET_VALUE"
    with pytest.raises(ValidationError):
        first.definition.name = "mutated"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("needle", "replacement", "match"),
    [
        ("end_date: 2022-12-31", "end_date: 2017-12-31", "start_date"),
        ("horizons: [1, 5, 20]", "horizons: [5, 1]", "horizons"),
        ("cost_bps_scenarios: [5, 10, 20]", "cost_bps_scenarios: [10, 5]", "cost"),
        ("correction: BH_FDR", "correction: SIDAK", "correction"),
        ("factor_ids: [book_to_price_mrq, momentum_120_20]", "factor_ids: [value, value]", "factor_ids"),
    ],
)
def test_invalid_factor_study_dimensions_are_rejected(
    needle: str, replacement: str, match: str
) -> None:
    """日期、因子、期限、成本和校正方法必须逐项严格校验。"""
    text = Path("configs/factor_studies/examples/factor_study.yaml").read_text(
        encoding="utf-8"
    )
    with pytest.raises(ValueError, match=match):
        FactorStudyConfigParser().parse(text.replace(needle, replacement))


def test_legacy_experiment_wrappers_are_rejected() -> None:
    """旧 kind、样本分段、治理和 initial_run 包装不得被兼容。"""
    legacy = """name: legacy
kind: FACTOR_STUDY
sample_windows: {}
governance: {correction: BH_FDR, test_budget: 1}
initial_run: {kind: FACTOR_STUDY}
"""
    with pytest.raises(ValueError, match="kind|start_date"):
        FactorStudyConfigParser().parse(legacy)


def test_market_cap_defaults_to_none_and_rejects_unknown_exposure() -> None:
    """旧研究缺少可选块时可读取，未知市值口径必须失败关闭。"""
    text = Path("configs/factor_studies/examples/factor_study.yaml").read_text(
        encoding="utf-8"
    )
    without_market_cap = text.replace(
        "market_cap:\n  exposure: LOG_TOTAL_MARKET_VALUE\n", ""
    )

    assert FactorStudyConfigParser().parse(
        without_market_cap
    ).definition.market_cap is None
    with pytest.raises(ValueError, match="market_cap.exposure"):
        FactorStudyConfigParser().parse(
            text.replace("LOG_TOTAL_MARKET_VALUE", "FLOAT_MARKET_VALUE")
        )
