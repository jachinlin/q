"""验证组件 JSON Schema 与能力组装在提交前执行。"""

from pathlib import Path

import pytest
import yaml

from quant_research.research_protocols import ResearchConfigResolver
from quant_research.strategies.definitions import ComponentRegistry


@pytest.mark.parametrize(
    "strategy_id", ("stock_multifactor", "dual_ma_trend", "etf_rotation")
)
def test_reference_configurations_satisfy_exact_component_schemas(
    strategy_id: str,
) -> None:
    source_root = Path(__file__).parents[3]
    text = (
        source_root
        / "configs"
        / "research"
        / "examples"
        / f"{strategy_id}.yaml"
    ).read_text(encoding="utf-8")
    resolved = ResearchConfigResolver().resolve_yaml(text)

    template = ComponentRegistry().validate(resolved.config)

    assert template.strategy_id == strategy_id


def test_component_schema_rejects_unknown_fields_before_submission() -> None:
    source_root = Path(__file__).parents[3]
    source = (
        source_root
        / "configs"
        / "research"
        / "examples"
        / "dual_ma_trend.yaml"
    ).read_text(encoding="utf-8")
    raw = yaml.safe_load(source)
    raw["signal"]["look_ahead"] = True
    resolved = ResearchConfigResolver().resolve_yaml(
        yaml.safe_dump(raw, allow_unicode=True, sort_keys=False)
    )

    with pytest.raises(ValueError, match="unknown fields: look_ahead"):
        ComponentRegistry().validate(resolved.config)


def test_component_catalog_publishes_full_draft_schema() -> None:
    descriptor = ComponentRegistry().descriptor("dual_ma_directional")

    assert descriptor.config_schema["$schema"].endswith("2020-12/schema")
    assert descriptor.config_schema["additionalProperties"] is False
    assert "short_window_sessions" in descriptor.config_schema["properties"]
