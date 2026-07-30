from pathlib import Path

import pytest

from quant_core.settings import Settings


def test_settings_resolves_runtime_paths_under_data_root(tmp_path: Path) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Asia/Shanghai\n", encoding="utf-8")

    settings = Settings.load(config, data_root=tmp_path / "runtime")

    assert settings.raw_root == tmp_path / "runtime" / "data" / "raw"
    assert settings.state_db == tmp_path / "runtime" / "state" / "quant.db"


def test_settings_rejects_data_root_inside_source_tree(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside source_root"):
        Settings.load(
            tmp_path / "base.yaml",
            data_root=tmp_path / "repo" / "data",
            source_root=tmp_path / "repo",
        )


def test_settings_rejects_unknown_timezone(tmp_path: Path) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Not/A_Timezone\n", encoding="utf-8")

    with pytest.raises(ValueError):
        Settings.load(config, data_root=tmp_path / "runtime")
