from pathlib import Path

import pytest

from quant_research.config import Settings


def test_settings_resolves_runtime_paths_under_data_root(tmp_path: Path) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Asia/Shanghai\n", encoding="utf-8")

    settings = Settings.load(config, tmp_path / "runtime")

    assert settings.raw_root == tmp_path / "runtime" / "raw"
    assert settings.state_db == tmp_path / "runtime" / "state" / "quant.db"


def test_settings_load_uses_environment_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Asia/Shanghai\n", encoding="utf-8")
    runtime_root = tmp_path / "runtime"
    monkeypatch.setenv("QUANT_CONFIG", str(config))
    monkeypatch.setenv("QUANT_DATA_ROOT", str(runtime_root))

    settings = Settings.load()

    assert settings.data_root == runtime_root


def test_settings_load_uses_project_config_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("QUANT_CONFIG", raising=False)
    monkeypatch.setenv("QUANT_DATA_ROOT", str(tmp_path / "runtime"))

    settings = Settings.load()

    assert str(settings.timezone) == "Asia/Shanghai"


def test_settings_rejects_data_root_inside_source_tree(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[2]

    with pytest.raises(ValueError, match="outside source tree"):
        Settings.load(
            tmp_path / "base.yaml",
            data_root=source_root / "data",
        )


def test_settings_rejects_unknown_timezone(tmp_path: Path) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Not/A_Timezone\n", encoding="utf-8")

    with pytest.raises(ValueError):
        Settings.load(config, data_root=tmp_path / "runtime")


@pytest.mark.parametrize("value", [0, 101, True, "10"])
def test_settings_rejects_untrusted_factor_partition_limits(
    tmp_path: Path, value: object
) -> None:
    config = tmp_path / "base.yaml"
    config.write_text(
        f"timezone: Asia/Shanghai\nmax_partition_size: {value!r}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_partition_size"):
        Settings.load(config, data_root=tmp_path / "runtime")


def test_settings_load_defaults_data_root_to_user_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Asia/Shanghai\n", encoding="utf-8")
    monkeypatch.setenv("QUANT_CONFIG", str(config))
    monkeypatch.delenv("QUANT_DATA_ROOT", raising=False)

    settings = Settings.load()

    assert settings.data_root == (Path.home() / ".q-data").resolve()


def test_settings_loads_configured_bootstrap_years(tmp_path: Path) -> None:
    config = tmp_path / "base.yaml"
    config.write_text(
        "timezone: Asia/Shanghai\nbootstrap_years: 25\n", encoding="utf-8"
    )

    settings = Settings.load(config, data_root=tmp_path / "runtime")

    assert settings.bootstrap_years == 25


@pytest.mark.parametrize("value", [0, -1, True, "20"])
def test_settings_rejects_invalid_bootstrap_years(
    tmp_path: Path, value: object
) -> None:
    config = tmp_path / "base.yaml"
    config.write_text(
        f"timezone: Asia/Shanghai\nbootstrap_years: {value!r}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="bootstrap_years"):
        Settings.load(config, data_root=tmp_path / "runtime")
