"""验证通用 Dashboard 设置的可信根存储和安全 HTTP 投影。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from quant_research.application.settings import DashboardSettingsService
from quant_research.bootstrap.dashboard import DashboardBootstrap
from quant_research.dashboard.app import create_dashboard_app
from quant_research.infrastructure.runtime_settings import DataRootEnvSettingsStore


def test_dashboard_boots_without_a_preconfigured_data_source_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUANT_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("QUANT_TUSHARE_TOKEN", raising=False)

    app = DashboardBootstrap.build_app(
        static_dir=tmp_path / "static",
        allowed_hosts=("testserver",),
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/settings")

    assert response.status_code == 200
    assert response.json()["data_source_token"] == {
        "configured": False,
        "source": "NONE",
        "updated_at": None,
    }


def test_data_root_env_store_preserves_comments_and_overrides_environment(
    tmp_path: Path,
) -> None:
    path = tmp_path / ".env"
    path.write_text("# local dashboard settings\n\n", encoding="utf-8")
    store = DataRootEnvSettingsStore(
        tmp_path,
        environment={"QUANT_TUSHARE_TOKEN": "environment-token"},
    )

    before = store.read_data_source_token()
    written = store.write_data_source_token("dashboard-token")

    assert before.value == "environment-token"
    assert before.source == "PROCESS_ENVIRONMENT"
    assert written.value == "dashboard-token"
    assert written.source == "DATA_ROOT_ENV"
    assert path.read_text(encoding="utf-8") == (
        "# local dashboard settings\n\nQUANT_TUSHARE_TOKEN=dashboard-token\n"
    )
    assert written.updated_at is not None


def test_clear_dashboard_token_reveals_environment_fallback(tmp_path: Path) -> None:
    store = DataRootEnvSettingsStore(
        tmp_path,
        environment={"QUANT_TUSHARE_TOKEN": "environment-token"},
    )
    store.write_data_source_token("dashboard-token")

    cleared = store.clear_data_source_token()

    assert cleared.value == "environment-token"
    assert cleared.source == "PROCESS_ENVIRONMENT"
    assert not (tmp_path / ".env").exists()


@pytest.mark.parametrize(
    "content",
    (
        "QUANT_TUSHARE_TOKEN=first\nQUANT_TUSHARE_TOKEN=second\n",
        "UNSUPPORTED=value\n",
        "QUANT_TUSHARE_TOKEN\n",
        "QUANT_TUSHARE_TOKEN=bad value\n",
    ),
)
def test_runtime_settings_file_fails_closed_on_invalid_content(
    tmp_path: Path,
    content: str,
) -> None:
    (tmp_path / ".env").write_text(content, encoding="utf-8")
    store = DataRootEnvSettingsStore(tmp_path, environment={})

    with pytest.raises(ValueError):
        store.read_data_source_token()


def test_settings_service_never_serializes_token(tmp_path: Path) -> None:
    store = DataRootEnvSettingsStore(tmp_path, environment={})
    service = DashboardSettingsService(store)

    payload = service.change_data_source_token(
        operation="SET",
        value="highly-sensitive-token",
    )

    status = cast(dict[str, object], payload["data_source_token"])
    assert status["configured"] is True
    assert status["source"] == "DATA_ROOT_ENV"
    assert isinstance(status["updated_at"], str)
    assert "highly-sensitive-token" not in str(payload)


def test_runtime_settings_rejects_symbolic_link_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside.env"
    outside.write_text("QUANT_TUSHARE_TOKEN=outside-token\n", encoding="utf-8")
    try:
        (tmp_path / ".env").symlink_to(outside)
    except OSError as error:
        pytest.skip(f"symbolic link capability unavailable: {type(error).__name__}")
    store = DataRootEnvSettingsStore(tmp_path, environment={})

    with pytest.raises(ValueError, match="symbolic link"):
        store.read_data_source_token()

    assert outside.read_text(encoding="utf-8").endswith("outside-token\n")


def test_settings_http_api_sets_and_clears_without_echoing_token(
    tmp_path: Path,
) -> None:
    service = DashboardSettingsService(
        DataRootEnvSettingsStore(tmp_path, environment={})
    )
    app = create_dashboard_app(
        service=cast(Any, object()),
        commands=cast(Any, object()),
        settings_service=service,
        experiment_service=cast(Any, object()),
        notebook_probe=cast(Any, object()),
        static_dir=tmp_path / "static",
        allowed_hosts=("testserver",),
    )
    client = TestClient(app)
    headers = {
        "X-Request-ID": "settings-request",
        "Origin": "http://testserver",
    }

    initial = client.get("/api/v1/settings")
    saved = client.patch(
        "/api/v1/settings",
        json={
            "data_source_token": {
                "operation": "SET",
                "value": "api-sensitive-token",
            }
        },
        headers=headers,
    )
    cleared = client.patch(
        "/api/v1/settings",
        json={"data_source_token": {"operation": "CLEAR"}},
        headers=headers,
    )

    assert initial.json()["data_source_token"]["configured"] is False
    assert saved.status_code == 200
    assert saved.json()["data_source_token"]["source"] == "DATA_ROOT_ENV"
    assert "api-sensitive-token" not in saved.text
    assert cleared.json()["data_source_token"] == {
        "configured": False,
        "source": "NONE",
        "updated_at": None,
    }


@pytest.mark.parametrize(
    "change",
    (
        {},
        {"data_source_token": {"operation": "SET"}},
        {"data_source_token": {"operation": "CLEAR", "value": "must-not-leak"}},
        {
            "data_source_token": {
                "operation": "SET",
                "value": "must-not-leak\nINJECT=value",
            }
        },
    ),
)
def test_settings_http_api_rejects_invalid_typed_changes_without_value_echo(
    tmp_path: Path,
    change: dict[str, object],
) -> None:
    app = create_dashboard_app(
        service=cast(Any, object()),
        commands=cast(Any, object()),
        settings_service=DashboardSettingsService(
            DataRootEnvSettingsStore(tmp_path, environment={})
        ),
        experiment_service=cast(Any, object()),
        notebook_probe=cast(Any, object()),
        static_dir=tmp_path / "static",
        allowed_hosts=("testserver",),
    )
    response = TestClient(app).patch(
        "/api/v1/settings",
        json=change,
        headers={
            "X-Request-ID": "settings-invalid",
            "Origin": "http://testserver",
        },
    )

    assert response.status_code == 422
    assert "must-not-leak" not in response.text
