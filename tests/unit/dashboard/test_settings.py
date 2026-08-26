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
    monkeypatch.delenv("QUANT_TUSHARE_PROXY_URL", raising=False)
    monkeypatch.delenv("QUANT_TUSHARE_REQUESTS_PER_MINUTE", raising=False)
    monkeypatch.delenv("QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS", raising=False)

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
    assert response.json()["data_source_rate_limit"] == {
        "requests_per_minute": 480,
        "source": "DEFAULT",
        "updated_at": None,
    }
    assert response.json()["data_source_proxy"] == {
        "url": None,
        "source": "NONE",
        "updated_at": None,
    }
    assert response.json()["data_source_concurrency"] == {
        "max_concurrent_requests": 4,
        "source": "DEFAULT",
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


def test_rate_limit_uses_environment_then_dashboard_then_default(tmp_path: Path) -> None:
    store = DataRootEnvSettingsStore(
        tmp_path,
        environment={"QUANT_TUSHARE_REQUESTS_PER_MINUTE": "200"},
    )

    environment = store.read_data_source_rate_limit()
    _, written, _, _ = store.apply_changes(
        token_operation=None,
        token_value=None,
        rate_limit_operation="SET",
        requests_per_minute=300,
    )
    _, cleared, _, _ = store.apply_changes(
        token_operation=None,
        token_value=None,
        rate_limit_operation="CLEAR",
        requests_per_minute=None,
    )
    default = DataRootEnvSettingsStore(tmp_path, environment={}).read_data_source_rate_limit()

    assert (environment.requests_per_minute, environment.source) == (
        200,
        "PROCESS_ENVIRONMENT",
    )
    assert (written.requests_per_minute, written.source) == (300, "DATA_ROOT_ENV")
    assert (cleared.requests_per_minute, cleared.source) == (
        200,
        "PROCESS_ENVIRONMENT",
    )
    assert (default.requests_per_minute, default.source) == (480, "DEFAULT")


def test_combined_settings_change_is_published_in_one_env_file(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("# preserved\n", encoding="utf-8")
    store = DataRootEnvSettingsStore(tmp_path, environment={})

    token, rate, proxy, concurrency = store.apply_changes(
        token_operation="SET",
        token_value="dashboard-token",
        rate_limit_operation="SET",
        requests_per_minute=10_000,
        proxy_operation="SET",
        proxy_url="https://proxy.example.test/",
        concurrency_operation="SET",
        max_concurrent_requests=8,
    )

    assert token.source == rate.source == proxy.source == concurrency.source == "DATA_ROOT_ENV"
    assert concurrency.max_concurrent_requests == 8
    assert proxy.value == "https://proxy.example.test"
    assert path.read_text(encoding="utf-8") == (
        "# preserved\n"
        "QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS=8\n"
        "QUANT_TUSHARE_PROXY_URL=https://proxy.example.test\n"
        "QUANT_TUSHARE_REQUESTS_PER_MINUTE=10000\n"
        "QUANT_TUSHARE_TOKEN=dashboard-token\n"
    )


def test_proxy_uses_environment_then_dashboard_then_none(tmp_path: Path) -> None:
    store = DataRootEnvSettingsStore(
        tmp_path,
        environment={"QUANT_TUSHARE_PROXY_URL": "https://environment.example/"},
    )

    environment = store.read_data_source_proxy()
    _, _, written, _ = store.apply_changes(
        token_operation=None,
        token_value=None,
        rate_limit_operation=None,
        requests_per_minute=None,
        proxy_operation="SET",
        proxy_url="https://dashboard.example/",
    )
    _, _, cleared, _ = store.apply_changes(
        token_operation=None,
        token_value=None,
        rate_limit_operation=None,
        requests_per_minute=None,
        proxy_operation="CLEAR",
        proxy_url=None,
    )
    none = DataRootEnvSettingsStore(tmp_path, environment={}).read_data_source_proxy()

    assert (environment.value, environment.source) == (
        "https://environment.example",
        "PROCESS_ENVIRONMENT",
    )
    assert (written.value, written.source) == (
        "https://dashboard.example",
        "DATA_ROOT_ENV",
    )
    assert (cleared.value, cleared.source) == (
        "https://environment.example",
        "PROCESS_ENVIRONMENT",
    )
    assert (none.value, none.source) == (None, "NONE")


def test_concurrency_uses_environment_then_dashboard_then_default(
    tmp_path: Path,
) -> None:
    store = DataRootEnvSettingsStore(
        tmp_path,
        environment={"QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS": "2"},
    )

    environment = store.read_data_source_concurrency()
    _, _, _, written = store.apply_changes(
        token_operation=None,
        token_value=None,
        rate_limit_operation=None,
        requests_per_minute=None,
        concurrency_operation="SET",
        max_concurrent_requests=8,
    )
    _, _, _, cleared = store.apply_changes(
        token_operation=None,
        token_value=None,
        rate_limit_operation=None,
        requests_per_minute=None,
        concurrency_operation="CLEAR",
        max_concurrent_requests=None,
    )
    default = DataRootEnvSettingsStore(
        tmp_path, environment={}
    ).read_data_source_concurrency()

    assert (environment.max_concurrent_requests, environment.source) == (
        2,
        "PROCESS_ENVIRONMENT",
    )
    assert (written.max_concurrent_requests, written.source) == (8, "DATA_ROOT_ENV")
    assert (cleared.max_concurrent_requests, cleared.source) == (
        2,
        "PROCESS_ENVIRONMENT",
    )
    assert (default.max_concurrent_requests, default.source) == (4, "DEFAULT")


def test_settings_change_validates_final_fallbacks_before_writing(tmp_path: Path) -> None:
    store = DataRootEnvSettingsStore(
        tmp_path,
        environment={"QUANT_TUSHARE_TOKEN": "invalid token"},
    )

    with pytest.raises(ValueError):
        store.apply_changes(
            token_operation=None,
            token_value=None,
            rate_limit_operation="SET",
            requests_per_minute=240,
        )

    assert not (tmp_path / ".env").exists()


@pytest.mark.parametrize(
    "content",
    (
        "QUANT_TUSHARE_TOKEN=first\nQUANT_TUSHARE_TOKEN=second\n",
        "UNSUPPORTED=value\n",
        "QUANT_TUSHARE_TOKEN\n",
        "QUANT_TUSHARE_TOKEN=bad value\n",
        "QUANT_TUSHARE_REQUESTS_PER_MINUTE=0\n",
        "QUANT_TUSHARE_REQUESTS_PER_MINUTE=10001\n",
        "QUANT_TUSHARE_REQUESTS_PER_MINUTE=48.0\n",
        "QUANT_TUSHARE_PROXY_URL=ftp://proxy.example\n",
        "QUANT_TUSHARE_PROXY_URL=https://user:password@proxy.example\n",
        "QUANT_TUSHARE_PROXY_URL=https://proxy.example/?query=yes\n",
        "QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS=0\n",
        "QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS=33\n",
        "QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS=4.0\n",
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
    assert payload["data_source_rate_limit"] == {
        "requests_per_minute": 480,
        "source": "DEFAULT",
        "updated_at": None,
    }
    assert payload["data_source_proxy"] == {
        "url": None,
        "source": "NONE",
        "updated_at": None,
    }
    assert payload["data_source_concurrency"] == {
        "max_concurrent_requests": 4,
        "source": "DEFAULT",
        "updated_at": None,
    }


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
    assert initial.json()["data_source_rate_limit"]["requests_per_minute"] == 480
    assert saved.status_code == 200
    assert saved.json()["data_source_token"]["source"] == "DATA_ROOT_ENV"
    assert "api-sensitive-token" not in saved.text
    assert cleared.json()["data_source_token"] == {
        "configured": False,
        "source": "NONE",
        "updated_at": None,
    }


def test_settings_http_api_atomically_changes_token_and_rate_limit(
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
    headers = {"X-Request-ID": "settings-combined", "Origin": "http://testserver"}

    saved = client.patch(
        "/api/v1/settings",
        json={
            "data_source_token": {"operation": "SET", "value": "secret-token"},
            "data_source_rate_limit": {
                "operation": "SET",
                "requests_per_minute": 240,
            },
            "data_source_proxy": {
                "operation": "SET",
                "url": "https://proxy.example.test/",
            },
            "data_source_concurrency": {
                "operation": "SET",
                "max_concurrent_requests": 8,
            },
        },
        headers=headers,
    )
    cleared = client.patch(
        "/api/v1/settings",
        json={"data_source_rate_limit": {"operation": "CLEAR"}},
        headers=headers,
    )

    assert saved.status_code == 200
    assert saved.json()["data_source_rate_limit"]["requests_per_minute"] == 240
    assert saved.json()["data_source_proxy"]["url"] == "https://proxy.example.test"
    assert saved.json()["data_source_proxy"]["source"] == "DATA_ROOT_ENV"
    assert saved.json()["data_source_proxy"]["updated_at"] is not None
    assert saved.json()["data_source_concurrency"]["max_concurrent_requests"] == 8
    assert saved.json()["data_source_concurrency"]["source"] == "DATA_ROOT_ENV"
    assert "secret-token" not in saved.text
    assert cleared.json()["data_source_rate_limit"] == {
        "requests_per_minute": 480,
        "source": "DEFAULT",
        "updated_at": None,
    }
    assert "QUANT_TUSHARE_TOKEN=secret-token" in (tmp_path / ".env").read_text(
        encoding="utf-8"
    )


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
        {"data_source_rate_limit": {"operation": "SET"}},
        {
            "data_source_rate_limit": {
                "operation": "CLEAR",
                "requests_per_minute": 200,
            }
        },
        {
            "data_source_rate_limit": {
                "operation": "SET",
                "requests_per_minute": 10001,
            }
        },
        {"data_source_proxy": {"operation": "SET"}},
        {
            "data_source_proxy": {
                "operation": "CLEAR",
                "url": "https://must-not-leak.example",
            }
        },
        {"data_source_concurrency": {"operation": "SET"}},
        {
            "data_source_concurrency": {
                "operation": "CLEAR",
                "max_concurrent_requests": 4,
            }
        },
        {
            "data_source_concurrency": {
                "operation": "SET",
                "max_concurrent_requests": 33,
            }
        },
        {
            "data_source_proxy": {
                "operation": "SET",
                "url": "ftp://must-not-leak.example",
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
