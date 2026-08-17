"""组合启动命令与子进程监督器的单元测试。"""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Sequence
from io import StringIO
from pathlib import Path
from typing import Any, Never, cast

import pytest
from typer.testing import CliRunner

from quant_research.bootstrap.cli import CliBootstrap
from quant_research.cli import create_app
from quant_research.cli.runtime import (
    _DashboardBrowser,
    _ProcessSpec,
    _ProcessSupervisor,
    _RuntimeCommands,
)
from quant_research.domain.errors import QuantError


class _FakeProcess:
    def __init__(self, pid: int, polls: Sequence[int | None]) -> None:
        self._polls = list(polls)
        self._last_poll = self._polls[-1] if self._polls else None
        self.pid = pid
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        if self._polls:
            self._last_poll = self._polls.pop(0)
        return self._last_poll

    def terminate(self) -> None:
        self.terminated = True
        self._last_poll = 0

    def kill(self) -> None:
        self.killed = True
        self._last_poll = 1

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self._last_poll or 0


class _FlushCountingStream(StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()


class _FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def _unexpected_services() -> Never:
    raise AssertionError("runtime command must not construct application services")


def test_start_help_exposes_runtime_ports_without_building_services() -> None:
    result = CliRunner().invoke(create_app(_unexpected_services), ["start", "--help"])

    assert result.exit_code == 0
    assert "--dashboard-port" in result.stdout
    assert "--notebook-port" not in result.stdout
    assert "--notebook-dir" in result.stdout


def test_pipeline_log_flushes_only_at_the_service_close_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    terminal = _FlushCountingStream()
    monkeypatch.setattr("quant_research.bootstrap.cli.sys.stderr", terminal)
    logger, file_stream = CliBootstrap._pipeline_logger(tmp_path)
    engine = _FakeEngine()

    logger.emit("info", "pipeline.probe")

    assert terminal.flush_count == 0
    assert file_stream.closed is False
    CliBootstrap._close_resources(
        logger,
        file_stream,
        cast(Any, engine),
    )
    assert terminal.flush_count == 1
    assert file_stream.closed is True
    assert engine.disposed is True


def test_runtime_specs_use_stable_module_entries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", lambda _name: object())

    specs = _RuntimeCommands._build_specs(
        dashboard_port=8100,
        notebook_dir=tmp_path,
    )

    assert [spec.name for spec in specs] == ["dashboard", "worker", "notebook"]
    assert "quant_research.bootstrap.dashboard:create_dashboard_app" in specs[0].command
    assert specs[1].command[-2:] == ("worker", "run")
    assert "--ServerApp.port=8009" in specs[2].command
    assert "--ServerApp.port_retries=0" in specs[2].command
    assert f"--ServerApp.root_dir={tmp_path.resolve()}" in specs[2].command
    assert "--IdentityProvider.token=" in specs[2].command
    tornado_argument = next(
        argument
        for argument in specs[2].command
        if argument.startswith("--ServerApp.tornado_settings=")
    )
    settings = json.loads(tornado_argument.split("=", 1)[1])
    assert settings == {
        "headers": {
            "Content-Security-Policy": (
                "frame-ancestors 'self' http://127.0.0.1:8100 "
                "http://localhost:8100; report-uri /api/security/csp-report"
            )
        }
    }


def test_runtime_rejects_dashboard_on_fixed_notebook_port(tmp_path: Path) -> None:
    with pytest.raises(QuantError) as raised:
        _RuntimeCommands._build_specs(
            dashboard_port=8009,
            notebook_dir=tmp_path,
        )

    assert raised.value.detail.code == "RUNTIME_PORT_CONFLICT"


def test_runtime_defaults_notebook_root_below_current_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    resolved = _RuntimeCommands._resolve_notebook_dir(None)

    assert resolved == tmp_path / "notebooks"
    assert resolved.is_dir()


def test_dashboard_browser_waits_for_health_and_opens_only_once() -> None:
    readiness = iter((False, True))
    probed: list[str] = []
    opened: list[str] = []

    def probe(url: str) -> bool:
        probed.append(url)
        return next(readiness)

    browser = _DashboardBrowser(
        8123,
        readiness_probe=probe,
        browser_opener=lambda url: not opened.append(url),
    )

    assert browser.open_when_ready() is False
    assert browser.open_when_ready() is True
    assert browser.open_when_ready() is True
    assert probed == [
        "http://127.0.0.1:8123/api/v1/health",
        "http://127.0.0.1:8123/api/v1/health",
    ]
    assert opened == ["http://127.0.0.1:8123"]


def test_supervisor_stops_other_services_when_one_process_exits() -> None:
    processes = [
        _FakeProcess(1, [None, 7]),
        _FakeProcess(2, [None]),
        _FakeProcess(3, [None]),
    ]
    pending = iter(processes)
    specs = tuple(
        _ProcessSpec(name, (name,)) for name in ("dashboard", "worker", "notebook")
    )

    result = _ProcessSupervisor(
        specs,
        process_factory=lambda _command: next(pending),
        sleep=lambda _seconds: None,
    ).run()

    assert result.exit_code == 7
    assert result.service == "dashboard"
    assert processes[0].terminated is False
    assert processes[1].terminated is True
    assert processes[2].terminated is True


def test_supervisor_stops_all_services_on_keyboard_interrupt() -> None:
    processes = [_FakeProcess(index, [None]) for index in range(1, 4)]
    pending = iter(processes)
    specs = tuple(_ProcessSpec(str(index), (str(index),)) for index in range(3))

    def interrupt(_seconds: float) -> Never:
        raise KeyboardInterrupt

    result = _ProcessSupervisor(
        specs,
        process_factory=lambda _command: next(pending),
        sleep=interrupt,
    ).run()

    assert result.exit_code == 0
    assert result.reason == "interrupted"
    assert all(process.terminated for process in processes)


def test_supervisor_reports_start_failure_and_stops_started_processes() -> None:
    started = _FakeProcess(1, [None])
    calls = 0

    def spawn(_command: Sequence[str]) -> _FakeProcess:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("unavailable")
        return started

    specs = (
        _ProcessSpec("dashboard", ("dashboard",)),
        _ProcessSpec("worker", ("worker",)),
    )

    with pytest.raises(QuantError) as raised:
        _ProcessSupervisor(specs, process_factory=spawn).run()

    assert raised.value.detail.code == "RUNTIME_PROCESS_START_FAILED"
    assert raised.value.detail.context["service"] == "worker"
    assert started.terminated is True
