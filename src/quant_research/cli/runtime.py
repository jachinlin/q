"""提供本地 Dashboard、Worker 与 Notebook 的组合启动命令。"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
import urllib.request
import webbrowser
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Protocol

import typer

from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError

_NOTEBOOK_PORT = 8009


class _ChildProcess(Protocol):
    @property
    def pid(self) -> int: ...

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True, slots=True)
class _ProcessSpec:
    name: str
    command: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _RuntimeResult:
    exit_code: int
    reason: str
    service: str | None


class _ProcessSupport:
    @staticmethod
    def spawn(command: Sequence[str]) -> _ChildProcess:
        return subprocess.Popen(command)


class _BrowserSupport:
    @staticmethod
    def dashboard_is_ready(url: str) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=0.25) as response:
                return int(response.status) == 200
        except OSError:
            return False

    @staticmethod
    def open_dashboard(url: str) -> bool:
        return webbrowser.open(url, new=2)


class _DashboardBrowser:
    def __init__(
        self,
        dashboard_port: int,
        *,
        readiness_probe: Callable[[str], bool] = (_BrowserSupport.dashboard_is_ready),
        browser_opener: Callable[[str], bool] = _BrowserSupport.open_dashboard,
    ) -> None:
        dashboard_root = f"http://127.0.0.1:{dashboard_port}"
        self._dashboard_url = dashboard_root
        self._health_url = f"{dashboard_root}/api/v1/health"
        self._readiness_probe = readiness_probe
        self._browser_opener = browser_opener
        self._attempted = False

    def open_when_ready(self) -> bool:
        if self._attempted:
            return True
        if not self._readiness_probe(self._health_url):
            return False
        self._attempted = True
        try:
            opened = self._browser_opener(self._dashboard_url)
        except Exception:  # noqa: BLE001 - browser launch must not stop services.
            opened = False
        typer.echo(
            json.dumps(
                {
                    "status": "BROWSER_OPENED" if opened else "BROWSER_OPEN_FAILED",
                    "url": self._dashboard_url,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            err=not opened,
        )
        return True


class _ProcessSupervisor:
    def __init__(
        self,
        specs: Sequence[_ProcessSpec],
        *,
        process_factory: Callable[[Sequence[str]], _ChildProcess] = (
            _ProcessSupport.spawn
        ),
        sleep: Callable[[float], None] = time.sleep,
        on_tick: Callable[[], bool] | None = None,
    ) -> None:
        self._specs = tuple(specs)
        self._process_factory = process_factory
        self._sleep = sleep
        self._on_tick = on_tick

    def run(self) -> _RuntimeResult:
        processes: list[tuple[_ProcessSpec, _ChildProcess]] = []
        try:
            for spec in self._specs:
                try:
                    process = self._process_factory(spec.command)
                except OSError as error:
                    raise QuantError(
                        ErrorDetail(
                            code="RUNTIME_PROCESS_START_FAILED",
                            severity=Severity.FATAL,
                            message="local runtime process failed to start",
                            context={
                                "service": spec.name,
                                "error_type": type(error).__name__,
                            },
                            remediation="inspect the local Python environment and retry",
                            retryable=False,
                        )
                    ) from error
                processes.append((spec, process))
            typer.echo(
                json.dumps(
                    {
                        "status": "RUNNING",
                        "services": {
                            spec.name: {"pid": process.pid}
                            for spec, process in processes
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            tick_complete = self._on_tick is None
            while True:
                for spec, process in processes:
                    exit_code = process.poll()
                    if exit_code is None:
                        continue
                    return _RuntimeResult(
                        exit_code=self._normalized_exit_code(exit_code),
                        reason="service_exited",
                        service=spec.name,
                    )
                if not tick_complete and self._on_tick is not None:
                    tick_complete = self._on_tick()
                self._sleep(0.2)
        except KeyboardInterrupt:
            return _RuntimeResult(exit_code=0, reason="interrupted", service=None)
        finally:
            self._stop(processes)

    @staticmethod
    def _normalized_exit_code(exit_code: int) -> int:
        return exit_code if 0 <= exit_code <= 255 else 1

    @staticmethod
    def _stop(processes: Sequence[tuple[_ProcessSpec, _ChildProcess]]) -> None:
        active = [process for _spec, process in processes if process.poll() is None]
        for process in active:
            with suppress(OSError):
                process.terminate()
        for process in active:
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                with suppress(OSError):
                    process.kill()
                with suppress(OSError, subprocess.TimeoutExpired):
                    process.wait(timeout=1.0)


class _RuntimeCommands:
    @staticmethod
    def register(application: typer.Typer) -> None:
        @application.command("start")
        def start_runtime(
            dashboard_port: Annotated[int, typer.Option(min=1, max=65535)] = 8000,
            notebook_dir: Annotated[
                Path | None,
                typer.Option(
                    exists=True,
                    file_okay=False,
                    dir_okay=True,
                    resolve_path=True,
                    show_default="CURRENT_DIR/notebooks",
                ),
            ] = None,
        ) -> None:
            """同时启动 Dashboard、Worker 与 JupyterLab Notebook。"""
            try:
                specs = _RuntimeCommands._build_specs(
                    dashboard_port=dashboard_port,
                    notebook_dir=_RuntimeCommands._resolve_notebook_dir(notebook_dir),
                )
                result = _ProcessSupervisor(
                    specs,
                    on_tick=_DashboardBrowser(
                        dashboard_port,
                    ).open_when_ready,
                ).run()
            except QuantError as error:
                from quant_research.cli.app import _CliSupport

                _CliSupport._emit_error(error)
            payload: Mapping[str, object] = {
                "status": "STOPPED" if result.exit_code == 0 else "FAILED",
                "reason": result.reason,
                "service": result.service,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            if result.exit_code != 0:
                raise typer.Exit(code=result.exit_code)

    @staticmethod
    def _build_specs(
        *,
        dashboard_port: int,
        notebook_dir: Path,
    ) -> tuple[_ProcessSpec, ...]:
        if dashboard_port == _NOTEBOOK_PORT:
            raise QuantError(
                ErrorDetail(
                    code="RUNTIME_PORT_CONFLICT",
                    severity=Severity.SEVERE,
                    message="dashboard port conflicts with the fixed notebook port",
                    context={"port": dashboard_port},
                    remediation="choose a dashboard port other than 8009",
                    retryable=False,
                )
            )
        if importlib.util.find_spec("jupyterlab") is None:
            raise QuantError(
                ErrorDetail(
                    code="NOTEBOOK_DEPENDENCY_MISSING",
                    severity=Severity.SEVERE,
                    message="JupyterLab is not installed",
                    context={"dependency": "jupyterlab"},
                    remediation="run `uv sync --group notebook` and retry",
                    retryable=False,
                )
            )
        python = sys.executable
        root = notebook_dir.resolve(strict=True)
        tornado_settings = _RuntimeCommands._notebook_tornado_settings(dashboard_port)
        return (
            _ProcessSpec(
                "dashboard",
                (
                    python,
                    "-m",
                    "uvicorn",
                    "quant_research.bootstrap.dashboard:create_dashboard_app",
                    "--factory",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(dashboard_port),
                    "--log-level",
                    "info",
                ),
            ),
            _ProcessSpec(
                "worker",
                (
                    python,
                    "-m",
                    "quant_research.bootstrap.cli",
                    "worker",
                    "run",
                ),
            ),
            _ProcessSpec(
                "notebook",
                (
                    python,
                    "-m",
                    "jupyterlab",
                    "--no-browser",
                    "--ServerApp.ip=127.0.0.1",
                    f"--ServerApp.port={_NOTEBOOK_PORT}",
                    "--ServerApp.port_retries=0",
                    f"--ServerApp.root_dir={root}",
                    f"--ServerApp.tornado_settings={tornado_settings}",
                    "--IdentityProvider.token=",
                ),
            ),
        )

    @staticmethod
    def _notebook_tornado_settings(dashboard_port: int) -> str:
        """生成仅允许当前本机 Dashboard 内嵌 JupyterLab 的设置。

        入参：
            dashboard_port：Dashboard 实际监听端口。
        返回值：
            返回可直接传给 ``ServerApp.tornado_settings`` 的确定性 JSON。
        异常：
            无主动抛出的异常；端口范围已由 CLI 参数和调用方校验。
        """
        content_security_policy = (
            "frame-ancestors 'self' "
            f"http://127.0.0.1:{dashboard_port} "
            f"http://localhost:{dashboard_port}; "
            "report-uri /api/security/csp-report"
        )
        return json.dumps(
            {"headers": {"Content-Security-Policy": content_security_policy}},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _resolve_notebook_dir(notebook_dir: Path | None) -> Path:
        if notebook_dir is not None:
            return notebook_dir
        default = Path.cwd() / "notebooks"
        try:
            default.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise QuantError(
                ErrorDetail(
                    code="NOTEBOOK_DIR_CREATE_FAILED",
                    severity=Severity.FATAL,
                    message="default notebook directory could not be created",
                    context={"error_type": type(error).__name__},
                    remediation=(
                        "create the notebooks directory or pass --notebook-dir"
                    ),
                    retryable=False,
                )
            ) from error
        return default.resolve(strict=True)
