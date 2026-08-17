"""注册 Worker 生命周期 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable

import typer

from quant_research.cli.app import ApplicationServices, _CliSupport


class _WorkerCommands:
    @staticmethod
    def register(
        group: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        @group.command("once")
        def run_worker_once() -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._worker_commands(services).once(),
                services_factory,
            )

        @group.command("run")
        def run_worker_forever() -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._worker_commands(services).run(),
                services_factory,
            )
