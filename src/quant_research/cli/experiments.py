"""注册实验提交与查询 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable

import typer

from quant_research.cli.app import ApplicationServices, _CliSupport


class _ExperimentCommands:
    @staticmethod
    def register(
        group: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        @group.command("submit")
        def submit_experiment(config: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._experiment_commands(services).submit(
                    config
                ),
                services_factory,
            )

        @group.command("show")
        def show_experiment(experiment_id: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._experiment_commands(services).show(
                    experiment_id
                ),
                services_factory,
            )
