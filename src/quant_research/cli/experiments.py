"""注册统一实验和 Run CLI 命令。"""

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
        @group.command("validate")
        def validate_experiment(config: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._experiment_commands(services).validate(
                    config
                ),
                services_factory,
            )

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

        @group.command("run")
        def create_run(experiment_id: str, run_config: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._experiment_commands(services).run(
                    experiment_id, run_config
                ),
                services_factory,
            )

        @group.command("rerun")
        def rerun(run_id: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._experiment_commands(services).rerun(
                    run_id
                ),
                services_factory,
            )

        @group.command("list")
        def list_experiments() -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._experiment_commands(services).list(),
                services_factory,
            )
