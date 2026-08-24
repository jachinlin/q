"""注册独立因子研究 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable

import typer

from quant_research.cli.app import ApplicationServices, _CliSupport


class _FactorStudyCommands:
    """把 `factor-studies` 子命令适配到应用服务。"""

    @staticmethod
    def register(
        group: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        """注册 validate、submit、show 和 list 命令。"""

        @group.command("validate")
        def validate(config: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._factor_study_commands(
                    services
                ).validate(config),
                services_factory,
            )

        @group.command("submit")
        def submit(config: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._factor_study_commands(
                    services
                ).submit(config),
                services_factory,
            )

        @group.command("show")
        def show(study_id: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._factor_study_commands(services).show(
                    study_id
                ),
                services_factory,
            )

        @group.command("list")
        def list_studies() -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._factor_study_commands(services).list(),
                services_factory,
            )


__all__ = ["_FactorStudyCommands"]
