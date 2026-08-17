"""注册后台任务 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable

import typer

from quant_research.cli.app import ApplicationServices, _CliSupport


class _TaskCommands:
    @staticmethod
    def register(
        group: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        @group.command("list")
        def list_tasks(
            status: str | None = typer.Option(None),
            limit: int = typer.Option(100),
            offset: int = typer.Option(0),
        ) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._task_commands(services).list(
                    status=status,
                    limit=limit,
                    offset=offset,
                ),
                services_factory,
            )

        @group.command("cancel")
        def cancel_task(task_id: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._task_commands(services).cancel(task_id),
                services_factory,
            )

        @group.command("retry")
        def retry_task(task_id: str) -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._task_commands(services).retry(task_id),
                services_factory,
            )
