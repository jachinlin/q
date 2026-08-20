"""注册数据流水线 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable

import typer

from quant_research.cli.app import ApplicationServices, _CliSupport
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError


class _DataCommands:
    @staticmethod
    def register(
        group: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        @group.command("bootstrap")
        def bootstrap(years: int = typer.Option(..., "--years", min=1)) -> None:
            _CliSupport._invoke(
                lambda services: services.pipeline.bootstrap(years=years),
                services_factory,
            )

        @group.command("update")
        def update(
            start: str | None = typer.Option(None),
            end: str | None = typer.Option(None),
        ) -> None:
            if (start is None) != (end is None):
                _CliSupport._emit_error(
                    QuantError(
                        ErrorDetail(
                            code="DATA_PIPELINE_ARGUMENT",
                            severity=Severity.SEVERE,
                            message="start and end must be supplied together",
                            context={},
                            remediation="provide both --start and --end or neither",
                            retryable=False,
                        )
                    )
                )
            parsed_start = (
                _CliSupport._parse_cli_date(start, "start")
                if start is not None
                else None
            )
            parsed_end = (
                _CliSupport._parse_cli_date(end, "end") if end is not None else None
            )
            _CliSupport._invoke(
                lambda services: services.pipeline.update(
                    start=parsed_start, end=parsed_end
                ),
                services_factory,
            )

        @group.command("localize")
        def localize(
            dataset: str,
            from_: str | None = typer.Option(None, "--from"),
            to: str | None = typer.Option(None, "--to"),
        ) -> None:
            parsed = _CliSupport._dataset_arg(dataset)
            start, end = _CliSupport._date_pair(from_, to)

            def execute(services: ApplicationServices) -> dict[str, object]:
                plan = services.pipeline.plan_update(
                    start=start,
                    end=end,
                    datasets=(parsed,),
                )
                if not plan.dataset_windows:
                    raise QuantError(
                        ErrorDetail(
                            code="DATA_LOCALIZE_NOT_REQUIRED",
                            severity=Severity.INFO,
                            message="the selected dataset has no localization window",
                            context={"dataset": parsed.value},
                            remediation="retry after the next update trigger",
                            retryable=False,
                        )
                    )
                window = plan.dataset_windows[0]
                return _CliSupport._localize_payload(
                    services.pipeline.localize(
                        parsed, start=window.start, end=window.end
                    )
                )

            _CliSupport._invoke(execute, services_factory)

        @group.command("localize-all")
        def localize_all(
            from_: str | None = typer.Option(None, "--from"),
            to: str | None = typer.Option(None, "--to"),
        ) -> None:
            start, end = _CliSupport._date_pair(from_, to)

            def execute(services: ApplicationServices) -> dict[str, object]:
                plan = services.pipeline.plan_update(start=start, end=end)
                return {
                    "datasets": [
                        _CliSupport._localize_payload(item)
                        for item in services.pipeline.localize_all(
                            windows=plan.dataset_windows
                        )
                    ]
                }

            _CliSupport._invoke(execute, services_factory)

        @group.command("curate")
        def curate(
            dataset: str,
            from_: str | None = typer.Option(None, "--from"),
            to: str | None = typer.Option(None, "--to"),
        ) -> None:
            start, end = _CliSupport._date_pair(from_, to)
            _CliSupport._invoke(
                lambda services: _CliSupport._curate_payload(
                    services.pipeline.curate(
                        _CliSupport._dataset_arg(dataset),
                        start=start,
                        end=end,
                    )
                ),
                services_factory,
            )

        @group.command("curate-all")
        def curate_all() -> None:
            _CliSupport._invoke(
                lambda services: {
                    "datasets": [
                        _CliSupport._curate_payload(item)
                        for item in services.pipeline.curate_all()
                    ]
                },
                services_factory,
            )

        @group.command("validate")
        def validate(dataset: str) -> None:
            _CliSupport._invoke(
                lambda services: {
                    "quality_run_id": str(
                        services.pipeline.validate(_CliSupport._dataset_arg(dataset))
                    )
                },
                services_factory,
            )

        @group.command("validate-all")
        def validate_all() -> None:
            _CliSupport._invoke(
                lambda services: {"quality_run_id": str(services.pipeline.validate())},
                services_factory,
            )
