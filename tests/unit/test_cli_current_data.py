"""CLI surface tests for the current-data-only design."""

import tomllib
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Never, cast

import pytest
from typer.testing import CliRunner

from quant_research.cli import create_app
from quant_research.cli.app import ApplicationServices, run
from quant_research.data.pipeline.dataset import (
    DataUpdatePlan,
    DataUpdateWindow,
    DataUpdateWindowBasis,
    LocalizeResult,
)
from quant_research.domain.enums import DatasetKind


def _unexpected_services() -> Never:
    raise AssertionError("help must not construct application services")


def test_package_exposes_only_qlab_command() -> None:
    configuration = tomllib.loads(
        (Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert configuration["project"]["scripts"] == {
        "qlab": "quant_research.bootstrap.cli:main"
    }


def test_cli_runner_uses_qlab_as_program_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Command:
        invocation: tuple[str, bool] | None = None

        def main(self, *, prog_name: str, standalone_mode: bool) -> None:
            self.invocation = (prog_name, standalone_mode)

    command = _Command()
    monkeypatch.setattr("typer.main.get_command", lambda _application: command)

    assert run(create_app(_unexpected_services)) == 0
    assert command.invocation == ("qlab", False)


def test_data_cli_exposes_validate_gate_without_snapshot_commands() -> None:
    result = CliRunner().invoke(create_app(_unexpected_services), ["data", "--help"])

    assert result.exit_code == 0
    assert "localize-all" in result.stdout
    assert "curate-all" in result.stdout
    assert "validate-all" in result.stdout
    assert "snapshot" not in result.stdout.lower()


def test_cli_exposes_only_single_strategy_study_workflow() -> None:
    """策略 CLI 只保留校验、提交、查询和列表。"""
    app = create_app(_unexpected_services)
    root = CliRunner().invoke(app, ["--help"])
    assert root.exit_code == 0
    assert "strategy-studies" in root.stdout
    assert "experiments" not in root.stdout
    group = CliRunner().invoke(app, ["strategy-studies", "--help"])
    assert group.exit_code == 0
    for command in ("validate", "submit", "show", "list"):
        assert command in group.stdout
    for removed in ("run", "rerun", "compare", "mark"):
        assert removed not in group.stdout


def test_data_commands_do_not_expose_full_and_bootstrap_requires_years() -> None:
    app = create_app(_unexpected_services)

    for command in (
        "bootstrap",
        "update",
        "localize",
        "localize-all",
        "curate",
        "curate-all",
    ):
        result = CliRunner().invoke(app, ["data", command, "--help"])
        assert result.exit_code == 0
        assert "--full" not in result.stdout
    bootstrap = CliRunner().invoke(app, ["data", "bootstrap", "--help"])
    assert "--years" in bootstrap.stdout
    assert "required" in bootstrap.stdout.lower()


def test_localize_cli_derives_dates_before_calling_strict_pipeline() -> None:
    class _Pipeline:
        planned: tuple[date | None, date | None] | None = None
        localized: tuple[date, date] | None = None

        def plan_update(
            self,
            *,
            start: date | None,
            end: date | None,
            datasets: tuple[DatasetKind, ...],
        ) -> DataUpdatePlan:
            self.planned = (start, end)
            window = DataUpdateWindow(
                dataset=datasets[0],
                basis=DataUpdateWindowBasis.INCREMENTAL,
                start=date(2026, 8, 10),
                end=date(2026, 8, 20),
                overlap_days=4,
                current_watermark=date(2026, 8, 14),
            )
            return DataUpdatePlan(
                window_mode="AUTO_INCREMENTAL",
                planned_at=datetime(2026, 8, 21, tzinfo=UTC),
                start=window.start,
                end=window.end,
                dataset_windows=(window,),
            )

        def localize(
            self, dataset: DatasetKind, *, start: date, end: date
        ) -> LocalizeResult:
            self.localized = (start, end)
            return LocalizeResult(dataset, 0, 1, 1)

    pipeline = _Pipeline()
    result = CliRunner().invoke(
        create_app(
            lambda: cast(
                ApplicationServices,
                SimpleNamespace(pipeline=pipeline, close=lambda: None),
            )
        ),
        ["data", "localize", "stock_daily_bar"],
    )

    assert result.exit_code == 0
    assert pipeline.planned == (None, None)
    assert pipeline.localized == (date(2026, 8, 10), date(2026, 8, 20))
