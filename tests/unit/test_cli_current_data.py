"""CLI surface tests for the current-data-only design."""

from typing import Never

from typer.testing import CliRunner

from quant_research.cli import create_app


def _unexpected_services() -> Never:
    raise AssertionError("help must not construct application services")


def test_data_cli_exposes_validate_gate_without_snapshot_commands() -> None:
    result = CliRunner().invoke(create_app(_unexpected_services), ["data", "--help"])

    assert result.exit_code == 0
    assert "localize-all" in result.stdout
    assert "curate-all" in result.stdout
    assert "validate-all" in result.stdout
    assert "snapshot" not in result.stdout.lower()


def test_curate_commands_expose_explicit_full_rebuild() -> None:
    app = create_app(_unexpected_services)

    curate = CliRunner().invoke(app, ["data", "curate", "--help"])
    curate_all = CliRunner().invoke(app, ["data", "curate-all", "--help"])

    assert curate.exit_code == 0
    assert curate_all.exit_code == 0
    assert "--full" in curate.stdout
    assert "--full" in curate_all.stdout
