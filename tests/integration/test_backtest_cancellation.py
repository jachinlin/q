"""Cancellation is atomic at the daily backtest boundary."""

from pathlib import Path

import pytest

import quant_core.backtest.artifacts as artifacts_module
import quant_core.backtest.engine as engine_module
from quant_core.backtest.engine import BacktestCancelled, BacktestEngine
from quant_core.portfolio import RebalancePlanner
from tests.integration.test_backtest_timeline import (
    _Data,
    _NeverCancelled,
    _Progress,
    _request,
    _RuleBook,
    _Targets,
)


class _CancelAfterOne(_Progress):
    def is_cancelled(self) -> bool:
        return len(self.calls) >= 1


class _AlreadyCancelled:
    def is_cancelled(self) -> bool:
        return True


def test_cancellation_preserves_closed_staging_without_manifest(tmp_path: Path) -> None:
    token = _CancelAfterOne()
    engine = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    )

    with pytest.raises(BacktestCancelled) as caught:
        engine.run(_request(), token, token)

    cancelled = caught.value
    assert cancelled.sessions_completed == 1
    assert cancelled.staging_dir.exists()
    assert not (cancelled.staging_dir / "manifest.json").exists()
    assert (cancelled.staging_dir / "diagnostic.json").exists()
    assert not list(tmp_path.glob("experiment_id=*/manifest.json"))


def test_initial_cancellation_does_not_start_a_session(tmp_path: Path) -> None:
    engine = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    )

    with pytest.raises(BacktestCancelled) as caught:
        engine.run(_request(), _Progress(), _AlreadyCancelled())

    assert caught.value.sessions_completed == 0
    assert (caught.value.staging_dir / "diagnostic.json").exists()
    assert not list(tmp_path.glob("experiment_id=*/manifest.json"))


def test_manifest_write_failure_preserves_original_error_and_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = artifacts_module._write_json

    def fail_manifest(path: Path, payload: dict[str, object]) -> None:
        if path.name == "manifest.json":
            raise OSError("manifest device failure")
        original(path, payload)

    monkeypatch.setattr(artifacts_module, "_write_json", fail_manifest)
    engine = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    )

    with pytest.raises(OSError, match="manifest device failure"):
        engine.run(_request(), _Progress(), _NeverCancelled())

    assert not list(tmp_path.glob("experiment_id=*/manifest.json"))
    diagnostics = list(tmp_path.glob(".staging-*/diagnostic.json"))
    assert len(diagnostics) == 1


def test_existing_successful_experiment_is_never_overwritten(tmp_path: Path) -> None:
    engine = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    )
    first = engine.run(_request(), _Progress(), _NeverCancelled())
    original_manifest = first.manifest_path.read_bytes()

    with pytest.raises(FileExistsError):
        engine.run(_request(), _Progress(), _NeverCancelled())

    assert first.manifest_path.read_bytes() == original_manifest


@pytest.mark.parametrize(
    "failure_point",
    [
        "calendar",
        "data",
        "strategy",
        "execution",
        "accounting",
        "append",
        "close",
        "validate",
    ],
)
def test_engine_failures_preserve_diagnostic_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_point: str
) -> None:
    data = _Data()
    targets = _Targets()
    writer_factory = artifacts_module.BacktestArtifactWriter

    def boom(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError(f"{failure_point} failure")

    if failure_point == "calendar":
        monkeypatch.setattr(data, "calendar", boom)
    elif failure_point == "data":
        monkeypatch.setattr(data, "market_slice", boom)
    elif failure_point == "strategy":
        monkeypatch.setattr(targets, "generate_target", boom)
    elif failure_point == "execution":
        monkeypatch.setattr(engine_module.ExecutionModel, "execute", boom)
    elif failure_point == "accounting":
        monkeypatch.setattr(engine_module.PortfolioAccount, "mark_to_market", boom)
    else:

        class FailingWriter(artifacts_module.BacktestArtifactWriter):
            def append_snapshot(self, *args: object, **kwargs: object) -> None:
                if failure_point == "append":
                    boom()
                super().append_snapshot(*args, **kwargs)

            def close(self) -> None:
                if failure_point == "close":
                    boom()
                super().close()

            def validate(self, *args: object, **kwargs: object):
                if failure_point == "validate":
                    boom()
                return super().validate(*args, **kwargs)

        writer_factory = FailingWriter

    with pytest.raises(RuntimeError, match=f"{failure_point} failure"):
        BacktestEngine(
            data,
            targets,
            _RuleBook(),
            RebalancePlanner(),
            artifact_root=tmp_path,
            writer_factory=writer_factory,
        ).run(_request(), _Progress(), _NeverCancelled())

    assert not list(tmp_path.glob("experiment_id=*/manifest.json"))
    assert list(tmp_path.glob(".staging-*/diagnostic.json"))
