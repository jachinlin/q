"""Immutable experiment orchestration and notebook APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from quant_core.experiments.client import ExperimentClient, ExperimentResult
    from quant_core.experiments.runtime import build_default_experiment_worker

__all__ = [
    "ExperimentClient",
    "ExperimentResult",
    "build_default_experiment_worker",
]


def __getattr__(name: str) -> Any:
    if name in {"ExperimentClient", "ExperimentResult"}:
        from quant_core.experiments.client import (
            ExperimentClient,
            ExperimentResult,
        )

        return {
            "ExperimentClient": ExperimentClient,
            "ExperimentResult": ExperimentResult,
        }[name]
    if name == "build_default_experiment_worker":
        from quant_core.experiments.runtime import build_default_experiment_worker

        return build_default_experiment_worker
    raise AttributeError(name)
