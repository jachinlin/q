"""Immutable experiment orchestration and notebook APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from quant_core.experiments.client import ExperimentClient, ExperimentResult

__all__ = ["ExperimentClient", "ExperimentResult"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from quant_core.experiments.client import (
            ExperimentClient,
            ExperimentResult,
        )

        return {
            "ExperimentClient": ExperimentClient,
            "ExperimentResult": ExperimentResult,
        }[name]
    raise AttributeError(name)
