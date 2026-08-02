"""Opt-in gates for expensive synthetic and release acceptance workloads."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-performance",
        action="store_true",
        default=False,
        help="run synthetic performance workloads",
    )
    parser.addoption(
        "--run-acceptance",
        action="store_true",
        default=False,
        help="run real snapshot release acceptance workloads",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "performance: synthetic performance workload")
    config.addinivalue_line("markers", "acceptance: real release acceptance workload")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    gates = (
        ("performance", "--run-performance"),
        ("acceptance", "--run-acceptance"),
    )
    for marker, option in gates:
        if config.getoption(option):
            continue
        reason = f"requires {option}"
        skip = pytest.mark.skip(reason=reason)
        for item in items:
            if item.get_closest_marker(marker) is not None:
                item.add_marker(skip)
