"""Tests for dependency-free process peak-RSS evidence."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.performance


def test_process_peak_rss_bytes_reports_positive_os_counter() -> None:
    """Native allocations must be covered by an OS process peak, not tracemalloc."""
    from tests.performance._process_memory import process_peak_rss_bytes

    assert process_peak_rss_bytes() > 0
