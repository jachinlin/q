"""Unit contract for the experiment lifecycle state machine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail
from quant_research.experiments.models import ExperimentStatus
from quant_research.experiments.registry import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    StateConflict,
    transition_timestamps,
    validate_transition,
)

ALLOWED = {
    (ExperimentStatus.CREATED, ExperimentStatus.QUEUED),
    (ExperimentStatus.QUEUED, ExperimentStatus.RUNNING),
    (ExperimentStatus.QUEUED, ExperimentStatus.CANCELLED),
    (ExperimentStatus.RUNNING, ExperimentStatus.SUCCEEDED),
    (ExperimentStatus.RUNNING, ExperimentStatus.FAILED),
    (ExperimentStatus.RUNNING, ExperimentStatus.CANCELLED),
}


def _reason() -> ErrorDetail:
    return ErrorDetail(
        code="DATA_UNAVAILABLE",
        severity=Severity.SEVERE,
        message="input partition is unavailable",
        context={"dataset": "prices"},
        remediation="publish the missing partition",
        retryable=False,
    )


def test_allowed_transition_table_matches_the_complete_contract() -> None:
    """Adding or removing a lifecycle edge must change the public state contract."""
    assert ALLOWED_TRANSITIONS == frozenset(ALLOWED)
    for expected, target in ALLOWED:
        validate_transition(expected, target)


@pytest.mark.parametrize(
    ("expected", "target"),
    [
        (expected, target)
        for expected in ExperimentStatus
        for target in ExperimentStatus
        if (expected, target) not in ALLOWED
    ],
)
def test_every_unlisted_transition_is_rejected(
    expected: ExperimentStatus, target: ExperimentStatus
) -> None:
    """An omitted edge, including terminal rollback, must never become valid."""
    with pytest.raises(InvalidTransition) as captured:
        validate_transition(expected, target)

    assert captured.value.detail.code == "EXPERIMENT_INVALID_TRANSITION"
    assert captured.value.detail.retryable is False
    assert captured.value.detail.context == {
        "expected": expected.value,
        "target": target.value,
    }


@pytest.mark.parametrize(
    "target", [ExperimentStatus.FAILED, ExperimentStatus.CANCELLED]
)
def test_reason_is_allowed_only_for_failure_or_cancellation(
    target: ExperimentStatus,
) -> None:
    """Failure disclosure is valid only on the two error-bearing targets."""
    expected = (
        ExperimentStatus.RUNNING
        if target is ExperimentStatus.FAILED
        else ExperimentStatus.QUEUED
    )
    validate_transition(expected, target, _reason())


def test_reason_is_rejected_for_non_error_target() -> None:
    """A success or ordinary transition must not carry misleading error details."""
    with pytest.raises(ValueError, match="FAILED or CANCELLED"):
        validate_transition(
            ExperimentStatus.RUNNING,
            ExperimentStatus.SUCCEEDED,
            _reason(),
        )


def test_exception_object_cannot_be_used_as_transition_reason() -> None:
    """An exception object must never cross into canonical audit serialization."""
    with pytest.raises(TypeError, match="ErrorDetail"):
        validate_transition(
            ExperimentStatus.RUNNING,
            ExperimentStatus.FAILED,
            RuntimeError("secret stack"),  # type: ignore[arg-type]
        )


def test_transition_times_are_normalized_and_preserve_existing_queue_time() -> None:
    """Lifecycle timestamps must be UTC and an existing queued_at is immutable."""
    east = timezone(timedelta(hours=8))
    existing_queue = datetime(2026, 8, 2, 8, tzinfo=east)
    later = datetime(2026, 8, 2, 9, tzinfo=east)

    queued = transition_timestamps(
        ExperimentStatus.CREATED,
        ExperimentStatus.QUEUED,
        queued_at=existing_queue,
        started_at=None,
        completed_at=None,
        now=later,
    )
    running = transition_timestamps(
        ExperimentStatus.QUEUED,
        ExperimentStatus.RUNNING,
        queued_at=queued.queued_at,
        started_at=None,
        completed_at=None,
        now=later,
    )
    completed = transition_timestamps(
        ExperimentStatus.RUNNING,
        ExperimentStatus.SUCCEEDED,
        queued_at=running.queued_at,
        started_at=running.started_at,
        completed_at=None,
        now=later + timedelta(hours=1),
    )

    assert queued.queued_at == datetime(2026, 8, 2, tzinfo=UTC)
    assert running.started_at == datetime(2026, 8, 2, 1, tzinfo=UTC)
    assert completed.completed_at == datetime(2026, 8, 2, 2, tzinfo=UTC)


def test_running_requires_queued_time_and_terminal_requires_started_time() -> None:
    """Missing predecessor timestamps must fail before persistence."""
    now = datetime(2026, 8, 2, tzinfo=UTC)
    with pytest.raises(ValueError, match="queued_at"):
        transition_timestamps(
            ExperimentStatus.QUEUED,
            ExperimentStatus.RUNNING,
            queued_at=None,
            started_at=None,
            completed_at=None,
            now=now,
        )
    with pytest.raises(ValueError, match="started_at"):
        transition_timestamps(
            ExperimentStatus.RUNNING,
            ExperimentStatus.FAILED,
            queued_at=now,
            started_at=None,
            completed_at=None,
            now=now,
        )


def test_queued_cancellation_sets_completion_without_started_time() -> None:
    """QUEUED to CANCELLED is the sole terminal transition without started_at."""
    now = datetime(2026, 8, 2, tzinfo=UTC)
    result = transition_timestamps(
        ExperimentStatus.QUEUED,
        ExperimentStatus.CANCELLED,
        queued_at=now,
        started_at=None,
        completed_at=None,
        now=now,
    )
    assert result.started_at is None
    assert result.completed_at == now


def test_naive_transition_clock_is_rejected() -> None:
    """A naive clock must not create ambiguous persisted timestamps."""
    with pytest.raises(ValueError, match="timezone-aware"):
        transition_timestamps(
            ExperimentStatus.CREATED,
            ExperimentStatus.QUEUED,
            queued_at=None,
            started_at=None,
            completed_at=None,
            now=datetime(2026, 8, 2, tzinfo=UTC).replace(tzinfo=None),
        )


def test_state_conflict_is_structured_and_non_retryable() -> None:
    """A stale CAS must identify expected, target, and safely observed actual state."""
    conflict = StateConflict(
        "experiment-1",
        ExperimentStatus.QUEUED,
        ExperimentStatus.RUNNING,
        ExperimentStatus.CANCELLED,
    )
    assert conflict.detail.code == "EXPERIMENT_STATE_CONFLICT"
    assert conflict.detail.retryable is False
    assert conflict.detail.context == {
        "experiment_id": "experiment-1",
        "expected": "QUEUED",
        "target": "RUNNING",
        "actual": "CANCELLED",
    }
