"""Behavior tests for explicit structured application logging."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from importlib import import_module
from importlib.util import find_spec
from io import StringIO
from pathlib import Path

import pytest


def _remove_local_timestamp(record: dict[str, object]) -> None:
    timestamp = record.pop("timestamp")
    assert isinstance(timestamp, str)
    parsed = datetime.fromisoformat(timestamp)
    assert parsed.utcoffset() == datetime.now().astimezone().utcoffset()


def test_structured_logging_module_is_available_to_runtime_composition() -> None:
    """Removing the explicit logging module must break runtime composition."""
    assert find_spec("quant_research.logging") is not None


def test_task_log_binding_api_is_removed() -> None:
    """The simplified task log API must not retain capability compatibility."""
    logging_module = import_module("quant_research.logging")

    assert not hasattr(logging_module, "TaskLogBinding")
    assert not hasattr(logging_module, "validate_task_log_binding")


def test_structured_logger_writes_stable_core_fields_and_redacts_context() -> None:
    """Dropping a core field or recursive redaction must break the JSONL contract."""
    logging_module = import_module("quant_research.logging")
    log_context_type = getattr(logging_module, "LogContext", None)
    structured_logger_type = getattr(logging_module, "StructuredLogger", None)
    assert log_context_type is not None
    assert structured_logger_type is not None
    stream = StringIO()
    logger = structured_logger_type(
        stream,
        context=log_context_type(
            request_id="request-7",
            strategy_study_id="study-7",
            task_id="task-7",
            attempt_id="attempt-7",
            worker_id="worker-7",
            stage="VALIDATE",
        ),
        sensitive_values=("sh", "alpha-secret-value"),
    )

    logger.emit(
        "info",
        "experiment.stage",
        context={
            "api_key": "public-key",
            "nested": [
                {"note": "token alpha-secret-value"},
                ValueError("password=alpha-secret-value"),
            ],
            "market": "sh close",
            "environment": {"HOME": "C:/private"},
        },
    )

    record = json.loads(stream.getvalue())
    assert list(record) == [
        "timestamp",
        "level",
        "event",
        "request_id",
        "strategy_study_id",
        "task_id",
        "attempt_id",
        "worker_id",
        "stage",
        "context",
    ]
    _remove_local_timestamp(record)
    assert record == {
        "level": "INFO",
        "event": "experiment.stage",
        "request_id": "request-7",
        "strategy_study_id": "study-7",
        "task_id": "task-7",
        "attempt_id": "attempt-7",
        "worker_id": "worker-7",
        "stage": "VALIDATE",
        "context": {
            "api_key": "[REDACTED]",
            "nested": [
                {"note": "token [REDACTED]"},
                {
                    "exception_type": "ValueError",
                    "message": "password=[REDACTED]",
                },
            ],
            "market": "sh close",
            "environment": "[REDACTED]",
        },
    }
    assert stream.getvalue().endswith("\n")


class _FailingLogStream:
    def write(self, _: str) -> None:
        raise OSError("authorization=do-not-leak")

    def flush(self) -> None:
        raise AssertionError("flush must not follow a failed write")


def test_structured_logger_ignores_sink_write_and_flush_failures() -> None:
    """Diagnostic sink failures must not replace the business result."""
    logging_module = import_module("quant_research.logging")
    logger = logging_module.StructuredLogger(_FailingLogStream())

    logger.emit("error", "task.failed")
    logger.flush()

    with pytest.raises(ValueError, match="level must not be empty"):
        logger.emit("", "task.failed")


def test_redaction_canonicalizes_hyphenated_secret_keys() -> None:
    """HTTP-style key spelling must not bypass recursive or environment redaction."""
    logging_module = import_module("quant_research.logging")
    secret = "hyphenated-secret-value"

    redacted = logging_module.redact_context(
        {
            "nested": {
                "X-API-Key": secret,
                "Api-Key": secret,
                "public-key": "visible",
            }
        }
    )

    assert redacted == {
        "nested": {
            "X-API-Key": "[REDACTED]",
            "Api-Key": "[REDACTED]",
            "public-key": "visible",
        }
    }
    assert logging_module.sensitive_environment_values(
        {
            "SERVICE-API-KEY": secret,
            "PUBLIC-MARKET": "visible-market-value",
        }
    ) == (secret,)


def test_task_log_manager_binds_diagnostics_to_controlled_artifact_paths(
    tmp_path: Path,
) -> None:
    """Task and staging paths must remain derived from their trusted roots."""
    logging_module = import_module("quant_research.logging")
    manager_type = getattr(logging_module, "TaskLogManager", None)
    assert manager_type is not None
    artifact_root = tmp_path / "artifacts"
    diagnostic_root = tmp_path / "state" / "task-logs"
    manager = manager_type(
        diagnostic_root=diagnostic_root,
        artifact_root=artifact_root,
    )
    context = logging_module.LogContext(
        request_id="request-7",
        strategy_study_id="00000000-0000-0000-0000-000000000701",
        task_id="00000000-0000-0000-0000-000000000702",
        attempt_id="00000000-0000-0000-0000-000000000703",
        worker_id="worker-7",
        stage="VALIDATE",
    )
    expected_diagnostic = (
        diagnostic_root
        / "task_id=00000000-0000-0000-0000-000000000702"
        / "attempt_id=00000000-0000-0000-0000-000000000703"
        / "run.log"
    )

    with manager.open(context) as session:
        assert session.path == expected_diagnostic
        assert not hasattr(session, "binding")
        session.logger.emit("info", "stage.started")
        outside = tmp_path / "caller-selected"
        outside.mkdir()
        with pytest.raises(ValueError, match="controlled task staging"):
            manager.materialize(context, outside)
        staging = artifact_root / ".task-staging" / ".staging-study-7"
        staging.mkdir(parents=True)
        materialized = manager.materialize(context, staging)

    assert manager.diagnostic_path(context) == expected_diagnostic
    assert materialized == staging / "run.log"
    assert materialized.read_bytes() == expected_diagnostic.read_bytes()
    record = json.loads(materialized.read_text(encoding="utf-8"))
    _remove_local_timestamp(record)
    assert record == {
        "level": "INFO",
        "event": "stage.started",
        "request_id": "request-7",
        "strategy_study_id": "00000000-0000-0000-0000-000000000701",
        "task_id": "00000000-0000-0000-0000-000000000702",
        "attempt_id": "00000000-0000-0000-0000-000000000703",
        "worker_id": "worker-7",
        "stage": "VALIDATE",
    }
    with pytest.raises(ValueError, match="task_id must be a canonical UUID or ULID"):
        manager.open(
            logging_module.LogContext(
                task_id="../escape",
                attempt_id="00000000-0000-0000-0000-000000000704",
            )
        )
    assert not (tmp_path / "escape").exists()


def test_task_log_manager_accepts_strategy_study_task_ulid(tmp_path: Path) -> None:
    """策略研究任务的 ULID 必须安全映射到受控诊断路径。"""
    logging_module = import_module("quant_research.logging")
    diagnostic_root = tmp_path / "state" / "task-logs"
    manager = logging_module.TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=tmp_path / "artifacts",
    )
    context = logging_module.LogContext(
        strategy_study_id="01M0M25S92XABB0B3347ST5KWV",
        task_id="01M0M25S924HFM59DP2CB9WK70",
        attempt_id="00000000-0000-0000-0000-000000000703",
        worker_id="worker-7",
    )

    with manager.open(context) as session:
        session.logger.emit("INFO", "task.claimed")

    assert manager.diagnostic_path(context) == (
        diagnostic_root
        / "task_id=01M0M25S924HFM59DP2CB9WK70"
        / "attempt_id=00000000-0000-0000-0000-000000000703"
        / "run.log"
    )


def test_structured_logger_omits_unused_optional_envelope_fields() -> None:
    logging_module = import_module("quant_research.logging")
    stream = StringIO()
    logger = logging_module.StructuredLogger(stream)

    logger.emit("info", "curate.started", stage="CURATE", context={"dataset": "x"})

    record = json.loads(stream.getvalue())
    _remove_local_timestamp(record)
    assert record == {
        "level": "INFO",
        "event": "curate.started",
        "stage": "CURATE",
        "context": {"dataset": "x"},
    }


def test_task_log_manager_rejects_an_ordinary_final_symlink(
    tmp_path: Path,
) -> None:
    """Following a final symlink must detach the stream from its derived path."""
    logging_module = import_module("quant_research.logging")
    diagnostic_root = tmp_path / "state" / "task-logs"
    manager = logging_module.TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=tmp_path / "artifacts",
    )
    context = logging_module.LogContext(
        task_id="00000000-0000-0000-0000-000000000712",
        attempt_id="00000000-0000-0000-0000-000000000713",
        worker_id="worker-7",
    )
    target = diagnostic_root / "ordinary-target.log"
    target.parent.mkdir(parents=True)
    target.write_text("ordinary\n", encoding="utf-8")
    final_path = manager.diagnostic_path(context)
    final_path.parent.mkdir(parents=True)
    try:
        final_path.symlink_to(target)
    except OSError as error:
        pytest.skip(
            f"ordinary file symlink capability unavailable: {type(error).__name__}"
        )

    with pytest.raises(ValueError, match="reparse point"):
        manager.open(context)


def test_task_log_materialization_requires_an_active_session(
    tmp_path: Path,
) -> None:
    """A closed session must not be reopened implicitly during materialization."""
    logging_module = import_module("quant_research.logging")
    artifact_root = tmp_path / "artifacts"
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=artifact_root,
    )
    context = logging_module.LogContext(
        task_id="00000000-0000-0000-0000-000000000722",
        attempt_id="00000000-0000-0000-0000-000000000723",
        worker_id="worker-7",
    )
    with manager.open(context) as session:
        session.logger.emit("info", "task.claimed")
    staging = artifact_root / ".task-staging" / ".staging-study-7"
    staging.mkdir(parents=True)

    with pytest.raises(ValueError, match="active session"):
        manager.materialize(context, staging)


def test_task_log_writer_treats_the_explicit_byte_limit_as_best_effort(
    tmp_path: Path,
) -> None:
    """A diagnostic size failure must not replace the business result."""
    logging_module = import_module("quant_research.logging")
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=tmp_path / "artifacts",
        max_log_bytes=256,
    )
    context = logging_module.LogContext(
        task_id="00000000-0000-0000-0000-000000000732",
        attempt_id="00000000-0000-0000-0000-000000000733",
        worker_id="worker-7",
    )

    with manager.open(context) as session:
        session.logger.emit("info", "oversized", context={"note": "x" * 512})

    assert manager.diagnostic_path(context).read_bytes() == b""


class _RecordingStream:
    def __init__(self) -> None:
        self.records: list[str] = []
        self.flushed = 0

    def write(self, value: str) -> object:
        self.records.append(value)
        return len(value)

    def flush(self) -> None:
        self.flushed += 1


class _FailingSecondaryStream:
    def write(self, _value: str) -> None:
        raise OSError("terminal unavailable")

    def flush(self) -> None:
        raise AssertionError("flush must not run after a failed secondary write")


def test_tee_log_stream_writes_both_streams_and_ignores_secondary_failure() -> None:
    """A closed terminal must not block the durable pipeline log."""
    logging_module = import_module("quant_research.logging")
    tee_type = getattr(logging_module, "TeeLogStream", None)
    assert tee_type is not None
    primary = _RecordingStream()
    logger = logging_module.StructuredLogger(
        tee_type(primary, _FailingSecondaryStream())
    )

    logger.emit("info", "pipeline.probe", context={"request": {"date": "2026-01-05"}})

    assert len(primary.records) == 1
    assert primary.flushed == 0
    record = json.loads(primary.records[0])
    assert record["event"] == "pipeline.probe"
    assert record["context"]["request"] == {"date": "2026-01-05"}


def test_tee_log_stream_primary_failure_does_not_block_secondary() -> None:
    """Either pipeline sink may fail without blocking the other sink."""
    logging_module = import_module("quant_research.logging")
    tee_type = getattr(logging_module, "TeeLogStream", None)
    assert tee_type is not None
    secondary = _RecordingStream()
    logger = logging_module.StructuredLogger(tee_type(_FailingLogStream(), secondary))

    logger.emit("info", "pipeline.probe")

    assert len(secondary.records) == 1
    assert secondary.flushed == 0


def test_emit_buffers_both_tee_sinks_until_explicit_flush() -> None:
    """Per-record writes must not flush either the pipeline file or terminal."""
    logging_module = import_module("quant_research.logging")
    primary = _RecordingStream()
    secondary = _RecordingStream()
    logger = logging_module.StructuredLogger(
        logging_module.TeeLogStream(primary, secondary)
    )

    logger.emit("info", "pipeline.first")
    logger.emit("info", "pipeline.second")

    assert primary.flushed == 0
    assert secondary.flushed == 0
    logger.flush()
    assert primary.flushed == 1
    assert secondary.flushed == 1


def test_concurrent_emit_keeps_complete_json_lines_without_flushing() -> None:
    """The logger lock must preserve whole JSONL records under concurrency."""
    logging_module = import_module("quant_research.logging")
    stream = _RecordingStream()
    logger = logging_module.StructuredLogger(stream)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda index: logger.emit(
                    "info", "concurrent.record", context={"index": index}
                ),
                range(100),
            )
        )

    assert stream.flushed == 0
    assert len(stream.records) == 100
    records = [json.loads(line) for line in stream.records]
    assert {record["context"]["index"] for record in records} == set(range(100))
    assert all(line.endswith("\n") for line in stream.records)


def test_task_log_flush_materialize_and_close_are_durability_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Task records stay buffered until an explicit durable lifecycle boundary."""
    logging_module = import_module("quant_research.logging")
    fsync_descriptors: list[int] = []
    monkeypatch.setattr(
        logging_module.os,
        "fsync",
        lambda descriptor: fsync_descriptors.append(descriptor),
    )
    artifact_root = tmp_path / "artifacts"
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=artifact_root,
    )
    context = logging_module.LogContext(
        task_id="00000000-0000-0000-0000-000000000742",
        attempt_id="00000000-0000-0000-0000-000000000743",
        worker_id="worker-7",
    )
    staging = artifact_root / ".task-staging" / ".staging-study-7"
    staging.mkdir(parents=True)

    with manager.open(context) as session:
        session.logger.emit("info", "task.first")
        session.logger.emit("info", "task.second")
        assert fsync_descriptors == []
        session.flush()
        assert len(fsync_descriptors) == 1
        manager.materialize(context, staging)
        assert len(fsync_descriptors) == 3

    assert len(fsync_descriptors) == 4


def test_task_log_flush_and_close_ignore_io_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Flush, fsync, and close failures remain diagnostic-only failures."""
    logging_module = import_module("quant_research.logging")
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=tmp_path / "artifacts",
    )
    context = logging_module.LogContext(
        task_id="00000000-0000-0000-0000-000000000752",
        attempt_id="00000000-0000-0000-0000-000000000753",
        worker_id="worker-7",
    )
    session = manager.open(context)
    session.logger.emit("info", "task.first")
    monkeypatch.setattr(
        logging_module.os,
        "fsync",
        lambda _descriptor: (_ for _ in ()).throw(OSError("fsync unavailable")),
    )

    session.flush()
    monkeypatch.setattr(logging_module.os, "fsync", lambda _descriptor: None)
    stream_type = type(session._stream)
    original_close = stream_type.close

    def close_then_fail(stream: object) -> None:
        original_close(stream)
        raise OSError("close unavailable")

    monkeypatch.setattr(stream_type, "close", close_then_fail)
    session.close()

    with pytest.raises(ValueError, match="task log session is closed"):
        session.flush()


def test_unavailable_task_log_materializes_a_safe_placeholder(tmp_path: Path) -> None:
    """A required success artifact must retain identities without raw I/O errors."""
    logging_module = import_module("quant_research.logging")
    artifact_root = tmp_path / "artifacts"
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=artifact_root,
    )
    context = logging_module.LogContext(
        request_id="00000000-0000-0000-0000-000000000763",
        strategy_study_id="00000000-0000-0000-0000-000000000761",
        task_id="00000000-0000-0000-0000-000000000762",
        attempt_id="00000000-0000-0000-0000-000000000763",
        worker_id="worker-7",
    )
    staging = artifact_root / ".task-staging" / ".staging-study-7"
    staging.mkdir(parents=True)

    path = manager.materialize_unavailable(
        context,
        staging,
        stage="ARTIFACT_VERIFY",
    )

    record = json.loads(path.read_text(encoding="utf-8"))
    _remove_local_timestamp(record)
    assert record == {
        "level": "WARNING",
        "event": "task.log_unavailable",
        "request_id": "00000000-0000-0000-0000-000000000763",
        "strategy_study_id": "00000000-0000-0000-0000-000000000761",
        "task_id": "00000000-0000-0000-0000-000000000762",
        "attempt_id": "00000000-0000-0000-0000-000000000763",
        "worker_id": "worker-7",
        "stage": "ARTIFACT_VERIFY",
        "context": {"reason": "diagnostic_log_unavailable"},
    }


def test_materialize_falls_back_when_all_task_log_writes_fail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An unusable diagnostic payload must degrade to the safe placeholder."""
    logging_module = import_module("quant_research.logging")
    artifact_root = tmp_path / "artifacts"
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=artifact_root,
    )
    context = logging_module.LogContext(
        strategy_study_id="00000000-0000-0000-0000-000000000781",
        task_id="00000000-0000-0000-0000-000000000782",
        attempt_id="00000000-0000-0000-0000-000000000783",
        worker_id="worker-7",
    )
    staging = artifact_root / ".task-staging" / ".staging-study-7"
    staging.mkdir(parents=True)

    with manager.open(context) as session:
        monkeypatch.setattr(
            session._stream,
            "write",
            lambda _value: (_ for _ in ()).throw(OSError("write unavailable")),
        )
        session.logger.emit("info", "task.claimed")
        path = manager.materialize(context, staging)

    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["event"] == "task.log_unavailable"
    assert record["task_id"] == context.task_id


def test_materialize_does_not_hide_task_log_correlation_conflicts(
    tmp_path: Path,
) -> None:
    """Best-effort fallback must not weaken task and attempt correlation checks."""
    logging_module = import_module("quant_research.logging")
    artifact_root = tmp_path / "artifacts"
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "state" / "task-logs",
        artifact_root=artifact_root,
    )
    context = logging_module.LogContext(
        task_id="00000000-0000-0000-0000-000000000792",
        attempt_id="00000000-0000-0000-0000-000000000793",
        worker_id="worker-7",
    )
    staging = artifact_root / ".task-staging" / ".staging-study-7"
    staging.mkdir(parents=True)

    with manager.open(context) as session:
        raw_stream = session._stream._stream
        raw_stream.write(
            json.dumps(
                {
                    "timestamp": datetime.now().astimezone().isoformat(),
                    "level": "INFO",
                    "event": "task.claimed",
                    "task_id": "00000000-0000-0000-0000-000000000799",
                    "attempt_id": context.attempt_id,
                    "worker_id": context.worker_id,
                },
                separators=(",", ":"),
            )
            + "\n"
        )

        with pytest.raises(ValueError, match="correlation fields"):
            manager.materialize(context, staging)

    assert not (staging / "run.log").exists()
