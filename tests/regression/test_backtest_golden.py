"""Golden artifact regression for a fixed deterministic daily backtest."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quant_core.backtest.artifacts as artifacts_module
from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.artifacts import (
    ArtifactEntry,
    BacktestArtifactWriter,
    ManifestContext,
    WriterState,
)
from quant_core.backtest.engine import (
    BacktestEngine,
    BacktestResult,
    StrategyRef,
)
from quant_core.backtest.models import (
    ExecutionBatch,
    ExecutionReason,
    FillResult,
    RejectResult,
)
from quant_core.backtest.rulebook import FeeBreakdown
from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio import (
    OrderIntent,
    OrderSide,
    RebalancePlanner,
    TargetPortfolio,
    TargetPosition,
)
from tests.integration.test_backtest_timeline import (
    _Data,
    _NeverCancelled,
    _Progress,
    _request,
    _RuleBook,
    _Targets,
)


def _context(experiment_id: UUID, *, one_day: bool = False) -> ManifestContext:
    request = _request()
    return ManifestContext(
        experiment_id,
        request.snapshot_id,
        request.strategy.strategy_id,
        request.strategy.version,
        request.start_date,
        request.start_date if one_day else request.end_date,
        request.benchmark,
        request.initial_cash_fen,
        request.rulebook_version,
        request.execution_config,
    )


def test_manifest_hashes_describe_closed_deterministic_artifacts(
    tmp_path: Path,
) -> None:
    result = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    ).run(_request(), _Progress(), _NeverCancelled())

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    artifacts = manifest["artifacts"]
    assert sorted(artifacts) == [
        "costs.parquet",
        "fills.parquet",
        "holdings.parquet",
        "nav.parquet",
        "targets.parquet",
    ]
    for name, entry in artifacts.items():
        artifact = result.artifact_dir / name
        assert entry["path"] == name
        assert entry["size_bytes"] == artifact.stat().st_size
        assert entry["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert pq.read_table(result.artifact_dir / "nav.parquet").num_rows == 3
    assert manifest["completed_sessions"] == 3
    assert pq.read_table(result.artifact_dir / "nav.parquet").to_pylist() == [
        {
            "trade_date": date(2024, 1, 5),
            "cash_fen": 200_000,
            "market_value_fen": 0,
            "nav_fen": 200_000,
            "benchmark_close": 3.0,
        },
        {
            "trade_date": date(2024, 1, 8),
            "cash_fen": 79_900,
            "market_value_fen": 120_000,
            "nav_fen": 199_900,
            "benchmark_close": 3.0,
        },
        {
            "trade_date": date(2024, 1, 9),
            "cash_fen": 189_800,
            "market_value_fen": 0,
            "nav_fen": 189_800,
            "benchmark_close": 3.0,
        },
    ]
    assert pq.read_table(result.artifact_dir / "holdings.parquet").to_pylist() == [
        {
            "trade_date": date(2024, 1, 8),
            "instrument_id": "SSE:600001",
            "total_quantity": 100,
            "sellable_quantity": 0,
            "cost_basis_fen": 120_100,
            "market_value_fen": 120_000,
        }
    ]
    assert pq.read_table(result.artifact_dir / "targets.parquet").to_pylist() == [
        {
            "signal_date": date(2024, 1, 5),
            "execute_date": date(2024, 1, 8),
            "position_index": 0,
            "instrument_id": "SSE:600001",
            "target_weight": 1.0,
            "score": 2.0,
            "reason_code": "TEST",
            "cash_weight": 0.0,
        },
        {
            "signal_date": date(2024, 1, 5),
            "execute_date": date(2024, 1, 8),
            "position_index": 1,
            "instrument_id": None,
            "target_weight": 0.0,
            "score": None,
            "reason_code": "CASH",
            "cash_weight": 0.0,
        },
        {
            "signal_date": date(2024, 1, 8),
            "execute_date": date(2024, 1, 9),
            "position_index": 0,
            "instrument_id": None,
            "target_weight": 1.0,
            "score": None,
            "reason_code": "CASH",
            "cash_weight": 1.0,
        },
    ]
    assert pq.read_table(result.artifact_dir / "fills.parquet").to_pylist() == [
        {
            "trade_date": date(2024, 1, 8),
            "result_index": 0,
            "instrument_id": "SSE:600001",
            "side": "BUY",
            "requested_quantity": 100,
            "filled_quantity": 100,
            "unfilled_quantity": 0,
            "price": 12.0,
            "gross_value_fen": 120_000,
            "reason_code": "FILLED",
            "detail": None,
        },
        {
            "trade_date": date(2024, 1, 9),
            "result_index": 0,
            "instrument_id": "SSE:600001",
            "side": "SELL",
            "requested_quantity": 100,
            "filled_quantity": 100,
            "unfilled_quantity": 0,
            "price": 11.0,
            "gross_value_fen": 110_000,
            "reason_code": "FILLED",
            "detail": None,
        },
    ]
    assert pq.read_table(result.artifact_dir / "costs.parquet").to_pylist() == [
        {
            "trade_date": date(2024, 1, 8),
            "result_index": 0,
            "instrument_id": "SSE:600001",
            "commission_fen": 100,
            "stamp_tax_fen": 0,
            "transfer_fee_fen": 0,
            "total_fees_fen": 100,
        },
        {
            "trade_date": date(2024, 1, 9),
            "result_index": 0,
            "instrument_id": "SSE:600001",
            "commission_fen": 100,
            "stamp_tax_fen": 0,
            "transfer_fee_fen": 0,
            "total_fees_fen": 100,
        },
    ]
    assert {name: entry["row_count"] for name, entry in artifacts.items()} == {
        "nav.parquet": 3,
        "holdings.parquet": 1,
        "targets.parquet": 3,
        "fills.parquet": 2,
        "costs.parquet": 2,
    }


def test_artifact_validation_rejects_a_tampered_nav_identity(tmp_path: Path) -> None:
    writer = BacktestArtifactWriter(
        tmp_path, UUID("00000000-0000-0000-0000-000000000003")
    )
    writer.append_snapshot(AccountSnapshot(_request().start_date, 100, (), 0, 100), 1.0)
    writer.close()
    schema = pq.read_schema(writer.staging_dir / "nav.parquet")
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "trade_date": _request().start_date,
                    "cash_fen": 100,
                    "market_value_fen": 0,
                    "nav_fen": 99,
                    "benchmark_close": 1.0,
                }
            ],
            schema=schema,
        ),
        writer.staging_dir / "nav.parquet",
    )

    with pytest.raises(ValueError, match="nav"):
        writer.validate(
            (_request().start_date,),
            _context(UUID("00000000-0000-0000-0000-000000000003"), one_day=True),
        )


def test_artifact_writer_rejects_publish_before_validation(tmp_path: Path) -> None:
    writer = BacktestArtifactWriter(
        tmp_path, UUID("00000000-0000-0000-0000-000000000004")
    )
    writer.close()

    with pytest.raises(ValueError, match="VALIDATED"):
        writer.publish()


def test_artifact_writer_rejects_manifest_entry_that_differs_from_validation(
    tmp_path: Path,
) -> None:
    writer = BacktestArtifactWriter(
        tmp_path, UUID("00000000-0000-0000-0000-000000000005")
    )
    writer.append_snapshot(AccountSnapshot(_request().start_date, 100, (), 0, 100), 1.0)
    writer.close()
    writer.validate(
        (_request().start_date,),
        _context(UUID("00000000-0000-0000-0000-000000000005"), one_day=True),
    )

    with pytest.raises(ValueError):
        (writer.staging_dir / "nav.parquet").write_bytes(b"tampered")
        writer.publish()


def test_partial_writer_construction_closes_created_writers_and_diagnoses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[object] = []

    class ObservedWriter:
        closed = False

        def close(self) -> None:
            self.closed = True

    def fail_second_writer(*args: object, **kwargs: object) -> ObservedWriter:
        del args, kwargs
        if len(created) == 1:
            raise OSError("second writer failure")
        writer = ObservedWriter()
        created.append(writer)
        return writer

    monkeypatch.setattr(artifacts_module.pq, "ParquetWriter", fail_second_writer)

    with pytest.raises(OSError, match="second writer failure"):
        BacktestArtifactWriter(tmp_path, UUID("00000000-0000-0000-0000-000000000006"))

    assert all(writer.closed for writer in created)
    assert list(tmp_path.glob(".staging-*/diagnostic.json"))


def test_validation_accepts_a_finite_negative_target_score(tmp_path: Path) -> None:
    experiment = UUID("00000000-0000-0000-0000-000000000008")
    writer = BacktestArtifactWriter(tmp_path, experiment)
    writer.append_snapshot(AccountSnapshot(_request().start_date, 100, (), 0, 100), 1.0)
    writer.append_target(
        TargetPortfolio(
            _request().start_date,
            date(2024, 1, 8),
            (TargetPosition(InstrumentId.parse("SSE:600001"), 1.0, -3.5, "TEST"),),
            0.0,
        )
    )
    writer.close()

    entries = writer.validate(
        (_request().start_date,), _context(experiment, one_day=True)
    )

    assert entries["targets.parquet"].row_count == 2


@pytest.mark.parametrize(
    "fills,costs,targets",
    [
        (
            [
                {
                    "trade_date": date(2024, 1, 5),
                    "result_index": 0,
                    "instrument_id": "SSE:600001",
                    "price": None,
                    "requested_quantity": 0,
                    "filled_quantity": 0,
                    "unfilled_quantity": 0,
                    "gross_value_fen": 0,
                }
            ],
            [],
            [],
        ),
        (
            [
                {
                    "trade_date": date(2024, 1, 5),
                    "result_index": 0,
                    "instrument_id": "SSE:600001",
                    "price": 1.0,
                    "requested_quantity": 0,
                    "filled_quantity": 1,
                    "unfilled_quantity": -1,
                    "gross_value_fen": 100,
                }
            ],
            [
                {
                    "trade_date": date(2024, 1, 5),
                    "result_index": 0,
                    "instrument_id": "SSE:600001",
                    "commission_fen": 0,
                    "stamp_tax_fen": 0,
                    "transfer_fee_fen": 0,
                    "total_fees_fen": 0,
                }
            ],
            [],
        ),
        (
            [
                {
                    "trade_date": date(2024, 1, 5),
                    "result_index": 0,
                    "instrument_id": "SSE:600001",
                    "price": 1.0,
                    "requested_quantity": 1,
                    "filled_quantity": 1,
                    "unfilled_quantity": 0,
                    "gross_value_fen": 100,
                }
            ],
            [
                {
                    "trade_date": date(2024, 1, 5),
                    "result_index": 0,
                    "instrument_id": "SSE:600002",
                    "commission_fen": 0,
                    "stamp_tax_fen": 0,
                    "transfer_fee_fen": 0,
                    "total_fees_fen": 0,
                }
            ],
            [],
        ),
        (
            [],
            [],
            [
                {
                    "signal_date": date(2024, 1, 5),
                    "execute_date": date(2024, 1, 8),
                    "position_index": 0,
                    "instrument_id": None,
                    "target_weight": 1.0,
                    "score": None,
                    "reason_code": "CASH",
                    "cash_weight": 1.0,
                },
                {
                    "signal_date": date(2024, 1, 5),
                    "execute_date": date(2024, 1, 8),
                    "position_index": 0,
                    "instrument_id": "SSE:600001",
                    "target_weight": 0.0,
                    "score": 0.0,
                    "reason_code": "TEST",
                    "cash_weight": 1.0,
                },
            ],
        ),
    ],
)
def test_parquet_row_validator_rejects_execution_cost_and_cash_tampering(
    fills: list[dict[str, object]],
    costs: list[dict[str, object]],
    targets: list[dict[str, object]],
) -> None:
    if targets:
        with pytest.raises((TypeError, ValueError)):
            artifacts_module._validate_targets(targets)
    else:
        with pytest.raises((TypeError, ValueError)):
            artifacts_module._validate_execution(fills, costs)


def test_artifact_execution_roundtrip_records_reject_and_partial_fill(
    tmp_path: Path,
) -> None:
    experiment = UUID("00000000-0000-0000-0000-000000000011")
    instrument = InstrumentId.parse("SSE:600001")
    reject = RejectResult(
        OrderIntent(instrument, OrderSide.BUY, 100, "TEST"),
        date(2024, 1, 5),
        100,
        ExecutionReason.VOLUME_CAP,
    )
    partial = FillResult(
        OrderIntent(instrument, OrderSide.BUY, 100, "TEST"),
        date(2024, 1, 5),
        100,
        50,
        50,
        10.0,
        5_000,
        FeeBreakdown(3, 0, 0, 3),
        ExecutionReason.VOLUME_CAP,
    )
    writer = BacktestArtifactWriter(tmp_path, experiment)
    writer.append_snapshot(AccountSnapshot(date(2024, 1, 5), 100, (), 0, 100), 1.0)
    writer.append_execution(ExecutionBatch(date(2024, 1, 5), (reject, partial), 100))
    writer.close()
    writer.validate((date(2024, 1, 5),), _context(experiment, one_day=True))
    path = writer.publish()
    fills = pq.read_table(path.parent / "fills.parquet").to_pylist()
    costs = pq.read_table(path.parent / "costs.parquet").to_pylist()
    assert [
        (
            row["result_index"],
            row["requested_quantity"],
            row["filled_quantity"],
            row["unfilled_quantity"],
            row["reason_code"],
        )
        for row in fills
    ] == [(0, 100, 0, 100, "VOLUME_CAP"), (1, 100, 50, 50, "VOLUME_CAP")]
    assert costs == [
        {
            "trade_date": date(2024, 1, 5),
            "result_index": 1,
            "instrument_id": "SSE:600001",
            "commission_fen": 3,
            "stamp_tax_fen": 0,
            "transfer_fee_fen": 0,
            "total_fees_fen": 3,
        }
    ]


@pytest.mark.parametrize(
    "name",
    [
        "nav.parquet",
        "holdings.parquet",
        "targets.parquet",
        "fills.parquet",
        "costs.parquet",
    ],
)
def test_validate_rejects_each_missing_required_artifact(
    tmp_path: Path, name: str
) -> None:
    experiment = UUID("00000000-0000-0000-0000-000000000012")
    writer = BacktestArtifactWriter(tmp_path, experiment)
    writer.append_snapshot(AccountSnapshot(date(2024, 1, 5), 100, (), 0, 100), 1.0)
    writer.close()
    (writer.staging_dir / name).unlink()
    with pytest.raises(ValueError, match="missing artifact"):
        writer.validate((date(2024, 1, 5),), _context(experiment, one_day=True))
    assert writer.state.value == "CLOSED"


def test_empty_artifacts_keep_hard_coded_arrow_schemas(tmp_path: Path) -> None:
    class NoneTargets:
        def generate_target(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    result = BacktestEngine(
        _Data(), NoneTargets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    ).run(_request(), _Progress(), _NeverCancelled())
    expected = {
        "nav.parquet": pa.schema(
            [
                pa.field("trade_date", pa.date32(), nullable=False),
                pa.field("cash_fen", pa.int64(), nullable=False),
                pa.field("market_value_fen", pa.int64(), nullable=False),
                pa.field("nav_fen", pa.int64(), nullable=False),
                pa.field("benchmark_close", pa.float64(), nullable=False),
            ]
        ),
        "holdings.parquet": pa.schema(
            [
                pa.field("trade_date", pa.date32(), nullable=False),
                pa.field("instrument_id", pa.string(), nullable=False),
                pa.field("total_quantity", pa.int64(), nullable=False),
                pa.field("sellable_quantity", pa.int64(), nullable=False),
                pa.field("cost_basis_fen", pa.int64(), nullable=False),
                pa.field("market_value_fen", pa.int64(), nullable=False),
            ]
        ),
        "targets.parquet": pa.schema(
            [
                pa.field("signal_date", pa.date32(), nullable=False),
                pa.field("execute_date", pa.date32(), nullable=False),
                pa.field("position_index", pa.int32(), nullable=False),
                pa.field("instrument_id", pa.string(), nullable=True),
                pa.field("target_weight", pa.float64(), nullable=False),
                pa.field("score", pa.float64(), nullable=True),
                pa.field("reason_code", pa.string(), nullable=False),
                pa.field("cash_weight", pa.float64(), nullable=False),
            ]
        ),
        "fills.parquet": pa.schema(
            [
                pa.field("trade_date", pa.date32(), nullable=False),
                pa.field("result_index", pa.int32(), nullable=False),
                pa.field("instrument_id", pa.string(), nullable=False),
                pa.field("side", pa.string(), nullable=False),
                pa.field("requested_quantity", pa.int64(), nullable=False),
                pa.field("filled_quantity", pa.int64(), nullable=False),
                pa.field("unfilled_quantity", pa.int64(), nullable=False),
                pa.field("price", pa.float64(), nullable=True),
                pa.field("gross_value_fen", pa.int64(), nullable=False),
                pa.field("reason_code", pa.string(), nullable=False),
                pa.field("detail", pa.string(), nullable=True),
            ]
        ),
        "costs.parquet": pa.schema(
            [
                pa.field("trade_date", pa.date32(), nullable=False),
                pa.field("result_index", pa.int32(), nullable=False),
                pa.field("instrument_id", pa.string(), nullable=False),
                pa.field("commission_fen", pa.int64(), nullable=False),
                pa.field("stamp_tax_fen", pa.int64(), nullable=False),
                pa.field("transfer_fee_fen", pa.int64(), nullable=False),
                pa.field("total_fees_fen", pa.int64(), nullable=False),
            ]
        ),
    }
    for name, schema in expected.items():
        table = pq.read_table(result.artifact_dir / name)
        assert pq.read_schema(result.artifact_dir / name) == schema
        if name != "nav.parquet":
            assert table.num_rows == 0


def _valid_result() -> BacktestResult:
    artifact_dir = Path("artifacts")
    return BacktestResult(
        UUID(int=1),
        artifact_dir,
        artifact_dir / "manifest.json",
        1,
        AccountSnapshot(date(2024, 1, 1), 0, (), 0, 0),
    )


@pytest.mark.parametrize(
    ("strategy_id", "version"),
    [
        pytest.param("", "v1", id="empty-strategy-id"),
        pytest.param("strategy", "", id="empty-version"),
        pytest.param(1, "v1", id="non-string-strategy-id"),
    ],
)
def test_strategy_ref_rejects_invalid_direct_construction(
    strategy_id: object, version: object
) -> None:
    with pytest.raises((TypeError, ValueError)):
        StrategyRef(strategy_id, version)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "invalid_request",
    [
        pytest.param(
            lambda: replace(_request(), experiment_id="not-a-uuid"),
            id="experiment-id-not-uuid",
        ),
        pytest.param(
            lambda: replace(_request(), snapshot_id="not-a-uuid"),
            id="snapshot-id-not-uuid",
        ),
        pytest.param(
            lambda: replace(_request(), strategy=object()),
            id="strategy-wrong-type",
        ),
        pytest.param(
            lambda: replace(_request(), start_date=datetime(2024, 1, 1, tzinfo=UTC)),
            id="start-date-datetime",
        ),
        pytest.param(
            lambda: replace(_request(), end_date=datetime(2024, 1, 3, tzinfo=UTC)),
            id="end-date-datetime",
        ),
        pytest.param(
            lambda: replace(
                _request(),
                start_date=date(2024, 1, 3),
                end_date=date(2024, 1, 1),
            ),
            id="start-date-after-end-date",
        ),
        pytest.param(
            lambda: replace(_request(), benchmark="not-an-instrument"),
            id="benchmark-wrong-type",
        ),
        pytest.param(
            lambda: replace(_request(), initial_cash_fen=-1),
            id="negative-initial-cash",
        ),
        pytest.param(
            lambda: replace(_request(), rulebook_version=""),
            id="empty-rulebook-version",
        ),
        pytest.param(
            lambda: replace(_request(), execution_config=object()),
            id="execution-config-wrong-type",
        ),
    ],
)
def test_backtest_request_rejects_invalid_direct_construction(
    invalid_request: object,
) -> None:
    assert callable(invalid_request)
    with pytest.raises((TypeError, ValueError)):
        invalid_request()


@pytest.mark.parametrize(
    "invalid_result",
    [
        pytest.param(
            lambda: BacktestResult(
                "not-a-uuid",
                Path("artifacts"),
                Path("artifacts/manifest.json"),
                1,
                _valid_result().final_snapshot,
            ),
            id="experiment-id-not-uuid",
        ),
        pytest.param(
            lambda: BacktestResult(
                UUID(int=1),
                "artifacts",
                Path("artifacts/manifest.json"),
                1,
                _valid_result().final_snapshot,
            ),
            id="artifact-dir-wrong-type",
        ),
        pytest.param(
            lambda: BacktestResult(
                UUID(int=1),
                Path("artifacts"),
                "artifacts/manifest.json",
                1,
                _valid_result().final_snapshot,
            ),
            id="manifest-path-wrong-type",
        ),
        pytest.param(
            lambda: BacktestResult(
                UUID(int=1),
                Path("artifacts"),
                Path("different/manifest.json"),
                1,
                _valid_result().final_snapshot,
            ),
            id="manifest-path-not-under-artifact-dir",
        ),
        pytest.param(
            lambda: BacktestResult(
                UUID(int=1),
                Path("artifacts"),
                Path("artifacts/manifest.json"),
                0,
                _valid_result().final_snapshot,
            ),
            id="zero-sessions-completed",
        ),
        pytest.param(
            lambda: BacktestResult(
                UUID(int=1),
                Path("artifacts"),
                Path("artifacts/manifest.json"),
                -1,
                _valid_result().final_snapshot,
            ),
            id="negative-sessions-completed",
        ),
        pytest.param(
            lambda: BacktestResult(
                UUID(int=1),
                Path("artifacts"),
                Path("artifacts/manifest.json"),
                1,
                object(),
            ),
            id="final-snapshot-wrong-type",
        ),
    ],
)
def test_backtest_result_rejects_invalid_direct_construction(
    invalid_result: object,
) -> None:
    assert callable(invalid_result)
    with pytest.raises((TypeError, ValueError)):
        invalid_result()


@pytest.mark.parametrize(
    "invalid_entry",
    [
        pytest.param(
            lambda: ArtifactEntry("/absolute.parquet", "schema", 0, 0, "0" * 64),
            id="absolute-path",
        ),
        pytest.param(
            lambda: ArtifactEntry("../escape.parquet", "schema", 0, 0, "0" * 64),
            id="parent-directory-escape",
        ),
        pytest.param(
            lambda: ArtifactEntry("nested/file.parquet", "schema", 0, 0, "0" * 64),
            id="nested-path",
        ),
        pytest.param(
            lambda: ArtifactEntry("data.parquet", "", 0, 0, "0" * 64),
            id="empty-schema",
        ),
        pytest.param(
            lambda: ArtifactEntry("data.parquet", "schema", -1, 0, "0" * 64),
            id="negative-row-count",
        ),
        pytest.param(
            lambda: ArtifactEntry("data.parquet", "schema", 0, -1, "0" * 64),
            id="negative-size-bytes",
        ),
        pytest.param(
            lambda: ArtifactEntry("data.parquet", "schema", 0, 0, "0" * 63),
            id="sha256-not-64-characters",
        ),
        pytest.param(
            lambda: ArtifactEntry("data.parquet", "schema", 0, 0, "A" * 64),
            id="sha256-not-lowercase-hex",
        ),
    ],
)
def test_artifact_entry_rejects_invalid_direct_construction(
    invalid_entry: object,
) -> None:
    assert callable(invalid_entry)
    with pytest.raises((TypeError, ValueError)):
        invalid_entry()


@pytest.mark.parametrize(
    "invalid_context",
    [
        pytest.param(
            lambda: replace(_context(UUID(int=1)), experiment_id="not-a-uuid"),
            id="experiment-id-not-uuid",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), snapshot_id="not-a-uuid"),
            id="snapshot-id-not-uuid",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), strategy_id=""),
            id="empty-strategy-id",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), strategy_version=""),
            id="empty-strategy-version",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), rulebook_version=""),
            id="empty-rulebook-version",
        ),
        pytest.param(
            lambda: replace(
                _context(UUID(int=1)),
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
            ),
            id="start-date-datetime",
        ),
        pytest.param(
            lambda: replace(
                _context(UUID(int=1)),
                end_date=datetime(2024, 1, 3, tzinfo=UTC),
            ),
            id="end-date-datetime",
        ),
        pytest.param(
            lambda: replace(
                _context(UUID(int=1)),
                start_date=date(2024, 1, 3),
                end_date=date(2024, 1, 1),
            ),
            id="start-date-after-end-date",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), benchmark="not-an-instrument"),
            id="benchmark-wrong-type",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), initial_cash_fen=-1),
            id="negative-initial-cash",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), execution_config=object()),
            id="execution-config-wrong-type",
        ),
        pytest.param(
            lambda: replace(_context(UUID(int=1)), schema_version=2),
            id="unsupported-schema-version",
        ),
    ],
)
def test_manifest_context_rejects_invalid_direct_construction(
    invalid_context: object,
) -> None:
    assert callable(invalid_context)
    with pytest.raises((TypeError, ValueError)):
        invalid_context()


def test_writer_state_rejects_unknown_value() -> None:
    with pytest.raises(ValueError):
        WriterState("INVALID")
