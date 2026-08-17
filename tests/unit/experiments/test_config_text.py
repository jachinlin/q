"""实验 YAML 文本入口的严格解析测试。"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_research.application.experiments import ExperimentClient
from quant_research.backtest.engine import StrategyRef
from quant_research.domain.enums import DatasetKind
from quant_research.experiments.config import (
    ExperimentConfigError,
    resolve_experiment_yaml_text,
)
from quant_research.experiments.query import ExperimentQuery
from quant_research.experiments.registry import ExperimentRegistry
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DataCatalogState,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.tasks.models import TaskStatus

_HASH = "a" * 64
_VALID_YAML = """strategy_id: etf_rotation
start_date: 2024-01-02
end_date: 2024-12-31
benchmark: 000300.SH
initial_cash_fen: 100000000
strategy_config: {}
"""


class _Catalog:
    """为配置解析测试提供固定的已验证日线目录。"""

    def require_validated_catalog(self) -> DataCatalogState:
        """返回当前已验证目录身份。"""
        return DataCatalogState(
            catalog_hash=_HASH,
            validated_catalog_hash=_HASH,
            quality_run_id=None,
            updated_at=datetime(2026, 8, 15, tzinfo=UTC),
            validated_at=datetime(2026, 8, 15, tzinfo=UTC),
        )

    def get_canonical_dataset(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        """返回覆盖 2024 年的日线或行业记录。"""
        assert dataset in {
            DatasetKind.DAILY_BAR,
            DatasetKind.INDUSTRY_CLASSIFICATION,
        }
        return CanonicalDatasetRecord(
            dataset=dataset,
            content_hash=_HASH,
            source="test",
            partitions=(),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            updated_at=datetime(2026, 8, 15, tzinfo=UTC),
        )


class _RuleBook:
    """提供配置身份所需的固定规则哈希。"""

    content_hash = "b" * 64


def test_yaml_text_uses_the_same_strict_resolution_contract() -> None:
    resolved = resolve_experiment_yaml_text(
        _VALID_YAML,
        catalog=_Catalog(),
        strategies={StrategyRef("etf_rotation"): object()},
        rulebook=_RuleBook(),  # type: ignore[arg-type]
    )

    assert resolved.data_hash == _HASH
    assert resolved.mapping["strategy_id"] == "etf_rotation"
    assert resolved.mapping["execution"] == {
        "max_volume_participation": 0.1,
        "reference_price": "OPEN",
        "slippage_bps": 5.0,
    }


def test_yaml_text_records_explicit_industry_dependency_in_config_identity() -> None:
    resolved = resolve_experiment_yaml_text(
        _VALID_YAML
        + "industry:\n"
        + "  taxonomy: 证监会行业分类\n"
        + "  unclassified_policy: EXCLUDE\n",
        catalog=_Catalog(),
        strategies={StrategyRef("etf_rotation"): object()},
        rulebook=_RuleBook(),  # type: ignore[arg-type]
    )

    assert resolved.mapping["industry"] == {
        "dataset": "industry_classification",
        "taxonomy": "证监会行业分类",
        "unclassified_policy": "EXCLUDE",
        "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
    }


@pytest.mark.parametrize(
    "config_yaml",
    (
        "!!python/object/apply:os.system ['whoami']",
        _VALID_YAML + "unknown_field: true\n",
        _VALID_YAML.replace("2024-12-31", "2025-01-01"),
    ),
)
def test_yaml_text_rejects_unsafe_or_invalid_documents(config_yaml: str) -> None:
    with pytest.raises(ExperimentConfigError):
        resolve_experiment_yaml_text(
            config_yaml,
            catalog=_Catalog(),
            strategies={StrategyRef("etf_rotation"): object()},
            rulebook=_RuleBook(),  # type: ignore[arg-type]
        )


def test_yaml_text_rejects_more_than_one_mibibyte() -> None:
    with pytest.raises(ExperimentConfigError, match="size limit"):
        resolve_experiment_yaml_text(
            "a" * 1_048_577,
            catalog=_Catalog(),
            strategies={StrategyRef("etf_rotation"): object()},
            rulebook=_RuleBook(),  # type: ignore[arg-type]
        )


def test_yaml_text_submission_atomically_creates_experiment_and_task(
    tmp_path: Path,
) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    query = ExperimentQuery(engine)
    queue = TaskQueue(engine)
    client = ExperimentClient(
        registry=ExperimentRegistry(engine),
        query=query,
        queue=queue,
        config_root=tmp_path,
        catalog=_Catalog(),
        strategies={StrategyRef("etf_rotation"): object()},
        rulebook=_RuleBook(),  # type: ignore[arg-type]
        environment_factory=lambda: {
            "source_identity_mode": "TREE",
            "source_hash": "c" * 64,
            "git_commit": None,
            "source_tree_hash": "c" * 64,
            "working_tree_dirty": False,
            "lockfile_path": "uv.lock",
            "lockfile_hash": "d" * 64,
            "python_version": "3.12.0",
        },
        clock=lambda: datetime(2026, 8, 15, tzinfo=UTC),
    )

    experiment, task = client.create_and_submit_from_yaml_text(
        _VALID_YAML,
        request_id="dashboard-request",
    )

    assert experiment.status.value == "QUEUED"
    assert task.status is TaskStatus.QUEUED
    assert task.experiment_id == experiment.id
    assert query.get(experiment.id).record.data_hash == _HASH
    engine.dispose()
