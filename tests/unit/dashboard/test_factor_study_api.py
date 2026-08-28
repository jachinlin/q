"""验证独立因子研究 API、矩阵合并、决策与可信产物边界。"""

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from quant_research.dashboard.factor_studies import (
    FactorStudyDashboardService,
    FactorStudyDecisionBody,
    FactorStudyRoutes,
)
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.factor_studies.config import FactorStudyConfigParser
from quant_research.factor_studies.models import (
    FactorDecisionMark,
    FactorStudyArtifactRecord,
    FactorStudyMetricRecord,
    FactorStudyRecord,
    FactorStudyStage,
    FactorStudyStatus,
)


class _Studies:
    """提供 API 单测需要的应用服务替身。"""

    def __init__(self, record: FactorStudyRecord) -> None:
        self.record = record
        self.list_filter: tuple[FactorStudyStatus | None, FactorDecisionMark | None] | None = None
        self.decision: tuple[object, FactorDecisionMark, str, str] | None = None

    def catalog(self) -> tuple[str, ...]:
        """返回固定因子目录。"""
        return ("book_to_price_mrq", "momentum_120_20")

    def validate(self, text: str):
        """使用真实严格解析器校验 YAML。"""
        return FactorStudyConfigParser().parse(text)

    def submit(self, text: str, *, actor: str) -> FactorStudyRecord:
        """记录提交边界并返回固定研究。"""
        self.validate(text)
        assert actor
        return self.record

    def list(self, *, limit: int, offset: int, status=None, decision=None):
        """记录筛选并返回固定研究。"""
        assert limit == 20 and offset == 0
        self.list_filter = (status, decision)
        return (self.record,)

    def show(self, study_id: str) -> FactorStudyRecord:
        """返回固定研究并校验身份。"""
        assert study_id == self.record.id
        return self.record

    def decide(self, study_id: str, key: object, mark: FactorDecisionMark, note: str, *, actor: str) -> FactorStudyRecord:
        """记录幂等决策调用。"""
        assert study_id == self.record.id
        self.decision = (key, mark, note, actor)
        return self.record

    def delete(self, study_id: str, *, actor: str) -> None:
        """接受终态删除。"""
        assert study_id == self.record.id and actor


def _published_record(tmp_path: Path) -> FactorStudyRecord:
    definition = FactorStudyConfigParser().parse_file(
        Path("configs/factor_studies/examples/factor_study.yaml")
    ).definition
    directory = tmp_path / "factor-studies" / "study-1"
    directory.mkdir(parents=True)
    frame = pl.DataFrame(
        {
            "signal_variant": ["DIRECTION_ADJUSTED"],
            "label_kind": ["THEORETICAL_FORWARD_RETURN"],
            "factor_ref": ["book_to_price_mrq"],
            "horizon": [5],
            "rank_ic_mean": [0.06],
            "pearson_ic_sample_std": [0.12],
            "rank_ic_valid_date_count": [42],
            "rank_ic_positive_streak_start": [date(2022, 1, 3)],
            "rank_ic_hac_t_stat": [2.4],
            "rank_ic_hac_hac_invalid_reason": [None],
            "monotonicity_mean": [0.8],
            "long_short_mean": [0.02],
            "break_even_cost_bps": [18.0],
            "total_turnover_mean": [0.7],
            "unexpected_summary_metric": ["kept"],
        }
    ).sort("signal_variant", "label_kind", "factor_ref", "horizon")
    path = directory / "summary.parquet"
    frame.write_parquet(path)
    content = path.read_bytes()
    entry: dict[str, JsonValue] = {
        "artifact_type": "summary",
        "relative_path": "summary.parquet",
        "content_hash": hashlib.sha256(content).hexdigest(),
        "byte_count": len(content),
        "row_count": 1,
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "primary_key": ["signal_variant", "label_kind", "factor_ref", "horizon"],
        "sort_key": ["signal_variant", "label_kind", "factor_ref", "horizon"],
    }
    manifest = canonical_json_bytes({"factor_study_id": "study-1", "artifacts": [entry]})
    (directory / "manifest.json").write_bytes(manifest)
    return FactorStudyRecord(
        id="study-1",
        definition=definition,
        config_hash="a" * 64,
        catalog_hash="b" * 64,
        status=FactorStudyStatus.SUCCEEDED,
        stage=FactorStudyStage.PUBLISH,
        task_id="task-1",
        artifact_dir=str(directory),
        manifest_hash=hashlib.sha256(manifest).hexdigest(),
        error=None,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        completed_at=datetime(2026, 1, 2, tzinfo=UTC),
        metrics=(
            FactorStudyMetricRecord(
                name="rank_ic_mean/DIRECTION_ADJUSTED/THEORETICAL_FORWARD_RETURN/book_to_price_mrq/5",
                value=0.06,
                unit="ratio",
                p_value=0.02,
                adjusted_p_value=0.04,
            ),
        ),
        artifacts=(
            FactorStudyArtifactRecord(
                artifact_type="summary",
                relative_path="summary.parquet",
                content_hash=cast(str, entry["content_hash"]),
                byte_count=cast(int, entry["byte_count"]),
                row_count=1,
                schema=cast(dict[str, str], entry["schema"]),
            ),
        ),
    )


def test_routes_validate_catalog_page_matrix_and_decision(tmp_path: Path) -> None:
    """专用路由应严格接收配置并合并显著性与人工决策矩阵。"""
    record = _published_record(tmp_path)
    studies = _Studies(record)
    service = FactorStudyDashboardService(cast(Any, studies), tmp_path)
    app = FastAPI()
    FactorStudyRoutes.mount(app, service)
    client = TestClient(app)
    yaml_text = Path("configs/factor_studies/examples/factor_study.yaml").read_text(encoding="utf-8")

    assert client.get("/api/v1/factor-studies/catalog").json()["factors"][0] == {"factor_id": "book_to_price_mrq"}
    validated = client.post("/api/v1/factor-studies/validate", json={"yaml": yaml_text})
    assert validated.status_code == 200
    assert len(validated.json()["config_hash"]) == 64
    assert client.post("/api/v1/factor-studies/validate", json={"yaml": yaml_text, "extra": True}).status_code == 422
    page = client.get("/api/v1/factor-studies?limit=20&status=SUCCEEDED&decision=UNREVIEWED")
    assert page.status_code == 200 and page.json()["items"][0]["unreviewed_count"] == 1
    assert studies.list_filter == (FactorStudyStatus.SUCCEEDED, FactorDecisionMark.UNREVIEWED)
    matrix = client.get("/api/v1/factor-studies/study-1/matrix").json()
    assert matrix["items"][0]["rank_ic_adjusted_p_value"] == 0.04
    summary_metrics = matrix["items"][0]["summary_metrics"]
    assert summary_metrics["pearson_ic_sample_std"] == 0.12
    assert summary_metrics["rank_ic_valid_date_count"] == 42
    assert summary_metrics["rank_ic_positive_streak_start"] == "2022-01-03"
    assert summary_metrics["rank_ic_hac_hac_invalid_reason"] is None
    assert summary_metrics["unexpected_summary_metric"] == "kept"
    assert set(summary_metrics) == {
        "rank_ic_mean",
        "pearson_ic_sample_std",
        "rank_ic_valid_date_count",
        "rank_ic_positive_streak_start",
        "rank_ic_hac_t_stat",
        "rank_ic_hac_hac_invalid_reason",
        "monotonicity_mean",
        "long_short_mean",
        "break_even_cost_bps",
        "total_turnover_mean",
        "unexpected_summary_metric",
    }

    decision = client.put(
        "/api/v1/factor-studies/study-1/decisions",
        json={
            "signal_variant": "DIRECTION_ADJUSTED",
            "label_kind": "THEORETICAL_FORWARD_RETURN",
            "factor_ref": "book_to_price_mrq",
            "horizon": 5,
            "mark": "CANDIDATE",
            "note": "保留",
        },
    )
    assert decision.status_code == 200
    assert studies.decision is not None and studies.decision[1:3] == (
        FactorDecisionMark.CANDIDATE,
        "保留",
    )


def test_decision_requires_a_real_published_summary_row(tmp_path: Path) -> None:
    """配置维度存在但 summary 未发布的组合也不得写入结论。"""
    studies = _Studies(_published_record(tmp_path))
    service = FactorStudyDashboardService(cast(Any, studies), tmp_path)
    body = FactorStudyDecisionBody(
        signal_variant="DIRECTION_ADJUSTED",
        label_kind="EXECUTABLE_FORWARD_RETURN",
        factor_ref="book_to_price_mrq",
        horizon=5,
        mark=FactorDecisionMark.DISCARDED,
        note="missing",
    )
    with pytest.raises(ValueError, match="published summary"):
        service.decide("study-1", body, "test")
    with pytest.raises(ValueError, match="unsupported artifact"):
        service.artifact("study-1", "stability", 1, 100, {})
