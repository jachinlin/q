from __future__ import annotations

import hashlib
from datetime import date

import polars as pl

from quant_research.application.factor_studies import publish_factor_run
from quant_research.factor_studies.models import FactorStudyConfig


def test_publication_is_immutable_and_hash_bound(tmp_path) -> None:
    config = FactorStudyConfig(
        factor_refs=("earnings_yield_ttm",),
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 31),
    )
    frame = pl.DataFrame({"value": [1.0]})
    path, digest = publish_factor_run(
        artifact_root=tmp_path,
        study_id="study-1",
        run_id="run-1",
        config=config,
        catalog_hash="a" * 64,
        source_hash="b" * 64,
        execution_descriptor={"requested_refs": [], "plan": []},
        environment={"source_hash": "b" * 64},
        outputs={"summary": frame, "ic": frame},
    )

    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert (path.parent / "summary.parquet").is_file()
    assert (path.parent / "ic.parquet").is_file()
    assert not (path.parent / "rank_ic.parquet").exists()
    try:
        publish_factor_run(
            artifact_root=tmp_path,
            study_id="study-1",
            run_id="run-1",
            config=config,
            catalog_hash="a" * 64,
            source_hash="b" * 64,
            execution_descriptor={"requested_refs": [], "plan": []},
            environment={"source_hash": "b" * 64},
            outputs={"summary": frame, "ic": frame},
        )
    except ValueError as error:
        assert "already exists" in str(error)
    else:
        raise AssertionError("immutable factor run publication was overwritten")
