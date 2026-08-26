from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.domain.enums import DatasetKind


def test_canonical_partition_uses_tushare_namespace(tmp_path: Path) -> None:
    store = CuratedPartitionStore(tmp_path / "canonical")
    frame = pl.DataFrame(
        schema=CANONICAL_SCHEMAS[DatasetKind.STOCK_MASTER].columns
    )
    partition, _ = store._publish_partition(
        DatasetKind.STOCK_MASTER, "all", frame
    )
    relative = partition.path.relative_to(tmp_path / "canonical")
    assert relative.parts[:3] == (
        "source=tushare",
        "dataset=stock_master",
        "all",
    )


def test_legacy_canonical_layout_requires_new_data_root(tmp_path: Path) -> None:
    root = tmp_path / "canonical"
    (root / "dataset=stock_master").mkdir(parents=True)
    with pytest.raises(ValueError, match="new QUANT_DATA_ROOT"):
        CuratedPartitionStore(root)
