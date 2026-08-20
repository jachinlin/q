"""原子发布并从最终目录复核研究运行产物。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl

from quant_research.data.contracts import JsonValue, canonical_json_bytes


class ResearchArtifactPublisher:
    """在可信根内发布不可变 Parquet/JSON 和自描述 Manifest。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, artifact_root: Path) -> None:
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        self._root = (artifact_root / "research").resolve()

    def publish(
        self,
        *,
        family_id: str,
        execution_id: str,
        run_id: str,
        frames: Mapping[str, pl.DataFrame],
        documents: Mapping[str, Mapping[str, JsonValue]],
        identity: Mapping[str, JsonValue],
    ) -> tuple[Path, str]:
        """发布运行目录，复核全部文件后返回 Manifest 路径和哈希。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        final = (self._root / family_id / execution_id / "runs" / run_id).resolve()
        if not final.is_relative_to(self._root):
            raise ValueError("research artifact path escaped trusted root")
        if final.exists():
            raise FileExistsError(f"immutable research run already exists: {run_id}")
        staging = (
            final.parent.parent / f".run-staging-{uuid4().hex[:8]}"
        ).resolve()
        staging.mkdir(parents=True, exist_ok=False)
        entries: list[dict[str, JsonValue]] = []
        try:
            for relative_path, frame in sorted(frames.items()):
                path = self._target(staging, relative_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                sorted_frame = self._stable_frame(frame)
                sorted_frame.write_parquet(path)
                entries.append(
                    self._entry(
                        staging,
                        path,
                        artifact_type="PARQUET",
                        schema={name: str(dtype) for name, dtype in sorted_frame.schema.items()},
                        row_count=sorted_frame.height,
                        primary_key=self._keys(relative_path, sorted_frame),
                        sort_order=self._keys(relative_path, sorted_frame),
                    )
                )
            for relative_path, document in sorted(documents.items()):
                path = self._target(staging, relative_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = canonical_json_bytes(document)
                path.write_bytes(payload)
                entries.append(
                    self._entry(
                        staging,
                        path,
                        artifact_type="JSON",
                        schema={},
                        row_count=None,
                        primary_key=(),
                        sort_order=(),
                    )
                )
            manifest: dict[str, JsonValue] = {
                "identity": dict(identity),
                "entries": cast(list[JsonValue], entries),
            }
            manifest_payload = canonical_json_bytes(manifest)
            (staging / "manifest.json").write_bytes(manifest_payload)
            final.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(final)
            manifest_path = final / "manifest.json"
            self._verify(final, manifest_path)
            return manifest_path, hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        except Exception:
            if staging.exists():
                for path in sorted(staging.rglob("*"), reverse=True):
                    if path.is_file():
                        path.unlink()
                    elif path.is_dir():
                        path.rmdir()
                staging.rmdir()
            raise

    def publish_selection(
        self,
        *,
        family_id: str,
        execution_id: str,
        document: Mapping[str, JsonValue],
    ) -> tuple[Path, str, int]:
        """原子发布执行级 ``selection.json`` 并复核其内容身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        execution_root = (self._root / family_id / execution_id).resolve()
        if not execution_root.is_relative_to(self._root):
            raise ValueError("research selection path escaped trusted root")
        execution_root.mkdir(parents=True, exist_ok=True)
        final = execution_root / "selection.json"
        if final.exists():
            raise FileExistsError(
                f"immutable research selection already exists: {execution_id}"
            )
        payload = canonical_json_bytes(document)
        staging = execution_root / f".selection.{uuid4().hex}.tmp"
        staging.write_bytes(payload)
        staging.rename(final)
        verified = final.read_bytes()
        if verified != payload:
            raise ValueError("research selection identity mismatch")
        return final, hashlib.sha256(verified).hexdigest(), len(verified)

    @staticmethod
    def _stable_frame(frame: pl.DataFrame) -> pl.DataFrame:
        if not isinstance(frame, pl.DataFrame):
            raise TypeError("research artifacts must be Polars DataFrames")
        preferred = [
            name
            for name in (
                "split",
                "signal_date",
                "trade_date",
                "ordinal",
                "instrument_id",
                "signal_id",
                "feature_id",
            )
            if name in frame.columns
        ]
        return frame.sort(preferred) if preferred else frame

    @staticmethod
    def _target(root: Path, relative_path: str) -> Path:
        if not relative_path or Path(relative_path).is_absolute():
            raise ValueError("artifact relative_path must be relative")
        target = (root / relative_path).resolve()
        if not target.is_relative_to(root):
            raise ValueError("artifact relative_path escaped run directory")
        return target

    @staticmethod
    def _entry(
        root: Path,
        path: Path,
        *,
        artifact_type: str,
        schema: Mapping[str, str],
        row_count: int | None,
        primary_key: tuple[str, ...],
        sort_order: tuple[str, ...],
    ) -> dict[str, JsonValue]:
        payload = path.read_bytes()
        return {
            "relative_path": path.relative_to(root).as_posix(),
            "artifact_type": artifact_type,
            "producer_component_id": "research_runtime",
            "producer_component_hash": hashlib.sha256(b"research_runtime").hexdigest(),
            "input_artifact_hashes": [],
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "byte_count": len(payload),
            "schema": dict(schema),
            "row_count": row_count,
            "primary_key": list(primary_key),
            "sort_order": list(sort_order),
            "quality_summary": {"verified": True},
        }

    @staticmethod
    def _keys(relative_path: str, frame: pl.DataFrame) -> tuple[str, ...]:
        declared = {
            "universe.parquet": ("split", "signal_date", "instrument_id"),
            "features/features.parquet": (
                "split",
                "signal_date",
                "instrument_id",
                "feature_id",
            ),
            "signals/signals.parquet": (
                "split",
                "signal_date",
                "instrument_id",
                "signal_id",
            ),
            "target_portfolios.parquet": (
                "split",
                "signal_date",
                "instrument_id",
            ),
            "fills.parquet": ("split", "trade_date", "ordinal"),
            "analytics/nav.parquet": ("split", "trade_date"),
        }.get(relative_path, ())
        return tuple(name for name in declared if name in frame.columns)

    @staticmethod
    def _verify(final: Path, manifest_path: Path) -> None:
        payload = cast(dict[str, JsonValue], json.loads(manifest_path.read_bytes()))
        entries = payload.get("entries")
        if not isinstance(entries, list):
            raise TypeError("research manifest entries are invalid")
        declared = {"manifest.json"}
        for raw in entries:
            if not isinstance(raw, dict):
                raise TypeError("research manifest entry is invalid")
            relative = raw.get("relative_path")
            if not isinstance(relative, str):
                raise TypeError("research manifest relative_path is invalid")
            path = (final / relative).resolve()
            if not path.is_relative_to(final) or not path.is_file():
                raise ValueError("research manifest file is missing")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != raw.get("content_hash") or len(data) != raw.get("byte_count"):
                raise ValueError("research artifact identity mismatch")
            if raw.get("artifact_type") == "PARQUET":
                frame = pl.read_parquet(path)
                if frame.height != raw.get("row_count"):
                    raise ValueError("research artifact row count mismatch")
                schema = {name: str(dtype) for name, dtype in frame.schema.items()}
                if schema != raw.get("schema"):
                    raise ValueError("research artifact schema mismatch")
                primary = raw.get("primary_key")
                ordering = raw.get("sort_order")
                if not isinstance(primary, list) or not all(
                    isinstance(item, str) for item in primary
                ):
                    raise TypeError("research artifact primary key is invalid")
                if not isinstance(ordering, list) or not all(
                    isinstance(item, str) for item in ordering
                ):
                    raise TypeError("research artifact sort order is invalid")
                if primary and (
                    frame.select(primary).null_count().to_numpy().sum() > 0
                    or frame.select(primary).n_unique() != frame.height
                ):
                    raise ValueError("research artifact primary key is invalid")
                if ordering and not frame.equals(frame.sort(ordering)):
                    raise ValueError("research artifact sort order mismatch")
            declared.add(relative)
        actual = {path.relative_to(final).as_posix() for path in final.rglob("*") if path.is_file()}
        if actual != declared:
            raise ValueError("research run contains undeclared artifacts")
