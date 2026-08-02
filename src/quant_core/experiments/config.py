"""Strict immutable experiment configuration resolution."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast

import yaml

from quant_core.backtest.engine import StrategyRef
from quant_core.backtest.models import ExecutionPrice
from quant_core.backtest.rulebook import MarketRuleBook
from quant_core.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_core.domain.enums import Board, DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import DatasetVersionId, InstrumentId, SnapshotId
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.repositories import DatasetVersionRecord, SnapshotRecord

_MAX_YAML_BYTES = 1_048_576
_TOP_LEVEL_KEYS = {
    "benchmark",
    "end_date",
    "execution",
    "initial_cash_fen",
    "rulebook_version",
    "schema_version",
    "snapshot_id",
    "start_date",
    "strategy_config",
    "strategy_id",
    "strategy_version",
    "universe",
}
_REQUIRED_TOP_LEVEL_KEYS = {
    "benchmark",
    "end_date",
    "initial_cash_fen",
    "rulebook_version",
    "schema_version",
    "snapshot_id",
    "start_date",
    "strategy_id",
    "strategy_version",
}
_EXECUTION_KEYS = {
    "max_volume_participation",
    "reference_price",
    "slippage_bps",
}
_UNIVERSE_KEYS = {
    "allowed_boards",
    "exclude_st",
    "exclude_suspended",
    "min_avg_amount_20d",
    "min_listing_days",
}


class ExperimentConfigError(QuantError):
    """An experiment input cannot be resolved to a concrete immutable config."""

    def __init__(self, message: str, *, field: str | None = None) -> None:
        context: dict[str, object] = {}
        if field is not None:
            context["field"] = field
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_CONFIG_INVALID",
                severity=Severity.FATAL,
                message=message,
                context=context,
                remediation="correct the experiment YAML and resolve it again",
                retryable=False,
            )
        )


class ExperimentCapabilityUnavailable(QuantError):
    """A runtime provider lacks a capability required by the experiment."""

    def __init__(
        self,
        missing: tuple[str, ...],
        *,
        provider: str,
        stage: str,
    ) -> None:
        self.missing = missing
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_PROVIDER_CAPABILITY_UNAVAILABLE",
                severity=Severity.FATAL,
                message=(
                    f"provider {provider} lacks required experiment capabilities: "
                    + ", ".join(missing)
                ),
                context={
                    "provider": provider,
                    "stage": stage,
                    "missing": list(missing),
                },
                remediation="use a provider profile that supplies every required input",
                retryable=False,
            )
        )


class ExperimentSnapshotCatalog(Protocol):
    """Snapshot metadata required to eliminate every experiment selector."""

    def latest_snapshot(self) -> SnapshotRecord | None: ...

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord: ...

    def get_dataset_version(
        self, identifier: DatasetVersionId
    ) -> DatasetVersionRecord: ...


@dataclass(frozen=True, slots=True)
class ResolvedExperimentConfig:
    """A selector-free canonical configuration plus its snapshot evidence hash."""

    _mapping: Mapping[str, JsonValue]
    snapshot_manifest_hash: str

    def __post_init__(self) -> None:
        mapping = _freeze_json(self._mapping)
        if not isinstance(mapping, Mapping):
            raise TypeError("resolved experiment config must be a mapping")
        plain = _thaw_json(mapping)
        canonical_json_bytes(plain)
        if not _is_sha256(self.snapshot_manifest_hash):
            raise ValueError("snapshot_manifest_hash must be a SHA-256 digest")
        object.__setattr__(self, "_mapping", mapping)

    @property
    def mapping(self) -> dict[str, JsonValue]:
        """Return an isolated mutable copy suitable for persistence and hashing."""
        value = _thaw_json(self._mapping)
        if not isinstance(value, dict):
            raise TypeError("validated resolved config stopped being a mapping")
        return cast(dict[str, JsonValue], value)

    @property
    def strategy_ref(self) -> StrategyRef:
        return StrategyRef(
            cast(str, self._mapping["strategy_id"]),
            cast(str, self._mapping["strategy_version"]),
        )

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId.parse(cast(str, self._mapping["snapshot_id"]))


def resolve_experiment_yaml(
    path: Path,
    *,
    config_root: Path,
    catalog: ExperimentSnapshotCatalog,
    strategies: Mapping[StrategyRef, object],
    rulebook: MarketRuleBook,
) -> ResolvedExperimentConfig:
    """Read one safe YAML envelope and resolve it against immutable runtime facts."""
    loaded = _load_yaml_mapping(path, config_root)
    _exact_keys(
        loaded,
        allowed=_TOP_LEVEL_KEYS,
        required=_REQUIRED_TOP_LEVEL_KEYS,
        label="experiment config",
    )
    if type(loaded["schema_version"]) is not int or loaded["schema_version"] != 1:
        raise ExperimentConfigError("schema_version must be the integer 1")
    strategy_ref = StrategyRef(
        _text(loaded["strategy_id"], "strategy_id"),
        _text(loaded["strategy_version"], "strategy_version"),
    )
    if not isinstance(strategies, Mapping) or strategy_ref not in strategies:
        raise ExperimentConfigError("unknown strategy or version")
    configured_rulebook = _text(loaded["rulebook_version"], "rulebook_version")
    actual_rulebook = getattr(rulebook, "version", None)
    if not isinstance(actual_rulebook, str) or configured_rulebook != actual_rulebook:
        raise ExperimentConfigError(
            "configured rulebook version does not match injected rulebook"
        )

    snapshot = _resolve_snapshot(loaded["snapshot_id"], catalog)
    daily = _daily_bar_version(snapshot, catalog)
    coverage_start, coverage_end = daily.start_date, daily.end_date
    if coverage_start is None or coverage_end is None or coverage_start > coverage_end:
        raise ExperimentConfigError("snapshot daily-bar coverage is empty")
    start = _resolve_date(loaded["start_date"], "snapshot_start", coverage_start)
    end = _resolve_date(loaded["end_date"], "snapshot_end", coverage_end)
    if start > end:
        raise ExperimentConfigError("start_date must not follow end_date")
    if start < coverage_start or end > coverage_end:
        raise ExperimentConfigError(
            "experiment date range is outside snapshot daily-bar coverage"
        )

    benchmark_text = _text(loaded["benchmark"], "benchmark")
    try:
        benchmark = InstrumentId.parse(benchmark_text).canonical()
    except (TypeError, ValueError) as error:
        raise ExperimentConfigError("benchmark must be a canonical instrument") from error
    initial_cash = loaded["initial_cash_fen"]
    if type(initial_cash) is not int or initial_cash <= 0:
        raise ExperimentConfigError("initial_cash_fen must be a positive integer")
    execution = _execution(loaded.get("execution", {}))
    universe = _universe(loaded.get("universe", {}))
    strategy_config = loaded.get("strategy_config", {})
    if not isinstance(strategy_config, Mapping):
        raise ExperimentConfigError("strategy_config must be a mapping")
    try:
        strategy_value = _plain_json(strategy_config)
        canonical_json_bytes(strategy_value)
    except (TypeError, ValueError) as error:
        raise ExperimentConfigError(
            "strategy_config must contain finite JSON values"
        ) from error

    mapping: dict[str, JsonValue] = {
        "benchmark": benchmark,
        "end_date": end.isoformat(),
        "execution": execution,
        "initial_cash_fen": initial_cash,
        "rulebook_version": configured_rulebook,
        "schema_version": 1,
        "snapshot_id": str(snapshot.id),
        "start_date": start.isoformat(),
        "strategy_config": strategy_value,
        "strategy_id": strategy_ref.strategy_id,
        "strategy_version": strategy_ref.version,
        "universe": universe,
    }
    return ResolvedExperimentConfig(mapping, snapshot.manifest_hash)


def require_provider_capabilities(
    capabilities: ProviderCapabilities,
    required: Sequence[str],
    *,
    provider: str,
    stage: str,
) -> None:
    """Reject missing declared capabilities before an experiment can write output."""
    if not isinstance(capabilities, ProviderCapabilities):
        raise TypeError("capabilities must be ProviderCapabilities")
    provider_name = _nonempty_text(provider, "provider")
    stage_name = _nonempty_text(stage, "stage")
    if isinstance(required, (str, bytes)) or not isinstance(required, Sequence):
        raise TypeError("required capabilities must be a sequence")
    normalized = tuple(required)
    if len(set(normalized)) != len(normalized):
        raise ValueError("required capabilities must be unique")
    missing = capabilities.missing(normalized)
    if missing:
        raise ExperimentCapabilityUnavailable(
            missing,
            provider=provider_name,
            stage=stage_name,
        )


def _load_yaml_mapping(path: Path, config_root: Path) -> dict[str, object]:
    if not isinstance(path, Path) or not isinstance(config_root, Path):
        raise TypeError("path and config_root must be Paths")
    source = _plain_config_file(path, config_root)
    try:
        raw = source.read_bytes()
    except OSError as error:
        raise ExperimentConfigError("experiment YAML cannot be read") from error
    if len(raw) > _MAX_YAML_BYTES:
        raise ExperimentConfigError("experiment YAML exceeds the size limit")
    try:
        text = raw.decode("utf-8")
        loaded = yaml.safe_load(text)
    except (UnicodeDecodeError, yaml.YAMLError) as error:
        raise ExperimentConfigError("experiment config must be safe YAML") from error
    if not isinstance(loaded, dict):
        raise ExperimentConfigError("experiment YAML root must be a mapping")
    if any(not isinstance(key, str) for key in loaded):
        raise ExperimentConfigError("experiment YAML keys must be strings")
    return cast(dict[str, object], loaded)


def _plain_config_file(path: Path, config_root: Path) -> Path:
    root = config_root.absolute()
    candidate = path.absolute()
    try:
        relative = candidate.relative_to(root)
    except ValueError as error:
        raise ExperimentConfigError("experiment YAML must be inside config_root") from error
    if not relative.parts:
        raise ExperimentConfigError("experiment YAML must name a file")
    for component in (root, *(root / Path(*relative.parts[:index]) for index in range(1, len(relative.parts) + 1))):
        try:
            observed = component.stat(follow_symlinks=False)
        except OSError as error:
            raise ExperimentConfigError("experiment YAML path does not exist") from error
        if component.is_symlink() or _is_reparse(observed):
            raise ExperimentConfigError(
                "experiment YAML path contains a link or reparse point"
            )
    if not stat.S_ISDIR(root.stat(follow_symlinks=False).st_mode):
        raise ExperimentConfigError("config_root must be a directory")
    final_status = candidate.stat(follow_symlinks=False)
    if not stat.S_ISREG(final_status.st_mode):
        raise ExperimentConfigError("experiment YAML must be a regular file")
    return candidate


def _resolve_snapshot(
    value: object, catalog: ExperimentSnapshotCatalog
) -> SnapshotRecord:
    selector = _text(value, "snapshot_id")
    if selector == "latest":
        snapshot = catalog.latest_snapshot()
        if snapshot is None:
            raise ExperimentConfigError("no published snapshot exists")
    else:
        try:
            identifier = SnapshotId.parse(selector)
        except ValueError as error:
            raise ExperimentConfigError(
                "snapshot_id must be latest or a canonical UUID"
            ) from error
        try:
            snapshot = catalog.get_snapshot(identifier)
        except KeyError as error:
            raise ExperimentConfigError("snapshot does not exist") from error
    if (
        snapshot.status is not SnapshotStatus.PUBLISHED
        or snapshot.published_at is None
    ):
        raise ExperimentConfigError("snapshot must be fully published")
    if not _is_sha256(snapshot.manifest_hash):
        raise ExperimentConfigError("snapshot manifest hash is invalid")
    return snapshot


def _daily_bar_version(
    snapshot: SnapshotRecord, catalog: ExperimentSnapshotCatalog
) -> DatasetVersionRecord:
    identifier = snapshot.dataset_versions.get(DatasetKind.DAILY_BAR.value)
    if identifier is None:
        raise ExperimentConfigError("snapshot has no daily-bar dataset")
    try:
        record = catalog.get_dataset_version(identifier)
    except KeyError as error:
        raise ExperimentConfigError("snapshot daily-bar dataset does not exist") from error
    if (
        record.dataset is not DatasetKind.DAILY_BAR
        or record.status != SnapshotStatus.PUBLISHED.value
    ):
        raise ExperimentConfigError("snapshot daily-bar dataset must be published")
    return record


def _resolve_date(value: object, selector: str, selected: date) -> date:
    if isinstance(value, datetime):
        raise ExperimentConfigError("experiment dates must not contain times")
    if isinstance(value, date):
        return value
    text = _text(value, selector)
    if text == selector:
        return selected
    try:
        parsed = date.fromisoformat(text)
    except ValueError as error:
        raise ExperimentConfigError(
            f"{selector.removeprefix('snapshot_')}_date must be {selector} or YYYY-MM-DD"
        ) from error
    if parsed.isoformat() != text:
        raise ExperimentConfigError("experiment date must use canonical YYYY-MM-DD")
    return parsed


def _execution(value: object) -> dict[str, JsonValue]:
    mapping = _mapping(value, "execution")
    _exact_keys(mapping, allowed=_EXECUTION_KEYS, required=set(), label="execution")
    reference = _text(mapping.get("reference_price", "OPEN"), "reference_price")
    try:
        reference_price = ExecutionPrice(reference)
    except ValueError as error:
        raise ExperimentConfigError("execution reference_price is invalid") from error
    slippage = _finite_nonnegative(mapping.get("slippage_bps", 5.0), "slippage_bps")
    participation = _finite_nonnegative(
        mapping.get("max_volume_participation", 0.10),
        "max_volume_participation",
    )
    if participation <= 0.0 or participation > 1.0:
        raise ExperimentConfigError(
            "max_volume_participation must be in (0, 1]"
        )
    return {
        "max_volume_participation": participation,
        "reference_price": reference_price.value,
        "slippage_bps": slippage,
    }


def _universe(value: object) -> dict[str, JsonValue]:
    mapping = _mapping(value, "universe")
    _exact_keys(mapping, allowed=_UNIVERSE_KEYS, required=set(), label="universe")
    listing_days = mapping.get("min_listing_days", 120)
    if type(listing_days) is not int or listing_days < 0:
        raise ExperimentConfigError(
            "universe min_listing_days must be a nonnegative integer"
        )
    raw_boards = mapping.get("allowed_boards", ["MAIN", "CHINEXT", "STAR"])
    if not isinstance(raw_boards, list) or not raw_boards:
        raise ExperimentConfigError("universe allowed_boards must be a nonempty list")
    try:
        boards = tuple(Board(_text(item, "allowed_board")) for item in raw_boards)
    except ValueError as error:
        raise ExperimentConfigError("universe allowed_boards is invalid") from error
    if len(set(boards)) != len(boards):
        raise ExperimentConfigError("universe allowed_boards must be unique")
    exclude_st = mapping.get("exclude_st", True)
    exclude_suspended = mapping.get("exclude_suspended", True)
    if type(exclude_st) is not bool or type(exclude_suspended) is not bool:
        raise ExperimentConfigError("universe exclusion flags must be booleans")
    minimum = mapping.get("min_avg_amount_20d")
    min_amount = (
        None
        if minimum is None
        else _finite_nonnegative(minimum, "min_avg_amount_20d")
    )
    return {
        "allowed_boards": cast(
            list[JsonValue], sorted(board.value for board in boards)
        ),
        "exclude_st": exclude_st,
        "exclude_suspended": exclude_suspended,
        "min_avg_amount_20d": min_amount,
        "min_listing_days": listing_days,
    }


def _exact_keys(
    mapping: Mapping[str, object],
    *,
    allowed: set[str],
    required: set[str],
    label: str,
) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ExperimentConfigError(f"unknown {label} key: {unknown[0]}")
    missing = sorted(required - set(mapping))
    if missing:
        raise ExperimentConfigError(f"missing {label} key: {missing[0]}")


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ExperimentConfigError(f"{field} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise ExperimentConfigError(f"{field} keys must be strings")
    return cast(dict[str, object], dict(value))


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ExperimentConfigError(f"{field} must be a nonempty trimmed string")
    return value


def _nonempty_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _finite_nonnegative(value: object, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value < 0
    ):
        raise ExperimentConfigError(f"{field} must be finite and nonnegative")
    return float(value)


def _plain_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            result[key] = _plain_json(item)
        return result
    if isinstance(value, list):
        return [_plain_json(item) for item in value]
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float and isfinite(value):
        return value
    raise TypeError("value is not finite JSON")


def _freeze_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        return cast(
            JsonValue,
            MappingProxyType(
                {str(key): _freeze_json(item) for key, item in sorted(value.items())}
            ),
        )
    if isinstance(value, (list, tuple)):
        return cast(JsonValue, tuple(_freeze_json(item) for item in value))
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float and isfinite(value):
        return value
    raise TypeError("value is not finite JSON")


def _thaw_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float and isfinite(value):
        return value
    raise TypeError("frozen value is not finite JSON")


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_reparse(status: os.stat_result) -> bool:
    return bool(getattr(status, "st_file_attributes", 0) & 0x400)


__all__ = [
    "ExperimentCapabilityUnavailable",
    "ExperimentConfigError",
    "ExperimentSnapshotCatalog",
    "ResolvedExperimentConfig",
    "require_provider_capabilities",
    "resolve_experiment_yaml",
]
