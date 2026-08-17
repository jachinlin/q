"""提供 config 模块的公开模型、协议与处理流程。"""

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

from quant_research.backtest.engine import StrategyRef
from quant_research.backtest.models import ExecutionPrice
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_research.domain.enums import Board, DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import InstrumentId
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DataCatalogState,
)

_MAX_YAML_BYTES = 1_048_576
_TOP_LEVEL_KEYS = {
    "benchmark",
    "end_date",
    "execution",
    "initial_cash_fen",
    "industry",
    "start_date",
    "strategy_config",
    "strategy_id",
    "universe",
}
_REQUIRED_TOP_LEVEL_KEYS = {
    "benchmark",
    "end_date",
    "initial_cash_fen",
    "start_date",
    "strategy_id",
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
_INDUSTRY_KEYS = {"taxonomy", "unclassified_policy"}


class ExperimentConfigError(QuantError):
    """表示实验流程中调用方需要识别的实验配置错误。

    入参：
        message：面向用户且已脱敏的错误或状态说明。
        field：字段。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

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
    """表示实验配置请求了当前数据供应商无法提供的能力。

    入参：
        missing：参与本次处理的缺失项；调用方不得依赖未声明的顺序。
        provider：数据供应商。
        stage：执行阶段。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

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


class ExperimentCatalog(Protocol):
    """表示实验流程中的实验数据目录及其业务不变量。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    def require_validated_catalog(self) -> DataCatalogState:
        """取得并确认``validated``数据目录。

        入参：
            无。
        返回值：
            返回``validated``数据目录（``DataCatalogState``）。
        异常：
            无。
        """
        ...

    def get_canonical_dataset(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        """读取Canonical数据集。

        入参：
            dataset：数据集。
        返回值：
            返回读取Canonical数据集后的Canonical数据集（``CanonicalDatasetRecord``）。
        异常：
            无。
        """
        ...


@dataclass(frozen=True, slots=True)
class ResolvedExperimentConfig:
    """定义实验流程使用的不可变配置及取值约束。

    入参：
        _mapping：参与本次处理的配置映射；调用方不得依赖未声明的顺序。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    _mapping: Mapping[str, JsonValue]
    data_hash: str

    def __post_init__(self) -> None:
        mapping = _ConfigSupport._freeze_json(self._mapping)
        if not isinstance(mapping, Mapping):
            raise TypeError("resolved experiment config must be a mapping")
        plain = _ConfigSupport._thaw_json(mapping)
        canonical_json_bytes(plain)
        if not _ConfigSupport._is_sha256(self.data_hash):
            raise ValueError("data_hash must be a SHA-256 digest")
        object.__setattr__(self, "_mapping", mapping)

    @property
    def mapping(self) -> dict[str, JsonValue]:
        """处理实验中的配置映射。

        入参：
            无。
        返回值：
            返回配置映射（``dict[str, JsonValue]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        """
        value = _ConfigSupport._thaw_json(self._mapping)
        if not isinstance(value, dict):
            raise TypeError("validated resolved config stopped being a mapping")
        return cast(dict[str, JsonValue], value)

    @property
    def strategy_ref(self) -> StrategyRef:
        """处理实验中的策略``ref``。

        入参：
            无。
        返回值：
            返回``ref``（``StrategyRef``）。
        异常：
            无。
        """
        return StrategyRef(cast(str, self._mapping["strategy_id"]))


def resolve_experiment_yaml(
    path: Path,
    *,
    config_root: Path,
    catalog: ExperimentCatalog,
    strategies: Mapping[StrategyRef, object],
    rulebook: MarketRuleBook,
) -> ResolvedExperimentConfig:
    """读取受信目录内的 YAML，并绑定当前已验证数据身份；该函数作为稳定公开 API 保留在模块级。

    入参：
        path：经可信根边界校验后使用的路径。
        config_root：所有派生路径必须位于其中的配置可信根目录。
        catalog：数据目录。
        strategies：参与本次处理的策略集合；调用方不得依赖未声明的顺序。
        rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
    返回值：
        返回解析实验YAML 文本后的实验YAML 文本（``ResolvedExperimentConfig``）。
    异常：
        路径越出可信根、文件缺失或完整性校验失败时传播对应文件异常。
    """
    loaded = _ConfigSupport._load_yaml_mapping(path, config_root)
    return _ConfigSupport._resolve_experiment_mapping(
        loaded,
        catalog=catalog,
        strategies=strategies,
        rulebook=rulebook,
    )


def resolve_experiment_yaml_text(
    config_yaml: str,
    *,
    catalog: ExperimentCatalog,
    strategies: Mapping[StrategyRef, object],
    rulebook: MarketRuleBook,
) -> ResolvedExperimentConfig:
    """解析内存中的安全 YAML，并绑定当前已验证数据身份；该函数作为稳定公开 API 保留在模块级。

    入参：
        config_yaml：用户提交的实验 YAML 原文；仅从受信配置根或内存文本解析。
        catalog：数据目录。
        strategies：参与本次处理的策略集合；调用方不得依赖未声明的顺序。
        rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
    返回值：
        返回解析实验YAML 文本``text``后的实验YAML 文本``text``（``ResolvedExperimentConfig``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``ExperimentConfigError``、``TypeError``。
    """
    if not isinstance(config_yaml, str):
        raise TypeError("config_yaml must be a string")
    payload = config_yaml.encode("utf-8")
    if len(payload) > _MAX_YAML_BYTES:
        raise ExperimentConfigError("experiment YAML exceeds the size limit")
    loaded = _ConfigSupport._load_yaml_bytes(payload)
    return _ConfigSupport._resolve_experiment_mapping(
        loaded,
        catalog=catalog,
        strategies=strategies,
        rulebook=rulebook,
    )


class _ConfigSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _resolve_experiment_mapping(
        loaded: Mapping[str, object],
        *,
        catalog: ExperimentCatalog,
        strategies: Mapping[StrategyRef, object],
        rulebook: MarketRuleBook,
    ) -> ResolvedExperimentConfig:
        _ConfigSupport._exact_keys(
            loaded,
            allowed=_TOP_LEVEL_KEYS,
            required=_REQUIRED_TOP_LEVEL_KEYS,
            label="experiment config",
        )
        strategy_ref = StrategyRef(
            _ConfigSupport._text(loaded["strategy_id"], "strategy_id")
        )
        if not isinstance(strategies, Mapping) or strategy_ref not in strategies:
            raise ExperimentConfigError("unknown strategy")
        rulebook_hash = getattr(rulebook, "content_hash", None)
        if not _ConfigSupport._is_sha256(rulebook_hash):
            raise ExperimentConfigError("injected rulebook content hash is invalid")

        state = catalog.require_validated_catalog()
        if (
            state.validated_catalog_hash is None
            or state.catalog_hash != state.validated_catalog_hash
        ):
            raise ExperimentConfigError(
                "current canonical data has not passed validate-all"
            )
        daily = catalog.get_canonical_dataset(DatasetKind.DAILY_BAR)
        coverage_start, coverage_end = daily.start_date, daily.end_date
        if (
            coverage_start is None
            or coverage_end is None
            or coverage_start > coverage_end
        ):
            raise ExperimentConfigError("canonical daily-bar coverage is empty")
        start = _ConfigSupport._resolve_date(loaded["start_date"], "start_date")
        end = _ConfigSupport._resolve_date(loaded["end_date"], "end_date")
        if start > end:
            raise ExperimentConfigError("start_date must not follow end_date")
        if start < coverage_start or end > coverage_end:
            raise ExperimentConfigError(
                "experiment date range is outside canonical daily-bar coverage"
            )

        benchmark_text = _ConfigSupport._text(loaded["benchmark"], "benchmark")
        try:
            benchmark = InstrumentId.parse(benchmark_text).canonical()
        except (TypeError, ValueError) as error:
            raise ExperimentConfigError(
                "benchmark must be a canonical instrument"
            ) from error
        initial_cash = loaded["initial_cash_fen"]
        if type(initial_cash) is not int or initial_cash <= 0:
            raise ExperimentConfigError("initial_cash_fen must be a positive integer")
        execution = _ConfigSupport._execution(loaded.get("execution", {}))
        universe = _ConfigSupport._universe(loaded.get("universe", {}))
        strategy_config = loaded.get("strategy_config", {})
        if not isinstance(strategy_config, Mapping):
            raise ExperimentConfigError("strategy_config must be a mapping")
        try:
            strategy_value = _ConfigSupport._plain_json(strategy_config)
            canonical_json_bytes(strategy_value)
        except (TypeError, ValueError) as error:
            raise ExperimentConfigError(
                "strategy_config must contain finite JSON values"
            ) from error

        industry = _ConfigSupport._industry(
            loaded.get("industry"), catalog=catalog, start=start, end=end
        )

        mapping: dict[str, JsonValue] = {
            "benchmark": benchmark,
            "end_date": end.isoformat(),
            "execution": execution,
            "initial_cash_fen": initial_cash,
            "rulebook_hash": cast(str, rulebook_hash),
            "start_date": start.isoformat(),
            "strategy_config": strategy_value,
            "strategy_id": strategy_ref.strategy_id,
            "universe": universe,
        }
        if industry is not None:
            mapping["industry"] = industry
        return ResolvedExperimentConfig(mapping, state.catalog_hash)

    @staticmethod
    def _industry(
        value: object,
        *,
        catalog: ExperimentCatalog,
        start: date,
        end: date,
    ) -> dict[str, JsonValue] | None:
        """校验显式行业依赖及其分类体系和未分类策略。"""
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise ExperimentConfigError("industry must be a mapping", field="industry")
        _ConfigSupport._exact_keys(
            value,
            allowed=_INDUSTRY_KEYS,
            required=_INDUSTRY_KEYS,
            label="industry",
        )
        taxonomy = _ConfigSupport._text(value["taxonomy"], "industry.taxonomy")
        unclassified_policy = _ConfigSupport._text(
            value["unclassified_policy"], "industry.unclassified_policy"
        )
        if unclassified_policy not in {"EXCLUDE", "UNCLASSIFIED"}:
            raise ExperimentConfigError(
                "industry.unclassified_policy must be EXCLUDE or UNCLASSIFIED",
                field="industry.unclassified_policy",
            )
        try:
            record = catalog.get_canonical_dataset(DatasetKind.INDUSTRY_CLASSIFICATION)
        except KeyError as error:
            raise ExperimentConfigError(
                "industry classification canonical dataset is missing",
                field="industry",
            ) from error
        if (
            record.start_date is None
            or record.end_date is None
            or start < record.start_date
            or end > record.end_date
        ):
            raise ExperimentConfigError(
                "experiment date range is outside industry coverage",
                field="industry",
            )
        return {
            "dataset": DatasetKind.INDUSTRY_CLASSIFICATION.value,
            "taxonomy": taxonomy,
            "unclassified_policy": unclassified_policy,
            "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
        }

    @staticmethod
    def _load_yaml_mapping(path: Path, config_root: Path) -> dict[str, object]:
        if not isinstance(path, Path) or not isinstance(config_root, Path):
            raise TypeError("path and config_root must be Paths")
        source, identities = _ConfigSupport._plain_config_file(path, config_root)
        raw = _ConfigSupport._read_config_handle(source, identities)
        return _ConfigSupport._load_yaml_bytes(raw)

    @staticmethod
    def _load_yaml_bytes(raw: bytes) -> dict[str, object]:
        try:
            loaded = yaml.safe_load(raw.decode("utf-8"))
        except (UnicodeDecodeError, yaml.YAMLError) as error:
            raise ExperimentConfigError(
                "experiment config must be safe YAML"
            ) from error
        if not isinstance(loaded, dict):
            raise ExperimentConfigError("experiment YAML root must be a mapping")
        if any(not isinstance(key, str) for key in loaded):
            raise ExperimentConfigError("experiment YAML keys must be strings")
        return cast(dict[str, object], loaded)

    @staticmethod
    def _plain_config_file(
        path: Path, config_root: Path
    ) -> tuple[Path, tuple[tuple[Path, _PathIdentity], ...]]:
        root = config_root.absolute()
        candidate = path.absolute()
        try:
            relative = candidate.relative_to(root)
        except ValueError as error:
            raise ExperimentConfigError(
                "experiment YAML must be inside config_root"
            ) from error
        if not relative.parts:
            raise ExperimentConfigError("experiment YAML must name a file")
        components = (
            root,
            *(
                root / Path(*relative.parts[:index])
                for index in range(1, len(relative.parts) + 1)
            ),
        )
        identities: list[tuple[Path, _PathIdentity]] = []
        for component in components:
            try:
                observed = component.stat(follow_symlinks=False)
            except OSError as error:
                raise ExperimentConfigError(
                    "experiment YAML path does not exist"
                ) from error
            if component.is_symlink() or _ConfigSupport._is_reparse(observed):
                raise ExperimentConfigError(
                    "experiment YAML path contains a link or reparse point"
                )
            identities.append((component, _ConfigSupport._path_identity(observed)))
        if not stat.S_ISDIR(root.stat(follow_symlinks=False).st_mode):
            raise ExperimentConfigError("config_root must be a directory")
        final_status = candidate.stat(follow_symlinks=False)
        if not stat.S_ISREG(final_status.st_mode):
            raise ExperimentConfigError("experiment YAML must be a regular file")
        return candidate, tuple(identities)

    @staticmethod
    def _read_config_handle(
        source: Path, identities: tuple[tuple[Path, _PathIdentity], ...]
    ) -> bytes:
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(source, flags)
        except OSError as error:
            raise ExperimentConfigError(
                "experiment YAML cannot be opened safely"
            ) from error
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise ExperimentConfigError("experiment YAML must be a regular file")
            if _ConfigSupport._path_identity(opened) != identities[-1][1]:
                raise ExperimentConfigError("experiment YAML changed before safe open")
            if opened.st_size > _MAX_YAML_BYTES:
                raise ExperimentConfigError("experiment YAML exceeds the size limit")
            payload = bytearray()
            while len(payload) <= _MAX_YAML_BYTES:
                remaining = _MAX_YAML_BYTES + 1 - len(payload)
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                payload.extend(chunk)
            after = os.fstat(descriptor)
        except OSError as error:
            raise ExperimentConfigError("experiment YAML cannot be read") from error
        finally:
            os.close(descriptor)
        if _ConfigSupport._path_identity(after) != _ConfigSupport._path_identity(
            opened
        ):
            raise ExperimentConfigError("experiment YAML changed while being read")
        if len(payload) > _MAX_YAML_BYTES:
            raise ExperimentConfigError("experiment YAML exceeds the size limit")
        _ConfigSupport._verify_config_identities(identities)
        return bytes(payload)

    @staticmethod
    def _verify_config_identities(
        identities: tuple[tuple[Path, _PathIdentity], ...],
    ) -> None:
        for component, expected in identities:
            try:
                observed = component.stat(follow_symlinks=False)
            except OSError as error:
                raise ExperimentConfigError(
                    "experiment YAML path changed while reading"
                ) from error
            if component.is_symlink() or _ConfigSupport._is_reparse(observed):
                raise ExperimentConfigError(
                    "experiment YAML path changed while reading"
                )
            if _ConfigSupport._path_identity(observed) != expected:
                raise ExperimentConfigError(
                    "experiment YAML path changed while reading"
                )

    @staticmethod
    def _path_identity(value: os.stat_result) -> _PathIdentity:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
            int(getattr(value, "st_file_attributes", 0)),
        )

    @staticmethod
    def _resolve_date(value: object, field: str) -> date:
        if isinstance(value, datetime):
            raise ExperimentConfigError("experiment dates must not contain times")
        if isinstance(value, date):
            return value
        text = _ConfigSupport._text(value, field)
        try:
            parsed = date.fromisoformat(text)
        except ValueError as error:
            raise ExperimentConfigError(f"{field} must be YYYY-MM-DD") from error
        if parsed.isoformat() != text:
            raise ExperimentConfigError("experiment date must use canonical YYYY-MM-DD")
        return parsed

    @staticmethod
    def _execution(value: object) -> dict[str, JsonValue]:
        mapping = _ConfigSupport._mapping(value, "execution")
        _ConfigSupport._exact_keys(
            mapping, allowed=_EXECUTION_KEYS, required=set(), label="execution"
        )
        reference = _ConfigSupport._text(
            mapping.get("reference_price", "OPEN"), "reference_price"
        )
        try:
            reference_price = ExecutionPrice(reference)
        except ValueError as error:
            raise ExperimentConfigError(
                "execution reference_price is invalid"
            ) from error
        slippage = _ConfigSupport._finite_nonnegative(
            mapping.get("slippage_bps", 5.0), "slippage_bps"
        )
        participation = _ConfigSupport._finite_nonnegative(
            mapping.get("max_volume_participation", 0.10),
            "max_volume_participation",
        )
        if participation <= 0.0 or participation > 1.0:
            raise ExperimentConfigError("max_volume_participation must be in (0, 1]")
        return {
            "max_volume_participation": participation,
            "reference_price": reference_price.value,
            "slippage_bps": slippage,
        }

    @staticmethod
    def _universe(value: object) -> dict[str, JsonValue]:
        mapping = _ConfigSupport._mapping(value, "universe")
        _ConfigSupport._exact_keys(
            mapping, allowed=_UNIVERSE_KEYS, required=set(), label="universe"
        )
        listing_days = mapping.get("min_listing_days", 120)
        if type(listing_days) is not int or listing_days < 0:
            raise ExperimentConfigError(
                "universe min_listing_days must be a nonnegative integer"
            )
        raw_boards = mapping.get("allowed_boards", ["MAIN", "CHINEXT", "STAR"])
        if not isinstance(raw_boards, list) or not raw_boards:
            raise ExperimentConfigError(
                "universe allowed_boards must be a nonempty list"
            )
        try:
            boards = tuple(
                Board(_ConfigSupport._text(item, "allowed_board"))
                for item in raw_boards
            )
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
            else _ConfigSupport._finite_nonnegative(minimum, "min_avg_amount_20d")
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

    @staticmethod
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

    @staticmethod
    def _mapping(value: object, field: str) -> dict[str, object]:
        if not isinstance(value, Mapping):
            raise ExperimentConfigError(f"{field} must be a mapping")
        if any(not isinstance(key, str) for key in value):
            raise ExperimentConfigError(f"{field} keys must be strings")
        return cast(dict[str, object], dict(value))

    @staticmethod
    def _text(value: object, field: str) -> str:
        if not isinstance(value, str) or not value or value != value.strip():
            raise ExperimentConfigError(f"{field} must be a nonempty trimmed string")
        return value

    @staticmethod
    def _nonempty_text(value: object, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a nonempty string")
        return value

    @staticmethod
    def _finite_nonnegative(value: object, field: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value < 0
        ):
            raise ExperimentConfigError(f"{field} must be finite and nonnegative")
        return float(value)

    @staticmethod
    def _plain_json(value: object) -> JsonValue:
        if isinstance(value, Mapping):
            result: dict[str, JsonValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                result[key] = _ConfigSupport._plain_json(item)
            return result
        if isinstance(value, list):
            return [_ConfigSupport._plain_json(item) for item in value]
        if value is None or isinstance(value, (str, bool)):
            return value
        if type(value) is int:
            return value
        if type(value) is float and isfinite(value):
            return value
        raise TypeError("value is not finite JSON")

    @staticmethod
    def _freeze_json(value: object) -> JsonValue:
        if isinstance(value, Mapping):
            return cast(
                JsonValue,
                MappingProxyType(
                    {
                        str(key): _ConfigSupport._freeze_json(item)
                        for key, item in sorted(value.items())
                    }
                ),
            )
        if isinstance(value, (list, tuple)):
            return cast(
                JsonValue, tuple(_ConfigSupport._freeze_json(item) for item in value)
            )
        if value is None or isinstance(value, (str, bool)):
            return value
        if type(value) is int:
            return value
        if type(value) is float and isfinite(value):
            return value
        raise TypeError("value is not finite JSON")

    @staticmethod
    def _thaw_json(value: object) -> JsonValue:
        if isinstance(value, Mapping):
            return {key: _ConfigSupport._thaw_json(item) for key, item in value.items()}
        if isinstance(value, tuple):
            return [_ConfigSupport._thaw_json(item) for item in value]
        if value is None or isinstance(value, (str, bool)):
            return value
        if type(value) is int:
            return value
        if type(value) is float and isfinite(value):
            return value
        raise TypeError("frozen value is not finite JSON")

    @staticmethod
    def _is_sha256(value: object) -> bool:
        return (
            isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        )

    @staticmethod
    def _is_reparse(status: os.stat_result) -> bool:
        return bool(getattr(status, "st_file_attributes", 0) & 0x400)


def require_provider_capabilities(
    capabilities: ProviderCapabilities,
    required: Sequence[str],
    *,
    provider: str,
    stage: str,
) -> None:
    """取得并确认数据供应商数据能力集合；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        capabilities：当前数据源确实支持的数据集和字段能力。
        required：参与本次处理的``required``；调用方不得依赖未声明的顺序。
        provider：数据供应商。
        stage：执行阶段。
    返回值：
        无。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``ExperimentCapabilityUnavailable``、``TypeError``、``ValueError``。
    """
    if not isinstance(capabilities, ProviderCapabilities):
        raise TypeError("capabilities must be ProviderCapabilities")
    provider_name = _ConfigSupport._nonempty_text(provider, "provider")
    stage_name = _ConfigSupport._nonempty_text(stage, "stage")
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


type _PathIdentity = tuple[int, int, int, int, int, int]


__all__ = [
    "ExperimentCapabilityUnavailable",
    "ExperimentCatalog",
    "ExperimentConfigError",
    "ResolvedExperimentConfig",
    "require_provider_capabilities",
    "resolve_experiment_yaml",
]
