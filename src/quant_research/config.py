"""提供python-module-conventions与应用配置相关的公开模型、协议与处理流程。"""

import os
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """定义量化研究流程使用的不可变配置及取值约束。

    入参：
        timezone：解析自然日边界和展示本地时间时使用的 IANA 时区。
        data_root：所有派生路径必须位于其中的数据可信根目录。
        max_partition_size：限制资源使用、数量或等待时间的上限分区字节数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Runtime paths and validated application settings.
    """

    model_config = SettingsConfigDict(extra="forbid")

    timezone: ZoneInfo
    data_root: Path
    max_partition_size: int = 100

    @field_validator("timezone", mode="before")
    @classmethod
    def validate_timezone(cls, value: str | ZoneInfo) -> ZoneInfo:
        """校验时区。

        入参：
            value：待校验或转换的值，类型为 ``str | ZoneInfo``。
        返回值：
            返回校验时区后的时区（``ZoneInfo``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if isinstance(value, ZoneInfo):
            return value
        try:
            return ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown timezone: {value}") from error

    @field_validator("max_partition_size", mode="before")
    @classmethod
    def validate_max_partition_size(cls, value: object) -> int:
        """校验上限分区字节数。

        入参：
            value：待校验或转换的值，类型为 ``object``。
        返回值：
            返回校验上限分区字节数后的上限分区字节数（``int``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if type(value) is not int or value <= 0 or value > 100:
            raise ValueError("max_partition_size must be an integer from 1 through 100")
        return value

    @property
    def raw_root(self) -> Path:
        """处理量化研究中的``raw``可信根目录。

        入参：
            无。
        返回值：
            返回 Raw 内容寻址对象和请求索引的存储根目录。
        异常：
            无。
        """
        return self.data_root / "raw"

    @property
    def curated_root(self) -> Path:
        """处理量化研究中的Curated 数据可信根目录。

        入参：
            无。
        返回值：
            返回 Canonical 分区及其 Manifest 的存储根目录。
        异常：
            无。
        """
        return self.data_root / "canonical"

    @property
    def artifact_root(self) -> Path:
        """处理量化研究中的产物可信根目录。

        入参：
            无。
        返回值：
            返回不可变实验和因子产物的存储根目录。
        异常：
            无。
        """
        return self.data_root / "artifacts"

    @property
    def state_db(self) -> Path:
        """返回任务、数据目录和实验元数据共用的 SQLite 文件路径。

        入参：
            无。
        返回值：
            返回任务、数据目录和实验元数据共用的 SQLite 文件路径。
        异常：
            无。
        """
        return self.data_root / "state" / "quant.db"

    @classmethod
    def load(
        cls,
        config_path: Path | None = None,
        data_root: Path | None = None,
    ) -> "Settings":
        """加载并校验约定资源。

        入参：
            config_path：应用 YAML 路径；省略时读取 ``QUANT_CONFIG``，否则使用项目默认配置。
            data_root：运行数据根目录；省略时读取 ``QUANT_DATA_ROOT``，否则使用 ``~/.q-data``。
        返回值：
            返回加载并校验量化研究后的``load``（``'Settings'``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Load YAML settings after ensuring runtime data is outside source tree.
        """
        resolved_source_tree = Path(__file__).resolve().parents[2]
        configured_path = os.environ.get("QUANT_CONFIG")
        effective_config_path = config_path if config_path is not None else (
            Path(configured_path)
            if configured_path
            else resolved_source_tree / "configs" / "base.yaml"
        )
        configured_data_root = os.environ.get("QUANT_DATA_ROOT")
        effective_data_root = data_root if data_root is not None else (
            Path(configured_data_root)
            if configured_data_root
            else Path.home() / ".q-data"
        )
        resolved_data_root = effective_data_root.resolve()
        if resolved_data_root.is_relative_to(resolved_source_tree):
            raise ValueError("data_root must be outside source tree")

        loaded = yaml.safe_load(effective_config_path.read_text(encoding="utf-8"))
        if loaded is None:
            loaded = {}
        if not isinstance(loaded, dict):
            raise TypeError("configuration must be a YAML mapping")

        return cls(**loaded, data_root=resolved_data_root)
