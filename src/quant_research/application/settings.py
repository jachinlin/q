"""提供 Dashboard 类型化运行设置的安全应用边界。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol


@dataclass(frozen=True, slots=True)
class DataSourceTokenSetting:
    """表示解析后的数据源 Token 及其非敏感来源证据。

    入参：明文值、来源和可选更新时间。
    返回值：构造只在应用进程内部传递的不可变设置；展示层不得序列化 ``value``。
    异常：构造阶段不主动校验；外部值应先调用 ``validated_value``。
    """

    value: str | None
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "NONE"]
    updated_at: datetime | None

    @staticmethod
    def validated_value(value: object) -> str:
        """校验可安全写入单行 dotenv 的数据源 Token。

        入参：来自 API、环境变量或数据根文件的候选值。
        返回值：未修改的合法 Token。
        异常：值不是字符串、为空、过长或含有 dotenv 注入字符时抛出类型或值错误。
        """
        if not isinstance(value, str):
            raise TypeError("data source token must be a string")
        if not value or len(value) > 512:
            raise ValueError("data source token length must be from 1 through 512")
        if value != value.strip():
            raise ValueError("data source token must not contain surrounding whitespace")
        if any(not (character.isascii() and (character.isalnum() or character in "._-")) for character in value):
            raise ValueError("data source token contains unsupported characters")
        return value


class RuntimeSettingsPort(Protocol):
    """定义 Dashboard 支持的类型化环境设置持久化端口。

    入参：由各方法签名给出。
    返回值：解析后的内部设置或完成写入后的状态。
    异常：可信根、文件格式或持久化失败时传播类型、值或文件系统异常。
    """

    @property
    def settings_path(self) -> Path:
        """返回受控的数据根 dotenv 路径。

        入参：无。
        返回值：唯一允许读写的设置文件绝对路径。
        异常：可信根状态失效时由实现抛出值错误。
        """
        ...

    def read_data_source_token(self) -> DataSourceTokenSetting:
        """读取 Dashboard 文件优先、进程环境变量回退的数据源 Token。

        入参：无。
        返回值：包含内部明文和非敏感来源证据的冻结设置。
        异常：文件格式、路径或候选值非法时由实现传播对应异常。
        """
        ...

    def write_data_source_token(self, value: str) -> DataSourceTokenSetting:
        """原子写入数据根 dotenv 并返回新的解析状态。

        入参：已完成单行安全校验的数据源 Token。
        返回值：以数据根文件为来源的新设置。
        异常：既有文件损坏或原子发布失败时由实现传播对应异常。
        """
        ...

    def clear_data_source_token(self) -> DataSourceTokenSetting:
        """清除 Dashboard 管理值并返回可能回退到环境变量的状态。

        入参：无。
        返回值：删除后重新按优先级解析的设置。
        异常：文件格式、路径或删除失败时由实现传播对应异常。
        """
        ...


class DashboardSettingsService:
    """提供不回显敏感值的 Dashboard 设置查询和修改用例。

    入参：实现可信根与 dotenv 不变量的运行设置端口。
    返回值：构造可供 HTTP 接口调用的类型化设置服务。
    异常：存储损坏、输入非法或写入失败时保持原异常语义。
    """

    def __init__(self, store: RuntimeSettingsPort) -> None:
        self._store = store

    def view(self) -> dict[str, object]:
        """返回不含 Token 明文、掩码或后缀的安全设置投影。

        入参：无。
        返回值：设置文件路径和数据源 Token 的配置状态、来源及更新时间。
        异常：底层文件无效或不可访问时传播对应异常。
        """
        return self._safe_view(self._store.read_data_source_token())

    def change_data_source_token(
        self,
        *,
        operation: Literal["SET", "CLEAR"],
        value: str | None,
    ) -> dict[str, object]:
        """按严格操作修改数据源 Token，并仅返回安全状态。

        入参：``SET`` 携带明文值；``CLEAR`` 必须省略值。
        返回值：修改后的安全设置投影。
        异常：操作和值组合非法时抛出类型或值错误；写入失败保持原语义。
        """
        if operation == "SET":
            validated = DataSourceTokenSetting.validated_value(value)
            return self._safe_view(self._store.write_data_source_token(validated))
        if operation == "CLEAR":
            if value is not None:
                raise ValueError("CLEAR data source token operation must omit value")
            return self._safe_view(self._store.clear_data_source_token())
        raise ValueError("unsupported data source token operation")

    def _safe_view(self, setting: DataSourceTokenSetting) -> dict[str, object]:
        return {
            "settings_path": str(self._store.settings_path),
            "data_source_token": {
                "configured": setting.value is not None,
                "source": setting.source,
                "updated_at": (
                    None if setting.updated_at is None else setting.updated_at.isoformat()
                ),
            },
        }
