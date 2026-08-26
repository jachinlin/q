"""提供 Dashboard 类型化运行设置的安全应用边界。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit


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


@dataclass(frozen=True, slots=True)
class DataSourceRateLimitSetting:
    """表示 Tushare 每分钟请求上限及其非敏感来源证据。

    入参：每分钟请求数、解析来源和可选文件更新时间。
    返回值：构造可安全展示的不可变限流设置。
    异常：构造阶段不主动校验；外部值应先调用 ``validated_value``。
    """

    DEFAULT_REQUESTS_PER_MINUTE = 480
    MIN_REQUESTS_PER_MINUTE = 1
    MAX_REQUESTS_PER_MINUTE = 10_000

    requests_per_minute: int
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "DEFAULT"]
    updated_at: datetime | None

    @classmethod
    def validated_value(cls, value: object) -> int:
        """校验每分钟请求数。

        入参：来自 API、进程环境或数据根文件的候选值。
        返回值：范围为 1 至 10000 的严格整数。
        异常：值不是严格整数或超出范围时抛出类型或值错误。
        """
        if type(value) is not int:
            raise TypeError("data source requests per minute must be an integer")
        if not cls.MIN_REQUESTS_PER_MINUTE <= value <= cls.MAX_REQUESTS_PER_MINUTE:
            raise ValueError(
                "data source requests per minute must be from 1 through 10000"
            )
        return value


@dataclass(frozen=True, slots=True)
class DataSourceProxySetting:
    """表示 Tushare HTTP 代理入口及其非敏感来源证据。

    入参：规范代理 URL、来源和可选更新时间。
    返回值：构造可供 SDK 内部使用和 Dashboard 展示的不可变设置。
    异常：构造阶段不主动校验；外部值应先调用 ``validated_value``。
    """

    value: str | None
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "NONE"]
    updated_at: datetime | None

    @staticmethod
    def validated_value(value: object) -> str:
        """校验并规范化 Tushare HTTP 代理 URL。

        入参：来自 API、进程环境或数据根文件的候选 URL。
        返回值：移除末尾斜杠的 HTTP 或 HTTPS URL。
        异常：类型、长度、结构或安全约束非法时抛出类型或值错误。
        """
        if not isinstance(value, str):
            raise TypeError("data source proxy URL must be a string")
        if not value or len(value) > 2048 or value != value.strip():
            raise ValueError("data source proxy URL length or whitespace is invalid")
        if any(character.isspace() or ord(character) < 32 for character in value):
            raise ValueError("data source proxy URL contains whitespace or controls")
        parsed = urlsplit(value)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise ValueError("data source proxy URL must use HTTP or HTTPS with a host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("data source proxy URL must not contain credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("data source proxy URL must not contain query or fragment")
        return value.rstrip("/")


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

    def read_data_source_rate_limit(self) -> DataSourceRateLimitSetting:
        """读取数据根优先、进程环境回退、最终使用默认值的限流设置。

        入参：无。
        返回值：完成来源解析的每分钟请求上限。
        异常：文件、环境变量或数值非法时传播对应异常。
        """
        ...

    def read_data_source_proxy(self) -> DataSourceProxySetting:
        """读取数据根优先、进程环境回退的 Tushare 代理 URL。

        入参：无。
        返回值：代理 URL 与来源证据；未配置时值为空。
        异常：文件、环境变量或 URL 非法时传播对应异常。
        """
        ...

    def apply_changes(
        self,
        *,
        token_operation: Literal["SET", "CLEAR"] | None,
        token_value: str | None,
        rate_limit_operation: Literal["SET", "CLEAR"] | None,
        requests_per_minute: int | None,
        proxy_operation: Literal["SET", "CLEAR"] | None,
        proxy_url: str | None,
    ) -> tuple[
        DataSourceTokenSetting,
        DataSourceRateLimitSetting,
        DataSourceProxySetting,
    ]:
        """原子应用一个或多个已经校验的类型化设置修改。

        入参：Token、限流和代理各自可选的操作和值。
        返回值：写入后重新解析的 Token、限流和代理状态。
        异常：组合非法或原子发布失败时传播对应异常。
        """
        ...


class DashboardSettingsService:
    """提供不回显敏感值的 Dashboard 设置查询和修改用例。

    入参：实现可信根与 dotenv 不变量的运行设置端口。
    返回值：构造可供 HTTP 接口调用的 Token 与限流类型化设置服务。
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
        return self._safe_view(
            self._store.read_data_source_token(),
            self._store.read_data_source_rate_limit(),
            self._store.read_data_source_proxy(),
        )

    def change(
        self,
        *,
        token_operation: Literal["SET", "CLEAR"] | None,
        token_value: str | None,
        rate_limit_operation: Literal["SET", "CLEAR"] | None,
        requests_per_minute: int | None,
        proxy_operation: Literal["SET", "CLEAR"] | None,
        proxy_url: str | None,
    ) -> dict[str, object]:
        """原子修改 Token、请求限流和/或代理并返回安全投影。

        入参：三个可选设置的操作和值；至少一个操作必须存在。
        返回值：写入后的完整安全设置投影。
        异常：组合非法时抛出类型或值错误；存储失败保持原语义。
        """
        if (
            token_operation is None
            and rate_limit_operation is None
            and proxy_operation is None
        ):
            raise ValueError("settings change must contain at least one operation")
        validated_token = token_value
        if token_operation == "SET":
            validated_token = DataSourceTokenSetting.validated_value(token_value)
        elif token_operation == "CLEAR":
            if token_value is not None:
                raise ValueError("CLEAR data source token operation must omit value")
        elif token_operation is not None:
            raise ValueError("unsupported data source token operation")

        validated_rate = requests_per_minute
        if rate_limit_operation == "SET":
            validated_rate = DataSourceRateLimitSetting.validated_value(
                requests_per_minute
            )
        elif rate_limit_operation == "CLEAR":
            if requests_per_minute is not None:
                raise ValueError("CLEAR data source rate limit must omit a value")
        elif rate_limit_operation is not None:
            raise ValueError("unsupported data source rate limit operation")

        validated_proxy = proxy_url
        if proxy_operation == "SET":
            validated_proxy = DataSourceProxySetting.validated_value(proxy_url)
        elif proxy_operation == "CLEAR":
            if proxy_url is not None:
                raise ValueError("CLEAR data source proxy must omit a URL")
        elif proxy_operation is not None:
            raise ValueError("unsupported data source proxy operation")

        token, rate, proxy = self._store.apply_changes(
            token_operation=token_operation,
            token_value=validated_token,
            rate_limit_operation=rate_limit_operation,
            requests_per_minute=validated_rate,
            proxy_operation=proxy_operation,
            proxy_url=validated_proxy,
        )
        return self._safe_view(token, rate, proxy)

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
            token, rate, proxy = self._store.apply_changes(
                token_operation="SET",
                token_value=validated,
                rate_limit_operation=None,
                requests_per_minute=None,
                proxy_operation=None,
                proxy_url=None,
            )
            return self._safe_view(token, rate, proxy)
        if operation == "CLEAR":
            if value is not None:
                raise ValueError("CLEAR data source token operation must omit value")
            token, rate, proxy = self._store.apply_changes(
                token_operation="CLEAR",
                token_value=None,
                rate_limit_operation=None,
                requests_per_minute=None,
                proxy_operation=None,
                proxy_url=None,
            )
            return self._safe_view(token, rate, proxy)
        raise ValueError("unsupported data source token operation")

    def _safe_view(
        self,
        token: DataSourceTokenSetting,
        rate_limit: DataSourceRateLimitSetting,
        proxy: DataSourceProxySetting,
    ) -> dict[str, object]:
        return {
            "settings_path": str(self._store.settings_path),
            "data_source_token": {
                "configured": token.value is not None,
                "source": token.source,
                "updated_at": (
                    None if token.updated_at is None else token.updated_at.isoformat()
                ),
            },
            "data_source_rate_limit": {
                "requests_per_minute": rate_limit.requests_per_minute,
                "source": rate_limit.source,
                "updated_at": (
                    None
                    if rate_limit.updated_at is None
                    else rate_limit.updated_at.isoformat()
                ),
            },
            "data_source_proxy": {
                "url": proxy.value,
                "source": proxy.source,
                "updated_at": (
                    None if proxy.updated_at is None else proxy.updated_at.isoformat()
                ),
            },
        }
