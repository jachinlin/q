"""定义 LOCALIZE 阶段从数据集到供应商的静态路由。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from quant_research.data.catalog import DATASET_CATALOG, DatasetCatalog
from quant_research.domain.enums import DatasetKind


@dataclass(frozen=True, slots=True, order=True)
class Route:
    """描述一个数据集到供应商及端点集合的静态路由。

    入参：
        priority：构造对象所需的同名字段，约束见类型标注。
        source：供应商标识。
    返回值：
        构造并返回 ``Route`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    priority: int
    source: str

    def __post_init__(self) -> None:
        if self.priority < 1 or not self.source:
            raise ValueError("route requires a positive priority and source")


class RoutingTable(Mapping[DatasetKind, tuple[Route, ...]]):
    """保存覆盖全部数据集的不可变路由表。

    入参：
        routes：构造对象所需的同名字段，约束见类型标注。
        catalog：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``RoutingTable`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(
        self,
        routes: Mapping[DatasetKind, tuple[Route, ...]],
        *,
        catalog: DatasetCatalog = DATASET_CATALOG,
    ) -> None:
        if set(routes) != set(catalog):
            raise ValueError("routing table must mention every dataset")
        normalized: dict[DatasetKind, tuple[Route, ...]] = {}
        for dataset, entries in routes.items():
            ordered = tuple(sorted(entries))
            if len({item.priority for item in ordered}) != len(ordered):
                raise ValueError(f"duplicate priority for {dataset.value}")
            supported = catalog[dataset].source_endpoints
            if any(item.source not in supported for item in ordered):
                raise ValueError(f"route source cannot supply {dataset.value}")
            normalized[dataset] = ordered
        self._routes = MappingProxyType(normalized)

    def __getitem__(self, key: DatasetKind) -> tuple[Route, ...]:
        """按键读取集合元素。

        入参：
            key：``key``。
        返回值：
            返回``getitem``（``tuple[Route, ...]``）。
        异常：
            无。
        """
        return self._routes[key]

    def __iter__(self) -> Iterator[DatasetKind]:
        """返回集合迭代器。

        入参：
            无。
        返回值：
            返回``iter``（``Iterator[DatasetKind]``）。
        异常：
            无。
        """
        return iter(self._routes)

    def __len__(self) -> int:
        """返回集合元素数量。

        入参：
            无。
        返回值：
            返回``len``（``int``）。
        异常：
            无。
        """
        return len(self._routes)

    def source_for(self, dataset: DatasetKind) -> str:
        """返回指定数据集唯一配置的数据源。

        入参：
            dataset：目标 Canonical 数据集标识。
        返回值：
            返回``for``（``str``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        routes = self[dataset]
        if not routes:
            raise ValueError(f"dataset {dataset.value} has no enabled source")
        return routes[0].source
