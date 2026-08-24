"""提供不绑定运行时数据依赖的稳定因子引用目录。"""

from __future__ import annotations


class FactorReferenceCatalog:
    """保存确定性因子目录。入参：有序引用。返回值：目录实例。异常：顺序或引用非法时抛出值错误。"""

    def __init__(self, references: tuple[str, ...]) -> None:
        if tuple(sorted(set(references))) != references or any(
            not item for item in references
        ):
            raise ValueError("factor references must be unique, nonempty and sorted")
        self._references = references

    def resolve(self, reference: str) -> str:
        """解析因子引用。入参：引用。返回值：规范引用。异常：未知引用时抛出值错误。"""
        if reference not in self._references:
            raise ValueError(f"unknown factor: {reference}")
        return reference

    def registered_references(self) -> tuple[str, ...]:
        """列出因子引用。入参：无。返回值：有序引用。异常：无。"""
        return self._references


__all__ = ["FactorReferenceCatalog"]
