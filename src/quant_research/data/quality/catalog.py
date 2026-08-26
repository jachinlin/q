"""定义数据质量规则的不可变目录与展示说明。"""

from __future__ import annotations

from dataclasses import dataclass

from quant_research.data.quality.rules import FOUNDATION_REQUIRED_DATASETS
from quant_research.domain.enums import DatasetKind, Severity

_ALL_DATASETS = tuple(sorted(FOUNDATION_REQUIRED_DATASETS, key=lambda item: item.value))
_BAR_DATASETS = (
    DatasetKind.STOCK_DAILY_BAR,
    DatasetKind.FUND_DAILY_BAR,
    DatasetKind.INDEX_DAILY_BAR,
)


@dataclass(frozen=True, slots=True)
class QualityRuleDefinition:
    """描述质量规则的稳定身份、适用范围及运行前置条件。

    入参：由字段声明给出。返回值：构造冻结目录项。异常：字段为空时抛出 ``ValueError``。
    """

    rule_id: str
    title: str
    description: str
    pass_criterion: str
    severity: Severity
    datasets: tuple[DatasetKind, ...]
    prerequisite_rules: tuple[str, ...] = ()
    prerequisite_datasets: tuple[DatasetKind, ...] = ()

    def __post_init__(self) -> None:
        if not all((self.rule_id, self.title, self.description, self.pass_criterion)):
            raise ValueError("quality rule definition text fields must not be empty")
        if not self.datasets:
            raise ValueError("quality rule definition must apply to a dataset")


QUALITY_RULE_CATALOG: tuple[QualityRuleDefinition, ...] = (
    QualityRuleDefinition(
        "required_dataset_missing",
        "基础数据集存在",
        "确认质量运行范围内的数据集已完成 Canonical 发布。",
        "至少存在一个 Canonical 分区。",
        Severity.FATAL,
        _ALL_DATASETS,
    ),
    QualityRuleDefinition(
        "required_dataset_empty",
        "基础数据集非空",
        "确认 Canonical 数据集包含可校验记录。",
        "数据集总行数至少为 1。",
        Severity.FATAL,
        _ALL_DATASETS,
        prerequisite_rules=("required_dataset_missing",),
    ),
    QualityRuleDefinition(
        "canonical_schema",
        "Canonical Schema 一致",
        "逐分区核对字段名称、顺序和类型是否匹配声明契约。",
        "Schema 不匹配分区数为 0。",
        Severity.FATAL,
        _ALL_DATASETS,
        prerequisite_rules=("required_dataset_missing",),
    ),
    QualityRuleDefinition(
        "trading_window_empty",
        "交易窗口包含开市日",
        "确认交易日历覆盖的流水线窗口内至少有一个开市日。",
        "开市日数量至少为 1。",
        Severity.FATAL,
        (DatasetKind.TRADE_CALENDAR,),
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "cross_partition_schema",
        "跨分区 Schema 一致",
        "核对同一数据集各分区之间的 Schema 是否完全一致。",
        "跨分区 Schema 不一致数为 0。",
        Severity.FATAL,
        _ALL_DATASETS,
        prerequisite_rules=("required_dataset_missing",),
    ),
    QualityRuleDefinition(
        "primary_key_duplicate",
        "主键唯一",
        "检查 Canonical 主键组合是否出现重复记录。",
        "重复主键组合数为 0。",
        Severity.FATAL,
        _ALL_DATASETS,
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "required_value_null",
        "关键字段完整",
        "检查研究所需字段是否存在不允许的空值。",
        "关键字段空值数为 0。",
        Severity.SEVERE,
        _ALL_DATASETS,
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "positive_finite_price",
        "交易价格有限且为正",
        "检查已交易日行情的开盘、最高、最低和收盘价格是否有限且严格大于零。",
        "非法交易价格记录数为 0。",
        Severity.SEVERE,
        _BAR_DATASETS,
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "ohlc_relationship",
        "OHLC 价格关系有效",
        "检查最高价和最低价是否包络开盘价与收盘价。",
        "违反 OHLC 关系的记录数为 0。",
        Severity.SEVERE,
        _BAR_DATASETS,
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "negative_volume",
        "成交量非负",
        "检查已交易日行情的成交量是否出现负值。",
        "负成交量记录数为 0。",
        Severity.SEVERE,
        _BAR_DATASETS,
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "trading_day_coverage",
        "交易日覆盖完整",
        "比较日行情日期与交易日历，识别行情最新日期之前缺失的开市日。",
        "缺失开市日数量为 0。",
        Severity.SEVERE,
        (DatasetKind.STOCK_DAILY_BAR,),
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
        prerequisite_datasets=(DatasetKind.TRADE_CALENDAR,),
    ),
    QualityRuleDefinition(
        "instrument_coverage",
        "证券主数据覆盖完整",
        "检查日行情中的证券代码是否都存在于证券主数据。",
        "未知证券代码数量为 0。",
        Severity.SEVERE,
        (DatasetKind.STOCK_DAILY_BAR,),
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
        prerequisite_datasets=(DatasetKind.STOCK_MASTER,),
    ),
    QualityRuleDefinition(
        "financial_availability",
        "财务可用时间有效",
        "检查可用于 PIT 研究的财务观测是否具有有效公告与可用时间。",
        "可用时间证据非法的记录数为 0。",
        Severity.SEVERE,
        (DatasetKind.STOCK_FINANCIAL_INDICATOR,),
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
    QualityRuleDefinition(
        "industry_state",
        "行业 PIT 状态有效",
        "检查行业事件日期、tombstone 与供应商重建可用性证据。",
        "非法行业事件数为 0。",
        Severity.FATAL,
        (DatasetKind.INDUSTRY_MEMBERSHIP,),
        prerequisite_rules=("canonical_schema", "required_dataset_empty"),
    ),
)


class QualityRuleCatalog:
    """提供质量规则目录的确定性查询。

    入参：无。返回值：构造无状态目录查询器。异常：无主动抛出的异常。
    """

    @staticmethod
    def for_datasets(
        datasets: tuple[DatasetKind, ...],
    ) -> tuple[QualityRuleDefinition, ...]:
        """按目录顺序返回至少适用于一个目标数据集的规则。

        入参：目标数据集。返回值：冻结目录项元组。异常：无主动抛出的异常。
        """
        selected = frozenset(datasets)
        return tuple(
            definition
            for definition in QUALITY_RULE_CATALOG
            if selected.intersection(definition.datasets)
        )
