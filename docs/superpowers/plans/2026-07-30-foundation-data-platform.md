# 工程基础与数据平台实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 建立可在 Windows 单机运行的 Python 工程，并打通经过测试的 `BaoStock → Raw Parquet → CanonicalMapper → Curated Parquet → 数据快照` 主链路。

**架构：** 采用模块化单体。BaoStock 采集器只产生不可变 `RawBatch`；Raw 分区发布后，规范映射器再读取并转换为供应商无关的 Curated 数据；SQLite 记录数据版本、质量检查和快照清单。因子、策略、回测、实验 Worker 与 Dashboard 不属于本计划。

**技术栈：** Python 3.12、uv、Pydantic v2、Polars、PyArrow、DuckDB、SQLAlchemy 2、Alembic、Typer、pytest、Ruff、mypy。

## 全局约束

- 目标环境为 Windows、本地单用户，不开放公网服务。
- 运行数据统一位于 `QUANT_DATA_ROOT`，不得提交 Git。
- Raw 数据保持供应商原貌且只写一次；规范化只能发生在 Raw 发布之后。
- 首个数据源为 BaoStock，但领域层、因子和策略不得出现 BaoStock 专有字段。
- 时间戳使用带时区 UTC 存储，展示层转换为 `Asia/Shanghai`。
- 已发布的数据版本和快照不可修改，快照通过内容哈希引用数据版本。
- 严重或致命数据质量问题必须阻止快照发布。
- 当前目录还不是 Git 仓库；任务 1 初始化仓库。

---

## 文件职责

| 路径 | 作用 |
|---|---|
| `pyproject.toml` | 包元数据、依赖、工具配置与 CLI 入口 |
| `.gitignore` | 排除密钥、运行状态、数据、产物与缓存 |
| `.env.example` | 记录环境变量名，不保存真实密钥 |
| `configs/base.yaml` | 本地路径、时区、日志等基础配置 |
| `src/quant_core/settings.py` | 类型化配置加载与目录校验 |
| `src/quant_core/domain/identifiers.py` | 稳定证券标识和运行标识 |
| `src/quant_core/domain/enums.py` | 交易所、板块、严重度等枚举 |
| `src/quant_core/errors.py` | 结构化错误和领域异常 |
| `src/quant_core/data/contracts.py` | `SourceClient`、`RawBatch`、`CanonicalMapper` 协议 |
| `src/quant_core/data/partitions.py` | 分区路径、内容哈希与原子发布 |
| `src/quant_core/data/sources/baostock.py` | BaoStock 登录、分页、分块与采集 |
| `src/quant_core/data/mappers/baostock.py` | BaoStock Raw 到规范模型的映射 |
| `src/quant_core/data/quality/*` | 数据质量规则、问题模型与阻断判定 |
| `src/quant_core/persistence/*` | SQLite 引擎、ORM、迁移和仓储 |
| `src/quant_core/data/snapshots.py` | 快照清单组装和发布 |
| `src/quant_core/data/pipelines/*` | 采集、映射、校验、发布编排 |
| `src/quant_core/cli.py` | 数据初始化、更新、校验、发布命令 |

### 任务 1：项目骨架与类型化配置

**文件：**
- 新建：`.gitignore`
- 新建：`.env.example`
- 新建：`pyproject.toml`
- 新建：`configs/base.yaml`
- 新建：`src/quant_core/__init__.py`
- 新建：`src/quant_core/settings.py`
- 新建：`tests/unit/test_settings.py`

**接口：** `Settings.load(config_path: Path, *, data_root: Path, source_root: Path | None = None) -> Settings`。

- [ ] **步骤 1：初始化 Git，并先写失败测试**

运行：`git init`

在 `tests/unit/test_settings.py` 中覆盖以下断言：

```python
def test_settings_resolves_runtime_paths_under_data_root(tmp_path: Path) -> None:
    config = tmp_path / "base.yaml"
    config.write_text("timezone: Asia/Shanghai\n", encoding="utf-8")
    settings = Settings.load(config, data_root=tmp_path / "runtime")
    assert settings.raw_root == tmp_path / "runtime" / "data" / "raw"
    assert settings.state_db == tmp_path / "runtime" / "state" / "quant.db"

def test_settings_rejects_data_root_inside_source_tree(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside source_root"):
        Settings.load(tmp_path / "base.yaml", data_root=tmp_path / "repo" / "data", source_root=tmp_path / "repo")
```

- [ ] **步骤 2：确认测试因模块不存在而失败**

运行：`uv run pytest tests/unit/test_settings.py -v`

预期：出现 `ModuleNotFoundError: No module named 'quant_core'`。

- [ ] **步骤 3：实现最小工程配置和 `Settings`**

`pyproject.toml` 固定 Python `>=3.12,<3.13`；运行依赖包含 `pydantic`、`pydantic-settings`、`pyyaml`、`polars`、`pyarrow`、`duckdb`、`sqlalchemy`、`alembic`、`typer`、`baostock`；开发依赖包含 `pytest`、`pytest-cov`、`ruff`、`mypy`。配置 `quant = "quant_core.cli:app"` 和 `src` 布局。

`Settings` 至少提供 `raw_root`、`curated_root`、`feature_root`、`artifact_root`、`state_db`；使用 `Path.resolve()` 校验运行目录不在源码树内，并用 `ZoneInfo` 校验时区。

- [ ] **步骤 4：安装依赖并验证**

运行：`uv sync --all-groups`

运行：`uv run pytest tests/unit/test_settings.py -v`

运行：`uv run ruff check src tests && uv run mypy src`

预期：测试全部通过，静态检查退出码为 0。

- [ ] **步骤 5：提交**

```bash
git add .gitignore .env.example pyproject.toml uv.lock configs/base.yaml src/quant_core tests/unit/test_settings.py
git commit -m "chore: bootstrap quant research project"
```

### 任务 2：领域标识、枚举与结构化错误

**文件：**
- 新建：`src/quant_core/domain/__init__.py`
- 新建：`src/quant_core/domain/identifiers.py`
- 新建：`src/quant_core/domain/enums.py`
- 新建：`src/quant_core/errors.py`
- 新建：`tests/unit/domain/test_identifiers.py`
- 新建：`tests/unit/test_errors.py`

**接口：** `InstrumentId.parse()`、`InstrumentId.canonical()`、`InstrumentId.to_baostock()`、`ErrorDetail`、`QuantError`。

- [ ] **步骤 1：先写标识符往返和错误保真测试**

```python
def test_instrument_id_round_trip() -> None:
    instrument = InstrumentId.parse("SSE:600000")
    assert instrument.canonical() == "SSE:600000"
    assert instrument.to_baostock() == "sh.600000"

def test_quant_error_preserves_detail() -> None:
    detail = ErrorDetail(code="DATA_SCHEMA_MISMATCH", severity=Severity.SEVERE, message="schema mismatch", context={"dataset": "daily_bar"}, remediation="inspect raw schema", retryable=False)
    error = QuantError(detail)
    assert error.detail == detail
```

- [ ] **步骤 2：运行测试并确认失败**

运行：`uv run pytest tests/unit/domain/test_identifiers.py tests/unit/test_errors.py -v`

- [ ] **步骤 3：实现领域类型**

实现 `Exchange`、`Board`、`Severity`、`DatasetKind`、`SnapshotStatus`。`InstrumentId` 必须是不可变值对象，严格校验六位代码和交易所；供应商代码转换只能位于数据边界。`ErrorDetail` 必须包含错误码、严重度、消息、上下文、修复建议和是否可重试。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/domain tests/unit/test_errors.py -v`

```bash
git add src/quant_core/domain src/quant_core/errors.py tests/unit/domain tests/unit/test_errors.py
git commit -m "feat: add domain identifiers and structured errors"
```

### 任务 3：数据源契约与 Raw 原子发布

**文件：**
- 新建：`src/quant_core/data/__init__.py`
- 新建：`src/quant_core/data/contracts.py`
- 新建：`src/quant_core/data/partitions.py`
- 新建：`tests/unit/data/test_contracts.py`
- 新建：`tests/unit/data/test_partitions.py`

**核心接口：**

```python
@dataclass(frozen=True)
class ProviderCapabilities:
    daily_bars: bool
    trade_calendar: bool
    instruments: bool
    security_status: bool
    financials_with_announcement_date: bool
    corporate_actions: bool
    adjustment_factors: bool

@dataclass(frozen=True)
class RawBatch:
    provider: str
    dataset: str
    request: Mapping[str, JsonValue]
    retrieved_at: datetime
    schema: tuple[str, ...]
    rows: Sequence[Mapping[str, JsonValue]]

class SourceClient(Protocol):
    def fetch_daily_bars(self, start: date, end: date, instruments: Sequence[InstrumentId] | None = None) -> Iterable[RawBatch]: ...

class CanonicalMapper(Protocol):
    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]: ...
```

- [ ] **步骤 1：写失败测试**

验证 `retrieved_at` 必须带时区、请求参数可规范序列化、相同 Arrow 表产生相同 SHA-256、发布前文件不可见、发布后只出现最终 `.parquet` 和 `.manifest.json`，重复写相同分区幂等，内容不同则拒绝覆盖。

- [ ] **步骤 2：运行测试**

运行：`uv run pytest tests/unit/data/test_contracts.py tests/unit/data/test_partitions.py -v`

预期：因接口和发布器不存在而失败。

- [ ] **步骤 3：实现契约和原子写入**

先在同一目标卷写临时文件，计算内容哈希并校验行数/schema，再通过 `Path.replace()` 发布；清单包含 `provider`、`dataset`、`request_hash`、`content_hash`、`row_count`、`schema_fingerprint`、`retrieved_at`。任何异常都不得留下已发布清单。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/data/test_contracts.py tests/unit/data/test_partitions.py -v`

```bash
git add src/quant_core/data tests/unit/data
git commit -m "feat: define source contracts and atomic raw storage"
```

### 任务 4：BaoStock 采集器与全市场语义

**文件：**
- 新建：`src/quant_core/data/sources/__init__.py`
- 新建：`src/quant_core/data/sources/baostock.py`
- 新建：`tests/unit/data/sources/test_baostock.py`
- 新建：`tests/integration/test_baostock_raw_ingest.py`

**接口：** `BaoStockClient.fetch_daily_bars(start, end, instruments=None) -> Iterable[RawBatch]`；`None` 和空序列都表示请求窗口内的全部历史证券。

- [ ] **步骤 1：先写供应商假对象测试**

覆盖登录/登出、分页、错误码转换、重试、日期与证券分块，并明确：

```python
assert collect(client.fetch_daily_bars(start, end, None)) == collect(client.fetch_daily_bars(start, end, []))
```

全量证券必须来自历史 `instrument` 清单，筛选条件为 `list_date <= end` 且 `delist_date is null or delist_date >= start`，不能只取请求结束日仍上市的证券。

- [ ] **步骤 2：运行单测并确认失败**

运行：`uv run pytest tests/unit/data/sources/test_baostock.py -v`

- [ ] **步骤 3：实现客户端**

封装 BaoStock SDK，通过注入网关实现可测试性；按配置限制单批证券数和日期跨度；每批请求记录规范化参数与全量范围哈希。采集阶段不得调用 `CanonicalMapper`。

- [ ] **步骤 4：执行离线集成测试**

运行：`uv run pytest tests/integration/test_baostock_raw_ingest.py -v`

预期：假网关生成多个不可变 Raw 分区，重复运行不重复发布。

- [ ] **步骤 5：提交**

```bash
git add src/quant_core/data/sources tests/unit/data/sources tests/integration/test_baostock_raw_ingest.py
git commit -m "feat: ingest chunked BaoStock raw data"
```

### 任务 5：规范映射与 Curated 数据集

**文件：**
- 新建：`src/quant_core/data/schemas.py`
- 新建：`src/quant_core/data/mappers/__init__.py`
- 新建：`src/quant_core/data/mappers/baostock.py`
- 新建：`tests/unit/data/mappers/test_baostock_mapper.py`
- 新建：`tests/integration/test_raw_to_curated.py`

**输出数据集：** `instrument`、`trade_calendar`、`daily_bar`、`security_status`、`financial_observation`、`corporate_action`。

- [ ] **步骤 1：写失败映射测试**

使用已落盘 Raw fixture，验证代码映射、数据类型、空值、价格/成交量、主键唯一性、`available_at` 和 `availability_source`。未知公告日财务记录必须保留但标记为不可用于 PIT 研究，不能用报告期末替代公告日。

- [ ] **步骤 2：运行测试**

运行：`uv run pytest tests/unit/data/mappers/test_baostock_mapper.py -v`

- [ ] **步骤 3：实现 schema 与纯映射器**

映射器只接收 `PublishedPartition`，不得持有 BaoStock 会话。输出按规范主键排序；schema 不匹配时抛出 `DATA_SCHEMA_MISMATCH`，并在上下文中记录 Raw 分区和字段差异。

- [ ] **步骤 4：验证 Raw 与 Curated 的边界**

运行：`uv run pytest tests/integration/test_raw_to_curated.py -v`

预期：先发布 Raw，再独立映射为 Curated；删除供应商假网关后仍可仅依赖 Raw 重建 Curated。

- [ ] **步骤 5：提交**

```bash
git add src/quant_core/data/schemas.py src/quant_core/data/mappers tests/unit/data/mappers tests/integration/test_raw_to_curated.py
git commit -m "feat: map BaoStock raw data to canonical datasets"
```

### 任务 6：SQLite 元数据、质量检查与快照

**文件：**
- 新建：`src/quant_core/persistence/database.py`
- 新建：`src/quant_core/persistence/orm.py`
- 新建：`src/quant_core/persistence/repositories.py`
- 新建：`src/quant_core/persistence/migrations/env.py`
- 新建：`src/quant_core/persistence/migrations/versions/0001_data_catalog.py`
- 新建：`src/quant_core/data/quality/models.py`
- 新建：`src/quant_core/data/quality/rules.py`
- 新建：`src/quant_core/data/quality/runner.py`
- 新建：`src/quant_core/data/snapshots.py`
- 新建：`tests/integration/test_snapshot_publication.py`

**事务接口：**

```python
class SnapshotPublisher:
    def publish(self, dataset_versions: Mapping[str, DatasetVersionId], quality_run_id: QualityRunId) -> SnapshotId: ...
```

- [ ] **步骤 1：写失败集成测试**

验证同一数据版本只能登记一次；快照清单完整记录内容哈希；存在 `FATAL` 问题时发布失败且不留下半成品；成功发布后清单与 SQLite 在同一事务结果下可见；快照记录不可更新。

- [ ] **步骤 2：运行测试**

运行：`uv run pytest tests/integration/test_snapshot_publication.py -v`

- [ ] **步骤 3：实现 ORM、迁移和仓储**

至少创建 `dataset_version`、`dataset_partition`、`quality_run`、`quality_issue`、`snapshot`、`snapshot_dataset`、`audit_log`。SQLite 启用 WAL、foreign keys 和 busy timeout；仓储方法负责事务，不向调用方暴露 ORM 对象。

- [ ] **步骤 4：实现质量规则和两阶段快照发布**

规则覆盖主键重复、空值、OHLC 关系、负成交量、交易日覆盖、证券覆盖、财务可用时间、跨分区 schema。先写临时清单，数据库事务登记快照，再原子重命名清单；启动恢复逻辑清理临时清单并核对数据库状态。

- [ ] **步骤 5：测试并提交**

运行：`uv run alembic upgrade head`

运行：`uv run pytest tests/integration/test_snapshot_publication.py -v`

```bash
git add src/quant_core/persistence src/quant_core/data/quality src/quant_core/data/snapshots.py tests/integration/test_snapshot_publication.py
git commit -m "feat: publish quality-gated immutable snapshots"
```

### 任务 7：数据管道、CLI 与端到端验收

**文件：**
- 新建：`src/quant_core/data/pipelines/__init__.py`
- 新建：`src/quant_core/data/pipelines/ingest.py`
- 新建：`src/quant_core/data/pipelines/curate.py`
- 新建：`src/quant_core/data/pipelines/publish.py`
- 新建：`src/quant_core/cli.py`
- 新建：`tests/integration/test_data_pipeline.py`
- 新建：`tests/integration/test_source_substitution.py`
- 新建：`tests/regression/test_data_snapshot_golden.py`

**CLI：** `quant data bootstrap`、`quant data update --start YYYY-MM-DD --end YYYY-MM-DD`、`quant data validate`、`quant data publish`、`quant data snapshots`。

- [ ] **步骤 1：写失败的 CLI 与恢复测试**

验证命令退出码、结构化错误、阶段断点、幂等重跑、Raw 已成功但 Curated 失败后的恢复，以及质量阻断时不会发布快照。

- [ ] **步骤 2：运行测试并确认失败**

运行：`uv run pytest tests/integration/test_data_pipeline.py -v`

- [ ] **步骤 3：实现四阶段编排**

阶段固定为 `INGEST_RAW → CURATE → VALIDATE → PUBLISH_SNAPSHOT`；每阶段写入运行 ID、输入/输出哈希、开始/结束时间和结构化错误。重跑时依据内容哈希复用已完成阶段，不依据文件是否存在进行猜测。

`quant data bootstrap` 默认以最新完整交易日为结束日，开始日取向前滚动 20 年；解析得到的准确起止交易日写入运行元数据。`quant data update` 从已发布快照的水位线增量采集，并重新拉取配置的重叠窗口以识别供应商修订。

- [ ] **步骤 4：建立黄金快照**

用小型离线供应商 fixture 生成固定清单、行数、schema 和关键样本值；黄金测试比较内容语义与哈希，不写入真实 `QUANT_DATA_ROOT`。

- [ ] **步骤 5：验证数据源替换边界**

在 `tests/integration/test_source_substitution.py` 中实现仅供测试的 `FakeTushareSourceClient` 与 `FakeTushareCanonicalMapper`。用与 BaoStock fixture 语义等价但字段名不同的 Raw 数据生成同一规范 Curated schema；随后运行相同的数据质量和快照发布流程，断言规范数据、快照清单和公共类型中均不包含 BaoStock/TuShare 专有字段。股票池、回测和 Dashboard 的完整替换证明留到第五份计划执行。

运行：`uv run pytest tests/integration/test_source_substitution.py -v`

预期：测试通过，且没有修改策略、回测或 Dashboard 模块。

- [ ] **步骤 6：运行总体验收**

运行：`uv run pytest tests/unit tests/integration/test_data_pipeline.py tests/integration/test_snapshot_publication.py tests/integration/test_source_substitution.py tests/regression/test_data_snapshot_golden.py -v`

运行：`uv run ruff format --check . && uv run ruff check src tests && uv run mypy src`

预期：全部退出码为 0。

- [ ] **步骤 7：提交**

```bash
git add src/quant_core/data/pipelines src/quant_core/cli.py tests/integration/test_data_pipeline.py tests/integration/test_source_substitution.py tests/regression/test_data_snapshot_golden.py
git commit -m "feat: complete reproducible data pipeline"
```

## 验收门槛

- 测试能证明真实执行顺序为 `BaoStock → Raw Parquet → CanonicalMapper → Curated Parquet → Snapshot`。
- `fetch_daily_bars(..., instruments=None)` 与空序列均表示时间窗内全部历史证券，并经过分块执行。
- 只依赖已发布 Raw 即可重建 Curated，不需要重新连接 BaoStock。
- 严重数据质量问题可以稳定阻止快照发布。
- 重复输入不会生成重复分区或不同快照内容。
- 测试用 TuShare 适配器可以替换 BaoStock 并生成相同规范数据与快照契约。
- 单元、集成、回归测试以及 Ruff、mypy 全部通过。
