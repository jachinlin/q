# CLI 模块设计与职责

本文说明 `src/quant_core/cli.py` 在个人 A 股量化系统中的职责、命令结构、依赖装配方式和错误输出契约。内容以当前代码实现为准。

## 1. 模块定位

`cli.py` 是系统的命令行入口和 Composition Root（依赖装配入口）。它负责：

- 定义 `quant data ...` 命令。
- 从环境变量和配置文件加载运行参数。
- 创建数据库、数据源、存储、质量检查和快照服务。
- 调用 `DataPipeline` 完成业务流程。
- 将成功结果和错误统一输出为 JSON。

它不负责实现行情采集、Raw/Curated 写入、质量规则或快照算法。这些能力分别位于 Source、Pipeline、Quality 和 Snapshot 模块中。

```text
quant data ...
    -> create_app
    -> build_default_services
    -> DataPipeline
       -> BaoStockClient
       -> RawPartitionStore
       -> CuratedPartitionStore
       -> QualityRunner
       -> SnapshotPublisher
    -> JSON stdout / stderr
```

## 2. `ApplicationServices`

```python
@dataclass(frozen=True, slots=True)
class ApplicationServices:
    pipeline: DataPipeline
    repository: MetadataRepository
```

`ApplicationServices` 是 CLI 使用的服务容器：

- `pipeline`：执行 bootstrap、update、validate 和 publish。
- `repository`：提供 Snapshot 等元数据的只读查询。

命令函数只依赖这个容器，不直接构造 BaoStock Client 或数据库。这使测试可以注入 Fake Pipeline 和 Fake Repository，而不访问真实网络和磁盘数据。

## 3. `create_app`

```python
def create_app(
    services_factory: Callable[[], ApplicationServices],
) -> typer.Typer:
```

`create_app` 创建 Typer 命令树：

```text
quant
└── data
    ├── bootstrap
    ├── update
    ├── validate
    ├── publish
    └── snapshots
```

它接收可注入的 `services_factory`，不会在 Python 模块导入时连接 BaoStock 或打开数据库。只有真正执行命令时，`_invoke` 才调用工厂创建服务。

## 4. 数据命令

### 4.1 `quant data bootstrap`

```powershell
uv run quant data bootstrap
```

调用：

```python
services.pipeline.bootstrap()
```

用途是初始化历史数据。当前 Pipeline 的默认业务约束是向前回溯 20 年，并执行完整四阶段流程：

```text
INGEST_RAW -> CURATE -> VALIDATE -> PUBLISH_SNAPSHOT
```

### 4.2 `quant data update`

自动增量更新：

```powershell
uv run quant data update
```

手工指定日期窗口：

```powershell
uv run quant data update --start 2026-01-01 --end 2026-01-31
```

规则：

- `--start` 和 `--end` 必须同时提供或同时省略。
- 日期必须是 `YYYY-MM-DD`。
- 同时省略时，由 Pipeline 根据最新已发布 Snapshot 的水位和重叠窗口计算更新范围。
- 当前 CLI 没有 `--instruments` 参数，不对外提供指定证券采集命令。

### 4.3 `quant data validate`

```powershell
uv run quant data validate
```

调用：

```python
services.pipeline.validate_latest()
```

它恢复最近一个已成功完成 Curated、但尚未完成后续阶段的兼容 run，重新验证 checkpoint 后执行质量检查。

### 4.4 `quant data publish`

```powershell
uv run quant data publish
```

调用：

```python
services.pipeline.publish_latest()
```

它恢复最近一个质量检查已成功的兼容 run，并发布不可变 Snapshot。存在 SEVERE 或 FATAL 质量问题时，Snapshot Publisher 会阻止发布。

### 4.5 `quant data snapshots`

```powershell
uv run quant data snapshots
```

该命令只读取 Repository，列出状态为 `PUBLISHED` 的 Snapshot，包括：

- Snapshot ID
- `as_of` 时间
- 状态
- Quality Run ID
- Dataset Version 映射

它不会触发采集、验证或发布。

## 5. `_invoke`：统一执行边界

```python
def _invoke(
    operation: Callable[[ApplicationServices], object],
    services_factory: Callable[[], ApplicationServices],
    *,
    add_status: bool = True,
) -> None:
```

所有命令都通过 `_invoke` 执行。它负责：

1. 调用 `services_factory()` 创建本次命令所需服务。
2. 执行 Pipeline 或 Repository 操作。
3. 捕获结构化 `QuantError`。
4. 将未知异常包装为 `DATA_PIPELINE_UNEXPECTED`。
5. 将成功 JSON 写到 stdout。
6. 将错误 JSON 写到 stderr，并以退出码 `2` 结束。

成功示例：

```json
{
  "dataset_versions": {
    "daily_bar": "..."
  },
  "quality_run_id": "...",
  "run_id": "...",
  "snapshot_id": "...",
  "status": "SUCCEEDED"
}
```

失败示例：

```json
{
  "error": {
    "code": "DATA_PIPELINE_ARGUMENT",
    "context": {},
    "message": "start and end must be supplied together",
    "remediation": "provide both --start and --end or neither",
    "retryable": false,
    "severity": "SEVERE"
  }
}
```

稳定的 JSON 契约便于 Dashboard、定时任务和自动化脚本消费，不需要解析人类可读日志。

## 6. `_result_payload`

`_result_payload` 将命令结果转换为 JSON object：

- `Mapping`：复制为普通字典。
- `PipelineResult`：转换为 run、snapshot、quality run 和 dataset-version 字段。
- 其他类型：抛出 `TypeError`，随后由 `_invoke` 转换为结构化意外错误。

Dataset Version 按 key 排序，JSON 使用 `sort_keys=True`，从而保持输出稳定，便于测试和机器比较。

## 7. 参数错误处理

`_parse_cli_date` 使用 `date.fromisoformat` 解析日期。失败时输出：

```text
code = DATA_PIPELINE_ARGUMENT
severity = SEVERE
retryable = false
```

`_emit_error` 是唯一的错误输出出口，负责输出完整 `ErrorDetail`：

- `code`
- `severity`
- `message`
- `context`
- `remediation`
- `retryable`

## 8. BaoStock Fetch Config Fingerprint

`_baostock_fetch_config_fingerprint` 将影响采集结果的配置序列化为 Canonical JSON，再计算 SHA-256。当前指纹包括：

- 全市场采集路由版本：`query_daily_history_k_AStock-per-open-date-v1`
- 指定证券路由版本：`query_history_k_data_plus-v1`
- 指定证券日期分块大小
- 指定证券数量分块大小
- 最大重试次数
- 重试退避间隔
- 可重试 BaoStock 错误码

该指纹进入 `PipelineVersions.fetch_config`。采集路由或配置发生变化后，系统会生成新的 run/checkpoint 身份，避免错误复用旧 INGEST_RAW 结果。

## 9. `build_default_services`

```python
def build_default_services() -> ApplicationServices:
```

这是实际运行环境的依赖装配函数，按以下顺序执行。

### 9.1 解析路径和配置

源码根目录由 `cli.py` 的位置推导。数据根目录必须通过环境变量提供：

```powershell
$env:QUANT_DATA_ROOT='D:\quant-data'
```

未设置时返回：

```text
CFG_DATA_ROOT_REQUIRED
```

配置文件默认使用：

```text
<source_root>/configs/base.yaml
```

可以通过下面的环境变量覆盖：

```powershell
$env:QUANT_CONFIG='D:\quant-config\production.yaml'
```

`Settings.load` 会校验数据目录不能位于源码树中，避免运行数据污染 Git 仓库。

### 9.2 初始化数据库

```python
upgrade_database(settings.state_db)
repository = MetadataRepository(create_sqlite_engine(settings.state_db))
```

每次 CLI 命令启动都会先将 SQLite schema 升级到 Alembic head，然后创建 Repository。

### 9.3 创建 BaoStock 客户端

CLI 创建两个相互独立的 Gateway 和 Client：

```python
source_gateway = BaoStockSdkGateway()
calendar_gateway = BaoStockSdkGateway()

source = BaoStockClient(source_gateway, None, source_config)
calendar_client = BaoStockClient(calendar_gateway, None, source_config)
```

- `source`：提供 Pipeline 的 instruments、trade calendar 和 daily bars 数据。
- `calendar_client`：提供 `BaoStockCalendarPolicy` 所需的交易日期窗口解析。

使用独立 Client 可以避免日历策略和正式采集共享登录状态及 SDK cursor 生命周期。

### 9.4 创建 Pipeline

`DataPipeline` 注入以下组件：

| 组件 | 作用 |
|---|---|
| `BaoStockClient` | 获取供应商 Raw 数据 |
| `BaoStockMapper` | 将已发布 Raw 映射到 Canonical schema |
| `BaoStockCalendarPolicy` | 解析 bootstrap/update 的准确交易日边界 |
| `RawPartitionStore` | 发布不可变 Raw Parquet 和 manifest |
| `CuratedPartitionStore` | 合并并发布规范数据版本 |
| `MetadataRepository` | 持久化 run、stage、quality 和 snapshot 元数据 |
| `QualityRunner` | 执行基础数据质量规则 |
| `SnapshotPublisher` | 发布质量门禁后的不可变 Snapshot |

Pipeline 还接收完整版本集合：

```python
PipelineVersions(
    source_adapter=BAOSTOCK_SOURCE_ADAPTER_VERSION,
    fetch_config=fetch_config_fingerprint,
    mapper=BAOSTOCK_MAPPER_VERSION,
    canonical_schema=CANONICAL_SCHEMA_VERSION,
    quality_rules=QUALITY_RULE_SET_VERSION,
    snapshot_manifest=SNAPSHOT_MANIFEST_VERSION,
)
```

任一稳定组件版本变化都会影响 Pipeline fingerprint，阻止不兼容 checkpoint 被复用。

## 10. `app`

文件末尾：

```python
app = create_app(build_default_services)
```

这是 `pyproject.toml` 中 `quant` 命令引用的 Typer 应用对象。模块导入只创建命令树，真实服务仍在命令执行时延迟构造。

## 11. 设计边界

`cli.py` 应继续只负责：

- 命令协议
- 参数解析
- 依赖装配
- JSON 输出
- 顶层错误边界

下列逻辑不应放入 `cli.py`：

- BaoStock cursor 遍历或供应商字段转换
- Raw/Curated 文件写入算法
- 质量规则实现
- Snapshot 生成算法
- 股票池、因子或回测逻辑
- Dashboard 页面逻辑

保持这一边界可以让 CLI、Dashboard、未来调度器和测试复用同一套应用服务，而不会复制数据业务规则。
