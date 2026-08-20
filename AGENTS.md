# AGENTS.md

## 项目定位

本项目是单用户、Windows 本地运行的 A 股量化研究平台。Python 包名为
`quant_research`，安装后的命令为 `quant`。项目尚未发布，不兼容旧接口、旧包、
旧数据库或旧产物；设计变化应直接落实到最终结构，不创建兼容转发层。


设计文档与代码契约冲突时，不要私自保留两套语义。确认权威口径后，在同一变更中
同步实现、测试和文档。

## 仓库结构

```text
src/quant_research/
├── application/       # 数据、实验、因子研究、任务与 Worker 用例
├── domain/            # 稳定领域枚举、标识和值对象
├── data/              # 数据目录、schema、流水线和研究读取契约
├── factors/           # 因子定义、注册、计算与统计内核
├── factor_studies/    # 独立因子研究模型和分析契约
├── universe/          # PIT 股票池
├── portfolio/         # 组合构建
├── strategies/        # 策略定义
├── backtest/          # 回测与执行
├── analytics/         # 绩效和归因
├── experiments/       # 实验配置、身份、状态机和产物登记
├── tasks/             # 任务模型与处理端口
├── infrastructure/    # SQLite、SQLAlchemy、Alembic、BaoStock 适配器
├── cli/               # Typer 输入输出适配器
├── dashboard/         # FastAPI app、routes、models 和 views
└── bootstrap/         # CLI、Dashboard、Worker 组合根

frontend/              # Vue Dashboard
configs/               # 应用配置、实验示例和交易规则
tests/                 # unit、integration、performance、acceptance、e2e
docs/architecture/     # 当前有效架构文档
```

## 环境与常用命令

项目要求 Python 3.12，使用 `uv` 管理环境：

```powershell
uv sync
uv run quant --help
```

后端验证：

```powershell
uv run ruff check src tests benchmarks
uv run mypy --show-error-codes

$testTemp = Join-Path $env:TEMP "quant-pytest-$PID"
uv run pytest --basetemp $testTemp

$acceptanceTemp = Join-Path $env:TEMP "quant-acceptance-$PID"
uv run pytest --run-performance --run-acceptance --basetemp $acceptanceTemp
```

Windows pytest 必须优先使用源码目录之外的短临时路径，避免数据根边界、SQLite、
文件锁和长路径造成假失败。

前端验证：

```powershell
Push-Location frontend
npm ci
npm test
npm run typecheck
npm run build
Pop-Location
```

常用运行命令：

```powershell
uv run quant data --help
uv run quant data bootstrap
uv run quant data update
uv run quant data localize-all
uv run quant data curate-all
uv run quant data validate-all
uv run quant experiments submit configs/experiments/examples/etf_rotation.yaml
uv run quant worker once
uv run quant worker run
uv run quant dashboard --port 8000
uv run quant start
```

环境变量：

- `QUANT_DATA_ROOT`：数据根目录，默认 `~/.q-data`，必须位于源码目录之外。
- `QUANT_CONFIG`：应用 YAML，默认 `configs/base.yaml`。
- `QUANT_WORKER_PROFILE`：`baostock`（默认）或 `offline-etf`。
- `QUANT_DASHBOARD_DEV_ORIGIN`：仅用于本地 Vite 开发的允许同源地址。

## 架构边界

依赖方向为：

```text
bootstrap → cli / dashboard → application → capabilities
    │                                      ▲
    └────────────→ infrastructure ─────────┘
```

必须遵守：

- `bootstrap` 是真实依赖的唯一组合根，可以依赖所有层。
- `cli` 不得导入 `dashboard`、`bootstrap` 或基础设施具体实现。
- Dashboard 的 app、models、routes 不得直接导入基础设施。
- `application` 不得导入 `cli`、`dashboard`、`bootstrap` 或基础设施具体实现。
- 业务能力包不得导入接口层或组合根。
- 基础设施实现消费者侧 Protocol，不得反向导入 UI 或组合根。
- 包导入期间不得连接网络、升级数据库、启动线程或扫描用户数据目录。
- `tests/unit/test_architecture_boundaries.py` 是最低架构门禁；新增边界时同步扩展测试。

## 数据层不变量

数据流水线固定为：

```text
LOCALIZE → CURATE → VALIDATE
```

- Raw 响应按 request hash 和 content hash 内容寻址；SQLite
  `raw_request`/`raw_object` 支持幂等和断点续抓。
- Canonical 分区按内容寻址；`canonical_dataset`/`canonical_partition` 保存唯一当前
  指针和输入身份。
- Curate 默认按 `canonical_partition.input_hash` 只重建 Raw 输入变化的分区；
  `--full` 强制重建。
- 只有 Canonical 内容变化才切换数据身份并使全局校验失效。
- `validate <dataset>` 只诊断；只有 `validate-all` 可以开放研究读取。
- `data_catalog_state.catalog_hash` 是全部当前 Canonical 数据集 `data_hash` 的目录身份。
- 实验和因子运行提交时捕获 `catalog_hash`；运行阶段发现漂移立即失败。
- 没有 dataset version、数据发布 Snapshot 或 `snapshot_id`；`AccountSnapshot` 仅表示
  账户状态对象。
- 研究代码只能通过 `CanonicalResearchRepository` 读取，不得直接扫描 Raw 或
  Canonical 路径。
- `QUANT_DATA_ROOT` 必须在源码树外；测试不得把生产数据根指向仓库目录。

Canonical 审计列为：

```text
source
available_at
availability_source
pit_usable
ingested_at
```

证券标识使用 `600000.SH`、`000300.SH`；风险标记为 `is_st`；估值数据集为
`daily_basic`。

## 因子、策略与实验不变量

- 因子使用唯一 `factor_id`，不使用 `id@version`。
- 因子输出列固定为 `trade_date`、`instrument_id`、`factor_id`、`value`、
  `available_at`、`is_valid`。
- 因子每次运行重新计算，不持久化跨运行缓存。
- 因子产物在运行内绑定 `catalog_hash`、股票池哈希、配置和代码哈希。
- 因子研究、策略信号和策略回测是不同产物，不得用其中一个替代另一个。
- 策略使用唯一 `strategy_id`。
- 交易规则只使用 `configs/rules/a_share.yaml`，其内容哈希进入实验身份。
- 实验 YAML 日期必须是明确的 `YYYY-MM-DD`，不接受 selector 或格式版本字段。
- 实验固定七阶段：
  `VALIDATE → UNIVERSE → FACTOR_COMPUTE → BACKTEST → ANALYTICS → ARTIFACT_VERIFY → REGISTER`。
- 每一阶段都必须检查捕获的数据身份；失败、取消和重试不得覆盖旧产物。

## 持久化与产物

- SQLite schema 由 Alembic 管理；修改 ORM 时必须同步 migration 和空库/现有库测试。
- 跨边界 DTO 使用冻结 dataclass 或严格、不可变 Pydantic model。
- 文件身份使用 SHA-256；JSON 使用确定性序列化。
- 发布采用同一文件系统临时目录加原子重命名，禁止覆盖既有不可变目录。
- Manifest 必须记录相对路径、哈希、字节数、schema、行数和输入身份。
- 只有从最终目录重新验证成功的产物才能登记为成功。
- 路径必须先解析并验证位于可信根内；不得接受任意用户文件路径。

## Python 工程约束

- Python 3.12，Ruff，严格 mypy。
- 公开 API 不使用兼容性 `*args`/`**kwargs`。
- 公开模块、类、方法和函数使用中文 docstring，说明职责、入参、返回值和关键异常；
  修改公开 API 时同步维护 docstring。
- 不新增模块级孤立函数。逻辑应归入职责明确的类，并按状态依赖选择实例、类或静态
  方法。稳定公开 API 或框架入口可以保留模块级函数，但必须在 docstring 中说明。
- 使用消费者侧 Protocol 注入依赖；业务模块不依赖具体 SQLite、ORM 或供应商类。
- 不在导入时执行外部副作用。
- 不引入旧包、旧字段或旧数据库的兼容分支。
- 保持确定性排序；不要依赖集合、文件系统或数据库未声明的顺序。
- 修改源码路径时同步检查因子源码哈希、Hatch wheel、Alembic、CLI entry point、测试和
  文档引用。

## 日志与错误

- 使用 `StructuredLogger` 输出 JSON Lines，并在写入前脱敏。
- 统一外层只包含时间、级别、事件、阶段和关联 ID；业务 `context` 由命令或阶段定义。
- Localize 记录完整供应商 request 和 Raw 发布证据。
- Curate 记录 Canonical 分区、输入身份、行数、内容哈希和指针结果。
- Validate 记录目录、规则、质量问题和门禁结果。
- Task、Worker 与 Experiment 记录各自 payload、attempt、progress 和 outcome。
- 不强制所有日志伪造统一 request 对象。
- CLI stdout 只输出成功 JSON；受控错误和日志写入 stderr。
- 未知异常必须在进程边界转换为结构化错误，不泄露 traceback 或敏感上下文。

## 测试要求

- 单元测试禁止访问真实网络；BaoStock SDK 必须由 fake/stub 隔离。
- 应用层测试优先注入 Protocol fake，不启动真实 SQLite 或 Worker 线程。
- 集成测试使用临时数据库和源码树外的数据根。
- 时间、UUID、环境身份和供应商响应必须可注入，避免不确定测试。
- 对统计公式、PIT、状态机和哈希使用字面量 oracle，不只做实现对实现比较。
- 修改 CLI 时验证命令树、JSON、stderr、退出码和资源关闭。
- 修改 Dashboard 时运行 API 单测和前端 test/typecheck/build。
- 修改 migration 时验证空数据库建库和现有数据库副本升级。
- 修改架构边界时运行 AST 门禁并扫描旧包引用。

## 变更交付

- 先检查工作树，保留用户已有改动；不要覆盖无关文件。
- 只修改任务范围内的行为，不顺带修复无关基线问题。
- 设计变化直接落实最终结构，不留下临时兼容层或双入口。
- 同步更新相关架构文档、测试和命令示例。
- 交付前至少运行 Ruff、严格 mypy 和相关 pytest；高风险或跨层变更运行完整后端与前端
  验证。
- 报告实际执行的验证、跳过项和剩余风险，不把未运行的检查写成通过。
