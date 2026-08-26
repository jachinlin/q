# Quant Research

Quant Research 是一个单用户、Windows 本地运行的 A 股量化研究平台。它把数据采集、
PIT 研究数据、因子分析、策略实验、回测、任务执行和 Dashboard 放在同一个可审计的
研究闭环中。

项目当前未发布，不承诺旧 Python 导入、旧数据库或旧研究产物兼容。Python 包统一为
`quant_research`，命令行入口为 `quant`。

## 能力概览

- BaoStock 数据采集，Raw 响应内容寻址和断点续抓；
- Canonical 分区、质量校验和全局研究门禁；
- PIT 股票池、七个股票因子和独立因子研究；
- ETF 轮动与股票多因子策略；
- 固定七阶段实验、回测、绩效分析和不可变产物登记；
- SQLite 持久化任务队列和本地 Worker；
- FastAPI + Vue 数据中心、实验中心、因子研究和任务运行界面；
- 结构化 CLI、JSON Lines 日志和敏感信息脱敏。

## 运行要求

- Windows；
- Python 3.12；
- [`uv`](https://docs.astral.sh/uv/)；
- Node.js 和 npm，仅构建或开发 Dashboard 前端时需要。

数据根必须位于源码目录之外。默认位置为 `~/qlab-data`，也可以在 PowerShell 中显式设置：

```powershell
$env:QUANT_DATA_ROOT = "D:\quant-data"
```

数据源 Token 可在 Dashboard「设置」页维护，也可直接写入数据根目录下的明文
`.env`：

```dotenv
QUANT_TUSHARE_TOKEN=<your-token>
```

不要在代码根目录创建或提交包含 Token 的 `.env`。

## 快速开始

### 1. 安装后端依赖

```powershell
uv sync --group notebook
uv run quant --help
```

### 2. 构建 Dashboard 前端

```powershell
Push-Location frontend
npm ci
npm run build
Pop-Location
```

### 3. 初始化数据

```powershell
uv run quant data bootstrap --years 5
```

首次初始化会通过 Tushare 全市场端点构建指定年数的基线。
数据完成 `LOCALIZE → CURATE → VALIDATE` 后，研究读取门禁才会开放。
也可以先启动本地服务，再在 Dashboard「数据中心」点击「初始化数据」提交相同的后台任务；
初始化未完成时，界面不会显示日常更新和质量运行入口。

如果已经存在本地数据，可以使用增量命令：

```powershell
uv run quant data update
```

### 4. 一次启动 Dashboard、Worker 与 Notebook

```powershell
uv run quant start
```

默认同时启动：

- Dashboard：<http://127.0.0.1:8000>；
- Worker：持续处理本地任务队列；
- JupyterLab：<http://127.0.0.1:8009>，Notebook 根目录默认为当前目录下的 `notebooks/`，本机模式默认禁用 token。

三个服务都固定监听或运行在本机；按 `Ctrl+C`，或任一服务退出时，监督器会统一关闭
其余进程。Dashboard 健康检查通过后会自动在默认浏览器打开。可使用
`--dashboard-port` 和 `--notebook-dir` 修改默认值；Notebook 端口固定为 `8009`。
需要单独运行服务时，仍可使用 `quant dashboard` 和 `quant worker run`。

## 数据流水线

数据生命周期固定为：

```text
LOCALIZE → CURATE → VALIDATE
```

常用命令：

```powershell
# 查看完整数据命令
uv run quant data --help

# 获取所有目录数据的 Raw 响应
uv run quant data localize-all

# 只重建输入发生变化的 Canonical 分区
uv run quant data curate-all

# 强制重建全部 Canonical 分区
uv run quant data curate-all --full

# 执行全局质量校验并开放研究门禁
uv run quant data validate-all
```

单数据集诊断：

```powershell
uv run quant data localize daily_bar --from 2026-01-01 --to 2026-01-31
uv run quant data curate daily_bar --from 2026-01-01 --to 2026-01-31
uv run quant data validate daily_bar
```

`validate <dataset>` 只生成诊断结果；只有 `validate-all` 能开放研究读取。

## 策略实验、因子研究与任务

提交实验只创建不可变实验和后台任务，不在 CLI 进程中同步执行：

```powershell
uv run quant experiments submit configs/experiments/examples/etf_rotation.yaml
uv run quant worker once
```

查看实验：

```powershell
uv run quant experiments show <experiment-id>
```

因子研究使用独立的扁平配置和命令组：

```powershell
uv run quant factor-studies validate configs/factor_studies/examples/factor_study.yaml
uv run quant factor-studies submit configs/factor_studies/examples/factor_study.yaml
uv run quant factor-studies show <factor-study-id>
uv run quant factor-studies list
```

管理任务：

```powershell
uv run quant tasks list
uv run quant tasks list --status FAILED --limit 50
uv run quant tasks cancel <task-id>
uv run quant tasks retry <task-id>
```

失败或取消的因子研究重试会复用同一任务和冻结配置并创建新 attempt；成功研究不可重跑。
策略实验的重跑继续创建新的 Run 和产物，任何成功产物都不可覆盖。

实验示例：

- `configs/experiments/examples/etf_rotation.yaml`
- `configs/experiments/examples/multifactor.yaml`
- `configs/experiments/examples/dual_ma_trend.yaml`

因子研究示例：`configs/factor_studies/examples/factor_study.yaml`。

## CLI 输出

一次性命令成功时，stdout 只输出一个排序稳定的 JSON 对象：

```json
{
  "experiment_id": "<uuid>",
  "experiment_status": "QUEUED",
  "status": "SUCCEEDED",
  "task_id": "<uuid>",
  "task_status": "QUEUED"
}
```

受控失败以结构化 JSON 写入 stderr，并使用退出码 `2`。运行日志也是 JSON Lines，调用方
应以退出码判断成功，并只从 stdout 解析一次性命令结果。

完整 CLI 契约见 [CLI 总体设计](docs/architecture/cli_design.md)。

## 前端开发

先启动 API，并显式允许 Vite 开发地址：

```powershell
$env:QUANT_DASHBOARD_DEV_ORIGIN = "http://127.0.0.1:5173"
uv run quant dashboard --port 8000
```

另一个终端启动 Vite：

```powershell
Push-Location frontend
npm run dev
```

Dashboard 写请求要求同源 JSON 和有效 `X-Request-ID`。界面不提供任意 SQL、Python 或
文件路径执行入口。

## 配置

| 环境变量 | 默认值 | 作用 |
|---|---|---|
| `QUANT_DATA_ROOT` | `~/qlab-data` | Raw、Canonical、SQLite、日志和产物根目录。 |
| `QUANT_CONFIG` | `configs/base.yaml` | 应用配置文件。 |
| `QUANT_WORKER_PROFILE` | `baostock` | `baostock` 或 `offline-etf` Worker Profile。 |
| `QUANT_DASHBOARD_DEV_ORIGIN` | 空 | 本地前端开发时允许的 Vite Origin。 |

基础应用配置：

```yaml
timezone: Asia/Shanghai
max_partition_size: 100
```

交易规则唯一来源为 `configs/rules/a_share.yaml`，其内容哈希进入实验身份。

## 架构

项目使用单一 `quant_research` wheel：

```text
src/quant_research/
├── application/       # 应用用例
├── domain/            # 领域对象
├── data/              # 数据流水线与研究读取
├── factors/           # 因子与统计内核
├── factor_studies/    # 独立因子研究
├── universe/          # 股票池
├── portfolio/         # 组合构建
├── strategies/        # 策略
├── backtest/          # 回测
├── analytics/         # 分析与归因
├── experiments/       # 实验
├── tasks/             # 任务模型
├── infrastructure/    # SQLite、Alembic、BaoStock
├── cli/               # Typer 适配器
├── dashboard/         # FastAPI 适配器
└── bootstrap/         # 组合根
```

研究代码只能通过 `CanonicalResearchRepository` 读取数据，不能直接扫描 Raw 或
Canonical 路径。实验提交时捕获当前 Catalog、源码、锁文件和交易规则身份；任一阶段
发现漂移都会失败，而不是静默切换输入。

## 开发与验证

后端：

```powershell
uv run ruff check src tests benchmarks
uv run mypy --show-error-codes

$testTemp = Join-Path $env:TEMP "quant-pytest-$PID"
uv run pytest --basetemp $testTemp

$acceptanceTemp = Join-Path $env:TEMP "quant-acceptance-$PID"
uv run pytest --run-performance --run-acceptance --basetemp $acceptanceTemp
```

前端：

```powershell
Push-Location frontend
npm test
npm run typecheck
npm run build
Pop-Location
```

Windows 下建议始终把 pytest `--basetemp` 放在源码目录之外，并使用短路径，以避免数据根
边界、SQLite 文件锁和长路径产生假失败。

## 架构文档

- [平台总体设计](docs/architecture/personal-a-share-quant-platform-design.md)
- [包结构](docs/architecture/package-layout.md)
- [数据层设计](docs/architecture/data-layer-design.md)
- [因子研究与分析总体设计](docs/architecture/factor-analysis-design.md)
- [CLI 总体设计](docs/architecture/cli_design.md)

`src/quant_research/data/catalog.py` 和 `src/quant_research/data/schemas.py` 分别是数据目录
与 Canonical schema 的可执行事实来源。

## 本地安全边界

- Dashboard 只监听 `127.0.0.1`；
- 写请求要求同源和 `X-Request-ID`；
- 数据根必须在源码目录之外；
- 日志和错误上下文在输出前脱敏；
- 研究入口不接受任意 SQL、Python 或不受信任文件路径；
- 单元测试不会访问真实供应商网络。
