# Dashboard、策略实验室与端到端验收实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 交付六页本地 Dashboard、三本可复现 Notebook 策略实验室、受控写操作以及覆盖数据到实验展示的端到端验收流程。

**架构：** Streamlit 页面只调用 `DashboardService`；服务再调用 `ExperimentQuery`、`TaskCommand` 和 `DataCatalogQuery`。页面不得创建数据库会话、拼 SQL 或接收任意文件路径。常规图表读取轻量物化摘要，持仓/成交下钻才通过 DuckDB 查询单个实验产物。Dashboard 与 Worker 为独立进程。

**技术栈：** Python 3.12、Streamlit、Plotly、Polars、DuckDB、Pydantic v2、JupyterLab、nbclient、pytest、Streamlit AppTest、Playwright（仅关键端到端流程）。

## 前置条件与约束

- 先完成前四份实施计划。
- Dashboard 默认只绑定 `127.0.0.1`，不提供任意 SQL、任意 Python 或任意路径入口。
- 页面缓存键包含实验 ID、产物哈希和查询参数；运行任务状态最多缓存 5 秒。
- 所有写操作必须经过命令服务校验并生成 `audit_event`。
- 一期不提供删除实验、修改结果、修改已发布快照或直接运行长回测的页面接口。
- Dashboard 关闭不能影响 Worker；Dashboard 重启后从 SQLite 和产物恢复状态。
- 界面文案使用中文，时间以 `Asia/Shanghai` 展示，内部仍保存 UTC。

---

## 文件职责

| 路径 | 作用 |
|---|---|
| `src/quant_core/dashboard/app.py` | 页面注册、全局布局、导航和启动 |
| `src/quant_core/dashboard/services.py` | 展示 DTO、查询门面和受控命令 |
| `src/quant_core/dashboard/components/*` | 可复用指标卡、表格、图表、状态和错误组件 |
| `src/quant_core/dashboard/pages/overview.py` | 系统、数据、实验和任务总览 |
| `src/quant_core/dashboard/pages/data_center.py` | 数据覆盖、版本、质量和更新任务 |
| `src/quant_core/dashboard/pages/experiments.py` | 实验筛选、比较、复制与研究标记 |
| `src/quant_core/dashboard/pages/backtest_analysis.py` | 净值、回撤、绩效、成交和归因 |
| `src/quant_core/dashboard/pages/factor_analysis.py` | RankIC、分层收益、覆盖率和相关性 |
| `src/quant_core/dashboard/pages/tasks.py` | 后台任务、进度、日志、取消和重试 |
| `notebooks/*.ipynb` | 快速入门、ETF 轮动、多因子研究示例 |
| `configs/dashboard.yaml` | 本地监听、刷新、缓存和日志展示配置 |

### 任务 1：Dashboard 服务层与安全 DTO

**文件：**
- 新建：`src/quant_core/dashboard/__init__.py`
- 新建：`src/quant_core/dashboard/services.py`
- 新建：`tests/unit/dashboard/test_services.py`
- 新建：`tests/integration/test_dashboard_queries.py`

**接口：**

```python
class DashboardService:
    def overview(self) -> OverviewDTO: ...
    def data_catalog(self, filters: DataCatalogFilters) -> DataCatalogDTO: ...
    def experiments(self, filters: ExperimentFilters) -> ExperimentListDTO: ...
    def experiment_detail(self, experiment_id: ExperimentId) -> ExperimentDetailDTO: ...
    def backtest_summary(self, experiment_id: ExperimentId) -> BacktestSummaryDTO: ...
    def factor_summary(self, experiment_id: ExperimentId, factor_id: str | None = None) -> FactorSummaryDTO: ...
    def tasks(self, filters: TaskFilters) -> TaskListDTO: ...
    def task_log(self, task_id: TaskId, tail_lines: int) -> TaskLogDTO: ...

class TaskCommand:
    def cancel(self, task_id: TaskId, actor: str, request_id: str) -> CommandResult: ...
    def retry(self, task_id: TaskId, actor: str, request_id: str) -> CommandResult: ...
```

- [ ] **步骤 1：先写 DTO 与访问边界测试**

DTO 必须是不可变 Pydantic 模型，页面可直接展示但不能获得 SQLAlchemy Session、DuckDB connection 或物理根目录。未知状态映射为灰色 `UNKNOWN`，不能映射为正常。不存在或未成功实验返回结构化展示错误。

- [ ] **步骤 2：写查询约束和路径安全测试**

实验明细只能从登记的产物索引解析；尝试传入 `..`、绝对路径或未登记文件必须失败。日志 `tail_lines` 限制为 1–5000，并仅能读取该任务登记的日志文件。

- [ ] **步骤 3：实现查询门面和命令门面**

列表查询分页并限制最大页大小；摘要优先读取物化文件；持仓与成交按实验 ID、日期和证券过滤后参数化查询。命令层复用任务/实验领域状态机，并在同一事务写审计事件。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/dashboard/test_services.py tests/integration/test_dashboard_queries.py -v`

```bash
git add src/quant_core/dashboard/services.py tests/unit/dashboard tests/integration/test_dashboard_queries.py
git commit -m "feat: add secure dashboard service layer"
```

### 任务 2：应用骨架、公共组件与系统总览

**文件：**
- 新建：`src/quant_core/dashboard/app.py`
- 新建：`src/quant_core/dashboard/components/__init__.py`
- 新建：`src/quant_core/dashboard/components/cards.py`
- 新建：`src/quant_core/dashboard/components/charts.py`
- 新建：`src/quant_core/dashboard/components/status.py`
- 新建：`src/quant_core/dashboard/components/errors.py`
- 新建：`src/quant_core/dashboard/pages/__init__.py`
- 新建：`src/quant_core/dashboard/pages/overview.py`
- 新建：`configs/dashboard.yaml`
- 新建：`tests/ui/test_dashboard_shell.py`
- 新建：`tests/ui/test_overview_page.py`

- [ ] **步骤 1：先写 Streamlit AppTest 失败测试**

断言应用标题、六个中文页面导航、数据快照状态、最近质量检查、实验统计、任务统计和两套基准策略卡片存在。服务抛出错误时显示结构化错误组件，不能显示完整堆栈或白屏。

- [ ] **步骤 2：实现应用工厂和依赖注入**

`create_app_services(settings) -> DashboardService` 使用 `st.cache_resource` 缓存只读服务。页面接收服务参数，测试注入假服务。全局配置固定宽布局、中文页面名和 Asia/Shanghai 展示格式。

- [ ] **步骤 3：实现总览和公共组件**

状态组件覆盖成功、警告、失败、运行、排队、取消、孤儿、未知。图表组件只接收 DTO/Polars DataFrame，不在组件内查询数据。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/ui/test_dashboard_shell.py tests/ui/test_overview_page.py -v`

```bash
git add src/quant_core/dashboard/app.py src/quant_core/dashboard/components src/quant_core/dashboard/pages/overview.py configs/dashboard.yaml tests/ui
git commit -m "feat: build dashboard shell and overview"
```

### 任务 3：数据中心页面

**文件：**
- 新建：`src/quant_core/dashboard/pages/data_center.py`
- 新建：`tests/ui/test_data_center_page.py`
- 新建：`tests/integration/test_data_center_commands.py`

- [ ] **步骤 1：写覆盖与质量展示测试**

页面显示数据集名称、供应商、起止日期、证券数、行数、数据版本、内容哈希缩写、最新快照、质量严重度与修复建议；支持按数据集和严重度过滤。没有数据时显示明确空状态。

- [ ] **步骤 2：写受控更新/重试测试**

更新按钮只提交 `DATA_UPDATE` 任务；日期校验要求开始不晚于结束；点击相同请求两次通过幂等键只产生一个活动任务。重试只针对已结束且可安全重试的数据任务，有活动尝试时拒绝。

- [ ] **步骤 3：实现页面与确认流程**

写操作前展示影响范围和快照不变性说明；执行结果展示请求 ID。严重质量问题以阻断状态显示，不允许页面直接“忽略并发布”。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/ui/test_data_center_page.py tests/integration/test_data_center_commands.py -v`

```bash
git add src/quant_core/dashboard/pages/data_center.py tests/ui/test_data_center_page.py tests/integration/test_data_center_commands.py
git commit -m "feat: add dashboard data center"
```

### 任务 4：实验中心页面

**文件：**
- 新建：`src/quant_core/dashboard/pages/experiments.py`
- 新建：`tests/ui/test_experiments_page.py`
- 新建：`tests/integration/test_experiment_commands.py`

- [ ] **步骤 1：写列表、筛选和比较测试**

支持按状态、策略、快照、研究标记、标签和日期筛选；列表显示实验 ID、名称、指纹缩写、快照、状态、核心指标和创建时间。比较限定 2–5 个成功实验，对齐显示配置差异、数据快照和指标，不把缺失指标填成 0。

- [ ] **步骤 2：写复制与研究记录测试**

复制配置生成新的实验 ID 和 `CREATED` 状态，原实验不变；更新 `ResearchMark`、追加标签和说明生成审计记录；完成实验的配置、快照、指标和产物控件为只读。

- [ ] **步骤 3：实现页面**

重复指纹以提示展示但允许提交。长任务只调用 `ExperimentClient.submit()`，页面立即返回任务 ID；不在 Streamlit 回调里运行回测。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/ui/test_experiments_page.py tests/integration/test_experiment_commands.py -v`

```bash
git add src/quant_core/dashboard/pages/experiments.py tests/ui/test_experiments_page.py tests/integration/test_experiment_commands.py
git commit -m "feat: add experiment research center"
```

### 任务 5：回测分析与因子分析页面

**文件：**
- 新建：`src/quant_core/dashboard/pages/backtest_analysis.py`
- 新建：`src/quant_core/dashboard/pages/factor_analysis.py`
- 新建：`tests/ui/test_backtest_analysis_page.py`
- 新建：`tests/ui/test_factor_analysis_page.py`
- 新建：`tests/performance/test_dashboard_summary_queries.py`

- [ ] **步骤 1：写回测图表契约测试**

必须展示核心指标、策略/基准净值、回撤、月度收益热力图、年度收益、费用与失败成交摘要、行业/风格暴露和归因。持仓/成交下钻必须选择单个实验和日期范围，且分页返回。

- [ ] **步骤 2：写因子图表契约测试**

必须展示 RankIC 时间序列和统计、分层收益、覆盖率、因子相关矩阵、中性化前后对比及缺失原因。因子和实验组合无摘要时给出可执行提示，不触发在线重算。

- [ ] **步骤 3：实现哈希感知缓存**

摘要查询使用 `st.cache_data`，键包含实验 ID、对应产物哈希、筛选参数和指标版本。运行中任务不读取未发布 staging 产物。实验状态或 manifest 哈希变化时自然失效。

- [ ] **步骤 4：执行 UI 和查询性能测试**

运行：`uv run pytest tests/ui/test_backtest_analysis_page.py tests/ui/test_factor_analysis_page.py -v`

运行：`uv run pytest tests/performance/test_dashboard_summary_queries.py -v --run-performance`

预期：总览与摘要查询本地冷启动 P95 小于 3 秒，缓存命中 P95 小于 500 毫秒；常规页面不扫描完整 `holdings.parquet`。

- [ ] **步骤 5：提交**

```bash
git add src/quant_core/dashboard/pages/backtest_analysis.py src/quant_core/dashboard/pages/factor_analysis.py tests/ui tests/performance/test_dashboard_summary_queries.py
git commit -m "feat: visualize backtest and factor analytics"
```

### 任务 6：任务与日志页面

**文件：**
- 新建：`src/quant_core/dashboard/pages/tasks.py`
- 新建：`tests/ui/test_tasks_page.py`
- 新建：`tests/integration/test_dashboard_task_lifecycle.py`

- [ ] **步骤 1：写任务状态与刷新测试**

列表显示状态、类型、实验、阶段、进度、Worker、心跳、开始/结束时间和结构化错误。运行任务每 2–5 秒刷新且缓存不超过 5 秒；心跳过期显示 `ORPHANED` 警告，不显示为普通失败。

- [ ] **步骤 2：写取消、重试与日志限制测试**

排队和运行任务可取消；结束任务不可取消；只有符合安全条件的结束任务可重试；孤儿重试必须二次确认；日志默认尾部 500 行，最大 5000 行，HTML 转义并屏蔽密钥。

- [ ] **步骤 3：实现页面和审计反馈**

所有命令携带 actor、request ID；成功后显示新尝试 ID，冲突时展示当前状态和可采取操作。页面不直接更新数据库。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/ui/test_tasks_page.py tests/integration/test_dashboard_task_lifecycle.py -v`

```bash
git add src/quant_core/dashboard/pages/tasks.py tests/ui/test_tasks_page.py tests/integration/test_dashboard_task_lifecycle.py
git commit -m "feat: operate background tasks from dashboard"
```

### 任务 7：Notebook 策略实验室、启动脚本与端到端验收

**文件：**
- 新建：`notebooks/00_quickstart.ipynb`
- 新建：`notebooks/10_etf_rotation.ipynb`
- 新建：`notebooks/20_multifactor.ipynb`
- 新建：`scripts/start-worker.ps1`
- 新建：`scripts/start-dashboard.ps1`
- 新建：`scripts/backup-state.ps1`
- 扩展：`src/quant_core/cli.py`
- 新建：`tests/notebooks/test_notebooks.py`
- 新建：`tests/e2e/test_dashboard_worker_independence.py`
- 新建：`tests/e2e/test_full_research_workflow.py`
- 新建：`tests/e2e/test_tushare_source_substitution.py`
- 新建：`README.md`

- [ ] **步骤 1：先写 Notebook 可执行测试**

使用 `nbclient` 在临时 `QUANT_DATA_ROOT` 执行三本 Notebook。Quickstart 检查环境、列快照、提交实验并读取结果；ETF 和多因子 Notebook 使用小型黄金快照，单元格不得依赖手工执行顺序，不得保存核心实现代码。

- [ ] **步骤 2：实现 Notebook 与中文说明**

每本 Notebook 固定随机种子、显式快照 ID、策略版本和配置；开头说明数据和规则口径，结尾登记研究说明。图表读取正式产物，不从临时 DataFrame 重新计算最终指标。

- [ ] **步骤 3：实现本地启动和备份脚本**

`start-worker.ps1` 运行 `uv run quant worker run`；`start-dashboard.ps1` 强制地址 `127.0.0.1` 并从配置读取端口；启动前检查目录和数据库迁移。`backup-state.ps1` 在一致性检查后复制 SQLite、快照清单和实验 manifest，不复制可重建缓存，并生成备份清单哈希。

- [ ] **步骤 4：运行进程独立性测试**

启动 Worker 和 Dashboard，提交小型实验后停止 Dashboard，等待 Worker 完成，再重启 Dashboard。断言任务未中断、结果可见、页面缓存按新 manifest 失效。

- [ ] **步骤 5：运行完整研究链路**

黄金链路固定为 `离线 BaoStock fixture → Raw → Curated → Quality → Snapshot → Universe → Factors → Experiment → Worker → Backtest → Analytics → DashboardService`。断言快照、指纹、任务、产物、指标和审计事件可串联追踪。

- [ ] **步骤 6：完成 TuShare 替换端到端证明**

`tests/e2e/test_tushare_source_substitution.py` 使用第一份计划中的 `FakeTushareSourceClient` 与 `FakeTushareCanonicalMapper` 生成和 BaoStock 黄金 fixture 语义等价的快照。对两个快照运行同一 `UniverseBuilder`、因子集、ETF/多因子策略、`BacktestEngine`、分析物化和 `DashboardService` 查询；断言无需修改上层配置 schema 或代码路径，规范结果在允许的数据源数值容差内一致，并扫描 `strategies`、`portfolio`、`backtest`、`analytics`、`experiments`、`dashboard` 的公共模型，确保不存在供应商专有字段名。

运行：`uv run pytest tests/e2e/test_tushare_source_substitution.py -v`

- [ ] **步骤 7：执行最终验证**

运行：`uv run pytest tests/unit tests/point_in_time tests/integration tests/regression tests/notebooks tests/ui tests/e2e -v`

运行：`uv run ruff format --check . && uv run ruff check src tests && uv run mypy src`

运行：`uv run streamlit run src/quant_core/dashboard/app.py --server.address 127.0.0.1 --server.headless true`

预期：测试与静态检查全部通过；Streamlit 健康检查成功，控制台未监听 `0.0.0.0`。

- [ ] **步骤 8：提交**

```bash
git add notebooks scripts src/quant_core/cli.py tests/notebooks tests/e2e README.md
git commit -m "feat: deliver local quant research workspace"
```

## 验收门槛

- 六个 Dashboard 页面全部通过服务层访问系统，无页面直连数据库或任意文件路径。
- 总览、数据中心、实验中心、回测分析、因子分析、任务与日志职责完整。
- 三本 Notebook 可以从空内核按顺序执行并复现实验。
- 停止 Dashboard 不影响 Worker，重启后状态与结果完整恢复。
- 所有写操作有状态校验和审计事件，完成实验不可被页面修改或删除。
- Dashboard 仅绑定 `127.0.0.1`，不提供任意 SQL/Python/路径入口。
- 模拟 TuShare 数据源替换测试证明股票池、因子、策略、回测、分析和 Dashboard 均不依赖 BaoStock 专有模型。
- 端到端黄金链路、pytest、Ruff 和 mypy 全部通过。
