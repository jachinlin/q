# `quant` CLI 总体设计

## 1. 文档定位

本文定义 `quant` 命令行接口的整体架构、命令树、输入输出协议、错误模型、资源生命周期、日志、安全边界和验收标准。

CLI 是本地量化平台的稳定进程接口，不是业务逻辑容器。它负责：

- 解析命令、参数和选项；
- 将输入转换为严格应用 DTO；
- 调用应用服务或能力端口；
- 将结果转换为确定性 JSON；
- 将领域错误转换为稳定错误结构和退出码；
- 保证命令级资源最终释放。

CLI 不得实现数据流水线、实验状态机、任务队列或 Worker 业务规则，也不得在命令模块中直接创建数据库、供应商客户端或网络连接。

## 2. 设计目标与非目标

### 2.1 目标

1. 命令树易于人工发现，也适合 PowerShell、批处理和外部进程调用。
2. 一次性命令使用机器可读、排序稳定的 JSON 输出。
3. 所有失败都有结构化错误、稳定错误码和明确退出码。
4. `--help` 不装配服务、不升级数据库、不连接供应商。
5. CLI 与 Dashboard 共享应用用例，不互相导入。
6. 数据、任务、实验和 Worker 命令保持一致的资源与错误边界。
7. 本地路径、环境变量和异常上下文在输出前完成敏感信息脱敏。

### 2.2 非目标

CLI 不负责：

- 提供交互式 TUI；
- 在 stdout 输出表格、进度动画或非结构化提示；
- 通过 CLI 参数重定义数据 schema、交易规则或实验格式；
- 在 `experiments submit` 中同步执行实验；
- 在任务重试时复写旧任务、旧实验或旧产物；
- 提供 `quant_core`、Snapshot、dataset version 等旧接口兼容层。

## 3. 进程入口与架构边界

安装入口固定为：

```toml
[project.scripts]
quant = "quant_research.bootstrap.cli:main"
```

调用链为：

```text
OS process
   │
   ▼
bootstrap.cli.main
   │
   ├── create_app(CliBootstrap.build_services)
   │             │
   │             └── command invocation 时才装配真实依赖
   ▼
cli.app.run
   │
   ▼
Typer command group
   │
   ▼
application service / capability port
```

模块职责：

| 模块 | 职责 |
|---|---|
| `bootstrap.cli` | 读取配置，升级数据库，装配 Pipeline、Queue、ExperimentClient 和 Worker。 |
| `cli.app` | 创建根命令树，定义端口、输出、错误和资源边界。 |
| `cli.data` | 注册数据命令并完成数据参数适配。 |
| `cli.tasks` | 注册任务查询、取消和重试命令。 |
| `cli.experiments` | 注册实验提交和查询命令。 |
| `cli.worker` | 注册 Worker 生命周期命令。 |
| `cli.runtime` | 注册本地组合启动命令并监督 Dashboard、Worker 与 Notebook 子进程。 |

依赖规则：

```text
bootstrap → cli → application / capability contracts
bootstrap → infrastructure
cli ↛ bootstrap
cli ↛ dashboard
cli ↛ infrastructure
application / capabilities ↛ cli
```

`dashboard` 命令只负责启动 Dashboard 组合根，不把 Dashboard 服务导入 CLI 命令实现。

## 4. 命令树

根命令不带子命令时显示帮助：

```text
quant
├── dashboard
├── start
├── data
│   ├── bootstrap
│   ├── update
│   ├── localize
│   ├── localize-all
│   ├── curate
│   ├── curate-all
│   ├── validate
│   └── validate-all
├── tasks
│   ├── list
│   ├── cancel
│   └── retry
├── experiments
│   ├── submit
│   └── show
└── worker
    ├── once
    └── run
```

命令名、位置参数、选项名、默认值、退出码和 JSON 字段共同构成 CLI 外部契约。项目不提供已删除接口的别名或转发命令。

## 5. 通用调用规则

### 5.1 帮助与发现

以下调用必须只构造 Typer 命令树，不调用 `services_factory`：

```bash
uv run quant --help
uv run quant data --help
uv run quant data curate --help
uv run quant tasks --help
```

帮助文本是面向人的终端输出，不受一次性命令 JSON 协议约束。帮助成功退出码为 `0`。

### 5.2 服务创建

每次实际命令调用：

1. 调用一次 `services_factory`；
2. 执行一个应用操作；
3. 将返回值转换为公开结果 DTO；
4. 无论成功或失败，都调用一次 `ApplicationServices.close()`；
5. 服务关闭失败时，如果此前没有错误，则转换为 `CLI_SERVICE_CLOSE_FAILED`。

`ApplicationServices.close()` 必须幂等。CLI 不在模块导入时持有活动数据库事务、网络会话或 Worker 线程。

### 5.3 输入规范

- 日期只接受明确的 `YYYY-MM-DD`。
- 成对日期选项必须同时出现或同时省略。
- dataset 参数必须是 `DatasetKind` 目录中的当前值。
- ID 使用系统生成的完整字符串，不进行模糊匹配。
- 配置文件路径由应用服务在可信配置根下解析。
- CLI 不接受任意 JSON/Python 表达式作为业务配置。
- 数值范围在进入应用层前验证。

## 6. 输出协议

### 6.1 stdout

一次性命令成功时，stdout 只写一个 UTF-8 JSON 对象，并以换行结束：

```text
json.dumps(payload, ensure_ascii=False, sort_keys=True)
```

若业务结果未包含 `status`，CLI 自动增加：

```json
{"status":"SUCCEEDED"}
```

stdout 不得混入日志、提示语、进度条、Python repr 或 traceback。字段值只允许 JSON 安全类型；日期和时间使用 ISO 8601 字符串，枚举使用稳定字符串值。

### 6.2 stderr

stderr 用于：

- 一个结构化失败对象；
- JSON Lines 运行日志；
- Uvicorn 等长驻进程的受控运行日志。

调用方应以退出码判断成功，以 stdout 解析一次性命令结果。stderr 可以包含多行结构化日志，不能假设只包含最终错误对象。

### 6.3 长驻命令

`quant dashboard`、`quant worker run` 和 `quant start` 是长驻进程：

- `dashboard` 把进程控制交给 Uvicorn，监听 `127.0.0.1`；
- `worker run` 持续轮询，收到 `SIGINT`、`SIGTERM` 或 Windows `SIGBREAK` 后请求安全停止；
- `worker run` 正常停止后输出 `{"status":"SUCCEEDED","stopped":true}`；
- `start` 以前台监督器启动三个独立子进程，任一子进程退出或收到 `Ctrl+C` 时统一关闭其余进程；
- 被操作系统强制终止时，不保证输出终态 JSON。

## 7. 错误与退出码

### 7.1 错误对象

所有受控失败统一输出：

```json
{
  "error": {
    "code": "DATASET_UNSUPPORTED",
    "severity": "SEVERE",
    "message": "dataset is not in the catalog",
    "context": {"dataset": "unknown"},
    "remediation": "correct the command arguments and retry",
    "retryable": false
  }
}
```

字段语义：

| 字段 | 语义 |
|---|---|
| `code` | 供调用方分支处理的稳定错误码。 |
| `severity` | 统一严重级别。 |
| `message` | 面向用户的简洁说明。 |
| `context` | 已脱敏的诊断上下文。 |
| `remediation` | 可执行的恢复建议。 |
| `retryable` | 相同输入在外部状态变化后是否值得重试。 |

### 7.2 退出码

| 退出码 | 含义 |
|---:|---|
| `0` | 命令成功，或帮助正常显示。 |
| `2` | 参数、领域、基础设施或资源关闭失败，已输出结构化错误。 |

Typer/Click 参数错误必须由进程边界转换为 `CLI_ARGUMENT_INVALID`，不得直接输出 Click 默认错误页。命令内部抛出的 `QuantError` 保留原错误码；未知异常分别包装为 `DATA_PIPELINE_UNEXPECTED` 或 `CLI_UNEXPECTED`，不向终端暴露 traceback。

进程被操作系统信号或运行时强制终止时，可以使用平台约定退出码，这不属于 CLI 业务错误协议。

### 7.3 脱敏

错误输出和日志写入前必须通过统一脱敏器处理：

- 环境变量中的敏感值；
- 凭证、token、cookie 和连接密钥；
- 不应公开的绝对路径片段；
- 供应商原始异常中的敏感字段。

脱敏自身失败时，输出固定安全占位错误，不回退输出原始上下文。

## 8. 数据命令

数据生命周期固定为：

```text
LOCALIZE → CURATE → VALIDATE
```

`validate <dataset>` 只诊断单个数据集；只有 `validate-all` 可以开放研究读取。

### 8.1 `data bootstrap`

```bash
uv run quant data bootstrap
```

执行首次完整数据构建。成功结果：

```json
{
  "data_hash": "<sha256>",
  "quality_run_id": "<uuid>",
  "run_id": "<uuid>",
  "status": "SUCCEEDED"
}
```

### 8.2 `data update`

```bash
uv run quant data update
uv run quant data update --start 2026-01-01 --end 2026-01-31
```

`--start` 与 `--end` 必须同时提供。省略时由数据流水线按当前目录状态确定更新窗口。

### 8.3 `data localize`

```bash
uv run quant data localize <dataset>
uv run quant data localize <dataset> --from 2026-01-01 --to 2026-01-31
uv run quant data localize <dataset> --full
```

选项：

| 选项 | 语义 |
|---|---|
| `--from` / `--to` | 显式请求窗口，必须成对出现。 |
| `--full` | 忽略增量跳过判定，按命令窗口重新本地化。 |

成功结果：

```json
{
  "dataset": "daily_bar",
  "fetched": 10,
  "raw_partitions": 10,
  "skipped": 2,
  "status": "SUCCEEDED"
}
```

`data localize-all` 使用相同窗口选项，结果放在 `datasets` 数组中，并保持数据目录顺序稳定。

### 8.4 `data curate`

```bash
uv run quant data curate <dataset>
uv run quant data curate <dataset> --from 2026-01-01 --to 2026-01-31
uv run quant data curate <dataset> --full
```

成功结果至少包含：

```json
{
  "content_hash": "<sha256>",
  "dataset": "daily_bar",
  "partitions": 12,
  "raw_inputs_read": 2,
  "rebuilt_partitions": 2,
  "reused_partitions": 10,
  "rows": 123456,
  "status": "SUCCEEDED"
}
```

`data curate-all` 只接受 `--full`，不接受局部日期窗口；增量边界由各数据集 Canonical 输入身份决定。

### 8.5 `data validate`

```bash
uv run quant data validate <dataset>
uv run quant data validate-all
```

成功结果：

```json
{
  "quality_run_id": "<uuid>",
  "status": "SUCCEEDED"
}
```

命令成功表示质量运行完成，不等同于单数据集诊断开放了研究门禁。调用方需要按业务错误和质量记录判断诊断结果。

## 9. 实验命令

### 9.1 `experiments submit`

```bash
uv run quant experiments submit configs/experiments/examples/etf_rotation.yaml
```

提交过程：

1. 在可信配置根下解析 YAML；
2. 严格验证实验配置；
3. 要求当前 Catalog 已验证；
4. 捕获数据、源码、锁文件和规则身份；
5. 创建不可变实验；
6. 创建并绑定后台任务；
7. 返回标识，不同步执行 Worker。

成功结果：

```json
{
  "experiment_id": "<uuid>",
  "experiment_status": "QUEUED",
  "status": "SUCCEEDED",
  "task_id": "<uuid>",
  "task_status": "QUEUED"
}
```

顶层 `status` 表示 CLI 调用成功，`experiment_status` 和 `task_status` 表示持久化业务对象状态，两者不得混用。

### 9.2 `experiments show`

```bash
uv run quant experiments show <experiment-id>
```

返回：

- 实验身份、状态、策略和数据哈希；
- 当前任务摘要；
- 任务尝试历史；
- 最新进度与安全错误摘要；
- 指标和已登记产物摘要。

公开结果不返回任务日志路径、数据库字段或未验证产物的可信路径。

## 10. 任务命令

### 10.1 `tasks list`

```bash
uv run quant tasks list
uv run quant tasks list --status FAILED --limit 50 --offset 0
```

约束：

- `status` 必须是当前 `TaskStatus`；
- `limit` 范围为 1 至 500，默认 100；
- `offset` 必须为非负整数，默认 0。

返回 `tasks`、`limit`、`offset` 和顶层命令状态。任务错误只公开 `code` 与 `retryable`，不展开内部异常。

### 10.2 `tasks cancel`

```bash
uv run quant tasks cancel <task-id>
```

CLI 使用严格取消语义：任务不存在或状态不允许取消时失败；成功时返回原 `task_id` 和转换后的 `task_status`。

### 10.3 `tasks retry`

```bash
uv run quant tasks retry <task-id>
```

重试克隆业务身份并创建新任务，不复用旧任务 ID。实验类任务还会创建新实验身份。返回原任务、原实验、新任务和新实验 ID。

## 11. Worker 命令

### 11.1 `worker once`

```bash
uv run quant worker once
```

最多领取并执行一个任务：

- 无可领取任务时返回 `worked=false`；
- 有任务时，必须等待持久化终态后返回任务与实验状态；
- Worker 完成但缺少持久化运行结果时视为错误。

### 11.2 `worker run`

```bash
uv run quant worker run
```

在当前进程主线程运行持续 Worker。CLI 安装并在退出时恢复信号处理器。无法安装安全停止信号时返回 `WORKER_SIGNAL_UNAVAILABLE`，不得退化为无法控制的后台循环。

Worker Profile 由 `QUANT_WORKER_PROFILE` 选择：

- `baostock`：默认真实本地 Profile；
- `offline-etf`：离线 ETF Profile。

Profile 选择影响组合根装配，不改变命令树或输出协议。

## 12. Dashboard 命令

```bash
uv run quant dashboard --port 8000
```

约束：

- `port` 范围为 1 至 65535，默认 8000；
- host 固定为 `127.0.0.1`，不得通过 CLI 暴露到所有网卡；
- 使用 `quant_research.bootstrap.dashboard:create_dashboard_app` factory；
- Dashboard 的数据库、服务和 `frontend/dist` 由 Dashboard 组合根装配；
- CLI 不导入 Dashboard 路由或持久化实现。

## 13. 组合启动命令

```bash
uv run quant start
```

该命令面向本地交互研究，一次启动三个独立进程：

- Dashboard：Uvicorn factory，默认监听 `127.0.0.1:8000`；
- Worker：等价于 `quant worker run`；
- Notebook：JupyterLab，监听 `127.0.0.1:8009`，不自动打开浏览器，并通过空 `IdentityProvider.token` 禁用认证 token。

选项：

| 选项 | 默认值 | 约束 |
|---|---:|---|
| `--dashboard-port` | `8000` | 1 至 65535，且不得使用固定 Notebook 端口 8009。 |
| `--notebook-dir` | `<当前目录>/notebooks` | 默认目录不存在时自动创建；显式路径必须是已存在的目录。 |

JupyterLab 由可选依赖组提供，首次使用前执行 `uv sync --group notebook`。监督器必须先验证
依赖和端口，再启动任何子进程；部分启动失败时关闭已经启动的进程。正常启动后输出包含
三个 PID 的 `RUNNING` 事件，停止时输出 `STOPPED` 或 `FAILED` 事件。子进程保留各自的
日志输出，因此该长驻命令不适合作为只解析单个 stdout JSON 的批处理接口。

Notebook 端口固定为 8009，不提供覆盖选项；`--dashboard-port 8009` 因端口冲突而被拒绝。
禁用 token 只适用于当前单用户本机运行模型；Notebook 必须继续绑定回环地址，不得改为
`0.0.0.0` 或其他可被局域网访问的地址。
Jupyter Server 的 `Content-Security-Policy` 根据实际 Dashboard 端口生成，只允许
`'self'`、`http://127.0.0.1:<dashboard-port>` 和
`http://localhost:<dashboard-port>` 作为 frame ancestor，不接受通配来源。
Dashboard 的 `/api/v1/health` 首次返回成功后，监督器只调用一次系统默认浏览器打开
Dashboard 根地址。健康检查尚未就绪时继续等待；浏览器调用失败只输出
`BROWSER_OPEN_FAILED` 事件，不终止已经运行的服务。
侧边栏和总览的 Notebook 入口统一进入 Dashboard `/notebook` 路由；该页面确认
`/api/v1/notebook/status` 就绪后，在主内容区内嵌
`http://127.0.0.1:8009/lab`，不新开浏览器标签。

CLI 只拼装稳定的模块入口命令，不导入 Dashboard、Worker 组合根或 Notebook 实现。

## 14. 配置与本地资源

CLI 使用以下环境变量：

| 环境变量 | 语义 |
|---|---|
| `QUANT_DATA_ROOT` | 数据根目录，默认 `~/.q-data`，必须位于源码目录之外。 |
| `QUANT_CONFIG` | 应用 YAML，默认 `<source_root>/configs/base.yaml`。 |
| `QUANT_WORKER_PROFILE` | Worker 组合 Profile。 |

生产组合根在实际命令调用时：

1. 解析源码根，并由 `Settings.load()` 解析配置路径和数据根；
2. 校验数据根位于源码树之外；
3. 将 SQLite 升级到当前 Alembic head；
4. 创建 Engine 和 Repository；
5. 装配 BaoStock、数据 Pipeline、任务队列、实验客户端和 Worker；
6. 将 Pipeline 日志刷新、文件关闭和 Engine dispose 注册为同一命令级关闭回调。

数据库升级失败、配置非法或数据根位于源码树内时，命令必须在执行任何业务写入前失败。

## 15. 日志契约

数据流水线日志写入：

```text
<data-root>/logs/data_pipeline.log
```

同时以 JSON Lines 镜像到 stderr。统一外层字段只包含时间、级别、事件、阶段和关联 ID；每个命令定义自己的业务 `context`：

- Localize 记录完整供应商 request 和 Raw 发布元数据；
- Curate 记录 Canonical 分区、内容哈希、行数、合并状态和指针结果；
- Validate 记录目录身份、规则、质量问题和门禁结果；
- Task、Worker 与 Experiment 记录各自 payload、attempt、progress 和 outcome。

日志不得改变 stdout 成功对象，也不得要求所有命令伪造统一 request schema。

日志写入是最佳努力的缓冲诊断通道。`StructuredLogger.emit()` 在 logger 写锁内只追加
一条完整 JSON Lines 记录，不逐条刷新；Pipeline 文件和 stderr 都只在服务正常关闭
边界显式刷新。写入、刷新或关闭 sink 失败不得改变业务命令的返回对象和退出语义，
进程异常终止时允许丢失尚未刷新的窗口。日志入参和结构错误不属于 sink 故障，仍正常
抛出。

任务日志只在显式刷新、封存、物化和正常关闭边界尝试 `flush+fsync`。任务日志文件的
打开、写入、刷新或关闭发生 I/O 故障时，Worker 继续执行任务；路径越界、重解析点、
Worker 所有权、根目录配置不一致和日志关联字段冲突仍必须失败。任务队列根据可信日志
根、数据库 task ID 和 attempt ID 自行推导日志路径，不接收路径参数，也不使用能力凭证
或文件 `dev/inode` 身份校验。成功实验缺少可用真实任务日志时，最终产物中的
必需 `run.log` 使用 `task.log_unavailable` WARNING 占位记录，保留可信关联标识且不包含
原始异常文本。该文件与真实日志使用同一 manifest 和产物验证契约；占位文件也无法
写入时，实验按整体产物存储失败处理。

## 16. 扩展命令的规则

新增命令必须遵循：

1. 在职责对应的 `cli/<group>.py` 注册，不继续扩大 `cli.app`；
2. 通过消费者侧 Protocol 或应用服务调用业务能力；
3. 不导入 `infrastructure`、`dashboard` 或 `bootstrap`；
4. 明确定义参数、JSON schema、错误码和退出码；
5. 结果包含稳定对象 ID，不返回内部 ORM 或文件句柄；
6. 帮助路径不创建服务；
7. 为服务关闭、异常包装和脱敏增加测试；
8. 同步更新本文和命令级 `--help`。

若一个命令需要长时间执行，优先提交后台任务并返回任务 ID；只有进程控制类操作才使用长驻前台命令。

## 17. 测试与验收

### 17.1 命令树测试

- 根命令和每个命令组无参数时显示帮助。
- `--help` 不调用 `services_factory`。
- 命令名、参数、选项、默认值和范围与本文一致。
- 已删除的 Snapshot 和旧包命令不存在。

### 17.2 输出与错误测试

- 成功 stdout 恰好包含一个可解析 JSON 对象。
- JSON 键排序稳定，中文不转义为 ASCII 序列。
- 失败 stdout 为空，stderr 包含结构化错误。
- 参数错误和领域错误退出码为 2。
- 未知异常不输出 traceback、repr 或敏感值。
- 日志输出不污染 stdout。

### 17.3 资源生命周期测试

- 每次实际调用只创建一次服务。
- 成功、领域失败、未知异常都关闭服务。
- `close()` 幂等。
- 执行失败优先于关闭失败；只有无既有失败时才报告关闭错误。
- Dashboard 与 Worker 的信号处理和退出路径恢复全局状态。
- 组合启动时的部分失败、子进程提前退出和 `Ctrl+C` 都不会留下孤儿进程。

### 17.4 集成与发布测试

- 使用源码目录外的临时 `QUANT_DATA_ROOT` 验证真实组合根。
- 单元测试使用 fake/stub，禁止真实网络访问。
- 验证空数据库建库和现有数据库升级。
- 构建 wheel 后在隔离环境运行 `quant --help`。
- Windows、PowerShell 和 UTF-8 输出均纳入验收。

## 18. 完成定义

CLI 只有同时满足以下条件才形成稳定进程契约：

- 命令树和帮助无需应用依赖即可构造；
- 一次性命令成功只输出一个确定性 JSON 对象；
- 所有受控失败具有结构化错误和退出码 2；
- 业务规则位于应用或能力层，CLI 只做输入输出适配；
- 真实依赖只在组合根装配，并在命令结束时可靠释放；
- 日志、错误和结果严格分流并完成敏感信息脱敏；
- 长驻进程能够响应平台信号安全退出；
- 数据、实验、任务和 Worker 命令保持身份、状态和重试语义；
- wheel 安装后的 `quant` 入口、帮助、输出和数据库资源均通过验收。
