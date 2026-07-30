# 实验登记、任务队列与 Worker 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 建立可复现的实验登记系统、SQLite 持久任务队列、独立 Worker 进程和 Notebook SDK，使长回测在关闭 Notebook 或 Dashboard 后仍能可靠完成。

**架构：** 每次研究提交先创建不可变实验配置和指纹，再创建可重试的任务尝试。SQLite 保存控制状态，实验目录保存大体量产物。Worker 在短事务中领取任务，执行期间通过心跳、进度和协作式取消与控制库交互。实验完成态不可回退，失败重试不会覆盖已成功产物。

**技术栈：** Python 3.12、SQLAlchemy 2、Alembic、SQLite WAL、Pydantic v2、PyYAML、Typer、structlog、pytest。

## 前置条件与约束

- 先完成前三份实施计划。
- 实验指纹包含策略 ID/版本、规范配置、快照 manifest 哈希、源码树或 Git 提交哈希、锁文件哈希和 RuleBook 版本。
- 相同指纹用于提示重复研究，不复用同一个实验记录。
- 任务领取事务必须短；回测和因子计算不得持有数据库写锁。
- Worker 默认每 2 秒轮询、每 10 秒心跳；60 秒无心跳标记为 `ORPHANED`。
- `ORPHANED` 不自动重跑，必须由用户显式创建新尝试。
- 日志和进度写入持久介质，不依赖进程内存。

---

## 文件职责

| 路径 | 作用 |
|---|---|
| `src/quant_core/experiments/models.py` | 实验配置、状态、标记和结果 DTO |
| `src/quant_core/experiments/fingerprint.py` | 实验指纹与环境哈希 |
| `src/quant_core/experiments/registry.py` | 创建、状态转换、产物和指标登记 |
| `src/quant_core/experiments/runner.py` | 配置解析、策略、回测、分析编排 |
| `src/quant_core/experiments/query.py` | Notebook 与 Dashboard 的只读查询 |
| `src/quant_core/tasks/models.py` | 任务、尝试、租约、进度和取消模型 |
| `src/quant_core/tasks/queue.py` | 入队、领取、心跳、取消、结束事务 |
| `src/quant_core/tasks/worker.py` | 单 Worker 轮询和运行时 |
| `src/quant_core/tasks/handlers.py` | 数据、因子、回测和报告任务处理器 |
| `src/quant_core/persistence/migrations/versions/0002_experiments_tasks.py` | 实验与任务表迁移 |
| `src/quant_core/cli.py` | 实验与 Worker 命令 |

### 任务 1：实验与任务数据库模型

**文件：**
- 扩展：`src/quant_core/persistence/orm.py`
- 扩展：`src/quant_core/persistence/repositories.py`
- 新建：`src/quant_core/persistence/migrations/versions/0002_experiments_tasks.py`
- 新建：`src/quant_core/experiments/models.py`
- 新建：`src/quant_core/tasks/models.py`
- 新建：`tests/integration/test_experiment_task_migration.py`

**状态模型：**

```python
class ExperimentStatus(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class ResearchMark(StrEnum):
    UNREVIEWED = "UNREVIEWED"
    BASELINE = "BASELINE"
    CANDIDATE = "CANDIDATE"
    DISCARDED = "DISCARDED"

class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    ORPHANED = "ORPHANED"
```

- [ ] **步骤 1：先写迁移与约束测试**

测试从 `0001` 升级到 `0002`，至少创建 `experiment`、`experiment_tag`、`experiment_metric`、`experiment_artifact`、`task`、`task_attempt`、`audit_event`。验证外键、唯一索引、状态 CHECK、UTC 时间字段和同一任务尝试序号唯一。

- [ ] **步骤 2：运行并确认失败**

运行：`uv run pytest tests/integration/test_experiment_task_migration.py -v`

- [ ] **步骤 3：实现迁移和领域 DTO**

ORM 使用字符串保存枚举值；数据库约束拒绝未知状态。JSON 配置保存规范文本和 SHA-256。`experiment` 只保存标量元数据和产物索引，不保存净值/持仓大表。

- [ ] **步骤 4：升级、测试并提交**

运行：`uv run alembic upgrade head`

运行：`uv run pytest tests/integration/test_experiment_task_migration.py -v`

```bash
git add src/quant_core/persistence src/quant_core/experiments/models.py src/quant_core/tasks/models.py tests/integration/test_experiment_task_migration.py
git commit -m "feat: add experiment and task persistence schema"
```

### 任务 2：实验指纹与产物契约

**文件：**
- 新建：`src/quant_core/experiments/fingerprint.py`
- 扩展：`src/quant_core/backtest/artifacts.py`
- 新建：`tests/unit/experiments/test_fingerprint.py`
- 新建：`tests/integration/test_experiment_artifact_contract.py`

**接口：**

```python
@dataclass(frozen=True)
class ExperimentFingerprintInput:
    strategy_id: str
    strategy_version: str
    resolved_config: Mapping[str, JsonValue]
    snapshot_manifest_hash: str
    source_hash: str
    lockfile_hash: str
    rulebook_version: str

def compute_fingerprint(value: ExperimentFingerprintInput) -> str: ...
```

- [ ] **步骤 1：先写规范化指纹测试**

键顺序和 YAML 表达差异不改变指纹；整数与浮点、缺省值与显式值按解析后的配置模型处理；策略版本、快照、源码、锁文件或 RuleBook 任一变化都改变指纹。

- [ ] **步骤 2：写产物完整性测试**

成功实验目录必须包含 `manifest.json`、`resolved_config.yaml`、`environment.json`、`metrics.json`、`nav.parquet`、`drawdown.parquet`、`holdings.parquet`、`targets.parquet`、`fills.parquet`、`costs.parquet`、`factor_metrics.parquet`、`attribution.parquet`、`quality_disclosure.json`、`report.html`、`run.log`。缺失、schema 错误或哈希不符时不得登记成功。

- [ ] **步骤 3：实现规范哈希和产物验证器**

JSON 使用排序键、UTF-8 和稳定数字编码。源码优先取 Git 提交；工作树非干净或无 Git 时计算受版本控制源码树哈希，并写入环境披露。产物先在 staging 目录完成，校验后原子发布。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/experiments/test_fingerprint.py tests/integration/test_experiment_artifact_contract.py -v`

```bash
git add src/quant_core/experiments/fingerprint.py src/quant_core/backtest/artifacts.py tests/unit/experiments tests/integration/test_experiment_artifact_contract.py
git commit -m "feat: fingerprint experiments and verify artifacts"
```

### 任务 3：实验注册表与状态机

**文件：**
- 新建：`src/quant_core/experiments/registry.py`
- 新建：`src/quant_core/experiments/query.py`
- 新建：`tests/unit/experiments/test_state_machine.py`
- 新建：`tests/integration/test_experiment_registry.py`

**接口：**

```python
class ExperimentRegistry:
    def create(self, config: ResolvedExperimentConfig, fingerprint: str) -> ExperimentId: ...
    def transition(self, experiment_id: ExperimentId, expected: ExperimentStatus, target: ExperimentStatus, reason: ErrorDetail | None = None) -> None: ...
    def register_success(self, experiment_id: ExperimentId, manifest: ArtifactManifest, metrics: Mapping[str, float]) -> None: ...
    def update_research(self, experiment_id: ExperimentId, mark: ResearchMark, tags: Sequence[str], note: str, actor: str) -> None: ...
```

- [ ] **步骤 1：写状态转换矩阵测试**

只允许 `CREATED→QUEUED→RUNNING→SUCCEEDED|FAILED|CANCELLED`，以及 `QUEUED→CANCELLED`。完成态不可回退。并发转换使用 `expected` 做比较交换，失败返回 `StateConflict`。

- [ ] **步骤 2：写不可变与审计测试**

成功实验的配置、快照、指纹、指标和产物不可编辑；研究标记、标签和说明允许追加/修改，但每次写操作生成包含主体、动作、对象、旧值、新值、请求 ID、UTC 时间的 `audit_event`。

- [ ] **步骤 3：实现注册表和只读查询**

实验创建每次生成新 UUID；查询可按状态、策略、快照、标记、标签和时间范围过滤；相同指纹返回重复提示但不阻止创建。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/experiments/test_state_machine.py tests/integration/test_experiment_registry.py -v`

```bash
git add src/quant_core/experiments/registry.py src/quant_core/experiments/query.py tests/unit/experiments tests/integration/test_experiment_registry.py
git commit -m "feat: manage immutable experiment lifecycle"
```

### 任务 4：SQLite 持久任务队列

**文件：**
- 新建：`src/quant_core/tasks/__init__.py`
- 新建：`src/quant_core/tasks/queue.py`
- 新建：`tests/integration/test_task_queue.py`
- 新建：`tests/integration/test_task_queue_concurrency.py`

**接口：**

```python
class TaskQueue:
    def enqueue(self, task_type: str, payload: Mapping[str, JsonValue], priority: int, experiment_id: ExperimentId | None = None) -> TaskId: ...
    def claim(self, worker_id: str, now: datetime) -> ClaimedTask | None: ...
    def heartbeat(self, attempt_id: TaskAttemptId, worker_id: str, progress: TaskProgress, now: datetime) -> None: ...
    def request_cancel(self, task_id: TaskId, actor: str) -> None: ...
    def finish(self, attempt_id: TaskAttemptId, worker_id: str, outcome: TaskOutcome) -> None: ...
    def mark_orphans(self, now: datetime, stale_after: timedelta) -> int: ...
```

- [ ] **步骤 1：写优先级、FIFO 和幂等入队测试**

高优先级先领取，同优先级按创建时间和 ID；带幂等键的相同活动任务只能入队一次；结束任务可创建新尝试但保留历史。

- [ ] **步骤 2：写并发领取测试**

启动两个独立 SQLAlchemy 会话同时领取一条任务，断言只有一个获得任务。领取在 `BEGIN IMMEDIATE` 中更新 `worker_id`、`locked_at`、`heartbeat_at` 和尝试记录，事务结束后才执行工作。

- [ ] **步骤 3：写心跳、孤儿和取消测试**

非持有者不能心跳或结束任务；超过 60 秒标记 `ORPHANED`；孤儿不重新入队；运行任务取消转 `CANCEL_REQUESTED`，排队任务可直接 `CANCELLED`；完成态再次操作幂等或返回明确冲突。

- [ ] **步骤 4：实现队列事务**

SQLite 启用 WAL 和 busy timeout；锁冲突按有限退避重试。进度 JSON schema 固定为 `stage`、`completed`、`total`、`message`，并校验 `0 <= completed <= total`。

- [ ] **步骤 5：运行测试并提交**

运行：`uv run pytest tests/integration/test_task_queue.py tests/integration/test_task_queue_concurrency.py -v`

```bash
git add src/quant_core/tasks tests/integration/test_task_queue.py tests/integration/test_task_queue_concurrency.py
git commit -m "feat: add durable SQLite task queue"
```

### 任务 5：Worker 运行时和任务处理器

**文件：**
- 新建：`src/quant_core/tasks/handlers.py`
- 新建：`src/quant_core/tasks/worker.py`
- 新建：`tests/unit/tasks/test_worker.py`
- 新建：`tests/integration/test_worker_recovery.py`

**接口：**

```python
class TaskHandler(Protocol):
    task_type: str
    def run(self, task: ClaimedTask, progress: ProgressSink, cancellation: CancellationToken) -> TaskOutcome: ...

class Worker:
    def run_forever(self) -> None: ...
    def run_once(self) -> bool: ...
```

- [ ] **步骤 1：写轮询、心跳和关闭测试**

使用可控时钟测试每 2 秒无任务轮询、运行时每 10 秒心跳、处理器完成后停止心跳线程、SIGINT/控制台关闭时不领取新任务且等待当前批次边界。

- [ ] **步骤 2：写处理器与故障映射测试**

注册 `DATA_UPDATE`、`FACTOR_COMPUTE`、`BACKTEST`、`REPORT`。未知类型转不可重试失败；结构化 `QuantError` 保留错误码和可重试性；未知异常映射为 `WORKER_UNHANDLED_ERROR` 并记录堆栈，日志不得包含密钥。

- [ ] **步骤 3：实现 Worker 和协作式取消**

Worker 每次只运行一个任务。心跳使用独立短会话；处理器通过 `ProgressSink` 更新持久进度，通过 `CancellationToken` 在日期/分区批次边界检查状态。任务完成后先验证产物，再提交实验与任务完成状态。

- [ ] **步骤 4：验证崩溃恢复**

集成测试强制终止假处理器，在虚拟时间超过 60 秒后执行孤儿扫描；断言任务为 `ORPHANED`、已有临时产物未发布、重新尝试创建新的 attempt ID。

- [ ] **步骤 5：测试并提交**

运行：`uv run pytest tests/unit/tasks/test_worker.py tests/integration/test_worker_recovery.py -v`

```bash
git add src/quant_core/tasks/handlers.py src/quant_core/tasks/worker.py tests/unit/tasks tests/integration/test_worker_recovery.py
git commit -m "feat: run recoverable background worker"
```

### 任务 6：实验编排器与 Notebook SDK

**文件：**
- 新建：`src/quant_core/experiments/runner.py`
- 扩展：`src/quant_core/experiments/__init__.py`
- 新建：`tests/unit/experiments/test_runner.py`
- 新建：`tests/integration/test_experiment_client.py`

**Notebook 接口：**

```python
client = ExperimentClient.from_default_settings()
experiment = client.create_from_yaml("configs/experiments/examples/multifactor.yaml")
task = client.submit(experiment.id)
client.wait(task.id, poll_seconds=2)
result = client.result(experiment.id)
```

- [ ] **步骤 1：写配置解析和阶段编排测试**

解析配置时固化全部缺省值、快照 ID、策略版本和规则版本。Runner 阶段固定为 `VALIDATE→UNIVERSE→FACTOR_COMPUTE→BACKTEST→ANALYTICS→ARTIFACT_VERIFY→REGISTER`，失败时记录最后阶段且不得跳过产物验证。

- [ ] **步骤 2：写客户端进程独立性测试**

`submit()` 只入队并立即返回；模拟销毁客户端后，由单独 Worker 实例继续完成；`wait()` 只轮询数据库；`result()` 仅在成功且产物通过校验时返回。

- [ ] **步骤 3：实现 Runner、Client 和 Result**

`ExperimentResult.metrics()` 读取 `metrics.json`，`nav()` 读取单个实验的 `nav.parquet`；路径必须通过已登记产物解析，不能接收任意用户路径。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/experiments/test_runner.py tests/integration/test_experiment_client.py -v`

```bash
git add src/quant_core/experiments tests/unit/experiments/test_runner.py tests/integration/test_experiment_client.py
git commit -m "feat: orchestrate experiments from notebooks"
```

### 任务 7：CLI、日志与端到端运行验收

**文件：**
- 扩展：`src/quant_core/cli.py`
- 新建：`src/quant_core/logging.py`
- 新建：`tests/integration/test_worker_cli.py`
- 新建：`tests/e2e/test_notebook_to_worker.py`

**CLI：** `quant worker run`、`quant worker once`、`quant tasks list`、`quant tasks cancel TASK_ID`、`quant tasks retry TASK_ID`、`quant experiments submit CONFIG`、`quant experiments show EXPERIMENT_ID`。

- [ ] **步骤 1：写 CLI 失败测试**

验证未知 ID、非法状态、重复取消、安全重试、结构化 JSON 输出和非零退出码。重试只允许结束任务且不存在活动尝试；已成功实验必须复制配置创建新实验，不能覆盖。

- [ ] **步骤 2：实现结构化日志**

每条日志至少包含 UTC 时间、级别、事件、`request_id`、`experiment_id`、`task_id`、`attempt_id`、`worker_id`、阶段和错误码。配置过滤器屏蔽令牌、密码和环境变量值；每个任务写独立 `run.log`。

- [ ] **步骤 3：运行真正的多进程端到端测试**

测试进程 A 创建并提交小型实验后退出；进程 B 执行 `quant worker once`；进程 C 查询结果。断言实验成功、任务成功、心跳/进度有记录、所有产物通过 manifest 校验。

- [ ] **步骤 4：执行总体验收**

运行：`uv run pytest tests/unit/experiments tests/unit/tasks tests/integration/test_task_queue.py tests/integration/test_worker_recovery.py tests/integration/test_experiment_client.py tests/integration/test_worker_cli.py tests/e2e/test_notebook_to_worker.py -v`

运行：`uv run ruff check src tests && uv run mypy src`

- [ ] **步骤 5：提交**

```bash
git add src/quant_core/cli.py src/quant_core/logging.py tests/integration/test_worker_cli.py tests/e2e/test_notebook_to_worker.py
git commit -m "feat: complete experiment worker workflow"
```

## 验收门槛

- 关闭 Notebook 或 Dashboard 后，独立 Worker 仍能完成已提交实验。
- 并发领取测试证明一条任务只能被一个 Worker 获得。
- 心跳过期任务进入 `ORPHANED`，且不会自动重跑。
- 完成实验的核心配置、快照、结果和产物不可修改。
- 每个写操作均有审计记录，每个失败均有结构化错误。
- 同一输入可以通过指纹识别，但每次提交都有独立实验 ID。
- pytest、Ruff 和 mypy 全部通过。
