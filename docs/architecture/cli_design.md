# `quant` CLI 总体设计

## 1. 边界

入口固定为 `quant_research.bootstrap.cli:main`。CLI 只解析输入、调用应用服务并输出稳定
JSON；不得实现数据流水线、研究状态机、数值算法或直接创建基础设施依赖。`--help` 不得
升级数据库、联网或启动 Worker。

成功 stdout 只包含一个排序稳定的 UTF-8 JSON 对象；日志和结构化错误写 stderr，未知
异常在进程边界转换且不泄露 traceback。日期只接受 `YYYY-MM-DD`，配置路径必须落在
可信配置根内。

## 2. 命令树

```text
quant
├── dashboard
├── start
├── data
│   ├── bootstrap
│   ├── update
│   ├── localize / localize-all
│   ├── curate / curate-all
│   └── validate / validate-all
├── research
│   ├── validate <yaml>
│   ├── submit <yaml>
│   ├── list
│   ├── show <family_id>
│   └── rerun <family_id>
├── components
│   └── list
├── tasks
│   ├── list
│   ├── cancel <task_id>
│   └── retry <task_id>
└── worker
    ├── once
    └── run
```

不存在 `experiments`、`factor-studies` 或其别名。

## 3. 研究命令

`research validate` 解析严格 YAML，返回规范化配置、组件能力结果、候选总数和按字段路径
稳定展开的前若干候选，不创建持久化对象。候选超过 256、字段路径不存在、类型错误、
样本区间重叠或能力不兼容时在提交前失败。

`research submit` 先执行同一校验，再原子创建不可变研究族、execution 和
`RESEARCH_EXPAND` 任务。返回 `family_id`、`execution_id`、`task_id` 和候选数；命令不
同步运行研究。

`research list/show` 查询研究族、执行、候选、分区指标、选型证据、TEST、标签和产物；
`research rerun` 捕获当前数据/源码/锁文件/规则身份并创建新 execution，不覆盖旧运行。

`components list` 返回组件描述、源码哈希、能力、必需数据集、支持信号类型、确定性声明
和 JSON Schema，以及三个策略模板。

## 4. 任务与 Worker

任务通过 `subject_kind/subject_id` 关联研究 execution、run 或数据操作。研究链固定为：

```text
RESEARCH_EXPAND
→ RESEARCH_RUN(TRAIN_VALIDATION) × N
→ RESEARCH_SELECT
→ RESEARCH_RUN(TEST) × 1
→ RESEARCH_REGISTER
```

取消在阶段或批次边界生效。任务重试保留旧 attempt；研究终态重试通过新 execution
完成。单 Worker 可串行执行，多 Worker 依赖 SQLite 短事务、租约和幂等键避免重复推进。

## 5. 资源与验收

每次命令实际调用只装配一次服务，并在成功或失败后幂等关闭。CLI 测试必须覆盖命令树、
帮助无副作用、严格 YAML、stdout/stderr、退出码、候选预览、ID 查询、取消/重试及资源
关闭；删除的旧命令必须返回命令不存在。
