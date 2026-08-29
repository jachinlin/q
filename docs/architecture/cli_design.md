# CLI 外部契约

文档状态：当前有效设计　·　日期：2026-08-22

安装命令为 `quant`。stdout 只输出成功 JSON；受控错误和结构化日志写 stderr。实验配置只接受
明确路径下的严格 YAML，不接受旧 research、components 或 factor-studies 命令。

## 数据命令

```console
quant data bootstrap --years <正整数>
quant data update
quant data localize [--dataset <name>] [--start YYYY-MM-DD --end YYYY-MM-DD]
quant data localize-all --start YYYY-MM-DD --end YYYY-MM-DD
quant data curate-all
quant data validate-all
```

- `bootstrap` 仅在没有 Canonical 数据时使用，必须指定年数。
- 底层 `localize/localize_all` 不支持隐式日期或 `full`；CLI 可先生成更新计划并把明确日期传入底层。
- `update` 固定执行 `LOCALIZE → CURATE → VALIDATE`，任务 payload 保存提交时固化计划。

## 实验与策略命令

```console
quant experiments validate <experiment-yaml>
quant experiments submit <experiment-yaml>
quant experiments run <experiment_id> <run-yaml>
quant experiments rerun <run_id>
quant experiments list
quant experiments show <experiment_id>
quant strategies list
```

- `submit` 原子创建 Experiment、首个 Run 和 `EXPERIMENT_RUN` 任务。
- `run` 显式追加一个参数 Run；不自动展开搜索空间，也不自动选择 TEST。
- `rerun` 复制历史冻结配置并创建新 Run、新任务和新产物目录，绝不覆盖。
- `strategies list` 返回三个策略 ID 以及截面五模块目录。

## Worker 与 Dashboard

```console
quant worker once
quant worker run
quant dashboard --port 8000
quant start
```

`worker once` 至多领取一个可见任务；`worker run` 持续轮询。策略回测与因子研究都使用唯一
`EXPERIMENT_RUN` handler，数据更新和全量校验保留各自任务类型。
