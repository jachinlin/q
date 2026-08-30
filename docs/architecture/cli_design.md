# CLI 外部契约

文档状态：当前有效设计　·　日期：2026-08-30

安装命令为 `qlab`。stdout 只输出成功 JSON；受控错误和结构化日志写 stderr。
研究配置只接受 `configs/` 可信根中的严格 YAML。

## 数据命令

```console
qlab data bootstrap --years <正整数>
qlab data update
qlab data localize [--dataset <name>] [--start YYYY-MM-DD --end YYYY-MM-DD]
qlab data localize-all --start YYYY-MM-DD --end YYYY-MM-DD
qlab data curate-all
qlab data validate-all
```

`update` 固定执行 `LOCALIZE → CURATE → VALIDATE`，任务 payload 保存提交时冻结的计划。

## 策略研究与因子研究

```console
qlab strategy-studies validate <study-yaml>
qlab strategy-studies submit <study-yaml>
qlab strategy-studies show <strategy-study-id>
qlab strategy-studies list
qlab strategies list

qlab factor-studies validate <study-yaml>
qlab factor-studies submit <study-yaml>
qlab factor-studies show <factor-study-id>
qlab factor-studies list
```

策略研究的 `submit` 原子创建一个 `StrategyStudy` 和一个 `STRATEGY_STUDY` 任务。
每项研究只执行一次；CLI 不提供追加 Run、重跑、标记或比较命令。需要调整或再次执行时，
复制原 YAML 并提交一项独立研究。因子研究保持独立生命周期和统计校正。

## Worker 与 Dashboard

```console
qlab worker once
qlab worker run
qlab dashboard --port 8000
qlab start
```

`worker once` 至多领取一个可见任务；`worker run` 持续轮询。策略研究任务固定执行
`VALIDATE → BACKTEST → ANALYTICS → PUBLISH`，因子研究和数据任务使用各自处理器。
