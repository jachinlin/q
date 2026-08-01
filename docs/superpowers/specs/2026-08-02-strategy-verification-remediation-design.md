# 策略验证缺口修复设计

## 1. 背景与目标

回测计划的任务 6 已完成 ETF 轮动、多因子策略及基础测试，但五轮修复复审后仍有六组承重验证缺口。缺口集中在 `PortfolioState` 不变量、PIT/股票池证据、ETF 异常信号、多因子数值链路、运行时组合约束和 YAML 配置装配。

本修复周期只补足这些已确认的验证缺口；若精确测试证明生产实现有误，才做最小生产代码修复。完成标准不是测试数量增加，而是独立复审者将原 Task 6 的 B–G 六组残项全部判定为关闭，且没有新增 Critical 或 Important。

## 2. 方案比较与选择

### 方案 A：继续单一大任务

把 B–G 全部交给一个实现任务。上下文集中，但任务跨度覆盖模型、数据契约、两类策略、组合构造和配置解析，已在前五轮证明容易出现宽泛断言和完成状态误判，因此不采用。

### 方案 B：按失效域拆成三个任务

按“数据边界”“数值语义”“运行时约束与配置装配”拆分。每个任务拥有明确输入、测试文件和独立复审门禁；一个任务失败不会模糊其他任务的完成状态。采用此方案。

### 方案 C：按源文件拆分

分别围绕 `base.py`、`etf_rotation.py`、`multifactor.py` 和 portfolio 文件组织任务。文件所有权直观，但 PIT、配置和约束行为跨文件，容易把端到端不变量切断，因此不采用。

## 3. 总体设计

修复周期包含三个顺序执行、分别复审的任务：

1. **数据与状态边界**：关闭原 B、C、D，包括 `PortfolioState` 的直接构造不变量、因子/股票池 schema 与时点契约、ETF 缺失或非法信号的 fail-closed 行为。
2. **多因子数值语义**：关闭原 E，以硬编码字面值证明 MAD 去极值、中性化、标准化、方向调整、类别内平均、类别加权和稳定并列排序的顺序及结果。
3. **运行时约束与配置装配**：关闭原 F、G，证明当前持仓和六项约束在策略运行时真实生效，并完整覆盖 ETF/多因子 `from_mapping()` 的非法配置矩阵。

每个任务都采用测试驱动方式：先提交能够在当前实现上暴露未验证分支的精确测试；测试失败时定位原因，只在确认生产缺陷后修改实现；最后运行任务测试、完整策略测试、关联回归和静态检查，再进入独立复审。

## 4. 任务一：数据与状态边界

### 4.1 范围

- `PortfolioState`：非整数金额、负值、持仓元素类型、重复证券、非 canonical 顺序、总市值求和、现金与仓位权重和、单仓权重与 `market_value / nav` 一致性。
- 因子矩阵：缺列、额外列、错误 dtype、空 `available_at`、重复请求 factor ref。
- 股票池矩阵：证券乱序、重复、非 canonical，原因码含空项，ADV 为空，行业为空字符串。
- 策略入口：多因子 invalid 行不得进入 target；ETF 对必需因子缺行、invalid finite、valid NaN/inf 和未来 `available_at` 必须 fail closed。
- ETF YAML：非 canonical ETF identifier 必须由 `from_mapping()` 拒绝。

### 4.2 测试约束

每个非法构造用例从合法基线只改变一个字段，并断言目标分支的异常或输出。不得用“更早失败的另一个不变量”作为覆盖证据。策略级用例必须同时断言 target、现金结果或审计原因，不能只断言底层 validator 抛错。

### 4.3 产出

- 精确的状态、PIT、股票池和 ETF 信号边界测试。
- 仅在测试揭示真实错误时产生最小实现修复。
- 独立复审确认原 B、C、D 全部关闭。

## 5. 任务二：多因子数值语义

### 5.1 范围

- 使用固定小型横截面和手算字面期望值，依次验证 MAD、行业/规模中性化、z-score、方向、类别内平均和四类权重合成。
- 分别证明三个风险因子的方向是 `-1`，而不是只检查最终排序。
- 改变 `industry_code`、`avg_amount_20d`、`log_market_cap` 时，只允许它们影响过滤、中性化或约束，不得作为 Alpha 直接加分。
- transform 产生的 `invalid_reason` 必须进入不可变审计结果。
- 最终 score 相同时必须按 canonical `instrument_id` 稳定排序。

### 5.2 数值 oracle 原则

测试中的最终期望值必须是硬编码字面值，允许使用明确的绝对误差比较；不得在测试内复制生产转换算法重新计算期望值。需要验证中间阶段时，通过公开的决策/审计结果或最小可观察接口断言，不为测试引入第二套生产算法。

### 5.3 产出

- 覆盖完整数值链路、风险方向、辅助字段隔离、审计和并列排序的测试。
- 必要的最小可观察性或实现修复。
- 独立复审确认原 E 全部关闭。

## 6. 任务三：运行时约束与配置装配

### 6.1 范围

- 当前持仓的 `current_weight` 必须进入 constructor candidates。
- 已不 eligible 的当前持仓仍参与换手计算。
- `max_position_weight`、`max_industry_weight`、`min_positions`、`max_positions`、`min_adv_amount`、`max_turnover` 分别设置成决定输出或失败的 binding 条件。
- 多因子 target 必须保留 T 日 `signal_date` 和 T+1 `execute_date`。
- 多因子 `from_mapping()` 必须拒绝未知 Alpha、辅助字段混入 Alpha、合法类别但错误方向、类别权重和错误、负数/NaN/inf 权重、非正持仓数、非法 MAD multiplier、constraints 未知/缺失/非法字段、非字符串 factor ref/category。
- ETF `from_mapping()` 必须拒绝非 canonical ETF identifier。

### 6.2 测试约束

六项组合约束必须在策略到 constructor 的实际调用路径上成为 binding；仅测试 constraint/config dataclass 构造器不算完成。配置测试必须调用 `from_mapping()` 或加载示例 YAML 的同一装配路径；直接构造 dataclass 不得替代 YAML parser 覆盖。

### 6.3 产出

- current holding、六项运行时约束和 T/T+1 的集成测试。
- ETF/多因子配置装配的完整非法矩阵。
- 独立复审确认原 F、G 全部关闭。

## 7. 错误处理与审计原则

- 数据 schema、dtype、时点、证券身份或证据不合法时一律 fail closed，不生成可能含未来信息的目标。
- 单证券因子缺失或 invalid 时按策略规则剔除，并保留稳定、可断言的原因码。
- 配置错误在实验装配阶段同步失败，不允许忽略未知字段或静默使用默认值。
- 运行时约束失败必须沿用 portfolio 层既有异常或原因码，不在策略层创造语义重复的新错误体系。

## 8. 验证与门禁

每个任务至少执行：

1. 目标测试文件的单测。
2. `uv run pytest tests/unit/strategies -q`。
3. 与修改接口相关的 portfolio/backtest 回归测试。
4. `uv run ruff format --check`、`uv run ruff check`、`uv run mypy src` 和 `git diff --check`。
5. 独立任务复审，逐项给出 CLOSED/OPEN 和文件行号证据。

三个任务全部复审通过后，重新运行完整策略测试并对原 Task 6 的 B–G 做一次汇总复审。汇总复审干净后，才解除 Task 6 的 BLOCKED 状态并进入 Task 7。真实 20 年验收仍受 `QUANT_ACCEPTANCE_SNAPSHOT_ID` 外部条件约束，不属于本修复周期。

## 9. 非目标

- 不处理已延期的 ETF 浮点溢出检查和等权尾差 Minor。
- 不实现 Task 7 分析、实验室或 Dashboard。
- 不重构策略公共接口、回测引擎或 portfolio 架构。
- 不清理工作区中既有 `.p*`、`.pytest-*` 和 `.tmp` 未跟踪目录。
