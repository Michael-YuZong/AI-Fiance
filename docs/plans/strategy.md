# Strategy Plan

这份文件只讲 `strategy`。

默认不要在普通 command 改动里打开它。  
只有当任务直接涉及 `strategy`、因子实验、历史回放、验证、归因或 promotion gate 时再读。

## 当前状态

`strategy` 已经完成第一版闭环：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

但当前仍是窄合同：

- universe：A 股高流动性普通股票
- 主目标：`20d_excess_return_vs_csi800_rank`
- 主 benchmark：`中证800`
- 当前验证：单标的时间序列 replay / validate / experiment
- 当前 experiment：只比较预定义 challenger

## 这层真正要解决什么

不是做“今天拍一个预测”。

真正目标是：

1. 在历史时间点 T 生成结构化预测
2. 用 T 当时可见的信息保留因子和证据快照
3. 在 T+20d 后验证结果
4. 归因：错在权重、因子、周期、regime、执行，还是数据链路
5. 再用 experiment 比较改法

也就是说，它是一个“策略训练场”，不是单次预测器。

## 当前已实现的 6 个命令

### `predict`

记录当前或单时点预测账本。

### `list`

列出已记录样本。

### `replay`

在历史时间点批量生成样本。

### `validate`

对 replay 样本做后验验证，当前重点看：

- `hit rate`
- `avg excess return`
- `avg cost-adjusted directional return`
- `avg max drawdown`
- `confidence bucket`

### `attribute`

把结果分到结构化归因标签。

当前 v1 归因标签：

- `confirmed_edge`
- `execution_cost_drag`
- `weight_misallocation`
- `universe_bias`
- `horizon_mismatch`
- `missing_factor`
- `regime_shift`
- `data_degradation_or_proxy_limit`

### `experiment`

在同一批历史样本上比较预定义 challenger。

当前 challenger：

- `baseline`
- `momentum_tilt`
- `defensive_tilt`
- `confirmation_tilt`

## 当前边界

这套东西已经能训练，但还没到“可推广的生产策略治理层”。

还没有完成的关键点：

- lag / visibility fixture
- overlap fixture
- benchmark fixture
- champion / challenger promotion gate
- rollback gate
- 更严格的 out-of-sample 验证
- 多标的 / cohort / cross-sectional validate

## 当前允许做什么，不允许做什么

### 允许

- 在既定 universe 内做 replay / validate / attribute / experiment
- 扩充归因标签，但要有验证价值
- 增加预定义 challenger，但要先有清晰金融含义
- 把合理外审 finding 沉淀到 tests / fixtures / rules

### 不允许

- 直接把 experiment 赢家推到生产链路
- 跳过 fixture 和治理，直接做自动挖因子
- 在没有锁定目标和 benchmark 的情况下同时扩多目标
- 把单标的时间序列结果包装成全市场截面 alpha 证明

## 当前下一步

1. lag / visibility fixture
2. overlap / benchmark fixture
3. champion-challenger promotion / rollback gate
4. 扩到更严格的 out-of-sample / cohort / cross-sectional validate

## 外审要求

`strategy` 不走普通功能的“改完再说”路径。

这里默认要同时过：

- 合同审
- 发散审
- round-based 收敛

而且合理 finding 不能只留在评论里，必须固化到：

- prompt
- hard rule / guard
- tests / fixtures
- backlog

相关 prompt：

- `docs/prompts/external_strategy_plan_reviewer.md`
- `docs/prompts/external_review_convergence_loop.md`

## 最小命令集

```bash
python -m src.commands.strategy predict 600519 --preview
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
```

## 最小测试集

```bash
pytest tests/test_storage/test_strategy_storage.py tests/test_commands/test_strategy_command.py tests/test_processors/test_strategy_processor.py tests/test_output/test_strategy_report.py -q
```
