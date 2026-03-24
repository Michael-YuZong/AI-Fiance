# Strategy Plan

这份文件只讲 `strategy`。

默认不要在普通 command 改动里打开它。只有当任务直接涉及：

- `strategy` 命令
- replay / validate / attribute / experiment
- fixture / governance
- promotion / rollback gate

时再读。

## 当前合同

| 项目 | 当前口径 |
| --- | --- |
| universe | A 股高流动性普通股票 |
| 主目标 | `20d_excess_return_vs_csi800_rank` |
| 主 benchmark | `中证800` |
| 当前验证 | 账本 validate 为主，已支持 multi-symbol replay / experiment 与 cross-sectional validate |
| 当前 experiment | 只比较预定义 challenger |
| 正式交付 | `validate / experiment` 已支持 `--client-final`，走 `report_guard / release_check / client_export` |

已完成的命令：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

## 这条线真正要解决什么

它不是“今天拍一个预测”，而是一个策略训练场：

1. 在历史时间点 `T` 生成结构化预测
2. 保留 `T` 当时可见的因子和证据快照
3. 在 `T+20d` 后验证结果
4. 把结果归因到权重、因子、周期、regime、执行或数据链路
5. 再用 experiment 比较改法

## 命令地图

| 命令 | 作用 |
| --- | --- |
| `predict` | 记录当前或单时点预测账本 |
| `list` | 列出已记录样本 |
| `replay` | 在历史时间点批量生成样本 |
| `validate` | 对 replay 样本做后验验证 |
| `attribute` | 给样本打结构化归因标签 |
| `experiment` | 在同一批样本上比较预定义 challenger |

当前 v1 归因标签：

- `confirmed_edge`
- `execution_cost_drag`
- `weight_misallocation`
- `universe_bias`
- `horizon_mismatch`
- `missing_factor`
- `regime_shift`
- `data_degradation_or_proxy_limit`

当前 challenger：

- `baseline`
- `momentum_tilt`
- `defensive_tilt`
- `confirmation_tilt`

## fixture / governance 状态

已完成：

- `benchmark fixture` v1
  `predict / replay` 会记录 benchmark 窗口、overlap、`as_of` 对齐和未来验证窗 readiness；
  `validate / experiment` 会汇总这层 fixture；
  overlap 不足或 `as_of` 未对齐时，主预测会退回 `no_prediction`。
- `lag / visibility fixture` v1
  `predict` 会把因子层的 `lag_fixture_ready / visibility_fixture_ready / point_in_time_ready` 汇总成结构化 fixture；
  replay / validate / experiment 会显式披露这层 fixture 的就绪状态或 `price-only replay` 的不适用边界；
  当没有任何可用的 point-in-time strategy candidate 因子时，主预测会退回 `no_prediction`。
- `overlap fixture` v1
  `predict / replay / validate / experiment` 会显式披露样本窗口、required gap 和 primary window overlap；
  `replay / validate / experiment` 在样本重叠时会把这层边界写进 summary / notes，不再只停留在 `overlap_policy` 字符串。
- `promotion / rollback gate` v1
  `experiment` 现在会产出结构化 `promotion_gate`，明确区分 `blocked / stay_on_baseline / queue_for_next_stage`，并显式披露 challenger 相对 baseline 的 validated rows、primary score / hit rate / excess / net / drawdown 增量；
  `validate` 和 `experiment` 也都会产出结构化 `rollback_gate`，区分 `blocked / hold / watchlist / rollback_candidate`，不再只给统计表而没有治理裁决。
- `out-of-sample / chronological cohort validate` v1
  `validate` 现在会显式切出 `development / holdout` 的 out-of-sample validate，并给出 `blocked / stable / watchlist`；
  同时也会固定拆 `earliest / middle / latest` cohort，比对 latest vs earliest 的退化；
  `experiment` 的 `promotion_gate` 已开始正式承认 variant 的 out-of-sample 状态，不再只看 aggregate 平均值。
- `cross-sectional validate` v1
  `validate` 现在会在同日多标的 cohort 足够时，显式计算 seed score 与 realized excess return 的横截面 rank correlation，以及高分组相对低分组的 spread；
  如果账本里还没有足够的同日多标的 cohort，会明确标记为 `blocked`，不再把单标的结果包装成横截面 rank 证明。
- `multi-symbol replay / experiment` v1
  `replay` 现在可以一次生成多标的样本供给，并显式展示 `Symbol Coverage` 和 `Same-Day Cohorts`；
  `experiment` 也已经扩到多标的 cohort，`promotion_gate` 会同时承认 `out-of-sample` 和 `cross-sectional` 状态，不再只看单标的 aggregate 平均值。
- `config-driven batch symbol source / cohort recipe` v1
  `replay / experiment` 现在支持直接从 `config/strategy_batches.yaml` 读取 batch source 和 cohort recipe；
  命令层已承认 `--batch-source / --cohort-recipe`，也支持不手输 symbols 直接跑多标的批次；
  summary 会显式展示 `Batch Source / Cohort Recipe`，而 replay row 的 `asset_reentry_gap_days` 也会真实回写。
- `client-final export` v1
  `validate / experiment` 现在支持正式 `--client-final`；
  会把当前 strategy 成稿送进 `report_guard / release_check / client_export`，产出 `markdown + html + pdf + release_manifest`；
  正式成稿当前只承认 `validate / experiment` 两类，不把 `predict / replay / attribute` 伪装成对外交付稿；
  如果缺少 `__external_review.md`，命令层会先自动在 `reports/reviews/strategy/...` 下生成首轮 BLOCKED scaffold，带好 `Pass A / Pass B` prompt 路径和收敛字段，再要求补齐独立外审。
- `strategy final readability` v1
  `validate / experiment` 成稿现在会固定先给 `这套策略是什么 / 这次到底看出来什么 / 执行摘要` 三段；
  先解释它是不是具体策略、这份报告到底在回答什么、现在能不能用/能不能切换，再往下展开 gate、fixture 和样本细节；
  `report_guard / release_check` 也已把这三段收成正式合同，避免报告重新退回只剩治理词和表格。

还没完成：

- 更长窗口和更多日期上的 promotion calibration / external review

## 允许与不允许

允许：

- 在既定 universe 内做 replay / validate / attribute / experiment
- 扩充归因标签，但要有验证价值
- 增加预定义 challenger，但要先有清晰金融含义
- 把合理外审 finding 沉淀到 tests / fixtures / rules

不允许：

- 直接把 experiment 赢家推到生产链路
- 跳过 fixture 和治理，直接做自动挖因子
- 在没锁定目标和 benchmark 的情况下同时扩多目标
- 把单标的时间序列结果包装成全市场截面 alpha 证明

## 当前下一步

1. 扩到更长窗口和更多日期上的 promotion calibration / external review

## 外审口径

`strategy` 默认要同时过：

- 合同审
- 发散审
- round-based 收敛

相关 prompt：

- [docs/prompts/external_strategy_plan_reviewer.md](../prompts/external_strategy_plan_reviewer.md)
- [docs/prompts/external_review_convergence_loop.md](../prompts/external_review_convergence_loop.md)
- 先看入口时可先读 [docs/prompts/README.md](../prompts/README.md)

## 最小相关文件

- `src/commands/strategy.py`
- `src/processors/strategy.py`
- `src/output/strategy_report.py`
- `src/storage/strategy.py`
- `tests/test_*strategy*`

## 最小命令集

```bash
python -m src.commands.strategy predict 600519 --preview
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --client-final
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --client-final
```

## 最小测试集

```bash
pytest tests/test_storage/test_strategy_storage.py tests/test_commands/test_strategy_command.py tests/test_processors/test_strategy_processor.py tests/test_output/test_strategy_report.py -q
```
