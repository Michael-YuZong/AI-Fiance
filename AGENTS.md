# AI-Finance Agent Handoff

## Default Read Order

不要一开工就扫完整个仓库。

默认只按下面顺序读：

1. 这份文件
2. [README.md](./README.md)
3. [docs/context_map.md](./docs/context_map.md)
4. 你要修改的 command / processor / renderer / test

只有在任务相关时再继续读：

- 配置问题：看 [config/README.md](./config/README.md)
- 强因子工程：看 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- `strategy`：看 [docs/plans/strategy.md](./docs/plans/strategy.md)
- 更完整的当前状态：看 [docs/status_snapshot.md](./docs/status_snapshot.md)
- 外审规则或 prompt：看 `docs/prompts/`

## Repo Identity

- 本仓库是本地 CLI 投研工作台，不是 web app，不是自动交易系统。
- 默认目标：提高输出质量、合同稳定性、工作流实用性。
- 除非用户明确要求，不要平白创造新表面层；优先把现有链路做稳。

## Working Rules

1. 先复现实例
   用真实命令或失败测试，不要空想。
2. 先修产品合同
   先修 command / processor / renderer / guard，不要只修文案。
3. 补测试
   至少加一条能防回归的测试。
4. 保留降级与来源说明
   缺数据时不要装成高确定性结论。
5. 更新文档
   如果合同、成熟度或 backlog 变了，要同步这份文件和相关专题文档。

## Progressive Disclosure Rules

- 不要默认打开所有 `.md` / `.yaml`。
- 不要把 `reports/`、`tmp/`、历史生成稿当成开工前默认上下文。
- [docs/architecture_v2.md](./docs/architecture_v2.md) 是历史参考，不是当前主合同。
- 大多数任务只需要：
  - 一个短入口文档
  - 一个专题文档
  - 相关代码和测试

## Maturity Snapshot

成熟区：

- `src/commands/scan.py`
- `src/commands/stock_analysis.py`
- `src/commands/stock_pick.py`
- `src/commands/fund_pick.py`
- `src/commands/etf_pick.py`
- `src/commands/research.py`
- `src/commands/risk.py`
- `src/commands/portfolio.py`
- `src/commands/compare.py`
- `src/commands/briefing.py`
- `src/commands/lookup.py`
- `src/commands/assistant.py`

可用但仍在迭代：

- `src/commands/discover.py`
- `src/commands/policy.py`
- `src/processors/decision_review.py`
- `src/scheduler.py`
- `src/commands/strategy.py`

弱或占位：

- `src/collectors/policy.py`
- `src/collectors/social_sentiment.py`
- `src/collectors/global_flow.py`
- scheduler 的持久化和运维监控层

更细版本见 [docs/status_snapshot.md](./docs/status_snapshot.md)。

## Current Priority Backlog

1. 强因子工程
   先按因子家族收口：价量、季节/日历、breadth/chips、质量/盈利修正，再到 ETF / 基金专属因子。
2. `strategy` fixtures and governance
   现在已经有 `predict / list / replay / validate / attribute / experiment`，下一步是 lag / visibility / overlap / benchmark fixture，以及 champion-challenger promotion / rollback gate。
3. `policy` v2
   继续提升扫描版、表格重 PDF/OFD 的抽取和 taxonomy。
4. Proxy signals
   把代理置信度和降级影响完整传到 pick 输出和 release / review guard。
5. `scheduler` v2
   做持久化 run history、失败可见性和运维状态。
6. 校准与学习
   深化 setup bucket、归因、长期月度学习闭环。

## Strategy Status

`strategy` 现在已经有第一版闭环：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

但它仍是窄合同：

- 只做 A 股高流动性普通股票
- 主目标固定为 `20d_excess_return_vs_csi800_rank`
- 仍是单标的时间序列 replay / validate / experiment
- experiment 只比较预定义 challenger，不允许直接反哺生产链路

详细合同见 [docs/plans/strategy.md](./docs/plans/strategy.md)。

## External Review Rules

- 外审永远不是一次性 checklist。
- 所有外审都必须：
  - 合同审
  - 发散审
  - round-based 收敛
- 只有满足收敛条件才允许停：
  - 连续两轮无新增 P0/P1
  - 上一轮阻塞项已关闭或降级
  - 没有新的实质性发散问题
  - 合理 finding 已经沉淀到 prompt / 规则 / tests / backlog 至少一层

常用 prompt：

- 研究问答：`docs/prompts/external_research_reviewer.md`
- 正式报告：`docs/prompts/external_financial_reviewer.md`
- 通用收敛循环：`docs/prompts/external_review_convergence_loop.md`
- `strategy` 计划专项：`docs/prompts/external_strategy_plan_reviewer.md`
- 强因子计划专项：`docs/prompts/external_factor_plan_reviewer.md`

## What To Run

常用命令：

```bash
python -m src.commands.scan 561380
python -m src.commands.research 561380 现在还能不能买
python -m src.commands.portfolio whatif buy 561380 2.1 20000
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
```

默认测试顺序：

1. 先跑相关 narrow tests
2. 再跑 `pytest -q`

如果只是改 `strategy`，先跑：

```bash
pytest tests/test_storage/test_strategy_storage.py tests/test_commands/test_strategy_command.py tests/test_processors/test_strategy_processor.py tests/test_output/test_strategy_report.py -q
```

## Guardrails

- 不要为了生成输出而削弱 `release_check.py` 或 `report_guard.py`
- 如果功能是 proxy-based 或降级路径，必须在输出里写清
- 如果 CLI 声称支持某种输入，renderer 和 guard 也必须承认这个合同
- 静默行为错配比明显 TODO 更糟，优先修

## Where Detail Lives

- 任务入口地图：[docs/context_map.md](./docs/context_map.md)
- YAML 地图：[config/README.md](./config/README.md)
- 当前状态细节：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 路线图总览：[plan.md](./plan.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
