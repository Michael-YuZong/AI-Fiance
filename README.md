# AI-Finance

本地优先的 CLI 投研工作台。

它不是 Web 产品，不是自动交易系统，也不是黑盒下单器。默认目标是把研究、推荐、组合、外审和交付做成一套可运行、可测试、可持续迭代的本地链路。

## 开工入口

不要一开工就扫完整仓库。默认顺序：

1. [AGENTS.md](./AGENTS.md)
2. [docs/context_map.md](./docs/context_map.md)
3. 你要改的 command / processor / renderer / test

只有任务相关时再展开：

- 当前状态与 backlog：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 路线图：[plan.md](./plan.md)
- YAML 入口：[config/README.md](./config/README.md)
- 高频 repo workflow skills：`[.codex/skills/ai-finance-report-final/SKILL.md](./.codex/skills/ai-finance-report-final/SKILL.md)`、`[.codex/skills/ai-finance-tushare-rollout/SKILL.md](./.codex/skills/ai-finance-tushare-rollout/SKILL.md)`
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子专题：[docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 详细历史：[docs/history/2026-04.md](./docs/history/2026-04.md)、[docs/history/2026-03.md](./docs/history/2026-03.md)

## 现在能做什么

- 单标的分析：`scan`、`stock_analysis`
- 推荐产出：`stock_pick`、`etf_pick`、`fund_pick`
- 市场简报：`briefing daily / weekly / noon / evening / market`
- 研究问答：`research`、`assistant`、`lookup`
- 自由情报采集：`intel`
- 组合与风险：`portfolio`、`risk`
- 策略学习：`strategy predict / list / replay / validate / attribute / experiment`
- 正式交付：`client-final`、`release_check`、`report_guard`
- 外审治理：`review_ledger`、`review_audit`

## 默认工作流

研究型成稿默认走：

1. 结构化底稿
2. `editor_payload.json + editor_prompt.md`
3. 独立 `editor subagent`
4. `Pass A` 结构审
5. `Pass B` 发散审
6. `final`

如果只是 patch-level 修复，默认停在：

1. 真实复现
2. 窄修复
3. narrow tests
4. 真实 spot check

## 输出合同

- `internal / preview`：内部观察、调试和当天快照，不等于正式交付
- `final / client-final`：默认包括 `markdown + html + pdf + release_manifest`
- 正式稿必须承认 `external_review`、`release_check` 和 `report_guard`

## 最常用命令

```bash
python -m src.commands.scan 300308
python -m src.commands.stock_analysis 300308
python -m src.commands.research 300308 现在还能不能买
python -m src.commands.portfolio whatif buy 300308 580 20000

python -m src.commands.stock_pick
python -m src.commands.etf_pick
python -m src.commands.fund_pick
python -m src.commands.briefing market
python -m src.commands.intel 收集有色金属相关情报

python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
```

## 默认别在这里找什么

- 逐日变更 log：去 [docs/history/2026-04.md](./docs/history/2026-04.md)
- 当前成熟度与 backlog：去 [docs/status_snapshot.md](./docs/status_snapshot.md)
- 更完整路线图：去 [plan.md](./plan.md)
- 大量 prompt 路由：去 [docs/prompts/README.md](./docs/prompts/README.md)
