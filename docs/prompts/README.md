# Prompt Map

这份文件只做 prompt 路由，不承载详细规则。

默认不要把 `docs/prompts/` 全读一遍。先看这份，再只打开当前任务对应的那一份。

## 最小读法

1. 先判断你是在做 `editor / reviewer / catalyst review / revision loop` 哪一种
2. 只打开对应 prompt
3. 多轮外审时再加 [external_review_convergence_loop.md](./external_review_convergence_loop.md)
4. 完整“出稿 -> 审稿 -> 修稿 -> final”时再加 [report_revision_loop.md](./report_revision_loop.md)

## Prompt 路由

| 场景 | 先读 | 不要误用在 |
| --- | --- | --- |
| 结构化底稿转 thesis-first 首页 | [financial_editor_writer.md](./financial_editor_writer.md) | reviewer 审稿、补新事实 |
| 热点主题催化漏抓的联网复核 | [financial_catalyst_web_researcher.md](./financial_catalyst_web_researcher.md) | 直接改推荐等级、把背景新闻冒充直接催化 |
| 正式研究型 Markdown `Pass A` 结构审 | [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md) | 发散审、计划文档 |
| 正式研究型 Markdown `Pass B` 发散审 | [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md) | 合同审、问答外审 |
| `research` 问答式输出外审 | [external_research_reviewer.md](./external_research_reviewer.md) | 完整客户成稿 |
| `strategy` 计划或方法论审稿 | [external_strategy_plan_reviewer.md](./external_strategy_plan_reviewer.md) | 业务成稿、代码 diff |
| 强因子路线 / 因子工程计划审稿 | [external_factor_plan_reviewer.md](./external_factor_plan_reviewer.md) | 具体报告 |
| 多轮外审收敛 | [external_review_convergence_loop.md](./external_review_convergence_loop.md) | 单轮 reviewer 本体 |
| 报告修订闭环 | [report_revision_loop.md](./report_revision_loop.md) | reviewer 本体 |

## 常见组合

正式报告外审：

1. [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md)
2. [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md)
3. [external_review_convergence_loop.md](./external_review_convergence_loop.md)

从底稿推到 final：

1. [financial_editor_writer.md](./financial_editor_writer.md)
2. 命中 `suspected_search_gap` 时，加 [financial_catalyst_web_researcher.md](./financial_catalyst_web_researcher.md)
3. [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md)
4. [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md)
5. [external_review_convergence_loop.md](./external_review_convergence_loop.md)
6. [report_revision_loop.md](./report_revision_loop.md)

## 默认不要做的事

- 不要把 `Pass A + Pass B` 混成同一个 reviewer 一次做完
- 不要让同一个 reviewer / 子 agent 同时做结构审和发散审
- 不要把 `external_research_reviewer.md` 拿去审完整客户成稿
- 不要把计划审稿 prompt 拿去审代码实现

## 相关入口

- 默认任务读法：[docs/context_map.md](../context_map.md)
- 外审 kit：[docs/review_kit/README.md](../review_kit/README.md)
- 当前状态：[docs/status_snapshot.md](../status_snapshot.md)
