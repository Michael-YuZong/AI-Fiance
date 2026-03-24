# Prompt Map

这份文件只做一件事：告诉你 `docs/prompts/` 里每份 prompt 是干什么的，先读哪份，什么时候不要误用。

默认不要把这个目录下所有 prompt 全读一遍。

## 默认读法

1. 先读这份 [README.md](./README.md)
2. 只打开当前任务对应的那一份 prompt
3. 如果是正式研究型 Markdown 成稿，先分清这是 `Pass A 结构审` 还是 `Pass B 发散审`
4. 如果要跑多轮外审，再额外配合 [external_review_convergence_loop.md](./external_review_convergence_loop.md)
5. 如果是“生成报告并反复修订到可交付”，再看 [report_revision_loop.md](./report_revision_loop.md)

## Prompt 路由

| 场景 | 先读哪份 | 典型输入 | 不要误用在 |
| --- | --- | --- | --- |
| 正式研究型 Markdown 报告结构审 | [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md) | 个股推荐、ETF/基金分析、简报、组合复盘、回测解读等完整成稿的 `Pass A` | 零提示发散审、计划文档 |
| 正式研究型 Markdown 报告发散审 | [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md) | 同一类完整成稿的 `Pass B`，专门抓框架外问题和逐段问题 | 结构化合同审、自然语言问答 |
| `research` 这类问答式输出外审 | [external_research_reviewer.md](./external_research_reviewer.md) | “现在还能不能买”“为什么最近市场别扭” 这类回答 | 完整报告、计划文档 |
| `strategy` 计划或方法论审稿 | [external_strategy_plan_reviewer.md](./external_strategy_plan_reviewer.md) | `strategy` 计划、实验设计、验证口径、归因框架 | 业务成稿、代码 diff |
| 强因子路线 / 因子工程计划审稿 | [external_factor_plan_reviewer.md](./external_factor_plan_reviewer.md) | 因子族规划、状态机、接入顺序、strategy 联动方案 | 具体报告、自然语言问答 |
| 任意外审的 round-based 收敛 | [external_review_convergence_loop.md](./external_review_convergence_loop.md) | 上述任一 reviewer prompt 的多轮运行流程 | 单轮 reviewer 本体 |
| 报告生成后的修订闭环 | [report_revision_loop.md](./report_revision_loop.md) | “先出稿，再审，再修，再审直到 PASS” 的作者工作流 | 纯 reviewer 角色本体 |

## 最常见组合

正式报告外审：

1. [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md)
2. 修正后交给**另一个 reviewer / 子 agent**跑 [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md)
3. [external_review_convergence_loop.md](./external_review_convergence_loop.md)

研究问答外审：

1. [external_research_reviewer.md](./external_research_reviewer.md)
2. [external_review_convergence_loop.md](./external_review_convergence_loop.md)

`strategy` 计划审稿：

1. [external_strategy_plan_reviewer.md](./external_strategy_plan_reviewer.md)
2. [external_review_convergence_loop.md](./external_review_convergence_loop.md)

强因子计划审稿：

1. [external_factor_plan_reviewer.md](./external_factor_plan_reviewer.md)
2. [external_review_convergence_loop.md](./external_review_convergence_loop.md)

报告生成到交付的完整闭环：

1. [report_revision_loop.md](./report_revision_loop.md)
2. [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md)
3. [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md)
4. [external_review_convergence_loop.md](./external_review_convergence_loop.md)

## 快速判断

如果你手里是：

- 一份完整 Markdown 成稿，要先做结构审：看 [external_financial_structural_reviewer.md](./external_financial_structural_reviewer.md)
- 一份结构审已经修过的完整 Markdown 成稿，要做第二视角：看 [external_financial_divergent_reviewer.md](./external_financial_divergent_reviewer.md)
- 一段直接回答用户问题的研究回答：先看 [external_research_reviewer.md](./external_research_reviewer.md)
- 一份还没开工实现的 `strategy` 方案：先看 [external_strategy_plan_reviewer.md](./external_strategy_plan_reviewer.md)
- 一份强因子路线图：先看 [external_factor_plan_reviewer.md](./external_factor_plan_reviewer.md)
- 你不是 reviewer，而是主执行者，要把外审跑到收敛：再加 [external_review_convergence_loop.md](./external_review_convergence_loop.md)
- 你不是单纯审稿，而是要把报告从草稿推到 final：再加 [report_revision_loop.md](./report_revision_loop.md)

## 默认不要做的事

- 不要把正式成稿的 `Pass A + Pass B` 混成同一个 reviewer 一次做完
- 不要让同一个 reviewer / 子 agent 既做结构审又做发散审
- 不要把 `external_research_reviewer.md` 拿去审完整客户成稿
- 不要把计划审稿 prompt 拿去审代码实现
- 不要只跑单轮 reviewer 就宣布“外审结束”
- 不要把 `report_revision_loop.md` 当成 reviewer 本体，它是主执行者工作流

## 相关入口

- 默认任务读法：[docs/context_map.md](../context_map.md)
- 外审 kit：[docs/review_kit/README.md](../review_kit/README.md)
- 当前状态：[docs/status_snapshot.md](../status_snapshot.md)
