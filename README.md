# AI-Finance

本地优先的 CLI 投研工作台。

它不是 Web 产品，不是自动交易系统，也不是“给个代码就替你买卖”的黑盒。默认目标是把研究、推荐、风险、策略学习、外审和交付做成一套可运行、可测试、可持续迭代的本地链路。

## 现在能做什么

- 单标的分析：`scan`、`stock_analysis`
- 推荐产出：`stock_pick`、`etf_pick`、`fund_pick`
- 市场简报：`briefing daily / weekly / noon / evening / market`
- 研究问答：`research`、`assistant`、`lookup`
- 组合与风险：`portfolio`、`risk`
- 策略学习：`strategy predict / list / replay / validate / attribute / experiment`
- 正式交付：`client_export`、`release_check`、`report_guard`
- 外审治理：`review_ledger`、`review_audit`

## 默认怎么读这个仓库

不要一开工就扫完整个仓库。

建议顺序：

1. [README.md](./README.md)
2. [AGENTS.md](./AGENTS.md)
3. [docs/context_map.md](./docs/context_map.md)
4. 只打开你要改的 command / processor / renderer / test

只有任务相关时再展开：

- 配置：看 [config/README.md](./config/README.md)
- `strategy`：看 [docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子维护：看 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 当前成熟度和 backlog：看 [docs/status_snapshot.md](./docs/status_snapshot.md)
- 更细历史变化：看 [docs/history/2026-03.md](./docs/history/2026-03.md)

## 当前成熟度

成熟区：

- `scan`
- `stock_analysis`
- `stock_pick`
- `fund_pick`
- `etf_pick`
- `research`
- `risk`
- `portfolio`
- `compare`
- `briefing`
- `lookup`
- `assistant`

可用但仍在迭代：

- `discover`
- `policy`
- `strategy`
- `decision_review / retrospect`
- `scheduler`

弱或占位：

- `collectors/policy.py`
- `collectors/social_sentiment.py`
- `collectors/global_flow.py`
- scheduler 的持久化和运维可见性层

## 当前主线

1. `strategy` fixtures and governance
   现在已经有 `predict / list / replay / validate / attribute / experiment`；`benchmark fixture` v1 已落地，下一步是 lag / visibility、overlap、promotion / rollback gate。
2. `policy` v2
   继续提升扫描版、表格重 PDF/OFD 的抽取和 taxonomy。
3. proxy signals repo-wide 收口
   继续把代理置信度、覆盖、限制和降级影响统一到更多 final / manifest / audit。
4. `scheduler` v2
   做持久化 run history、失败可见性和运维状态。
5. 校准与学习
   深化 setup bucket、归因和长期月度学习闭环。

## 最近更新

- 共享 `client-final` 运行编排已落地：`scan / stock_analysis / stock_pick / etf_pick / fund_pick / briefing` 现在统一走 `final_runner`，复用 `detail 写盘 -> release_check -> report_guard -> markdown/html/pdf/manifest 导出`。
- 外审流转已收成双 pass：正式稿默认要求 `结构审 + 发散审 + round-based 收敛`，并显式记录不同执行者；缺 review 时会先自动补 scaffold。
- `stock_pick / etf_pick` 的候选池不再是单纯按成交额截断，而是先硬过滤、再按行业保广度，减少热门方向把池子挤满。
- 观察稿合同已统一：`scan / stock_analysis / stock_pick / etf_pick / fund_pick / briefing` 都开始显式写 `为什么还不升级 / 升级条件 / 正式动作阈值`。
- `briefing`、`pick`、单标的稿都开始更系统地披露 `proxy_contract`、证据时点、来源和降级边界，避免把代理信号写成原始全量数据。
- `stock_analysis` 的 A 股单标的发散审新增了更严格的四层拆分：`宏观背景 / 主题逻辑 / 个股直接催化 / 正式动作触发` 必须分开，不能再把宏观晨报或行业早报冒充成个股直接催化。

## 输出合同

`internal / preview`：

- 给内部观察、调试和当天快照
- 不等于正式对外交付

`final`：

- 默认包括 `markdown + html + pdf`
- 需要同时存在 `external_review.md` 和 `release_manifest.json`
- `release_check` 或 `report_guard` 没过时，不应落 final

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

python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6

python -m src.commands.review_ledger
python -m src.commands.review_audit
```

## 文档地图

- 默认读法：[docs/context_map.md](./docs/context_map.md)
- 当前状态与 backlog：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 路线图总览：[plan.md](./plan.md)
- YAML 地图：[config/README.md](./config/README.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子专题：[docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 详细变更归档：[docs/history/2026-03.md](./docs/history/2026-03.md)
