---
name: tushare-data-adapter
description: Use when the task is ad-hoc Tushare data research, endpoint discovery, quick CSV/export work, or natural-language finance data lookup. In the AI-Finance repo, prefer existing collectors/config/contracts first; fall back to direct Tushare calls only when the repo has no wrapped path yet.
---

# Tushare Data Adapter

基于 Tushare 官方 `tushare-data` skill 的适配版。

目标不是替代 AI-Finance 成熟报告链，而是把“自然语言查数/拉数/导出/试接口”这类请求变成可执行流程，同时优先复用本仓库已有的：

- `src/collectors/*`
- `src/processors/*`
- `config/config.yaml`
- `source / as_of / latest_date / fallback / disclosure` 合同

## 什么时候用

优先用于这几类请求：

- “看看这只股票/指数/ETF 最近怎么样”
- “帮我拉两年日线 / 导出 CSV”
- “最近哪个板块最强 / 哪些方向吸金”
- “查财报趋势 / 估值 / 资金流 / 北向 / 龙虎榜”
- “这个 Tushare 接口值不值得接”
- “先快速验证一个自然语言研究问题，再决定要不要下沉进主链”

不要把这个 skill 当成成熟成稿主路径。若任务是：

- `briefing / scan / stock_analysis / stock_pick / etf_pick / fund_pick` 的正式稿
- `Tushare 新接口正式接入 mature 主链`

应优先配合：

- [ai-finance-report-final](/Users/bilibili/.codex/skills/ai-finance-report-final/SKILL.md)
- [ai-finance-tushare-rollout](/Users/bilibili/.codex/skills/ai-finance-tushare-rollout/SKILL.md)

## 默认决策顺序

### 1. 先判断是不是仓库内已有能力

如果当前就在 AI-Finance 仓库里，先查：

- 有没有现成 collector
- 有没有现成 command 可以直接跑 spot check
- 有没有现成成熟报告已经消费这条数据

优先级：

1. 复用仓库现有 collector / command
2. 用仓库配置补一个最小 spot check
3. 只有仓库还没接时，才临时直调 Tushare

不要在仓库已经有共享 collector 的情况下，再额外手搓一套 `ts.pro_api()` 查询流程。

### 2. 再判断用户要的是哪类任务

- `lookup / compare / export`
- `单标的快查`
- `板块/资金/市场结构快查`
- `宏观/跨资产快查`
- `新接口探索`

按任务选最小接口集，不要一上来把同类接口全拉一遍。

### 3. 输出时必须带合同

不管是仓库 collector 还是临时 Tushare 查询，结论都要带：

- `source`
- `as_of`
- `latest_date`（如适用）
- `fallback`
- `disclosure`

空表、权限不足、频控、陈旧日期，都不能伪装成 fresh 命中。

## 环境规则

### AI-Finance 仓库内

默认不依赖 `TUSHARE_TOKEN` 环境变量。

优先走仓库配置：

- `config/config.yaml`
- 若用户只要示例口径，可参考 [config.example.yaml](/Users/bilibili/fiance/AI-Finance/config/config.example.yaml)

仓库里 Tushare 初始化在：

- [src/collectors/base.py](/Users/bilibili/fiance/AI-Finance/src/collectors/base.py)

所以在 repo 内：

- 优先复用 `BaseCollector._tushare_pro()` / `_ts_call()`
- 不要单独重新维护 token 初始化逻辑

### 仓库外

若不在 AI-Finance 仓库上下文里，再按官方 `tushare-data` 口径检查：

1. `tushare` 包是否已安装
2. `TUSHARE_TOKEN` 是否可用
3. 需要时做轻量冒烟测试

## 常用意图到接口的最小映射

### 行情 / 趋势

- 股票/ETF/基金：`daily`, `weekly`, `monthly`, `daily_basic`
- 指数：`index_daily`, `index_weekly`, `index_monthly`, `index_dailybasic`

### 财务 / 估值 / 公司质量

- `income`
- `fina_indicator`
- `balancesheet`
- `cashflow`
- `forecast`
- `express`

### 行业 / 板块 / 主题

- 标准行业：`index_classify`, `index_member_all`, `sw_daily`, `ci_index_member`, `ci_daily`
- 板块专题：`tdx_index`, `tdx_member`, `tdx_daily`, `dc_index`, `dc_daily`

### 资金 / 情绪 / 事件

- `moneyflow`, `moneyflow_mkt_dc`
- `top_list`, `top_inst`
- `stk_auction`, `stk_limit`, `limit_list_d`
- `hm_detail`
- `ggt_top10`, `ccass_hold`, `ccass_hold_detail`
- `broker_recommend`, `report_rc`
- `irm_qa_sh`, `irm_qa_sz`, `stk_surv`

### ETF / 基金 / 转债 / 黄金 / 外汇

- ETF：`etf_basic`, `etf_index`, `etf_share_size`, `fund_adj`, `fund_factor_pro`
- 转债：`cb_basic`, `cb_daily`, `cb_factor_pro`, `cb_issue`, `cb_share`
- 黄金：`sge_basic`, `sge_daily`
- 外汇：`fx_obasic`, `fx_daily`

## 在 AI-Finance 里怎么落

### 若只是临时研究/导出

可以：

- 跑最小 Python spot check
- 用现有 command 做一次真实样本验证
- 导出 CSV / markdown 摘要

### 若判断“值得正式接入”

不要停在临时脚本。

下一步应切到：

- [ai-finance-tushare-rollout](/Users/bilibili/.codex/skills/ai-finance-tushare-rollout/SKILL.md)

并完成：

1. collector 合同
2. processor / ranking / evidence sink
3. renderer / disclosure / guard
4. tests
5. docs 更新

## 快查输出风格

优先给：

1. 一句话结论
2. 关键数字/对比
3. 数据来源和时点
4. 限制/降级说明

不要直接甩大表，除非用户明确要原始表。

## 不要做的事

- 不要把 ad-hoc 快查伪装成成熟正式稿
- 不要绕开仓库已有 collector 去长期维护第二套直调逻辑
- 不要把权限不足/空表解释成“没有催化/没有风险/没有资金流”
- 不要把 Tushare 官方通用 skill 和 AI-Finance 成熟报告 skill 混成一条链

## 官方来源

本 skill 参考：

- [Tushare Skills 安装说明](https://tushare.pro/document/1?doc_id=450)
- [官方 tushare-data SKILL.md](https://raw.githubusercontent.com/waditu-tushare/skills/master/tushare-data/SKILL.md)

这里做的适配主要是：

- 在 AI-Finance 仓库内优先复用现有 collectors/contracts
- 明确与 mature report / rollout skill 的边界
- 强化 `source/as_of/fallback/disclosure` 的诚实口径
