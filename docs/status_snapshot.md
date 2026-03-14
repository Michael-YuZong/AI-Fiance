# Status Snapshot

这份文件只回答三件事：

1. 现在哪些功能最成熟
2. 现在最该继续做什么
3. 最近有哪些会影响判断的变化

## 成熟度

### 已成熟

- `scan / stock_analysis`
- `stock_pick`
- `fund_pick`
- `etf_pick`
- `research`
- `risk`
- `portfolio`
- `compare`
- `briefing`
- `lookup / assistant`

### 可用但仍在迭代

- `discover`
- `policy`
- `decision_review / retrospect`
- `scheduler`
- `strategy`

### 仍偏弱或仍未统一

- proxy signals 仍是代理，不是原始全量 feed
- repo-wide point-in-time 合同仍未完全统一
- `scheduler` 的持久化和运维监控还没做完

## 当前主线 backlog

1. 强因子工程
2. `strategy` fixture + governance
3. `policy` v2 深化
4. proxy signals repo-wide 收口
5. `scheduler` v2
6. 更长期的校准和自学习

强因子详细合同见 [docs/plans/strong_factors.md](./plans/strong_factors.md)。

## 最近重要变化

### 2026-03-14

- `strategy` 已经有第一版闭环：
  - `predict`
  - `list`
  - `replay`
  - `validate`
  - `attribute`
  - `experiment`
- `strategy experiment` 当前只做预定义 challenger，对同一批历史样本比较权重方案，不允许直接改生产链路。
- 技术链路已补：
  - `K线形态`
  - `量价 / 动量背离`

### 2026-03-13

- `policy` 已支持更稳的长文页面抽取、PDF/OFD 直链和补抽、事实/推断/待确认分层、第一版 taxonomy。
- `portfolio whatif`、`research`、`retrospect`、pick 报告已通过统一 `horizon` 和 trade handoff 语言打通。
- `fund_pick / etf_pick / stock_pick` 的 pick-history、coverage、rerun diff、release guard 已明显收口。
- `compare` 已从双标截断升级成真正多标比较。
- `briefing` 已恢复并稳定了日版结构。

## 现在不该误判的地方

- [docs/architecture_v2.md](./architecture_v2.md) 是历史文档，不要把里面的“原始设计约束”当成现在的真实合同。
- `strategy` 已可用，但它仍是窄版研究闭环，不是全市场截面策略引擎。
- `policy` 已强很多，但扫描版 / 表格重原文仍是明确降级边界。
