"""Tests for release consistency checks."""

from __future__ import annotations

from src.commands.release_check import check_generic_client_report, check_stock_pick_client_report


def test_release_check_flags_internal_process_and_score_drift() -> None:
    client = """# 今日个股推荐

模型版本: `x`

## A股

| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| 中际旭创 | 50 | 70 | 50 | 47 | 30 | 正式推荐 |
"""
    source = """### 6. [A] 中际旭创 (300308)  — 无信号

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 36/100 | RSI 47.0 · 前低 · 量能比 0.94 |
| 基本面 | 70/100 | 中际旭创 PE 56.3x |
| 催化面 | 50/100 | 个股相关头条 4 条 |
| 相对强弱 | 47/100 | 主线相关 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 30/100 | 年化波动 59.4% |
"""
    findings = check_stock_pick_client_report(client, source)
    assert any("内部过程词" in item for item in findings)
    assert any("中际旭创 技术" in item for item in findings)


def test_release_check_passes_clean_stock_pick_client_report() -> None:
    client = """# 今日个股推荐

**数据完整度：** 本轮新闻/事件覆盖基本正常。
- A股 结构化事件覆盖 50% / 高置信公司新闻覆盖 100%

## A股

为什么今天优先看新易盛：因为主线、相对强弱和交易结构更匹配。
为什么暂不推荐中际旭创：因为技术确认不足。
为什么宁德时代暂不推：因为事件窗口刚打开。

| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| 新易盛 | 44 | 70 | 40 | 87 | 20 | 正式推荐 |
| 中际旭创 | 36 | 70 | 50 | 47 | 30 | 看好但暂不推荐 |

### 新易盛 (300502)

为什么能进正式推荐：

- 基本面 70：新易盛 PE 52.5x。
- 相对强弱 87：相对基准更强。
- 技术面 44：MACD 零轴下方金叉。

当前最需要防的点：

- 风险特征 20：高波动。

**催化证据来源：**

- `结构化事件`：[新易盛：公司将出席2026年OFC大会](https://example.com/xys)（证券时报 / 2026-03-11）

### 中际旭创 (300308)

为什么仍然值得继续看：

- 基本面 70：中际旭创 PE 56.3x。
- 催化面 50：个股相关头条 4 条。

为什么今天不放进正式推荐：

- 技术面 36：RSI 47.0 · 前低 · 量能比 0.94。
- 风险特征 30：年化波动 59.4%。
"""
    source = """### 1. [A] 新易盛 (300502)  ⭐⭐⭐ 较强机会

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 44/100 | MACD 零轴下方金叉 |
| 基本面 | 70/100 | 新易盛 PE 52.5x |
| 催化面 | 40/100 | 个股相关头条 5 条 |
| 相对强弱 | 87/100 | 相对基准更强 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 20/100 | 高波动 |

---
### 6. [A] 中际旭创 (300308)  — 无信号

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 36/100 | RSI 47.0 · 前低 · 量能比 0.94 |
| 基本面 | 70/100 | 中际旭创 PE 56.3x |
| 催化面 | 50/100 | 个股相关头条 4 条 |
| 相对强弱 | 47/100 | 主线相关 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 30/100 | 年化波动 59.4% |
"""
    findings = check_stock_pick_client_report(client, source)
    assert findings == []


def test_release_check_passes_detailed_stock_pick_client_report() -> None:
    client = """# 今日个股推荐（详细版）

**数据完整度：** 本轮新闻/事件覆盖基本正常。
- A股 结构化事件覆盖 50% / 高置信公司新闻覆盖 100%

为什么今天先看这些票：因为今天不是全面追高环境，更适合抓少数还具备胜率和赔率的标的。
为什么 A 股还能做：因为景气和相对强弱还在。
为什么港股先观察：因为结构化事件已出现，但催化确认还不够。

- 新易盛的相对强弱和基本面仍在高分区。
- 中际旭创的基本面还在，但技术确认不够。
- 港股今天更像等财报窗口确认，不像立即执行日。
- 美股只有 Meta 还同时兼顾位置和基本面。
- 今天更适合先小仓，不适合把观点直接打满。
- A 股今天的重点更偏景气主线，不是低估值捡漏。
- 港股现在更看事件窗口，不看短线追涨。
- 美股今天更讲究位置和赔率，而不是谁故事更大。

### 1. [A] 新易盛 (300502)  ⭐⭐⭐ 较强机会

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 44/100 | MACD 零轴下方金叉 |
| 基本面 | 70/100 | 新易盛 PE 52.5x |
| 催化面 | 40/100 | 个股相关头条 5 条 |
| 相对强弱 | 87/100 | 相对基准更强 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 20/100 | 高波动 |

**催化证据来源：**
- `结构化事件`：[新易盛：公司将出席2026年OFC大会](https://example.com/xys)（证券时报 / 2026-03-11）

### 2. [A] 中际旭创 (300308)  — 无信号

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 36/100 | RSI 47.0 · 前低 · 量能比 0.94 |
| 基本面 | 70/100 | 中际旭创 PE 56.3x |
| 催化面 | 50/100 | 个股相关头条 4 条 |
| 相对强弱 | 47/100 | 主线相关 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 30/100 | 年化波动 59.4% |
"""
    source = """### 1. [A] 新易盛 (300502)  ⭐⭐⭐ 较强机会

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 44/100 | MACD 零轴下方金叉 |
| 基本面 | 70/100 | 新易盛 PE 52.5x |
| 催化面 | 40/100 | 个股相关头条 5 条 |
| 相对强弱 | 87/100 | 相对基准更强 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 20/100 | 高波动 |

### 2. [A] 中际旭创 (300308)  — 无信号

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 36/100 | RSI 47.0 · 前低 · 量能比 0.94 |
| 基本面 | 70/100 | 中际旭创 PE 56.3x |
| 催化面 | 50/100 | 个股相关头条 4 条 |
| 相对强弱 | 47/100 | 主线相关 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 30/100 | 年化波动 59.4% |
"""
    findings = check_stock_pick_client_report(client, source)
    assert findings == []


def test_release_check_flags_repeated_stock_explanations() -> None:
    client = """# 今日个股推荐

## A股

为什么今天优先看新易盛：因为主线、相对强弱和交易结构更匹配。
为什么暂不推荐中际旭创：因为技术确认不足。
为什么宁德时代暂不推：因为事件窗口刚打开。

| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| 新易盛 | 44 | 70 | 40 | 87 | 20 | 正式推荐 |
| 中际旭创 | 36 | 70 | 50 | 47 | 30 | 看好但暂不推荐 |

### 新易盛 (300502)

为什么能进正式推荐：

- 相对强弱仍占优，说明它不是市场最先被放弃的方向。
- 相对强弱仍占优，说明它不是市场最先被放弃的方向。
- 相对强弱仍占优，说明它不是市场最先被放弃的方向。
"""
    source = """### 1. [A] 新易盛 (300502)  ⭐⭐⭐ 较强机会

**八维雷达：**
| 维度 | 得分 | 核心信号 |
| --- | --- | --- |
| 技术面 | 44/100 | MACD 零轴下方金叉 |
| 基本面 | 70/100 | 新易盛 PE 52.5x |
| 催化面 | 40/100 | 个股相关头条 5 条 |
| 相对强弱 | 87/100 | 相对基准更强 |
| 筹码结构 | 33/100 | 北向增持 |
| 风险特征 | 20/100 | 高波动 |
"""
    findings = check_stock_pick_client_report(client, source)
    assert any("解释文案重复过多" in item for item in findings)


def test_release_check_flags_intraday_language_without_supporting_factors() -> None:
    client = """# 今日 ETF 分析

## 为什么这么判断

- 今天更适合看盘中跟随。
- 开盘就可以直接做。

## 值得继续看的地方

- 主题方向没有坏。

## 现在不适合激进的地方

- 波动仍然偏高。
- 不适合满仓。
"""
    findings = check_generic_client_report(client, "scan")
    assert any("盘中/开盘执行语言" in item for item in findings)


def test_release_check_passes_retro_spect_report() -> None:
    client = """# 决策回溯

## 原始决策

- 这笔交易发生在 2026-03-02。
- 当时是规则驱动买入。

## 为什么当时会做这个决定

- 当时均线和 MACD 都偏 bullish。
- 量价结构显示为放量上攻。
- thesis 认为产业趋势还在。

## 后验路径

- 5 日后收益 +8.00%。
- 20 日后收益 +10.00%。

## 复盘结论

- 结果判断：结果兑现。
- 复盘摘要：顺势决策且后验结果匹配。
- 具体解释：价格路径先打到标准目标位。

### 1. 宁德时代 (300750)
"""
    findings = check_generic_client_report(client, "retrospect")
    assert findings == []


def test_release_check_allows_intraday_language_with_supporting_terms() -> None:
    client = """# 今日 ETF 分析

## 为什么这么判断

- 当前价格站上 VWAP，盘中状态偏强。
- 开盘缺口不大，但首30分钟回到今开上方。

## 值得继续看的地方

- 量价没有明显转弱。

## 现在不适合激进的地方

- 仍要控制仓位。
- 如果重新跌回 VWAP 下方，不适合追。

## 当前更合适的动作

- 先等首30分钟结构确认，再决定是否跟随。
"""
    findings = check_generic_client_report(client, "scan")
    assert findings == []


def test_release_check_does_not_mistake_open_price_reference_for_intraday_claim() -> None:
    client = """# 今日晨报

## 为什么今天这么判断

- 重点验证原油是否收盘低于开盘价。
- 如果成立，说明地缘情绪有降温迹象。
- 如果不成立，说明防守主线还在。

## 今天怎么做

- 总仓位先维持中性偏低。
"""
    findings = check_generic_client_report(client, "briefing")
    assert not any("盘中/开盘执行语言" in item for item in findings)


def test_release_check_flags_empty_fund_profile_table() -> None:
    client = """# 今日 ETF 分析

## 为什么这么判断

- 红利方向还没坏。

## 基金画像
| 项目 | 内容 |
| --- | --- |
| 基金类型 | — |
| 基金公司 | — |
| 基金经理 | — |
| 成立日期 | — |
| 业绩比较基准 | — |

## 值得继续看的地方

- 方向没有坏。

## 现在不适合激进的地方

- 位置不低。
- 仍然要控制仓位。
"""
    findings = check_generic_client_report(client, "scan")
    assert any("基金画像基础字段缺失" in item for item in findings)


def test_release_check_allows_populated_fund_profile_table() -> None:
    client = """# 今日 ETF 分析

## 为什么这么判断

- 红利方向还没坏。

## 基金画像
| 项目 | 内容 |
| --- | --- |
| 基金类型 | 股票型 / 被动指数型 |
| 基金公司 | 华泰柏瑞基金 |
| 基金经理 | 柳军、李茜 |
| 成立日期 | 2006-11-17 / 22.2299亿份 |
| 业绩比较基准 | 上证红利指数 |

## 值得继续看的地方

- 方向没有坏。

## 现在不适合激进的地方

- 位置不低。
- 仍然要控制仓位。
"""
    findings = check_generic_client_report(client, "scan")
    assert not any("基金画像基础字段缺失" in item for item in findings)


def test_release_check_allows_blank_line_before_fund_profile_table() -> None:
    client = """# 今日 ETF 分析

## 为什么这么判断

- 红利方向还没坏。

## 基金画像

| 项目 | 内容 |
| --- | --- |
| 基金类型 | 股票型 / 被动指数型 |
| 基金公司 | 华泰柏瑞基金 |
| 基金经理 | 柳军、李茜 |
| 成立日期 | 2006-11-17 / 22.2299亿份 |
| 业绩比较基准 | 上证红利指数 |

## 值得继续看的地方

- 方向没有坏。

## 现在不适合激进的地方

- 位置不低。
- 仍然要控制仓位。
"""
    findings = check_generic_client_report(client, "scan")
    assert not any("基金画像章节存在，但未找到标准画像表" in item for item in findings)
