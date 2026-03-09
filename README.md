# AI-Finance

个人投资决策辅助 Agent。

这个项目不做“替你买卖”的黑盒，不做短线预测，不输出置信度。它做的是把行情、宏观、新闻、组合、风险和研究流程整理成一套本地优先、可联网兜底的工具链，让人和 agent 都能更快得到结构化判断。

## 适合谁

- 想用 ETF / 指数 / 商品期货做研究，不想手工翻几十个页面的人
- 想把自己的投资框架变成一组可重复执行命令的人
- 想在 Codex / Claude Code 里直接说需求，而不是死记命令的人

## 核心原则

- AI 负责信息整理、结构化输出和逻辑推演
- 人负责最终判断和交易执行
- 程序优先，本地缓存优先，联网补查兜底
- 不构成投资建议

## 现在已经能做什么

当前仓库已经是可运行版本，不是单个 demo 脚本。核心能力包括：

- `scan`：单标的六维扫描
- `briefing`：每日晨报 / 每周周报
- `snap`：盘中快照
- `compare`：同类标的横向对比
- `portfolio`：持仓、日志、目标权重、thesis
- `discover`：基于 watchlist 和 regime 的主动发现
- `regime`：宏观体制判断
- `policy`：政策关键词 / URL 解读
- `risk`：回撤、VaR、相关性、压力测试
- `backtest`：简单规则回测
- `research`：本地研究问答
- `lookup`：中文主题名 / ETF 名称转代码
- `assistant`：自然语言总入口

## 这套系统现在的特点

- 晨报不再只看 watchlist，已经加入“新闻主线”和“关键宏观资产”区块
- 可固定跟踪原油、美元、VIX、10Y、铜、黄金等非 watchlist 资产
- 中文 ETF 名称不再只靠少量硬编码，已经支持全量基金名称表动态搜索
- A 股 ETF 日线已做备用源回退：`AKShare/Eastmoney -> Yahoo .SS/.SZ`
- 新闻支持源偏好，例如 Reuters / Bloomberg / Financial Times
- Skill 里已经写明：本地数据不够时允许联网补查

## 快速开始

### 1. 安装

推荐 Python `3.11+`。

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 准备配置

```bash
cp config/config.example.yaml config/config.yaml
```

按需填写：

- `fred`
- `tushare`

即使 key 没填全，很多基础命令也能先跑通。

### 3. 跑一个最小例子

```bash
python -m src.commands.scan 561380
python -m src.commands.briefing daily
python -m src.commands.lookup 有色金属ETF代码是多少
python -m src.commands.assistant 帮我写今天的晨报
```

## 最常用命令

### 标的与研究

```bash
python -m src.commands.scan 561380
python -m src.commands.scan QQQM
python -m src.commands.compare 561380 GLD QQQM
python -m src.commands.lookup 芯片ETF代码是多少
python -m src.commands.research 当前宏观环境对561380意味着什么
```

### 简报与发现

```bash
python -m src.commands.briefing daily
python -m src.commands.briefing weekly
python -m src.commands.briefing daily --news-source Reuters --news-source Bloomberg
python -m src.commands.discover
python -m src.commands.discover 电网
python -m src.commands.regime
python -m src.commands.policy 电网
```

### 组合、风险与回测

```bash
python -m src.commands.portfolio status
python -m src.commands.portfolio log buy 561380 2.23 10000
python -m src.commands.portfolio set-target 561380 0.30
python -m src.commands.portfolio rebalance
python -m src.commands.portfolio review 2026-03
python -m src.commands.risk report
python -m src.commands.risk stress "美股崩盘"
python -m src.commands.backtest macd_golden_cross 561380 3y
```

### 自然语言入口

```bash
python -m src.commands.assistant 帮我写今天的晨报
python -m src.commands.assistant 帮我写今天的晨报 要有路透和彭博的消息
python -m src.commands.assistant 分析一下有色金属ETF
python -m src.commands.assistant 芯片ETF代码是多少
python -m src.commands.assistant 如果美股跌20%我的组合会怎样
```

## 数据与回退策略

这是当前项目最重要的一层设计。

### 标的代码识别

- 先查 `config/asset_aliases.yaml`
- 再查 watchlist 里的名称和代码
- 再查全量基金名称表
- 仍不足时，agent 可以联网补查

### A 股 ETF 日线

- 首选 `AKShare / Eastmoney`
- 失败时回退到 `Yahoo Finance` 的 `.SS / .SZ`

### 新闻

- 首选 `RSS`
- 可按源偏好过滤或排序
- 本地新闻不足时，允许 agent 联网补查

### 晨报

晨报现在是三层结构：

1. 今日主线
2. 新闻主线
3. 关键宏观资产 + watchlist + 组合跟踪

也就是说，它已经不是“只把 watchlist 打个表”。

## 配置文件

最重要的几个配置：

- `config/config.example.yaml`
  运行时配置模板
- `config/watchlist.yaml`
  观察池
- `config/asset_aliases.yaml`
  中文别名和常用映射
- `config/news_feeds.yaml`
  新闻源、源偏好、必带源
- `config/market_monitors.yaml`
  原油、美元、VIX、10Y、铜、黄金等固定监控
- `config/event_calendar.yaml`
  本地事件日历
- `config/stress_scenarios.yaml`
  压力测试场景
- `config/rules.yaml`
  简单规则配置

## 输出长什么样

### `scan`

目前会输出六块：

- 宏观环境
- 板块与产业链
- 资金与情绪
- 跨市场联动
- 技术面
- 估值面

标记规则：

- `✅` 偏强 / 偏有利
- `⚠️` 中性 / 信号不充分
- `❌` 偏弱 / 偏拥挤 / 偏不利

### `briefing daily`

现在通常包含：

- 今日主线
- 新闻主线
- 关键宏观资产
- 隔夜与主要资产
- 宏观与流动性
- 市场概览
- 全球资金流代理
- 情绪代理
- Watchlist 雷达
- 重点观察
- 关注提醒
- 今日已知事件
- 组合与 Thesis
- 行动建议

## 边界和诚实说明

当前系统已经可用，但这些边界要明确：

- 不做个股推荐
- 不做自动交易
- 不做短线预测
- 社媒情绪和全球资金流仍有代理成分，不是假装有机构级原始数据
- 估值面目前仍偏“价格位置代理”，还不是完整 PE / PB / PEG 体系
- 新闻是“程序优先 + 联网兜底”，不是纯人工编辑晨报

## 对 agent 的使用约定

如果你把它当 agent 的后端工具库，推荐遵守这个顺序：

1. 先跑本地命令
2. 本地命令失败时先试备用源
3. 仍缺关键事实时联网补查
4. 最终回答里明确区分“程序输出”和“联网补充”

这条规则也已经写进 `skill/SKILL.md`。

## 项目结构

```text
config/      配置、别名、新闻源、watchlist、规则、事件
data/        SQLite、缓存、产业链图谱、历史数据
docs/        架构文档
skill/       Claude Code / Codex Skill
src/         collectors / processors / storage / output / commands
tests/       单元测试
```

## 测试

```bash
python -m pytest -q
```

当前本地已验证通过。

## 当前阶段

可以把它理解成一个已经能日常使用的 `v0.x`：

- 能看标的
- 能写晨报
- 能做组合和风险基础管理
- 能接自然语言入口
- 能在本地数据不足时切到联网兜底

还没有结束，但已经不是“未来规划”。
