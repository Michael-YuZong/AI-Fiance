# AI-Finance

本地优先的 CLI 投研工作台。

它不是网页产品，不是自动交易系统，也不是“给一个代码就替你买卖”的黑盒。  
它做的是把研究、推荐、风险、组合、日报、复盘和外审流程串成一套能日常使用的本地工具链。

截至 2026-03-13，这个项目已经不是 demo，而是一个可运行、可测试、可持续迭代的 `AI 金融专家 v0.x`。

## 这项目现在是什么

可以把它理解成一套本地 CLI 版投研系统，主链路是：

1. `assistant / lookup / research` 识别问题
2. `scan / compare / discover / stock_pick / fund_pick / etf_pick / policy / risk` 做分析
3. `client_report / opportunity_report / policy_report / briefing` 输出结果
4. `release_check / report_guard` 做交付门禁
5. `portfolio / review / retrospect` 记录交易并复盘

现在已经能覆盖：

- 单标研究
- ETF / 场外基金 / 个股推荐
- 组合状态、风险报告、压力测试
- 晨报 / 午报 / 晚报 / 周报
- 研究问答
- 月度决策复盘
- 客户稿 / 详细稿 / 外审门禁

## 这项目不是什么

- 不是自动下单系统
- 不是高频或盘口级策略系统
- 不是“只靠一个分数决定买卖”的推荐器
- 不是纯前端展示项目
- 不是“只会生成一篇报告”的文案机器人

## 当前状态快照

截至 2026-03-13，整体状态可以这样看：

### 已成熟

- `scan / stock_analysis`
- `stock_pick`
- `fund_pick`
- `etf_pick`
- `risk`
- `portfolio`
- `research`
- `compare`
- `briefing`
- `lookup / assistant`

### 可用但还在迭代

- `discover`
- `policy`
- `decision_review / retrospect`
- `scheduler`

### 仍偏弱或仍未全仓库统一

- 代理信号仍是代理，不是机构级原始全量 feed
- 时点正确性 / 证据溯源还没有扩成 repo-wide contract
- 执行成本 / 可成交性 / 风险预算已经有 v1，但还没接到所有 pick 流水线
- 评分校准 / 归因已经有 v1，但还没形成长期月度学习闭环

### 当前回归状态

- `334 passed, 1 skipped`

## 现在已经做好的关键能力

### 1. 单标研究

- `scan` 和 `stock_analysis` 是最稳的核心链路
- 已有八维分析、风险、情景、执行框架、图表、数据盲区说明
- 支持 ETF、场外基金、A 股、港股、美股

### 2. Pick 流水线

- `stock_pick` 已是最产品化功能
- `fund_pick` 已从固定候选比较器升级为全市场场外基金预筛
- `etf_pick` 已接入共享快照、覆盖率披露、rerun diff、release guard
- 客户稿现在会显式写持有周期 / 打法，而不只是“能不能追”

### 3. 研究问答

- `research` 已不是平铺模块输出
- 现在会区分：
  - 市场状态 / 风格问答
  - 标的研究 / 交易问题
  - 政策影响 / 主题问答
  - 组合风险 / 场景问答
- 现在还能回答：
  - 现在适合上多少仓位
  - 做不做得进去
  - 大致执行成本高不高

### 4. 组合、风险、复盘

- `portfolio` 支持持仓、交易日志、目标权重、rebalance、thesis、月度 review
- `risk` 支持相关性、VaR/CVaR、Beta、压力测试
- `portfolio whatif` 已上线：
  - 买/卖一笔后组合会变成什么样
  - 单票上限是否超标
  - 波动 / Beta 会不会被放大
  - 流动性、参与率、滑点、费用大概怎样
- `decision_review / retrospect` 已有 v1：
  - benchmark-relative excess return
  - setup bucket 校准
  - first-pass 结果归因
  - 时点 / 执行快照回放

### 5. 报告交付链

- `briefing` 已有晨报 / 午报 / 晚报 / 周报
- `client_report` / `opportunity_report` / `policy_report` 已形成正式输出链
- `release_check` / `report_guard` 已把外审和 final export 固化成流程

## 计划完成度

项目路线图见 [plan.md](./plan.md)。  
如果按阶段看，当前完成度大概是：

| 阶段 | 主题 | 当前状态 |
| --- | --- | --- |
| A | `research` 升级为真正研究入口 | 已完成主版本 |
| B | 代理信号升级 | 已完成第一轮，`research / briefing` 已接入 |
| C | 政策与宏观链路升级 | 部分完成，`policy` 明显增强但原文/PDF 仍未彻底打完 |
| D | 组合构建与风险预算 | 已完成 v1，先落在 `portfolio / whatif / review` |
| E | 时点正确性与证据溯源 | 已完成 v1，先落在 `portfolio / review / research` |
| F | 评分校准、归因与自我学习 | 已完成 v1，先落在 `decision_review / retrospect` |
| G | 执行成本与可成交性 | 已完成 v1，先落在 `portfolio whatif + research` |
| H | 调度与运营闭环 | 仍是下一阶段主任务 |

### 当前最该继续做的事

1. `discover` v2
2. 把 `portfolio whatif / research / retrospect` 这套 action 语言继续接到 `stock_pick / fund_pick / etf_pick`
3. 继续补 proxy signals 的 repo-wide 披露
4. 做 `scheduler` v2
5. 继续深化 `policy` 的长文 / PDF / OFD 抽取

## 快速开始

### 1. 安装

推荐 Python `3.13+`。

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置

```bash
cp config/config.example.yaml config/config.yaml
```

常见需要填的配置：

- `tushare`
- `fred`

如果你要调风险阈值、扫描门槛、技术参数，再按需参考：

- `config/config.advanced.example.yaml`

### 3. 跑最小例子

```bash
python -m src.commands.scan 561380
python -m src.commands.briefing daily
python -m src.commands.research 为什么最近市场有点别扭
python -m src.commands.research 561380 现在适合上多少仓位，做得进去吗
python -m src.commands.portfolio whatif buy 561380 2.1 20000
```

## 最常用命令

### 单标研究

```bash
python -m src.commands.scan 561380
python -m src.commands.stock_analysis 300750
python -m src.commands.compare 561380 GLD QQQM
python -m src.commands.lookup 芯片ETF代码是多少
python -m src.commands.research 561380 现在还能不能买
python -m src.commands.research 561380 现在适合上多少仓位，做得进去吗
```

### Pick / 发现

```bash
python -m src.commands.discover
python -m src.commands.etf_pick
python -m src.commands.fund_pick
python -m src.commands.stock_pick --market all --top 10
```

### 报告

```bash
python -m src.commands.briefing daily
python -m src.commands.briefing noon
python -m src.commands.briefing evening
python -m src.commands.briefing weekly
python -m src.commands.policy 电网
```

### 组合 / 风险 / 复盘

```bash
python -m src.commands.portfolio status
python -m src.commands.portfolio log buy 561380 2.23 10000
python -m src.commands.portfolio whatif buy 561380 2.1 20000
python -m src.commands.portfolio set-target 561380 0.30
python -m src.commands.portfolio rebalance
python -m src.commands.portfolio review 2026-03
python -m src.commands.risk report
python -m src.commands.risk stress "美股崩盘"
```

### 自然语言入口

```bash
python -m src.commands.assistant 帮我写今天的晨报
python -m src.commands.assistant 分析一下有色金属ETF
python -m src.commands.assistant 如果美股跌20%我的组合会怎样
```

## 数据策略

当前的总体设计是：

- A 股 / ETF / 场外基金 / 指数 / 宏观：`Tushare-first`
- 不足时回退：`AKShare / efinance / yfinance`
- 新闻：RSS 聚合优先，必要时联网兜底
- 组合与报告：优先本地数据、本地缓存、本地快照

重点不是“绝不降级”，而是：

1. 能先跑
2. 降级要诚实写出来
3. 不把代理或缺失数据装成硬事实

## 输出风格

现在的大多数核心输出都不是“先甩分数”，而是更偏决策型结构：

- 一句话结论
- 证据
- 风险与不确定性
- 下一步 / 执行框架

其中：

- `scan` 更偏完整研究卡
- `briefing` 更偏日报/周报
- `research` 更偏即时问答
- `portfolio whatif` 更偏交易预演
- `retrospect` 更偏事后复盘

## 边界和诚实说明

这些边界必须明确：

- 不自动交易
- 不保证收益
- 代理信号仍是代理，不是全量原始资金 / 社媒 / 机构级 feed
- ETF / 基金的一部分基本面仍可能来自成分股或重仓股加权代理
- `whatif` 的执行成本目前是 v1 估算，不等于真实成交回报单
- `retrospect` 的校准和归因目前也是 v1，不是完整量化研究平台

## 对新 session / 新 agent 的约定

不要先问“这个项目是做什么的”。  
先看：

1. [AGENTS.md](./AGENTS.md)
2. [plan.md](./plan.md)
3. 当前要改的 command / processor / renderer
4. 对应 tests

项目现在已经形成明确迭代方法：

1. 先复现真实问题
2. 先修产品合同
3. 再修代码
4. 补测试
5. 跑真实命令样例
6. 必要时接外审
7. 更新 `AGENTS.md` / `plan.md`

## 项目结构

```text
config/   配置、watchlist、stress scenarios、数据源设置
data/     本地 JSON / SQLite / score history
docs/     prompt、复审文档、方法说明
reports/  各类内部稿、客户稿、复盘归档
src/      commands / processors / collectors / output / storage
tests/    回归测试
```

## 测试

全量回归：

```bash
pytest -q
```

如果只是改某一条链，先跑对应 targeted tests，再跑全量。

## 当前一句话总结

它现在已经是一个“成熟的个人 / 小团队 AI 投研工作台”，强项是研究、推荐、风险、交付和复盘已经连成闭环；接下来最重要的工作不是再堆新命令，而是把时点、归因、执行和组合约束继续扩成全仓库统一合同。
