# AI-Finance

个人投资决策辅助 Agent。

这个项目不做“替你买卖”的黑盒，不做拍脑袋短线预测，也不把单一指标包装成确定答案。它做的是把行情、估值、宏观、新闻、组合、风险和研究流程整理成一套本地优先、可联网兜底、对 agent 友好的工具链。

当前版本的核心定位已经很明确：

- A 股相关数据以 `Tushare-first` 为默认架构
- 支持 ETF、场外基金、A 股个股、港股、美股的统一研究入口
- 支持晨报 / 午报 / 晚报 / 周报，以及组合跟踪和复盘闭环
- 已支持 `stock_pick` 个股精选，不再局限于“只看 ETF”

## 适合谁

- 想研究 ETF、场外基金、A 股/港股/美股，不想手工翻几十个页面的人
- 想把自己的投资框架变成一组可重复执行命令的人
- 想在 Codex / Claude Code 里直接说需求，而不是死记命令的人
- 想先用程序把事实、结构和风险框出来，再自己做最终判断的人

## 核心原则

- AI 负责信息整理、结构化输出和逻辑推演
- 人负责最终判断和交易执行
- A 股 / 基金 / 宏观 / 资金面尽量优先走 `Tushare`
- 本地缓存优先，联网补查兜底
- 所有输出都默认按“研究辅助，不构成投资建议”处理

## 现在已经能做什么

当前仓库已经是可运行版本，不是单个 demo 脚本。核心能力包括：

- `scan`：单标的八维扫描，支持 ETF / 场外基金 / A 股 / 港股 / 美股
- `stock_pick`：跨市场个股精选，输出候选、介入条件、仓位和止损框架
- `briefing`：每日晨报 / 午报 / 晚报 / 每周周报
- `snap`：盘中快照
- `compare`：同类标的横向对比
- `portfolio`：持仓、日志、目标权重、thesis、月度复盘 / 决策回溯
- `discover`：基于 watchlist 和 regime 的主动发现
- `regime`：宏观体制判断
- `policy`：政策关键词 / URL 解读
- `risk`：回撤、VaR、相关性、压力测试
- `backtest`：简单规则回测
- `research`：本地研究问答
- `lookup`：中文主题名 / ETF 名称转代码
- `assistant`：自然语言总入口

## 这套系统现在的特点

- A 股数据链路已经从旧版 `AKShare-first` 迁到 `Tushare-first`
- 估值面不再只是“价格位置代理”，已经接入 `PE / PB / PS / ROE` 以及营收增速、毛利率等真实指标
- 对 ETF / 指数 / 场外基金，估值与基本面会按指数成分或重仓股做加权代理，不再只看价格相对高低
- 晨报不再只看 watchlist，已经加入“新闻主线”“关键宏观资产”“组合跟踪”“昨日验证回顾”
- `briefing` 不只支持 `daily` / `weekly`，还支持 `noon` / `evening`
- 中文资产识别不再只靠少量硬编码，已经支持别名、watchlist、Tushare 基金表和实时兜底搜索
- 场外基金不再只能看净值，已经能输出基金画像、持仓结构、行业暴露和基金经理风格
- 数据不足时允许自动回退到 `AKShare / efinance / yfinance / RSS / 联网补查`

## 快速开始

### 1. 安装

推荐 Python `3.13+`。

```bash
python3.13 -m venv .venv
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

即使 key 没填全，很多基础命令也能先跑通；只是 A 股相关能力会更频繁回退到备用源。

如果你要调技术参数、风险阈值或扫描门槛，不要直接把基础配置堆满，按需从下面这个文件复制对应片段即可：

- `config/config.advanced.example.yaml`

### 3. 跑一个最小例子

```bash
python -m src.commands.scan 561380
python -m src.commands.scan 022365
python -m src.commands.briefing daily
python -m src.commands.lookup 有色金属ETF代码是多少
python -m src.commands.assistant 帮我写今天的晨报
```

## 最常用命令

### 标的研究

```bash
python -m src.commands.scan 561380
python -m src.commands.scan 022365
python -m src.commands.scan QQQM
python -m src.commands.compare 561380 GLD QQQM
python -m src.commands.lookup 芯片ETF代码是多少
python -m src.commands.research 当前宏观环境对561380意味着什么
```

### 晨报、发现与策略跟踪

```bash
python -m src.commands.briefing daily
python -m src.commands.briefing noon
python -m src.commands.briefing evening
python -m src.commands.briefing weekly
python -m src.commands.briefing daily --news-source Reuters --news-source Bloomberg
python -m src.commands.discover
python -m src.commands.discover 电网
python -m src.commands.regime
python -m src.commands.policy 电网
```

### 个股精选

```bash
python -m src.commands.stock_pick --market all --top 10
python -m src.commands.stock_pick --market cn --sector 科技 --top 15
python -m src.commands.stock_pick --market us --sector 消费 --top 10
```

### 组合、风险与回测

```bash
python -m src.commands.portfolio status
python -m src.commands.portfolio log buy 561380 2.23 10000
python -m src.commands.portfolio set-target 561380 0.30
python -m src.commands.portfolio rebalance
python -m src.commands.portfolio review 2026-03
python -m src.commands.portfolio review 2026-03 --symbol 300750
python -m src.commands.portfolio review 2026-03 --lookahead 20 --stop-pct 0.08 --target-pct 0.15
python -m src.commands.risk report
python -m src.commands.risk stress "美股崩盘"
python -m src.commands.backtest macd_golden_cross 561380 3y
```

### 自然语言入口

```bash
python -m src.commands.assistant 帮我写今天的晨报
python -m src.commands.assistant 帮我写今天的晨报 要有路透和彭博的消息
python -m src.commands.assistant 分析一下有色金属ETF
python -m src.commands.assistant 分析一下基金022365
python -m src.commands.assistant 芯片ETF代码是多少
python -m src.commands.assistant 如果美股跌20%我的组合会怎样
```

## 数据与回退策略

这是当前项目最重要的一层设计。

### 数据源优先级

对项目当前已经接好的链路，可以这样理解：

- **A 股个股 / ETF / 场外基金 / 指数 / 宏观 / 资金 / 财务**：优先 `Tushare`
- **Tushare 不覆盖、权限不足或接口失败**：自动回退到 `AKShare / efinance / yfinance`
- **新闻**：优先本地 RSS 聚合，必要时允许 agent 联网补查
- **基金画像、经理、持仓细节**：以 `Tushare + AKShare/Xueqiu` 组合补齐

这意味着系统不是“只要 Tushare 缺一项就跑不动”，而是：

1. 先走更稳、更结构化的 `Tushare`
2. 缺权限或缺覆盖时自动降级
3. 最终回答里应明确哪些是程序输出，哪些是联网补充

### Tushare 积分档位怎么理解

README 这里不去复述一整份官方权限表，而只写和本项目直接相关的使用口径：

- **未配置 token / 低积分**：基础命令仍可跑，但 A 股链路会更频繁回退，口径一致性和完整性会变差
- **推荐至少 `2000+` 积分**：这是当前项目的大多数 A 股研究能力的默认设计档位，个股行情、复权、估值快照、财务指标、宏观、北向资金、融资融券、基金列表/净值等能力基本都能覆盖
- **希望尽量少回退，建议 `5000+`**：部分 ETF/龙虎榜机构类接口在官方单接口页会标更高权限门槛，这种情况下更高积分会更稳

项目本身不会把“必须有某个高积分接口”当成硬前提。权限不足时，优先降级，不直接报废整条分析链。

### 项目里实际会用到的 Tushare API

当前主要覆盖这些方向：

- **行情**：`daily` + `adj_factor`
- **估值/基本面**：`daily_basic` + `fina_indicator`
- **财报**：`income` / `balancesheet` / `cashflow`
- **宏观**：`cn_pmi` / `cn_cpi` / `cn_ppi` / `cn_m` / `cn_sf` / `shibor_lpr` / `shibor` / `cn_gdp`
- **指数**：`index_daily` + `index_weight`
- **资金面**：`moneyflow_hsgt` + `hsgt_top10`
- **情绪面**：`top_list` / `top_inst`
- **杠杆面**：`margin`
- **风险面**：`pledge_stat`
- **基金/ETF**：`fund_basic` / `fund_nav` / `fund_daily`

### 标的代码识别

- 先查 `config/asset_aliases.yaml`
- 再查 watchlist 里的名称和代码
- 再查 Tushare 基金/证券名称表
- 仍不足时，再让 agent 联网补查

### A 股个股日线

- 首选 `Tushare daily + adj_factor`
- 失败时回退到 `AKShare / Eastmoney`
- 再失败回退到 `Yahoo Finance` 的 `.SS / .SZ`

### A 股 ETF 日线

- 首选 `Tushare fund_daily`
- 失败时回退到 `AKShare / Eastmoney`
- 再失败回退到 `Yahoo Finance` 的 `.SS / .SZ`

### 场外基金净值

- 首选 `Tushare fund_nav`
- 失败时回退到 `AKShare`

### 新闻

- 首选 `RSS`
- 可按源偏好过滤或排序
- 本地新闻不足时，允许 agent 联网补查

## 配置文件

推荐按“常改”和“低频调参”两层理解：

### 大多数用户只需要改这几个

- `config/config.yaml`
  最低可用运行配置
- `config/watchlist.yaml`
  观察池
- `config/asset_aliases.yaml`
  中文别名和常用映射

### 低频调参或高级源配置

- `config/config.advanced.example.yaml`
  技术参数、风险阈值、扫描门槛、数据源路径
- `config/news_feeds.yaml`
  新闻源、源偏好、必带源
- `config/market_monitors.yaml`
  原油、美元、VIX、10Y、铜、黄金等固定监控
- `config/market_overview.yaml`
  晨报里的国内指数和隔夜外盘面板
- `config/event_calendar.yaml`
  本地事件日历
- `config/catalyst_profiles.yaml`
  行业/主题的催化映射
- `config/stress_scenarios.yaml`
  压力测试场景
- `config/rules.yaml`
  简单规则配置
- `config/stock_pools.yaml`
  `stock_pick` 的 A 股 / 港股 / 美股候选池

## 输出长什么样

### `scan`

现在默认是“先结论，再展开”的结构，而不是先扔一大堆分数：

- 一句话结论
- 当前判断
- 核心驱动
- 当前最重要的矛盾
- 风险点
- 观察指标
- 情景分析
- 操作框架

如果标的是场外基金，还会额外附带：

- 基金画像
- 基金成分分析
- 基金经理风格分析

需要更机械的维度细节时，再下钻到八维分数和附录。

### `briefing`

`daily` 现在按“倒金字塔”结构生成，先写主线和行动，再展开细节：

- 昨日验证回顾
- 主线判断与行动
- 市场全景
- 驱动与催化
- 今日验证点
- 组合与持仓
- 附录

`noon` 会承接当日晨报，检查上午验证情况并给出下午观察点。

`evening` 会回看全天兑现度、主线偏差和次日跟踪要点。

生成的简报会归档到 `reports/`，供下一交易日做闭环复盘。

### `stock_pick`

`stock_pick` 会输出：

- 当前扫描范围与通过门槛数量
- 入选市场分布
- 每只个股的八维雷达
- 结论、风险提示、介入条件、仓位、止损与目标参考
- 数据盲区与降级说明

## 边界和诚实说明

当前系统已经可用，但这些边界要明确：

- 支持 `stock_pick` 个股精选，但它给的是候选和框架，不是“替你下结论”
- 不做自动交易
- 不做保证收益的短线预测
- 社媒情绪、全球资金流、ETF 份额等部分维度仍带代理成分，不是假装有机构级全量原始数据
- 估值面已经接入 `PE / PB / PS / ROE` 等真实指标；但 ETF / 指数 / 场外基金的部分增长与财务项，仍可能基于成分股或重仓股做加权代理
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
reports/     晨报、扫描结果等归档
skill/       Claude Code / Codex Skill
src/         collectors / processors / storage / output / commands
tests/       单元测试
```

## 测试

```bash
python -m pytest -q
```

## 当前阶段

可以把它理解成一个已经能日常使用的 `v0.x`：

- 能分析 ETF、场外基金和个股
- 能写晨报、午报、晚报、周报
- 能做个股精选、组合管理和风险评估
- 能接自然语言入口
- 能在本地数据不足时自动降级和联网兜底

还没有结束，但已经不是“未来规划”。
