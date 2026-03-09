# AI-Finance

个人投资决策辅助 Agent。这个项目不做“替你买卖”的黑盒，而是把行情、宏观、规则、风险和研究流程整理成一套本地可运行的工具链，帮助你更快地看清楚：

- 现在发生了什么
- 这个标的强还是弱
- 当前价格位置拥不拥挤
- 组合风险是不是在失控
- 你的买入逻辑还成不成立

核心原则：

- AI 负责信息整理、结构化输出和逻辑推演
- 人负责最终判断和下单
- 不做短线预测
- 不输出“置信度”
- 不构成投资建议

## 这个项目在做什么

项目设计上覆盖 7 个模块：

1. 主动发现机会：扫描 watchlist、事件、异动和产业链传导
2. 标的扫描：从宏观、资金、技术面、估值面等角度输出打分卡
3. 组合管理：记录持仓、成本、仓位和 thesis
4. 情报简报：生成晨报、周报和事件快报
5. 交互式研究：自由提问，联动已有数据回答
6. 风险管理：相关性、VaR、压力测试、集中度预警
7. 简易回测：验证规则有没有明显失效

当前仓库已经实现到 Phase 5 基础版，重点是把“看标的、看组合、看风险、看规则”和“直接说需求”这两条主链路先跑通：

- 配置加载、日志、重试、缓存
- A 股 ETF / 美股 / 港股 / 商品行情采集骨架
- 技术指标引擎
- 六维扫描基础版：宏观、产业链、资金情绪、跨市场、技术面、估值面
- Phase 5 代理模块：社媒情绪代理、全球资金流代理、本地事件日历
- SQLite 基础存储
- `scan`、`briefing`、`snap`、`compare`、`portfolio`、`discover`、`regime`、`policy` 命令可运行
- `risk`、`backtest`、`research` 命令已接通 Phase 4 基础版
- `assistant` 自然语言入口已可用，不会命令的人可以直接说需求

## 当前支持什么

当前能直接使用的核心命令有：

```bash
python -m src.commands.scan <symbol>
python -m src.commands.briefing daily
python -m src.commands.briefing weekly
python -m src.commands.snap <symbol>
python -m src.commands.compare <symbol1> <symbol2> ...
python -m src.commands.portfolio status
python -m src.commands.portfolio log buy 561380 2.23 10000
python -m src.commands.portfolio set-target 561380 0.30
python -m src.commands.portfolio rebalance
python -m src.commands.portfolio thesis set 561380 --core "..." --validation "..." --stop "..." --period "6-12个月"
python -m src.commands.portfolio review 2026-03
python -m src.commands.discover 电网
python -m src.commands.regime
python -m src.commands.policy 电网
python -m src.commands.risk report
python -m src.commands.risk stress "美股崩盘"
python -m src.commands.backtest macd_golden_cross 561380 3y
python -m src.commands.research 当前宏观环境对561380意味着什么
python -m src.commands.assistant 帮我写今天的晨报
```

例如：

```bash
python -m src.commands.scan 561380
python -m src.commands.scan QQQM
python -m src.commands.scan HSTECH
python -m src.commands.scan AU0
```

命令会自动识别标的类型，然后：

1. 拉取历史行情
2. 统一成 OHLCV 数据格式
3. 计算技术指标
4. 汇总宏观、产业链、资金情绪和跨市场代理信号
5. 把行情写入本地 SQLite
6. 输出 Markdown 报告或命令结果

当前支持的标的识别规则在 `config/config.yaml` 中配置：

- `^[0-9]{6}$` -> A 股 ETF
- `^[0-9]{5}$` -> 港股
- `^HSTECH$` -> 港股科技指数
- `^[A-Z]{1,5}$` -> 美股 / ETF
- `^[A-Z]{1,2}[0-9]$` -> 商品期货主力代码

## 报告里会看到什么

`scan` 输出的是一份 Markdown 打分卡，目前会看到六块：

- 宏观环境
- 板块与产业链
- 资金与情绪
- 跨市场联动
- 技术面：MACD、均线系统、RSI、DMI/ADX、量能、K 线形态
- 估值面：当前先用“价格位置代理”代替真实 PE/PB 估值

输出形式是：

- `✅`：偏强 / 偏便宜 / 偏有利
- `⚠️`：中性 / 信号不充分
- `❌`：偏弱 / 偏拥挤 / 偏不利

注意：现在的“估值面”还不是真正基本面估值，只是 Phase 1 的占位实现。后面会接 PE/PB 分位、PEG、股息率等更完整的数据。

## 怎么安装

推荐 Python 3.11+。当前仓库在本机 Python 3.9 也已做过兼容性验证，但目标环境仍建议用 3.11+。

1. 创建虚拟环境

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 准备配置文件

```bash
cp config/config.example.yaml config/config.yaml
```

4. 按需填写 API key

目前配置里预留了：

- `fred`: FRED 宏观数据 API key
- `tushare`: Tushare token

如果你先跑 `scan`、`briefing`、`discover`、`regime`、`policy` 这些基础命令，很多情况下即便没填完整 key 也能先跑通主要链路。

## 怎么用

### 1. 先改配置

主配置文件：
[config.yaml](/Users/bilibili/fiance/AI-Finance/config/config.yaml)

示例配置：
[config.example.yaml](/Users/bilibili/fiance/AI-Finance/config/config.example.yaml)

这里主要控制：

- API keys
- 缓存目录和数据库路径
- 风险阈值
- 技术指标参数
- 标的类型识别规则

### 2. 执行扫描

```bash
python -m src.commands.scan 561380
```

如果你想指定配置文件：

```bash
python -m src.commands.scan 561380 --config config/config.yaml
```

### 3. 生成简报

```bash
python -m src.commands.briefing daily
python -m src.commands.briefing weekly
```

### 4. 查看盘中快照

```bash
python -m src.commands.snap 561380
```

### 5. 做横向对比

```bash
python -m src.commands.compare 518880 518800 159934
python -m src.commands.compare 561380 GLD QQQM
```

### 6. 管理组合

查看组合：

```bash
python -m src.commands.portfolio status
```

记录买入：

```bash
python -m src.commands.portfolio log buy 561380 2.23 10000
```

设置目标权重并查看再平衡建议：

```bash
python -m src.commands.portfolio set-target 561380 0.30
python -m src.commands.portfolio rebalance
```

设置 thesis 并做检查：

```bash
python -m src.commands.portfolio thesis set 561380 \
  --core "AI 驱动电力需求上行，电网投资提速" \
  --validation "电网投资完成额同比 > 10%" \
  --stop "趋势转弱且景气下修" \
  --period "6-12个月"

python -m src.commands.portfolio thesis check
```

做月度复盘：

```bash
python -m src.commands.portfolio review 2026-03
```

### 7. 做 Phase 3 研究辅助

主动发现：

```bash
python -m src.commands.discover
python -m src.commands.discover 电网
```

宏观体制识别：

```bash
python -m src.commands.regime
```

政策解读：

```bash
python -m src.commands.policy 电网
python -m src.commands.policy https://example.com/policy-page
```

### 8. 做 Phase 4 风险与回测

组合风险报告：

```bash
python -m src.commands.risk report
```

查看相关性：

```bash
python -m src.commands.risk correlation
```

跑压力测试：

```bash
python -m src.commands.risk stress "美股崩盘"
python -m src.commands.risk stress "人民币急贬"
```

做规则回测：

```bash
python -m src.commands.backtest macd_golden_cross 561380 3y
python -m src.commands.backtest oversold_rebound HSTECH 3y
```

研究问答：

```bash
python -m src.commands.research 当前宏观环境对561380意味着什么
python -m src.commands.research 如果美股跌20%我的组合会怎样
python -m src.commands.research 我的持仓相关性高不高
```

`research` 当前是 Phase 4 的本地启发式实现：会联动宏观、标的快照、组合风险和预设压力场景做结构化回答；如果部分外部数据源不可用，会自动降级，不会直接中断。

### 9. 不会命令也能用

如果你不想记命令，可以直接用自然语言入口：

```bash
python -m src.commands.assistant 帮我写今天的晨报
python -m src.commands.assistant 看看561380现在值不值得关注
python -m src.commands.assistant 对比 QQQM 和 GLD
python -m src.commands.assistant 如果美股跌20%我的组合会怎样
```

它会先自动判断你的意图，再路由到已有命令。判断不稳时，会回退到 `research`。

### 10. 查看本地数据

扫描后会写入：

- SQLite 数据库：`data/investment.db`
- 缓存目录：`data/cache/`
- 组合文件：`data/portfolio.json`
- 交易日志：`data/trade_log.json`

这样后续可以减少重复请求。

## 项目结构

- `config/`: 配置、watchlist、规则与压力测试场景
- `data/`: 本地数据、产业链图谱、历史 regime、缓存和 SQLite
- `docs/`: 架构文档
- `skill/`: Claude Code Skill 定义
- `src/collectors/`: 数据采集层
- `src/processors/`: 技术指标、打分、风险、回测等处理层
- `src/storage/`: SQLite 和持仓数据存储层
- `src/output/`: Markdown 报告输出层
- `src/commands/`: 命令入口层
- `tests/`: 单元测试

## 当前实现进度

- 已完成：Phase 1 到 Phase 5 基础版
- 已可运行：`scan`、`briefing`、`snap`、`compare`、`portfolio`、`discover`、`regime`、`policy`、`risk`、`backtest`、`research`
- 已有代理增强：社媒情绪代理、全球资金流代理、本地事件日历、自然语言命令路由
- 已有真实数据接入：A 股 ETF、港股代理行情、美股 ETF、商品期货、部分中国宏观数据
- 已有启发式研究模块：事件驱动发现、历史 regime 类比、政策模板/URL 解读、thesis 健康检查、月度操作复盘、自然语言问答

也就是说，仓库现在已经不是只有一个 `scan`，而是日常盯盘、主题研究、基础组合管理和自然语言入口都能开始用了。当前仍然诚实保留一个边界：社媒情绪和全球资金流还是代理版，不是假装有机构级原始数据。

## 测试

运行测试：

```bash
python -m pytest -q
```

当前仓库已经包含最小测试覆盖：

- 技术指标
- 风险分析
- 回测引擎
- SQLite 存储
- 中国宏观采集器基础行为
- 组合仓位与再平衡
- 简报渲染
- Regime 识别
- Policy 关键词解析
- Thesis 存储

## 后续计划

接下来优先补这几块：

- 更完整的宏观 / 财报 / 政策事件日历
- 更细的自然语言意图识别与自动参数补全
- 如果接入外部渠道，再补通知 / 推送能力

## 免责声明

本项目只做研究辅助和风险提示，不提供收益承诺，不构成任何投资建议。
