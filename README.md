# AI-Finance

本地优先的 CLI 投研工作台。

它不是网页产品，不是自动交易系统，也不是“给个代码就替你买卖”的黑盒。它做的是把研究、推荐、风险、组合、日报、复盘和外审流程串成一套可运行、可测试、可持续迭代的本地工具链。

## 30 秒读完

- 最强主链路：`scan / stock_analysis / stock_pick / fund_pick / etf_pick / research / risk / portfolio`
- 当前最重要的新研究层：`strategy`
- 默认入口：先看 [docs/context_map.md](./docs/context_map.md)
- YAML 入口：先看 [config/README.md](./config/README.md)
- 路线图入口：先看 [plan.md](./plan.md)

## 渐进式披露读法

默认不要把整个仓库的 `.md` 和 `.yaml` 全读一遍。

建议顺序：

1. 先读这个文件，只拿项目定位和入口。
2. 再读 [docs/context_map.md](./docs/context_map.md)，按任务决定接下来只看哪些文件。
3. 如果任务涉及配置，再读 [config/README.md](./config/README.md)。
4. 如果任务涉及 `strategy`，再读 [docs/plans/strategy.md](./docs/plans/strategy.md)。
5. 最后才打开你要修改的 command / processor / renderer / test。

## 它现在是什么

可以把它理解成一套本地 CLI 投研系统，主链路是：

1. `assistant / lookup / research` 识别问题
2. `scan / compare / discover / policy / pick` 做分析或推荐
3. `client_report / opportunity_report / briefing / policy_report` 输出结果
4. `release_check / report_guard` 做交付门禁
5. `portfolio / risk / retrospect / strategy` 负责预演、验证、归因和复盘

## 它不是什么

- 不是自动下单系统
- 不是高频或盘口级策略系统
- 不是只靠一个分数决定买卖的推荐器
- 不是前端展示项目
- 不是一次性生成一篇报告就结束的文案机器人

## 当前状态

成熟区：

- `scan / stock_analysis`
- `stock_pick / fund_pick / etf_pick`
- `research`
- `risk / portfolio`
- `compare`
- `briefing`
- `lookup / assistant`

可用但仍在迭代：

- `discover`
- `policy`
- `scheduler`
- `retrospect / decision_review`
- `strategy` 的治理层与 fixture

更细的成熟度、backlog、最近变化，放在 [AGENTS.md](./AGENTS.md) 和 [docs/status_snapshot.md](./docs/status_snapshot.md)。

## 快速开始

### 安装

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 最小配置

```bash
cp config/config.example.yaml config/config.yaml
```

大多数用户只需要先填：

- `api_keys.tushare`
- `api_keys.fred`

### 最小命令集

```bash
python -m src.commands.scan 561380
python -m src.commands.briefing daily
python -m src.commands.research 为什么最近市场有点别扭
python -m src.commands.portfolio whatif buy 561380 2.1 20000
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
```

## 常用入口

研究与分析：

```bash
python -m src.commands.scan 561380
python -m src.commands.stock_analysis 300750
python -m src.commands.compare 561380 GLD QQQM
python -m src.commands.research 561380 现在还能不能买
```

推荐与发现：

```bash
python -m src.commands.discover
python -m src.commands.etf_pick
python -m src.commands.fund_pick
python -m src.commands.stock_pick --market all --top 10
```

组合、风险、策略：

```bash
python -m src.commands.portfolio status
python -m src.commands.risk report
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
```

## 配置和文档入口

- YAML 配置地图：[config/README.md](./config/README.md)
- 任务读法地图：[docs/context_map.md](./docs/context_map.md)
- 当前状态快照：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 路线图总览：[plan.md](./plan.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)

## 历史文档说明

- [docs/architecture_v2.md](./docs/architecture_v2.md) 仍保留，但它是历史架构草案，不再是默认入口。
- `reports/`、`tmp/` 下的大多数 `.md` 是生成产物，不应作为开工前默认上下文。
