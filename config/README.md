# Config Map

默认不要把 `config/` 下面所有 YAML 都读一遍。

如果任务不是配置相关，理想状态下：

- `0` 个 YAML
- 最多只读 `1` 个入口 YAML 作确认

如果任务是配置相关，再按下面的最小集合去读。

## 默认先看什么

大多数配置任务先看：

- `config/config.yaml`
- `config/config.example.yaml`

只有在改高级参数、默认 profile、运行时降级时，再看：

- `config/config.advanced.example.yaml`

## 按任务读哪些 YAML

| 任务 | 默认只读 | 只在必要时再读 |
| --- | --- | --- |
| 账户 / API / 默认路径 | `config/config.yaml` | `config/config.example.yaml`、`config/config.advanced.example.yaml` |
| `stock_pick / fund_pick / etf_pick` | `config/config.yaml`、`config/watchlist.yaml`、`config/stock_pools.yaml` | `config/catalyst_profiles.yaml`、特定 fast profile |
| `strategy replay / experiment` | `config/strategy_batches.yaml` | `config/watchlist.yaml`、`config/config.yaml` |
| `briefing / policy / intelligence` | `config/news_feeds.yaml`、`config/event_calendar.yaml`、`config/market_monitors.yaml` | `config/market_overview.yaml` |
| `lookup / assistant` | `config/asset_aliases.yaml` | 无 |
| 风险 / 回测 | `config/rules.yaml`、`config/stress_scenarios.yaml` | `config/config.review.yaml` |
| review / guard / 降级测试 | `config/config.review.yaml` | `config/news_feeds.empty.yaml`、`config/stock_pools_review.yaml` |

## 常见 YAML 分组

主配置：

- `config/config.yaml`
- `config/config.example.yaml`
- `config/config.advanced.example.yaml`

观察池 / 候选池 / 别名：

- `config/watchlist.yaml`
- `config/stock_pools.yaml`
- `config/asset_aliases.yaml`
- `config/strategy_batches.yaml`

情报 / 简报 / 市场监控：

- `config/news_feeds.yaml`
- `config/event_calendar.yaml`
- `config/market_monitors.yaml`
- `config/market_overview.yaml`

策略 / 风险 / 规则：

- `config/strategy_batches.yaml`
- `config/rules.yaml`
- `config/stress_scenarios.yaml`

review / profile：

- `config/catalyst_profiles.yaml`
- `config/config.review.yaml`
- `config/config.stock_pick_fast.yaml`
- `config/config.etf_pick_fast.yaml`
- `config/config.fund_pick_proxy_news.yaml`
- `config/stock_pools_review.yaml`

说明：
- `config/config.stock_pick_fast.yaml` 现在不只是轻量新闻源，还会同步收窄候选池、并发和慢链开关；显式传这个 profile 时，应按 stock_pick 快路径合同运行。
- 即使 `config/config.stock_pick_fast.yaml` 把全局新闻压成轻量模式，个股级 `get_stock_news()` 里的结构化情报与 `e互动` 仍然保留，不应被误关掉。
- `config/config.etf_pick_fast.yaml` 现在默认保留 `ETF light fund_profile`，不会再通过 `skip_fund_profile` 把 `跟踪指数 / 份额变化 / 场内基金技术状态` 整条产品层静默关掉；这份 profile 只继续收紧外部情报和跨市场慢链。

## 默认不要读的 YAML

下面这些通常只在专项任务里再打开：

- `config/news_feeds.empty.yaml`
- `config/config.review.yaml`
- `config/config.stock_pick_fast.yaml`
- `config/config.etf_pick_fast.yaml`
- `config/config.fund_pick_proxy_news.yaml`
- `config/stock_pools_review.yaml`

## 目标

如果任务只是改一条主链路，理想状态下只需要打开 `0` 到 `3` 个 YAML，而不是把整个 `config/` 全部读进上下文。
