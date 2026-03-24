# Config Map

默认不要把 `config/` 下面所有 YAML 都读一遍。

先看这份文件，按任务只打开相关配置。普通 patch 的目标是只读 `1` 到 `3` 个 YAML。

## 默认先看什么

大多数任务先看这两个：

- `config/config.yaml`
  本地实际生效配置
- `config/config.example.yaml`
  最小模板

只有在改技术参数、风险阈值、默认 profile 时，再看：

- `config/config.advanced.example.yaml`

## 按任务读哪些 YAML

| 任务 | 先读 | 只在必要时再读 |
| --- | --- | --- |
| 账户 / API / 默认路径 | `config/config.yaml` | `config/config.example.yaml`、`config/config.advanced.example.yaml` |
| `stock_pick / fund_pick / etf_pick` | `config/config.yaml`、`config/watchlist.yaml`、`config/stock_pools.yaml` | `config/catalyst_profiles.yaml`、特定 fast profile |
| `strategy replay / experiment` | `config/strategy_batches.yaml` | `config/watchlist.yaml`、`config/config.yaml` |
| `briefing / policy / news` | `config/news_feeds.yaml`、`config/event_calendar.yaml`、`config/market_monitors.yaml` | `config/market_overview.yaml` |
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

新闻 / 简报 / 市场监控：

- `config/news_feeds.yaml`
- `config/event_calendar.yaml`
- `config/market_monitors.yaml`
- `config/market_overview.yaml`

策略 / 风险 / 规则：

- `config/strategy_batches.yaml`
- `config/rules.yaml`
- `config/stress_scenarios.yaml`

pick / review / profile：

- `config/catalyst_profiles.yaml`
- `config/config.review.yaml`
- `config/config.stock_pick_fast.yaml`
- `config/config.etf_pick_fast.yaml`
- `config/config.fund_pick_proxy_news.yaml`
- `config/stock_pools_review.yaml`

## 默认不需要读的 YAML

下面这些通常只在专项任务里用到：

- `config/news_feeds.empty.yaml`
- `config/config.review.yaml`
- `config/config.stock_pick_fast.yaml`
- `config/config.etf_pick_fast.yaml`
- `config/config.fund_pick_proxy_news.yaml`
- `config/stock_pools_review.yaml`

## 目标

如果任务只是改一条主链路，理想状态下只需要打开 `1` 到 `3` 个 YAML，而不是把整个 `config/` 全部读进上下文。
