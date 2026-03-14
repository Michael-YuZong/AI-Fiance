# Config Map

默认不要把 `config/` 下面所有 YAML 都读一遍。

先看这份文件，按任务只打开相关配置。

## 第一步：大多数任务只看这两个

- `config/config.yaml`
  本地实际生效配置
- `config/config.example.yaml`
  最小模板

## 按用途读哪些 YAML

### 账户 / API / 调度 / 默认路径

- `config/config.yaml`
- `config/config.example.yaml`
- `config/config.advanced.example.yaml`
  只有要改技术参数、风险阈值、扫描门槛时再看

### 观察池 / 别名 / 候选池

- `config/watchlist.yaml`
  常用观察池
- `config/stock_pools.yaml`
  `stock_pick` 的港股 / 美股候选池
- `config/asset_aliases.yaml`
  `lookup / assistant` 的中文别名映射

### 新闻 / 简报 / 市场监控

- `config/news_feeds.yaml`
  主新闻源
- `config/news_feeds.empty.yaml`
  只有做极限降级测试时才看
- `config/event_calendar.yaml`
  事件日历
- `config/market_monitors.yaml`
  宏观资产监控对象
- `config/market_overview.yaml`
  市场概览展示配置

### 风险 / 回测 / 压力测试

- `config/stress_scenarios.yaml`
  压力测试场景
- `config/rules.yaml`
  回测 / 规则策略

### pick / 催化 / review 专用

- `config/catalyst_profiles.yaml`
  催化画像
- `config/config.review.yaml`
  review / guard 相关配置
- `config/config.etf_pick_fast.yaml`
  ETF pick 的快速 profile
- `config/config.fund_pick_proxy_news.yaml`
  基金 pick 的代理新闻 profile
- `config/stock_pools_review.yaml`
  review 用候选池

## 默认不需要读的 YAML

下面这些不是大多数任务的入口：

- `config/news_feeds.empty.yaml`
- `config/config.review.yaml`
- `config/config.etf_pick_fast.yaml`
- `config/config.fund_pick_proxy_news.yaml`
- `config/stock_pools_review.yaml`

只有在做：

- review / guard
- profile 切换
- 降级测试
- speed profile

时再打开。

## 推荐读法

### 修 `scan / research / risk`

先读：

- `config/config.yaml`

### 修 `stock_pick / fund_pick / etf_pick`

先读：

- `config/config.yaml`
- `config/watchlist.yaml`
- `config/stock_pools.yaml`

必要时再读：

- `config/catalyst_profiles.yaml`
- 特定 pick profile

### 修 `briefing / policy / news`

先读：

- `config/news_feeds.yaml`
- `config/event_calendar.yaml`
- `config/market_monitors.yaml`

### 修 `lookup / assistant`

先读：

- `config/asset_aliases.yaml`

## 目标

如果一个任务只是改一个主链路，理想状态下只需要打开 `1` 到 `3` 个 YAML，而不是把整个 `config/` 全部读进上下文。
