# Investment Agent Skill

## 这是什么

个人投资决策辅助工具库，默认走“本地程序优先，联网补洞兜底”。

如果用户直接说自然语言需求，优先用 `assistant` 做路由；只有在用户明确指定命令或你已经确定场景时，才直接调用具体命令。

## 默认工作方式

1. 先判断用户是在要 `晨报 / 单标的分析 / 机会发现 / 对比 / 风险`
2. 先跑本地命令和缓存，再试备用数据源
3. 关键事实仍缺失时允许联网补查
4. 默认先给结论和动作，再按需展开证据和附录

## 最小命令集

| 命令 | 什么时候用 | 示例 |
| --- | --- | --- |
| `assistant <请求>` | 用户不会记命令，或只会说自然语言 | `assistant 分析一下黄金ETF` |
| `briefing daily` | 看今天主线、行动和验证点 | `briefing daily --news-source Reuters` |
| `scan <代码>` | 深度分析单个标的 | `scan 561380` |
| `discover [主题]` | 找新机会或扫某个主题 | `discover 半导体` |
| `compare <代码...>` | 同类标的怎么选 | `compare 561380 512400` |
| `risk report` | 看组合风险和集中度 | `risk report` |

其余命令如 `portfolio / backtest / policy / regime / research / snap` 属于第二层，需要时再打开对应文档。

## 渐进式披露

### 第一层：大多数时候只需要这些

- [assistant](commands/assistant.md)
- [briefing](commands/briefing.md)
- [scan](commands/scan.md)
- [discover](commands/discover.md)

### 第二层：用户明确提出时再展开

- `compare`、`risk`、`portfolio`、`backtest`、`policy`、`regime`、`research`、`snap`

### 第三层：只有正式成稿或校验时才看

- [晨报高级规则](references/briefing-quality.md)

## 配置也按两层处理

大多数用户通常只需要改这 3 个文件：

- `config/config.yaml`
- `config/watchlist.yaml`
- `config/asset_aliases.yaml`

低频调参和高级源配置放在这些文件里，通常不用碰：

- `config/config.advanced.example.yaml`
- `config/news_feeds.yaml`
- `config/catalyst_profiles.yaml`
- `config/market_monitors.yaml`
- `config/market_overview.yaml`
- `config/event_calendar.yaml`

## 失败时怎么降级

当本地命令数据不全时，按这个顺序继续：

1. 本地缓存
2. 备用源
3. 联网补查
4. 在最终输出里明确区分“程序输出”和“联网补充”

联网时优先官方、交易所、一级媒体、央行、指数公司、基金公司和主流行情源。

## 运行方式

```bash
python -m src.commands.<命令名> <参数>
```
