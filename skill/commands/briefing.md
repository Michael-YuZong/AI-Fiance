# /briefing

## 输入

- `daily` 或 `weekly`
- 可选 `--news-source <来源>`，例如 `Reuters`、`Bloomberg`

## 执行流程

1. 汇总市场、宏观、持仓和事件数据
2. 若本地新闻或行情源不足，先用代理指标补齐主线；必要时允许联网补新闻和事件
3. 生成结构化简报

## 输出

- 晨报或周报 Markdown
