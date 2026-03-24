# Investment Agent Skill

这是仓库内投研命令体系的入口页，不是完整说明书。

默认不要把 `skill/commands/` 和 `skill/references/` 全部一起读完。先看这份，再按任务只打开一个命令说明或一份参考文档。

## 默认路由

1. 用户只会说自然语言
   先用 [assistant](./commands/assistant.md)
2. 用户明确要看市场主线或晨报
   看 [commands/briefing.md](./commands/briefing.md)
3. 用户明确要分析单个标的
   看 [commands/scan.md](./commands/scan.md)
4. 用户要找机会或扫主题
   看 [commands/discover.md](./commands/discover.md)
5. 其它命令和参考资料
   先看 [commands/README.md](./commands/README.md) 和 [references/README.md](./references/README.md)

## 默认工作方式

- 本地程序优先，联网补洞兜底
- 先给结论和动作，再按需展开证据
- `今天 / 现在 / 盘中` 默认把盘中快照和日线结论分开
- ETF / 基金先讲产品本身，再讲行业代理
- A 股数据默认 `Tushare first`

## Markdown 成稿闭环

只要当前任务会输出一份可独立阅读的研究型 Markdown 成稿，就默认走外审闭环：

- 第一版不是终稿
- 每份成稿都要单独走 `外部审稿 -> 修正 -> 再审`
- 不要停在“缺 external review 文件”的汇报；应先把 review 记录补出来，再继续推进到收敛或明确阻塞
- `final` 默认应是完整详细解释版，不是摘要版
- 值得长期采纳的外审意见，要同步沉淀到 [docs/history/report_review_lessons.md](../docs/history/report_review_lessons.md) 或对应门禁

这套规则见：

- [references/markdown-review-loop.md](./references/markdown-review-loop.md)
- [docs/prompts/README.md](../docs/prompts/README.md)

## A 股数据优先级

- A 股相关数据默认先走 `Tushare`
- 只有在确认无数据、权限不足或字段/单位仍不匹配时，才降级到 `AKShare / efinance / Yahoo`
- 常用接口分工、字段和单位校验，见 [references/tushare-a-share.md](./references/tushare-a-share.md)

## 最小命令集

| 命令 | 什么时候用 | 示例 |
| --- | --- | --- |
| `assistant <请求>` | 用户不会记命令或只会说自然语言 | `assistant 分析一下黄金ETF` |
| `briefing daily` | 看今天主线、行动和验证点 | `briefing daily --news-source Reuters` |
| `scan <代码>` | 深度分析单个标的 | `scan 561380` |
| `discover [主题]` | 找新机会或扫某个主题 | `discover 半导体` |
| `compare <代码...>` | 同类标的怎么选 | `compare 561380 512400` |
| `risk report` | 看组合风险和集中度 | `risk report` |

## 配置与降级

大多数场景通常只需要看：

- `config/config.yaml`
- `config/watchlist.yaml`
- `config/asset_aliases.yaml`

数据不全时默认按这个顺序降级：

1. 本地缓存
2. 备用源
3. 联网补查
4. 在最终输出里明确区分“程序输出”和“联网补充”

## 运行方式

```bash
python -m src.commands.<命令名> <参数>
```
