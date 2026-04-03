# Review Ledger Schema

这份 schema 描述的是外审记录被结构化索引后，最小应该有哪些字段。

## Record Fields

| field | meaning |
| --- | --- |
| `path` | 原始 review markdown 路径 |
| `series_id` | 同一条 review loop 的系列 ID，通常由文件名去掉 `_roundN` 得到 |
| `title` | 文档标题 |
| `protocol` | `structured_round / legacy_round_note / legacy_unstructured` |
| `round` | 当前轮次 |
| `previous_round` | 上一轮轮次 |
| `review_target` | 审稿对象展示值 |
| `review_target_ref` | 审稿对象链接目标 |
| `review_prompt` | reviewer prompt 展示值 |
| `review_prompt_ref` | reviewer prompt 链接目标 |
| `review_mode` | 审稿方式，例如 `Pass A -> Pass B` |
| `status` | `PASS / BLOCKED / ...` |
| `new_p0_p1` | 本轮是否有新增高优先级问题 |
| `previous_round_closed` | 上一轮高优先级问题是否关闭 |
| `converged` | 本轮是否收敛 |
| `recommend_continue` | 是否建议继续下一轮 |
| `allow_delivery` | 是否允许正式交付 |
| `allow_implementation` | 是否允许开始实现 |
| `sections` | 当前记录中出现的二级标题 |
| `metadata` | 顶部元数据原始映射 |
| `convergence` | `收敛结论` 区原始映射 |

## Summary Fields

| field | meaning |
| --- | --- |
| `total_records` | 全部 review markdown 数 |
| `total_series` | 去重后的 review loop 数 |
| `latest_pass_series` | 最新 round 状态为 `PASS` 的 loop 数 |
| `latest_blocked_series` | 最新 round 状态为 `BLOCKED` 的 loop 数 |
| `converged_series` | 最新 round 已收敛的 loop 数 |
| `active_series` | 仍需继续下一轮的 loop 数 |

## 默认 active 判定逻辑

一个 loop 属于 active，如果最新 round 满足任一条件：

- `status != PASS`
- `recommend_continue == 是`
- `converged == 否`

## 审计输出建议字段

如果项目继续做 review audit，建议派生下面字段：

- `severity`
- `category`
- `title`
- `detail`
- `path`
- `series_id`
- `round`
