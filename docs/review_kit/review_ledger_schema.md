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
| `review_target` | 审稿对象的展示值 |
| `review_target_ref` | 审稿对象链接目标 |
| `review_prompt` | reviewer prompt 展示值 |
| `review_prompt_ref` | reviewer prompt 链接目标 |
| `review_mode` | 审稿方式，例如 `合同审 + 发散审` |
| `decision` | `结论` 区的主判断 |
| `status` | `PASS / BLOCKED / ...` |
| `new_p0_p1` | 本轮是否有新增高优先级问题 |
| `previous_round_closed` | 上一轮高优先级问题是否关闭 |
| `converged` | 本轮是否收敛 |
| `recommend_continue` | 是否建议继续下一轮 |
| `allow_delivery` | 是否允许成稿交付 |
| `allow_implementation` | 是否允许开始实现 |
| `sections` | 当前记录中出现的二级标题 |
| `metadata` | 顶部元数据原始映射 |
| `convergence` | `收敛结论` 区原始映射 |

## Ledger Summary Fields

| field | meaning |
| --- | --- |
| `total_records` | 全部 review markdown 数 |
| `total_series` | 去重后的 review loop 数 |
| `latest_pass_series` | 最新 round 状态为 `PASS` 的 loop 数 |
| `latest_blocked_series` | 最新 round 状态为 `BLOCKED` 的 loop 数 |
| `converged_series` | 最新 round 已收敛的 loop 数 |
| `active_series` | 仍需继续下一轮的 loop 数 |
| `legacy_round_note_series` | 旧式带 round 但未采用结构化收敛协议的 loop 数 |
| `legacy_unstructured_series` | 完全旧式的非结构化 review loop 数 |

## 建议判定逻辑

默认一个 loop 属于 active，如果最新 round 满足任一条件：

- `status != PASS`
- `recommend_continue == 是`
- `converged == 否`

## 迁移建议

- 新项目不一定要完全保留这些字段名
- 但至少应保留：
  - `series_id`
  - `round`
  - `status`
  - `new_p0_p1`
  - `converged`
  - `recommend_continue`
  - `review_target`
  - `review_prompt`

没有这些字段，就很难做真正的收敛追踪

## 审计扩展

如果项目要继续做外审治理审计，建议再基于 ledger 派生一层 audit 输出，至少包含：

- `severity`
- `category`
- `title`
- `detail`
- `path`
- `series_id`
- `round`

这样可以继续审：

- round 合同是否完整
- 同一 series 的 target / prompt / previous_round 是否漂移
- actionable finding 是否真的沉淀到 prompt / guard / tests / backlog

建议默认只对 `protocol=structured_round` 的记录做 audit，不把旧式 review 文档误判成当前协议的 blocker。
