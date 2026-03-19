# 外审能力迁移清单

把这套能力搬到别的项目时，按这个顺序做。

## 第一层：先搬规则

- [ ] 搬 `external_review_convergence_loop.md`
- [ ] 搬目标 reviewer prompt
- [ ] 搬 `review_record_template.md`
- [ ] 搬 `review_ledger_schema.md`
- [ ] 在新项目里明确“什么叫 PASS / BLOCKED”

## 第二层：再搬流程

- [ ] 明确每轮都必须有记录
- [ ] 明确必须做合同审 + 发散审
- [ ] 明确合理 finding 必须固化到 prompt / guard / tests / backlog 至少一层
- [ ] 明确停止条件不是“reviewer 说看起来没问题”，而是达到收敛条件

## 第三层：再搬工具

- [ ] 搬 `src/reporting/review_ledger.py`
- [ ] 可选搬 `src/commands/review_ledger.py`
- [ ] 约定 review records 目录
- [ ] 至少能输出一份 latest-by-series summary

## 第四层：最后才搬项目专属 guard

- [ ] 新项目是否需要 `report_guard` 一类 final gate
- [ ] 新项目是否需要 release consistency check
- [ ] 哪些 finding 要变成 hard rule
- [ ] 哪些 finding 只应保留在 lesson/backlog

## 迁移后第一轮自检

- [ ] 能不能跑出 round 1 review record
- [ ] 能不能跑到 round 2
- [ ] round 2 的 finding 能不能沉淀进系统
- [ ] ledger 能不能看出这条 loop 还没收敛
- [ ] 收敛后能不能明确写出停止理由
