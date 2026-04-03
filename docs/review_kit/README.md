# External Review Kit

这份文件是 `review_kit` 的入口页，不是完整说明书。

默认不要把本目录所有文档一起读完。先看这份，再按任务只打开一到两份模板。

## 这套 kit 是干什么的

它不是“再给一个 reviewer prompt”，而是把外审做成一套可迁移的系统：

- 外审按轮次收敛，不是一轮点评
- finding 不能只留在评论里，要沉淀到 prompt / rule / test / backlog
- review 记录要能被索引、追踪、审计

如果你要把这套能力交给别的项目或别的 Agent 直接复现，优先看仓库里的可移植打包件：

- [kits/external_review_system/README.md](../../kits/external_review_system/README.md)
- [kits/external_review_system/SKILL.md](../../kits/external_review_system/SKILL.md)

也可以直接导出一份独立目录：

```bash
python -m src.commands.export_review_kit --out tmp/external_review_system_kit --archive
```

## 默认读法

1. 先看 [docs/prompts/README.md](../prompts/README.md)
2. 再看这份 [README.md](./README.md)
3. 然后只按任务打开下面之一：
   - 审稿记录模板：[review_record_template.md](./review_record_template.md)
   - ledger 字段定义：[review_ledger_schema.md](./review_ledger_schema.md)
   - 迁移清单：[migration_checklist.md](./migration_checklist.md)

## 本目录每份文件做什么

| 文件 | 用途 | 什么时候看 |
| --- | --- | --- |
| [review_record_template.md](./review_record_template.md) | 每轮外审记录模板 | 要落 round 记录时 |
| [review_ledger_schema.md](./review_ledger_schema.md) | review 记录字段和语义 | 要写 parser / audit / 校验时 |
| [migration_checklist.md](./migration_checklist.md) | 把这套外审流程迁到别的项目 | 要复制这套能力时 |

## 最小使用路径

在本仓库里，最小闭环通常是：

1. 选 reviewer prompt
   先看 [docs/prompts/README.md](../prompts/README.md)
   正式研究型 Markdown 成稿默认是 `Pass A 结构审 + Pass B 发散审` 两段，不是一个 reviewer 一把做完。
2. 产出 round 1 记录
   用 [review_record_template.md](./review_record_template.md)
3. 跑 round-based 收敛
   配合 `external_review_convergence_loop.md`
4. 建索引 / 做治理审计
   用 `review_ledger` 和 `review_audit`

## 本仓库常用命令

```bash
python -m src.commands.review_ledger
python -m src.commands.review_ledger --json-out reports/reviews/review_ledger_index.json --markdown-out reports/reviews/review_ledger_summary.md
python -m src.commands.review_audit
```

默认扫描：

- `reports/reviews/*.md`

## 什么时候不需要看这份

- 只是改单个业务 command
- 只是修 renderer / processor bug
- 只是跑一轮具体 reviewer prompt，而不是搭外审流程

## 迁移时别做的事

- 不要只复制 reviewer prompt，不复制记录模板和 schema
- 不要只做单轮外审就宣布收敛
- 不要让 finding 只留在 review 文档里
- 不要没有 ledger / audit 就声称“治理闭环已完成”
