# External Review Kit

这套 kit 的目标不是“再给一个 reviewer prompt”，而是把外审变成一套可迁移的系统。

适合复制到别的项目里的最小组成：

1. `docs/prompts/`
   - reviewer prompt
   - convergence loop prompt
   - revision loop prompt
2. `docs/review_kit/review_record_template.md`
3. `docs/review_kit/review_ledger_schema.md`
4. `docs/review_kit/migration_checklist.md`
5. `src/reporting/review_ledger.py`
6. 可选：`src/commands/review_ledger.py`
7. 可选：`src/reporting/review_audit.py` / `src/commands/review_audit.py`

## 这套 kit 解决什么问题

- 外审不是一次性点评，而是 round-based 收敛循环
- 发散审找到的问题，不允许只停在评论里
- finding 要能沉淀成 prompt / hard rule / tests / backlog
- review 记录要能被索引、统计、追踪
- 换项目后不需要先重新发明流程

## 最小落地顺序

1. 先复制 prompt 和本目录下的模板文档
2. 在新项目里选一个最小审稿对象
   - 一份报告
   - 一个计划
   - 一段研究输出
3. 用 [review_record_template.md](./review_record_template.md) 产出 round 1 审稿记录
4. 用 `external_review_convergence_loop.md` 继续跑 round 2, round 3
5. 把合理 finding 固化到：
   - prompt
   - hard rule / guard / workflow
   - tests / fixtures
   - lesson / backlog
6. 用 `review_ledger.py` 建索引，确认哪些 loop 还没收敛
7. 用 `review_audit.py` 审当前 `structured-round` 合同和沉淀去向是否真的成立

## 在本仓库怎么用

```bash
python -m src.commands.review_ledger
python -m src.commands.review_ledger --json-out reports/reviews/review_ledger_index.json --markdown-out reports/reviews/review_ledger_summary.md
python -m src.commands.review_audit
```

默认扫描：

- `reports/reviews/*.md`

输出内容：

- 当前有多少 review records
- 有多少条 review series
- 哪些 series 已收敛
- 哪些 series 还在 active loop
- 哪些 prompt 正在被使用
- 哪些当前 `structured-round` review records 在 round 合同或沉淀去向上有问题

## 可迁移时哪些是项目无关的

这些几乎可以直接复制：

- round-based 收敛规则
- 发散审必须单列
- 合理 finding 必须固化
- review record 模板
- ledger schema
- ledger parser / indexer
- review governance audit（默认只审结构化 round 协议）

## 哪些需要按新项目适配

- 审稿对象的命名
- `PASS / BLOCKED / allow delivery` 的门禁语义
- 哪些 finding 应该进 code guard，哪些进测试
- reviewer prompt 里的领域知识

## 推荐的目录结构

```text
docs/prompts/
docs/review_kit/
reports/reviews/
src/reporting/review_ledger.py
src/commands/review_ledger.py
src/reporting/review_audit.py
src/commands/review_audit.py
```

## 迁移时不要做的事

- 不要只复制 prompt，不复制模板和固化规则
- 不要把外审当成一次性点评
- 不要让 finding 只留在 review 文档里
- 不要在没有 ledger/index 的情况下宣称“已经收敛”
