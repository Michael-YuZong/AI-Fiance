# External Review System Skill

你现在的任务不是“写一份 reviewer prompt”，而是把外审做成一套可复用的系统。

## 先记住这 6 条硬规则

1. 第一版产物不是终稿。
2. 每次正式交付前，必须先走 `Pass A 结构审 -> 修正 -> Pass B 发散审`。
3. `Pass A` 和 `Pass B` 必须由不同 reviewer / 子 agent 执行。
4. 每一轮都必须落结构化 review record，不允许只留聊天记录。
5. finding 不能只停在 review 文档里，必须分流到 `prompt / guard / tests / backlog` 至少一层。
6. 没有独立外审 PASS，`final` 不能放行。

## 你的工作顺序

1. 先读 [integration_blueprint.md](./integration_blueprint.md)
2. 再读 [migration_checklist.md](./migration_checklist.md)
3. 把 `prompts/` 和 `templates/` 搬进目标项目
4. 如果目标项目是 Python，优先搬 `python/` 里的参考实现
5. 在目标项目里接入：
   - review scaffold
   - review ledger
   - review audit
   - final gate
6. 最后才补该项目自己的领域检查项

## 你在目标项目里至少要交付什么

- 一套 reviewer prompt
- 一份 round-based review template
- 一个 review records 目录
- 一个 latest-by-series summary
- 一个 audit 命令
- 一个 final gate

## 何时算迁移完成

只有当下面都成立，才算“复现了同样机制”：

1. 目标项目生成正式产物时，缺 review 文件不会直接放行。
2. review 文件写成 `PASS` 但缺少 reviewer 分工、缺少收敛字段时，也不会放行。
3. ledger 能看出哪些 loop 还在 active。
4. audit 能指出哪些 review 记录本身不合格。
5. 目标项目团队能把新增 finding 继续固化进系统，而不是每次重新掉坑。

## 不要误做的事

- 不要只复制 `Pass A` prompt，不复制记录模板和 gate。
- 不要只做单轮 review 就宣布闭环完成。
- 不要让作者自己在同一上下文里同时完成结构审和发散审。
- 不要为了让 final 能出来，就绕过 gate。
