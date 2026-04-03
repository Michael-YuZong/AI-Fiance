# External Review System Kit

这是一份可移植的外审机制打包件。

目标不是复制当前仓库的金融业务逻辑，而是把下面这套机制搬到别的项目里：

- 双 reviewer 分阶段外审
- round-based 收敛记录
- `PASS / BLOCKED` 明确状态机
- `final` 导出前的独立外审门禁
- review ledger 索引
- review audit 治理审计
- finding 必须沉淀到 `prompt / guard / tests / backlog` 至少一层

## 给别的 Agent 的默认读法

1. [SKILL.md](./SKILL.md)
2. [integration_blueprint.md](./integration_blueprint.md)
3. [migration_checklist.md](./migration_checklist.md)
4. `prompts/`
5. `templates/`
6. `python/`

## 这包里有什么

- `SKILL.md`
  面向另一个 Agent 的接手说明，先讲不可打破的机制，再讲落地顺序。
- `integration_blueprint.md`
  把“这套机制到底由哪些层组成”讲清楚。
- `migration_checklist.md`
  按阶段迁移时的核对清单。
- `prompts/`
  通用版 `Pass A / Pass B / convergence loop / revision loop` prompt。
- `templates/`
  通用 review record 模板和 ledger schema。
- `python/`
  一套可直接抄到 Python 项目里的参考实现：
  - `review_record_utils.py`
  - `review_scaffold.py`
  - `review_ledger.py`
  - `review_audit.py`
  - `final_gate.py`
  - CLI wrapper

## 这套机制的最小闭环

如果你只想先复现“同样的外审能力”，最少要搬下面 5 件事：

1. 双 reviewer 协议
   `Pass A 结构审` 和 `Pass B 发散审` 必须分离，而且执行者不能是同一个 reviewer / 子 agent。
2. round 记录
   每轮都要落成结构化 review markdown，而不是只留聊天记录。
3. 收敛规则
   停止条件必须是 round-based 收敛，不是“reviewer 说差不多了”。
4. final gate
   正式输出不能绕过独立外审 PASS。
5. ledger + audit
   需要能看出哪些 loop 还没收口，哪些记录虽然写成 PASS 但协议没满足。

## 推荐迁移顺序

1. 先搬 `prompts/` 和 `templates/`
2. 再把 `python/review_*` 搬到目标项目
3. 再把目标项目自己的 `final` 写入链路接到 `python/final_gate.py`
4. 最后补目标项目专属的 domain checks

## 不要直接照搬的部分

- 不要把“金融”两个字换掉就算迁移完成
- 不要把目标项目特有的事实核验项写死在这套通用 prompt 里
- 不要把项目专属规则塞回通用 parser；专属规则应挂在目标项目自己的 validator / audit hook 上

## 自检标准

迁完后，至少要能证明：

1. 目标项目能自动生成 round 1 review scaffold
2. reviewer 记录能被 ledger 正确解析
3. audit 能发现缺少 reviewer 分工、缺少闭环、PASS 但正文还有 actionable finding
4. final 导出在 review 未 PASS 时会被挡住
5. final 导出在 review PASS 后能写 manifest
