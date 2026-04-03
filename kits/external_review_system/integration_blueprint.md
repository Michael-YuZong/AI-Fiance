# 外审机制集成蓝图

这份蓝图只回答一件事：

这套外审机制最少由哪些层组成，接到别的项目里时应当怎么拆。

## 1. 系统骨架

完整形态分 6 层：

1. `artifact producer`
   你的项目本来就会产出某种正式交付物，例如报告、方案、PRD、分析稿、规范文档。
2. `review scaffold`
   当 review 记录不存在时，自动补出 round 1 scaffold，而不是直接报“缺 review 文件”。
3. `Pass A / Pass B`
   用两个 reviewer 做结构审和发散审。
4. `final gate`
   正式交付前检查 review 是否 PASS，协议字段是否完整。
5. `review ledger`
   把所有 review loop 做成结构化索引。
6. `review audit`
   反过来审“review 自己是否按协议完成”。

## 2. 角色分工

### 主作者

- 负责生成产物
- 负责接 reviewer finding 并修正
- 负责把合理 finding 固化到系统
- 不能自己兼任两轮 reviewer

### 结构审 reviewer

- 先抓硬问题
- 先看事实、结构、口径、执行、合同自洽
- 不负责自由发散

### 发散审 reviewer

- 专门抓 checklist 外问题
- 先做零提示审，再做逐段审
- 不重复机械跑结构审

## 3. 产物路径建议

你不必照搬路径名，但建议保留这 3 类目录：

- 正式产物目录
  例如 `reports/final/`
- review 目录
  例如 `reports/reviews/`
- manifest 目录
  可以和 review 放同目录，也可以并列

关键不是名字，而是：

- review 文件路径必须可由正式产物路径稳定映射出来
- manifest 路径也要能稳定映射出来

## 4. review 记录合同

每一轮 review 至少要有：

- 一句话总评
- 主要问题
- 独立答案
- 框架外问题
- 零提示发散审
- 建议沉淀
- 收敛结论

`收敛结论` 至少要能抽出：

- `round`
- `previous_round`
- `状态`
- `无新的 P0/P1`
- `本轮是否收敛`
- `是否建议继续下一轮`
- `允许作为成稿交付`
- `结构审执行者`
- `发散审执行者`

## 5. final gate 最小职责

最小 gate 不需要懂你的业务评分，但必须负责：

1. 找到对应 review 文件
2. 校验 review 文件是否存在
3. 校验必要章节和必要字段是否齐全
4. 校验 `Pass A / Pass B` 是否分离
5. 校验状态是否真的允许交付
6. 写 release manifest

更强的 gate 再追加：

- 正文是否是详细版
- 业务合同是否齐全
- manifest 与正文是否对齐
- domain-specific 规则

## 6. ledger 最小职责

ledger 不负责挡 final，它负责“看全局”。

至少要能回答：

- 总共有多少条 review record
- 总共有多少个 review loop
- 哪些 loop 最新状态是 PASS
- 哪些 loop 还在 active
- 哪些记录是 legacy 协议

## 7. audit 最小职责

audit 不负责业务结论对不对，它负责检查 review 协议有没有被执行到位。

最少要审：

- 缺少必需章节
- 缺少 `previous_round`
- 缺少 reviewer 分工
- reviewer 分工实际是同一个人
- PASS 但正文还有 actionable finding
- 上一轮问题没有在下一轮闭环

## 8. 如何挂领域钩子

这套 kit 故意不写死你的业务规则。

目标项目里应该这样扩展：

1. reviewer prompt 层
   把“本项目最容易出硬伤的事实和执行问题”补进结构审 prompt。
2. final gate 层
   把“哪些合同缺失时绝不能交付”做成 validator。
3. audit 层
   把“manifest 应该记录哪些项目专属合同”做成额外 audit hook。

不要把所有领域规则都塞回通用 parser。

## 9. 迁移完成后的 smoke test

至少跑这 5 步：

1. 生成一份正式产物
2. 确认系统自动补出 round 1 review scaffold
3. 手动把 review 改成不合格 PASS，确认 final gate 会拦住
4. 把 review 改成合格 PASS，确认 final gate 放行并写 manifest
5. 跑 ledger 和 audit，确认它们都能给出可读结果
