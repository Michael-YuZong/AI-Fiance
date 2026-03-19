# AI-Finance 路线图

这份文件只保留路线图总览和当前优先级。

默认不要把它当成长篇实现说明来读。
如果任务只涉及单个命令，先看 [docs/context_map.md](./docs/context_map.md)；如果任务只涉及 `strategy`，直接看 [docs/plans/strategy.md](./docs/plans/strategy.md)。

## 北极星

目标不是“功能越来越多”，而是让系统对真实投资问题形成完整闭环：

1. 识别问题
2. 拉取当时可见证据
3. 形成研究判断
4. 映射到组合和风险预算
5. 产出可交付结果
6. 接入外审
7. 进入监控、复盘、归因和策略学习

## 当前阶段表

| 阶段 | 主题 | 当前状态 | 下一步 |
| --- | --- | --- | --- |
| A | `research` 入口化 | 已完成主版本 | 保持和 pick / portfolio 合同同步 |
| B | 代理信号升级 | 已完成第一轮 | 收口到 pick / guard 全链路 |
| C | `policy` 升级 | 部分完成 | 深挖扫描版、表格重 PDF/OFD 和 taxonomy |
| D | 组合构建与风险预算 | 已完成 v1 | 往更多推荐链路扩 |
| E | 时点正确性与证据溯源 | 已完成 v1 | 补 fixtures 和更严格 point-in-time 覆盖 |
| F | 评分校准、归因、自学习 | 已完成 v1 | 深化 setup bucket 和长期学习闭环 |
| G | 执行成本与可成交性 | 已完成 v1 | 扩到更多 pick / release 场景 |
| H | 调度与运营闭环 | 仍是 v1 | 做持久化 run history、失败可见性、运维状态 |
| I | `strategy` 研究层 | 已完成第一版闭环 | 做 fixture + governance，再扩横截面验证 |
| J | 强因子工程 | 已完成 v1 收口 | 进入维护；剩余 point-in-time / lag / calibration 归入 E / F / I |
| K | 外审能力扩展 | 已启动 v1 | 已有 structured-round ledger + audit；下一步扩证据与时点专项审计 |

## 当前主线

### 1. `strategy` fixture + governance

`strategy` 现在已经有：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

但仍缺：

- lag / visibility fixture
- overlap / benchmark fixture
- champion-challenger promotion gate
- rollback gate
- 更严格的 out-of-sample / cohort / cross-sectional validate

详细合同见 [docs/plans/strategy.md](./docs/plans/strategy.md)。

### 2. `policy` v2

主方向不是再堆模板，而是：

- 更稳的长文 / 扫描件抽取
- 更细的 taxonomy
- 更强的事实 / 推断 / 待确认分层

### 3. Proxy signals 收口

把 `social_sentiment / global_flow` 的：

- confidence
- limitation
- downgrade impact

完整传到：

- pick 输出
- release check
- report guard

### 4. `scheduler` v2

补基础设施而不是再加新任务类型：

- run history
- failure visibility
- durable state
- 需要时再接 automation

### 5. 外审能力扩展

外审不该只靠 reviewer prompt，还需要专门的外审治理审计器。
当前口径统一为：

- `review_ledger`
  负责索引所有外审记录，包括旧式 review 文档
- `review_audit`
  只审当前 `structured-round` 协议，不把历史旧模板误当成 active blocker

第一批先做：

- review consistency audit
  - 审 `round / previous_round / status / continue` 是否自洽
  - 审同一 review series 的 target / prompt 是否漂移
- solidification audit
  - 审 finding 有没有真正沉淀到：
    - prompt
    - hard rule / guard / workflow
    - tests / fixtures
    - lesson / backlog

后续再做：

- evidence audit
- point-in-time audit
- regression diff audit
- experiment statistics audit
- attribution audit

外审能力扩展默认同时承担一层代码质量优化：

- 优先抽共享 helper
- 避免 parser / audit / guard 各自复制合同解析逻辑
- 新规则先复用现有 ledger / template / schema，不要平行再造一套

### 6. 校准与学习

深化已经进入产品层的 setup / breadth / 质量因子，不再把“继续加新因子”当主线：

- setup bucket 复盘
- 因子阈值再校准
- 长期月度学习闭环

### 已收口专题：强因子工程

阶段 J 现在按 `v1 已收口` 管理，不再作为当前主开发主线。当前结案边界是：

- J-1 ~ J-5 已完成第一次 family-level 收口
- 因子已进入 processor / renderer / action wording / tests
- `review_audit` 当前对 `structured-round` 外审协议审计为 `0 active findings`
- 后续同类问题进入日常 today final / 外审节奏，不再单独挂成强因子开发 blocker

剩余长尾不再归入阶段 J 主开发：

- `J-4 EPS 修正` 的可靠 point-in-time 源接入 -> 归到阶段 E / I
- `J-2 政策事件窗` 的 lag / visibility fixture -> 归到阶段 E / I
- setup / breadth / 质量阈值再校准 -> 归到阶段 F

详细合同见 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)。

## 外审规则

所有功能都按统一外审协议走：

1. 合同审
2. 发散审
3. round-based 收敛
4. 合理 finding 固化到：
   - prompt
   - 硬规则 / guard
   - tests / fixtures
   - lesson / backlog

收敛条件：

- 连续两轮无新增 P0/P1
- 上一轮阻塞已关闭或降级
- 没有新的实质性发散问题
- 合理 finding 已完成固化分流

常用 prompt：

- `docs/prompts/external_financial_reviewer.md`
- `docs/prompts/external_research_reviewer.md`
- `docs/prompts/external_review_convergence_loop.md`
- `docs/prompts/external_strategy_plan_reviewer.md`
- `docs/prompts/external_factor_plan_reviewer.md`
- 可迁移 kit：`docs/review_kit/README.md`
- ledger/index：`python -m src.commands.review_ledger`
- governance audit：`python -m src.commands.review_audit`

## 完成定义

某项能力要被视为“完成”，至少要满足：

- 有清晰 CLI 或工作流入口
- 输出结构稳定
- 降级路径明确
- 有最小必要测试
- 接入外审
- 已与上下游模块打通
- 能说清“什么时候可信，什么时候只作参考”

## 默认快路径

以后默认按快路径推进新功能，避免每个 patch 都拉长链路：

- `patch-level`
  - 真实复现
  - 局部修复
  - narrow tests
  - 真实 spot check
  - 默认不跑 today final / 外审
- `family-level`
  - 在 patch 已稳定成组后，再跑 today final
  - 再接 `release_check / report_guard / 外审`
- `stage-level`
  - 只在专题真正收口或治理边界变化时，做 lesson / audit / backlog / 文档固化

详细规则见 [docs/process/feature_fast_loop.md](./docs/process/feature_fast_loop.md)。

## 详细文档入口

- 默认任务读法：[docs/context_map.md](./docs/context_map.md)
- 当前状态与最近变化：[docs/status_snapshot.md](./docs/status_snapshot.md)
- YAML 地图：[config/README.md](./config/README.md)
- 强因子专题：[docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
