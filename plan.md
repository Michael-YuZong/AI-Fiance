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
| J | 强因子工程 | 进行中 | 先收口 `J-1` 价量结构与 setup，再推进 `J-2` 事件窗 |

## 当前主线

### 1. 强因子工程

先按因子家族推进，而不是零散补点：

- 价量结构
- 季节 / 日历
- breadth / chips
- 质量 / 盈利修正
- ETF / 基金专属因子

每个家族都要一起改：

- processor
- renderer
- action wording
- tests
- 外审

详细合同见 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)。

### 2. `strategy` fixture + governance

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

### 3. `policy` v2

主方向不是再堆模板，而是：

- 更稳的长文 / 扫描件抽取
- 更细的 taxonomy
- 更强的事实 / 推断 / 待确认分层

### 4. Proxy signals 收口

把 `social_sentiment / global_flow` 的：

- confidence
- limitation
- downgrade impact

完整传到：

- pick 输出
- release check
- report guard

### 5. `scheduler` v2

补基础设施而不是再加新任务类型：

- run history
- failure visibility
- durable state
- 需要时再接 automation

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

## 完成定义

某项能力要被视为“完成”，至少要满足：

- 有清晰 CLI 或工作流入口
- 输出结构稳定
- 降级路径明确
- 有最小必要测试
- 接入外审
- 已与上下游模块打通
- 能说清“什么时候可信，什么时候只作参考”

## 详细文档入口

- 默认任务读法：[docs/context_map.md](./docs/context_map.md)
- 当前状态与最近变化：[docs/status_snapshot.md](./docs/status_snapshot.md)
- YAML 地图：[config/README.md](./config/README.md)
- 强因子专题：[docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
