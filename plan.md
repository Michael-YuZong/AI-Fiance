# AI-Finance 路线图

这份文件只保留路线图总览和当前优先级。

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
| B | 代理信号升级 | 已完成第一轮 | 继续扩到更多 final / audit |
| C | `policy` 升级 | 部分完成 | 深挖扫描版、表格重 PDF/OFD 和 taxonomy |
| D | 组合构建与风险预算 | 已完成 v1 | 往更多推荐链路扩 |
| E | 时点正确性与证据溯源 | 已完成 v1 | 补 fixtures 和更严格 point-in-time 覆盖 |
| F | 评分校准、归因、自学习 | 已完成 v1 | 深化 setup bucket 和长期学习闭环 |
| G | 执行成本与可成交性 | 已完成 v1 | 扩到更多 pick / release 场景 |
| H | 调度与运营闭环 | 仍是 v1 | 做持久化 run history、失败可见性、运维状态 |
| I | `strategy` 研究层 | 已完成第一版闭环 | 做 fixture + governance，再扩横截面验证 |
| J | 强因子工程 | 已完成 v1 收口 | 进入维护；剩余 point-in-time / lag / calibration 归入 E / F / I |
| K | 外审能力扩展 | 已启动 v1 | 扩证据、时点、回归和归因专项审计 |

## 当前主线

### 1. `strategy` fixture + governance

已实现：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

当前下一步：

- 更长窗口和更多日期上的 promotion calibration / external review

详细合同见 [docs/plans/strategy.md](./docs/plans/strategy.md)。

### 2. `policy` v2

主方向：

- 更稳的长文 / 扫描件抽取
- 更细的 taxonomy
- 更强的事实 / 推断 / 待确认分层

### 3. proxy signals 收口

继续把 `social_sentiment / global_flow` 的：

- confidence
- limitation
- downgrade impact

统一传到：

- pick 输出
- final manifest
- `review_audit`
- 更多需要 point-in-time 披露的回溯链路

### 4. `scheduler` v2

补基础设施而不是再加新任务类型：

- run history
- failure visibility
- durable state

### 5. 外审能力扩展

外审默认要同时过：

1. 合同审
2. 发散审
3. round-based 收敛
4. finding 固化到 prompt / rule / test / backlog 至少一层

主线继续围绕：

- evidence audit
- point-in-time audit
- regression diff audit
- experiment statistics audit
- attribution audit

### 6. 校准与学习

当前重点不是继续堆新因子，而是：

- setup bucket 复盘
- 因子阈值再校准
- 长期月度学习闭环

### 已收口专题：强因子工程

阶段 J 已按 `v1 已收口` 管理，不再作为主开发主线。

剩余长尾迁移如下：

- `J-4 EPS 修正` 的可靠 point-in-time 源接入 -> 阶段 E / I
- `J-2 政策事件窗` 的 lag / visibility fixture -> 阶段 E / I
- setup / breadth / 质量阈值再校准 -> 阶段 F

详细合同见 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)。

## 默认快路径

- `patch-level`
  - 真实复现
  - 局部修复
  - narrow tests
  - 真实 spot check
- `family-level`
  - patch 成组后再跑 today final
  - 再接 `release_check / report_guard / 外审`
- `stage-level`
  - 专题真正收口时再做 lesson / audit / backlog / 文档固化

详细规则见 [docs/process/feature_fast_loop.md](./docs/process/feature_fast_loop.md)。

## 详细文档入口

- 默认任务读法：[docs/context_map.md](./docs/context_map.md)
- 当前状态与 backlog：[docs/status_snapshot.md](./docs/status_snapshot.md)
- YAML 地图：[config/README.md](./config/README.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子专题：[docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 详细变更归档：[docs/history/2026-03.md](./docs/history/2026-03.md)
