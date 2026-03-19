# 功能开发快路径

这份文档只回答一个问题：

**如何把“开发一个新功能 -> 审查 -> 收口”的默认成本压下来。**

目标不是降低质量，而是避免每次都把：

- 全仓库上下文
- today final
- round-based 外审
- 宽测试

全部一起跑，导致 token、时间和注意力都被稀释。

## 默认原则

1. 一次只推进一个最小合同
   不要把 “因子计算 + 渲染 + pick + final + 外审 + strategy” 混成同一轮 patch。
2. patch-level 默认不跑 today final / 外审
   只要功能还在局部开发阶段，就先停在：
   - 真实复现
   - 局部修复
   - 窄测试
   - 真实 spot check
3. 只有达到 family-level，才重跑 today final
   也就是一组相关 patch 已经收拢成稳定合同，才值得跑：
   - today final
   - release_check / report_guard
   - round-based 外审
4. 输出只保留最小证据包
   每一轮默认只交：
   - 改了什么
   - 跑了什么
   - 真实 spot check 看到了什么
   - 还有什么没做

## 三层完成定义

### 1. Patch-level

适用场景：

- 单个因子
- 单个 renderer 合同
- 单个 guard 规则
- 单个 collector/fallback 修复

必须完成：

- 一个真实复现实例
- 一次定向修复
- 一组 narrow tests
- 一次真实命令 spot check

默认不做：

- today final
- 外审循环
- 宽回归

### 2. Family-level

适用场景：

- 一组同类因子完成第一轮收口
- 一个完整分析维度改完
- 一个 pick/report 合同明显变化

必须完成：

- patch-level 全部要求
- 相关 narrow regression
- 至少一条 today final
- release_check / report_guard
- round-based 外审收敛

### 3. Stage-level

适用场景：

- 一个专题明确准备切出主开发线
- backlog、成熟度、交接方式改变

必须完成：

- family-level 已完成
- lesson / audit / backlog 至少固化一层
- 文档更新：
  - `AGENTS.md`
  - `plan.md`
  - `docs/status_snapshot.md`
  - 需要时 `docs/context_map.md`

## 默认执行顺序

### A. 开工前

只读最小上下文：

1. `AGENTS.md`
2. `README.md`
3. `docs/context_map.md`
4. 你要改的 command / processor / renderer / test

默认不要读：

- 历史报告全文
- 大量 review 记录
- 无关专题文档

### B. Patch-level 开发循环

1. 用真实命令或失败测试复现
2. 只修最靠近合同的位置
3. 补测试
4. 跑窄测试
5. 跑 1 个真实 spot check
6. 记录：
   - 改动文件
   - 测试结果
   - 真实命令结果

### C. Family-level 收口循环

只有在下面条件同时满足时才进入：

- patch 不再频繁改接口
- 真实 spot check 已稳定
- 因子/合同已经够成一组

再执行：

1. 重跑 today final
2. 跑 `release_check`
3. 跑 `report_guard`
4. 跑 round-based 外审
5. 固化 findings

## 默认验证矩阵

### 最小矩阵

- 一个 processor test
- 一个 renderer/guard test（如果影响输出）
- 一个真实 `scan` 或对应命令

### 何时加宽

- 影响共享 helper
- 影响 pick_history / release_check / report_guard
- 影响多条 command 共用 renderer
- 影响 today final 合同

## 交付格式

默认不要写成长流水账。

每轮只给四类信息：

1. 改了什么
2. 跑了什么
3. 真实看到什么
4. 下一步是否还需要 family-level 收口

## 什么时候最容易浪费 token

下面这些默认都应该避免：

- 每个小 patch 都重新解释整个项目
- 每个小 patch 都跑 today final 和外审
- 没形成稳定合同就先做大范围 review
- 每次都把历史争论完整复述一遍

## 一句话规则

**patch 看局部，family 看交付，stage 看治理。**
