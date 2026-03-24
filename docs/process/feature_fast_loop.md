# 功能开发快路径

这份文档只回答一个问题：

如何把“开发一个新功能 -> 验证 -> 收口”的默认成本压下来，同时不牺牲合同质量。

## 一句话规则

**patch 看局部，family 看交付，stage 看治理。**

## 默认原则

1. 一次只推进一个最小合同
   不要把因子计算、渲染、pick、final、外审、strategy 混成同一轮 patch。
2. patch-level 默认不跑 today final / 外审
   先停在真实复现、局部修复、窄测试和 spot check。
3. 只有达到 family-level，才值得重跑长链路
   包括 today final、`release_check / report_guard`、round-based 外审。
4. 每轮只保留最小证据包
   只说改了什么、跑了什么、真实看到了什么、还没做什么。

## 三层完成定义

| 层级 | 适用场景 | 必须完成 | 默认不做 |
| --- | --- | --- | --- |
| `patch-level` | 单个因子、单个 renderer 合同、单个 guard、单个 fallback 修复 | 真实复现、定向修复、narrow tests、真实 spot check | today final、外审循环、宽回归 |
| `family-level` | 一组同类因子、一个完整分析维度、一个 pick/report 合同明显变化 | patch-level 全部要求、相关回归、today final、`release_check / report_guard`、外审收敛 | stage 级 lesson / audit 固化 |
| `stage-level` | 一个专题准备切出主开发线，或治理边界明显变化 | family-level 已完成、lesson / audit / backlog 固化、入口文档同步 | 无 |

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

### B. Patch-level 循环

1. 用真实命令或失败测试复现
2. 只修最靠近合同的位置
3. 补测试
4. 跑窄测试
5. 跑 `1` 个真实 spot check
6. 记录最小证据包

### C. Family-level 循环

只有在下面条件同时满足时才进入：

- patch 不再频繁改接口
- 真实 spot check 已稳定
- 因子 / 合同已经够成一组

再执行：

1. 重跑 today final
2. 跑 `release_check`
3. 跑 `report_guard`
4. 跑 round-based 外审
5. 固化 findings

## 默认验证矩阵

最小矩阵：

- 一个 processor test
- 一个 renderer / guard test
- 一个真实命令

何时加宽：

- 影响共享 helper
- 影响 `pick_history / release_check / report_guard`
- 影响多条 command 共用 renderer
- 影响 today final 合同

## 默认避免

- 每个小 patch 都重新解释整个项目
- 每个小 patch 都跑 today final 和外审
- 没形成稳定合同就先做大范围 review
- 每次都把历史争论完整复述一遍
