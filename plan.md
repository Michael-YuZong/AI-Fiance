# AI-Finance 路线图

这份文件只保留路线图总览。

如果任务只是改单个 command / renderer，先看 [docs/context_map.md](./docs/context_map.md)。如果任务只涉及 `strategy`，直接看 [docs/plans/strategy.md](./docs/plans/strategy.md)。

## 北极星

目标不是“功能越来越多”，而是形成研究闭环：

1. 拉取当时可见证据
2. 形成研究判断
3. 映射到推荐、组合和风险
4. 产出可交付结果
5. 接入外审
6. 进入监控、复盘和校准

## 当前主线

### P0 数据源升级主线

1. `10000 分 Tushare phase 2：补剩余缺口 + 退已覆盖旧链`
   这条主线已经从“先把高价值接口接进来”，进入“把剩余缺口补齐，并把已被覆盖的旧 AKShare 主链退掉”的阶段。

   当前已基本落地：
   - 股票主链：`ths_index / ths_daily / ths_hot / ths_member`
   - 股票风险与筹码：`st / stk_high_shock / stk_alert / cyq_perf / cyq_chips`
   - 指数/行业标准链：`index_basic / index_daily / index_dailybasic / index_weight / idx_factor_pro / index_global / 申万/中信行业框架 / index_weekly / index_monthly`
   - ETF 非实时研究链：`etf_basic / etf_index / etf_share_size / fund_daily / fund_adj / fund_factor_pro`

   当前下一步：
   - 把 `index_weekly / index_monthly` 的周/月线写法、来源披露和最终收口统一
   - 已被 Tushare 覆盖的 `AKShare` 主路径继续系统性退场，不再长期双轨混跑
   - 可见性、最终收口和 client-safe 披露统一
   - 继续把第二阶段 backlog 往前推：
     - 已开始开发并进入主链：`daily_info / sz_daily_info / stk_factor_pro / stk_surv / fx_basic / fx_daily`
     - 已接入 collector + processor：`stk_ah_comparison / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / cb_issue / cb_share / sge_basic / sge_daily / tdx_index / tdx_member / tdx_daily / ggt_top10 / ccass_hold_detail / hm_detail / dc_index / dc_daily / moneyflow_mkt_dc / report_rc`
     - 当前已前置到 `briefing / compare / fund_pick` 首屏，并开始下沉 `TDX结构专题 / DC结构专题 / 港股辅助层 / 转债辅助层 / 研报辅助层`；下一步重点是把同批信号继续统一到更多成熟稿首屏和长期对比链，而不是再停在辅助因子层

   原则不是“多接几个接口”，而是：
   - 优先把剩余接口真正下沉到 `briefing / scan / stock_analysis / stock_pick / etf_pick / fund_pick / risk`
   - 新主链接稳后，顺手退掉被覆盖的旧实现，不保留两套长期平行低质量路径
   - 每接一层都明确 `source / as_of / fallback / point-in-time / client-safe disclosure`

   专项路线见 [docs/plans/tushare_10000.md](./docs/plans/tushare_10000.md)。

### P1 持续研究主线

1. `事件消化与研究理解`
   把财报 / 公告 / 政策 / 交易所 / IR / 媒体报道这类情报，从“抓到”推进到“解释改变了什么”。
2. `研究记忆与 thesis ledger`
   让系统稳定回答“上次怎么看、这次什么变了、观点是否升级/降级”。
3. `连续跟踪与监控`
   继续补观察名单、事件日历、复查队列、旧稿状态和 thesis 触发器。
4. `strategy` 下沉为后台置信度层
   把历史验证状态、退化提醒和排序置信度继续压进 `pick / analysis / briefing / portfolio`。
5. `组合联动与置信度收敛`
   继续把单篇判断映射到主题重复度、风格暴露、建议冲突和组合优先级。

### P2 平台级主线

1. `policy` v2
2. proxy signals repo-wide 收口
3. `scheduler` v2
4. 校准与学习
5. 外审能力扩展

## 阶段快照

| 主题 | 当前状态 | 下一步 |
| --- | --- | --- |
| 推荐 / 分析主链 | 成熟 | 继续退已覆盖的 AKShare 旧链，收口 `index_weekly / index_monthly` 的周/月线写法、披露和最终可见性，并把第二阶段 Tushare backlog 纳入正式排期 |
| editor / theme playbook | v1 收口 | 继续扩大主题卡和首页表达稳定性 |
| `strategy` | 第一版闭环 | 继续做更长窗口 calibration 和后台下沉 |
| 强因子工程 | v1 收口 | 进入维护，剩余问题并回 `strategy / calibration` |
| `policy` | 部分完成 | 强化抽取、表格和 taxonomy |
| `scheduler` | v1 | 做持久化和运维可见性 |

## 当前不追求什么

- 不默认扩新报告品类
- 不把 `strategy` 做成独立大产品
- 不把 proxy 信号包装成原始全量 feed
- 不为了产出 final 而削弱 `release_check / report_guard`

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

## 详细信息去哪看

- 当前成熟度与 backlog：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 默认读法：[docs/context_map.md](./docs/context_map.md)
- YAML 地图：[config/README.md](./config/README.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子专题：[docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 详细历史：[docs/history/2026-04.md](./docs/history/2026-04.md)、[docs/history/2026-03.md](./docs/history/2026-03.md)
