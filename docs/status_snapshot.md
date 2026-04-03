# Status Snapshot

这份文件只回答四件事：

1. 现在哪些功能最成熟
2. 当前主线 backlog 是什么
3. 现在最容易误判的边界是什么
4. 最近哪些变化会影响开发判断

逐日详细 log 已从这里移出，统一放到：

- [docs/history/2026-04.md](./history/2026-04.md)
- [docs/history/2026-03.md](./history/2026-03.md)

## 成熟度

已成熟：

- `scan / stock_analysis`
- `stock_pick`
- `fund_pick`
- `etf_pick`
- `research`
- `risk`
- `portfolio`
- `compare`
- `briefing`
- `lookup / assistant`

可用但仍在迭代：

- `discover`
- `policy`
- `decision_review / retrospect`
- `scheduler`
- `strategy`

仍偏弱或仍未统一：

- proxy signals 仍是代理，不是原始全量 feed；虽然 mature 主链已开始显式披露，但 repo-wide point-in-time 完整度仍在补
- `strategy` 已有多标的 replay / experiment 与 cross-sectional validate，但仍不是全市场截面策略引擎
- `scheduler` 的持久化和运维监控还没做完

## 当前主线 backlog

1. `Tushare 10000 分` 数据源升级与旧接口替换
   这条主线现在已进入第二阶段：股票、指数/行业和 ETF 非实时主链的第一批接口已基本接好，当前重点改成把已被覆盖的旧 `AKShare` 主链系统性退掉，并把 `index_weekly / index_monthly` 的可见稿面、口径统一和最终收口做稳。`fund_factor_pro`、`broker_recommend`、`上证e互动 / 深证互动易` 已不再是主缺口，当前主要是稳定披露和例外处理。除此之外，第二阶段 backlog 还新增了 `daily_info / sz_daily_info / stk_factor_pro / stk_surv / stk_ah_comparison / fx_basic / fx_daily / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / sge_basic / sge_daily`。专项路线见 [docs/plans/tushare_10000.md](./plans/tushare_10000.md)。
2. 事件消化与研究理解
   把财报 / 公告 / 政策 / 交易所 / IR / 媒体报道这类情报从“抓到”推进到“解释”。
3. 研究记忆与 thesis ledger
   让系统能写清“上次为什么看、这次什么变了、观点是否升级/降级”。
4. `strategy` 下沉为后台置信度层
   继续把历史验证状态、退化提醒和排序置信度压进 `pick / analysis / briefing / portfolio`。
5. 连续跟踪与监控
   继续补观察名单、触发器命中、事件日历、旧稿复查和 thesis 变更提醒。
6. 组合联动与研究置信度收敛
   继续把单篇判断映射到主题重复度、风格暴露、建议冲突和组合优先级。
7. `policy` v2
8. proxy signals repo-wide 收口
9. `scheduler` v2
10. 校准与学习
11. 外审能力扩展
12. 强因子维护模式

## 当前不该误判的边界

- [docs/history/architecture_v2.md](./history/architecture_v2.md) 是历史文档，不是当前主合同。
- `strategy` 已可用，但仍是窄版研究闭环，不是 production alpha engine。
- `policy` 已可用，但扫描版 / 表格重原文仍是明确降级边界。
- ETF / 基金的 `基本面` 更多是产品质量和代理映射，不应直接当成底层行业基本面确认。
- 低覆盖率稿件不应继续伪装成完整终稿，必要时应退成摘要观察稿。
- “情报偏弱”不等于利空；对成熟稿应显式写成边界，而不是漏出 miss 诊断语。

## 当前最重要的短摘要

- `briefing` 已按“情报总台”口径重构：首页与正文都会优先前置多条可链接情报，而不是单条 lead item。
- `event_digest -> editor_payload -> client_report` 共享情报链 v1 已收口：多条相关情报、信号类型/强弱、来源层级、空情报窗口表达，已在 mature 主链统一。
- `10000` 分 Tushare 升级仍是 repo 级优先主线，但口径已经从“广撒接口”切到“补剩余缺口 + 退已覆盖旧链”：股票主链、指数/行业标准链和 ETF 非实时主链第一阶段已基本接好，当前优先是统一 `index_weekly / index_monthly` 的成熟稿面表达、继续退已被覆盖的 `AKShare` 主路径，并把可见性和最终收口做稳。`fund_factor_pro / broker_recommend / e互动` 已经进入稳定消费，不再算主缺口。
- `10000` 分计划外的第二阶段 backlog 也已补出来，不再只围着股票/ETF/指数/行业第一阶段打转：当前已确认但尚未接入的高价值数据包括 `daily_info / sz_daily_info / stk_factor_pro / stk_surv / stk_ah_comparison / fx_basic / fx_daily / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / sge_basic / sge_daily`。
- `briefing / market_drivers / market_pulse` 已补第一版 freshness 合同：A 股盘面快照会显式带 `as_of / latest_date / is_fresh`，`briefing` 只会把 fresh same-day 的盘面源前置成“今日信号”，不再让旧缓存冒充今天的 A 股结论。
- `market_drivers` 当前已切到 `Tushare ths_index + ths_daily` 主链来生成 `concept_spot / industry_spot`；`ths_hot` 也改成了“全量拉取 -> 过滤 A 股 -> 取最新交易日 + 最新时点”的快照方式。当前真正仍可能结构性空窗的，主要是 AKShare live 兜底链，而不是 `ths_*` 本身。
- ETF 研究链已进一步切到 `Tushare` 主链：`etf_basic / etf_index / etf_share_size / fund_daily / fund_adj` 已同时进入 `etf_pick / scan / compare / portfolio`；已被覆盖的旧 `AKShare` ETF 概况/日线/持仓补名路径不再作为默认主路径。当前正式稿也会把 `跟踪指数 / 指数代码 / 最新总份额 / 最新总规模 / 最近份额或规模变化` 写进 `ETF专用信息` 和推荐理由；`fund_factor_pro` 也已进入稳定消费，不再是主缺口，当前主要是继续收拢可见性和异常披露。
- 指数专题里 `index_weekly / index_monthly` 已进入成熟研究主链消费；`briefing / scan / stock_analysis / etf_pick` 正文里都会显式出现 `周月节奏`，它们不再只是周/月视角补充层，当前重点是统一周/月线口径和最终收口。
- `valuation` 这轮又继续退掉一批已被 Tushare 覆盖的旧 `AKShare` 路径：ETF 净值历史已只认 `fund_nav`，ETF 规模快照已切到 `etf_share_size`，个股财务代理已只认 `daily_basic + fina_indicator`；仍保留的 AKShare 主要是实时/分钟或 10000 分尚未一对一覆盖的侧路。
- `stock_analysis` PDF 首页分页已单独修过：现在首页判断层和 `## 一句话结论` 之间会强制打印分页，不再把执行摘要挤在第一页底部。
- `briefing` 的盘面标签也补了正负过滤：负涨幅行业/概念不会再被写成 `走强 / 领涨`，共享 `market_event_rows` 里会按 `承压` 处理。
- `stock_pick` 的 observe-only 信息密度已补回：恢复 `第二批`、`低门槛继续跟踪`、`代表样本复核卡`，不再被 observe-only 分支过度压缩。
- `stock_analysis / scan / etf_pick / fund_pick / stock_pick` 的客户稿已清掉大部分内部 miss 诊断语，旧闻回放也开始明确降成“历史基线”而不是新增催化。
- 连续研究主线已从“有状态”推进到“有 thesis_state / What Changed / review queue / review action / run history”。
- `strategy` 多标的 validate / experiment 已真实进入正式稿，但当前定位仍是后台置信度层，不再是独立大产品。

## 最近会影响开发判断的变化

这里只保留会改变默认开发判断的短摘要；日级细节见 history。

- 2026-04-01
  默认入口文档继续压短：`README / plan / docs/prompts/README` 已改成渐进式披露入口，逐日 log 不再放在默认开工上下文。
- 2026-04-01
  共享 runtime 又收了一轮：`BaseCollector` 的默认 Tushare timeout 已从 `12s` 收紧到 `8s`，`stock_pick` 非 `client-final` 且无显式 `--config` 时会自动跳过跨市场代理、market monitor 与板块驱动慢链，并轻量收窄并发/候选池。
- 2026-04-01
  `stock_pick` 预览 runtime 已继续下沉到 engine：默认预览还会跳过 `proxy_signals / signal_confidence / 逐票行业补查`，优先保证首屏筛选速度；正式 `client-final` 合同不受影响。
- 2026-04-01
  情报链 v1 已在 `briefing / scan / stock_analysis / stock_pick / etf_pick / fund_pick` 上收口；成熟稿统一支持多条相关情报、来源层级和 client-safe 空情报窗口。
- 2026-04-01
  `briefing` 的情报回填已变成多级链路：轻量 RSS -> 广覆盖 RSS -> `Tushare market intelligence` -> query-group 搜索。
- 2026-04-01
  `briefing` 的情报优先级已改成 `A股盘面/热股/观察池 > 外部新闻补充`；盘面行会显式带 `信号类型 / 强弱 / 结论`，`briefing_light` 也改成国内盘面优先、不再默认让 Reuters/Bloomberg 宏观标题抢第一屏。
- 2026-04-01
  `briefing / market_drivers / market_pulse` 已补第一版 freshness 合同：`industry_spot / concept_spot / hot_rank / zt_pool / strong_pool / dt_pool` 现在都会显式带 `as_of / latest_date / is_fresh`，`briefing` 只会前置 fresh same-day 的 A 股盘面快照；对这类高速盘面源，抓取失败后也不再自动回退 stale cache。
- 2026-04-01
  这条 freshness 合同又继续收紧到“同日快盘面不复用缓存”：`industry_spot / concept_spot / hot_rank / zt_pool / strong_pool / dt_pool` 在当天默认 live fetch，不再复用盘中缓存冒充收盘后盘面；`briefing` 的 `_timed_collect` 也改成 daemon-thread timeout，避免超时降级后后台线程继续拖住 `client-final`。
- 2026-04-01
  用户补到 `10000` 分后，`briefing / market_drivers` 现已真实优先走 `ths_index + ths_daily` 的同花顺主题/行业盘面；`ths_hot` 也不再按 `market=A / trade_date` 错误调空，而是改成“全量热榜 -> A股过滤 -> 最新交易日 + 最新时点”的本地筛选合同。
- 2026-04-01
  ETF 链已开始切到 ETF 专用合同：`fund_profile` 会补 `etf_snapshot`，显式带 `跟踪指数 / 指数代码 / 交易所 / ETF类型 / 最新份额规模`；`scan` 和 `etf_pick` 的客户稿也已恢复并保住 `基金画像 / ETF专用信息 / 持仓 / 行业暴露`，不再允许后续开发静默吞掉。
- 2026-04-01
  `release_check / report_guard` 现已新增第一版“保功能门禁”：不是只防接口升级，而是默认防任何开发静默吞功能。当前已先锁住 `stock_pick observe-only` 的 `第二批 / 代表样本复核卡`、`etf_pick` 的 `基金画像`，以及 `scan` 源稿已有的 `基金画像 / 持仓 / 行业暴露`。
- 2026-04-01
  `briefing --client-final` 默认关闭 query-group 搜索回填，优先用 `Tushare market intelligence + 广覆盖 RSS` 完成情报补齐，先保证正式稿稳定落盘；需要更重的搜索扩搜时，改走显式配置而不是默认主链。
- 2026-04-01
  `briefing` 这套 `signal_type / signal_strength / signal_conclusion` 已下沉到共享 `event_digest`；`scan / stock_analysis / stock_pick / etf_pick / fund_pick` 的首页 `关键新闻 / 关键证据`、正文 `事件消化` 和证据行都会显式写出“这条情报是什么、强弱如何、当前偏什么结论”。
- 2026-04-01
  `briefing` 在 `concept_spot / hot_rank` 空窗时，已开始优先合成 `A股主题活跃 / A股主题跟踪` 这类主题级信号：例如把 `涨停集中：化学制药` 提炼成 `创新药/医药`，把 `观察池前排：中际旭创` 提炼成 `AI算力/光模块`，不再只剩原始行业桶计数。
- 2026-04-01
  `market_drivers._volatile_frame_report` 现在不会再把空 frame 写成 fresh；这类盘面源会显式落成 `diagnosis = empty_or_blocked`，避免下游把“当日空窗”误解成“今天没有主线”。
- 2026-04-01
  `stock_pick` observe-only 已补回 `03-20` 那批最有价值的信息密度，但不回退成超长 appendix。
- 2026-03-31
  客户稿里的 `未命中... / 覆盖率摘要 / 当前可前置的一手情报有限` 这类诊断语已基本清出 mature 主链。
- 2026-03-29
  连续研究主线已从“有状态”推进到“有执行动作、复查队列和 thesis 状态机”。
- 2026-03-28
  `strategy` 已真实跑进 today final，但当前最值钱的方向仍是继续下沉到后台置信度，而不是扩独立报告。

## 相关入口

- 默认任务读法：[docs/context_map.md](./context_map.md)
- 路线图总览：[plan.md](../plan.md)
- YAML 地图：[config/README.md](../config/README.md)
- `Tushare 10000 分` 专题：[docs/plans/tushare_10000.md](./plans/tushare_10000.md)
- `strategy` 专题：[docs/plans/strategy.md](./plans/strategy.md)
- 强因子专题：[docs/plans/strong_factors.md](./plans/strong_factors.md)
- 详细 history：
  - [docs/history/2026-04.md](./history/2026-04.md)
  - [docs/history/2026-03.md](./history/2026-03.md)
