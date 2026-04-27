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

- `intel`
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
   这条主线现在已进入第二阶段：股票、指数/行业和 ETF 非实时主链的第一批接口已基本接好，当前重点改成把已被覆盖的旧 `AKShare` 主链系统性退掉，并把 `index_weekly / index_monthly` 的可见稿面、口径统一和最终收口做稳。`fund_factor_pro`、`broker_recommend`、`上证e互动 / 深证互动易` 已不再是主缺口；第二阶段 backlog 里原本待排期的 `stk_ah_comparison / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / cb_issue / cb_share / sge_basic / sge_daily / tdx_index / tdx_member / tdx_daily / ggt_top10 / ccass_hold_detail / hm_detail / dc_index / dc_daily / moneyflow_mkt_dc / report_rc` 也已进入共享主链，而且 `briefing / compare / fund_pick` 首屏已经能看到其中一批信号。当前真实剩余项不再是“有没有接口”，而是把这批信号继续统一到更多成熟稿首页和长期对比链。专项路线见 [docs/plans/tushare_10000.md](./plans/tushare_10000.md)。
2. 事件消化与研究理解
   把财报 / 公告 / 政策 / 交易所 / IR / 媒体报道这类情报从“抓到”推进到“解释”。
2.5. 自由情报采集链
   把 `intel` 从第一版搜索/结构化混合快照推进到更稳定的主题 query、命中去噪和可复用情报卡，并继续把它收成 mature 报告家族的统一情报收集上游，不再让 `briefing / stock_analysis / stock_pick / scan / etf_pick / fund_pick / research` 各自长一套新闻回填逻辑。
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
- `intel` 已从独立 ad-hoc 命令进入共享上游：`briefing / stock_analysis / stock_pick / scan / etf_pick / fund_pick / research` 现在都已开始复用 `collect_intel_news_report`，报告链自己的职责保留在 `event_digest / client_report / report_guard`，不再让各命令各写一套情报抓取与 query-group 回填。
- `intel` 这轮已进入 v2：共享上游现在会在进入报告链前先做 `去重/聚类 -> 主题分组 -> 来源分层 -> 代表情报选择`。这意味着 mature 报告家族即使不单独改 renderer，也会先收到更干净的情报集合，而不是原始 search/rss 噪音列表。
- `intel v2` 这轮也开始真正下沉到摘要层：`news_report` 现在会统一带 `summary_lines / lead_summary`，`editor_payload / client_report` 会优先前置这些聚类摘要，再补原始链接证据。也就是说，报告首页不再只能吃“原始新闻条目”，而是开始直接消费共享情报摘要。
- `intel` 这轮又补了“市场级大事假设”共享层：当上游只剩 `proxy`、摘要却已经明显在说 `地缘/停火/风险偏好/利率/关税` 这类市场级变化时，`collect_market_aware_intel_news_report(...)` 会先按这类大事假设优先补搜，再回到主题/标的查询；这层已经接到 `stock_analysis / scan / stock_pick / research / fund_pick / etf_pick`，不再只在 `briefing` 里单点修补。
- `event_digest -> editor_payload -> client_report` 共享情报链 v1 已收口：多条相关情报、信号类型/强弱、主要影响、结论、传导路径、来源层级、空情报窗口表达，已在 mature 主链统一；首页关键情报不能退回“只贴新闻标题”。
- `event_digest` 对个股 IR / 互动易 / e互动 / 投资者问答的分类已补硬边界：这类管理层口径默认归入 `公告类型：投资者关系/路演纪要`，只有真实政策主体或政策文件才允许写成 `政策影响层`，避免把公司产品问答包装成政策催化。
- 个股聚合层已补 `technical < 30 / catalyst < 20 / risk < 20` 硬 gate：两项及以上未过直接 `无信号`，单项未过封顶观察级；`trade_state / action_plan / discover_next_step / release_check / report_guard` 同步承认这条合同，不能再靠基本面、相对强弱或结构化事件把弱信号包装成 `较强机会`。
- 共享首页 `情报摘要` 这轮又继续去机器口径：如果聚类只落到 `综合/其他` 这类默认桶，首页现在会明确改写成“暂未稳定归到单一主题、先按背景线索理解”，不再把分类器默认标签和“独立利好数量”直接暴露给客户。
- `10000` 分 Tushare 升级仍是 repo 级优先主线，但口径已经从“广撒接口”切到“补剩余缺口 + 退已覆盖旧链”：股票主链、指数/行业标准链和 ETF 非实时主链第一阶段已基本接好，当前优先是统一 `index_weekly / index_monthly` 的成熟稿面表达、继续退已被覆盖的 `AKShare` 主路径，并把可见性和最终收口做稳。`fund_factor_pro / broker_recommend / e互动` 已经进入稳定消费，不再算主缺口。
- `10000` 分计划的第二阶段 backlog 已继续往前收：`daily_info / sz_daily_info / stk_factor_pro / stk_surv / fx_basic / fx_daily / stk_ah_comparison / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / cb_issue / cb_share / sge_basic / sge_daily / tdx_index / tdx_member / tdx_daily / ggt_top10 / ccass_hold_detail / hm_detail / dc_index / dc_daily / moneyflow_mkt_dc / report_rc` 都已进入 `market_overview / context / valuation / market_cn / market_drivers / fund_profile / commodity / opportunity_engine / news / renderer`。`briefing / compare / fund_pick` 已开始把 `市场结构 / A/H 比价 / 公募渠道环境 / 可转债映射 / 黄金现货锚定` 前置到可见层，`opportunity_engine` 也已开始下沉 `TDX结构专题 / DC结构专题 / 港股辅助层 / 研报辅助层 / 转债辅助层`；当前剩余项主要是把这批新能力继续统一到更多 mature 首屏，而不是再补 collector。
- `fx_daily` 这轮也补上了失败降级合同：权限、频控或 Tushare IP 限制现在会按缺失快照处理，不再把 `briefing / regime / research` 这类依赖宏观上下文的命令直接打死。
- `report_guard / release_check` 这轮又把成熟稿 fastpath 从 `briefing / fund_pick` 扩到了 `stock_pick / etf_pick / scan`：前提仍然是 `release_check` 通过、正文满足详细稿门槛、review 只是 scaffold/clean 状态；真实 BLOCKED review 仍会继续拦截，不会被自动放行。`briefing` 已做过真实 `client-final` spot check，当前自动生成的 round-based PASS review 已能稳定落盘。
- `briefing / market_drivers / market_pulse` 已补第一版 freshness 合同：A 股盘面快照会显式带 `as_of / latest_date / is_fresh`，`briefing` 只会把 fresh same-day 的盘面源前置成“今日信号”，不再让旧缓存冒充今天的 A 股结论。
- `market_drivers` 当前已切到 `Tushare ths_index + ths_daily` 主链来生成 `concept_spot / industry_spot`；`ths_hot` 也改成了“全量拉取 -> 过滤 A 股 -> 取最新交易日 + 最新时点”的快照方式。当前真正仍可能结构性空窗的，主要是 AKShare live 兜底链，而不是 `ths_*` 本身。
- ETF 研究链已进一步切到 `Tushare` 主链：`etf_basic / etf_index / etf_share_size / fund_daily / fund_adj` 已同时进入 `etf_pick / scan / compare / portfolio`；已被覆盖的旧 `AKShare` ETF 概况/日线/持仓补名路径不再作为默认主路径。当前正式稿也会把 `跟踪指数 / 指数代码 / 最新总份额 / 最新总规模 / 最近份额或规模变化` 写进 `ETF专用信息` 和推荐理由；`fund_factor_pro` 也已进入稳定消费，不再是主缺口，当前主要是继续收拢可见性和异常披露。
- 指数专题里 `index_weekly / index_monthly` 已进入成熟研究主链消费；`briefing / scan / stock_analysis / etf_pick` 正文里都会显式出现 `周月节奏`，它们不再只是周/月视角补充层，当前重点是统一周/月线口径和最终收口。
- `valuation` 这轮又继续退掉一批已被 Tushare 覆盖的旧 `AKShare` 路径：ETF 净值历史已只认 `fund_nav`，ETF 规模快照已切到 `etf_share_size`，个股财务代理已只认 `daily_basic + fina_indicator`；`valuation` 本身已不再保留 AKShare helper，仍保留的 AKShare 主要是 `market_cn` / `fund_profile` 里的实时、分钟或 10000 分尚未一对一覆盖的侧路。
- `fund_profile` 这轮也继续退了一层：`cn_fund` 持仓默认已切到 `Tushare fund_portfolio + stock_basic` 主路，只有 Tushare 持仓缺失时才允许 `AKShare` 持仓兜底。当前剩余 AKShare 更明确地收缩在 `market_cn` 的实时/侧路和 `fund_profile` 的开放式基金补充细节。
- `fund_profile / valuation` 这轮又补了一层“退旧链但不吞成熟信息块”的边界：`cn_etf` 只有在 Tushare 已拿到 ETF 核心身份时，才允许旧 `AKShare overview` 做经理/基准等轻量补洞；如果 Tushare 核心仍是空表，就明确按 `基金概况缺失` 降级，不再偷偷回到旧主链。同时 ETF `fund_nav` 若只返回较旧净值，也会回传最新可用时点，不再因为相对今天超过窗口而被硬清空。
- `event_digest / editor_payload / client_report` 又收了一轮首屏证据排序：`e互动 / 投资者关系 / 路演纪要 / 业绩说明会 / broker` 这类更直接的证据现在会排在 generic framework rows 之前；`stock_analysis / stock_pick` 命中 `stk_factor_pro / A-H 比价 / 可转债映射 / 机构调研` 时，也会更早前置到正文前段，而不是只埋在因子表里。
- `stock_pick` 的 `client-final` discovery 快路径也补齐了最后一个共享缺口：现在只继续跳 `unlock/pledge` 这类硬慢源，不再默认跳过 `broker_recommend`。这意味着卖方共识命中时，已经能更稳定地进入候选筛选和前段证据层，不会再被 fastpath 自己静默掐掉。
- `stock_pick --client-final` 的 discovery / preview 快路径现在明确分层：预筛阶段跳过逐票 direct news、龙虎榜/竞价/涨跌停、行业指数逐票补查、资金流行业/概念代理和 IR/调研慢结构化接口，只保留轻量高价值结构化源；入围补强再补公司级情报。缺口会按 `runtime_skip` 披露，不允许慢链偷偷绕回导致正式稿卡死。
- `stock_pick / stock_analysis / scan` 的主题归因这轮又补了一层“结构化优先”合同：`theme_playbook` 不再把市场 `day_theme` 或 `business_scope` 里的 `旅游 / 餐饮 / 再生资源` 之类噪音直接写成个股主线。现在会先看 `sector / industry / industry_framework_label / chain_nodes / tushare_theme_industry`，只有确实对齐时才允许 `day_theme` 进入个股主线叙述。
- `stock_analysis / scan` 的单标的直跑 metadata merge 这轮也补掉了同类污染：`cn_stock` 在已有标准行业时，会先按 `industry / sector` 做 broad-sector 与 chain 节点归因，再把 `main_business / business_scope / company_intro` 只当 fallback。`盐湖股份 (000792)` 这类同时写了 `碳酸锂 / 电池 / AI材料` 的公司，当前真实 spot check 已从错误的 `光伏主链` 拉回 `农业 / 种植链`。
- 上面这条合同已经不只停在 sidecar：`000792` 的 `stock_analysis / scan` 外审都已从 scaffold 收敛到 `PASS`，`client-final` 真实导出也已完成。也就是说，这次修的是共享主题归因合同，不是只修 payload、不敢重刷 final 的半收口。
- `stock_analysis / scan` 的共享 `_subject_theme_context(...)` 这轮又补了一层刷新合同：如果 analysis 里现成 `theme_playbook` 还只是 sector-level，就不能直接沿用旧 context，必须结合 `business_scope / company_intro` 重新评估同赛道细主题；但升级仍受硬行业 bridge 约束，跨赛道公司简介只能当噪音。真实 spot check 里，`002709` 已从泛 `材料` 升成 `锂电`，而 `600096 / 000792` 仍稳定保留 `农业 / 种植链`。
- `etf_pick` 这轮也补上了对应合同：winner 现在会把 `fund_profile / theme_playbook` 显式写回 `payload / editor_payload`，不再只留 `fund_sections`；共享 `_subject_theme_context(...)` 对 `cn_etf / cn_fund / cn_index` 会优先按 `跟踪指数 / 业绩比较基准 / sector / industry_framework_label` 识别主题，不再让 `chain_nodes` 里像 `消费电子零部件及组装 / 电子` 这类噪音把消费 ETF 首页误写成 `信息技术 / AI硬件链`。
- `etf_pick --client-final` 当前已去掉两条容易走错的旧路径：不再复用 same-day `internal_detail + payload` bundle，也不再对入围 ETF 逐只做 full reanalysis 慢链。正式稿现在统一走单一路径：`discover -> hydrate finalists full profile -> refresh shared ETF report fields -> finalize/export`；今天的 `python -m src.commands.etf_pick --top 1 --client-final` 已按这条新链真实落出 `2026-04-17` 的 `md/html/pdf`。
- `stock_pick / briefing --client-final` 这一轮也回到了单一路径：仍会落 `payload / editor_payload / prompt` 侧车做审计和 continuity，但默认不再复用 same-day `internal -> finalize/export` bundle，也不再走 `editor fallback` 私有捷径。`briefing` 只保留 `_load_same_day_briefing(...)` 给午盘/夜盘同日校验用，不再把它当正式稿导出快路径。
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
- 2026-04-05
  `intel` 已从独立命令下沉成共享情报上游：`briefing / stock_analysis / stock_pick / scan / etf_pick / fund_pick / research` 现在统一复用 `collect_intel_news_report(...)`，并会直接消费 `summary_lines / lead_summary`，不再各自维护一套 RSS/search 回填逻辑。
- 2026-04-05
  `briefing` 的旧 query-group 新闻回填已改成 `合并 query 一次优先，未覆盖组最多补两次` 的非阻塞补充层；回填后还会保留 `主题聚类 / 来源分层` 摘要，不再默认把同一批 query groups 全跑一遍才生成晨报。
- 2026-04-06
  `etf_pick` 的观察稿可读性门禁和 renderer 已继续收口：`release_check / report_guard` 现在会拦“边界声明/缺失披露过密”的 observe-style 客户稿；同时 `render_etf_pick` 的 observe + summary-only 分支已统一走紧缩版，`数据完整度 / 交付等级 / 数据限制` 会压成少量清单，`事件消化` 和 `What Changed` 允许更人话的同义改写，不再因为去掉 `先按 / 不把` 这类机器口吻而被合同误判成缺失。
- 2026-04-08
  Claude 外审里那类“低信噪比 / 自我辩护口气 / 首页情报混写”的批评，已经继续下沉到共享门禁与共享首页合同：`editor_payload` 首页现在会强制把 `外部情报 / 结构证据 / 情报摘要` 分开写，不再把带链接的长句和结构判断混在一条里；同时 observe-style `scan / stock_analysis / stock_pick / fund_pick` 的低信噪比口气也继续受 `release_check / report_guard` 约束。
- 2026-04-08
  `scan --client-final` 的观察稿 client-final 已补成真正可交付：observe 分支不再走旧的 raw detailed rewrite，而是复用共享紧缩渲染路径；`scan_300274_2026-04-08_client_final` 已真实通过 `release_check = 0`、自动把 review scaffold 收敛成 PASS，并落出 `md/html/pdf`。
- 2026-04-08
  `stock_analysis` 也已跟上同类 fastpath：clean `stock_analysis` final 现在会和 `scan` 一样把 review scaffold 自动收敛成 PASS，不再因为空 scaffold 卡住交付；`stock_analysis_300274_2026-04-08_final` 已真实落出 `md/html/pdf`。
- 2026-04-08
  `scan / stock_analysis` 的 observe 紧缩分支已补回 `图表速览`：共享 compact renderer 现在会在复用 `render_scan(...)` 后重新插入 `分析看板 / 阶段走势 / 技术指标` 图表块，不再因为 same-day 紧缩渲染把图表静默吞掉。
- 2026-04-08
  `briefing` 的情报回填合同已从“主题 query 先跑”改成“proxy-only 时先按宏观/地缘假设优先补搜”：当晨报只剩 `proxy` 且首页摘要明显在说 `国际局势 / 能源与地缘 / 风险偏好` 时，会先尝试 `美伊 / 伊朗 / 以色列 / 停火 / 休战` 这类市场级情报，再轮到行业主题，避免 AI/题材新闻把当天宏观大事压过去。
- 2026-04-08
  `briefing` 首页取材也已补成“有外链情报时至少先露一条”：`editor_payload._briefing_news_lines(...)` 不再让 `market_event_rows` 把真实外部情报全部挤掉；但当前中文财经主源仍主要依赖 Google News RSS / 搜索回退，质量已明显改善，仍未达到真正的 `Tushare news first / 一级媒体 first`。
- 2026-04-08
  共享情报首页口径继续收口：`editor_payload / client_report` 现在会把 `情报摘要` 改写成人话，并在 ETF/基金等主题产品上补 `关系说明`，解释“这条情报先作用于哪条指数/板块主线，再怎么传导到当前标的”，不再直接暴露 `主题聚类 / 相近线索 / 独立新增催化` 这类机器口径。
- 2026-04-11
  主题合同这轮开始补“交易角色”层：`theme_playbook -> editor_payload -> client_report` 已新增 `主线核心 / 主线扩散 / 强波段 / 轮动 / 观察` 这类共享分层，`stock_pick / etf_pick / scan / stock_analysis` 也开始可见 `主线仓 / 卫星仓 / 波段仓 / 轮动仓 / 观察仓`。目标不是多发一行术语，而是把“方向没错”和“该按什么仓位身份去做”拆开写，避免再把所有科技方向机械压成 `波段跟踪（2-6周）`。
- 2026-04-11
  `briefing` 的科技主线口径也开始从泛 `AI/半导体` 大桶往 `硬科技 / AI硬件链` 收：当前判主线、主题跟踪和回填 query 会优先看 `半导体 / 光模块 / 液冷 / 存储 / PCB` 这类硬件链，不再把软件/应用层和硬件链过早混写成同一层“科技主线”。
- 2026-04-11
  交易角色这轮又补了“否定语义 + 角色优先级”收口：像 `不是当前主线 / 不宜主攻 / 不是科技主线本体` 这类句子，不会再因为含有 `主线 / 核心` 字样被误抬成 `主线核心`；同时 `细分扩散 / 第二梯队 / 接力` 会优先落成 `主线扩散`，`轮动 / 高低切 / 防守` 和 `副主线 / 强波段` 也不再轻易被主线词覆盖。
- 2026-04-11
  动态主线和情报链又收了一层共享合同：`briefing / opportunity_engine` 现在会把行业/概念/市场脉冲纳入当天主题判断，避免把科技、电网、有色这类方向写成固定标签；`event_digest` 也已识别 `停火 / 休战 / 风险偏好修复` 这类风险资产利好语义，不再因为标题里出现“风险”就误判成利空；反过来，`停火` 但同时出现 `僵局 / 遭袭 / 受损` 时会归为 `地缘扰动`，不再误写成风险偏好利好。
- 2026-04-11
  `scan / stock_analysis --client-final` 不再默认关掉主题情报扩搜；ETF scan 会复用 `etf_pick` 的 ETF/intel 外部情报回填，避免同一只 ETF 在 `etf_pick` 能看到半导体链情报、到 `scan` 却被写成零催化。`briefing` 的新闻回填也从“已有 3 条标题就停止”改成优先检查可点击证据；无直链 RSS 会显式使用搜索回退链接，不冒充一手来源。
- 2026-04-11
  动态交易角色又补了一层证据优先级：当主题命中、相对强弱和催化已经共振，且没有明确“不是主线 / 不宜主攻”这类否定语义时，不再因为正文里出现 `轮动 / 切换 / 观察 / 修复` 就机械降成 `轮动仓`。这会把类似 `159516` 这类硬科技 ETF 写成 `主线扩散 / 卫星仓`，同时动作仍可保持观察、回调参与或小仓纪律。
- 2026-04-11
  动态交易角色这轮开始正式吃 `event_digest`：共享层不再只看静态主题词和技术/风险分数，而会把 `高重要性 / thesis变化 / 强信号` 的事件解释也算进主题证据。这样像“主题强、催化强，但执行仍偏观察”的标的，会优先落成 `主线扩散 / 卫星仓`，而不是又被谨慎措辞压回 `轮动仓`；同时 `主线核心` 也补了执行门槛，弱技术/弱风险特征不会再被硬抬成核心仓。
- 2026-04-11
  首页情报排序这轮也收成共享合同：`editor_payload` 在 `scan / stock_analysis / stock_pick / etf_pick / fund_pick / briefing` 里会优先把“和当前标的/主题直接相关”的外部情报排到市场背景之前；如果 `event_digest` 已给出高置信主题催化，它也可以压过泛市场背景 lead。目标是先回答“这只/这条线为什么值得看”，再补“今天的大环境是什么”。
- 2026-04-11
  首页 `关键新闻 / 关键证据` 的标签合同这轮也补稳了：共享层现在会强保留 `结构证据` 与 `外部情报` 的双标签，不让 `关系说明 / 情报摘要` 把直链证据挤掉；如果只剩无关背景新闻，会显式退成“未拿到可点击外部情报”，不再把泛市场大事硬塞成单标的首页新闻。
- 2026-04-11
  首页判断这轮又补了一层共享“决策拆分”合同：`scan / stock_analysis / stock_pick / etf_pick / fund_pick` 现在都会显式写 `赛道判断 / 载体判断 / 执行卡 / 尾部风险`，不再把“赛道成立但载体不优”“动作偏观察但方向未坏”混成一句总评；`release_check` 和 `external_financial_* reviewer` prompt 也同步把这四层收成硬检查，避免外审再因为“方向大体对、语气够克制”而漏放结构性错位。
- 2026-04-11
  报告落盘命名也补成 ASCII-safe 合同：`etf_pick` 的 internal/final 文件名不再直接拼中文主题，而是统一走 ASCII slug，避免终端、桌面链接和 HTML 路径出现中文/转义乱码。
- 2026-04-11
  首页 `执行卡` 这轮又补了一层“去机械化”合同：对 `主线扩散 / 卫星仓` 这类方向，首页不再把价位写成像是可以直接机械挂单的模型输出，而会显式提醒“先看触发 / 失效 / 第一次兑现”；同时也不再要求所有确认都齐才允许第一笔，小仓试错和回踩承接确认现在会直接写进动作卡。
- 2026-04-11
  `scan` 和 `etf_pick` 的 ETF 主题角色这轮也重新统一了：共享 `infer_theme_trading_role(...)` 现在会显式吃 `指数技术确认 / 份额净创设 / 结构证据` 这类 ETF 自身承接信号，不再因为正文里有 `观察 / 修复 / 等确认` 这类执行语气，就把同一只硬科技 ETF 在 `scan` 里降回 `轮动`、在 `etf_pick` 里又写成 `主线扩散`。
- 2026-04-12
  ETF/基金/指数 observe-only 合同又继续收了一层：`editor_payload` 的 sidecar 快照现在也会过滤 `peer ETF / 持仓股标题 / 泛市场标题` 这类无直连覆盖时的 proxy 脏情报，不再把它们留给后续 editor/subagent；同时 `client_report` 的 observe-only 动作卡已统一写成 `观察为主（偏回避） + 定性关键位/观察仓`，不再在 `scan / etf_pick / fund_pick` 的观察稿里泄露 `0.870-0.874 / ≤2%` 这类机械挂单位。
- 2026-04-12
  `scan / stock_analysis` 的 `What Changed` 和首页时点披露也补了共享 continuity 合同：当 thesis repo 还没写稳时，比较基线会优先取前一日 `editor_payload`，不再把同日更早重刷错当“上次复查”；同时首页会显式写出“相对强弱高分只是前一段主线惯性”“情绪代理仍沿用某日快照”“指数强但产品层仍弱，执行先以产品层修复为准”。
- 2026-04-11
  `briefing` / 市场结构卡的成交额单位也补了归一：`daily_info / sz_daily_info` 这类源如果给的是元或万元，不再直接拼进 `万亿` 文案里；最新成稿已把原先那类夸张的 `13678.82万亿` 修正回 `1.37万亿` 口径。
- 2026-04-11
  `client_report` 全文件回归重新收绿；顺手补回几条可读性保功能合同：ETF 指数层确认不再双句号，结构化覆盖说明会单独成句，观察稿的短线/中线提示只在全稿仍有正式动作票时出现，summary-only 基金稿保留 `正式动作阈值 / 升级条件 / 当前只看什么`。
- 2026-04-12
  单标的 mature 客户稿这轮正式改成“执行先于解释”：`scan / stock_analysis / etf_pick / fund_pick` 的 `client-final / detailed` 现在都会在 `## 首页判断` 之前先给 `## 先看执行` 四问卡，只先回答 `看不看 / 怎么触发 / 多大仓位 / 哪里止损`，正文后段再解释原因。观察稿会自动收成 `观察仓 / 暂不出手` 和 `关键支撑失效就重评` 这类非机械口径，避免在 observe-only 稿里泄露精确挂单位。
- 2026-04-12
  榜单型 mature 客户稿这轮也同步切到“执行先于解释”：`stock_pick / briefing` 现在会在 `## 首页判断` 之前先给榜单版 `## 先看执行`，仍然围绕 `看不看 / 怎么触发 / 多大仓位 / 哪里止损` 四问组织，但内容改成组合/市场级口径，不再要求读者先翻过首页长解释才知道今天能不能动。`stock_pick` 会把“先看哪几只、单票试仓上限、组合止损纪律”直接抬到最上面；`briefing` 会把“今天是不是结构性行情、主线验证点、分批节奏和失效条件”直接前置。
- 2026-04-12
  `stock_pick / briefing` 这轮又继续收了一层去重合同：首屏 `## 先看执行` 仍保留四问，但后文不再重复一遍同样的执行动作。`stock_pick` 的 `## 今日动作摘要` 已改成只讲结构和优先级的 `## 名单结构`；`briefing` 的 `## 执行摘要 + ## 今日最重要的判断 + ## 今天怎么做` 已收成 `## 市场结构摘要 + ## 执行补充`。对应 `release_check / report_guard` 也已承认新标题，不再因为旧 heading 白名单把新稿误拦。
- 2026-04-12
  `stock_pick` 正式推荐稿的首屏执行卡又收紧了一层：前排 1-2 只现在必须直接前置各自的 `建仓区 / 首仓口径 / 失效位`，不再允许把“真正出手去看复核卡”“首屏不写统一止损价”这种废话塞进首屏。`release_check / report_guard` 也已新增对应门禁，避免榜单稿再次退回只给名单、不给执行位的空动作卡。
- 2026-04-12
  `etf_pick` 的 observe-only 首屏也补了一层“关键观察位前置”合同：如果底层已经有明确的 `stop_ref / target_ref / 观察仓上限`，首屏不再统一抹成“关键支撑失效就重评 / 先按观察仓”，而会直接写出 `关键观察位 + 失效位 + 若触发的观察仓口径`；但如果底层仍没有安全买区，首屏还是保持观察稿，不把它伪装成正式建仓卡。
- 2026-04-22
  `stock_pick / etf_pick / client_report` 这轮又补了一层首页动作去噪合同：A 股 `关联ETF / 低门槛` 现在只允许从 `top + watch_positive` 这层优先覆盖池里取，不再让未入选 coverage 样本把无关主题 ETF 塞进首页；同时 ETF observe-only 的仓位句式会区分“短句触发前先按观察仓”和“长句解释型触发前按观察仓”，避免共享清洗把动作边界改坏。
- 2026-04-16
  `scan / stock_analysis --client-final` 对 `cn_stock` 已默认启用 today 快照；A 股分钟线现在走 AkShare A 股分时 collector，不再错误回落到 yfinance/Yahoo ticker。client-final 情报链在轻量配置给到空 feed 时也会恢复默认 RSS feed，避免正式稿继续 proxy-only。600584 spot check 已确认 `情报模式=live`、`stk_factor_pro` 可见，客户稿保留 `今日盘中视角` 且 `分钟级快照 as_of=2026-04-16 15:00`；当前真实剩余缺口集中在行业宽度 / 龙头确认和个股级资金/筹码硬确认。
- 2026-04-16
  600584 这轮又继续把上面那两条真实缺口收了一层：`relative_strength` 现在会在 `industry_spot / concept_spot` 缺少上涨家数时回退吃 `Tushare dc_index` 的 `上涨家数 / 下跌家数 / 领涨股`，不再把行业宽度 / 龙头确认一律写成观察提示；`筹码结构` 也把 `上一交易日 T+1 直连` 正式纳入共享合同，`moneyflow / cyq_perf / cyq_chips` 即使晚一天到，也会显式写成“上一交易日直连”而不是直接退回行业代理。当前 `stock_analysis_600584_2026-04-16_final` 首屏 `还差什么` 已收空，`证据硬度` 会改写成“辅助层仍有代理，但关键直连层已补到：个股资金流与真实筹码按 T+1 直连解读，行业宽度 / 龙头确认已命中”。
- 2026-04-16
  外审 workflow 这轮补掉了一条假闭环：`review scaffold` 和“缺 review 文件”不再允许被 `final_runner / report_guard` 自动抹成 PASS。当前只有“已经存在非占位 review，且正文无 actionable finding、只是缺 round closure”的情况，才允许 auto-close；纯 scaffold 现在会继续保持 `BLOCKED`。这也意味着 4 月里那批带 `Codex Structural Reviewer (auto-close)` 的历史 review 记录，需要按“未必做过真实双 reviewer 外审”重新理解，不能再把它们默认当成高质量外审已完成的证据。
- 2026-04-08
  `scan / stock_analysis` 的成稿门禁也补掉了一条假阳性：`release_check` 不再把“已接入龙虎榜/竞价/涨跌停边界，但当前未命中”这类边界披露误判成盘中执行语言；同时 `render_scan_detailed` 对 `cn_etf / cn_fund` 的观察稿会保留 `## 当前更合适的动作`，避免 ETF/基金 scan 客户稿被门禁拦成“缺解释版”。
- 2026-04-05
  `client_export._export_pdf()` 已补成更强的 Edge 回收合同：同目标 PDF 会先清 stale `--print-to-pdf` 进程，新导出使用独立进程组，稳定/超时后按进程组回收，不再让 `briefing` 被 orphaned Edge 头less 进程拖住。
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
