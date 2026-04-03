# AI-Finance Agent Handoff

这份文件只保留：

1. 默认读法
2. 共享工作规则
3. 当前主线与成熟度
4. 常用命令 / guardrail

逐日变更记录不再堆在这里；详细 history 统一放到：

- [docs/history/2026-04.md](./docs/history/2026-04.md)
- [docs/history/2026-03.md](./docs/history/2026-03.md)

## Default Read Order

不要一开工就扫完整个仓库。

默认只按下面顺序读：

1. 这份文件
2. [README.md](./README.md)
3. [docs/context_map.md](./docs/context_map.md)
4. 你要修改的 command / processor / renderer / test

只有任务相关时再继续读：

- 配置问题：看 [config/README.md](./config/README.md)
- `Tushare 10000 分` 接口升级：看 [docs/plans/tushare_10000.md](./docs/plans/tushare_10000.md)
- `strategy`：看 [docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子维护：看 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 当前状态：看 [docs/status_snapshot.md](./docs/status_snapshot.md)
- 更细历史变化：先看 [docs/history/2026-04.md](./docs/history/2026-04.md)，再看 [docs/history/2026-03.md](./docs/history/2026-03.md)
- 外审规则或 prompt：先看 [docs/prompts/README.md](./docs/prompts/README.md)

## Repo Identity

- 本仓库是本地 CLI 投研工作台，不是 web app，不是自动交易系统。
- 默认目标：提高输出质量、合同稳定性、工作流实用性。
- 除非用户明确要求，不要平白创造新表面层；优先把现有链路做稳。

## Working Rules

1. 先复现实例
   用真实命令或失败测试，不要空想。
2. 先修产品合同
   先修 command / processor / renderer / guard，不要只修文案。
3. 补测试
   至少加一条能防回归的测试。
4. 保留降级与来源说明
   缺数据时不要装成高确定性结论。
5. 更新文档
   如果合同、成熟度或 backlog 变了，要同步这份文件和相关专题文档。
6. 做共享层整理
   如果出现重复的解析、合同映射、审计逻辑，优先抽共享 helper，不要让外审 / guard / renderer 各长一套。
7. 默认走快路径
   新功能先按 [docs/process/feature_fast_loop.md](./docs/process/feature_fast_loop.md) 做 `patch-level -> family-level -> stage-level` 分层推进，不要每个小 patch 都重跑 today final 和外审。
8. 成稿任务不要停在“缺外审文件”
   只要当前任务会产出 `final / client_final` 或可独立交付的研究型 Markdown，就不要只汇报“没有 external review”；应继续补 review 记录并推进到收敛，除非被真实外部依赖或用户明确暂停阻断。
9. 热点主题 `催化 0 分` 先排除漏抓
   如果当前是 `live` 模式，但热点主题 / 高讨论度方向出现 `直连新闻 0 条`、`coverage.diagnosis = suspected_search_gap` 或明显不符合常识的 `0` 结果，默认先按“待 AI 联网复核”处理，不要直接写成零催化或利空。
10. 对外优先用“情报”口径
   财报 / 公告 / 政策 / 交易所 / IR / 媒体报道都属于情报；“新闻”只是其中一种来源。底层类名可暂保留，但用户可见说明、首页和摘要应优先写“情报链 / 情报属性 / 关键情报”。
11. 连续可收口问题默认一次性收完
   如果同一 command / processor / renderer / helper 家族里还有相邻、低风险、下一步已明确的收口项，默认继续一起修完、补测试并做真实 spot check，不要每修一个小点就停下来再问；只有在合同取舍明显、用户体验 tradeoff 大、或真实外部依赖阻塞时才暂停确认。
12. `fresh but empty` 也算坏合同
   对 `briefing / market_drivers / market_pulse` 这类盘面情报源，`frame` 为空时不能继续标成 fresh；应显式降成缺失/阻塞，并让下游走合成主题信号或其他降级路径。
13. 任何开发都不能静默丢功能
   不只是接口升级；任何开发改动默认都要检查同家族成熟稿有没有把旧的高价值块吞掉。至少对 `stock_pick observe-only`、`ETF/基金画像`、`持仓/行业暴露` 这类已形成价值的模块，补 `release_check / report_guard / 回归测试` 中的保功能门禁。只有在明确决定重构或下掉某块功能、并同步更新文档、门禁和测试时，才允许移除。

## Editor Stage Rules

对研究型成稿，默认顺序是：

1. 结构化底稿 / 规则版成稿
2. `editor_payload / editor_prompt`
3. 独立 `editor subagent`
4. `Pass A 结构审`
5. `Pass B 发散审`
6. `final`

执行要求：

- `editor` 默认由独立 subagent 执行，不要由主作者在同一上下文里顺手二改。
- `editor subagent` 只能改正文表达和首页判断层，不能补新事实、不能改推荐等级、不能把观察稿写成推荐稿。
- 结构审和发散审必须审的是 editor 改后的版本，不是规则版底稿。
- 当前协作环境里，`subagent` 是默认主路径；API/key 调用只作为脱离当前会话自动化运行时的后备。
- 如果命令已经能产出 `editor_payload.json + editor_prompt.md`，后续接手 agent 默认优先复用这两个侧车，而不是从 final Markdown 反推首页逻辑。
- 如果 `催化面` 被标成 `suspected_search_gap / 待 AI 联网复核`，优先读取 `catalyst_web_review_payload.json + catalyst_web_review_prompt.md + catalyst_web_review.md`，由独立 agent 做联网复核。
- 如果正式稿 sidecar 已落 `catalyst_web_review.md`，它就不是可选附件；在 `待补` 模板未被独立 agent 填完前，`report_guard` 应直接拦 final。

## Progressive Disclosure Rules

- 不要默认打开所有 `.md` / `.yaml`。
- 不要把 `reports/`、`tmp/`、`docs/history/`、历史生成稿当成开工前默认上下文。
- [docs/history/architecture_v2.md](./docs/history/architecture_v2.md) 是历史参考，不是当前主合同。
- 大多数任务只需要：
  - 一个短入口文档
  - 一个专题文档
  - 相关代码和测试
- 功能开发默认先停在 patch-level：
  - 真实复现
  - 窄修复
  - narrow tests
  - 真实 spot check
  只有达到 family-level，才重跑 today final / release_check / report_guard / 外审。

## Maturity Snapshot

成熟区：

- `src/commands/scan.py`
- `src/commands/stock_analysis.py`
- `src/commands/stock_pick.py`
- `src/commands/fund_pick.py`
- `src/commands/etf_pick.py`
- `src/commands/research.py`
- `src/commands/risk.py`
- `src/commands/portfolio.py`
- `src/commands/compare.py`
- `src/commands/briefing.py`
- `src/commands/lookup.py`
- `src/commands/assistant.py`

可用但仍在迭代：

- `src/commands/discover.py`
- `src/commands/policy.py`
- `src/processors/decision_review.py`
- `src/scheduler.py`
- `src/commands/strategy.py`

弱或占位：

- `src/collectors/policy.py`
- `src/collectors/social_sentiment.py`
- `src/collectors/global_flow.py`
- scheduler 的持久化和运维监控层

## Current Priority Backlog

1. `Tushare 10000 分` 数据源升级与旧接口替换
   优先把新解锁的高价值接口接进 mature 主链，并替换旧的低配或不稳定路径。股票链 `ths_member / st / stk_high_shock / stk_alert / cyq_perf / cyq_chips / 个股资金流向 / 两融 / 龙虎榜-竞价-涨跌停边界 / broker_recommend / e互动` 已打通到 `scan / stock_analysis / stock_pick / briefing`；ETF 链 `etf_basic / etf_index / etf_share_size / fund_factor_pro / idx_factor_pro` 也已进入主路径。接下来默认优先做被覆盖旧链的系统性退场、`index_weekly / index_monthly` 这类已接 helper 的成熟消费，以及第二阶段 backlog（`daily_info / sz_daily_info / stk_factor_pro / stk_surv / stk_ah_comparison / fx_basic / fx_daily / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / sge_basic / sge_daily`）的正式排期。专项路线见 [docs/plans/tushare_10000.md](./docs/plans/tushare_10000.md)。
2. 事件消化与研究理解
   把财报 / 公告 / 政策 / 交易所 / IR / 媒体报道这类情报从“抓到”推进到“解释”；优先补事件分层、重要性排序、共享解释层，以及 `财报摘要 / 公告类型 / 政策影响层 + impact_summary + thesis_scope` 的正式合同。
3. 研究记忆与 thesis ledger
   让系统能持续写清“上次为什么看、这次什么变了、观点是否升级/降级”，不再每篇都像从零开始。
4. `strategy` 下沉为后台置信度层
   当前不是继续扩独立报告存在感，而是把它压成 `pick / analysis` 背后的历史验证状态、退化提醒和排序置信度。
5. 连续跟踪与监控
   做观察名单、触发器命中、事件日历、旧稿复查和 thesis 变更提醒，把“点状出稿”推进成“连续研究”。
6. 组合联动与研究置信度收敛
   继续把单篇判断映射到主题重复度、风格暴露、建议冲突和组合优先级。
7. `policy` v2
   继续提升扫描版、表格重 PDF/OFD 的抽取和 taxonomy。
8. proxy signals repo-wide 收口
9. `scheduler` v2
10. 校准与学习
11. 外审能力扩展
12. 强因子维护模式

## Recent Contract Summary

这里只保留会影响默认开发判断的短摘要；逐日 log 已移到 history 文档。

- 2026-04-01：情报链 v1 已收口到 mature 主链，`briefing / scan / stock_analysis / stock_pick / etf_pick / fund_pick` 都已统一到 client-safe 口径与多条相关情报合同。
- 2026-04-01：`briefing` 已转成“情报总台”写法，优先广覆盖、多外链、多源情报，不再只靠单条 lead item。
- 2026-04-01：`briefing` 的主情报顺序已进一步收紧为 `A股盘面/热股/观察池 > 外部新闻补充`；盘面行默认带 `信号类型 / 强弱 / 结论`，`briefing_light` 也改成国内盘面优先，避免 Reuters/Bloomberg 宏观标题继续挤掉 A 股热点。
- 2026-04-01：`briefing / market_drivers / market_pulse` 已补第一版 freshness 合同：`industry_spot / concept_spot / hot_rank / zt_pool / strong_pool / dt_pool` 现在都会显式带 `as_of / latest_date / is_fresh`，`briefing` 只会把 fresh same-day 的 A 股盘面快照前置成“今日信号”；这类高速盘面源也已禁用抓取失败后的 stale cache 回退，避免旧盘面冒充今天结论。
- 2026-04-01：上面这条 freshness 合同又继续收紧到“同日快盘面不复用缓存”：`industry_spot / concept_spot / hot_rank / zt_pool / strong_pool / dt_pool` 在当天默认 live fetch，不再复用盘中缓存冒充收盘后盘面；`briefing` 的 `_timed_collect` 也改成 daemon-thread timeout，避免超时降级后后台线程继续拖住 `client-final`。
- 2026-04-01：用户把 Tushare 升到 `10000` 分后，`briefing / market_drivers` 已改成真实优先走 `ths_index + ths_daily` 的同花顺主题/行业盘面，不再默认把 `AKShare board spot` 当主链；`ths_hot` 也不再错误按 `market=A / trade_date` 调空结果，而是改成“全量拉取 -> 过滤 A 股 -> 取最新交易日 + 最新时点”的热股快照。
- 2026-04-03：`10000` 分升级仍是 repo 级优先主线；股票专题已基本收口，ETF 主链的 `etf_share_size / fund_factor_pro / idx_factor_pro` 也已进入共享 collector + processor + renderer + guard 路径，`etf_pick / scan / compare` 已能同时写出 `跟踪指数 + 份额变化 + 场内基金技术状态 + 外部情报状态`。默认下一步优先做已被覆盖旧链的系统性退场，以及 `index_weekly / index_monthly` 的成熟消费。
- 2026-04-02：股票链已把 `broker_recommend` 接进共享 collector，并下沉到 `scan / stock_analysis / stock_pick / briefing` 的 `卖方共识热度 / 卖方一致预期过热 / 推荐理由 / 风险提示 / 观察点`；历史月份名单现在显式按 `stale` 披露，不会再伪装成 fresh 共识升温。
- 2026-04-02：股票链已把 `上证e互动 / 深证互动易` 接进共享 `news` collector，并下沉到 `scan / stock_analysis / stock_pick / briefing` 的 `结构化事件 / 关键证据 / 事件消化 / What Changed`；即使全局新闻走 `proxy/empty`，个股级 `e互动` 也会继续拉取，但只按“补充证据”处理，不会冒充正式公告或 same-day fresh 强催化。
- 2026-04-01：ETF 链已开始切到 ETF 专用合同：`fund_profile` 现在会补 `etf_snapshot`，显式带 `跟踪指数 / 指数代码 / 交易所 / ETF类型 / 最新份额规模`；`scan` 与 `etf_pick` 的客户稿也已把 `基金画像 / ETF专用信息 / 持仓 / 行业暴露` 作为保功能门禁的一部分，不允许后续开发静默吞掉。
- 2026-04-03：ETF phase 2 的 `fund_factor_pro` 已接进共享 `fund_profile` collector，并下沉到 `opportunity_engine / etf_pick / scan / client_report / opportunity_report`；客户稿里的 `场内基金技术状态` 现在会显式写出 `趋势/动能 + 日期`，`release_check` 也已补保功能门禁，后续改动不能再把这块静默吞掉。
- 2026-04-03：ETF phase 2 的剩余缺口也已收口：`etf_share_size` 默认会拉近 7 个开盘日做两点变化，`etf_pick` 的“为什么先看它”和 `compare` 的 `ETF产品层对比` 会把 `跟踪指数 + 份额变化 + 外部情报状态` 一起写出来；`config/config.etf_pick_fast.yaml` 也改成默认保留 `light fund_profile`，不再把 ETF 产品层静默关掉。
- 2026-04-03：`briefing / stock_pick` 的正式稿现已把 `broker_recommend` 抬到可见证据层；`e互动` 能力也已接进主链，但只在当天真实命中 IR 证据时才允许写进 final，不要为了“展示新功能”硬塞假条目。
- 2026-04-03：`stock_analysis` 的 PDF 首屏布局已补分页：首页判断层与 `## 一句话结论` 之间默认强制打印分页，避免 PDF 首页把执行摘要挤成一团。
- 2026-04-03：`briefing / market_event_rows` 的盘面标签已补正负过滤：负涨幅行业和概念不再被写成 `走强 / 领涨`，共享层会按 `承压` 处理。
- 2026-04-03：`Tushare 10000 分` 计划已补出第二阶段 backlog：`daily_info / sz_daily_info / stk_factor_pro / stk_surv / stk_ah_comparison / fx_basic / fx_daily / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / sge_basic / sge_daily` 当前都属于“已解锁、未接、也未排期”的下一批候选接口。
- 2026-04-01：`briefing --client-final` 现在默认关闭 query-group 搜索回填，优先用 `Tushare market intelligence + 广覆盖 RSS` 完成情报补齐，避免晨报正式稿再次被搜索慢链拖住。
- 2026-04-01：`briefing` 这套 `signal_type / signal_strength / signal_conclusion` 现已下沉到共享 `event_digest`；`scan / stock_analysis / stock_pick / etf_pick / fund_pick` 的首页 `关键新闻 / 关键证据`、正文 `事件消化` 和证据行都会显式写出“这条情报是什么、强弱如何、当前偏什么结论”，不再只贴标题。
- 2026-04-01：`stock_pick` observe-only 信息密度已补回，重新保留 `第二批`、`低门槛继续跟踪`、`代表样本复核卡`。
- 2026-04-02：股票链第一批 `ths_member + st / stk_high_shock / stk_alert` 已接进共享 collector 合同，并下沉到 `scan / stock_analysis / stock_pick / briefing`；这批字段默认都带 `source / as_of / fallback / disclosure`，权限失败/空表/频控时按缺失处理，不再把空结果写成 fresh 命中或“已通过”。
- 2026-04-02：股票链第二批 `cyq_perf / cyq_chips` 已接进共享筹码快照合同，并下沉到 `scan / stock_analysis / stock_pick / briefing`；如果最新可用筹码日期不是当前交易日，就必须显式标成 `stale / 非当期`，不能把旧筹码写成当天资金确认或 fresh 命中。
- 2026-04-02：股票链第三批已把 `moneyflow / margin_detail / top_list / top_inst / stk_auction / stk_limit / limit_list_d` 接进共享 collector 合同，并通过 `ths_member + moneyflow_ind_ths / moneyflow_cnt_ths` 做个股主题资金流代理；`scan / stock_analysis / stock_pick / briefing` 现在都会真实消费 `个股资金承接 / 两融拥挤 / 龙虎榜-竞价-涨跌停边界`，把它写进排序、推荐理由、风险提示、关键证据和事件消化。
- 2026-04-02：上面这批股票 P1 数据默认都带 `source / as_of / latest_date / fallback / disclosure`；权限失败、空表、频控或非当期明细会显式降成 `缺失 / stale / 观察`，不再把旧两融、旧资金流或空的打板专题写成当天 fresh 命中。
- 2026-04-02：指数/行业标准链第一批已接进共享 `industry_index` collector：`market_drivers.industry_spot` 现在优先走 `申万行业指数 -> 中信行业指数 -> ths 行业盘面 -> AKShare board`，不再让 `AKShare board spot` 充当默认行业主链；`scan / stock_analysis / stock_pick / etf_pick / briefing` 也开始直接消费 `申万/中信行业框架`，不再主要靠 `sector / chain_nodes / 板块名字符串` 模糊匹配。
- 2026-04-02：指数专题主链第二批已接进共享 `index_topic` collector：`index_basic / index_dailybasic / index_weight / index_daily / idx_factor_pro / index_global` 现在会统一进入 `valuation / market_cn / market_overview`，并继续下沉到 `scan / stock_analysis / stock_pick / etf_pick / briefing`；这批路径默认都带 `source / as_of / is_fresh / fallback / disclosure`，不再把 `AKShare` 或 `yfinance` 写成默认指数主链。
- 2026-03-31：`scan / etf_pick / fund_pick / stock_analysis / stock_pick` 的内部 miss 诊断语已从客户稿清掉，空情报窗口统一改成 client-safe 表达。
- 2026-03-29：连续研究主线已从“有状态”推进到“有队列 / 有复查动作 / 有 What Changed / 有 thesis_state.v1”。
- 2026-03-28：`strategy` 多标的 validate / experiment 已真实落到正式稿，但当前定位仍是后台置信度层，而不是独立大产品。

## Strategy Status

`strategy` 当前已有第一版闭环：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

但它仍是窄合同：

- 只做 A 股高流动性普通股票
- 主目标固定为 `20d_excess_return_vs_csi800_rank`
- replay / experiment 已支持 multi-symbol cohort，但仍不是全市场截面策略引擎
- experiment 只比较预定义 challenger，不允许直接反哺生产链路

详细合同见 [docs/plans/strategy.md](./docs/plans/strategy.md)。

## External Review Rules

- 外审永远不是一次性 checklist。
- 不要停在“缺 review 文件”的口头汇报；正式成稿链默认要把 review 文件补出来再继续。
- 用户明确要 `final` 时，不要向用户回报“还缺 review 文件”；先把外审补到可交付或明确真实阻塞，再汇报结果。
- 如果知道 `final` 目标路径，就按该路径补 `__external_review.md`，然后继续修正 / 再审；缺文件本身不是停止条件。
- 对 `stock_pick / etf_pick / fund_pick`，如果整份稿件没有可执行动作，外审和成稿都必须显式按 `观察稿 / 今日无正式推荐` 处理，不能继续使用 `推荐 / 核心主线 / 低门槛可执行` 这类包装。
- 所有外审都必须：
  - 合同审
  - 发散审
  - round-based 收敛

常用 prompt：

- 研究问答：`docs/prompts/external_research_reviewer.md`
- 正式报告：`docs/prompts/external_financial_reviewer.md`
- 通用收敛循环：`docs/prompts/external_review_convergence_loop.md`
- `strategy` 计划专项：`docs/prompts/external_strategy_plan_reviewer.md`
- 强因子计划专项：`docs/prompts/external_factor_plan_reviewer.md`

## What To Run

常用命令：

```bash
python -m src.commands.scan 561380
python -m src.commands.research 561380 现在还能不能买
python -m src.commands.portfolio whatif buy 561380 2.1 20000
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
```

默认测试顺序：

1. 先跑相关 narrow tests
2. 再跑 `pytest -q`

如果只是改 `strategy`，先跑：

```bash
pytest tests/test_storage/test_strategy_storage.py tests/test_commands/test_strategy_command.py tests/test_processors/test_strategy_processor.py tests/test_output/test_strategy_report.py -q
```

## Guardrails

- 不要为了生成输出而削弱 `release_check.py` 或 `report_guard.py`
- 如果功能是 proxy-based 或降级路径，必须在输出里写清
- 如果 CLI 声称支持某种输入，renderer 和 guard 也必须承认这个合同
- 静默行为错配比明显 TODO 更糟，优先修

## Where Detail Lives

- 任务入口地图：[docs/context_map.md](./docs/context_map.md)
- YAML 地图：[config/README.md](./config/README.md)
- 当前状态细节：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 路线图总览：[plan.md](./plan.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
- 详细变更归档：
  - [docs/history/2026-04.md](./docs/history/2026-04.md)
  - [docs/history/2026-03.md](./docs/history/2026-03.md)
