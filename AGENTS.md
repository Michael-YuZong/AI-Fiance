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
- 高频 workflow skills：看 `[.codex/skills/ai-finance-report-final/SKILL.md](./.codex/skills/ai-finance-report-final/SKILL.md)`、`[.codex/skills/ai-finance-tushare-rollout/SKILL.md](./.codex/skills/ai-finance-tushare-rollout/SKILL.md)`
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
   优先把新解锁的高价值接口接进 mature 主链，并替换旧的低配或不稳定路径。股票链 `ths_member / st / stk_high_shock / stk_alert / cyq_perf / cyq_chips / 个股资金流向 / 两融 / 龙虎榜-竞价-涨跌停边界 / broker_recommend / e互动` 已打通到 `scan / stock_analysis / stock_pick / briefing`；ETF 链 `etf_basic / etf_index / etf_share_size / fund_factor_pro / idx_factor_pro` 也已进入主路径。默认分析合同现已分叉：`cn_stock` 先看公司 + 板块/主题/行业行情与资金承接，不默认跑指数专题；`cn_etf / cn_index / 被动或联接类 cn_fund` 继续保留指数主线、`周月节奏` 和跟踪指数技术状态；主动基金回到基金经理、持仓和风格暴露主线。接下来默认优先做被覆盖旧链的系统性退场、`index_weekly / index_monthly` 这类已接 helper 的成熟消费，以及第二阶段 backlog（`daily_info / sz_daily_info / stk_factor_pro / stk_surv / stk_ah_comparison / fx_basic / fx_daily / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / sge_basic / sge_daily`）的正式排期。专项路线见 [docs/plans/tushare_10000.md](./docs/plans/tushare_10000.md)。
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

这里只保留会影响默认开发判断的短摘要；逐日 log 放到 history 文档，不再堆在这里。

- `briefing / scan / stock_analysis / stock_pick / etf_pick / fund_pick` 现在都按“情报链”口径写作：多条相关情报、`signal_type / signal_strength / signal_conclusion`、`主要影响 / 传导`、client-safe 空情报窗口已进入共享层；首页关键情报不能只贴标题，必须写清偏利多/偏利空/中性、先影响什么、再看什么验证。
- 个股 IR / 互动易 / e互动 / 投资者问答属于公司管理层口径，默认归入 `公告类型：投资者关系/路演纪要`；只有真实政策主体或政策文件才允许写成 `政策影响层`，不要把公司产品问答包装成政策催化。
- `briefing / market_drivers / market_pulse` 已承认 same-day freshness 合同；空 frame 不再写成 fresh，负涨幅行业/概念也不会再被写成 `走强 / 领涨`。
- `Tushare 10000 分` 第一阶段已基本收口：股票、指数/行业、ETF 非实时主链都已切到 `Tushare-first`；当前重点改成退已被覆盖的 `AKShare` 旧链、统一 `index_weekly / index_monthly` 的可见写法，并推进第二阶段 backlog。
- 默认分析合同已正式分叉：个股主链不再把指数专题当默认补充层，优先写公司、板块/主题、行业行情和资金承接；ETF / 指数 / 被动或联接类基金保留 `index_topic_bundle / 周月节奏 / 跟踪指数技术状态` 这套指数主线；主动基金回到基金经理、持仓和风格暴露主线。这套主动/被动判断现已抽成共享 helper，并已下沉到 `scan / stock_analysis / fund_pick / compare / renderer`，后续不要再在命令层各写一版启发式。
- `stock_analysis / scan` 的单标的个股 detailed 现在明确拆成两层：首屏继续只回答 `看不看 / 怎么触发 / 多大仓位 / 哪里止损`，正文前段新增 `公司研究层判断`，单独解释“公司/赛道还值不值得跟”“基本面低分怎么理解”“为什么还不适合中线重仓”。`基本面低分` 默认解释成当前位置/性价比不足，不再默认等同于公司价值判死；同时个股观察稿首屏也必须直接落当前触发位 / 观察仓上限 / 失效位，不再允许退回“先别给精确买点 / 不先给机械止损位”这类泛化提示；ETF / 基金 detailed 仍不进入这层公司研究口径。
- 个股聚合层现在有硬 gate：`technical < 30 / catalyst < 20 / risk < 20` 任一项未过时，`cn_stock` 不能输出 `较强机会 / 强机会`；两项及以上未过直接 `无信号`，单项未过封顶观察级。这条合同已同步到 `trade_state / action_plan / discover_next_step / release_check / report_guard`，不要再补回“基本面或相对强弱单维度把弱信号抬成推荐”的捷径。
- `etf_pick --client-final` 当前已回到单一路径：不再复用 same-day internal/payload 快路径，也不再对入围 ETF 逐只做 full reanalysis 慢链；现在统一走 `discover -> hydrate full profile for finalists -> refresh shared ETF report fields -> finalize/export`。后续如果要提速，优先修共享 export/runtime，不要再补回旧 bundle 复用分支。
- `stock_pick / briefing --client-final` 当前也已回到单一路径：仍会落 `payload / editor_payload / prompt` 侧车做审计与 continuity，但默认不再复用 same-day `internal -> finalize/export` 或 `editor fallback` 捷径。后续如果要提速，优先修共享 collector/runtime/export，不要再补回私有复用分支。
- `stock_pick --client-final` 的 discovery / preview 快路径已明确分层：预筛阶段会跳过逐票 direct news、龙虎榜/竞价/涨跌停、行业指数逐票补查、资金流行业/概念代理和 IR/调研慢结构化接口，只保留轻量高价值结构化源；入围补强再补公司级情报。不要再把这些慢链补回 discovery。
- `stock_pick / stock_analysis / scan` 的 `theme_playbook` 现在默认按“结构化 metadata 优先”收主题：先看 `sector / industry / industry_framework_label / tushare_theme_industry / chain_nodes`，只有 `day_theme` 与个股真实行业对齐时，才允许把它抬成个股主线；`business_scope` 这类超长经营范围默认不再直接参与主题归因，避免把 `旅游 / 餐饮 / 再生资源` 之类噪音误写成个股主题边界。
- `stock_analysis / scan` 的单标的直跑链这轮也补上了同一套 shared contract：`cn_stock` 在已有标准行业时，`_merge_metadata(...)` 先按 `industry / sector` 做 broad sector 和 chain 节点归因，只有行业仍缺失时才让 `main_business / business_scope / company_intro` 做 fallback。`000792` 的真实 spot check 已确认这能把错误的 `光伏主链` 拉回 `农业 / 种植链`，且 `client-final` 的 `事件消化 / What Changed / 分数口径` 已重新对齐并可交付。
- `stock_analysis / scan` 的主题收口这轮又补了一层“行业层旧 context 不得压住公司画像重算”合同：如果现成 `theme_playbook` 还只是 sector-level，占位旧上下文不能直接复用；共享 `_subject_theme_context(...)` 必须结合 `business_scope / company_intro` 再重算一次。同样重要的是，新算出来的细主题只有落在该硬行业的默认 bridge 里才允许升级；否则继续退回行业层，避免 `600096` 这类农业票被公司简介里的 `新能源 / 新材料` 反向带偏。真实 spot check 里，`002709` 已能从泛 `材料` 升成 `锂电`，而 `600096 / 000792` 仍稳定停在 `农业 / 种植链`。
- `etf_pick` 这轮也补上了 ETF 专属主题归因与 payload 合同：`winner` 现在必须把 `fund_profile / theme_playbook` 一起写入 `payload/editor_payload`，不能只留 `fund_sections`；共享 `_subject_theme_context(...)` 对 `cn_etf / cn_fund / cn_index` 会优先按 `跟踪指数 / 业绩比较基准 / sector / industry_framework_label` 识别主题，不再让 `chain_nodes` 里像 `消费电子零部件及组装 / 电子` 这类噪音把消费 ETF 首页误写成 `信息技术 / AI硬件链`。
- ETF phase 2 已进入稳定消费：`etf_share_size / fund_factor_pro / idx_factor_pro` 已能在 `etf_pick / scan / compare` 中同时写出 `跟踪指数 + 份额变化 + 场内基金技术状态 + 外部情报状态`。
- `broker_recommend` 与 `上证e互动 / 深证互动易` 已接进共享主链；正式稿只在真实命中时展示，不允许为了“展示新功能”硬塞假证据。
- `stock_pick observe-only`、`ETF/基金画像`、`持仓/行业暴露` 等高价值块已进入保功能门禁；任何开发改动默认都不能静默吞掉它们。

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
