# AI-Finance Agent Handoff

## Default Read Order

不要一开工就扫完整个仓库。

默认只按下面顺序读：

1. 这份文件
2. [README.md](./README.md)
3. [docs/context_map.md](./docs/context_map.md)
4. 你要修改的 command / processor / renderer / test

只有任务相关时再继续读：

- 配置问题：看 [config/README.md](./config/README.md)
- `strategy`：看 [docs/plans/strategy.md](./docs/plans/strategy.md)
- 强因子维护：看 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- 更完整的当前状态：看 [docs/status_snapshot.md](./docs/status_snapshot.md)
- 更细历史变化：看 [docs/history/2026-03.md](./docs/history/2026-03.md)
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

1. `strategy` fixtures and governance
   已有 `predict / list / replay / validate / attribute / experiment`；`benchmark fixture`、`lag / visibility fixture`、`overlap fixture`、`promotion / rollback gate`、`out-of-sample / chronological cohort validate`、`cross-sectional validate`、`multi-symbol replay / experiment`、`config-driven batch symbol source / cohort recipe` 已补成结构化合同，下一步是更长窗口上的 promotion calibration / external review。
2. `policy` v2
   继续提升扫描版、表格重 PDF/OFD 的抽取和 taxonomy。
3. Proxy signals
   把代理置信度、覆盖、限制和降级影响继续统一到更多 final / manifest / audit。
4. `scheduler` v2
   做持久化 run history、失败可见性和运维状态。
5. 校准与学习
   深化 setup bucket、归因、长期月度学习闭环。
6. 外审能力扩展
   在现有 `review_ledger / review_audit` 上继续扩 evidence / point-in-time / regression / attribution 审计。
7. 强因子进入维护
   阶段 J 按 `v1 已收口` 管理，剩余 point-in-time / lag / calibration 归入其他 backlog。

## Recent Changes Summary

- 2026-03-24
  `stock_analysis` 的种业个股外审把一个共享边界正式收口：以后做 A 股单标的发散审，必须显式拆开 `宏观背景 / 主题逻辑 / 个股直接催化 / 正式动作触发` 四层，不能再把宏观晨报或行业早报冒充成 `龙头公告/业绩` 或个股级直接催化；对应 lesson 和 prompt 已更新，processor 侧也已锁住只有命中个股级 leader items 才能给 `龙头公告/业绩` 加分。
- 2026-03-23
  `scan / stock_analysis / stock_pick / etf_pick / fund_pick / briefing` 的 `client-final` 导出链已开始走共享 final runner：现在统一复用 `detail 写盘 -> release_check -> review guard -> markdown/html/pdf/manifest 导出` 这套编排，不再让各命令各写一套 `export_reviewed_markdown_bundle` 流程；后续继续收 timeout、review scaffold 和 artifact logging 时，只需要改共享层。
- 2026-03-23
  共享 final runner 现在也会处理 review scaffold / auto-close：缺 `__external_review.md` 时会先自动补首轮 scaffold；如果现有 review 正文三段都已无 actionable finding、只是还没形成 round-based PASS 闭环，会自动归档当前 round 并补一轮 `PASS` 收敛记录，减少正式稿继续卡在 review 文本细节上。
- 2026-03-23
  `Tushare` 已补成 repo 级超时合同：`BaseCollector._tushare_pro()` 现在会按 `tushare_timeout_seconds` 统一初始化客户端超时，避免只有 `briefing` 外层做线程超时兜底。同时 client HTML 表格已修正 markdown 转义 `\|` 的解析，`证据时点与来源` 里的 `数据覆盖` 不会再被错误拆成多列。
- 2026-03-23
  `strategy validate / experiment` 的正式稿现在会固定前置 `这套策略是什么 / 这次到底看出来什么 / 执行摘要`：先解释它是不是具体策略、这份报告到底在回答什么、现在能不能用/能不能切换，再往下展开 fixture、gate 和样本细节；`report_guard / release_check` 也已把这三段收成正式合同。
- 2026-03-23
  `strategy validate / experiment` 现在已接入正式 `client-final` 交付链：命令层承认 `--client-final`，会走 `report_guard / release_check / client_export` 产出 `markdown + html + pdf + release_manifest`；正式成稿当前只承认这两类，不把 `predict / replay / attribute` 包装成对外交付稿。若缺 `__external_review.md`，命令会先自动在 `reports/reviews/strategy/...` 下生成首轮 BLOCKED scaffold，再要求补齐双 reviewer 外审。
- 2026-03-23
  `etf_pick / fund_pick` 的正式交付口径继续收紧：如果排第一的标的本身仍是 `观察 / 暂不出手 / 持有优于追高`，交付层现在会直接退成 `观察优先稿`，不会再把 `推荐` 标题和 `观察` 动作混写；`做多` 这类方向偏向也会和 `当前建议` 分开写，避免出现 `做多；观察为主` 这种误导动作。
- 2026-03-23
  `ETF / 基金` pick 的 `筹码结构` 已明确降成展示层 `辅助项`，不再在客户稿里渲染成会干扰判断的硬分数；release_check 和外审 prompt 也会把“辅助项硬打分”“本机绝对图片路径”“持仓名称空白”“模板句重复过多”当成正式阻塞或高优先级 finding。
- 2026-03-22
  `strategy` 的 `predict / replay / validate / attribute / experiment` 输出已补成更像研究稿的结构：现在顶部会固定给 `执行摘要`，先把“当前判断、这意味着什么、最主要问题、下一步”讲清楚，再往下展开 fixture、表格和账本细节，不再一上来就是偏调试口径的 ledger dump。
- 2026-03-22
  `strategy` 已补上第一版 `config-driven batch symbol source / cohort recipe`：`replay / experiment` 现在支持直接从 `config/strategy_batches.yaml` 读取 batch source 和 cohort recipe；命令层已承认 `--batch-source / --cohort-recipe`，也支持不手输 symbols 直接跑多标的批次；summary 会显式展示 `Batch Source / Cohort Recipe`，而 replay row 里的 `asset_reentry_gap_days` 也会真实回写，不再只是 CLI 表面参数。
- 2026-03-22
  `strategy` 已补上第一版 `overlap fixture`：`predict / replay / validate / experiment` 现在会显式披露样本窗口、required gap 和 primary window overlap；如果 replay 样本彼此重叠，会把这层边界写进 summary / notes，并给出 `overlap_fixture_blocked` 标记，不再只剩 `overlap_policy` 这条静态字符串。
- 2026-03-22
  `strategy` 已补上第一版 `lag / visibility fixture`：`predict` 现在会把因子层的 `lag / visibility / point-in-time` 就绪状态汇总成独立 fixture 并显式展示；如果可用的 point-in-time strategy candidate 因子为 0，会直接退回 `no_prediction`。`replay / validate / experiment` 也会带出这层 fixture，但对当前 `price-only replay` 会明确标记为 `not_applicable`。
- 2026-03-22
  `scan` 观察型成稿现在会把“方向没坏但还不能动”的信息前置写清：`无信号` 这类标题在中期逻辑仍在时会自动软化成 `中期逻辑未坏，短线暂无信号`，`当前判断` 会多写一行 `为什么还不升级` 和 `升级条件`，`催化面` 也会拆成 `直接催化 / 舆情环境` 两层，不再把纯热度误写成已经形成直接催化；`季节/日历` 样本很薄时会显式降成辅助参考口径。
- 2026-03-22
  上面这套观察型口径现在已经下沉到 `stock_pick / etf_pick / fund_pick`：ETF/基金 pick 的 `dimension_rows` 不再各写各的旧摘要，而是复用共享的维度 summary；观察稿会显式写 `为什么还不升级 / 升级条件`；个股观察稿和代表样本 appendix 的 `催化拆解` 也开始固定先拆 `直接催化 / 舆情环境` 两层。
- 2026-03-22
  外审 prompt 现在要求像投研机构一样补一轮 `逐段/逐节审稿`：不只做 checklist 和框架外发散，还要按成稿顺序逐段判断“这段在解决什么问题、有没有真的解决、是否被后文表格推翻”。同时把 `ETF/基金 标签-基准-持仓一致性`、`观察稿升级触发器`、`nan/检查符号误导` 固化成长期审稿 lesson。
- 2026-03-22
  `scan / stock_pick / etf_pick / fund_pick` 的客户稿开头现在统一补了高密度 `执行摘要 / 今日动作摘要`：会先给 `当前建议 / 置信度 / 适用周期 / 空仓怎么做 / 持仓怎么做 / 首次仓位 / 主要利好利空`，不再把动作、仓位和风险拆散到后文多个章节里。
- 2026-03-22
  `scan` 的港股科技 ETF 分析链路已补齐三条硬合同：`宏观敏感度` 现在按自身满分做 summary 阈值，不再把 `30/40` 误写成逆风；命中精确基准但缺 PE 时，不再回退到不相干主题指数，而是优先保留精确基准并在可用时回填 `前五大重仓加权PE`；验证点里的 `关键支撑` 现在只允许取低于现价的真实支撑位，避免把上方均线误写成支撑。
- 2026-03-21
  `stock_pick / scan / stock_analysis` 现在会显式写出 `相对强弱基准`；同时 `历史相似样本` 在 `95%区间` 明显跨过中性线或样本质量偏弱时，会自动退成附注口径，不再默认占用完整验证篇幅。
- 2026-03-21
  `stock_pick` 的催化面不再把 `高管/股东净减持` 这类负面结构化事件误记成正向结构化催化；这类事件现在只允许作为负面/谨慎信号处理。
- 2026-03-21
  `stock_pick / scan / stock_analysis` 的催化面已补成 `A股个股` 的行业/主题差异化权重矩阵：通用行业画像现在会按 `科技 / 军工 / 能源 / 高股息 / 医药 / 消费` 重配催化子项上限，`半导体 / 电网 / 黄金 / 有色` 这类主题画像还能再做细分覆盖；当前仍明确不扩到港美股。
- 2026-03-21
  `stock_pick` 客户稿如果当天没有任何达到动作阈值的候选，现在会显式退成观察稿，不再继续输出 `推荐 / 核心主线 / 低门槛可执行` 这类容易把观察名单伪装成交易建议的包装。
- 2026-03-21
  `briefing market` 已补成结构化大盘分析：现在会固定分析 `上证指数 / 中证核心(沪深300) / 创业板指`，显式给出 `均线排列`、`周线/月线 MACD`、`市场宽度`、`成交量能`、`情绪极端` 和 `板块轮动`，不再只停留在概览表和零散行业描述。
- 2026-03-21
  `stock_pick / etf_pick` 的前置候选池不再是单纯按成交额 `head()` 截断；现在会在同样的候选上限内按行业保广度，减少单一热门方向把候选池挤满。
- 2026-03-21
  研究型 `final` 如果只卡在缺 `__external_review.md`，主执行者现在应继续把预期路径上的外审记录补齐，而不是停在“缺文件”汇报。
- 2026-03-22
  主执行者对用户的默认交付口径进一步收紧：用户明确要看 `final` 时，不要把“缺 `__external_review.md`”当成对用户可见结果；默认动作是先补外审、收敛、再只给正式 `final`。如果这轮先跑的是 sector/theme 过滤稿，`final / internal` 文件名也必须保留过滤范围，不能覆盖通用全市场成稿路径。
- 2026-03-22
  `stock_pick` 的无动作观察稿已切到更紧凑的成稿合同：默认不再保留完整代表样本 appendix，而是改成 `观察触发器` 清单，直接前置 `为什么继续看 / 主要卡点 / 升级条件 / 关键盯盘价位`。如果是 `--sector` 过滤稿，客户稿也会显式说明“这是主题内相对排序，不是跨主题分散候选池”。
- 2026-03-22
  正式 `final` 的外审工作流不再允许把 `结构审 + 发散审` 混成同一个 reviewer pass。现在 prompt 路由已经拆成 `Pass A: external_financial_structural_reviewer` 和 `Pass B: external_financial_divergent_reviewer`，review 记录与 `report_guard / review_audit` 也会显式要求写出 `结构审执行者 / 发散审执行者`，且两者不能是同一个 reviewer / 子 agent。
- 2026-03-22
  `stock_pick` 的观察稿现在进一步把“正式动作阈值”写成贴近真实分层逻辑的解释：先说明什么情况下能从 `观察为主` 升到 `看好但暂不推荐`，再说明即使分层接近放行，只要动作栏仍是 `暂不出手 / 观察为主 / 先按观察仓`，也还不算正式动作票。
- 2026-03-22
  `etf_pick / fund_pick` 的观察稿也开始复用同一套“桥接句 + 正式动作阈值 + 结构化覆盖优先解释”合同：现在会先讲“方向还在，但还不是正式动作”，再把 `交付等级 / 当前结论 / 数据完整度` 如何共同决定是否可执行写清。
- 2026-03-22
  `strategy` 已补上第一版 `promotion / rollback gate`：`experiment` 现在会正式产出结构化 `promotion_gate`，区分 `blocked / stay_on_baseline / queue_for_next_stage`；`validate / experiment` 也都会显式给出 `rollback_gate`，区分 `blocked / hold / watchlist / rollback_candidate`，不再只给统计表和口头边界。
- 2026-03-22
  `strategy` 已补上第一版 `out-of-sample / chronological cohort validate`：`validate` 现在会固定切出 `development / holdout` 的 out-of-sample 对比，并显式给出 `blocked / stable / watchlist`；同时也会拆 `earliest / middle / latest` cohort，比对 latest 是否相对 earliest 退化。`experiment` 的 `promotion_gate` 也开始正式承认 variant 的 out-of-sample 状态，不再只看 aggregate 均值。
- 2026-03-22
  `strategy` 已补上第一版 `cross-sectional validate`：`validate` 现在会在同日多标的 cohort 足够时，显式计算 seed score 与 realized excess return 的横截面 rank correlation，以及高分组相对低分组的 spread；如果账本里还没有足够的同日多标的 cohort，会明确标记为 `blocked`，不再把单标的结果包装成横截面 rank 证明。
- 2026-03-22
  `strategy` 已补上第一版 `multi-symbol replay / experiment`：`replay` 现在可以一次生成多标的样本供给，并显式展示 `Symbol Coverage` 和 `Same-Day Cohorts`；`experiment` 也已经扩到多标的 cohort，`promotion_gate` 会同时承认 `out-of-sample` 和 `cross-sectional` 状态，不再只看单标的 aggregate 平均值。
- 2026-03-21
  `strategy` 的 `benchmark fixture` 已补成第一版结构化合同，`predict / replay / validate / experiment` 都能明确披露 benchmark 窗口、overlap、as_of 对齐和未来验证窗 readiness。
- 2026-03-21
  `briefing / research / retrospect` 已开始显式承认 `proxy_contract`，不会再把代理说明散落在不同段落里。
- 2026-03-21
  `stock_pick / etf_pick / fund_pick` 的客户稿和 final manifest 已开始正式带出 `proxy_contract`。

更细的日级变更记录见 [docs/history/2026-03.md](./docs/history/2026-03.md)。

## Strategy Status

`strategy` 现在已经有第一版闭环：

- `predict`
- `list`
- `replay`
- `validate`
- `attribute`
- `experiment`

但它仍是窄合同：

- 只做 A 股高流动性普通股票
- 主目标固定为 `20d_excess_return_vs_csi800_rank`
- replay / experiment 已支持 multi-symbol cohort 供给，但仍不是全市场截面策略引擎
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
- 只有满足收敛条件才允许停：
  - 连续两轮无新增 P0/P1
  - 上一轮阻塞项已关闭或降级
  - 没有新的实质性发散问题
  - 合理 finding 已经沉淀到 prompt / 规则 / tests / backlog 至少一层

常用 prompt：

- 研究问答：`docs/prompts/external_research_reviewer.md`
- 正式报告：`docs/prompts/external_financial_reviewer.md`
- 通用收敛循环：`docs/prompts/external_review_convergence_loop.md`
- `strategy` 计划专项：`docs/prompts/external_strategy_plan_reviewer.md`
- 强因子计划专项：`docs/prompts/external_factor_plan_reviewer.md`
- 可迁移 kit：`docs/review_kit/README.md`
- ledger/index：`python -m src.commands.review_ledger`
- governance audit：`python -m src.commands.review_audit`

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
- 详细变更归档：[docs/history/2026-03.md](./docs/history/2026-03.md)
