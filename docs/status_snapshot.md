# Status Snapshot

这份文件只回答四件事：

1. 现在哪些功能最成熟
2. 当前主线 backlog 是什么
3. 现在最容易误判的边界是什么
4. 最近有哪些会影响开发判断的变化

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

- proxy signals 仍是代理，不是原始全量 feed；虽然 `pick / briefing / research / retrospect` 已开始显式披露，但 repo-wide point-in-time 完整度仍在补
- `strategy` 已有多标的 replay / experiment 与 cross-sectional validate，但仍不是全市场截面策略引擎
- `scheduler` 的持久化和运维监控还没做完

## 当前主线 backlog

1. `strategy` fixtures and governance
   `benchmark fixture`、`lag / visibility fixture`、`overlap fixture`、`promotion / rollback gate`、`out-of-sample / chronological cohort validate`、`cross-sectional validate`、`multi-symbol replay / experiment`、`config-driven batch symbol source / cohort recipe` v1 已完成，下一步是更长窗口上的 promotion calibration / external review。
2. `policy` v2
   继续提升扫描版、表格重 PDF/OFD 的抽取和 taxonomy。
3. proxy signals repo-wide 收口
   把代理置信度、覆盖、限制和降级影响继续统一到 final / manifest / audit。
4. `scheduler` v2
   做持久化 run history、失败可见性和运维状态。
5. 校准与学习
   深化 setup bucket、阈值、归因和长期月度学习闭环。
6. 外审能力扩展
   在现有 `review_ledger / review_audit` 之上继续扩 evidence / point-in-time / regression / attribution 审计。
7. 强因子维护模式
   阶段 J 已收口，剩余 point-in-time / lag / calibration 问题迁到其他 backlog。

## 当前不该误判的边界

- [docs/history/architecture_v2.md](./history/architecture_v2.md) 是历史文档，不是当前主合同。
- `strategy` 已可用，但它仍是窄版研究闭环，不是 production alpha engine。
- `policy` 已可用，但扫描版 / 表格重原文仍是明确降级边界。
- ETF / 基金的 `基本面` 更多是产品质量和代理映射，不应直接当成底层行业基本面确认。
- 低覆盖率稿件不应继续伪装成完整终稿，必要时应退成摘要观察稿。

## 最近重要变化

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
  `strategy validate / experiment` 现在已接入正式 `client-final` 交付链：命令层承认 `--client-final`，并会走 `report_guard / release_check / client_export` 产出 `markdown + html + pdf + release_manifest`；正式成稿当前只承认这两类，不把 `predict / replay / attribute` 伪装成对外交付稿。若缺 `__external_review.md`，命令会先自动在 `reports/reviews/strategy/...` 下生成首轮 BLOCKED scaffold，再要求补齐双 reviewer 外审。
- 2026-03-23
  `etf_pick / fund_pick` 的交付分层继续收紧：如果排第一的标的本身仍是 `观察 / 暂不出手 / 持有优于追高`，交付层现在会直接退成 `观察优先稿`，不再把 `推荐` 标题和 `观察` 动作混写；`做多` 这类方向偏向也会和 `当前建议` 分开写，避免出现 `做多；观察为主` 这类会误导执行的摘要。
- 2026-03-23
  `ETF / 基金` pick 的 `筹码结构` 已明确降成展示层 `辅助项`，不再在客户稿里渲染成会干扰判断的硬分数；release_check 与结构审 prompt 也已把“辅助项硬打分”“本机绝对图片路径”“持仓名称空白”“模板句重复过多”收成正式阻塞或高优先级 finding。
- 2026-03-22
  `strategy` 的 `predict / replay / validate / attribute / experiment` 输出已补成更像研究稿的结构：顶部现在会先给 `执行摘要`，把“当前判断、这意味着什么、最主要问题、下一步”前置讲清楚，再往下展开 fixture、cohort、gate 和账本表格，不再一上来就是偏调试口径的 ledger 视图。
- 2026-03-22
  `strategy` 已补上第一版 `config-driven batch symbol source / cohort recipe`：`replay / experiment` 现在支持直接从 `config/strategy_batches.yaml` 读取批量 symbol source 和 cohort recipe，也支持只给 `--batch-source` 不手输 symbols；summary 会显式展示 `Batch Source / Cohort Recipe`，并把实际 `asset_gap_days` 回写到 replay row 的 `cohort_contract`。
- 2026-03-22
  `strategy` 已补上第一版 `multi-symbol replay / experiment`：`replay` 现在可以一次生成多标的样本供给，并显式展示 `Symbol Coverage` 和 `Same-Day Cohorts`；`experiment` 也已经扩到多标的 cohort，`promotion_gate` 会同时承认 `out-of-sample` 和 `cross-sectional` 状态，不再只看单标的 aggregate 平均值。
- 2026-03-22
  `strategy` 已补上第一版 `cross-sectional validate`：`validate` 现在会在同日多标的 cohort 足够时，显式计算 seed score 与 realized excess return 的横截面 rank correlation，以及高分组相对低分组的 spread；如果账本里还没有足够的同日多标的 cohort，会明确标记为 `blocked`，不再把单标的结果包装成横截面 rank 证明。
- 2026-03-22
  `strategy` 已补上第一版 `out-of-sample / chronological cohort validate`：`validate` 现在会固定切出 `development / holdout` 的 out-of-sample 对比，并显式给出 `blocked / stable / watchlist`；同时也会拆 `earliest / middle / latest` cohort，单独看 latest 是否相对 earliest 退化。`experiment` 的 `promotion_gate` 也开始承认 variant 的 out-of-sample 状态，不再只看 aggregate 均值。
- 2026-03-22
  `strategy` 已补上第一版 `promotion / rollback gate`：`experiment` 现在会正式产出结构化 `promotion_gate`，区分 `blocked / stay_on_baseline / queue_for_next_stage`，不再只有 champion 排名和口头备注；`validate / experiment` 也都会显式给出 `rollback_gate`，区分 `blocked / hold / watchlist / rollback_candidate`，把当前 baseline 是否该继续 hold 还是进入 rollback 讨论写成结构化合同。
- 2026-03-22
  `strategy` 已补上第一版 `overlap fixture`：`predict / replay / validate / experiment` 现在会显式披露样本窗口、required gap 和 primary window overlap；如果 replay 样本彼此重叠，会把这层边界写进 summary / notes，并给出 `overlap_fixture_blocked` 标记，不再只剩 `overlap_policy` 这条静态字符串。
- 2026-03-22
  `strategy` 已补上第一版 `lag / visibility fixture`：`predict` 现在会把因子层的 `lag / visibility / point-in-time` 就绪状态汇总成独立 fixture 并显式展示；如果可用的 point-in-time strategy candidate 因子为 0，会直接退回 `no_prediction`。`replay / validate / experiment` 也会带出这层 fixture，但对当前 `price-only replay` 会明确标记为 `not_applicable`，不再让这层边界停留在 processor 私有状态。
- 2026-03-22
  `scan` 的观察型客户稿/详细稿现在会把“中期逻辑仍在，但短线不能动”的信息更高密度地前置出来：`无信号` 在逻辑未坏时会自动软化成 `中期逻辑未坏，短线暂无信号`，`当前判断` 会额外给出 `为什么还不升级` 和 `升级条件`，`催化面` 也会拆成 `直接催化 / 舆情环境` 两层，避免把纯新闻热度误写成已经形成直接事件催化；`季节/日历` 样本过薄时会显式降成辅助参考口径。
- 2026-03-22
  这套观察型成稿合同已经同步下沉到 `stock_pick / etf_pick / fund_pick`：ETF/基金 pick 的 `这只为什么是这个分` 不再停留在 command 私有摘要，而是复用共享维度 summary；观察稿现在会显式写 `为什么还不升级 / 升级条件`；个股观察稿和代表样本 appendix 的 `催化拆解` 也会固定先拆 `直接催化 / 舆情环境` 两层。
- 2026-03-22
  外审 prompt 现在要求像投研机构一样补一轮 `逐段/逐节审稿`：不只做 checklist 和框架外发散，还要按成稿顺序逐段判断“这段在解决什么问题、有没有真的解决、是否被后文表格推翻”。同时把 `ETF/基金 标签-基准-持仓一致性`、`观察稿升级触发器`、`nan/检查符号误导` 固化成长期审稿 lesson。
- 2026-03-22
  `scan / stock_pick / etf_pick / fund_pick` 的客户稿开头现在统一补了高密度 `执行摘要 / 今日动作摘要`：会先给 `当前建议 / 置信度 / 适用周期 / 空仓怎么做 / 持仓怎么做 / 首次仓位 / 主要利好利空`，不再把动作、仓位和风险拆散到后文多个章节里。
- 2026-03-22
  `scan` 的港股科技 ETF 分析链路已补齐三条硬合同：`宏观敏感度` 的 summary 现在会按维度自身满分解释，不再把 `30/40` 误写成逆风；命中精确基准但缺 PE 时，不再回退到不相干主题指数，而是优先保留精确基准并在可用时回填 `前五大重仓加权PE`；验证点里的 `关键支撑` 现在只允许取低于现价的真实支撑位，避免把上方均线误写成支撑。
- 2026-03-21
  `stock_pick / scan / stock_analysis` 现在会显式带出 `相对强弱基准`；同时 `历史相似样本` 在 `95%区间` 明显跨过中性线或样本质量偏弱时，会自动退成附注口径，不再默认占用完整验证篇幅。
- 2026-03-21
  `stock_pick` 的催化面不再把 `高管/股东净减持` 这类负面结构化事件误算成正向结构化催化；这类事件当前只允许作为负面/谨慎信号处理。
- 2026-03-21
  `stock_pick / scan / stock_analysis` 的催化面已补成 `A股个股` 的行业/主题差异化权重矩阵：通用行业画像现在会按 `科技 / 军工 / 能源 / 高股息 / 医药 / 消费` 重配催化子项上限，`半导体 / 电网 / 黄金 / 有色` 这类主题画像还能再做细分覆盖；当前仍明确不扩到港美股。
- 2026-03-21
  `stock_pick` 客户稿如果当天没有任何达到动作阈值的候选，现在会显式退成观察稿，不再继续输出 `推荐 / 核心主线 / 低门槛可执行` 这类容易把观察名单伪装成交易建议的包装。
- 2026-03-21
  `briefing market` 现在会固定产出结构化大盘分析：覆盖 `上证指数 / 中证核心(沪深300) / 创业板指` 的 `均线排列`、`周线/月线 MACD`、`市场宽度`、`成交量能`、`情绪极端指标`，并新增独立的 `板块轮动` 区块，不再只靠概览表和主线叙事。
- 2026-03-21
  `stock_pick / etf_pick` 的前置候选池不再是单纯按成交额 `head()` 截断；现在会在同样的候选上限内按行业保广度，减少单一热门方向把候选池挤满。
- 2026-03-21
  研究型 `final` 如果只缺 `__external_review.md`，当前默认动作不再是停下来报缺失，而是继续把预期路径上的外审记录补齐并推进到收敛或明确阻塞。
- 2026-03-22
  `final` 交付口径继续收紧：用户明确要看正式成稿时，主执行者不应把“缺 `__external_review.md`”当成对用户可见的阶段结果；默认动作是先补外审、推进到收敛，再只交付正式 `final`。另外，`stock_pick` 的 sector 过滤稿现在也会把过滤范围写进 `final / internal` 文件名，避免板块稿覆盖通用全市场 final。
- 2026-03-22
  `stock_pick` 的无动作观察稿不再沿用重模板：现在会压成 `观察触发器` 清单，直接写 `为什么继续看 / 主要卡点 / 升级条件 / 关键盯盘价位`，不再默认保留完整代表样本 appendix。对 `--sector` 过滤稿，客户稿会额外提示“这是主题内相对排序，不是跨主题分散候选池”。
- 2026-03-22
  正式 `final` 的外审流转已拆成双 pass：`Pass A 结构审` 和 `Pass B 发散审` 现在是两份不同 prompt，且默认要求由不同 reviewer / 子 agent 执行。`report_guard` 与 `review_audit` 也开始要求 review 记录显式写出 `结构审执行者 / 发散审执行者`，避免同一个 reviewer 自己把结构审和发散审一次做完。
- 2026-03-22
  `stock_pick` 观察稿对“正式动作阈值”的解释继续收紧：现在会先解释评分分层如何从 `观察为主` 升到 `看好但暂不推荐`，再解释为什么即使分层接近放行，只要动作栏仍是 `暂不出手 / 观察为主 / 先按观察仓`，也不能被写成正式动作票。
- 2026-03-22
  `etf_pick / fund_pick` 的观察稿也开始显式写“桥接句 + 正式动作阈值 + 结构化覆盖优先解释”：对外会先说明“方向还在，但仍是观察资格，不等于现在就能做”，并把 `交付等级 / 当前结论 / 数据完整度` 三层一起交代清楚。
- 2026-03-21
  `strategy` 的 `benchmark fixture` 已补成结构化合同，`predict / replay / validate / experiment` 都会带出 benchmark 窗口、overlap、as_of 对齐和未来验证窗 readiness。
- 2026-03-21
  `briefing / research / retrospect` 已开始正式带出 `proxy_contract`，代理说明不再散落在证据和风险里。
- 2026-03-21
  `stock_pick / etf_pick / fund_pick` 的客户稿和 final manifest 已开始显式承认 `proxy_contract`。
- 2026-03-20
  客户稿整体更强调“先给结论，再给解释”；观察 / 回避口径会更明确写出触发条件和关键盯盘价位。
- 2026-03-18
  ETF / 基金发现链已做过一轮性能收口，瓶颈已从串行扫描转向少数深分析取数。

更细变更归档见 [docs/history/2026-03.md](./history/2026-03.md)。

## 相关入口

- 默认任务读法：[docs/context_map.md](./context_map.md)
- 路线图总览：[plan.md](../plan.md)
- YAML 地图：[config/README.md](../config/README.md)
- `strategy` 专题：[docs/plans/strategy.md](./plans/strategy.md)
- 强因子专题：[docs/plans/strong_factors.md](./plans/strong_factors.md)
