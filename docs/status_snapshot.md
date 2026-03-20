# Status Snapshot

这份文件只回答三件事：

1. 现在哪些功能最成熟
2. 现在最该继续做什么
3. 最近有哪些会影响判断的变化

## 成熟度

### 已成熟

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

### 可用但仍在迭代

- `discover`
- `policy`
- `decision_review / retrospect`
- `scheduler`
- `strategy`

### 仍偏弱或仍未统一

- proxy signals 仍是代理，不是原始全量 feed；pick 链已开始显式披露，但 repo-wide 还没完全统一
- repo-wide point-in-time 合同仍未完全统一
- `scheduler` 的持久化和运维监控还没做完

## 当前主线 backlog

1. `strategy` fixture + governance
2. `policy` v2 深化
3. proxy signals repo-wide 收口
4. `scheduler` v2
5. 更长期的校准和自学习
6. 外审能力扩展与代码质量收口
7. 强因子维护模式

强因子详细合同见 [docs/plans/strong_factors.md](./plans/strong_factors.md)，但它现在已经从主开发线切到维护模式。

当前已明确的一组 ETF/基金外审缺口，也已进入 backlog：
- 低覆盖率时不该继续输出完整终稿模板，而应退成摘要观察稿
- `回避 / 观察` 结论下不应继续保留完整交易动作表
- ETF/基金的 `基本面` 高分需要区分“产品质量/代理映射”与“真实行业基本面”
- 长期缺失的维度不能只靠 `未纳入该维度` 反复披露，评分层要补权或归一化
- 正式报告外审要补成双层：`rich prompt 结构化审稿 + 零提示发散审`
- pick / final manifest / review audit 里，代理信号的 `置信度 / 限制 / 降级影响` 需要继续往 briefing / research / retrospect 等链路扩

### 2026-03-16（阶段 J v1 收线）

- 强因子工程不再作为当前主开发主线；J-1 ~ J-5 继续保留在产品层，但按维护模式处理。
- `review_audit` 对当前 `structured-round` 外审协议审计结果为 `0 active findings`。
- `stock_pick / fund_pick / etf_pick / briefing` 的 `2026-03-16` final、external review、manifest 已全部重刷，manifest 统一带 `factor_contract`。
- `briefing` 的 A 股观察池改为复用成熟的个股发现链，不再自己维护一套平行分析循环。
- 统一导出器的 report theme 已升级：标题、表格、列表、引用和强调语句都按全局主题处理；后续所有走 `client_export` 的 HTML/PDF 默认继承这套渲染。当前按 HTML-first 处理，图片不再做按页自适应缩放，统一按稳定宽度渲染。
- 统一导出器的 HTML 现在会把本地图片嵌入成 `data:` URI，导出的单文件 HTML 可以直接分享，不再依赖作者机器上的本地绝对路径。
- 剩余长尾已迁出：
  - `J-4 EPS 修正` 的可靠 point-in-time 源接入 -> `strategy / point-in-time coverage`
  - `J-2 政策事件窗` 的 lag / visibility fixture -> `strategy / point-in-time coverage`
  - setup / breadth / 质量阈值再校准 -> `校准和自学习`

## 最近重要变化

### 2026-03-21（proxy signals 开始进入 pick 正式合同）

- `stock_pick / etf_pick / fund_pick` 现在不再只是“内部知道用了代理信号”，而是会在客户稿里显式给出 `市场风格代理 / 情绪代理` 的当前判断、置信度/覆盖、主要限制和降级影响。
- `analyze_opportunity()` 已补出稳定的 `proxy_signals` payload：市场层优先复用 `global_flow`，单标的层则用价格/量能行为生成 `social_sentiment` 代理；这层信息不再只活在 `briefing / research`。
- pick final 的 manifest 现在会带 `proxy_contract`；`review_audit` 也开始审这层，如果 `stock_pick / etf_pick / fund_pick` 的 manifest 缺少代理合同，会直接报 active finding，而不是默默放过。

### 2026-03-20（客户稿更强调“先给结论，再给解释”）

- `scan / stock_pick / etf_pick / fund_pick` 的客户稿 major section 现在会统一补一行 `先看结论`，先告诉读者“这一段只回答什么”，不再要求用户通读整段后自己总结主旨。
- `scan / stock_analysis / stock_pick / etf_pick / fund_pick` 的 `观察 / 回避` 口径动作区现在新增 `触发买点条件`：即使今天不给 `建议买入区间`，也会明确写出“什么条件出现后才值得升级成可执行方案”。当前优先落成更像交易语言的模板，例如 `回踩关键支撑不破 / MA20-MA60 向上拐头 / 放量站上前高或压力位 / 相对强弱转正`，避免报告只剩信息、没有动作帮助。
- `stock_pick` 的客户详细稿现在不再把所有观察票都展开成完整长稿：`正式推荐` 继续保留完整八维、催化、硬检查和风险拆解；`看好但暂不推荐 / 观察为主` 会压成短版卡片，只保留“为什么继续看 / 为什么不升级 / 触发条件 / 关键盯盘价位 / 证据口径”。为满足 final 门禁，观察名单同时会额外保留 1 只 `代表样本详细拆解`，让成稿里仍有完整八维合同可复核。
- 基于 [stock_pick_calibration_2026-03-20_internal.md](/Users/bilibili/fiance/AI-Finance/reports/retrospects/internal/stock_pick_calibration_2026-03-20_internal.md) 的快复盘，个股评级的两条 `⭐⭐⭐` 路径已做轻量校准：`基本面/催化主导` 路径把技术门槛从 `40` 放宽到 `35`；`趋势延续` 路径把门槛从 `tech>=60 / relative>=70 / risk>=55 / catalyst>=35` 放宽到 `tech>=55 / relative>=65 / risk>=45 / catalyst>=25`。这次只修“强趋势票被过度保守对待”的问题，不动 `⭐⭐⭐⭐` 门槛，也不把低覆盖观察稿抬成正式推荐。
- 同一批观察/回避稿现在还会额外给 `关键盯盘价位`：优先用已有的 `建议买入区间 / 建议减仓区间 / stop_ref / target_ref` 把下沿和上沿直接写出来，尽量让读者看完就知道下一步该盯哪些价位，而不是只剩抽象条件。

### 2026-03-20（推荐口径去“只会观察”）

- `stock_pick / etf_pick` 不再只在“没有正式推荐”时才给替代说法；现在默认都会显式给出 `短线优先 + 中线优先` 两档建议，让成稿不再只停在信息展示。
- `stock_pick` 的 A 股部分现在会拆成两批：第一批 `核心主线`，第二批 `低门槛可执行 + 关联ETF平替`。低门槛批次会从完整分析池里额外挑出“一手参考不超过约 1 万元”的候选；关联 ETF 平替会按个股的 sector/chain_nodes 给出更适合承接方向、但不必硬扛单票波动的主题 ETF。
- `etf_pick` payload 现在会把候选自动拆成 `short_term / medium_term` 两档，客户稿会把这两档直接渲染成执行入口，而不是只保留单一 winner 的观察描述。

### 2026-03-20（ETF / 基金推荐合同补强）

- ETF / 基金低覆盖率稿件现在会真正退成 `摘要观察稿`，不再只是 final 上挂一个“降级观察稿”标签却继续输出整套长模板。
- `回避 / 观察` 结论下的 ETF / 基金客户稿会自动裁掉 `建议买入区间 / 建议减仓区间 / 止损 / 目标 / 预演命令 / 仓位管理` 这类完整动作表，只保留 `配置视角 / 交易视角 / 重新评估条件 / 适用时段` 等必要项。
- ETF / 基金维度表里的 `基本面` 现在显式按 `产品质量/基本面代理` 披露，`筹码结构` 也按 `辅助项` 披露；评分摘要会说明这些分数更接近产品结构、跟踪机制和主题代理，不直接等同于底层行业基本面或资金确认。
- 正式报告外审合同也已补成双层：除了原来的 rich prompt 审稿，还必须额外留一段 `零提示发散审`。`report_guard` 和 `review_audit` 会把这层当成正式门禁，避免 reviewer 只按写死 checklist 找问题。
- `etf_pick / fund_pick` 的客户稿现在会显式区分 `配置视角` 和 `交易视角`，不再只剩一套统一且偏保守的动作描述。
- ETF/基金催化面新增 `产品/跟踪方向催化`：优先看跟踪基准、行业暴露、核心成分的关键词共振，不再把泛主题新闻当成主要催化来源。
- 趋势延续型 ETF/Fund 的动作口径已放宽：即使还没到满配 `⭐⭐⭐`，只要 `技术 / 相对强弱 / 风险收益 / 方向催化` 已经形成基础共振，就不再一律打回普通 `观望`；当前会保留 `观望偏多 + 波段跟踪` 这类更符合产品载体特性的表达。
- `scan / stock_analysis / etf_pick / fund_pick` 的动作区新增 `建议买入区间` 和 `建议减仓区间`。当前口径刻意使用区间而不是单点，和 `介入条件 / 止损参考 / 目标参考` 一起组成更完整的执行合同。

### 2026-03-20（个股推荐校准：少错杀强趋势延续票）

- `macro_reverse` 不再机械把所有原本能到 `⭐⭐⭐` 的个股统一压回 `⭐⭐`。
- 现在会先看候选是否具备“走势/基本面韧性”：
  - `技术 + 相对强弱` 明显占优，或
  - `基本面 + 催化` 已经足够硬，且技术没有明显失真
- 只有扛不住这层检查的候选，才会继续被压回 `⭐⭐`。
- 同时补了一条更克制的“趋势延续”晋级路径：`技术 / 相对强弱 / 风险收益` 已共振、但 `基本面 / 催化` 还没满配的强趋势票，不再永远卡在观察桶里。
- `etf_pick / fund_pick` 的盘中全市场发现不再把 final 自己写成“实时快照稿”；当前会标成 `盘中快照成稿`，表示 HTML/PDF/Markdown、review、manifest 都已正式落盘，只是发现源仍是盘中实时/缓存快照，不等同日终 Tushare 正式快照。

### 2026-03-18（ETF / 基金发现链提速）

- `build_market_context` 现在会并行预热中国宏观、事件日历、driver、pulse、watchlist returns、benchmark returns，ETF/Fund 启动段不再全串行。
- `discover_opportunities / discover_fund_opportunities` 和 `etf_pick` 的 watchlist fallback 现在按有界并发跑逐标的分析，不再一只一只串行扫完。
- 基金画像共享表 `fund_basic / fund_company / manager_directory / rating_all` 已补进程级缓存，减少并发场景下重复读 cache 文件。
- 当前真实 7 只 ETF 小样本实测：`build_market_context` 约 `20s`，逐只分析从约 `43s` 压到约 `19s`，总墙钟从约 `62s` 降到约 `39-40s`。

### 2026-03-15（强因子工程 J-1 ~ J-5 收口）

强因子工程全链路完成第一次 family-level 收口：

**J-1（价量结构与 setup）**
- `technical.py` 新增 `setup_analysis()`，检测三类 setup：假突破（bullish/bearish）、支撑结构（breakdown/failed_recovery）、压缩启动 vs 情绪追价
- `generate_scorecard()` 集成 `setup` 维度
- `opportunity_engine.py` 接入：技术维度（假突破识别/支撑结构/压缩启动）、叙事层、介入条件
- 新增 4 组 test，共 18 个技术测试通过

**J-2（季节/日历/事件窗）**
- `_seasonality_dimension()` 全量升级：显式样本边界、样本不足时降级、财报窗口覆盖全行业、节假日窗口（HOLIDAY_WINDOWS）、商品季节性（COMMODITY_WINDOWS）、指数调整分层加分
- 政策事件窗始终为 `observation_only`（无法固定 lag）
- 新增 8 组 test

### 2026-03-17（负因子补强）

- 强因子链路从“正因子更成体系、负因子更多靠 warning”补成更均衡的双向表达
- `催化面` 新增 `主题逆风`，ETF/基金/主题标的不再只有正向催化，没有产业链逆风表达
- `相对强弱` 现在对 `弱于基准 / 板块走弱 / 行业扩散不足 / 龙头掉队` 给出轻中度拖累，而不是统一记 0 分
- `筹码结构` 现在对 `拥挤过热 / 资金流出 / 高管减持` 给出轻中度拖累，同时继续保留“不要把行业代理写成个股优势”的约束
- `季节/日历` 现在会在样本充分时，对 `同月历史显著逆风` 给出负分；样本不足仍维持降级而不是硬判
- 强因子拆解和 core signal 也同步改成能展示拖累因素，不再只讲正面亮点
- `fundamental / macro` 的核心因子现在也补齐了 `factor_id / factor_meta`，`关键强因子拆解` 和下游 factor contract 不再漏掉估值、质量、景气/信用逆风这类中度拖累

### 2026-03-17（K 线形态与中国日线主链收紧）

- `technical.py` 的 K 线形态库已从基础版扩到更完整的形态学集合：`三内升/三内降`、`看涨/看跌母子线`、`平顶/平底镊子线`、`上吊线` 等已纳入最近 1-3 根 K 线识别
- `opportunity_engine.py` 已把这些新形态接入技术评分、动作建议和正文叙事，不再只是因子表里多几个名字
- `ChinaMarketCollector` 现在对 A 股/ETF 的 Tushare 日线做主链重试；`analyze_opportunity` 首轮历史抓取失败时，会先二次直连中国市场主链，再决定是否降级到快照占位
- 实测 `300750 / 515220 / 588200` 当前都能直接拿到 `Tushare 日线`，不再需要靠快照占位去画技术图

### 2026-03-17（功能开发快路径）

- 已把“新功能开发 -> 收口”的默认流程正式收成快路径：
  - `patch-level`：真实复现 + 窄修复 + narrow tests + 真实 spot check
  - `family-level`：today final + `release_check / report_guard / 外审`
  - `stage-level`：lesson / audit / backlog / 文档固化
- 目的不是降低质量，而是避免每个小 patch 都重跑整条长链，导致 token、上下文和外审成本过高
- 入口文档：[docs/process/feature_fast_loop.md](./process/feature_fast_loop.md)

**J-3（breadth/chips）**
- 新增 `_sector_breadth_detail()` 辅助函数：从 industry_spot 提取上涨家数比例
- `_relative_strength_dimension()` 新增：行业宽度（15pts）、龙头确认（10pts）
- `_chips_dimension()` 新增：拥挤度风险（observation_only）
- 所有 J-3 因子显式 proxy_level = "sector_proxy"
- 新增 5 组 test

**factor_meta.py（共享因子元数据合同层）**
- `FactorMeta` dataclass：factor_id / family / source_type / visibility_class / proxy_level / state / supports_scoring / supports_strategy_candidate / degraded / lag_days
- `FACTOR_REGISTRY` 覆盖 J-1 ~ J-5 全部因子
- `FactorState` 状态机：observation_only → scoring_supportive → production_factor → strategy_challenger → champion_candidate
- **已消费**：`FACTOR_REGISTRY` 现在被 opportunity_engine.py import；`_factor_row()` 输出携带 `factor_id`，J-5 全部因子已注册 factor_id；下游 decision_review / strategy 可按 factor_id 过滤状态

**J-5（ETF/基金专属因子）**
- 9 个子因子全部落地：折溢价、ETF份额申赎（j5_etf_share_change）、跟踪误差（j5_tracking_error，优先实际数值/降级基准清晰度代理）、成分集中度、主题纯度、业绩基准披露、风格漂移、经理稳定性、费率结构
- 所有因子 `_factor_row` 输出已携带 `factor_id`，对应 FACTOR_REGISTRY 注册条目
- ETF份额申赎数据缺失时显示"信息项"（T+1 lag，直连数据源待接入）
- 跟踪误差数据缺失时降级为基准清晰度代理（代理分上限 6 分）

**J-4（质量/盈利修正/估值协同）**
- `_fundamental_dimension()` 对 cn_stock/hk/us 新增：
  - 经营现金流质量（j4_cashflow_quality，cfps，季报 T+45 天 lag）
  - 杠杆压力（j4_leverage，debt_to_assets/current_ratio，季报 T+45 天 lag）
  - 盈利动量（j4_earnings_momentum，始终 observation_only，display_score="观察提示"）
- ETF/基金不产生 J-4 因子
- 新增 7 组 test

**总计：422 tests（含新增 J-5 补测 7 组），全部通过**

---

### 2026-03-15（外审 kit + review ledger）

- 外审能力已补成可迁移 kit：
  - `docs/review_kit/README.md`
  - `docs/review_kit/review_record_template.md`
  - `docs/review_kit/review_ledger_schema.md`
  - `docs/review_kit/migration_checklist.md`
- 新增 `review_ledger` parser/index：
  - `src/reporting/review_ledger.py`
  - `src/commands/review_ledger.py`
- 现在可以直接结构化索引 `reports/reviews/*.md`，看哪些外审 loop 已收敛、哪些还在 active。

### 2026-03-15（外审能力扩展：review audit）

- 新增外审治理审计：
  - `src/reporting/review_audit.py`
  - `src/commands/review_audit.py`
- 第一版先审两类问题：
  - review consistency
  - solidification completeness
- 现已明确分流：
  - `review_ledger` 会收录全部外审记录
  - `review_audit` 只审 `structured-round` 协议，旧式 review 文档只作为历史样本保留
- 同时抽出了共享解析层：
  - `src/reporting/review_record_utils.py`
  用来减少 `review_ledger / review_audit` 的重复解析逻辑。


## 现在不该误判的地方

- [docs/architecture_v2.md](./architecture_v2.md) 是历史文档，不要把里面的“原始设计约束”当成现在的真实合同。
- `strategy` 已可用，但它仍是窄版研究闭环，不是全市场截面策略引擎。
- `policy` 已强很多，但扫描版 / 表格重原文仍是明确降级边界。
