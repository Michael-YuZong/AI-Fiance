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

- proxy signals 仍是代理，不是原始全量 feed
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
