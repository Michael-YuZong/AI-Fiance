# AI-Finance Agent Handoff

## Default Read Order

不要一开工就扫完整个仓库。

默认只按下面顺序读：

1. 这份文件
2. [README.md](./README.md)
3. [docs/context_map.md](./docs/context_map.md)
4. 你要修改的 command / processor / renderer / test

只有在任务相关时再继续读：

- 配置问题：看 [config/README.md](./config/README.md)
- 强因子工程：看 [docs/plans/strong_factors.md](./docs/plans/strong_factors.md)
- `strategy`：看 [docs/plans/strategy.md](./docs/plans/strategy.md)
- 更完整的当前状态：看 [docs/status_snapshot.md](./docs/status_snapshot.md)
- 外审规则或 prompt：看 `docs/prompts/`

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

## Progressive Disclosure Rules

- 不要默认打开所有 `.md` / `.yaml`。
- 不要把 `reports/`、`tmp/`、历史生成稿当成开工前默认上下文。
- [docs/architecture_v2.md](./docs/architecture_v2.md) 是历史参考，不是当前主合同。
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

更细版本见 [docs/status_snapshot.md](./docs/status_snapshot.md)。

## Current Priority Backlog

1. `strategy` fixtures and governance
   现在已经有 `predict / list / replay / validate / attribute / experiment`，下一步是 lag / visibility / overlap / benchmark fixture，以及 champion-challenger promotion / rollback gate。
2. `policy` v2
   继续提升扫描版、表格重 PDF/OFD 的抽取和 taxonomy。
3. Proxy signals
   把代理置信度和降级影响完整传到 pick 输出和 release / review guard。
4. `scheduler` v2
   做持久化 run history、失败可见性和运维状态。
5. 校准与学习
   深化 setup bucket、归因、长期月度学习闭环。
6. 外审能力扩展
   `review_audit` 已启动，当前只审 `structured-round` 外审协议；旧式 review 文档由 `review_ledger` 收录但不当成 active blocker。下一步再扩 evidence / point-in-time / regression / attribution 审计。
7. 强因子进入维护
   - 阶段 J 按 `v1 已收口` 管理，不再作为主开发主线。
   - 剩余 `J-4 EPS 修正 point-in-time`、`J-2 政策事件窗 lag/visibility` 转入 `strategy fixtures / point-in-time coverage`。
   - setup / breadth / 质量阈值再校准转入“校准与学习”，不再单独挂在强因子开发下。

## Recent Changes

- 2026-03-19
  `briefing market` 的 A 股观察池开始复用 `briefing` 主链已经拿到的 `china_macro / global_proxy / monitor_rows / regime / news_report / drivers / pulse / events`，不再在观察池里再建一轮 market context。当前 full market briefing 仍然慢，但瓶颈已从“重复拉上游上下文”收缩到“A 股候选本身的深分析”。
- 2026-03-19
  `briefing` 的主线 taxonomy 已扩容，不再只在 `能源冲击 / 利率驱动成长 / 中国政策 / AI半导体` 这几个粗桶里来回落；现在新增并区分 `黄金避险 / 红利银行防守 / 宽基修复 / 电网公用事业`，输出也拆成 `背景框架 + 交易主线候选 + 次主线候选`。A 股观察池的行业分布会回灌到主线评分，避免全靠新闻关键词决定当天主线。
- 2026-03-19
  `briefing` 新增 `market` 模式，正式把现有 `market_context / 市场全景 / A股初筛观察池` 收成“全市场行情简报”；现在可以直接看当天市场温度、风格、资金、主线和验证点，不必再从晨报里手工拼上下文。
- 2026-03-18
  ETF / fund discover 链已开始做性能收口：`build_market_context` 改成并行预热独立段，`discover_opportunities / discover_fund_opportunities / etf_pick watchlist fallback` 改成有界并发分析；基金画像里的 `fund_basic / fund_company / manager_directory / rating_all` 也补了进程级共享缓存。当前 7 只 ETF 小样本实测从 `context 19-20s + analysis 43s` 压到 `context 20s + analysis 19s`，瓶颈已从串行扫描转到少数单票新闻/主题抓取。
- 2026-03-16
  `client_export` 的 HTML 导出现在会把本地图片嵌入成 `data:` URI，单文件 HTML 直接发给别人也能看到图，不再依赖本地绝对路径。
- 2026-03-16
  `scan / stock_analysis` 的客户 final 已改成“内部详细分析结构 + 客户友好章节名”，不再默认走摘要式客户稿；`client_export` 也补上了 Markdown 图片渲染，图表不会再在 PDF 里退化成普通链接。
- 2026-03-16
  `client_export` 的全局 HTML/PDF 主题已升级：标题、表格、引用、列表、强调语句统一用新的 report theme 渲染；支持 `**加粗**`、`*斜体*`、`==高亮==`、代码 chip。当前按 HTML-first 处理，不再做按页自适应缩图，图片统一按稳定宽度渲染，避免同一份报告里出现一大一小。
- 2026-03-16
  `stock_pick / fund_pick / etf_pick / briefing` 的 `2026-03-16` final、external review、manifest 已全部重刷；manifest 现在都带 `factor_contract`，`review_audit` 对 `structured-round` 结果为 `0 active findings`。
- 2026-03-16
  `briefing` 的 A 股观察池不再自己维护一套平行分析循环，改为复用成熟的个股发现链，输出里继续保留 `全市场初筛 -> shortlist -> 完整分析` 的披露口径。
- 2026-03-16
  阶段 J（强因子工程）按 `v1 已收口` 切出主开发主线；`review_audit` 当前对 `structured-round` 审计为 `0 active findings`，剩余 point-in-time / lag / calibration 问题已迁到其他 backlog。
- 2026-03-16
  `scan / stock_pick / etf_pick / fund_pick` 的客户口径现在会按 `generated_at` 和持有周期区分“今天剩余交易时段 / 下一个交易日 / 今天申赎决策 / 下一个开放日”，共享合同在 `src/processors/trade_handoff.py`。
- 2026-03-16
  `history_fallback` 命中时，图表层现在直接不出图；不再渲染“降级快照卡”或任何占位历史技术图，避免把快照占位样本误读成真实 K 线走势。
- 2026-03-16
  单标的分析和客户稿的 `证据时点与来源 / 分析元数据` 现在会显式写出 `行情来源`，并区分 `Tushare / AKShare / Yahoo / 本地实时快照占位 / 代理 ETF` 等历史链路，不再只写泛化的数据源说明。
- 2026-03-17
  强因子链路不再偏“只会加分不会扣分”。`催化 / 相对强弱 / 筹码结构 / 季节日历` 已补成更均衡的双向因子：支持和拖累都能进评分、正文和强因子拆解；当前策略是“证据够硬才扣分”，避免把轻微信号硬写成大逆风。
- 2026-03-17
  `fundamental / macro` 也已开始双向化：高估值、弱增长、低 ROE、低毛利、现金流转负、高杠杆、敏感度完全逆风、景气/信用收缩都会形成轻中度拖累；但像科技在通缩链条里未必天然吃亏，这类因子继续按板块映射理性判定，不做机械扣分。
- 2026-03-17
  `fundamental / macro` 的核心因子现在也补齐了 `factor_id / factor_meta`：`关键强因子拆解`、factor contract summary 和后续下游不再只看技术/筹码，估值、质量、信用/景气逆风也能按同一合同被识别出来。
- 2026-03-17
  K线形态不再只覆盖少数基础组合。`technical.py` 现已补入 `三内升/三内降`、`看涨/看跌母子线`、`平顶/平底镊子线`、`上吊线` 等形态，并已接入技术评分、动作建议和正文叙事；A 股/ETF 的日线抓取也新增了 Tushare 主链重试，`analyze_opportunity` 首轮失败时会先再直连一次中国市场主链，尽量避免误退到快照占位历史。
- 2026-03-15
  基金画像链已把 Tushare `fund_manager / fund_company / fund_div / fund_portfolio` 接进 `fund_profile`，当前是 Tushare 优先、AKShare 补充 richer 字段。

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
- 仍是单标的时间序列 replay / validate / experiment
- experiment 只比较预定义 challenger，不允许直接反哺生产链路

详细合同见 [docs/plans/strategy.md](./docs/plans/strategy.md)。

## External Review Rules

- 外审永远不是一次性 checklist。
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

默认开发节奏：

1. patch-level：真实复现 + narrow tests + 真实 spot check
2. family-level：today final + `release_check` + `report_guard` + 外审
3. stage-level：lesson / audit / backlog / 文档固化

详细快路径见 [docs/process/feature_fast_loop.md](./docs/process/feature_fast_loop.md)。

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
