# AI-Finance Agent Handoff

## Read This First

- This repo is a local CLI research stack, not a web app and not an auto-trading system.
- Do not ask the user to re-explain what the project does before reading this file, `README.md`, and the command you are about to touch.
- Default goal: improve output quality, contract stability, and workflow usefulness. Avoid inventing new surfaces unless the user asks.

## What Is Mature

- `src/commands/scan.py` and `src/commands/stock_analysis.py`
  The eight-dimension analysis path is the strongest core capability.
- `src/commands/stock_pick.py`
  This is the most productized feature: scoring, daily baseline snapshots, diffing, internal/client outputs. As of 2026-03-13 the client-facing pick sections also surface explicit holding-period / execution-horizon language instead of only saying whether chasing is appropriate.
- `src/commands/fund_pick.py`
  As of 2026-03-13 this is no longer just a fixed-candidate comparer. It now does full-universe open-end fund pre-screening, explicit theme/style/manager filters, client/detail output, release gating, and same-day baseline-vs-rerun score diffing.
- `src/commands/etf_pick.py`
  ETF pick now shares the same coverage disclosure, score-history snapshotting, rerun diffing, and release-guard workflow as the stronger pick pipelines, and its client output now exposes the intended holding period / play style more explicitly.
- `src/commands/risk.py`
  Risk report, correlation, VaR/CVaR, drawdown, scenario stress are already coherent.
- `src/commands/portfolio.py`
  Holdings, trade log, target weights, rebalance, thesis, and monthly review are usable. As of 2026-03-13 it also has a real `whatif` trade-preview path with first-pass risk-budget / tradability / execution-cost estimates, and trade logs now persist minimal decision/execution snapshots for later retrospective review.
- `src/commands/lookup.py` and `src/commands/assistant.py`
  Chinese asset resolution and natural-language routing are stable enough for daily use.
- `src/commands/research.py`
  As of 2026-03-13 this now behaves like a real research entrypoint instead of a flat module dump. It classifies market / asset / policy / portfolio questions, uses a lighter market-diagnosis path, includes rule-based scenario-probability framing, ranks evidence by importance, carries proxy-confidence notes through market and flow answers, and now also answers symbol-level position-sizing / tradability questions by reusing the portfolio `whatif` contract instead of only giving directional commentary.
- `src/commands/compare.py` + `src/output/opportunity_report.py`
  As of 2026-03-13 this now supports real multi-symbol comparison instead of silently truncating to 2 symbols.
- `src/commands/briefing.py` + `src/output/briefing.py`
  As of 2026-03-13 the daily briefing path is no longer just “usable”. It now has internal/detail archiving under `reports/briefings/internal`, client-final release gating, independent external review export, client-safe non-intraday wording, explicit `数据完整度` disclosure, a hard downgrade path when macro asset monitors fail to refresh in time, and a Tushare-priority full-A-share pre-screen block that explicitly discloses `初筛池 -> shortlist -> 完整分析` instead of only repeating the static watchlist.

## What Is Usable But Still Needs Iteration

- `src/commands/discover.py`
  Useful, but still mainly ETF-pool and rules driven. Discovery quality is below the fully productized pick outputs.
- `src/processors/decision_review.py` + `src/output/retrospect_report.py`
  As of 2026-03-13 monthly review is no longer just path replay. It now includes benchmark-relative excess return, simple setup-bucket calibration, first-pass result attribution, and renders stored timing/execution snapshots. It is still v1 and only partially solves project-wide point-in-time correctness / calibration.
- `src/commands/policy.py` and `src/processors/policy_engine.py`
  Useful for keyword/URL interpretation. As of 2026-03-13 it now exposes template confidence, matched aliases, policy direction, stage, timeline cues, and more explicit watchlist mapping. It also separates extracted正文事实、模板/规则推断、未确认项 for official long-form notice pages, with stronger HTML公告页标题/元信息/时间线抽取 and explicit attachment downgrade notes. It is still template and rule heavy, and attached PDF/OFD originals are not fully parsed yet.
- `src/scheduler.py`
  As of 2026-03-13 it is no longer a placeholder. It can list jobs, run one-off tasks, and start APScheduler with configured jobs. It is still v1: no persistent job state, alerting, or automation UI integration.

## What Is Still Weak Or Placeholder

- `src/collectors/policy.py`
  Placeholder; do not mistake it for the real policy engine.
- `src/collectors/social_sentiment.py` and `src/collectors/global_flow.py`
  These are still proxy-signal modules, not direct full-fidelity data feeds. As of 2026-03-13 they now expose explicit confidence labels, limitations, and downgrade-impact notes so downstream outputs can stop presenting them like hard facts.
- Project-wide point-in-time correctness, calibration, and execution-cost integration
  There is now a portfolio/review v1, but it is not yet a repo-wide contract. `scan`/pick/report flows still do not all emit the same evidence-provenance, tradability, or calibration language.
- Scheduler persistence and operational monitoring
  The scheduler can run jobs now, but it still lacks durable ops features.

## Iteration Method

This project became reliable through repeated cycles, not one-shot generation. Keep using that method:

1. Reproduce the issue with a real command or a failing test.
2. Fix the product contract first, not just the code path.
3. Add or update tests so the same regression does not return.
4. Preserve downgrade paths and source-confidence notes when data is incomplete.
5. Update this file when command contracts, maturity status, or the active backlog changes.

When in doubt, optimize for:

- fewer silent fallbacks
- clearer output contracts
- stronger source grounding
- better user-facing explanations of missing data and downgrade logic

## Current Priority Backlog

1. `discover` v2
   Move discovery quality closer to the productized pick outputs: less rule-only ranking, better pre-screen quality, and clearer linkage from discovered theme -> candidate pool -> formal pick pipeline.
2. Propagate portfolio-construction v1 into pick flows
   `portfolio whatif` / `decision_review` / `research` now share first-pass risk-budget, execution-cost, attribution, and timing-snapshot contracts. Next step is to let ETF/fund/stock pick outputs hand off into the same action language.
3. Proxy signals
   Expose confidence and downgrade impact from social/global-flow proxies more explicitly in reports.
4. Scheduler v2
   Add persistent run history, failure visibility, and possibly automation integration if the user asks for recurring workflows in the app.
5. Policy v2
   Keep improving official-source extraction, especially for longer raw pages/PDF-like content and stricter policy taxonomy.
6. Pick pipeline consolidation
   `src/commands/pick_history.py` now holds shared snapshot/history helpers. Continue consolidating ETF/fund/stock pick contracts there instead of duplicating scoring-history and coverage logic per command.

## Recent Changes

- 2026-03-13
  Pick renderers (`stock_pick` client sections, `fund_pick`, `etf_pick`) now surface explicit holding-period / execution-horizon labels such as `短线交易（1-2周）` / `中线配置（1-3月）` / `观察期`, instead of only saying whether the setup is suitable for chasing.
- 2026-03-13
  `etf_pick`/shared pick-history coverage disclosure now uses the full set of completed analyses instead of the truncated `top` list, and single-candidate client copy no longer auto-downgrades a still-`标准推荐稿` report.
- 2026-03-13
  `compare` now passes all input symbols through the engine and renders multi-symbol output with ranking, multi-column dimension comparison, and scenario picks.
- 2026-03-13
  Daily `briefing` regained the `2.5 Watchlist` section so renderer output and tests line up again.
- 2026-03-13
  `src/scheduler.py` was upgraded from placeholder text to a usable CLI scheduler.
- 2026-03-13
  `policy` now reports template confidence, matched aliases, policy direction, stage, timeline cues, and stronger watchlist impact reasons.
- 2026-03-13
  `policy` now repairs common official-page encoding issues, extracts title/metadata/body facts more reliably from long-form notice pages, separates正文事实 vs 模板/规则推断 vs 待确认项 in the renderer, and explicitly downgrades when an announcement page still depends on attached PDF/OFD originals.
- 2026-03-13
  `research` now classifies question type and renders structured answers with a direct answer, evidence, uncertainty, and next-step sections.
- 2026-03-13
  `research` now gives lightweight market diagnosis without calling the heavier market-context builder, uses cache-snapshot market overview fallback to avoid timeout-prone open questions, gives explicit no-holdings downgrade answers for portfolio-risk questions, and adds more concrete execution next steps for asset questions.
- 2026-03-13
  `research` now also injects rule-based scenario-probability framing, uses real proxy flow in market diagnosis instead of a neutral placeholder, ranks evidence lines by question type instead of simple group order, and carries policy evidence into asset questions when the user explicitly asks with policy context.
- 2026-03-13
  `social_sentiment` and `global_flow` now emit proxy confidence, limitations, and downgrade-impact metadata; `research` and `briefing` started consuming that contract so proxy evidence is less likely to be mistaken for confirmed hard data.
- 2026-03-13
  `portfolio whatif` was added as a first-pass combination of portfolio construction, risk-budgeting, and execution-cost preview. It estimates projected weight/exposure, pre/post vol+beta, tradability label, participation rate, slippage, fee drag, and records the timing assumptions explicitly.
- 2026-03-13
  `portfolio log` now persists minimal `decision_snapshot` and `execution_snapshot` payloads, so retrospective review can distinguish historical snapshots from current backfill.
- 2026-03-13
  `decision_review` / `retrospect_report` now include benchmark-relative excess return, setup-bucket calibration, attribution labels (`alpha兑现` / `更多来自贝塔顺风` / etc.), and stored timing/execution snapshots.
- 2026-03-13
  `research` asset-trade questions such as “上多少仓位 / 做得进去吗” now reuse the same trade-preview contract: it can answer with a first-pass suggested max weight, tradability label, estimated total cost, and a timing snapshot instead of only repeating trend commentary.
- 2026-03-13
  `fund_pick` now does real full-universe open-end fund screening with theme/style/manager filters, coverage disclosure, same-day baseline snapshots, rerun diffing, and catalyst fallback when live news/event coverage degrades.
- 2026-03-13
  `etf_pick` now uses the shared pick-history snapshot layer and the same release-gated coverage/rerun-diff contract as fund pick.
- 2026-03-13
  `src/commands/pick_history.py` was introduced to centralize pick coverage summaries, baseline snapshots, score-change explanations, and degraded-news catalyst fallback.
- 2026-03-13
  `briefing daily` now has a real finalization chain: internal reports archive to `reports/briefings/internal`, client output must include `宏观领先指标 + 数据完整度 + 重点观察`, macro-monitor refresh failures downgrade coverage instead of silently using old cached prices, and the daily client-final can ship through external review + manifest like the stronger pick pipelines.
- 2026-03-13
  `briefing daily` now also uses a Tushare-priority full-A-share pre-screen for its client/detail `A股观察池`, and both the rendered copy and manifest disclose that this is `全市场初筛 -> 少量样本完整分析`, not a full per-stock deep scan.

## Commands You Will Actually Use

- Research / analysis
  `python -m src.commands.scan 561380`
  `python -m src.commands.stock_analysis 300750`
  `python -m src.commands.compare 561380 GLD QQQM`
- Reports
  `python -m src.commands.briefing daily`
  `python -m src.commands.briefing noon`
  `python -m src.commands.briefing evening`
  `python -m src.commands.briefing weekly`
- Picks / discovery
  `python -m src.commands.discover`
  `python -m src.commands.etf_pick`
  `python -m src.commands.stock_pick --market all --top 10`
  `python -m src.commands.fund_pick`
- Portfolio / risk
  `python -m src.commands.portfolio status`
  `python -m src.commands.portfolio whatif buy 561380 2.1 20000`
  `python -m src.commands.risk report`
- Scheduler
  `python -m src.scheduler list`
  `python -m src.scheduler run daily_briefing`
  `python -m src.scheduler serve`

## Tests To Run Before You Claim Something Is Better

- Narrow tests first
  `pytest tests/test_commands/test_compare.py tests/test_output/test_opportunity_report.py tests/test_output/test_briefing.py tests/test_scheduler.py tests/test_commands/test_fund_pick.py tests/test_commands/test_pick_history.py tests/test_output/test_client_report.py tests/test_commands/test_release_check.py tests/test_commands/test_report_guard.py tests/test_processors/test_portfolio_actions.py tests/test_processors/test_decision_review.py tests/test_output/test_retrospect_report.py tests/test_commands/test_portfolio_command.py tests/test_storage/test_portfolio.py -q`
- Then broader regression
  `pytest -q`

If a report contract changes, update both renderer tests and any command/helper tests that assert section names or table shape.

## Release And Review Guardrails

- Client-facing export is guarded by:
  `src/commands/release_check.py`
  `src/commands/report_guard.py`
- Do not weaken those guardrails just to make output generation easier.
- If a feature is proxy-based or downgraded, say so in the output instead of hiding it.

## Working Style For Future Agents

- Prefer targeted, high-signal improvements over broad rewrites.
- Fix silent behavior mismatches immediately. They are more damaging than obvious TODOs.
- Keep CLI behavior and rendered markdown aligned. If the parser claims multi-input support, the output layer must honor it.
- Use the existing loop: real task -> critique -> revise -> test -> document.
- Research Q&A external review prompt:
  `docs/prompts/external_research_reviewer.md`
- Fund/ETF pick maturity now depends on four linked contracts staying aligned:
  discovery/pre-screen -> client renderer -> release checks -> review guard/export
