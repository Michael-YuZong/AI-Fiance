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
  As of 2026-03-13 this is no longer just a fixed-candidate comparer. It now does full-universe open-end fund pre-screening, explicit theme/style/manager filters, client/detail output, release gating, same-day baseline-vs-rerun score diffing, and a structured `action.horizon` contract that explains why a candidate is better treated as observation / short-term / swing / medium-term / long-term.
- `src/commands/etf_pick.py`
  ETF pick now shares the same coverage disclosure, score-history snapshotting, rerun diffing, and release-guard workflow as the stronger pick pipelines, and its client output now exposes the intended holding period / play style through the same structured `action.horizon` contract.
- `src/commands/risk.py`
  Risk report, correlation, VaR/CVaR, drawdown, scenario stress are already coherent.
- `src/commands/portfolio.py`
  Holdings, trade log, target weights, rebalance, thesis, and monthly review are usable. As of 2026-03-13 it also has a real `whatif` trade-preview path with first-pass risk-budget / tradability / execution-cost estimates, and trade logs now persist minimal decision/execution snapshots plus a structured horizon snapshot for later retrospective review.
- `src/commands/lookup.py` and `src/commands/assistant.py`
  Chinese asset resolution and natural-language routing are stable enough for daily use.
- `src/commands/research.py`
  As of 2026-03-13 this now behaves like a real research entrypoint instead of a flat module dump. It classifies market / asset / policy / portfolio questions, uses a lighter market-diagnosis path, includes rule-based scenario-probability framing, ranks evidence by importance, carries proxy-confidence notes through market and flow answers, and now also answers symbol-level position-sizing / tradability questions by reusing the portfolio `whatif` contract, including explicit horizon fit / misfit language instead of only giving directional commentary.
- `src/commands/compare.py` + `src/output/opportunity_report.py`
  As of 2026-03-13 this now supports real multi-symbol comparison instead of silently truncating to 2 symbols.
- `src/commands/briefing.py` + `src/output/briefing.py`
  As of 2026-03-13 the daily briefing path is no longer just “usable”. It now has internal/detail archiving under `reports/briefings/internal`, client-final release gating, independent external review export, client-safe non-intraday wording, explicit `数据完整度` disclosure, a hard downgrade path when macro asset monitors fail to refresh in time, and a Tushare-priority full-A-share pre-screen block that explicitly discloses `初筛池 -> shortlist -> 完整分析` instead of only repeating the static watchlist.

## What Is Usable But Still Needs Iteration

- `src/commands/discover.py`
  Useful, but still mainly ETF-pool and rules driven. Discovery quality is below the fully productized pick outputs.
- `src/processors/decision_review.py` + `src/output/retrospect_report.py`
  As of 2026-03-13 monthly review is no longer just path replay. It now includes benchmark-relative excess return, simple setup-bucket calibration, first-pass result attribution, explicit horizon calibration, and renders stored timing/execution snapshots. It is still v1 and only partially solves project-wide point-in-time correctness / calibration.
- `src/commands/policy.py` and `src/processors/policy_engine.py`
  Useful for keyword/URL interpretation. As of 2026-03-13 it now exposes template confidence, matched aliases, policy direction, stage, timeline cues, and more explicit watchlist mapping. It also separates extracted正文事实、模板/规则推断、未确认项 for official long-form notice pages, reports `来源判断 / 抽取覆盖 / 附件标题`, ranks正文事实 away from pure date tags and boilerplate notice copy, supports direct PDF/OFD URLs plus first-pass PDF/OFD attachment补抽 on official notice pages, and renders a structured `政策分类法` contract. It is still template and rule heavy, and complex scanned/table-heavy originals remain a degradation boundary.
- `src/scheduler.py`
  As of 2026-03-13 it is no longer a placeholder. It can list jobs, run one-off tasks, and start APScheduler with configured jobs. It is still v1: no persistent job state, alerting, or automation UI integration.

## What Is Still Weak Or Placeholder

- `src/collectors/policy.py`
  Placeholder; do not mistake it for the real policy engine.
- `src/collectors/social_sentiment.py` and `src/collectors/global_flow.py`
  These are still proxy-signal modules, not direct full-fidelity data feeds. As of 2026-03-13 they now expose explicit confidence labels, limitations, and downgrade-impact notes so downstream outputs can stop presenting them like hard facts.
- Project-wide point-in-time correctness, calibration, and execution-cost integration
  There is now a portfolio/review v1 plus shared provenance/handoff helpers, but policy long-form extraction, backtest/history fixtures, and some release-guard wording still do not enforce a single repo-wide contract.
- Scheduler persistence and operational monitoring
  The scheduler can run jobs now, but it still lacks durable ops features.

## Iteration Method

This project became reliable through repeated cycles, not one-shot generation. Keep using that method:

1. Reproduce the issue with a real command or a failing test.
2. Fix the product contract first, not just the code path.
3. Add or update tests so the same regression does not return.
4. Preserve downgrade paths and source-confidence notes when data is incomplete.
5. Update this file when command contracts, maturity status, or the active backlog changes.
6. External review is never checklist-only: every review pass must include a divergent pass that deliberately searches for framework-external issues, missing controls, and better validation ideas.
7. If a divergent-review finding is judged valid, do not leave it as one-off commentary. Route it into at least one durable layer: prompt, hard rule / guard, test / fixture, or a tracked lesson/backlog item.
8. All external review is round-based: every round must compare against the previous round, record new vs carried P0/P1 items, and continue until the convergence conditions in `plan.md` are met.

When in doubt, optimize for:

- fewer silent fallbacks
- clearer output contracts
- stronger source grounding
- better user-facing explanations of missing data and downgrade logic

## Current Priority Backlog

1. `strategy` fixtures and governance
   `strategy` now has a usable `predict / list / replay / validate / attribute / experiment` loop. Next step is not widening scope, but hardening lag / visibility / overlap / benchmark fixtures, then adding champion-challenger promotion and rollback gates. Keep the approved A-share liquid-stock universe, fixed `20`-day benchmark-relative excess-return target, fixed overlap contract, point-in-time lag rules, and `no_prediction` cases. Do not widen scope to ETF/fund/multi-asset or auto-factor discovery yet.
2. Policy v2
   Keep improving official-source extraction, especially scanned/table-heavy PDF/OFD handling, and deepen the policy taxonomy beyond the current first-pass contract.
3. Proxy signals
   Finish propagating confidence and downgrade-impact wording into all pick outputs plus release/review guards.
4. Scheduler v2
   Add persistent run history, failure visibility, and possibly automation integration if the user asks for recurring workflows in the app.
5. Scoring calibration v2
   Deepen setup-bucket calibration and attribution beyond the current first-pass monthly review.
6. Pick pipeline consolidation
   `src/commands/pick_history.py` now holds shared snapshot/history helpers. Continue consolidating ETF/fund/stock pick contracts there instead of duplicating scoring-history and coverage logic per command.

## Recent Changes

- 2026-03-14
  `strategy` now has a dedicated phase in `plan.md`, a dedicated plan reviewer prompt, and a completed round-based external-review loop under `reports/reviews/strategy_plan_review_2026-03-14_round{1,2,3}.md`. The plan gate passed after locking `v1` target/universe/overlap/lag/champion-challenger contracts, so the next allowed step is `I-1` prediction ledger implementation rather than more plan design.
- 2026-03-14
  `src/commands/strategy.py`, `src/processors/strategy.py`, `src/storage/strategy.py`, and `src/output/strategy_report.py` now implement `strategy I-1` prediction ledger. v1 is intentionally narrow: single-symbol `predict` + `list`, fixed A-share liquid-stock universe, fixed `20`-day benchmark-relative excess-return target against CSI800, explicit `no_prediction` gating, and persisted factor/provenance snapshots.
- 2026-03-14
  `strategy` now also has a narrow `replay + validate` loop: historical single-symbol non-overlap replay samples, persisted validation snapshots, hit-rate / avg excess / cost-adjusted directional return / confidence-bucket summary, and explicit disclosure that this is still time-series validation rather than full cross-sectional rank validation.
- 2026-03-14
  `strategy` now also has first-pass `attribute + experiment`: validated samples can be bucketed into structured labels such as `weight_misallocation / execution_cost_drag / universe_bias / confirmed_edge`, and predefined replay challengers (`baseline / momentum_tilt / defensive_tilt / confirmation_tilt`) can be compared on the same historical sample set without feeding production chains directly.
- 2026-03-14
  Client-facing analysis/pick reports now expose disabled intraday provenance as `分钟级快照 as_of`, and `release_check` no longer mistakes that metadata row for unsupported intraday execution language.
- 2026-03-13
  Pick renderers now use a structured `action.horizon` contract with explicit fit / misfit language, so ETF/fund/stock outputs can distinguish `观察期` / `短线交易（3-10日）` / `波段跟踪（2-6周）` / `中线配置（1-3月）` / `长线配置（6-12月）` instead of only printing a flat timeframe string.
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
  `policy` now repairs common official-page encoding issues, extracts title/metadata/body facts more reliably from long-form notice pages, separates正文事实 vs 模板/规则推断 vs 待确认项 in the renderer, reports source-authority + coverage-scope + attachment-title contracts, ranks正文事实 away from pure date tags / notice boilerplate, and explicitly downgrades when an announcement page still depends on attached PDF/OFD originals.
- 2026-03-13
  `policy` now also supports direct PDF URL extraction and first-pass PDF attachment补抽 on official notice pages, upgrades extraction status to `PDF正文已抽取` / `公告页正文 + PDF附件已补抽`, and renders a structured `政策分类法` section so policy outputs are no longer only theme-name + beneficiary-chain summaries.
- 2026-03-13
  `policy` now also supports direct OFD URL extraction and first-pass OFD attachment补抽 on official notice pages, upgrades extraction status to `OFD正文已抽取` / `公告页正文 + PDF/OFD附件已补抽`, and keeps the degradation note focused on still-hard cases such as scanned/table-heavy originals.
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
  `portfolio whatif`, trade logging, `research` asset-trade Q&A, and `decision_review` / `retrospect_report` now all carry a structured horizon contract so outputs can explain why a setup is being treated as observation / swing / medium-term / long-term and where cycle mismatch likely happened.
- 2026-03-13
  ETF/fund/stock client reports and scan-style outputs now hand off directly into `portfolio whatif` with cycle-aware wording, so reports no longer stop at “偏短线 / 偏中线” and instead tell the user how to preflight a real order against portfolio limits.
- 2026-03-13
  `fund_pick` now does real full-universe open-end fund screening with theme/style/manager filters, coverage disclosure, same-day baseline snapshots, rerun diffing, and catalyst fallback when live news/event coverage degrades.
- 2026-03-13
  `etf_pick` now uses the shared pick-history snapshot layer and the same release-gated coverage/rerun-diff contract as fund pick.
- 2026-03-13
  `src/commands/pick_history.py` was introduced to centralize pick coverage summaries, baseline snapshots, score-change explanations, and degraded-news catalyst fallback.
- 2026-03-13
  `discover` and `briefing` now hand opportunities off to `portfolio whatif`, so the first-touch discovery/briefing surfaces no longer stop at "watch this" and instead point into the same pre-trade risk-budget workflow as pick reports.
- 2026-03-13
  Stage-E point-in-time provenance now has a shared helper; `research`, `scan / stock_analysis`, stock-pick detailed output, `fund_pick`, and `etf_pick` all expose evidence timing, source class, and explicit point-in-time boundary notes instead of burying them in generic metadata.
- 2026-03-13
  `briefing daily` now has a real finalization chain: internal reports archive to `reports/briefings/internal`, client output must include `宏观领先指标 + 数据完整度 + 重点观察`, macro-monitor refresh failures downgrade coverage instead of silently using old cached prices, and the daily client-final can ship through external review + manifest like the stronger pick pipelines.
- 2026-03-13
  `briefing daily` now also uses a Tushare-priority full-A-share pre-screen for its client/detail `A股观察池`, and both the rendered copy and manifest disclose that this is `全市场初筛 -> 少量样本完整分析`, not a full per-stock deep scan.
- 2026-03-13
  `stock_pick` history/baseline/catalyst-fallback logic now reuses the shared `src/commands/pick_history.py` layer instead of maintaining a parallel implementation, and its coverage disclosure now prefers the full completed-analysis population when available.

## Commands You Will Actually Use

- Research / analysis
  `python -m src.commands.scan 561380`
  `python -m src.commands.stock_analysis 300750`
  `python -m src.commands.compare 561380 GLD QQQM`
  `python -m src.commands.strategy predict 600519 --preview`
  `python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6`
  `python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview`
  `python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview`
  `python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6`
  `python -m src.commands.strategy list --limit 10`
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
- External review must be two-pass: contract review first, then a divergent review that asks what the current prompt/task definition failed to ask.
- External review does not stop after one pass. Run it in rounds until convergence: no new P0/P1 across two consecutive rounds, prior blockers closed or downgraded, and no new material divergent findings.
- `strategy` is gated differently from ordinary feature work: do not start implementation until the plan in `plan.md` has passed the dedicated external plan-review loop.
- Research Q&A external review prompt:
  `docs/prompts/external_research_reviewer.md`
- Universal external-review convergence loop:
  `docs/prompts/external_review_convergence_loop.md`
- `strategy` plan external review prompt:
  `docs/prompts/external_strategy_plan_reviewer.md`
- Fund/ETF pick maturity now depends on four linked contracts staying aligned:
  discovery/pre-screen -> client renderer -> release checks -> review guard/export
