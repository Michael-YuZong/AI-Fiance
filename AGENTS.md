# AI-Finance Agent Handoff

## Read This First

- This repo is a local CLI research stack, not a web app and not an auto-trading system.
- Do not ask the user to re-explain what the project does before reading this file, `README.md`, and the command you are about to touch.
- Default goal: improve output quality, contract stability, and workflow usefulness. Avoid inventing new surfaces unless the user asks.

## What Is Mature

- `src/commands/scan.py` and `src/commands/stock_analysis.py`
  The eight-dimension analysis path is the strongest core capability.
- `src/commands/stock_pick.py`
  This is the most productized feature: scoring, daily baseline snapshots, diffing, internal/client outputs.
- `src/commands/risk.py`
  Risk report, correlation, VaR/CVaR, drawdown, scenario stress are already coherent.
- `src/commands/portfolio.py`
  Holdings, trade log, target weights, rebalance, thesis, and monthly review are usable.
- `src/commands/lookup.py` and `src/commands/assistant.py`
  Chinese asset resolution and natural-language routing are stable enough for daily use.
- `src/commands/compare.py` + `src/output/opportunity_report.py`
  As of 2026-03-13 this now supports real multi-symbol comparison instead of silently truncating to 2 symbols.
- `src/output/briefing.py`
  Daily/noon/evening/weekly structure is usable again; the daily watchlist subsection was restored on 2026-03-13 to match test and output contract.

## What Is Usable But Still Needs Iteration

- `src/commands/discover.py` and `src/commands/etf_pick.py`
  Useful, but still mainly ETF-pool and rules driven.
- `src/commands/fund_pick.py`
  Works as a candidate comparator, not yet a full-market fund selector.
- `src/commands/research.py`
  As of 2026-03-13 it now classifies question type and answers in a clearer structure: direct answer, evidence, uncertainty, next step. It is no longer just a flat module dump, but it still needs stronger market-level diagnosis and better evidence ranking.
- `src/commands/policy.py` and `src/processors/policy_engine.py`
  Useful for keyword/URL interpretation. As of 2026-03-13 it now exposes template confidence, matched aliases, policy direction, stage, timeline cues, and more explicit watchlist mapping. It is still template and rule heavy.
- `src/scheduler.py`
  As of 2026-03-13 it is no longer a placeholder. It can list jobs, run one-off tasks, and start APScheduler with configured jobs. It is still v1: no persistent job state, alerting, or automation UI integration.

## What Is Still Weak Or Placeholder

- `src/collectors/policy.py`
  Placeholder; do not mistake it for the real policy engine.
- `src/collectors/social_sentiment.py` and `src/collectors/global_flow.py`
  These are proxy-signal modules, not direct full-fidelity data feeds.
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

1. `fund_pick`
   Expand beyond the fixed candidate list into a real searchable universe with explicit filters and downgrade notes.
2. `research` v2
   Improve market-level diagnosis when no explicit symbol exists, and rank evidence instead of listing every usable module.
3. Proxy signals
   Expose confidence and downgrade impact from social/global-flow proxies more explicitly in reports.
4. Scheduler v2
   Add persistent run history, failure visibility, and possibly automation integration if the user asks for recurring workflows in the app.
5. Policy v2
   Keep improving official-source extraction, especially for longer raw pages/PDF-like content and stricter policy taxonomy.

## Recent Changes

- 2026-03-13
  `compare` now passes all input symbols through the engine and renders multi-symbol output with ranking, multi-column dimension comparison, and scenario picks.
- 2026-03-13
  Daily `briefing` regained the `2.5 Watchlist` section so renderer output and tests line up again.
- 2026-03-13
  `src/scheduler.py` was upgraded from placeholder text to a usable CLI scheduler.
- 2026-03-13
  `policy` now reports template confidence, matched aliases, policy direction, stage, timeline cues, and stronger watchlist impact reasons.
- 2026-03-13
  `research` now classifies question type and renders structured answers with a direct answer, evidence, uncertainty, and next-step sections.

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
  `python -m src.commands.risk report`
- Scheduler
  `python -m src.scheduler list`
  `python -m src.scheduler run daily_briefing`
  `python -m src.scheduler serve`

## Tests To Run Before You Claim Something Is Better

- Narrow tests first
  `pytest tests/test_commands/test_compare.py tests/test_output/test_opportunity_report.py tests/test_output/test_briefing.py tests/test_scheduler.py -q`
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
