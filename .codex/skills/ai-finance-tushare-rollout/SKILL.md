---
name: ai-finance-tushare-rollout
description: Use when integrating new Tushare endpoints into AI-Finance, replacing covered AKShare paths, or rolling Tushare 10000-point data into mature stock, ETF, index, and industry workflows.
---

# AI-Finance Tushare Rollout

Use this skill when the task is “接 Tushare 新接口”, “下掉被覆盖的 AKShare”, or “把新数据真正吃进成熟主链”.

## Goal

Do not stop at collector wrappers.

A rollout is only considered integrated when it reaches at least:

- shared collector contract
- processor / ranking / evidence layer
- renderer / guard / customer-facing disclosure

## Default path

1. Find the current responsibility split:
   - existing collector
   - old fallback path
   - where the data should affect ranking or evidence
2. Add the Tushare path to the shared collector.
3. Normalize fields and always carry:
   - `source`
   - `as_of`
   - `latest_date` when relevant
   - `fallback`
   - `disclosure`
4. Sink it into mature outputs:
   - `briefing`
   - `scan`
   - `stock_analysis`
   - `stock_pick`
   - `etf_pick`
   - `fund_pick`
   - `compare / portfolio / risk` when relevant
5. Only after the new path is stable, retire the covered AKShare path.

## Hard rules

- Empty data cannot pretend to be fresh.
- Permission failure, rate limit, stale date, and empty result must remain visible as downgrade states.
- Do not keep long-term dual main paths once Tushare clearly covers the same responsibility.
- Do not delete uncovered realtime/minute/side routes just to make the repo look clean.

## AKShare retirement rule

Retire AKShare only when all three are true:

1. Tushare covers the same responsibility.
2. The new path is already consumed by mature reports.
3. Tests and at least one real spot check pass.

Otherwise keep AKShare as an explicit fallback, not a hidden default.

## Verification

- Collector narrow tests
- Processor / renderer narrow tests
- One real command spot check
- Update `docs/plans/tushare_10000.md` and `docs/status_snapshot.md` when backlog or maturity changes
