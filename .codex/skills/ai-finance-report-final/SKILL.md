---
name: ai-finance-report-final
description: Use when editing or generating mature AI-Finance reports or final/client-final deliverables, especially for briefing, scan, stock_analysis, stock_pick, etf_pick, and fund_pick. Covers editor payload reuse, external review closure, release_check/report_guard, and first-screen evidence visibility.
---

# AI-Finance Report Final

Use this skill when the task touches a mature report command, homepage judgment layer, `client-final`, external review closure, or customer-facing evidence visibility.

## Default workflow

1. Reproduce with a real command or a narrow failing test.
2. Patch shared contract first:
   - command
   - processor
   - renderer
   - guard
3. Keep `source / as_of / fallback / disclosure` honest.
4. Run narrow tests first, then a real spot check.
5. Only after family-level stability, rerun today final or full export.

## Report-specific rules

- Prefer `editor_payload.json + editor_prompt.md` if they already exist.
- `editor` is a separate subagent step, not “main agent writes and lightly rewrites”.
- Structural review and divergent review must examine the editor-modified version, not the raw rule draft.
- Do not stop at “missing external review file” if the task is to deliver a final report. Create or complete the review record and continue unless a real external dependency blocks you.

## Evidence visibility rules

- First screen should show meaningful evidence, not generic framework filler.
- Customer-facing wording should prefer `情报 / 关键情报 / 证据`, not internal miss diagnosis.
- If a new evidence source is added but buried below generic lines, the feature is not really integrated yet.
- If the current sample does not actually hit a source, disclose absence; do not fabricate a visible example.

## PDF / export checks

When touching report layout or export:

- Check HTML and PDF both.
- Watch first-page density and accidental clipping.
- If PDF is visually broken, fix layout rather than weakening export guard.

## Final verification checklist

- Narrow tests for changed command/processor/output path
- One real command spot check
- `release_check` still strict
- `report_guard` still strict
- Final report does not leak internal diagnostic language
