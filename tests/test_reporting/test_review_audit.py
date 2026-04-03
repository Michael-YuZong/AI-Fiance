from __future__ import annotations

import json
from pathlib import Path

from src.reporting.review_lessons import active_lesson_ids
from src.reporting.review_audit import build_review_audit, render_review_audit_markdown


def _write_review(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_active_review_lessons_include_macro_vs_direct_catalyst_boundary() -> None:
    assert "L039" in active_lesson_ids()


def test_review_audit_flags_missing_solidification_for_actionable_findings(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "demo_round1.md",
        "\n".join(
            [
                "# Demo",
                "",
                "- review_target：`docs/demo.md`",
                "- review_prompt：`docs/prompts/demo.md`",
                "",
                "## 结论",
                "",
                "`go with conditions`",
                "",
                "## 主要问题",
                "",
                "1. `P1`：主问题仍未关闭",
                "",
                "## 框架外问题",
                "",
                "1. 存在框架外风险",
                "",
                "## 零提示发散审",
                "",
                "1. 不看模板时，第一反应仍然是主问题没有真正收口。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：是",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "缺少必需外审段落" in titles
    assert "finding 没有沉淀去向" in titles


def test_review_audit_flags_missing_zero_prompt_divergence_section(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "demo_round1.md",
        "\n".join(
            [
                "# Demo",
                "",
                "- review_target：`docs/demo.md`",
                "- review_prompt：`docs/prompts/demo.md`",
                "",
                "## 结论",
                "",
                "`go with conditions`",
                "",
                "## 主要问题",
                "",
                "1. `P2`：还有小问题",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题",
                "",
                "## 建议沉淀",
                "",
                "- prompt",
                "  - 补 reviewer 约束",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "缺少必需外审段落" in titles


def test_review_audit_flags_round_drift_and_previous_round_mismatch(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "foo_round1.md",
        "\n".join(
            [
                "# Foo",
                "",
                "- review_target：`docs/foo.md`",
                "- review_prompt：`docs/prompts/foo.md`",
                "",
                "## 结论",
                "",
                "`go with conditions`",
                "",
                "## 主要问题",
                "",
                "1. `P2`：需要继续修",
                "",
                "## 框架外问题",
                "",
                "1. 无新的框架外阻塞问题",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- prompt",
                "  - 补 reviewer 约束",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )
    _write_review(
        tmp_path / "foo_round3.md",
        "\n".join(
            [
                "# Foo",
                "",
                "- review_target：`docs/foo_v2.md`",
                "- review_prompt：`docs/prompts/foo_v2.md`",
                "- previous_round：`foo_round1.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞项",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- backlog",
                "  - 跟踪后续实现",
                "",
                "## 收敛结论",
                "",
                "- round：3",
                "- previous_round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "同一审稿序列的 target 漂移" in titles
    assert "同一审稿序列的 prompt 漂移" in titles
    assert "round 序号不连续" in titles


def test_review_audit_flags_pass_continue_conflict_and_renders_markdown(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "bar_round1.md",
        "\n".join(
            [
                "# Bar",
                "",
                "- review_target：`docs/bar.md`",
                "- review_prompt：`docs/prompts/bar.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- lesson / backlog",
                "  - 保留后续提醒",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "PASS 记录仍要求继续下一轮" in titles

    markdown = render_review_audit_markdown(audit)
    assert "# External Review Audit" in markdown
    assert "| severity | category | series | round | title | file |" in markdown


def test_review_audit_flags_single_round_pass_without_repair_loop(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "single_pass_round1.md",
        "\n".join(
            [
                "# Single pass",
                "",
                "- review_target：`docs/single.md`",
                "- review_prompt：`docs/prompts/single.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P2`：解释链还没收口。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示再看，最先冒出来的问题仍然是解释链断裂。",
                "",
                "## 建议沉淀",
                "",
                "- prompt / test",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "PASS 记录正文仍有 actionable finding" in titles
    assert "单轮 PASS 缺少回修闭环" in titles
    assert "主要问题还没进入下一轮闭环" in titles


def test_review_audit_flags_missing_close_or_carry_between_rounds(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "handoff_round1.md",
        "\n".join(
            [
                "# Handoff",
                "",
                "- review_target：`docs/handoff.md`",
                "- review_prompt：`docs/prompts/handoff.md`",
                "",
                "## 结论",
                "",
                "`hold`",
                "",
                "## 主要问题",
                "",
                "1. `P2`：证据链还不完整。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示再看，最明显的问题还是证据链薄。",
                "",
                "## 建议沉淀",
                "",
                "- prompt / test",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )
    _write_review(
        tmp_path / "handoff_round2.md",
        "\n".join(
            [
                "# Handoff",
                "",
                "- review_target：`docs/handoff.md`",
                "- review_prompt：`docs/prompts/handoff.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. 当前没有新的实质性问题。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "",
                "## 收敛结论",
                "",
                "- round：2",
                "- previous_round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "上一轮问题没有在下一轮闭环登记" in titles


def test_review_audit_flags_actionable_round_without_followup_round(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "stalled_round1.md",
        "\n".join(
            [
                "# Stalled",
                "",
                "- review_target：`docs/stalled.md`",
                "- review_prompt：`docs/prompts/stalled.md`",
                "",
                "## 结论",
                "",
                "`hold`",
                "",
                "## 主要问题",
                "",
                "1. `P2`：证据链仍需补全。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示再看，最明显的问题仍是证据不够扎实。",
                "",
                "## 建议沉淀",
                "",
                "- prompt / test",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "主要问题还没进入下一轮闭环" in titles


def test_review_audit_skips_legacy_round_notes(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "legacy_round1.md",
        "\n".join(
            [
                "# Legacy",
                "",
                "- review_target：`docs/legacy.md`",
                "- review_prompt：`docs/prompts/legacy.md`",
                "",
                "## 一句话总评",
                "",
                "旧模板只有一句话总评，没有结构化收敛结论。",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)

    assert audit["summary"]["audited_records"] == 0
    assert audit["summary"]["skipped_legacy_records"] == 1
    assert audit["summary"]["total_findings"] == 0


def test_review_audit_accepts_numbered_alias_sections(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "alias_round1.md",
        "\n".join(
            [
                "# Alias",
                "",
                "- review_target：`docs/alias.md`",
                "- review_prompt：`docs/prompts/alias.md`",
                "",
                "## 1. 一句话总评",
                "",
                "`go with conditions`",
                "",
                "## 3. 主要问题",
                "",
                "1. `P2`：仍有小问题",
                "",
                "## 7. 框架外问题",
                "",
                "1. 无新的实质性框架外阻塞问题",
                "",
                "## 8. 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- prompt / backlog",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}

    assert "缺少必需外审段落" not in titles


def test_review_audit_flags_manifest_missing_factor_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/stock_picks/final/stock_picks_cn_2026-03-16_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/stock_picks/final/stock_picks_cn_2026-03-16_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"report_type": "stock_pick", "artifacts": {}}, ensure_ascii=False), encoding="utf-8")
    _write_review(
        tmp_path / "stock_pick_round1.md",
        "\n".join(
            [
                "# Stock pick",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "manifest 缺少 factor_contract" in titles


def test_review_audit_flags_manifest_missing_proxy_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/etf_picks/final/etf_pick_2026-03-21_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/etf_picks/final/etf_pick_2026-03-21_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "etf_pick",
                "artifacts": {"factor_contract": {"families": {"J-5": 1}}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "etf_pick_round1.md",
        "\n".join(
            [
                "# ETF pick",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "manifest 缺少 proxy_contract" in titles


def test_review_audit_flags_briefing_manifest_missing_proxy_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/briefings/final/daily_briefing_2026-03-21_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/briefings/final/daily_briefing_2026-03-21_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "briefing",
                "artifacts": {"factor_contract": {"families": {"J-3": 1}}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "briefing_round1.md",
        "\n".join(
            [
                "# Briefing",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "manifest 缺少 proxy_contract" in titles


def test_review_audit_flags_manifest_missing_theme_playbook_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/scans/etfs/final/scan_159981_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/scans/etfs/final/scan_159981_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "scan",
                "artifacts": {"symbol": "159981"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "scan_round1.md",
        "\n".join(
            [
                "# Scan",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "manifest 缺少 theme_playbook_contract" in titles


def test_review_audit_flags_incomplete_theme_playbook_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/stocks/final/stock_analysis_300308_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/stocks/final/stock_analysis_300308_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "stock_analysis",
                "artifacts": {
                    "theme_playbook_contract": {
                        "label": "信息技术",
                        "playbook_level": "sector",
                        "theme_match_status": "ambiguous_conflict",
                        "subtheme_bridge_confidence": "high",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "stock_analysis_round1.md",
        "\n".join(
            [
                "# Stock Analysis",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "theme_playbook_contract 缺少冲突候选主题" in titles
    assert "theme_playbook_contract 缺少下钻主线" in titles


def test_review_audit_flags_manifest_missing_event_digest_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/scans/etfs/final/scan_159981_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/scans/etfs/final/scan_159981_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "scan",
                "artifacts": {"theme_playbook_contract": {"label": "信息技术", "playbook_level": "sector"}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "scan_event_digest_round1.md",
        "\n".join(
            [
                "# Scan",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "manifest 缺少 event_digest_contract" in titles


def test_review_audit_flags_incomplete_event_digest_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/stocks/final/stock_analysis_300308_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/stocks/final/stock_analysis_300308_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "stock_analysis",
                "artifacts": {
                    "event_digest_contract": {
                        "status": "已消化",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "stock_analysis_event_digest_round1.md",
        "\n".join(
            [
                "# Stock Analysis",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "event_digest_contract 信息不完整" in titles
    assert "event_digest_contract 缺少有效事件分层" in titles


def test_review_audit_flags_partial_event_digest_deep_fields(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/stocks/final/stock_analysis_300308_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/stocks/final/stock_analysis_300308_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "stock_analysis",
                "artifacts": {
                    "event_digest_contract": {
                        "status": "已消化",
                        "lead_layer": "公告",
                        "lead_detail": "公告类型：中标/订单",
                        "changed_what": "把研究重点推进到公司级执行层。",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "stock_analysis_event_digest_round2.md",
        "\n".join(
            [
                "# Stock Analysis",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "event_digest_contract 深度字段不完整" in titles


def test_review_audit_flags_event_digest_missing_importance_reason(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/stocks/final/stock_analysis_300308_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/stocks/final/stock_analysis_300308_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "stock_analysis",
                "artifacts": {
                    "event_digest_contract": {
                        "status": "已消化",
                        "lead_layer": "公告",
                        "lead_detail": "公告类型：中标/订单",
                        "impact_summary": "盈利 / 景气",
                        "thesis_scope": "thesis变化",
                        "changed_what": "把研究重点推进到公司级执行层。",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "stock_analysis_event_digest_round3.md",
        "\n".join(
            [
                "# Stock Analysis",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "event_digest_contract 深度字段不完整" in titles


def test_review_audit_flags_manifest_missing_what_changed_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/scans/etfs/final/scan_159981_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/scans/etfs/final/scan_159981_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "scan",
                "artifacts": {
                    "event_digest_contract": {
                        "status": "已消化",
                        "lead_layer": "公告",
                        "changed_what": "把研究重点推进到公司级执行层。",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "scan_what_changed_round1.md",
        "\n".join(
            [
                "# Scan",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "manifest 缺少 what_changed_contract" in titles


def test_review_audit_flags_incomplete_what_changed_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/stocks/final/stock_analysis_300308_2026-03-28_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/stocks/final/stock_analysis_300308_2026-03-28_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "stock_analysis",
                "artifacts": {
                    "what_changed_contract": {
                        "previous_view": "核心假设是 `种业政策催化兑现`。",
                        "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                        "conclusion_label": "升级",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "stock_analysis_what_changed_round1.md",
        "\n".join(
            [
                "# Stock Analysis",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "what_changed_contract 缺少当前事件理解" in titles
    assert "what_changed_contract 缺少状态触发" in titles
    assert "what_changed_contract 缺少状态解释" not in titles


def test_review_audit_accepts_complete_what_changed_contract(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/scans/etfs/final/scan_159981_2026-03-29_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/scans/etfs/final/scan_159981_2026-03-29_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "scan",
                "artifacts": {
                    "what_changed_contract": {
                        "previous_view": "核心假设是 `800G 光模块放量兑现`。",
                        "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                        "conclusion_label": "升级",
                        "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
                        "state_trigger": "事件完成消化",
                        "state_summary": "当前事件已完成消化并更新主导事件，thesis 可以按更高确定性理解。",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "scan_what_changed_complete_round1.md",
        "\n".join(
            [
                "# Scan",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "what_changed_contract 缺少状态触发" not in titles
    assert "what_changed_contract 缺少状态解释" not in titles


def test_review_audit_flags_missing_what_changed_state_summary(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/scans/etfs/final/scan_159981_2026-03-29_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    manifest_path = tmp_path / "reports/reviews/scans/etfs/final/scan_159981_2026-03-29_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "scan",
                "artifacts": {
                    "what_changed_contract": {
                        "previous_view": "核心假设是 `800G 光模块放量兑现`。",
                        "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                        "conclusion_label": "升级",
                        "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
                        "state_trigger": "事件完成消化",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "scan_what_changed_missing_state_summary_round1.md",
        "\n".join(
            [
                "# Scan",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "what_changed_contract 缺少状态解释" in titles


def test_review_audit_flags_incomplete_catalyst_web_review_from_manifest(tmp_path: Path) -> None:
    report_path = tmp_path / "reports/scans/etfs/final/scan_512480_2026-03-26_client_final.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    catalyst_review = tmp_path / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review.md"
    catalyst_review.parent.mkdir(parents=True, exist_ok=True)
    catalyst_review.write_text(
        "\n".join(
            [
                "# Catalyst Web Review | scan | 2026-03-26",
                "",
                "## 1. 半导体ETF (512480)",
                "",
                "### 复核结论",
                "",
                "- 结论：待补",
                "",
                "### 关键证据",
                "",
                "- 待补",
            ]
        ),
        encoding="utf-8",
    )
    manifest_path = tmp_path / "reports/reviews/scans/etfs/final/scan_512480_2026-03-26_client_final__release_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "report_type": "scan",
                "artifacts": {
                    "editor_artifacts": {
                        "catalyst_web_review": str(catalyst_review),
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_review(
        tmp_path / "scan_round1.md",
        "\n".join(
            [
                "# Scan",
                "",
                f"- 审稿对象：[{report_path.name}]({report_path})",
                "- 适用 prompt：`docs/prompts/external_financial_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "催化联网复核仍停留在待补模板" in titles


def test_review_audit_flags_missing_split_review_roles(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "demo_round1.md",
        "\n".join(
            [
                "# Demo",
                "",
                "- review_target：`docs/demo.md`",
                "- review_prompt：`docs/prompts/external_financial_structural_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "缺少分阶段外审执行者" in titles


def test_review_audit_flags_same_split_review_role(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "demo_round1.md",
        "\n".join(
            [
                "# Demo",
                "",
                "- review_target：`docs/demo.md`",
                "- review_prompt：`docs/prompts/external_financial_divergent_reviewer.md`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 主要问题",
                "",
                "1. `P3`：无阻塞。",
                "",
                "## 框架外问题",
                "",
                "1. 当前没有新的实质性框架外阻塞问题。",
                "",
                "## 零提示发散审",
                "",
                "1. 零提示复核后，没有新的高优先级问题。",
                "",
                "## 建议沉淀",
                "",
                "- workflow",
                "  - 维持现有协议。",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 结构审执行者：gpt-5.4 / reviewer_shared",
                "- 发散审执行者：gpt-5.4 / reviewer_shared",
            ]
        ),
    )

    audit = build_review_audit(tmp_path)
    titles = {item["title"] for item in audit["findings"]}
    assert "结构审与发散审使用了同一执行者" in titles
