from __future__ import annotations

import json
from pathlib import Path

from src.reporting.review_audit import build_review_audit, render_review_audit_markdown


def _write_review(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


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
