from __future__ import annotations

from pathlib import Path

from src.reporting.review_ledger import build_review_ledger, parse_review_record, render_review_ledger_markdown


def _write_review(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_parse_review_record_extracts_round_and_convergence_fields(tmp_path: Path) -> None:
    path = tmp_path / "strategy_plan_review_2026-03-14_round3.md"
    _write_review(
        path,
        "\n".join(
            [
                "# `strategy` 计划外审结果",
                "",
                "- 审稿对象：[plan.md](/tmp/plan.md)",
                "- 适用 prompt：[external_strategy_plan_reviewer.md](/tmp/external_strategy_plan_reviewer.md)",
                "- previous_round：[round2.md](/tmp/round2.md)",
                "- 审稿方式：`合同审 + 发散审`",
                "",
                "## 结论",
                "",
                "`go`",
                "",
                "## 收敛结论",
                "",
                "- round：3",
                "- previous_round：2",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 是否允许开始实现：是",
            ]
        ),
    )

    record = parse_review_record(path)

    assert record.series_id == "strategy_plan_review_2026-03-14"
    assert record.protocol == "structured_round"
    assert record.round == 3
    assert record.previous_round == 2
    assert record.status == "PASS"
    assert record.converged == "是"
    assert record.review_target_ref == "/tmp/plan.md"
    assert record.review_prompt_ref == "/tmp/external_strategy_plan_reviewer.md"


def test_build_review_ledger_groups_latest_rounds_and_active_loops(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "foo_round1.md",
        "\n".join(
            [
                "# Foo",
                "",
                "- review_target：`docs/foo.md`",
                "- review_prompt：`docs/prompts/foo.md`",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：BLOCKED",
                "- 本轮新增 P0/P1：是",
                "- 上一轮 P0/P1 是否已关闭：否",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )
    _write_review(
        tmp_path / "bar_round1.md",
        "\n".join(
            [
                "# Bar",
                "",
                "- review_target：`docs/bar.md`",
                "- review_prompt：`docs/prompts/bar.md`",
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
    _write_review(
        tmp_path / "foo_round2.md",
        "\n".join(
            [
                "# Foo",
                "",
                "- review_target：`docs/foo.md`",
                "- review_prompt：`docs/prompts/foo.md`",
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
            ]
        ),
    )

    ledger = build_review_ledger(tmp_path)

    assert ledger["summary"]["total_records"] == 3
    assert ledger["summary"]["total_series"] == 2
    assert ledger["summary"]["active_series"] == 0
    assert ledger["summary"]["converged_series"] == 2
    assert {row["series_id"] for row in ledger["latest_records"]} == {"foo", "bar"}


def test_build_review_ledger_separates_legacy_round_notes(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "legacy_round1.md",
        "\n".join(
            [
                "# Legacy review round 1",
                "",
                "- review_target：`docs/legacy.md`",
                "- review_prompt：`docs/prompts/legacy.md`",
                "",
                "## 一句话总评",
                "",
                "这还是旧模板。",
            ]
        ),
    )

    ledger = build_review_ledger(tmp_path)

    assert ledger["summary"]["legacy_round_note_series"] == 1
    assert ledger["summary"]["legacy_unstructured_series"] == 0
    assert ledger["latest_records"][0]["protocol"] == "legacy_round_note"


def test_parse_review_record_normalizes_status_and_continue_flags(tmp_path: Path) -> None:
    path = tmp_path / "normalized_round2.md"
    _write_review(
        path,
        "\n".join(
            [
                "# Normalized",
                "",
                "- 审稿对象：[demo.md](/tmp/demo.md)",
                "",
                "## 收敛结论",
                "",
                "- round：2",
                "- previous_round：1",
                "- 状态：PASS（有条件）",
                "- 本轮新增 P0/P1：否（仅保留 P2）",
                "- 上一轮 P0/P1 是否已关闭：是（已关闭）",
                "- 本轮是否收敛：是（条件性通过）",
                "- 是否建议继续下一轮：否（无需下一轮）",
            ]
        ),
    )

    record = parse_review_record(path)

    assert record.status == "PASS"
    assert record.new_p0_p1 == "否"
    assert record.previous_round_closed == "是"
    assert record.converged == "是"
    assert record.recommend_continue == "否"


def test_render_review_ledger_markdown_contains_summary_and_series_table(tmp_path: Path) -> None:
    _write_review(
        tmp_path / "research_round1.md",
        "\n".join(
            [
                "# Research",
                "",
                "- review_target：`reports/research/demo.md`",
                "- review_prompt：`docs/prompts/external_research_reviewer.md`",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：BLOCKED",
                "- 本轮新增 P0/P1：是",
                "- 上一轮 P0/P1 是否已关闭：否",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
    )

    markdown = render_review_ledger_markdown(build_review_ledger(tmp_path))

    assert "# External Review Ledger" in markdown
    assert "| series | round | status | converged | continue | target |" in markdown
    assert "`reports/research/demo.md`" in markdown
    assert "## Legacy Round Notes" in markdown
