from __future__ import annotations

from pathlib import Path

from src.reporting.review_audit import build_review_audit
from src.reporting.review_ledger import parse_review_record
from src.reporting.review_scaffold import ensure_external_review_scaffold, maybe_autoclose_external_review


def test_review_scaffold_creates_parseable_strategy_review(tmp_path: Path) -> None:
    target = tmp_path / "reports/strategy/validate/final/strategy_validate_600519_2026-03-23_client_final.md"
    review = tmp_path / "reports/reviews/strategy/validate/final/strategy_validate_600519_2026-03-23_client_final__external_review.md"
    detail = tmp_path / "reports/strategy/validate/internal/strategy_validate_600519_2026-03-23_internal_detail.md"

    ensure_external_review_scaffold(
        review_path=review,
        markdown_path=target,
        report_type="strategy",
        report_kind="validate",
        detail_source=detail,
    )

    text = review.read_text(encoding="utf-8")
    record = parse_review_record(review)

    assert "external_financial_structural_reviewer.md" in text
    assert "external_financial_divergent_reviewer.md" in text
    assert "strategy validate --client-final" in text
    assert record.status == "BLOCKED"
    assert record.round == 1
    assert record.review_target_ref == str(target)
    assert record.review_prompt_ref.endswith("external_financial_structural_reviewer.md")


def test_review_scaffold_satisfies_round_audit_sections(tmp_path: Path) -> None:
    review = tmp_path / "reports/reviews/strategy/experiment/final/strategy_experiment_600519_2026-03-23_client_final__external_review.md"
    ensure_external_review_scaffold(
        review_path=review,
        markdown_path=tmp_path / "reports/strategy/experiment/final/strategy_experiment_600519_2026-03-23_client_final.md",
        report_type="strategy",
        report_kind="experiment",
    )

    audit = build_review_audit(tmp_path / "reports/reviews")
    titles = {item["title"] for item in audit["findings"]}

    assert "缺少必需外审段落" not in titles
    assert "缺少分阶段外审执行者" not in titles


def test_review_scaffold_uses_custom_generated_by_label(tmp_path: Path) -> None:
    review = tmp_path / "reports/reviews/briefings/final/daily_briefing_2026-03-23_client_final__external_review.md"
    ensure_external_review_scaffold(
        review_path=review,
        markdown_path=tmp_path / "reports/briefings/final/daily_briefing_2026-03-23_client_final.md",
        report_type="briefing",
        scaffold_generated_by="briefing daily --client-final",
    )

    text = review.read_text(encoding="utf-8")
    assert "briefing daily --client-final" in text


def test_review_scaffold_uses_generic_solidification_copy_for_non_strategy(tmp_path: Path) -> None:
    review = tmp_path / "reports/reviews/briefings/final/daily_briefing_2026-03-23_client_final__external_review.md"
    ensure_external_review_scaffold(
        review_path=review,
        markdown_path=tmp_path / "reports/briefings/final/daily_briefing_2026-03-23_client_final.md",
        report_type="briefing",
    )

    text = review.read_text(encoding="utf-8")
    assert "当前成稿链" in text
    assert "strategy final" not in text


def test_maybe_autoclose_external_review_writes_round2_pass(tmp_path: Path) -> None:
    review = tmp_path / "reports/reviews/briefings/final/daily_briefing_2026-03-23_client_final__external_review.md"
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        "\n".join(
            [
                "# `briefing` 外审结果",
                "",
                "## 一句话总评",
                "已无新问题。",
                "",
                "## 主要问题",
                "- 无新的实质问题。",
                "",
                "## 独立答案",
                "- 当前只差 round-based 收敛闭环。",
                "",
                "## 框架外问题",
                "- 无新的实质性框架外问题。",
                "",
                "## 零提示发散审",
                "- 无新的实质性发散问题。",
                "",
                "## 建议沉淀",
                "- workflow",
                "  - 保留共享收敛逻辑。",
                "",
                "## 收敛结论",
                "- round：1",
                "- 状态：BLOCKED",
                "- 无新的 P0/P1：是",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
                "- 允许作为成稿交付：否",
                "- 是否允许开始实现：否",
                "- 结构审执行者：`pending_structural_reviewer`",
                "- 发散审执行者：`pending_divergent_reviewer`",
            ]
        ),
        encoding="utf-8",
    )

    changed = maybe_autoclose_external_review(
        review_path=review,
        markdown_path=tmp_path / "reports/briefings/final/daily_briefing_2026-03-23_client_final.md",
        report_type="briefing",
        scaffold_generated_by="briefing daily --client-final",
    )

    assert changed is True
    archived = review.with_name("daily_briefing_2026-03-23_client_final__external_review_round1.md")
    assert archived.exists()
    text = review.read_text(encoding="utf-8")
    assert "- round：2" in text
    assert "- previous_round：1" in text
    assert "- 状态：PASS" in text


def test_maybe_autoclose_external_review_keeps_generated_scaffold_blocked(tmp_path: Path) -> None:
    review = tmp_path / "reports/reviews/stock_picks/final/stock_picks_cn_2026-04-08_final__external_review.md"
    target = tmp_path / "reports/stock_picks/final/stock_picks_cn_2026-04-08_final.md"
    detail = tmp_path / "reports/stock_picks/internal/stock_picks_cn_2026-04-08_internal_detail.md"

    ensure_external_review_scaffold(
        review_path=review,
        markdown_path=target,
        report_type="stock_pick",
        detail_source=detail,
        scaffold_generated_by="stock_pick --client-final",
    )

    changed = maybe_autoclose_external_review(
        review_path=review,
        markdown_path=target,
        report_type="stock_pick",
        detail_source=detail,
        scaffold_generated_by="stock_pick --client-final",
    )

    assert changed is False
    archived = review.with_name("stock_picks_cn_2026-04-08_final__external_review_round1.md")
    assert not archived.exists()
    text = review.read_text(encoding="utf-8")
    assert "- 状态：BLOCKED" in text
    assert "自动生成的 review scaffold" in text
