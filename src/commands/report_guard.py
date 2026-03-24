"""Hard gates for research markdown finalization."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from src.reporting.review_lessons import active_lesson_ids
from src.reporting.review_record_utils import (
    bullet_block_items,
    canonicalize_sections,
    clean_text,
    has_actionable_content,
    normalize_status,
    normalize_yes_no,
    parse_bullet_mapping,
    round_from_text,
    split_sections,
)
from src.utils.config import resolve_project_path


REPORT_TYPES = {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan", "retrospect", "strategy"}
REQUIRED_REVIEW_SECTIONS = (
    "一句话总评",
    "主要问题",
    "独立答案",
    "零提示发散审",
    "收敛结论",
)
DETAILED_FINAL_MARKERS = {
    "stock_pick": ("八维雷达", "催化拆解", "硬排除检查", "风险拆解", "历史相似样本"),
    "stock_analysis": ("## 为什么这么判断", "## 硬检查", "## 分维度详解"),
    "briefing": ("## 为什么今天这么判断", "## 宏观领先指标", "## 数据完整度", "## 今天怎么做", "## 重点观察", "## 今日A股观察池"),
    "fund_pick": ("## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只基金为什么是这个分", "## 标准化分类", "## 为什么不是另外几只"),
    "etf_pick": ("## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只ETF为什么是这个分", "## 标准化分类", "## 关键证据", "## 为什么不是另外几只"),
    "scan": ("## 为什么这么判断", "## 硬检查", "## 分维度详解"),
    "retrospect": ("## 原始决策", "## 为什么当时会做这个决定", "## 后验路径", "## 复盘结论"),
}


def _stock_pick_required_markers(markdown_text: str) -> tuple[Any, ...]:
    observe_only = "| 报告定位 | 观察稿 |" in markdown_text or "当前没有达到正式动作阈值的个股" in markdown_text
    if observe_only:
        return (
            "## 今日动作摘要",
            "## 催化证据来源",
            "## 历史相似样本附注",
            ("## A股", "## 港股", "## 美股"),
            "升级条件",
            "关键盯盘价位",
        )
    return DETAILED_FINAL_MARKERS["stock_pick"]


def _strategy_required_markers(markdown_text: str) -> tuple[Any, ...]:
    if "# Strategy Validation" in markdown_text:
        return (
            "## 这套策略是什么",
            "## 这次到底看出来什么",
            "## 执行摘要",
            "## 总体结果",
            "## Rollback Gate",
            ("## Out-Of-Sample Validation", "## Chronological Cohorts", "## Cross-Sectional Validation"),
        )
    if "# Strategy Experiment" in markdown_text:
        return (
            "## 这套策略是什么",
            "## 这次到底看出来什么",
            "## 执行摘要",
            "## Promotion Gate",
            "## Rollback Gate",
            "## 变体对比",
        )
    return (
        "## 这套策略是什么",
        "## 这次到底看出来什么",
        "## 执行摘要",
        ("## 总体结果", "## Promotion Gate"),
        "## Rollback Gate",
    )


class ReportGuardError(RuntimeError):
    """Raised when a report violates hard workflow rules."""


@dataclass(frozen=True)
class ReviewSummary:
    review_path: Path
    status: str
    approved: bool


def _extract_review_section(review_text: str, section_title: str) -> str:
    pattern = re.compile(
        rf"^##+\s*{re.escape(section_title)}\s*$\n?(.*?)(?=^##+\s+\S|\Z)",
        re.M | re.S,
    )
    match = pattern.search(review_text)
    return (match.group(1).strip() if match else "")


def ensure_report_task_registered(report_type: str) -> None:
    if report_type not in REPORT_TYPES:
        known = ", ".join(sorted(REPORT_TYPES))
        raise ReportGuardError(f"未注册的研究成稿类型 `{report_type}`；当前只允许: {known}")


def review_path_for(markdown_path: Path) -> Path:
    reports_root = resolve_project_path("reports")
    target = markdown_path.resolve()
    relative = target.relative_to(reports_root.resolve())
    stem = relative.with_suffix("")
    review_root = resolve_project_path("reports/reviews")
    return review_root / stem.parent / f"{stem.name}__external_review.md"


def manifest_path_for(markdown_path: Path) -> Path:
    reports_root = resolve_project_path("reports")
    target = markdown_path.resolve()
    relative = target.relative_to(reports_root.resolve())
    stem = relative.with_suffix("")
    manifest_root = resolve_project_path("reports/reviews")
    return manifest_root / stem.parent / f"{stem.name}__release_manifest.json"


def _validate_review_text(review_text: str) -> ReviewSummary | None:
    raw_sections = split_sections(review_text)
    sections = canonicalize_sections(raw_sections)
    missing = [title for title in REQUIRED_REVIEW_SECTIONS if not _extract_review_section(review_text, title)]
    if missing:
        raise ReportGuardError("外部评审意见缺少必要章节: " + "、".join(missing))

    convergence_text = sections.get("收敛结论", "")
    convergence = parse_bullet_mapping(convergence_text.splitlines())
    status = normalize_status(convergence.get("状态", ""))
    approved_flag = normalize_yes_no(convergence.get("允许作为成稿交付", ""))
    no_p1_flag = normalize_yes_no(convergence.get("无新的 P0/P1", ""))
    converged_flag = normalize_yes_no(convergence.get("本轮是否收敛", ""))
    continue_flag = normalize_yes_no(convergence.get("是否建议继续下一轮", ""))
    round_value = round_from_text(convergence.get("round", ""))
    previous_round_value = round_from_text(convergence.get("previous_round", ""))
    structural_reviewer = clean_text(convergence.get("结构审执行者", ""))
    divergent_reviewer = clean_text(convergence.get("发散审执行者", ""))

    if not status:
        raise ReportGuardError("外部评审意见缺少 `状态：PASS/BLOCKED`")
    if approved_flag not in {"是", "否"}:
        raise ReportGuardError("外部评审意见缺少 `允许作为成稿交付：是/否`")
    if no_p1_flag not in {"是", "否"}:
        raise ReportGuardError("外部评审意见缺少 `无新的 P0/P1：是/否`")
    if round_value is None:
        raise ReportGuardError("外部评审意见缺少 `round`；正式成稿必须保留 round-based 收敛字段。")
    if converged_flag not in {"是", "否"}:
        raise ReportGuardError("外部评审意见缺少 `本轮是否收敛：是/否`。")
    if continue_flag not in {"是", "否"}:
        raise ReportGuardError("外部评审意见缺少 `是否建议继续下一轮：是/否`。")
    if round_value > 1 and previous_round_value is None:
        raise ReportGuardError("第 2 轮及之后的外审意见缺少 `previous_round`。")
    if not structural_reviewer:
        raise ReportGuardError("外部评审意见缺少 `结构审执行者`；正式成稿外审必须显式记录 Pass A 执行者。")
    if not divergent_reviewer:
        raise ReportGuardError("外部评审意见缺少 `发散审执行者`；正式成稿外审必须显式记录 Pass B 执行者。")
    if _normalize_reviewer_identity(structural_reviewer) == _normalize_reviewer_identity(divergent_reviewer):
        raise ReportGuardError("外部评审意见不合格：`结构审执行者` 与 `发散审执行者` 不能是同一个 reviewer / 子 agent。")

    approved = approved_flag == "是" and no_p1_flag == "是" and status == "PASS"

    review_prelude = review_text.split("## 收敛结论", 1)[0]
    unresolved_p0_p1 = re.search(r"^###\s*P[01][^\n]*(未关闭|未修正|仍未|仍然系统性存在)", review_prelude, re.M)
    newly_found_p0_p1 = re.search(r"(新增|新发现)[^。\n]{0,20}P[01]", review_prelude)
    delivery_conflict = "对外客户稿需先修正" in review_prelude or "仍有结论层模板化" in review_prelude
    if no_p1_flag == "是" and (unresolved_p0_p1 or newly_found_p0_p1):
        raise ReportGuardError("外部评审正文与收敛结论冲突：正文仍记录未关闭或新增的 P0/P1，但收敛结论写成 `无新的 P0/P1：是`。")
    if approved_flag == "是" and status == "PASS" and delivery_conflict:
        raise ReportGuardError("外部评审正文与交付结论冲突：正文仍写着需要先修正，但收敛结论已允许成稿交付。")
    if status == "PASS" and converged_flag != "是":
        raise ReportGuardError("外部评审尚未收敛：`状态：PASS` 时必须同时写明 `本轮是否收敛：是`。")
    if status == "PASS" and continue_flag != "否":
        raise ReportGuardError("外部评审收敛结论冲突：`状态：PASS` 时 `是否建议继续下一轮` 必须为“否”。")

    if status == "PASS":
        actionable_sections = [
            title for title in ("主要问题", "框架外问题", "零提示发散审") if has_actionable_content(sections.get(title, ""))
        ]
        if round_value == 1 and actionable_sections:
            raise ReportGuardError(
                "外部评审缺少回修闭环：round 1 仍有 actionable finding，不能直接 PASS。"
            )
        if round_value > 1:
            carried_items = bullet_block_items(convergence_text, "carried_p0_p1")
            closed_items = bullet_block_items(convergence_text, "closed_items")
            if not carried_items and not closed_items:
                raise ReportGuardError(
                    "外部评审收敛证据不足：多轮 PASS 记录必须显式写出 `closed_items` 或 `carried_p0_p1`。"
                )
    return ReviewSummary(review_path=Path(), status=status, approved=approved)


def _normalize_reviewer_identity(value: str) -> str:
    text = clean_text(value).strip().lower()
    if text.startswith("`") and text.endswith("`") and len(text) >= 2:
        text = text[1:-1].strip()
    return re.sub(r"\s+", " ", text)


def load_review_summary(markdown_path: Path) -> ReviewSummary:
    review_path = review_path_for(markdown_path)
    if not review_path.exists():
        raise ReportGuardError(
            "外部评审未完成：缺少评审意见文件 "
            f"`{review_path}`。不要停在“没有 review 文件”的状态；应先补外部评审记录并收敛后，再写入 final。"
        )
    review_text = review_path.read_text(encoding="utf-8")
    summary = _validate_review_text(review_text)
    assert summary is not None
    if not summary.approved:
        raise ReportGuardError(f"外部评审尚未通过：`{review_path}` 仍标记为 `{summary.status}` 或未明确允许交付。")
    return ReviewSummary(review_path=review_path, status=summary.status, approved=True)


def ensure_detailed_final_content(report_type: str, markdown_text: str) -> None:
    if report_type == "stock_pick":
        required_markers = _stock_pick_required_markers(markdown_text)
    elif report_type == "strategy":
        required_markers = _strategy_required_markers(markdown_text)
    else:
        required_markers = DETAILED_FINAL_MARKERS.get(report_type, ())
    missing = []
    for marker in required_markers:
        if isinstance(marker, tuple):
            if not any(option in markdown_text for option in marker):
                missing.append(" / ".join(marker))
            continue
        if marker not in markdown_text:
            missing.append(marker)
    if missing:
        raise ReportGuardError(
            "成稿必须是详细解释版，当前内容缺少关键章节: " + "、".join(missing)
        )


def _write_manifest(
    *,
    report_type: str,
    markdown_path: Path,
    review_path: Path,
    extra: Mapping[str, Any] | None = None,
) -> Path:
    manifest_path = manifest_path_for(markdown_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {
        "report_type": report_type,
        "markdown": str(markdown_path),
        "review": str(review_path),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hard_rules": {
            "independent_review_required": True,
            "explanation_required": True,
            "detailed_final_required": True,
            "release_check_required": True,
            "final_requires_review_pass": True,
            "active_lessons": active_lesson_ids(),
        },
    }
    if extra:
        payload["artifacts"] = dict(extra)
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def export_reviewed_markdown_bundle(
    *,
    report_type: str,
    markdown_text: str,
    markdown_path: Path,
    release_findings: Sequence[str] | None = None,
    extra_manifest: Mapping[str, Any] | None = None,
) -> Dict[str, Path]:
    ensure_report_task_registered(report_type)
    findings = [str(item).strip() for item in (release_findings or []) if str(item).strip()]
    if findings:
        raise ReportGuardError("发布前一致性校验失败: " + "；".join(findings))
    ensure_detailed_final_content(report_type, markdown_text)

    review_summary = load_review_summary(markdown_path)
    from src.output.client_export import export_markdown_bundle

    bundle = export_markdown_bundle(markdown_text, markdown_path, allow_unreviewed_final=True)
    manifest_path = _write_manifest(
        report_type=report_type,
        markdown_path=markdown_path,
        review_path=review_summary.review_path,
        extra=extra_manifest,
    )
    bundle["review"] = review_summary.review_path
    bundle["manifest"] = manifest_path
    return bundle


def exported_bundle_lines(bundle: Mapping[str, Any]) -> list[str]:
    lines: list[str] = []
    markdown_path = bundle.get("markdown")
    html_path = bundle.get("html")
    pdf_path = bundle.get("pdf")
    if markdown_path:
        lines.append(f"[client markdown] {markdown_path}")
    if html_path:
        lines.append(f"[client html] {html_path}")
    if pdf_path:
        lines.append(f"[client pdf] {pdf_path}")
    return lines
