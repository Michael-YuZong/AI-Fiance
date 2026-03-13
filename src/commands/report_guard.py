"""Hard gates for research markdown finalization."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from src.reporting.review_lessons import active_lesson_ids
from src.utils.config import resolve_project_path


REPORT_TYPES = {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan", "retrospect"}
REQUIRED_REVIEW_HEADINGS = (
    "## 一句话总评",
    "## 主要问题",
    "## 独立答案",
    "## 收敛结论",
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


class ReportGuardError(RuntimeError):
    """Raised when a report violates hard workflow rules."""


@dataclass(frozen=True)
class ReviewSummary:
    review_path: Path
    status: str
    approved: bool


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
    missing = [heading for heading in REQUIRED_REVIEW_HEADINGS if heading not in review_text]
    if missing:
        raise ReportGuardError("外部评审意见缺少必要章节: " + "、".join(missing))

    status_match = re.search(r"状态[：:]\s*(PASS|BLOCKED)", review_text, re.I)
    approved_match = re.search(r"允许作为成稿交付[：:]\s*(是|否)", review_text)
    no_p1_match = re.search(r"无新的 P0/P1[：:]\s*(是|否)", review_text)

    if not status_match:
        raise ReportGuardError("外部评审意见缺少 `状态：PASS/BLOCKED`")
    if not approved_match:
        raise ReportGuardError("外部评审意见缺少 `允许作为成稿交付：是/否`")
    if not no_p1_match:
        raise ReportGuardError("外部评审意见缺少 `无新的 P0/P1：是/否`")

    status = status_match.group(1).upper()
    approved = approved_match.group(1) == "是" and no_p1_match.group(1) == "是" and status == "PASS"
    return ReviewSummary(review_path=Path(), status=status, approved=approved)


def load_review_summary(markdown_path: Path) -> ReviewSummary:
    review_path = review_path_for(markdown_path)
    if not review_path.exists():
        raise ReportGuardError(
            "外部评审未完成：缺少评审意见文件 "
            f"`{review_path}`。只有通过独立外审并收敛后，才允许写入 final。"
        )
    review_text = review_path.read_text(encoding="utf-8")
    summary = _validate_review_text(review_text)
    assert summary is not None
    if not summary.approved:
        raise ReportGuardError(f"外部评审尚未通过：`{review_path}` 仍标记为 `{summary.status}` 或未明确允许交付。")
    return ReviewSummary(review_path=review_path, status=summary.status, approved=True)


def ensure_detailed_final_content(report_type: str, markdown_text: str) -> None:
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
