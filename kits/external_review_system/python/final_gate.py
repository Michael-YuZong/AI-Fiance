"""Minimal final gate for round-based external review workflows."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Sequence

from .review_record_utils import (
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


REQUIRED_REVIEW_SECTIONS = (
    "一句话总评",
    "主要问题",
    "独立答案",
    "零提示发散审",
    "收敛结论",
)

Validator = Callable[[str, Mapping[str, Any] | None], Sequence[str]]


class ReviewGateError(RuntimeError):
    """Raised when an artifact violates the review workflow rules."""


@dataclass(frozen=True)
class ReviewSummary:
    review_path: Path
    status: str
    approved: bool


def _extract_review_section(review_text: str, section_title: str) -> str:
    pattern = re.compile(rf"^##+\s*{re.escape(section_title)}\s*$\n?(.*?)(?=^##+\s+\S|\Z)", re.M | re.S)
    match = pattern.search(review_text)
    return match.group(1).strip() if match else ""


def review_path_for(artifact_path: Path, *, outputs_root: Path, reviews_root: Path) -> Path:
    target = artifact_path.resolve()
    relative = target.relative_to(outputs_root.resolve())
    stem = relative.with_suffix("")
    return reviews_root.resolve() / stem.parent / f"{stem.name}__external_review.md"


def manifest_path_for(artifact_path: Path, *, outputs_root: Path, reviews_root: Path) -> Path:
    target = artifact_path.resolve()
    relative = target.relative_to(outputs_root.resolve())
    stem = relative.with_suffix("")
    return reviews_root.resolve() / stem.parent / f"{stem.name}__release_manifest.json"


def _normalize_reviewer_identity(value: str) -> str:
    text = clean_text(value).strip().lower()
    if text.startswith("`") and text.endswith("`") and len(text) >= 2:
        text = text[1:-1].strip()
    return re.sub(r"\s+", " ", text)


def _validate_review_text(review_path: Path, review_text: str) -> ReviewSummary:
    raw_sections = split_sections(review_text)
    sections = canonicalize_sections(raw_sections)
    missing = [title for title in REQUIRED_REVIEW_SECTIONS if not _extract_review_section(review_text, title)]
    if missing:
        raise ReviewGateError("外部评审意见缺少必要章节: " + "、".join(missing))

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
        raise ReviewGateError("外部评审意见缺少 `状态：PASS/BLOCKED`")
    if approved_flag not in {"是", "否"}:
        raise ReviewGateError("外部评审意见缺少 `允许作为成稿交付：是/否`")
    if no_p1_flag not in {"是", "否"}:
        raise ReviewGateError("外部评审意见缺少 `无新的 P0/P1：是/否`")
    if round_value is None:
        raise ReviewGateError("外部评审意见缺少 `round`；正式成稿必须保留 round-based 收敛字段。")
    if converged_flag not in {"是", "否"}:
        raise ReviewGateError("外部评审意见缺少 `本轮是否收敛：是/否`。")
    if continue_flag not in {"是", "否"}:
        raise ReviewGateError("外部评审意见缺少 `是否建议继续下一轮：是/否`。")
    if round_value > 1 and previous_round_value is None:
        raise ReviewGateError("第 2 轮及之后的外审意见缺少 `previous_round`。")
    if not structural_reviewer:
        raise ReviewGateError("外部评审意见缺少 `结构审执行者`。")
    if not divergent_reviewer:
        raise ReviewGateError("外部评审意见缺少 `发散审执行者`。")
    if _normalize_reviewer_identity(structural_reviewer) == _normalize_reviewer_identity(divergent_reviewer):
        raise ReviewGateError("外部评审意见不合格：`结构审执行者` 与 `发散审执行者` 不能是同一个 reviewer / 子 agent。")

    if status == "PASS" and converged_flag != "是":
        raise ReviewGateError("外部评审尚未收敛：`状态：PASS` 时必须同时写明 `本轮是否收敛：是`。")
    if status == "PASS" and continue_flag != "否":
        raise ReviewGateError("外部评审收敛结论冲突：`状态：PASS` 时 `是否建议继续下一轮` 必须为“否”。")

    actionable_sections = [
        title
        for title in ("主要问题", "框架外问题", "零提示发散审")
        if has_actionable_content(sections.get(title, ""))
    ]
    if status == "PASS" and round_value == 1 and actionable_sections:
        raise ReviewGateError("外部评审缺少回修闭环：round 1 仍有 actionable finding，不能直接 PASS。")
    if status == "PASS" and round_value > 1:
        carried_items = bullet_block_items(convergence_text, "carried_p0_p1")
        closed_items = bullet_block_items(convergence_text, "closed_items")
        if not carried_items and not closed_items:
            raise ReviewGateError("外部评审收敛证据不足：多轮 PASS 记录必须显式写出 `closed_items` 或 `carried_p0_p1`。")

    approved = approved_flag == "是" and no_p1_flag == "是" and status == "PASS"
    return ReviewSummary(review_path=review_path, status=status, approved=approved)


def load_review_summary(artifact_path: Path, *, outputs_root: Path, reviews_root: Path) -> ReviewSummary:
    review_path = review_path_for(artifact_path, outputs_root=outputs_root, reviews_root=reviews_root)
    if not review_path.exists():
        raise ReviewGateError(
            "外部评审未完成：缺少评审意见文件 "
            f"`{review_path}`。应先补 review scaffold、完成双 reviewer 外审并收敛后，再放行 final。"
        )
    summary = _validate_review_text(review_path, review_path.read_text(encoding="utf-8"))
    if not summary.approved:
        raise ReviewGateError(f"外部评审尚未通过：`{review_path}` 仍标记为 `{summary.status}` 或未明确允许交付。")
    return summary


def ensure_required_markers(artifact_text: str, required_markers: Sequence[str | tuple[str, ...]]) -> None:
    missing = []
    for marker in required_markers:
        if isinstance(marker, tuple):
            if not any(option in artifact_text for option in marker):
                missing.append(" / ".join(marker))
            continue
        if marker not in artifact_text:
            missing.append(marker)
    if missing:
        raise ReviewGateError("正式交付物缺少关键章节或标记: " + "、".join(missing))


def write_release_manifest(
    *,
    artifact_path: Path,
    outputs_root: Path,
    reviews_root: Path,
    artifact_type: str,
    review_path: Path,
    extra_manifest: Mapping[str, Any] | None = None,
) -> Path:
    manifest_path = manifest_path_for(artifact_path, outputs_root=outputs_root, reviews_root=reviews_root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {
        "artifact_type": artifact_type,
        "artifact": str(artifact_path),
        "review": str(review_path),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hard_rules": {
            "independent_review_required": True,
            "round_based_convergence_required": True,
            "final_requires_review_pass": True,
        },
    }
    if extra_manifest:
        payload["artifacts"] = dict(extra_manifest)
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest_path


def run_final_gate(
    *,
    artifact_path: Path,
    outputs_root: Path,
    reviews_root: Path,
    artifact_type: str = "artifact",
    artifact_text: str = "",
    validators: Sequence[Validator] | None = None,
    extra_manifest: Mapping[str, Any] | None = None,
) -> Dict[str, Path]:
    findings = []
    for validator in list(validators or []):
        findings.extend(str(item).strip() for item in validator(artifact_text, extra_manifest) if str(item).strip())
    if findings:
        raise ReviewGateError("发布前一致性校验失败: " + "；".join(findings))

    review_summary = load_review_summary(artifact_path, outputs_root=outputs_root, reviews_root=reviews_root)
    manifest_path = write_release_manifest(
        artifact_path=artifact_path,
        outputs_root=outputs_root,
        reviews_root=reviews_root,
        artifact_type=artifact_type,
        review_path=review_summary.review_path,
        extra_manifest=extra_manifest,
    )
    return {"review": review_summary.review_path, "manifest": manifest_path}
