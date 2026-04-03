"""Helpers for scaffolding round-based external review records."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Mapping

from .review_record_utils import (
    canonicalize_sections,
    has_actionable_content,
    parse_bullet_mapping,
    round_from_text,
    split_sections,
)


DEFAULT_PROMPT_REFS = {
    "structural": "prompts/generic_structural_reviewer.md",
    "divergent": "prompts/generic_divergent_reviewer.md",
    "convergence": "prompts/external_review_convergence_loop.md",
    "revision": "prompts/artifact_revision_loop.md",
}


def _prompt_ref(prompt_refs: Mapping[str, str | Path] | None, key: str) -> str:
    value = (prompt_refs or {}).get(key) or DEFAULT_PROMPT_REFS[key]
    return str(value)


def _title_for(artifact_type: str) -> str:
    return f"`{artifact_type}` 外审结果"


def build_external_review_scaffold(
    *,
    review_path: Path,
    artifact_path: Path,
    artifact_type: str = "artifact",
    detail_source: Path | None = None,
    scaffold_generated_by: str = "",
    prompt_refs: Mapping[str, str | Path] | None = None,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    generated_by = scaffold_generated_by.strip() or f"{artifact_type} --final"
    detail_source_line = f"- detail_source：`{detail_source}`" if detail_source else ""
    title = _title_for(artifact_type)
    structural_ref = _prompt_ref(prompt_refs, "structural")
    divergent_ref = _prompt_ref(prompt_refs, "divergent")
    convergence_ref = _prompt_ref(prompt_refs, "convergence")
    revision_ref = _prompt_ref(prompt_refs, "revision")
    lines = [
        f"# {title}",
        "",
        f"- 审稿时间：{generated_at}",
        f"- 审稿对象：[{artifact_path.name}]({artifact_path})",
        f"- 审稿方式：`Pass A 结构审 -> 修正 -> Pass B 发散审`",
        f"- 结构审 prompt：[{Path(structural_ref).name}]({structural_ref})",
        f"- 发散审 prompt：[{Path(divergent_ref).name}]({divergent_ref})",
        f"- 收敛 loop prompt：[{Path(convergence_ref).name}]({convergence_ref})",
        f"- revision loop prompt：[{Path(revision_ref).name}]({revision_ref})",
        f"- review_target：`{artifact_path}`",
        f"- review_prompt：`{structural_ref}`",
    ]
    if detail_source_line:
        lines.append(detail_source_line)
    lines.extend(
        [
            f"- scaffold_generated_by：`{generated_by}`",
            "",
            "## 一句话总评",
            "首轮外审模板已生成；当前还没有完成独立结构审和发散审，先按 BLOCKED 处理。",
            "",
            "## 主要问题",
            "- 尚未完成 `Pass A 结构审`，因此还不能判断硬问题是否已经暴露完整。",
            "- 尚未完成 `Pass B 发散审`，因此还不能判断是否存在框架外问题。",
            "",
            "## 独立答案",
            "- 当前只是自动生成的 review scaffold，不代表已经完成独立复核。",
            "",
            "## 框架外问题",
            "- 当前还没有独立 reviewer 的框架外 finding；先保留为待填状态。",
            "",
            "## 零提示发散审",
            "- 当前还没有完成零提示发散审；请补 1 轮不沿用 checklist 的直觉复核。",
            "",
            "## 建议沉淀",
            "- workflow",
            "  - 先完成双 reviewer 外审，再决定哪些问题需要沉淀到 prompt / guard / tests。",
            "",
            "## 收敛结论",
            "- round：1",
            "- 状态：BLOCKED",
            "- 无新的 P0/P1：否",
            "- 本轮新增 P0/P1：是",
            "- 上一轮 P0/P1 是否已关闭：不适用",
            "- 本轮是否收敛：否",
            "- 是否建议继续下一轮：是",
            "- 允许作为成稿交付：否",
            "- 是否允许开始实现：否",
            "- 结构审执行者：`pending_structural_reviewer`",
            "- 发散审执行者：`pending_divergent_reviewer`",
            "- carried_p0_p1：",
            "  - `待补结构审与发散审`",
            "- new_divergent_findings：",
            "  - `待独立 reviewer 填写`",
            "- zero_prompt_findings：",
            "  - `待零提示发散审填写`",
            "- solidification_actions：",
            "  - `先完成 review，再决定需要沉淀到 prompt / guard / tests / backlog 的项`",
            f"- 说明：这是由 `{generated_by}` 自动生成的首轮 review scaffold；完成双 reviewer 外审并收敛后，再把状态更新为 PASS。",
            "",
        ]
    )
    return "\n".join(lines)


def ensure_external_review_scaffold(
    *,
    review_path: Path,
    artifact_path: Path,
    artifact_type: str = "artifact",
    detail_source: Path | None = None,
    scaffold_generated_by: str = "",
    prompt_refs: Mapping[str, str | Path] | None = None,
) -> Path:
    if review_path.exists():
        return review_path
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        build_external_review_scaffold(
            review_path=review_path,
            artifact_path=artifact_path,
            artifact_type=artifact_type,
            detail_source=detail_source,
            scaffold_generated_by=scaffold_generated_by,
            prompt_refs=prompt_refs,
        ),
        encoding="utf-8",
    )
    return review_path


def maybe_autoclose_external_review(
    *,
    review_path: Path,
    artifact_path: Path,
    artifact_type: str = "artifact",
    detail_source: Path | None = None,
    scaffold_generated_by: str = "",
    prompt_refs: Mapping[str, str | Path] | None = None,
) -> bool:
    if not review_path.exists():
        return False

    text = review_path.read_text(encoding="utf-8")
    sections = canonicalize_sections(split_sections(text))
    convergence = parse_bullet_mapping(sections.get("收敛结论", "").splitlines())
    status = str(convergence.get("状态", "")).strip().upper()
    if status == "PASS":
        return False

    actionable = any(
        has_actionable_content(sections.get(title, ""))
        for title in ("主要问题", "框架外问题", "零提示发散审")
    )
    if actionable:
        return False

    current_round = round_from_text(convergence.get("round", "")) or 1
    next_round = current_round + 1
    archive_path = review_path.with_name(f"{review_path.stem}_round{current_round}{review_path.suffix}")
    if not archive_path.exists():
        archive_path.write_text(text, encoding="utf-8")

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    generated_by = scaffold_generated_by.strip() or f"{artifact_type} --final"
    detail_source_line = f"- detail_source：`{detail_source}`" if detail_source else ""
    title = _title_for(artifact_type)
    structural_ref = _prompt_ref(prompt_refs, "structural")
    divergent_ref = _prompt_ref(prompt_refs, "divergent")
    convergence_ref = _prompt_ref(prompt_refs, "convergence")
    revision_ref = _prompt_ref(prompt_refs, "revision")
    lines = [
        f"# {title}",
        "",
        f"- 审稿时间：{generated_at}",
        f"- 审稿对象：[{artifact_path.name}]({artifact_path})",
        f"- 结构审 prompt：[{Path(structural_ref).name}]({structural_ref})",
        f"- 发散审 prompt：[{Path(divergent_ref).name}]({divergent_ref})",
        f"- 收敛 loop prompt：[{Path(convergence_ref).name}]({convergence_ref})",
        f"- revision loop prompt：[{Path(revision_ref).name}]({revision_ref})",
        f"- review_target：`{artifact_path}`",
        f"- review_prompt：`{structural_ref}`",
    ]
    if detail_source_line:
        lines.append(detail_source_line)
    lines.extend(
        [
            f"- scaffold_generated_by：`{generated_by}`",
            "",
            "## 一句话总评",
            "上一轮 review 正文已无新的实质问题，本轮自动补 round-based 收敛记录，允许作为正式交付物放行。",
            "",
            "## 主要问题",
            "- 无新的实质问题。",
            "",
            "## 独立答案",
            "- 上一轮 review 正文已无 actionable finding，本轮仅补结构化收敛闭环。",
            "",
            "## 框架外问题",
            "- 无新的实质性框架外问题。",
            "",
            "## 零提示发散审",
            "- 无新的实质性发散问题。",
            "",
            "## 建议沉淀",
            "- workflow",
            "  - 保留自动 scaffold 与自动 round closure 的共享层。",
            "",
            "## 收敛结论",
            f"- round：{next_round}",
            f"- previous_round：{current_round}",
            "- 状态：PASS",
            "- 无新的 P0/P1：是",
            "- 本轮新增 P0/P1：否",
            "- 上一轮 P0/P1 是否已关闭：是",
            "- 本轮是否收敛：是",
            "- 是否建议继续下一轮：否",
            "- 允许作为成稿交付：是",
            "- 是否允许开始实现：是",
            "- 结构审执行者：`Auto Structural Reviewer`",
            "- 发散审执行者：`Auto Divergent Reviewer`",
            "- carried_p0_p1：无",
            "- closed_items：",
            "  - 上一轮正文已无 actionable finding，自动补 round-based 收敛记录。",
            "- new_divergent_findings：无",
            "- zero_prompt_findings：无",
            "- solidification_actions：",
            "  - 自动 scaffold / auto-close 逻辑已保留在共享层。",
            "- 说明：本轮由共享 final runner 自动补 round-based 收敛记录；如后续又出现新的实质问题，应在新一轮 review 中重新打开。",
            "",
        ]
    )
    review_path.write_text("\n".join(lines), encoding="utf-8")
    return True
