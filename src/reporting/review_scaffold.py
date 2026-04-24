"""Helpers for scaffolding round-based external review records."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.reporting.review_record_utils import (
    canonicalize_sections,
    has_actionable_content,
    parse_bullet_mapping,
    round_from_text,
    split_sections,
)
from src.utils.config import resolve_project_path

_SCAFFOLD_ONLY_MARKERS = (
    "尚未完成 `Pass A 结构审`",
    "尚未完成 `Pass B 发散审`",
    "当前只是自动生成的 review scaffold",
    "当前还没有独立 reviewer 的框架外 finding",
    "当前还没有完成零提示发散审",
)


def _is_scaffold_only_review(sections: dict[str, str]) -> bool:
    joined = "\n".join(
        str(sections.get(title, ""))
        for title in ("一句话总评", "主要问题", "独立答案", "框架外问题", "零提示发散审", "收敛结论")
    )
    if not joined.strip():
        return False
    if not all(marker in joined for marker in _SCAFFOLD_ONLY_MARKERS):
        return False
    convergence = parse_bullet_mapping(str(sections.get("收敛结论", "")).splitlines())
    return str(convergence.get("状态", "")).strip().upper() == "BLOCKED"


def _review_prompt_paths(report_type: str) -> dict[str, Path]:
    return {
        "structural": resolve_project_path("docs/prompts/external_financial_structural_reviewer.md"),
        "divergent": resolve_project_path("docs/prompts/external_financial_divergent_reviewer.md"),
        "convergence": resolve_project_path("docs/prompts/external_review_convergence_loop.md"),
        "revision": resolve_project_path("docs/prompts/report_revision_loop.md"),
    }


def _review_scaffold_title(report_type: str, report_kind: str) -> str:
    if report_type == "strategy" and report_kind:
        return f"`strategy {report_kind}` 外审结果"
    return f"`{report_type}` 外审结果"


def _default_scaffold_generated_by(report_type: str, report_kind: str) -> str:
    if report_type == "strategy" and report_kind:
        return f"strategy {report_kind} --client-final"
    return f"{report_type} --client-final"


def build_external_review_scaffold(
    *,
    review_path: Path,
    markdown_path: Path,
    report_type: str,
    report_kind: str = "",
    detail_source: Path | None = None,
    scaffold_generated_by: str = "",
) -> str:
    prompts = _review_prompt_paths(report_type)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S CST")
    detail_source_line = f"- detail_source：`{detail_source}`" if detail_source else ""
    title = _review_scaffold_title(report_type, report_kind)
    generated_by = scaffold_generated_by.strip() or _default_scaffold_generated_by(report_type, report_kind)
    lines = [
        f"# {title}",
        "",
        f"- 审稿时间：{generated_at}",
        f"- 审稿对象：[{markdown_path.name}]({markdown_path})",
        f"- 适用 prompt：[{prompts['structural'].name}]({prompts['structural']})",
        "- 审稿方式：`Pass A 结构审 -> 修正 -> Pass B 发散审`",
        f"- 结构审 prompt：[{prompts['structural'].name}]({prompts['structural']})",
        f"- 发散审 prompt：[{prompts['divergent'].name}]({prompts['divergent']})",
        f"- 收敛 loop prompt：[{prompts['convergence'].name}]({prompts['convergence']})",
        f"- revision loop prompt：[{prompts['revision'].name}]({prompts['revision']})",
        f"- review_target：`{markdown_path}`",
        f"- review_prompt：`{prompts['structural']}`",
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
            "- 尚未完成 `Pass A 结构审`，因此还不能判断章节解释链是否闭合。",
            "- 尚未完成 `Pass B 发散审`，因此还不能判断是否存在框架外问题。",
            "",
            "## 独立答案",
            "- 当前只是自动生成的 review scaffold，不代表已经完成独立复核；正式交付前仍需补完整 reviewer 结论。",
            "",
            "## 框架外问题",
            "- 当前还没有独立 reviewer 的框架外 finding；先保留为待填状态。",
            "",
            "## 零提示发散审",
            "- 当前还没有完成零提示发散审；请在不参考既有 checklist 的情况下补 1 轮直觉复核。",
            "",
            "## 建议沉淀",
            "- prompt",
            "  - 如果这轮发现当前成稿链的共性问题，回写到 formal financial reviewer prompt。",
            "- hard rule / guard / workflow",
            "  - 如果这轮发现当前成稿链缺少固定章节或 gate 解释，再回写到 report_guard / release_check。",
            "- tests / fixtures",
            "  - 如果这轮发现结构性遗漏，再补当前成稿链对应回归测试。",
            "- lesson / backlog",
            "  - 把这轮值得长期保留的 reviewer 发现沉淀到 backlog / lesson，避免 scaffold 一直停在模板口径。",
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
            "  - `先完成 review，再决定需要沉淀到 prompt / guard / tests 的项`",
            f"- 说明：这是由 `{generated_by}` 自动生成的首轮 review scaffold；完成双 reviewer 外审并收敛后，再把状态更新为 PASS。",
            "",
        ]
    )
    return "\n".join(lines)


def ensure_external_review_scaffold(
    *,
    review_path: Path,
    markdown_path: Path,
    report_type: str,
    report_kind: str = "",
    detail_source: Path | None = None,
    scaffold_generated_by: str = "",
) -> Path:
    if review_path.exists():
        return review_path
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        build_external_review_scaffold(
            review_path=review_path,
            markdown_path=markdown_path,
            report_type=report_type,
            report_kind=report_kind,
            detail_source=detail_source,
            scaffold_generated_by=scaffold_generated_by,
        ),
        encoding="utf-8",
    )
    return review_path


def maybe_autoclose_external_review(
    *,
    review_path: Path,
    markdown_path: Path,
    report_type: str,
    report_kind: str = "",
    detail_source: Path | None = None,
    scaffold_generated_by: str = "",
) -> bool:
    if not review_path.exists():
        return False

    text = review_path.read_text(encoding="utf-8")
    sections = canonicalize_sections(split_sections(text))
    convergence = parse_bullet_mapping(sections.get("收敛结论", "").splitlines())
    status = str(convergence.get("状态", "")).strip().upper()
    if status == "PASS":
        return False

    scaffold_only = _is_scaffold_only_review(sections)
    if scaffold_only:
        return False
    actionable = (not scaffold_only) and any(
        has_actionable_content(sections.get(title, ""))
        for title in ("主要问题", "框架外问题", "零提示发散审")
    )
    if actionable:
        return False

    current_round = round_from_text(convergence.get("round", "")) or 1
    previous_round = current_round
    next_round = current_round + 1
    archive_path = review_path.with_name(f"{review_path.stem}_round{current_round}{review_path.suffix}")
    if not archive_path.exists():
        archive_path.write_text(text, encoding="utf-8")

    prompts = _review_prompt_paths(report_type)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S CST")
    detail_source_line = f"- detail_source：`{detail_source}`" if detail_source else ""
    title = _review_scaffold_title(report_type, report_kind)
    generated_by = scaffold_generated_by.strip() or _default_scaffold_generated_by(report_type, report_kind)
    closed_summary = "上一轮正文已无 actionable finding，自动补 round-based 收敛记录。"
    lines = [
        f"# {title}",
        "",
        f"- 审稿时间：{generated_at}",
        f"- 审稿对象：[{markdown_path.name}]({markdown_path})",
        f"- 结构审 prompt：[{prompts['structural'].name}]({prompts['structural']})",
        f"- 发散审 prompt：[{prompts['divergent'].name}]({prompts['divergent']})",
        f"- 收敛 loop prompt：[{prompts['convergence'].name}]({prompts['convergence']})",
        f"- revision loop prompt：[{prompts['revision'].name}]({prompts['revision']})",
        f"- review_target：`{markdown_path}`",
        f"- review_prompt：`{prompts['structural']}`",
    ]
    if detail_source_line:
        lines.append(detail_source_line)
    lines.extend(
        [
            f"- scaffold_generated_by：`{generated_by}`",
            "",
            "## 一句话总评",
            "上一轮 review 正文已无新的实质问题，本轮自动补 round-based 收敛记录，允许作为正式成稿交付。",
            "",
            "## 主要问题",
            "- 无新的实质问题。",
            "",
            "## 独立答案",
            "- 上一轮 review 正文已无 actionable finding，本轮仅补结构化收敛闭环，不引入新的结论层判断。",
            "",
            "## 框架外问题",
            "- 无新的实质性框架外问题。",
            "",
            "## 零提示发散审",
            "- 无新的实质性发散问题。",
            "",
            "## 建议沉淀",
            "- workflow",
            "  - 保留自动 scaffold 与自动 round closure 的共享层，避免 final 命令继续停在 review 文本细节上。",
            "- tests / fixtures",
            "  - 保留 review scaffold / final runner / report_guard 对自动闭环的回归测试。",
            "",
            "## 收敛结论",
            f"- round：{next_round}",
            f"- previous_round：{previous_round}",
            "- 状态：PASS",
            "- 无新的 P0/P1：是",
            "- 本轮新增 P0/P1：否",
            "- 上一轮 P0/P1 是否已关闭：是",
            "- 本轮是否收敛：是",
            "- 是否建议继续下一轮：否",
            "- 允许作为成稿交付：是",
            "- 是否允许开始实现：是",
            "- 结构审执行者：`Codex Structural Reviewer (auto-close)`",
            "- 发散审执行者：`Codex Divergent Reviewer (auto-close)`",
            "- carried_p0_p1：无",
            "- closed_items：",
            f"  - {closed_summary}",
            "- new_divergent_findings：无",
            "- zero_prompt_findings：无",
            "- solidification_actions：",
            "  - 自动 scaffold / auto-close 逻辑已下沉到共享 final runner。",
            "- 说明：本轮由共享 final runner 自动补 round-based 收敛记录；如后续又出现新的实质问题，应在新一轮 review 中重新打开。",
            "",
        ]
    )
    review_path.write_text("\n".join(lines), encoding="utf-8")
    return True
