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
    "etf_pick": ("## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只ETF为什么是这个分", "## 标准化分类", "## 基金画像", "## 关键证据", "## 为什么不是另外几只"),
    "scan": ("## 为什么这么判断", "## 硬检查", "## 分维度详解"),
    "retrospect": ("## 原始决策", "## 为什么当时会做这个决定", "## 后验路径", "## 复盘结论"),
}
_THEME_PLAYBOOK_ALIGNMENT_REPORT_TYPES = {"scan", "stock_analysis", "stock_pick", "etf_pick", "fund_pick", "briefing"}


def _stock_pick_required_markers(markdown_text: str) -> tuple[Any, ...]:
    observe_only = "| 报告定位 | 观察稿 |" in markdown_text or "当前没有达到正式动作阈值的个股" in markdown_text
    if observe_only:
        return (
            "## 今日动作摘要",
            "## 催化证据来源",
            "## 历史相似样本附注",
            ("## A股", "## 港股", "## 美股"),
            "### 第二批：继续跟踪",
            ("### 第二批：低门槛 / 观察替代", "### 第二批：低门槛 / 关联ETF"),
            "## 代表样本复核卡",
            "升级条件",
            "关键盯盘价位",
        )
    return DETAILED_FINAL_MARKERS["stock_pick"]


def _strategy_required_markers(markdown_text: str) -> tuple[Any, ...]:
    if "# Strategy Validation" in markdown_text:
        return (
            "## 动作卡片",
            "## 当前结论",
            "## 这套策略是什么",
            "## 这次到底看出来什么",
            "## 执行摘要",
            "## 总体结果",
            "## Rollback Gate",
            ("## Out-Of-Sample Validation", "## Chronological Cohorts", "## Cross-Sectional Validation"),
        )
    if "# Strategy Experiment" in markdown_text:
        return (
            "## 动作卡片",
            "## 当前结论",
            "## 这套策略是什么",
            "## 这次到底看出来什么",
            "## 执行摘要",
            "## Promotion Gate",
            "## Rollback Gate",
            "## 变体对比",
        )
    return (
        "## 动作卡片",
        "## 当前结论",
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


def _contains_any(text: str, needles: Sequence[str]) -> bool:
    haystack = str(text or "")
    return any(str(needle).strip() and str(needle) in haystack for needle in needles)


def _section_text(markdown_text: str, heading: str) -> str:
    lines = markdown_text.splitlines()
    collecting = False
    collected: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and stripped.startswith("## "):
            break
        if collecting:
            collected.append(line)
    return "\n".join(collected).strip()


def _ensure_theme_playbook_alignment(report_type: str, markdown_text: str, extra_manifest: Mapping[str, Any] | None) -> None:
    if report_type not in _THEME_PLAYBOOK_ALIGNMENT_REPORT_TYPES:
        return
    contract = dict((extra_manifest or {}).get("theme_playbook_contract") or {})
    if not contract:
        return
    playbook_level = str(contract.get("playbook_level") or "").strip()
    label = str(contract.get("label") or "").strip()
    if playbook_level not in {"theme", "sector"} or not label:
        raise ReportGuardError(
            "theme_playbook_contract 不完整：缺少 `playbook_level` 或 `label`，无法校验正文里的主题边界。"
        )
    if playbook_level != "sector":
        return
    theme_match_status = str(contract.get("theme_match_status") or "").strip()
    theme_match_candidates = [
        str(item).strip()
        for item in list(contract.get("theme_match_candidates") or [])
        if str(item).strip()
    ]
    if theme_match_status == "ambiguous_conflict":
        if not theme_match_candidates:
            raise ReportGuardError(
                "theme_playbook_contract 不完整：当前稿件标记为 `ambiguous_conflict`，但缺少 `theme_match_candidates`。"
            )
        boundary_markers = ("主题边界", "先按行业层", "行业层理解", "还在打架")
        if not _contains_any(markdown_text, boundary_markers):
            raise ReportGuardError(
                "主题 playbook 合同错配：manifest 已标记当前稿件属于行业层冲突稿，但正文没有写出 `主题边界` 或行业层冲突提示。"
            )
    bridge_confidence = str(contract.get("subtheme_bridge_confidence") or "").strip()
    bridge_top_label = str(contract.get("subtheme_bridge_top_label") or "").strip()
    if bridge_confidence in {"high", "medium"}:
        if not bridge_top_label:
            raise ReportGuardError(
                "theme_playbook_contract 不完整：行业层 bridge 已标成中高置信度，但缺少 `subtheme_bridge_top_label`。"
            )
        bridge_markers = ("细分观察", "优先留意", "更偏向", bridge_top_label)
        if not _contains_any(markdown_text, bridge_markers):
            raise ReportGuardError(
                "主题 playbook 合同错配：manifest 已给出行业层下钻主线，但正文没有写出 `细分观察` 或对应的细分提示。"
            )


def _ensure_event_digest_alignment(report_type: str, markdown_text: str, extra_manifest: Mapping[str, Any] | None) -> None:
    if report_type not in {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan"}:
        return
    contract = dict((extra_manifest or {}).get("event_digest_contract") or {})
    if not contract:
        return
    status = str(contract.get("status") or "").strip()
    changed_what = str(contract.get("changed_what") or "").strip()
    if status not in {"待补充", "待复核", "已消化"} or not changed_what:
        raise ReportGuardError(
            "event_digest_contract 不完整：缺少 `status` 或 `changed_what`，无法校验正文里的事件消化合同。"
        )
    event_section = _section_text(markdown_text, "## 事件消化")
    if not event_section:
        raise ReportGuardError(
            "事件消化合同错配：manifest 已写入 event_digest_contract，但正文缺少 `## 事件消化`。"
        )
    if status not in event_section:
        raise ReportGuardError(
            f"事件消化合同错配：manifest 已标记事件状态 `{status}`，但正文 `事件消化` 没把这个状态写出来。"
        )
    if "这件事改变了什么" not in event_section:
        raise ReportGuardError(
            "事件消化合同错配：正文 `事件消化` 缺少“这件事改变了什么”，还停在事件罗列层。"
        )
    lead_layer = str(contract.get("lead_layer") or "").strip()
    if lead_layer and lead_layer not in event_section:
        raise ReportGuardError(
            f"事件消化合同错配：manifest 已标记事件分层 `{lead_layer}`，但正文 `事件消化` 没显式写出这层。"
        )
    lead_detail = str(contract.get("lead_detail") or "").strip()
    if lead_detail and lead_detail not in event_section:
        raise ReportGuardError(
            f"事件消化合同错配：manifest 已标记事件细分 `{lead_detail}`，但正文 `事件消化` 没把这层写出来。"
        )
    impact_summary = str(contract.get("impact_summary") or "").strip()
    impact_line = next(
        (line.strip() for line in event_section.splitlines() if line.strip().startswith("- 影响层与性质：")),
        "",
    )
    if impact_summary and impact_summary not in impact_line:
        raise ReportGuardError(
            f"事件消化合同错配：manifest 已标记影响层 `{impact_summary}`，但正文 `事件消化` 没写清它影响的是哪一层。"
        )
    thesis_scope = str(contract.get("thesis_scope") or "").strip()
    if thesis_scope and thesis_scope not in impact_line:
        raise ReportGuardError(
            f"事件消化合同错配：manifest 已标记事件性质 `{thesis_scope}`，但正文 `事件消化` 没写清它是 thesis 变化还是一次性噪音。"
        )
    importance_reason = str(contract.get("importance_reason") or "").strip()
    if importance_reason and "优先级判断" not in event_section:
        raise ReportGuardError(
            "事件消化合同错配：manifest 已写入优先级判断，但正文 `事件消化` 没解释为什么该前置或先不升级。"
        )


def _ensure_what_changed_alignment(report_type: str, markdown_text: str, extra_manifest: Mapping[str, Any] | None) -> None:
    if report_type not in {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan"}:
        return
    contract = dict((extra_manifest or {}).get("what_changed_contract") or {})
    if not contract:
        return
    previous_view = str(contract.get("previous_view") or "").strip()
    change_summary = str(contract.get("change_summary") or "").strip()
    conclusion_label = str(contract.get("conclusion_label") or "").strip()
    state_trigger = str(contract.get("state_trigger") or "").strip()
    state_summary = str(contract.get("state_summary") or "").strip()
    current_event_understanding = str(contract.get("current_event_understanding") or "").strip()
    if not previous_view or not change_summary or not conclusion_label:
        raise ReportGuardError(
            "what_changed_contract 不完整：缺少 `previous_view` / `change_summary` / `conclusion_label`，无法校验连续研究变化摘要。"
        )
    what_changed_section = _section_text(markdown_text, "## What Changed")
    if not what_changed_section:
        raise ReportGuardError(
            "What Changed 合同错配：manifest 已写入 what_changed_contract，但正文缺少 `## What Changed`。"
        )
    if "上次怎么看" not in what_changed_section:
        raise ReportGuardError(
            "What Changed 合同错配：正文 `What Changed` 缺少“上次怎么看”。"
        )
    if "这次什么变了" not in what_changed_section:
        raise ReportGuardError(
            "What Changed 合同错配：正文 `What Changed` 缺少“这次什么变了”。"
        )
    if "结论变化" not in what_changed_section:
        raise ReportGuardError(
            "What Changed 合同错配：正文 `What Changed` 缺少“结论变化”。"
        )
    if current_event_understanding and "当前事件理解" not in what_changed_section:
        raise ReportGuardError(
            "What Changed 合同错配：manifest 已写入当前事件理解，但正文 `What Changed` 缺少“当前事件理解”。"
        )
    if conclusion_label not in what_changed_section:
        raise ReportGuardError(
            f"What Changed 合同错配：manifest 已标记结论变化 `{conclusion_label}`，但正文 `What Changed` 没写出来。"
        )
    if state_trigger and ("触发：" not in what_changed_section or state_trigger not in what_changed_section):
        raise ReportGuardError(
            f"What Changed 合同错配：manifest 已写入状态触发 `{state_trigger}`，但正文 `What Changed` 没解释这次为什么升级、削弱或待复核。"
        )
    if state_summary and ("状态解释" not in what_changed_section or state_summary not in what_changed_section):
        raise ReportGuardError(
            "What Changed 合同错配：manifest 已写入状态解释，但正文 `What Changed` 没把这次状态机原因落成完整解释。"
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


_CATALYST_WEB_REVIEW_DECISIONS = (
    "已确认直接催化",
    "只有主题级催化",
    "未确认新增催化",
    "原链路疑似漏抓",
)


def _ensure_completed_catalyst_web_review(extra_manifest: Mapping[str, Any] | None) -> None:
    artifacts = dict((extra_manifest or {}).get("editor_artifacts") or {})
    review_ref = str(artifacts.get("catalyst_web_review") or "").strip()
    if not review_ref:
        return
    review_path = Path(review_ref)
    if not review_path.exists():
        raise ReportGuardError(
            f"催化联网复核未完成：缺少 `{review_path}`。命中 `suspected_search_gap` 的正式稿，必须先补完 `catalyst_web_review.md`。"
            "可先运行 `python -m src.commands.catalyst_review next --with-prompt` 捞出下一条待复核任务。"
        )
    review_text = review_path.read_text(encoding="utf-8")
    if "当前没有命中 `待 AI 联网复核` 的条目。" in review_text:
        return
    if "### 复核结论" not in review_text:
        raise ReportGuardError(
            f"催化联网复核不合格：`{review_path}` 缺少 `### 复核结论`。"
        )
    if any(token in review_text for token in ("- 结论：待补", "\n- 待补\n")):
        raise ReportGuardError(
            f"催化联网复核尚未完成：`{review_path}` 仍是待补模板。先完成独立 agent 联网复核，再写 final。"
            "可先运行 `python -m src.commands.catalyst_review next --with-prompt` 捞出下一条待复核任务。"
        )
    if not any(decision in review_text for decision in _CATALYST_WEB_REVIEW_DECISIONS):
        raise ReportGuardError(
            f"催化联网复核不合格：`{review_path}` 没有明确写出复核结论。"
        )


_BRIEFING_A_SHARE_SIGNAL_TOKENS = (
    "A股热股前排：",
    "A股行业走强：",
    "A股概念领涨：",
    "A股涨停集中：",
    "A股强势股池：",
)
_HOMEPAGE_KEY_EVIDENCE_HEADINGS = ("### 关键新闻 / 关键证据", "## 关键证据", "## 今日情报看板")
_GENERIC_MARKET_HEADLINE_PREFIXES = _BRIEFING_A_SHARE_SIGNAL_TOKENS + (
    "A股主题活跃：",
    "A股主题跟踪：",
    "`市场情报`：",
)


def _ensure_briefing_market_snapshot_freshness(report_type: str, markdown_text: str, extra_manifest: Mapping[str, Any] | None) -> None:
    if report_type != "briefing":
        return
    contract = dict((extra_manifest or {}).get("market_snapshot_contract") or {})
    if not contract:
        raise ReportGuardError("briefing 缺少 `market_snapshot_contract`，无法校验 A 股盘面快照 freshness。")
    if not bool(contract.get("stale_detected")):
        return
    warning_line = str(contract.get("warning_line") or "").strip()
    if warning_line and warning_line not in markdown_text:
        raise ReportGuardError("briefing A股盘面快照已判定 stale，但客户稿未显式写出 freshness 降级提示。")
    leaked = [token for token in _BRIEFING_A_SHARE_SIGNAL_TOKENS if token in markdown_text]
    if leaked:
        raise ReportGuardError(
            "briefing A股盘面快照未通过 freshness 校验，但客户稿仍前置了盘面信号："
            + " / ".join(leaked)
        )


def _ensure_top_signal_quality(report_type: str, markdown_text: str) -> None:
    if report_type not in {"briefing", "etf_pick", "fund_pick", "scan", "stock_analysis", "stock_pick"}:
        return
    section = ""
    for heading in _HOMEPAGE_KEY_EVIDENCE_HEADINGS:
        section = _section_text(markdown_text, heading)
        if section:
            break
    if not section:
        return

    items = [line.strip()[2:].strip() for line in section.splitlines() if line.strip().startswith("- ")]
    if not items:
        return
    linked_items = [item for item in items if "http://" in item or "https://" in item]
    signalful_items = [
        item for item in items if ("信号：" in item or "信号类型：" in item) and ("结论：" in item or "主要影响：" in item)
    ]
    market_only_items = [item for item in items if item.startswith(_GENERIC_MARKET_HEADLINE_PREFIXES)]

    if report_type == "briefing" and not linked_items:
        raise ReportGuardError("briefing 首页 `关键新闻 / 关键证据` 没有任何可点击外部情报，当前更像盘面摘要而不是晨报情报板。")
    if linked_items and not signalful_items:
        raise ReportGuardError(
            f"{report_type} 首页虽然前置了链接情报，但没有把 `信号/强弱/结论` 写清，仍然更像新闻堆砌。"
        )
    if market_only_items and len(market_only_items) == len(items):
        raise ReportGuardError(
            f"{report_type} 首页 `关键新闻 / 关键证据` 全被盘面句占满，当前是“盘面句顶替新闻位”，没有真正前置外部情报。"
        )


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
    _ensure_completed_catalyst_web_review(extra_manifest)
    ensure_detailed_final_content(report_type, markdown_text)
    _ensure_briefing_market_snapshot_freshness(report_type, markdown_text, extra_manifest)
    _ensure_top_signal_quality(report_type, markdown_text)
    _ensure_event_digest_alignment(report_type, markdown_text, extra_manifest)
    _ensure_what_changed_alignment(report_type, markdown_text, extra_manifest)
    _ensure_theme_playbook_alignment(report_type, markdown_text, extra_manifest)

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
