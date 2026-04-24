"""Catalyst web-review sidecar builders for suspected search-gap cases."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from src.output.theme_playbook import build_theme_playbook_context, playbook_hint_line
from src.processors.provenance import build_analysis_provenance


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


_DECISION_VALUES = {
    "已确认直接催化",
    "只有主题级催化",
    "未确认新增催化",
    "原链路疑似漏抓",
}


def _search_groups_text(groups: Sequence[Sequence[Any]]) -> List[str]:
    lines: List[str] = []
    for group in groups:
        parts = [_safe_text(item) for item in list(group or []) if _safe_text(item)]
        if parts:
            lines.append(" / ".join(parts))
    return lines


def _analysis_review_item(analysis: Mapping[str, Any]) -> Dict[str, Any] | None:
    dimensions = dict(analysis.get("dimensions") or {})
    catalyst = dict(dimensions.get("catalyst") or {})
    coverage = dict(catalyst.get("coverage") or {})
    diagnosis = _safe_text(coverage.get("diagnosis"))
    if diagnosis != "suspected_search_gap" and not bool(coverage.get("ai_web_search_recommended")):
        return None
    metadata = dict(analysis.get("metadata") or {})
    search_groups = [list(group or []) for group in list(coverage.get("search_groups") or [])]
    thematic_groups = [_search_groups_text([group])[0] for group in search_groups[1:] if _search_groups_text([group])]
    playbook = build_theme_playbook_context(
        thematic_groups,
        metadata.get("chain_nodes"),
        analysis.get("taxonomy_summary"),
        analysis.get("name"),
        analysis.get("symbol"),
        metadata.get("sector"),
        dict(analysis.get("day_theme") or {}).get("label") or analysis.get("day_theme"),
        catalyst.get("summary"),
    )
    provenance = build_analysis_provenance(analysis)
    return {
        "name": _safe_text(analysis.get("name")),
        "symbol": _safe_text(analysis.get("symbol")),
        "asset_type": _safe_text(analysis.get("asset_type")),
        "generated_at": _safe_text(analysis.get("generated_at")),
        "market_data_as_of": _safe_text(provenance.get("market_data_as_of")),
        "day_theme": _safe_text(dict(analysis.get("day_theme") or {}).get("label") or analysis.get("day_theme")),
        "sector": _safe_text(metadata.get("sector")),
        "chain_nodes": [str(item).strip() for item in list(metadata.get("chain_nodes") or []) if str(item).strip()],
        "primary_chain": _safe_text(metadata.get("primary_chain")),
        "theme_role": _safe_text(metadata.get("theme_role")),
        "evidence_keywords": [str(item).strip() for item in list(metadata.get("evidence_keywords") or []) if str(item).strip()],
        "theme_name": _safe_text(playbook.get("label")),
        "theme_family": _safe_text(playbook.get("theme_family")),
        "playbook_hint": _safe_text(playbook_hint_line(playbook)),
        "catalyst_summary": _safe_text(catalyst.get("summary")),
        "catalyst_diagnosis": diagnosis or "unknown",
        "news_mode": _safe_text(coverage.get("news_mode")) or "unknown",
        "search_groups": search_groups,
        "search_result_count": int(coverage.get("search_result_count") or 0),
        "structured_event": bool(coverage.get("structured_event")),
        "forward_event": bool(coverage.get("forward_event")),
        "ai_web_search_recommended": bool(coverage.get("ai_web_search_recommended")),
    }


def is_catalyst_web_review_template(text: str) -> bool:
    return "- 结论：待补" in text or "\n- 待补\n" in text


def catalyst_web_review_has_completed_conclusion(text: str) -> bool:
    return any(value in text for value in _DECISION_VALUES) and not is_catalyst_web_review_template(text)


def _extract_section_block(text: str, title: str) -> str:
    pattern = re.compile(rf"^###\s*{re.escape(title)}\s*$\n?(.*?)(?=^###\s+\S|\Z)", re.M | re.S)
    match = pattern.search(text)
    return _safe_text(match.group(1) if match else "")


def parse_catalyst_web_review(text: str) -> Dict[str, Dict[str, Any]]:
    sections = re.split(r"(?m)^##\s+\d+\.\s+", text)
    results: Dict[str, Dict[str, Any]] = {}
    for chunk in sections[1:]:
        lines = chunk.splitlines()
        if not lines:
            continue
        header = _safe_text(lines[0])
        match = re.match(r"(.+?)\s*\(([^()]+)\)\s*$", header)
        if not match:
            continue
        name = _safe_text(match.group(1))
        symbol = _safe_text(match.group(2))
        body = "\n".join(lines[1:])
        decision_block = _extract_section_block(body, "复核结论")
        decision = ""
        for line in decision_block.splitlines():
            cleaned = _safe_text(line).lstrip("- ").strip()
            if cleaned.startswith("结论："):
                decision = _safe_text(cleaned.split("：", 1)[1])
                break
        key_evidence = [
            _safe_text(line).lstrip("- ").strip()
            for line in _extract_section_block(body, "关键证据").splitlines()
            if _safe_text(line).startswith("- ")
        ]
        impacts = [
            _safe_text(line).lstrip("- ").strip()
            for line in _extract_section_block(body, "影响判断").splitlines()
            if _safe_text(line).startswith("- ")
        ]
        boundaries = [
            _safe_text(line).lstrip("- ").strip()
            for line in _extract_section_block(body, "边界").splitlines()
            if _safe_text(line).startswith("- ")
        ]
        results[symbol] = {
            "name": name,
            "symbol": symbol,
            "decision": decision,
            "key_evidence": [item for item in key_evidence if item and item != "待补"],
            "impact": [item for item in impacts if item and item != "待补"],
            "boundaries": [item for item in boundaries if item and item != "待补"],
            "completed": decision in _DECISION_VALUES and not is_catalyst_web_review_template(body),
        }
    return results


def load_catalyst_web_review(path: str | Path) -> Dict[str, Dict[str, Any]]:
    review_path = Path(path)
    if not review_path.exists():
        return {}
    return parse_catalyst_web_review(review_path.read_text(encoding="utf-8"))


def preserve_existing_catalyst_web_review(path: Path, content: str) -> str:
    if not path.exists():
        return content
    existing = path.read_text(encoding="utf-8")
    if catalyst_web_review_has_completed_conclusion(existing):
        return existing
    return content


def attach_catalyst_web_review_to_analysis(
    analysis: Mapping[str, Any],
    review_lookup: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    symbol = _safe_text(analysis.get("symbol"))
    summary = dict(review_lookup.get(symbol) or {})
    if not summary:
        return dict(analysis)
    enriched = dict(analysis)
    enriched["catalyst_web_review"] = summary
    dimensions = dict(enriched.get("dimensions") or {})
    catalyst = dict(dimensions.get("catalyst") or {})
    if summary.get("completed"):
        decision = _safe_text(summary.get("decision"))
        evidence = list(summary.get("key_evidence") or [])
        impact = list(summary.get("impact") or [])
        catalyst["web_review"] = summary
        catalyst["summary"] = (
            f"联网复核：{decision}。{impact[0]}"
            if impact
            else f"联网复核：{decision}。"
        ).strip()
        coverage = dict(catalyst.get("coverage") or {})
        coverage["diagnosis"] = "web_review_completed"
        coverage["ai_web_search_recommended"] = False
        catalyst["coverage"] = coverage
        if evidence:
            catalyst["web_review_evidence"] = evidence
    dimensions["catalyst"] = catalyst
    enriched["dimensions"] = dimensions
    return enriched


def build_catalyst_web_review_packet(
    *,
    report_type: str,
    subject: str,
    generated_at: str,
    analyses: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for analysis in analyses:
        item = _analysis_review_item(analysis)
        if not item:
            continue
        key = (item["symbol"], item["name"])
        if key in seen:
            continue
        seen.add(key)
        items.append(item)
    return {
        "report_type": report_type,
        "packet_version": "catalyst-web-review-v1",
        "subject": subject,
        "generated_at": generated_at,
        "items": items,
    }


def render_catalyst_web_review_prompt(packet: Mapping[str, Any]) -> str:
    items = list(packet.get("items") or [])
    lines = [
        "# Catalyst Web Review Prompt",
        "",
        "请按 `docs/prompts/financial_catalyst_web_researcher.md` 里的合同执行一次独立联网复核。",
        "",
        "要求：",
        "- 只回答最近 7-14 天有没有足以影响判断的催化证据。",
        "- 明确区分：已确认直接催化 / 只有主题级催化 / 未确认新增催化 / 原链路疑似漏抓。",
        "- 不要改推荐等级，只回答会不会改变原结论。",
        "- 不要把主题背景新闻冒充成公司或标的直接催化。",
        "",
        f"- report_type: `{_safe_text(packet.get('report_type'))}`",
        f"- subject: `{_safe_text(packet.get('subject'))}`",
        f"- generated_at: `{_safe_text(packet.get('generated_at'))}`",
        "",
        "## 待复核清单",
        "",
    ]
    if not items:
        lines.append("- 当前没有命中 `待 AI 联网复核` 的条目。")
        return "\n".join(lines).rstrip()
    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"### {index}. {_safe_text(item.get('name'))} ({_safe_text(item.get('symbol'))})",
                "",
                f"- 资产类型：`{_safe_text(item.get('asset_type'))}`",
                f"- 市场数据 as_of：`{_safe_text(item.get('market_data_as_of')) or '—'}`",
                f"- 主题 / 家族：`{_safe_text(item.get('theme_name')) or '未命中'}` / `{_safe_text(item.get('theme_family')) or '未标注'}`",
                f"- 行业 / 板块：`{_safe_text(item.get('sector')) or '未标注'}`",
                f"- 当前催化诊断：`{_safe_text(item.get('catalyst_diagnosis'))}`",
                f"- 当前催化摘要：{_safe_text(item.get('catalyst_summary')) or '—'}",
                f"- 新闻模式：`{_safe_text(item.get('news_mode'))}`；搜索结果数：`{item.get('search_result_count', 0)}`",
            ]
        )
        search_groups = _search_groups_text(item.get("search_groups") or [])
        if search_groups:
            lines.append("- 已尝试关键词组：")
            for group in search_groups[:5]:
                lines.append(f"  - `{group}`")
        if _safe_text(item.get("playbook_hint")):
            lines.append(f"- 主题认知提示：{_safe_text(item.get('playbook_hint'))}")
        chain_nodes = [str(node).strip() for node in list(item.get("chain_nodes") or []) if str(node).strip()]
        if chain_nodes:
            lines.append(f"- chain nodes：`{' / '.join(chain_nodes[:6])}`")
        evidence_keywords = [str(node).strip() for node in list(item.get("evidence_keywords") or []) if str(node).strip()]
        if evidence_keywords:
            lines.append(f"- 证据关键词：`{' / '.join(evidence_keywords[:10])}`")
        lines.append("")
    return "\n".join(lines).rstrip()


def render_catalyst_web_review_scaffold(packet: Mapping[str, Any]) -> str:
    items = list(packet.get("items") or [])
    lines = [
        f"# Catalyst Web Review | {_safe_text(packet.get('report_type'))} | {_safe_text(packet.get('generated_at'))[:10]}",
        "",
        "当前状态：待独立 agent / subagent 联网复核。只有完成下面条目的复核后，才能把“疑似漏抓”改写成“确实无催化”或“已确认催化”。",
        "",
    ]
    if not items:
        lines.append("- 当前没有命中 `待 AI 联网复核` 的条目。")
        return "\n".join(lines).rstrip()
    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"## {index}. {_safe_text(item.get('name'))} ({_safe_text(item.get('symbol'))})",
                "",
                "### 当前管线结论",
                "",
                f"- 催化诊断：`{_safe_text(item.get('catalyst_diagnosis'))}`",
                f"- 当前催化摘要：{_safe_text(item.get('catalyst_summary')) or '—'}",
                f"- 主题 / 家族：`{_safe_text(item.get('theme_name')) or '未命中'}` / `{_safe_text(item.get('theme_family')) or '未标注'}`",
                "",
                "### 建议联网检索方向",
                "",
            ]
        )
        search_groups = _search_groups_text(item.get("search_groups") or [])
        if search_groups:
            for group in search_groups[:5]:
                lines.append(f"- `{group}`")
        else:
            lines.append("- 先按主题、标的名、代码和链条关键词补检。")
        evidence_keywords = [str(node).strip() for node in list(item.get("evidence_keywords") or []) if str(node).strip()]
        if evidence_keywords:
            lines.append(f"- 证据关键词补检：`{' / '.join(evidence_keywords[:10])}`")
        lines.extend(
            [
                "",
                "### 复核结论",
                "",
                "- 结论：待补",
                "",
                "### 关键证据",
                "",
                "- 待补",
                "",
                "### 影响判断",
                "",
                "- 待补",
                "",
                "### 边界",
                "",
                "- 待补",
                "",
            ]
        )
    return "\n".join(lines).rstrip()
