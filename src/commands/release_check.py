"""Release consistency checks for client-facing Markdown reports."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

from src.reporting.review_lessons import format_lesson_finding
from src.utils.config import resolve_project_path


BANNED_CLIENT_PHRASES = [
    "项目内初筛",
    "外部复核",
    "这轮修掉了什么",
    "模型版本",
    "评审闭环",
    "当日基准版",
    "本版口径变更",
    "当前输出角色",
]

RAW_EXCEPTION_PATTERNS = (
    "Too Many Requests",
    "Traceback",
    "ProxyError",
    "ConnectionError",
    "RemoteDisconnected",
    "SSLError",
    "ReadTimeout",
    "HTTPError",
)

INTRADAY_CLAIM_TERMS = ("盘中", "首30分钟", "集合竞价", "竞价", "VWAP", "开盘缺口", "相对今开", "相对昨收", "日内位置")
INTRADAY_EVIDENCE_TERMS = (
    "VWAP",
    "相对昨收",
    "相对今开",
    "日内位置",
    "盘中状态",
    "开盘缺口",
    "首30分钟",
    "分钟线",
    "竞价高开且量比放大",
    "竞价明显低开",
    "竞价量比放大",
)
AUCTION_EVIDENCE_TERMS = ("竞价成交", "未匹配量", "封单", "开盘缺口", "竞价量能", "竞价高开且量比放大", "竞价明显低开", "竞价量比放大")

GENERIC_OPERATION_PREFIXES = (
    "介入条件：",
    "首次仓位：",
    "加仓节奏：",
    "止损参考：",
    "建议仓位：",
    "单标的上限：",
    "建议止损：",
    "目标参考：",
    "当前动作：",
    "单票上限",
    "执行原则",
    "`政策催化`",
    "`龙头公告",
    "`海外映射",
    "`研报/新闻",
    "`结构化事件",
    "`负面事件",
    "`新闻热度",
    "`前瞻催化",
)

PICK_OPERATION_PREFIXES = (
    "周期理由：",
    "不适合打法：",
    "为什么按这个周期理解：",
    "现在不适合的打法：",
    "现在不适合：",
    "加仓节奏：",
)

STOCK_CARD_SUBHEADINGS = (
    "为什么继续看它：",
    "为什么现在不升级成正式推荐：",
    "下一步怎么盯：",
    "证据口径：",
)

STOCK_SIGNAL_HINTS = (
    "技术",
    "基本面",
    "催化",
    "相对强弱",
    "相对基准",
    "风险",
    "财报",
    "年报",
    "回购",
    "分红",
    "减持",
    "增持",
    "披露",
    "订单",
    "业绩",
    "样本",
    "MACD",
    "RSI",
)

GENERIC_EVIDENCE_TITLE_KEYS = (
    "global market headlines",
    "breaking stock market news",
    "stock price & latest news",
    "stock quote price and forecast",
    "historical prices and data",
)
RAW_INTEL_SUMMARY_PREFIXES = (
    "情报摘要：主题聚类：",
    "情报摘要：来源分层：",
    "情报摘要：当前更值得先看的代表情报来自：",
)
READABILITY_HEDGE_TOKENS = ("不把", "不等于", "当前更像", "先按")
MISSING_DISCLOSURE_TOKENS = ("缺失", "降级", "不可用", "空表", "按缺失处理", "覆盖率")

HOMEPAGE_KEY_EVIDENCE_HEADINGS = ("### 关键新闻 / 关键证据", "## 关键证据", "## 今日情报看板")
GENERIC_MARKET_SIGNAL_PREFIXES = (
    "A股涨停集中：",
    "A股概念领涨：",
    "A股行业走强：",
    "A股热股前排：",
    "A股强势股池：",
    "A股主题活跃：",
    "A股主题跟踪：",
    "`市场情报`：",
)

REGIME_LABEL_TOKENS = ("recovery", "stagflation", "deflation", "overheating")
REGIME_BASIS_TOKENS = ("PMI", "PPI", "CPI", "信用脉冲", "M1-M2", "社融", "美元", "新订单", "政策")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pre-release consistency checks for client Markdown reports.")
    parser.add_argument("report_type", choices=["stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan", "retrospect", "strategy"], help="Report type to validate")
    parser.add_argument("--client", required=True, help="Path to client-facing Markdown")
    parser.add_argument("--source", default="", help="Path to source/detail Markdown")
    parser.add_argument("--editor-prompt", default="", help="Optional path to editor_prompt.md for sidecar-vs-final consistency checks")
    return parser


def is_clean_release_check(findings: List[str] | Tuple[str, ...] | None) -> bool:
    """Return ``True`` when release findings are empty after trimming."""
    return not any(str(item).strip() for item in (findings or []))


def _read(path: str) -> str:
    return Path(resolve_project_path(path)).read_text(encoding="utf-8")


def _parse_markdown_table(lines: List[str], start_index: int) -> Tuple[List[str], List[List[str]]]:
    table_lines: List[str] = []
    started = False
    for line in lines[start_index:]:
        stripped = line.strip()
        if not stripped and not started:
            continue
        if not stripped.startswith("|"):
            break
        started = True
        table_lines.append(line.rstrip("\n"))
    if len(table_lines) < 2:
        return [], []
    header = [cell.strip() for cell in table_lines[0].strip("|").split("|")]
    rows: List[List[str]] = []
    for line in table_lines[2:]:
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return header, rows


def _bullets_in_section(text: str, heading: str) -> List[str]:
    lines = text.splitlines()
    collecting = False
    bullets: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and stripped.startswith("## "):
            break
        if collecting and stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
    return bullets


def _bullets_in_section_any(text: str, headings: Tuple[str, ...]) -> List[str]:
    for heading in headings:
        bullets = _bullets_in_section(text, heading)
        if bullets:
            return bullets
    return []


def _table_mapping_in_section(text: str, heading: str) -> Dict[str, str]:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != heading:
            continue
        _, rows = _parse_markdown_table(lines, index + 1)
        mapping: Dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            mapping[str(row[0]).strip()] = str(row[1]).strip()
        return mapping
    return {}


def _section_exists(text: str, heading: str) -> bool:
    return any(line.strip() == heading for line in text.splitlines())


def _contains_any(text: str, needles: Tuple[str, ...]) -> bool:
    haystack = str(text or "")
    return any(str(needle) and str(needle) in haystack for needle in needles)


def _pick_reason_heading(report_type: str, text: str) -> str:
    options = {
        "fund_pick": ("## 为什么先看它", "## 为什么推荐它"),
        "etf_pick": ("## 为什么先看它", "## 为什么推荐它"),
    }.get(report_type, ())
    for heading in options:
        if _section_exists(text, heading):
            return heading
    return options[-1] if options else ""


def _strategy_report_kind(text: str) -> str:
    if "# Strategy Validation" in text or "## 总体结果" in text:
        return "validation"
    if "# Strategy Experiment" in text or "## Promotion Gate" in text or "## 变体对比" in text:
        return "experiment"
    return "unknown"


def _section_items(text: str, heading: str) -> List[str]:
    lines = text.splitlines()
    collecting = False
    items: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and (stripped.startswith("# ") or stripped.startswith("## ")):
            break
        if not collecting or not stripped:
            continue
        if stripped.startswith("|"):
            break
        if stripped.startswith("- "):
            items.append(stripped[2:].strip())
            continue
        if stripped.startswith(">"):
            items.append(stripped[1:].strip())
            continue
        items.append(stripped)
    return items


def _section_items_any(text: str, headings: Tuple[str, ...]) -> List[str]:
    for heading in headings:
        items = _section_items(text, heading)
        if items:
            return items
    return []


def _explanation_bullets(text: str) -> List[str]:
    bullets: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        body = stripped[2:].strip()
        if not body or body.startswith(GENERIC_OPERATION_PREFIXES):
            continue
        bullets.append(body)
    return bullets


def _section_text(text: str, heading: str) -> str:
    lines = text.splitlines()
    collecting = False
    collected: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and stripped.startswith("### "):
            break
        if collecting and stripped.startswith("## "):
            break
        if collecting:
            collected.append(line)
    return "\n".join(collected).strip()


def _text_without_section(text: str, heading: str) -> str:
    lines = text.splitlines()
    collecting = False
    kept: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and stripped.startswith("## "):
            collecting = False
        if not collecting:
            kept.append(line)
    return "\n".join(kept).strip()


def _homepage_v2_findings(client_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    requires_homepage = report_type in {"etf_pick", "fund_pick", "stock_pick", "stock_analysis", "briefing", "scan"}
    if "## 首页判断" not in client_text:
        if requires_homepage:
            findings.append(format_lesson_finding("L002", f"[P1] {report_type} 客户稿缺少首页判断：首页主叙事没有真正进入正式稿正文。"))
        return findings
    required = (
        "### 宏观面",
        "### 板块 / 主题认知",
        "### 情绪与热度",
        "### 微观面",
        "### 动作建议与结论",
    )
    for heading in required:
        if heading not in client_text:
            findings.append(format_lesson_finding("L002", f"[P2] {report_type} 首页判断缺少章节: {heading}"))
    theme_section = _section_text(client_text, "### 板块 / 主题认知")
    if theme_section:
        weak_tokens = ("配置价值", "方向没坏", "模板", "更适合理解为")
        if sum(1 for token in weak_tokens if token in theme_section) >= 2 and len(theme_section) < 80:
            findings.append(format_lesson_finding("L040", f"[P2] {report_type} 首页“板块 / 主题认知”仍偏模板化，没有明显写出这个主题到底在交易什么。"))
    sentiment_section = _section_text(client_text, "### 情绪与热度")
    if sentiment_section and any(token in sentiment_section for token in ("直接催化", "已经兑现", "已经形成买点")):
        findings.append(format_lesson_finding("L040", f"[P1] {report_type} 首页把“情绪与热度”写成了直接催化或已兑现买点。"))
    return findings


def _homepage_decision_layer_findings(client_text: str, report_type: str) -> List[str]:
    if report_type not in {"stock_pick", "stock_analysis", "etf_pick", "fund_pick", "scan"}:
        return []
    findings: List[str] = []
    theme_items = _section_items(client_text, "### 板块 / 主题认知")
    micro_items = _section_items(client_text, "### 微观面")
    action_items = _section_items(client_text, "### 动作建议与结论")
    macro_items = _section_items(client_text, "### 宏观面")

    if theme_items and not any("赛道判断：" in item for item in theme_items):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 首页没有把“赛道判断”单独写出来；当前仍容易把主题强弱、载体适配和动作建议混成一个结论。",
            )
        )
    if micro_items and not any("载体判断：" in item for item in micro_items):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 首页没有把“载体判断”单独写出来；读者会看不清到底是赛道不行，还是当前产品/个股不是最佳主攻载体。",
            )
        )
    execution_items = [item for item in action_items if "执行卡：" in item]
    if action_items and not execution_items:
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 首页没有显式给出“执行卡”；当前更像研究摘要，不像交易前能快速落地的动作卡。",
            )
        )
    execution_blob = " ".join(execution_items)
    action_blob = " ".join(action_items)
    if execution_items and not any(token in execution_blob for token in ("触发", "确认", "承接", "买点")):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P2] {report_type} 首页 `执行卡` 没有写清最关键的触发条件，当前仍像方向判断，不像执行判断。",
            )
        )
    price_like = bool(re.search(r"([0-9]+(?:\.[0-9]+)?\s*-\s*[0-9]+(?:\.[0-9]+)?)|([0-9]+\.[0-9]{2,3})", action_blob))
    if price_like and execution_items and not any(token in execution_blob for token in ("失效位", "止损", "跌破", "减仓", "目标")):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 首页已经给了精确价位，但 `执行卡` 仍没写清失效位/减仓逻辑，读起来更像机械价位模板，不像交易计划。",
            )
        )
    risk_items = [*macro_items, *micro_items, *action_items]
    if risk_items and not any("尾部风险：" in item for item in risk_items):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P2] {report_type} 首页没有把“尾部风险”单独写出来；macro 和风险约束还在散落描述，没形成真正的风险边界。",
            )
        )
    return findings


def _theme_playbook_surface_findings(
    client_text: str,
    report_type: str,
    editor_theme_playbook: Mapping[str, Any] | None = None,
) -> List[str]:
    findings: List[str] = []
    if report_type not in {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan"}:
        return findings
    theme_section = _section_text(client_text, "### 板块 / 主题认知")
    micro_section = _section_text(client_text, "### 微观面")
    body_text = _text_without_section(client_text, "## 首页判断")
    homepage_text = "\n".join(part for part in (theme_section, micro_section) if part).strip()
    if not homepage_text:
        return findings
    conflict_markers = ("还在打架", "硬写成单一细主题")
    has_conflict_signal = _contains_any(homepage_text, conflict_markers) or (
        "行业层" in homepage_text and "细主题" in homepage_text
    )
    if has_conflict_signal and "主题边界" not in client_text:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 首页已经写出行业层冲突/退回边界，但正文没有显式落 `主题边界`，主题合同在首页和正文之间断了。",
            )
        )
    bridge_markers = ("可优先留意", "更偏向", "细分线", "下钻方向")
    has_bridge_signal = _contains_any(homepage_text, bridge_markers)
    if has_bridge_signal and "细分观察" not in client_text:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 首页已经给出 sector bridge / 细分观察线索，但正文没有显式落 `细分观察`，读者会只看到首页谨慎、正文却缺少对应边界。",
            )
        )
    playbook = dict(editor_theme_playbook or {})
    playbook_level = str(playbook.get("playbook_level") or "").strip()
    playbook_label = str(playbook.get("label") or "").strip()
    if playbook_level == "theme" and playbook_label and theme_section and playbook_label not in theme_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 editor_payload 已明确命中 `{playbook_label}`，但首页 `板块 / 主题认知` 没把这条主题显式写出来。",
            )
        )
    hard_sector = str(playbook.get("hard_sector_label") or "").strip()
    if (
        hard_sector == "金融"
        and playbook_label == "高股息 / 红利"
        and _contains_any(client_text, ("证券", "券商", "非银", "多元金融"))
    ):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 把 `证券 / 券商 / 非银` 这类金融子行业直接包装成 `高股息 / 红利`；红利最多只能当软风格线索，首页主主题至少要退回 `金融` 行业层。",
            )
        )
    if playbook_level != "sector":
        return findings
    theme_match_status = str(playbook.get("theme_match_status") or "").strip()
    theme_match_candidates = tuple(
        str(item).strip()
        for item in list(playbook.get("theme_match_candidates") or [])
        if str(item).strip()
    )
    if theme_match_status == "ambiguous_conflict" and "主题边界" in client_text and theme_match_candidates:
        if not _contains_any(body_text, theme_match_candidates):
            findings.append(
                format_lesson_finding(
                    "L002",
                    f"[P2] {report_type} 的 editor_payload 已标记 `ambiguous_conflict`，但正文 `主题边界` 没写出具体冲突候选。",
                )
            )
    bridge_confidence = str(playbook.get("subtheme_bridge_confidence") or "").strip()
    bridge_top_label = str(playbook.get("subtheme_bridge_top_label") or "").strip()
    if bridge_confidence in {"high", "medium"} and "细分观察" in client_text and bridge_top_label and bridge_top_label not in body_text:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 editor_payload 已给出下钻主线 `{bridge_top_label}`，但正文 `细分观察` 没把这条细分线写出来。",
            )
        )
    return findings


def _editor_prompt_theme_contract(prompt_text: str) -> Dict[str, Any]:
    items = _section_items(prompt_text, "## Theme Playbook")
    if not items:
        return {}
    mapping: Dict[str, str] = {}
    for item in items:
        if "：" not in item:
            continue
        key, value = item.split("：", 1)
        mapping[key.strip()] = value.strip()
    if not mapping:
        return {}
    candidates = tuple(part.strip() for part in mapping.get("易混主题候选", "").split("/") if part.strip())
    bridge_labels = tuple(part.strip() for part in mapping.get("行业层下钻方向", "").split("/") if part.strip())
    signal_line = mapping.get("当前下钻线索", "")
    top_signal_label = ""
    if signal_line:
        first_chunk = signal_line.split("/", 1)[0].strip()
        top_signal_label = first_chunk.split("<-", 1)[0].strip()
    if not top_signal_label and bridge_labels:
        top_signal_label = bridge_labels[0]
    return {
        "label": mapping.get("主题", ""),
        "theme_match_status": mapping.get("主题匹配状态", ""),
        "theme_match_candidates": candidates,
        "bridge_confidence": mapping.get("行业层下钻置信度", ""),
        "bridge_labels": bridge_labels,
        "bridge_top_label": top_signal_label,
    }


def _editor_prompt_theme_findings(client_text: str, report_type: str, editor_prompt_text: str = "") -> List[str]:
    findings: List[str] = []
    if report_type not in {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan"}:
        return findings
    contract = _editor_prompt_theme_contract(editor_prompt_text)
    if not contract:
        return findings
    body_text = _text_without_section(client_text, "## 首页判断")
    candidates = tuple(str(item).strip() for item in contract.get("theme_match_candidates", ()) if str(item).strip())
    if (
        str(contract.get("theme_match_status") or "").strip() == "ambiguous_conflict"
        and "主题边界" in client_text
        and candidates
        and not _contains_any(body_text, candidates)
    ):
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 editor_prompt 已要求保留冲突候选，但正文 `主题边界` 没把这些具体主题写出来。",
            )
        )
    bridge_confidence = str(contract.get("bridge_confidence") or "").strip()
    bridge_top_label = str(contract.get("bridge_top_label") or "").strip()
    if bridge_confidence in {"high", "medium"} and "细分观察" in client_text and bridge_top_label and bridge_top_label not in body_text:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 editor_prompt 已给出下钻主线 `{bridge_top_label}`，但正文 `细分观察` 没把它写出来。",
            )
        )
    return findings


def _event_digest_surface_findings(
    client_text: str,
    report_type: str,
    event_digest_contract: Mapping[str, Any] | None = None,
) -> List[str]:
    findings: List[str] = []
    if report_type not in {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan"}:
        return findings
    contract = dict(event_digest_contract or {})
    status = str(contract.get("status") or "").strip()
    changed_what = str(contract.get("changed_what") or "").strip()
    if not status and not changed_what:
        return findings
    event_section = _section_text(client_text, "## 事件消化")
    if not event_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 客户稿缺少 `## 事件消化`，事件状态和“这件事改变了什么”没有正式落到正文。",
            )
        )
        return findings
    normalized_event_section = _normalize_event_digest_surface_text(event_section)
    if status and status not in event_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 event_digest_contract 已标记 `{status}`，但正文 `事件消化` 没把这个状态写出来。",
            )
        )
    if "这件事改变了什么" not in event_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 `事件消化` 缺少“这件事改变了什么”，还停在事件罗列层。",
            )
        )
    lead_layer = str(contract.get("lead_layer") or "").strip()
    if lead_layer and lead_layer not in event_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 event_digest_contract 已标记 `{lead_layer}`，但正文 `事件消化` 没显式写出事件分层。",
            )
        )
    lead_detail = str(contract.get("lead_detail") or "").strip()
    if lead_detail and _normalize_event_digest_surface_text(lead_detail) not in normalized_event_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 event_digest_contract 已标记 `{lead_detail}`，但正文 `事件消化` 没把事件细分写出来。",
            )
        )
    impact_summary = str(contract.get("impact_summary") or "").strip()
    impact_line = next(
        (line.strip() for line in event_section.splitlines() if line.strip().startswith("- 影响层与性质：")),
        "",
    )
    if impact_summary and impact_summary not in impact_line:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 event_digest_contract 已标记影响层 `{impact_summary}`，但正文 `事件消化` 没写清它影响的是哪一层。",
            )
        )
    thesis_scope = str(contract.get("thesis_scope") or "").strip()
    if thesis_scope and thesis_scope not in impact_line:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 event_digest_contract 已标记事件性质 `{thesis_scope}`，但正文 `事件消化` 没写清它是 thesis 变化还是一次性噪音。",
            )
        )
    importance_reason = str(contract.get("importance_reason") or "").strip()
    if importance_reason and "优先级判断" not in event_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P2] {report_type} 的 event_digest_contract 已写入优先级判断，但正文 `事件消化` 没解释为什么该前置或先不升级。",
            )
        )
    return findings


def _what_changed_surface_findings(
    client_text: str,
    report_type: str,
    what_changed_contract: Mapping[str, Any] | None = None,
) -> List[str]:
    findings: List[str] = []
    if report_type not in {"stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan"}:
        return findings
    contract = dict(what_changed_contract or {})
    previous_view = str(contract.get("previous_view") or "").strip()
    change_summary = str(contract.get("change_summary") or "").strip()
    conclusion_label = str(contract.get("conclusion_label") or "").strip()
    state_trigger = str(contract.get("state_trigger") or "").strip()
    state_summary = str(contract.get("state_summary") or "").strip()
    current_event_understanding = str(contract.get("current_event_understanding") or "").strip()
    if not previous_view and not change_summary and not conclusion_label:
        return findings
    what_changed_section = _section_text(client_text, "## What Changed")
    if not what_changed_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 客户稿缺少 `## What Changed`，没有正式回答上次怎么看、这次什么变了、结论有没有变化。",
            )
        )
        return findings
    if "上次怎么看" not in what_changed_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 `What Changed` 缺少“上次怎么看”，连续研究还没落到正文。",
            )
        )
    if "这次什么变了" not in what_changed_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 `What Changed` 缺少“这次什么变了”，还不能快速回答研究变化点。",
            )
        )
    if "结论变化" not in what_changed_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 `What Changed` 缺少“结论变化”，升级/降级还没正式落到正文。",
            )
        )
    if current_event_understanding and "当前事件理解" not in what_changed_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 what_changed_contract 已写入当前事件理解，但正文 `What Changed` 没把这层研究理解落出来。",
            )
        )
    if conclusion_label and conclusion_label not in what_changed_section:
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 what_changed_contract 已标记 `{conclusion_label}`，但正文 `What Changed` 没把这个结论变化写出来。",
            )
        )
    if state_trigger and ("触发：" not in what_changed_section or state_trigger not in what_changed_section):
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 what_changed_contract 已写入状态触发 `{state_trigger}`，但正文 `What Changed` 没解释这次为什么升级、削弱或待复核。",
            )
        )
    if state_summary and ("状态解释" not in what_changed_section or state_summary not in what_changed_section):
        findings.append(
            format_lesson_finding(
                "L002",
                f"[P1] {report_type} 的 what_changed_contract 已写入状态解释，但正文 `What Changed` 没把这次状态机原因落成完整解释。",
            )
        )
    return findings


def _normalize_duplicate_text(text: str) -> str:
    line = str(text).strip()
    if not line:
        return ""
    for prefix in (*GENERIC_OPERATION_PREFIXES, *PICK_OPERATION_PREFIXES):
        if line.startswith(prefix):
            line = line[len(prefix) :].strip()
            break
    line = re.sub(r"[（(][^）)]{1,20}[）)]", "", line)
    line = re.sub(r"`([^`]+)`", r"\1", line)
    line = re.sub(r"\s+", "", line)
    return line


def _normalize_signal_text(text: str) -> str:
    line = str(text).strip()
    if not line:
        return ""
    line = re.sub(r"[；;。]\s*当前图形标签：.*$", "", line)
    replacements = (
        ("美股开盘前观察", "晚间外盘观察"),
        ("开盘前观察", "盘前观察"),
    )
    for source, target in replacements:
        line = line.replace(source, target)
    line = re.sub(r"`([^`]+)`", r"\1", line)
    line = re.sub(r"\s+", "", line)
    return line


def _normalize_event_digest_surface_text(text: str) -> str:
    line = _normalize_signal_text(text)
    if not line:
        return ""
    replacements = (
        ("当前更像", "更像"),
        ("现在处在", "更像"),
        ("不把", "别把"),
        ("不等于", "不代表"),
        ("先按", "按"),
        ("先别", "别"),
        ("情报属性：", ""),
        ("当前结论：", "结论："),
    )
    for old, new in replacements:
        line = line.replace(old, new)
    line = re.sub(r"[。；;，,:：·/（）()\-]+", "", line)
    return line


def _operation_bullets(text: str) -> List[str]:
    bullets: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        body = stripped[2:].strip()
        if any(body.startswith(prefix) for prefix in PICK_OPERATION_PREFIXES):
            bullets.append(body)
    return bullets


def _duplicate_explanation_findings(text: str, *, max_repeat: int = 2) -> List[str]:
    findings: List[str] = []
    normalized: Dict[str, List[str]] = {}
    for item in _explanation_bullets(text):
        key = _normalize_duplicate_text(item)
        if not key:
            continue
        normalized.setdefault(key, []).append(item)
    for items in normalized.values():
        if len(items) > max_repeat:
            findings.append(format_lesson_finding("L003", f"[P1] 解释文案重复过多（{len(items)} 次），像模板而不像成稿: {items[0]}"))
    return findings


def _duplicate_operation_findings(text: str, *, max_repeat: int = 2, scope: str = "报告") -> List[str]:
    findings: List[str] = []
    normalized: Dict[str, List[str]] = {}
    for item in _operation_bullets(text):
        key = _normalize_duplicate_text(item)
        if not key:
            continue
        normalized.setdefault(key, []).append(item)
    for items in normalized.values():
        if len(items) > max_repeat:
            findings.append(format_lesson_finding("L003", f"[P1] {scope} 的周期/动作文案重复过多（{len(items)} 次），像模板而不像成稿: {items[0]}"))
    return findings


def _stock_pick_observe_density_findings(text: str) -> List[str]:
    findings: List[str] = []
    observe_only = "| 报告定位 | 观察稿 |" in text or "当前没有达到正式动作阈值的个股" in text
    if not observe_only:
        return findings
    if "## 观察名单代表样本详细拆解" in text:
        findings.append(
            format_lesson_finding(
                "L037",
                "[P1] stock_pick 观察稿仍保留完整代表样本 appendix；无动作场景应优先压成触发器清单，而不是继续展开完整八维拆解。",
            )
        )
    lead_count = text.count("**先看结论：**")
    if lead_count > 6:
        findings.append(
            format_lesson_finding(
                "L037",
                f"[P2] stock_pick 观察稿的“先看结论”重复过多（{lead_count} 次），更像模板噪音而不是高密度观察稿。",
            )
        )
    return findings


def _stock_pick_feature_retention_findings(text: str) -> List[str]:
    findings: List[str] = []
    observe_only = "| 报告定位 | 观察稿 |" in text or "当前没有达到正式动作阈值的个股" in text
    if not observe_only:
        return findings
    if "## 观察名单复核卡" not in text:
        findings.append(
            format_lesson_finding(
                "L041",
                "[P1] stock_pick 观察稿丢了 `观察名单复核卡`，observe-only 不能只剩触发器表，至少要保住 top 标的的轻量复核层。",
            )
        )
    if "### 第二批：继续跟踪" not in text:
        findings.append(
            format_lesson_finding(
                "L041",
                "[P1] stock_pick 观察稿丢了 `第二批：继续跟踪`，任何开发改动都不应把旧的观察层信息密度静默吞掉。",
            )
        )
    if not any(marker in text for marker in ("### 第二批：低门槛 / 观察替代", "### 第二批：低门槛 / 关联ETF")):
        findings.append(
            format_lesson_finding(
                "L041",
                "[P1] stock_pick 观察稿丢了 `第二批：低门槛 / 观察替代` 或 `第二批：低门槛 / 关联ETF`，任何开发改动都不应把低门槛跟踪层静默吞掉。",
            )
        )
    if "## 代表样本复核卡" not in text:
        findings.append(
            format_lesson_finding(
                "L041",
                "[P1] stock_pick 观察稿丢了 `代表样本复核卡`，任何开发改动都不应把代表样本复核层静默吞掉。",
            )
        )
    return findings


def _stock_pick_first_screen_execution_findings(text: str) -> List[str]:
    if "| 报告定位 | 观察稿 |" in text or "当前没有达到正式动作阈值的个股" in text:
        return []
    table = _table_mapping_in_section(text, "## 先看执行")
    if not table:
        return []
    findings: List[str] = []
    trigger_text = str(table.get("怎么触发", "")).strip()
    position_text = str(table.get("多大仓位", "")).strip()
    stop_text = str(table.get("哪里止损", "")).strip()
    if "不硬拼统一买点" in trigger_text or "对应个股复核卡里的介入条件兑现" in trigger_text:
        findings.append(
            format_lesson_finding(
                "L002",
                "[P1] stock_pick 正式稿首屏 `怎么触发` 仍是泛化提示，没有把前排标的各自的建仓区或触发位直接写出来。",
            )
        )
    if "首屏不写统一止损价" in stop_text:
        findings.append(
            format_lesson_finding(
                "L002",
                "[P1] stock_pick 正式稿首屏 `哪里止损` 仍在回避具体失效位；榜单首屏至少要把前排标的各自的止损/失效位前置出来。",
            )
        )
    if position_text == "单票 `2% - 5%` 试仓":
        findings.append(
            format_lesson_finding(
                "L002",
                "[P2] stock_pick 正式稿首屏 `多大仓位` 仍是统一模板句，没有把前排标的的首仓口径和组合层约束一起写清。",
            )
        )
    return findings


def _fund_profile_findings(text: str) -> List[str]:
    findings: List[str] = []
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != "## 基金画像":
            continue
        header, rows = _parse_markdown_table(lines, idx + 1)
        if header[:2] != ["项目", "内容"]:
            findings.append(format_lesson_finding("L005", "[P1] 基金画像章节存在，但未找到标准画像表"))
            return findings
        payload = {row[0]: row[1] for row in rows if len(row) >= 2}
        required = ("基金类型", "基金公司", "基金经理", "成立日期", "业绩比较基准")
        for key in required:
            value = str(payload.get(key, "")).strip()
            if value in ("", "—", "nan", "None"):
                findings.append(format_lesson_finding("L005", f"[P1] 基金画像基础字段缺失: {key}"))
        return findings
    return findings


def _fund_holdings_readability_findings(text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    lines = text.splitlines()
    target_headers = {"### 前五大持仓", "### 前十大持仓"}
    for idx, line in enumerate(lines):
        if line.strip() not in target_headers:
            continue
        header, rows = _parse_markdown_table(lines, idx + 1)
        if len(header) < 2 or "名称" not in header[1]:
            continue
        for row in rows:
            if len(row) < 2:
                continue
            code = str(row[0]).strip()
            name = str(row[1]).strip()
            if code and name in {"", "—", "nan", "None"}:
                findings.append(format_lesson_finding("L005", f"[P2] {report_type} 的持仓表仍有名称空白，客户稿可读性不足：{code}"))
                return findings
        return findings
    return findings


def _source_feature_retention_findings(client_text: str, source_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    if not source_text:
        return findings
    if report_type != "scan":
        return findings
    feature_markers = (
        ("## 基金画像", "基金画像"),
        ("### 资产配置", "资产配置"),
        (("### 前十大持仓", "### 前五大持仓"), "持仓展开"),
        ("### 行业暴露", "行业暴露"),
        ("## 基金经理风格分析", "基金经理风格分析"),
    )
    for marker, label in feature_markers:
        if isinstance(marker, tuple):
            source_has = any(option in source_text for option in marker)
            client_has = any(option in client_text for option in marker)
        else:
            source_has = marker in source_text
            client_has = marker in client_text
        if source_has and not client_has:
            findings.append(
                format_lesson_finding(
                    "L041",
                    f"[P1] {report_type} 客户稿丢了 `{label}` 这类旧有高价值功能块；任何开发改动都不应把已存在的有效模块静默吞掉。",
                )
            )
    return findings


def _pick_fund_profile_feature_retention_findings(client_text: str, source_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    if report_type != "etf_pick" or not source_text:
        return findings
    feature_markers = (
        ("场内基金技术状态", "场内基金技术状态"),
    )
    for marker, label in feature_markers:
        if marker in source_text and marker not in client_text:
            findings.append(
                format_lesson_finding(
                    "L041",
                    f"[P1] {report_type} 客户稿丢了 `{label}` 这类 ETF 产品层技术状态字段；任何开发改动都不应把已下沉的新主链功能静默吞掉。",
                )
            )
    return findings


def _pick_auxiliary_score_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"etf_pick", "fund_pick"}:
        return []
    if re.search(r"\|\s*筹码结构(?:（辅助项）)?\s*\|\s*\d+\s*/\s*\d+\s*\|", text):
        return [
            format_lesson_finding(
                "L038",
                f"[P1] {report_type} 仍把 `筹码结构` 辅助项渲染成硬分数，容易让读者误以为这项参与了主排序。",
            )
        ]
    return []


def _absolute_asset_path_findings(text: str, report_type: str) -> List[str]:
    if re.search(r"/Users/[^)\s]+/reports/assets/", text):
        return [
            format_lesson_finding(
                "L039",
                f"[P1] {report_type} 客户稿仍引用本机绝对图片路径，换设备或分享后会失效。",
            )
        ]
    return []


def _pick_lead_density_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"etf_pick", "fund_pick"}:
        return []
    lead_count = text.count("**先看结论：**")
    if lead_count > 6:
        return [
            format_lesson_finding(
                "L003",
                f"[P2] {report_type} 的“先看结论”重复过多（{lead_count} 次），更像模板噪音而不是高密度成稿。",
            )
        ]
    return []


def _standard_taxonomy_findings(text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != "## 标准化分类":
            continue
        table_start = None
        for probe in range(idx + 1, len(lines)):
            stripped = lines[probe].strip()
            if stripped.startswith("|"):
                table_start = probe
                break
            if stripped.startswith("## "):
                break
        if table_start is None:
            findings.append(format_lesson_finding("L005", f"[P1] {report_type} 缺少标准化分类表"))
            return findings
        header, rows = _parse_markdown_table(lines, table_start)
        if header[:2] != ["维度", "结果"]:
            findings.append(format_lesson_finding("L005", f"[P1] {report_type} 标准化分类章节存在，但未找到标准分类表"))
            return findings
        payload = {row[0]: row[1] for row in rows if len(row) >= 2}
        required = ("产品形态", "载体角色", "管理方式", "暴露类型", "主方向")
        for key in required:
            value = str(payload.get(key, "")).strip()
            if value in ("", "—", "nan", "None"):
                findings.append(format_lesson_finding("L005", f"[P1] {report_type} 标准化分类缺失关键字段: {key}"))
        return findings
    return findings


def _extract_delivery_tier_label(text: str) -> str:
    if not text:
        return ""
    candidates = [*_section_items(text, "## 交付等级"), *text.splitlines()]
    for item in candidates:
        match = re.search(r"交付等级\s*[：:]\s*`?([^`\n]+?)`?(?:。|$)", str(item).strip())
        if match:
            return match.group(1).strip()
    return ""


def _extract_pick_passed_pool(text: str) -> int | None:
    patterns = (
        r"完整分析:\s*`?(\d+)`?",
        r"再对其中\s*`?(\d+)`?\s*只做完整分析",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def _extract_pick_coverage_total(text: str) -> int | None:
    match = re.search(r"覆盖率的分母是今天进入完整分析的\s*`?(\d+)`?\s*只", text)
    if match:
        return int(match.group(1))
    return None


def _delivery_tier_findings(client_text: str, source_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    items = _section_items(client_text, "## 交付等级")
    if len(items) < 2:
        findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿解释性不足：'交付等级' 至少需要等级和适用口径两条说明"))
    if not any("初筛" in item and "完整分析" in item for item in items):
        findings.append(format_lesson_finding("L002", f"[P2] {report_type} 交付等级没有说明“初筛 -> 少量完整分析”的真实流程"))

    client_label = _extract_delivery_tier_label(client_text)
    source_label = _extract_delivery_tier_label(source_text)
    if source_label and client_label and client_label != source_label:
        findings.append(format_lesson_finding("L005", f"[P1] {report_type} 客户稿与详细稿交付等级不一致: client={client_label} source={source_label}"))

    effective_label = source_label or client_label
    if effective_label and effective_label != "标准推荐稿":
        observe_markers = (
            "观察优先",
            "不按正式推荐稿理解",
            "不是正式买入稿",
            "不代表完整全市场优选结论",
            "只适合当作兜底观察名单",
            "只适合按观察优先处理",
        )
        if not any(marker in client_text for marker in observe_markers):
            findings.append(format_lesson_finding("L002", f"[P1] {report_type} 当前是 `{effective_label}`，但客户稿没有明确按观察优先/非正式推荐处理"))
        first_heading = next((line.strip() for line in client_text.splitlines() if line.strip().startswith("# ")), "")
        if "推荐" in first_heading:
            findings.append(format_lesson_finding("L018", f"[P2] {report_type} 当前是 `{effective_label}`，标题仍写成“推荐”，容易高估这份稿件的可执行性"))
        if "## 为什么推荐它" in client_text:
            findings.append(format_lesson_finding("L018", f"[P2] {report_type} 当前是 `{effective_label}`，观察稿章节仍写成“为什么推荐它”，建议改成“为什么先看它”"))
    return findings


def _pick_delivery_consistency_findings(client_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    passed_pool = _extract_pick_passed_pool(client_text)
    coverage_total = _extract_pick_coverage_total(client_text)
    if passed_pool is not None and coverage_total is not None and passed_pool != coverage_total:
        findings.append(
            format_lesson_finding(
                "L031",
                f"[P1] {report_type} 覆盖率分母与完整分析样本不一致: coverage_total={coverage_total} passed_pool={passed_pool}",
            )
        )

    delivery_label = _extract_delivery_tier_label(client_text)
    if delivery_label == "标准推荐稿":
        conflict_markers = (
            "只能按观察优先或降级稿处理",
            "只能按观察优先处理",
            "不按正式推荐稿理解",
        )
        alternative_items = _section_items(client_text, "## 为什么不是另外几只")
        if any(marker in item for marker in conflict_markers for item in alternative_items):
            findings.append(
                format_lesson_finding(
                    "L032",
                    f"[P1] {report_type} 当前仍是 `标准推荐稿`，但单候选说明把它改写成了观察/降级稿口径",
                )
            )
    return findings


def _preferred_sector_track_findings(client_text: str, report_type: str) -> List[str]:
    if report_type != "etf_pick":
        return []
    match = re.search(r"偏好主题\s*[:：]\s*([^\n|]+)", client_text)
    if not match:
        return []
    preferred_label = match.group(1).strip()
    preferred = {item.strip() for item in re.split(r"[/／、,，]\s*", preferred_label) if item.strip() and item.strip() != "未指定"}
    if not preferred:
        return []
    allowed_aliases = {
        "科技": {"科技", "信息技术", "AI硬件", "AI算力", "通信", "半导体", "芯片", "CPO", "光模块", "游戏", "传媒"},
        "半导体": {"半导体", "芯片", "科创芯片", "半导体设备", "半导体材料"},
        "通信": {"通信", "通信设备", "CPO", "光模块", "5G", "6G", "数据中心", "运营商"},
        "医药": {"医药", "创新药", "医疗", "CXO", "CRO", "医疗器械"},
        "创新药": {"创新药", "港股医药", "医药", "FDA", "临床"},
        "电网": {"电网", "电力设备", "智能电网", "特高压", "储能"},
        "传媒": {"传媒", "游戏", "动漫", "AIGC"},
        "游戏": {"游戏", "传媒", "动漫"},
        "宽基": {"宽基", "沪深300", "中证A500", "A500", "中证500", "上证50"},
        "黄金": {"黄金", "贵金属"},
        "商品": {"黄金", "贵金属", "商品", "能源", "原油", "煤炭"},
        "能源": {"能源", "原油", "油气", "煤炭", "化工"},
        "高股息": {"高股息", "红利", "银行", "公用事业", "运营商"},
    }
    allowed_terms = set(preferred)
    for label in preferred:
        allowed_terms.update(allowed_aliases.get(label, set()))

    sector_markers = {
        "黄金": ("黄金", "贵金属"),
        "能源": ("能源", "原油", "油气", "煤炭", "化工"),
        "高股息": ("红利", "高股息", "银行ETF"),
        "传媒": ("游戏", "传媒", "动漫"),
        "医药": ("创新药", "医药", "医疗", "CXO", "CRO", "医疗器械"),
        "通信": ("通信", "CPO", "光模块", "5G", "6G", "数据中心", "运营商"),
        "半导体": ("半导体", "芯片", "科创芯片"),
        "电网": ("电网", "电力", "特高压", "智能电网", "储能"),
    }
    track_lines = [
        line.strip()
        for line in client_text.splitlines()
        if re.match(r"^\|\s*(优先观察|次级观察|补充观察|第一推荐|第二推荐|第三推荐|短线|中线|波段)\s*\|", line.strip())
    ]
    findings: List[str] = []
    for line in track_lines:
        if any(token in line for token in ("对冲", "防守", "避险")):
            continue
        for sector, markers in sector_markers.items():
            if sector in allowed_terms or any(alias in allowed_terms for alias in markers):
                continue
            if any(marker in line for marker in markers):
                findings.append(
                    format_lesson_finding(
                        "L032",
                        f"[P1] {report_type} 当前偏好主题是 `{preferred_label}`，但可见推荐/观察分层混入 `{sector}` 方向：{line}",
                    )
                )
                break
    return findings


def _observe_only_packaging_findings(client_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    has_formal_pick = bool(re.search(r"\|\s*正式推荐\s*\|", client_text)) or any(
        marker in client_text for marker in ("为什么能进正式推荐", "## 为什么推荐它")
    )
    delivery_label = _extract_delivery_tier_label(client_text)
    current_action = _table_value(client_text, "当前建议") or _table_value(client_text, "当前动作")
    observe_only = (
        "观察" in delivery_label
        or (not has_formal_pick and ("暂不出手" in client_text or "观察为主" in client_text or client_text.count("看好但暂不推荐") >= 2))
        or any(marker in current_action for marker in ("观察", "暂不出手", "回避", "等待"))
    )
    if not observe_only:
        return findings

    stale_formal_markers = (
        "## 其余正式推荐",
        "继续按正式推荐框架",
        "仍可作为正式推荐框架",
        "仍按正式推荐框架",
        "正式推荐框架下的单只优先对象",
        "标准推荐稿里的单只优先对象",
        "并入上面的正式推荐层",
        "并入正式推荐层",
    )
    leaked = [marker for marker in stale_formal_markers if marker in client_text]
    if leaked:
        findings.append(
            format_lesson_finding(
                "L033",
                f"[P1] {report_type} 当前是观察/降级稿，但仍泄露正式推荐包装：{', '.join(leaked[:3])}",
            )
        )
    hard_action_rows = []
    for match in re.finditer(r"\|\s*当前(?:建议|动作)\s*\|\s*([^|\n]+?)\s*\|", client_text):
        value = match.group(1).strip()
        if any(token in value for token in ("做多", "买入", "加仓", "正式推荐")) and not any(
            token in value for token in ("观察", "等待", "等右侧", "待确认", "等确认", "不追高", "持有优于追高")
        ):
            hard_action_rows.append(value)
    if hard_action_rows:
        findings.append(
            format_lesson_finding(
                "L033",
                f"[P1] {report_type} 当前是观察/降级稿，但执行表仍写成硬动作：{', '.join(hard_action_rows[:3])}",
            )
        )

    first_heading = next((line.strip() for line in client_text.splitlines() if line.strip().startswith("# ")), "")
    if "推荐" in first_heading:
        findings.append(
            format_lesson_finding(
                "L033",
                f"[P1] {report_type} 当前整份稿件没有可执行候选，但标题仍写成“推荐”，应明确改成观察稿或写清今日无正式推荐。",
            )
        )

    packaging_labels = []
    for token, label in (
        ("第一批：核心主线", "核心主线"),
        ("低门槛可执行", "低门槛可执行"),
        ("短线先看", "短线先看"),
        ("中线先看", "中线先看"),
    ):
        if token in client_text:
            packaging_labels.append(f"`{label}`")
    if packaging_labels:
        findings.append(
            format_lesson_finding(
                "L033",
                f"[P1] {report_type} 当前整份稿件没有可执行候选，但仍使用 {' / '.join(packaging_labels)} 这类推荐包装，容易把观察名单误读成交易建议。",
            )
        )
    if "做多；观察" in client_text or "观察；做多" in client_text:
        findings.append(
            format_lesson_finding(
                "L033",
                f"[P1] {report_type} 把方向判断和观察状态直接拼成同一条动作建议（如 `做多；观察为主`），读者会误解成已经允许执行。",
            )
        )
    return findings


def _client_stock_table(text: str, market_heading: str = "## A股") -> Dict[str, Dict[str, str]]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() == market_heading:
            table_start = None
            for probe in range(idx + 1, len(lines)):
                if lines[probe].strip().startswith("|"):
                    table_start = probe
                    break
            if table_start is None:
                return {}
            header, rows = _parse_markdown_table(lines, table_start)
            if header[:7] != ["标的", "技术", "基本面", "催化", "相对强弱", "风险", "结论"]:
                return {}
            payload: Dict[str, Dict[str, str]] = {}
            for row in rows:
                if len(row) < 7:
                    continue
                payload[row[0]] = {
                    "technical": row[1],
                    "fundamental": row[2],
                    "catalyst": row[3],
                    "relative_strength": row[4],
                    "risk": row[5],
                    "conclusion": row[6],
                }
            return payload
    return {}


def _analysis_client_dimension_map(text: str) -> Dict[str, str]:
    return _pick_client_dimension_map(text, "## 为什么这么判断")


def _pick_client_dimension_map(text: str, heading: str) -> Dict[str, str]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != heading:
            continue
        table_start = None
        for probe in range(idx + 1, len(lines)):
            if lines[probe].strip().startswith("|"):
                table_start = probe
                break
            if lines[probe].strip().startswith("## "):
                break
        if table_start is None:
            return {}
        header, rows = _parse_markdown_table(lines, table_start)
        if header[:3] != ["维度", "分数", "为什么是这个分"]:
            return {}
        payload: Dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            payload[str(row[0]).strip()] = str(row[1]).strip()
        return payload
    return {}


def _analysis_source_dimension_map(text: str) -> Dict[str, str]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != "## 八维评分":
            continue
        table_start = None
        for probe in range(idx + 1, len(lines)):
            if lines[probe].strip().startswith("|"):
                table_start = probe
                break
            if lines[probe].strip().startswith("## "):
                break
        if table_start is None:
            return {}
        header, rows = _parse_markdown_table(lines, table_start)
        if header[:4] != ["维度", "得分", "一句话判断", "详情"]:
            return {}
        payload: Dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            payload[str(row[0]).strip()] = str(row[1]).strip()
        return payload
    return {}


def _analysis_source_consistency_findings(client_text: str, source_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    client_map = _analysis_client_dimension_map(client_text)
    if not client_map:
        findings.append(f"[P1] {report_type} 客户稿未解析出“为什么这么判断”维度表，无法做源稿一致性校验")
        return findings
    source_map = _analysis_source_dimension_map(source_text)
    if not source_map:
        findings.append(f"[P1] {report_type} 详细稿未解析出“八维评分”维度表，无法做源稿一致性校验")
        return findings
    for label, client_score in client_map.items():
        source_score = source_map.get(label)
        if source_score is None:
            findings.append(f"[P1] {report_type} 客户稿维度在详细稿里不存在: {label}")
            continue
        if _normalized_score_token(client_score) != _normalized_score_token(source_score):
            findings.append(
                f"[P1] {report_type} 客户稿与详细稿分数不一致: {label} client={client_score} source={source_score}"
            )
    return findings


def _normalized_score_token(value: str) -> str:
    text = str(value).strip()
    if text in {"", "—", "—/100", "缺失", "信息项", "不适用"}:
        return "MISSING"
    return text


def _pick_source_consistency_findings(client_text: str, source_text: str, report_type: str, heading: str) -> List[str]:
    findings: List[str] = []
    client_map = _pick_client_dimension_map(client_text, heading)
    if not client_map:
        findings.append(f"[P1] {report_type} 客户稿未解析出维度评分表，无法做源稿一致性校验")
        return findings
    source_map = _analysis_source_dimension_map(source_text)
    if not source_map:
        findings.append(f"[P1] {report_type} 详细稿未解析出“八维评分”维度表，无法做源稿一致性校验")
        return findings
    for label, client_score in client_map.items():
        source_score = source_map.get(label)
        if source_score is None:
            findings.append(f"[P1] {report_type} 客户稿维度在详细稿里不存在: {label}")
            continue
        if _normalized_score_token(client_score) != _normalized_score_token(source_score):
            findings.append(
                f"[P1] {report_type} 客户稿与详细稿分数不一致: {label} client={client_score} source={source_score}"
            )
    return findings


def _briefing_source_consistency_findings(client_text: str, source_text: str) -> List[str]:
    findings: List[str] = []
    client_why = _bullets_in_section(client_text, "## 为什么今天这么判断")
    client_actions = _bullets_in_section_any(client_text, ("## 执行补充", "## 今天怎么做"))
    source_headlines: List[str] = []
    for heading in ("### 1.1 今日主线", "### 2.1 今日主线回顾", "### 3.2 明日主线预判"):
        for item in _section_items(source_text, heading):
            if item not in source_headlines:
                source_headlines.append(item)
    source_actions: List[str] = []
    for heading in ("### 1.2 今天怎么做", "### 1.2 周末怎么跟踪", "### 3.4 明日操作建议"):
        for item in _section_items(source_text, heading):
            if item not in source_actions:
                source_actions.append(item)
    missing_source_parts = False
    if not source_headlines:
        findings.append("[P1] briefing 详细稿缺少“1.1 今日主线 / 2.1 今日主线回顾 / 3.2 明日主线预判”内容，无法做源稿一致性校验")
        missing_source_parts = True
    if not source_actions:
        findings.append("[P1] briefing 详细稿缺少“1.2 今天怎么做 / 1.2 周末怎么跟踪 / 3.4 明日操作建议”内容，无法做源稿一致性校验")
        missing_source_parts = True
    if missing_source_parts:
        return findings
    normalized_headlines = {_normalize_briefing_consistency_line(item) for item in source_headlines}
    normalized_actions = {_normalize_briefing_consistency_line(item) for item in source_actions}
    for item in client_why:
        if _normalize_briefing_consistency_line(item) not in normalized_headlines:
            findings.append(f"[P1] briefing 客户稿理由在详细稿主线章节中不存在: {item}")
    for item in client_actions:
        if _normalize_briefing_consistency_line(item) not in normalized_actions:
            findings.append(f"[P1] briefing 客户稿动作在详细稿行动章节中不存在: {item}")
    return findings


def _normalize_briefing_consistency_line(text: str) -> str:
    line = str(text).strip()
    replacements = (
        (r"开盘\s*30\s*分钟", "早段"),
        (r"开盘后先观察\s*\d+\s*分钟", "先观察早段延续性"),
        (r"明天开盘前", "明早"),
        (r"盘中", "交易时段"),
        (r"日内", "当天"),
    )
    for pattern, repl in replacements:
        line = re.sub(pattern, repl, line)
    return line


def _normalize_markdown(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.splitlines()).strip()


def _client_stock_sections(text: str) -> List[Dict[str, str]]:
    lines = text.splitlines()
    heading_pattern = re.compile(r"^###\s+(?P<name>.+?)\s+\((?P<symbol>[A-Za-z0-9.\-]+)\)(?:\s*\|\s*(?P<label>.+))?\s*$")
    sections: List[Dict[str, str]] = []
    index = 0
    while index < len(lines):
        match = heading_pattern.match(lines[index].strip())
        if not match:
            index += 1
            continue
        start = index + 1
        end = start
        while end < len(lines):
            stripped = lines[end].strip()
            if stripped.startswith("## ") or heading_pattern.match(stripped):
                break
            end += 1
        sections.append(
            {
                "name": match.group("name").strip(),
                "symbol": match.group("symbol").strip(),
                "label": str(match.group("label") or "").strip(),
                "body": "\n".join(lines[start:end]).strip(),
            }
        )
        index = end
    return sections


def _source_stock_sections(text: str) -> Dict[str, Dict[str, str]]:
    pattern = re.compile(
        r"^###\s+\d+\.\s+\[(?P<market>[A-Z]+)\]\s+(?P<name>.+?)\s+\((?P<symbol>[A-Za-z0-9.\-]+)\)\s+(?P<label>.+?)\n(?P<body>.*?)(?=^---\n|^###\s+\d+\.|\Z)",
        re.M | re.S,
    )
    payload: Dict[str, Dict[str, str]] = {}
    for match in pattern.finditer(text):
        payload[match.group("name").strip()] = {
            "symbol": match.group("symbol").strip(),
            "label": match.group("label").strip(),
            "body": match.group("body").strip(),
        }
    return payload


def _dimension_signal_map(section_text: str) -> Dict[str, Dict[str, str]]:
    lines = section_text.splitlines()
    table_start = None
    for idx, line in enumerate(lines):
        if line.strip() == "**八维雷达：**":
            for probe in range(idx + 1, len(lines)):
                stripped = lines[probe].strip()
                if stripped.startswith("|"):
                    table_start = probe
                    break
                if stripped.startswith("**") or stripped.startswith("## ") or stripped.startswith("### "):
                    break
            break
    if table_start is None:
        return {}
    header, rows = _parse_markdown_table(lines, table_start)
    if header[:3] != ["维度", "得分", "核心信号"]:
        return {}
    payload: Dict[str, Dict[str, str]] = {}
    for row in rows:
        if len(row) < 3:
            continue
        payload[row[0].strip()] = {"score": row[1].strip(), "signal": row[2].strip()}
    return payload


def _section_block_lines(section_body: str, heading: str, stop_headings: Tuple[str, ...]) -> List[str]:
    lines = section_body.splitlines()
    collecting = False
    items: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and stripped in stop_headings:
            break
        if not collecting or not stripped:
            continue
        items.append(stripped)
    return items


def _line_looks_stock_specific(line: str) -> bool:
    stripped = str(line).strip()
    if not stripped:
        return False
    if stripped.startswith("|") or stripped.startswith("- "):
        return True
    return any(token in stripped for token in STOCK_SIGNAL_HINTS)


def _stock_section_structure_findings(client_text: str) -> List[str]:
    findings: List[str] = []
    for section in _client_stock_sections(client_text):
        body = section.get("body", "")
        if "**八维雷达：**" in body:
            continue
        if not any(heading in body for heading in STOCK_CARD_SUBHEADINGS):
            continue
        for heading in STOCK_CARD_SUBHEADINGS:
            block = _section_block_lines(body, heading, STOCK_CARD_SUBHEADINGS)
            if block:
                continue
            findings.append(
                format_lesson_finding(
                    "L035",
                    f"[P1] `{section['name']} ({section['symbol']})` 的 `{heading}` 下面没有实质内容，像半成品或串页残留。",
                )
            )
    return findings


def _stock_section_identity_findings(client_text: str, source_text: str) -> List[str]:
    findings: List[str] = []
    source_sections = _source_stock_sections(source_text)
    if not source_sections:
        return findings

    known_symbols = {
        name: str(payload.get("symbol", "")).strip()
        for name, payload in source_sections.items()
        if name
    }
    for section in _client_stock_sections(client_text):
        name = section.get("name", "")
        symbol = section.get("symbol", "")
        body = section.get("body", "")
        if not body:
            continue
        source_section = source_sections.get(name)
        if source_section and source_section.get("symbol") and str(source_section.get("symbol")) != symbol:
            findings.append(
                format_lesson_finding(
                    "L034",
                    f"[P1] 客户稿单票标题与详细稿不一致: `{name}` client={symbol} source={source_section.get('symbol')}",
                )
            )

        client_dimension_map = _dimension_signal_map(body)
        source_dimension_map = _dimension_signal_map(str(source_section.get("body", ""))) if source_section else {}
        for dimension in ("技术面", "基本面", "催化面", "相对强弱", "风险特征"):
            client_row = client_dimension_map.get(dimension)
            source_row = source_dimension_map.get(dimension)
            if not client_row or not source_row:
                continue
            if _normalize_signal_text(client_row.get("signal")) != _normalize_signal_text(source_row.get("signal")):
                findings.append(
                    format_lesson_finding(
                        "L034",
                        f"[P1] `{name} ({symbol})` 的 `{dimension}` 核心信号与详细稿不一致，像串标或拼页: client={client_row.get('signal')} source={source_row.get('signal')}",
                    )
                )

        suspicious_lines: List[Tuple[str, str, str]] = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not _line_looks_stock_specific(line):
                continue
            for other_name, other_symbol in known_symbols.items():
                if other_name == name or other_symbol == symbol:
                    continue
                if other_symbol and other_symbol in line:
                    suspicious_lines.append((other_name, other_symbol, line))
                    break
                if other_name and other_name in line:
                    suspicious_lines.append((other_name, other_symbol, line))
                    break
        if suspicious_lines:
            other_name, other_symbol, line = suspicious_lines[0]
            findings.append(
                format_lesson_finding(
                    "L034",
                    f"[P1] `{name} ({symbol})` 段落混入了 `{other_name} ({other_symbol})` 的股票级内容，像串标/拼页: {line}",
                )
            )
    return findings


def _retrospect_source_consistency_findings(client_text: str, source_text: str) -> List[str]:
    if _normalize_markdown(client_text) == _normalize_markdown(source_text):
        return []
    return ["[P1] retrospect 客户稿与内部详细稿不一致：当前流程要求复盘成稿与内部详细稿保持同稿发布"]


def _source_stock_dimensions(text: str) -> Dict[str, Dict[str, str]]:
    pattern = re.compile(
        r"^###\s+\d+\.\s+\[A\]\s+(?P<name>.+?)\s+\((?P<symbol>\d{6})\)\s+(?P<label>.+?)\n(?P<body>.*?)(?=^---\n|^###\s+\d+\.|\Z)",
        re.M | re.S,
    )
    payload: Dict[str, Dict[str, str]] = {}
    for match in pattern.finditer(text):
        body = match.group("body")
        dim_match = re.search(
            r"\| 技术面 \| (?P<technical>\d+)/100 \|.*?\n"
            r"\| 基本面 \| (?P<fundamental>\d+)/100 \|.*?\n"
            r"\| 催化面 \| (?P<catalyst>\d+)/100 \|.*?\n"
            r"\| 相对强弱 \| (?P<relative_strength>\d+)/100 \|.*?\n"
            r"\| 筹码结构(?:（辅助项）)? \| .*?\n"
            r"\| 风险特征 \| (?P<risk>\d+)/100 \|",
            body,
            re.S,
        )
        if not dim_match:
            continue
        payload[match.group("name")] = {
            "technical": dim_match.group("technical"),
            "fundamental": dim_match.group("fundamental"),
            "catalyst": dim_match.group("catalyst"),
            "relative_strength": dim_match.group("relative_strength"),
            "risk": dim_match.group("risk"),
            "label": match.group("label").strip(),
        }
    return payload


def check_stock_pick_client_report(
    client_text: str,
    source_text: str,
    *,
    editor_theme_playbook: Mapping[str, Any] | None = None,
    editor_prompt_text: str = "",
    event_digest_contract: Mapping[str, Any] | None = None,
    what_changed_contract: Mapping[str, Any] | None = None,
) -> List[str]:
    findings: List[str] = []

    for phrase in BANNED_CLIENT_PHRASES:
        if phrase in client_text:
            findings.append(format_lesson_finding("L001", f"[P1] 客户稿出现内部过程词: {phrase}"))

    if client_text.count("为什么") < 3:
        findings.append(format_lesson_finding("L002", "[P2] 客户稿解释性不足：'为什么' 类型说明明显不够"))
    if "数据完整度" not in client_text:
        findings.append(format_lesson_finding("L013", "[P1] 个股成稿缺少数据完整度/覆盖率说明"))
    if "当前置信度" in client_text:
        findings.append(format_lesson_finding("L023", "[P1] 个股成稿仍把样本置信度写成“当前置信度”，容易被误读成总推荐置信度"))
    if "估值偏高或财务安全边际不足" in client_text:
        findings.append(format_lesson_finding("L025", "[P2] 个股成稿仍使用“估值偏高或财务安全边际不足”模板句，未拆开真实原因"))
    if "结构化事件覆盖" in client_text and "分母" not in client_text:
        findings.append(format_lesson_finding("L024", "[P2] 个股成稿披露了覆盖率，但没有说明分母定义"))
    for line in client_text.splitlines():
        if "北向增持估计" in line and all(token not in line for token in ("行业", "板块", "代理")):
            findings.append(format_lesson_finding("L012", "[P1] 个股成稿把板块/行业北向代理写成了像个股专属信号"))
            break
    if "催化证据来源" not in client_text:
        findings.append(format_lesson_finding("L014", "[P1] 个股成稿缺少可直接复核的催化证据来源"))
    if "历史相似样本" not in client_text:
        findings.append(format_lesson_finding("L017", "[P1] 个股成稿缺少历史相似样本/置信度章节"))
    else:
        if "非重叠样本" not in client_text:
            findings.append(format_lesson_finding("L030", "[P1] 个股成稿引用了历史相似样本，但没有说明严格去重后的非重叠样本数"))
        if "95%区间" not in client_text:
            findings.append(format_lesson_finding("L030", "[P2] 个股成稿引用了历史相似样本，但没有展示胜率置信区间"))
        if "样本质量" not in client_text:
            findings.append(format_lesson_finding("L030", "[P2] 个股成稿引用了历史相似样本，但没有展示样本质量判断"))
    findings.extend(_observe_only_packaging_findings(client_text, "stock_pick"))
    findings.extend(_stock_pick_observe_density_findings(client_text))
    findings.extend(_stock_pick_feature_retention_findings(client_text))
    findings.extend(_stock_pick_first_screen_execution_findings(client_text))
    findings.extend(_duplicate_explanation_findings(client_text, max_repeat=2))
    findings.extend(_duplicate_operation_findings(client_text, max_repeat=2, scope="客户稿"))
    findings.extend(_duplicate_operation_findings(source_text, max_repeat=2, scope="详细稿"))
    findings.extend(_regime_basis_findings(client_text, "stock_pick"))
    findings.extend(_evidence_quality_findings(client_text, "stock_pick"))
    findings.extend(_top_signal_readability_findings(client_text, "stock_pick"))
    findings.extend(_readability_density_findings(client_text, "stock_pick"))
    findings.extend(_stock_section_structure_findings(client_text))
    findings.extend(_stock_section_identity_findings(client_text, source_text))
    findings.extend(_theme_playbook_surface_findings(client_text, "stock_pick", editor_theme_playbook))
    findings.extend(_editor_prompt_theme_findings(client_text, "stock_pick", editor_prompt_text))
    findings.extend(_event_digest_surface_findings(client_text, "stock_pick", event_digest_contract))
    findings.extend(_what_changed_surface_findings(client_text, "stock_pick", what_changed_contract))
    if len(_explanation_bullets(client_text)) < 8:
        findings.append(format_lesson_finding("L002", "[P2] 客户稿解释性不足：实质性解释条目太少"))
    findings.extend(_intraday_claim_findings(client_text))

    source_map = _source_stock_dimensions(source_text)
    if not source_map:
        findings.append("[P1] 详细稿未解析出 A股 八维表，无法做发布前一致性校验")
        return findings

    client_table = _client_stock_table(client_text, "## A股")
    if client_table:
        for name, client_row in client_table.items():
            if name not in source_map:
                findings.append(f"[P1] 客户稿中的标的在详细稿里不存在: {name}")
                continue
            source_row = source_map[name]
            for key, label in (
                ("technical", "技术"),
                ("fundamental", "基本面"),
                ("catalyst", "催化"),
                ("relative_strength", "相对强弱"),
                ("risk", "风险"),
            ):
                if str(client_row[key]) != str(source_row[key]):
                    findings.append(
                        f"[P1] 客户稿与详细稿分数不一致: {name} {label} client={client_row[key]} source={source_row[key]}"
                    )
        return findings

    client_detail_map = _source_stock_dimensions(client_text)
    if not client_detail_map:
        findings.append("[P1] 客户稿既没有 A股 汇总表，也没有可解析的详细八维表，无法做发布前一致性校验")
        return findings

    for name, client_row in client_detail_map.items():
        if name not in source_map:
            findings.append(f"[P1] 客户详细稿中的标的在内部详细稿里不存在: {name}")
            continue
        source_row = source_map[name]
        for key, label in (
            ("technical", "技术"),
            ("fundamental", "基本面"),
            ("catalyst", "催化"),
            ("relative_strength", "相对强弱"),
            ("risk", "风险"),
        ):
            if str(client_row[key]) != str(source_row[key]):
                findings.append(
                    f"[P1] 客户详细稿与内部详细稿分数不一致: {name} {label} client={client_row[key]} source={source_row[key]}"
                )
    return findings


def check_generic_client_report(
    client_text: str,
    report_type: str,
    source_text: str = "",
    *,
    editor_theme_playbook: Mapping[str, Any] | None = None,
    editor_prompt_text: str = "",
    event_digest_contract: Mapping[str, Any] | None = None,
    what_changed_contract: Mapping[str, Any] | None = None,
) -> List[str]:
    findings: List[str] = []
    for phrase in BANNED_CLIENT_PHRASES:
        if phrase in client_text:
            findings.append(format_lesson_finding("L001", f"[P1] 客户稿出现内部过程词: {phrase}"))
    for token in RAW_EXCEPTION_PATTERNS:
        if token in client_text:
            findings.append(format_lesson_finding("L029", f"[P1] 客户稿暴露了原始异常/系统报错信息: {token}"))

    minimum_why = {
        "briefing": 1,
        "fund_pick": 2,
        "etf_pick": 2,
        "scan": 1,
        "stock_analysis": 1,
        "retrospect": 1,
        "strategy": 0,
    }.get(report_type, 1)
    if client_text.count("为什么") < minimum_why:
        findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿解释性不足：缺少足够的“为什么”说明"))
    findings.extend(_duplicate_explanation_findings(client_text, max_repeat=2))
    findings.extend(_intraday_claim_findings(client_text))
    findings.extend(_execution_safety_findings(client_text, report_type))
    findings.extend(_evidence_quality_findings(client_text, report_type))
    findings.extend(_external_evidence_vs_packaging_findings(client_text, report_type))
    findings.extend(_peer_etf_evidence_findings(client_text, report_type))
    findings.extend(_top_signal_quality_findings(client_text, report_type))
    findings.extend(_top_signal_readability_findings(client_text, report_type))
    findings.extend(_readability_density_findings(client_text, report_type))
    findings.extend(_observe_execution_card_findings(client_text, report_type))
    findings.extend(_regime_basis_findings(client_text, report_type))
    findings.extend(_homepage_v2_findings(client_text, report_type))
    findings.extend(_homepage_decision_layer_findings(client_text, report_type))
    findings.extend(_theme_playbook_surface_findings(client_text, report_type, editor_theme_playbook))
    findings.extend(_editor_prompt_theme_findings(client_text, report_type, editor_prompt_text))
    findings.extend(_event_digest_surface_findings(client_text, report_type, event_digest_contract))
    findings.extend(_what_changed_surface_findings(client_text, report_type, what_changed_contract))

    scan_observe_only = (
        report_type == "scan"
        and ("当前更适合按 `观察为主`" in client_text or "观察名单里" in client_text or "当前建议仍是 `回避`" in client_text)
    )

    required_headings = {
        "briefing": [("## 市场结构摘要", "## 执行摘要"), "## 为什么今天这么判断", "## 宏观判断依据", "## 宏观领先指标", "## 数据完整度", "## 证据时点与来源", ("## 怎么用这份晨报", "## 执行补充", "## 今天怎么做"), "## 重点观察", "## 今日A股观察池", "## A股观察池升级条件"],
        "fund_pick": ["## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只基金为什么是这个分", "## 标准化分类"],
        "etf_pick": ["## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只ETF为什么是这个分", "## 标准化分类", "## 关键证据"],
        "scan": ["## 为什么这么判断"] if scan_observe_only else ["## 为什么这么判断", "## 当前更合适的动作"],
        "stock_analysis": ["## 为什么这么判断", "## 当前更合适的动作"],
        "retrospect": ["## 原始决策", "## 为什么当时会做这个决定", "## 后验路径", "## 复盘结论"],
        "strategy": ["## 动作卡片", "## 当前结论", "## 这套策略是什么", "## 这次到底看出来什么", "## 执行摘要"],
    }.get(report_type, [])
    for heading in required_headings:
        if isinstance(heading, tuple):
            if not any(option in client_text for option in heading):
                findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿缺少解释性章节: {' / '.join(heading)}"))
            continue
        if heading not in client_text:
            findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿缺少解释性章节: {heading}"))

    if report_type == "briefing":
        summary_items = _section_items_any(client_text, ("## 市场结构摘要", "## 执行摘要"))
        has_summary_table = (
            all(token in client_text for token in ("| 当前判断 |", "| 优先动作 |", "| 中期背景 / 当天主线 |"))
            or all(token in client_text for token in ("| 看不看 |", "| 怎么触发 |", "| 中期背景 / 当天主线 |"))
        )
        if len(summary_items) < 3 and not has_summary_table:
            findings.append(format_lesson_finding("L002", "[P2] briefing 客户稿缺少高密度执行摘要：至少要先交代判断、动作和背景。"))
        if len(_bullets_in_section(client_text, "## 为什么今天这么判断")) < 3:
            findings.append(format_lesson_finding("L002", "[P2] briefing 客户稿解释性不足：'为什么今天这么判断' 至少需要 3 条理由"))
        if len(_bullets_in_section(client_text, "## 宏观判断依据")) < 2:
            findings.append(format_lesson_finding("L027", "[P2] briefing 宏观判断依据不足：至少要交代 2 条 regime 依据。"))
        if len(_bullets_in_section(client_text, "## 数据完整度")) < 2:
            findings.append(format_lesson_finding("L013", "[P2] briefing 客户稿缺少“数据完整度”说明：至少要交代覆盖和缺失/代理口径。"))
        if len(_bullets_in_section(client_text, "## 重点观察")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] briefing 客户稿解释性不足：'重点观察' 至少需要 2 条可执行观察点"))
        a_share_items = _section_items(client_text, "## 今日A股观察池")
        if len(a_share_items) < 2:
            findings.append(format_lesson_finding("L013", "[P2] briefing 客户稿缺少“A股全市场观察池”说明：至少要交代全市场/初筛池与完整分析口径。"))
        elif not any(token in " ".join(a_share_items) for token in ("全市场", "初筛池", "完整分析", "Tushare")):
            findings.append(format_lesson_finding("L013", "[P2] briefing A股观察池章节没有讲清全市场初筛口径。"))
        if "## 宏观领先指标" not in client_text:
            findings.append(format_lesson_finding("L027", "[P2] briefing 客户稿缺少“宏观领先指标”章节，未来 3-6 个月判断不够透明"))
        elif len(_bullets_in_section(client_text, "## 宏观领先指标")) < 3:
            findings.append(format_lesson_finding("L027", "[P2] briefing 宏观领先指标解释不足：至少要讲清景气、价格链条和信用脉冲中的 3 条。"))
        completeness_items = _section_items(client_text, "## 数据完整度")
        if completeness_items and not any(token in " ".join(completeness_items) for token in ("覆盖", "缺失", "代理")):
            findings.append(format_lesson_finding("L013", "[P2] briefing 数据完整度章节没有讲清覆盖、缺失或代理口径。"))
        evidence_items = _section_items(client_text, "## 证据时点与来源")
        evidence_text = " ".join(evidence_items) if evidence_items else client_text
        if len(evidence_items) < 2 and not all(token in client_text for token in ("分析生成时间", "时点边界", "A股观察池来源")):
            findings.append(format_lesson_finding("L013", "[P2] briefing 证据时点与来源不足：至少要交代生成时间、观察池来源和时点边界。"))
        elif not all(token in evidence_text for token in ("分析生成时间", "时点边界", "A股观察池来源")):
            findings.append(format_lesson_finding("L013", "[P2] briefing 证据时点与来源不足：至少要交代生成时间、观察池来源和时点边界。"))
        if len(_bullets_in_section(client_text, "## A股观察池升级条件")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] briefing A股观察池升级条件不足：至少要说明为什么还不升级，以及要等什么触发器。"))
        if "直接催化：" not in client_text or "信息环境：" not in client_text:
            findings.append(format_lesson_finding("L040", "[P2] briefing 的主题跟踪还没拆成“直接催化 / 信息环境”，容易把热度和催化混在一起。"))
    elif report_type == "fund_pick":
        why_heading = _pick_reason_heading(report_type, client_text)
        if len(_bullets_in_section(client_text, why_heading)) < 3:
            findings.append(format_lesson_finding("L002", f"[P2] fund_pick 客户稿解释性不足：'{why_heading.replace('## ', '')}' 至少需要 3 条理由"))
        if len(_section_items(client_text, "## 为什么不是另外几只")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] fund_pick 客户稿解释性不足：'为什么不是另外几只' 需要至少给出备选原因或候选不足说明"))
        if "覆盖率" in client_text and "分母" not in client_text:
            findings.append(format_lesson_finding("L024", "[P2] fund_pick 披露了覆盖率，但没有说明分母定义"))
        findings.extend(_delivery_tier_findings(client_text, source_text, report_type))
        findings.extend(_pick_delivery_consistency_findings(client_text, report_type))
        findings.extend(_observe_only_packaging_findings(client_text, report_type))
        findings.extend(_standard_taxonomy_findings(client_text, report_type))
        findings.extend(_pick_auxiliary_score_findings(client_text, report_type))
        findings.extend(_pick_lead_density_findings(client_text, report_type))
        findings.extend(_fund_holdings_readability_findings(client_text, report_type))
        findings.extend(_absolute_asset_path_findings(client_text, report_type))
    elif report_type == "etf_pick":
        why_heading = _pick_reason_heading(report_type, client_text)
        if len(_bullets_in_section(client_text, why_heading)) < 3:
            findings.append(format_lesson_finding("L002", f"[P2] etf_pick 客户稿解释性不足：'{why_heading.replace('## ', '')}' 至少需要 3 条理由"))
        if len(_section_items(client_text, "## 为什么不是另外几只")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] etf_pick 客户稿解释性不足：'为什么不是另外几只' 需要至少给出备选原因或候选不足说明"))
        if "覆盖率" in client_text and "分母" not in client_text:
            findings.append(format_lesson_finding("L024", "[P2] etf_pick 披露了覆盖率，但没有说明分母定义"))
        findings.extend(_delivery_tier_findings(client_text, source_text, report_type))
        findings.extend(_pick_delivery_consistency_findings(client_text, report_type))
        findings.extend(_observe_only_packaging_findings(client_text, report_type))
        findings.extend(_preferred_sector_track_findings(client_text, report_type))
        findings.extend(_standard_taxonomy_findings(client_text, report_type))
        findings.extend(_fund_profile_findings(client_text))
        findings.extend(_pick_auxiliary_score_findings(client_text, report_type))
        findings.extend(_pick_lead_density_findings(client_text, report_type))
        findings.extend(_fund_holdings_readability_findings(client_text, report_type))
        findings.extend(_absolute_asset_path_findings(client_text, report_type))
    elif report_type == "scan":
        if len(_bullets_in_section(client_text, "## 值得继续看的地方")) < 1:
            findings.append(format_lesson_finding("L002", "[P2] scan 客户稿缺少正向理由：'值得继续看的地方' 至少要有 1 条"))
        if len(_bullets_in_section(client_text, "## 现在不适合激进的地方")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] scan 客户稿缺少反向理由：'现在不适合激进的地方' 至少要有 2 条"))
        findings.extend(_fund_profile_findings(client_text))
        findings.extend(_fund_holdings_readability_findings(client_text, report_type))
        findings.extend(_absolute_asset_path_findings(client_text, report_type))
    elif report_type == "stock_analysis":
        if len(_bullets_in_section(client_text, "## 值得继续看的地方")) < 1:
            findings.append(format_lesson_finding("L002", "[P2] stock_analysis 客户稿缺少正向理由：'值得继续看的地方' 至少要有 1 条"))
        if len(_bullets_in_section(client_text, "## 现在不适合激进的地方")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] stock_analysis 客户稿缺少反向理由：'现在不适合激进的地方' 至少要有 2 条"))
        if "## 历史相似样本验证" in client_text:
            if "非重叠样本" not in client_text:
                findings.append(format_lesson_finding("L030", "[P1] stock_analysis 引用了历史相似样本，但没有说明非重叠样本数"))
            if "95%区间" not in client_text:
                findings.append(format_lesson_finding("L030", "[P2] stock_analysis 引用了历史相似样本，但没有展示胜率置信区间"))
            if "样本质量" not in client_text:
                findings.append(format_lesson_finding("L030", "[P2] stock_analysis 引用了历史相似样本，但没有展示样本质量"))
    elif report_type == "retrospect":
        if client_text.count("### ") < 1:
            findings.append(format_lesson_finding("L002", "[P2] retrospect 客户稿至少要展开 1 笔具体决策。"))
        if len(_explanation_bullets(client_text)) < 6:
            findings.append(format_lesson_finding("L002", "[P2] retrospect 客户稿解释性不足：复盘理由和结论太少。"))
    elif report_type == "strategy":
        strategy_kind = _strategy_report_kind(client_text)
        internal_terms = ("已回写账本", "production ready", "生产链路", "live baseline")
        for term in internal_terms:
            if term in client_text:
                findings.append(format_lesson_finding("L001", f"[P1] strategy 客户稿仍暴露内部流程词: {term}"))
        if "| 项目 | 结论 |" not in client_text:
            findings.append(format_lesson_finding("L002", "[P2] strategy 客户稿缺少结构化执行摘要表：应先把当前判断、主要问题和下一步前置写清。"))
        if "当前动作：" not in client_text:
            findings.append(format_lesson_finding("L002", "[P2] strategy 客户稿缺少动作卡片里的“当前动作”句，首屏还不够像可读成稿。"))
        if strategy_kind == "validation":
            for heading in ("## 动作卡片", "## 当前结论", "## 这套策略是什么", "## 这次到底看出来什么", "## 总体结果", "## Rollback Gate"):
                if heading not in client_text:
                    findings.append(format_lesson_finding("L002", f"[P2] strategy validate 客户稿缺少解释性章节: {heading}"))
            if "hit rate:" not in client_text or "平均超额收益" not in client_text:
                findings.append(format_lesson_finding("L002", "[P2] strategy validate 客户稿没有把 hit rate 和平均超额收益前置写清。"))
            if not any(token in client_text for token in ("## Out-Of-Sample Validation", "## Chronological Cohorts", "## Cross-Sectional Validation")):
                findings.append(format_lesson_finding("L002", "[P2] strategy validate 客户稿缺少样本稳定性章节：至少要交代 OOS / cohort / cross-sectional 之一。"))
        elif strategy_kind == "experiment":
            for heading in ("## 动作卡片", "## 当前结论", "## 这套策略是什么", "## 这次到底看出来什么", "## Promotion Gate", "## Rollback Gate", "## 变体对比"):
                if heading not in client_text:
                    findings.append(format_lesson_finding("L002", f"[P2] strategy experiment 客户稿缺少解释性章节: {heading}"))
            if "| variant |" not in client_text:
                findings.append(format_lesson_finding("L002", "[P2] strategy experiment 客户稿缺少结构化 variant 对比表。"))
            if "当前 champion:" not in client_text or "baseline:" not in client_text:
                findings.append(format_lesson_finding("L002", "[P2] strategy experiment 客户稿没有把 baseline / champion / challenger 关系前置写清。"))
        else:
            findings.append(format_lesson_finding("L002", "[P1] strategy 客户稿没有明确是 validate 还是 experiment 成稿；当前正式交付只支持这两类。"))

    if any(token in client_text for token in ("3-6个月", "未来3-6个月", "未来 3-6 个月", "中期判断")):
        macro_tokens = ("PMI", "PPI", "CPI", "社融", "M1-M2", "剪刀差")
        hits = sum(1 for token in macro_tokens if token in client_text)
        if hits < 3:
            findings.append(format_lesson_finding("L027", "[P2] 报告使用了中期宏观判断语气，但没有把 PMI/PPI/CPI/信用脉冲 的角色讲清楚。"))
    if source_text:
        if report_type in {"scan", "stock_analysis"}:
            findings.extend(_analysis_source_consistency_findings(client_text, source_text, report_type))
            findings.extend(_source_feature_retention_findings(client_text, source_text, report_type))
        elif report_type == "fund_pick":
            findings.extend(_pick_source_consistency_findings(client_text, source_text, report_type, "## 这只基金为什么是这个分"))
        elif report_type == "etf_pick":
            findings.extend(_pick_source_consistency_findings(client_text, source_text, report_type, "## 这只ETF为什么是这个分"))
            findings.extend(_pick_fund_profile_feature_retention_findings(client_text, source_text, report_type))
        elif report_type == "briefing":
            findings.extend(_briefing_source_consistency_findings(client_text, source_text))
        elif report_type == "retrospect":
            findings.extend(_retrospect_source_consistency_findings(client_text, source_text))
    return findings


def _intraday_claim_findings(text: str) -> List[str]:
    findings: List[str] = []
    normalized = re.sub(r"^\|\s*盘中快照 as_of\s*\|.*$", "", text, flags=re.M)
    normalized = normalized.replace("盘中快照成稿", "")
    normalized = normalized.replace("盘中快照", "")
    normalized = normalized.replace("盘中实时/缓存快照", "")
    normalized = normalized.replace("盘中实时快照", "")
    normalized = re.sub(r"^.*已接入龙虎榜/竞价/涨跌停边界.*$", "", normalized, flags=re.M)
    normalized = re.sub(r"^.*未命中明确龙虎榜/打板确认.*$", "", normalized, flags=re.M)
    normalized = re.sub(r"^.*当前未见明确打板过热风险.*$", "", normalized, flags=re.M)
    normalized = re.sub(r"^.*打板专题接口当前不可用.*$", "", normalized, flags=re.M)
    risky_opening_patterns = (
        r"开盘.{0,8}(做|买|追|加仓|执行|跟随|介入)",
        r"明天开盘.{0,8}(做|买|追|加仓|执行|跟随|介入)",
    )
    has_intraday_claim = any(term in normalized for term in INTRADAY_CLAIM_TERMS) or any(
        re.search(pattern, normalized) for pattern in risky_opening_patterns
    )
    if has_intraday_claim and not any(term in normalized for term in INTRADAY_EVIDENCE_TERMS):
        findings.append(format_lesson_finding("L004", "[P1] 报告使用盘中/开盘执行语言，但没有展示对应盘中因子或数据依据（如 VWAP、相对今开、日内位置、开盘缺口、首30分钟）。"))
    if "集合竞价" in normalized and not any(term in normalized for term in AUCTION_EVIDENCE_TERMS):
        findings.append(format_lesson_finding("L004", "[P2] 报告提到集合竞价，但没有对应竞价因子依据；当前不应把普通日线结论写成竞价判断。"))
    return findings


def _table_value(text: str, label: str) -> str:
    match = re.search(rf"\|\s*{re.escape(label)}\s*\|\s*([^|\n]+)\|", text)
    return match.group(1).strip() if match else ""


def _execution_safety_findings(text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    buy_range = _table_value(text, "建议买入区间")
    stop_text = _table_value(text, "止损参考")
    trim_range = _table_value(text, "建议减仓区间")
    if not buy_range or "暂不设" in buy_range or not stop_text:
        range_match = None
        stop_match = None
    else:
        range_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)", buy_range)
        stop_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", stop_text)
        if range_match and stop_match:
            buy_low = float(range_match.group(1))
            stop_ref = float(stop_match.group(1))
            if stop_ref >= buy_low:
                findings.append(format_lesson_finding("L036", f"[P1] {report_type} 的止损参考高于或等于买入区间下沿，执行参数自相矛盾。"))
                return findings
            if (buy_low - stop_ref) / buy_low < 0.01:
                findings.append(format_lesson_finding("L036", f"[P1] {report_type} 的买入区间下沿离止损过近（<1%），实操中容易被正常波动洗掉。"))
    if trim_range and report_type in {"etf_pick", "fund_pick", "scan"}:
        trim_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)", trim_range)
        if trim_match:
            trim_low = float(trim_match.group(1))
            trim_high = float(trim_match.group(2))
            if trim_low > 0 and (trim_high - trim_low) / trim_low > 0.05:
                findings.append(format_lesson_finding("L036", f"[P2] {report_type} 的建议减仓区间过宽（>5%），更像模型粗框，不像可执行分批计划。"))
    if report_type in {"etf_pick", "fund_pick", "scan", "stock_analysis"}:
        watch_text = " ".join(
            item
            for item in (
                _table_value(text, "怎么触发"),
                _table_value(text, "先看什么"),
            )
            if item
        )
        upper_match = re.search(r"上沿(?:先|再)看\s*`?([0-9]+(?:\.[0-9]+)?)`?", watch_text)
        pressure_values: List[float] = []
        for match in re.finditer(r"(?:高点|前高|压力)[^0-9]{0,12}([0-9]+(?:\.[0-9]+)?)", text):
            try:
                value = float(match.group(1))
            except (TypeError, ValueError):
                continue
            if value > 0:
                pressure_values.append(value)
        if upper_match and pressure_values:
            upper_ref = float(upper_match.group(1))
            nearest_pressure = min(pressure_values)
            if upper_ref > nearest_pressure * 1.03:
                findings.append(format_lesson_finding("L036", f"[P2] {report_type} 的首屏上沿 `{upper_ref:.3f}` 跳过了更近的压力位 `{nearest_pressure:.3f}`，执行位和强因子压力位不自洽。"))
    return findings


def _evidence_quality_findings(text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    evidence_items = [*_section_items(text, "## 关键证据"), *_section_items(text, "## 催化证据来源")]
    for item in evidence_items:
        lowered = str(item).lower()
        if any(token in lowered for token in GENERIC_EVIDENCE_TITLE_KEYS):
            findings.append(format_lesson_finding("L037", f"[P1] {report_type} 的关键证据混入了通用新闻/行情页，不像可直接支撑催化判断的有效证据: {item}"))
            break
    return findings


def _external_evidence_vs_packaging_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"etf_pick", "fund_pick", "scan"}:
        return []
    findings: List[str] = []
    low_direct_news = bool(re.search(r"高置信直接新闻覆盖\s*0%", text))
    homepage_items = _section_items_any(text, HOMEPAGE_KEY_EVIDENCE_HEADINGS)
    homepage_no_clickable = any(
        item.startswith("外部情报：") and "未拿到可点击外部情报" in item
        for item in homepage_items
    )
    if not (low_direct_news or homepage_no_clickable):
        return findings
    strong_packaging_markers = (
        "| 优先推荐 |",
        "| 交付等级 | 标准推荐稿",
        "## 为什么推荐它",
        "# 今日ETF推荐",
        "# 今日场外基金推荐",
    )
    if any(marker in text for marker in strong_packaging_markers):
        findings.append(
            format_lesson_finding(
                "L037",
                f"[P1] {report_type} 直连外部情报覆盖仍为 0 或首页没有可点击外部情报，却继续使用“优先推荐 / 标准推荐稿”包装；这类样本应先按观察/候选处理。",
            )
        )
    return findings


def _peer_etf_evidence_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"etf_pick", "scan"}:
        return []
    findings: List[str] = []
    evidence_items = [*_section_items(text, "## 关键证据"), *_section_items(text, "## 催化证据来源")]
    for item in evidence_items:
        if "ETF" not in item:
            continue
        if not any(token in item for token in ("净申购", "净赎回", "份额净创设", "份额净赎回")):
            continue
        if any(
            marker in item
            for marker in (
                "同赛道产品",
                "赛道热度",
                "板块热度",
                "行业热度",
                "赛道佐证",
                "份额申赎确认",
                "本ETF",
                "本 ETF",
                "本产品",
                "当前ETF",
                "当前 ETF",
                "当前产品",
            )
        ):
            continue
        findings.append(
            format_lesson_finding(
                "L037",
                f"[P2] {report_type} 把其他 ETF 的申购/份额变化直接当成核心证据，却没标明它只是赛道热度佐证，容易把同赛道产品信号误读成当前标的自身确认。",
            )
        )
        break
    return findings


def _top_signal_quality_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"briefing", "etf_pick", "fund_pick", "scan", "stock_analysis", "stock_pick"}:
        return []
    findings: List[str] = []
    items = _section_items_any(text, HOMEPAGE_KEY_EVIDENCE_HEADINGS)
    if not items:
        return findings

    linked_items = [item for item in items if "http://" in item or "https://" in item]
    signalful_items = [
        item for item in items if ("信号：" in item or "信号类型：" in item) and ("结论：" in item or "主要影响：" in item)
    ]
    market_only_items = [item for item in items if item.startswith(GENERIC_MARKET_SIGNAL_PREFIXES)]
    disclosed_no_external = any(
        item.startswith("外部情报：") and "未拿到可点击外部情报" in item
        for item in items
    )

    if report_type == "briefing" and not linked_items and not disclosed_no_external:
        findings.append(
            format_lesson_finding(
                "L037",
                "[P1] briefing 首页 `关键新闻 / 关键证据` 没有任何可点击外部情报，当前更像盘面摘要，不像可核验的晨报情报板。",
            )
        )
    if linked_items and not signalful_items:
        findings.append(
            format_lesson_finding(
                "L037",
                f"[P1] {report_type} 首页虽然前置了链接情报，但没有把 `信号/强弱/结论` 写清，仍然更像新闻堆砌而不是研究判断。",
            )
        )
    if market_only_items and len(market_only_items) == len(items):
        findings.append(
            format_lesson_finding(
                "L037",
                f"[P1] {report_type} 首页 `关键新闻 / 关键证据` 全被盘面句占满，当前是“盘面句顶替新闻位”，没有真正前置外部情报。",
            )
        )
    return findings


def _top_signal_readability_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"briefing", "etf_pick", "fund_pick", "scan", "stock_analysis", "stock_pick"}:
        return []
    findings: List[str] = []
    items = _section_items_any(text, HOMEPAGE_KEY_EVIDENCE_HEADINGS)
    if not items:
        return findings

    if any(any(prefix in item for prefix in RAW_INTEL_SUMMARY_PREFIXES) for item in items):
        findings.append(
            format_lesson_finding(
                "L037",
                f"[P1] {report_type} 首页 `关键新闻 / 关键证据` 仍暴露原始情报聚类口径（如 `主题聚类：...`），不像给客户看的可读摘要。",
            )
        )

    linked_items = [item for item in items if "http://" in item or "https://" in item]
    structured_items = [
        item
        for item in items
        if item.startswith("结构证据：")
        or (not item.startswith("外部情报：") and ("信号：" in item or "信号类型：" in item))
    ]
    if linked_items and structured_items:
        has_external_label = any(item.startswith("外部情报：") for item in linked_items)
        has_structured_label = any(item.startswith("结构证据：") for item in items)
        if not (has_external_label and has_structured_label):
            findings.append(
                format_lesson_finding(
                    "L037",
                    f"[P2] {report_type} 首页把外部情报和结构证据混写在一起，却没有显式标清“外部情报 / 结构证据”，读者很难判断哪条是新闻、哪条是结构判断。",
                )
            )
    return findings


def _readability_density_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"etf_pick", "fund_pick", "scan", "stock_analysis", "stock_pick"}:
        return []
    observe_only_markers = (
        "| 当前建议 | 观察为主",
        "| 当前建议 | 观察为主（偏回避）",
        "| 交付等级 | 观察稿",
        "| 交付等级 | 代理观察稿",
        "| 交付等级 | 降级观察稿",
        "| 报告定位 | 观察稿 |",
        "当前交付等级：观察稿",
        "当前交付等级：代理观察稿",
        "当前交付等级：降级观察稿",
        "当前交付等级：`观察稿`",
        "当前交付等级：`代理观察稿`",
        "当前交付等级：`降级观察稿`",
        "当前没有达到正式动作阈值的个股",
        "今天没有正式动作票",
    )
    if not any(marker in text for marker in observe_only_markers):
        return []
    hedge_total = sum(text.count(token) for token in READABILITY_HEDGE_TOKENS)
    disclosure_items = [
        *_section_items(text, "## 数据完整度"),
        *_section_items(text, "## 交付等级"),
        *_section_items(text, "## 数据限制与说明"),
    ]
    missing_like_items = [
        item
        for item in disclosure_items
        if any(token in item for token in MISSING_DISCLOSURE_TOKENS)
    ]
    if hedge_total >= 14 or (len(disclosure_items) >= 8 and len(missing_like_items) >= 6):
        return [
            format_lesson_finding(
                "L003",
                f"[P2] {report_type} 的边界声明/缺失披露过密，当前更像在反复防误读而不是给结论；缺失项应合并成少量脚注或清单，避免整篇信噪比过低。",
            )
        ]
    return []


def _observe_execution_card_findings(text: str, report_type: str) -> List[str]:
    if report_type not in {"etf_pick", "fund_pick", "scan", "stock_analysis"}:
        return []
    findings: List[str] = []
    observe_only_markers = (
        "| 交付等级 | 观察稿",
        "| 交付等级 | 代理观察稿",
        "| 交付等级 | 降级观察稿",
        "| 报告定位 | 观察稿 |",
        "当前交付等级：观察稿",
        "当前交付等级：代理观察稿",
        "当前交付等级：降级观察稿",
        "当前交付等级：`观察稿`",
        "当前交付等级：`代理观察稿`",
        "当前交付等级：`降级观察稿`",
    )
    if not any(marker in text for marker in observe_only_markers):
        return []
    precise_row_markers = (
        "| 首次仓位 |",
        "| 止损参考 |",
        "建议买入区间：",
        "第一减仓位：",
        "第二减仓位：",
    )
    current_action = _table_value(text, "当前动作")
    if current_action and any(marker in current_action for marker in ("回避", "暂不")) and "观察" not in current_action:
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 当前已经是观察稿，但动作卡仍把 `当前动作` 直接写成 `{current_action}`；观察稿应明确写成“观察为主（偏回避）”这类观察口径，而不是继续像执行指令。",
            )
        )
    if any(marker in text for marker in precise_row_markers):
        findings.append(
            format_lesson_finding(
                "L040",
                f"[P1] {report_type} 当前已经是观察稿，但正文仍保留精确仓位/止损/减仓位这类执行卡；观察稿应先收成触发条件和观察重点，不要继续给机械挂单位。",
            )
        )
    return findings


def _regime_basis_findings(text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    if report_type not in {"stock_pick", "etf_pick"}:
        return findings
    lowered = text.lower()
    if not any(token in lowered for token in REGIME_LABEL_TOKENS):
        return findings
    items = _section_items(text, "## 宏观判断依据")
    if len(items) < 2:
        findings.append(format_lesson_finding("L027", f"[P1] {report_type} 写了 macro regime，但没有单独解释这次为什么判断成该背景。"))
        return findings
    token_hits = sum(1 for token in REGIME_BASIS_TOKENS if token in " ".join(items))
    if token_hits < 2:
        findings.append(format_lesson_finding("L027", f"[P2] {report_type} 的 regime 判断依据不够具体，至少应交代 PMI/PPI/CPI/信用/美元 等驱动中的 2 项。"))
    return findings


def main() -> None:
    args = build_parser().parse_args()
    client_text = _read(args.client)
    source_text = _read(args.source) if args.source else ""
    editor_prompt_text = _read(args.editor_prompt) if args.editor_prompt else ""
    if args.report_type == "stock_pick":
        if not args.source:
            raise SystemExit("stock_pick 一致性校验必须提供 --source")
        findings = check_stock_pick_client_report(client_text, source_text, editor_prompt_text=editor_prompt_text)
    else:
        findings = check_generic_client_report(client_text, args.report_type, source_text=source_text, editor_prompt_text=editor_prompt_text)
    if findings:
        print("发布前一致性校验未通过：")
        for item in findings:
            print(f"- {item}")
        raise SystemExit(1)
    print("发布前一致性校验通过。")


if __name__ == "__main__":
    main()
