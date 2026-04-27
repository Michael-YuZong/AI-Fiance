"""Editor payload builders and thesis-first homepage renderers."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

from src.output.theme_playbook import (
    build_theme_playbook_context,
    infer_theme_trading_role,
    playbook_hint_line,
    sector_subtheme_bridge_items,
    subject_theme_label,
    summarize_sector_subtheme_bridge,
)
from src.output.pick_ranking import analysis_is_actionable, rank_market_items, strategy_confidence_status
from src.output.technical_signal_labels import compact_technical_signal_text
from src.output.event_digest import (
    _should_skip_instrument_proxy_news,
    build_event_digest,
    effective_intelligence_link,
    event_digest_action_line,
    event_digest_homepage_lines,
    format_intelligence_attributes,
    intelligence_attribute_labels,
)
from src.processors.horizon import build_horizon_expression_packet
from src.storage.strategy import StrategyRepository
from src.storage.thesis import ThesisRepository, build_thesis_state_transition, compare_event_digest_snapshots


REGIME_LABELS = {
    "recovery": "温和复苏",
    "overheating": "过热",
    "stagflation": "滞涨",
    "deflation": "偏弱/通缩",
}

DIMENSION_LABELS: Sequence[Tuple[str, str]] = (
    ("technical", "技术面"),
    ("fundamental", "基本面"),
    ("catalyst", "催化面"),
    ("relative_strength", "相对强弱"),
    ("risk", "风险特征"),
    ("macro", "宏观敏感度"),
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _markdown_link(label: str, link: str) -> str:
    text = _safe_text(label)
    url = _safe_text(link)
    if text and url:
        return f"[{text}]({url})"
    return text or url


def _google_news_search_link(title: Any, source: Any = "") -> str:
    query = " ".join(part for part in (_safe_text(title), _safe_text(source)) if part).strip()
    if not query:
        return ""
    return f"https://news.google.com/search?q={quote(query)}"


def _source_directness_label(row: Mapping[str, Any], *, theme_level: bool = False) -> str:
    tags = _intelligence_tags(row, theme_level=theme_level)
    if "一手直连" in tags:
        return "一手直连"
    if "媒体直连" in tags:
        return "媒体直连"
    if theme_level and "主题级情报" in tags:
        return "主题级情报"
    return ""


def _freshness_label(row: Mapping[str, Any], *, as_of: Any = None) -> str:
    tags = intelligence_attribute_labels(row, as_of=as_of)
    if "新鲜情报" in tags:
        return "新鲜情报"
    if "旧闻回放" in tags:
        return "旧闻回放"
    return ""


def _intelligence_tags(
    row: Mapping[str, Any],
    *,
    as_of: Any = None,
    theme_level: bool = False,
    previous_reviewed_at: Any = None,
) -> List[str]:
    tags = intelligence_attribute_labels(row, as_of=as_of, previous_reviewed_at=previous_reviewed_at)
    if theme_level and "主题级情报" not in tags:
        tags.append("主题级情报")
    return tags


def _event_digest_signal_line(event_digest: Mapping[str, Any]) -> str:
    digest = dict(event_digest or {})
    signal_type = _safe_text(digest.get("signal_type")) or _safe_text(digest.get("lead_detail")) or _safe_text(digest.get("lead_layer"))
    signal_strength = _safe_text(digest.get("signal_strength")) or "中"
    signal_conclusion = _safe_text(digest.get("signal_conclusion")) or _safe_text(digest.get("conclusion"))
    latest_signal_at = _safe_text(digest.get("latest_signal_at"))
    if signal_type and signal_conclusion:
        line = f"信号类型：`{signal_type}`；信号强弱：`{signal_strength}`；结论：{signal_conclusion}"
        if latest_signal_at:
            line += f"；最新情报时点：`{latest_signal_at}`"
        return line
    if not latest_signal_at:
        return ""
    return f"最新情报时点：`{latest_signal_at}`。"


def _event_digest_history_line(event_digest: Mapping[str, Any]) -> str:
    digest = dict(event_digest or {})
    history_note = _safe_text(digest.get("history_note"))
    if history_note:
        return history_note
    previous_reviewed_at = _safe_text(digest.get("previous_reviewed_at"))
    if previous_reviewed_at:
        return f"上次复查时间：`{previous_reviewed_at}`。"
    return ""


def _append_unique_line(lines: List[str], line: str, *, limit: int | None = None) -> None:
    text = _safe_text(line)
    if not text:
        return
    if text in lines:
        return
    if limit is not None and len(lines) >= limit:
        return
    lines.append(text)


def _homepage_emphasis(text: Any) -> str:
    line = _safe_text(text)
    if not line:
        return ""
    return re.sub(r"`([^`]+)`", lambda match: f"**{match.group(1)}**", line)


def _homepage_signal_conclusion(signal_type: str, impact: str = "") -> str:
    signal = _safe_text(signal_type)
    target = _safe_text(impact) or "相关方向"
    if signal in {"主线增强", "行业催化", "主线活跃", "板块活跃"}:
        return f"偏利多，先看 `{target}` 能否从局部走向扩散。"
    if signal in {"龙头确认", "热度抬升", "观察池前排"}:
        return f"偏利多，但先按 `{target}` 的跟涨/扩散确认处理。"
    if signal in {"医药催化", "AI应用催化", "AI硬件催化"}:
        return f"偏利多，先看 `{target}` 能否继续拿到价格与成交确认。"
    if signal in {"政策催化", "电网投资催化"}:
        return f"偏利多，先看 `{target}` 能否从政策/招标线索落到订单、盈利或价格承接。"
    if signal.startswith("财报摘要"):
        return f"中性偏事件驱动，先等 `{target}` 的实际披露结果验证，不把日历本身写成超预期。"
    if signal in {"资金承接", "卖方共识"}:
        return f"中性偏多，先看 `{target}` 是否继续获得资金和预期差确认。"
    if signal == "地缘缓和":
        return "偏利多风险偏好，先看黄金/原油回落与成长弹性修复。"
    if signal in {"地缘扰动", "避险交易", "能源冲击"}:
        return "偏利空风险偏好，先看黄金、防守和能源资产是否继续走强。"
    if signal == "利率预期":
        return "偏利多成长估值，但仍要等价格共振，不把标题直接当成动作信号。"
    if not signal:
        return ""
    return f"中性偏观察，先把它当 `{target}` 的辅助线索。"


def _homepage_news_direction(conclusion: Any) -> str:
    text = _safe_text(conclusion)
    if "偏利空" in text or "利空" in text:
        return "偏利空"
    if "偏利多" in text or "利多" in text or "中性偏多" in text:
        return "偏利多"
    if "待复核" in text:
        return "待复核"
    return "中性"


def _infer_homepage_news_signal(title: Any, category: Any = "") -> Tuple[str, str]:
    raw_text = f"{_safe_text(title)} {_safe_text(category)}"
    raw_text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_text)
    raw_text = re.sub(r"https?://\S+", "", raw_text)
    blob = raw_text.lower()
    geo_blocker = any(
        token in blob
        for token in ("陷入僵局", "僵局", "遭袭", "遇袭", "袭击", "空袭", "受损", "受创", "紧张升级", "冲突升级")
    )
    if any(token in blob for token in ("停火", "休战", "缓和", "结束战争", "ceasefire", "truce", "de-escalat")) and not geo_blocker:
        return "地缘缓和", "黄金/原油/风险偏好"
    if geo_blocker or any(token in blob for token in ("伊朗", "以色列", "中东", "war", "strike", "missile", "conflict")):
        return "地缘扰动", "黄金/原油/风险偏好"
    if any(token in blob for token in ("创新药", "医药", "制药", "药业", "cxo", "fda", "临床", "license-out", "bd", "授权")):
        return "医药催化", "创新药/医药"
    if any(token in blob for token in ("国家电网", "南方电网", "特高压", "输变电", "配网", "变压器", "电力设备", "电网招标")):
        return "电网投资催化", "电网/特高压"
    if any(token in blob for token in ("ai硬件", "硬科技", "新易盛", "中际旭创", "华工科技", "cpo", "光模块", "算力", "服务器", "液冷", "hbm", "semiconductor", "芯片", "半导体", "nvidia", "nvda", "6g")):
        return "AI硬件催化", "AI硬件链"
    if any(token in blob for token in ("智谱", "kimi", "deepseek", "大模型", "模型", "agent", "应用")):
        return "AI应用催化", "AI软件/应用"
    if any(token in blob for token in ("业绩", "财报", "年报", "一季报", "季报", "盈利", "指引")):
        return "财报摘要：盈利/指引", "盈利/估值"
    if any(token in blob for token in ("政策", "国务院", "部署", "支持", "规划", "招标")):
        return "政策催化", "政策预期/景气"
    if any(token in blob for token in ("主力资金", "净买入", "资金流", "融资", "北向")):
        return "资金承接", "资金偏好"
    if any(token in blob for token in ("金股", "券商", "评级", "研报", "目标价")):
        return "卖方共识", "资金偏好/预期差"
    if any(token in blob for token in ("黄金", "gold", "贵金属")):
        return "避险交易", "黄金/防守"
    if any(token in blob for token in ("原油", "oil", "opec")):
        return "能源冲击", "原油/能源"
    if any(token in blob for token in ("债券", "bond", "fed", "rate", "yield", "利率")):
        return "利率预期", "成长估值/风险偏好"
    return "信息环境：新闻/舆情脉冲", "估值/资金偏好"


def _subject_news_impact_target(subject: Mapping[str, Any], row: Mapping[str, Any] | None = None, fallback: Any = "") -> str:
    payload = dict(subject or {})
    raw = dict(row or {})
    explicit = (
        _safe_text(raw.get("impact_summary"))
        or _safe_text(raw.get("impact"))
        or _safe_text(raw.get("main_impact"))
        or _safe_text(raw.get("target"))
        or _safe_text(raw.get("signal_target"))
        or _safe_text(fallback)
    )
    if explicit and explicit not in {"相关方向", "观察池核心资产"}:
        return explicit
    metadata = dict(payload.get("metadata") or {})
    taxonomy = dict(metadata.get("taxonomy") or metadata.get("fund_taxonomy") or {})
    theme_profile = dict(metadata.get("theme_profile") or taxonomy.get("theme_profile") or {})
    fund_profile = dict(payload.get("fund_profile") or {})
    overview = dict(fund_profile.get("overview") or {})
    etf_info = dict(fund_profile.get("etf_info") or {})
    day_theme = dict(payload.get("day_theme") or {}).get("label") if isinstance(payload.get("day_theme"), Mapping) else payload.get("day_theme")
    candidates = (
        metadata.get("primary_chain"),
        theme_profile.get("primary_chain"),
        metadata.get("theme_role"),
        theme_profile.get("theme_role"),
        metadata.get("tracked_index_name"),
        etf_info.get("跟踪指数"),
        overview.get("业绩比较基准"),
        metadata.get("industry_framework_label"),
        metadata.get("industry"),
        metadata.get("sector"),
        dict(payload.get("theme_playbook") or {}).get("label"),
        day_theme,
        payload.get("name"),
    )
    for item in candidates:
        text = _safe_text(item)
        if text and text not in {"—", "未指定", "综合/其他"}:
            return text
    return explicit or "相关方向"


def _homepage_news_transmission_text(
    signal_type: Any,
    impact: Any,
    subject: Mapping[str, Any] | None = None,
    conclusion: Any = "",
) -> str:
    signal = _safe_text(signal_type)
    target = _safe_text(impact) or _subject_news_impact_target(dict(subject or {}))
    direction = _homepage_news_direction(conclusion)
    asset_type = _safe_text(dict(subject or {}).get("asset_type"))
    if "地缘" in signal or "避险" in signal or "能源冲击" in signal:
        return f"{direction}，先影响 `{target}`，再看黄金/原油、成长和防守资产的相对强弱是否验证。"
    if asset_type in {"cn_etf", "cn_fund", "cn_index"}:
        return f"{direction}，先影响 `{target}` 的主题预期，再看跟踪指数、核心成分和份额/价格是否同向确认。"
    if asset_type in {"cn_stock", "hk", "us"}:
        return f"{direction}，先影响 `{target}` 的景气或资金偏好，再看订单、财报、价格和成交能否验证。"
    if asset_type == "market_briefing":
        return f"{direction}，先影响 `{target}` 的风险偏好，再看成交、宽度和主线扩散是否验证。"
    return f"{direction}，先影响 `{target}`，再看价格、成交和后续事件是否验证。"


def _append_news_interpretation(
    line: Any,
    *,
    subject: Mapping[str, Any] | None = None,
    signal_type: Any = "",
    signal_strength: Any = "",
    impact: Any = "",
    conclusion: Any = "",
) -> str:
    text = _safe_text(line)
    if not text:
        return ""
    row: Dict[str, Any] = {}
    signal = _safe_text(signal_type)
    strength = _safe_text(signal_strength)
    target = _safe_text(impact)
    if target in {"相关方向", "观察池核心资产"}:
        target = ""
    if not signal:
        signal_match = re.search(r"信号(?:类型)?：[`*]*([^`*；）)]+)[`*]*", text)
        if signal_match:
            signal = _safe_text(signal_match.group(1))
    if not strength:
        strength_match = re.search(r"(?:信号强弱|强弱)：[`*]*([^`*；）)]+)[`*]*", text)
        if strength_match:
            strength = _safe_text(strength_match.group(1))
    inferred_signal, inferred_target = ("", "")
    if not signal or not target or signal in {"信息环境：新闻/舆情脉冲", "主题/市场情报"}:
        inferred_signal, inferred_target = _infer_homepage_news_signal(text)
    if inferred_signal and (
        not signal
        or (
            signal in {"信息环境：新闻/舆情脉冲", "主题/市场情报"}
            and inferred_signal != "信息环境：新闻/舆情脉冲"
        )
    ):
        signal = inferred_signal
    if not target and inferred_target and (
        inferred_signal != "信息环境：新闻/舆情脉冲"
        or signal in {"信息环境：新闻/舆情脉冲", "主题/市场情报"}
    ):
        target = inferred_target
    if not target:
        target = _subject_news_impact_target(dict(subject or {}), row, impact)
    if not strength:
        strength = "中"
    conclusion_text = _safe_text(conclusion)
    if not conclusion_text:
        conclusion_match = re.search(r"结论：([^；）)]+)", text)
        if conclusion_match:
            conclusion_text = _safe_text(conclusion_match.group(1))
    if not conclusion_text:
        conclusion_text = _homepage_signal_conclusion(signal, target)
    additions: List[str] = []
    if "信号类型：" not in text and "信号：" not in text and signal:
        additions.append(f"信号类型：`{signal}`")
    if "信号强弱：" not in text and "强弱：" not in text and strength:
        additions.append(f"信号强弱：`{strength}`")
    if "主要影响：" not in text and "关注 `" not in text and target:
        additions.append(f"主要影响：`{target}`")
    if "结论：" not in text and conclusion_text:
        additions.append(f"结论：{conclusion_text}")
    if "传导：" not in text:
        additions.append(f"传导：{_homepage_news_transmission_text(signal, target, subject, conclusion_text)}")
    if not additions:
        return text
    return f"{text}；" + "；".join(additions)


def _ensure_homepage_news_signal_bundle(text: Any) -> str:
    line = _safe_text(text)
    if not line:
        return line
    signal_match = re.search(r"信号类型：[`*]*([^`*；]+)[`*]*", line) or re.search(r"信号：[`*]*([^`*；]+)[`*]*", line)
    if not signal_match:
        return line
    signal_type = _safe_text(signal_match.group(1))
    impact_match = (
        re.search(r"主要影响：[`*]*([^`*；]+)[`*]*", line)
        or re.search(r"关注 [`*]*([^`*；]+)[`*]*", line)
        or re.search(r"更直接影响 [`*]*([^`*；]+)[`*]*", line)
    )
    impact = _safe_text(impact_match.group(1)) if impact_match else ""
    conclusion_match = re.search(r"结论：([^；）)]+)", line)
    conclusion = _safe_text(conclusion_match.group(1)) if conclusion_match else _homepage_signal_conclusion(signal_type, impact)
    if "结论：" not in line and conclusion:
        line = f"{line}；结论：{conclusion}"
    if "传导：" not in line:
        transmission = _homepage_news_transmission_text(signal_type, impact, {}, conclusion)
        line = f"{line}；传导：{transmission}"
    return line


def _briefing_client_safe_text(value: Any) -> str:
    line = _safe_text(value)
    if not line:
        return ""
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


def _entry_focus_text(value: Any) -> str:
    line = _safe_text(value)
    if not line:
        return ""
    normalized = line
    normalized = re.sub(r"^(先看|先等|等待|等|观察)\s*", "", normalized)
    normalized = re.sub(r"(后)?再看$", "", normalized)
    normalized = re.sub(r"(后)?再决定(?:是否)?升级风险偏好$", "", normalized)
    normalized = re.sub(r"(后)?再考虑(?:分批)?介入$", "", normalized)
    normalized = re.sub(r"[，,；;、\s]+$", "", normalized).strip()
    if normalized in {"确认", "技术确认", "右侧确认"}:
        return "技术确认和相对强弱是否一起改善"
    return normalized or line


def _falsifier_homepage_line(value: Any, *, suffix: str) -> str:
    line = _safe_text(value).strip().strip("`").rstrip("。；;，, ")
    if not line:
        return ""
    if line.startswith("如果"):
        if any(token in line for token in ("不能继续往乐观方向写", "不该继续写成今天的优先方向")):
            return f"{line}。"
        prefix = line
    else:
        prefix = f"如果出现 `{line}`"
    return f"{prefix}，{suffix}"


def _crowding_homepage_line(value: Any) -> str:
    line = _safe_text(value).strip().strip("`").rstrip("。；;，, ")
    if not line:
        return ""
    line = re.sub(r"^(轮动和拥挤度上，?)?", "", line).strip()
    line = re.sub(r"^[^：:]{0,24}?重点看[:：]", "", line).strip()
    return f"轮动和拥挤度上，要重点看：{line}"


def _stage_pattern_homepage_line(value: Any) -> str:
    line = _safe_text(value).strip().strip("`").rstrip("。；;，, ")
    if not line:
        return ""
    line = re.sub(r"^(更常见的是|更常|往往会|往往处在|常处在)\s*", "", line).strip()
    if line.startswith("处在"):
        line = line[2:].strip()
    return f"常见阶段更像 **{line}**。"


def _is_observe_style_text(text: Any) -> bool:
    line = _safe_text(text)
    if not line:
        return False
    return any(marker in line for marker in ("观察", "回避", "暂不出手", "等待", "先按观察仓"))


def _flatten_text(*values: Any) -> str:
    parts: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            parts.append(_flatten_text(*value.values()))
            continue
        if isinstance(value, (list, tuple, set)):
            parts.append(_flatten_text(*list(value)))
            continue
        text = str(value).strip()
        if text:
            parts.append(text)
    return " | ".join(item for item in parts if item)


def _thesis_previous_view(record: Mapping[str, Any] | None) -> str:
    thesis = dict(record or {})
    if not thesis:
        return "上次还没有可复用的 thesis / 事件记忆，这次先把当前判断落成第一版研究记忆。"
    parts: List[str] = []
    core_assumption = _safe_text(thesis.get("core_assumption") or thesis.get("core_hypothesis"))
    validation_metric = _safe_text(thesis.get("validation_metric"))
    holding_period = _safe_text(thesis.get("holding_period"))
    if core_assumption:
        parts.append(f"核心假设是 `{core_assumption}`")
    if validation_metric:
        parts.append(f"验证指标看 `{validation_metric}`")
    if holding_period:
        parts.append(f"预期周期是 `{holding_period}`")
    snapshot = dict(thesis.get("event_digest_snapshot") or {})
    status = _safe_text(snapshot.get("status"))
    layer = _safe_text(snapshot.get("lead_layer"))
    detail = _safe_text(snapshot.get("lead_detail"))
    title = _safe_text(snapshot.get("lead_title"))
    impact_summary = _safe_text(snapshot.get("impact_summary"))
    thesis_scope = _safe_text(snapshot.get("thesis_scope"))
    if status or layer:
        previous_event = f"事件边界是 `{status or '待补充'} / {layer or '新闻'}`"
        if title:
            previous_event += f" / {title}"
        parts.append(previous_event)
    detail_parts: List[str] = []
    if detail:
        detail_parts.append(detail)
    if impact_summary:
        detail_parts.append(f"更直接影响 `{impact_summary}`")
    if thesis_scope:
        detail_parts.append(f"当时先按 `{thesis_scope}` 处理")
    if detail_parts:
        parts.append("；".join(detail_parts))
    return "；".join(parts) or "上次还没有稳定的 thesis 口径，这次先以当前判断为准。"


def _current_judgment_view(subject: Mapping[str, Any], *, bucket: str = "") -> str:
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(subject.get("trade_state")) or _safe_text(dict(subject.get("narrative") or {}).get("judgment", {}).get("state"))
    direction = _safe_text(action.get("direction"))
    theme = _safe_text(subject_theme_label(subject, allow_day_theme=True))
    parts: List[str] = []
    for value in (trade_state, direction, _safe_text(bucket), theme):
        if value and value not in parts:
            parts.append(value)
    return " / ".join(parts[:3]) or "当前先按这次事件快照和正文判断理解。"


def _current_event_understanding(event_digest: Mapping[str, Any] | None) -> str:
    digest = dict(event_digest or {})
    detail = _safe_text(digest.get("lead_detail"))
    impact_summary = _safe_text(digest.get("impact_summary"))
    thesis_scope = _safe_text(digest.get("thesis_scope"))
    parts: List[str] = []
    if detail:
        parts.append(detail)
    if impact_summary:
        parts.append(f"更直接影响 `{impact_summary}`")
    if thesis_scope:
        parts.append(f"当前更像 `{thesis_scope}`")
    return "；".join(parts)


def _what_changed_conclusion_label(
    thesis: Mapping[str, Any] | None,
    current_event_digest: Mapping[str, Any] | None,
    delta: Mapping[str, Any] | None,
) -> str:
    thesis_record = dict(thesis or {})
    current = dict(current_event_digest or {})
    if not thesis_record:
        return "首次跟踪"
    previous_snapshot = dict(thesis_record.get("event_digest_snapshot") or {})
    current_status = _safe_text(current.get("status"))
    if current_status == "待复核":
        return "待复核"
    if not previous_snapshot:
        return "首次跟踪"
    change_type = _safe_text(dict(delta or {}).get("change_type"))
    if change_type == "status_up":
        return "升级"
    if change_type == "status_down":
        return "降级"
    return "维持"


def _report_editor_payload_dirs(report_type: str) -> List[Path]:
    if report_type == "scan":
        return [
            PROJECT_ROOT / "reports/scans/internal",
            PROJECT_ROOT / "reports/scans/etfs/internal",
            PROJECT_ROOT / "reports/scans/funds/internal",
        ]
    if report_type == "stock_analysis":
        return [PROJECT_ROOT / "reports/stock_analysis/internal"]
    return []


def _load_previous_editor_payload_context(
    symbol: Any,
    *,
    report_type: str = "",
    generated_at: Any = None,
) -> Dict[str, Any]:
    normalized = _safe_text(symbol)
    if not normalized or not report_type:
        return {}
    current_stamp = _safe_text(generated_at)
    current_day = current_stamp[:10]
    pattern_prefix = "scan" if report_type == "scan" else "stock_analysis" if report_type == "stock_analysis" else ""
    if not pattern_prefix:
        return {}

    best_prior_day_context: Dict[str, Any] = {}
    best_prior_day_key = ""
    best_same_day_context: Dict[str, Any] = {}
    best_same_day_key = ""
    for root in _report_editor_payload_dirs(report_type):
        if not root.exists():
            continue
        pattern = f"{pattern_prefix}_{normalized}_*_editor_payload.json"
        for path in sorted(root.glob(pattern)):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            subject = dict(payload.get("subject") or {})
            if _safe_text(subject.get("symbol")) != normalized:
                continue
            payload_stamp = (
                _safe_text(subject.get("generated_at"))
                or _safe_text(dict(payload.get("event_digest") or {}).get("as_of"))
                or _safe_text(dict(payload.get("event_digest") or {}).get("latest_signal_at"))
            )
            payload_day = payload_stamp[:10]
            if current_day and payload_day:
                if payload_day > current_day:
                    continue
                if payload_day == current_day:
                    if current_stamp and payload_stamp and payload_stamp >= current_stamp:
                        continue
                    if not current_stamp:
                        continue
            key = payload_stamp or payload_day or path.name
            context = {
                "event_digest": dict(payload.get("event_digest") or {}),
                "what_changed": dict(payload.get("what_changed") or {}),
                "reviewed_at": payload_stamp or payload_day,
                "path": str(path),
            }
            if current_day and payload_day and payload_day < current_day:
                if best_prior_day_key and key <= best_prior_day_key:
                    continue
                best_prior_day_key = key
                best_prior_day_context = context
                continue
            if best_same_day_key and key <= best_same_day_key:
                continue
            best_same_day_key = key
            best_same_day_context = context
    return best_prior_day_context or best_same_day_context


def _load_thesis_record(
    symbol: Any,
    thesis_repo: ThesisRepository | None = None,
    *,
    report_type: str = "",
    generated_at: Any = None,
) -> Dict[str, Any]:
    repo = thesis_repo or ThesisRepository()
    normalized = _safe_text(symbol)
    if not normalized:
        return {}
    try:
        record = dict(repo.get(normalized) or {})
    except Exception:
        record = {}
    if dict(record.get("event_digest_snapshot") or {}):
        return record

    fallback = _load_previous_editor_payload_context(
        normalized,
        report_type=report_type,
        generated_at=generated_at,
    )
    previous_digest = dict(fallback.get("event_digest") or {})
    reviewed_at = _safe_text(fallback.get("reviewed_at"))
    if previous_digest:
        snapshot = dict(previous_digest)
        if reviewed_at and not _safe_text(snapshot.get("recorded_at")):
            snapshot["recorded_at"] = reviewed_at
        if reviewed_at and not _safe_text(snapshot.get("as_of")):
            snapshot["as_of"] = reviewed_at
        merged = dict(record)
        merged.setdefault("event_digest_snapshot", snapshot)
        if reviewed_at:
            merged.setdefault("event_digest_updated_at", reviewed_at)
            merged.setdefault("updated_at", reviewed_at)
        return merged
    return record


def _thesis_reviewed_at(thesis: Mapping[str, Any] | None) -> str:
    record = dict(thesis or {})
    snapshot = dict(record.get("event_digest_snapshot") or {})
    return (
        _safe_text(snapshot.get("recorded_at"))
        or _safe_text(record.get("event_digest_updated_at"))
        or _safe_text(record.get("updated_at"))
    )


def _annotate_event_digest_with_history(
    event_digest: Mapping[str, Any] | None,
    thesis: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    digest = dict(event_digest or {})
    if not digest:
        return {}
    reviewed_at = _thesis_reviewed_at(thesis)
    if reviewed_at:
        digest["previous_reviewed_at"] = reviewed_at
    return digest


def build_what_changed_summary(
    subject: Mapping[str, Any],
    event_digest: Mapping[str, Any],
    *,
    bucket: str = "",
    thesis_repo: ThesisRepository | None = None,
    thesis: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    digest = dict(event_digest or {})
    if not digest:
        return {}
    thesis_record = dict(thesis or {})
    if not thesis_record:
        thesis_repo = thesis_repo or ThesisRepository()
        thesis_record = _load_thesis_record(_safe_text(subject.get("symbol")), thesis_repo=thesis_repo)
    previous_snapshot = dict(thesis_record.get("event_digest_snapshot") or {})
    delta = compare_event_digest_snapshots(previous_snapshot, digest)
    state_transition: Dict[str, Any] = {}
    if previous_snapshot:
        state_transition = build_thesis_state_transition(thesis_record, digest, delta, source="what_changed")
    conclusion_label = _safe_text(state_transition.get("state")) or _what_changed_conclusion_label(thesis_record, digest, delta)
    return {
        "previous_view": _thesis_previous_view(thesis_record),
        "change_summary": _safe_text(delta.get("summary")) or _safe_text(digest.get("changed_what")),
        "conclusion_label": conclusion_label,
        "state_trigger": _safe_text(state_transition.get("trigger")),
        "state_summary": _safe_text(state_transition.get("summary")),
        "current_view": _current_judgment_view(subject, bucket=bucket),
        "current_event_understanding": _current_event_understanding(digest),
    }


def summarize_what_changed_contract(summary: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(summary or {})
    if not payload:
        return {}
    compact: Dict[str, Any] = {"contract_version": "what_changed.v1"}
    for key in (
        "previous_view",
        "change_summary",
        "conclusion_label",
        "state_trigger",
        "state_summary",
        "current_view",
        "current_event_understanding",
    ):
        value = _safe_text(payload.get(key))
        if value:
            compact[key] = value
    return compact


def _dimension_score_map(dimensions: Mapping[str, Any]) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    for key, _ in DIMENSION_LABELS:
        value = dict(dimensions.get(key) or {}).get("score")
        try:
            scores[key] = float(value or 0)
        except (TypeError, ValueError):
            scores[key] = 0.0
    return scores


def _dimension_summary(dimensions: Mapping[str, Any], key: str) -> str:
    return _safe_text(dict(dimensions.get(key) or {}).get("summary"))


def _extract_price_candidates(text: Any) -> List[float]:
    values: List[float] = []
    for raw in re.findall(r"([0-9]+(?:\.[0-9]+)?)", _safe_text(text)):
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            values.append(value)
    return values


def _extract_labeled_resistance_prices(text: Any) -> List[float]:
    blob = _safe_text(text)
    values: List[float] = []

    def append_match(match: re.Match[str]) -> None:
        raw = match.group(1)
        start, end = match.span(1)
        prev_char = blob[start - 1 : start]
        next_text = blob[end : end + 4]
        # Do not treat the window length in labels like MA20 / 近20日高点 as a price.
        if prev_char.isalpha() or re.match(r"\s*(?:日|/)", next_text):
            return
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return
        if value > 0 and value not in values:
            values.append(value)

    specific_patterns = (
        r"\bMA\d+\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
        r"近\s*\d+(?:\s*/\s*\d+)?\s*日高点[^0-9]{0,12}([0-9]+(?:\.[0-9]+)?)",
        r"(?:摆动前高|前高|高点|压力位|压力|压制)[^0-9A-Za-z]{0,16}([0-9]+(?:\.[0-9]+)?)",
        r"(?:摆动前高|前高|高点|压力位|压力|压制)[^。；;\n]{0,40}?([0-9]+(?:\.[0-9]+)?)",
    )
    for pattern in specific_patterns:
        for match in re.finditer(pattern, blob):
            append_match(match)
    return values


def _subject_last_close_value(subject: Mapping[str, Any]) -> float:
    metrics = dict(subject.get("metrics") or {})
    try:
        return float(metrics.get("last_close") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _subject_factor(subject: Mapping[str, Any], *, factor_id: str = "", factor_name: str = "") -> Mapping[str, Any]:
    dimensions = dict(subject.get("dimensions") or {})
    for dimension in dimensions.values():
        for factor in list(dict(dimension or {}).get("factors") or []):
            if not isinstance(factor, Mapping):
                continue
            if factor_id and _safe_text(factor.get("factor_id")) == factor_id:
                return factor
            if factor_name and factor_name in _safe_text(factor.get("name")):
                return factor
    return {}


def _subject_near_resistance_level(subject: Mapping[str, Any]) -> float:
    factor = _subject_factor(subject, factor_id="j1_resistance_zone") or _subject_factor(subject, factor_name="压力位")
    blob = " ".join(
        item
        for item in (
            _safe_text(factor.get("signal")),
            _safe_text(factor.get("detail")),
        )
        if item
    )
    labeled_candidates = _extract_labeled_resistance_prices(blob)
    candidates = labeled_candidates or _extract_price_candidates(blob)
    last_close = _subject_last_close_value(subject)
    if last_close > 0:
        near = [value for value in candidates if value > last_close * 1.001 and value <= last_close * 1.12]
        if near:
            return min(near)
    if labeled_candidates:
        return min(labeled_candidates)
    for value in candidates:
        if value > 1:
            return value
    return 0.0


def _stock_near_resistance_level(subject: Mapping[str, Any]) -> float:
    if _safe_text(subject.get("asset_type")) != "cn_stock":
        return 0.0
    return _subject_near_resistance_level(subject)


def _stock_observe_entry_focus(subject: Mapping[str, Any], action: Mapping[str, Any]) -> str:
    near_resistance = _stock_near_resistance_level(subject)
    if near_resistance > 0:
        return (
            f"近端压力 `{near_resistance:.3f}` 放量站上并回踩不破；"
            "MA20 / MA60 只做波段或中线确认，不是第一笔必要条件"
        )
    return _entry_focus_text(action.get("entry")) or "右侧确认"


def _stock_observe_position_line(position: str) -> str:
    if position and any(ch.isdigit() for ch in position) and not re.search(r"(?:≤|<=|不超过|上限)?\s*2\s*%", position):
        return f"没触发前 `0%`；若触发第一笔按 `{position}`；确认延续后再考虑加到 `5%` 观察仓。"
    return (
        "没触发前 `0%`；突破回踩确认后第一笔 `2% - 3%` 试错；"
        "若次日不回落且相对强弱不丢，最多加到 `5%` 观察仓。"
    )


def _homepage_focus_text(text: str) -> str:
    line = _safe_text(text)
    if not line:
        return "`右侧确认`"
    if "`" in line:
        return line
    return f"`{line}`"


def _top_bottom_dimensions(dimensions: Mapping[str, Any]) -> tuple[tuple[str, float], tuple[str, float]]:
    scores = _dimension_score_map(dimensions)
    ordered = [(key, score) for key, score in scores.items() if key != "chips"]
    if not ordered:
        return ("fundamental", 0.0), ("technical", 0.0)
    strongest = max(ordered, key=lambda item: item[1])
    weakest = min(ordered, key=lambda item: item[1])
    return strongest, weakest


def _bucket_text(trade_state: str, direction: str) -> str:
    blob = " ".join(part for part in (trade_state, direction) if part)
    if any(token in blob for token in ("回避", "观察", "等待", "暂不")):
        return "当前更像观察阶段，短期先看确认，不把主题逻辑直接翻译成交易动作。"
    if any(token in blob for token in ("偏多", "做多", "推荐", "试仓")):
        return "当前方向没有完全走坏，但更像等待确认后的参与窗口，不是情绪上头时直接追的阶段。"
    return "当前更适合先看阶段和触发条件，再决定要不要执行。"


def _no_signal_notice(
    trade_state: str,
    direction: str,
    *,
    observe_only: bool = False,
) -> str:
    blob = " ".join(part for part in (trade_state, direction) if part)
    if observe_only or any(token in blob for token in ("观察", "暂不", "回避")):
        return "今天先按观察稿处理；后文重点是升级条件、优先顺序和关键证据，不把细节直接当成推荐升级。"
    return ""


def _history_as_of_day(subject: Mapping[str, Any]) -> str:
    history = subject.get("history")
    try:
        if history is not None and hasattr(history, "empty") and not history.empty and "date" in history.columns:
            return _safe_text(history["date"].iloc[-1])[:10]
    except Exception:
        return ""
    return ""


def _stale_market_snapshot_line(subject: Mapping[str, Any], *, sentiment: bool = False) -> str:
    generated_day = _safe_text(subject.get("generated_at"))[:10]
    history_day = _history_as_of_day(subject)
    if not generated_day or not history_day or generated_day <= history_day:
        return ""
    if sentiment:
        return f"当前仍使用 `{history_day}` 的行情快照，情绪代理今天还没刷新。"
    return "宏观月度因子这轮没有新增月频更新，先沿用最近一版快照；别把同一组 PMI / CPI / PPI 数字当成今天的新变化。"


def _macro_lines(
    regime: Mapping[str, Any],
    day_theme: str,
    market_hint: str = "",
    flow_hint: str = "",
    *,
    subject: Mapping[str, Any] | None = None,
) -> List[str]:
    lines: List[str] = []
    regime_name = _safe_text(regime.get("current_regime"))
    if regime_name:
        label = REGIME_LABELS.get(regime_name, regime_name)
        if day_theme:
            lines.append(f"宏观这里只作尾部风险约束：中期背景更接近 `{label}`，但当天主线仍先看 `{day_theme}`，不要用 macro 一票否决赛道。")
        else:
            lines.append(f"宏观这里只作尾部风险约束：中期背景更接近 `{label}`，先用它解释顺风逆风，不直接替代赛道判断。")
    if flow_hint:
        lines.append(flow_hint)
    elif market_hint:
        lines.append(market_hint)
    if day_theme and not any(day_theme in line for line in lines):
        lines.append(f"今天市场更明确在交易 `{day_theme}` 这条主线，后文细节都应该回到这条主线上理解。")
    stale_line = _stale_market_snapshot_line(subject or {})
    if stale_line and stale_line not in lines:
        lines.append(stale_line)
    return lines[:4]


def _sentiment_lines(subject: Mapping[str, Any], selection_context: Mapping[str, Any] | None = None) -> List[str]:
    lines: List[str] = []
    proxy_signals = dict(subject.get("proxy_signals") or {})
    social = dict(dict(proxy_signals.get("social_sentiment") or {}).get("aggregate") or {})
    interpretation = _safe_text(social.get("interpretation"))
    if interpretation:
        lines.append(interpretation)
    limitations = list(social.get("limitations") or [])
    if limitations:
        lines.append(_safe_text(limitations[0]))
    coverage_lines = list(dict(selection_context or {}).get("coverage_lines") or [])
    if coverage_lines:
        coverage_blob = " / ".join(_safe_text(item) for item in coverage_lines[:2] if _safe_text(item))
        if coverage_blob:
            lines.append(f"今天的信息热度更适合当成辅助层，当前覆盖状态是：{coverage_blob}。")
    if not lines:
        relative = float(dict(subject.get("dimensions") or {}).get("relative_strength", {}).get("score") or 0)
        catalyst = float(dict(subject.get("dimensions") or {}).get("catalyst", {}).get("score") or 0)
        if relative >= 60 and catalyst >= 50:
            lines.append("情绪和热度没有明显拖后腿，但也还没强到可以单靠拥挤度去改写动作判断。")
        else:
            lines.append("情绪与热度更像辅助信息，当前还不足以单独改写动作判断。")
    stale_line = _stale_market_snapshot_line(subject, sentiment=True)
    if stale_line and stale_line not in lines:
        lines.append(stale_line)
    return lines[:3]


def _news_lines(subject: Mapping[str, Any], *, previous_reviewed_at: Any = None) -> List[str]:
    dimensions = dict(subject.get("dimensions") or {})
    catalyst = dict(dimensions.get("catalyst") or {})
    catalyst_web_review = dict(subject.get("catalyst_web_review") or catalyst.get("web_review") or {})
    news_report = dict(subject.get("news_report") or {})
    as_of = (
        _safe_text(subject.get("generated_at"))
        or _safe_text(dict(subject.get("provenance") or {}).get("analysis_generated_at"))
        or _safe_text(dict(subject.get("provenance") or {}).get("catalyst_evidence_as_of"))
    )
    lines: List[str] = []
    context_lines: List[str] = []
    asset_type = _safe_text(subject.get("asset_type"))
    max_items = 4 if asset_type == "cn_etf" else 2
    symbol = _safe_text(subject.get("symbol"))
    raw_news_items = [
        dict(item or {})
        for item in list(news_report.get("items") or [])
        if not _should_skip_instrument_proxy_news(subject, dict(item or {}))
    ]
    summary_lines = [
        _safe_text(item)
        for item in list(news_report.get("summary_lines") or [])
        if _safe_text(item)
    ]

    def _is_diagnostic_row(row: Mapping[str, Any] | str) -> bool:
        if isinstance(row, str):
            text = _safe_text(row)
            source = ""
        else:
            text = _safe_text(dict(row).get("title"))
            source = _safe_text(dict(row).get("source"))
        blob = " ".join(part for part in (text, source) if part)
        return any(
            token in blob
            for token in (
                "内部覆盖率摘要",
                "当前没有抓到高置信直连证据",
                "当前可前置的一手情报有限",
                "催化判断更多依赖结构化事件或行业映射",
                "当前更依赖主题逻辑和后文证据来理解",
                "不把情报空白直接误读成逻辑失效",
                "覆盖率",
                "待 AI 联网复核",
            )
        )

    for item in list(catalyst_web_review.get("key_evidence") or [])[:max_items]:
        title = _safe_text(item)
        if title:
            lines.append(f"`联网复核补充`：{title}")

    web_review_used = bool(lines)
    if not web_review_used:
        summary_limit = 1 if raw_news_items else 0
        for item in summary_lines[:summary_limit]:
            context_lines.append(f"情报摘要：{_humanize_news_summary_line(item)}")
        relevance_line = _intelligence_relevance_line(subject)
        if relevance_line:
            context_lines.append(f"关系说明：{relevance_line}")

    if not web_review_used:
        for item in raw_news_items[:max_items]:
            row = dict(item or {})
            if _is_diagnostic_row(row):
                continue
            title = _safe_text(row.get("title"))
            source = _safe_text(row.get("source"))
            date = _safe_text(row.get("date") or row.get("published_at"))
            link = effective_intelligence_link(row, symbol=symbol)
            if not title:
                continue
            prefix = " · ".join(part for part in (date, source) if part)
            title_text = _markdown_link(title, link)
            tags = _intelligence_tags(row, as_of=as_of, theme_level=True, previous_reviewed_at=previous_reviewed_at)
            tag_text = f"`{format_intelligence_attributes(tags)}` · " if tags else ""
            base_line = f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}"
            lines.append(
                _append_news_interpretation(
                    base_line,
                    subject=subject,
                    signal_type=row.get("signal_type") or row.get("lead_detail"),
                    signal_strength=row.get("signal_strength") or row.get("importance_label"),
                    impact=row.get("impact_summary") or row.get("impact"),
                    conclusion=row.get("signal_conclusion") or row.get("conclusion"),
                )
            )

    if not lines:
        for item in list(catalyst.get("evidence") or [])[:max_items]:
            row = dict(item or {})
            if _is_diagnostic_row(row):
                continue
            title = _safe_text(row.get("title"))
            source = _safe_text(row.get("source"))
            date = _safe_text(row.get("date"))
            link = effective_intelligence_link(row, symbol=symbol)
            if not title:
                continue
            prefix = " · ".join(part for part in (date, source) if part)
            title_text = _markdown_link(title, link)
            tags = _intelligence_tags(row, as_of=as_of, previous_reviewed_at=previous_reviewed_at)
            tag_text = f"`{format_intelligence_attributes(tags)}` · " if tags else ""
            base_line = f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}"
            lines.append(
                _append_news_interpretation(
                    base_line,
                    subject=subject,
                    signal_type=row.get("signal_type") or row.get("lead_detail"),
                    signal_strength=row.get("signal_strength") or row.get("importance_label"),
                    impact=row.get("impact_summary") or row.get("impact"),
                    conclusion=row.get("signal_conclusion") or row.get("conclusion"),
                )
            )
    if not lines:
        for item in list(catalyst.get("theme_news") or [])[:max_items]:
            row = dict(item or {})
            if _should_skip_instrument_proxy_news(subject, row):
                continue
            if _is_diagnostic_row(row):
                continue
            title = _safe_text(row.get("title"))
            source = _safe_text(row.get("source"))
            date = _safe_text(row.get("date"))
            link = effective_intelligence_link(row, symbol=symbol)
            if not title:
                continue
            prefix = " · ".join(part for part in (date, source) if part)
            title_text = _markdown_link(title, link)
            tags = _intelligence_tags(row, as_of=as_of, theme_level=True, previous_reviewed_at=previous_reviewed_at)
            tag_text = f"`{format_intelligence_attributes(tags)}` · " if tags else ""
            theme_line = f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}"
            lines.append(
                "主题级新闻："
                + _append_news_interpretation(
                    theme_line,
                    subject=subject,
                    signal_type=row.get("signal_type") or row.get("lead_detail"),
                    signal_strength=row.get("signal_strength") or row.get("importance_label"),
                    impact=row.get("impact_summary") or row.get("impact"),
                    conclusion=row.get("signal_conclusion") or row.get("conclusion"),
                )
            )
    if not lines:
        for item in list(subject.get("evidence") or [])[:max_items]:
            row = dict(item or {})
            if _is_diagnostic_row(row):
                continue
            title = _safe_text(row.get("title")) or _safe_text(item)
            source = _safe_text(row.get("source"))
            date = _safe_text(row.get("date"))
            link = effective_intelligence_link(row, symbol=symbol)
            if not title:
                continue
            prefix = " · ".join(part for part in (date, source) if part)
            title_text = _markdown_link(title, link)
            tags = _intelligence_tags(row, as_of=as_of, previous_reviewed_at=previous_reviewed_at)
            tag_text = f"`{format_intelligence_attributes(tags)}` · " if tags else ""
            base_line = f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}"
            lines.append(
                _append_news_interpretation(
                    base_line,
                    subject=subject,
                    signal_type=row.get("signal_type") or row.get("lead_detail"),
                    signal_strength=row.get("signal_strength") or row.get("importance_label"),
                    impact=row.get("impact_summary") or row.get("impact"),
                    conclusion=row.get("signal_conclusion") or row.get("conclusion"),
                )
            )
    return [*context_lines[:2], *lines[:max_items]]


def _humanize_news_summary_line(text: Any) -> str:
    summary = _safe_text(text)
    if not summary:
        return ""
    if summary.startswith("主题聚类："):
        body = summary.split("：", 1)[1].strip()
        if body:
            if "，" in body or "," in body:
                return f"这批外部情报主要围绕 {body}，先看其中哪一条和当前标的或主题最直接。"
            match = re.match(r"(.+?)\s+(\d+)\s*条$", body)
            if match:
                label = match.group(1).strip()
                count = match.group(2).strip()
                if label in {"综合/其他", "综合", "其他"}:
                    return (
                        f"这批外部情报暂时没能稳定归到单一主题；去重后先留下 {count} 条背景线索，"
                        "更适合当背景补充，不直接当成新增催化。"
                    )
                return f"这批外部情报主要围绕 {label}；这里的 {count} 条，是把重复报道合并后的线索组数，不是 {count} 个独立利好。"
            if body in {"综合/其他", "综合", "其他"}:
                return "这批外部情报暂时没能稳定归到单一主题，更像多条背景线索混在一起；先当背景补充，不直接当成新增催化。"
            return f"这批外部情报主要围绕 {body}，先看其中哪一条和当前标的或主题最直接。"
    if summary.startswith("来源分层："):
        body = summary.split("：", 1)[1].strip()
        if body:
            return f"来源上以 {body} 为主。"
    if summary.startswith("当前更值得先看的代表情报来自："):
        body = summary.split("：", 1)[1].strip()
        if body:
            return f"当前更值得先看的代表情报，主要来自 {body}。"
    return summary


def _intelligence_relevance_line(subject: Mapping[str, Any]) -> str:
    asset_type = _safe_text(subject.get("asset_type"))
    name = _safe_text(subject.get("name")) or _safe_text(subject.get("symbol"))
    metadata = dict(subject.get("metadata") or {})
    if asset_type == "cn_etf":
        fund_profile = dict(subject.get("fund_profile") or {})
        overview = dict(fund_profile.get("overview") or {})
        etf_info = dict(fund_profile.get("etf_info") or {})
        tracked_index = (
            _safe_text(metadata.get("tracked_index_name"))
            or _safe_text(etf_info.get("跟踪指数"))
            or _safe_text(overview.get("业绩比较基准"))
        )
        sector = _safe_text(metadata.get("sector"))
        if tracked_index:
            return f"这些情报先用来解释 `{tracked_index}` 这条指数/板块主线，只有进一步传导到成分股和跟踪指数，才算对 `{name}` 更直接。"
        if sector:
            return f"这些情报先用来解释 `{sector}` 这条主题线的背景，不能把每条行业旧闻都直接当成 `{name}` 的新增催化。"
    if asset_type == "cn_fund":
        taxonomy = dict(metadata.get("taxonomy") or metadata.get("fund_taxonomy") or {})
        taxonomy_profile = dict(metadata.get("theme_profile") or taxonomy.get("theme_profile") or {})
        benchmark = (
            _safe_text(metadata.get("tracked_index_name"))
            or _safe_text(metadata.get("benchmark"))
            or _safe_text(metadata.get("benchmark_name"))
            or _safe_text(dict(subject.get("fund_profile") or {}).get("overview", {}).get("业绩比较基准"))
        )
        style = (
            _safe_text(metadata.get("primary_chain"))
            or _safe_text(taxonomy.get("primary_chain"))
            or _safe_text(taxonomy_profile.get("primary_chain"))
            or _safe_text(metadata.get("sector"))
            or _safe_text(metadata.get("category"))
        )
        if benchmark:
            return f"这些情报先用来解释 `{benchmark}` 方向，只有落到持仓和经理风格上，才算对 `{name}` 更直接。"
        if style:
            return f"这些情报先用来解释 `{style}` 方向的环境变化，不能把主题级旧闻直接当成 `{name}` 的单独催化。"
    if asset_type in {"cn_stock", "hk", "us"}:
        sector = _safe_text(metadata.get("sector"))
        if sector:
            return f"这些情报先看会不会改写 `{name}` 所在的 `{sector}` 行业或公司执行层，不把单纯题材热度直接当成公司级催化。"
        return f"这些情报先看会不会改写 `{name}` 的公司执行层，不把主题级旧闻直接当成公司级催化。"
    return ""


def _no_intelligence_homepage_line() -> str:
    return "当前可前置的外部情报仍偏少，先把主题逻辑和后文证据合在一起理解。"


def _homepage_news_limit(subject: Mapping[str, Any]) -> int:
    return 5 if _safe_text(subject.get("asset_type")) == "cn_etf" else 3


def _homepage_news_key(line: Any) -> str:
    text = _safe_text(line)
    if not text:
        return ""
    markdown_match = re.search(r"\[([^\]]+)\]\(", text)
    if markdown_match:
        text = markdown_match.group(1)
    elif "：" in text:
        text = text.split("：", 1)[-1]
    text = re.sub(
        r"\s*[-|·]\s*(财联社|新浪财经|证券时报|中国证券报|上海证券报|api\d*\.cls\.cn|[A-Za-z0-9.-]+\.(?:cn|com|net|org))\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s*[-|·]\s*[\u4e00-\u9fffA-Za-z0-9]{2,12}(?:网|社|报|在线|财经|之星)\s*$", "", text)
    text = re.sub(r"^\d{1,2}:\d{2}(?::\d{2})?\s*", "", text)
    text = re.sub(r"^[\[【(（]\s*", "", text)
    text = re.sub(r"\s*[\]】)）]\s*$", "", text)
    text = re.sub(r"^\d{1,2}:\d{2}(?::\d{2})?\s*[【\[]\s*", "", text)
    text = re.sub(r"\s*[】\]]\s*$", "", text)
    text = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


_GENERIC_INTELLIGENCE_KEYWORDS = {
    "etf",
    "ETF",
    "lof",
    "LOF",
    "基金",
    "主题",
    "指数",
    "中证",
    "国泰",
    "华夏",
    "易方达",
    "招商",
    "南方",
    "平安",
    "长城",
    "汇添富",
    "广发",
    "富国",
    "嘉实",
    "博时",
    "银华",
    "鹏华",
    "华宝",
    "天弘",
    "工银瑞信",
    "基金公司",
    "市场",
    "A股",
    "a股",
}


def _flatten_keyword_text(*values: Any) -> str:
    parts: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            parts.append(_flatten_keyword_text(*value.values()))
        elif isinstance(value, (list, tuple, set)):
            parts.append(_flatten_keyword_text(*value))
        else:
            text = _safe_text(value)
            if text:
                parts.append(text)
    return " ".join(part for part in parts if part)


def _subject_intelligence_keywords(subject: Mapping[str, Any], digest: Mapping[str, Any]) -> List[str]:
    metadata = dict(subject.get("metadata") or {})
    taxonomy = dict(metadata.get("taxonomy") or metadata.get("fund_taxonomy") or {})
    taxonomy_profile = dict(metadata.get("theme_profile") or taxonomy.get("theme_profile") or {})
    fund_profile = dict(subject.get("fund_profile") or {})
    overview = dict(fund_profile.get("overview") or {})
    etf_info = dict(fund_profile.get("etf_info") or {})
    asset_type = _safe_text(subject.get("asset_type"))
    day_theme_label = "" if asset_type in {"cn_stock", "hk", "us"} else dict(subject.get("day_theme") or {}).get("label")
    raw_values: List[Any] = [
        subject.get("name"),
        subject.get("symbol"),
        metadata.get("sector"),
        metadata.get("industry"),
        metadata.get("industry_framework_label"),
        metadata.get("tracked_index_name"),
        metadata.get("chain_nodes"),
        metadata.get("theme_family"),
        metadata.get("primary_chain"),
        metadata.get("theme_role"),
        metadata.get("evidence_keywords"),
        metadata.get("preferred_sector_aliases"),
        metadata.get("mainline_tags"),
        taxonomy,
        taxonomy_profile,
        overview.get("业绩比较基准"),
        etf_info.get("跟踪指数"),
        day_theme_label,
        dict(subject.get("theme_playbook") or {}).get("label"),
        digest.get("theme_label"),
    ]
    blob = _flatten_keyword_text(*raw_values)
    candidates = set()
    for token in re.split(r"[\s,，/、|｜:：()（）\\-]+", blob):
        clean = token.strip()
        if len(clean) >= 2 and clean not in _GENERIC_INTELLIGENCE_KEYWORDS:
            candidates.add(clean)
    for keyword in (
        "半导体",
        "芯片",
        "材料设备",
        "设备",
        "AI算力",
        "算力",
        "光模块",
        "CPO",
        "光通信",
        "数据中心",
        "交换机",
        "PCB",
        "液冷",
        "存储",
        "通信设备",
        "卫星互联网",
        "智能电网",
        "特高压",
        "储能",
        "有色",
        "黄金",
        "铜",
        "铝",
        "电网",
        "创新药",
        "license-out",
        "BD授权",
        "CXO",
        "CRO",
        "CDMO",
        "游戏",
        "版号",
        "AIGC",
        "白酒",
    ):
        if keyword in blob:
            candidates.add(keyword)
    return sorted(candidates, key=len, reverse=True)


def _news_line_subject_relevance(line: Any, subject: Mapping[str, Any], digest: Mapping[str, Any]) -> int:
    text = _safe_text(line)
    if not text:
        return -99
    base_text = re.split(r"；(?:信号类型|信号|信号强弱|主要影响|结论|传导)：", text, maxsplit=1)[0]
    score = 0
    keywords = _subject_intelligence_keywords(subject, digest)
    keyword_hit = any(keyword and keyword in base_text for keyword in keywords)
    if keyword_hit:
        score += 5
    lead_title = _safe_text(dict(digest).get("lead_title"))
    if lead_title and lead_title in text:
        score += 4
    if any(marker in base_text for marker in ("公告类型：", "财报摘要：", "扩产", "投产", "业绩", "订单", "净创设", "成分权重")):
        score += 2
    market_background_markers = (
        "美伊",
        "伊朗",
        "特朗普",
        "停火",
        "投行如何看待",
        "A股放量",
        "后市机会",
        "沪指",
        "创业板",
        "大盘",
        "市场爆涨",
        "市场大涨",
    )
    if any(marker in base_text for marker in market_background_markers) and not keyword_hit:
        score -= 6
    return score


def _sort_news_lines_by_subject_relevance(
    lines: Sequence[str],
    subject: Mapping[str, Any],
    digest: Mapping[str, Any],
) -> List[str]:
    return sorted(
        [item for item in lines if _safe_text(item)],
        key=lambda item: _news_line_subject_relevance(item, subject, digest),
        reverse=True,
    )


def _filter_homepage_linked_news_lines(
    lines: Sequence[str],
    subject: Mapping[str, Any],
    digest: Mapping[str, Any],
) -> List[str]:
    asset_type = _safe_text(subject.get("asset_type"))
    min_score = 1 if asset_type in {"cn_stock", "hk", "us", "cn_fund"} else 0
    scored = [
        (item, _news_line_subject_relevance(item, subject, digest))
        for item in lines
        if _safe_text(item)
    ]
    relevant = [item for item, score in scored if score >= min_score]
    if relevant:
        return relevant
    return []


def _append_unique_news_line(lines: List[str], seen_keys: set[str], line: Any, *, limit: int) -> None:
    text = _safe_text(line)
    if not text:
        return
    key = _homepage_news_key(text)
    if key and key in seen_keys:
        return
    before = len(lines)
    _append_unique_line(lines, text, limit=limit)
    if len(lines) > before and key:
        seen_keys.add(key)


def _format_homepage_evidence_line(line: Any) -> str:
    text = _safe_text(line)
    if not text:
        return ""
    if text.startswith(("外部情报：", "结构证据：", "情报摘要：")):
        return text
    if "](" in text:
        clean = text
        preserve_signal_bundle = any(marker in clean for marker in ("；信号：", "；信号类型：", "；信号强弱：", "；结论："))
        trim_markers = (
            (
                "；来源层级：",
                "；复查语境：",
                "；事件理解：",
            )
            if preserve_signal_bundle
            else (
                "；来源层级：",
                "；信号：",
                "；信号类型：",
                "；信号强弱：",
                "；结论：",
                "；更直接影响",
                "；当前更像",
                "；事件理解：",
            )
        )
        for marker in trim_markers:
            if marker in clean:
                clean = clean.split(marker, 1)[0].rstrip("；;，, ")
                break
        return f"外部情报：{clean}"
    if any(marker in text for marker in ("信号类型：", "信号强弱：", "结论：", "事件理解：")):
        return f"结构证据：{text}"
    return text


def _missing_clickable_intelligence_line() -> str:
    return "外部情报：本轮未拿到可点击外部情报；当前先按结构证据和情报摘要理解，不把盘面摘要误写成可核验新闻。"


def _event_digest_lead_evidence_line(event_digest: Mapping[str, Any], subject: Mapping[str, Any] | None = None) -> str:
    digest = dict(event_digest or {})
    title = _safe_text(digest.get("lead_title"))
    if not title:
        return ""
    link = _safe_text(digest.get("lead_link"))
    evidence = _markdown_link(title, link)
    prefix = "外部情报" if link else "结构证据"
    signal_type = _safe_text(digest.get("signal_type"))
    signal_strength = _safe_text(digest.get("signal_strength") or digest.get("importance_label")) or "中"
    signal_conclusion = _safe_text(digest.get("signal_conclusion") or digest.get("conclusion"))
    latest_signal_at = _safe_text(digest.get("latest_signal_at"))
    parts = [f"{prefix}：{evidence}"]
    if signal_type:
        parts.append(f"信号类型：`{signal_type}`")
        parts.append(f"信号强弱：`{signal_strength}`")
    if signal_conclusion:
        parts.append(f"结论：{signal_conclusion}")
    if latest_signal_at:
        parts.append(f"最新情报时点：`{latest_signal_at}`")
    return _append_news_interpretation(
        "；".join(parts),
        subject=subject or {},
        signal_type=signal_type,
        signal_strength=signal_strength,
        impact=digest.get("impact_summary"),
        conclusion=signal_conclusion,
    )


def _has_clickable_homepage_evidence(lines: Sequence[str]) -> bool:
    return any("http://" in _safe_text(item) or "https://" in _safe_text(item) for item in lines)


def _news_lines_with_event_digest(subject: Mapping[str, Any], event_digest: Mapping[str, Any]) -> List[str]:
    digest = dict(event_digest or {})
    raw_news_lines = _news_lines(subject, previous_reviewed_at=digest.get("previous_reviewed_at"))
    digest_lines = event_digest_homepage_lines(digest, [])
    lines: List[str] = []
    seen_keys: set[str] = set()
    limit = _homepage_news_limit(subject)
    prefer_linked_news_first = _safe_text(subject.get("asset_type")) == "cn_etf"
    if _safe_text(digest.get("thesis_scope")) == "历史基线":
        filtered_news_lines = [item for item in raw_news_lines if "旧闻回放" not in item]
        raw_news_lines = filtered_news_lines or raw_news_lines[:1]
    linked_news_lines = [item for item in raw_news_lines if "](" in item]
    plain_news_lines = [item for item in raw_news_lines if item not in linked_news_lines]
    linked_news_lines = _sort_news_lines_by_subject_relevance(linked_news_lines, subject, digest)
    plain_news_lines = _sort_news_lines_by_subject_relevance(plain_news_lines, subject, digest)
    linked_news_lines = _filter_homepage_linked_news_lines(linked_news_lines, subject, digest)
    raw_news_lines = [*linked_news_lines, *plain_news_lines]
    lead_evidence_line = _event_digest_lead_evidence_line(digest, subject)
    if lead_evidence_line and _news_line_subject_relevance(lead_evidence_line, subject, digest) < 0:
        lead_evidence_line = ""
    structure_line = _format_homepage_evidence_line(_event_digest_signal_line(digest))
    if structure_line:
        structure_line = _append_news_interpretation(
            structure_line,
            subject=subject,
            signal_type=digest.get("signal_type") or digest.get("lead_detail") or digest.get("lead_layer"),
            signal_strength=digest.get("signal_strength") or digest.get("importance_label"),
            impact=digest.get("impact_summary"),
            conclusion=digest.get("signal_conclusion") or digest.get("conclusion"),
        )
    if lead_evidence_line and structure_line:
        signal_type = _safe_text(digest.get("signal_type"))
        asset_type = _safe_text(subject.get("asset_type"))
        duplicate_signal_bundle = bool(signal_type and signal_type in lead_evidence_line)
        product_profile_subject = asset_type in {"cn_etf", "cn_fund", "cn_index"}
        stock_direct_subject = asset_type == "cn_stock"
        if lead_evidence_line.startswith("结构证据：") or (
            duplicate_signal_bundle and (product_profile_subject or stock_direct_subject)
        ):
            structure_line = ""
    digest_context_line = ""
    for item in digest_lines:
        text = _safe_text(item)
        if any(token in text for token in ("事件状态", "上次复查", "自上次复查", "首次跟踪")):
            digest_context_line = _format_homepage_evidence_line(item)
            break
    summary_lines = [_format_homepage_evidence_line(item) for item in digest_lines if _safe_text(item).startswith("情报摘要：")]
    if linked_news_lines:
        if prefer_linked_news_first:
            digest_strength = _safe_text(digest.get("signal_strength") or digest.get("importance_label"))
            digest_scope = _safe_text(digest.get("thesis_scope"))
            digest_importance = _safe_text(digest.get("importance"))
            digest_layer = _safe_text(digest.get("lead_layer"))
            digest_detail = _safe_text(digest.get("lead_detail"))
            lead_is_financial_calendar = digest_layer == "财报" and "财报日历" in digest_detail
            lead_score = _news_line_subject_relevance(lead_evidence_line, subject, digest)
            top_linked_score = max(
                (_news_line_subject_relevance(item, subject, digest) for item in linked_news_lines),
                default=-99,
            )
            lead_is_stronger_subject_evidence = (
                lead_evidence_line
                and (lead_is_financial_calendar or lead_score >= top_linked_score)
                and (digest_strength in {"强", "高"} or digest_scope == "thesis变化" or digest_importance == "high")
            )
            if lead_is_stronger_subject_evidence:
                _append_unique_news_line(lines, seen_keys, lead_evidence_line, limit=limit)
                _append_unique_news_line(lines, seen_keys, structure_line, limit=limit)
            linked_quota = (
                max(limit - len(lines) - 1, 1)
                if structure_line and not lead_is_stronger_subject_evidence
                else max(limit - len(lines), 1)
            )
            linked_added = 0
            for item in linked_news_lines:
                before = len(lines)
                _append_unique_news_line(lines, seen_keys, _format_homepage_evidence_line(item), limit=limit)
                if len(lines) > before:
                    linked_added += 1
                if linked_added >= linked_quota:
                    break
            if not lead_is_stronger_subject_evidence:
                _append_unique_news_line(lines, seen_keys, lead_evidence_line, limit=limit)
                _append_unique_news_line(lines, seen_keys, structure_line, limit=limit)
            _append_unique_news_line(lines, seen_keys, digest_context_line, limit=limit)
            for item in summary_lines[:1]:
                _append_unique_news_line(lines, seen_keys, item, limit=limit)
        else:
            _append_unique_news_line(lines, seen_keys, lead_evidence_line, limit=limit)
            if not lead_evidence_line:
                _append_unique_news_line(lines, seen_keys, structure_line, limit=limit)
            linked_added = 0
            for item in linked_news_lines:
                before = len(lines)
                _append_unique_news_line(lines, seen_keys, _format_homepage_evidence_line(item), limit=limit)
                if len(lines) > before:
                    linked_added += 1
                if linked_added >= max(limit - 1, 1):
                    break
            if lead_evidence_line:
                _append_unique_news_line(lines, seen_keys, structure_line, limit=limit)
            _append_unique_news_line(lines, seen_keys, digest_context_line, limit=limit)
            if len(lines) < limit:
                for item in summary_lines[:1]:
                    _append_unique_news_line(lines, seen_keys, item, limit=limit)
    else:
        _append_unique_news_line(lines, seen_keys, lead_evidence_line, limit=limit)
        _append_unique_news_line(lines, seen_keys, structure_line, limit=limit)
        _append_unique_news_line(lines, seen_keys, digest_context_line, limit=limit)
        for item in plain_news_lines:
            _append_unique_news_line(lines, seen_keys, _format_homepage_evidence_line(item), limit=limit)
        for item in summary_lines[:1]:
            _append_unique_news_line(lines, seen_keys, item, limit=limit)
        if not digest_lines and not structure_line and not lead_evidence_line:
            _append_unique_news_line(lines, seen_keys, _event_digest_history_line(digest), limit=limit)
            if not _safe_text(digest.get("history_note")):
                _append_unique_news_line(lines, seen_keys, _event_digest_signal_line(digest), limit=limit)
    for item in raw_news_lines:
        _append_unique_news_line(lines, seen_keys, _format_homepage_evidence_line(item), limit=limit)
    for item in plain_news_lines:
        _append_unique_news_line(lines, seen_keys, _format_homepage_evidence_line(item), limit=limit)
    deduped: List[str] = []
    final_seen: set[str] = set()
    for item in lines:
        _append_unique_news_line(deduped, final_seen, item, limit=limit)
    if (
        structure_line
        and not lead_evidence_line
        and any("http://" in _safe_text(item) or "https://" in _safe_text(item) for item in deduped)
        and not any(_safe_text(item).startswith("结构证据：") for item in deduped)
    ):
        rebuilt = [structure_line]
        for item in deduped:
            if _safe_text(item) == structure_line:
                continue
            rebuilt.append(item)
        deduped = rebuilt[:limit]
        final_seen = {
            key
            for item in deduped
            if (key := _homepage_news_key(_safe_text(item)))
        }
    meaningful_non_clickable = bool(lead_evidence_line or structure_line or plain_news_lines or summary_lines or digest_lines)
    if not _has_clickable_homepage_evidence(deduped):
        if meaningful_non_clickable and len(deduped) >= limit:
            deduped = deduped[: max(limit - 1, 0)]
            final_seen = {
                key
                for item in deduped
                if (key := _homepage_news_key(_safe_text(item)))
            }
        if not deduped:
            _append_unique_news_line(deduped, final_seen, _no_intelligence_homepage_line(), limit=limit)
            _append_unique_news_line(deduped, final_seen, _missing_clickable_intelligence_line(), limit=limit)
        elif not raw_news_lines and not lead_evidence_line and _safe_text(digest.get("status")) == "待补充" and len(deduped) < limit:
            _append_unique_news_line(deduped, final_seen, _no_intelligence_homepage_line(), limit=limit)
        elif meaningful_non_clickable and len(deduped) < limit:
            _append_unique_news_line(deduped, final_seen, _missing_clickable_intelligence_line(), limit=limit)
    return deduped[:limit]


def _micro_lines(subject: Mapping[str, Any]) -> List[str]:
    dimensions = dict(subject.get("dimensions") or {})
    catalyst_web_review = dict(subject.get("catalyst_web_review") or dict(dimensions.get("catalyst") or {}).get("web_review") or {})
    strongest, weakest = _top_bottom_dimensions(dimensions)
    strongest_label = dict(DIMENSION_LABELS).get(strongest[0], strongest[0])
    weakest_label = dict(DIMENSION_LABELS).get(weakest[0], weakest[0])
    strongest_summary = _dimension_summary(dimensions, strongest[0]) or "当前是相对更能支撑继续看的那一项。"
    weakest_summary = _dimension_summary(dimensions, weakest[0]) or "这是当前最影响动作升级的一项。"
    catalyst_score = float(dict(dimensions.get("catalyst") or {}).get("score") or 0)
    relative_score = float(dict(dimensions.get("relative_strength") or {}).get("score") or 0)
    technical_signal_text = compact_technical_signal_text(subject.get("history"))
    contradiction = "逻辑没坏，但确认还没补齐。"
    if strongest[0] == "fundamental" and weakest[0] in {"technical", "relative_strength"}:
        contradiction = "底层逻辑不算差，但价格和动量还没把这层逻辑翻译成买点。"
    elif strongest[0] == "catalyst" and weakest[0] == "risk":
        contradiction = "事件并不弱，但风险收益比还没站到舒服的一侧。"
    elif weakest[0] == "catalyst":
        contradiction = "当前最大问题不是没故事，而是直接催化和确认还不够。"
    if relative_score >= 70 and catalyst_score <= 5:
        strongest_summary = (
            f"{strongest_summary.rstrip('。')}。"
            "但这层高分更多是在反映前一段主线和价格惯性的滞后结果，没有新增直接情报时它本身也会先回落。"
        ).strip()
        contradiction = "相对强弱分数还在，但它更多是在反映前一段主线惯性；新增直接情报没回来前，别把这个高分直接读成新一轮确认。"
    if catalyst_web_review.get("completed"):
        decision = _safe_text(catalyst_web_review.get("decision"))
        impact = list(catalyst_web_review.get("impact") or [])
        strongest_summary = strongest_summary
        weakest_summary = (
            f"联网复核后的结论是 `{decision or '已补充复核'}`。"
            + (f" {impact[0]}" if impact else "")
        ).strip()
        contradiction = "已经补完联网复核，但复核结论更多是在修正证据边界，不等于自动升级成可做买点。"
    if technical_signal_text:
        if strongest[0] == "technical" and technical_signal_text not in strongest_summary:
            strongest_summary = f"{strongest_summary} {technical_signal_text}".strip()
        elif weakest[0] == "technical" and technical_signal_text not in weakest_summary:
            weakest_summary = f"{weakest_summary} {technical_signal_text}".strip()
        elif technical_signal_text not in contradiction:
            contradiction = f"{contradiction} {technical_signal_text}".strip()
    metadata = dict(subject.get("metadata") or {})
    index_snapshot = dict(
        metadata.get("index_technical_snapshot")
        or dict(metadata.get("index_topic_bundle") or {}).get("technical_snapshot")
        or {}
    )
    index_trend = _safe_text(index_snapshot.get("trend_label"))
    index_momentum = _safe_text(index_snapshot.get("momentum_label"))
    fund_trend = _safe_text(metadata.get("fund_factor_trend_label"))
    fund_momentum = _safe_text(metadata.get("fund_factor_momentum_label"))
    if (
        _safe_text(subject.get("asset_type")) in {"cn_etf", "cn_fund"}
        and index_trend in {"修复中", "趋势偏强"}
        and fund_trend == "趋势偏弱"
    ):
        divergence_line = (
            f"还要承认一个分歧：跟踪指数现在是 `{index_trend}`"
            + (f" / `{index_momentum}`" if index_momentum else "")
            + f"，但产品层还是 `{fund_trend}`"
            + (f" / `{fund_momentum}`" if fund_momentum else "")
            + "；赛道背景先看指数，真要执行先以产品层修复为准。"
        )
        if divergence_line not in contradiction:
            contradiction = f"{contradiction} {divergence_line}".strip()
    asset_type = _safe_text(subject.get("asset_type"))
    technical_score = float(dict(dimensions.get("technical") or {}).get("score") or 0)
    if asset_type in {"cn_etf", "cn_fund", "cn_index"} and technical_score < 35 and technical_signal_text:
        technical_context = "技术面要拆开看：中期趋势或产品状态不等于短线买点；当前技术分低，主要卡在近端压力、假突破或赔率。"
        if technical_context not in contradiction:
            contradiction = f"{contradiction} {technical_context}".strip()
    return [
        f"现在最能支撑继续看的，是 `{strongest_label}`：{strongest_summary}",
        f"真正压住结论的，是 `{weakest_label}`：{weakest_summary}",
        f"这份稿当前最大的矛盾是：{contradiction}",
    ]


def _portfolio_overlap_homepage_line(subject: Mapping[str, Any]) -> str:
    summary = dict(subject.get("portfolio_overlap_summary") or {})
    overlap_label = _safe_text(summary.get("overlap_label"))
    summary_line = _safe_text(summary.get("summary_line")).rstrip("。；;，, ")
    style_hint = _safe_text(summary.get("style_priority_hint")).rstrip("。；;，, ")
    if not overlap_label and not summary_line and not style_hint:
        return ""
    lead = "和现有持仓的关系上"
    if overlap_label:
        lead = f"{lead}，这条更像 `{overlap_label}`"
    parts: List[str] = []
    if summary_line:
        parts.append(summary_line)
    if style_hint:
        parts.append(style_hint)
    if not parts:
        return f"{lead}。"
    if overlap_label:
        return f"{lead}：{'；'.join(parts)}。"
    return f"{lead}，{'；'.join(parts)}。"


def _strategy_background_confidence(subject: Mapping[str, Any]) -> Dict[str, Any]:
    embedded = dict(subject.get("strategy_background_confidence") or {})
    if embedded:
        return embedded
    symbol = _safe_text(subject.get("symbol"))
    if not symbol:
        return {}
    try:
        return dict(StrategyRepository().summarize_background_confidence(symbol) or {})
    except Exception:
        return {}


def _strategy_background_upgrade_guard_line(subject: Mapping[str, Any], *, observe_only: bool = False) -> str:
    if not observe_only:
        return ""
    confidence = _strategy_background_confidence(subject)
    if not confidence:
        return ""
    status = strategy_confidence_status({"strategy_background_confidence": confidence})
    reason = _safe_text(confidence.get("reason")) or _safe_text(confidence.get("summary"))
    if status == "degraded":
        return f"策略后台置信度当前是 `退化`。{reason} 观察稿先不要只凭题材热度或单日强势升级成动作。"
    if status == "watch":
        return f"策略后台置信度当前是 `观察`。{reason} 这次信号先只作辅助说明，不单靠它把观察稿升级成动作。"
    if status == "stable":
        return "策略后台置信度当前是 `稳定`，但它只算辅助加分；观察稿真要升级，仍要等当下确认条件一起满足。"
    return ""


def _action_lines(
    subject: Mapping[str, Any],
    *,
    observe_only: bool = False,
    event_digest: Mapping[str, Any] | None = None,
    soften_watch_levels: bool = False,
) -> List[str]:
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(subject.get("trade_state"))
    horizon = dict(action.get("horizon") or {})
    lines: List[str] = []
    direction = _safe_text(action.get("direction")) or trade_state or "观察为主"
    position = _safe_text(action.get("position"))
    stop = _safe_text(action.get("stop"))
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    try:
        target_ref = float(action.get("target_ref") or 0.0)
    except (TypeError, ValueError):
        target_ref = 0.0
    buy_range = _safe_text(action.get("buy_range"))
    trim_range = _safe_text(action.get("trim_range"))

    def _usable_range(text: str) -> str:
        line = _safe_text(text)
        if not line:
            return ""
        if any(token in line for token in ("暂不设", "先等右侧确认", "等待确认", "不设")):
            return ""
        return line

    def _usable_position(text: str) -> str:
        line = _safe_text(text)
        if not line:
            return ""
        if any(token in line for token in ("暂不", "观察", "回避", "等待", "不出手")):
            return ""
        return line

    buy_range = _usable_range(buy_range)
    trim_range = _usable_range(trim_range)
    position = _usable_position(position)
    asset_type = _safe_text(subject.get("asset_type"))
    near_resistance = _stock_near_resistance_level(subject)
    watch_levels = ""
    if asset_type == "cn_stock" and near_resistance > 0 and stop_ref > 0:
        watch_levels = f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；近端压力先看 `{near_resistance:.3f}` 能不能放量消化"
    elif asset_type == "cn_stock" and near_resistance > 0:
        watch_levels = f"近端压力先看 `{near_resistance:.3f}` 能不能放量消化"
    elif near_resistance > 0 and stop_ref > 0:
        watch_levels = f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；近端压力先看 `{near_resistance:.3f}` 能不能放量消化"
    elif near_resistance > 0:
        watch_levels = f"近端压力先看 `{near_resistance:.3f}` 能不能放量消化"
    elif buy_range and trim_range:
        watch_levels = f"回踩先看 `{buy_range}` 一带的承接；反弹再看 `{trim_range}` 一带的承压"
    elif stop_ref > 0 and target_ref > 0:
        watch_levels = f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；上沿先看 `{target_ref:.3f}` 附近能不能放量突破"
    elif buy_range:
        watch_levels = f"先看 `{buy_range}` 一带的承接"
    elif stop_ref > 0:
        watch_levels = f"先看 `{stop_ref:.3f}` 上方能不能稳住"
    entry_focus = (
        _stock_observe_entry_focus(subject, action)
        if asset_type == "cn_stock"
        else (_entry_focus_text(action.get("entry")) or "技术确认和相对强弱是否一起改善")
    )

    soft_observe = observe_only or any(token in f"{direction} {trade_state}" for token in ("观察", "暂不", "回避"))
    digest_action_line = event_digest_action_line(event_digest or {}, observe_only=soft_observe)
    strategy_guard_line = _strategy_background_upgrade_guard_line(subject, observe_only=soft_observe)

    qualitative_watch_levels = ""
    if watch_levels:
        if buy_range and trim_range:
            qualitative_watch_levels = "回踩先看关键支撑承接；反弹再看前高/压力位能否放量站上"
        elif buy_range and target_ref > 0:
            qualitative_watch_levels = "回踩先看关键支撑承接；如果继续上行，再看前高/压力位能否放量突破"
        elif buy_range:
            qualitative_watch_levels = "先看关键支撑承接"
        elif stop_ref > 0 and target_ref > 0:
            qualitative_watch_levels = "下沿先看关键支撑能不能稳住；上沿先看前高/压力位能否放量突破"
        elif stop_ref > 0:
            qualitative_watch_levels = "先看关键下沿能不能稳住"
        else:
            qualitative_watch_levels = "先把关键位当观察点，不急着写成机械价位卡"

    if soft_observe:
        if digest_action_line:
            lines.append(digest_action_line)
        if strategy_guard_line:
            lines.append(strategy_guard_line)
        lines.append(f"空仓先别急着直接找买点，升级触发器先看 {_homepage_focus_text(entry_focus)}。")
        if asset_type == "cn_stock":
            position_line = _stock_observe_position_line(position)
            lines.append(f"已有仓位先按观察名单理解，确认前不因为今天这份稿去追补仓；仓位先记：{position_line}")
            if watch_levels:
                lines.append(f"关键位先看：{watch_levels}。")
            if stop_ref > 0:
                lines.append(f"失效位先看 `{stop_ref:.3f}`，跌破就先处理，不把观察稿硬扛成持仓。")
            elif stop:
                lines.append(f"失效处理先按 `{stop}`。")
        else:
            if watch_levels:
                watch_line = qualitative_watch_levels if soften_watch_levels and qualitative_watch_levels else watch_levels
                lines.append(f"关键位先看：{watch_line}。")
            elif qualitative_watch_levels:
                lines.append(f"先把关键位当观察点看：{qualitative_watch_levels}。")
            lines.append(
                "已有仓位先按观察名单理解，确认前不因为今天这份稿去追补仓"
                "；真正的执行升级仍要等确认条件先回来。"
            )
    else:
        if digest_action_line:
            lines.append(digest_action_line)
        lines.append(f"如果要参与，先按 {_homepage_focus_text(entry_focus or '回踩确认')} 这类确认去等，而不是把这条结论当成当天就要追进去。")
        lines.append(f"仓位先按 `{_safe_text(action.get('position')) or '小仓分批'}`，止损按 `{_safe_text(action.get('stop')) or '关键支撑失效'}` 管理。")
        if watch_levels:
            lines.append(f"关键位先看 {watch_levels}。")
    if _safe_text(horizon.get("fit_reason")):
        lines.append(f"当前更适合按 `{_safe_text(horizon.get('label')) or '当前周期'}` 理解：{_safe_text(horizon.get('fit_reason'))}")
    return lines[:4]


def _decision_track_line(subject: Mapping[str, Any], playbook: Mapping[str, Any]) -> str:
    label = (
        _safe_text(playbook.get("label"))
        or subject_theme_label(subject, allow_day_theme=True)
        or _safe_text(dict(subject.get("day_theme") or {}).get("label"))
        or "当前主题"
    )
    role = _safe_text(playbook.get("trading_role_label"))
    if role == "主线核心":
        return f"赛道判断：`{label}` 仍是当前更该优先看的主线方向，不要把赛道成立和具体买点混成一个判断。"
    if role == "主线扩散":
        return f"赛道判断：`{label}` 仍在主线里，但当前更像主线向细分扩散的分支，不等于最强主攻位。"
    if role == "强波段":
        return f"赛道判断：`{label}` 更像副主线/强波段，方向没坏，但打法要和主攻仓分开。"
    if role == "轮动":
        return f"赛道判断：`{label}` 当前更像轮动方向，不宜直接当主攻线。"
    return f"赛道判断：先按 `{label}` 理解，不把背景线或题材热度直接翻译成交易动作。"


def _decision_vehicle_line(subject: Mapping[str, Any], playbook: Mapping[str, Any]) -> str:
    subject_label = _subject_display_label(subject)
    dimensions = dict(subject.get("dimensions") or {})
    weakest: tuple[str, float] = ("risk", 0.0)
    if dimensions:
        _, weakest = _top_bottom_dimensions(dimensions)
    weakest_label = dict(DIMENSION_LABELS).get(weakest[0], weakest[0])
    role = _safe_text(playbook.get("trading_role_label"))
    asset_type = _safe_text(subject.get("asset_type"))
    carrier_label = "主攻载体" if asset_type in {"cn_etf", "cn_fund"} else "主攻票"
    if role in {"主线核心", "主线扩散"}:
        return (
            f"载体判断：赛道方向和 `{subject_label}` 要分开看；前者没坏，"
            f"但后者当前最卡的是 `{weakest_label}`，所以还不是最顺手的 `{carrier_label}`。"
        )
    if role == "轮动":
        return f"载体判断：`{subject_label}` 更像这条线里的轮动表达工具，适合观察或回踩参与，不宜当第一主攻载体。"
    if role == "强波段":
        return f"载体判断：`{subject_label}` 更像用来表达这条副主线/强波段，不适合直接包装成长时间主攻仓。"
    return f"载体判断：`{subject_label}` 当前更像观察对象，先等确认，再决定能不能升级成真正的执行载体。"


def _decision_execution_card_lines(
    subject: Mapping[str, Any],
    playbook: Mapping[str, Any],
    *,
    observe_only: bool = False,
) -> List[str]:
    action = dict(subject.get("action") or {})
    role = _safe_text(playbook.get("trading_role_label"))
    position_label = _safe_text(playbook.get("trading_position_label"))
    buy_range = _safe_text(action.get("buy_range"))
    trim_range = _safe_text(action.get("trim_range"))
    raw_position = _safe_text(action.get("position"))
    stop_text = _safe_text(action.get("stop"))
    target_text = _safe_text(action.get("target"))
    asset_type = _safe_text(subject.get("asset_type"))
    near_resistance = _stock_near_resistance_level(subject)
    entry_focus = (
        _stock_observe_entry_focus(subject, action)
        if asset_type == "cn_stock"
        else (_entry_focus_text(action.get("entry")) or "右侧确认")
    )
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    try:
        target_ref = float(action.get("target_ref") or 0.0)
    except (TypeError, ValueError):
        target_ref = 0.0

    def _usable_range(text: str) -> str:
        value = _safe_text(text)
        if not value:
            return ""
        if any(token in value for token in ("暂不设", "先等右侧确认", "等待确认", "不设")):
            return ""
        return value

    buy_range = _usable_range(buy_range)
    trim_range = _usable_range(trim_range)
    lines: List[str] = []
    exclusion_reasons = [str(item).strip() for item in list(subject.get("exclusion_reasons") or []) if str(item).strip()]
    if bool(subject.get("excluded")) and exclusion_reasons:
        lines.append(
            "执行卡：已触发硬排除 "
            f"`{'；'.join(exclusion_reasons[:2])}`，这页只能当观察和风险提示，不能按正式推荐执行。"
        )
    trigger_line = f"执行卡：{'没触发前只记' if observe_only else '触发前先等'} {_homepage_focus_text(entry_focus)}"
    if buy_range and not observe_only:
        trigger_line += f"，优先看 `{buy_range}` 一带承接"
    if role == "主线扩散":
        if observe_only:
            trigger_line += f"；这类 `{position_label or '卫星仓'}` 真要升级，也是在回踩承接没坏、相对强弱不丢后再小仓试，不是现在就挂单位"
        else:
            trigger_line += f"；这类 `{position_label or '卫星仓'}` 不必等所有确认都齐，只要回踩承接没坏、相对强弱不丢，就可以先用小仓试"
    trigger_line += "。"
    lines.append(trigger_line)
    if observe_only:
        if asset_type == "cn_stock":
            position_line = _stock_observe_position_line(raw_position).rstrip("。")
            lines.append(f"执行卡：{position_line}。")
            if stop_ref > 0:
                if near_resistance > 0:
                    lines.append(f"执行卡：失效位先看 `{stop_ref:.3f}`；第一段先看 `{near_resistance:.3f}` 近端压力能不能消化。")
                elif trim_range:
                    lines.append(f"执行卡：失效位先看 `{stop_ref:.3f}`；第一次兑现先看 `{trim_range}` 一带。")
                elif target_ref > 0:
                    lines.append(f"执行卡：失效位先看 `{stop_ref:.3f}`；第一次兑现先看 `{target_ref:.3f}` 附近。")
                else:
                    lines.append(f"执行卡：失效位先看 `{stop_ref:.3f}`，别把观察稿硬扛成持仓。")
            elif stop_text:
                lines.append(f"执行卡：失效处理先按 `{stop_text}`。")
            return lines
        lines.append("执行卡：观察稿阶段先记触发、失效和第一次兑现框架，不把买点、止损和目标写成机械挂单位。")
        return lines
    if role == "主线扩散" or buy_range or trim_range or stop_text or target_text:
        lines.append("执行卡：这些价位先按观察带/承压带理解，不按机械挂单位；真正先看的还是触发、失效和第一次兑现。")
    if stop_text:
        lines.append(f"执行卡：失效位先看 `{stop_text}`，别把“逻辑还在”当成可以硬扛回撤。")
    if trim_range:
        lines.append(f"执行卡：第一次减仓先看 `{trim_range}` 一带；只有真正站稳后，才谈第二目标。")
    elif target_text:
        lines.append(f"执行卡：目标别机械地一步看到 `{target_text}`，先看第一段承压/减仓能不能兑现。")
    return lines


def _decision_tail_risk_line(subject: Mapping[str, Any]) -> str:
    dimensions = dict(subject.get("dimensions") or {})
    macro_summary = _dimension_summary(dimensions, "macro")
    risk_summary = _dimension_summary(dimensions, "risk")
    if macro_summary:
        return f"尾部风险：macro 这里只作尾部约束，不单独改写赛道判断；真正要防的是 `{macro_summary}` 持续恶化。"
    if risk_summary:
        return f"尾部风险：真正要防的不是“逻辑还在”，而是 `{risk_summary}`；这层如果继续恶化，执行要先降档。"
    return "尾部风险：即使方向判断没坏，执行上也要优先尊重失效位、仓位和确认条件。"


def _inject_homepage_contract_line(
    items: Sequence[str],
    line: str,
    *,
    preserve_prefixes: Sequence[str] = (),
) -> List[str]:
    rows = [_safe_text(item) for item in list(items or []) if _safe_text(item)]
    decision_line = _safe_text(line)
    if not decision_line or decision_line in rows:
        return rows
    insert_at = 0
    while insert_at < len(rows):
        current = rows[insert_at]
        if any(current.startswith(prefix) for prefix in preserve_prefixes):
            insert_at += 1
            continue
        break
    return [*rows[:insert_at], decision_line, *rows[insert_at:]]


def _apply_homepage_decision_contract(
    *,
    subject: Mapping[str, Any],
    playbook: Mapping[str, Any],
    macro_lines: Sequence[str],
    theme_lines: Sequence[str],
    micro_lines: Sequence[str],
    action_lines: Sequence[str],
    observe_only: bool = False,
) -> tuple[List[str], List[str], List[str], List[str]]:
    macro_items = [item for item in list(macro_lines or []) if _safe_text(item)]
    theme_items = [item for item in list(theme_lines or []) if _safe_text(item)]
    micro_items = [item for item in list(micro_lines or []) if _safe_text(item)]
    action_items = [item for item in list(action_lines or []) if _safe_text(item)]
    macro_items = list(dict.fromkeys(macro_items))
    theme_items = _inject_homepage_contract_line(
        list(dict.fromkeys(theme_items)),
        _decision_track_line(subject, playbook),
        preserve_prefixes=("硬分类：",),
    )
    micro_items = _inject_homepage_contract_line(
        list(dict.fromkeys(micro_items)),
        _decision_vehicle_line(subject, playbook),
        preserve_prefixes=(
            "策略后台置信度：",
            "本页重点分析对象是",
            "组合里",
            "当前更像在比较谁更接近触发条件",
        ),
    )
    action_items = list(
        dict.fromkeys(
            [
                *_decision_execution_card_lines(subject, playbook, observe_only=observe_only),
                *action_items,
                _decision_tail_risk_line(subject),
            ]
        )
    )
    return macro_items, theme_items, micro_items, action_items


def _subject_display_label(subject: Mapping[str, Any]) -> str:
    name = _safe_text(subject.get("name"))
    symbol = _safe_text(subject.get("symbol"))
    if name and symbol:
        return f"{name} ({symbol})"
    return name or symbol or "当前对象"


def _normalized_theme_label(value: Any) -> str:
    text = _safe_text(value).lower()
    if not text:
        return ""
    return re.sub(r"[\s`/、,，；;：:（）()\-]+", "", text)


def _same_theme_label(left: Any, right: Any) -> bool:
    left_text = _normalized_theme_label(left)
    right_text = _normalized_theme_label(right)
    return bool(left_text and right_text and left_text == right_text)


def _stock_pick_total_judgment_line(
    market_label: str,
    day_theme: str,
    subject_theme: str,
    *,
    has_actionable: bool,
) -> str:
    prefix = f"今天这份个股{'推荐' if has_actionable else '观察'}稿更适合按 `{market_label}` 范围理解；"
    if day_theme and subject_theme and not _same_theme_label(day_theme, subject_theme):
        if has_actionable:
            return prefix + f"当前市场主线背景偏 `{day_theme}`，但这页真正先看 `{subject_theme}` 这条线里谁已经先走到可执行边界。"
        return prefix + f"当前市场主线背景偏 `{day_theme}`，但这页真正先看 `{subject_theme}` 这条线里谁更接近确认边界。"
    if day_theme:
        progress = (
            "已经有少数标的从方向判断走到可执行边界。"
            if has_actionable
            else "主题和方向还在，但大多数标的仍缺价格与动量确认。"
        )
        return prefix + f"当前主线偏 `{day_theme}`，{progress}"
    progress = "已经有少数标的开始接近执行。" if has_actionable else "主题和方向还在，但大多数标的仍缺价格与动量确认。"
    return prefix + f"当前先按结构性轮动理解，{progress}"


def _conclusion_line(subject: Mapping[str, Any], *, observe_only: bool = False) -> str:
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(subject.get("trade_state")) or _safe_text(action.get("direction"))
    display_state = trade_state or "观察为主"
    if observe_only and "回避" in display_state and "观察" not in display_state:
        display_state = "观察为主（偏回避）"
    if observe_only or any(token in trade_state for token in ("观察", "暂不", "回避")):
        return f"结论：今天更适合把它放在观察名单里，而不是直接升级成交易动作；当前建议仍是 `{display_state}`。"
    return f"结论：这条方向可以继续跟，但执行上仍要尊重 `{trade_state or action.get('direction', '分批参与')}` 这层边界。"


def _soften_stock_analysis_action_lines(action_lines: Sequence[str]) -> List[str]:
    softened: List[str] = []
    for line in action_lines:
        text = _safe_text(line)
        if not text:
            continue
        if any(token in text for token in ("首次仓位按", "止损按", "下沿先看", "上沿先看")):
            continue
        softened.append(text)
    template_line = "真正升级前，只按首页的触发、建仓、仓位和失效条件复核；没补到新增催化前，不把观察稿当强看多。"
    if softened and template_line not in softened:
        softened.append(template_line)
    return softened[:4]


def _stock_analysis_conclusion_line(subject: Mapping[str, Any]) -> str:
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(subject.get("trade_state")) or _safe_text(action.get("direction"))
    if "回避" in trade_state and "观察" not in trade_state:
        return "结论：今天更适合把它放在观察名单里，而不是直接升级成交易动作；当前先按 `观察为主（偏回避）` 理解。"
    return _conclusion_line(subject)


def _market_hint_from_context(selection_context: Mapping[str, Any], regime: Mapping[str, Any], day_theme: str) -> str:
    flow = dict(dict(selection_context.get("proxy_contract") or {}).get("market_flow") or {})
    interpretation = _safe_text(flow.get("interpretation"))
    if interpretation:
        return interpretation
    if day_theme:
        return f"板块轮动上，今天更该先围绕 `{day_theme}` 理解顺风和逆风，而不是把所有题材都当成全面 risk-on。"
    regime_name = _safe_text(regime.get("current_regime"))
    if regime_name:
        return f"风格和流动性判断先服从 `{REGIME_LABELS.get(regime_name, regime_name)}` 这层大背景。"
    return ""


def _build_homepage_v2(
    *,
    summary: str,
    macro_lines: Sequence[str],
    theme_lines: Sequence[str],
    news_lines: Sequence[str],
    sentiment_lines: Sequence[str],
    micro_lines: Sequence[str],
    action_lines: Sequence[str],
    conclusion: str,
) -> Dict[str, Any]:
    return {
        "version": "thesis-first-v2",
        "total_judgment": summary,
        "macro_lines": [item for item in macro_lines if _safe_text(item)],
        "theme_lines": [item for item in theme_lines if _safe_text(item)],
        "news_lines": [_ensure_homepage_news_signal_bundle(item) for item in news_lines if _safe_text(item)],
        "sentiment_lines": [item for item in sentiment_lines if _safe_text(item)],
        "micro_lines": [item for item in micro_lines if _safe_text(item)],
        "action_lines": [item for item in action_lines if _safe_text(item)],
        "conclusion": conclusion,
    }


def _dict_snapshot(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _section_snapshot(value: Any) -> Dict[str, Any] | List[Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (str, bytes)):
        return {}
    if isinstance(value, Sequence):
        return list(value)
    return {}


def _json_safe_snapshot(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe_snapshot(item) for key, item in value.items()}
    if isinstance(value, (str, bytes)):
        return _safe_text(value)
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_snapshot(item) for item in value]
    class_name = value.__class__.__name__
    if class_name == "DataFrame" and hasattr(value, "tail") and hasattr(value, "to_dict"):
        try:
            return value.tail(20).to_dict(orient="records")
        except Exception:  # pragma: no cover - defensive sidecar cleanup
            return _safe_text(value)
    if class_name == "Series" and hasattr(value, "to_dict"):
        try:
            return _json_safe_snapshot(value.to_dict())
        except Exception:  # pragma: no cover - defensive sidecar cleanup
            return _safe_text(value)
    return _safe_text(value)


def _editor_subject_snapshot(subject: Mapping[str, Any], *, fallback_asset_type: str = "") -> Dict[str, Any]:
    payload = dict(subject or {})
    asset_type = _safe_text(payload.get("asset_type") or fallback_asset_type)
    dimensions_snapshot = _dict_snapshot(payload.get("dimensions"))
    action_snapshot = _dict_snapshot(payload.get("action"))
    horizon_expression = build_horizon_expression_packet(_dict_snapshot(action_snapshot.get("horizon")))
    if isinstance(dimensions_snapshot, dict):
        dimensions_snapshot = _sanitize_subject_snapshot_dimensions(
            {**payload, "asset_type": asset_type},
            dimensions_snapshot,
        )
    snapshot = {
        "name": _safe_text(payload.get("name")),
        "symbol": _safe_text(payload.get("symbol")),
        "asset_type": asset_type,
        "generated_at": _safe_text(payload.get("generated_at")),
        "taxonomy_summary": _safe_text(payload.get("taxonomy_summary")),
        "trade_state": _safe_text(payload.get("trade_state"))
        or _safe_text(_dict_snapshot(_dict_snapshot(payload.get("narrative")).get("judgment")).get("state")),
        "day_theme": _dict_snapshot(payload.get("day_theme")),
        "action": action_snapshot,
        "horizon_expression": horizon_expression,
        "narrative": _dict_snapshot(payload.get("narrative")),
        "dimensions": dimensions_snapshot,
        "metadata": _dict_snapshot(payload.get("metadata")),
        "theme_playbook": _dict_snapshot(payload.get("theme_playbook")),
        "fund_sections": _section_snapshot(payload.get("fund_sections")),
        "market_event_rows": list(payload.get("market_event_rows") or [])[:6],
        "notes": [str(item).strip() for item in list(payload.get("notes") or []) if str(item).strip()],
    }
    cleaned: Dict[str, Any] = {}
    for key, value in snapshot.items():
        safe_value = _json_safe_snapshot(value)
        if safe_value in ("", {}, [], None):
            continue
        cleaned[key] = safe_value
    return cleaned


def _sanitize_subject_snapshot_dimensions(subject: Mapping[str, Any], dimensions: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot = dict(dimensions or {})
    asset_type = _safe_text(subject.get("asset_type")).lower()
    if asset_type not in {"cn_etf", "cn_fund", "cn_index"}:
        return snapshot
    catalyst = dict(snapshot.get("catalyst") or {})
    theme_news = list(catalyst.get("theme_news") or [])
    if not theme_news:
        return snapshot
    filtered_theme_news: List[Any] = []
    for item in theme_news:
        if isinstance(item, Mapping):
            row = dict(item or {})
            if _safe_text(row.get("title")) and _should_skip_instrument_proxy_news(subject, row):
                continue
            filtered_theme_news.append(row)
        else:
            filtered_theme_news.append(item)
    if filtered_theme_news:
        catalyst["theme_news"] = filtered_theme_news
    else:
        catalyst.pop("theme_news", None)
    snapshot["catalyst"] = catalyst
    return snapshot


def _enrich_subject_theme_context(context: Mapping[str, Any], subject: Mapping[str, Any]) -> Dict[str, Any]:
    enriched = dict(context or {})
    subject_payload = dict(subject or {})
    if not enriched:
        return {}
    enriched.update(infer_theme_trading_role(enriched, subject_payload, subject=subject_payload))
    return enriched


def _enrich_theme_context_with_event_digest(
    context: Mapping[str, Any],
    subject: Mapping[str, Any],
    event_digest: Mapping[str, Any],
) -> Dict[str, Any]:
    if not context:
        return {}
    subject_payload = {**dict(subject or {}), "event_digest": dict(event_digest or {})}
    return _enrich_subject_theme_context(context, subject_payload)


def _subject_theme_context(subject: Mapping[str, Any], *, explicit_key: str = "") -> Dict[str, Any]:
    subject_payload = dict(subject or {})
    metadata = dict(subject.get("metadata") or {})
    fund_profile = dict(subject.get("fund_profile") or {})
    fund_overview = dict(fund_profile.get("overview") or {})
    existing_context = dict(subject.get("theme_playbook") or {})
    existing_level = _safe_text(existing_context.get("playbook_level"))
    if existing_context.get("key") and existing_level != "sector":
        return _enrich_subject_theme_context(existing_context, subject_payload)
    asset_type = _safe_text(subject_payload.get("asset_type"))
    fund_like = asset_type in {"cn_etf", "cn_fund", "cn_index"}
    benchmark_hint = _safe_text(
        metadata.get("tracked_index_name")
        or metadata.get("benchmark")
        or metadata.get("benchmark_name")
        or metadata.get("index_framework_label")
        or subject.get("benchmark_name")
        or fund_overview.get("业绩比较基准")
        or fund_overview.get("跟踪标的")
    )
    if fund_like:
        sector_text = _safe_text(metadata.get("sector"))
        industry_text = _safe_text(metadata.get("industry_framework_label"))
        day_theme_label = _safe_text(dict(subject.get("day_theme") or {}).get("label"))
        taxonomy = dict(metadata.get("taxonomy") or metadata.get("fund_taxonomy") or {})
        taxonomy_profile = dict(metadata.get("theme_profile") or taxonomy.get("theme_profile") or {})
        theme_profile_terms = [
            item
            for item in (
                taxonomy_profile.get("primary_chain"),
                taxonomy_profile.get("theme_family"),
                taxonomy_profile.get("theme_role"),
                metadata.get("primary_chain"),
                metadata.get("theme_family"),
                metadata.get("theme_role"),
                taxonomy.get("primary_chain"),
                taxonomy.get("theme_family"),
                taxonomy.get("theme_role"),
            )
            if _safe_text(item)
        ]
        for sequence_key in ("evidence_keywords", "preferred_sector_aliases", "mainline_tags"):
            for item in list(metadata.get(sequence_key) or taxonomy.get(sequence_key) or taxonomy_profile.get(sequence_key) or []):
                if _safe_text(item):
                    theme_profile_terms.append(item)
        raw_chain_nodes = list(metadata.get("chain_nodes") or taxonomy.get("chain_nodes") or taxonomy_profile.get("chain_nodes") or [])
        # Fund/ETF chain_nodes are often noisy expansion hints. Only let them
        # steer identity when a structured theme profile supports them, or when
        # the harder index/sector identity is missing.
        chain_nodes = raw_chain_nodes if theme_profile_terms or not (sector_text or benchmark_hint or industry_text) else []
        identity_metadata = {
            "name": subject.get("name"),
            "symbol": subject.get("symbol"),
            "sector": sector_text,
            "industry_framework_label": industry_text,
            "tracked_index_name": metadata.get("tracked_index_name"),
            "benchmark": metadata.get("benchmark") or benchmark_hint,
            "benchmark_name": metadata.get("benchmark_name"),
            "index_framework_label": metadata.get("index_framework_label"),
            "primary_chain": metadata.get("primary_chain") or taxonomy.get("primary_chain") or taxonomy_profile.get("primary_chain"),
            "theme_family": metadata.get("theme_family") or taxonomy.get("theme_family") or taxonomy_profile.get("theme_family"),
            "theme_role": metadata.get("theme_role") or taxonomy.get("theme_role") or taxonomy_profile.get("theme_role"),
            "theme_profile_terms": theme_profile_terms,
            "chain_nodes": chain_nodes,
            "evidence_keywords": metadata.get("evidence_keywords")
            or taxonomy.get("evidence_keywords")
            or taxonomy_profile.get("evidence_keywords"),
            "preferred_sector_aliases": metadata.get("preferred_sector_aliases")
            or taxonomy.get("preferred_sector_aliases")
            or taxonomy_profile.get("preferred_sector_aliases"),
            "mainline_tags": metadata.get("mainline_tags")
            or taxonomy.get("mainline_tags")
            or taxonomy_profile.get("mainline_tags"),
            "main_business": benchmark_hint,
            "tushare_theme_industry": metadata.get("tushare_theme_industry"),
        }
        base_values = (
            explicit_key,
            identity_metadata,
            subject.get("name"),
            subject.get("symbol"),
            sector_text,
            industry_text,
            benchmark_hint,
            chain_nodes,
            theme_profile_terms,
            subject.get("taxonomy_summary"),
            subject.get("fund_sections"),
        )
        context_values = (
            subject.get("notes"),
            day_theme_label,
            dict(subject.get("narrative") or {}).get("headline"),
            dict(subject.get("narrative") or {}).get("playbook"),
        )
    else:
        base_values = (
            explicit_key,
            metadata,
            subject.get("name"),
            subject.get("symbol"),
            metadata.get("sector"),
            metadata.get("industry_framework_label"),
            metadata.get("chain_nodes"),
            metadata.get("business_scope"),
            metadata.get("company_intro"),
            subject.get("taxonomy_summary"),
            subject.get("fund_sections"),
        )
        context_values = (
            subject.get("notes"),
            dict(subject.get("day_theme") or {}).get("label"),
            dict(subject.get("narrative") or {}).get("headline"),
            dict(subject.get("narrative") or {}).get("playbook"),
        )
    rebuilt_identity_context = build_theme_playbook_context(*base_values)
    identity_context = rebuilt_identity_context or existing_context
    context_context = build_theme_playbook_context(
        subject.get("name"),
        metadata.get("sector"),
        metadata.get("industry_framework_label"),
        [] if fund_like else metadata.get("chain_nodes"),
        *context_values,
    )
    if identity_context.get("key") and _safe_text(identity_context.get("playbook_level")) != "sector":
        if (
            _safe_text(context_context.get("theme_match_status")) == "ambiguous_conflict"
            and _safe_text(context_context.get("playbook_level")) == "sector"
        ):
            return _enrich_subject_theme_context(context_context, subject_payload)
        return _enrich_subject_theme_context(identity_context, subject_payload)
    if _safe_text(identity_context.get("playbook_level")) == "sector":
        enriched_context = dict(identity_context)
        hard_sector_key = _safe_text(identity_context.get("hard_sector_key"))
        if _safe_text(context_context.get("theme_match_status")) == "ambiguous_conflict":
            enriched_context["theme_match_status"] = context_context.get("theme_match_status", "")
            enriched_context["theme_match_reason"] = context_context.get("theme_match_reason", "")
            enriched_context["theme_match_candidates"] = list(context_context.get("theme_match_candidates") or [])
        if hard_sector_key:
            bridge_items = sector_subtheme_bridge_items(
                hard_sector_key,
                *base_values,
                *context_values,
            )
            bridge_summary = summarize_sector_subtheme_bridge(bridge_items)
            enriched_context["subtheme_bridge"] = bridge_items
            enriched_context["subtheme_bridge_confidence"] = bridge_summary.get("confidence", "none")
            enriched_context["subtheme_bridge_reason"] = bridge_summary.get("reason", "")
            enriched_context["subtheme_bridge_top_key"] = bridge_summary.get("top_key", "")
            enriched_context["subtheme_bridge_top_label"] = bridge_summary.get("top_label", "")
        return _enrich_subject_theme_context(enriched_context, subject_payload)
    own_context = build_theme_playbook_context(
        *base_values,
        subject.get("notes"),
    )
    if own_context.get("key"):
        return _enrich_subject_theme_context(own_context, subject_payload)
    return _enrich_subject_theme_context(
        build_theme_playbook_context(
        *base_values,
        subject.get("notes"),
        dict(subject.get("day_theme") or {}).get("label"),
        dict(subject.get("narrative") or {}).get("headline"),
        dict(subject.get("narrative") or {}).get("playbook"),
        ),
        subject_payload,
    )


def _theme_lines(playbook: Mapping[str, Any], subject: Mapping[str, Any]) -> List[str]:
    if not playbook:
        return ["这只标的所在主题当前没有命中 playbook，首页更应该老实依赖当天事实层，不要硬编主题故事。"]
    lines: List[str] = []
    metadata = dict(subject.get("metadata") or {})
    taxonomy = dict(metadata.get("taxonomy") or metadata.get("fund_taxonomy") or {})
    taxonomy_profile = dict(metadata.get("theme_profile") or taxonomy.get("theme_profile") or {})
    hard_sector = _safe_text(playbook.get("hard_sector_label"))
    theme_family = _safe_text(playbook.get("theme_family"))
    playbook_level = _safe_text(playbook.get("playbook_level"))
    transmission = list(playbook.get("transmission_path") or [])
    stage_pattern = list(playbook.get("stage_pattern") or [])
    crowding = list(playbook.get("rotation_and_crowding") or [])
    falsifiers = list(playbook.get("falsifiers") or [])
    subtheme_bridge = [dict(item) for item in list(playbook.get("subtheme_bridge") or []) if dict(item)]
    bridge_confidence = _safe_text(playbook.get("subtheme_bridge_confidence")) or "none"
    bridge_reason = _safe_text(playbook.get("subtheme_bridge_reason"))
    bridge_top_label = _safe_text(playbook.get("subtheme_bridge_top_label"))
    theme_match_status = _safe_text(playbook.get("theme_match_status"))
    theme_match_reason = _safe_text(playbook.get("theme_match_reason"))
    theme_match_candidates = [str(item).strip() for item in list(playbook.get("theme_match_candidates") or []) if str(item).strip()]
    if playbook_level == "sector" and hard_sector:
        lines.append(f"当前更适合先按 `{hard_sector}` 行业层去理解，先回答盈利周期、政策和风格顺逆风，再决定要不要往更细主题上落。")
    elif hard_sector and theme_family:
        lines.append(f"从硬分类看，它更接近 `{hard_sector}`；从软主题看，这次更像一条 `{theme_family}` 线。")
    elif hard_sector:
        lines.append(f"从硬分类看，它更接近 `{hard_sector}`，这决定了它不该和所有热门题材混成同一种写法。")
    taxonomy_sector = _safe_text(metadata.get("sector") or taxonomy.get("sector"))
    primary_chain = _safe_text(metadata.get("primary_chain") or taxonomy.get("primary_chain") or taxonomy_profile.get("primary_chain"))
    theme_role = _safe_text(metadata.get("theme_role") or taxonomy.get("theme_role") or taxonomy_profile.get("theme_role"))
    asset_type = _safe_text(subject.get("asset_type"))
    if asset_type in {"cn_etf", "cn_fund", "cn_index"} and primary_chain:
        role_suffix = f"，链路角色是 `{theme_role}`" if theme_role else ""
        sector_prefix = f"`{taxonomy_sector}` / " if taxonomy_sector else ""
        lines.append(
            f"标准分类补充：产品画像把它落到 {sector_prefix}`{primary_chain}`{role_suffix}；"
            "如果基金名和持仓暴露不一致，优先按真实主链条解释。"
        )
    hint = playbook_hint_line(playbook)
    if hint:
        lines.append(hint)
    if playbook_level == "sector" and theme_match_status == "ambiguous_conflict" and theme_match_candidates:
        lines.append(f"当前先不要把它硬写成单一细主题，因为 `{' / '.join(theme_match_candidates[:3])}` 这几条线还在打架。")
        if theme_match_reason:
            lines.append(f"冲突原因：{theme_match_reason}")
    if playbook_level == "sector" and subtheme_bridge:
        bridge_labels = " / ".join(f"`{_safe_text(item.get('label'))}`" for item in subtheme_bridge[:3] if _safe_text(item.get("label")))
        if bridge_labels:
            if bridge_confidence == "high" and bridge_top_label:
                lines.append(f"结合当前上下文，行业层内部已经更偏向 `{bridge_top_label}` 这条细分线，但在缺直接催化或更硬验证前，正文仍先按行业层来写。")
            elif bridge_confidence == "medium" and bridge_top_label:
                lines.append(f"结合当前上下文，可优先留意 `{bridge_top_label}` 这条细分线；但这层线索还不够把行业层直接改写成已确认主题。")
            elif bridge_confidence == "low" and bridge_top_label:
                lines.append(f"当前只出现了偏向 `{bridge_top_label}` 的单点线索，这更像观察方向，不足以下钻成确定主题。")
            else:
                lines.append(f"如果后续催化继续往细分方向收敛，优先看 {bridge_labels} 这些 repo 内已定义的下钻方向，再决定要不要从行业层切到细主题。")
            if bridge_reason:
                lines.append(f"这层下钻判断主要依据：{bridge_reason}")
    if transmission:
        lines.append(f"更像样的理解路径是：{transmission[0]}")
    if crowding:
        lines.append(_crowding_homepage_line(crowding[0]))
    if falsifiers:
        lines.append(_falsifier_homepage_line(falsifiers[0], suffix="这类首页就不能再往乐观方向写。"))
    bullish = list(playbook.get("bullish_drivers") or [])
    risks = list(playbook.get("risks") or [])
    variables = list(playbook.get("variables") or [])
    if not transmission and bullish:
        lines.append(f"这类主题最常见的顺风来自：{bullish[0]}")
    if not falsifiers and risks:
        lines.append(f"真正要防的是：{risks[0]}")
    if variables:
        lines.append(f"写这类首页时，优先联想到：{variables[0]}")
    if stage_pattern:
        lines.append(_stage_pattern_homepage_line(stage_pattern[0]))
    return lines[:5]


def _briefing_summary_line(
    regime: Mapping[str, Any],
    day_theme: str,
    headline_lines: Sequence[str],
    news_lines: Sequence[str] = (),
) -> str:
    regime_name = _safe_text(regime.get("current_regime"))
    regime_label = REGIME_LABELS.get(regime_name, regime_name) if regime_name else ""
    joined_news = " ".join(_safe_text(item) for item in list(news_lines or []))
    strong_growth = []
    if any(token in joined_news for token in ("创新药", "医药", "制药", "CXO", "临床", "license-out", "授权")):
        strong_growth.append("创新药/医药")
    if any(token in joined_news for token in ("新易盛", "中际旭创", "华工科技", "光模块", "cpo", "算力", "服务器", "液冷", "hbm", "存储", "半导体", "芯片")):
        strong_growth.append("AI硬件链")
    if any(token in joined_news for token in ("智谱", "kimi", "deepseek", "大模型", "agent", "应用")):
        strong_growth.append("AI软件/应用")
    risk_on_tail = ""
    has_geo_repair = any(token in joined_news for token in ("停火", "休战", "ceasefire", "truce", "缓和", "结束战争"))
    has_geo_blocker = any(token in joined_news for token in ("陷入僵局", "僵局", "遭袭", "遇袭", "袭击", "空袭", "受损", "受创", "紧张升级", "冲突升级"))
    if has_geo_repair and not has_geo_blocker:
        risk_on_tail = "外部上还有中东缓和在抬风险偏好"
    if strong_growth:
        tail = "；" + risk_on_tail if risk_on_tail else ""
        return (
            "今天更像强修复，不是趋势反转确认；前排主线集中在 `"
            + " / ".join(strong_growth[:2])
            + f"`{tail}，先看量能和强势方向能否继续扩散。"
        )
    if day_theme and regime_label:
        return f"今天市场更像结构性轮动日，主线偏 `{day_theme}`，但整体仍运行在 `{regime_label}` 背景里，不把晨报理解成单一板块推荐。"
    if day_theme:
        return f"今天市场更像结构性轮动日，主线偏 `{day_theme}`，但这不等于只有这一条线值得看。"
    if headline_lines:
        return _safe_text(headline_lines[0])
    return "今天晨报先回答市场在交易什么，再决定哪些方向值得继续跟踪。"


def _briefing_theme_lines(playbook: Mapping[str, Any], day_theme: str) -> List[str]:
    if not playbook:
        if day_theme:
            return [f"今天更像在交易 `{day_theme}` 这条主线，但晨报层只把它当结构性主线，不把它直接写成唯一可做方向。"]
        return ["今天没有单一主题完全压过其他变量，更适合先按市场结构和轮动来理解。"]
    lines: List[str] = []
    hard_sector = _safe_text(playbook.get("hard_sector_label"))
    theme_family = _safe_text(playbook.get("theme_family"))
    playbook_level = _safe_text(playbook.get("playbook_level"))
    transmission = list(playbook.get("transmission_path") or [])
    crowding = list(playbook.get("rotation_and_crowding") or [])
    falsifiers = list(playbook.get("falsifiers") or [])
    subtheme_bridge = [dict(item) for item in list(playbook.get("subtheme_bridge") or []) if dict(item)]
    bridge_confidence = _safe_text(playbook.get("subtheme_bridge_confidence")) or "none"
    bridge_reason = _safe_text(playbook.get("subtheme_bridge_reason"))
    bridge_top_label = _safe_text(playbook.get("subtheme_bridge_top_label"))
    theme_match_status = _safe_text(playbook.get("theme_match_status"))
    theme_match_reason = _safe_text(playbook.get("theme_match_reason"))
    theme_match_candidates = [str(item).strip() for item in list(playbook.get("theme_match_candidates") or []) if str(item).strip()]
    hint = playbook_hint_line(playbook)
    if day_theme:
        lines.append(f"今天最值得跟踪的主线偏 `{day_theme}`，但这只是市场里的相对主线，不等于其他方向全部失效。")
    if hard_sector and theme_family:
        lines.append(f"从硬分类看，这条主线更接近 `{hard_sector}`；从软主题看，更像一条 `{theme_family}` 线。")
    elif hard_sector:
        lines.append(f"从硬分类看，这条主线更接近 `{hard_sector}`，更适合按行业轮动而不是单票执行来理解。")
    if hint:
        lines.append(hint)
    if playbook_level == "sector" and theme_match_status == "ambiguous_conflict" and theme_match_candidates:
        lines.append(f"当前先不要把这条主线硬写成单一细主题，因为 `{' / '.join(theme_match_candidates[:3])}` 这几条线还没完全拉开。")
        if theme_match_reason:
            lines.append(f"冲突原因：{theme_match_reason}")
    if playbook_level == "sector" and subtheme_bridge:
        bridge_labels = " / ".join(f"`{_safe_text(item.get('label'))}`" for item in subtheme_bridge[:3] if _safe_text(item.get("label")))
        if bridge_labels:
            if bridge_confidence == "high" and bridge_top_label:
                lines.append(f"结合今天已有线索，这条行业主线内部已经更偏向 `{bridge_top_label}`，但晨报层仍先按行业轮动理解，不直接落成单一细主题。")
            elif bridge_confidence == "medium" and bridge_top_label:
                lines.append(f"结合今天已有线索，可优先跟踪 `{bridge_top_label}` 这条细分线，但还不适合把整条主线直接写成它。")
            elif bridge_confidence == "low" and bridge_top_label:
                lines.append(f"当前只有偏向 `{bridge_top_label}` 的弱线索，更适合作为细分观察方向，而不是主线定性。")
            else:
                lines.append(f"如果市场进一步往细分方向收敛，优先观察 {bridge_labels} 这些已定义的下钻方向，而不是一直停在泛行业口径。")
            if bridge_reason:
                lines.append(f"这层下钻判断主要依据：{bridge_reason}")
    if transmission:
        lines.append(f"更像样的市场理解路径是：{transmission[0]}")
    if crowding:
        lines.append(_crowding_homepage_line(crowding[0]))
    if falsifiers:
        lines.append(_falsifier_homepage_line(falsifiers[0], suffix="这条主线就不该继续写成今天的优先方向。"))
    return lines[:5]


def _briefing_news_lines(payload: Mapping[str, Any], event_digest: Mapping[str, Any] | None = None) -> List[str]:
    digest = dict(event_digest or {})
    max_lines = 5
    workflow_markers = (
        "检查 watchlist",
        "A股盘前检查",
        "上午验证",
        "下午验证",
        "明日验证",
        "下个交易日 09:00",
        "收盘复核",
        "盘后复核",
        "复核日内强弱",
        "次日晨报",
    )
    briefing_subject = {
        "asset_type": "market_briefing",
        "name": "市场晨报",
        "day_theme": {"label": _safe_text(payload.get("day_theme"))},
    }

    def _signal_hint(title: str, category: str = "") -> tuple[str, str]:
        blob = f"{title} {category}".lower()
        geo_blocker = any(
            token in blob
            for token in ("陷入僵局", "僵局", "遭袭", "遇袭", "袭击", "空袭", "受损", "受创", "紧张升级", "冲突升级")
        )
        if any(token in blob for token in ("停火", "休战", "缓和", "结束战争", "ceasefire", "truce", "de-escalat")) and not geo_blocker:
            return "地缘缓和", "黄金/原油/风险偏好"
        if geo_blocker or any(token in blob for token in ("伊朗", "以色列", "中东", "war", "strike", "missile", "conflict")):
            return "地缘扰动", "黄金/原油/风险偏好"
        if any(token in blob for token in ("创新药", "医药", "制药", "药业", "cxo", "fda", "临床", "license-out", "bd", "授权")):
            return "医药催化", "创新药/医药"
        if any(token in blob for token in ("国家电网", "南方电网", "特高压", "输变电", "配网", "变压器", "电力设备", "电网招标")):
            return "电网投资催化", "电网/特高压"
        if any(token in blob for token in ("ai硬件", "硬科技", "新易盛", "中际旭创", "华工科技", "cpo", "光模块", "算力", "服务器", "液冷", "hbm", "semiconductor", "芯片", "半导体", "nvidia", "nvda", "6g")):
            return "AI硬件催化", "AI硬件链"
        if any(token in blob for token in ("智谱", "kimi", "deepseek", "大模型", "模型", "agent", "应用")):
            return "AI应用催化", "AI软件/应用"
        if any(token in blob for token in ("业绩", "财报", "年报", "一季报", "季报", "盈利", "指引")):
            return "财报摘要：盈利/指引", "盈利/估值"
        if any(token in blob for token in ("政策", "国务院", "部署", "支持", "规划", "招标")):
            return "政策催化", "政策预期/景气"
        if any(token in blob for token in ("黄金", "gold", "贵金属")):
            return "避险交易", "黄金/防守"
        if any(token in blob for token in ("原油", "oil", "opec")):
            return "能源冲击", "原油/能源"
        if any(token in blob for token in ("债券", "bond", "fed", "rate", "yield", "利率")):
            return "利率预期", "成长估值/风险偏好"
        return "信息环境：新闻/舆情脉冲", "估值/资金偏好"

    def _signal_conclusion(signal_type: str, impact: str = "") -> str:
        signal = _safe_text(signal_type)
        target = _safe_text(impact) or "相关方向"
        if signal in {"主线增强", "行业催化", "主线活跃", "板块活跃"}:
            return f"偏利多，先看 `{target}` 能否从局部走向扩散。"
        if signal in {"龙头确认", "热度抬升", "观察池前排"}:
            return f"偏利多，但先按 `{target}` 的跟涨/扩散确认处理。"
        if signal in {"医药催化", "AI应用催化", "AI硬件催化"}:
            return f"偏利多，先看 `{target}` 能否继续拿到价格与成交确认。"
        if signal in {"政策催化", "电网投资催化"}:
            return f"偏利多，先看 `{target}` 能否从政策/招标线索落到订单、盈利或价格承接。"
        if signal.startswith("财报摘要"):
            return f"中性偏事件驱动，先等 `{target}` 的实际披露结果验证，不把日历本身写成超预期。"
        if signal == "地缘缓和":
            return "偏利多风险偏好，先看黄金/原油回落与成长弹性修复。"
        if signal in {"地缘扰动", "避险交易", "能源冲击"}:
            return "偏利空风险偏好，先看黄金、防守和能源资产是否继续走强。"
        if signal == "利率预期":
            return "偏利多成长估值，但仍要等价格共振，不把标题直接当成动作信号。"
        return f"中性偏观察，先把它当 `{target}` 的辅助线索。"

    def _looks_like_news(text: str) -> bool:
        line = _safe_text(text).strip()
        if not line:
            return False
        if line.startswith(("背景框架:", "次主线候选:", "若冲突：", "当前判断：")):
            return False
        if any(token in line for token in workflow_markers):
            return False
        return "\n" in line or any(token in line for token in ("财联社", "Reuters", "路透", "Bloomberg", "彭博", "→", "->", "公告", "订单", "招标"))

    def _linkify_unlinked_news_text(text: str) -> str:
        line = _safe_text(text)
        if not line or "http://" in line or "https://" in line:
            return line
        match = re.search(r"\*\*(.+?)\*\*\s*\(([^)]+)\)", line)
        if not match:
            return line
        title = _safe_text(match.group(1))
        source = _safe_text(match.group(2))
        if not title or not source:
            return line
        signal_type, impact = _signal_hint(title, source)
        conclusion = _signal_conclusion(signal_type, impact)
        transmission = _homepage_news_transmission_text(signal_type, impact, briefing_subject, conclusion)
        link = _google_news_search_link(title, source)
        if not link:
            return line
        return (
            f"外部情报：{_markdown_link(title, link)}"
            f"（搜索回退：`{source}`；信号：`{signal_type}`；强弱：`中`；结论：{conclusion}；传导：{transmission}）"
        )

    def _format_item(row: Mapping[str, Any]) -> str:
        title = _safe_text(row.get("title"))
        if not title:
            return ""
        source = _safe_text(row.get("source") or row.get("configured_source"))
        date = _safe_text(row.get("published_at") or row.get("date"))
        link = _safe_text(row.get("link"))
        if not link:
            link = _google_news_search_link(title, source)
        category = _safe_text(row.get("category"))
        signal_type = _safe_text(row.get("signal_type"))
        signal_strength = _safe_text(row.get("signal_strength"))
        impact = ""
        inferred_signal, inferred_impact = _signal_hint(title, category)
        if not signal_type:
            signal_type = inferred_signal
        elif inferred_signal == "地缘缓和" and signal_type == "地缘扰动":
            signal_type = inferred_signal
        elif inferred_signal == "地缘扰动" and signal_type == "地缘缓和":
            signal_type = inferred_signal
        impact = inferred_impact
        if not signal_strength:
            freshness_bucket = _safe_text(row.get("freshness_bucket"))
            signal_strength = "高" if freshness_bucket == "fresh" else "中" if freshness_bucket == "recent" else "低"
        conclusion = _safe_text(row.get("signal_conclusion")) or _signal_conclusion(signal_type, impact)
        tags = _intelligence_tags(
            row,
            as_of=_safe_text(payload.get("generated_at")),
            previous_reviewed_at=digest.get("previous_reviewed_at"),
        )
        compact_tags = [
            part
            for part in (
                _freshness_label(row, as_of=_safe_text(payload.get("generated_at"))),
                _source_directness_label(row),
            )
            if _safe_text(part)
        ]
        prefix = " · ".join(part for part in (*[part for part in (date, source) if part], *compact_tags) if part)
        title_text = _markdown_link(title, link)
        detail = f"{prefix}：{title_text}" if prefix else title_text
        if signal_type:
            detail += f"；信号类型：`{signal_type}`"
        if signal_strength:
            detail += f"；信号强弱：`{signal_strength}`"
        if impact:
            detail += f"；主要影响：`{impact}`"
        if tags:
            detail += f"；情报属性：`{format_intelligence_attributes(tags)}`"
        if conclusion:
            detail += f"；结论：{conclusion}"
        elif signal_type:
            fallback_conclusion = _signal_conclusion(signal_type, impact)
            if fallback_conclusion:
                detail += f"；结论：{fallback_conclusion}"
        return detail

    def _canonical(text: str) -> str:
        cleaned = _safe_text(text)
        cleaned = re.sub(r"\[[^\]]+\]\([^)]+\)", "", cleaned)
        cleaned = cleaned.replace("**", "").replace("`", "")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def _headline_head(text: str) -> str:
        line = _safe_text(text)
        link_match = re.search(r"\[([^\]]+)\]\([^)]+\)", line)
        if link_match:
            return _canonical(link_match.group(1)).strip("：: ")
        first = _canonical(str(text).splitlines()[0])
        for splitter in ("；结论：", "（信号：", "(signal:"):
            if splitter in first:
                first = first.split(splitter, 1)[0]
        if "：" in first:
            first = first.split("：")[-1]
        elif ":" in first and "http" not in first:
            first = first.split(":")[-1]
        return first.strip("：: ")

    def _priority_score(text: str) -> int:
        line = _safe_text(text)
        score = 0
        if "http://" in line or "https://" in line:
            score += 40
        if any(token in line for token in ("停火", "休战", "美伊", "伊朗", "以色列", "中东", "ceasefire", "truce", "risk appetite")):
            score += 55
        if any(token in line for token in ("降息", "加息", "美联储", "Fed", "利率", "收益率", "yield", "关税", "制裁", "贸易战", "出口管制")):
            score += 45
        if any(token in line for token in ("创新药", "医药", "港股创新药ETF", "智谱", "新易盛", "光模块", "算力", "停火", "休战", "中东", "结束战争")):
            score += 35
        if any(token in line for token in ("财联社", "Reuters", "路透", "Bloomberg", "证券时报", "上海证券报", "中国证券报")):
            score += 15
        if any(token in line for token in ("GLD", "黄金", "高股息", "红利", "防守")):
            score -= 5
        if any(token in line for token in workflow_markers):
            score -= 100
        return score

    lines: List[str] = []
    seen: set[str] = set()
    seen_heads: set[str] = set()

    def _ensure_signal_conclusion_line(text: str) -> str:
        line = _safe_text(text)
        if not line:
            return line
        signal_match = re.search(r"信号类型：`([^`]+)`", line) or re.search(r"信号：`([^`]+)`", line)
        signal_type = _safe_text(signal_match.group(1)) if signal_match else ""
        impact_match = re.search(r"主要影响：`([^`]+)`", line) or re.search(r"关注 `([^`]+)`", line)
        impact = _safe_text(impact_match.group(1)) if impact_match else ""
        conclusion_match = re.search(r"结论：([^；）)]+)", line)
        conclusion = _safe_text(conclusion_match.group(1)) if conclusion_match else (_signal_conclusion(signal_type, impact) if signal_type else "")
        return _append_news_interpretation(
            line,
            subject=briefing_subject,
            signal_type=signal_type,
            signal_strength="",
            impact=impact,
            conclusion=conclusion,
        )

    def _append_line(text: str) -> bool:
        text = _ensure_signal_conclusion_line(text)
        key = _canonical(text)
        head = _headline_head(text)
        if not text or not key:
            return False
        if key in seen or (head and head in seen_heads):
            return False
        seen.add(key)
        if head:
            seen_heads.add(head)
        lines.append(text)
        return len(lines) >= max_lines

    market_lines: List[str] = []
    priority_market_lines: List[str] = []
    linked_market_lines: List[str] = []
    raw_news_lines: List[str] = []
    theme_lines: List[str] = []

    for row in list(payload.get("market_event_rows") or []):
        title = _safe_text(row[1] if len(row) > 1 else "")
        date = _safe_text(row[0] if len(row) > 0 else "")
        source = _safe_text(row[2] if len(row) > 2 else "")
        strength = _safe_text(row[3] if len(row) > 3 else "")
        impact = _safe_text(row[4] if len(row) > 4 else "")
        link = _safe_text(row[5] if len(row) > 5 else "")
        signal_type = _safe_text(row[6] if len(row) > 6 else "") or "主题/市场情报"
        conclusion = _safe_text(row[7] if len(row) > 7 else "")
        if not title:
            continue
        workflow_blob = " ".join(part for part in (title, date, source, impact, conclusion) if part)
        if any(token in workflow_blob for token in workflow_markers):
            continue
        prefix_parts = [part for part in (date, source) if part and part not in {"—", "待定"}]
        prefix = " · ".join(prefix_parts)
        title_text = _markdown_link(title, link)
        detail = f"{prefix}：{title_text}" if prefix else title_text
        detail += f"（信号：`{signal_type}`；强弱：`{strength or '中'}`；关注 `{impact or '观察池核心资产'}`）"
        if conclusion:
            detail += f"；结论：{conclusion}"
        if not link:
            detail = f"结构证据：{detail}"
        if signal_type and signal_type != "主题/市场情报":
            priority_market_lines.append(detail)
        elif _priority_score(f"{title} {impact} {detail}") >= 30:
            priority_market_lines.append(detail)
        elif link:
            linked_market_lines.append(detail)
        else:
            market_lines.append(detail)
    news_items = list(dict(payload.get("news_report") or {}).get("items") or [])
    for item in news_items:
        text = _format_item(dict(item or {}))
        if text:
            raw_news_lines.append(text)
    for row in list(payload.get("theme_tracking_rows") or []):
        direction = _safe_text(row[0] if len(row) > 0 else "")
        catalyst = _safe_text(row[1] if len(row) > 1 else "")
        risk = _safe_text(row[4] if len(row) > 4 else "")
        if not direction:
            continue
        detail = f"{direction}：{catalyst or '当前更多依赖主线延续和盘面承接'}"
        if risk:
            detail += f"；主要风险是 {risk}"
        theme_lines.append(f"结构证据：{detail}")
    raw_news_lines.sort(key=_priority_score, reverse=True)
    theme_lines.sort(key=_priority_score, reverse=True)
    linked_market_lines.sort(key=_priority_score, reverse=True)
    had_structured_external_sources = bool(priority_market_lines or raw_news_lines or linked_market_lines or theme_lines or market_lines)

    if raw_news_lines:
        _append_line(raw_news_lines.pop(0))

    while len(lines) < max_lines and (priority_market_lines or raw_news_lines or linked_market_lines or theme_lines or market_lines):
        if priority_market_lines and _append_line(priority_market_lines.pop(0)):
            break
        if raw_news_lines and _append_line(raw_news_lines.pop(0)):
            break
        if linked_market_lines and _append_line(linked_market_lines.pop(0)):
            break
        if theme_lines and _append_line(theme_lines.pop(0)):
            break
        if market_lines and (not lines or len(lines) >= 2 or not raw_news_lines):
            if _append_line(market_lines.pop(0)):
                break
    for pool in (payload.get("core_event_lines") or [], payload.get("headline_lines") or []):
        for item in list(pool or []):
            text = _briefing_client_safe_text(item)
            if not _looks_like_news(text):
                continue
            text = _linkify_unlinked_news_text(text)
            if _append_line(text):
                break
    if raw_news_lines:
        history_line = _event_digest_history_line(digest)
        if history_line:
            _append_unique_line(lines, history_line, limit=4)
    if not lines:
        lines = event_digest_homepage_lines(digest, [])
    formatted_lines = [_format_homepage_evidence_line(item) for item in lines if _format_homepage_evidence_line(item)]
    generic_missing_line = _missing_clickable_intelligence_line()
    formatted_lines = [item for item in formatted_lines if item != generic_missing_line]
    if not _has_clickable_homepage_evidence(formatted_lines):
        if not formatted_lines:
            formatted_lines = [generic_missing_line]
        elif had_structured_external_sources and len(formatted_lines) < max_lines:
            formatted_lines = [*formatted_lines, generic_missing_line]
    return formatted_lines[:max_lines]


def _briefing_micro_lines(payload: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]], headline_lines: Sequence[str]) -> List[str]:
    lines: List[str] = []
    meta = dict(payload.get("a_share_watch_meta") or {})
    pool_size = meta.get("pool_size")
    complete_size = meta.get("complete_analysis_size")
    if pool_size or complete_size:
        lines.append(
            f"A股观察池当前是 `初筛 {pool_size or '—'} -> 完整分析 {complete_size or '—'}`，更适合当观察名单，不等于今天已经有正式动作票。"
        )
    top = _first_named_item(candidates)
    if top:
        top_name = _safe_text(top.get("name"))
        top_symbol = _safe_text(top.get("symbol"))
        trade_state = _safe_text(top.get("trade_state"))
        top_context = {
            **top,
            "day_theme": {"label": _safe_text(payload.get("day_theme"))},
        }
        top_playbook = _subject_theme_context(top_context)
        dims = dict(top.get("dimensions") or {})
        strongest, weakest = _top_bottom_dimensions(dims)
        weakest_label = dict(DIMENSION_LABELS).get(weakest[0], weakest[0])
        weakest_summary = _dimension_summary(dims, weakest[0]) or "确认还不够。"
        lines.append(f"现在相对更值得继续跟踪的是 `{top_name} ({top_symbol})`，但它当前仍是 `{trade_state or '观察为主'}`，不是正式动作票。")
        conflict_line = _candidate_conflict_line(top_context, top_playbook)
        if conflict_line:
            lines.append(conflict_line)
        portfolio_overlap_line = _portfolio_overlap_homepage_line(top_context)
        if portfolio_overlap_line:
            lines.append(portfolio_overlap_line)
        lines.append(f"真正卡住升级的，更多是 `{weakest_label}`：{weakest_summary}")
    elif len(headline_lines) > 1:
        lines.append(_safe_text(headline_lines[1]))
    if not lines:
        lines.append("当前更适合先看主线与观察池，不把市场判断直接翻译成满仓动作。")
    return lines[:3]


def _strategy_background_confidence_line(subject: Mapping[str, Any]) -> str:
    confidence = _strategy_background_confidence(subject)
    if not confidence:
        return ""
    label = _safe_text(confidence.get("label")) or "观察"
    reason = _safe_text(confidence.get("reason")) or _safe_text(confidence.get("summary"))
    if label == "稳定":
        return f"策略后台置信度：`稳定`。{reason} 当前只当辅助加分，不单独替代基本面和事件判断。"
    if label == "退化":
        return f"策略后台置信度：`退化`。{reason} 排序不直接翻空，但当前应下调置信度。"
    return f"策略后台置信度：`观察`。{reason} 这次信号只能做辅助说明，不单独升级动作。"


def _attach_strategy_background_confidence(
    items: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows = [dict(item or {}) for item in list(items or []) if dict(item or {})]
    if not rows:
        return []
    try:
        repository = StrategyRepository()
    except Exception:
        return rows
    enriched: List[Dict[str, Any]] = []
    cache: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = _safe_text(row.get("symbol"))
        if symbol:
            if symbol not in cache:
                try:
                    cache[symbol] = dict(repository.summarize_background_confidence(symbol) or {})
                except Exception:
                    cache[symbol] = {}
            if cache[symbol]:
                row["strategy_background_confidence"] = cache[symbol]
        enriched.append(row)
    return enriched


def _candidate_conflict_line(subject: Mapping[str, Any], playbook: Mapping[str, Any]) -> str:
    if _safe_text(playbook.get("theme_match_status")) != "ambiguous_conflict":
        return ""
    candidates = [str(item).strip() for item in list(playbook.get("theme_match_candidates") or []) if str(item).strip()]
    if not candidates:
        return ""
    label = _subject_display_label(subject)
    joined = " / ".join(candidates[:3])
    return f"`{label}` 当前更适合先按行业层观察，因为 `{joined}` 这几条线还在打架，不要硬落单一细主题。"


def _inject_conflict_line(micro_lines: Sequence[str], subject: Mapping[str, Any], playbook: Mapping[str, Any], *, preserve_prefix: int = 0) -> List[str]:
    lines = [str(item).strip() for item in list(micro_lines or []) if str(item).strip()]
    conflict_line = _candidate_conflict_line(subject, playbook)
    if not conflict_line:
        return lines
    prefix = lines[:preserve_prefix]
    suffix = lines[preserve_prefix:]
    return [*prefix, conflict_line, *suffix]


def build_stock_analysis_editor_packet(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    subject = dict(analysis)
    regime = dict(subject.get("regime") or {})
    day_theme = _safe_text(dict(subject.get("day_theme") or {}).get("label")) or _safe_text(subject.get("day_theme"))
    playbook = _subject_theme_context(subject)
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(dict(subject.get("narrative") or {}).get("judgment", {}).get("state"))
    direction = _safe_text(action.get("direction"))
    editor_bucket = _safe_text(subject.get("editor_bucket"))
    observe_only = (bool(editor_bucket) and editor_bucket != "正式推荐") or any(
        token in f"{trade_state} {direction}" for token in ("观察", "暂不", "回避")
    )
    no_signal = _no_signal_notice(trade_state, direction)
    thesis = _load_thesis_record(
        _safe_text(subject.get("symbol")),
        report_type="stock_analysis",
        generated_at=subject.get("generated_at"),
    )
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    playbook = _enrich_theme_context_with_event_digest(playbook, subject, event_digest)
    summary = " ".join(
        part
        for part in (
            no_signal,
            _bucket_text(trade_state, direction),
        )
        if _safe_text(part)
    )
    strategy_confidence_line = _strategy_background_confidence_line(subject)
    micro_lines = _micro_lines(subject)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    macro_lines, theme_lines, micro_lines, action_lines = _apply_homepage_decision_contract(
        subject=subject,
        playbook=playbook,
        macro_lines=_macro_lines(regime, day_theme, market_hint="", flow_hint="", subject=subject),
        theme_lines=_theme_lines(playbook, subject),
        micro_lines=micro_lines,
        action_lines=_soften_stock_analysis_action_lines(_action_lines(subject, event_digest=event_digest)),
        observe_only=observe_only,
    )
    packet = {
        "report_type": "stock_analysis",
        "packet_version": "editor-v2",
        "subject": _editor_subject_snapshot(subject, fallback_asset_type=_safe_text(subject.get("asset_type"))),
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject, event_digest, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=summary,
            macro_lines=macro_lines,
            theme_lines=theme_lines,
            news_lines=_news_lines_with_event_digest(subject, event_digest),
            sentiment_lines=_sentiment_lines(subject),
            micro_lines=micro_lines,
            action_lines=action_lines,
            conclusion=_stock_analysis_conclusion_line(subject),
        ),
    }
    return packet


def build_etf_pick_editor_packet(payload: Mapping[str, Any]) -> Dict[str, Any]:
    winner = dict(payload.get("winner") or {})
    alternatives = list(payload.get("alternatives") or [])
    selection_context = dict(payload.get("selection_context") or {})
    regime = dict(payload.get("regime") or {})
    day_theme = _safe_text(dict(payload.get("day_theme") or {}).get("label")) or _safe_text(payload.get("day_theme"))
    observe_only = bool(selection_context.get("delivery_observe_only"))
    subject = {
        **winner,
        "day_theme": {"label": day_theme},
        "generated_at": _safe_text(winner.get("generated_at")) or _safe_text(payload.get("generated_at")),
    }
    playbook = _subject_theme_context(subject)
    subject_label = _subject_display_label(winner)
    no_signal = _no_signal_notice(
        _safe_text(winner.get("trade_state")),
        _safe_text(dict(winner.get("action") or {}).get("direction")),
        observe_only=observe_only,
    )
    alternatives_blob = "、".join(
        _subject_display_label(item)
        for item in alternatives[:2]
        if _subject_display_label(item)
    )
    summary = (
        " ".join(
            part
            for part in (
                no_signal,
                f"本页重点看 `{subject_label}`。",
                _bucket_text(_safe_text(winner.get("trade_state")), _safe_text(dict(winner.get("action") or {}).get("direction"))),
            )
            if _safe_text(part)
        )
    )
    thesis = _load_thesis_record(_safe_text(winner.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    playbook = _enrich_theme_context_with_event_digest(playbook, subject, event_digest)
    news_lines = _news_lines_with_event_digest(subject, event_digest)
    if not news_lines:
        news_lines = [_no_intelligence_homepage_line()]
    micro_lines = _micro_lines(subject)
    strategy_confidence_line = _strategy_background_confidence_line(subject)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    if alternatives_blob:
        micro_lines = [f"本页重点分析对象是 `{subject_label}`；补充观察还包括 `{alternatives_blob}`。", *micro_lines]
    else:
        micro_lines = [f"本页重点分析对象是 `{subject_label}`。", *micro_lines]
    micro_lines = _inject_conflict_line(micro_lines, subject, playbook, preserve_prefix=1)
    macro_lines, theme_lines, micro_lines, action_lines = _apply_homepage_decision_contract(
        subject=subject,
        playbook=playbook,
        macro_lines=_macro_lines(
            regime,
            day_theme,
            market_hint=_market_hint_from_context(selection_context, regime, day_theme),
            flow_hint="",
            subject=winner,
        ),
        theme_lines=_theme_lines(playbook, subject),
        micro_lines=micro_lines,
        action_lines=_action_lines(subject, observe_only=observe_only, event_digest=event_digest),
        observe_only=observe_only,
    )
    packet = {
        "report_type": "etf_pick",
        "packet_version": "editor-v2",
        "subject": _editor_subject_snapshot(subject, fallback_asset_type="cn_etf"),
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
            "selection_context": selection_context,
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject, event_digest, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=summary,
            macro_lines=macro_lines,
            theme_lines=theme_lines,
            news_lines=news_lines,
            sentiment_lines=_sentiment_lines(subject, selection_context),
            micro_lines=micro_lines,
            action_lines=action_lines,
            conclusion=_conclusion_line(subject, observe_only=observe_only),
        ),
    }
    return packet


def _first_named_item(items: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    for item in items:
        if _safe_text(item.get("name")) or _safe_text(item.get("symbol")):
            return dict(item)
    return dict(items[0]) if items else {}


def _briefing_entity_candidates(items: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in items:
        row = dict(item or {})
        if row.get("briefing_reuse_only"):
            continue
        if not (_safe_text(row.get("name")) or _safe_text(row.get("symbol"))):
            continue
        rows.append(row)
    return rows


def _watch_symbol_set(items: Sequence[Mapping[str, Any]]) -> set[str]:
    return {
        _safe_text(item.get("symbol"))
        for item in items
        if _safe_text(item.get("symbol"))
    }


def _preferred_stock_pick_subject(
    items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    prefer_actionable: bool,
) -> Dict[str, Any]:
    ranked = rank_market_items(items, watch_symbols)
    if prefer_actionable:
        for item in ranked:
            if analysis_is_actionable(item, watch_symbols):
                return dict(item)
    else:
        for item in ranked:
            if not analysis_is_actionable(item, watch_symbols):
                return dict(item)
    return _first_named_item(ranked)


def build_stock_pick_editor_packet(payload: Mapping[str, Any]) -> Dict[str, Any]:
    top = _attach_strategy_background_confidence(payload.get("top") or [])
    coverage_analyses = _attach_strategy_background_confidence(payload.get("coverage_analyses") or [])
    watch_positive = _attach_strategy_background_confidence(payload.get("watch_positive") or [])
    regime = dict(payload.get("regime") or {})
    day_theme = _safe_text(dict(payload.get("day_theme") or {}).get("label")) or _safe_text(payload.get("day_theme"))
    sector_filter = _safe_text(payload.get("sector_filter"))
    market_label = _safe_text(payload.get("market_label")) or "全市场"
    watch_symbols = _watch_symbol_set(watch_positive)
    ranked_pool = list(top or coverage_analyses or watch_positive)
    subject_pool = list(top or ranked_pool or watch_positive)
    has_actionable = any(analysis_is_actionable(item, watch_symbols) for item in ranked_pool)
    observe_only = not has_actionable
    subject = (
        _preferred_stock_pick_subject(
            subject_pool,
            watch_symbols,
            prefer_actionable=has_actionable,
        )
        or _first_named_item(top)
        or _first_named_item(watch_positive)
    )
    subject_context = {
        **subject,
        "day_theme": {"label": day_theme},
        "generated_at": _safe_text(subject.get("generated_at")) or _safe_text(payload.get("generated_at")),
        "metadata": {**dict(subject.get("metadata") or {}), "sector": sector_filter or dict(subject.get("metadata") or {}).get("sector")},
        "taxonomy_summary": subject.get("taxonomy_summary") or sector_filter or market_label,
    }
    playbook = _subject_theme_context(subject_context)
    subject_label = _subject_display_label(subject_context)
    subject_theme = _safe_text(playbook.get("label")) or _safe_text(subject_theme_label(subject_context))
    no_signal = _no_signal_notice(
        _safe_text(subject.get("trade_state")),
        _safe_text(dict(subject.get("action") or {}).get("direction")),
        observe_only=observe_only,
    )
    summary = " ".join(
        part
        for part in (
            no_signal,
            f"本页重点看 `{subject_label}`。",
            _stock_pick_total_judgment_line(
                market_label,
                day_theme or "未识别",
                subject_theme,
                has_actionable=has_actionable,
            ),
        )
        if _safe_text(part)
    )
    thesis = _load_thesis_record(_safe_text(subject_context.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject_context, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    playbook = _enrich_theme_context_with_event_digest(playbook, subject_context, event_digest)
    news_lines = _news_lines_with_event_digest(subject_context, event_digest)
    if not news_lines:
        news_lines = [_no_intelligence_homepage_line()]
    action_lines = _action_lines(subject, observe_only=observe_only, event_digest=event_digest)
    if sector_filter:
        action_lines = [f"当前范围是 `{sector_filter}` 主题内相对排序，不是跨主题分散候选池。", *action_lines]
    micro_lines = _micro_lines(subject) if subject else ["当前更像在比较谁更接近触发条件，而不是已经给出满仓答案。"]
    strategy_confidence_line = _strategy_background_confidence_line(subject_context)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject_context)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    conflict_line = _candidate_conflict_line(subject_context, playbook)
    if conflict_line:
        micro_lines = [conflict_line, *micro_lines]
    macro_lines, theme_lines, micro_lines, action_lines = _apply_homepage_decision_contract(
        subject=subject_context,
        playbook=playbook,
        macro_lines=_macro_lines(
            regime,
            day_theme,
            market_hint=f"今天先按 `{market_label}` 范围看结构性机会，不把它理解成全市场统一主线。",
            subject=subject_context,
        ),
        theme_lines=_theme_lines(playbook, subject_context),
        micro_lines=micro_lines,
        action_lines=action_lines,
        observe_only=observe_only,
    )
    return {
        "report_type": "stock_pick",
        "packet_version": "editor-v2",
        "subject": _editor_subject_snapshot(
            subject_context,
            fallback_asset_type=_safe_text(subject.get("asset_type") or "cn_stock"),
        ),
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
            "market_label": market_label,
            "sector_filter": sector_filter,
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject_context, event_digest, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=summary,
            macro_lines=macro_lines,
            theme_lines=theme_lines,
            news_lines=news_lines,
            sentiment_lines=_sentiment_lines(subject_context, {"coverage_lines": payload.get("coverage_lines") or []}),
            micro_lines=micro_lines,
            action_lines=action_lines,
            conclusion=(
                "结论：今天这份个股稿更适合先看观察与升级条件。"
                if observe_only
                else "结论：今天已经有少数标的接近执行边界，但仍应按确认和仓位纪律参与。"
            ),
        ),
    }


def build_fund_pick_editor_packet(payload: Mapping[str, Any]) -> Dict[str, Any]:
    winner = dict(payload.get("winner") or {})
    selection_context = dict(payload.get("selection_context") or {})
    regime = dict(payload.get("regime") or {})
    day_theme = _safe_text(dict(payload.get("day_theme") or {}).get("label")) or _safe_text(payload.get("day_theme"))
    observe_only = bool(selection_context.get("delivery_observe_only"))
    subject = {
        **winner,
        "day_theme": {"label": day_theme},
        "generated_at": _safe_text(winner.get("generated_at")) or _safe_text(payload.get("generated_at")),
    }
    playbook = _subject_theme_context(subject)
    no_signal = _no_signal_notice(
        _safe_text(winner.get("trade_state")),
        _safe_text(dict(winner.get("action") or {}).get("direction")),
        observe_only=observe_only,
    )
    summary = " ".join(
        part
        for part in (
            no_signal,
            _bucket_text(_safe_text(winner.get("trade_state")), _safe_text(dict(winner.get("action") or {}).get("direction"))),
        )
        if _safe_text(part)
    )
    summary = " ".join(
        part
        for part in (
            summary,
            "这份场外基金稿更该先回答申赎窗口、主题暴露和确认条件，而不是把它写成一笔立即重仓的动作。",
        )
        if _safe_text(part)
    )
    thesis = _load_thesis_record(_safe_text(winner.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    playbook = _enrich_theme_context_with_event_digest(playbook, subject, event_digest)
    strategy_confidence_line = _strategy_background_confidence_line(subject)
    micro_lines = _micro_lines(subject)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    macro_lines, theme_lines, micro_lines, action_lines = _apply_homepage_decision_contract(
        subject=subject,
        playbook=playbook,
        macro_lines=_macro_lines(
            regime,
            day_theme,
            market_hint=_market_hint_from_context(selection_context, regime, day_theme),
            subject=winner,
        ),
        theme_lines=_theme_lines(playbook, subject),
        micro_lines=_inject_conflict_line(micro_lines, subject, playbook),
        action_lines=_action_lines(subject, observe_only=observe_only, event_digest=event_digest),
        observe_only=observe_only,
    )
    return {
        "report_type": "fund_pick",
        "packet_version": "editor-v2",
        "subject": _editor_subject_snapshot(subject, fallback_asset_type="cn_fund"),
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
            "selection_context": selection_context,
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject, event_digest, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=summary,
            macro_lines=macro_lines,
            theme_lines=theme_lines,
            news_lines=_news_lines_with_event_digest(subject, event_digest)
            or [_no_intelligence_homepage_line()],
            sentiment_lines=_sentiment_lines(subject, selection_context),
            micro_lines=micro_lines,
            action_lines=action_lines,
            conclusion=_conclusion_line(subject, observe_only=observe_only),
        ),
    }


def build_briefing_editor_packet(payload: Mapping[str, Any]) -> Dict[str, Any]:
    regime = dict(payload.get("regime") or {})
    day_theme = _safe_text(payload.get("day_theme"))
    candidates = _briefing_entity_candidates(list(payload.get("a_share_watch_candidates") or []))
    headline_lines = [_briefing_client_safe_text(item) for item in list(payload.get("headline_lines") or []) if _briefing_client_safe_text(item)]
    action_lines = [_briefing_client_safe_text(item) for item in list(payload.get("action_lines") or []) if _briefing_client_safe_text(item)]
    macro_items = [_briefing_client_safe_text(item) for item in list(payload.get("macro_items") or []) if _briefing_client_safe_text(item)]
    quality_lines = [_briefing_client_safe_text(item) for item in list(payload.get("quality_lines") or []) if _briefing_client_safe_text(item)]
    subject_context = {"name": "A股市场", "asset_type": "market_briefing", "day_theme": {"label": day_theme}, "notes": headline_lines}
    playbook = _subject_theme_context(subject_context, explicit_key=day_theme)
    macro_lines = _macro_lines(
        regime,
        day_theme,
        market_hint=_market_hint_from_context({"proxy_contract": dict(payload.get("proxy_contract") or {})}, regime, day_theme),
        subject=payload,
    )
    if macro_items:
        macro_lines.extend(macro_items[:2])
    candidates_with_confidence = _attach_strategy_background_confidence(candidates)
    strategy_source = next((item for item in candidates_with_confidence if _strategy_background_confidence(item)), {})
    strategy_confidence_line = _strategy_background_confidence_line(strategy_source)
    micro_lines = _briefing_micro_lines(payload, candidates_with_confidence, headline_lines)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    sentiment_lines = []
    market_flow = dict(dict(payload.get("proxy_contract") or {}).get("market_flow") or {})
    if _safe_text(market_flow.get("interpretation")):
        sentiment_lines.append(_safe_text(market_flow.get("interpretation")))
    social = dict(dict(payload.get("proxy_contract") or {}).get("social_sentiment") or {})
    if social:
        sentiment_lines.append(
            f"情绪与热度当前更多是代理层提示：覆盖 `{social.get('covered', '—')}/{social.get('total', '—')}`，更适合辅助判断拥挤度。"
        )
    if quality_lines:
        sentiment_lines.append(quality_lines[0])
    thesis = _load_thesis_record(_safe_text(payload.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(payload, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    playbook = _enrich_theme_context_with_event_digest(playbook, subject_context, event_digest)
    news_lines = _briefing_news_lines(payload, event_digest)
    briefing_action_lines = list(action_lines)
    if strategy_confidence_line:
        briefing_action_lines = [
            "策略后台置信度只作辅助约束，不替代今天的宏观与主题判断。",
            *briefing_action_lines,
        ]
    return {
        "report_type": "briefing",
        "packet_version": "editor-v2",
        "subject": _editor_subject_snapshot(
            {
                **subject_context,
                "generated_at": _safe_text(payload.get("generated_at")),
            },
            fallback_asset_type="market_briefing",
        ),
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
            "mode": _safe_text(payload.get("mode")),
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject_context, event_digest, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=_briefing_summary_line(regime, day_theme, headline_lines, news_lines),
            macro_lines=macro_lines[:4],
            theme_lines=_briefing_theme_lines(playbook, day_theme),
            news_lines=news_lines,
            sentiment_lines=sentiment_lines[:3] or ["情绪与热度更适合当辅助层，不替代宏观与主线判断。"],
            micro_lines=micro_lines[:3],
            action_lines=(briefing_action_lines[:3] or ["先按晨报理解当天主线和观察条件，真正执行还要回到单标的确认。"]),
            conclusion="结论：晨报先回答今天市场在交易什么、哪些方向值得看，再把单标的动作交给后文或单独分析稿。",
        ),
    }


def _generic_packet(report_type: str, payload: Mapping[str, Any], *, subject: str = "", report_kind: str = "") -> Dict[str, Any]:
    return {
        "report_type": report_type,
        "packet_version": "editor-v1",
        "report_kind": report_kind,
        "subject": subject or _safe_text(payload.get("symbol")) or _safe_text(payload.get("name")) or report_type,
        "summary": _safe_text(payload.get("generated_at")) or "client-final sidecar",
    }


def build_scan_editor_packet(analysis: Mapping[str, Any], bucket: str = "") -> Dict[str, Any]:
    subject = dict(analysis)
    regime = dict(subject.get("regime") or {})
    day_theme = _safe_text(dict(subject.get("day_theme") or {}).get("label")) or _safe_text(subject.get("day_theme"))
    bucket_label = bucket or _safe_text(analysis.get("editor_bucket"))
    playbook = _subject_theme_context(subject)
    action = dict(subject.get("action") or {})
    subject_label = _subject_display_label(subject)
    trade_state = _safe_text(dict(subject.get("narrative") or {}).get("judgment", {}).get("state"))
    direction = _safe_text(action.get("direction"))
    no_signal = _no_signal_notice(trade_state, direction)
    summary = " ".join(
        part
        for part in (
            no_signal,
            f"本页重点看 `{subject_label}`。",
            _bucket_text(trade_state, direction),
        )
        if _safe_text(part)
    )
    if bucket_label:
        summary = f"{summary.rstrip('。')}。当前更适合按 `{bucket_label}` 档位理解。"
    thesis = _load_thesis_record(
        _safe_text(subject.get("symbol")),
        report_type="scan",
        generated_at=subject.get("generated_at"),
    )
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    playbook = _enrich_theme_context_with_event_digest(playbook, subject, event_digest)
    news_lines = _news_lines_with_event_digest(subject, event_digest) or [_no_intelligence_homepage_line()]
    strategy_confidence_line = _strategy_background_confidence_line(subject)
    micro_lines = _micro_lines(subject)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    observe_only = bool(bucket_label and "观察" in bucket_label)
    macro_lines, theme_lines, micro_lines, action_lines = _apply_homepage_decision_contract(
        subject=subject,
        playbook=playbook,
        macro_lines=_macro_lines(regime, day_theme, market_hint="", flow_hint="", subject=subject),
        theme_lines=_theme_lines(playbook, subject),
        micro_lines=_inject_conflict_line(micro_lines, subject, playbook),
        action_lines=_action_lines(
            subject,
            observe_only=observe_only,
            event_digest=event_digest,
            soften_watch_levels=observe_only and _safe_text(subject.get("asset_type")) != "cn_stock",
        ),
        observe_only=observe_only,
    )
    return {
        "report_type": "scan",
        "packet_version": "editor-v2",
        "subject": _editor_subject_snapshot(subject, fallback_asset_type=_safe_text(subject.get("asset_type"))),
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
            "bucket": bucket_label,
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject, event_digest, bucket=bucket_label, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=summary,
            macro_lines=macro_lines,
            theme_lines=theme_lines,
            news_lines=news_lines,
            sentiment_lines=_sentiment_lines(subject),
            micro_lines=micro_lines,
            action_lines=action_lines,
            conclusion=_conclusion_line(subject, observe_only=observe_only),
        ),
    }


def build_strategy_editor_packet(payload: Mapping[str, Any], *, report_kind: str = "", subject: str = "") -> Dict[str, Any]:
    return _generic_packet("strategy", payload, subject=subject or "strategy", report_kind=report_kind)


def build_retrospect_editor_packet(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return _generic_packet("retrospect", payload, subject="portfolio retrospect")


def summarize_theme_playbook_contract(playbook: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(playbook or {})
    if not payload:
        return {}
    theme_match_candidates = [
        str(item).strip()
        for item in list(payload.get("theme_match_candidates") or [])
        if str(item).strip()
    ]
    bridge_items = [dict(item) for item in list(payload.get("subtheme_bridge") or []) if dict(item)]
    bridge_candidates = [
        _safe_text(item.get("label"))
        for item in bridge_items
        if _safe_text(item.get("label"))
    ]
    summary: Dict[str, Any] = {
        "contract_version": "theme_playbook.v1",
        "key": _safe_text(payload.get("key")),
        "label": _safe_text(payload.get("label")),
        "playbook_level": _safe_text(payload.get("playbook_level")),
        "hard_sector_key": _safe_text(payload.get("hard_sector_key")),
        "hard_sector_label": _safe_text(payload.get("hard_sector_label")),
        "theme_family": _safe_text(payload.get("theme_family")),
        "theme_match_status": _safe_text(payload.get("theme_match_status")),
        "theme_match_reason": _safe_text(payload.get("theme_match_reason")),
        "theme_match_candidates": theme_match_candidates[:4],
        "subtheme_bridge_confidence": _safe_text(payload.get("subtheme_bridge_confidence")),
        "subtheme_bridge_reason": _safe_text(payload.get("subtheme_bridge_reason")),
        "subtheme_bridge_top_key": _safe_text(payload.get("subtheme_bridge_top_key")),
        "subtheme_bridge_top_label": _safe_text(payload.get("subtheme_bridge_top_label")),
        "subtheme_bridge_candidates": bridge_candidates[:4],
        "trading_role_key": _safe_text(payload.get("trading_role_key")),
        "trading_role_label": _safe_text(payload.get("trading_role_label")),
        "trading_position_label": _safe_text(payload.get("trading_position_label")),
        "trading_role_summary": _safe_text(payload.get("trading_role_summary")),
    }
    compact: Dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, list):
            if value:
                compact[key] = value
            continue
        if _safe_text(value):
            compact[key] = value
    return compact


def _bullet_lines(items: Iterable[str]) -> List[str]:
    return [f"- {_homepage_emphasis(item)}" for item in items if _safe_text(item)]


_HOMEPAGE_SECTION_FALLBACKS: Dict[str, List[str]] = {
    "宏观面": ["当前宏观层没有额外强信号，先按中性背景理解，不把缺失写成明确顺风。"],
    "板块 / 主题认知": ["当前主题线索有限，先按事实层和产品属性理解，不硬编细主题。"],
    "关键新闻 / 关键证据": ["当前更依赖现有结构化事件和代理证据来理解，不把情报空白直接误读成逻辑失效。"],
    "情绪与热度": ["情绪与热度当前更适合作为辅助层，不单独改写动作判断。"],
    "微观面": ["微观层当前更适合看价格、资金和确认条件，不把单一因子当成动作触发。"],
    "动作建议与结论": ["当前先按观察和确认条件处理，不把它升级成正式动作。"],
}

_SENTIMENT_HOMEPAGE_FORBIDDEN_TOKENS = ("直接催化", "已经兑现", "已经形成买点")


def _sanitize_sentiment_homepage_lines(items: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    for item in items:
        text = _safe_text(item)
        if not text:
            continue
        if any(token in text for token in _SENTIMENT_HOMEPAGE_FORBIDDEN_TOKENS):
            text = "情绪与热度当前只作辅助层，更多反映关注度和拥挤度变化，不单独改写动作判断。"
        cleaned.append(text)
    return cleaned


def render_editor_homepage(packet: Mapping[str, Any]) -> str:
    homepage = dict(packet.get("homepage") or {})
    if homepage.get("version") != "thesis-first-v2":
        return ""
    lines = ["## 首页判断", ""]
    total_judgment = _safe_text(homepage.get("total_judgment"))
    if total_judgment:
        lines.extend([total_judgment, ""])
    sections = [
        ("宏观面", homepage.get("macro_lines") or []),
        ("板块 / 主题认知", homepage.get("theme_lines") or []),
        ("关键新闻 / 关键证据", homepage.get("news_lines") or []),
        ("情绪与热度", homepage.get("sentiment_lines") or []),
        ("微观面", homepage.get("micro_lines") or []),
        ("动作建议与结论", homepage.get("action_lines") or []),
    ]
    for heading, section_lines in sections:
        display_lines = list(section_lines) or list(_HOMEPAGE_SECTION_FALLBACKS.get(heading) or [])
        if heading == "情绪与热度":
            display_lines = _sanitize_sentiment_homepage_lines(display_lines)
        lines.extend([f"### {heading}", ""])
        lines.extend(_bullet_lines(display_lines))
        lines.append("")
    conclusion = _safe_text(homepage.get("conclusion"))
    if conclusion:
        lines.append(conclusion)
    return "\n".join(lines).rstrip()


def render_financial_editor_prompt(packet: Mapping[str, Any]) -> str:
    report_type = _safe_text(packet.get("report_type")) or "unknown"
    packet_version = _safe_text(packet.get("packet_version")) or "editor-v1"
    subject = _dict_snapshot(packet.get("subject"))
    homepage = dict(packet.get("homepage") or {})
    playbook = dict(packet.get("theme_playbook") or {})
    event_digest = dict(packet.get("event_digest") or {})
    what_changed = dict(packet.get("what_changed") or {})
    horizon_expression = _dict_snapshot(packet.get("horizon_expression")) or _dict_snapshot(subject.get("horizon_expression"))
    if not horizon_expression:
        horizon_expression = build_horizon_expression_packet(
            _dict_snapshot(_dict_snapshot(subject.get("action")).get("horizon"))
        )
    lines = [
        "# Financial Editor Packet",
        "",
        f"- report_type: `{report_type}`",
        f"- packet_version: `{packet_version}`",
        "",
        "## 写作合同",
        "",
        "- 不能补新事实、不能改推荐等级、不能把观察稿写成推荐稿。",
        "- 主题认知只能帮助你组织判断，不能偷写成当天已验证的直接催化。",
        "- 首页必须先给阶段判断，再按宏观面 / 板块主题认知 / 关键新闻与证据 / 情绪热度 / 微观面 / 动作建议与结论展开。",
        "- 对单标的/ETF/基金首页，必须把 `赛道判断 / 载体判断 / 执行卡 / 尾部风险` 这四层显式拆开，不要把“赛道成立”和“当前就能下单”写成一句。",
        "- `执行卡` 不能只给机械价位；至少要写清触发条件、失效位，以及第一次减仓/目标上修的边界。",
        "- 如果底稿已经形成事件消化结论，要把 `待补充 / 待复核 / 已消化` 和“这件事改变了什么”写清楚。",
        "- 如果底稿里已经有高质量催化证据或联网复核证据，应优先前置到 `关键新闻 / 关键证据`，不要埋到后文。",
        "",
    ]
    if homepage:
        lines.extend(
            [
                "## 当前首页骨架",
                "",
                f"- 总判断：{_safe_text(homepage.get('total_judgment'))}",
            ]
        )
        for heading, key in (
            ("宏观面", "macro_lines"),
            ("板块 / 主题认知", "theme_lines"),
            ("关键新闻 / 关键证据", "news_lines"),
            ("情绪与热度", "sentiment_lines"),
            ("微观面", "micro_lines"),
            ("动作建议与结论", "action_lines"),
        ):
            values = list(homepage.get(key) or [])
            if values:
                lines.append(f"- {heading}：{' / '.join(_safe_text(item) for item in values[:3])}")
        if _safe_text(homepage.get("conclusion")):
            lines.append(f"- 结论：{_safe_text(homepage.get('conclusion'))}")
        lines.append("")
    if horizon_expression:
        forbidden_raw = horizon_expression.get("forbidden_terms")
        if isinstance(forbidden_raw, str):
            forbidden_terms = [item.strip() for item in re.split(r"[/,，、]", forbidden_raw) if item.strip()]
        else:
            forbidden_terms = [str(item).strip() for item in list(forbidden_raw or []) if str(item).strip()]
        lines.extend(
            [
                "## Horizon Expression Contract",
                "",
                f"- 合同版本：`{_safe_text(horizon_expression.get('contract_version')) or 'horizon_expression.v1'}`",
                f"- 规则判定：`{_safe_text(horizon_expression.get('setup_code'))}` / {_safe_text(horizon_expression.get('setup_label'))}",
                f"- LLM 写法：{_safe_text(horizon_expression.get('write_as'))}",
                f"- 写作提示：{_safe_text(horizon_expression.get('prompt_hint'))}",
                "- 编辑边界：只能润色周期和形态表达，不能改变推荐等级、动作方向、硬 gate 或观察/推荐属性。",
            ]
        )
        if forbidden_terms:
            lines.append(f"- 禁止误写：{' / '.join(forbidden_terms[:8])}")
        lines.append("")
    if event_digest:
        lines.extend(
            [
                "## Event Digest",
                "",
                f"- 状态：{_safe_text(event_digest.get('status')) or '待补充'}",
                f"- 事件分层：{_safe_text(event_digest.get('lead_layer')) or '新闻'}",
            ]
        )
        if _safe_text(event_digest.get("lead_detail")):
            lines.append(f"- 事件细分：{_safe_text(event_digest.get('lead_detail'))}")
        if list(event_digest.get("intelligence_attributes") or []):
            lines.append(
                "- 情报属性："
                + format_intelligence_attributes(list(event_digest.get("intelligence_attributes") or []))
            )
        if _safe_text(event_digest.get("impact_summary")):
            lines.append(f"- 影响层：{_safe_text(event_digest.get('impact_summary'))}")
        if _safe_text(event_digest.get("thesis_scope")):
            lines.append(f"- 影响性质：{_safe_text(event_digest.get('thesis_scope'))}")
        if _safe_text(event_digest.get("importance_reason")):
            lines.append(f"- 优先级判断：{_safe_text(event_digest.get('importance_reason'))}")
        if _safe_text(event_digest.get("changed_what")):
            lines.append(f"- 这件事改变了什么：{_safe_text(event_digest.get('changed_what'))}")
        if _safe_text(event_digest.get("next_step")):
            lines.append(f"- 现在更该做什么：{_safe_text(event_digest.get('next_step'))}")
        if _safe_text(event_digest.get("latest_signal_at")):
            lines.append(f"- 最新情报时点：{_safe_text(event_digest.get('latest_signal_at'))}")
        if _safe_text(event_digest.get("previous_reviewed_at")):
            lines.append(f"- 上次复查时间：{_safe_text(event_digest.get('previous_reviewed_at'))}")
        if _safe_text(event_digest.get("history_note")):
            lines.append(f"- 与上次复查相比：{_safe_text(event_digest.get('history_note'))}")
        lines.append("")
    if what_changed:
        lines.extend(
            [
                "## What Changed",
                "",
                f"- 上次怎么看：{_safe_text(what_changed.get('previous_view'))}",
                f"- 这次什么变了：{_safe_text(what_changed.get('change_summary'))}",
                f"- 当前事件理解：{_safe_text(what_changed.get('current_event_understanding'))}",
                (
                    "- 结论变化：`"
                    + (_safe_text(what_changed.get("conclusion_label")) or "维持")
                    + "`；当前更像 `"
                    + (_safe_text(what_changed.get("current_view")) or "当前判断")
                    + "`"
                    + (
                        f"；触发：{_safe_text(what_changed.get('state_trigger'))}"
                        if _safe_text(what_changed.get("state_trigger"))
                        else ""
                    )
                ),
            ]
        )
        if _safe_text(what_changed.get("state_summary")):
            lines.append(f"- 状态解释：{_safe_text(what_changed.get('state_summary'))}")
        lines.append("")
    if playbook:
        lines.extend(
            [
                "## Theme Playbook",
                "",
                f"- 主题：{_safe_text(playbook.get('label'))}",
            ]
        )
        if _safe_text(playbook.get("hard_sector_label")):
            lines.append(f"- 硬分类：{_safe_text(playbook.get('hard_sector_label'))}")
        if _safe_text(playbook.get("theme_family")):
            lines.append(f"- 主题家族：{_safe_text(playbook.get('theme_family'))}")
        if _safe_text(playbook.get("theme_match_status")):
            lines.append(f"- 主题匹配状态：{_safe_text(playbook.get('theme_match_status'))}")
        if _safe_text(playbook.get("theme_match_reason")):
            lines.append(f"- 主题匹配说明：{_safe_text(playbook.get('theme_match_reason'))}")
        conflict_candidates = [str(item).strip() for item in list(playbook.get("theme_match_candidates") or []) if str(item).strip()]
        if conflict_candidates:
            lines.append(f"- 易混主题候选：{' / '.join(conflict_candidates[:4])}")
        bridge_items = [dict(item) for item in list(playbook.get("subtheme_bridge") or []) if dict(item)]
        bridge_confidence = _safe_text(playbook.get("subtheme_bridge_confidence")) or "none"
        bridge_reason = _safe_text(playbook.get("subtheme_bridge_reason"))
        if bridge_items:
            bridge_labels = " / ".join(_safe_text(item.get("label")) for item in bridge_items[:4] if _safe_text(item.get("label")))
            if bridge_labels:
                lines.append(f"- 行业层下钻方向：{bridge_labels}")
            lines.append(f"- 行业层下钻置信度：{bridge_confidence}")
            if bridge_reason:
                lines.append(f"- 下钻判断依据：{bridge_reason}")
            matched_bridge = [item for item in bridge_items[:3] if list(item.get("matched_tokens") or [])]
            if matched_bridge:
                signal_line = " / ".join(
                    f"{_safe_text(item.get('label'))} <- {', '.join(str(token) for token in list(item.get('matched_tokens') or [])[:2])}"
                    for item in matched_bridge
                    if _safe_text(item.get("label"))
                )
                if signal_line:
                    lines.append(f"- 当前下钻线索：{signal_line}")
            if bridge_confidence in {"high", "medium"}:
                lines.append("- 下钻写作边界：当前最多只能写成“更偏向/可优先留意某条细分线”，不能把行业层稿件直接改成已确认的细主题。")
            else:
                lines.append("- 下钻写作边界：当前只允许把细分方向写成观察清单，不允许把行业层稿件落成某条确定主题。")
        for label, key in (
            ("市场通常在交易什么", "market_logic"),
            ("典型传导链", "transmission_path"),
            ("常见所处阶段", "stage_pattern"),
            ("轮动与拥挤度", "rotation_and_crowding"),
            ("常见正向驱动", "bullish_drivers"),
            ("常见反向风险", "risks"),
            ("证伪信号", "falsifiers"),
            ("应优先联想到的变量", "variables"),
            ("不能误写成直接催化", "guardrails"),
        ):
            items = list(playbook.get(key) or [])
            if items:
                lines.append(f"- {label}：{items[0]}")
        lines.append("")
    lines.extend(
        [
            "## 输出格式",
            "",
            "必须只输出首页判断层，不要把后文详细分析重写一遍。",
            "",
            "```md",
            "## 首页判断",
            "",
            "一句话总判断",
            "",
            "### 宏观面",
            "",
            "- ...",
            "",
            "### 板块 / 主题认知",
            "",
            "- 赛道判断：...",
            "",
            "- ...",
            "",
            "### 关键新闻 / 关键证据",
            "",
            "- ...",
            "",
            "### 情绪与热度",
            "",
            "- ...",
            "",
            "### 微观面",
            "",
            "- 载体判断：...",
            "",
            "- ...",
            "",
            "### 动作建议与结论",
            "",
            "- 执行卡：...",
            "- 尾部风险：...",
            "",
            "- ...",
            "",
            "结论：...",
            "```",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"
