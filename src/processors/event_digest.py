"""Shared event-digestion helpers for research outputs."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Sequence

import pandas as pd


EVENT_DIGEST_STATUSES = {"待补充", "待复核", "已消化"}
EVENT_TYPES = {"财报", "公告", "政策", "新闻", "行业主题事件"}

_HIGH_IMPORTANCE = "high"
_MEDIUM_IMPORTANCE = "medium"
_LOW_IMPORTANCE = "low"

_EARNINGS_KEYS = (
    "财报",
    "业绩",
    "业绩预告",
    "季报",
    "年报",
    "中报",
    "快报",
    "指引",
    "earnings",
    "results",
    "guidance",
    "profit",
    "revenue",
    "q1",
    "q2",
    "q3",
    "q4",
)
_ANNOUNCEMENT_KEYS = (
    "公告",
    "订单",
    "中标",
    "回购",
    "分红",
    "增持",
    "减持",
    "合作",
    "并购",
    "重组",
    "解禁",
    "股权激励",
    "定增",
    "扩产",
    "投产",
    "量产",
    "capex",
)
_POLICY_KEYS = (
    "政策",
    "国常会",
    "国务院",
    "发改委",
    "工信部",
    "财政部",
    "证监会",
    "央行",
    "降准",
    "降息",
    "医保",
    "补贴",
    "指导意见",
    "征求意见",
    "利率",
    "会议",
)
_THEME_KEYS = (
    "产业链",
    "主题",
    "新品",
    "发布会",
    "模型",
    "算力",
    "chiplet",
    "hbm",
    "机器人",
    "低空",
    "卫星",
    "苹果链",
    "华为链",
    "英伟达链",
)

_DIRECT_LAYERS = {"结构化事件", "龙头公告/业绩", "负面事件", "政策催化"}
_THEME_LAYERS = {"产品/跟踪方向催化", "主题级关键新闻", "海外映射", "行业与主题跟踪"}
_CALENDAR_LAYERS = {"前瞻催化"}
_HEAT_LAYERS = {"新闻热度", "研报/新闻密度"}


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _trim_text(value: Any, *, limit: int = 36) -> str:
    text = _safe_text(value)
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 1)] + "…"


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(stamp):
        return None
    if stamp.tzinfo is not None:
        try:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        except TypeError:
            stamp = stamp.tz_localize(None)
    return stamp


def _contains_any(text: str, needles: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(str(needle).lower() in lowered for needle in needles if str(needle).strip())


def _importance_label(score: int) -> str:
    if score >= 85:
        return _HIGH_IMPORTANCE
    if score >= 65:
        return _MEDIUM_IMPORTANCE
    return _LOW_IMPORTANCE


def _importance_text(value: str) -> str:
    return {
        _HIGH_IMPORTANCE: "高",
        _MEDIUM_IMPORTANCE: "中",
        _LOW_IMPORTANCE: "低",
    }.get(_safe_text(value), "中")


def _classify_event_type(*, layer: str, title: str, source: str = "") -> str:
    blob = " ".join(part for part in (layer, title, source) if part)
    if layer == "政策催化" or _contains_any(blob, _POLICY_KEYS):
        return "政策"
    if layer == "龙头公告/业绩" or _contains_any(blob, _EARNINGS_KEYS):
        return "财报"
    if layer in {"结构化事件", "负面事件"} or _contains_any(blob, _ANNOUNCEMENT_KEYS):
        return "公告"
    if layer in _THEME_LAYERS or _contains_any(blob, _THEME_KEYS):
        return "行业主题事件"
    if layer in _CALENDAR_LAYERS:
        if _contains_any(blob, _EARNINGS_KEYS):
            return "财报"
        if _contains_any(blob, _POLICY_KEYS):
            return "政策"
        if _contains_any(blob, _ANNOUNCEMENT_KEYS):
            return "公告"
        return "行业主题事件"
    return "新闻"


def _event_importance_score(
    *,
    event_type: str,
    layer: str,
    event_date: Any = None,
    as_of: Any = None,
) -> int:
    score = {
        "财报": 92,
        "公告": 90,
        "政策": 88,
        "行业主题事件": 72,
        "新闻": 60,
    }.get(event_type, 55)
    if layer in _DIRECT_LAYERS:
        score += 6
    if layer in _THEME_LAYERS:
        score -= 4
    if layer in _CALENDAR_LAYERS:
        score -= 10
    if layer in _HEAT_LAYERS:
        score -= 24
    event_stamp = _to_timestamp(event_date)
    as_of_stamp = _to_timestamp(as_of)
    if event_stamp is not None and as_of_stamp is not None:
        delta_days = (as_of_stamp.normalize() - event_stamp.normalize()).days
        if delta_days >= 21:
            score -= 30
        elif delta_days >= 8:
            score -= 12
        elif delta_days < 0 and layer in _CALENDAR_LAYERS:
            score += 4
    return max(min(score, 100), 0)


def _analysis_candidates(analysis: Mapping[str, Any]) -> List[Dict[str, Any]]:
    catalyst = dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {})
    generated_at = analysis.get("generated_at")
    candidates: List[Dict[str, Any]] = []
    for raw in list(catalyst.get("evidence") or []):
        item = dict(raw or {})
        layer = _safe_text(item.get("layer")) or "证据"
        title = _safe_text(item.get("title"))
        if not title:
            continue
        event_type = _classify_event_type(
            layer=layer,
            title=title,
            source=_safe_text(item.get("source")),
        )
        importance_score = _event_importance_score(
            event_type=event_type,
            layer=layer,
            event_date=item.get("date"),
            as_of=generated_at,
        )
        candidates.append(
            {
                "title": title,
                "layer": layer,
                "source": _safe_text(item.get("source")),
                "date": _safe_text(item.get("date")),
                "event_type": event_type,
                "importance_score": importance_score,
                "importance": _importance_label(importance_score),
                "direct": layer in _DIRECT_LAYERS and layer not in _CALENDAR_LAYERS,
            }
        )
    for raw in list(catalyst.get("theme_news") or []):
        item = dict(raw or {})
        title = _safe_text(item.get("title"))
        if not title:
            continue
        layer = _safe_text(item.get("layer")) or "主题级关键新闻"
        importance_score = _event_importance_score(
            event_type="行业主题事件",
            layer=layer,
            event_date=item.get("date"),
            as_of=generated_at,
        )
        candidates.append(
            {
                "title": title,
                "layer": layer,
                "source": _safe_text(item.get("source")),
                "date": _safe_text(item.get("date")),
                "event_type": "行业主题事件",
                "importance_score": importance_score,
                "importance": _importance_label(importance_score),
                "direct": False,
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            -int(item.get("importance_score", 0) or 0),
            _safe_text(item.get("date")),
            _safe_text(item.get("title")),
        ),
    )


def _briefing_candidates(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    day_theme = _safe_text(payload.get("day_theme"))
    candidates: List[Dict[str, Any]] = []
    for line in list(payload.get("core_event_lines") or []):
        text = _safe_text(line)
        if not text:
            continue
        title_match = re.search(r"\*\*(.+?)\*\*", text)
        title = _safe_text(title_match.group(1) if title_match else text.splitlines()[0])
        event_type = _classify_event_type(layer="核心事件", title=title)
        importance_score = _event_importance_score(
            event_type=event_type,
            layer="核心事件",
            event_date=payload.get("generated_at"),
            as_of=payload.get("generated_at"),
        )
        candidates.append(
            {
                "title": title,
                "layer": "核心事件",
                "source": "",
                "date": _safe_text(payload.get("generated_at")),
                "event_type": event_type,
                "importance_score": importance_score,
                "importance": _importance_label(importance_score),
                "direct": event_type in {"财报", "政策"},
            }
        )
    for row in list(payload.get("theme_tracking_rows") or [])[:2]:
        direction = _safe_text(row[0] if len(row) > 0 else "")
        catalyst = _safe_text(row[1] if len(row) > 1 else "")
        title = " / ".join(part for part in (direction, catalyst) if part)
        if not title:
            continue
        importance_score = _event_importance_score(
            event_type="行业主题事件",
            layer="行业与主题跟踪",
            event_date=payload.get("generated_at"),
            as_of=payload.get("generated_at"),
        )
        candidates.append(
            {
                "title": title,
                "layer": "行业与主题跟踪",
                "source": day_theme,
                "date": _safe_text(payload.get("generated_at")),
                "event_type": "行业主题事件",
                "importance_score": importance_score,
                "importance": _importance_label(importance_score),
                "direct": False,
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            -int(item.get("importance_score", 0) or 0),
            _safe_text(item.get("title")),
        ),
    )


def _analysis_status(
    *,
    analysis: Mapping[str, Any],
    top_candidate: Mapping[str, Any] | None,
) -> tuple[str, str]:
    catalyst = dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {})
    coverage = dict(catalyst.get("coverage") or {})
    web_review = dict(analysis.get("catalyst_web_review") or catalyst.get("web_review") or {})
    if bool(web_review.get("completed")):
        return "已消化", "联网复核已完成，当前更清楚的是直接催化和主题级背景的边界。"
    if _safe_text(coverage.get("diagnosis")) == "suspected_search_gap" or bool(coverage.get("ai_web_search_recommended")):
        return "待复核", "当前命中 `suspected_search_gap`，需要先补联网复核，再判断是否真的没有新增催化。"
    if _safe_text(coverage.get("diagnosis")) == "stale_live_only":
        return "待补充", "当前命中的多是旧闻回放或背景线索，新情报还不足以回答这次为什么升级。"
    if _safe_text(coverage.get("diagnosis")) == "theme_only_live":
        return "待补充", "当前只有主题级情报，尚未进入公司/产品级直接催化，先别把背景线索误读成动作点。"
    if not top_candidate:
        return "待补充", "当前没有足够可前置的结构化事件或高质量新闻，暂时还不能明确回答这件事改变了什么。"
    if bool(top_candidate.get("direct")) and _safe_text(top_candidate.get("importance")) in {_HIGH_IMPORTANCE, _MEDIUM_IMPORTANCE}:
        return "已消化", "当前已经有足以写进研究判断的直接事件，可以回答它改变了什么，但动作仍要看价格确认。"
    return "待补充", "当前更像标题级线索、前瞻日历或主题背景，研究判断还要继续补传导和验证层。"


def _briefing_status(payload: Mapping[str, Any], top_candidate: Mapping[str, Any] | None) -> tuple[str, str]:
    quality_blob = "\n".join(_safe_text(item) for item in list(payload.get("quality_lines") or []))
    if "待 AI 联网复核" in quality_blob or "待复核" in quality_blob:
        return "待复核", "当前新闻覆盖或主题检索仍需复核，不把空白直接写成没有主线。"
    if not top_candidate:
        return "待补充", "当前晨报更多是在整理盘面和日历，尚未形成足够明确的事件传导主线。"
    if _safe_text(top_candidate.get("importance")) == _HIGH_IMPORTANCE and bool(top_candidate.get("direct")):
        return "已消化", "当前已经能回答市场在交易什么，以及这条线先影响哪一层资产。"
    return "待补充", "当前更像主线候选或背景催化，仍要继续补传导链和持续性验证。"


def _what_changed(status: str, *, event_type: str) -> str:
    if status == "待复核":
        return "当前首先改变的是证据边界：这条线可能不是零催化，而是要先把主题背景和直接催化重新分开复核。"
    if event_type == "财报":
        if status == "已消化":
            return "核心变化在盈利/指引预期，后续要看它能否继续改写利润和估值锚。"
        return "已经把研究重心推到盈利/指引窗口，但还要补清它到底改变了收入、利润还是估值预期。"
    if event_type == "公告":
        if status == "已消化":
            return "核心变化在公司层面的订单、产能或资本动作，后续要看能否转成业绩兑现。"
        return "已经把注意力推到公司层动作，但还要补清它会怎样传导到盈利、现金流或股东回报。"
    if event_type == "政策":
        if status == "已消化":
            return "核心变化在行业景气和资金偏好，后续要看政策是否能落到订单、需求或估值重定价。"
        return "已经把主题从背景推进到政策变量，但还要补清政策究竟改变了需求、供给还是风险偏好。"
    if event_type == "行业主题事件":
        if status == "已消化":
            return "核心变化在主题关注度和产业链映射，后续要看能否收敛成更直接的公司或产品催化。"
        return "已经把板块线索指向具体主题事件，但还要补它能否扩散成持续主线，而不是一日热度。"
    if status == "已消化":
        return "核心变化在短线风险偏好和预期差，后续要看是否能转成持续承接。"
    return "目前更多只是新闻提示或情绪脉冲，尚不足以单独改写研究判断。"


def _summary_rows(
    *,
    status: str,
    top_event_type: str,
    top_event_title: str,
    importance: str,
    what_changed: str,
    status_reason: str,
) -> List[List[str]]:
    top_label = "未命中可前置事件"
    if top_event_type and top_event_title:
        top_label = f"{top_event_type}｜{top_event_title}"
    elif top_event_type:
        top_label = top_event_type
    rows = [
        ["当前状态", status],
        ["最该前置的事件", top_label],
        ["重要性", _importance_text(importance)],
        ["这件事改变了什么", what_changed],
    ]
    if status_reason:
        rows.append(["当前边界", status_reason])
    return rows


def _summary_lines(rows: Sequence[Sequence[Any]]) -> List[str]:
    lines: List[str] = []
    for row in rows:
        if len(row) < 2:
            continue
        key = _safe_text(row[0])
        value = _safe_text(row[1])
        if key and value:
            lines.append(f"{key}：{value}")
    return lines


def _homepage_line(status: str, top_event_type: str, top_event_title: str, what_changed: str) -> str:
    if status == "待复核":
        return f"事件消化：`{status}`。{what_changed}"
    if top_event_type and top_event_title:
        return f"事件消化：`{status}`。当前更该前置的是 `{top_event_type}`「{_trim_text(top_event_title)}」，{what_changed}"
    if top_event_type:
        return f"事件消化：`{status}`。当前更该前置的是 `{top_event_type}`，{what_changed}"
    return f"事件消化：`{status}`。{what_changed}"


def build_analysis_event_digest(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    candidates = _analysis_candidates(analysis)
    top = dict(candidates[0]) if candidates else {}
    top_event_type = _safe_text(top.get("event_type")) or "新闻"
    top_event_title = _safe_text(top.get("title"))
    importance = _safe_text(top.get("importance")) or _LOW_IMPORTANCE
    status, status_reason = _analysis_status(analysis=analysis, top_candidate=top or None)
    what_changed = _what_changed(status, event_type=top_event_type)
    rows = _summary_rows(
        status=status,
        top_event_type=top_event_type,
        top_event_title=top_event_title,
        importance=importance,
        what_changed=what_changed,
        status_reason=status_reason,
    )
    return {
        "contract_version": "event_digest.v1",
        "status": status,
        "top_event_type": top_event_type,
        "top_event_title": top_event_title or "未命中可前置事件",
        "importance": importance,
        "what_changed": what_changed,
        "status_reason": status_reason,
        "homepage_line": _homepage_line(status, top_event_type, top_event_title, what_changed),
        "summary_rows": rows,
        "summary_lines": _summary_lines(rows),
    }


def build_briefing_event_digest(payload: Mapping[str, Any]) -> Dict[str, Any]:
    candidates = _briefing_candidates(payload)
    top = dict(candidates[0]) if candidates else {}
    top_event_type = _safe_text(top.get("event_type")) or ("行业主题事件" if _safe_text(payload.get("day_theme")) else "新闻")
    top_event_title = _safe_text(top.get("title")) or _safe_text(payload.get("day_theme")) or "今日主线仍在整理中"
    importance = _safe_text(top.get("importance")) or _LOW_IMPORTANCE
    status, status_reason = _briefing_status(payload, top or None)
    what_changed = _what_changed(status, event_type=top_event_type)
    rows = _summary_rows(
        status=status,
        top_event_type=top_event_type,
        top_event_title=top_event_title,
        importance=importance,
        what_changed=what_changed,
        status_reason=status_reason,
    )
    return {
        "contract_version": "event_digest.v1",
        "status": status,
        "top_event_type": top_event_type,
        "top_event_title": top_event_title,
        "importance": importance,
        "what_changed": what_changed,
        "status_reason": status_reason,
        "homepage_line": _homepage_line(status, top_event_type, top_event_title, what_changed),
        "summary_rows": rows,
        "summary_lines": _summary_lines(rows),
    }


def summarize_event_digest_contract(digest: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(digest or {})
    status = _safe_text(payload.get("status"))
    top_event_type = _safe_text(payload.get("top_event_type"))
    what_changed = _safe_text(payload.get("what_changed"))
    summary: Dict[str, Any] = {
        "contract_version": "event_digest.v1",
        "status": status,
        "top_event_type": top_event_type,
        "top_event_title": _safe_text(payload.get("top_event_title")),
        "importance": _safe_text(payload.get("importance")),
        "what_changed": what_changed,
        "status_reason": _safe_text(payload.get("status_reason")),
    }
    compact: Dict[str, Any] = {}
    for key, value in summary.items():
        if key == "contract_version" or _safe_text(value):
            compact[key] = value
    return compact
