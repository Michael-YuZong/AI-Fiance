"""Editor payload builders and thesis-first homepage renderers."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.output.theme_playbook import (
    build_theme_playbook_context,
    playbook_hint_line,
    sector_subtheme_bridge_items,
    summarize_sector_subtheme_bridge,
)
from src.output.pick_ranking import analysis_is_actionable, rank_market_items, strategy_confidence_status
from src.output.technical_signal_labels import compact_technical_signal_text
from src.output.event_digest import (
    build_event_digest,
    effective_intelligence_link,
    event_digest_action_line,
    event_digest_homepage_lines,
    format_intelligence_attributes,
    intelligence_attribute_labels,
)
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

def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _markdown_link(label: str, link: str) -> str:
    text = _safe_text(label)
    url = _safe_text(link)
    if text and url:
        return f"[{text}]({url})"
    return text or url


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
    latest_signal_at = _safe_text(digest.get("latest_signal_at"))
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
    importance_reason = _safe_text(snapshot.get("importance_reason"))
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
    if importance_reason:
        detail_parts.append(f"当时的优先级判断是：{importance_reason}")
    if detail_parts:
        parts.append("；".join(detail_parts))
    return "；".join(parts) or "上次还没有稳定的 thesis 口径，这次先以当前判断为准。"


def _current_judgment_view(subject: Mapping[str, Any], *, bucket: str = "") -> str:
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(subject.get("trade_state")) or _safe_text(dict(subject.get("narrative") or {}).get("judgment", {}).get("state"))
    direction = _safe_text(action.get("direction"))
    theme = _safe_text(dict(subject.get("day_theme") or {}).get("label")) or _safe_text(subject.get("day_theme"))
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
    importance_reason = _safe_text(digest.get("importance_reason"))
    parts: List[str] = []
    if detail:
        parts.append(detail)
    if impact_summary:
        parts.append(f"更直接影响 `{impact_summary}`")
    if thesis_scope:
        parts.append(f"当前更像 `{thesis_scope}`")
    if importance_reason:
        parts.append(f"优先级判断是：{importance_reason}")
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


def _load_thesis_record(symbol: Any, thesis_repo: ThesisRepository | None = None) -> Dict[str, Any]:
    repo = thesis_repo or ThesisRepository()
    normalized = _safe_text(symbol)
    if not normalized:
        return {}
    try:
        return dict(repo.get(normalized) or {})
    except Exception:
        return {}


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
    if observe_only or any(token in blob for token in ("观察", "暂不", "回避", "观望")):
        return "今天没有有效动作信号；后文主要用来说明边界、观察条件和参考信息，不要把后文细节误读成推荐升级。"
    return ""


def _macro_lines(regime: Mapping[str, Any], day_theme: str, market_hint: str = "", flow_hint: str = "") -> List[str]:
    lines: List[str] = []
    regime_name = _safe_text(regime.get("current_regime"))
    if regime_name:
        label = REGIME_LABELS.get(regime_name, regime_name)
        if day_theme:
            lines.append(f"中期背景更接近 `{label}`，当天主线则落在 `{day_theme}`，不能把短线主线和中期 regime 混成同一层判断。")
        else:
            lines.append(f"中期背景更接近 `{label}`，这决定了首页先看顺风还是逆风，而不是只看单日涨跌。")
    if flow_hint:
        lines.append(flow_hint)
    elif market_hint:
        lines.append(market_hint)
    if day_theme and not any(day_theme in line for line in lines):
        lines.append(f"今天市场更明确在交易 `{day_theme}` 这条主线，后文细节都应该回到这条主线上理解。")
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
            lines.append("情绪和热度没有拖后腿，但也还没强到可以单靠拥挤度去升级成直接动作。")
        else:
            lines.append("情绪与热度更像辅助信息，当前还不足以单独把它写成新的直接催化。")
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
    asset_type = _safe_text(subject.get("asset_type"))
    max_items = 4 if asset_type == "cn_etf" else 2
    symbol = _safe_text(subject.get("symbol"))

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

    if not lines:
        for item in list(news_report.get("items") or [])[:max_items]:
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
            lines.append(f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}")

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
            lines.append(f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}")
    if not lines:
        for item in list(catalyst.get("theme_news") or [])[:max_items]:
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
            tags = _intelligence_tags(row, as_of=as_of, theme_level=True, previous_reviewed_at=previous_reviewed_at)
            tag_text = f"`{format_intelligence_attributes(tags)}` · " if tags else ""
            theme_line = f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}"
            lines.append(f"主题级新闻：{theme_line}")
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
            lines.append(f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}")
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
            lines.append(f"{tag_text}{prefix}：{title_text}" if prefix else f"{tag_text}{title_text}")
    return lines[:max_items]


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


def _news_lines_with_event_digest(subject: Mapping[str, Any], event_digest: Mapping[str, Any]) -> List[str]:
    digest = dict(event_digest or {})
    raw_news_lines = _news_lines(subject, previous_reviewed_at=digest.get("previous_reviewed_at"))
    digest_lines = event_digest_homepage_lines(digest, [])
    lines: List[str] = []
    seen_keys: set[str] = set()
    limit = _homepage_news_limit(subject)
    if _safe_text(digest.get("thesis_scope")) == "历史基线":
        raw_news_lines = [item for item in raw_news_lines if "旧闻回放" not in item]
    linked_news_lines = [item for item in raw_news_lines if "](" in item]
    plain_news_lines = [item for item in raw_news_lines if item not in linked_news_lines]
    if linked_news_lines:
        lead_digest_lines = [item for item in digest_lines if "信号强弱" in item or "结论：" in item]
        for item in (lead_digest_lines[:1] or digest_lines[:1]):
            _append_unique_news_line(lines, seen_keys, item, limit=limit)
        for item in linked_news_lines[: max(limit - 1, 1)]:
            _append_unique_news_line(lines, seen_keys, item, limit=limit)
    else:
        digest_fallback_lines = digest_lines[:2] if not raw_news_lines else digest_lines
        for item in digest_fallback_lines:
            _append_unique_news_line(lines, seen_keys, item, limit=limit)
        if not digest_lines:
            _append_unique_news_line(lines, seen_keys, _event_digest_history_line(digest), limit=limit)
            if not _safe_text(digest.get("history_note")):
                _append_unique_news_line(lines, seen_keys, _event_digest_signal_line(digest), limit=limit)
    for item in raw_news_lines:
        _append_unique_news_line(lines, seen_keys, item, limit=limit)
    for item in plain_news_lines:
        _append_unique_news_line(lines, seen_keys, item, limit=limit)
    deduped: List[str] = []
    final_seen: set[str] = set()
    for item in lines:
        _append_unique_news_line(deduped, final_seen, item, limit=limit)
    return deduped[:limit]


def _micro_lines(subject: Mapping[str, Any]) -> List[str]:
    dimensions = dict(subject.get("dimensions") or {})
    catalyst_web_review = dict(subject.get("catalyst_web_review") or dict(dimensions.get("catalyst") or {}).get("web_review") or {})
    strongest, weakest = _top_bottom_dimensions(dimensions)
    strongest_label = dict(DIMENSION_LABELS).get(strongest[0], strongest[0])
    weakest_label = dict(DIMENSION_LABELS).get(weakest[0], weakest[0])
    strongest_summary = _dimension_summary(dimensions, strongest[0]) or "当前是相对更能支撑继续看的那一项。"
    weakest_summary = _dimension_summary(dimensions, weakest[0]) or "这是当前最影响动作升级的一项。"
    technical_signal_text = compact_technical_signal_text(subject.get("history"))
    contradiction = "逻辑没坏，但确认还没补齐。"
    if strongest[0] == "fundamental" and weakest[0] in {"technical", "relative_strength"}:
        contradiction = "底层逻辑不算差，但价格和动量还没把这层逻辑翻译成买点。"
    elif strongest[0] == "catalyst" and weakest[0] == "risk":
        contradiction = "事件并不弱，但风险收益比还没站到舒服的一侧。"
    elif weakest[0] == "catalyst":
        contradiction = "当前最大问题不是没故事，而是直接催化和确认还不够。"
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


def _action_lines(subject: Mapping[str, Any], *, observe_only: bool = False, event_digest: Mapping[str, Any] | None = None) -> List[str]:
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
    watch_levels = ""
    if buy_range and trim_range:
        watch_levels = f"回踩先看 `{buy_range}` 一带的承接；反弹再看 `{trim_range}` 一带的承压"
    elif stop_ref > 0 and target_ref > 0:
        watch_levels = f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；上沿先看 `{target_ref:.3f}` 附近能不能放量突破"
    elif buy_range:
        watch_levels = f"先看 `{buy_range}` 一带的承接"
    elif stop_ref > 0:
        watch_levels = f"先看 `{stop_ref:.3f}` 上方能不能稳住"
    entry_focus = _entry_focus_text(action.get("entry")) or "技术确认和相对强弱是否一起改善"

    soft_observe = observe_only or any(token in f"{direction} {trade_state}" for token in ("观察", "暂不", "回避"))
    digest_action_line = event_digest_action_line(event_digest or {}, observe_only=soft_observe)
    strategy_guard_line = _strategy_background_upgrade_guard_line(subject, observe_only=soft_observe)

    if soft_observe:
        if digest_action_line:
            lines.append(digest_action_line)
        if strategy_guard_line:
            lines.append(strategy_guard_line)
        lines.append(f"空仓先别急着直接找买点，更合理的是先看 `{entry_focus}` 这类确认。")
        observe_position_clause = ""
        if position:
            observe_position_clause = f"；即便后面条件回来，首次建仓也先按 `{position}` 理解"
        lines.append(
            "已有仓位先按观察名单理解，不因为今天这份稿去追补仓"
            f"{observe_position_clause}；真正的执行升级，仍要等确认条件先回来。"
        )
        if watch_levels:
            lines.append(f"先把关键位当观察点看：{watch_levels}。")
    else:
        if digest_action_line:
            lines.append(digest_action_line)
        lines.append(f"如果要参与，先按 `{entry_focus or '回踩确认'}` 这类确认去等，而不是把这条结论当成当天就要追进去。")
        lines.append(f"仓位先按 `{_safe_text(action.get('position')) or '小仓分批'}`，止损按 `{_safe_text(action.get('stop')) or '关键支撑失效'}` 管理。")
        if watch_levels:
            lines.append(f"关键位先看 {watch_levels}。")
    if _safe_text(horizon.get("fit_reason")):
        lines.append(f"当前更适合按 `{_safe_text(horizon.get('label')) or '当前周期'}` 理解：{_safe_text(horizon.get('fit_reason'))}")
    return lines[:4]


def _subject_display_label(subject: Mapping[str, Any]) -> str:
    name = _safe_text(subject.get("name"))
    symbol = _safe_text(subject.get("symbol"))
    if name and symbol:
        return f"{name} ({symbol})"
    return name or symbol or "当前对象"


def _conclusion_line(subject: Mapping[str, Any], *, observe_only: bool = False) -> str:
    action = dict(subject.get("action") or {})
    trade_state = _safe_text(subject.get("trade_state")) or _safe_text(action.get("direction"))
    if observe_only or any(token in trade_state for token in ("观察", "暂不", "回避")):
        return f"结论：今天更适合把它放在观察名单里，而不是直接升级成交易动作；当前建议仍是 `{trade_state or '观察为主'}`。"
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
    template_line = "真正升级前，先把观察重点和组合预演看清，不先给精确仓位、止损和目标模板。"
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
        "news_lines": [item for item in news_lines if _safe_text(item)],
        "sentiment_lines": [item for item in sentiment_lines if _safe_text(item)],
        "micro_lines": [item for item in micro_lines if _safe_text(item)],
        "action_lines": [item for item in action_lines if _safe_text(item)],
        "conclusion": conclusion,
    }


def _subject_theme_context(subject: Mapping[str, Any], *, explicit_key: str = "") -> Dict[str, Any]:
    base_values = (
        explicit_key,
        subject.get("name"),
        subject.get("symbol"),
        dict(subject.get("metadata") or {}).get("sector"),
        subject.get("taxonomy_summary"),
        subject.get("fund_sections"),
    )
    context_values = (
        subject.get("notes"),
        dict(subject.get("day_theme") or {}).get("label"),
        dict(subject.get("narrative") or {}).get("headline"),
        dict(subject.get("narrative") or {}).get("playbook"),
    )
    identity_context = build_theme_playbook_context(*base_values)
    context_context = build_theme_playbook_context(
        *base_values,
        *context_values,
    )
    if identity_context.get("key") and _safe_text(identity_context.get("playbook_level")) != "sector":
        if (
            _safe_text(context_context.get("theme_match_status")) == "ambiguous_conflict"
            and _safe_text(context_context.get("playbook_level")) == "sector"
        ):
            return context_context
        return identity_context
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
        return enriched_context
    own_context = build_theme_playbook_context(
        *base_values,
        subject.get("notes"),
    )
    if own_context.get("key"):
        return own_context
    return build_theme_playbook_context(
        *base_values,
        subject.get("notes"),
        dict(subject.get("day_theme") or {}).get("label"),
        dict(subject.get("narrative") or {}).get("headline"),
        dict(subject.get("narrative") or {}).get("playbook"),
    )


def _theme_lines(playbook: Mapping[str, Any], subject: Mapping[str, Any]) -> List[str]:
    if not playbook:
        return ["这只标的所在主题当前没有命中 playbook，首页更应该老实依赖当天事实层，不要硬编主题故事。"]
    lines: List[str] = []
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
    if any(token in joined_news for token in ("智谱", "新易盛", "光模块", "算力", "AI", "大模型", "agent")):
        strong_growth.append("AI应用/算力")
    risk_on_tail = ""
    if any(token in joined_news for token in ("停火", "休战", "中东", "ceasefire", "truce", "缓和")):
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

    def _signal_hint(title: str, category: str = "") -> tuple[str, str]:
        blob = f"{title} {category}".lower()
        if any(token in blob for token in ("停火", "休战", "缓和", "结束战争", "ceasefire", "truce", "de-escalat")):
            return "地缘缓和", "黄金/原油/风险偏好"
        if any(token in blob for token in ("伊朗", "以色列", "中东", "war", "strike", "missile", "conflict")):
            return "地缘扰动", "黄金/原油/风险偏好"
        if any(token in blob for token in ("创新药", "医药", "制药", "药业", "cxo", "fda", "临床", "license-out", "bd", "授权")):
            return "医药催化", "创新药/医药"
        if any(token in blob for token in ("智谱", "kimi", "deepseek", "大模型", "模型", "agent")):
            return "AI应用催化", "AI应用/国产模型"
        if any(token in blob for token in ("新易盛", "中际旭创", "华工科技", "cpo", "光模块", "算力", "semiconductor", "芯片", "nvidia", "nvda")):
            return "海外科技映射", "AI算力/光模块"
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
        if signal in {"医药催化", "AI应用催化", "海外科技映射"}:
            return f"偏利多，先看 `{target}` 能否继续拿到价格与成交确认。"
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

    def _format_item(row: Mapping[str, Any]) -> str:
        title = _safe_text(row.get("title"))
        if not title:
            return ""
        source = _safe_text(row.get("source") or row.get("configured_source"))
        date = _safe_text(row.get("published_at") or row.get("date"))
        link = _safe_text(row.get("link"))
        category = _safe_text(row.get("category"))
        signal_type = _safe_text(row.get("signal_type"))
        signal_strength = _safe_text(row.get("signal_strength"))
        impact = ""
        inferred_signal, inferred_impact = _signal_hint(title, category)
        if not signal_type:
            signal_type = inferred_signal
        elif inferred_signal == "地缘缓和" and signal_type == "地缘扰动":
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
        prefix_parts = [part for part in (date, source) if part]
        if signal_type:
            prefix_parts.append(signal_type)
        if signal_strength:
            prefix_parts.append(f"强度 {signal_strength}")
        if tags:
            prefix_parts.append(format_intelligence_attributes(tags))
        prefix = " · ".join(prefix_parts)
        title_text = _markdown_link(title, link)
        detail = f"{prefix}：{title_text}" if prefix else title_text
        if conclusion:
            detail += f"；结论：{conclusion}"
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
    def _append_line(text: str) -> bool:
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
        theme_lines.append(detail)
    raw_news_lines.sort(key=_priority_score, reverse=True)
    theme_lines.sort(key=_priority_score, reverse=True)
    linked_market_lines.sort(key=_priority_score, reverse=True)

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
            if _append_line(text):
                return lines[:max_lines]
    if raw_news_lines:
        history_line = _event_digest_history_line(digest)
        if history_line:
            _append_unique_line(lines, history_line, limit=4)
        return lines[:max_lines]
    if lines:
        return lines[:max_lines]
    return event_digest_homepage_lines(digest, [])


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
    no_signal = _no_signal_notice(trade_state, direction)
    thesis = _load_thesis_record(_safe_text(subject.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
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
    packet = {
        "report_type": "stock_analysis",
        "packet_version": "editor-v2",
        "subject": {
            "name": _safe_text(subject.get("name")),
            "symbol": _safe_text(subject.get("symbol")),
            "asset_type": _safe_text(subject.get("asset_type")),
        },
        "today_context": {
            "regime": regime,
            "day_theme": day_theme,
        },
        "theme_playbook": playbook,
        "event_digest": event_digest,
        "what_changed": build_what_changed_summary(subject, event_digest, thesis=thesis),
        "homepage": _build_homepage_v2(
            summary=summary,
            macro_lines=_macro_lines(regime, day_theme, market_hint="", flow_hint=""),
            theme_lines=_theme_lines(playbook, subject),
            news_lines=_news_lines_with_event_digest(subject, event_digest),
            sentiment_lines=_sentiment_lines(subject),
            micro_lines=micro_lines,
            action_lines=_soften_stock_analysis_action_lines(_action_lines(subject, event_digest=event_digest)),
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
    packet = {
        "report_type": "etf_pick",
        "packet_version": "editor-v2",
        "subject": {
            "name": _safe_text(winner.get("name")),
            "symbol": _safe_text(winner.get("symbol")),
            "asset_type": _safe_text(winner.get("asset_type") or "cn_etf"),
        },
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
            macro_lines=_macro_lines(
                regime,
                day_theme,
                market_hint=_market_hint_from_context(selection_context, regime, day_theme),
                flow_hint="",
            ),
            theme_lines=_theme_lines(playbook, subject),
            news_lines=news_lines,
            sentiment_lines=_sentiment_lines(subject, selection_context),
            micro_lines=micro_lines,
            action_lines=_action_lines(subject, observe_only=observe_only, event_digest=event_digest),
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
    subject_pool = list(watch_positive or ranked_pool)
    has_actionable = any(analysis_is_actionable(item, watch_symbols) for item in ranked_pool)
    observe_only = not has_actionable
    subject = (
        _preferred_stock_pick_subject(
            subject_pool,
            watch_symbols,
            prefer_actionable=has_actionable,
        )
        or _first_named_item(watch_positive)
        or _first_named_item(top)
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
            f"今天这份个股{'推荐' if has_actionable else '观察'}稿更适合按 `{market_label}` 范围理解；"
            f"当前主线偏 `{day_theme or '未识别'}`，"
            + (
                "已经有少数标的从方向判断走到可执行边界。"
                if has_actionable
                else "主题和方向还在，但大多数标的仍缺价格与动量确认。"
            ),
        )
        if _safe_text(part)
    )
    thesis = _load_thesis_record(_safe_text(subject_context.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject_context, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
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
    return {
        "report_type": "stock_pick",
        "packet_version": "editor-v2",
        "subject": {
            "name": _safe_text(subject.get("name")) or "stock_pick",
            "symbol": _safe_text(subject.get("symbol")),
            "asset_type": _safe_text(subject.get("asset_type") or "cn_stock"),
        },
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
            macro_lines=_macro_lines(regime, day_theme, market_hint=f"今天先按 `{market_label}` 范围看结构性机会，不把它理解成全市场统一主线。"),
            theme_lines=_theme_lines(playbook, subject_context),
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
    strategy_confidence_line = _strategy_background_confidence_line(subject)
    micro_lines = _micro_lines(subject)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    return {
        "report_type": "fund_pick",
        "packet_version": "editor-v2",
        "subject": {
            "name": _safe_text(winner.get("name")),
            "symbol": _safe_text(winner.get("symbol")),
            "asset_type": _safe_text(winner.get("asset_type") or "cn_fund"),
        },
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
            macro_lines=_macro_lines(
                regime,
                day_theme,
                market_hint=_market_hint_from_context(selection_context, regime, day_theme),
            ),
            theme_lines=_theme_lines(playbook, subject),
            news_lines=_news_lines_with_event_digest(subject, event_digest)
            or [_no_intelligence_homepage_line()],
            sentiment_lines=_sentiment_lines(subject, selection_context),
            micro_lines=_inject_conflict_line(micro_lines, subject, playbook),
            action_lines=_action_lines(subject, observe_only=observe_only, event_digest=event_digest),
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
        "subject": {
            "name": _safe_text(payload.get("mode")) or "briefing",
            "symbol": "",
            "asset_type": "market_briefing",
        },
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
    thesis = _load_thesis_record(_safe_text(subject.get("symbol")))
    event_digest = _annotate_event_digest_with_history(
        build_event_digest(subject, theme_playbook=playbook, previous_reviewed_at=_thesis_reviewed_at(thesis)),
        thesis,
    )
    news_lines = _news_lines(subject, previous_reviewed_at=event_digest.get("previous_reviewed_at"))
    if not news_lines:
        news_lines = [_no_intelligence_homepage_line()]
    news_lines = event_digest_homepage_lines(event_digest, news_lines)
    strategy_confidence_line = _strategy_background_confidence_line(subject)
    micro_lines = _micro_lines(subject)
    if strategy_confidence_line:
        micro_lines = [strategy_confidence_line, *micro_lines]
    portfolio_overlap_line = _portfolio_overlap_homepage_line(subject)
    if portfolio_overlap_line:
        micro_lines = [*micro_lines[:1], portfolio_overlap_line, *micro_lines[1:]] if micro_lines else [portfolio_overlap_line]
    return {
        "report_type": "scan",
        "packet_version": "editor-v2",
        "subject": {
            "name": _safe_text(subject.get("name")),
            "symbol": _safe_text(subject.get("symbol")),
            "asset_type": _safe_text(subject.get("asset_type")),
        },
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
            macro_lines=_macro_lines(regime, day_theme, market_hint="", flow_hint=""),
            theme_lines=_theme_lines(playbook, subject),
            news_lines=news_lines,
            sentiment_lines=_sentiment_lines(subject),
            micro_lines=_inject_conflict_line(micro_lines, subject, playbook),
            action_lines=_action_lines(subject, event_digest=event_digest),
            conclusion=_conclusion_line(subject),
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
    homepage = dict(packet.get("homepage") or {})
    playbook = dict(packet.get("theme_playbook") or {})
    event_digest = dict(packet.get("event_digest") or {})
    what_changed = dict(packet.get("what_changed") or {})
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
            "- ...",
            "",
            "### 动作建议与结论",
            "",
            "- ...",
            "",
            "结论：...",
            "```",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"
