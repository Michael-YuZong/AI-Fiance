"""Client-facing report renderers."""

from __future__ import annotations

from collections import Counter, defaultdict
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from src.output.analysis_charts import AnalysisChartRenderer
from src.output.opportunity_report import (
    OpportunityReportRenderer,
    _catalyst_structure_rows,
    _catalyst_factor_rows,
    _decision_gate_explanation,
    _dimension_summary_text,
    _dimension_rows as _detail_dimension_rows,
    _factor_rows,
    _hard_check_inline,
    _hard_check_rows,
    _intraday_lines,
    _manager_profile_text,
    _primary_upgrade_trigger,
    _scan_topline_text,
    _visual_lines,
)
from src.output.editor_payload import (
    _humanize_news_summary_line,
    _strategy_background_confidence,
    build_briefing_editor_packet,
    build_etf_pick_editor_packet,
    build_fund_pick_editor_packet,
    build_scan_editor_packet,
    build_stock_analysis_editor_packet,
    build_stock_pick_editor_packet,
    render_editor_homepage,
)
from src.output.event_digest import (
    _is_diagnostic_intelligence_row,
    compact_importance_reason,
    effective_intelligence_link,
    format_intelligence_attributes,
    intelligence_attribute_labels,
    intelligence_source_lane,
    render_event_digest_section,
    review_history_context_text,
    sort_event_items,
)
from src.output.theme_playbook import representative_theme_label, subject_theme_label, subject_theme_terms
from src.output.pick_ranking import (
    analysis_is_actionable as _shared_analysis_is_actionable,
    bucket_priority as _shared_bucket_priority,
    rank_market_items as _shared_rank_market_items,
    recommendation_bucket as _shared_recommendation_bucket,
    score_dimension as _shared_score_dimension,
    strategy_confidence_status as _shared_strategy_confidence_status,
)
from src.output.technical_signal_labels import append_technical_trigger_text, compact_technical_signal_text
from src.processors.factor_meta import factor_meta_payload
from src.processors.provenance import build_analysis_provenance
from src.processors.trade_handoff import portfolio_whatif_handoff
from src.utils.fund_taxonomy import taxonomy_rows, uses_index_mainline

GENERIC_DIMENSION_SUMMARIES = {
    "技术面有亮点，但还没有形成满配共振。",
    "结构化事件已出现，但高质量公司级新闻确认还不够，当前更像事件在前、市场共识在后。",
    "当前未抓到高质量公司级新闻或结构化事件，先按信息不足处理，不直接视为利空。",
    "风险收益比不占优，需更严控节奏。",
}
GENERIC_FACTOR_SIGNALS = {
    "未命中高置信个股直连新闻，个股催化暂不计分",
    "未命中高置信个股直连新闻，海外映射暂不计分",
    "未命中高置信个股直连新闻，个股催化暂不计分",
}
STRONG_FACTOR_FAMILY_LABELS = {
    "J-1": "价量结构",
    "J-2": "季节/事件窗",
    "J-3": "宽度/筹码",
    "J-4": "质量/盈利",
    "J-5": "ETF/基金专属",
    "M-1": "宏观/风格",
}
STRONG_FACTOR_FAMILY_PRIORITY = {
    "J-1": 0,
    "J-5": 1,
    "J-3": 2,
    "J-2": 3,
    "J-4": 4,
    "M-1": 5,
}
RELATED_ETF_PROXY_MAP = [
    (
        ("人工智能", "ai", "算力", "软件", "科技"),
        [
            ("515070", "人工智能ETF", "不想直接扛单票业绩和高价波动时，先用 AI 主题篮子跟方向。"),
        ],
    ),
    (
        ("半导体", "芯片", "光模块"),
        [
            ("512480", "半导体ETF", "更适合把单票波动换成半导体篮子暴露。"),
            ("588200", "科创芯片ETF", "想保留科创芯片弹性但不想只押单票时，可先用芯片ETF承接。"),
        ],
    ),
    (
        ("电网", "电力", "特高压", "储能"),
        [
            ("561380", "电网 ETF", "更适合用设备链篮子替代单票执行。"),
        ],
    ),
    (
        ("医药", "创新药", "医疗"),
        [
            ("513120", "港股创新药ETF", "更适合用医药方向篮子替代单票事件波动。"),
        ],
    ),
    (
        ("有色", "铜", "铝", "黄金股"),
        [
            ("512400", "有色金属ETF", "更适合用资源篮子替代单票周期波动。"),
        ],
    ),
]

_CHART_RENDERER: AnalysisChartRenderer | None = None


def _ensure_analysis_visuals(item: Mapping[str, Any] | None) -> Dict[str, str]:
    if not isinstance(item, dict):
        return {}
    visuals = dict(item.get("visuals") or {})
    if visuals:
        return visuals
    history = item.get("history")
    if not isinstance(history, pd.DataFrame) or history.empty:
        return {}
    global _CHART_RENDERER
    if _CHART_RENDERER is None:
        _CHART_RENDERER = AnalysisChartRenderer()
    visuals = _CHART_RENDERER.render(item)
    if visuals:
        item["visuals"] = dict(visuals)
    return dict(item.get("visuals") or {})


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):+.1%}"
    except (TypeError, ValueError):
        return "—"


def _fmt_ratio(value: Any) -> str:
    try:
        return f"{float(value):.0%}"
    except (TypeError, ValueError):
        return "—"


def _fmt_pct_interval(low: Any, high: Any) -> str:
    try:
        return f"{float(low):.0%} ~ {float(high):.0%}"
    except (TypeError, ValueError):
        return "—"


def _fmt_return_interval(low: Any, high: Any) -> str:
    try:
        return f"{float(low):+.1%} ~ {float(high):+.1%}"
    except (TypeError, ValueError):
        return "—"


def _append_sentence(base: str, extra: str) -> str:
    head = str(base).strip()
    tail = str(extra).strip()
    if not tail:
        return head
    if not head:
        return tail
    if head.endswith(("。", "！", "？")):
        return f"{head}{tail}"
    return f"{head} {tail}"


def _report_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    stamp = pd.Timestamp(parsed)
    if stamp.tzinfo is not None:
        try:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        except TypeError:
            stamp = stamp.tz_localize(None)
    return stamp


def _contextualize_market_proxy_interpretation(interpretation: str, regime: Mapping[str, Any]) -> str:
    text = str(interpretation).strip()
    if not text:
        return text
    regime_name = str(dict(regime or {}).get("current_regime", "")).strip()
    lowered = text.lower()
    if "risk-on" in lowered and regime_name in {"stagflation", "deflation"}:
        return _append_sentence(text, f"这更像短线风险偏好修复，不等于中期 `{regime_name}` 背景已经切回成长主导。")
    if ("偏防守" in text or "抗跌" in text) and regime_name == "recovery":
        return _append_sentence(text, "这更像短线避险回摆，不等于中期 `recovery` 框架失效。")
    return text


def _regime_basis_section(
    regime: Mapping[str, Any],
    *,
    day_theme: str = "",
    heading: str = "## 宏观判断依据",
    emphasize: bool = True,
) -> List[str]:
    regime_payload = dict(regime or {})
    regime_name = str(regime_payload.get("current_regime", "")).strip()
    basis_lines = [str(item).strip() for item in regime_payload.get("basis_lines", []) if str(item).strip()]
    reasoning = [str(item).strip() for item in regime_payload.get("reasoning", []) if str(item).strip()]
    macro_lines = list(dict.fromkeys([*basis_lines, *reasoning]))
    if not regime_name or not macro_lines:
        return []
    lines = [heading, ""]
    lines.extend(_section_lead_lines("这段只回答为什么把今天的中期背景判断成这个 regime，不把切换写成无依据结论。", emphasize=emphasize))
    for item in macro_lines[:4]:
        lines.append(f"- {item}")
    if day_theme:
        lines.append(f"- 当天主线写成 `{day_theme}`，是这层中期背景里的短线表达，不等于 macro regime 重新切档。")
    return lines


def _section_lead_lines(text: str, *, emphasize: bool = True) -> List[str]:
    line = str(text).strip()
    if not line:
        return []
    if emphasize:
        return [f"**先看结论：** {line}", ""]
    return [line, ""]


def render_what_changed_section(payload: Mapping[str, Any], *, heading: str = "## What Changed") -> List[str]:
    summary = dict(payload or {})
    if not summary:
        return []
    previous_view = _pick_client_safe_line(summary.get("previous_view") or "")
    change_summary = _pick_client_safe_line(summary.get("change_summary") or "")
    conclusion_label = str(summary.get("conclusion_label") or "").strip()
    state_trigger = str(summary.get("state_trigger") or "").strip()
    state_summary = _pick_client_safe_line(summary.get("state_summary") or "")
    current_view = _pick_client_safe_line(summary.get("current_view") or "")
    current_event_understanding = _pick_client_safe_line(summary.get("current_event_understanding") or "")
    lines = [heading, ""]
    if previous_view:
        lines.append(f"- 上次怎么看：{previous_view}")
    if change_summary:
        lines.append(f"- 这次什么变了：{change_summary}")
    if current_event_understanding:
        lines.append(f"- 当前事件理解：{current_event_understanding}")
    if conclusion_label:
        line = f"- 结论变化：`{conclusion_label}`"
        if current_view:
            line += f"；当前更像 `{current_view}`"
        if state_trigger:
            line += f"；触发：{state_trigger}"
        lines.append(line + "。")
    if state_summary:
        lines.append(f"- 状态解释：{state_summary}")
    return lines if len(lines) > 2 else []


def _client_safe_comparison_basis_label(label: Any) -> str:
    text = str(label or "").strip()
    if text == "当日基准版":
        return "今天首个快照版"
    return text or "对比基准"


def _compact_event_digest_label(lead_detail: str, lead_layer: str) -> str:
    detail = _pick_client_safe_line(lead_detail or "").strip()
    if detail.startswith("主题事件："):
        detail = detail.split("：", 1)[1].strip()
    if "；" in detail:
        detail = detail.split("；", 1)[0].strip()
    return detail or _pick_client_safe_line(lead_layer or "").strip()


def render_compact_event_digest_section(payload: Mapping[str, Any], *, heading: str = "## 事件消化") -> List[str]:
    digest = dict(payload or {})
    if not digest:
        return []
    status = _pick_client_safe_line(digest.get("status") or "")
    lead_layer = _pick_client_safe_line(digest.get("lead_layer") or "")
    lead_detail = _pick_client_safe_line(digest.get("lead_detail") or "")
    lead_label = _compact_event_digest_label(
        str(digest.get("lead_detail") or ""),
        str(digest.get("lead_layer") or ""),
    )
    signal_conclusion = _pick_client_safe_line(digest.get("signal_conclusion") or "")
    impact_summary = _pick_client_safe_line(digest.get("impact_summary") or "")
    thesis_scope = _pick_client_safe_line(digest.get("thesis_scope") or "")
    importance_reason = _pick_client_safe_line(digest.get("importance_reason") or "")
    changed_what = _pick_client_safe_line(digest.get("changed_what") or "")
    lead_title = _pick_client_safe_line(digest.get("lead_title") or "")
    lead_link = _pick_client_safe_line(digest.get("lead_link") or "")
    next_step = _pick_client_safe_line(digest.get("next_step") or "")
    lead_tags = [
        _pick_client_safe_line(item)
        for item in list(digest.get("intelligence_attributes") or [])
        if _pick_client_safe_line(item)
    ]
    if not lead_tags:
        lead_tags = intelligence_attribute_labels(
            {
                "layer": str(digest.get("lead_layer") or ""),
                "lead_detail": str(digest.get("lead_detail") or ""),
            },
            as_of=_pick_client_safe_line(digest.get("latest_signal_at") or digest.get("as_of") or ""),
            first_tracking=not _pick_client_safe_line(digest.get("previous_reviewed_at") or ""),
            previous_reviewed_at=_pick_client_safe_line(digest.get("previous_reviewed_at") or ""),
        )
    lines = [heading, ""]
    overview_parts: List[str] = []
    if status:
        overview_parts.append(f"事件状态：`{status}`")
    if lead_layer:
        overview_parts.append(f"事件分层：`{lead_layer}`")
    if overview_parts:
        lines.append(f"- {'；'.join(overview_parts)}。")
    if lead_detail:
        lines.append(f"- 事件细分：`{lead_detail}`。")
    impact_parts: List[str] = []
    if lead_label and signal_conclusion:
        impact_parts.append(f"`{lead_label}` 当前结论：{signal_conclusion.rstrip('。；;，, ')}")
    elif signal_conclusion:
        impact_parts.append(signal_conclusion.rstrip("。；;，, "))
    if impact_summary:
        impact_parts.append(f"主要影响 `{impact_summary}`")
    if thesis_scope:
        impact_parts.append(f"性质偏 `{thesis_scope}`")
    if impact_parts:
        lines.append(f"- 影响层与性质：{'；'.join(impact_parts)}。")
    if importance_reason:
        lines.append(f"- 优先级判断：{importance_reason.rstrip('。；;，, ')}。")
    if changed_what:
        lines.append(f"- 这件事改变了什么：{changed_what.rstrip('。；;，, ')}。")
    if lead_title:
        lead_text = f"[{lead_title}]({lead_link})" if lead_link else lead_title
        lines.append(f"- 当前前置事件：{lead_text}")
    if lead_tags:
        lines.append(f"- 情报属性：`{format_intelligence_attributes(lead_tags)}`。")
    if next_step:
        lines.append(f"- 现在更该做什么：{next_step}")
    return lines if len(lines) > 2 else []


def render_compact_what_changed_section(payload: Mapping[str, Any], *, heading: str = "## What Changed") -> List[str]:
    summary = dict(payload or {})
    if not summary:
        return []
    previous_view = _pick_client_safe_line(summary.get("previous_view") or "")
    change_summary = _pick_client_safe_line(summary.get("change_summary") or "")
    conclusion_label = _pick_client_safe_line(summary.get("conclusion_label") or "")
    state_trigger = _pick_client_safe_line(summary.get("state_trigger") or "")
    state_summary = _pick_client_safe_line(summary.get("state_summary") or "")
    current_view = _pick_client_safe_line(summary.get("current_view") or "")
    current_event_understanding = _pick_client_safe_line(summary.get("current_event_understanding") or "")
    lines = [heading, ""]
    if previous_view:
        lines.append(f"- 上次怎么看：{previous_view.rstrip('。；;，, ')}。")
    if change_summary:
        lines.append(f"- 这次什么变了：{change_summary.rstrip('。；;，, ')}。")
    if current_event_understanding:
        lines.append(f"- 当前事件理解：{current_event_understanding.rstrip('。；;，, ')}。")
    current_parts: List[str] = []
    if conclusion_label:
        current_parts.append(f"结论变化：`{conclusion_label}`")
    if current_view:
        current_parts.append(f"当前结论 `{current_view}`")
    if state_trigger:
        current_parts.append(f"触发：{state_trigger.rstrip('。；;，, ')}")
    if current_parts:
        lines.append(f"- {'；'.join(current_parts)}。")
    if state_summary:
        lines.append(f"- 状态解释：{state_summary.rstrip('。；;，, ')}。")
    return lines if len(lines) > 2 else []


def _is_observe_style_text(text: Any) -> bool:
    line = _pick_client_safe_line(text)
    if not line:
        return False
    return any(marker in line for marker in ("观察", "回避", "暂不出手", "等待", "等右侧", "待确认", "等确认", "不追高", "先按观察仓"))


def _holding_name_text(name: Any, code: Any) -> str:
    text = str(name).strip()
    if text.lower() in {"", "nan", "none"}:
        fallback = str(code).strip()
        return fallback or "—"
    return text


def _reason_fingerprint(text: str) -> str:
    line = str(text or "").strip()
    if not line:
        return ""
    line = re.sub(r"^[^：:]{1,18}[：:]\s*", "", line)
    line = re.sub(r"`([^`]+)`", r"\1", line)
    line = re.sub(r"[（(][^）)]{1,20}[）)]", "", line)
    line = re.sub(r"\s+", "", line)
    return line


def _analysis_constraint_hint(analysis: Mapping[str, Any]) -> str:
    dimensions = dict(analysis.get("dimensions") or {})
    ordered = [
        ("relative_strength", "相对强弱"),
        ("technical", "技术面"),
        ("catalyst", "催化面"),
        ("risk", "风险特征"),
        ("seasonality", "季节/日历"),
    ]
    candidates: List[Tuple[int, str, str]] = []
    for key, label in ordered:
        dimension = dict(dimensions.get(key) or {})
        score = dimension.get("score")
        if score is None:
            continue
        reason = _dimension_reason_text(dimension, positive=False)
        reason = str(reason or "").strip()
        if not reason or reason in GENERIC_DIMENSION_SUMMARIES:
            continue
        candidates.append((int(score), label, reason.replace("；", "，")))
    if not candidates:
        return ""
    candidates.sort(key=lambda item: item[0])
    _, label, reason = candidates[0]
    if len(reason) > 32:
        reason = reason[:32].rstrip("，；。 ") + "..."
    return f"先看{label}里的“{reason}”能不能先改善。"


def _asset_uses_index_horizon(payload: Mapping[str, Any]) -> bool:
    return uses_index_mainline(payload)


def _index_horizon_summary_lines(
    payload: Mapping[str, Any],
    *,
    bundle_key: str = "index_topic_bundle",
    market_lines_key: str = "domestic_market_lines",
) -> List[str]:
    asset_type = str(payload.get("asset_type", "")).strip()
    if asset_type and not _asset_uses_index_horizon(payload):
        return []
    bundle = dict(payload.get(bundle_key) or {})
    history_snapshots = dict(bundle.get("history_snapshots") or {})
    snapshot = dict(bundle.get("index_snapshot") or {})
    technical = dict(bundle.get("technical_snapshot") or {})
    index_name = (
        str(snapshot.get("index_name", "")).strip()
        or str(payload.get("name", "")).strip()
        or str(payload.get("benchmark_name", "")).strip()
        or str(payload.get("benchmark", "")).strip()
        or "相关指数"
    )
    source_label = str(snapshot.get("display_label") or technical.get("source_label") or "指数主链").strip()

    def _trend_summary(period: str, row: Mapping[str, Any]) -> str:
        summary = str(row.get("summary", "")).strip()
        trend_label = str(row.get("trend_label", "")).strip()
        momentum_label = str(row.get("momentum_label", "")).strip()
        latest_date = str(row.get("latest_date", "")).strip()
        parts: List[str] = []
        if summary:
            parts.append(summary)
        for extra in (trend_label, momentum_label):
            if extra and extra not in summary:
                parts.append(extra)
        if not parts:
            parts = ["缺失"]
        text = "，".join(parts)
        if latest_date:
            text += f"（{latest_date}）"
        return f"- {period}：{index_name} {text}。"

    weekly = dict(history_snapshots.get("weekly") or {})
    monthly = dict(history_snapshots.get("monthly") or {})
    horizon_lines: List[str] = []
    if weekly.get("status") == "matched" or monthly.get("status") == "matched":
        horizon_lines.extend(["## 周月节奏", ""])
        if weekly.get("status") == "matched":
            horizon_lines.append(_trend_summary("周线", weekly))
        if monthly.get("status") == "matched":
            horizon_lines.append(_trend_summary("月线", monthly))
        weekly_trend = str(weekly.get("trend_label", "")).strip()
        monthly_trend = str(monthly.get("trend_label", "")).strip()
        if weekly_trend and monthly_trend:
            if weekly_trend in {"趋势偏强", "修复中"} and monthly_trend in {"趋势偏强", "修复中"}:
                horizon_lines.append(f"- 结论：`{index_name}` 的周月节奏同向偏强，今天更适合按延续看，不把单日波动误判成反转。")
            elif weekly_trend == monthly_trend:
                horizon_lines.append(f"- 结论：`{index_name}` 的周月节奏同向，但强弱还不算满配共振，今天更适合看确认。")
            else:
                horizon_lines.append(f"- 结论：`{index_name}` 的周月节奏暂时分歧，今天更像节奏切换，不宜把单日涨跌直接写成趋势翻转。")
        elif weekly_trend or monthly_trend:
            horizon_lines.append(f"- 结论：`{index_name}` 当前只拿到一层周期确认，另一层暂按缺失处理，不补写成共振。")
        if source_label:
            horizon_lines.append(f"- 来源：{source_label}。")
        return horizon_lines

    market_lines = [str(item).strip() for item in list(payload.get(market_lines_key) or []) if str(item).strip()]
    structured_lines = [line for line in market_lines if "周线" in line or "月线" in line]
    if not structured_lines:
        return []
    horizon_lines.extend(["## 周月节奏", ""])
    for item in structured_lines[:4]:
        horizon_lines.append(f"- {item.rstrip('。')}。")
    body = " ".join(structured_lines)
    if any(token in body for token in ("趋势偏强", "修复中")) and not any(token in body for token in ("趋势偏弱", "承压震荡")):
        horizon_lines.append("- 结论：周月节奏同向偏强，今天更适合按延续看，不把单日波动误判成反转。")
    elif any(token in body for token in ("趋势偏弱", "承压震荡")) and any(token in body for token in ("趋势偏强", "修复中")):
        horizon_lines.append("- 结论：周月节奏暂时分歧，今天更像节奏切换，不宜把单日涨跌直接写成趋势翻转。")
    elif len(structured_lines) >= 2:
        horizon_lines.append("- 结论：`周月节奏` 已进入正文判断，不再只停留在表格里。")
    else:
        horizon_lines.append("- `周月节奏` 已进入正文判断，不再只停留在表格里。")
    return horizon_lines


def _pick_visual_lines(visuals: Optional[Mapping[str, str]], *, nested: bool = False) -> List[str]:
    lines = list(_visual_lines(visuals))
    if not lines:
        return []
    if not nested:
        return lines

    rewritten: List[str] = []
    for line in lines:
        if line == "## 图表速览":
            rewritten.extend(["#### 图表速览", ""])
            continue
        if line.startswith("### "):
            rewritten.append("#### " + line[4:])
            continue
        rewritten.append(line)
    return rewritten


def _observe_pick_track_rows(track_rows: Sequence[Sequence[str]]) -> List[List[str]]:
    labels = ("优先观察", "次级观察", "补充观察")
    rewritten: List[List[str]] = []
    for index, row in enumerate(track_rows):
        current = list(row)
        if not current:
            continue
        current[0] = labels[index] if index < len(labels) else f"观察 {index + 1}"
        rewritten.append(current)
    return rewritten


def _track_rows_with_subject_first(
    track_rows: Sequence[Sequence[str]],
    subject: Mapping[str, Any],
) -> List[List[str]]:
    symbol = str(subject.get("symbol") or "").strip()
    name = str(subject.get("name") or symbol).strip()
    if not symbol:
        return [list(row) for row in track_rows]

    def _row_symbol(row: Sequence[str]) -> str:
        if len(row) < 2:
            return ""
        match = re.search(r"\(([^()]+)\)\s*$", str(row[1]).strip())
        return match.group(1).strip() if match else ""

    subject_row: List[str] | None = None
    remaining_rows: List[List[str]] = []
    for raw_row in track_rows:
        row = list(raw_row)
        if _row_symbol(row) == symbol:
            subject_row = row
            continue
        remaining_rows.append(row)
    if subject_row is None:
        horizon = _analysis_horizon_profile(subject)
        subject_row = [
            "优先推荐",
            f"{name} ({symbol})",
            horizon.get("label", "观察期"),
            _analysis_track_reason(subject),
        ]
    return [subject_row, *remaining_rows]


def _observe_pick_track_summary(track_rows: Sequence[Sequence[str]]) -> str:
    names: List[str] = []
    for row in track_rows[:3]:
        if len(row) < 2:
            continue
        label = str(row[0]).strip() or "观察"
        target = str(row[1]).strip()
        names.append(f"{label}：`{target.split(' (', 1)[0]}`")
    return "；".join(names)


def _signal_confidence_lines(analysis: Mapping[str, Any]) -> List[str]:
    confidence = dict(analysis.get("signal_confidence") or {})
    if not confidence:
        return [
            "**历史相似样本：** 当前还没有输出这层统计。",
            "",
            *_table(
                ["指标", "结果"],
                [
                    ["状态", "当前未输出历史相似样本统计"],
                    ["非重叠样本", "未输出"],
                    ["20日胜率区间", "95%区间 `—`"],
                    ["样本质量", "未输出"],
                    ["处理原则", "当前稿件未产出历史相似样本结论，先不要把它当成已有历史命中率验证的建议。"],
                ],
            ),
        ]
    if not confidence.get("available"):
        return [
            "**历史相似样本：** 当前不给这层置信度。",
            "",
            *_table(
                ["指标", "结果"],
                [
                    ["状态", "当前不给这层置信度"],
                    ["原因", str(confidence.get("reason", "样本或数据置信度不足。"))],
                    ["非重叠样本", "未输出"],
                    ["20日胜率区间", "95%区间 `—`"],
                    ["样本质量", "未输出（降级）"],
                    ["处理原则", "宁可不报，也不拿低置信历史样本给当前建议背书。"],
                ],
            ),
        ]

    def _weak_signal_confidence(payload: Mapping[str, Any]) -> bool:
        try:
            ci_low = float(payload.get("win_rate_20d_ci_low")) if payload.get("win_rate_20d_ci_low") is not None else None
        except (TypeError, ValueError):
            ci_low = None
        try:
            ci_high = float(payload.get("win_rate_20d_ci_high")) if payload.get("win_rate_20d_ci_high") is not None else None
        except (TypeError, ValueError):
            ci_high = None
        try:
            sample_quality = int(payload.get("sample_quality_score")) if payload.get("sample_quality_score") is not None else None
        except (TypeError, ValueError):
            sample_quality = None
        try:
            confidence_score = int(payload.get("confidence_score")) if payload.get("confidence_score") is not None else None
        except (TypeError, ValueError):
            confidence_score = None
        try:
            non_overlapping = int(payload.get("non_overlapping_count", payload.get("sample_count", 0)) or 0)
        except (TypeError, ValueError):
            non_overlapping = 0
        ci_crosses_mid = ci_low is not None and ci_high is not None and ci_low < 0.50 < ci_high
        weak_score = (sample_quality is not None and sample_quality < 60) or (confidence_score is not None and confidence_score < 60)
        return ci_crosses_mid or weak_score or non_overlapping < 15

    weak_confidence = _weak_signal_confidence(confidence)
    lines = [
        (
            f"**{'历史相似样本附注' if weak_confidence else '历史相似样本'}：** {confidence.get('summary', '')}"
            or f"同标的近似样本 `{confidence.get('sample_count', '—')}` 个。"
        ),
        "",
    ]
    rows = (
        [
            ["样本范围", str(confidence.get("scope", "同标的日线相似场景"))],
            ["非重叠样本", str(confidence.get("non_overlapping_count", confidence.get("sample_count", "—")))],
            ["20日胜率区间", f"95%区间 `{_fmt_pct_interval(confidence.get('win_rate_20d_ci_low'), confidence.get('win_rate_20d_ci_high'))}`"],
            [
                "20日中位收益区间",
                f"bootstrap 区间 `{_fmt_return_interval(confidence.get('median_return_20d_ci_low'), confidence.get('median_return_20d_ci_high'))}`",
            ],
            ["样本质量", f"{confidence.get('sample_quality_label', '—')} ({confidence.get('sample_quality_score', '—')}/100)"],
            ["样本置信度", f"{confidence.get('confidence_label', '—')} ({confidence.get('confidence_score', '—')}/100)"],
        ]
        if weak_confidence
        else [
            ["样本范围", str(confidence.get("scope", "同标的日线相似场景"))],
            ["候选池", str(confidence.get("candidate_pool", "—"))],
            ["非重叠样本", str(confidence.get("non_overlapping_count", confidence.get("sample_count", "—")))],
            ["样本覆盖", f"{confidence.get('coverage_months', '—')} 个月 / {confidence.get('coverage_span_days', '—')} 天"],
            ["20日胜率", _fmt_ratio(confidence.get("win_rate_20d"))],
            ["20日胜率区间", f"95%区间 `{_fmt_pct_interval(confidence.get('win_rate_20d_ci_low'), confidence.get('win_rate_20d_ci_high'))}`"],
            ["20日平均收益", _fmt_pct(confidence.get("avg_return_20d"))],
            ["20日中位收益", _fmt_pct(confidence.get("median_return_20d"))],
            [
                "20日中位收益区间",
                f"bootstrap 区间 `{_fmt_return_interval(confidence.get('median_return_20d_ci_low'), confidence.get('median_return_20d_ci_high'))}`",
            ],
            ["20日平均最大回撤", _fmt_pct(confidence.get("avg_mae_20d"))],
            ["止损触发率", _fmt_ratio(confidence.get("stop_hit_rate"))],
            ["目标触达率", _fmt_ratio(confidence.get("target_hit_rate"))],
            ["样本质量", f"{confidence.get('sample_quality_label', '—')} ({confidence.get('sample_quality_score', '—')}/100)"],
            ["样本置信度", f"{confidence.get('confidence_label', '—')} ({confidence.get('confidence_score', '—')}/100)"],
        ]
    )
    lines.extend(_table(["指标", "结果"], rows))
    sample_dates = list(confidence.get("sample_dates") or [])
    scope = str(confidence.get("scope", "同标的日线相似场景")).strip() or "同标的日线相似场景"
    non_overlapping = confidence.get("non_overlapping_count", confidence.get("sample_count", "—"))
    coverage_months = confidence.get("coverage_months", "—")
    quality_label = str(confidence.get("sample_quality_label", "—")).strip() or "—"
    quality_score = confidence.get("sample_quality_score", "—")
    confidence_label = str(confidence.get("confidence_label", "—")).strip() or "—"
    confidence_score = confidence.get("confidence_score", "—")
    lines.extend(
        [
            "",
            f"- 这层更像 `{scope}` 的历史统计：当前保留 `{non_overlapping}` 个非重叠样本，覆盖约 `{coverage_months}` 个月，不直接替代本次总推荐判断。",
            f"- 当前样本质量 `{quality_label}`（{quality_score}/100）、样本置信度 `{confidence_label}`（{confidence_score}/100）；严格口径会先去掉未来窗口重叠样本，避免把同一段走势重复算成多次命中。",
        ]
    )
    if weak_confidence:
        lines.append("- 当前区间仍明显跨过中性线或样本质量偏弱，这层统计只作边界附注，不单独抬高推荐等级。")
    quality_notes = [str(item).strip() for item in (confidence.get("quality_notes") or []) if str(item).strip()]
    for item in quality_notes[:3]:
        lines.append(f"- 样本质量提示：{item}")
    if sample_dates:
        lines.extend(
            [
                f"最近可比样本日期：`{' / '.join(sample_dates[:5])}`",
                f"注：{confidence.get('reason', '仅使用同标的当时可见的日线量价和技术状态，不重建历史新闻与财报快照。')}",
            ]
        )
    return lines


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> List[str]:
    def _escape(value: Any) -> str:
        text = str(value).strip()
        if value is None or text.lower() in {"nan", "nat", "none"}:
            text = "—"
        return text.replace("|", "\\|").replace("\n", "<br>")

    lines = [
        "| " + " | ".join(_escape(header) for header in headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_escape(cell) for cell in row) + " |")
    return lines


def _score(analysis: Mapping[str, Any], dimension: str) -> int:
    return _shared_score_dimension(analysis, dimension)


def _score_value(value: Any) -> Optional[float]:
    if value in (None, "", "—", "缺失", "信息项", "不适用"):
        return None
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        text = text.split("/", 1)[0].strip()
    digits = "".join(ch for ch in text if ch.isdigit() or ch in {".", "-"})
    if not digits:
        return None
    try:
        return float(digits)
    except ValueError:
        return None


_PICK_DIMENSION_LABELS: Dict[str, Tuple[str, ...]] = {
    "technical": ("技术面", "技术"),
    "catalyst": ("催化面", "催化"),
    "relative_strength": ("相对强弱",),
    "risk": ("风险特征", "风险"),
    "fundamental": ("产品质量/基本面代理", "基本面", "产品/基本面"),
}


def _pick_dimension_score(analysis: Mapping[str, Any], dimension: str) -> Optional[int]:
    """Return a visible pick score only when the report actually carries one."""

    dimensions = dict(analysis.get("dimensions") or {})
    raw_dimension = dimensions.get(dimension)
    if isinstance(raw_dimension, Mapping):
        value = _score_value(raw_dimension.get("score"))
        if value is not None:
            return int(round(value))
    labels = _PICK_DIMENSION_LABELS.get(dimension, (dimension,))
    for row in list(analysis.get("dimension_rows") or []):
        row_values = list(row) if isinstance(row, (list, tuple)) else []
        if len(row_values) < 2:
            continue
        label = str(row_values[0]).strip()
        if any(label == candidate or candidate in label for candidate in labels):
            value = _score_value(row_values[1])
            if value is not None:
                return int(round(value))
    return None


def _pick_signal_state(analysis: Mapping[str, Any], *, has_existing_position: bool = False) -> Dict[str, Any]:
    technical = _pick_dimension_score(analysis, "technical")
    catalyst = _pick_dimension_score(analysis, "catalyst")
    relative = _pick_dimension_score(analysis, "relative_strength")
    risk = _pick_dimension_score(analysis, "risk")
    fundamental = _pick_dimension_score(analysis, "fundamental")
    scores = {
        "technical": technical,
        "catalyst": catalyst,
        "relative_strength": relative,
        "risk": risk,
        "fundamental": fundamental,
    }
    rating = dict(analysis.get("rating") or {})
    action = dict(analysis.get("action") or {})
    action_text = " ".join(
        str(action.get(key, "")).strip()
        for key in ("direction", "entry", "position", "scaling_plan")
        if str(action.get(key, "")).strip()
    )
    rating_label = str(rating.get("label", "")).strip()
    if (
        technical is not None
        and catalyst is not None
        and technical < 30
        and catalyst < 20
    ) or rating_label == "无信号":
        reason = f"技术{technical if technical is not None else '—'} / 催化{catalyst if catalyst is not None else '—'}"
        if risk is not None:
            reason += f" / 风险{risk}"
        if relative is not None and relative >= 80:
            reason += f"；相对强弱{relative}只作滞后备注，不抵消双低信号"
        return {"code": "NO_SIGNAL", "label": "今日无信号", "scores": scores, "reason": reason}
    if technical is not None and catalyst is not None and technical >= 60 and catalyst >= 60 and (risk is None or risk >= 50):
        reason = f"技术{technical} / 催化{catalyst}"
        if risk is not None:
            reason += f" / 风险{risk}"
        return {"code": "BUY_CANDIDATE", "label": "可执行候选", "scores": scores, "reason": reason}
    if has_existing_position and catalyst is not None and catalyst < 40:
        return {
            "code": "REDUCE",
            "label": "已有仓位降风险",
            "scores": scores,
            "reason": f"已有仓位但催化{catalyst}偏弱，优先降风险而不是加仓",
        }
    if any(token in action_text for token in ("减仓", "降风险", "卖出", "降低仓位")):
        return {"code": "REDUCE", "label": "降风险", "scores": scores, "reason": _pick_client_safe_line(action_text)}
    reason_parts = []
    if technical is not None:
        reason_parts.append(f"技术{technical}")
    if catalyst is not None:
        reason_parts.append(f"催化{catalyst}")
    if relative is not None:
        reason_parts.append(f"相对强弱{relative}")
    return {
        "code": "HOLD_WATCH",
        "label": "持仓/观察",
        "scores": scores,
        "reason": " / ".join(reason_parts) or "信号不完整，先保留观察",
    }


def _pick_scores_text(scores: Mapping[str, Any]) -> str:
    labels = (
        ("technical", "技术"),
        ("catalyst", "催化"),
        ("risk", "风险"),
        ("relative_strength", "相对强弱"),
    )
    parts = [f"{label}{scores.get(key)}" for key, label in labels if scores.get(key) is not None]
    return " / ".join(parts) if parts else "未完整评分"


def _pick_state_summary(state: Mapping[str, Any]) -> str:
    reason = str(state.get("reason", "")).strip()
    if reason:
        return _pick_client_safe_line(reason)
    return _pick_scores_text(dict(state.get("scores") or {}))


def _pick_no_signal_next_review(event_digest: Mapping[str, Any], state: Mapping[str, Any]) -> str:
    title = str(event_digest.get("lead_title") or "").strip()
    next_step = str(event_digest.get("next_step") or "").strip()
    if any(token in title for token in ("财报", "披露", "一季报", "年报")):
        return "正式披露后 24 小时内重评；披露前不把预约日历当成业绩兑现。"
    if next_step:
        return _pick_client_safe_line(next_step)
    scores = dict(state.get("scores") or {})
    technical = scores.get("technical")
    catalyst = scores.get("catalyst")
    if technical is not None and catalyst is not None:
        return f"等技术从 {technical} 修复到 30 以上，或催化从 {catalyst} 修复到 20 以上后再重评。"
    return "等技术面或催化面至少一项改善后再重评。"


def _pick_no_signal_candidate_items(payload: Mapping[str, Any], winner: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    candidates: List[Mapping[str, Any]] = []
    seen: set[str] = set()

    def _append(item: Mapping[str, Any]) -> None:
        symbol = str(dict(item).get("symbol", "")).strip()
        name = str(dict(item).get("name", "")).strip()
        key = symbol or name
        if not key or key in seen:
            return
        candidates.append(dict(item))
        seen.add(key)

    for item in [winner, *list(payload.get("catalyst_analyses") or [])]:
        if isinstance(item, Mapping):
            _append(item)
    for item in list(dict(payload.get("recommendation_tracks") or {}).values()):
        if isinstance(item, Mapping):
            _append(item)
    for item in list(payload.get("alternatives") or []):
        if isinstance(item, Mapping):
            _append(item)
    return candidates


def _pick_no_signal_ranking_rows(payload: Mapping[str, Any], winner: Mapping[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for index, item in enumerate(_pick_no_signal_candidate_items(payload, winner)[:8], start=1):
        state = _pick_signal_state(item)
        name = str(item.get("name") or item.get("symbol") or "—").strip()
        symbol = str(item.get("symbol") or "").strip()
        label = f"{name} ({symbol})" if symbol and symbol not in name else name
        rows.append(
            [
                str(index),
                label,
                str(state.get("code", "HOLD_WATCH")),
                _pick_scores_text(dict(state.get("scores") or {})),
                _pick_state_summary(state),
            ]
        )
    if not rows:
        state = _pick_signal_state(winner)
        rows.append(["1", str(winner.get("name") or winner.get("symbol") or "—"), str(state.get("code")), _pick_scores_text(dict(state.get("scores") or {})), _pick_state_summary(state)])
    return rows


def _pick_no_signal_event_tree_lines(winner: Mapping[str, Any], event_digest: Mapping[str, Any], asset_label: str) -> List[str]:
    title = str(event_digest.get("lead_title") or "").strip() or "当前前置事件"
    action = dict(winner.get("action") or {})
    upper = action.get("target_ref") or action.get("target")
    lower = action.get("stop_ref") or action.get("stop")
    upper_text = f"{float(upper):.3f}" if isinstance(upper, (int, float)) else str(upper or "关键压力位").strip()
    lower_text = f"{float(lower):.3f}" if isinstance(lower, (int, float)) else str(lower or "关键失效位").strip()
    if len(upper_text) > 24:
        upper_text = "关键压力位"
    if len(lower_text) > 24:
        lower_text = "关键失效位"
    return [
        f"- 事件：{_pick_client_safe_line(title)}",
        f"- 超预期：先看 `{upper_text}` 能否放量站稳；只有价格确认和技术/催化分数同步修复后，{asset_label} 才从 `NO_SIGNAL` 升到 `HOLD_WATCH / BUY_CANDIDATE` 复核。",
        f"- 符合预期：继续 `NO_SIGNAL`，让估值和拥挤度消化，不因为事件发生本身追价。",
        f"- 低于预期：若跌破 `{lower_text}` 或主线情绪继续退潮，已有仓位优先降风险；空仓继续不介入。",
    ]


def _pick_no_signal_event_digest_lines(event_digest: Mapping[str, Any]) -> List[str]:
    if not event_digest:
        return ["- 事件状态：暂无足够强的前置事件，本轮只保留信号闸门结论。"]
    status = str(event_digest.get("status") or "待复核").strip()
    lead_layer = str(event_digest.get("lead_layer") or "事件").strip()
    lead_detail = str(event_digest.get("lead_detail") or "").strip()
    impact_summary = str(event_digest.get("impact_summary") or "待确认").strip()
    thesis_scope = str(event_digest.get("thesis_scope") or "待确认").strip()
    changed = str(event_digest.get("changed_what") or "").strip()
    importance = str(event_digest.get("importance_reason") or "").strip()
    next_step = str(event_digest.get("next_step") or "").strip()
    lines = [
        f"- 事件状态：{status}",
        f"- 事件分层：{lead_layer}" + (f" / {lead_detail}" if lead_detail else ""),
        f"- 影响层与性质：{impact_summary}；{thesis_scope}",
    ]
    if changed:
        lines.append(f"- 这件事改变了什么：{_pick_client_safe_line(changed)}")
    if importance:
        lines.append(f"- 优先级判断：{_pick_client_safe_line(importance)}")
    if next_step:
        lines.append(f"- 下一步：{_pick_client_safe_line(next_step)}")
    return lines


def _pick_no_signal_what_changed_lines(what_changed: Mapping[str, Any]) -> List[str]:
    if not what_changed:
        return []
    lines: List[str] = []
    previous = str(what_changed.get("previous_view") or "").strip()
    change = str(what_changed.get("change_summary") or "").strip()
    conclusion = str(what_changed.get("conclusion_label") or "").strip()
    current_event = str(what_changed.get("current_event_understanding") or "").strip()
    trigger = str(what_changed.get("state_trigger") or "").strip()
    summary = str(what_changed.get("state_summary") or "").strip()
    if previous:
        lines.append(f"- 上次怎么看：{_pick_client_safe_line(previous)}")
    if change:
        lines.append(f"- 这次什么变了：{_pick_client_safe_line(change)}")
    if conclusion:
        lines.append(f"- 结论变化：{_pick_client_safe_line(conclusion)}")
    if current_event:
        lines.append(f"- 当前事件理解：{_pick_client_safe_line(current_event)}")
    if trigger:
        lines.append(f"- 触发：{_pick_client_safe_line(trigger)}")
    if summary:
        lines.append(f"- 状态解释：{_pick_client_safe_line(summary)}")
    return lines


def _render_pick_no_signal_report(
    payload: Mapping[str, Any],
    winner: Mapping[str, Any],
    *,
    generated_at: str,
    asset_label: str,
    selection_context: Mapping[str, Any],
    theme_packet: Mapping[str, Any],
) -> str:
    state = _pick_signal_state(winner)
    event_digest = dict(theme_packet.get("event_digest") or {})
    what_changed = dict(theme_packet.get("what_changed") or {})
    name = str(winner.get("name") or winner.get("symbol") or asset_label).strip()
    symbol = str(winner.get("symbol") or "").strip()
    subject = f"{name} ({symbol})" if symbol and symbol not in name else name
    next_review = _pick_no_signal_next_review(event_digest, state)
    scores = dict(state.get("scores") or {})
    technical = scores.get("technical", "—")
    catalyst = scores.get("catalyst", "—")
    risk = scores.get("risk", "—")
    relative = scores.get("relative_strength", "—")
    lines = [
        f"# 今日{asset_label}无信号 | {generated_at}",
        "",
        "## 结论",
        "",
        f"- 今日 `{subject}` 为 `NO_SIGNAL`：技术 `{technical}` / 催化 `{catalyst}` / 风险 `{risk}`；不新开仓。",
        f"- 相对强弱 `{relative}` 只作滞后备注，不能抵消技术和催化的双低信号。",
        f"- 下次评估：{next_review}",
        "",
        "## 为什么是 NO_SIGNAL",
        "",
        f"- 闸门规则：`技术面 < 30` 且 `催化面 < 20` 时，直接短路为 `NO_SIGNAL`，不再展开长篇八维叙事。",
        "- 当前动作：空仓不介入；已有仓位只按既有组合纪律处理，不能因为这份稿新增仓位。",
        "- 这份短稿的目标是减少噪音：没有可执行信号时，结论必须比解释更靠前。",
        "",
        "## 候选池排序",
        "",
        f"- 本轮初筛 `{selection_context.get('scan_pool', '—')}` 只，完整分析 `{selection_context.get('passed_pool', '—')}` 只；下表只展开动作状态，不把低信号标的包装成推荐。",
        "",
        *_table(["排名", "标的", "状态", "分数", "说明"], _pick_no_signal_ranking_rows(payload, winner)),
        "",
        "## 事件消化",
        "",
        *_pick_no_signal_event_digest_lines(event_digest),
        "",
        "## 事件决策树",
        "",
        *_pick_no_signal_event_tree_lines(winner, event_digest, asset_label),
    ]
    what_changed_lines = _pick_no_signal_what_changed_lines(what_changed)
    if what_changed_lines:
        lines.extend(["", "## What Changed", "", *what_changed_lines])
    coverage_lines = [str(item).strip() for item in list(selection_context.get("coverage_lines") or []) if str(item).strip()]
    lines.extend(
        [
            "",
            "## 触发后再评估",
            "",
            "- 从 `NO_SIGNAL` 升到 `HOLD_WATCH`：技术或催化至少一项修复到闸门线上方，再看价格是否确认。",
            "- 从 `HOLD_WATCH` 升到 `BUY_CANDIDATE`：技术和催化都要过 60，且风险分不能继续恶化。",
            "- 如果只有相对强弱高、但催化和技术没有修复，维持 `NO_SIGNAL / HOLD_WATCH`，不写成交易机会。",
            "",
            "## 数据边界",
            "",
            f"- 交付等级：`{selection_context.get('delivery_tier_label', '观察优先稿')}`。观察优先不是正式买入稿。",
        ]
    )
    if selection_context.get("coverage_note"):
        lines.append(f"- 覆盖说明：{selection_context.get('coverage_note')}")
    for item in coverage_lines[:2]:
        lines.append(f"- {item}")
    if selection_context.get("coverage_total"):
        lines.append(f"- 覆盖率的分母是今天进入完整分析的 `{selection_context.get('coverage_total')}` 只{asset_label}。")
    return "\n".join(lines).rstrip()


def _market_label(asset_type: str) -> str:
    return {
        "cn_stock": "A股",
        "hk": "港股",
        "us": "美股",
        "cn_etf": "ETF",
        "cn_fund": "场外基金",
    }.get(asset_type, "综合")


def _recommendation_bucket(
    analysis: Mapping[str, Any],
    watch_symbols: Optional[set[str]] = None,
) -> str:
    return _shared_recommendation_bucket(analysis, watch_symbols)


def _analysis_is_actionable(
    analysis: Mapping[str, Any],
    watch_symbols: Optional[set[str]] = None,
) -> bool:
    return _shared_analysis_is_actionable(analysis, watch_symbols)


def _analysis_horizon_profile(analysis: Mapping[str, Any]) -> Dict[str, str]:
    action = dict(analysis.get("action") or {})
    narrative = dict(analysis.get("narrative") or {})
    judgment = str(dict(narrative.get("judgment") or {}).get("state", ""))
    return _pick_horizon_profile(action, judgment)


def _horizon_track_bucket(horizon: Mapping[str, Any]) -> str:
    code = str(horizon.get("family_code") or horizon.get("code") or "").strip()
    label = str(horizon.get("label", "")).strip()
    if code in {"short_term", "swing"} or "短线" in label or "波段" in label:
        return "short"
    if code in {"position_trade", "long_term_allocation"} or "中线" in label or "长线" in label:
        return "medium"
    return ""


def _analysis_track_bucket(analysis: Mapping[str, Any]) -> str:
    return _horizon_track_bucket(_analysis_horizon_profile(analysis))


def _analysis_track_reason(analysis: Mapping[str, Any]) -> str:
    horizon = _analysis_horizon_profile(analysis)
    fit_reason = str(horizon.get("fit_reason", "")).strip()
    if fit_reason:
        return _pick_client_safe_line(fit_reason)
    positives = _top_dimension_reasons(analysis, top_n=1)
    if positives:
        return positives[0]
    return _bucket_summary_text("看好但暂不推荐", analysis)


def _market_recommendation_tracks(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
) -> Dict[str, Mapping[str, Any]]:
    candidates = [item for item in ranked_items if _analysis_is_actionable(item, watch_symbols)]

    tracks: Dict[str, Mapping[str, Any]] = {}
    used_symbols: set[str] = set()

    short_exact = [item for item in candidates if _analysis_track_bucket(item) == "short"]
    medium_exact = [item for item in candidates if _analysis_track_bucket(item) == "medium"]

    if short_exact:
        tracks["short"] = short_exact[0]
        used_symbols.add(str(short_exact[0].get("symbol", "")))
    if medium_exact:
        for item in medium_exact:
            symbol = str(item.get("symbol", ""))
            if symbol not in used_symbols:
                tracks["medium"] = item
                used_symbols.add(symbol)
                break

    for bucket_name in ("short", "medium", "third"):
        if bucket_name in tracks:
            continue
        for item in candidates:
            symbol = str(item.get("symbol", ""))
            if symbol in used_symbols:
                continue
            tracks[bucket_name] = item
            used_symbols.add(symbol)
            break
    return tracks


def _market_track_summary_text(tracks: Mapping[str, Mapping[str, Any]]) -> str:
    short_item = dict(tracks.get("short") or {})
    medium_item = dict(tracks.get("medium") or {})
    third_item = dict(tracks.get("third") or {})
    short_name = str(short_item.get("name", short_item.get("symbol", ""))).strip()
    medium_name = str(medium_item.get("name", medium_item.get("symbol", ""))).strip()
    third_name = str(third_item.get("name", third_item.get("symbol", ""))).strip()
    parts: List[str] = []
    if short_name:
        parts.append(f"优先：`{short_name}`")
    if medium_name:
        parts.append(f"次优：`{medium_name}`")
    if third_name:
        parts.append(f"第三：`{third_name}`")
    if parts:
        return "；".join(parts)
    return ""


def _market_watch_summary_text(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    allow_soft_short: bool = True,
) -> str:
    for item in ranked_items:
        name = str(item.get("name", item.get("symbol", ""))).strip()
        if not name:
            continue
        bucket = _recommendation_bucket(item, watch_symbols)
        if bucket == "看好但暂不推荐" and allow_soft_short:
            return f"短线先看：`{name}`；中线暂不单列"
        return f"没有正式动作票；优先观察：`{name}`"
    return ""


def _market_track_rows(tracks: Mapping[str, Mapping[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for bucket_name, label in (("short", "优先推荐"), ("medium", "次优推荐"), ("third", "第三推荐")):
        item = dict(tracks.get(bucket_name) or {})
        if not item:
            continue
        horizon = _analysis_horizon_profile(item)
        rows.append(
            [
                label,
                f"{item.get('name', '—')} ({item.get('symbol', '—')})",
                horizon.get("label", "观察期"),
                _analysis_track_reason(item),
            ]
        )
    return rows


def _market_watch_rows(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    limit: int = 2,
) -> List[List[str]]:
    rows: List[List[str]] = []
    seen: set[str] = set()
    labels = ("优先观察", "次级观察")
    for item in ranked_items:
        if _analysis_is_actionable(item, watch_symbols):
            continue
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        rows.append(
            [
                labels[len(rows)] if len(rows) < len(labels) else f"观察 {len(rows) + 1}",
                f"{item.get('name', '—')} ({symbol})",
                _analysis_horizon_profile(item).get("label", "观察期"),
                _analysis_track_reason(item),
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def _market_watch_trigger_rows(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    limit: int = 3,
) -> List[List[str]]:
    rows: List[List[str]] = []
    seen: set[str] = set()
    labels = ("优先观察", "次级观察", "补充观察")
    for item in ranked_items:
        if _analysis_is_actionable(item, watch_symbols):
            continue
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        gate_text = _pick_client_safe_line(_decision_gate_explanation(item))
        strategy_upgrade_text = _strategy_background_upgrade_text(item)
        if strategy_upgrade_text:
            gate_text = _append_sentence(gate_text, strategy_upgrade_text)
        rows.append(
            [
                labels[len(rows)] if len(rows) < len(labels) else f"观察 {len(rows) + 1}",
                f"{item.get('name', '—')} ({symbol})",
                _analysis_track_reason(item),
                gate_text,
                _pick_client_safe_line(_primary_upgrade_trigger(item)),
                _observe_watch_levels(item) or "先等关键位和动能一起改善。",
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def _market_followup_watch_rows(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    skip: int = 2,
    limit: int = 2,
) -> List[List[str]]:
    rows: List[List[str]] = []
    seen: set[str] = set()
    skipped = 0
    for item in ranked_items:
        if _analysis_is_actionable(item, watch_symbols):
            continue
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        if skipped < skip:
            skipped += 1
            continue
        cautions = _merge_reason_lines(
            _bottom_dimension_reasons(item, top_n=1),
            list(dict(item.get("narrative") or {}).get("cautions") or []),
            max_items=2,
        )
        why_wait = _pick_client_safe_line(cautions[0]) if cautions else _pick_client_safe_line(_decision_gate_explanation(item))
        if not why_wait:
            why_wait = "当前还缺更硬的右侧确认。"
        next_watch = _pick_client_safe_line(_primary_upgrade_trigger(item))
        if not next_watch:
            next_watch = _observe_trigger_condition(
                item,
                _analysis_horizon_profile(item),
                default_text="先看关键位、动能和量能能不能一起改善。",
                qualitative_only=True,
            )
        rows.append(
            [
                f"{item.get('name', '—')} ({symbol})",
                why_wait,
                next_watch,
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def _observe_representative_card_lines(
    analysis: Mapping[str, Any],
    *,
    watch_symbols: set[str],
    day_theme: str,
    seen_identities: set[tuple[str, str, str]] | None = None,
) -> List[str]:
    name = str(analysis.get("name", "—")).strip() or "—"
    symbol = str(analysis.get("symbol", "—")).strip() or "—"
    action = dict(analysis.get("action") or {})
    horizon = _analysis_horizon_profile(analysis)
    handoff = portfolio_whatif_handoff(
        symbol=symbol,
        horizon=horizon,
        direction=str(action.get("direction", "")),
        asset_type=str(analysis.get("asset_type", "")),
        reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
        generated_at=str(analysis.get("generated_at", "")),
    )
    positives = _merge_reason_lines(
        _top_dimension_reasons(analysis, top_n=1),
        list(dict(analysis.get("narrative") or {}).get("positives") or []),
        max_items=2,
    )
    why_watch = _pick_client_safe_line(positives[0]) if positives else _analysis_track_reason(analysis)
    gate_text = _pick_client_safe_line(_decision_gate_explanation(analysis)) or "当前仍缺更硬的右侧确认。"
    strategy_upgrade_text = _strategy_background_upgrade_text(analysis)
    if strategy_upgrade_text:
        gate_text = _append_sentence(gate_text, strategy_upgrade_text)
    trigger_text = _pick_client_safe_line(_primary_upgrade_trigger(analysis)) or "先看关键位、动能和量能能不能一起改善。"
    asset_type = str(analysis.get("asset_type", "")).strip()
    watch_levels = _observe_watch_levels(analysis, qualitative_only=asset_type != "cn_stock") or "先等关键位和动能一起改善。"
    theme_context = dict(analysis)
    if day_theme and not subject_theme_label(theme_context):
        theme_context["day_theme"] = {"label": day_theme}
    packet = build_scan_editor_packet(theme_context, bucket=_recommendation_bucket(analysis, watch_symbols))
    evidence_lines = _evidence_lines_with_event_digest(
        list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []),
        event_digest=packet.get("event_digest") or {},
        max_items=1,
        as_of=analysis.get("generated_at"),
        symbol=analysis.get("symbol"),
        seen_identities=seen_identities,
    )
    has_raw_evidence = bool(list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []))
    evidence_line = (
        evidence_lines[0]
        if evidence_lines
        else "- 同类前置证据已在本报告前文共享区展示，这里不再重复展开。"
        if has_raw_evidence
        else "- 当前更适合先按结构化披露和后文证据理解。"
    )
    score_line = _observe_card_score_line(analysis)
    hard_check_line = _pick_client_safe_line(_hard_check_inline(dict(analysis)))
    return [
        f"### {name} ({symbol})",
        "",
        f"- 为什么继续看：{why_watch}",
        f"- 现在主要卡点：{gate_text}",
        f"- 升级条件：{trigger_text}",
        f"- 关键盯盘价位：{watch_levels}",
        f"- 当前更合适的动作：{_pick_client_safe_line(action.get('entry', '先看确认，不急着给仓位。')) or '先看确认，不急着给仓位。'}",
        *( [f"- 八维速览：{score_line}"] if score_line else [] ),
        *( [f"- 硬检查：{hard_check_line}"] if hard_check_line else [] ),
        f"- 首次仓位：{_observe_stock_trial_position_text(action) if asset_type == 'cn_stock' else '观察稿阶段先按观察仓理解，确认回来前不预设精确比例。'}",
        f"- 组合预演：{handoff.get('summary', '先跑组合预演，再决定真实金额。')}",
        f"- 预演命令：`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`",
        "- 前置证据：",
        evidence_line,
    ]


def _cn_stock_lot_cost(analysis: Mapping[str, Any]) -> Optional[float]:
    if str(analysis.get("asset_type", "")) != "cn_stock":
        return None
    try:
        last_close = float(dict(analysis.get("metrics") or {}).get("last_close") or 0.0)
    except (TypeError, ValueError):
        return None
    if last_close <= 0:
        return None
    return last_close * 100


def _affordable_stock_rows(
    items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    max_lot_cost: float = 10000.0,
    limit: int = 2,
    actionable_only: bool = True,
) -> List[List[str]]:
    affordable = [
        item
        for item in ClientReportRenderer._rank_market_items(items, watch_symbols)
        if (not actionable_only or _analysis_is_actionable(item, watch_symbols))
        and (_cn_stock_lot_cost(item) or float("inf")) <= max_lot_cost
    ]
    rows: List[List[str]] = []
    seen: set[str] = set()
    for item in affordable:
        symbol = str(item.get("symbol", ""))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        cost = _cn_stock_lot_cost(item)
        rows.append(
            [
                f"{item.get('name', '—')} ({symbol})",
                "—" if cost is None else f"{int(round(cost))} 元/100股",
                _analysis_horizon_profile(item).get("label", "观察期"),
                _analysis_track_reason(item),
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def _analysis_theme_text(analysis: Mapping[str, Any]) -> str:
    parts = [str(analysis.get("name", "")), *subject_theme_terms(analysis)]
    return " ".join(part for part in parts if part).lower()


def _related_etf_rows(
    items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    limit: int = 2,
) -> List[List[str]]:
    ranked = ClientReportRenderer._rank_market_items(items, watch_symbols)
    matched: Dict[str, Dict[str, Any]] = {}
    for item in ranked:
        theme_text = _analysis_theme_text(item)
        if not theme_text:
            continue
        stock_name = str(item.get("name", item.get("symbol", ""))).strip()
        for keywords, etfs in RELATED_ETF_PROXY_MAP:
            if not any(keyword.lower() in theme_text for keyword in keywords):
                continue
            for etf_symbol, etf_name, reason in etfs:
                payload = matched.setdefault(
                    etf_symbol,
                    {
                        "name": etf_name,
                        "symbol": etf_symbol,
                        "reason": reason,
                        "sources": [],
                    },
                )
                if stock_name and stock_name not in payload["sources"]:
                    payload["sources"].append(stock_name)
            break

    rows: List[List[str]] = []
    for etf_symbol, payload in matched.items():
        sources = list(payload.get("sources") or [])
        rows.append(
            [
                f"{payload.get('name', '—')} ({etf_symbol})",
                " / ".join(sources[:2]) or "当前主线方向",
                str(payload.get("reason", "更适合先用主题篮子承接方向。")),
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def _stock_pick_proxy_coverage_grouped(
    top: Sequence[Mapping[str, Any]],
    coverage_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
) -> Dict[str, List[Mapping[str, Any]]]:
    selected_symbols = {
        str(item.get("symbol", "")).strip()
        for item in list(top or [])
        if str(item.get("symbol", "")).strip()
    }
    selected_symbols.update(symbol for symbol in watch_symbols if str(symbol).strip())

    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    seen: set[str] = set()
    for item in [*list(top or []), *list(coverage_items or [])]:
        symbol = str(dict(item or {}).get("symbol", "")).strip()
        if not symbol or symbol in seen or symbol not in selected_symbols:
            continue
        grouped[_market_label(str(dict(item or {}).get("asset_type", "")))].append(item)
        seen.add(symbol)
    return grouped


def _payload_track_summary_text(tracks: Mapping[str, Mapping[str, Any]]) -> str:
    short_item = dict(tracks.get("short_term") or {})
    medium_item = dict(tracks.get("medium_term") or {})
    third_item = dict(tracks.get("third_term") or {})
    short_name = str(short_item.get("name", "")).strip()
    medium_name = str(medium_item.get("name", "")).strip()
    third_name = str(third_item.get("name", "")).strip()
    parts: List[str] = []
    if short_name:
        parts.append(f"优先推荐：`{short_name}`")
    if medium_name:
        parts.append(f"次优推荐：`{medium_name}`")
    if third_name:
        parts.append(f"第三推荐：`{third_name}`")
    if parts:
        return "；".join(parts)
    return ""


def _payload_track_rows(tracks: Mapping[str, Mapping[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for key, label in (("short_term", "优先推荐"), ("medium_term", "次优推荐"), ("third_term", "第三推荐")):
        item = dict(tracks.get(key) or {})
        if not item:
            continue
        rows.append(
            [
                label,
                f"{item.get('name', '—')} ({item.get('symbol', '—')})",
                str(item.get("horizon_label", "观察期")),
                _pick_client_safe_line(item.get("reason", "当前先按观察跟踪理解。")),
            ]
        )
    return rows


def _track_row_summary(rows: Sequence[Sequence[str]]) -> List[List[str]]:
    summary_rows: List[List[str]] = []
    for row in rows[:3]:
        if len(row) < 4:
            continue
        summary_rows.append([str(row[0]), f"{row[1]}；{row[3]}"])
    return summary_rows


def _present_action_row(label: str, value: Any) -> List[List[str]]:
    text = _pick_client_safe_line(value)
    if not str(text).strip():
        return []
    return [[label, text]]


def _execution_range_text(value: Any) -> str:
    text = _pick_client_safe_line(value)
    if not text or "暂不设" in text:
        return ""
    return text


def _execution_range_bounds(value: Any) -> Tuple[str, str]:
    text = _execution_range_text(value)
    if not text:
        return ("", "")
    match = re.search(
        r"([0-9]+(?:\.[0-9]+)?)\s*(?:-|–|—|~|～|至|到)\s*([0-9]+(?:\.[0-9]+)?)",
        text,
    )
    if not match:
        return ("", "")
    low = match.group(1)
    high = match.group(2)
    try:
        if float(low) > float(high):
            low, high = high, low
    except ValueError:
        return ("", "")
    return (low, high)


def _execution_range_has_safe_stop_gap(buy_range: str, stop_text: str, *, min_gap: float = 0.01) -> bool:
    range_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*-\s*([0-9]+(?:\.[0-9]+)?)", str(buy_range))
    stop_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", str(stop_text))
    if not range_match or not stop_match:
        return True
    buy_low = float(range_match.group(1))
    stop_ref = float(stop_match.group(1))
    if buy_low <= 0 or stop_ref <= 0 or stop_ref >= buy_low:
        return False
    return (buy_low - stop_ref) / buy_low >= min_gap


def _safe_buy_range_text(action: Mapping[str, Any]) -> str:
    buy_range = _execution_range_text(action.get("buy_range", ""))
    stop_text = _pick_client_safe_line(action.get("stop", ""))
    if not buy_range:
        return ""
    if stop_text and not _execution_range_has_safe_stop_gap(buy_range, stop_text):
        return ""
    return buy_range


def _trim_watch_text(action: Mapping[str, Any]) -> str:
    trim_range = _execution_range_text(action.get("trim_range", ""))
    if not trim_range:
        return ""
    first_trim, second_trim = _execution_range_bounds(trim_range)
    if first_trim and second_trim:
        return (
            f"反弹先看 `{first_trim}` 附近第一次承压；"
            f"若放量站上，再看 `{second_trim}` 一带是否做第二次减仓。"
        )
    return f"先看 `{trim_range}` 一带的承压，别把反弹空间直接想满。"


def _trim_execution_rows(action: Mapping[str, Any]) -> List[List[str]]:
    trim_range = _execution_range_text(action.get("trim_range", ""))
    if not trim_range:
        return []
    first_trim, second_trim = _execution_range_bounds(trim_range)
    if not (first_trim and second_trim):
        return [["减仓参考", trim_range]]
    target_text = _pick_client_safe_line(action.get("target", ""))
    upgrade_text = (
        f"只有放量站上 `{second_trim}` 且催化、相对强弱和量能继续增强，"
        "才考虑继续上修。"
    )
    if target_text:
        upgrade_text = (
            f"只有放量站上 `{second_trim}` 且催化、相对强弱和量能继续增强，"
            f"才考虑把目标继续上修到 `{target_text}`。"
        )
    return [
        ["第一减仓位", f"`{first_trim}` 附近先兑现第一段反弹，不把第一波空间一次性坐回去。"],
        ["第二减仓位", f"若放量站上 `{first_trim}`，再看 `{second_trim}` 一带是否做第二次减仓。"],
        ["上修条件", upgrade_text],
    ]


def _observe_card_score_line(analysis: Mapping[str, Any]) -> str:
    labels = (
        ("technical", "技术"),
        ("fundamental", "基本面"),
        ("catalyst", "催化"),
        ("relative_strength", "相对强弱"),
        ("chips", "筹码"),
        ("risk", "风险"),
    )
    parts: List[str] = []
    for key, label in labels:
        score = _score(analysis, key)
        if score < 0:
            continue
        parts.append(f"{label} `{score}`")
    return " / ".join(parts)


def _top_observe_card_items(
    items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
    *,
    limit: int = 3,
) -> List[Mapping[str, Any]]:
    picked: List[Mapping[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if _analysis_is_actionable(item, watch_symbols):
            continue
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        picked.append(item)
        if len(picked) >= limit:
            break
    return picked


def _bucket_priority(bucket: str) -> int:
    return _shared_bucket_priority(bucket)


def _top_dimension_reasons(analysis: Mapping[str, Any], top_n: int = 2) -> List[str]:
    dimension_labels = {
        "technical": "技术面",
        "fundamental": "基本面",
        "catalyst": "催化面",
        "relative_strength": "相对强弱",
        "risk": "风险特征",
        "macro": "宏观敏感度",
    }
    rows: List[Tuple[int, str, str]] = []
    for key, label in dimension_labels.items():
        dimension = analysis.get("dimensions", {}).get(key, {})
        reason = _dimension_reason_text(dimension, positive=True)
        rows.append(
            (
                _score(analysis, key),
                label,
                reason,
            )
        )
    rows.sort(key=lambda item: item[0], reverse=True)
    return [f"{label} {score}：{reason}" for score, label, reason in rows[:top_n] if reason]


def _bottom_dimension_reasons(analysis: Mapping[str, Any], top_n: int = 2) -> List[str]:
    dimension_labels = {
        "technical": "技术面",
        "fundamental": "基本面",
        "catalyst": "催化面",
        "relative_strength": "相对强弱",
        "risk": "风险特征",
    }
    rows: List[Tuple[int, str, str]] = []
    for key, label in dimension_labels.items():
        dimension = analysis.get("dimensions", {}).get(key, {})
        reason = _dimension_reason_text(dimension, positive=False)
        rows.append(
            (
                _score(analysis, key),
                label,
                reason,
            )
        )
    rows.sort(key=lambda item: item[0])
    return [f"{label} {score}：{reason}" for score, label, reason in rows[:top_n] if reason]


def _position_management_lines(action: Mapping[str, Any]) -> List[str]:
    lines = [
        f"首次仓位按 `{action.get('position', '小仓位分批')}` 执行，不一次打满。",
    ]
    if action.get("max_portfolio_exposure"):
        lines.append(f"单票上限按 `{action['max_portfolio_exposure']}` 控制。")
    if action.get("scaling_plan"):
        lines.append(f"加仓节奏按 `{action['scaling_plan']}` 执行。")
    if action.get("stop"):
        lines.append(f"止损参考是 `{action['stop']}`，不是口头上的“看情况”。")
    return lines


def _evidence_lines(
    items: Sequence[Mapping[str, Any]],
    *,
    max_items: int = 3,
    as_of: Any = None,
    previous_reviewed_at: Any = None,
    symbol: Any = "",
) -> List[str]:
    lines: List[str] = []
    as_of_stamp = _report_timestamp(as_of)
    ranked_items = sort_event_items(items, as_of=as_of, previous_reviewed_at=previous_reviewed_at)

    def _compact_signal_label(value: Any) -> str:
        text = _pick_client_safe_line(str(value or "").strip())
        if not text:
            return ""
        if not text.startswith("主题事件："):
            return text
        if "；" in text:
            text = text.split("；", 1)[0]
        for prefix in ("主题事件：", "财报摘要：", "公告类型：", "政策影响层："):
            if text.startswith(prefix):
                text = text[len(prefix) :]
                break
        return text.strip("：: ").strip()

    def _compact_title_text(value: Any) -> str:
        text = _pick_client_safe_line(str(value or "").strip())
        if not text:
            return ""
        for prefix in ("指数成分权重：", "指数技术面：", "跟踪指数框架：", "行业暴露画像：", "跟踪成分画像："):
            if text.startswith(prefix):
                text = text[len(prefix) :]
                break
        parts = [part.strip() for part in text.split("；") if part.strip()]
        if len(parts) <= 2:
            return "；".join(parts) if parts else text
        return "；".join(parts[:2]) + "。"

    def _signal_strength_label(value: Any) -> str:
        text = str(value or "").strip().lower()
        return {
            "high": "强",
            "medium": "中",
            "low": "弱",
        }.get(text, str(value or "").strip() or "中")

    fallback_symbol = str(symbol or "").strip()
    for ranked in ranked_items:
        raw = dict(ranked.get("_raw") or {})
        candidate_symbol = str(raw.get("symbol") or raw.get("secCode") or "").strip()
        if candidate_symbol:
            fallback_symbol = candidate_symbol
            break
    for ranked in ranked_items[:max_items]:
        item = dict(ranked.get("_raw") or {})
        if _is_diagnostic_intelligence_row(item):
            continue
        title = _pick_client_safe_line(
            str(item.get("title", "")).strip() or str(ranked.get("title", "")).strip()
        )
        if not title:
            continue
        title = _compact_title_text(title) or title
        layer = _pick_client_safe_line(
            str(item.get("layer", "")).strip() or str(ranked.get("layer", "")).strip() or "证据"
        )
        source = str(item.get("source", "")).strip()
        date = str(item.get("date", "")).strip()
        link = effective_intelligence_link(item, symbol=fallback_symbol)
        title_text = f"[{title}]({link})" if link else title
        suffix_parts = [part for part in (source, date) if part]
        event_stamp = _report_timestamp(date)
        peer_etf_flow = (
            "ETF" in title
            and any(token in title for token in ("净申购", "净赎回", "份额净创设", "份额净赎回"))
            and (not fallback_symbol or fallback_symbol not in title)
        )
        if (
            as_of_stamp is not None
            and event_stamp is not None
            and event_stamp <= as_of_stamp
            and str(layer).strip() in {"政策催化", "海外映射", "龙头公告/业绩", "新闻热度", "证据"}
        ):
            age_days = (as_of_stamp.normalize() - event_stamp.normalize()).days
            if age_days >= 8:
                suffix_parts.append(f"已过去 {age_days} 天，时效在衰减")
        suffix = f"（{' / '.join(suffix_parts)}）" if suffix_parts else ""
        line = f"- `{layer}`：{title_text}{suffix}"
        if peer_etf_flow:
            line += "；定位：`赛道热度佐证 / 同赛道产品`"
        raw_lead_detail = (
            str(ranked.get("lead_detail", "")).strip()
            or str(item.get("lead_detail", "")).strip()
        )
        signal_type = _pick_client_safe_line(
            str(ranked.get("signal_type", "")).strip() or raw_lead_detail
        ) or layer
        signal_type = _compact_signal_label(signal_type) or signal_type
        intelligence_tags = intelligence_attribute_labels(
            {**item, "source_directness_rank": ranked.get("source_directness_rank"), "freshness_rank": ranked.get("freshness_rank")},
            as_of=as_of_stamp,
            previous_reviewed_at=previous_reviewed_at,
        )
        stale_baseline = "旧闻回放" in intelligence_tags and "新鲜情报" not in intelligence_tags
        signal_strength = "中" if stale_baseline else _signal_strength_label(ranked.get("signal_strength") or ranked.get("importance"))
        line += f"；信号类型：`{signal_type}`；信号强弱：`{signal_strength}`"
        signal_conclusion = _pick_client_safe_line(str(ranked.get("signal_conclusion", "")).strip())
        if stale_baseline:
            signal_conclusion = "中性，当前更多是历史基线，不把它直接当成新增催化。"
        if signal_conclusion:
            line += f"；结论：{signal_conclusion}"
        if intelligence_tags:
            line += f"；情报属性：`{format_intelligence_attributes(intelligence_tags)}`"
            source_lane = intelligence_source_lane(intelligence_tags)
            if source_lane:
                line += f"；来源层级：`{source_lane}`"
        review_context = review_history_context_text({"previous_reviewed_at": previous_reviewed_at})
        if review_context and not lines:
            line += f"；复查语境：{review_context}"
        display_thesis_scope = _pick_client_safe_line(str(ranked.get("thesis_scope", "")).strip())
        if stale_baseline:
            display_thesis_scope = "历史基线"
        importance_reason = _pick_client_safe_line(compact_importance_reason(ranked.get("importance_reason")))
        if stale_baseline:
            importance_reason = "先把它当历史基线看，不把旧闻直接升级成新增催化"
        understanding = "；".join(
            part
            for part in (
                (f"先把它理解成 `{_compact_signal_label(raw_lead_detail)}`" if _compact_signal_label(raw_lead_detail) else ""),
                (
                    f"更直接影响 `{_pick_client_safe_line(str(ranked.get('impact_summary', '')).strip())}`"
                    if _pick_client_safe_line(str(ranked.get("impact_summary", "")).strip())
                    else ""
                ),
                (
                    f"当前更像 `{display_thesis_scope}`"
                    if display_thesis_scope
                    else ""
                ),
                (
                    "先按辅助线索看，不单独升级动作"
                    if str(raw_lead_detail).startswith("主题事件：")
                    and display_thesis_scope == "一次性噪音"
                    else importance_reason
                ),
            )
            if part
        )
        if understanding:
            line += f"；事件理解：{understanding}"
        lines.append(line)
    return lines


def _evidence_identity(item: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("title", "") or "").strip().lower(),
        str(item.get("source", "") or "").strip().lower(),
        str(item.get("date", "") or item.get("published_at", "") or item.get("as_of", "") or "").strip().lower(),
    )


def _subject_evidence_terms(subject: Mapping[str, Any], event_digest: Mapping[str, Any]) -> List[str]:
    payload = dict(subject or {})
    metadata = dict(payload.get("metadata") or {})
    taxonomy = dict(metadata.get("taxonomy") or metadata.get("fund_taxonomy") or {})
    theme_profile = dict(metadata.get("theme_profile") or taxonomy.get("theme_profile") or {})
    digest = dict(event_digest or {})
    values: List[Any] = [
        metadata.get("sector"),
        metadata.get("industry"),
        metadata.get("industry_framework_label"),
        metadata.get("tracked_index_name"),
        metadata.get("benchmark"),
        metadata.get("benchmark_name"),
        metadata.get("index_framework_label"),
        metadata.get("chain_nodes"),
        metadata.get("primary_chain"),
        metadata.get("theme_family"),
        metadata.get("theme_role"),
        taxonomy.get("primary_chain"),
        taxonomy.get("theme_family"),
        taxonomy.get("theme_role"),
        theme_profile.get("primary_chain"),
        theme_profile.get("theme_family"),
        theme_profile.get("theme_role"),
        theme_profile.get("evidence_keywords"),
        theme_profile.get("preferred_sector_aliases"),
        theme_profile.get("mainline_tags"),
        payload.get("taxonomy_summary"),
        dict(payload.get("theme_playbook") or {}).get("label"),
        digest.get("theme_label"),
        digest.get("lead_title"),
    ]
    values.extend(subject_theme_terms(payload, allow_day_theme=False))
    terms: List[str] = []
    seen: set[str] = set()
    generic = {"基金", "ETF", "etf", "指数", "主题", "市场", "南方", "平安", "长城", "华夏", "招商"}
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            values.extend(value.values())
            continue
        if isinstance(value, (list, tuple, set)):
            values.extend(list(value))
            continue
        text = _pick_client_safe_line(str(value or "").strip()).strip("`")
        if len(text) < 2 or text in generic:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        terms.append(text)
    return sorted(terms, key=len, reverse=True)


def _evidence_item_matches_subject(
    item: Mapping[str, Any],
    subject: Mapping[str, Any] | None,
    event_digest: Mapping[str, Any] | None,
) -> bool:
    payload = dict(subject or {})
    asset_type = str(payload.get("asset_type") or "").strip()
    if asset_type != "cn_fund":
        return True
    row = dict(item or {})
    text = " ".join(
        str(row.get(key) or "").strip()
        for key in ("title", "lead_detail", "signal_type", "signal_conclusion", "impact_summary", "source")
        if str(row.get(key) or "").strip()
    )
    if not text:
        return False
    terms = _subject_evidence_terms(payload, dict(event_digest or {}))
    if any(term and term in text for term in terms):
        return True
    # Product-structure rows are still valid for fund reports even when they
    # do not mention the theme by name. Generic news/舆情 rows are not.
    structural_markers = ("成分权重", "跟踪指数", "行业/指数框架", "标准指数框架", "业绩基准", "费率结构")
    return any(marker in text for marker in structural_markers)


def _merged_evidence_items(
    items: Sequence[Mapping[str, Any]],
    *,
    event_digest: Mapping[str, Any] | None = None,
    seen_identities: set[tuple[str, str, str]] | None = None,
    subject: Mapping[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set(seen_identities or set())
    for item in list(items or []):
        row = dict(item or {})
        if not str(row.get("title", "") or "").strip():
            continue
        if _is_diagnostic_intelligence_row(row):
            continue
        if not _evidence_item_matches_subject(row, subject, event_digest):
            continue
        identity = _evidence_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(row)
        if seen_identities is not None:
            seen_identities.add(identity)
    digest = dict(event_digest or {})
    for item in list(digest.get("items") or []):
        row = dict(item or {})
        if not str(row.get("title", "") or "").strip():
            continue
        if _is_diagnostic_intelligence_row(row):
            continue
        if not _evidence_item_matches_subject(row, subject, event_digest):
            continue
        identity = _evidence_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(row)
        if seen_identities is not None:
            seen_identities.add(identity)
    return merged


def _evidence_lines_with_event_digest(
    items: Sequence[Mapping[str, Any]],
    *,
    event_digest: Mapping[str, Any] | None = None,
    max_items: int = 3,
    as_of: Any = None,
    symbol: Any = "",
    seen_identities: set[tuple[str, str, str]] | None = None,
    subject: Mapping[str, Any] | None = None,
) -> List[str]:
    digest = dict(event_digest or {})
    return _evidence_lines(
        _merged_evidence_items(items, event_digest=digest, seen_identities=seen_identities, subject=subject),
        max_items=max_items,
        as_of=as_of,
        previous_reviewed_at=digest.get("previous_reviewed_at"),
        symbol=symbol,
    )


def _briefing_intelligence_board_lines(
    payload: Mapping[str, Any],
    *,
    event_digest: Mapping[str, Any] | None = None,
    max_items: int = 6,
) -> List[str]:
    digest = dict(event_digest or {})
    lines: List[str] = []
    seen: set[str] = set()
    news_report = dict(payload.get("news_report") or {})

    def _append_unique(line: str) -> None:
        normalized = re.sub(r"\s+", " ", str(line or "")).strip()
        if not normalized or normalized in seen or len(lines) >= max_items:
            return
        seen.add(normalized)
        lines.append(normalized)

    for item in list(news_report.get("summary_lines") or [])[:2]:
        text = str(item or "").strip()
        if not text:
            continue
        text = _humanize_news_summary_line(text)
        _append_unique(
            f"- `情报摘要`：{text}；信号类型：`主题聚类/来源分层`；信号强弱：`中`；主要影响：`情报筛选`"
        )

    for row in list(payload.get("index_signal_rows") or [])[:2]:
        if len(lines) >= max_items:
            break
        label = str(row[0] if len(row) > 0 else "").strip()
        latest = str(row[1] if len(row) > 1 else "").strip()
        change = str(row[2] if len(row) > 2 else "").strip()
        ma_label = str(row[3] if len(row) > 3 else "").strip()
        weekly_macd = str(row[4] if len(row) > 4 else "").strip()
        monthly_macd = str(row[5] if len(row) > 5 else "").strip()
        volume_label = str(row[6] if len(row) > 6 else "").strip()
        summary = str(row[7] if len(row) > 7 else "").strip()
        if not label:
            continue
        signal_parts = [part for part in (latest, change, ma_label, weekly_macd, monthly_macd, volume_label) if part and part != "N/A"]
        detail = " / ".join(signal_parts[:4]) if signal_parts else "结构快照"
        conclusion = summary or "周月节奏已接入"
        _append_unique(
            f"- `市场结构`：{label}；信号类型：`周月节奏`；信号强弱：`中`；主要影响：`核心指数`；结论：{detail} / {conclusion}"
        )

    for row in list(payload.get("market_signal_rows") or [])[:3]:
        if len(lines) >= max_items:
            break
        label = str(row[0] if len(row) > 0 else "").strip()
        value = str(row[1] if len(row) > 1 else "").strip()
        signal_label = str(row[2] if len(row) > 2 else "").strip()
        detail = str(row[3] if len(row) > 3 else "").strip()
        if not label:
            continue
        if label == "交易结构":
            _append_unique(
                f"- `市场结构`：{label} {value}；信号类型：`交易结构`；信号强弱：`{signal_label or '中'}`；主要影响：`A股市场`；结论：{detail or '交易结构快照已接入'}"
            )
        elif label in {"市场宽度", "成交量能", "情绪极端"}:
            _append_unique(
                f"- `盘面指标`：{label} {value}；信号类型：`市场结构`；信号强弱：`{signal_label or '中'}`；主要影响：`A股市场`；结论：{detail or '结构性盘面快照已接入'}"
            )

    def _row_priority(row: Sequence[Any]) -> tuple[int, int, int]:
        source = str(row[2] if len(row) > 2 else "").strip()
        signal = str(row[6] if len(row) > 6 else "").strip()
        title = str(row[1] if len(row) > 1 else "").strip()
        impact = str(row[4] if len(row) > 4 else "").strip()
        conclusion = str(row[7] if len(row) > 7 else "").strip()
        link = str(row[5] if len(row) > 5 else "").strip()
        blob = " ".join(part for part in (source, signal, title, impact, conclusion) if part)
        priority = 5
        if source in {"公司公告/结构化", "互动易/投资者关系"}:
            priority = 0
        elif source == "卖方共识专题":
            priority = 1
        elif source in {"个股资金流向专题", "两融专题", "龙虎榜/打板专题", "交易所风险专题", "研报辅助层"}:
            priority = 2
        elif source in {"TDX结构专题", "DC结构专题", "港股/短线辅助", "转债辅助层"}:
            priority = 3
        elif source in {"申万行业框架", "中信行业框架", "同花顺主题成分"}:
            priority = 4
        strength = str(row[3] if len(row) > 3 else "").strip()
        strength_rank = {"高": 0, "中": 1, "低": 2}.get(strength, 1)
        link_rank = 0 if link else 1
        if any(token in blob for token in ("公告", "财报", "披露", "年报", "季报", "互动", "投资者关系", "e互动", "路演纪要", "业绩说明会", "投资者问答")):
            priority = min(priority, 1)
        if any(token in blob for token in ("卖方共识", "券商金股", "券商推荐", "broker_recommend")):
            priority = min(priority, 1)
        if any(token in blob for token in ("研报", "盈利预测", "目标价", "卖方评级", "report_rc")):
            priority = min(priority, 2)
        return (priority, strength_rank, link_rank)

    ranked_market_rows = sorted(list(payload.get("market_event_rows") or []), key=_row_priority)

    for row in ranked_market_rows:
        title = str(row[1] if len(row) > 1 else "").strip()
        date = str(row[0] if len(row) > 0 else "").strip()
        source = str(row[2] if len(row) > 2 else "").strip()
        strength = str(row[3] if len(row) > 3 else "").strip() or "中"
        impact = str(row[4] if len(row) > 4 else "").strip() or "观察池核心资产"
        link = str(row[5] if len(row) > 5 else "").strip()
        signal_type = str(row[6] if len(row) > 6 else "").strip() or "主题/市场情报"
        conclusion = str(row[7] if len(row) > 7 else "").strip()
        if not title:
            continue
        title_text = f"[{title}]({link})" if link else title
        suffix_parts = [part for part in (source, date) if part and part != "—"]
        suffix = f"（{' / '.join(suffix_parts)}）" if suffix_parts else ""
        conclusion_text = f"；结论：{conclusion}" if conclusion else ""
        _append_unique(
            f"- `市场情报`：{title_text}{suffix}；信号类型：`{signal_type}`；信号强弱：`{strength}`；主要影响：`{impact}`{conclusion_text}"
        )
    news_items = list(news_report.get("items") or [])
    for line in _evidence_lines(
        news_items,
        max_items=max_items,
        as_of=payload.get("generated_at"),
        previous_reviewed_at=digest.get("previous_reviewed_at"),
    ):
        _append_unique(line)
    for row in list(payload.get("theme_tracking_rows") or []):
        direction = str(row[0] if len(row) > 0 else "").strip()
        catalyst = str(row[1] if len(row) > 1 else "").strip() or "当前更多依赖主线延续和盘面承接"
        risk = str(row[4] if len(row) > 4 else "").strip()
        if not direction:
            continue
        extra = f"；主要风险：`{risk}`" if risk else ""
        _append_unique(
            f"- `方向线索`：{direction}；信号类型：`方向/主题线索`；信号强弱：`中`；当前看点：`{catalyst}`{extra}"
        )
    for item in list(payload.get("core_event_lines") or []):
        text = str(item or "").strip()
        if not text:
            continue
        _append_unique(
            f"- `主线情报`：{text}；信号类型：`主线/背景情报`；信号强弱：`中`"
        )
    digest_items = [
        dict(item or {})
        for item in list(digest.get("items") or [])
        if str(item.get("source", "") or "").strip().lower() != "briefing"
        or effective_intelligence_link(item)
    ]
    for line in _evidence_lines(
        digest_items,
        max_items=max_items,
        as_of=payload.get("generated_at"),
        previous_reviewed_at=digest.get("previous_reviewed_at"),
    ):
        _append_unique(line)
    return lines[:max_items]


def _dimension_row_lookup(rows: Sequence[Sequence[Any]]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for row in list(rows or []):
        if not row:
            continue
        label = str(row[0] if len(row) > 0 else "").strip()
        value = str(row[1] if len(row) > 1 else "").strip()
        reason = str(row[2] if len(row) > 2 else "").strip()
        if label and (value or reason):
            lookup[label] = f"{value}；{reason}".strip("；")
    return lookup


def _fund_section_table_value(section_lines: Sequence[str], label: str) -> str:
    target = str(label or "").strip()
    if not target:
        return ""
    for raw in list(section_lines or []):
        line = str(raw or "").strip()
        if not (line.startswith("|") and line.endswith("|")):
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 2:
            continue
        if parts[0] == target:
            return parts[1]
    return ""


def _analysis_find_factor(
    analysis: Mapping[str, Any],
    *,
    factor_name: str = "",
    factor_id: str = "",
    dimension_key: str = "",
) -> Dict[str, Any]:
    if not factor_name and not factor_id:
        return {}
    dimensions = dict(dict(analysis or {}).get("dimensions") or {})
    dimension_items = [dimensions.get(dimension_key)] if dimension_key else dimensions.values()
    for dimension in dimension_items:
        for factor in list(dict(dimension or {}).get("factors") or []):
            item = dict(factor or {})
            if not item:
                continue
            name = str(item.get("name", "")).strip()
            fid = str(item.get("factor_id", "")).strip()
            if factor_name and name == factor_name:
                return item
            if factor_id and fid == factor_id:
                return item
    return {}


def _fund_sales_front_signal(analysis: Mapping[str, Any]) -> str:
    fund_profile = dict(dict(analysis or {}).get("fund_profile") or {})
    sales_ratio_snapshot = dict(fund_profile.get("sales_ratio_snapshot") or {})
    if not sales_ratio_snapshot:
        return ""
    latest_year = str(sales_ratio_snapshot.get("latest_year", "") or "").strip()
    lead_channel = str(sales_ratio_snapshot.get("lead_channel", "") or "").strip()
    lead_ratio = ""
    for item in list(sales_ratio_snapshot.get("channel_mix") or [])[:1]:
        channel = str(dict(item or {}).get("channel", "") or "").strip()
        ratio = dict(item or {}).get("ratio")
        if channel and not lead_channel:
            lead_channel = channel
        if ratio is not None:
            try:
                lead_ratio = f"{float(ratio):.2f}%"
            except (TypeError, ValueError):
                lead_ratio = ""
    summary = _pick_client_safe_line(sales_ratio_snapshot.get("summary") or "")
    compact = " / ".join(part for part in [latest_year, lead_channel, lead_ratio] if part)
    if compact and summary:
        return f"{compact}；{summary}"
    return compact or summary


def _front_factor_signal(factor: Mapping[str, Any]) -> str:
    item = dict(factor or {})
    if not item:
        return ""
    meta = dict(item.get("factor_meta") or {})
    if meta.get("degraded"):
        return ""
    signal = _pick_client_safe_line(item.get("signal") or item.get("detail") or "")
    lowered = signal.lower()
    if not signal or any(token in lowered for token in ("缺失", "空表", "未找到", "不可用", "非当期", "blocked")):
        return ""
    return signal


def _analysis_dimension_score_text(analysis: Mapping[str, Any], dimension_key: str) -> str:
    dimension = dict(dict(analysis or {}).get("dimensions", {}).get(dimension_key) or {})
    score = dimension.get("score")
    max_score = dimension.get("max_score", 100)
    if score is None:
        return "—"
    if isinstance(score, float) and score.is_integer():
        score = int(score)
    return f"{score}/{max_score}"


def _stock_company_research_section(
    analysis: Mapping[str, Any],
    *,
    bucket: str,
    playbook: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    subject = dict(analysis or {})
    if str(subject.get("asset_type", "")).strip() not in {"cn_stock", "hk", "us"}:
        return []

    name = str(subject.get("name", "")).strip() or str(subject.get("symbol", "")).strip() or "当前公司"
    action = dict(subject.get("action") or {})
    theme_label = representative_theme_label({**subject, "theme_playbook": dict(playbook or {})})
    if bucket == "正式推荐":
        main_answer = "这版已经从纯风控稿进入“研究和执行都能落地”的阶段。"
        follow_view = (
            f"`{name}` 所在的 `{theme_label}` 方向仍值得继续跟，研究判断和执行层开始同向，更接近可执行的中线 / 波段对象。"
            if theme_label
            else f"`{name}` 仍值得继续跟，研究判断和执行层开始同向，更接近可执行的中线 / 波段对象。"
        )
    elif bucket == "看好但暂不推荐":
        main_answer = "这是“研究可以继续跟、交易还要等确认”的过渡稿。"
        follow_view = (
            f"`{name}` 所在的 `{theme_label}` 逻辑未破，仍值得继续跟；只是更像“研究能看、交易未到位”，还不是舒服的重仓窗口。"
            if theme_label
            else f"`{name}` 的逻辑未破，仍值得继续跟；只是更像“研究能看、交易未到位”，还不是舒服的重仓窗口。"
        )
    else:
        main_answer = "这是观察 / 风控稿，重点先回答“现在能不能动”。"
        follow_view = (
            f"`{name}` 所在的 `{theme_label}` 不是直接判死，仍值得继续跟；只是当前证据更支持“继续观察”，还不足以支撑中线重仓。"
            if theme_label
            else f"`{name}` 不是直接判死，仍值得继续跟；只是当前证据更支持“继续观察”，还不足以支撑中线重仓。"
        )

    fundamental_dimension = dict(subject.get("dimensions", {}).get("fundamental") or {})
    technical_dimension = dict(subject.get("dimensions", {}).get("technical") or {})
    catalyst_dimension = dict(subject.get("dimensions", {}).get("catalyst") or {})
    relative_dimension = dict(subject.get("dimensions", {}).get("relative_strength") or {})
    risk_dimension = dict(subject.get("dimensions", {}).get("risk") or {})

    fundamental_score = fundamental_dimension.get("score")
    fundamental_score_text = _analysis_dimension_score_text(subject, "fundamental")
    fundamental_signals: List[str] = []
    for factor_name in ("个股估值", "价格位置", "现金流质量", "ROE", "盈利增速", "PEG 代理"):
        factor = _analysis_find_factor(subject, factor_name=factor_name, dimension_key="fundamental")
        signal = _front_factor_signal(factor)
        if signal and signal not in fundamental_signals:
            short_label = factor_name.replace("个股", "").replace("代理", "").strip()
            fundamental_signals.append(f"{short_label} {signal}".strip())
    if fundamental_score is None:
        fundamental_text = "当前基本面还缺关键可比信息，这里更该理解成“研究不完整”，不要直接翻译成公司好坏。"
    elif float(fundamental_score) <= 10:
        fundamental_text = f"这里这档低分（`{fundamental_score_text}`）更像“当前位置 / 性价比偏紧”：估值、盈利和现金流这组交易性价比不舒服，不代表公司价值归零。"
    elif float(fundamental_score) < 40:
        fundamental_text = f"这里这档分数（`{fundamental_score_text}`）更像“性价比一般”：研究层仍有短板，但不代表公司质地本身很差。"
    elif float(fundamental_score) < 70:
        fundamental_text = f"这里这档分数（`{fundamental_score_text}`）说明公司层支撑存在，但还没有舒服到可以忽略位置和风控。"
    else:
        fundamental_text = f"这里这档分数（`{fundamental_score_text}`）说明公司层支撑相对扎实，但是否重仓仍要看催化和执行节奏。"
    if fundamental_signals:
        fundamental_text += f" 当前前置证据主要是：{'；'.join(fundamental_signals[:3])}。"

    why_not_heavy_parts: List[str] = []
    for label, summary in (
        ("基本面", _pick_client_safe_line(fundamental_dimension.get("summary") or "")),
        ("催化面", _pick_client_safe_line(catalyst_dimension.get("summary") or "")),
        ("技术面", _pick_client_safe_line(technical_dimension.get("summary") or "")),
        ("风险特征", _pick_client_safe_line(risk_dimension.get("summary") or "")),
    ):
        if summary:
            why_not_heavy_parts.append(f"{label}：{summary.rstrip('。')}")
    if why_not_heavy_parts:
        why_not_heavy = "；".join(why_not_heavy_parts[:3]) + "。"
    else:
        why_not_heavy = "当前公司研究和交易层还没完全同向。"
    if bucket != "正式推荐":
        why_not_heavy += " 所以现在先观察，不急着上中线重仓。"
    else:
        why_not_heavy += " 所以即使可以参与，仓位和节奏也仍要尊重风控。"

    upgrade_needs: List[str] = []
    trigger_text = _pick_client_safe_line(_primary_upgrade_trigger(subject))
    if str(subject.get("asset_type", "")).strip() == "cn_stock" and bucket != "正式推荐":
        trigger_text = _pick_client_safe_line(
            _observe_trigger_condition(
                subject,
                dict(action.get("horizon") or {}),
                default_text="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                qualitative_only=False,
            )
        )
    if trigger_text:
        upgrade_needs.append(f"交易确认：{trigger_text}")
    leading_catalyst = _front_factor_signal(
        _analysis_find_factor(subject, factor_name="前瞻催化", dimension_key="catalyst")
    ) or _front_factor_signal(
        _analysis_find_factor(subject, factor_name="结构化事件", dimension_key="catalyst")
    )
    if leading_catalyst:
        upgrade_needs.append(f"公司验证：别只停在 {leading_catalyst} 这一条线索上")
    else:
        upgrade_needs.append("公司验证：财报 / 订单 / 分红 / 管理层口径等直连证据继续补齐")
    if fundamental_score is None or float(fundamental_score) < 40:
        upgrade_needs.append("性价比：估值和安全边际回到更舒服的位置")
    relative_summary = _pick_client_safe_line(relative_dimension.get("summary") or "")
    if relative_summary:
        upgrade_needs.append(f"资金确认：继续验证“{relative_summary.rstrip('。')}”这层能不能站住")
    upgrade_text = "；".join(upgrade_needs[:3]) + "。"
    term_translation = _stock_observe_term_translation_text(subject)

    lines = [
        "## 公司研究层判断",
        "",
        "这段只回答“这家公司 / 方向还值不值得继续跟”，不重复上面的买卖纪律。",
        "",
    ]
    lines.extend(
        _table(
            ["判断项", "一句话"],
            [
                ["研究定位", main_answer],
                ["一句话看法", follow_view],
                ["低分别误读", fundamental_text],
                ["为什么还不重仓", why_not_heavy],
                *( [["这几个词怎么读", term_translation]] if term_translation else [] ),
                ["升级要看什么", upgrade_text],
            ],
        )
    )
    return lines


def _insert_stock_company_research_section(
    markdown_text: str,
    analysis: Mapping[str, Any],
    *,
    bucket: str,
    playbook: Optional[Mapping[str, Any]] = None,
) -> str:
    text = str(markdown_text or "").rstrip()
    section_lines = _stock_company_research_section(
        analysis,
        bucket=bucket,
        playbook=playbook,
    )
    if not text or not section_lines:
        return text
    if "## 公司研究层判断" in text:
        return _replace_markdown_section(text, "## 公司研究层判断", section_lines)
    before_heading = (
        "## 执行摘要"
        if "## 执行摘要" in text
        else "## 图表速览"
        if "## 图表速览" in text
        else "## 为什么这么判断"
    )
    return _insert_markdown_section_before(text, before_heading, section_lines)


def _ensure_terminal_cn_period(text: str) -> str:
    cleaned = _pick_client_safe_line(text or "").rstrip()
    if not cleaned:
        return ""
    if cleaned.endswith(("。", "！", "？")):
        return cleaned
    return f"{cleaned}。"


def _dimension_display_label(
    analysis: Mapping[str, Any],
    dimension_key: str,
    default_label: str,
    dimension: Mapping[str, Any],
) -> str:
    asset_type = str(analysis.get("asset_type", "")).strip()
    if asset_type == "cn_stock" and dimension_key == "fundamental":
        score = dimension.get("score")
        try:
            numeric_score = float(score) if score is not None else None
        except (TypeError, ValueError):
            numeric_score = None
        if numeric_score is not None and numeric_score <= 40:
            return "基本面（当前位置/性价比）"
    return default_label


def _stock_front_reason_lines(analysis: Mapping[str, Any]) -> List[str]:
    lines: List[str] = []

    stock_factor = _analysis_find_factor(
        analysis,
        factor_name="股票技术面状态",
        factor_id="j1_stk_factor_pro",
    )
    stock_signal = _front_factor_signal(stock_factor)
    if stock_signal:
        lines.append(f"技术层确认：{_ensure_terminal_cn_period(stock_signal)}")

    ah_factor = _analysis_find_factor(
        analysis,
        factor_name="跨市场比价",
        factor_id="j3_ah_comparison",
    )
    ah_signal = _front_factor_signal(ah_factor)
    if ah_signal:
        lines.append(f"跨市场层确认：{_ensure_terminal_cn_period(ah_signal)}")

    cb_factor = _analysis_find_factor(
        analysis,
        factor_name="可转债映射",
        factor_id="j4_convertible_bond_proxy",
    )
    cb_signal = _front_factor_signal(cb_factor)
    if cb_signal:
        lines.append(f"转债层补充：{_ensure_terminal_cn_period(cb_signal)}")

    for row in list(dict(analysis or {}).get("market_event_rows") or []):
        title = str(row[1] if len(row) > 1 else "").strip()
        source = str(row[2] if len(row) > 2 else "").strip()
        signal_type = str(row[6] if len(row) > 6 else "").strip()
        conclusion = _pick_client_safe_line(str(row[7] if len(row) > 7 else "").strip())
        if not any("机构调研" in part for part in (title, source, signal_type)):
            continue
        detail = title
        if conclusion and conclusion not in detail:
            detail = f"{detail}；{conclusion}"
        if detail:
            lines.append(f"调研层补充：{_ensure_terminal_cn_period(detail)}")
        break

    return lines[:4]


def _etf_front_reason_lines(analysis: Mapping[str, Any]) -> List[str]:
    rows = _dimension_row_lookup(list(dict(analysis or {}).get("dimension_rows") or []))
    fund_profile = dict(dict(analysis or {}).get("fund_profile") or {})
    overview = dict(fund_profile.get("overview") or {})
    fund_sections = list(dict(analysis or {}).get("fund_sections") or [])
    lines: List[str] = []

    product_signal = str(
        overview.get("ETF场内技术状态", "") or _fund_section_table_value(fund_sections, "场内基金技术状态")
    ).strip()
    if product_signal:
        lines.append(f"产品层确认：场内基金技术状态当前是 `{product_signal}`，先看 ETF 本身的趋势和动能是否允许持有。")

    share_change = str(
        overview.get("ETF最近份额变化", "") or _fund_section_table_value(fund_sections, "最近份额变化")
    ).strip()
    if share_change:
        lines.append(f"申赎线索：{share_change}。")

    size_change = str(
        overview.get("ETF最近规模变化", "") or _fund_section_table_value(fund_sections, "最近规模变化")
    ).strip()
    if size_change:
        lines.append(f"规模线索：{size_change}。")

    index_signal = rows.get("跟踪指数技术状态", "")
    if index_signal:
        lines.append(f"指数层确认：{_ensure_terminal_cn_period(index_signal)}")

    product_factor_signal = rows.get("场内基金技术状态（ETF/基金专属）", "")
    if product_factor_signal and all(product_factor_signal not in item for item in lines):
        lines.append(f"产品因子确认：{_ensure_terminal_cn_period(product_factor_signal)}")

    return lines[:4]


def _fund_front_reason_lines(analysis: Mapping[str, Any]) -> List[str]:
    lines: List[str] = []
    sales_signal = _fund_sales_front_signal(analysis)
    if sales_signal:
        lines.append(f"渠道层确认：{sales_signal}。")

    gold_factor = _analysis_find_factor(
        analysis,
        factor_name="现货锚定",
        factor_id="j5_gold_spot_anchor",
    )
    gold_signal = _pick_client_safe_line(gold_factor.get("signal") or gold_factor.get("detail") or "")
    if gold_signal:
        lines.append(f"现货链确认：{gold_signal}。")

    return lines[:3]


def _briefing_reason_bullets(
    headline_lines: Sequence[str],
    intelligence_board_lines: Sequence[str],
    action_lines: Sequence[str],
) -> List[str]:
    bullets: List[str] = []
    seen: set[str] = set()

    def _append(text: str) -> None:
        line = _pick_client_safe_line(text or "")
        normalized = re.sub(r"\s+", " ", str(line or "")).strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        bullets.append(normalized)

    for item in list(headline_lines or [])[:4]:
        _append(item)

    for item in list(intelligence_board_lines or []):
        cleaned = re.sub(r"^-\s*", "", str(item or "").strip())
        if not cleaned:
            continue
        if "；结论：" in cleaned:
            cleaned = cleaned.split("；结论：", 1)[1]
        elif "；主要影响：" in cleaned:
            cleaned = cleaned.split("；主要影响：", 1)[1]
        cleaned = cleaned.replace("`", "")
        cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cleaned)
        _append(cleaned)
        if len(bullets) >= 3:
            break

    if len(bullets) < 3:
        for item in list(action_lines or []):
            _append(item)
            if len(bullets) >= 3:
                break

    return bullets[:3]


def _briefing_evidence_rows(payload: Mapping[str, Any], evidence_rows: Sequence[Sequence[Any]]) -> List[List[str]]:
    normalized: List[List[str]] = []
    for row in list(evidence_rows or []):
        if not row:
            continue
        label = str(row[0] if len(row) > 0 else "").strip()
        value = _pick_client_safe_line(row[1] if len(row) > 1 else "") or "—"
        if not label:
            continue
        if label == "生成时间":
            label = "分析生成时间"
        elif label in {"A股观察池", "观察池来源"}:
            label = "A股观察池来源"
        normalized.append([label, value])

    existing_labels = {row[0] for row in normalized if row}

    if "分析生成时间" not in existing_labels:
        generated_at = str(payload.get("generated_at", "")).strip()
        normalized.append(["分析生成时间", generated_at or "—"])
    if "A股观察池来源" not in existing_labels:
        watch_meta = dict(payload.get("a_share_watch_meta") or {})
        pool_size = watch_meta.get("pool_size")
        complete_size = watch_meta.get("complete_analysis_size")
        source_text = "Tushare 优先全市场初筛"
        if pool_size not in (None, "") or complete_size not in (None, ""):
            source_text += f"；初筛 `{pool_size or '—'}` 只，完整分析 `{complete_size or '—'}` 只。"
        normalized.append(["A股观察池来源", source_text])
    if "时点边界" not in existing_labels:
        normalized.append(["时点边界", "默认只使用生成时点前可见的宏观、外部新闻和观察池快照。"])
    if "市场结构快照" not in existing_labels:
        market_signal_rows = list(payload.get("market_signal_rows") or [])
        index_signal_rows = list(payload.get("index_signal_rows") or [])
        structure_bits: List[str] = []
        if market_signal_rows:
            for row in market_signal_rows[:3]:
                label = str(row[0] if len(row) > 0 else "").strip()
                value = str(row[1] if len(row) > 1 else "").strip()
                signal_label = str(row[2] if len(row) > 2 else "").strip()
                detail = str(row[3] if len(row) > 3 else "").strip()
                if label == "交易结构":
                    structure_bits.append(
                        f"{label} {value}；{signal_label or '中'}；{detail or '交易结构快照已接入'}"
                    )
                    break
        if index_signal_rows:
            row = index_signal_rows[0]
            label = str(row[0] if len(row) > 0 else "").strip()
            ma_label = str(row[3] if len(row) > 3 else "").strip()
            weekly_macd = str(row[4] if len(row) > 4 else "").strip()
            monthly_macd = str(row[5] if len(row) > 5 else "").strip()
            volume_label = str(row[6] if len(row) > 6 else "").strip()
            summary = str(row[7] if len(row) > 7 else "").strip()
            if label:
                bits = [item for item in (ma_label, weekly_macd, monthly_macd, volume_label) if item and item != "N/A"]
                if summary:
                    bits.append(summary)
                structure_bits.insert(0, f"{label} " + " / ".join(bits[:4]))
        if structure_bits:
            normalized.append(["市场结构快照", "；".join(structure_bits[:2])])

    ordered_labels = ["分析生成时间", "A股观察池来源", "时点边界"]
    head = [row for label in ordered_labels for row in normalized if row and row[0] == label]
    tail = [row for row in normalized if row and row[0] not in ordered_labels]
    return head + tail


def _analysis_provenance_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    provenance = dict(analysis.get("provenance") or {})
    rebuilt = build_analysis_provenance(analysis)
    for key in (
        "analysis_generated_at",
        "market_data_as_of",
        "market_data_source",
        "relative_benchmark_name",
        "relative_benchmark_symbol",
        "intraday_as_of",
        "catalyst_evidence_as_of",
        "catalyst_sources",
        "catalyst_sources_text",
        "catalyst_diagnosis",
        "catalyst_web_review_decision",
        "catalyst_web_review_impact",
        "news_mode",
        "point_in_time_note",
        "notes",
    ):
        provenance[key] = rebuilt.get(key, provenance.get(key))
    rows: List[List[str]] = [
        ["分析生成时间", provenance.get("analysis_generated_at", "—")],
        ["行情 as_of", provenance.get("market_data_as_of", "—")],
        ["行情来源", provenance.get("market_data_source", "—")],
        [
            "相对强弱基准",
            (
                f"{provenance.get('relative_benchmark_name', '—')} ({provenance.get('relative_benchmark_symbol', '')})"
                if str(provenance.get("relative_benchmark_symbol", "")).strip()
                else provenance.get("relative_benchmark_name", "—")
            ),
        ],
        ["分钟级快照 as_of", provenance.get("intraday_as_of", "未启用")],
        ["催化证据 as_of", _pick_client_safe_line(provenance.get("catalyst_evidence_as_of", "—")) or "—"],
        ["催化来源", _pick_client_safe_line(provenance.get("catalyst_sources_text", "—")) or "—"],
        ["催化诊断", _pick_client_safe_line(provenance.get("catalyst_diagnosis", "—")) or "—"],
        ["联网复核结论", provenance.get("catalyst_web_review_decision", "—")],
        ["联网复核影响", provenance.get("catalyst_web_review_impact", "—")],
        ["情报模式", provenance.get("news_mode", "unknown")],
        ["时点边界", _pick_client_safe_line(provenance.get("point_in_time_note", "默认只使用生成时点前可见信息。")) or "默认只使用生成时点前可见信息。"],
    ]
    for item in list(provenance.get("notes") or [])[:2]:
        rows.append(["时点/溯源提醒", _pick_client_safe_line(item) or "—"])
    return rows


def _analysis_provenance_lines(analysis: Mapping[str, Any]) -> List[str]:
    rows = _analysis_provenance_rows(analysis)
    lines = ["## 证据时点与来源", ""]
    lines.extend(_table(["项目", "说明"], rows))
    return lines


def _dimension_reason_text(dimension: Mapping[str, Any], *, positive: bool) -> str:
    factors = list(dimension.get("factors") or [])
    candidates: List[Tuple[float, str]] = []
    zero_count_signals: List[str] = []
    for factor in factors:
        signal = str(factor.get("signal", "")).strip()
        if not signal:
            continue
        if signal in GENERIC_FACTOR_SIGNALS:
            continue
        if signal.startswith("个股相关头条 0 条") or signal.startswith("覆盖源 0 个"):
            zero_count_signals.append(signal)
            continue
        display = str(factor.get("display_score", "")).strip()
        if display in {"缺失", "不适用", "信息项"}:
            continue
        score = _score_value(display)
        maximum = _score_value(str(factor.get("max", "")))
        if score is None or maximum in (None, 0):
            continue
        ratio = score / maximum
        if positive and ratio <= 0:
            continue
        if not positive and ratio >= 0.75:
            continue
        candidates.append((ratio, signal))

    if candidates:
        candidates.sort(key=lambda item: item[0], reverse=positive)
        return candidates[0][1]

    if not positive and zero_count_signals:
        return zero_count_signals[0]

    core_signal = str(dimension.get("core_signal", "")).strip()
    if core_signal and core_signal not in GENERIC_DIMENSION_SUMMARIES:
        return core_signal
    summary = str(dimension.get("summary", "")).strip()
    if summary and summary not in GENERIC_DIMENSION_SUMMARIES:
        return summary
    return core_signal or summary


def _merge_reason_lines(
    primary: Sequence[str],
    secondary: Sequence[str],
    *,
    max_items: int = 3,
) -> List[str]:
    merged: List[str] = []
    seen = set()
    for source in (primary, secondary):
        for item in source:
            text = str(item).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            merged.append(text)
            if len(merged) >= max_items:
                return merged
    return merged


def _take_diverse_reason_lines(
    candidates: Sequence[str],
    used_counts: Counter[str] | None,
    *,
    max_items: int = 3,
) -> List[str]:
    if used_counts is None:
        return list(candidates[:max_items])

    picked: List[str] = []
    for limit in (1, 2):
        for item in candidates:
            text = str(item).strip()
            if not text or text in picked:
                continue
            fingerprint = _reason_fingerprint(text)
            if fingerprint and used_counts[fingerprint] >= limit:
                continue
            picked.append(text)
            if len(picked) >= max_items:
                break
        if len(picked) >= max_items:
            break

    for item in picked:
        fingerprint = _reason_fingerprint(item)
        if fingerprint:
            used_counts[fingerprint] += 1
    return picked


def _analysis_section_lines(
    analysis: Mapping[str, Any],
    bucket: str,
    *,
    day_theme: str = "",
    used_positive_reasons: Counter[str] | None = None,
    used_caution_reasons: Counter[str] | None = None,
    generated_at: str = "",
) -> List[str]:
    name = str(analysis.get("name", ""))
    symbol = str(analysis.get("symbol", ""))
    action = dict(analysis.get("action") or {})
    narrative = dict(analysis.get("narrative") or {})
    horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")), context=name)
    handoff = portfolio_whatif_handoff(
        symbol=symbol,
        horizon=horizon,
        direction=str(action.get("direction", "")),
        asset_type=str(analysis.get("asset_type", "")),
        reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
        generated_at=str(analysis.get("generated_at", "")) or generated_at,
    )
    positive_candidates = _merge_reason_lines(
        _top_dimension_reasons(analysis, top_n=3),
        list(narrative.get("positives") or []),
        max_items=6,
    )
    caution_candidates = _merge_reason_lines(
        _bottom_dimension_reasons(analysis, top_n=3),
        list(narrative.get("cautions") or []),
        max_items=6,
    )
    positives = _take_diverse_reason_lines(positive_candidates, used_positive_reasons, max_items=3)
    cautions = _take_diverse_reason_lines(caution_candidates, used_caution_reasons, max_items=3)
    if len(cautions) > 2:
        cautions = cautions[:2]
    portfolio_overlap_lines = _portfolio_overlap_lines(analysis)
    theme_context = dict(analysis)
    if day_theme and not subject_theme_label(theme_context):
        theme_context["day_theme"] = {"label": day_theme}
    packet = build_scan_editor_packet(theme_context, bucket=bucket)
    theme_lines = _theme_playbook_reason_lines(dict(packet.get("theme_playbook") or {}))
    front_reason_lines = _stock_front_reason_lines(analysis)
    cycle_context = front_reason_lines[0] if front_reason_lines else (positives[0] if positives else "")
    caution_context = cautions[0] if cautions else _analysis_constraint_hint(analysis)
    trigger_context = _pick_client_safe_line(_primary_upgrade_trigger(analysis))
    watch_context = _observe_watch_levels(analysis)

    lines = [
        f"### {name} ({symbol})",
        "",
        f"**先看结论：** {_analysis_section_takeaway(analysis, bucket)}",
        "",
    ]
    if bucket == "正式推荐":
        lines.append("为什么能进正式推荐：")
        lines.append("")
        for item in theme_lines[:2]:
            lines.append(f"- {item}")
        for item in front_reason_lines:
            lines.append(f"- {item}")
        for item in positives[:3]:
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "当前最需要防的点：",
                "",
            ]
        )
        for item in cautions[:3]:
            lines.append(f"- {item}")
    else:
        lines.append("为什么仍然值得继续看：")
        lines.append("")
        for item in theme_lines[:2]:
            lines.append(f"- {item}")
        for item in front_reason_lines:
            lines.append(f"- {item}")
        for item in positives[:3]:
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "为什么今天不放进正式推荐：",
                "",
            ]
        )
        for item in cautions[:3]:
            lines.append(f"- {item}")
    if portfolio_overlap_lines:
        lines.extend(["", "和现有持仓怎么配：", ""])
        for item in portfolio_overlap_lines:
            lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "操作上怎么做：",
            "",
        ]
    )
    if horizon:
        style_line = _pick_client_safe_line(horizon["style"])
        if cycle_context and cycle_context not in style_line:
            style_line = _append_sentence(style_line, cycle_context)
        horizon_line = f"- 持有周期：{horizon['label']}"
        if name:
            horizon_line += f"。对{name}来说，当前更适合按这个周期去拿。"
        else:
            horizon_line += "。当前更适合按这个周期去拿。"
        if style_line:
            horizon_line += style_line
        lines.append(horizon_line)
        if horizon.get("fit_reason"):
            fit_reason = _pick_client_safe_line(horizon["fit_reason"])
            if cycle_context:
                fit_reason = f"{cycle_context}，所以当前更适合按 `{horizon['label']}` 去拿，不只按单条催化去赌。"
            elif trigger_context and trigger_context not in fit_reason:
                fit_reason = _append_sentence(fit_reason, f"当前更该围绕 `{trigger_context}` 这类确认去等。")
            lines.append(f"- 为什么按这个周期理解：{fit_reason}")
        if horizon.get("misfit_reason"):
            misfit_reason = _pick_client_safe_line(horizon["misfit_reason"])
            if caution_context:
                misfit_reason = f"眼下更该先处理 `{caution_context}`，不适合直接把它当长期底仓或纯超短去做。"
            lines.append(f"- 现在不适合的打法：{misfit_reason}")
    if handoff.get("timing_summary"):
        timing_line = _pick_client_safe_line(handoff["timing_summary"])
        hint = _analysis_constraint_hint(analysis)
        if hint and hint not in timing_line:
            timing_line = _append_sentence(timing_line, hint)
        lines.append(f"- 适用时段：{timing_line}")
    lines.append(f"- 介入条件：{_pick_client_safe_line(action.get('entry', '等待进一步确认'))}")
    if bucket != "正式推荐":
        observe_only_bucket = bucket == "观察为主"
        lines.append(
            "- 触发买点条件："
            + _observe_trigger_condition(
                analysis,
                horizon,
                default_text="等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                qualitative_only=observe_only_bucket,
            )
        )
        watch_levels = _observe_watch_levels(analysis, qualitative_only=observe_only_bucket)
        if watch_levels:
            lines.append(f"- 关键盯盘价位：{watch_levels}")
    buy_range = _safe_buy_range_text(action)
    if buy_range and "暂不设" not in buy_range:
        lines.append(f"- 建议买入区间：{_pick_client_safe_line(buy_range)}")
    scaling_line = _pick_client_safe_line(action.get("scaling_plan", "确认后再考虑第二笔"))
    if scaling_line in {"分 2-3 批建仓，每次确认后加仓", "分 2-3 批建仓"}:
        buy_range_hint = _safe_buy_range_text(action)
        entry_hint = _pick_client_safe_line(action.get("entry", ""))
        if buy_range_hint and "暂不设" not in buy_range_hint:
            scaling_line = f"先按 `{buy_range_hint}` 一带分 2-3 批承接"
            if entry_hint:
                scaling_line = _append_sentence(scaling_line, f"确认 `{entry_hint}` 后再补第二笔。")
        elif entry_hint:
            scaling_line = f"先分 2-3 批试仓，确认 `{entry_hint}` 后再补第二笔。"
    elif scaling_line == "先列观察名单，不预设加仓":
        if trigger_context:
            scaling_line = f"先列观察名单，等 `{trigger_context}` 命中后再讨论第二笔。"
        elif watch_context:
            scaling_line = f"先列观察名单，先看 `{watch_context}` 一带的承接是否站稳，再讨论第二笔。"
        elif name:
            scaling_line = f"先列观察名单，先看 `{name}` 的确认条件有没有回来，再讨论第二笔。"
    if cycle_context and cycle_context not in scaling_line:
        scaling_line = _append_sentence(scaling_line, cycle_context)
    elif trigger_context and trigger_context not in scaling_line:
        scaling_line = _append_sentence(scaling_line, f"更适合等 `{trigger_context}` 命中后再考虑第二笔。")
    elif watch_context and watch_context not in scaling_line:
        scaling_line = _append_sentence(scaling_line, f"关键位先看 `{watch_context}`。")
    lines.extend(
        [
            f"- 首次仓位：{_pick_client_safe_line(action.get('position', '小仓位分批'))}",
            f"- 加仓节奏：{scaling_line}",
            f"- 止损参考：{_pick_client_safe_line(action.get('stop', '重新跌破关键支撑就处理'))}",
            f"- 组合落单前：{handoff['summary']}",
            f"- 预演命令：`{handoff['command']}`",
        ]
    )
    trim_rows = _trim_execution_rows(action)
    if trim_rows:
        insertion_index = max(len(lines) - 3, 0)
        trim_lines = [f"- {label}：{text}" for label, text in trim_rows]
        lines[insertion_index:insertion_index] = trim_lines
    return lines


def _analysis_watch_card_lines(
    analysis: Mapping[str, Any],
    bucket: str,
    *,
    day_theme: str = "",
    used_positive_reasons: Counter[str],
    used_caution_reasons: Counter[str],
    generated_at: str,
    seen_identities: set[tuple[str, str, str]] | None = None,
) -> List[str]:
    name = str(analysis.get("name", ""))
    symbol = str(analysis.get("symbol", ""))
    action = dict(analysis.get("action") or {})
    narrative = dict(analysis.get("narrative") or {})
    horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")), context=name)
    handoff = portfolio_whatif_handoff(
        symbol=symbol,
        horizon=horizon,
        direction=str(action.get("direction", "")),
        asset_type=str(analysis.get("asset_type", "")),
        reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
        generated_at=str(analysis.get("generated_at", "")) or generated_at,
    )
    positive_candidates = _merge_reason_lines(
        _top_dimension_reasons(analysis, top_n=2),
        list(narrative.get("positives") or []),
        max_items=4,
    )
    caution_candidates = _merge_reason_lines(
        _bottom_dimension_reasons(analysis, top_n=2),
        list(narrative.get("cautions") or []),
        max_items=4,
    )
    positives = _take_diverse_reason_lines(positive_candidates, used_positive_reasons, max_items=2)
    cautions = _take_diverse_reason_lines(caution_candidates, used_caution_reasons, max_items=2)
    portfolio_overlap_lines = _portfolio_overlap_lines(analysis)
    observe_only_bucket = bucket == "观察为主"
    watch_levels = _observe_watch_levels(analysis, qualitative_only=observe_only_bucket)
    trigger_line = _observe_trigger_condition(
        analysis,
        horizon,
        default_text="等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
        qualitative_only=observe_only_bucket,
    )
    constraint_hint = _analysis_constraint_hint(analysis)
    if constraint_hint and constraint_hint not in trigger_line:
        trigger_line = _append_sentence(trigger_line, constraint_hint)
    theme_context = dict(analysis)
    if day_theme and not subject_theme_label(theme_context):
        theme_context["day_theme"] = {"label": day_theme}
    packet = build_scan_editor_packet(theme_context, bucket=bucket)
    theme_lines = _theme_playbook_reason_lines(dict(packet.get("theme_playbook") or {}))
    score_line = _observe_card_score_line(analysis)
    hard_check_line = _pick_client_safe_line(_hard_check_inline(dict(analysis)))
    headline_label = "一句话判断" if bucket == "正式推荐" else "当前判断"

    lines = [
        f"### {name} ({symbol}) | {bucket}",
        "",
        f"**{headline_label}：** {_analysis_section_takeaway(analysis, bucket)}",
        "",
        "为什么继续看它：",
        "",
    ]
    for item in theme_lines[:2]:
        lines.append(f"- {item}")
    for item in positives[:2]:
        lines.append(f"- {item}")
    lines.extend(["", "为什么现在不升级成正式推荐：", ""])
    for item in cautions[:2]:
        lines.append(f"- {item}")
    lines.extend(_pick_upgrade_lines(analysis))
    if portfolio_overlap_lines:
        lines.extend(["", "和现有持仓怎么配：", ""])
        for item in portfolio_overlap_lines:
            lines.append(f"- {item}")
    lines.extend(["", "下一步怎么盯：", ""])
    if horizon.get("label"):
        watch_profile = _pick_client_safe_line(horizon.get("fit_reason") or horizon.get("style") or "先看确认，不急着按正式动作理解。")
        lines.append(f"- 对 `{name}` 来说，当前更像{horizon['label']}：{watch_profile}")
    lines.append(f"- 对 `{name}` 来说，触发买点条件是：{trigger_line}")
    if watch_levels:
        lines.append(f"- 关键盯盘价位：{watch_levels}")
    if score_line:
        lines.append(f"- 八维速览：{score_line}")
    if hard_check_line:
        lines.append(f"- 硬检查：对 `{name}` 来说，{hard_check_line}")
    lines.append(f"- 首次仓位：{_pick_client_safe_line(action.get('position', '≤2% 观察仓，或先不出手'))}")
    lines.extend(["", "证据口径：", ""])
    evidence_lines = _evidence_lines_with_event_digest(
        list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []),
        event_digest=packet.get("event_digest") or {},
        max_items=2,
        as_of=analysis.get("generated_at"),
        symbol=analysis.get("symbol"),
        seen_identities=seen_identities,
    )
    if evidence_lines:
        lines.extend(evidence_lines)
        if not any("直连情报" in item or "高置信直连" in item for item in evidence_lines):
            lines.append(f"- 对 `{name}` 来说，当前证据更偏结构化披露与公告日历，不是直连情报催化型驱动。")
    elif list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []):
        lines.append("- 同类前置证据已在本报告前文共享区展示，这里不再重复展开。")
    else:
        provenance = dict(analysis.get("provenance") or build_analysis_provenance(analysis))
        lines.append(
            f"- `{name}` 当前没有高置信直连情报催化，先按 `"
            + str(provenance.get("catalyst_sources_text", "结构化事件/代理来源"))
            + "` 这层来源理解。"
        )
    return lines


def _analysis_detail_appendix_lines(analysis: Mapping[str, Any]) -> List[str]:
    catalyst_dimension = dict(analysis.get("dimensions", {}).get("catalyst") or {})
    risk_dimension = dict(analysis.get("dimensions", {}).get("risk") or {})
    evidence = list(catalyst_dimension.get("evidence") or [])
    evidence_lines = _evidence_lines_with_event_digest(
        evidence,
        event_digest=build_scan_editor_packet(analysis, bucket=_recommendation_bucket(analysis)).get("event_digest") or {},
        max_items=4,
        as_of=analysis.get("generated_at"),
        symbol=analysis.get("symbol"),
    )
    lines = [
        f"### {analysis.get('name', '—')} ({analysis.get('symbol', '—')})",
        "",
        "**八维雷达：**",
    ]
    lines.extend(_table(["维度", "得分", "核心信号"], _detail_dimension_rows(dict(analysis))))
    upgrade_lines = _pick_upgrade_lines(analysis)
    if upgrade_lines:
        lines.extend(["", "**为什么现在还不升级：**"])
        lines.extend(upgrade_lines)
    lines.extend(
        [
            "",
            f"**催化拆解：** 当前催化分 `{catalyst_dimension.get('score', '缺失')}/{catalyst_dimension.get('max_score', 100)}`。",
        ]
    )
    lines.extend(_table(["层次", "当前判断", "说明"], _catalyst_structure_rows(catalyst_dimension)))
    lines.append("")
    lines.extend(_table(["催化子项", "层级", "当前信号", "得分"], _client_catalyst_factor_rows(catalyst_dimension)))
    lines.extend(["", "**催化证据来源：**", ""])
    if evidence_lines:
        lines.extend(evidence_lines)
    else:
        provenance = dict(analysis.get("provenance") or build_analysis_provenance(analysis))
        lines.append(
            "当前没有高置信直连情报催化，先按 `"
            + str(provenance.get("catalyst_sources_text", "结构化事件/代理来源"))
            + "` 这层来源理解。"
        )
    lines.extend(["", f"**硬排除检查：** {_hard_check_inline(dict(analysis))}"])
    lines.extend(_table(["检查项", "状态", "说明"], _hard_check_rows(dict(analysis))))
    lines.extend(
        [
            "",
            f"**风险拆解：** 当前风险分 `{risk_dimension.get('score', '缺失')}/{risk_dimension.get('max_score', 100)}`。分数越低，说明波动、窗口和相关性压力越大。",
        ]
    )
    lines.extend(_table(["风险子项", "当前信号", "说明", "得分"], _client_factor_rows(risk_dimension)))
    lines.extend(["", *_signal_confidence_lines(analysis)])
    return lines


def _stock_pick_shared_evidence_lines(
    items: Sequence[Mapping[str, Any]],
    *,
    seen_identities: set[tuple[str, str, str]] | None = None,
) -> List[str]:
    for item in items:
        evidence = list(dict(item.get("dimensions", {}).get("catalyst") or {}).get("evidence") or [])
        evidence_lines = _evidence_lines_with_event_digest(
            evidence,
            event_digest=build_scan_editor_packet(item, bucket=_recommendation_bucket(item)).get("event_digest") or {},
            max_items=2,
            as_of=item.get("generated_at"),
            symbol=item.get("symbol"),
            seen_identities=seen_identities,
        )
        if evidence_lines:
            return [
                "- 当前观察名单里的催化先按可直接复核的结构化事件/前瞻日历理解，不把缺新闻自动写成利空。",
                *evidence_lines,
            ]
    return [
        "- 当前观察名单没有足够高置信的公司级直连催化，主要按结构化事件、前瞻日历和覆盖率口径理解。",
        "- 这类“缺直连、不等于利空”的样本更适合先观察，不直接写成强执行型推荐。",
    ]


def _stock_pick_shared_signal_confidence_lines(items: Sequence[Mapping[str, Any]]) -> List[str]:
    for item in items:
        confidence = dict(item.get("signal_confidence") or {})
        if not confidence.get("available"):
            continue
        try:
            ci_low = float(confidence.get("win_rate_20d_ci_low")) if confidence.get("win_rate_20d_ci_low") is not None else None
        except (TypeError, ValueError):
            ci_low = None
        try:
            ci_high = float(confidence.get("win_rate_20d_ci_high")) if confidence.get("win_rate_20d_ci_high") is not None else None
        except (TypeError, ValueError):
            ci_high = None
        weak_confidence = ci_low is not None and ci_high is not None and ci_low < 0.50 < ci_high
        try:
            non_overlapping = int(confidence.get("non_overlapping_count", confidence.get("sample_count", 0)) or 0)
        except (TypeError, ValueError):
            non_overlapping = 0
        name = str(item.get("name", item.get("symbol", ""))).strip() or "当前代表样本"
        lines = [
            f"- 这份观察名单先用 `{name}` 的同标的历史样本做边界参考，不把它直接当成整份名单的总胜率。",
            f"- 非重叠样本 `{confidence.get('non_overlapping_count', confidence.get('sample_count', '—'))}` 个；20日胜率 95%区间 `{_fmt_pct_interval(confidence.get('win_rate_20d_ci_low'), confidence.get('win_rate_20d_ci_high'))}`。"
            + (" 当前仍属小样本，更适合当边界参考，不适合直接锚定成高确定性胜率。" if non_overlapping < 20 else ""),
            "- 这类历史样本只说明“过去类似形态并不差”，不说明“当前这一笔已经完成触发”；样本给的是边界，不是免确认通行证。",
            f"- 样本质量 `{confidence.get('sample_quality_label', '—')}`（{confidence.get('sample_quality_score', '—')}/100）；低置信样本只作执行边界，不直接抬高推荐等级。",
        ]
        if weak_confidence:
            lines.append("- 当前 95%区间仍跨过中性线，这层更适合当附注而不是主要推荐证据。")
        return lines
    return [
        "- 历史相似样本仍坚持严格非重叠样本口径；拿不到高置信历史时宁可不报，也不拿低置信样本给推荐背书。",
        "- 当前能引用时，至少会写清 `非重叠样本 / 95%区间 / 样本质量` 三项边界。",
    ]


def _market_pick_scope_text(item: Mapping[str, Any], *, generated_at: str = "") -> str:
    action = dict(item.get("action") or {})
    narrative = dict(item.get("narrative") or {})
    horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")), context=str(item.get("name", "")))
    handoff = portfolio_whatif_handoff(
        symbol=str(item.get("symbol", "")),
        horizon=horizon,
        direction=str(action.get("direction", "")),
        asset_type=str(item.get("asset_type", "")),
        reference_price=dict(item.get("metrics") or {}).get("last_close"),
        generated_at=str(item.get("generated_at", "")) or generated_at,
    )
    scope = str(handoff.get("headline_scope", "")).strip()
    return scope or "今天"


def _scan_dimension_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    technical_signal_text = compact_technical_signal_text(analysis.get("history"))
    labels = [
        ("technical", "技术面"),
        ("fundamental", "基本面"),
        ("catalyst", "催化面"),
        ("relative_strength", "相对强弱"),
        ("risk", "风险特征"),
        ("macro", "宏观敏感度"),
    ]
    for key, label in labels:
        dimension = analysis.get("dimensions", {}).get(key, {})
        score = dimension.get("score")
        max_score = dimension.get("max_score", 100)
        display = "—" if score is None else f"{score}/{max_score}"
        reason = _dimension_summary_text(key, dimension)
        if key == "technical" and technical_signal_text and technical_signal_text not in reason:
            reason = f"{reason} {technical_signal_text}".strip() if reason and reason != "—" else technical_signal_text
        rows.append([str(dimension.get("display_name", label)), display, reason])
    return rows


def _scan_hard_check_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in analysis.get("hard_checks", []) or []:
        rows.append(
            [
                str(item.get("name", "")).strip() or "—",
                str(item.get("status", "")).strip() or "—",
                str(item.get("detail", "")).strip() or "—",
            ]
        )
    return rows


def _scan_factor_rows(dimension: Mapping[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for factor in dimension.get("factors", []) or []:
        rows.append(
            [
                str(factor.get("name", "")).strip() or "—",
                _pick_client_safe_line(str(factor.get("signal", "")).strip()) or "—",
                _pick_client_safe_line(str(factor.get("detail", "")).strip()) or "—",
                str(factor.get("display_score", "")).strip() or "—",
            ]
        )
    return rows


def _strong_factor_payload(factor: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(factor.get("factor_meta") or {})
    if payload:
        return payload
    factor_id = str(factor.get("factor_id", "")).strip()
    return factor_meta_payload(factor_id) if factor_id else {}


def _is_notable_strong_factor(factor: Mapping[str, Any]) -> bool:
    factor_id = str(factor.get("factor_id", "")).strip()
    signal = str(factor.get("signal", "")).strip()
    if not factor_id or not signal:
        return False
    skip_tokens = ("缺失", "无法评估", "未接入", "未披露", "数据解析异常", "未识别到明确")
    if any(token in signal for token in skip_tokens):
        return False
    payload = _strong_factor_payload(factor)
    if payload and (bool(payload.get("degraded")) or not bool(payload.get("point_in_time_ready", True))):
        return True
    flagged_tokens = (
        "背离",
        "假突破",
        "失效",
        "压力",
        "估值",
        "启动",
        "扩散",
        "龙头",
        "拥挤",
        "偏高",
        "偏弱",
        "流出",
        "现金流",
        "杠杆",
        "ROE",
        "毛利率",
        "盈利增速",
        "信用脉冲",
        "收缩",
        "景气",
        "弱于基准",
        "逆风",
        "承压",
        "风格漂移",
        "跟踪误差",
        "份额",
        "财报窗口",
        "政策事件",
        "节假日窗口",
        "商品季节性",
        "指数调整",
    )
    if any(token in signal for token in flagged_tokens):
        return True
    try:
        awarded = float(factor.get("awarded", 0) or 0)
        maximum = float(factor.get("max", 0) or 0)
    except (TypeError, ValueError):
        return False
    return maximum > 0 and abs(awarded / maximum) >= 0.45


def _strong_factor_takeaway(factor: Mapping[str, Any], *, asset_type: str) -> str:
    name = str(factor.get("name", "")).strip()
    signal = str(factor.get("signal", "")).strip()
    detail = str(factor.get("detail", "")).strip()
    payload = _strong_factor_payload(factor)
    degraded_reason = str(payload.get("degraded_reason") or payload.get("notes") or "").strip()
    if payload and (bool(payload.get("degraded")) or not bool(payload.get("point_in_time_ready", True))):
        return f"当前先按辅助观察处理，不把它单独当成主评分买点；主要原因是 {degraded_reason or 'point-in-time 边界还不够硬'}。"
    if "背离" in name or "背离" in signal:
        return "价格和动量/量能没有完全同步，动作上更适合等背离修复，再考虑追价或加仓。"
    if "假突破" in name or "假突破" in signal:
        return "关键位没有站稳时，更容易出现冲高回落或跌破后反抽，执行上先等假突破消化。"
    if "支撑结构" in name or "支撑" in signal:
        return "先确认支撑位有没有真正守住；只要支撑仍脆弱，就不适合把它当成确定性右侧。"
    if "压力位" in name or "承压" in signal:
        return "上方近端压力会直接影响反弹空间和加速概率；先确认承压位能不能被有效消化。"
    if "压缩启动" in name or "波动压缩" in name:
        return "只有压缩后再放量突破，才更像干净启动；如果已经进入情绪释放区，追价盈亏比会变差。"
    if "跟踪误差" in name or "跟踪误差" in signal:
        return "ETF/被动基金不只是方向对不对，更要看产品有没有稳定跟住标的，这直接影响持有质量。"
    if "风格漂移" in name or "风格漂移" in signal:
        return "主动基金如果偏离原本打法，后续收益来源会更难判断，所以它决定的是“能不能长期信任这个产品画像”。"
    if "份额" in name or "申赎" in name:
        return "份额变化反映的是产品层资金是否继续进出，能帮助判断这波行情是扩散还是只剩存量博弈。"
    if "宽度" in name or "扩散" in signal or "龙头" in name:
        return "这层看的是板块是不是只有个别龙头硬撑，还是已经扩散成更可持续的主线。"
    if "超额拐点" in name or "弱于基准" in signal:
        return "这层决定的是资金有没有真的轮到它；持续弱于基准时，更适合把它当观察而不是主线确认。"
    if "北向/南向" in name or "机构资金承接" in name or "流出" in signal:
        return "资金流更适合回答“有没有增量承接”；持续流出时不等于一定下跌，但会压缩短线赔率。"
    if "月度胜率" in name or "逆风" in signal:
        return "时间窗口只提供赔率偏好，不单独决定方向；样本充分的逆风月，动作上就该更保守。"
    if "财报窗口" in name or "政策事件" in name or "节假日窗口" in name or "商品季节性" in name:
        return "这类窗口更适合回答“现在为什么值得看”，但不会单独替代趋势、仓位和止损判断。"
    if "估值" in name or "PE" in signal or "PEG" in name:
        return "估值层回答的是未来兑现要有多硬；估值越贵，就越需要盈利或主线继续兑现来消化。"
    if "盈利增速" in name or "ROE" in name or "毛利率" in name:
        return "这层看的是生意本身的赚钱能力和增长质量，决定的是这波逻辑有没有基本面承托。"
    if "盈利动量" in name or "现金流质量" in name or "杠杆压力" in name:
        return "这层决定的是基本面质量够不够支撑持有，不只是今天能不能涨。"
    if "敏感度向量" in name or "景气方向" in name or "价格链条" in name or "信用脉冲" in name or "regime" in name:
        return "宏观层回答的是外部环境有没有帮它抬估值、给订单和风险偏好加分；这里逆风时，动作上就该更保守。"
    if asset_type in {"cn_etf", "cn_fund"}:
        return "对基金产品来说，这类因子主要在回答“方向之外，产品本身是否值得拿”。"
    return detail or "这层因子主要是把“为什么能看、为什么别急着追”讲清楚。"


def _strong_factor_rows_from_dimensions(
    dimensions: Mapping[str, Any],
    *,
    asset_type: str,
    max_items: int = 5,
) -> List[List[str]]:
    candidates: List[Tuple[Tuple[int, int, float, int, str], str, List[str]]] = []
    for dimension in dict(dimensions or {}).values():
        for factor in list(dict(dimension or {}).get("factors") or []):
            if not _is_notable_strong_factor(factor):
                continue
            payload = _strong_factor_payload(factor)
            family = str(payload.get("family", "")).strip()
            family_label = STRONG_FACTOR_FAMILY_LABELS.get(family, family or "未归类")
            signal = str(factor.get("signal", "")).strip() or "—"
            try:
                awarded = float(factor.get("awarded", 0) or 0)
                maximum = float(factor.get("max", 0) or 0)
            except (TypeError, ValueError):
                awarded = 0.0
                maximum = 0.0
            signal_weight = 1 if any(token in signal for token in ("背离", "假突破", "失效", "拥挤", "偏高", "偏弱", "风格漂移", "压力", "流出", "逆风", "收缩", "杠杆", "现金流", "估值")) else 0
            ratio = awarded / maximum if maximum > 0 else 0.0
            name = str(factor.get("name", "")).strip()
            candidates.append(
                (
                    (
                        STRONG_FACTOR_FAMILY_PRIORITY.get(family, 99),
                        -signal_weight,
                        -abs(ratio),
                        0 if ratio < 0 else 1,
                        name,
                    ),
                    family,
                    [
                        f"{name}（{family_label}）",
                        signal,
                        _strong_factor_takeaway(factor, asset_type=asset_type),
                    ],
                )
            )
    rows: List[List[str]] = []
    seen_names: set[str] = set()
    family_counts: Counter[str] = Counter()
    sorted_candidates = sorted(candidates, key=lambda item: item[0])

    for _, family, row in sorted_candidates:
        label = row[0]
        if label in seen_names:
            continue
        family_key = family or "__unclassified__"
        if family_counts[family_key] >= 2:
            continue
        rows.append(row)
        seen_names.add(label)
        family_counts[family_key] += 1
        if len(rows) >= max(int(max_items), 0):
            break

    if len(rows) >= max(int(max_items), 0):
        return rows

    min_fill_threshold = min(max(int(max_items), 0), 3)
    if len(rows) >= min_fill_threshold:
        return rows

    for _, _, row in sorted_candidates:
        label = row[0]
        if label in seen_names:
            continue
        rows.append(row)
        seen_names.add(label)
        if len(rows) >= max(int(max_items), 0):
            break
    return rows


def _fund_profile_sections(analysis: Mapping[str, Any]) -> List[str]:
    fund_profile = dict(analysis.get("fund_profile") or {})
    asset_type = str(analysis.get("asset_type", "")).strip()
    if not fund_profile and asset_type != "cn_etf":
        return []
    metadata = dict(analysis.get("metadata") or {})
    rows = _dimension_row_lookup(list(dict(analysis or {}).get("dimension_rows") or []))
    overview = dict(fund_profile.get("overview") or {})
    etf_snapshot = dict(fund_profile.get("etf_snapshot") or {})
    fund_factor_snapshot = dict(fund_profile.get("fund_factor_snapshot") or {})
    if asset_type == "cn_etf" and not fund_profile:
        tracked_index_name = str(
            metadata.get("tracked_index_name")
            or metadata.get("benchmark")
            or metadata.get("index_framework_label")
            or dict(metadata.get("index_topic_bundle") or {}).get("index_snapshot", {}).get("index_name")
            or analysis.get("benchmark_name")
            or ""
        ).strip()
        tracked_index_code = str(
            metadata.get("tracked_index_symbol")
            or metadata.get("index_code")
            or dict(metadata.get("index_topic_bundle") or {}).get("index_snapshot", {}).get("index_code")
            or analysis.get("benchmark_symbol")
            or ""
        ).strip()
        product_signal = str(rows.get("场内基金技术状态（ETF/基金专属）", "")).strip()
        if "；" in product_signal:
            product_signal = product_signal.split("；", 1)[0].strip()
        if product_signal.startswith("场内基金技术因子 "):
            product_signal = product_signal.replace("场内基金技术因子 ", "", 1).strip()
        overview = {
            "基金类型": "ETF / 场内",
            "业绩比较基准": tracked_index_name or "—",
            "ETF类型": "场内ETF",
            "ETF基准指数中文全称": tracked_index_name or "—",
            "ETF基准指数代码": tracked_index_code or "—",
            "ETF场内技术状态": product_signal or "—",
        }
    if etf_snapshot:
        def _is_blank(value: Any) -> bool:
            if value in ("", None):
                return True
            try:
                return bool(pd.isna(value))
            except Exception:
                return False

        def _fill_if_blank(key: str, value: Any) -> None:
            if _is_blank(value):
                return
            if _is_blank(overview.get(key)):
                overview[key] = value

        latest_close = None
        history = analysis.get("history")
        if isinstance(history, pd.DataFrame) and not history.empty:
            for column in ("收盘", "close"):
                if column in history.columns:
                    try:
                        latest_close = float(pd.to_numeric(history[column], errors="coerce").dropna().iloc[-1])
                    except Exception:
                        latest_close = None
                    if latest_close is not None:
                        break
        total_size_value = etf_snapshot.get("total_size_yi", etf_snapshot.get("total_size", ""))
        if total_size_value in ("", None) or pd.isna(total_size_value):
            try:
                total_share_raw = float(etf_snapshot.get("total_share"))
            except (TypeError, ValueError):
                total_share_raw = None
            if total_share_raw is not None and latest_close is not None:
                total_size_value = f"约{(total_share_raw * latest_close / 10000.0):.2f}亿元（按最近收盘估算）"
        share_change_text = str(etf_snapshot.get("share_change_text", "") or "").strip()
        size_change_text = str(etf_snapshot.get("size_change_text", "") or "").strip()
        share_as_of = str(etf_snapshot.get("share_as_of", "") or "").strip()
        if not share_change_text and share_as_of and etf_snapshot.get("total_share") not in ("", None):
            share_change_text = f"截至 {share_as_of} 仅有单日快照，不能据此写成净创设/净赎回"
        if not size_change_text and share_as_of and total_size_value not in ("", None, ""):
            size_change_text = f"截至 {share_as_of} 仅有单日规模口径，不能据此写成规模扩张/收缩"
        _fill_if_blank("ETF类型", etf_snapshot.get("etf_type", ""))
        _fill_if_blank("交易所", etf_snapshot.get("exchange", ""))
        _fill_if_blank("ETF基准指数中文全称", etf_snapshot.get("index_name", ""))
        _fill_if_blank("ETF基准指数代码", etf_snapshot.get("index_code", ""))
        _fill_if_blank("ETF基准指数发布机构", etf_snapshot.get("index_publisher", ""))
        _fill_if_blank("ETF基准指数调样周期", etf_snapshot.get("index_rebalance_cycle", ""))
        _fill_if_blank("ETF份额规模日期", etf_snapshot.get("share_as_of", ""))
        _fill_if_blank("ETF总份额", etf_snapshot.get("total_share_yi", etf_snapshot.get("total_share", "")))
        _fill_if_blank("ETF总规模", total_size_value)
        _fill_if_blank("ETF最近份额变化", share_change_text)
        _fill_if_blank("ETF最近规模变化", size_change_text)
    factor_trend = str(fund_factor_snapshot.get("trend_label", "") or "").strip()
    factor_momentum = str(fund_factor_snapshot.get("momentum_label", "") or "").strip()
    factor_date = str(fund_factor_snapshot.get("latest_date", "") or fund_factor_snapshot.get("trade_date", "") or "").strip()
    factor_text = ""
    if factor_trend:
        factor_text = factor_trend
        if factor_momentum:
            factor_text += f" / {factor_momentum}"
        if factor_date:
            factor_text += f"（{factor_date}）"
    if factor_text and not str(overview.get("ETF场内技术状态", "") or "").strip():
        overview["ETF场内技术状态"] = factor_text
    style = dict(fund_profile.get("style") or {})
    manager = dict(fund_profile.get("manager") or {})
    company = dict(fund_profile.get("company") or {})
    dividends = dict(fund_profile.get("dividends") or {})
    sales_ratio_snapshot = dict(fund_profile.get("sales_ratio_snapshot") or {})
    lines: List[str] = ["## 基金画像", ""]
    overview_rows = [
        ["基金类型", overview.get("基金类型", "—")],
        ["基金公司", overview.get("基金管理人", "—")],
        ["基金经理", overview.get("基金经理人", "—")],
        ["成立日期", overview.get("成立日期", overview.get("成立日期/规模", "—"))],
        ["首发规模", overview.get("首发规模", "—")],
        ["净资产规模", overview.get("净资产规模", "—")],
        ["业绩比较基准", overview.get("业绩比较基准", "—")],
    ]
    lines.extend(_table(["项目", "内容"], overview_rows))

    if asset_type == "cn_etf" or any(
        str(overview.get(key, "")).strip()
        for key in (
            "ETF类型",
            "交易所",
            "ETF基准指数中文全称",
            "ETF基准指数代码",
            "ETF总份额",
            "ETF总规模",
        )
    ):
        etf_rows = [
            ["ETF类型", overview.get("ETF类型", "—")],
            ["交易所", overview.get("交易所", "—")],
            ["跟踪指数", overview.get("ETF基准指数中文全称", overview.get("业绩比较基准", "—"))],
            ["指数代码", overview.get("ETF基准指数代码", "—")],
            ["指数发布机构", overview.get("ETF基准指数发布机构", "—")],
            ["调样周期", overview.get("ETF基准指数调样周期", "—")],
            ["场内基金技术状态", overview.get("ETF场内技术状态", "—")],
            ["最新总份额", overview.get("ETF总份额", "—")],
            ["最新总规模", overview.get("ETF总规模", overview.get("净资产规模", "—"))],
            ["份额规模日期", overview.get("ETF份额规模日期", "—")],
            ["最近份额变化", overview.get("ETF最近份额变化", "—") or "—"],
            ["最近规模变化", overview.get("ETF最近规模变化", "—") or "—"],
        ]
        lines.extend(["", "### ETF专用信息", ""])
        lines.extend(_table(["维度", "内容"], etf_rows))

    company_rows = [
        ["公司简称", company.get("short_name", "—")],
        ["所在城市", " / ".join(part for part in [company.get("province", ""), company.get("city", "")] if part) or "—"],
        ["总经理", company.get("general_manager", "—")],
        ["官网", company.get("website", "—")],
    ]
    if any(str(row[1]).strip() not in {"", "—"} for row in company_rows):
        lines.extend(["", "### 基金公司补充", ""])
        lines.extend(_table(["维度", "内容"], company_rows))
    if sales_ratio_snapshot:
        channel_rows = [
            [str(item.get("channel", "—")), f"{float(item.get('ratio')):.2f}%"]
            for item in list(sales_ratio_snapshot.get("channel_mix") or [])[:5]
            if item.get("channel") not in (None, "", []) and item.get("ratio") is not None
        ]
        if channel_rows:
            lines.extend(["", "### 公募渠道环境", ""])
            lines.extend(
                _table(
                    ["渠道", "保有占比"],
                    [["统计年度", sales_ratio_snapshot.get("latest_year", "—")], ["主导渠道", sales_ratio_snapshot.get("lead_channel", "—")], *channel_rows],
                )
            )
            summary = str(sales_ratio_snapshot.get("summary", "")).strip()
            if summary:
                lines.extend(["", f"- {summary}"])
    dividend_rows = list(dividends.get("rows") or [])
    if dividend_rows:
        lines.extend(["", "### 分红记录", ""])
        lines.extend(
            _table(
                ["公告日", "除息日", "派息日", "每份分红", "进度"],
                [
                    [
                        item.get("ann_date", "—"),
                        item.get("ex_date", "—"),
                        item.get("pay_date", "—"),
                        f"{float(item.get('div_cash')):.4f}" if item.get("div_cash") is not None else "—",
                        item.get("progress", "—"),
                    ]
                    for item in dividend_rows[:3]
                ],
            )
        )

    style_rows = [
        ["风格标签", " / ".join(style.get("tags") or []) or "—"],
        ["仓位画像", style.get("positioning", "—")],
        ["选股方式", style.get("selection", "—")],
        ["风格一致性", style.get("consistency", "—")],
        ["经理画像", _manager_profile_text(manager, overview)],
    ]
    lines.extend(["", "## 基金经理风格分析", ""])
    lines.extend(_table(["维度", "说明"], style_rows))
    taxonomy = dict(style.get("taxonomy") or {})
    if taxonomy:
        lines.extend(["", "### 标准化分类", ""])
        lines.extend(_table(["维度", "结果"], taxonomy_rows(taxonomy)))
        lines.extend(["", f"- {taxonomy.get('summary', '当前分类只作为产品标签，不替代净值、持仓和交易判断。')}"])

    asset_mix = list(fund_profile.get("asset_allocation") or fund_profile.get("asset_mix") or [])
    if asset_mix:
        mix_rows = [
            [
                str(item.get("资产类型", "—")),
                (
                    f"{ratio_value:.2f}%"
                    if isinstance(
                        (
                            ratio_value := next(
                                (
                                    float(value)
                                    for value in (
                                        item.get("仓位占比"),
                                        item.get("占总资产比例"),
                                        item.get("占净值比例"),
                                    )
                                    if value is not None and str(value).strip() != ""
                                ),
                                float("nan"),
                            )
                        ),
                        float,
                    )
                    and ratio_value == ratio_value
                    else "—"
                ),
            ]
            for item in asset_mix[:6]
        ]
        lines.extend(["", "### 资产配置", ""])
        lines.extend(_table(["资产类型", "仓位占比"], mix_rows))

    top_holdings = list(fund_profile.get("top_holdings") or [])
    if top_holdings:
        holding_rows = [
            [
                item.get("股票代码", "—"),
                _holding_name_text(item.get("股票名称"), item.get("股票代码")),
                f"{item.get('占净值比例', 0.0):.2f}%",
                item.get("季度", "—"),
            ]
            for item in top_holdings[:5]
        ]
        lines.extend(["", "### 前五大持仓", ""])
        lines.extend(_table(["代码", "名称", "占净值比例", "披露期"], holding_rows))

    industry_rows = list(fund_profile.get("industry_allocation") or [])
    if industry_rows:
        rows = [
            [
                item.get("行业类别", "—"),
                f"{item.get('占净值比例', 0.0):.2f}%",
                item.get("截止时间", "—"),
            ]
            for item in industry_rows[:5]
        ]
        lines.extend(["", "### 行业暴露", ""])
        lines.extend(_table(["行业", "占净值比例", "截止时间"], rows))
    return lines


def _delivery_tier_section(selection_context: Mapping[str, Any], *, asset_label: str) -> List[str]:
    selection = dict(selection_context or {})
    label = str(selection.get("delivery_tier_label", "未标注")).strip() or "未标注"
    observe_only = bool(selection.get("delivery_observe_only"))
    summary_only = bool(selection.get("delivery_summary_only"))
    notes = [str(item).strip() for item in (selection.get("delivery_notes") or []) if str(item).strip()]
    lines = [
        "## 交付等级",
        "",
        f"- 当前交付等级：`{label}`。",
    ]
    if observe_only:
        lines.append(f"- 这是一份 `{asset_label}` 观察优先稿，不按正式推荐稿理解。")
    else:
        lines.append(f"- 这份 `{asset_label}` 稿件按标准推荐稿编排，但执行上仍要遵守仓位和止损。")
    if summary_only:
        lines.append(f"- 当前覆盖率不足以支撑完整 `{asset_label}` 模板，本次按摘要版交付。")
    if notes:
        for item in notes[:3]:
            lines.append(f"- {item}")
    else:
        lines.append("- 当前没有额外降级或回退说明。")
    return lines


def _compact_delivery_tier_section(selection_context: Mapping[str, Any], *, asset_label: str) -> List[str]:
    selection = dict(selection_context or {})
    label = str(selection.get("delivery_tier_label", "未标注")).strip() or "未标注"
    observe_only = bool(selection.get("delivery_observe_only"))
    summary_only = bool(selection.get("delivery_summary_only"))
    notes = [str(item).strip().rstrip("。") for item in (selection.get("delivery_notes") or []) if str(item).strip()]
    lines = ["## 交付等级", ""]
    lead = f"当前交付等级：`{label}`；"
    if observe_only:
        lead += f"这份 `{asset_label}` 稿件按观察优先理解，不按正式推荐稿看。"
    else:
        lead += f"这份 `{asset_label}` 稿件按标准推荐稿编排，但执行上仍要尊重仓位和止损。"
    lines.append(f"- {lead}")
    detail_parts: List[str] = []
    if summary_only:
        detail_parts.append(f"当前覆盖率不足以支撑完整 `{asset_label}` 模板，本次按摘要版交付")
    for item in notes[:2]:
        if item and item not in detail_parts:
            detail_parts.append(item)
    if detail_parts:
        lines.append(f"- {'；'.join(detail_parts)}。")
    return lines


def _compact_pick_data_completeness_section(selection_context: Mapping[str, Any], *, asset_label: str) -> List[str]:
    selection = dict(selection_context or {})
    coverage_lines = [str(item).strip().rstrip("。") for item in (selection.get("coverage_lines") or []) if str(item).strip()]
    lines = ["## 数据完整度", ""]
    primary_parts: List[str] = []
    coverage_note = str(selection.get("coverage_note", "")).strip().rstrip("。")
    if coverage_note:
        primary_parts.append(coverage_note)
    structured_preface = _structured_coverage_preface(coverage_lines, asset_label=asset_label)
    if structured_preface:
        primary_parts.append(structured_preface.rstrip("。"))
    if coverage_lines:
        coverage_blob = " / ".join(coverage_lines[:2])
        if selection.get("coverage_total"):
            coverage_blob += f"；分母是进入完整分析的 `{selection.get('coverage_total')}` 只 {asset_label}"
        primary_parts.append(coverage_blob)
    if structured_preface:
        lines.append(f"- {structured_preface}")
        primary_parts = [part for part in primary_parts if part.rstrip("。") != structured_preface.rstrip("。")]
    if primary_parts:
        lines.append(f"- {'；'.join(primary_parts)}。")
    else:
        lines.append("- 当前没有额外覆盖率备注，默认按已进入完整分析的样本理解。")

    snapshot_parts: List[str] = []
    if selection.get("baseline_snapshot_at"):
        role = "今天首个快照版" if selection.get("is_daily_baseline") else "今天修正版"
        snapshot_parts.append(f"本次输出角色：{role}；当日首个基准快照时间是 `{selection.get('baseline_snapshot_at')}`")
    if selection.get("comparison_basis_at"):
        basis_label = _client_safe_comparison_basis_label(selection.get("comparison_basis_label", "对比基准"))
        snapshot_parts.append(f"分数变化对比的是 `{basis_label} {selection.get('comparison_basis_at')}`")
    if selection.get("model_version_warning"):
        snapshot_parts.append(str(selection.get("model_version_warning")).strip().rstrip("。"))
    if snapshot_parts:
        lines.append(f"- {'；'.join(snapshot_parts)}。")
    return lines


def _tighten_observe_client_markdown(markdown_text: str) -> str:
    text = str(markdown_text or "")
    text = text.replace("当前更像在", "现在在")
    replacements = (
        ("当前更像", "现在处在"),
        ("不等于", "不代表"),
        ("不把", "别把"),
        ("先按辅助线索看，不单独升级动作", "只当辅助线索，不单独升级动作"),
        ("先按辅助线索理解，不单独升级动作", "只当辅助线索，不单独升级动作"),
        ("先把关键位当观察点看：", "关键位先看："),
        ("先把它当历史基线看", "把它当历史基线看"),
        ("先把它理解成", "这里按"),
        ("先把它理解成 `", "这里按 `"),
        ("当前先按 `", "当前按 `"),
        ("当前仍先按 `", "当前仍按 `"),
        ("当前更该前置的是", "当前先记住的是"),
        ("先别把", "别把"),
        ("先别急着", "别急着"),
        ("先别", "别"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    text = re.sub(r"(?<!优)(?<!前)先按 `", "按 `", text)
    text = re.sub(r"(?<!优)(?<!前)先按 ([^`。；\n]+)", r"按 \1", text)
    # Final pass: reduce repeated hedge wording in observe-only reports so the
    # reader sees a decision and trigger first, not layers of defensive phrasing.
    text = re.sub(r"(?<!优)(?<!前)先按", "按", text)
    text = text.replace("不把", "别把")
    text = text.replace("不等于", "不代表")
    text = text.replace("当前更像", "更接近")
    text = re.sub(r"(指数层确认：[^。\n]+；)按(指数主链理解)", r"\1先按\2", text)
    text = text.replace("因此现在处在静态博弈", "因此当前更像静态博弈")
    text = text.replace("就别把它写成正式申赎动作", "就不把它写成正式申赎动作")
    text = re.sub(r"(?<!触发前先)(?<!触发前)按观察仓 / 暂不出手", "先按观察仓 / 暂不出手", text)
    text = re.sub(r"；{2,}", "；", text)
    return text


def _pick_delivery_summary_only(selection_context: Mapping[str, Any]) -> bool:
    return bool(dict(selection_context or {}).get("delivery_summary_only"))


def _has_zero_direct_external_coverage(selection_context: Mapping[str, Any]) -> bool:
    coverage_lines = [str(item).strip() for item in list(dict(selection_context or {}).get("coverage_lines") or [])]
    return any("高置信直接新闻覆盖 0%" in item for item in coverage_lines)


def _observe_safe_delivery_notes(notes: Sequence[Any]) -> List[str]:
    formal_note_markers = (
        "继续按正式推荐框架",
        "仍可作为正式推荐框架",
        "仍按正式推荐框架",
    )
    safe_notes = [
        str(item).strip()
        for item in notes
        if str(item).strip() and not any(marker in str(item) for marker in formal_note_markers)
    ]
    observe_note = "今天先给一个观察优先对象，不按正式买入稿理解。"
    if observe_note not in safe_notes:
        safe_notes.append(observe_note)
    return safe_notes


def _normalized_observe_packaging_context(
    selection_context: Mapping[str, Any],
    *,
    asset_label: str,
) -> Dict[str, Any]:
    normalized = dict(selection_context or {})
    if not _has_zero_direct_external_coverage(normalized):
        if bool(normalized.get("delivery_observe_only")):
            normalized["delivery_notes"] = _observe_safe_delivery_notes(normalized.get("delivery_notes") or [])
        return normalized
    normalized["delivery_observe_only"] = True
    current_label = str(normalized.get("delivery_tier_label", "")).strip()
    if not current_label or "推荐稿" in current_label:
        normalized["delivery_tier_label"] = "降级观察稿"
    notes = _observe_safe_delivery_notes(normalized.get("delivery_notes") or [])
    downgrade_note = f"高置信直接新闻覆盖仍为 0，本轮 `{asset_label}` 先按观察/候选处理，不继续沿用正式推荐包装。"
    if downgrade_note not in notes:
        notes.append(downgrade_note)
    normalized["delivery_notes"] = notes
    return normalized


def _proxy_contract_section(
    proxy_contract: Mapping[str, Any],
    *,
    winner: Optional[Mapping[str, Any]] = None,
    regime: Optional[Mapping[str, Any]] = None,
    heading: str = "## 代理信号与限制",
    emphasize: bool = True,
) -> List[str]:
    contract = dict(proxy_contract or {})
    market_flow = dict(contract.get("market_flow") or {})
    social_summary = dict(contract.get("social_sentiment") or {})
    winner_proxy = dict(dict(winner or {}).get("proxy_signals") or {})
    social_payload = dict(winner_proxy.get("social_sentiment") or {})
    social_aggregate = dict(social_payload.get("aggregate") or {})

    rows: List[List[str]] = []
    if market_flow:
        interpretation = _contextualize_market_proxy_interpretation(
            str(market_flow.get("interpretation", "")).strip(),
            dict(regime or {}),
        )
        confidence = str(market_flow.get("confidence_label", "低")).strip() or "低"
        coverage = str(market_flow.get("coverage_summary", "无有效代理样本")).strip() or "无有效代理样本"
        limitation = str(market_flow.get("limitation", "")).strip() or "当前没有额外说明。"
        downgrade = str(market_flow.get("downgrade_impact", "")).strip() or "当前没有额外降级影响说明。"
        if interpretation:
            rows.append(["市场风格代理", interpretation, f"`{confidence}` / {coverage}", limitation, downgrade])

    if social_aggregate:
        interpretation = str(social_aggregate.get("interpretation", "")).strip()
        confidence = str(social_aggregate.get("confidence_label", "低")).strip() or "低"
        limitation = str(next(iter(social_aggregate.get("limitations") or []), "")).strip() or "当前没有额外说明。"
        downgrade = str(social_aggregate.get("downgrade_impact", "")).strip() or "当前没有额外降级影响说明。"
        coverage = "日涨跌 / 5日涨跌 / 20日涨跌 / 量能比 / 趋势"
        confidence_text = (
            "参考价值高，证据层级中等"
            if confidence == "高"
            else "参考价值中等，证据层级中等"
            if confidence == "中"
            else "参考价值有限，证据层级偏低"
        )
        if interpretation:
            rows.append(["情绪代理", interpretation, f"{confidence_text} / {coverage}", limitation, downgrade])
    elif social_summary:
        covered = social_summary.get("covered", 0)
        total = social_summary.get("total", 0)
        labels = dict(social_summary.get("confidence_labels") or {})
        interpretation = f"已对 `{covered}/{total}` 只候选生成情绪代理；覆盖分布 {labels or {'低': 0}}。"
        limitation = str(social_summary.get("limitation", "")).strip() or "当前没有额外说明。"
        downgrade = str(social_summary.get("downgrade_impact", "")).strip() or "当前没有额外降级影响说明。"
        coverage = str(social_summary.get("coverage_summary", "")).strip() or f"{covered}/{total}"
        rows.append(["情绪代理", interpretation, f"证据层级中等 / {coverage}", limitation, downgrade])

    if not rows:
        return []

    lines = [heading, ""]
    lines.extend(_section_lead_lines("这段只回答哪些判断来自代理层、这些代理能信到什么程度。", emphasize=emphasize))
    lines.extend(_table(["代理层", "当前判断", "置信度/覆盖", "主要限制", "降级影响"], rows))
    return lines


def _pick_reassessment_condition(
    winner: Mapping[str, Any],
    horizon: Mapping[str, Any],
    default_text: str,
) -> str:
    action = dict(winner.get("action") or {})
    entry = str(action.get("entry", "")).strip()
    if entry:
        return _pick_client_safe_line(entry)
    fit_reason = str(horizon.get("fit_reason", "")).strip()
    if fit_reason:
        return _pick_client_safe_line(fit_reason)
    return _pick_client_safe_line(default_text)


def _entry_trigger_phrase(entry: str) -> str:
    text = str(entry).strip().rstrip("。；;,， ")
    if not text:
        return ""
    if "完整日线" in text or "补齐日线" in text:
        return "补齐日线并确认 MA20 / MA60 拐头"
    if "MA20" in text and "MA60" in text:
        return "MA20 / MA60 向上拐头"
    if "MA20" in text:
        return "MA20 向上拐头"
    if any(token in text for token in ("回踩", "回撤")):
        return "回踩关键支撑不破"
    if any(token in text for token in ("放量", "突破", "前高", "压力")):
        return "放量站上前高/压力位"
    return text


def _derived_trigger_phrases(analysis: Mapping[str, Any]) -> List[str]:
    dimensions = dict(analysis.get("dimensions") or {})
    technical = dict(dimensions.get("technical") or {})
    relative = dict(dimensions.get("relative_strength") or {})
    catalyst = dict(dimensions.get("catalyst") or {})
    technical_score = technical.get("score")
    relative_score = relative.get("score")
    catalyst_score = catalyst.get("score")
    technical_text = " ".join(
        str(part).strip()
        for part in (
            technical.get("summary", ""),
            technical.get("core_signal", ""),
            *[factor.get("signal", "") for factor in technical.get("factors", [])],
            *[factor.get("detail", "") for factor in technical.get("factors", [])],
        )
        if str(part).strip()
    )
    relative_text = " ".join(
        str(part).strip()
        for part in (
            relative.get("summary", ""),
            relative.get("core_signal", ""),
            *[factor.get("signal", "") for factor in relative.get("factors", [])],
        )
        if str(part).strip()
    )
    phrases: List[str] = []
    if technical_text and any(token in technical_text for token in ("完整日线历史", "日线历史缺失", "本地实时快照", "补齐日线")):
        phrases.append("补齐日线并确认 MA20 / MA60 拐头")
    elif technical_text or technical_score is not None:
        if any(token in technical_text for token in ("支撑", "前低", "斐波那契", "承接", "near_lower")):
            phrases.append("回踩关键支撑不破")
        if any(token in technical_text for token in ("压力", "承压", "假突破", "前高", "突破位", "阻力")):
            phrases.append("放量站上前高/压力位")
        if any(token in technical_text for token in ("MA20", "MA60", "均线")) or (technical_score is not None and int(technical_score or 0) <= 35):
            phrases.append("MA20 / MA60 向上拐头")
    if (relative_score is not None and int(relative_score or 0) <= 35) or any(
        token in relative_text for token in ("相对基准", "行业宽度", "板块涨跌幅", "跑输基准", "龙头")
    ):
        phrases.append("相对强弱转正")
    if catalyst_score is not None and int(catalyst_score or 0) <= 10 and not phrases:
        phrases.append("出现新的催化确认")
    deduped: List[str] = []
    seen = set()
    for phrase in phrases:
        key = phrase.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
        if len(deduped) >= 2:
            break
    return deduped


def _trigger_phrase_family(phrase: str) -> str:
    text = str(phrase).strip()
    if not text:
        return ""
    if "补齐日线" in text:
        return "history"
    if "MA20" in text or "MA60" in text:
        return "ma"
    if "相对强弱" in text:
        return "relative"
    if "回踩关键支撑" in text:
        return "support"
    if "前高" in text or "压力位" in text:
        return "breakout"
    if "催化" in text:
        return "catalyst"
    return text


def _compose_trigger_sentence(phrases: Sequence[str], *, buy_range: str) -> str:
    items = [str(item).strip().rstrip("。；;,， ") for item in phrases if str(item).strip()]
    if not items:
        return ""
    first = items[0]
    if not first.startswith(("先等", "等")):
        prefix_sep = " " if re.match(r"[A-Za-z0-9]", first) else ""
        first = f"先等{prefix_sep}{first}"
    parts = [first]
    if len(items) >= 2:
        second = items[1]
        if second.startswith("先等"):
            second = second[2:].strip()
        elif second.startswith("等"):
            second = second[1:].strip()
        if not second.startswith("再看"):
            second = f"再看{second}"
        parts.append(second)
    sentence = "，".join(part for part in parts if part)
    if buy_range and "暂不设" not in buy_range:
        return f"{sentence}；如果触发，再优先看 `{buy_range}` 一带的承接。"
    return f"{sentence}；触发前先别急着给精确买入价。"


def _observe_current_action_label(item: Mapping[str, Any]) -> str:
    payload = dict(item or {})
    action = dict(payload.get("action") or {})
    judgment = dict(dict(payload.get("narrative") or {}).get("judgment") or {})
    trade_state = _pick_client_safe_line(payload.get("trade_state") or judgment.get("state") or "")
    direction = _pick_client_safe_line(action.get("direction") or "")
    if _is_observe_style_text(trade_state) or _is_observe_style_text(direction):
        label = trade_state if _is_observe_style_text(trade_state) else "观察为主"
        if direction and any(marker in direction for marker in ("回避", "暂不")) and "偏回避" not in label:
            label = f"{label}（偏回避）"
        return label
    return direction or trade_state or "观察为主"


def _observe_soft_allocation_view(text: Any) -> str:
    line = _pick_client_safe_line(text)
    if not line:
        return ""
    if re.search(r"\d", line) or "%" in line or "仓位" in line or "分批" in line:
        return "先按观察仓 / 卫星仓理解，确认回来前不预设精确比例。"
    return line


def _analysis_last_close_value(analysis: Mapping[str, Any]) -> float:
    metrics = dict(analysis.get("metrics") or {})
    try:
        last_close = float(metrics.get("last_close") or 0.0)
    except (TypeError, ValueError):
        last_close = 0.0
    if last_close > 0:
        return last_close
    history = analysis.get("history")
    if isinstance(history, pd.DataFrame) and not history.empty and "close" in history.columns:
        try:
            return float(history["close"].astype(float).iloc[-1])
        except (TypeError, ValueError, IndexError):
            return 0.0
    return 0.0


def _extract_price_candidates(text: Any) -> List[float]:
    candidates: List[float] = []
    for raw in re.findall(r"([0-9]+(?:\.[0-9]+)?)", str(text or "")):
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            candidates.append(value)
    return candidates


def _extract_labeled_resistance_prices(text: Any) -> List[float]:
    values: List[float] = []
    blob = str(text or "")
    for match in re.finditer(r"(?:高点|前高|压力)[^0-9]{0,12}([0-9]+(?:\.[0-9]+)?)", blob):
        try:
            value = float(match.group(1))
        except (TypeError, ValueError):
            continue
        if value > 0:
            values.append(value)
    return values


def _analysis_ma_level(analysis: Mapping[str, Any], label: str) -> float:
    technical = dict(analysis.get("technical") or {})
    ma_system = dict(technical.get("ma_system") or {})
    ma_values = dict(ma_system.get("mas") or {})
    try:
        value = float(ma_values.get(label) or 0.0)
    except (TypeError, ValueError):
        value = 0.0
    if value > 0:
        return value
    factor = _analysis_find_factor(analysis, factor_name="均线", dimension_key="technical")
    signal_blob = " ".join(
        part
        for part in (
            str(factor.get("signal", "")).strip(),
            str(factor.get("detail", "")).strip(),
        )
        if part
    )
    match = re.search(rf"{re.escape(label)}\s*([0-9]+(?:\.[0-9]+)?)", signal_blob)
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return 0.0


def _observe_near_resistance_level(
    analysis: Mapping[str, Any],
    *,
    allow_target_fallback: bool = True,
) -> float:
    last_close = _analysis_last_close_value(analysis)
    factor = (
        _analysis_find_factor(analysis, factor_id="j1_resistance_zone")
        or _analysis_find_factor(analysis, factor_name="压力位", dimension_key="technical")
    )
    text = " ".join(
        part
        for part in (
            str(factor.get("signal", "")).strip(),
            str(factor.get("detail", "")).strip(),
        )
        if part
    )
    labeled_candidates = _extract_labeled_resistance_prices(text)
    candidates = labeled_candidates or _extract_price_candidates(text)
    if last_close > 0:
        near_candidates = [
            value
            for value in candidates
            if value > last_close * 1.001 and value <= last_close * 1.12
        ]
        if near_candidates:
            return min(near_candidates)
    if labeled_candidates:
        return min(labeled_candidates)
    for value in candidates:
        if value > 1:
            return value
    if allow_target_fallback:
        action = dict(analysis.get("action") or {})
        try:
            target_ref = float(action.get("target_ref") or 0.0)
        except (TypeError, ValueError):
            target_ref = 0.0
        if last_close > 0 and target_ref > last_close * 1.001 and target_ref <= last_close * 1.12:
            return target_ref
    return 0.0


def _observe_stock_entry_text(analysis: Mapping[str, Any]) -> str:
    action = dict(analysis.get("action") or {})
    buy_range = _safe_buy_range_text(action)
    if buy_range and "暂不设" not in buy_range:
        return f"优先看 `{buy_range}` 一带承接，不在中间价位抢。"
    ma20 = _analysis_ma_level(analysis, "MA20")
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    if ma20 > 0 and stop_ref > 0:
        return f"先看 `MA20 {ma20:.3f}` 一带回踩承接；若回踩更深，再看 `{stop_ref:.3f}` 上方能否止跌。"
    if ma20 > 0:
        return f"先看 `MA20 {ma20:.3f}` 一带的回踩承接。"
    if stop_ref > 0:
        return f"更适合靠近 `{stop_ref:.3f}` 上方企稳后再看，不在中间位置抢。"
    return "先等回踩承接或右侧确认后再看。"


def _observe_stock_tactical_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    action = dict(analysis.get("action") or {})
    near_resistance = _observe_near_resistance_level(analysis, allow_target_fallback=False)
    ma20 = _analysis_ma_level(analysis, "MA20")
    ma60 = _analysis_ma_level(analysis, "MA60")
    catalyst_score = int(dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {}).get("score") or 0)
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0

    short_line = "只做右侧确认；没出现放量突破和回踩确认前，不抢第一笔。"
    if near_resistance > 0 and stop_ref > 0:
        short_line = (
            f"只做右侧：先看 `{near_resistance:.3f}` 近端压力能否放量站上并回踩不破；"
            f"MA20 / MA60 只做波段或中线确认，不是第一笔硬门槛；"
            f"失败或失守 `{stop_ref:.3f}` 就继续观察。"
        )
    elif near_resistance > 0:
        short_line = (
            f"只做右侧：先看 `{near_resistance:.3f}` 近端压力能否放量站上并回踩不破；"
            "MA20 / MA60 只做波段或中线确认，不是第一笔硬门槛；没确认前不追。"
        )

    swing_line = "更适合等回踩承接后再分批，不在中间位置硬追。"
    buy_range = _safe_buy_range_text(action)
    if buy_range and stop_ref > 0:
        swing_line = f"更适合等 `{buy_range}` 一带承接再分批；日线失守 `{stop_ref:.3f}` 先处理。"
    elif ma20 > 0 and stop_ref > 0:
        swing_line = f"更适合等 `MA20 {ma20:.3f}` 一带企稳再分批；有效跌破 `{stop_ref:.3f}` 先认错。"
    elif stop_ref > 0:
        swing_line = f"更适合等靠近 `{stop_ref:.3f}` 的支撑区企稳再分批；跌破就先处理。"

    medium_anchor = ""
    if near_resistance > 0 and ma60 > 0:
        medium_anchor = f"`{near_resistance:.3f}` / `MA60 {ma60:.3f}`"
    elif near_resistance > 0:
        medium_anchor = f"`{near_resistance:.3f}`"
    elif ma60 > 0:
        medium_anchor = f"`MA60 {ma60:.3f}`"
    medium_line = "现在先不急着按中线仓做；要先等趋势、催化和资金确认重新站到一边。"
    if medium_anchor and catalyst_score <= 20:
        medium_line = f"现在先不急着按中线仓做；至少要等 {medium_anchor} 这一带真正站稳，并补到新增公司级催化，再考虑升级。"
    elif medium_anchor:
        medium_line = f"现在先不急着按中线仓做；至少要等 {medium_anchor} 这一带真正站稳，再考虑把观察升级成中线仓。"

    rows = [
        ["哪里建仓", _observe_stock_entry_text(analysis)],
        ["仓位节奏", _observe_stock_trial_position_text(action)],
        ["短线打法", short_line],
        ["波段打法", swing_line],
        ["中线打法", medium_line],
    ]
    evidence_hardness = _stock_observe_evidence_hardness_text(analysis)
    if evidence_hardness:
        rows.append(["证据硬度", evidence_hardness])
    data_gap = _stock_observe_data_gap_text(analysis)
    if data_gap:
        rows.append(["还差什么", data_gap])
    return rows


def _stock_observe_execution_section(analysis: Mapping[str, Any]) -> List[str]:
    if str(analysis.get("asset_type", "")).strip() != "cn_stock":
        return []
    rows = [
        ["怎么用这段", "上面的 `先看执行` 已经给了触发、建仓、仓位和止损；这里不重复参数，只补三种打法。"],
        *[
            row
            for row in _observe_stock_tactical_rows(analysis)
            if str(row[0]).strip() in {"短线打法", "波段打法", "中线打法"}
        ],
    ]
    return _summary_block_lines(
        rows,
        heading="## 执行拆解",
        lead="这段只补打法差异：短线怎么做、波段怎么做、中线还差什么，不再把首页已经给出的价格和仓位重复一遍。",
        emphasize=False,
    )


def _observe_watch_levels(analysis: Mapping[str, Any], *, qualitative_only: bool = False) -> str:
    action = dict(analysis.get("action") or {})
    asset_type = str(analysis.get("asset_type", "")).strip()
    buy_range = _safe_buy_range_text(action)
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    try:
        target_ref = float(action.get("target_ref") or 0.0)
    except (TypeError, ValueError):
        target_ref = 0.0
    trim_watch_text = _trim_watch_text(action)
    near_resistance = _observe_near_resistance_level(analysis)
    ma20 = _analysis_ma_level(analysis, "MA20")

    if asset_type == "cn_stock":
        lower_watch = ""
        if buy_range and "暂不设" not in buy_range:
            lower_watch = f"建仓先看 `{buy_range}` 一带承接"
        elif ma20 > 0:
            lower_watch = f"建仓先看 `MA20 {ma20:.3f}` 一带承接"
        elif stop_ref > 0:
            lower_watch = f"下沿先看 `{stop_ref:.3f}` 上方企稳"

        upper_watch = ""
        if near_resistance > 0:
            upper_watch = f"上沿先看 `{near_resistance:.3f}` 近端压力能否放量消化"
        elif target_ref > 0:
            upper_watch = f"上沿再看 `{target_ref:.3f}` 附近能不能放量突破"
        elif trim_watch_text:
            upper_watch = trim_watch_text

        if qualitative_only:
            if lower_watch and upper_watch:
                return "回踩先看承接；反弹先看近端压力能否被放量消化。"
            if lower_watch:
                return "先看回踩承接。"
            if upper_watch:
                return "先看近端压力能不能被放量消化。"
            return ""

        parts = [part for part in (lower_watch, upper_watch) if part]
        return "；".join(parts)

    if qualitative_only:
        if near_resistance > 0 and stop_ref > 0:
            return "下沿先看关键支撑能不能稳住；上沿先看近端压力能否放量消化。"
        if near_resistance > 0:
            return "先看近端压力能不能被放量消化。"
        if buy_range and trim_watch_text:
            return "回踩先看关键支撑承接；反弹先看前高/压力位能否放量站上。"
        if buy_range and target_ref > 0:
            return "回踩先看关键支撑承接；如果继续上行，再看前高/压力位能否放量突破。"
        if buy_range:
            return "回踩先看关键支撑承接。"
        if stop_ref > 0 and target_ref > 0:
            return "下沿先看关键支撑能不能稳住；上沿先看前高/压力位能否放量突破。"
        if stop_ref > 0:
            return "先看关键下沿能不能稳住，别先跌破支撑。"
        if trim_watch_text:
            return "先看前高/压力位附近的承压与放量情况，别把反弹空间直接想满。"
        if target_ref > 0:
            return "先看前高/压力位能不能放量突破。"
        return ""

    if near_resistance > 0 and stop_ref > 0:
        return f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；上沿先看 `{near_resistance:.3f}` 近端压力能否放量消化。"
    if near_resistance > 0:
        return f"上沿先看 `{near_resistance:.3f}` 近端压力能否放量消化。"
    if buy_range and trim_watch_text:
        return f"回踩先看 `{buy_range}` 一带的承接；{trim_watch_text}"
    if buy_range and target_ref > 0:
        return f"回踩先看 `{buy_range}` 一带的承接；如果继续上行，再看 `{target_ref:.3f}` 附近能不能放量突破。"
    if buy_range:
        return f"回踩先看 `{buy_range}` 一带的承接。"
    if stop_ref > 0 and target_ref > 0:
        return f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；上沿先看 `{target_ref:.3f}` 附近能不能放量突破。"
    if stop_ref > 0:
        return f"先看 `{stop_ref:.3f}` 上方能不能稳住，别先跌破关键下沿。"
    if trim_watch_text:
        return trim_watch_text
    if target_ref > 0:
        return f"先看 `{target_ref:.3f}` 附近能不能放量突破。"
    return ""


def _strip_precise_price_disclaimer(text: Any) -> str:
    line = _pick_client_safe_line(text)
    if not line:
        return ""
    for phrase in (
        "；触发前先别急着给精确买入价",
        "；触发前别急着给精确买入价",
        "触发前先别急着给精确买入价；",
        "触发前别急着给精确买入价；",
    ):
        line = line.replace(phrase, "")
    line = re.sub(r"[；;]{2,}", "；", line).strip("；;,， ")
    return f"{line}。" if line and not line.endswith("。") else line


def _observe_stock_trial_position_text(action: Mapping[str, Any]) -> str:
    position = _pick_client_safe_line(action.get("position", ""))
    if position and re.search(r"\d", position):
        if re.search(r"(?:≤|<=|不超过|上限)?\s*2\s*%", position):
            return (
                "没触发前 `0%`；突破回踩确认后第一笔 `2% - 3%` 试错；"
                "若次日不回落且相对强弱不丢，最多加到 `5%` 观察仓。"
            )
        return _pick_client_safe_line(
            f"没触发前 `0%`；若触发第一笔按 {position}；确认延续后再考虑加到 `5%` 观察仓。"
        )
    return (
        "没触发前 `0%`；突破回踩确认后第一笔 `2% - 3%` 试错；"
        "若次日不回落且相对强弱不丢，最多加到 `5%` 观察仓。"
    )


def _observe_stock_stop_text(action: Mapping[str, Any]) -> str:
    stop = _pick_client_safe_line(action.get("stop", ""))
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    if stop and re.search(r"\d", stop):
        return stop
    if stop_ref > 0:
        return f"失效位先看 `{stop_ref:.3f}`；跌破就先处理，不把观察稿硬扛成持仓。"
    if stop:
        return stop
    return "跌破关键支撑就先处理。"


def _observe_stock_execution_text(analysis: Mapping[str, Any]) -> str:
    action = dict(analysis.get("action") or {})
    near_resistance = _observe_near_resistance_level(analysis)
    parts = [
        _observe_stock_trial_position_text(action).rstrip("。"),
        _observe_stock_stop_text(action).rstrip("。"),
    ]
    entry_text = _observe_stock_entry_text(analysis).rstrip("。")
    if entry_text:
        parts.append(entry_text)
    if near_resistance > 0:
        parts.append(f"真正先看 `{near_resistance:.3f}` 近端压力能不能先被消化")
    return _pick_client_safe_line("；".join(part for part in parts if part))


def _stock_observe_term_translation_text(analysis: Mapping[str, Any]) -> str:
    if str(analysis.get("asset_type", "")).strip() != "cn_stock":
        return ""
    notes: List[str] = [
        "`样本置信度` 看的是历史上类似图形靠不靠谱，不是这次结论的总把握",
    ]
    proxy_text = _stock_observe_proxy_limitations_text(analysis)
    if "T+1" in proxy_text or "上一交易日直连" in proxy_text:
        notes.append("`T+1 直连` 指上一交易日的直连数据，不是今天盘中的新增资金")
    if "行业宽度/龙头确认" in proxy_text or "行业宽度 / 龙头确认" in _stock_observe_data_gap_text(analysis):
        notes.append("`行业宽度 / 龙头确认` 看的是板块是不是普涨、龙头有没有一起走强")
    return _pick_client_safe_line("；".join(notes[:3]))


def _stock_observe_has_proxy_limitations(analysis: Mapping[str, Any]) -> bool:
    dimensions = dict(analysis.get("dimensions") or {})
    for key in ("chips", "relative_strength", "catalyst", "fundamental"):
        dimension = dict(dimensions.get(key) or {})
        blobs = [
            str(dimension.get("summary", "")).strip(),
            str(dimension.get("core_signal", "")).strip(),
            *[
                " ".join(
                    str(factor.get(field, "")).strip()
                    for field in ("name", "signal", "detail")
                    if str(factor.get(field, "")).strip()
                )
                for factor in dimension.get("factors", [])
                if isinstance(factor, Mapping)
            ],
        ]
        if any("代理" in blob or "缺失" in blob or "低置信" in blob for blob in blobs if blob):
            return True
    return False


def _stock_observe_proxy_limitations_text(analysis: Mapping[str, Any]) -> str:
    dimensions = dict(analysis.get("dimensions") or {})
    chips = dict(dimensions.get("chips") or {})
    relative = dict(dimensions.get("relative_strength") or {})
    chip_factors = [dict(item or {}) for item in list(chips.get("factors") or []) if isinstance(item, Mapping)]
    relative_factors = [dict(item or {}) for item in list(relative.get("factors") or []) if isinstance(item, Mapping)]
    capital_flow_factor = next(
        (
            factor
            for factor in chip_factors
            if str(factor.get("name", "")).strip() in {"机构资金承接", "资金承接"}
        ),
        {},
    )
    real_chip_factor = next((factor for factor in chip_factors if str(factor.get("name", "")).strip() == "真实筹码分布"), {})
    real_chip_support_factors = [
        factor
        for factor in chip_factors
        if str(factor.get("name", "")).strip() in {"筹码胜率", "平均成本位置", "套牢盘压力", "筹码密集区"}
    ]
    breadth_factor = next((factor for factor in relative_factors if str(factor.get("name", "")).strip() == "行业宽度"), {})
    leader_factor = next((factor for factor in relative_factors if str(factor.get("name", "")).strip() == "龙头确认"), {})
    capital_flow_blob = " ".join(
        str(capital_flow_factor.get(field, "")).strip()
        for field in ("signal", "detail")
        if str(capital_flow_factor.get(field, "")).strip()
    )
    real_chip_blob = " ".join(
        str(real_chip_factor.get(field, "")).strip()
        for field in ("signal", "detail")
        if str(real_chip_factor.get(field, "")).strip()
    )
    real_chip_support_blob = " ".join(
        " ".join(
            str(factor.get(field, "")).strip()
            for field in ("signal", "detail")
            if str(factor.get(field, "")).strip()
        )
        for factor in real_chip_support_factors
    )
    breadth_ready = bool(breadth_factor) and "观察提示" not in str(breadth_factor.get("display_score", ""))
    leader_ready = bool(leader_factor) and "观察提示" not in str(leader_factor.get("display_score", ""))
    if ("个股主力净" in capital_flow_blob or "T+1 直连" in capital_flow_blob) and (
        "T+1 直连" in real_chip_blob
        or "上一交易日" in real_chip_blob
        or "T+1 直连" in real_chip_support_blob
        or "上一交易日" in real_chip_support_blob
    ) and breadth_ready and leader_ready:
        return "辅助层里还有行业/板块代理，但关键直连证据已经补到：个股资金流和真实筹码先按上一交易日直连数据看，行业宽度/龙头确认也已命中。"
    if _stock_observe_has_proxy_limitations(analysis):
        return "这里还有一部分是行业/板块代理，不是这只股票自己的逐笔资金，所以更适合帮我们看方向，不单独当成个股硬确认。"
    return ""


def _stock_observe_evidence_hardness_text(analysis: Mapping[str, Any]) -> str:
    if str(analysis.get("asset_type", "")).strip() != "cn_stock":
        return ""
    parts: List[str] = []
    confidence = dict(analysis.get("signal_confidence") or {})
    if confidence.get("available"):
        label = str(confidence.get("confidence_label", "未标注")).strip() or "未标注"
        score = confidence.get("confidence_score", "—")
        sample_count = confidence.get("non_overlapping_count", confidence.get("sample_count", "—"))
        win_rate = _fmt_ratio(confidence.get("win_rate_20d"))
        parts.append(
            f"这层更像历史纪律参考，不是这次结论的总把握：样本置信度 `{label}`（{score}/100）、"
            f"非重叠样本 {sample_count} 个、20日胜率约 {win_rate}"
        )
    elif confidence:
        reason = str(confidence.get("reason", "当前不给历史样本置信度。")).strip()
        parts.append(f"这层更像历史纪律参考，不是这次结论的总把握：{reason}")
    proxy_text = _stock_observe_proxy_limitations_text(analysis)
    if proxy_text:
        parts.append(proxy_text)
    return _pick_client_safe_line("；".join(part for part in parts if part))


def _stock_observe_data_gap_text(analysis: Mapping[str, Any]) -> str:
    if str(analysis.get("asset_type", "")).strip() != "cn_stock":
        return ""
    gaps: List[str] = []
    provenance = build_analysis_provenance(analysis)
    intraday = dict(analysis.get("intraday") or {})
    intraday_as_of = str(provenance.get("intraday_as_of", "")).strip()
    if not bool(intraday.get("enabled")) or not intraday_as_of or intraday_as_of == "未启用":
        gaps.append("分钟级盘口未启用")
    elif bool(intraday.get("fallback_mode")):
        gaps.append("分钟级盘口退化为日K/实时快照")
    news_mode = str(provenance.get("news_mode", "")).strip()
    if news_mode and news_mode != "live":
        gaps.append(f"实时情报仍是 `{news_mode}`")
    dimensions = dict(analysis.get("dimensions") or {})
    technical = dict(dimensions.get("technical") or {})
    technical_factors = [dict(item or {}) for item in list(technical.get("factors") or []) if isinstance(item, Mapping)]
    stk_factor = next((item for item in technical_factors if str(item.get("factor_id", "")) == "j1_stk_factor_pro" or "stk_factor_pro" in str(item.get("signal", ""))), {})
    if stk_factor and any(token in " ".join(str(stk_factor.get(key, "")) for key in ("signal", "detail")) for token in ("缺失", "不可用", "未返回")):
        gaps.append("stk_factor_pro 未命中 fresh")
    relative = dict(dimensions.get("relative_strength") or {})
    relative_factors = [dict(item or {}) for item in list(relative.get("factors") or []) if isinstance(item, Mapping)]
    relative_gap_blob = " ".join(
        str(part).strip()
        for part in (
            relative.get("summary", ""),
            *[
                " ".join(
                    str(factor.get(field, "")).strip()
                    for field in ("signal", "detail", "display_score")
                    if str(factor.get(field, "")).strip()
                )
                for factor in relative_factors
                if str(factor.get("name", "")).strip() in {"行业宽度", "龙头确认"}
            ],
        )
        if str(part).strip()
    )
    if any(token in relative_gap_blob for token in ("缺失", "低置信代理", "暂不判断", "观察提示")):
        gaps.append("行业宽度 / 龙头确认未补齐")
    chips = dict(dimensions.get("chips") or {})
    chip_factors = [dict(item or {}) for item in list(chips.get("factors") or []) if isinstance(item, Mapping)]
    capital_flow_factor = next(
        (
            factor
            for factor in chip_factors
            if str(factor.get("name", "")).strip() in {"机构资金承接", "资金承接"}
        ),
        {},
    )
    real_chip_factor = next((factor for factor in chip_factors if str(factor.get("name", "")).strip() == "真实筹码分布"), {})
    capital_flow_blob = " ".join(
        str(capital_flow_factor.get(field, "")).strip()
        for field in ("signal", "detail", "display_score")
        if str(capital_flow_factor.get(field, "")).strip()
    )
    real_chip_blob = " ".join(
        str(real_chip_factor.get(field, "")).strip()
        for field in ("signal", "detail", "display_score")
        if str(real_chip_factor.get(field, "")).strip()
    )
    capital_flow_gap = bool(capital_flow_blob) and "个股主力净" not in capital_flow_blob and "T+1 直连" not in capital_flow_blob
    real_chip_gap = bool(real_chip_blob) and (
        any(token in real_chip_blob for token in ("缺失", "blocked", "empty"))
        or ("非当期" in real_chip_blob and "T+1 直连" not in real_chip_blob and "上一交易日" not in real_chip_blob)
    )
    if capital_flow_gap or real_chip_gap:
        gaps.append("个股级资金/筹码确认仍不足")
    deduped: List[str] = []
    seen = set()
    for item in gaps:
        key = item.strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(key)
    if not deduped:
        return ""
    return "离更高置信还差：" + "；".join(deduped[:5]) + "。"


def _observe_trigger_condition(
    analysis: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    default_text: str,
    qualitative_only: bool = False,
) -> str:
    action = dict(analysis.get("action") or {})
    entry = _pick_client_safe_line(action.get("entry", ""))
    buy_range = _safe_buy_range_text(action)
    watch_buy_range = "" if qualitative_only else buy_range
    entry_phrase = _entry_trigger_phrase(entry)
    derived_phrases = _derived_trigger_phrases(analysis)
    strategy_upgrade_text = _strategy_background_upgrade_text(analysis)
    asset_type = str(analysis.get("asset_type", "")).strip()
    near_resistance = _observe_near_resistance_level(analysis)
    breakout_phrase = (
        f"近端压力 `{near_resistance:.3f}` 放量站上并回踩不破"
        if asset_type == "cn_stock" and near_resistance > 0
        else ""
    )

    def _finalize(text: str, *, append_history_trigger: bool = True) -> str:
        line = _pick_client_safe_line(text)
        if strategy_upgrade_text and strategy_upgrade_text not in line:
            line = _append_sentence(line, strategy_upgrade_text)
        if append_history_trigger:
            return append_technical_trigger_text(line, analysis.get("history"))
        return line

    if entry:
        if breakout_phrase:
            return _finalize(
                f"短线只看{breakout_phrase}；MA20 / MA60 只作为波段或中线加仓确认，不是第一笔必要条件。",
                append_history_trigger=False,
            )
        phrases: List[str] = []
        if entry_phrase:
            phrases.append(entry_phrase)
        elif entry:
            phrases.append(entry.rstrip("。；;,， "))
        seen_families = {_trigger_phrase_family(item) for item in phrases if _trigger_phrase_family(item)}
        for phrase in derived_phrases:
            family = _trigger_phrase_family(phrase)
            if not phrase or phrase in phrases or (family and family in seen_families):
                continue
            phrases.append(phrase)
            if family:
                seen_families.add(family)
            if len(phrases) >= 2:
                break
        return _finalize(_compose_trigger_sentence(phrases, buy_range=watch_buy_range))
    if buy_range and "暂不设" not in buy_range and not qualitative_only:
        return _finalize(f"先等价格回到 `{buy_range}` 一带并确认承接，再决定要不要动。")
    if derived_phrases:
        return _finalize(_compose_trigger_sentence(derived_phrases, buy_range=watch_buy_range))
    constraint = _analysis_constraint_hint(analysis)
    if constraint:
        return _finalize(constraint)
    fit_reason = str(horizon.get("fit_reason", "")).strip()
    if fit_reason:
        return _finalize(f"先等{fit_reason}，再决定要不要升级成可执行方案。")
    return _finalize(default_text)


def _analysis_section_takeaway(analysis: Mapping[str, Any], bucket: str) -> str:
    conclusion = str(analysis.get("conclusion", "")).strip()
    takeaway = conclusion or _bucket_summary_text(bucket, analysis)
    if bucket != "正式推荐":
        hint = _analysis_constraint_hint(analysis)
        if hint:
            takeaway = _append_sentence(takeaway, hint)
    return _pick_client_safe_line(takeaway)


def _pick_upgrade_lines(analysis: Mapping[str, Any]) -> List[str]:
    why_text = _pick_client_safe_line(_decision_gate_explanation(analysis))
    technical_signal_text = compact_technical_signal_text(analysis.get("history"))
    if technical_signal_text and technical_signal_text not in why_text:
        why_text = f"{why_text} {technical_signal_text}".strip()
    trigger_text = append_technical_trigger_text(_pick_client_safe_line(_primary_upgrade_trigger(analysis)), analysis.get("history"))
    lines: List[str] = []
    strategy_upgrade_text = _strategy_background_upgrade_text(analysis)
    if why_text:
        lines.append(f"- 为什么还不升级：{why_text}")
    if strategy_upgrade_text:
        lines.append(f"- 后台置信度约束：{strategy_upgrade_text}")
    if trigger_text:
        lines.append(f"- 升级条件：{trigger_text}")
    return lines


def _observe_pick_action_rows(
    winner: Mapping[str, Any],
    horizon: Mapping[str, Any],
    handoff: Mapping[str, Any],
    playbook: Mapping[str, Any],
    *,
    default_reassessment: str,
    default_trigger: str,
) -> List[List[str]]:
    asset_type = str(winner.get("asset_type", "")).strip()
    rows = [
        ["当前动作", _observe_current_action_label(winner)],
        ["持有周期", horizon.get("label", "未单独标注")],
    ]
    trading_role_label = str(playbook.get("trading_role_label") or "").strip()
    trading_position_label = str(playbook.get("trading_position_label") or "").strip()
    trading_role_summary = _pick_client_safe_line(playbook.get("trading_role_summary", ""))
    if trading_role_label and trading_position_label:
        trading_text = f"`{trading_role_label}` / `{trading_position_label}`"
        if trading_role_summary:
            trading_text += f"：{trading_role_summary}"
        rows.append(["仓位身份", trading_text])
    allocation_view = str(horizon.get("allocation_view") or playbook.get("allocation", "")).strip()
    if allocation_view:
        rows.append(["配置视角", _observe_soft_allocation_view(allocation_view)])
    trading_view = str(horizon.get("trading_view") or playbook.get("trend", "")).strip()
    if trading_view:
        rows.append(["交易视角", _pick_client_safe_line(trading_view)])
    watch_levels = _observe_watch_levels(winner, qualitative_only=asset_type not in {"cn_stock", "cn_etf", "cn_fund"})
    rows.extend(
        [
            ["触发条件", _observe_trigger_condition(winner, horizon, default_text=default_trigger, qualitative_only=True)],
            *_present_action_row("先看什么", watch_levels),
            [
                "怎么用这段",
                (
                    "上面的 `先看执行` 已经给了观察位、仓位和失效位；这里不重复挂单细节，只补升级条件。"
                    if asset_type == "cn_etf"
                    else "上面的 `先看执行` 已经给了是否申赎、仓位和重评边界；这里不重复模板话术，只补升级条件。"
                    if asset_type == "cn_fund"
                    else "上面的 `先看执行` 已经给了关键位和动作边界；这里不重复挂单细节，只补升级条件。"
                ),
            ],
            ["重新评估条件", _pick_reassessment_condition(winner, horizon, default_reassessment)],
            ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易/申赎窗口理解。"))],
        ]
    )
    return rows


def _etf_pick_term_translation_lines(
    winner: Mapping[str, Any],
    selection_context: Mapping[str, Any],
) -> List[str]:
    lines = [
        "`跟踪指数`：这只 ETF 背后在跟哪条主线；它说明方向归属，不等于这只 ETF 自己已经最强。",
        "`份额变化`：更像场内申赎强弱；它能帮我们看资金有没有回流，不等于当天一定涨跌。",
    ]
    if _has_zero_direct_external_coverage(selection_context):
        lines.append("`高置信直连情报 / 结构化披露`：前者更接近能直接回源的一手证据，后者更适合补背景；直连偏少时，先别把它当强催化。")
    else:
        lines.append("`T+1 直连`：上一交易日的场内直连数据，不是今天盘中每一分钟的即时资金流。")
    return lines[:3]


def _summary_join_text(
    items: Sequence[Any],
    *,
    max_items: int = 2,
    fallback: str = "",
) -> str:
    cleaned: List[str] = []
    seen: set[str] = set()
    for item in items:
        line = _pick_client_safe_line(item).strip()
        if not line:
            continue
        line = line.rstrip("。；;,， ")
        fingerprint = _reason_fingerprint(line)
        if fingerprint and fingerprint in seen:
            continue
        if fingerprint:
            seen.add(fingerprint)
        cleaned.append(line)
        if len(cleaned) >= max_items:
            break
    if cleaned:
        return "；".join(cleaned)
    return _pick_client_safe_line(fallback)


def _summary_confidence_text(
    item: Mapping[str, Any],
    *,
    selection_context: Optional[Mapping[str, Any]] = None,
) -> str:
    confidence = dict(item.get("signal_confidence") or {})
    if confidence:
        if confidence.get("available"):
            label = str(confidence.get("confidence_label", "未标注")).strip() or "未标注"
            score = confidence.get("confidence_score", "—")
            sample_count = confidence.get("non_overlapping_count", confidence.get("sample_count", "—"))
            return f"{label}（{score}/100；非重叠样本 {sample_count} 个）"
        reason = str(confidence.get("reason", "当前不给历史样本置信度。")).strip()
        return _pick_client_safe_line(f"偏保守；{reason}")

    context = dict(selection_context or {})
    delivery_label = str(context.get("delivery_tier_label", "")).strip()
    coverage_note = str(context.get("coverage_note", "")).strip().rstrip("。")
    if delivery_label:
        if bool(context.get("delivery_observe_only")) or "降级" in delivery_label:
            detail = coverage_note or "先按观察和触发条件理解"
            return f"偏保守（{delivery_label}；{detail}）"
        detail = coverage_note or "当前按完整分析样本理解"
        return f"中等（{delivery_label}；{detail}）"

    winner_proxy = dict(item.get("proxy_signals") or {})
    social_aggregate = dict(dict(winner_proxy.get("social_sentiment") or {}).get("aggregate") or {})
    if social_aggregate:
        label = str(social_aggregate.get("confidence_label", "低")).strip() or "低"
        return f"未单独量化；情绪/行为代理当前为 `{label}`，但不等于整体胜率"

    return "未单独量化；按当前证据、覆盖率和代理信号理解"


def _summary_empty_position_action_text(
    item: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    default_trigger: str,
    observe_only: bool = False,
) -> str:
    action = dict(item.get("action") or {})
    direction = _pick_client_safe_line(action.get("direction", ""))
    entry = _pick_client_safe_line(action.get("entry", ""))
    buy_range = _safe_buy_range_text(action)
    position = _pick_client_safe_line(action.get("position", ""))
    asset_type = str(item.get("asset_type", "")).strip()
    if observe_only:
        trigger = _strip_precise_price_disclaimer(
            _observe_trigger_condition(item, horizon, default_text=default_trigger, qualitative_only=asset_type != "cn_stock")
        ).rstrip("。；;,， ")
        watch_levels = _observe_watch_levels(item, qualitative_only=asset_type != "cn_stock").rstrip("。；;,， ")
        if asset_type == "cn_stock":
            position_text = _observe_stock_trial_position_text(action).rstrip("。；;,， ")
            stop_text = _observe_stock_stop_text(action).rstrip("。；;,， ")
            parts = [trigger]
            if watch_levels:
                parts.append(f"关键位先看 {watch_levels}")
            parts.extend(part for part in (position_text, stop_text) if part)
            return _pick_client_safe_line("；".join(part for part in parts if part))
        if watch_levels:
            return _pick_client_safe_line(f"{trigger}；先把 `{watch_levels}` 当观察点，触发前不新开仓，也不预设机械仓位和止损。")
        return _pick_client_safe_line(f"{trigger}；触发前不新开仓，也不预设机械仓位和止损。")
    non_action_blob = " / ".join(part for part in (direction, entry, position) if part)
    non_action_markers = ("暂不出手", "观察", "回避", "等待更好窗口", "先别急着动手")
    if any(marker in non_action_blob for marker in non_action_markers):
        trigger = _observe_trigger_condition(item, horizon, default_text=default_trigger).rstrip("。；;,， ")
        return _pick_client_safe_line(f"{trigger}；触发前先不新开仓。")

    parts: List[str] = []
    if entry:
        parts.append(entry.rstrip("。；;,， "))
    elif buy_range:
        parts.append(f"优先看 `{buy_range}` 一带的承接")
    else:
        parts.append(_observe_trigger_condition(item, horizon, default_text=default_trigger))
    if buy_range and buy_range not in " ".join(parts):
        parts.append(f"更具体先看 `{buy_range}` 一带")
    if position:
        parts.append(f"首次仓位按 `{position}`")
    return _pick_client_safe_line("；".join(part for part in parts if part))


def _summary_holder_action_text(
    item: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    default_text: str,
) -> str:
    action = dict(item.get("action") or {})
    narrative = dict(item.get("narrative") or {})
    judgment = dict(narrative.get("judgment") or {})
    trade_state = _pick_client_safe_line(item.get("trade_state") or judgment.get("state", ""))
    target = _pick_client_safe_line(action.get("target", ""))
    stop = _pick_client_safe_line(action.get("stop", ""))
    scaling_plan = _pick_client_safe_line(action.get("scaling_plan", ""))
    trim_watch_text = _trim_watch_text(action)

    parts: List[str] = []
    if trade_state:
        parts.append(f"先按 `{trade_state}` 管")
    if trim_watch_text:
        parts.append(trim_watch_text)
    elif target:
        parts.append(f"目标先看 `{target}`")
    if scaling_plan and not any(marker in scaling_plan for marker in ("暂不", "观察")):
        if scaling_plan.startswith(("先按 `", "按 `")):
            parts.append(f"加仓{re.sub(r'^先按', '按', scaling_plan, count=1)}")
        elif "`" in scaling_plan:
            parts.append(f"加仓只按 {scaling_plan}")
        else:
            parts.append(f"加仓只按 `{scaling_plan}`")
    if stop:
        parts.append(f"止损参考 `{stop}`")
    elif horizon.get("misfit_reason"):
        parts.append(_pick_client_safe_line(horizon.get("misfit_reason")))
    if not parts:
        parts.append(_pick_client_safe_line(default_text))
    return _pick_client_safe_line("；".join(part for part in parts if part))


def _single_asset_exec_summary_rows(
    item: Mapping[str, Any],
    horizon: Mapping[str, Any],
    handoff: Mapping[str, Any],
    *,
    selection_context: Optional[Mapping[str, Any]] = None,
    theme_playbook: Optional[Mapping[str, Any]] = None,
    status_label: str = "",
    default_trigger: str,
    default_holder_text: str,
) -> List[List[str]]:
    action = dict(item.get("action") or {})
    narrative = dict(item.get("narrative") or {})
    direction = _pick_client_safe_line(action.get("direction", ""))
    context = dict(selection_context or {})
    delivery_label = str(context.get("delivery_tier_label", "")).strip()
    current_action, observe_conflict = _current_action_summary(
        item,
        selection_context=selection_context,
        status_label=status_label,
    )
    positives = _merge_reason_lines(
        list(item.get("positives") or []) + list(narrative.get("positives") or []),
        _top_dimension_reasons(item, top_n=3),
        max_items=5,
    )
    caution_fallbacks: List[str] = []
    entry_text = _pick_client_safe_line(action.get("entry", ""))
    if observe_conflict and str(item.get("asset_type", "")).strip() == "cn_stock":
        entry_text = _observe_lead_trigger_text(item, horizon, default_trigger=default_trigger)
    if entry_text:
        caution_fallbacks.append(f"执行层仍要求等待确认：{entry_text}")
    risk_summary = _pick_client_safe_line(dict(item.get("dimensions", {}).get("risk", {}) or {}).get("summary", ""))
    if risk_summary:
        caution_fallbacks.append(f"风险特征提示：{risk_summary}")
    misfit_reason = _pick_client_safe_line(horizon.get("misfit_reason", ""))
    if misfit_reason:
        caution_fallbacks.append(f"当前打法约束：{misfit_reason}")
    cautions = _merge_reason_lines(
        list(item.get("cautions") or []) + list(narrative.get("cautions") or []),
        caution_fallbacks,
        max_items=5,
    )

    rows: List[List[str]] = [["当前建议", current_action]]
    if observe_conflict and direction and direction != current_action and not any(marker in direction for marker in ("回避", "暂不")):
        rows.append(["方向偏向", direction])
    if delivery_label:
        rows.append(["交付等级", delivery_label])
    elif status_label:
        rows.append(["结论强度", status_label])
    rows.extend(
        [
            ["置信度", _summary_confidence_text(item, selection_context=selection_context)],
            ["适用周期", horizon.get("label", "未单独标注")],
            ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易/申赎窗口理解。"))],
        ]
    )
    strategy_background_text = _strategy_background_summary_text(item)
    if strategy_background_text:
        rows.append(["后台置信度", strategy_background_text])
    rows.extend(_theme_playbook_summary_rows(theme_playbook))
    if observe_conflict or current_action == "观察为主" or status_label == "观察为主":
        rows.append(
            [
                "观察重点",
                _summary_empty_position_action_text(
                    item,
                    horizon,
                    default_trigger=default_trigger,
                    observe_only=observe_conflict,
                ),
            ]
        )
    else:
        rows.extend(
            [
                ["空仓怎么做", _summary_empty_position_action_text(item, horizon, default_trigger=default_trigger)],
                ["持仓怎么做", _summary_holder_action_text(item, horizon, default_text=default_holder_text)],
                ["首次仓位", _pick_client_safe_line(action.get("position", "小仓位分批"))],
            ]
        )
        if action.get("max_portfolio_exposure"):
            rows.append(["仓位上限", _pick_client_safe_line(action.get("max_portfolio_exposure"))])
        if handoff.get("summary"):
            rows.append(["组合前提", _pick_client_safe_line(handoff.get("summary"))])
    rows.extend(
        [
            [
                "主要利好",
                _summary_join_text(
                    positives,
                    max_items=2,
                    fallback="当前更大的优势在方向和结构还没完全走坏。",
                ),
            ],
            [
                "主要利空",
                _summary_join_text(
                    cautions,
                    max_items=2,
                    fallback=default_holder_text,
                ),
            ],
        ]
    )
    return rows


def _summary_block_lines(
    rows: Sequence[Sequence[str]],
    *,
    heading: str,
    lead: str,
    emphasize: bool = True,
) -> List[str]:
    if not rows:
        return []
    lines = [heading, ""]
    lines.extend(_section_lead_lines(lead, emphasize=emphasize))
    lines.extend(_table(["项目", "建议"], rows))
    return lines


def _current_action_summary(
    item: Mapping[str, Any],
    *,
    selection_context: Optional[Mapping[str, Any]] = None,
    status_label: str = "",
) -> Tuple[str, bool]:
    action = dict(item.get("action") or {})
    narrative = dict(item.get("narrative") or {})
    judgment = dict(narrative.get("judgment") or {})
    trade_state = _pick_client_safe_line(item.get("trade_state") or judgment.get("state", ""))
    direction = _pick_client_safe_line(action.get("direction", ""))
    context = dict(selection_context or {})
    delivery_label = str(context.get("delivery_tier_label", "")).strip()
    delivery_observe_only = bool(context.get("delivery_observe_only")) or "观察稿" in delivery_label or "降级" in delivery_label
    observe_conflict = delivery_observe_only or _is_observe_style_text(trade_state) or (
        _is_observe_style_text(direction) and direction != trade_state
    )
    if observe_conflict:
        current_action = trade_state or direction or status_label or "观察为主"
    else:
        current_action = _summary_join_text(
            [direction, trade_state],
            max_items=2,
            fallback=status_label or "观察为主",
        )
    if observe_conflict and direction and direction != current_action:
        if any(marker in direction for marker in ("回避", "暂不")) and "偏回避" not in current_action:
            current_action = f"{current_action}（偏回避）"
    return current_action, observe_conflict


def _observe_lead_trigger_text(
    item: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    default_trigger: str,
) -> str:
    asset_type = str(item.get("asset_type", "")).strip()
    if asset_type == "cn_stock":
        base = _strip_precise_price_disclaimer(
            _observe_trigger_condition(
                item,
                horizon,
                default_text=default_trigger,
                qualitative_only=False,
            )
        )
        return base
    base = _observe_trigger_condition(
        item,
        horizon,
        default_text=default_trigger,
        qualitative_only=True,
    )
    if asset_type != "cn_etf":
        return base
    watch_levels = _observe_watch_levels(item, qualitative_only=False)
    if watch_levels and re.search(r"\d", watch_levels):
        return _pick_client_safe_line(f"{base.rstrip('。；;,， ')}；关键观察位：{watch_levels}")
    return base


def _observe_lead_position_text(item: Mapping[str, Any], action: Mapping[str, Any]) -> str:
    asset_type = str(item.get("asset_type", "")).strip()
    if asset_type == "cn_stock":
        return _observe_stock_trial_position_text(action)
    position = _pick_client_safe_line(action.get("position", ""))
    if asset_type == "cn_etf" and position and re.search(r"\d", position):
        observe_prefix = (
            "触发前按观察仓 / 暂不出手。"
            if any(mark in position for mark in ("，", ","))
            else "触发前先按观察仓 / 暂不出手。"
        )
        return _pick_client_safe_line(f"若触发也只按 {position}；{observe_prefix}")
    if position and not re.search(r"\d", position):
        return position
    if any(marker in position for marker in ("暂不", "观察")):
        return position
    return "先按观察仓 / 暂不出手"


def _observe_lead_stop_text(item: Mapping[str, Any], action: Mapping[str, Any]) -> str:
    asset_type = str(item.get("asset_type", "")).strip()
    if asset_type == "cn_stock":
        return _observe_stock_stop_text(action)
    stop = _pick_client_safe_line(action.get("stop", ""))
    if asset_type == "cn_etf" and stop and re.search(r"\d", stop):
        return stop
    if stop and not re.search(r"\d", stop):
        return stop
    return "关键支撑失效就重评，不先给机械止损位。"


def _first_screen_execution_rows(
    item: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    selection_context: Optional[Mapping[str, Any]] = None,
    status_label: str = "",
    default_trigger: str,
) -> List[List[str]]:
    action = dict(item.get("action") or {})
    current_action, observe_conflict = _current_action_summary(
        item,
        selection_context=selection_context,
        status_label=status_label,
    )
    rows: List[List[str]] = [
        ["看不看", current_action],
        [
            "怎么触发",
            _observe_lead_trigger_text(item, horizon, default_trigger=default_trigger)
            if observe_conflict
            else _observe_trigger_condition(
                item,
                horizon,
                default_text=default_trigger,
                qualitative_only=False,
            ),
        ],
    ]
    if observe_conflict:
        if str(item.get("asset_type", "")).strip() == "cn_stock":
            rows.append(["哪里建仓", _observe_stock_entry_text(item)])
            evidence_hardness = _stock_observe_evidence_hardness_text(item)
            data_gap = _stock_observe_data_gap_text(item)
        rows.extend(
            [
                ["多大仓位", _observe_lead_position_text(item, action)],
                ["哪里止损", _observe_lead_stop_text(item, action)],
            ]
        )
        if str(item.get("asset_type", "")).strip() == "cn_stock" and evidence_hardness:
            rows.append(["证据硬度", evidence_hardness])
        if str(item.get("asset_type", "")).strip() == "cn_stock" and data_gap:
            rows.append(["还差什么", data_gap])
        return rows
    rows.extend(
        [
            ["多大仓位", _pick_client_safe_line(action.get("position", "小仓位分批"))],
            ["哪里止损", _pick_client_safe_line(action.get("stop", "重新跌破关键支撑就处理"))],
        ]
    )
    return rows


def _first_screen_execution_block(
    item: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    selection_context: Optional[Mapping[str, Any]] = None,
    status_label: str = "",
    default_trigger: str,
    heading: str = "## 先看执行",
) -> str:
    rows = _first_screen_execution_rows(
        item,
        horizon,
        selection_context=selection_context,
        status_label=status_label,
        default_trigger=default_trigger,
    )
    if not rows:
        return ""
    lead = "如果你只看最上面，先看这几件事：看不看、怎么触发、多大仓位、哪里止损。后面都在解释为什么。"
    if any(str(row[0]).strip() == "哪里建仓" for row in rows):
        lead = "如果你只看最上面，先看这几件事：看不看、怎么触发、哪里建仓、多大仓位、哪里止损。后面都在解释为什么。"
    return "\n".join(
        _summary_block_lines(
            rows,
            heading=heading,
            lead=lead,
            emphasize=False,
        )
    ).rstrip()


def _briefing_position_summary_text(action_lines: Sequence[str]) -> str:
    for raw in list(action_lines or []):
        text = _pick_client_safe_line(raw)
        if not text:
            continue
        if any(token in text for token in ("小仓", "分批", "仓位", "打满", "节奏", "试仓", "预演")):
            return text
    return "先按常规节奏 / 小仓分批，不在晨报里默认打满。"


def _briefing_stop_summary_text(
    verification_rows: Sequence[Sequence[Any]],
    alerts: Sequence[str],
    theme_rows: Sequence[Sequence[Any]],
) -> str:
    if verification_rows:
        row = list(verification_rows[0] or [])
        label = str(row[1] if len(row) > 1 else "").strip() or "主线验证"
        criterion = str(row[2] if len(row) > 2 else "").strip()
        fail_case = str(row[4] if len(row) > 4 else "").strip() or "验证不成立"
        if criterion:
            return f"`{label}` 若不能 `{criterion}`，先按 `{fail_case}` 处理。"
        return f"`{label}` 一旦验证不成立，就先按 `{fail_case}` 处理。"
    for raw in list(alerts or []):
        text = _pick_client_safe_line(raw)
        if text:
            return text
    if theme_rows:
        row = list(theme_rows[0] or [])
        direction = str(row[0] if len(row) > 0 else "").strip() or "当前主线"
        risk = str(row[4] if len(row) > 4 else "").strip() or "风险开始兑现"
        return f"如果 `{direction}` 的验证继续走弱，或 `{risk}` 开始兑现，就先降回防守。"
    return "主线验证不成立就先降回防守，不因为晨报结论硬扛。"


def _briefing_usage_lines(
    headline_lines: Sequence[str],
    action_lines: Sequence[str],
    verification_rows: Sequence[Sequence[Any]],
    theme_rows: Sequence[Sequence[Any]],
    *,
    strict_source_consistency: bool = False,
) -> List[str]:
    if strict_source_consistency:
        mirrored_actions: List[str] = []
        seen: set[str] = set()
        for raw in list(action_lines or []):
            text = _pick_client_safe_line(raw)
            normalized = re.sub(r"\s+", " ", str(text or "")).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            mirrored_actions.append(normalized)
            if len(mirrored_actions) >= 3:
                break
        if mirrored_actions:
            return mirrored_actions

    lines: List[str] = []
    if headline_lines:
        lines.append("先用上面的 `先看执行` 判断今天更偏进攻、观察还是防守，再决定要不要继续往下看。")
    if verification_rows:
        row = list(verification_rows[0] or [])
        label = str(row[1] if len(row) > 1 else "").strip() or "主线验证"
        criterion = str(row[2] if len(row) > 2 else "").strip() or "验证条件"
        lines.append(f"晨报先负责定节奏：只有 `{label}` 真正满足 `{criterion}`，才考虑把主线判断升级成动作。")
    elif theme_rows:
        row = list(theme_rows[0] or [])
        direction = str(row[0] if len(row) > 0 else "").strip() or "主线方向"
        catalyst = str(row[1] if len(row) > 1 else "").strip() or "盘面承接"
        lines.append(f"今天真正要盯的是 `{direction}` 这条线和 `{catalyst}` 能不能延续，不是一看到同主题上涨就全面跟。")
    elif action_lines:
        lines.append("晨报先回答市场节奏和验证条件，真正下单仍要回到对应的单标的 / ETF 复核卡。")
    else:
        lines.append("晨报先回答市场节奏和验证条件，真正下单仍要回到对应的单标的 / ETF 复核卡。")
    lines.append("真到执行层，买点、仓位和失效位仍以对应复核卡为准，别把晨报直接当成挂单指令。")
    return lines[:3]


def _briefing_term_translation_lines(
    *,
    verification_rows: Sequence[Sequence[Any]],
    a_share_watch_meta: Mapping[str, Any],
    theme_rows: Sequence[Sequence[Any]],
) -> List[str]:
    lines = [
        "`主线验证`：今天最该盯的确认条件；成立再提高风险暴露，不成立就回到防守。",
        "`市场结构`：指数、量能、宽度这些底层背景；它告诉我们环境顺不顺，不等于单只票马上能买。",
    ]
    pool_size = int(dict(a_share_watch_meta or {}).get("pool_size") or 0)
    complete_size = int(dict(a_share_watch_meta or {}).get("complete_analysis_size") or 0)
    if pool_size or complete_size:
        lines.append(
            f"`A股观察池`：今天值得继续盯的名单；当前是全市场初筛 `{pool_size}` -> 完整分析 `{complete_size}`，不是直接买入清单。"
        )
    elif theme_rows:
        lines.append("`重点方向`：先看哪条主线还在延续；它更像观察顺序，不是自动下单清单。")
    elif verification_rows:
        lines.append("`验证点`：今天最需要被盘面确认的条件；没确认前，判断就先停在观察层。")
    return lines[:3]


def _briefing_first_screen_block(
    *,
    headline_lines: Sequence[str],
    action_lines: Sequence[str],
    verification_rows: Sequence[Sequence[Any]],
    theme_rows: Sequence[Sequence[Any]],
    alerts: Sequence[str],
) -> str:
    view_text = _pick_client_safe_line(headline_lines[0] if headline_lines else "") or "今天先看风险控制，再看进攻节奏。"
    action_text = _pick_client_safe_line(action_lines[0] if action_lines else "") or "先按常规节奏分批确认。"
    plain_view = view_text.strip("*` ").strip()
    action_markers = ("观察", "跟踪", "确认", "防守", "进攻", "回避", "修复", "行情", "趋势", "出手", "仓位", "风险")
    looks_like_theme_label = (
        bool(plain_view)
        and len(plain_view) <= 24
        and not any(marker in plain_view for marker in action_markers)
        and ("/" in plain_view or "链" in plain_view or "主线" in plain_view or "方向" in plain_view)
    )
    if looks_like_theme_label:
        view_text = "先跟踪主线验证，不因为单日修复直接放大仓位。"
    trigger_parts: List[str] = [action_text]
    if verification_rows:
        row = list(verification_rows[0] or [])
        label = str(row[1] if len(row) > 1 else "").strip()
        criterion = str(row[2] if len(row) > 2 else "").strip()
        if label and criterion:
            trigger_parts.append(f"真正升级先看 `{label}` 能否 `{criterion}`。")
    elif theme_rows:
        row = list(theme_rows[0] or [])
        direction = str(row[0] if len(row) > 0 else "").strip()
        catalyst = str(row[1] if len(row) > 1 else "").strip()
        if direction or catalyst:
            focus = " / ".join(part for part in [direction, catalyst] if part)
            trigger_parts.append(f"优先盯 `{focus}` 是否延续。")
    rows = [
        ["看不看", view_text],
        ["怎么触发", " ".join(part for part in trigger_parts if part).strip()],
        ["多大仓位", _briefing_position_summary_text(action_lines)],
        ["哪里止损", _briefing_stop_summary_text(verification_rows, alerts, theme_rows)],
    ]
    return "\n".join(
        _summary_block_lines(
            rows,
            heading="## 先看执行",
            lead="如果你只看最上面，先看这四件事：看不看、怎么触发、多大仓位、哪里止损。后面都在解释为什么。",
            emphasize=False,
        )
    ).rstrip()


def _stock_pick_stop_summary_text(has_actionable: bool) -> str:
    if has_actionable:
        return "首屏不写统一止损价；真的出手时，只按对应个股复核卡里的失效条件处理。组合层仍坚持单票先小仓，不为单一观点硬扛。"
    return "没触发前先不出手；观察票若主线扩散和价格确认继续失败，就维持观察不升级。"


def _stock_pick_market_priority_items(
    grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    watch_symbols: set[str],
    *,
    limit: int = 2,
) -> List[Tuple[str, Mapping[str, Any]]]:
    rows: List[Tuple[str, Mapping[str, Any]]] = []
    seen: set[str] = set()
    for market_name in ("A股", "港股", "美股"):
        items = list(grouped.get(market_name) or [])
        if not items:
            continue
        ranked = ClientReportRenderer._rank_market_items(items, watch_symbols)
        tracks = _market_recommendation_tracks(ranked, watch_symbols)
        for bucket in ("short", "medium", "third"):
            item = dict(tracks.get(bucket) or {})
            symbol = str(item.get("symbol", "")).strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            rows.append((market_name, item))
            if len(rows) >= limit:
                return rows
    return rows


def _stock_pick_exec_candidate_label(market_name: str, item: Mapping[str, Any]) -> str:
    name = str(item.get("name", item.get("symbol", "—"))).strip() or "—"
    return f"{market_name}：{name}"


def _stock_pick_exec_trigger_text(market_name: str, item: Mapping[str, Any]) -> str:
    action = dict(item.get("action") or {})
    label = _stock_pick_exec_candidate_label(market_name, item)
    entry = _pick_client_safe_line(action.get("entry", "")).rstrip("。；;,， ")
    if entry:
        return f"{label} {entry}。"
    return f"{label} 先等右侧确认回来。"


def _stock_pick_exec_entry_text(market_name: str, item: Mapping[str, Any]) -> str:
    action = dict(item.get("action") or {})
    label = _stock_pick_exec_candidate_label(market_name, item)
    buy_range = _execution_range_text(action.get("buy_range", ""))
    if buy_range:
        return f"{label} 建仓先看 `{buy_range}` 一带承接。"
    entry = _pick_client_safe_line(action.get("entry", "")).rstrip("。；;,， ")
    if entry:
        return f"{label} {entry}。"
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    if stop_ref > 0:
        return f"{label} 更适合靠近 `{stop_ref:.3f}` 上方企稳后再看。"
    return f"{label} 先等更清楚的承接位出来。"


def _stock_pick_exec_position_text(market_name: str, item: Mapping[str, Any]) -> str:
    action = dict(item.get("action") or {})
    label = _stock_pick_exec_candidate_label(market_name, item)
    position = _pick_client_safe_line(action.get("position", "小仓位分批"))
    return f"{label} {position}"


def _stock_pick_exec_stop_text(market_name: str, item: Mapping[str, Any]) -> str:
    action = dict(item.get("action") or {})
    label = _stock_pick_exec_candidate_label(market_name, item)
    stop = _pick_client_safe_line(action.get("stop", "重新跌破关键支撑就处理"))
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    if stop and stop_ref > 0 and not re.search(r"\d", stop):
        return f"{label} {stop}；关键失效位先看 `{stop_ref:.3f}`。"
    if stop:
        return f"{label} {stop}"
    if stop_ref > 0:
        return f"{label} 失守 `{stop_ref:.3f}` 就先处理。"
    return f"{label} 重新跌破关键支撑就处理。"


def _stock_pick_first_screen_block(
    *,
    grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    coverage_grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    watch_symbols: set[str],
    has_actionable: bool,
    expanded_view_text: bool = False,
) -> str:
    entry_parts: List[str] = []
    if has_actionable:
        priority_items = _stock_pick_market_priority_items(grouped, watch_symbols, limit=2)
        trigger_text = (
            "；".join(_stock_pick_exec_trigger_text(market_name, item) for market_name, item in priority_items)
            if priority_items
            else "今天只做最接近确认的前排 1-2 只。"
        )
        if "真正出手要等对应个股复核卡里的介入条件兑现" not in trigger_text:
            trigger_text = f"{trigger_text}；真正出手要等对应个股复核卡里的介入条件兑现。".strip("；")
        entry_parts = [
            _stock_pick_exec_entry_text(market_name, item) for market_name, item in priority_items
        ]
        position_parts = [
            _stock_pick_exec_position_text(market_name, item) for market_name, item in priority_items
        ]
        position_text = "单票 `2% - 5%` 试仓"
        stop_parts = [_stock_pick_exec_stop_text(market_name, item) for market_name, item in priority_items]
        if stop_parts:
            stop_parts.append("任一前排票失守自己的失效位就先处理，不为单一观点硬扛。")
        stop_text = "；".join(stop_parts) if stop_parts else _stock_pick_stop_summary_text(has_actionable)
        view_text = (
            "今天可以做，但只做前排 1-2 只，不把整份名单当全面进攻。"
            if expanded_view_text
            else "今天可以做，但只适合先小仓、分批确认。"
        )
    else:
        watch_text = _stock_pick_market_watch_text(grouped, watch_symbols)
        trigger_text = (
            f"先盯 {watch_text}；只记观察名单和触发条件，等确认后再谈动作。"
            if watch_text
            else "今天先不新开仓；只记观察名单和触发条件，等确认后再谈动作。"
        )
        view_text = "今天先观察，不把观察名单直接写成下单清单。"
        position_text = "当前先不出手"
        stop_text = _stock_pick_stop_summary_text(has_actionable)
    rows = [
        ["看不看", view_text],
        ["怎么触发", trigger_text],
    ]
    if has_actionable and entry_parts:
        rows.append(["哪里建仓", "；".join(entry_parts)])
    rows.extend(
        [
            ["多大仓位", position_text],
            ["哪里止损", stop_text],
        ]
    )
    lead = "如果你只看最上面，先看这四件事：看不看、怎么触发、多大仓位、哪里止损。后面都在解释为什么。"
    if has_actionable and entry_parts:
        lead = "如果你只看最上面，先看这五件事：看不看、怎么触发、哪里建仓、多大仓位、哪里止损。后面都在解释为什么。"
    return "\n".join(
        _summary_block_lines(
            rows,
            heading="## 先看执行",
            lead=lead,
            emphasize=False,
        )
    ).rstrip()


def _strategy_background_summary_text(analysis: Mapping[str, Any]) -> str:
    confidence = dict(_strategy_background_confidence(analysis) or {})
    if not confidence:
        return ""
    status = _shared_strategy_confidence_status({"strategy_background_confidence": confidence})
    reason = _pick_client_safe_line(confidence.get("reason") or confidence.get("summary"))
    if status == "degraded":
        return f"退化：{reason} 当前应先下调置信度，不单靠它升级动作。"
    if status == "watch":
        return f"观察：{reason} 这次信号先只作辅助说明。"
    if status == "stable":
        return f"稳定：{reason} 这层只作辅助加分，不单独替代当前事实层。"
    return ""


def _strategy_background_upgrade_text(analysis: Mapping[str, Any]) -> str:
    confidence = dict(_strategy_background_confidence(analysis) or {})
    if not confidence:
        return ""
    status = _shared_strategy_confidence_status({"strategy_background_confidence": confidence})
    if status == "degraded":
        return "后台验证最近退化，先不要只凭题材热度或单日强势升级动作。"
    if status == "watch":
        return "后台验证当前只到观察，这次先只作辅助说明，不单靠它升级动作。"
    if status == "stable":
        return "后台验证当前稳定，但它只算辅助加分；真正升级仍要等当下确认回来。"
    return ""


def _portfolio_overlap_lines(subject: Mapping[str, Any]) -> List[str]:
    summary = dict(subject.get("portfolio_overlap_summary") or {})
    if not summary:
        return []
    lines: List[str] = []
    summary_line = _pick_client_safe_line(summary.get("summary_line"))
    if summary_line:
        lines.append(summary_line)
    overlap_label = str(summary.get("overlap_label") or "").strip()
    conflict_label = _pick_client_safe_line(summary.get("conflict_label") or "")
    if overlap_label or conflict_label:
        lines.append("组合联动："
                     + (overlap_label or "—")
                     + (f"；{conflict_label}" if conflict_label else ""))
    style_summary_line = _pick_client_safe_line(summary.get("style_summary_line") or "")
    if style_summary_line:
        lines.append(f"风格与方向：{style_summary_line}")
    style_priority_hint = _pick_client_safe_line(summary.get("style_priority_hint") or "")
    if style_priority_hint:
        lines.append(f"组合优先级：{style_priority_hint}")
    return lines[:3]


def _theme_playbook_summary_rows(playbook: Optional[Mapping[str, Any]]) -> List[List[str]]:
    payload = dict(playbook or {})
    if not payload:
        return []
    theme_key = str(payload.get("key", "")).strip()
    playbook_level = str(payload.get("playbook_level", "")).strip()
    sector_label = str(payload.get("label", "")).strip() or str(payload.get("hard_sector_label", "")).strip() or "行业层"
    trading_role_label = str(payload.get("trading_role_label", "")).strip()
    trading_position_label = str(payload.get("trading_position_label", "")).strip()
    trading_role_summary = str(payload.get("trading_role_summary", "")).strip()
    theme_match_status = str(payload.get("theme_match_status", "")).strip()
    theme_match_candidates = [str(item).strip() for item in list(payload.get("theme_match_candidates") or []) if str(item).strip()]
    bridge_confidence = str(payload.get("subtheme_bridge_confidence", "")).strip() or "none"
    bridge_top_label = str(payload.get("subtheme_bridge_top_label", "")).strip()
    bridge_items = [dict(item) for item in list(payload.get("subtheme_bridge") or []) if dict(item)]
    bridge_labels = [str(item.get("label", "")).strip() for item in bridge_items if str(item.get("label", "")).strip()]

    rows: List[List[str]] = []
    if theme_key == "semiconductor" and playbook_level and playbook_level != "sector":
        rows.append(["主题定位", "当前先按 `半导体链` 理解，先看产业催化有没有继续兑现，再看风险偏好和资金承接能不能把价格留住。不要和软件/应用层提前混成一个大科技桶。"])
        if bridge_top_label:
            rows.append(["细分观察", f"细分上优先跟 `{bridge_top_label}` 这条线的订单、capex 和涨价确认；没有新增证据前，不把它直接写成全面加速。"])
        if trading_role_label and trading_position_label:
            role_text = f"`{trading_role_label}` / `{trading_position_label}`"
            if trading_role_summary:
                role_text += f"：{trading_role_summary}"
            rows.append(["交易分层", role_text])
        return rows
    if playbook_level and playbook_level != "sector":
        rows.append(["主题定位", f"当前更接近 `{sector_label}` 这条方向；正文和复核卡默认按这条主题线理解，当天盘面主线只作为市场背景。"])
        if trading_role_label and trading_position_label:
            role_text = f"`{trading_role_label}` / `{trading_position_label}`"
            if trading_role_summary:
                role_text += f"：{trading_role_summary}"
            rows.append(["交易分层", role_text])
        return rows
    if playbook_level == "sector":
        if theme_match_status == "ambiguous_conflict" and theme_match_candidates:
            joined = " / ".join(theme_match_candidates[:3])
            rows.append(["主题边界", f"当前先按 `{sector_label}` 行业层理解，`{joined}` 这几条线还没拉开，不适合硬落单一细主题。"])
        else:
            rows.append(["主题边界", f"当前先按 `{sector_label}` 行业层理解，先把行业逻辑、景气和风格顺逆风看清，再决定要不要往更细主题上落。"])
    if playbook_level != "sector" or not bridge_labels:
        return rows
    if bridge_confidence == "high" and bridge_top_label:
        text = f"当前仍先按 `{sector_label}` 行业层理解，但可优先跟踪 `{bridge_top_label}`；这最多只算偏向，不等于已确认细主题。"
    elif bridge_confidence == "medium" and bridge_top_label:
        text = f"当前仍先按 `{sector_label}` 行业层理解，但可优先留意 `{bridge_top_label}`；没有更多确认前，不把它写成确定主线。"
    else:
        joined = " / ".join(f"`{label}`" for label in bridge_labels[:3])
        text = f"如果后续要继续下钻，先观察 {joined} 这些细分方向；当前不足以下单一主题结论。"
    rows.append(["细分观察", text])
    if trading_role_label and trading_position_label:
        role_text = f"`{trading_role_label}` / `{trading_position_label}`"
        if trading_role_summary:
            role_text += f"：{trading_role_summary}"
        rows.append(["交易分层", role_text])
    return rows


def _theme_playbook_explainer_lines(playbook: Optional[Mapping[str, Any]]) -> List[str]:
    return [f"{label}：{text}" for label, text in _theme_playbook_summary_rows(playbook)]


def _theme_playbook_reason_lines(playbook: Optional[Mapping[str, Any]]) -> List[str]:
    # Repeated candidate cards need theme/context hints, but generic trading-role
    # boilerplate belongs in the execution table rather than every reason block.
    return [
        line
        for line in _theme_playbook_explainer_lines(playbook)
        if not line.startswith("交易分层：")
    ]


def _stock_pick_market_priority_text(
    grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    watch_symbols: set[str],
    *,
    bucket: str,
) -> str:
    parts: List[str] = []
    for market_name in ("A股", "港股", "美股"):
        items = list(grouped.get(market_name) or [])
        if not items:
            continue
        ranked = ClientReportRenderer._rank_market_items(items, watch_symbols)
        tracks = _market_recommendation_tracks(ranked, watch_symbols)
        item = dict(tracks.get(bucket) or {})
        if not item:
            continue
        name = str(item.get("name", item.get("symbol", ""))).strip()
        if name:
            parts.append(f"{market_name}：{name}")
    return "；".join(parts)


def _stock_pick_market_watch_text(
    grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    watch_symbols: set[str],
) -> str:
    parts: List[str] = []
    for market_name in ("A股", "港股", "美股"):
        items = list(grouped.get(market_name) or [])
        if not items:
            continue
        ranked = ClientReportRenderer._rank_market_items(items, watch_symbols)
        watch_rows = _market_watch_rows(ranked, watch_symbols, limit=1)
        if not watch_rows:
            continue
        name = str(watch_rows[0][1]).split(" (", 1)[0].strip()
        if name:
            parts.append(f"{market_name}：{name}")
    return "；".join(parts)


def _stock_pick_summary_rows(
    *,
    day_theme: str,
    regime: str,
    sector_filter: str,
    theme_playbook: Optional[Mapping[str, Any]],
    grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    coverage_grouped: Mapping[str, Sequence[Mapping[str, Any]]],
    watch_symbols: set[str],
    has_actionable: bool,
    include_execution: bool = True,
) -> List[List[str]]:
    report_theme = representative_theme_label({"theme_playbook": theme_playbook, "sector_filter": sector_filter})
    affordable_rows = _affordable_stock_rows(coverage_grouped.get("A股", []), watch_symbols)
    related_etf_rows = _related_etf_rows(coverage_grouped.get("A股", []), watch_symbols)
    empty_parts: List[str] = []
    if has_actionable:
        empty_parts.append("先从短线/中线优先里挑 1 只，单票按 `2% - 5%` 试仓")
        if affordable_rows:
            empty_parts.append(f"买不起高价龙头时先看 `{affordable_rows[0][0].split(' (', 1)[0]}`")
        if related_etf_rows:
            empty_parts.append(f"不想硬扛单票波动时可先用 `{related_etf_rows[0][0].split(' (', 1)[0]}` 观察方向")
        empty_text = "；".join(empty_parts)
        holder_text = "已有仓位先留强去弱；没破关键位和止损前不乱砍，反弹确认后再考虑加仓。"
        first_position = "单票 `2% - 5%` 试仓"
    else:
        empty_text = "今天先不新开仓；只记观察名单和触发条件，等确认后再谈动作。"
        holder_text = "已有仓位先按止损和主线强弱管理，不因为一篇观察稿去追补仓。"
        first_position = "当前先不出手"

    rows: List[List[str]] = [
        ["报告定位", "推荐稿" if has_actionable else "观察稿"],
        ["中期背景 / 当天主线", f"`{regime}` / `{day_theme}`"],
        [
            "范围说明",
            (
                f"当前是 `{sector_filter}` 主题内相对排序，不是跨主题分散候选池。"
                if sector_filter
                else "当前按市场范围做相对排序，默认不是组合级分散配置建议。"
            ),
        ],
        [
            "置信度",
            "未做整份名单统一量化；每只票按各自八维评分、硬排除和样本边界理解。",
        ],
    ]
    if report_theme:
        rows.append(["代表主题", f"`{report_theme}`"])
    rows.extend(_theme_playbook_summary_rows(theme_playbook))
    if has_actionable:
        rows.extend(
            [
                [
                    "短线优先",
                    _stock_pick_market_priority_text(grouped, watch_symbols, bucket="short")
                    or "今天没有可单列的短线动作票。",
                ],
                [
                    "中线优先",
                    _stock_pick_market_priority_text(grouped, watch_symbols, bucket="medium")
                    or "今天没有可单列的中线动作票。",
                ],
            ]
        )
    else:
        rows.append(
            [
                "优先观察",
                _stock_pick_market_watch_text(grouped, watch_symbols) or "当前没有可优先观察的名单。",
            ]
        )
    if include_execution:
        rows.extend(
            [
                ["空仓怎么做", empty_text],
                ["持仓怎么做", holder_text],
                ["首次仓位", first_position],
            ]
        )
    rows.extend(
        [
            [
                "主要利好",
                (
                    (
                        f"当前仍能分出结构性优先级，市场背景看 `{day_theme}`，代表主题先按 `{report_theme}` 理解。"
                        if report_theme
                        else f"当前仍能分出结构性优先级，主线先按 `{day_theme}` 理解。"
                    )
                    if has_actionable
                    else "名单里仍有值得继续跟踪的观察对象，不是完全没有方向。"
                ),
            ],
            [
                "主要利空",
                (
                    f"不是全市场普涨环境，仍在 `{regime}` 背景里，更适合先小仓、等确认。"
                    if has_actionable
                    else "当前没有达到正式动作阈值的个股，不适合硬写成可直接下单的推荐单。"
                ),
            ],
        ]
    )
    return rows


def _market_collective_watch_explanation(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
) -> str:
    observe_items = [item for item in ranked_items if not _analysis_is_actionable(item, watch_symbols)][:5]
    if len(observe_items) < 2:
        return ""
    relative_scores = [_score(item, "relative_strength") for item in observe_items]
    weak_relative = [score for score in relative_scores if score <= 40]
    if len(weak_relative) >= max(2, len(observe_items) - 1):
        return (
            "这批票当前共同卡在相对强弱：更像主题方向没坏，但个股右侧扩散和价格确认还没完成；"
            "低分更接近整理/未确认，不直接等于主线已经被证伪。"
        )
    return ""


def _observe_delivery_bridge_text(asset_label: str) -> str:
    if asset_label == "场外基金":
        return "当前方向还在，但更像保留申赎观察资格，不等于净值位置、确认信号和覆盖率都已完成确认。"
    return "当前方向还在，但更像保留观察资格，不等于价格、节奏和数据完整度都已完成确认。"


def _observe_delivery_threshold_lines(
    *,
    asset_label: str,
    trade_state: str,
    delivery_label: str,
) -> List[str]:
    action_label = "申赎" if asset_label == "场外基金" else "交易"
    normalized_state = _pick_client_safe_line(trade_state or "观察为主") or "观察为主"
    normalized_delivery = _pick_client_safe_line(delivery_label or "观察稿") or "观察稿"
    return [
        "## 正式动作阈值",
        "",
        f"这段只回答什么情况下，这份{asset_label}观察稿才会升级成可执行{action_label}稿。",
        "",
        "- 先过结构门：技术确认、相对强弱/趋势、催化或数据完整度里，至少两项不再拖后腿，不能只靠单一高分硬撑。",
        f"- 再过动作门：只要当前结论仍是 `{normalized_state}`，就不把它写成正式{action_label}动作。",
        f"- 交付层也要一起放行：如果当前仍标成 `{normalized_delivery}`，就更适合继续观察，不把“先看它”误读成“现在就能做”。",
    ]


def _structured_coverage_preface(coverage_lines: Sequence[str], *, asset_label: str) -> str:
    structured_positive = any(
        "结构化事件覆盖" in line
        and "结构化事件覆盖 0%" not in line
        and "结构化事件覆盖 0%（0/" not in line
        for line in coverage_lines
    )
    direct_news_zero = any(
        token in line
        for line in coverage_lines
        for token in (
            "高置信直接情报覆盖 0%",
            "高置信直接新闻覆盖 0%",
            "高置信公司级直连情报覆盖 0%",
            "高置信公司新闻覆盖 0%",
        )
    )
    if not (structured_positive and direct_news_zero):
        return ""
    if asset_label == "场外基金":
        return "当前证据更偏结构化事件、基金画像和持仓/基准映射，不是直连情报催化型驱动。"
    if asset_label == "ETF":
        return "当前证据更偏结构化事件、产品画像和持仓/基准映射，不是直连情报催化型驱动。"
    if asset_label == "个股":
        return "当前证据更偏结构化事件与公告日历，不是直连情报催化型驱动；`0%` 只代表没命中高置信个股直连情报。"
    return f"当前证据更偏结构化事件与公告日历，不是{asset_label}的直连情报催化型驱动；`0%` 只代表没命中高置信个股直连情报。"


def _briefing_summary_rows(
    *,
    headline_lines: Sequence[str],
    action_lines: Sequence[str],
    regime: Mapping[str, Any],
    day_theme: str,
    a_share_watch_meta: Mapping[str, Any],
    quality_lines: Sequence[str],
    theme_playbook: Optional[Mapping[str, Any]] = None,
    index_signal_rows: Sequence[Sequence[Any]] | None = None,
    market_signal_rows: Sequence[Sequence[Any]] | None = None,
    rotation_rows: Sequence[Sequence[Any]] | None = None,
    include_execution: bool = True,
) -> List[List[str]]:
    rows: List[List[str]] = []
    if include_execution and headline_lines:
        rows.append(["当前判断", str(headline_lines[0]).strip()])
    if include_execution and action_lines:
        rows.append(["优先动作", str(action_lines[0]).strip()])
    regime_name = str(dict(regime or {}).get("current_regime", "")).strip()
    if regime_name or day_theme:
        rows.append(["中期背景 / 当天主线", f"{regime_name or '未标注'} / {day_theme or '未标注'}"])
    if index_signal_rows:
        row = list(index_signal_rows)[0]
        label = str(row[0] if len(row) > 0 else "").strip()
        ma_label = str(row[3] if len(row) > 3 else "").strip()
        weekly_macd = str(row[4] if len(row) > 4 else "").strip()
        monthly_macd = str(row[5] if len(row) > 5 else "").strip()
        volume_label = str(row[6] if len(row) > 6 else "").strip()
        summary = str(row[7] if len(row) > 7 else "").strip()
        parts = [item for item in (ma_label, weekly_macd, monthly_macd, volume_label) if item and item != "N/A"]
        if summary:
            parts.append(summary)
        rows.append(["周月节奏", f"{label or '核心指数'} / " + " / ".join(parts[:4]) if parts else label or "已接入"])
    if market_signal_rows:
        for row in list(market_signal_rows):
            label = str(row[0] if len(row) > 0 else "").strip()
            value = str(row[1] if len(row) > 1 else "").strip()
            signal_label = str(row[2] if len(row) > 2 else "").strip()
            detail = str(row[3] if len(row) > 3 else "").strip()
            if label == "交易结构":
                rows.append(["市场结构", f"{value or '—'} / {signal_label or '中'} / {detail or '结构快照已接入'}"])
                break
            if label == "市场宽度":
                rows.append(["市场宽度", f"{value or '—'} / {signal_label or '中'} / {detail or '宽度快照已接入'}"])
                break
    if rotation_rows:
        row = list(rotation_rows)[0]
        leader = str(row[1] if len(row) > 1 else "").strip()
        laggard = str(row[2] if len(row) > 2 else "").strip()
        rotation = str(row[3] if len(row) > 3 else "").strip()
        if leader or laggard or rotation:
            rows.append(["轮动", " / ".join(part for part in [leader or "—", laggard or "—", rotation or "轮动快照已接入"] if part)])
    rows.extend(_theme_playbook_summary_rows(theme_playbook))
    pool_size = int(dict(a_share_watch_meta or {}).get("pool_size") or 0)
    complete_size = int(dict(a_share_watch_meta or {}).get("complete_analysis_size") or 0)
    if pool_size or complete_size:
        rows.append(["A股观察池", f"全市场初筛 `{pool_size}` -> 完整分析 `{complete_size}`。"])
    if quality_lines:
        rows.append(["当前限制", str(quality_lines[0]).strip()])
    return rows[:7]


def _briefing_quality_detail_lines(quality_lines: Sequence[str]) -> List[str]:
    detail_tokens = (
        "代理",
        "改用",
        "覆盖",
        "缺失",
        "复核",
        "免费源",
        "client-final",
        "自动跳过",
        "超时阈值",
        "主题新闻扩搜",
        "轻量新闻源配置",
        "global proxy",
        "market monitor",
    )
    detail_lines = [
        str(item).strip()
        for item in quality_lines
        if str(item).strip() and any(token in str(item) for token in detail_tokens)
    ]
    return detail_lines or [str(item).strip() for item in quality_lines if str(item).strip()]


def _briefing_watch_upgrade_lines(candidates: Sequence[Mapping[str, Any]]) -> List[str]:
    rows: List[str] = []
    items = [
        dict(item or {})
        for item in list(candidates or [])
        if not dict(item or {}).get("briefing_reuse_only")
        and (str(dict(item or {}).get("name", "")).strip() or str(dict(item or {}).get("symbol", "")).strip())
    ]
    if not items:
        return [
            "当前A股观察池更像全市场方向筛选，今天还没有能直接升级成正式动作票的样本。",
            "升级要等价格、成交或主线扩散里至少一项先给出更清晰的确认。",
        ]
    rows.append("当前A股观察池更像全市场方向筛选，不等于今天已经出现正式动作票。")
    for item in items[:2]:
        name = str(item.get("name", item.get("symbol", ""))).strip()
        symbol = str(item.get("symbol", "")).strip()
        label = f"{name} ({symbol})" if symbol else name
        why_text = _pick_client_safe_line(_decision_gate_explanation(item))
        strategy_upgrade_text = _strategy_background_upgrade_text(item)
        if strategy_upgrade_text:
            why_text = _append_sentence(why_text, strategy_upgrade_text)
        trigger_text = _primary_upgrade_trigger(item)
        if why_text:
            rows.append(f"`{label}` 主要卡在：{why_text}")
        if trigger_text:
            rows.append(f"`{label}` 升级条件：{trigger_text}")
    return rows


def _rename_markdown_heading(markdown_text: str, mapping: Mapping[str, str]) -> str:
    if not markdown_text:
        return ""
    lines = markdown_text.splitlines()
    rewritten: List[str] = []
    for line in lines:
        stripped = line.strip()
        rewritten.append(mapping.get(stripped, line))
    return "\n".join(rewritten).rstrip()


def _replace_markdown_section(markdown_text: str, heading: str, replacement_lines: Sequence[str]) -> str:
    if not markdown_text:
        return "\n".join(replacement_lines).rstrip()
    lines = markdown_text.splitlines()
    start: Optional[int] = None
    end = len(lines)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped == heading:
            start = idx
            continue
        if start is not None and idx > start and stripped.startswith("## "):
            end = idx
            break
    if start is None:
        updated = list(lines)
        if updated and updated[-1].strip():
            updated.append("")
        updated.extend(replacement_lines)
        return "\n".join(updated).rstrip()
    updated = list(lines[:start]) + list(replacement_lines) + list(lines[end:])
    return "\n".join(updated).rstrip()


def _compress_multiname_homepage_action_section(
    markdown_text: str,
    *,
    stance_lines: Sequence[str],
) -> str:
    if not markdown_text:
        return ""
    lines = markdown_text.splitlines()
    start: Optional[int] = None
    end = len(lines)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "### 动作建议与结论":
            start = idx
            continue
        if start is not None and idx > start and (stripped.startswith("### ") or stripped.startswith("## ")):
            end = idx
            break
    if start is None:
        return markdown_text.rstrip()
    replacement = [
        "### 动作建议与结论",
        "",
        "真正的执行口径以上面的 `先看执行` 为准，这里不重复买点、仓位和止损。",
    ]
    for item in stance_lines:
        text = str(item or "").strip()
        if text:
            replacement.append(f"- {text}")
    updated = list(lines[:start]) + replacement + list(lines[end:])
    return "\n".join(updated).rstrip()


def _insert_markdown_section_before(markdown_text: str, before_heading: str, section_lines: Sequence[str]) -> str:
    if not markdown_text or not list(section_lines):
        return markdown_text.rstrip()
    marker = before_heading.strip()
    lines = markdown_text.splitlines()
    insert_at = len(lines)
    for idx, line in enumerate(lines):
        if line.strip() == marker:
            insert_at = idx
            break
    prefix = list(lines[:insert_at])
    if prefix and prefix[-1].strip():
        prefix.append("")
    merged = prefix + list(section_lines) + [""] + list(lines[insert_at:])
    return "\n".join(merged).rstrip()


def _upsert_visual_section(
    markdown_text: str,
    visuals: Optional[Mapping[str, str]],
    *,
    before_heading: str = "## 为什么这么判断",
) -> str:
    visual_lines = _pick_visual_lines(visuals)
    if not markdown_text or not visual_lines:
        return markdown_text.rstrip()
    if "## 图表速览" in markdown_text:
        return _replace_markdown_section(markdown_text, "## 图表速览", visual_lines)
    return _insert_markdown_section_before(markdown_text, before_heading, visual_lines)


def _insert_print_page_break_before(markdown_text: str, heading: str) -> str:
    marker = '<div class="report-page-break"></div>'
    if marker in markdown_text:
        return markdown_text.rstrip()
    return _insert_markdown_section_before(markdown_text, heading, [marker])


def _inject_scan_reasoning_table(markdown_text: str, analysis: Mapping[str, Any]) -> str:
    if "## 为什么这么判断" in markdown_text:
        return markdown_text
    summary_lines = [
        "## 为什么这么判断",
        "",
        *_table(["维度", "分数", "为什么是这个分"], _scan_dimension_rows(analysis)),
        "",
    ]
    marker = "## 当前判断"
    if marker not in markdown_text:
        return markdown_text.rstrip() + "\n\n" + "\n".join(summary_lines).rstrip()
    return markdown_text.replace(marker, "\n".join(summary_lines) + marker, 1)


def _inject_scan_exec_summary(markdown_text: str, analysis: Mapping[str, Any]) -> str:
    if "## 执行摘要" in markdown_text:
        return markdown_text
    action = dict(analysis.get("action") or {})
    narrative = dict(analysis.get("narrative") or {})
    horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")), context=str(analysis.get("name", "")))
    handoff = portfolio_whatif_handoff(
        symbol=str(analysis.get("symbol", "")),
        horizon=horizon,
        direction=str(action.get("direction", "")),
        asset_type=str(analysis.get("asset_type", "")),
        reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
        generated_at=str(analysis.get("generated_at", "")),
    )
    summary_rows = _single_asset_exec_summary_rows(
        analysis,
        horizon,
        handoff,
        theme_playbook=dict(build_scan_editor_packet(analysis, bucket=_recommendation_bucket(analysis)).get("theme_playbook") or {}),
        status_label=_recommendation_bucket(analysis),
        default_trigger="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
        default_holder_text="已有仓位先按止损和关键位管理，不把当前判断直接当成继续加仓的理由。",
    )
    summary_lines = _summary_block_lines(
        summary_rows,
        heading="## 执行摘要",
        lead="这段只回答能不能动、空仓和持仓分别怎么做、仓位大概多大，以及为什么不是更激进的动作。",
    )
    if not summary_lines:
        return markdown_text
    marker = "## 为什么这么判断"
    block = "\n".join(summary_lines).rstrip() + "\n\n"
    if marker not in markdown_text:
        return markdown_text.rstrip() + "\n\n" + block.rstrip()
    return markdown_text.replace(marker, block + marker, 1)


def _inject_scan_theme_context(markdown_text: str, playbook: Mapping[str, Any]) -> str:
    lines = [f"- {item}" for item in _theme_playbook_explainer_lines(playbook)[:2]]
    if not lines:
        return markdown_text
    marker = "## 为什么这么判断\n\n"
    if marker not in markdown_text:
        return markdown_text
    if any(line in markdown_text for line in lines):
        return markdown_text
    return markdown_text.replace(marker, marker + "\n".join(lines) + "\n\n", 1)


def _trim_observe_only_scan_execution(markdown_text: str, analysis: Mapping[str, Any]) -> str:
    bucket = _recommendation_bucket(analysis)
    if bucket != "观察为主":
        return markdown_text

    skip_sections = {"## 仓位管理", "## 组合落单前", "## 当前更合适的动作"}
    stripped_rows = (
        "| 空仓怎么做 |",
        "| 持仓怎么做 |",
        "| 组合落单前 |",
        "| 预演命令 |",
        "| 配置视角 |",
        "| 交易视角 |",
        "| 适合谁 |",
        "| 持有周期 |",
        "| 不适合打法 |",
        "| 介入条件 |",
        "| 止损 |",
        "| 建议买入区间 |",
        "| 首次仓位 |",
        "| 加仓节奏 |",
        "| 建议减仓区间 |",
        "| 第一减仓位 |",
        "| 第二减仓位 |",
        "| 上修条件 |",
        "| 目标参考 |",
    )
    stripped_bullets = (
        "- 命令：`portfolio whatif buy",
        "- 单标的仓位不超过",
        "- 初始仓位",
        "- 加仓条件",
    )

    lines: List[str] = []
    skipping = False
    for raw_line in markdown_text.splitlines():
        line = raw_line.rstrip()
        if line.startswith("## "):
            skipping = line in skip_sections
            if skipping:
                continue
        if skipping:
            continue
        if any(marker in line for marker in stripped_rows):
            continue
        if any(marker in line for marker in stripped_bullets):
            continue
        lines.append(raw_line)
    return "\n".join(lines).rstrip()


def _rewrite_scan_theme_language(markdown_text: str, analysis: Mapping[str, Any], packet: Mapping[str, Any]) -> str:
    theme_key = str(dict(packet.get("theme_playbook") or {}).get("key", "")).strip()
    if theme_key != "semiconductor":
        return markdown_text

    rewritten = markdown_text
    replacements = {
        "逻辑未完全破坏，但价格、催化和资金尚未形成新的共振": "半导体链的中期逻辑没有被证伪，但价格、催化和资金还没有重新站到一边",
        "这只 ETF 更像在买 `科技` 方向的被动暴露": "这只 ETF 更像在买 `半导体设备与设计制造链` 的被动暴露",
        "这只基金更像在买`科技`方向的被动暴露，当前标签是 `科技主题 / 被动跟踪`。": "这只基金更像在买`半导体设备与设计制造链`的被动暴露，当前标签是 `半导体主题 / 被动跟踪`。",
        "风格标签 | 科技主题 / 被动跟踪 |": "风格标签 | 半导体主题 / 被动跟踪 |",
        "选股方式 | 核心不是基金经理主动选股，而是跟踪 `科技` 暴露及其对应基准。 |": "选股方式 | 核心不是基金经理主动选股，而是跟踪 `半导体链` 暴露及其对应基准。 |",
        "主题 `科技`，基准匹配度有限": "主题 `半导体`，基准匹配度较高",
        "当前 3 月不在 `科技` 行业常见旺季前置窗口；": "当前 3 月不在 `半导体` 常见旺季前置窗口；",
        "可能对 科技 行业有政策催化；": "可能对 半导体 / 先进制造方向有政策催化；",
        "说明成长估值压力缓解，科技方向更容易从修复走向扩散。": "说明成长估值压力缓解，半导体这类高 beta 成长方向更容易从修复走向扩散。",
        "趋势交易者可以重新评估右侧介入。": "趋势跟踪信号开始改善，可以重新评估是否从观察升级。",
        "左侧观察价值仍在。": "继续保留观察价值。",
        "`科技` 暴露仍有配置价值": "`半导体链` 暴露仍有跟踪价值",
    }
    for source, target in replacements.items():
        rewritten = rewritten.replace(source, target)
    return rewritten


def _taxonomy_section(winner: Mapping[str, Any]) -> List[str]:
    rows = list(winner.get("taxonomy_rows") or [])
    if not rows:
        rows = [
            ["产品形态", "—"],
            ["载体角色", "—"],
            ["管理方式", "—"],
            ["暴露类型", "—"],
            ["主方向", "—"],
            ["份额类别", "—"],
        ]
    summary = str(winner.get("taxonomy_summary", "")).strip()
    lines = [
        "## 标准化分类",
        "",
    ]
    lines.extend(_table(["维度", "结果"], rows))
    lines.extend(["", f"- {summary or '当前分类只作为产品标签，不替代净值、持仓和交易判断。'}"])
    return lines


def _summary_only_explainer_sections(
    winner: Mapping[str, Any],
    alternatives: Sequence[Mapping[str, Any]],
    *,
    event_digest: Mapping[str, Any] | None = None,
    evidence_fallback: str,
    no_alternative_text: str,
) -> List[str]:
    lines: List[str] = []
    lines.extend(["", *_taxonomy_section(winner)])
    if not dict(winner.get("visuals") or {}):
        lines.extend(["", "## 图表与详细分析说明", ""])
        lines.append("- 本次稿件没有生成 K 线/阶段走势图表。")
        lines.append("- 当前按摘要观察稿交付；如果完整日线历史没有稳定拿到，图表层不会按完整分析稿生成。")
    lines.extend(["", "## 关键证据", ""])
    evidence_lines = _evidence_lines_with_event_digest(
        list(winner.get("evidence") or []),
        event_digest=event_digest,
        max_items=2,
        as_of=winner.get("generated_at"),
    )
    if evidence_lines:
        lines.extend(evidence_lines)
    else:
        lines.append(f"- {evidence_fallback}")
    strong_factor_rows = _strong_factor_rows_from_dimensions(
        winner.get("dimensions", {}),
        asset_type=str(winner.get("asset_type", "")) or "cn_etf",
        max_items=3,
    )
    if strong_factor_rows:
        lines.extend(["", "## 关键强因子拆解", ""])
        lines.extend(_table(["因子", "当前信号", "这意味着什么"], strong_factor_rows))
    lines.extend(["", *_analysis_provenance_lines(winner)])
    lines.extend(["", "## 为什么不是另外几只", ""])
    if alternatives:
        for index, item in enumerate(alternatives[:2], start=1):
            lines.extend(
                [
                    f"### {index}. {item.get('name', '')} ({item.get('symbol', '')})",
                    "",
                ]
            )
            cautions = [str(reason).strip() for reason in item.get("cautions", []) if str(reason).strip()]
            if cautions:
                for reason in cautions[:2]:
                    lines.append(f"- {reason}")
            else:
                lines.append("- 当前没有额外可展开的备选理由。")
            lines.append("")
    else:
        lines.append(f"- {no_alternative_text}")
        lines.append("- 当前更适合先按这一个观察对象理解，不把名单不足误读成自动强推。")
    return lines


def _bucket_summary_text(bucket: str, analysis: Mapping[str, Any]) -> str:
    raw_label = str(dict(analysis.get("rating") or {}).get("label", "未评级")).strip() or "未评级"
    topline = _scan_topline_text(analysis)
    if topline not in {"", "未评级"} and topline != raw_label:
        return f"{topline}。"
    if bucket == "正式推荐":
        return "逻辑和执行条件都还在，可以小仓分批，但不适合重仓追高。"
    if bucket == "看好但暂不推荐":
        return "方向没坏，但今天更像观察和等待确认，不适合直接激进出手。"
    return "当前主要价值在观察，不在立即执行。"


def _pick_horizon_profile(action: Mapping[str, Any], trade_state: str = "", *, context: str = "") -> Dict[str, str]:
    structured = dict(action.get("horizon") or {})
    if structured:
        label = str(structured.get("label", "")).strip().replace("(", "（").replace(")", "）")
        style = str(structured.get("style", "")).strip()
        fit_reason = str(structured.get("fit_reason", "")).strip()
        misfit_reason = str(structured.get("misfit_reason", "")).strip()
        allocation_view = str(structured.get("allocation_view", "")).strip()
        trading_view = str(structured.get("trading_view", "")).strip()
        if label:
            ctx = f"（{context}）" if context else ""
            return {
                "code": str(structured.get("code", "")).strip(),
                "family_code": str(structured.get("family_code", "")).strip() or str(structured.get("code", "")).strip(),
                "setup_code": str(structured.get("setup_code", "")).strip(),
                "setup_label": str(structured.get("setup_label", "")).strip(),
                "label": label,
                "style": f"{style}{ctx}" if style else "",
                "fit_reason": f"{fit_reason}{ctx}" if fit_reason else "",
                "misfit_reason": f"{misfit_reason}{ctx}" if misfit_reason else "",
                "allocation_view": f"{allocation_view}{ctx}" if allocation_view else "",
                "trading_view": f"{trading_view}{ctx}" if trading_view else "",
            }

    raw = str(action.get("timeframe", "")).strip()
    trade_state = str(trade_state).strip()
    direction = str(action.get("direction", "")).strip()
    label = raw.replace("(", "（").replace(")", "）") if raw else ""
    ctx = f"（{context}）" if context else ""

    if "长线" in label:
        return {
            "code": "long_term_allocation",
            "family_code": "long_term_allocation",
            "label": label,
            "style": f"更适合作为中长期底仓来跟踪{ctx}，允许短线波动，但要持续复核主线、基本面和风险预算。",
            "fit_reason": f"更依赖中长期逻辑{ctx}，而不是一两天的节奏变化。",
            "misfit_reason": f"不适合按纯短线追涨杀跌来理解{ctx}。",
        }
    if "中线" in label:
        return {
            "code": "position_trade",
            "family_code": "position_trade",
            "label": label,
            "style": f"更像 1-3 个月的分批配置或波段跟踪{ctx}，不按隔日涨跌去做快进快出。",
            "fit_reason": f"更适合围绕一段完整主线分批拿{ctx}，而不是只看日内波动。",
            "misfit_reason": f"不适合直接当成超短节奏仓{ctx}，也别默认长到长期不复核。",
        }
    if "短线" in label:
        return {
            "code": "short_term",
            "family_code": "short_term",
            "label": label,
            "style": f"更看催化、趋势和执行节奏{ctx}，适合盯右侧确认和止损，不适合当成长线底仓。",
            "fit_reason": f"当前优势更多集中在催化和节奏{ctx}，不在长周期基本面。",
            "misfit_reason": f"不适合当成长线配置仓{ctx}。",
        }
    if "波段" in label:
        return {
            "code": "swing",
            "family_code": "swing",
            "label": label,
            "style": f"更适合按几周级别的波段节奏去跟踪{ctx}，等确认和回踩，不靠单日冲动去追。",
            "fit_reason": f"趋势和轮动有基础{ctx}，但还更依赖未来几周节奏。",
            "misfit_reason": f"不适合当长期底仓{ctx}，也不适合只按单条消息去赌超短。",
        }

    fallback_blob = " ".join(part for part in (raw, trade_state, direction) if part)
    if any(token in fallback_blob for token in ("等待", "观察", "回避", "暂不出手")):
        ctx = f"（{context}）" if context else ""
        return {
            "code": "watch",
            "family_code": "watch",
            "label": "观察期",
            "style": f"现在先看窗口和确认信号{ctx}，不建议急着把它定义成短线执行仓或长线配置仓。",
            "fit_reason": f"当前信号还没共振到足以支撑正式动作{ctx}，先观察更稳妥。",
            "misfit_reason": f"不适合直接按明确的长线或短线打法去执行{ctx}。",
        }
    return {}


def _pick_client_safe_line(text: Any) -> str:
    line = str(text).strip()
    if not line:
        return ""
    replacements = (
        (r"近\s*(\d+)\s*日未命中直接政策催化", r"近 \1 日直接政策情报偏弱"),
        (r"未命中直接龙头公告", "龙头公告/业绩直连情报偏弱"),
        (r"未命中直接海外映射", "海外映射直连情报偏弱"),
        (r"未来\s*(\d+)\s*日未命中直接催化事件", r"未来 \1 日前瞻催化窗口暂不突出"),
        (r"近\s*(\d+)\s*日未命中明确财报/年报事件窗口", r"近 \1 日财报/年报窗口暂不突出"),
        (r"近\s*(\d+)\s*日未命中明确高管/大股东增减持", r"近 \1 日暂未看到明确高管/大股东增减持"),
        (r"近\s*(\d+)\s*日未命中明确稀释/监管负面", r"近 \1 日暂未看到明确稀释/监管负面"),
        (r"近\s*(\d+)\s*日未命中明确主题/产业链逆风头条", r"近 \1 日暂未看到明确主题/产业链逆风"),
        (r"未命中明确结构化公司事件", "结构化公司事件暂不突出"),
        (r"未命中高置信直连源", "当前前置证据以结构化披露和主题线索为主"),
        (r"未命中显式日期", "日期未单独披露"),
        (r"当前\s*前置事件先看", "当前更该前置的是"),
        (r"内部覆盖率摘要", "覆盖率摘要"),
        (r"当前没有抓到高置信直连证据，催化判断更多依赖结构化事件或行业映射。", "当前前置的一手情报偏少，判断更多参考结构化披露和行业线索。"),
        (r"当前没有高置信直连证据，摘要判断主要依赖覆盖率、基金画像和现有代理信号。", "当前前置的一手情报偏少，摘要判断更多参考覆盖率、基金画像和现有代理信号。"),
        (r"当前没有可直接复核的高置信直连证据，催化判断更多依赖结构化事件、基金画像或历史有效信号。", "当前可直接复核的一手情报偏少，判断更多参考结构化披露、基金画像和历史有效信号。"),
        (r"当前没有可直接复核的高置信直连证据，催化判断更多依赖结构化事件或历史有效信号。", "当前可直接复核的一手情报偏少，判断更多参考结构化披露和历史有效信号。"),
        (r"当前新增直接情报偏少，且主题检索疑似漏抓；先不把它写成零催化", "当前新增直接情报偏少，且主题检索可能有漏抓；先不把它写成零催化"),
        (r"当前新增直接情报偏少，且本轮覆盖有降级；先按低置信观察处理", "当前新增直接情报偏少，且本轮情报覆盖有降级；先按低置信观察处理"),
        (r"当前只有主题级情报，个股级新增证据还不够", "当前主要还是主题级情报，个股级新增证据还不够"),
        (r"新鲜情报\s*0\s*条", "新鲜情报偏少"),
        (r"覆盖源\s*0\s*个", "情报覆盖偏窄"),
        (r"为保证[^。]*`client-final` 可交付，本轮自动跳过跨市场代理与 market monitor 慢链。", "本轮先按国内宏观与本地行情上下文生成，跨市场代理快线暂未启用。"),
        (r"observation_only", "观察提示"),
        (r"lag/visibility fixture", "时点一致性校验"),
        (r"近\s*(\d+)\s*日未命中跟踪基准/行业暴露/核心成分共振催化", r"近 \1 日跟踪方向共振情报偏弱"),
        (r"眼下更卡在催化面还停在“[^”]+”", "眼下更卡在催化面还缺新增直接情报确认"),
        (r"在催化面还停在“[^”]+”改善前", "在催化面新增直接情报确认回来前"),
        (r"眼下更卡在风险特征还停在“[^”]+”", "眼下更卡在风险收益比还不够舒服"),
        (r"在风险特征还停在“[^”]+”改善前", "在风险收益比重新变舒服前"),
        (r"眼下更卡在相对强弱还停在“[^”]+”", "眼下更卡在相对强弱还没转强"),
        (r"在相对强弱还停在“[^”]+”改善前", "在相对强弱重新转强前"),
        (r"眼下更卡在季节/日历还停在“[^”]+”", "眼下更卡在时间窗口还不占优"),
        (r"在季节/日历还停在“[^”]+”改善前", "在时间窗口重新改善前"),
        (r"先看催化面里的“[^”]+”能不能先改善", "先看新增直接情报能不能先补回来"),
        (r"先看风险特征里的“[^”]+”能不能先改善", "先看风险收益比能不能先修复"),
        (r"先看相对强弱里的“[^”]+”能不能先改善", "先看相对强弱能不能先转强"),
        (r"先看季节/日历里的“[^”]+”能不能先改善", "先看时间窗口能不能先改善"),
        (r"先等催化面还停在“[^”]+”改善，再讨论第二笔", "先等新增直接情报确认回来，再讨论第二笔"),
        (r"先等风险特征还停在“[^”]+”改善，再讨论第二笔", "先等风险收益比修复后，再讨论第二笔"),
        (r"先等相对强弱还停在“[^”]+”改善，再讨论第二笔", "先等相对强弱转强后，再讨论第二笔"),
        (r"先等季节/日历还停在“[^”]+”改善，再讨论第二笔", "先等时间窗口改善后，再讨论第二笔"),
        (r"开盘\s*30\s*分钟", "早段"),
        (r"开盘后先观察\s*\d+\s*分钟", "先观察早段延续性"),
        (r"美股开盘前观察", "晚间外盘观察"),
        (r"开盘前观察", "盘前观察"),
        (r"明天开盘前", "明早"),
        (r"盘中", "交易时段"),
        (r"隔日涨跌", "短期涨跌"),
        (r"只按隔夜消息", "只按单条消息"),
        (r"纯隔夜交易", "纯超短交易"),
        (r"隔夜交易", "超短交易"),
        (r"^(.*?)[：:]\s*\[Errno [^\]]+\][^。；]*$", r"\1：当前数据源暂不可用，已按可用数据降级处理。"),
        (r"^(.*?)[：:]\s*Operation not permitted:[^。；]*$", r"\1：当前数据源暂不可用，已按可用数据降级处理。"),
        (r"当前不可用，本轮按缺失处理：[^\n。；]*", "当前不可用，本轮按缺失处理。"),
    )
    for pattern, repl in replacements:
        line = re.sub(pattern, repl, line)
    line = line.replace("。。", "。")
    if "内部覆盖率摘要" in line:
        return ""
    return line


def _client_factor_rows(dimension: Mapping[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for row in _factor_rows(dict(dimension or {})):
        current = list(row)
        if len(current) >= 2:
            current[1] = _pick_client_safe_line(current[1]) or "—"
        if len(current) >= 3:
            current[2] = _pick_client_safe_line(current[2]) or "—"
        rows.append(current)
    return rows


def _client_catalyst_factor_rows(dimension: Mapping[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for row in _catalyst_factor_rows(dict(dimension or {})):
        current = list(row)
        if len(current) >= 3:
            current[2] = _pick_client_safe_line(current[2]) or "—"
        rows.append(current)
    return rows


def _client_visible_blind_spot_lines(items: Sequence[Any], *, focus_name: str = "") -> List[str]:
    lines: List[str] = []
    saw_scan_failure = False
    for raw in items:
        line = _pick_client_safe_line(raw).strip()
        if not line:
            continue
        if "扫描失败" in line:
            saw_scan_failure = True
            continue
        if line not in lines:
            lines.append(line)
    if saw_scan_failure:
        focus_blob = f"，不影响当前首页重点对象 `{focus_name}` 的正文判断" if focus_name else ""
        lines.insert(0, f"部分候选在完整分析阶段取数失败，已按可用数据降级处理{focus_blob}。")
    return lines[:3]


def _sanitize_client_markdown(text: str) -> str:
    lines: List[str] = []
    for raw in str(text or "").splitlines():
        stripped = raw.strip()
        if not stripped:
            lines.append(raw)
            continue
        lines.append(_pick_client_safe_line(raw))
    return "\n".join(lines)


def _client_safe_markdown_lines(lines: Sequence[str]) -> List[str]:
    sanitized: List[str] = []
    for raw in list(lines or []):
        text = str(raw)
        if not text.strip():
            sanitized.append(text)
            continue
        sanitized.append(_pick_client_safe_line(text))
    return sanitized


class ClientReportRenderer:
    """Render concise client-facing reports from structured payloads."""

    @staticmethod
    def _prepend_editor_homepage(
        markdown_text: str,
        homepage_markdown: str,
        *,
        lead_markdown: str = "",
    ) -> str:
        homepage = str(homepage_markdown or "").strip()
        lead = str(lead_markdown or "").strip()
        if not homepage:
            if not lead:
                return markdown_text.rstrip()
            lines = markdown_text.splitlines()
            if not lines:
                return lead
            title = lines[0]
            body = "\n".join(lines[1:]).lstrip()
            parts = [title, "", lead]
            if body:
                parts.extend(["", body])
            return "\n".join(parts).rstrip()
        homepage = _sanitize_client_markdown(homepage)
        lines = markdown_text.splitlines()
        if not lines:
            if lead:
                return "\n\n".join([lead, homepage]).rstrip()
            return homepage
        title = lines[0]
        body = "\n".join(lines[1:]).lstrip()
        parts = [title]
        if lead:
            parts.extend(["", lead])
        parts.extend(["", homepage])
        if body:
            parts.extend(["", body])
        return "\n".join(parts).rstrip()

    @staticmethod
    def _briefing_client_safe_line(text: Any) -> str:
        line = str(text).strip()
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

    @staticmethod
    def _rank_market_items(
        items: Sequence[Mapping[str, Any]],
        watch_symbols: set[str],
    ) -> List[Mapping[str, Any]]:
        return _shared_rank_market_items(items, watch_symbols)

    def render_stock_picks_detailed(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        day_theme = str(payload.get("day_theme", {}).get("label", "未识别"))
        regime = str(payload.get("regime", {}).get("current_regime", "unknown"))
        market_label = str(payload.get("market_label", "全市场")).strip() or "全市场"
        sector_filter = str(payload.get("sector_filter", "")).strip()
        top = list(payload.get("top") or [])
        coverage_items = list(payload.get("coverage_analyses") or top)
        watch_symbols = {
            str(item.get("symbol", ""))
            for item in (payload.get("watch_positive") or [])
            if str(item.get("symbol", "")).strip()
        }
        grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for item in top:
            grouped[_market_label(str(item.get("asset_type", "")))].append(item)
        coverage_grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for item in coverage_items:
            coverage_grouped[_market_label(str(item.get("asset_type", "")))].append(item)
        proxy_coverage_grouped = _stock_pick_proxy_coverage_grouped(top, coverage_items, watch_symbols)
        has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in top)
        stock_pick_packet = build_stock_pick_editor_packet(payload)
        summary_rows = _stock_pick_summary_rows(
            day_theme=day_theme,
            regime=regime,
            sector_filter=sector_filter,
            theme_playbook=dict(stock_pick_packet.get("theme_playbook") or {}),
            grouped=grouped,
            coverage_grouped=proxy_coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
            include_execution=False,
        )
        compact_observe = not has_actionable
        lead_markdown = _stock_pick_first_screen_block(
            grouped=grouped,
            coverage_grouped=proxy_coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
        )

        lines = [
            f"# {'今日个股推荐（详细版）' if has_actionable else '今日个股观察（详细版）'} | {generated_at}",
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 名单结构",
                    lead=(
                        "这段只保留名单结构、优先级和主题边界，不重复首屏已经给出的执行口径。"
                    ),
                ),
                "",
            ]
        )
        coverage = dict(payload.get("stock_pick_coverage") or {})
        coverage_lines = list(coverage.get("lines") or [])
        if coverage_lines:
            structured_preface = _structured_coverage_preface(coverage_lines, asset_label="个股")
            structure_driven_coverage = any(
                "结构化事件覆盖" in item
                and (
                    "高置信公司新闻覆盖 0%" in item
                    or "高置信公司级直连情报覆盖 0%" in item
                    or "高置信直接情报覆盖 0%" in item
                )
                for item in coverage_lines
            )
            if compact_observe:
                lines.extend(["## 数据完整度", ""])
                compact_note = (
                    f"{coverage.get('note', '未标注')} 分母是进入详细分析的样本，不是全市场扫描池。"
                )
                if structure_driven_coverage:
                    compact_note = f"{compact_note} 当前证据更偏结构化事件与公告日历，不是直连个股情报催化。"
                elif coverage_lines:
                    compact_note = f"{compact_note} {coverage_lines[0]}"
                lines.append(f"- {compact_note}")
            else:
                detail_note = (
                    f"{coverage.get('note', '未标注')} 分母是进入详细分析的样本，不是全市场扫描池。"
                )
                lines.append(f"**数据完整度：** {detail_note}")
                if structure_driven_coverage and structured_preface:
                    lines.append(f"- {structured_preface}")
                elif coverage_lines:
                    lines.append(f"- {coverage_lines[0]}")
            lines.append("")
        proxy_section = _proxy_contract_section(
            dict(payload.get("proxy_contract") or {}),
            regime=dict(payload.get("regime") or {}),
            heading="## 市场代理信号",
        )
        if proxy_section:
            lines.extend(proxy_section)
            lines.append("")
        regime_section = _regime_basis_section(dict(payload.get("regime") or {}), day_theme=day_theme)
        if regime_section:
            lines.extend(regime_section)
            lines.append("")
        event_digest_lines = _client_safe_markdown_lines(
            render_compact_event_digest_section(stock_pick_packet.get("event_digest") or {})
            if compact_observe
            else render_event_digest_section(stock_pick_packet.get("event_digest") or {})
        )
        if event_digest_lines:
            lines.extend(event_digest_lines)
            lines.append("")
        market_event_rows: List[Sequence[Any]] = []
        for bucket in (top, coverage_items, payload.get("watch_positive") or []):
            for item in bucket:
                for row in list(dict(item or {}).get("market_event_rows") or []):
                    if row:
                        market_event_rows.append(row)
        if market_event_rows:
            lines.extend(
                [
                    "## 关键证据",
                    "",
                    *_briefing_intelligence_board_lines(
                        {"market_event_rows": market_event_rows},
                        max_items=4,
                    ),
                    "",
                ]
            )
        what_changed_lines = (
            render_compact_what_changed_section(stock_pick_packet.get("what_changed") or {})
            if compact_observe
            else render_what_changed_section(stock_pick_packet.get("what_changed") or {})
        )
        if what_changed_lines:
            lines.extend(what_changed_lines)
            lines.append("")

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked_items = self._rank_market_items(items, watch_symbols)
            market_has_actionable = any(
                _analysis_is_actionable(item, watch_symbols)
                and str(item.get("symbol", "")).strip() not in watch_symbols
                for item in ranked_items
            )
            tracks = _market_recommendation_tracks(ranked_items, watch_symbols)
            track_summary = _market_track_summary_text(tracks)
            scope_text = _market_pick_scope_text(ranked_items[0], generated_at=str(payload.get("generated_at", "")))
            if market_has_actionable and track_summary:
                lines.append(f"- {market_name}{scope_text}{track_summary}")
            elif not market_has_actionable:
                watch_summary = _market_watch_summary_text(ranked_items, watch_symbols, allow_soft_short=has_actionable)
                if watch_summary:
                    lines.append(f"- {market_name}{scope_text}{watch_summary}")
            affordable_rows = _affordable_stock_rows(
                proxy_coverage_grouped.get(market_name, []),
                watch_symbols,
                actionable_only=has_actionable,
            )
            related_etf_rows = _related_etf_rows(proxy_coverage_grouped.get(market_name, []) or ranked_items, watch_symbols)
            if market_name == "A股" and affordable_rows:
                label = "低门槛可执行先看" if has_actionable else "低门槛继续跟踪先看"
                lines.append(f"- {market_name}{label}：`{affordable_rows[0][0].split(' (', 1)[0]}`")
            if market_name == "A股" and related_etf_rows:
                label = "关联ETF平替先看" if has_actionable else "关联ETF观察先看"
                lines.append(f"- {market_name}{label}：`{related_etf_rows[0][0].split(' (', 1)[0]}`")

        lines.extend(
            [
                "",
                (
                    "更适合的做法仍然是：`先小仓、等确认、分批做，不把一条观点直接打满。`"
                    if has_actionable
                    else "更适合的做法仍然是：`先记观察条件，不急着给仓位，等确认后再谈动作。`"
                ),
            ]
        )

        shared_reference_items = self._rank_market_items(top, watch_symbols)
        has_formal = any(_recommendation_bucket(item, watch_symbols) == "正式推荐" for item in top)
        seen_evidence_identities: set[tuple[str, str, str]] = set()
        if top and not has_formal:
            lines.extend(["", "## 催化证据来源", ""])
            lines.extend(_stock_pick_shared_evidence_lines(shared_reference_items, seen_identities=seen_evidence_identities))
            lines.extend(["", "## 历史相似样本附注" if not has_actionable else "## 历史相似样本验证", ""])
            if not has_actionable:
                lines.extend(_section_lead_lines("这层只保留边界参考，不单独支撑今天出手。"))
            lines.extend(_stock_pick_shared_signal_confidence_lines(shared_reference_items))

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked = self._rank_market_items(items, watch_symbols)
            market_has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in ranked)
            used_reason_lines: Counter[str] = Counter()
            lines.extend(["", f"## {market_name}", ""])
            if not compact_observe:
                lines.extend(_section_lead_lines("这段先看分层建议，再决定要不要往下读单票说明。"))

            table_rows = []
            for item in ranked[:5]:
                table_rows.append(
                    [
                        str(item.get("name", "")),
                        str(_score(item, "technical")),
                        str(_score(item, "fundamental")),
                        str(_score(item, "catalyst")),
                        str(_score(item, "relative_strength")),
                        str(_score(item, "risk")),
                        _recommendation_bucket(item, watch_symbols),
                    ]
                )
            lines.extend(_table(["标的", "技术", "基本面", "催化", "相对强弱", "风险", "结论"], table_rows))
            collective_watch_explanation = _market_collective_watch_explanation(ranked, watch_symbols)
            if compact_observe and collective_watch_explanation:
                lines.extend(["", f"- {collective_watch_explanation}"])

            track_rows = _market_track_rows(_market_recommendation_tracks(ranked, watch_symbols))
            watch_rows = _market_watch_rows(ranked, watch_symbols) if not market_has_actionable else []
            if track_rows:
                lines.extend(["", "### 第一批：核心主线", ""])
                lines.extend(_section_lead_lines("这批是今天最该先看的短线/中线主名单。"))
                lines.extend(_table(["层次", "标的", "更适合的周期", "为什么先看"], track_rows))
            elif watch_rows:
                lines.extend(["", "### 第一批：优先观察", ""])
                if compact_observe:
                    lines.extend(["", "- 这批只是更值得继续跟踪的观察名单，不代表现在就该出手。"])
                else:
                    lines.extend(_section_lead_lines("这批只是更值得继续跟踪的观察名单，不代表现在就该出手。"))
                lines.extend(_table(["层次", "标的", "更适合的周期", "为什么继续看"], watch_rows))

            if compact_observe:
                featured_visual_item = next((item for item in ranked if _ensure_analysis_visuals(item)), None)
                if featured_visual_item is not None:
                    visual_lines = _pick_visual_lines(_ensure_analysis_visuals(featured_visual_item), nested=True)
                    if visual_lines:
                        lines.append("")
                        lines.extend(visual_lines)
                trigger_rows = _market_watch_trigger_rows(ranked, watch_symbols)
                if trigger_rows:
                    lines.extend(["", "### 观察触发器", ""])
                    lines.extend(
                        _table(
                            ["层次", "标的", "为什么继续看", "主要卡点", "升级条件", "关键盯盘价位"],
                            trigger_rows,
                        )
                    )
                followup_rows = _market_followup_watch_rows(ranked, watch_symbols)
                lines.extend(["", "### 第二批：继续跟踪", ""])
                if followup_rows:
                    lines.extend(["", "- 第一批先看最接近触发的几只；这批是值得继续记在观察名单里的补充对象。"])
                    lines.extend(_table(["标的", "为什么还没进第一批", "现在更该看什么"], followup_rows))
                else:
                    lines.extend(
                        [
                            "",
                            "- 当前进入详细分析的观察票仍集中在第一批重点对象，暂时没有更后排但仍值得单列的第二批补充标的。",
                            "- 如果后续观察池继续扩容，新增补充对象会继续落在这里。",
                        ]
                    )
                affordable_rows = _affordable_stock_rows(
                    proxy_coverage_grouped.get(market_name, []),
                    watch_symbols,
                    actionable_only=False,
                )
                related_etf_rows = _related_etf_rows(proxy_coverage_grouped.get(market_name, []) or ranked, watch_symbols)
                if market_name == "A股":
                    lines.extend(["", "### 第二批：低门槛 / 观察替代", ""])
                    if affordable_rows or related_etf_rows:
                        lines.extend(["", "- 方向还值得跟踪，但不想直接扛高价或单票波动时，这批更适合先放在观察清单里。"])
                    else:
                        lines.extend(
                            [
                                "",
                                "- 当前没有更合适的低门槛股票或关联 ETF 替代对象，这一层先按缺失保留，不把旧模块静默吞掉。",
                            ]
                        )
                if market_name == "A股" and affordable_rows:
                    lines.extend(["", "#### 低门槛继续跟踪", ""])
                    lines.extend(_table(["标的", "一手参考", "更适合的周期", "为什么继续看"], affordable_rows))
                if market_name == "A股" and related_etf_rows:
                    lines.extend(["", "#### 关联ETF观察", ""])
                    lines.extend(_table(["关联ETF", "更适合替代哪个方向", "什么时候更适合用它"], related_etf_rows))
                continue

            affordable_rows = _affordable_stock_rows(
                proxy_coverage_grouped.get(market_name, []),
                watch_symbols,
                actionable_only=has_actionable,
            )
            related_etf_rows = _related_etf_rows(proxy_coverage_grouped.get(market_name, []) or ranked, watch_symbols)
            if market_name == "A股" and market_has_actionable and (affordable_rows or related_etf_rows):
                lines.extend(["", "### 第二批：低门槛 / 关联ETF", ""])
                lines.extend(_section_lead_lines("这批解决的是买不起龙头或不想硬扛单票波动时，先看什么。"))
            if market_name == "A股" and affordable_rows:
                lines.extend(["", "#### 低门槛可执行", ""])
                lines.extend(_table(["标的", "一手参考", "更适合的周期", "为什么先看"], affordable_rows))
            if market_name == "A股" and related_etf_rows and not market_has_actionable:
                lines.extend(["", "### 第二批：观察替代", ""])
                lines.extend(_section_lead_lines("单票暂时不出手，但方向还值得跟踪时，可以先用关联 ETF 做平滑观察。"))
                lines.extend(["", "#### 关联ETF观察", ""])
            elif market_name == "A股" and related_etf_rows:
                lines.extend(["", "#### 关联ETF平替", ""])
            if market_name == "A股" and related_etf_rows:
                lines.extend(_table(["关联ETF", "更适合替代哪个方向", "什么时候更适合用它"], related_etf_rows))

            for item in ranked[:3]:
                bucket = _recommendation_bucket(item, watch_symbols)
                if bucket == "正式推荐":
                    lines.extend(
                        [
                            "",
                            f"### {item.get('name', '—')} ({item.get('symbol', '—')}) | {bucket}",
                            "",
                            f"**一句话判断：** {str(item.get('conclusion', '')).strip() or _bucket_summary_text(bucket, item)}",
                            "",
                        ]
                    )
                    visual_lines = _pick_visual_lines(item.get("visuals"), nested=True)
                    if not visual_lines:
                        visual_lines = _pick_visual_lines(_ensure_analysis_visuals(item), nested=True)
                    if visual_lines:
                        lines.extend(visual_lines)
                        lines.append("")
                    lines.extend(
                        [
                            *_analysis_section_lines(
                                item,
                                bucket,
                                day_theme=day_theme,
                                used_positive_reasons=used_reason_lines,
                                used_caution_reasons=used_reason_lines,
                                generated_at=str(payload.get("generated_at", "")),
                            ),
                            "",
                            "**八维雷达：**",
                        ]
                    )
                    lines.extend(_table(["维度", "得分", "核心信号"], _detail_dimension_rows(dict(item))))

                    catalyst_dimension = dict(item.get("dimensions", {}).get("catalyst") or {})
                    lines.extend(
                        [
                            "",
                            f"**催化拆解：** 当前催化分 `{catalyst_dimension.get('score', '缺失')}/{catalyst_dimension.get('max_score', 100)}`。",
                        ]
                    )
                    lines.extend(_table(["层次", "当前判断", "说明"], _catalyst_structure_rows(catalyst_dimension)))
                    lines.append("")
                    lines.extend(_table(["催化子项", "层级", "当前信号", "得分"], _client_catalyst_factor_rows(catalyst_dimension)))
                    if any(str(factor.get("display_score", "")).startswith("-") for factor in catalyst_dimension.get("factors", [])):
                        lines.extend(["", "- 注：催化总分按 0 封底；负面事件会先体现在子项扣分和正文风险提示里。"])
                    evidence = list(catalyst_dimension.get("evidence") or [])
                    evidence_lines = _evidence_lines_with_event_digest(
                        evidence,
                        event_digest=build_scan_editor_packet(item, bucket=_recommendation_bucket(item, watch_symbols)).get("event_digest") or {},
                        max_items=2,
                        as_of=item.get("generated_at") or payload.get("generated_at"),
                        symbol=item.get("symbol"),
                        seen_identities=seen_evidence_identities,
                    )
                    lines.extend(["", "**催化证据来源：**", ""])
                    if evidence_lines:
                        lines.extend(evidence_lines)
                    elif evidence:
                        lines.append("同类前置证据已在本报告前文共享区展示，这里不再重复展开。")
                    else:
                        provenance = dict(item.get("provenance") or build_analysis_provenance(item))
                        lines.append(
                            "当前没有高置信直连情报催化，先按 `"
                            + str(provenance.get("catalyst_sources_text", "结构化事件/代理来源"))
                            + "` 这层来源理解。"
                        )
                    lines.extend(["", "**证据时点与来源：**", ""])
                    lines.extend(_table(["项目", "说明"], _analysis_provenance_rows(item)))

                    lines.extend(
                        [
                            "",
                            f"**硬排除检查：** {_hard_check_inline(dict(item))}",
                        ]
                    )
                    lines.extend(_table(["检查项", "状态", "说明"], _hard_check_rows(dict(item))))

                    risk_dimension = dict(item.get("dimensions", {}).get("risk") or {})
                    lines.extend(
                        [
                            "",
                            f"**风险拆解：** 当前风险分 `{risk_dimension.get('score', '缺失')}/{risk_dimension.get('max_score', 100)}`。分数越低，说明波动、窗口和相关性压力越大。",
                        ]
                    )
                    lines.extend(_table(["风险子项", "当前信号", "说明", "得分"], _client_factor_rows(risk_dimension)))
                    lines.extend(["", *_signal_confidence_lines(item)])
                else:
                    lines.extend(
                        [
                            "",
                            *_analysis_watch_card_lines(
                                item,
                                bucket,
                                day_theme=day_theme,
                                used_positive_reasons=used_reason_lines,
                                used_caution_reasons=used_reason_lines,
                                generated_at=str(payload.get("generated_at", "")),
                                seen_identities=seen_evidence_identities,
                            ),
                        ]
                    )

        if compact_observe:
            observe_card_items = _top_observe_card_items(shared_reference_items, watch_symbols, limit=3)
            if observe_card_items:
                observe_positive_reasons: Counter[str] = Counter()
                observe_caution_reasons: Counter[str] = Counter()
                lines.extend(["", "## 观察名单复核卡", ""])
                lines.extend(["", "- 前面先给触发器和第二批，这里把最该盯的 2-3 只压成轻量复核卡，方便你直接比较排序理由。"])
                for item in observe_card_items:
                    lines.extend(
                        [
                            "",
                            *_analysis_watch_card_lines(
                                item,
                                _recommendation_bucket(item, watch_symbols),
                                day_theme=day_theme,
                                used_positive_reasons=observe_positive_reasons,
                                used_caution_reasons=observe_caution_reasons,
                                generated_at=str(payload.get("generated_at", "")),
                                seen_identities=seen_evidence_identities,
                            ),
                        ]
                    )
            representative_item = next((item for item in shared_reference_items if not _analysis_is_actionable(item, watch_symbols)), None)
            if representative_item is not None:
                lines.extend(["", "## 代表样本复核卡", ""])
                lines.extend(["", "- 上面先比排序理由，这里再留 1 只代表样本做组合预演和执行口径复核。"])
                lines.extend(
                    [
                        "",
                        *_observe_representative_card_lines(
                            representative_item,
                            watch_symbols=watch_symbols,
                            day_theme=day_theme,
                            seen_identities=seen_evidence_identities,
                        ),
                    ]
                )
            lines.extend(
                [
                    "",
                    "## 仓位纪律",
                    "",
                    "- 当前没有正式动作票，空仓先不新开仓；已有仓位只按止损和关键位管理。",
                    "- 这份观察稿更适合回答“谁最接近触发、触发条件是什么”，不适合直接当作分散建仓清单。",
                ]
            )
        else:
            lines.extend(
                [
                    "## 仓位管理",
                    "",
                    "这份推荐的前提不是“看对了就重仓”，而是先把错误成本控住。",
                    "",
                ]
            )
            lines.extend(
                _table(
                    ["场景", "建议"],
                    [
                        ["首次建仓", "单只个股首次更适合 `2% - 5%` 试仓"],
                        ["加仓条件", "只有在回踩确认、趋势延续或催化兑现后再加"],
                        ["单票上限", "高波动票尽量不要超过账户的 `8% - 10%`"],
                        ["执行原则", "先管仓位，再谈观点对不对"],
                    ],
                )
            )

        blind_spots = [str(item).strip() for item in (payload.get("blind_spots") or []) if str(item).strip()]
        if blind_spots:
            lines.extend(["", "## 数据限制与说明", ""])
            for item in (blind_spots[:1] if compact_observe else blind_spots):
                lines.append(f"- {item}")

        rendered = _sanitize_client_markdown("\n".join(lines).rstrip())
        homepage = _compress_multiname_homepage_action_section(
            render_editor_homepage(stock_pick_packet),
            stance_lines=[
                (
                    "组合先只开最接近确认的 1-2 只，不扩散到整份名单。"
                    if has_actionable
                    else "今天先看观察名单和升级条件，不把观察票直接写成下单票。"
                )
            ],
        )
        rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
        return _tighten_observe_client_markdown(rendered) if compact_observe else rendered

    def render_stock_picks(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        day_theme = str(payload.get("day_theme", {}).get("label", "未识别"))
        regime = str(payload.get("regime", {}).get("current_regime", "unknown"))
        market_label = str(payload.get("market_label", "")).strip()
        sector_filter = str(payload.get("sector_filter", "")).strip()
        top = list(payload.get("top") or [])
        coverage_items = list(payload.get("coverage_analyses") or top)
        watch_symbols = {
            str(item.get("symbol", ""))
            for item in (payload.get("watch_positive") or [])
            if str(item.get("symbol", "")).strip()
        }
        grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for item in top:
            grouped[_market_label(str(item.get("asset_type", "")))].append(item)
        coverage_grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for item in coverage_items:
            coverage_grouped[_market_label(str(item.get("asset_type", "")))].append(item)
        proxy_coverage_grouped = _stock_pick_proxy_coverage_grouped(top, coverage_items, watch_symbols)
        used_reason_lines: Counter[str] = Counter()
        has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in top)
        compact_observe = not has_actionable
        stock_pick_packet = build_stock_pick_editor_packet(payload)
        summary_rows = _stock_pick_summary_rows(
            day_theme=day_theme,
            regime=regime,
            sector_filter=sector_filter,
            theme_playbook=dict(stock_pick_packet.get("theme_playbook") or {}),
            grouped=grouped,
            coverage_grouped=proxy_coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
            include_execution=False,
        )
        lead_markdown = _stock_pick_first_screen_block(
            grouped=grouped,
            coverage_grouped=proxy_coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
            expanded_view_text=True,
        )

        lines = [
            f"# {'今日个股推荐' if has_actionable else '今日个股观察'} | {generated_at}",
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 名单结构",
                    lead="这段只保留名单结构、优先级和主题边界，不重复首屏已经给出的执行口径。",
                ),
                "",
            ]
        )
        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked_items = self._rank_market_items(items, watch_symbols)
            market_has_actionable = any(
                _analysis_is_actionable(item, watch_symbols)
                and str(item.get("symbol", "")).strip() not in watch_symbols
                for item in ranked_items
            )
            tracks = _market_recommendation_tracks(ranked_items, watch_symbols)
            track_summary = _market_track_summary_text(tracks)
            scope_text = _market_pick_scope_text(ranked_items[0], generated_at=str(payload.get("generated_at", "")))
            if market_has_actionable and track_summary:
                lines.append(f"- {market_name}{scope_text}{track_summary}")
            elif not market_has_actionable:
                watch_summary = _market_watch_summary_text(ranked_items, watch_symbols, allow_soft_short=has_actionable)
                if watch_summary:
                    lines.append(f"- {market_name}{scope_text}{watch_summary}")
            affordable_rows = _affordable_stock_rows(proxy_coverage_grouped.get(market_name, []), watch_symbols)
            related_etf_rows = _related_etf_rows(proxy_coverage_grouped.get(market_name, []) or ranked_items, watch_symbols)
            if market_name == "A股" and affordable_rows:
                label = "低门槛可执行先看" if has_actionable else "低门槛继续跟踪先看"
                lines.append(f"- {market_name}{label}：`{affordable_rows[0][0].split(' (', 1)[0]}`")
            if market_name == "A股" and related_etf_rows:
                label = "关联ETF平替先看" if has_actionable else "关联ETF观察先看"
                lines.append(f"- {market_name}{label}：`{related_etf_rows[0][0].split(' (', 1)[0]}`")
        lines.extend(
            [
                "",
                (
                    "今天没有哪只票适合一把梭。更合理的做法仍然是：`先小仓，等回踩或确认后再加。`"
                    if has_actionable
                    else "今天先不出手更合理。更适合的做法仍然是：`先看触发条件，等确认后再谈仓位。`"
                ),
            ]
        )
        proxy_section = _proxy_contract_section(
            dict(payload.get("proxy_contract") or {}),
            regime=dict(payload.get("regime") or {}),
            heading="## 市场代理信号",
        )
        if proxy_section:
            lines.extend(["", *proxy_section])
        regime_section = _regime_basis_section(dict(payload.get("regime") or {}), day_theme=day_theme)
        if regime_section:
            lines.extend(["", *regime_section])
        event_digest_lines = _client_safe_markdown_lines(
            render_compact_event_digest_section(stock_pick_packet.get("event_digest") or {})
            if compact_observe
            else render_event_digest_section(stock_pick_packet.get("event_digest") or {})
        )
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = (
            render_compact_what_changed_section(stock_pick_packet.get("what_changed") or {})
            if compact_observe
            else render_what_changed_section(stock_pick_packet.get("what_changed") or {})
        )
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
        market_event_rows: List[Sequence[Any]] = []
        for bucket in (top, coverage_items, payload.get("watch_positive") or []):
            for item in bucket:
                for row in list(dict(item or {}).get("market_event_rows") or []):
                    if row:
                        market_event_rows.append(row)
        if market_event_rows:
            lines.extend(
                [
                    "",
                    "## 关键证据",
                    "",
                    *_briefing_intelligence_board_lines(
                        {"market_event_rows": market_event_rows},
                        max_items=4,
                    ),
                ]
            )

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked = self._rank_market_items(items, watch_symbols)
            market_has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in ranked)
            lines.extend(
                [
                    "",
                    f"## {market_name}",
                    "",
                ]
            )
            lines.extend(_section_lead_lines("这段先看分层建议，再决定要不要往下读单票原因和动作。"))
            table_rows = []
            for item in ranked[:4]:
                table_rows.append(
                    [
                        str(item.get("name", "")),
                        str(_score(item, "technical")),
                        str(_score(item, "fundamental")),
                        str(_score(item, "catalyst")),
                        str(_score(item, "relative_strength")),
                        str(_score(item, "risk")),
                        _recommendation_bucket(item, watch_symbols),
                    ]
                )
            lines.extend(_table(["标的", "技术", "基本面", "催化", "相对强弱", "风险", "结论"], table_rows))

            track_rows = _market_track_rows(_market_recommendation_tracks(ranked, watch_symbols))
            watch_rows = _market_watch_rows(ranked, watch_symbols) if not market_has_actionable else []
            if track_rows:
                lines.extend(["", "### 第一批：核心主线", ""])
                lines.extend(_section_lead_lines("这批是今天的主名单，先看短线和中线各是谁。"))
                lines.extend(_table(["层次", "标的", "更适合的周期", "为什么先看"], track_rows))
            elif watch_rows:
                lines.extend(["", "### 第一批：优先观察", ""])
                lines.extend(_section_lead_lines("这批只是更值得继续跟踪的观察名单，不代表现在就该出手。"))
                lines.extend(_table(["层次", "标的", "更适合的周期", "为什么继续看"], watch_rows))

            affordable_rows = _affordable_stock_rows(
                proxy_coverage_grouped.get(market_name, []),
                watch_symbols,
                actionable_only=market_has_actionable,
            )
            related_etf_rows = _related_etf_rows(proxy_coverage_grouped.get(market_name, []) or ranked, watch_symbols)
            if market_name == "A股" and market_has_actionable and (affordable_rows or related_etf_rows):
                lines.extend(["", "### 第二批：低门槛 / 关联ETF", ""])
                lines.extend(_section_lead_lines("这批解决的是单价门槛太高或更想先用ETF承接方向时看什么。"))
            elif market_name == "A股" and (affordable_rows or related_etf_rows):
                lines.extend(["", "### 第二批：低门槛 / 观察替代", ""])
                lines.extend(_section_lead_lines("方向还值得跟踪，但不想直接扛高价或单票波动时，可以先看这批补位选择。"))
            if market_name == "A股" and affordable_rows:
                heading = "#### 低门槛可执行" if market_has_actionable else "#### 低门槛继续跟踪"
                reason_label = "为什么先看" if market_has_actionable else "为什么继续看"
                lines.extend(["", heading, ""])
                lines.extend(_table(["标的", "一手参考", "更适合的周期", reason_label], affordable_rows))
            if market_name == "A股" and related_etf_rows and not market_has_actionable:
                lines.extend(["", "#### 关联ETF观察", ""])
            elif market_name == "A股" and related_etf_rows:
                lines.extend(["", "#### 关联ETF平替", ""])
            if market_name == "A股" and related_etf_rows:
                lines.extend(_table(["关联ETF", "更适合替代哪个方向", "什么时候更适合用它"], related_etf_rows))

            formal = [item for item in ranked if _recommendation_bucket(item, watch_symbols) == "正式推荐"]
            for item in formal[:3]:
                lines.extend(
                    [
                        "",
                        *_analysis_section_lines(
                            item,
                            "正式推荐",
                            day_theme=day_theme,
                            used_positive_reasons=used_reason_lines,
                            used_caution_reasons=used_reason_lines,
                            generated_at=str(payload.get("generated_at", "")),
                        ),
                    ]
                )

            soft_watch_items = [item for item in ranked if _recommendation_bucket(item, watch_symbols) == "看好但暂不推荐"]
            if soft_watch_items:
                lines.extend(["", "### 看好但暂不推荐", ""])
                for item in soft_watch_items[:2]:
                    lines.extend(
                        _analysis_section_lines(
                            item,
                            "看好但暂不推荐",
                            day_theme=day_theme,
                            used_positive_reasons=used_reason_lines,
                            used_caution_reasons=used_reason_lines,
                            generated_at=str(payload.get("generated_at", "")),
                        )
                    )
                    lines.append("")

            observe_items = [item for item in ranked if _recommendation_bucket(item, watch_symbols) == "观察为主"]
            if observe_items:
                lines.extend(["", "### 观察为主", ""])
                for item in observe_items[:2]:
                    lines.extend(
                        _analysis_section_lines(
                            item,
                            "观察为主",
                            day_theme=day_theme,
                            used_positive_reasons=used_reason_lines,
                            used_caution_reasons=used_reason_lines,
                            generated_at=str(payload.get("generated_at", "")),
                        )
                    )
                    lines.append("")

        lines.extend(
            [
                "## 仓位管理",
                "",
                "这份个股推荐不是“看到名字就直接满仓买”。更合理的是按风险分层：",
                "",
            ]
        )
        lines.extend(
            _table(
                ["场景", "建议"],
                [
                    ["首次建仓", "单只个股首次更适合 `2% - 5%` 试仓"],
                    ["加仓条件", "只有在回踩确认、趋势延续或催化兑现后再加"],
                    ["单票上限", "高波动票尽量不要超过账户的 `8% - 10%`"],
                    ["执行原则", "先管仓位，再谈观点对不对"],
                ],
            )
        )
        rendered = _sanitize_client_markdown("\n".join(lines).rstrip())
        homepage = _compress_multiname_homepage_action_section(
            render_editor_homepage(stock_pick_packet),
            stance_lines=[
                (
                    "组合先只开最接近确认的 1-2 只，不扩散到整份名单。"
                    if has_actionable
                    else "今天先看观察名单和升级条件，不把观察票直接写成下单票。"
                )
            ],
        )
        rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
        return _tighten_observe_client_markdown(rendered) if compact_observe else rendered

    def render_scan_detailed(
        self,
        analysis: Dict[str, Any],
        *,
        prepend_homepage: bool = True,
        keep_observe_execution: bool = False,
    ) -> str:
        bucket = _recommendation_bucket(analysis)
        compact_observe = bucket != "正式推荐"
        asset_type = str(analysis.get("asset_type", "")).strip()
        packet = build_scan_editor_packet(analysis, bucket=bucket)
        if not keep_observe_execution and asset_type in {"cn_etf", "cn_fund"}:
            keep_observe_execution = True
        rendered = OpportunityReportRenderer().render_scan(analysis, visuals=analysis.get("visuals"))
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        if bucket == "观察为主":
            compact_rendered = self.render_scan(analysis)
            if not keep_observe_execution:
                compact_rendered = _trim_observe_only_scan_execution(compact_rendered, analysis)
            if not prepend_homepage:
                marker = "\n## 一句话结论\n"
                if marker in compact_rendered:
                    title_line = compact_rendered.splitlines()[0] if compact_rendered.splitlines() else ""
                    compact_rendered = f"{title_line}{marker}{compact_rendered.split(marker, 1)[1]}"
            compact_rendered = _upsert_visual_section(
                compact_rendered,
                _ensure_analysis_visuals(analysis),
            )
            what_changed_lines = render_what_changed_section(packet.get("what_changed") or {})
            if what_changed_lines:
                compact_rendered = _replace_markdown_section(compact_rendered, "## What Changed", what_changed_lines)
            compact_rendered = _insert_stock_company_research_section(
                compact_rendered,
                analysis,
                bucket=bucket,
                playbook=packet.get("theme_playbook") or {},
            )
            lines = compact_rendered.splitlines()
            if lines and lines[0].startswith("# "):
                lines[0] = f"# {name} ({symbol}) | 详细分析 | {generated_at}"
            return "\n".join(lines).rstrip()
        lines = rendered.splitlines()
        if lines and lines[0].startswith("# "):
            lines[0] = f"# {name} ({symbol}) | 详细分析 | {generated_at}"
        rewritten = _rename_markdown_heading(
            "\n".join(lines).rstrip(),
            {
                "## 硬性检查": "## 硬检查",
                "## 值得继续看的理由": "## 值得继续看的地方",
                "## 现在不适合激进的理由": "## 现在不适合激进的地方",
                "## 操作建议": "## 当前更合适的动作",
                **({"## 观察方式": "## 当前更合适的动作"} if keep_observe_execution else {}),
                "## 分析元数据": "## 证据时点与来源",
            },
        )
        rewritten = _inject_scan_reasoning_table(rewritten, analysis)
        rewritten = _inject_scan_exec_summary(rewritten, analysis)
        if not keep_observe_execution:
            rewritten = _trim_observe_only_scan_execution(rewritten, analysis)
        rewritten = _replace_markdown_section(rewritten, "## 证据时点与来源", _analysis_provenance_lines(analysis))
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(packet.get("event_digest") or {}))
        if event_digest_lines:
            rewritten = _insert_markdown_section_before(rewritten, "## 关键证据", event_digest_lines)
            rewritten = _insert_markdown_section_before(rewritten, "## 证据时点与来源", event_digest_lines) if "## 事件消化" not in rewritten else rewritten
        what_changed_lines = render_what_changed_section(packet.get("what_changed") or {})
        if what_changed_lines:
            rewritten = _insert_markdown_section_before(rewritten, "## 关键证据", what_changed_lines)
            rewritten = _insert_markdown_section_before(rewritten, "## 证据时点与来源", what_changed_lines) if "## What Changed" not in rewritten else rewritten
        horizon_lines = _index_horizon_summary_lines(analysis)
        if horizon_lines:
            rewritten = _insert_markdown_section_before(rewritten, "## 关键证据", horizon_lines)
        rewritten = _inject_scan_theme_context(rewritten, dict(packet.get("theme_playbook") or {}))
        rewritten = _rewrite_scan_theme_language(rewritten, analysis, packet)
        rewritten = _insert_stock_company_research_section(
            rewritten,
            analysis,
            bucket=bucket,
            playbook=packet.get("theme_playbook") or {},
        )
        sanitized = _sanitize_client_markdown(rewritten)
        if not prepend_homepage:
            return sanitized
        action = dict(analysis.get("action") or {})
        narrative = dict(analysis.get("narrative") or {})
        horizon = _pick_horizon_profile(
            action,
            str(dict(narrative.get("judgment") or {}).get("state", "")),
            context=name,
        )
        lead_markdown = _first_screen_execution_block(
            analysis,
            horizon,
            status_label=bucket,
            default_trigger="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
        )
        homepage = render_editor_homepage(packet)
        return self._prepend_editor_homepage(sanitized, homepage, lead_markdown=lead_markdown)

    def render_scan(self, analysis: Dict[str, Any]) -> str:
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        bucket = _recommendation_bucket(analysis)
        compact_observe = bucket != "正式推荐"
        narrative = dict(analysis.get("narrative") or {})
        action = dict(analysis.get("action") or {})
        horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")), context=name)
        handoff = portfolio_whatif_handoff(
            symbol=symbol,
            horizon=horizon,
            direction=str(action.get("direction", "")),
            asset_type=str(analysis.get("asset_type", "")),
            reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
            generated_at=str(analysis.get("generated_at", "")),
        )
        packet = build_scan_editor_packet(analysis, bucket=bucket)
        summary_rows = _single_asset_exec_summary_rows(
            analysis,
            horizon,
            handoff,
            theme_playbook=dict(packet.get("theme_playbook") or {}),
            status_label=bucket,
            default_trigger="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
            default_holder_text="已有仓位先按止损和关键位管理，不把当前判断直接当成继续加仓的理由。",
        )
        lead_markdown = _first_screen_execution_block(
            analysis,
            horizon,
            status_label=bucket,
            default_trigger="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
        )
        notes = [
            _pick_client_safe_line(str(item).strip())
            for item in (analysis.get("notes") or [])
            if _pick_client_safe_line(str(item).strip())
        ]
        upgrade_trigger_text = _pick_client_safe_line(_primary_upgrade_trigger(analysis))
        if bucket == "观察为主" and str(analysis.get("asset_type", "")).strip() == "cn_stock":
            upgrade_trigger_text = _pick_client_safe_line(
                _observe_trigger_condition(
                    analysis,
                    horizon,
                    default_text="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                    qualitative_only=False,
                )
            )
        lines = [
            f"# {name} ({symbol}) | 客户版分析 | {generated_at}",
            "",
            "## 一句话结论",
            "",
            f"`{_bucket_summary_text(bucket, analysis)}`",
            "",
            str(narrative.get("headline") or analysis.get("conclusion") or "").replace("**", ""),
            "",
            *_summary_block_lines(
                summary_rows,
                heading="## 执行摘要",
                lead="这段只回答能不能动、空仓和持仓分别怎么做、仓位大概多大，以及为什么不是更激进的动作。",
            ),
            "",
            f"**升级触发器：** {upgrade_trigger_text}",
            "",
            "## 为什么这么判断",
            "",
            *_section_lead_lines("这段先看八维里哪几项在支撑，哪几项在拖后腿。"),
        ]
        for item in _theme_playbook_explainer_lines(dict(packet.get("theme_playbook") or {}))[:2]:
            lines.append(f"- {item}")
        if _theme_playbook_explainer_lines(dict(packet.get("theme_playbook") or {}))[:2]:
            lines.append("")
        stock_front_lines = _stock_front_reason_lines(analysis)
        for item in stock_front_lines:
            lines.append(f"- {item}")
        if stock_front_lines:
            lines.append("")
        lines.extend(_table(["维度", "分数", "为什么是这个分"], _scan_dimension_rows(analysis)))
        hard_check_rows = _scan_hard_check_rows(analysis)
        if hard_check_rows:
            lines.extend(["", "## 硬检查", ""])
            lines.extend(_table(["检查项", "状态", "说明"], hard_check_rows))
        lines.extend(
            [
                "",
                "## 值得继续看的地方",
                "",
                *_section_lead_lines("这段只看还能支撑你继续观察或持有它的理由。"),
            ]
        )
        positives = _merge_reason_lines(
            list(narrative.get("positives") or []),
            _top_dimension_reasons(analysis, top_n=3),
            max_items=5,
        )
        for item in positives[:3]:
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "## 现在不适合激进的地方",
                "",
                *_section_lead_lines("这段只看今天不适合激进处理的拖累项。"),
            ]
        )
        caution_fallbacks = list(_bottom_dimension_reasons(analysis, top_n=3))
        entry_text = str(action.get("entry", "")).strip()
        if bucket == "观察为主" and str(analysis.get("asset_type", "")).strip() == "cn_stock":
            entry_text = _observe_trigger_condition(
                analysis,
                horizon,
                default_text="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                qualitative_only=False,
            )
        if entry_text:
            caution_fallbacks.append(f"执行层仍要求等待确认：{entry_text}")
        risk_summary = str(analysis.get("dimensions", {}).get("risk", {}).get("summary", "")).strip()
        if risk_summary:
            caution_fallbacks.append(f"风险特征提示：{risk_summary}")
        cautions = _merge_reason_lines(
            list(narrative.get("cautions") or []),
            caution_fallbacks,
            max_items=5,
        )
        for item in cautions[:3]:
            lines.append(f"- {item}")
        event_digest_lines = _client_safe_markdown_lines(
            render_compact_event_digest_section(packet.get("event_digest") or {})
            if compact_observe
            else render_event_digest_section(packet.get("event_digest") or {})
        )
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = (
            render_compact_what_changed_section(packet.get("what_changed") or {})
            if compact_observe
            else render_what_changed_section(packet.get("what_changed") or {})
        )
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
        horizon_lines = _index_horizon_summary_lines(analysis)
        if horizon_lines:
            lines.extend(["", *horizon_lines])
        portfolio_overlap_lines = _portfolio_overlap_lines(analysis)
        if portfolio_overlap_lines:
            lines.extend(["", "## 与现有持仓的关系", ""])
            for item in portfolio_overlap_lines:
                lines.append(f"- {item}")
        catalyst_dimension = dict(analysis.get("dimensions", {}).get("catalyst") or {})
        evidence_lines = _evidence_lines_with_event_digest(
            list(catalyst_dimension.get("evidence") or []),
            event_digest=packet.get("event_digest") or {},
            max_items=5,
            as_of=analysis.get("generated_at"),
            symbol=analysis.get("symbol"),
        )
        if evidence_lines:
            lines.extend(
                [
                    "",
                    "## 关键证据",
                    "",
                ]
            )
            lines.extend(evidence_lines)
        strong_factor_rows = _strong_factor_rows_from_dimensions(
            analysis.get("dimensions", {}),
            asset_type=str(analysis.get("asset_type", "")),
        )
        if strong_factor_rows:
            lines.extend(
                [
                    "",
                    "## 关键强因子拆解",
                    "",
                ]
            )
            lines.extend(_table(["因子", "当前信号", "这意味着什么"], strong_factor_rows))
        lines.extend(["", *_analysis_provenance_lines(analysis)])
        lines.extend(
            [
                "",
                "## 当前更合适的动作",
                "",
                *_section_lead_lines(
                    "先看动作、周期和触发条件，再决定要不要往下看区间和仓位。"
                    if bucket == "正式推荐"
                    else "先看动作和触发条件，再决定要不要给它执行优先级。"
                ),
            ]
        )
        show_precise_execution = bucket != "观察为主" and _shared_analysis_is_actionable(analysis)
        observe_current_action = _observe_current_action_label(analysis) if bucket == "观察为主" else action.get("direction", "观察为主")
        observe_allocation_view = _observe_soft_allocation_view(
            horizon.get("allocation_view") or str(dict(narrative.get("playbook") or {}).get("allocation", ""))
        )
        observe_watch_levels = _observe_watch_levels(
            analysis,
            qualitative_only=bucket == "观察为主" and str(analysis.get("asset_type", "")) != "cn_stock",
        )
        observe_entry_condition = _pick_client_safe_line(action.get("entry", "等待进一步确认"))
        if bucket == "观察为主" and str(analysis.get("asset_type", "")) == "cn_stock":
            trigger_condition = _strip_precise_price_disclaimer(
                _observe_trigger_condition(
                    analysis,
                    horizon,
                    default_text="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                    qualitative_only=False,
                )
            )
            entry_condition = _observe_stock_entry_text(analysis)
            observe_entry_condition = _pick_client_safe_line(
                f"{trigger_condition.rstrip('。；;,， ')}；{entry_condition}"
            )
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", observe_current_action],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["周期理由", _pick_client_safe_line(horizon.get("fit_reason", horizon.get("style", "先按当前动作、仓位和止损框架理解。")))],
                    ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把当前动作自动理解成另一种更长或更短的打法。"))],
                    *(
                        [["配置视角", observe_allocation_view]]
                        if observe_allocation_view
                        else []
                    ),
                    *(
                        [["交易视角", _pick_client_safe_line(horizon.get("trading_view") or str(dict(narrative.get("playbook") or {}).get("trend", "")))]]
                        if (horizon.get("trading_view") or str(dict(narrative.get("playbook") or {}).get("trend", "")).strip())
                        else []
                    ),
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["介入条件", observe_entry_condition],
                    [
                        "触发买点条件",
                        _strip_precise_price_disclaimer(
                            _observe_trigger_condition(
                                analysis,
                                horizon,
                                default_text="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                                qualitative_only=bucket == "观察为主" and str(analysis.get("asset_type", "")) != "cn_stock",
                            )
                        ),
                    ],
                    *_present_action_row("关键盯盘价位", observe_watch_levels),
                    *(
                        [
                            ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                            ["预演命令", f"`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`"],
                            *_present_action_row("建议买入区间", _safe_buy_range_text(action)),
                            ["首次仓位", _pick_client_safe_line(action.get("position", "小仓位分批"))],
                            ["加仓节奏", _pick_client_safe_line(action.get("scaling_plan", "确认后再考虑第二笔"))],
                            *_trim_execution_rows(action),
                            ["止损参考", _pick_client_safe_line(action.get("stop", "重新跌破关键支撑就处理"))],
                            *_present_action_row("目标参考", action.get("target", "")),
                        ]
                        if show_precise_execution
                        else (
                            [
                                ["执行参数", _observe_stock_execution_text(analysis)],
                                *_observe_stock_tactical_rows(analysis),
                            ]
                            if str(analysis.get("asset_type", "")) == "cn_stock"
                            else [[
                                "执行参数",
                                "当前是观察稿，不前置精确仓位、止损和目标模板；先看确认条件和关键位。",
                            ]]
                        )
                    ),
                ],
            )
        )
        if show_precise_execution:
            lines.extend(["", "## 仓位管理", ""])
            for item in _position_management_lines(action):
                lines.append(f"- {item}")
            lines.extend(["", "## 组合落单前", ""])
            lines.append(f"- {handoff.get('summary', '先跑组合预演，再决定真实金额。')}")
            lines.append(f"- 命令：`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`")
        signal_confidence_lines = _signal_confidence_lines(analysis) if "signal_confidence" in analysis else []
        if signal_confidence_lines:
            lines.extend(["", "## 历史相似样本验证", ""])
            lines.extend(signal_confidence_lines)
        fund_sections = _fund_profile_sections(analysis)
        if fund_sections:
            lines.extend(["", *fund_sections])
        lines.extend(["", "## 分维度详解", ""])
        ordered_dimensions = [
            ("technical", "技术面"),
            ("fundamental", "基本面"),
            ("catalyst", "催化面"),
            ("relative_strength", "相对强弱"),
            ("chips", "筹码结构"),
            ("risk", "风险特征"),
            ("seasonality", "季节/日历"),
            ("macro", "宏观敏感度"),
        ]
        for key, label in ordered_dimensions:
            dimension = analysis.get("dimensions", {}).get(key, {})
            display_label = _dimension_display_label(analysis, key, label, dimension)
            score = dimension.get("score")
            max_score = dimension.get("max_score", 100)
            display = "—" if score is None else f"{score}/{max_score}"
            lines.extend(
                [
                    "",
                    f"### {display_label} {display}",
                    "",
                    str(dimension.get("summary", "")).strip() or "当前没有额外说明。",
                    "",
                ]
            )
            factor_rows = _scan_factor_rows(dimension)
            if factor_rows:
                lines.extend(_table(["因子", "当前值/信号", "为什么这么评", "得分"], factor_rows))
        if notes:
            lines.extend(["", "## 数据限制与说明", ""])
            for item in (notes[:1] if compact_observe else notes[:3]):
                lines.append(f"- {item}")
        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(packet)
        rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
        if compact_observe:
            rendered = _tighten_observe_client_markdown(rendered)
            return rendered.replace("触发前别急着给精确买入价", "触发前先别急着给精确买入价")
        return rendered

    def render_stock_analysis(self, analysis: Dict[str, Any]) -> str:
        rendered = self.render_scan(analysis)
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        lines = rendered.splitlines()
        if lines:
            lines[0] = f"# {name} ({symbol}) | 个股详细分析 | {generated_at}"
        return "\n".join(lines).rstrip()

    def render_stock_analysis_detailed(self, analysis: Dict[str, Any]) -> str:
        rendered = self.render_scan_detailed(
            analysis,
            prepend_homepage=False,
            keep_observe_execution=True,
        )
        bucket = _recommendation_bucket(analysis)
        compact_observe = bucket != "正式推荐"
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        lines = rendered.splitlines()
        if lines and lines[0].startswith("# "):
            lines[0] = f"# {name} ({symbol}) | 个股详细分析 | {generated_at}"
        updated = "\n".join(lines).rstrip()
        stock_packet = build_stock_analysis_editor_packet({**analysis, "editor_bucket": bucket})
        stock_what_changed_lines = render_what_changed_section(stock_packet.get("what_changed") or {})
        if stock_what_changed_lines:
            updated = _replace_markdown_section(updated, "## What Changed", stock_what_changed_lines)
        updated = _insert_stock_company_research_section(
            updated,
            analysis,
            bucket=bucket,
            playbook=stock_packet.get("theme_playbook") or {},
        )
        action = dict(analysis.get("action") or {})
        narrative = dict(analysis.get("narrative") or {})
        horizon = _pick_horizon_profile(
            action,
            str(dict(narrative.get("judgment") or {}).get("state", "")),
            context=name,
        )
        lead_markdown = _first_screen_execution_block(
            analysis,
            horizon,
            status_label=bucket,
            default_trigger="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
        )
        homepage = render_editor_homepage(stock_packet)
        combined = self._prepend_editor_homepage(
            _insert_print_page_break_before(updated, "## 一句话结论"),
            homepage,
            lead_markdown=lead_markdown,
        )
        intraday_lines = _intraday_lines(analysis)
        if intraday_lines:
            combined = _insert_markdown_section_before(combined, '<div class="report-page-break"></div>', intraday_lines)
        if compact_observe:
            execution_section = _stock_observe_execution_section(analysis)
            if execution_section:
                combined = _insert_markdown_section_before(combined, "## 风险提示", execution_section)
            combined = _tighten_observe_client_markdown(combined)
            if stock_what_changed_lines:
                combined = _replace_markdown_section(combined, "## What Changed", stock_what_changed_lines)
            return combined.replace("触发前别急着给精确买入价", "触发前先别急着给精确买入价")
        return combined

    def render_briefing(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        headline_lines = [self._briefing_client_safe_line(item) for item in list(payload.get("headline_lines") or [])]
        action_lines = [self._briefing_client_safe_line(item) for item in list(payload.get("action_lines") or [])]
        quality_lines = [self._briefing_client_safe_line(item) for item in list(payload.get("quality_lines") or [])]
        theme_rows = list(payload.get("theme_tracking_rows") or [])
        verification_rows = list(payload.get("verification_rows") or [])
        macro_asset_rows = list(payload.get("macro_asset_rows") or [])
        macro_items = list(payload.get("macro_items") or [])
        a_share_watch_rows = list(payload.get("a_share_watch_rows") or [])
        a_share_watch_lines = [self._briefing_client_safe_line(item) for item in list(payload.get("a_share_watch_lines") or [])]
        a_share_watch_meta = dict(payload.get("a_share_watch_meta") or {})
        a_share_watch_candidates = list(payload.get("a_share_watch_candidates") or [])
        named_a_share_watch_candidates = [
            dict(item or {})
            for item in a_share_watch_candidates
            if not dict(item or {}).get("briefing_reuse_only")
            and (str(dict(item or {}).get("name", "")).strip() or str(dict(item or {}).get("symbol", "")).strip())
        ]
        portfolio_lines = [self._briefing_client_safe_line(item) for item in list(payload.get("portfolio_lines") or [])]
        portfolio_table_rows = list(payload.get("portfolio_table_rows") or [])
        data_coverage = str(payload.get("data_coverage", "")).strip()
        missing_sources = str(payload.get("missing_sources", "")).strip()
        regime = dict(payload.get("regime") or {})
        day_theme = str(payload.get("day_theme", "")).strip()
        evidence_rows = list(payload.get("evidence_rows") or [])
        sanitized_payload = {
            **payload,
            "headline_lines": headline_lines,
            "action_lines": action_lines,
            "quality_lines": quality_lines,
            "a_share_watch_lines": a_share_watch_lines,
            "portfolio_lines": portfolio_lines,
        }
        briefing_packet = build_briefing_editor_packet(sanitized_payload)
        intelligence_board_lines = _briefing_intelligence_board_lines(
            sanitized_payload,
            event_digest=briefing_packet.get("event_digest") or {},
            max_items=6,
        )
        why_lines = _briefing_reason_bullets(headline_lines, intelligence_board_lines, action_lines)
        normalized_evidence_rows = _briefing_evidence_rows(payload, evidence_rows)
        summary_theme_playbook = dict(briefing_packet.get("theme_playbook") or {})
        if not _theme_playbook_summary_rows(summary_theme_playbook) and named_a_share_watch_candidates:
            top_candidate = {**dict(named_a_share_watch_candidates[0] or {}), "day_theme": {"label": day_theme}}
            summary_theme_playbook = dict(build_scan_editor_packet(top_candidate, bucket="观察稿").get("theme_playbook") or {})
        summary_rows = _briefing_summary_rows(
            headline_lines=headline_lines,
            action_lines=action_lines,
            regime=regime,
            day_theme=day_theme,
            a_share_watch_meta=a_share_watch_meta,
            quality_lines=quality_lines,
            theme_playbook=summary_theme_playbook,
            index_signal_rows=list(payload.get("index_signal_rows") or []),
            market_signal_rows=list(payload.get("market_signal_rows") or []),
            rotation_rows=list(payload.get("rotation_rows") or []),
            include_execution=False,
        )
        watch_upgrade_lines = [self._briefing_client_safe_line(item) for item in _briefing_watch_upgrade_lines(named_a_share_watch_candidates)]
        strategy_summary_text = ""
        for item in named_a_share_watch_candidates[:2]:
            strategy_summary_text = _strategy_background_upgrade_text(item)
            if strategy_summary_text:
                break
        if strategy_summary_text:
            summary_rows = [*summary_rows, ["后台置信度", strategy_summary_text]]
        alerts = [self._briefing_client_safe_line(item) for item in list(payload.get("alerts") or [])]
        rich_usage_guidance = bool(
            macro_items
            or evidence_rows
            or quality_lines
            or dict(payload.get("proxy_contract") or {})
            or data_coverage
            or missing_sources
        )
        lead_markdown = _briefing_first_screen_block(
            headline_lines=headline_lines,
            action_lines=action_lines,
            verification_rows=verification_rows,
            theme_rows=theme_rows,
            alerts=alerts,
        )
        lines = [
            f"# 今日晨报 | {generated_at}",
            "",
        ]
        lines.extend(
            _summary_block_lines(
                summary_rows,
                heading="## 市场结构摘要",
                lead="这段只保留中期背景、市场结构、轮动和观察池边界，不重复首屏已经给出的执行口径。",
            )
        )
        lines.extend(["", "## 为什么今天这么判断", ""])
        if why_lines:
            for item in why_lines:
                lines.append(f"- {item}")
        else:
            lines.append("- 今天的判断不是看单一涨跌，而是看波动、主线和资金是否真正共振。")
        if intelligence_board_lines:
            lines.extend(["", "## 今日情报看板", ""])
            lines.extend(_section_lead_lines("这段只做广覆盖情报前置：优先看今天新增了什么、来自哪里、信号属于哪一层，以及强弱大概在哪。", emphasize=False))
            lines.extend(intelligence_board_lines)
        briefing_horizon_lines = _index_horizon_summary_lines(payload)
        if briefing_horizon_lines:
            lines.extend(["", *briefing_horizon_lines])
        regime_section = _regime_basis_section(regime, day_theme=day_theme, emphasize=False)
        if regime_section:
            lines.extend(["", *regime_section])
        if macro_items:
            lines.extend(["", "## 宏观领先指标", ""])
            for item in macro_items[:5]:
                lines.append(f"- {item}")
        lines.extend(["", "## 数据完整度", ""])
        lines.append(f"- 本次覆盖：{data_coverage or '未标注'}。")
        lines.append(f"- 当前缺失：{missing_sources or '无'}。")
        detail_lines = _briefing_quality_detail_lines(quality_lines)
        for item in detail_lines[:2]:
            lines.append(f"- {item}")
        proxy_section = _proxy_contract_section(
            dict(payload.get("proxy_contract") or {}),
            heading="## 市场代理信号",
            emphasize=False,
        )
        if proxy_section:
            lines.extend(["", *proxy_section])
        if normalized_evidence_rows:
            lines.extend(["", "## 证据时点与来源", ""])
            lines.extend(_table(["项目", "说明"], normalized_evidence_rows))
        lines.extend(["", "## 执行补充" if rich_usage_guidance else "## 怎么用这份晨报", ""])
        for item in _briefing_usage_lines(
            headline_lines,
            action_lines,
            verification_rows,
            theme_rows,
            strict_source_consistency=rich_usage_guidance,
        ):
            lines.append(f"- {item}")
        lines.extend(["", "## 这几个词怎么读", ""])
        for item in _briefing_term_translation_lines(
            verification_rows=verification_rows,
            a_share_watch_meta=a_share_watch_meta,
            theme_rows=theme_rows,
        ):
            lines.append(f"- {item}")
        lines.extend(["", "## 重点观察", ""])
        for item in _theme_playbook_explainer_lines(summary_theme_playbook)[:2]:
            lines.append(f"- {item}")
        if theme_rows:
            for row in theme_rows[:2]:
                direction = row[0] if len(row) > 0 else "重点方向"
                catalyst = row[1] if len(row) > 1 else "暂无"
                risk = row[4] if len(row) > 4 else "暂无"
                lines.append(f"- `{direction}`：先看 `{catalyst}` 是否延续，同时防范 `{risk}`。")
        elif verification_rows:
            for row in verification_rows[:2]:
                label = row[1] if len(row) > 1 else "验证点"
                criterion = row[2] if len(row) > 2 else "暂无"
                lines.append(f"- `{label}`：重点看 `{criterion}` 是否兑现。")
        else:
            lines.append("- 当前先看主线是否延续，再决定是否升级风险偏好。")
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(briefing_packet.get("event_digest") or {}))
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = render_what_changed_section(briefing_packet.get("what_changed") or {})
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
        if portfolio_lines or portfolio_table_rows:
            lines.extend(["", "## 组合与持仓", ""])
            for item in portfolio_lines[:5]:
                lines.append(f"- {item}")
            if portfolio_table_rows:
                lines.extend(
                    [
                        "",
                        *_table(
                            ["标的", "方向", "成本", "现价", "浮盈亏", "核心论点", "当前状态"],
                            portfolio_table_rows[:5],
                        ),
                    ]
                )
        lines.extend(["", "## 今日关键数据", ""])
        key_rows = macro_asset_rows[:5]
        if key_rows:
            lines.extend(_table(["资产", "最新价", "1日", "5日", "20日", "状态", "异常"], key_rows))
        else:
            lines.append("暂无关键资产快照。")
        lines.extend(["", "## 今日A股观察池", ""])
        if a_share_watch_lines:
            for item in a_share_watch_lines[:3]:
                lines.append(f"- {item}")
        else:
            lines.append("- A股全市场观察池暂未生成。")
        if a_share_watch_rows:
            lines.extend(
                [
                    "",
                    *_table(
                        ["排名", "标的", "行业", "评级", "当前状态", "首次仓位"],
                        a_share_watch_rows[:5],
                    ),
                ]
            )
        else:
            lines.extend(["", "暂无可用的 A 股全市场观察池结果。"])
        if watch_upgrade_lines:
            lines.extend(["", "## A股观察池升级条件", ""])
            lines.extend(_section_lead_lines("这段只回答观察池里的方向为什么还不能升成正式动作，以及接下来该盯什么触发器。", emphasize=False))
            for item in watch_upgrade_lines[:5]:
                lines.append(f"- {item}")
            if strategy_summary_text:
                lines.append("- 策略后台置信度只作辅助约束，不替代今天的宏观与主题判断。")
        lines.extend(["", "## 今天最值得看的 3 个方向", ""])
        if theme_rows:
            for index, row in enumerate(theme_rows[:3], start=1):
                direction = row[0] if len(row) > 0 else f"方向 {index}"
                catalyst = row[1] if len(row) > 1 else "暂无"
                logic = row[2] if len(row) > 2 else "暂无"
                risk = row[4] if len(row) > 4 else "暂无"
                info_env = row[5] if len(row) > 5 else ""
                lines.extend(
                    [
                        f"### {index}. {direction}",
                        "",
                        f"- 为什么值得看：{logic}",
                        f"- 直接催化：{catalyst}",
                        f"- 信息环境：{info_env or '当前更多是主线背景和盘面观察，不等于直接催化已经兑现。'}",
                        f"- 主要风险：{risk}",
                        "",
                    ]
                )
        else:
            lines.append("暂无明确主线方向。")
        lines.extend(["## 今天最该盯的验证点", ""])
        if verification_rows:
            lines.extend(_table(["验证点", "怎么判断", "如果成立", "如果不成立"], [row[1:] for row in verification_rows[:5]]))
        else:
            lines.append("暂无。")
        lines.extend(["", "## 今天最重要的风险提醒", ""])
        for item in alerts[:3]:
            lines.append(f"- {item}")
        rendered = "\n".join(lines).rstrip()
        homepage = _compress_multiname_homepage_action_section(
            render_editor_homepage(briefing_packet),
            stance_lines=[
                "今天先按结构性行情理解：主线若继续扩散，再提高风险暴露；如果验证不成立，就回到防守和观察。"
            ],
        )
        return self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)

    def render_fund_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        winner.setdefault("asset_type", "cn_fund")
        alternatives = list(payload.get("alternatives") or [])
        selection_context = _normalized_observe_packaging_context(
            dict(payload.get("selection_context") or {}),
            asset_label="场外基金",
        )
        playbook = dict(dict(winner.get("narrative") or {}).get("playbook") or {})
        horizon = _pick_horizon_profile(
            dict(winner.get("action") or {}),
            str(winner.get("trade_state", "")),
        )
        handoff = portfolio_whatif_handoff(
            symbol=str(winner.get("symbol", "")),
            horizon=horizon,
            direction=str(dict(winner.get("action") or {}).get("direction", "")),
            asset_type=str(winner.get("asset_type", "")) or "cn_fund",
            reference_price=winner.get("reference_price"),
            generated_at=str(winner.get("generated_at", "")) or str(payload.get("generated_at", "")),
        )
        observe_only = bool(selection_context.get("delivery_observe_only"))
        summary_only = _pick_delivery_summary_only(selection_context)
        compact_observe = observe_only
        theme_packet = build_fund_pick_editor_packet(payload)
        playbook = {**playbook, **dict(theme_packet.get("theme_playbook") or {})}
        signal_state = _pick_signal_state(winner)
        if signal_state.get("code") == "NO_SIGNAL":
            return _render_pick_no_signal_report(
                payload,
                winner,
                generated_at=generated_at,
                asset_label="场外基金",
                selection_context=selection_context,
                theme_packet=theme_packet,
            )
        summary_rows = _single_asset_exec_summary_rows(
            winner,
            horizon,
            handoff,
            selection_context=selection_context,
            theme_playbook=dict(theme_packet.get("theme_playbook") or {}),
            status_label=str(winner.get("trade_state", "")) or ("观察优先" if observe_only else "推荐"),
            default_trigger="先等覆盖率、申赎窗口和右侧确认一起改善，再决定要不要给买入区间。",
            default_holder_text="已有仓位先按止损和申赎节奏管理，不把当前判断直接当成继续加仓的理由。",
        )
        lead_markdown = _first_screen_execution_block(
            winner,
            horizon,
            selection_context=selection_context,
            status_label=str(winner.get("trade_state", "")) or ("观察优先" if observe_only else "推荐"),
            default_trigger="先等覆盖率、申赎窗口和右侧确认一起改善，再决定要不要给买入区间。",
        )
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        lead_line = (
            f"如果按{handoff.get('decision_scope', '今天的申赎决策')}先排一个观察优先的场外基金对象，我先看：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
            if observe_only
            else f"如果按{handoff.get('decision_scope', '今天的申赎决策')}只看一只场外基金，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
        )
        title = "今日场外基金观察" if observe_only else "今日场外基金推荐"
        lines = [
            f"# {title} | {generated_at}",
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 执行摘要",
                    lead="首页判断已经先给了总结论；这段只继续回答能不能申赎、空仓和持仓分别怎么处理、仓位大概多大，以及主要利好利空是什么。",
                ),
                "",
            ]
        )
        if observe_only and (not compact_observe or summary_only):
            lines.extend(
                _observe_delivery_threshold_lines(
                    asset_label="场外基金",
                    trade_state=str(winner.get("trade_state", "")),
                    delivery_label=str(selection_context.get("delivery_tier_label", "")),
                )
            )
            lines.extend([""])
            upgrade_lines = _pick_upgrade_lines(winner)
            if upgrade_lines and not summary_only:
                lines.extend(["## 升级条件", ""])
                lines.extend(upgrade_lines)
                lines.extend([""])
        visual_lines = _pick_visual_lines(winner.get("visuals"))
        if visual_lines:
            lines.extend(visual_lines)
            lines.append("")
        if selection_context:
            lines.extend(
                [
                    f"> 发现方式: {selection_context.get('discovery_mode_label', '未标注')} | 初筛池: {selection_context.get('scan_pool', '—')} | 完整分析: {selection_context.get('passed_pool', '—')}",
                    f"> 主题过滤: {selection_context.get('theme_filter_label', '未指定')} | 风格过滤: {selection_context.get('style_filter_label', '不限')} | 管理人过滤: {selection_context.get('manager_filter_label', '未指定')}",
                    "",
                ]
            )
        if compact_observe:
            lines.extend(_compact_pick_data_completeness_section(selection_context, asset_label="场外基金"))
            lines.extend(_compact_delivery_tier_section(selection_context, asset_label="场外基金"))
        else:
            lines.extend(["## 数据完整度", ""])
            lines.extend(_section_lead_lines("这段只回答这份稿靠不靠谱、哪些地方是降级或代理。", emphasize=False))
            if selection_context.get("coverage_note"):
                lines.append(f"- {selection_context.get('coverage_note')}")
            else:
                lines.append("- 当前没有额外覆盖率备注，默认按已进入完整分析的样本理解。")
            structured_preface = _structured_coverage_preface(selection_context.get("coverage_lines", []), asset_label="场外基金")
            if structured_preface:
                lines.append(f"- {structured_preface}")
            for item in selection_context.get("coverage_lines", [])[:2]:
                lines.append(f"- {item}")
            if selection_context.get("coverage_total"):
                lines.append(f"- 覆盖率的分母是今天进入完整分析的 `{selection_context.get('coverage_total')}` 只基金，不是全市场全部开放式基金。")
            if selection_context.get("baseline_snapshot_at"):
                role = "今天首个快照版" if selection_context.get("is_daily_baseline") else "今天修正版"
                lines.append(f"- 本次输出角色：{role}；当日首个基准快照时间是 `{selection_context.get('baseline_snapshot_at')}`。")
            if selection_context.get("comparison_basis_at"):
                basis_label = _client_safe_comparison_basis_label(selection_context.get("comparison_basis_label", "对比基准"))
                lines.append(
                    f"- 分数变化对比的是 `{basis_label} {selection_context.get('comparison_basis_at')}`。"
                )
            if selection_context.get("model_version_warning"):
                lines.append(f"- {selection_context.get('model_version_warning')}")
            lines.append("")
            lines.extend(_delivery_tier_section(selection_context, asset_label="场外基金"))
        lines.extend([""])
        lines.extend(_section_lead_lines("这段只回答这是不是正式成稿，能不能按正式推荐理解。", emphasize=False))
        proxy_section = _proxy_contract_section(
            dict(selection_context.get("proxy_contract") or {}),
            winner=winner,
            regime=dict(payload.get("regime") or {}),
            emphasize=False,
        )
        if proxy_section:
            lines.extend(proxy_section)
            lines.extend([""])
        regime_section = _regime_basis_section(
            dict(payload.get("regime") or {}),
            day_theme=str(payload.get("day_theme", {}).get("label", "")),
            emphasize=False,
        )
        if regime_section:
            lines.extend(regime_section)
            lines.extend([""])
        lines.extend(
            [
            why_heading,
            "",
            *_section_lead_lines("这段只看它为什么能进入今天名单，不重复展开全套动作。", emphasize=False),
            ]
        )
        for item in _theme_playbook_explainer_lines(dict(theme_packet.get("theme_playbook") or {}))[:2]:
            lines.append(f"- {item}")
        for item in winner.get("positives", [])[:3]:
            lines.append(f"- {item}")
        for item in _fund_front_reason_lines(winner):
            lines.append(f"- {item}")
        event_digest_lines = _client_safe_markdown_lines(
            render_compact_event_digest_section(theme_packet.get("event_digest") or {})
            if compact_observe
            else render_event_digest_section(theme_packet.get("event_digest") or {})
        )
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = (
            render_compact_what_changed_section(theme_packet.get("what_changed") or {})
            if compact_observe
            else render_what_changed_section(theme_packet.get("what_changed") or {})
        )
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
        structure_rows = [
            list(row)
            for row in (winner.get("market_event_rows") or [])
            if str(list(row)[2] if len(list(row)) > 2 else "").strip() in {"TDX结构专题", "DC结构专题", "港股/短线辅助", "转债辅助层", "研报辅助层"}
        ]
        if structure_rows:
            lines.extend(["", "## 结构辅助层", ""])
            lines.extend(
                _briefing_intelligence_board_lines(
                    {"market_event_rows": structure_rows},
                    max_items=4,
                )
            )
        horizon_lines = _index_horizon_summary_lines(winner)
        if horizon_lines:
            lines.extend(["", *horizon_lines])
        portfolio_overlap_lines = _portfolio_overlap_lines(winner)
        if portfolio_overlap_lines:
            lines.extend(["", "## 与现有持仓的关系", ""])
            for item in portfolio_overlap_lines:
                lines.append(f"- {item}")
        lines.extend(["", "## 这只基金为什么是这个分", ""])
        lines.extend(_section_lead_lines("这段先看分数结构，不要只盯最高分那一项。", emphasize=False))
        lines.extend(
            _table(
                ["维度", "分数", "为什么是这个分"],
                winner.get("dimension_rows", []),
            )
        )
        if observe_only:
            upgrade_lines = _pick_upgrade_lines(winner)
            if upgrade_lines:
                lines.extend(["", "## 升级条件", ""] if compact_observe else [""])
                lines.extend(upgrade_lines)
        if summary_only:
            lines.extend(["", "## 当前只看什么", ""])
            lines.extend(_section_lead_lines("今天先看动作和触发条件；没有触发前，不给精确买点。", emphasize=False))
            lines.extend(
                _table(
                    ["项目", "建议"],
                    _observe_pick_action_rows(
                        winner,
                        horizon,
                        handoff,
                        playbook,
                        default_reassessment="等覆盖率和右侧确认一起改善后再重新评估是否升级为完整推荐。",
                        default_trigger="先等覆盖率、申赎窗口和右侧确认一起改善，再决定要不要给买入区间。",
                    ),
                )
            )
            lines.extend(
                _summary_only_explainer_sections(
                    winner,
                    alternatives,
                    event_digest=theme_packet.get("event_digest") or {},
                    evidence_fallback="当前可前置的外部情报仍偏少，先结合覆盖率、基金画像和后文证据理解。",
                    no_alternative_text="当前可进入完整评分的基金候选不足，暂时没有可并列展开的第二候选。",
                )
            )
            blind_spots = [str(item).strip() for item in selection_context.get("blind_spots", []) if str(item).strip()]
            if blind_spots:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in (blind_spots[:1] if compact_observe else blind_spots[:3]):
                    lines.append(f"- {item}")
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
            if compact_observe:
                rendered = _tighten_observe_client_markdown(rendered)
                return rendered.replace("触发前别急着给精确买入价", "触发前先别急着给精确买入价")
            return rendered
        strong_factor_rows = _strong_factor_rows_from_dimensions(
            winner.get("dimensions", {}),
            asset_type=str(winner.get("asset_type", "")) or "cn_fund",
        )
        if strong_factor_rows:
            lines.extend(["", "## 关键强因子拆解", ""])
            lines.extend(_table(["因子", "当前信号", "这意味着什么"], strong_factor_rows))
        lines.extend(["", *_taxonomy_section(winner)])
        if winner.get("score_changes"):
            lines.extend(["", "## 跟今天首个快照版相比", ""])
            for item in winner.get("score_changes", [])[:4]:
                lines.append(
                    f"- `{item.get('label', '维度')}` 从 `{item.get('previous', '—')}` 变到 `{item.get('current', '—')}`：{item.get('reason', '')}"
                )
        evidence_lines = _evidence_lines_with_event_digest(
            list(winner.get("evidence") or []),
            event_digest=theme_packet.get("event_digest") or {},
            max_items=3,
            as_of=winner.get("generated_at") or payload.get("generated_at"),
            symbol=winner.get("symbol"),
            subject=winner,
        )
        lines.extend(["", "## 关键证据", ""])
        if evidence_lines:
            lines.extend(evidence_lines)
        else:
            lines.append("- 当前可直接复核的一手情报仍偏少，先结合结构化披露、基金画像和后文证据理解。")
        lines.extend(["", *_analysis_provenance_lines(winner)])
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _section_lead_lines(
                "今天先看动作和触发条件；没有触发前，不给精确买点。"
                if observe_only
                else "先看动作、买入区间和仓位，再决定要不要往下读细节。"
                ,
                emphasize=False,
            )
        )
        if observe_only:
            fund_rows = _observe_pick_action_rows(
                winner,
                horizon,
                handoff,
                playbook,
                default_reassessment="等确认信号更完整后，再决定是否升级成正式申赎动作。",
                default_trigger="先等申赎窗口、技术确认和覆盖率一起改善，再决定要不要给买入区间。",
            )
        else:
            fund_rows = [
                ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                ["持有周期", horizon.get("label", "未单独标注")],
                *_present_action_row(
                    "仓位身份",
                    (
                        f"`{_pick_client_safe_line(playbook.get('trading_role_label', ''))}` / `{_pick_client_safe_line(playbook.get('trading_position_label', ''))}`"
                        + (
                            f"：{_pick_client_safe_line(playbook.get('trading_role_summary', ''))}"
                            if _pick_client_safe_line(playbook.get("trading_role_summary", ""))
                            else ""
                        )
                    )
                    if _pick_client_safe_line(playbook.get("trading_role_label", ""))
                    and _pick_client_safe_line(playbook.get("trading_position_label", ""))
                    else "",
                ),
                ["适用打法", _pick_client_safe_line(horizon.get("style", "先按当前动作、仓位和止损框架理解，不把它默认当成长线配置。"))],
                ["为什么按这个周期看", _pick_client_safe_line(horizon.get("fit_reason", "当前更适合按已有动作和仓位框架理解。"))],
                ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把它自动理解成另一种更长或更短的打法。"))],
            ]
            if horizon.get("allocation_view") or str(playbook.get("allocation", "")).strip():
                fund_rows.append(["配置视角", _pick_client_safe_line(horizon.get("allocation_view") or str(playbook.get("allocation", "")))])
            if horizon.get("trading_view") or str(playbook.get("trend", "")).strip():
                fund_rows.append(["交易视角", _pick_client_safe_line(horizon.get("trading_view") or str(playbook.get("trend", "")))])
            fund_rows.extend(
                [
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可申赎窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', 'portfolio whatif buy 标的 最新净值 计划金额')}`"],
                    ["介入条件", _pick_client_safe_line(winner.get("action", {}).get("entry", "等回撤再看"))],
                    *_present_action_row("建议买入区间", _safe_buy_range_text(dict(winner.get("action") or {}))),
                    ["首次仓位", _pick_client_safe_line(winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2"))],
                    ["加仓节奏", _pick_client_safe_line(winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔"))],
                    *_trim_execution_rows(dict(winner.get("action") or {})),
                    ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
                ]
            )
        lines.extend(_table(["项目", "建议"], fund_rows))
        fund_sections = winner.get("fund_sections") or _fund_profile_sections(winner)
        if fund_sections:
            lines.extend(["", *fund_sections])
        if selection_context.get("blind_spots"):
            lines.extend(["", "## 数据限制与说明", ""])
            blind_spots = list(selection_context.get("blind_spots", []) or [])
            for item in (blind_spots[:1] if compact_observe else blind_spots[:3]):
                lines.append(f"- {item}")
        lines.extend(["", "## 为什么不是另外几只", ""])
        if alternatives:
            for index, item in enumerate(alternatives[:3], start=1):
                lines.extend(
                    [
                        f"### {index}. {item.get('name', '')} ({item.get('symbol', '')})",
                        "",
                    ]
                )
                for reason in item.get("cautions", [])[:3]:
                    lines.append(f"- {reason}")
                lines.append("")
        else:
            lines.append("- 今天可进入完整评分且未被硬排除的基金候选只有 1 只，没有形成可并列展开的第二候选。")
            if observe_only:
                lines.append("- 这也是本次只能按观察优先或降级稿处理、不能把它写成正式强推荐的原因之一。")
            else:
                lines.append("- 单候选不等于自动降级，是否按正式推荐理解仍以本页 `交付等级` 为准。")
        if observe_only:
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
            return _tighten_observe_client_markdown(rendered) if compact_observe else rendered
        lines.extend(
            [
                "## 仓位管理",
                "",
            ]
        )
        for item in winner.get("positioning_lines", []):
            lines.append(f"- {item}")
        lines.extend(["", "## 组合落单前", ""])
        lines.append(f"- {handoff.get('summary', '先跑组合预演，再决定真实金额。')}")
        lines.append(f"- 命令：`{handoff.get('command', 'portfolio whatif buy 标的 最新净值 计划金额')}`")
        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(theme_packet)
        return self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)

    def render_etf_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        winner.setdefault("asset_type", "cn_etf")
        alternatives = list(payload.get("alternatives") or [])
        recommendation_tracks = dict(payload.get("recommendation_tracks") or {})
        selection_context = _normalized_observe_packaging_context(
            dict(payload.get("selection_context") or {}),
            asset_label="ETF",
        )
        playbook = dict(dict(winner.get("narrative") or {}).get("playbook") or {})
        horizon = _pick_horizon_profile(
            dict(winner.get("action") or {}),
            str(winner.get("trade_state", "")),
        )
        handoff = portfolio_whatif_handoff(
            symbol=str(winner.get("symbol", "")),
            horizon=horizon,
            direction=str(dict(winner.get("action") or {}).get("direction", "")),
            asset_type=str(winner.get("asset_type", "")) or "cn_etf",
            reference_price=winner.get("reference_price"),
            generated_at=str(winner.get("generated_at", "")) or str(payload.get("generated_at", "")),
        )
        observe_only = bool(selection_context.get("delivery_observe_only"))
        summary_only = _pick_delivery_summary_only(selection_context)
        compact_observe = observe_only
        theme_packet = build_etf_pick_editor_packet(payload)
        playbook = {**playbook, **dict(theme_packet.get("theme_playbook") or {})}
        signal_state = _pick_signal_state(winner)
        if signal_state.get("code") == "NO_SIGNAL":
            return _render_pick_no_signal_report(
                payload,
                winner,
                generated_at=generated_at,
                asset_label="ETF",
                selection_context=selection_context,
                theme_packet=theme_packet,
            )
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        track_summary = _payload_track_summary_text(recommendation_tracks)
        track_rows = _payload_track_rows(recommendation_tracks)
        if observe_only:
            track_rows = _track_rows_with_subject_first(track_rows, winner)
        display_track_rows = _observe_pick_track_rows(track_rows) if observe_only else track_rows
        display_track_summary = _observe_pick_track_summary(display_track_rows) if observe_only else track_summary
        track_count = len(track_rows)
        if observe_only:
            if display_track_summary:
                if track_count >= 2:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先排观察顺序：{display_track_summary}"
                else:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先排观察优先级：{display_track_summary}"
            else:
                lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先排一个观察优先的 ETF 对象，我先看：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
        else:
            if display_track_summary:
                if track_count >= 2:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先分层理解：{display_track_summary}"
                else:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先看这一档：{display_track_summary}"
            else:
                lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}只看一只 ETF，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
        summary_rows = list(
            _single_asset_exec_summary_rows(
                winner,
                horizon,
                handoff,
                selection_context=selection_context,
                theme_playbook=dict(theme_packet.get("theme_playbook") or {}),
                status_label=str(winner.get("trade_state", "")) or ("观察优先" if observe_only else "推荐"),
                default_trigger="先等技术确认、催化覆盖和数据完整度一起改善，再决定要不要给买入区间。",
                default_holder_text="已有仓位先按止损和主线节奏管理，不把当前判断直接当成继续追涨的理由。",
            )
        )
        lead_markdown = _first_screen_execution_block(
            winner,
            horizon,
            selection_context=selection_context,
            status_label=str(winner.get("trade_state", "")) or ("观察优先" if observe_only else "推荐"),
            default_trigger="先等技术确认、催化覆盖和数据完整度一起改善，再决定要不要给买入区间。",
        )
        if display_track_rows:
            summary_rows = _track_row_summary(display_track_rows) + summary_rows
        title = "今日ETF观察" if observe_only else "今日ETF推荐"
        lines = [
            f"# {title} | {generated_at}",
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 执行摘要",
                    lead=(
                        "首页判断已经先给了总结论；这段只继续回答今天能不能动、空仓和持仓分别怎么做、仓位大概多大，以及观察顺序怎么排。"
                        if observe_only
                        else "首页判断已经先给了总结论；这段只继续回答今天能不能动、空仓和持仓分别怎么做、仓位大概多大，以及短线中线先看谁。"
                    ),
                ),
                "",
            ]
        )
        if observe_only and not compact_observe:
            lines.extend(
                _observe_delivery_threshold_lines(
                    asset_label="ETF",
                    trade_state=str(winner.get("trade_state", "")),
                    delivery_label=str(selection_context.get("delivery_tier_label", "")),
                )
            )
            lines.extend([""])
            upgrade_lines = _pick_upgrade_lines(winner)
            if upgrade_lines:
                lines.extend(["## 升级条件", ""])
                lines.extend(upgrade_lines)
                lines.extend([""])
        visual_lines = _pick_visual_lines(winner.get("visuals"))
        if visual_lines:
            lines.extend(visual_lines)
            lines.append("")
        if selection_context:
            preferred_sector_label = str(selection_context.get("preferred_sector_label", "")).strip()
            preferred_sector_suffix = f" | 偏好主题: {preferred_sector_label}" if preferred_sector_label and preferred_sector_label != "未指定" else ""
            lines.extend(
                [
                    f"> 发现方式: {selection_context.get('discovery_mode_label', '未标注')} | 初筛池: {selection_context.get('scan_pool', '—')} | 完整分析: {selection_context.get('passed_pool', '—')}",
                    f"> 主题过滤: {selection_context.get('theme_filter_label', '未指定')}{preferred_sector_suffix}",
                    "",
                ]
            )
        if compact_observe:
            lines.extend(_compact_pick_data_completeness_section(selection_context, asset_label="ETF"))
        else:
            lines.extend(["## 数据完整度", ""])
            if selection_context.get("coverage_note"):
                lines.append(f"- {selection_context.get('coverage_note')}")
            else:
                lines.append("- 当前没有额外覆盖率备注，默认按已进入完整分析的样本理解。")
            structured_preface = _structured_coverage_preface(selection_context.get("coverage_lines", []), asset_label="ETF")
            if structured_preface:
                lines.append(f"- {structured_preface}")
            for item in selection_context.get("coverage_lines", [])[:2]:
                lines.append(f"- {item}")
            if selection_context.get("coverage_total"):
                lines.append(f"- 覆盖率的分母是今天进入完整分析的 `{selection_context.get('coverage_total')}` 只 ETF，不是全市场全部 ETF。")
            if selection_context.get("baseline_snapshot_at"):
                role = "今天首个快照版" if selection_context.get("is_daily_baseline") else "今天修正版"
                lines.append(f"- 本次输出角色：{role}；当日首个基准快照时间是 `{selection_context.get('baseline_snapshot_at')}`。")
            if selection_context.get("comparison_basis_at"):
                basis_label = _client_safe_comparison_basis_label(selection_context.get("comparison_basis_label", "对比基准"))
                lines.append(
                    f"- 分数变化对比的是 `{basis_label} {selection_context.get('comparison_basis_at')}`。"
                )
            if selection_context.get("model_version_warning"):
                lines.append(f"- {selection_context.get('model_version_warning')}")
        lines.extend(_compact_delivery_tier_section(selection_context, asset_label="ETF") if compact_observe else _delivery_tier_section(selection_context, asset_label="ETF"))
        lines.extend([""])
        if observe_only:
            lines.extend(["## 这几个词怎么读", ""])
            for item in _etf_pick_term_translation_lines(winner, selection_context):
                lines.append(f"- {item}")
            lines.extend([""])
        proxy_section = _proxy_contract_section(
            dict(selection_context.get("proxy_contract") or {}),
            winner=winner,
            regime=dict(payload.get("regime") or {}),
        )
        if proxy_section:
            lines.extend(proxy_section)
            lines.extend([""])
        regime_section = _regime_basis_section(dict(payload.get("regime") or {}), day_theme=str(payload.get("day_theme", {}).get("label", "")))
        if regime_section:
            lines.extend(regime_section)
            lines.extend([""])
        if display_track_rows and not compact_observe:
            lines.extend(["", "## 当前分层建议", ""])
            lines.extend(_table(["层次", "标的", "更适合的周期", "为什么先看"], display_track_rows))
        lines.extend(["", why_heading, ""])
        for item in _theme_playbook_explainer_lines(dict(theme_packet.get("theme_playbook") or {}))[:2]:
            lines.append(f"- {item}")
        for item in winner.get("positives", [])[:4]:
            lines.append(f"- {item}")
        for item in _etf_front_reason_lines(winner):
            lines.append(f"- {item}")
        event_digest_lines = _client_safe_markdown_lines(
            render_compact_event_digest_section(theme_packet.get("event_digest") or {})
            if compact_observe
            else render_event_digest_section(theme_packet.get("event_digest") or {})
        )
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = (
            render_compact_what_changed_section(theme_packet.get("what_changed") or {})
            if compact_observe
            else render_what_changed_section(theme_packet.get("what_changed") or {})
        )
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
        structure_rows = [
            list(row)
            for row in (winner.get("market_event_rows") or [])
            if str(list(row)[2] if len(list(row)) > 2 else "").strip() in {"TDX结构专题", "港股/短线辅助", "转债辅助层"}
        ]
        if structure_rows:
            lines.extend(["", "## 结构辅助层", ""])
            lines.extend(
                _briefing_intelligence_board_lines(
                    {"market_event_rows": structure_rows},
                    max_items=4,
                )
            )
        horizon_lines = _index_horizon_summary_lines(winner)
        if horizon_lines:
            lines.extend(["", *horizon_lines])
        portfolio_overlap_lines = _portfolio_overlap_lines(winner)
        if portfolio_overlap_lines:
            lines.extend(["", "## 与现有持仓的关系", ""])
            for item in portfolio_overlap_lines:
                lines.append(f"- {item}")
        lines.extend(["", "## 这只ETF为什么是这个分", ""])
        lines.extend(
            _table(
                ["维度", "分数", "为什么是这个分"],
                winner.get("dimension_rows", []),
            )
        )
        if observe_only:
            upgrade_lines = _pick_upgrade_lines(winner)
            if upgrade_lines:
                lines.extend(["", "## 升级条件", ""] if compact_observe else [""])
                lines.extend(upgrade_lines)
        if summary_only:
            lines.extend(["", "## 当前只看什么", ""])
            lines.extend(_section_lead_lines("今天先看动作和触发条件；没有触发前，不给精确买点。"))
            summary_rows = _observe_pick_action_rows(
                winner,
                horizon,
                handoff,
                playbook,
                default_reassessment="等覆盖率恢复和右侧确认共振后，再决定是否升级成完整交易稿。",
                default_trigger="先等技术确认、催化覆盖和数据完整度一起改善，再决定要不要给买入区间。",
            )
            if display_track_rows:
                summary_rows = _track_row_summary(display_track_rows) + summary_rows
            horizon_lines = _index_horizon_summary_lines(winner)
            if horizon_lines:
                lines.extend(["", *horizon_lines])
            lines.extend(_table(["项目", "建议"], summary_rows))
            fund_sections = winner.get("fund_sections") or _fund_profile_sections(winner)
            if fund_sections:
                lines.extend(["", *fund_sections])
            lines.extend(
                _summary_only_explainer_sections(
                    winner,
                    alternatives,
                    event_digest=theme_packet.get("event_digest") or {},
                    evidence_fallback="当前可前置的外部情报仍偏少，先结合结构化披露、覆盖率和后文证据理解。",
                    no_alternative_text="当前可进入完整评分的 ETF 候选不足，暂时没有可并列展开的第二候选。",
                )
            )
            notes = _client_visible_blind_spot_lines(payload.get("notes") or [], focus_name=str(winner.get("name", "")))
            if notes:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in (notes[:1] if compact_observe else notes):
                    lines.append(f"- {item}")
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
            return _tighten_observe_client_markdown(rendered) if compact_observe else rendered
        strong_factor_rows = _strong_factor_rows_from_dimensions(
            winner.get("dimensions", {}),
            asset_type=str(winner.get("asset_type", "")) or "cn_etf",
        )
        if strong_factor_rows:
            lines.extend(["", "## 关键强因子拆解", ""])
            lines.extend(_table(["因子", "当前信号", "这意味着什么"], strong_factor_rows))
        lines.extend(["", *_taxonomy_section(winner)])
        if winner.get("score_changes"):
            lines.extend(["", "## 跟今天首个快照版相比", ""])
            for item in winner.get("score_changes", [])[:4]:
                lines.append(
                    f"- `{item.get('label', '维度')}` 从 `{item.get('previous', '—')}` 变到 `{item.get('current', '—')}`：{item.get('reason', '')}"
                )
        evidence_lines = _evidence_lines_with_event_digest(
            list(winner.get("evidence") or []),
            event_digest=theme_packet.get("event_digest") or {},
            max_items=3,
            as_of=winner.get("generated_at") or payload.get("generated_at"),
            symbol=winner.get("symbol"),
            subject=winner,
        )
        lines.extend(["", "## 关键证据", ""])
        if evidence_lines:
            lines.extend(evidence_lines)
        else:
            lines.append("- 当前可直接复核的一手情报仍偏少，先结合结构化披露和后文证据理解。")
        lines.extend(["", *_analysis_provenance_lines(winner)])
        lines.extend(["", "## 怎么做", ""])
        lines.append(
            "今天先看动作和触发条件；没有触发前，不给精确买点。"
            if observe_only
            else "先看动作、买入区间和仓位，再决定要不要往下读细节。"
        )
        lines.append("")
        if observe_only:
            etf_rows = _observe_pick_action_rows(
                winner,
                horizon,
                handoff,
                playbook,
                default_reassessment="等技术确认和催化覆盖一起改善后，再决定是否升级成正式交易动作。",
                default_trigger="先等技术确认、催化覆盖和右侧共振一起改善，再决定要不要给买入区间。",
            )
        else:
            etf_rows = [
                ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                ["持有周期", horizon.get("label", "未单独标注")],
                *_present_action_row(
                    "仓位身份",
                    (
                        f"`{_pick_client_safe_line(playbook.get('trading_role_label', ''))}` / `{_pick_client_safe_line(playbook.get('trading_position_label', ''))}`"
                        + (
                            f"：{_pick_client_safe_line(playbook.get('trading_role_summary', ''))}"
                            if _pick_client_safe_line(playbook.get("trading_role_summary", ""))
                            else ""
                        )
                    )
                    if _pick_client_safe_line(playbook.get("trading_role_label", ""))
                    and _pick_client_safe_line(playbook.get("trading_position_label", ""))
                    else "",
                ),
                ["适用打法", _pick_client_safe_line(horizon.get("style", "先按当前动作、仓位和止损框架理解，不把它默认当成长线配置。"))],
                ["为什么按这个周期看", _pick_client_safe_line(horizon.get("fit_reason", "当前更适合按已有动作和仓位框架理解。"))],
                ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把它自动理解成另一种更长或更短的打法。"))],
            ]
            if horizon.get("allocation_view") or str(playbook.get("allocation", "")).strip():
                etf_rows.append(["配置视角", _pick_client_safe_line(horizon.get("allocation_view") or str(playbook.get("allocation", "")))])
            if horizon.get("trading_view") or str(playbook.get("trend", "")).strip():
                etf_rows.append(["交易视角", _pick_client_safe_line(horizon.get("trading_view") or str(playbook.get("trend", "")))])
            etf_rows.extend(
                [
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', 'portfolio whatif buy 标的 最新价 计划金额')}`"],
                    ["介入条件", _pick_client_safe_line(winner.get("action", {}).get("entry", "等回撤再看"))],
                    *_present_action_row("建议买入区间", _safe_buy_range_text(dict(winner.get("action") or {}))),
                    ["首次仓位", _pick_client_safe_line(winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2"))],
                    ["加仓节奏", _pick_client_safe_line(winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔"))],
                    *_trim_execution_rows(dict(winner.get("action") or {})),
                    ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
                    *_present_action_row("目标参考", winner.get("action", {}).get("target", "")),
                ]
            )
        if display_track_rows:
            etf_rows = _track_row_summary(display_track_rows) + etf_rows
        lines.extend(_table(["项目", "建议"], etf_rows))
        fund_sections = winner.get("fund_sections") or _fund_profile_sections(winner)
        if fund_sections:
            lines.extend(["", *fund_sections])
        track_symbols = {
            str(dict(item).get("symbol", "")).strip()
            for item in recommendation_tracks.values()
            if str(dict(item).get("symbol", "")).strip()
        }
        formal_alternatives = [
            item for item in alternatives
            if str(dict(item).get("symbol", "")).strip() in track_symbols
        ]
        remaining_alternatives = [
            item for item in alternatives
            if str(dict(item).get("symbol", "")).strip() not in track_symbols
        ]
        if formal_alternatives:
            alternatives_heading = "## 其余观察对象" if observe_only else "## 其余正式推荐"
            lines.extend(["", alternatives_heading, ""])
            for index, item in enumerate(formal_alternatives[:2], start=2):
                lines.extend(
                    [
                        f"### {index}. {item.get('name', '')} ({item.get('symbol', '')})",
                        "",
                    ]
                )
                positives = [str(reason).strip() for reason in item.get("positives", []) if str(reason).strip()]
                for reason in (positives[:2] or [str(_pick_client_safe_line(_analysis_track_reason(item))).strip()]):
                    if reason:
                        lines.append(f"- {reason}")
                action = dict(item.get("action") or {})
                upgrade = str(action.get("entry", "")).strip()
                if upgrade:
                    lines.append(f"- 执行上更适合：{_pick_client_safe_line(upgrade)}")
                lines.append("")
        lines.extend(["", "## 为什么不是另外几只", ""])
        if remaining_alternatives:
            for index, item in enumerate(remaining_alternatives[:2], start=1):
                lines.extend(
                    [
                        f"### {index}. {item.get('name', '')} ({item.get('symbol', '')})",
                        "",
                    ]
                )
                for reason in item.get("cautions", [])[:3]:
                    lines.append(f"- {reason}")
                lines.append("")
        elif formal_alternatives:
            labels = [
                f"`{item.get('name', '')} ({item.get('symbol', '')})`"
                for item in formal_alternatives[:2]
                if str(item.get("name", "")).strip() or str(item.get("symbol", "")).strip()
            ]
            joined = "、".join(labels)
            layer_label = "观察顺序层" if observe_only else "正式推荐层"
            lines.append(f"- 其余高分候选已并入上面的{layer_label}，不再把它们误写成被排除对象。")
            if joined:
                lines.append(f"- 这次并入{layer_label}的备选主要是 {joined}，上面已经分别写了继续看的原因和执行边界。")
        else:
            lines.append("- 今天可进入完整评分且未被硬排除的 ETF 候选只有 1 只，没有形成可并列展开的第二候选。")
            if observe_only:
                lines.append("- 这也是本次只能按观察优先或降级稿处理、不能把它写成正式强推荐的原因之一。")
            else:
                lines.append("- 单候选不等于自动降级，是否按正式推荐理解仍以本页 `交付等级` 为准。")
        if observe_only:
            notes = _client_visible_blind_spot_lines(payload.get("notes") or [], focus_name=str(winner.get("name", "")))
            if notes:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in (notes[:1] if compact_observe else notes):
                    lines.append(f"- {item}")
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            rendered = self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
            return _tighten_observe_client_markdown(rendered) if compact_observe else rendered
        lines.extend(["## 仓位管理", ""])
        for item in winner.get("positioning_lines", []):
            lines.append(f"- {item}")
        lines.extend(["", "## 组合落单前", ""])
        lines.append(f"- {handoff.get('summary', '先跑组合预演，再决定真实金额。')}")
        lines.append(f"- 命令：`{handoff.get('command', 'portfolio whatif buy 标的 最新价 计划金额')}`")
        notes = _client_visible_blind_spot_lines(payload.get("notes") or [], focus_name=str(winner.get("name", "")))
        if notes:
            lines.extend(["", "## 数据限制与说明", ""])
            for item in notes:
                lines.append(f"- {item}")
        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(theme_packet)
        return self._prepend_editor_homepage(rendered, homepage, lead_markdown=lead_markdown)
