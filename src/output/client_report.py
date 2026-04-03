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
    _manager_profile_text,
    _primary_upgrade_trigger,
    _scan_topline_text,
    _visual_lines,
)
from src.output.editor_payload import (
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
from src.utils.fund_taxonomy import taxonomy_rows

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


def _is_observe_style_text(text: Any) -> bool:
    line = _pick_client_safe_line(text)
    if not line:
        return False
    return any(marker in line for marker in ("观察", "回避", "暂不出手", "等待", "先按观察仓"))


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


def _index_horizon_summary_lines(
    payload: Mapping[str, Any],
    *,
    bundle_key: str = "index_topic_bundle",
    market_lines_key: str = "domestic_market_lines",
) -> List[str]:
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
    labels = ("观察优先", "补充观察")
    rewritten: List[List[str]] = []
    for index, row in enumerate(track_rows):
        current = list(row)
        if not current:
            continue
        current[0] = labels[index] if index < len(labels) else f"观察 {index + 1}"
        rewritten.append(current)
    return rewritten


def _observe_pick_track_summary(track_rows: Sequence[Sequence[str]]) -> str:
    names: List[str] = []
    for row in track_rows[:2]:
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
    code = str(horizon.get("code", "")).strip()
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

    for bucket_name in ("short", "medium"):
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
    short_name = str(short_item.get("name", short_item.get("symbol", ""))).strip()
    medium_name = str(medium_item.get("name", medium_item.get("symbol", ""))).strip()
    if short_name and medium_name:
        return f"短线先看：`{short_name}`；中线先看：`{medium_name}`"
    if short_name:
        return f"短线先看：`{short_name}`；中线暂不单列"
    if medium_name:
        return f"中线先看：`{medium_name}`；短线暂不单列"
    return ""


def _market_watch_summary_text(
    ranked_items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
) -> str:
    for item in ranked_items:
        name = str(item.get("name", item.get("symbol", ""))).strip()
        if not name:
            continue
        bucket = _recommendation_bucket(item, watch_symbols)
        if bucket == "看好但暂不推荐":
            return f"今天没有正式动作票；观察先看：`{name}`"
        return f"今天没有正式动作票；优先观察：`{name}`"
    return ""


def _market_track_rows(tracks: Mapping[str, Mapping[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for bucket_name, label in (("short", "短线优先"), ("medium", "中线优先")):
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
    watch_levels = _observe_watch_levels(analysis) or "先等关键位和动能一起改善。"
    packet = build_scan_editor_packet(dict(analysis, day_theme={"label": day_theme} if day_theme else {}), bucket=_recommendation_bucket(analysis, watch_symbols))
    evidence_lines = _evidence_lines_with_event_digest(
        list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []),
        event_digest=packet.get("event_digest") or {},
        max_items=1,
        as_of=analysis.get("generated_at"),
        symbol=analysis.get("symbol"),
    )
    evidence_line = evidence_lines[0] if evidence_lines else "- 当前更适合先按结构化披露和后文证据理解。"
    return [
        f"### {name} ({symbol})",
        "",
        f"- 为什么继续看：{why_watch}",
        f"- 现在主要卡点：{gate_text}",
        f"- 升级条件：{trigger_text}",
        f"- 关键盯盘价位：{watch_levels}",
        f"- 当前更合适的动作：{_pick_client_safe_line(action.get('entry', '先看确认，不急着给仓位。')) or '先看确认，不急着给仓位。'}",
        f"- 首次仓位：{_pick_client_safe_line(action.get('position', '≤2% 观察仓，或先不出手'))}",
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
    metadata = dict(analysis.get("metadata") or {})
    chain_nodes = [str(item).strip() for item in (metadata.get("chain_nodes") or []) if str(item).strip()]
    parts = [
        str(analysis.get("name", "")),
        str(metadata.get("sector", "")),
        str(metadata.get("industry", "")),
        *chain_nodes,
    ]
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


def _payload_track_summary_text(tracks: Mapping[str, Mapping[str, Any]]) -> str:
    short_item = dict(tracks.get("short_term") or {})
    medium_item = dict(tracks.get("medium_term") or {})
    short_name = str(short_item.get("name", "")).strip()
    medium_name = str(medium_item.get("name", "")).strip()
    if short_name and medium_name:
        return f"短线先看：`{short_name}`；中线先看：`{medium_name}`"
    if short_name:
        return f"短线先看：`{short_name}`"
    if medium_name:
        return f"中线先看：`{medium_name}`"
    return ""


def _payload_track_rows(tracks: Mapping[str, Mapping[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for key, label in (("short_term", "短线优先"), ("medium_term", "中线优先")):
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


def _evidence_identity(item: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(item.get("layer", "") or "").strip().lower(),
        str(item.get("title", "") or "").strip().lower(),
        str(item.get("source", "") or "").strip().lower(),
        str(item.get("date", "") or item.get("published_at", "") or item.get("as_of", "") or "").strip().lower(),
    )


def _merged_evidence_items(
    items: Sequence[Mapping[str, Any]],
    *,
    event_digest: Mapping[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in list(items or []):
        row = dict(item or {})
        if not str(row.get("title", "") or "").strip():
            continue
        if _is_diagnostic_intelligence_row(row):
            continue
        identity = _evidence_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(row)
    digest = dict(event_digest or {})
    for item in list(digest.get("items") or []):
        row = dict(item or {})
        if not str(row.get("title", "") or "").strip():
            continue
        if _is_diagnostic_intelligence_row(row):
            continue
        identity = _evidence_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(row)
    return merged


def _evidence_lines_with_event_digest(
    items: Sequence[Mapping[str, Any]],
    *,
    event_digest: Mapping[str, Any] | None = None,
    max_items: int = 3,
    as_of: Any = None,
    symbol: Any = "",
) -> List[str]:
    digest = dict(event_digest or {})
    return _evidence_lines(
        _merged_evidence_items(items, event_digest=digest),
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

    def _append_unique(line: str) -> None:
        normalized = re.sub(r"\s+", " ", str(line or "")).strip()
        if not normalized or normalized in seen or len(lines) >= max_items:
            return
        seen.add(normalized)
        lines.append(normalized)

    def _row_priority(row: Sequence[Any]) -> tuple[int, int, int]:
        source = str(row[2] if len(row) > 2 else "").strip()
        signal = str(row[6] if len(row) > 6 else "").strip()
        link = str(row[5] if len(row) > 5 else "").strip()
        priority = 5
        if source in {"公司公告/结构化", "互动易/投资者关系"}:
            priority = 0
        elif source == "卖方共识专题":
            priority = 1
        elif source in {"个股资金流向专题", "两融专题", "龙虎榜/打板专题", "交易所风险专题"}:
            priority = 2
        elif source in {"申万行业框架", "中信行业框架", "同花顺主题成分"}:
            priority = 4
        strength = str(row[3] if len(row) > 3 else "").strip()
        strength_rank = {"高": 0, "中": 1, "低": 2}.get(strength, 1)
        link_rank = 0 if link else 1
        signal_lower = signal.lower()
        if any(token in signal_lower for token in ("公告", "互动", "投资者关系", "卖方")):
            priority = min(priority, 1)
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
    news_items = list(dict(payload.get("news_report") or {}).get("items") or [])
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
        lines.append(f"指数层确认：{index_signal}。")

    product_factor_signal = rows.get("场内基金技术状态（ETF/基金专属）", "")
    if product_factor_signal and all(product_factor_signal not in item for item in lines):
        lines.append(f"产品因子确认：{product_factor_signal}。")

    return lines[:4]


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

    for item in list(headline_lines or [])[1:4]:
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
        ["新闻模式", provenance.get("news_mode", "unknown")],
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
    if day_theme:
        theme_context["day_theme"] = {"label": day_theme}
    packet = build_scan_editor_packet(theme_context, bucket=bucket)
    theme_lines = _theme_playbook_explainer_lines(dict(packet.get("theme_playbook") or {}))

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
        lines.append(f"- 持有周期：{horizon['label']}。{_pick_client_safe_line(horizon['style'])}")
        if horizon.get("fit_reason"):
            lines.append(f"- 为什么按这个周期理解：{_pick_client_safe_line(horizon['fit_reason'])}")
        if horizon.get("misfit_reason"):
            lines.append(f"- 现在不适合的打法：{_pick_client_safe_line(horizon['misfit_reason'])}")
    if handoff.get("timing_summary"):
        timing_line = _pick_client_safe_line(handoff["timing_summary"])
        hint = _analysis_constraint_hint(analysis)
        if hint and hint not in timing_line:
            timing_line = _append_sentence(timing_line, hint)
        lines.append(f"- 适用时段：{timing_line}")
    lines.append(f"- 介入条件：{_pick_client_safe_line(action.get('entry', '等待进一步确认'))}")
    if bucket != "正式推荐":
        lines.append(
            "- 触发买点条件："
            + _observe_trigger_condition(
                analysis,
                horizon,
                default_text="等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
            )
        )
        watch_levels = _observe_watch_levels(analysis)
        if watch_levels:
            lines.append(f"- 关键盯盘价位：{watch_levels}")
    buy_range = _safe_buy_range_text(action)
    if buy_range and "暂不设" not in buy_range:
        lines.append(f"- 建议买入区间：{_pick_client_safe_line(buy_range)}")
    lines.extend(
        [
            f"- 首次仓位：{_pick_client_safe_line(action.get('position', '小仓位分批'))}",
            f"- 加仓节奏：{_pick_client_safe_line(action.get('scaling_plan', '确认后再考虑第二笔'))}",
            f"- 建议减仓区间：{_pick_client_safe_line(action.get('trim_range', '先看前高附近承压与突破'))}",
            f"- 止损参考：{_pick_client_safe_line(action.get('stop', '重新跌破关键支撑就处理'))}",
            f"- 组合落单前：{handoff['summary']}",
            f"- 预演命令：`{handoff['command']}`",
        ]
    )
    return lines


def _analysis_watch_card_lines(
    analysis: Mapping[str, Any],
    bucket: str,
    *,
    day_theme: str = "",
    used_positive_reasons: Counter[str],
    used_caution_reasons: Counter[str],
    generated_at: str,
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
    watch_levels = _observe_watch_levels(analysis)
    trigger_line = _observe_trigger_condition(
        analysis,
        horizon,
        default_text="等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
    )
    constraint_hint = _analysis_constraint_hint(analysis)
    if constraint_hint and constraint_hint not in trigger_line:
        trigger_line = _append_sentence(trigger_line, constraint_hint)
    theme_context = dict(analysis)
    if day_theme:
        theme_context["day_theme"] = {"label": day_theme}
    packet = build_scan_editor_packet(theme_context, bucket=bucket)
    theme_lines = _theme_playbook_explainer_lines(dict(packet.get("theme_playbook") or {}))

    lines = [
        f"### {name} ({symbol}) | {bucket}",
        "",
        f"**先看结论：** {_analysis_section_takeaway(analysis, bucket)}",
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
        lines.append(f"- 当前更像{horizon['label']}：{watch_profile}")
    lines.append(f"- 对 `{name}` 来说，触发买点条件是：{trigger_line}")
    if watch_levels:
        lines.append(f"- 关键盯盘价位：{watch_levels}")
    lines.append(f"- 首次仓位：{_pick_client_safe_line(action.get('position', '≤2% 观察仓，或先不出手'))}")
    lines.extend(["", "证据口径：", ""])
    evidence_lines = _evidence_lines_with_event_digest(
        list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []),
        event_digest=packet.get("event_digest") or {},
        max_items=2,
        as_of=analysis.get("generated_at"),
        symbol=analysis.get("symbol"),
    )
    if evidence_lines:
        lines.extend(evidence_lines)
        if not any("直连情报" in item or "高置信直连" in item for item in evidence_lines):
            lines.append("- 当前证据更偏结构化披露与公告日历，不是直连情报催化型驱动。")
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


def _stock_pick_shared_evidence_lines(items: Sequence[Mapping[str, Any]]) -> List[str]:
    for item in items:
        evidence = list(dict(item.get("dimensions", {}).get("catalyst") or {}).get("evidence") or [])
        evidence_lines = _evidence_lines_with_event_digest(
            evidence,
            event_digest=build_scan_editor_packet(item, bucket=_recommendation_bucket(item)).get("event_digest") or {},
            max_items=2,
            as_of=item.get("generated_at"),
            symbol=item.get("symbol"),
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
    if not fund_profile:
        return []
    asset_type = str(analysis.get("asset_type", "")).strip()
    overview = dict(fund_profile.get("overview") or {})
    etf_snapshot = dict(fund_profile.get("etf_snapshot") or {})
    fund_factor_snapshot = dict(fund_profile.get("fund_factor_snapshot") or {})
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
    lines.extend(["", "### 基金风格", ""])
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
        lines.append(f"- 这份 `{asset_label}` 稿件仍按正式推荐框架编排，但执行上仍要遵守仓位和止损。")
    if summary_only:
        lines.append(f"- 当前覆盖率不足以支撑完整 `{asset_label}` 模板，本次按摘要版交付。")
    if notes:
        for item in notes[:3]:
            lines.append(f"- {item}")
    else:
        lines.append("- 当前没有额外降级或回退说明。")
    return lines


def _pick_delivery_summary_only(selection_context: Mapping[str, Any]) -> bool:
    return bool(dict(selection_context or {}).get("delivery_summary_only"))


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
        if interpretation:
            rows.append(["情绪代理", interpretation, f"`{confidence}` / {coverage}", limitation, downgrade])
    elif social_summary:
        covered = social_summary.get("covered", 0)
        total = social_summary.get("total", 0)
        labels = dict(social_summary.get("confidence_labels") or {})
        interpretation = f"已对 `{covered}/{total}` 只候选生成情绪代理；置信度分布 {labels or {'低': 0}}。"
        limitation = str(social_summary.get("limitation", "")).strip() or "当前没有额外说明。"
        downgrade = str(social_summary.get("downgrade_impact", "")).strip() or "当前没有额外降级影响说明。"
        coverage = str(social_summary.get("coverage_summary", "")).strip() or f"{covered}/{total}"
        rows.append(["情绪代理", interpretation, coverage, limitation, downgrade])

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


def _observe_watch_levels(analysis: Mapping[str, Any]) -> str:
    action = dict(analysis.get("action") or {})
    buy_range = _safe_buy_range_text(action)
    trim_range = _execution_range_text(action.get("trim_range", ""))
    try:
        stop_ref = float(action.get("stop_ref") or 0.0)
    except (TypeError, ValueError):
        stop_ref = 0.0
    try:
        target_ref = float(action.get("target_ref") or 0.0)
    except (TypeError, ValueError):
        target_ref = 0.0

    if buy_range and trim_range:
        return f"回踩先看 `{buy_range}` 一带的承接；如果反弹延续，再看 `{trim_range}` 一带的承压。"
    if buy_range and target_ref > 0:
        return f"回踩先看 `{buy_range}` 一带的承接；如果继续上行，再看 `{target_ref:.3f}` 附近能不能放量突破。"
    if buy_range:
        return f"回踩先看 `{buy_range}` 一带的承接。"
    if stop_ref > 0 and target_ref > 0:
        return f"下沿先看 `{stop_ref:.3f}` 上方能不能稳住；上沿先看 `{target_ref:.3f}` 附近能不能放量突破。"
    if stop_ref > 0:
        return f"先看 `{stop_ref:.3f}` 上方能不能稳住，别先跌破关键下沿。"
    if trim_range:
        return f"先看 `{trim_range}` 一带的承压，别把反弹空间直接想满。"
    if target_ref > 0:
        return f"先看 `{target_ref:.3f}` 附近能不能放量突破。"
    return ""


def _observe_trigger_condition(
    analysis: Mapping[str, Any],
    horizon: Mapping[str, Any],
    *,
    default_text: str,
) -> str:
    action = dict(analysis.get("action") or {})
    entry = _pick_client_safe_line(action.get("entry", ""))
    buy_range = _safe_buy_range_text(action)
    entry_phrase = _entry_trigger_phrase(entry)
    derived_phrases = _derived_trigger_phrases(analysis)
    strategy_upgrade_text = _strategy_background_upgrade_text(analysis)

    def _finalize(text: str) -> str:
        line = _pick_client_safe_line(text)
        if strategy_upgrade_text and strategy_upgrade_text not in line:
            line = _append_sentence(line, strategy_upgrade_text)
        return append_technical_trigger_text(line, analysis.get("history"))

    if entry:
        phrases = [entry_phrase] if entry_phrase else [entry.rstrip("。；;,， ")]
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
        return _finalize(_compose_trigger_sentence(phrases, buy_range=buy_range))
    if buy_range and "暂不设" not in buy_range:
        return _finalize(f"先等价格回到 `{buy_range}` 一带并确认承接，再决定要不要动。")
    if derived_phrases:
        return _finalize(_compose_trigger_sentence(derived_phrases, buy_range=buy_range))
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
    rows = [
        ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
        ["持有周期", horizon.get("label", "未单独标注")],
    ]
    allocation_view = str(horizon.get("allocation_view") or playbook.get("allocation", "")).strip()
    if allocation_view:
        rows.append(["配置视角", _pick_client_safe_line(allocation_view)])
    trading_view = str(horizon.get("trading_view") or playbook.get("trend", "")).strip()
    if trading_view:
        rows.append(["交易视角", _pick_client_safe_line(trading_view)])
    rows.extend(
        [
            ["触发买点条件", _observe_trigger_condition(winner, horizon, default_text=default_trigger)],
            ["首次建仓", _pick_client_safe_line(winner.get("action", {}).get("position", "当前先不出手；若后续触发，再按小仓试探处理"))],
            *_present_action_row("关键盯盘价位", _observe_watch_levels(winner)),
            ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
            ["重新评估条件", _pick_reassessment_condition(winner, horizon, default_reassessment)],
            ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易/申赎窗口理解。"))],
        ]
    )
    return rows


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
) -> str:
    action = dict(item.get("action") or {})
    direction = _pick_client_safe_line(action.get("direction", ""))
    entry = _pick_client_safe_line(action.get("entry", ""))
    buy_range = _safe_buy_range_text(action)
    position = _pick_client_safe_line(action.get("position", ""))
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
    trim_range = _execution_range_text(action.get("trim_range", ""))
    target = _pick_client_safe_line(action.get("target", ""))
    stop = _pick_client_safe_line(action.get("stop", ""))
    scaling_plan = _pick_client_safe_line(action.get("scaling_plan", ""))

    parts: List[str] = []
    if trade_state:
        parts.append(f"先按 `{trade_state}` 管")
    if trim_range:
        parts.append(f"反弹先看 `{trim_range}` 一带的承压")
    elif target:
        parts.append(f"目标先看 `{target}`")
    if scaling_plan and not any(marker in scaling_plan for marker in ("暂不", "观察")):
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
    judgment = dict(narrative.get("judgment") or {})
    trade_state = _pick_client_safe_line(item.get("trade_state") or judgment.get("state", ""))
    direction = _pick_client_safe_line(action.get("direction", ""))
    observe_conflict = _is_observe_style_text(trade_state) or (
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
    positives = _merge_reason_lines(
        list(item.get("positives") or []) + list(narrative.get("positives") or []),
        _top_dimension_reasons(item, top_n=3),
        max_items=5,
    )
    caution_fallbacks: List[str] = []
    entry_text = _pick_client_safe_line(action.get("entry", ""))
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
    context = dict(selection_context or {})
    delivery_label = str(context.get("delivery_tier_label", "")).strip()
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
        rows.append(["观察重点", _summary_empty_position_action_text(item, horizon, default_trigger=default_trigger)])
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
    playbook_level = str(payload.get("playbook_level", "")).strip()
    sector_label = str(payload.get("label", "")).strip() or str(payload.get("hard_sector_label", "")).strip() or "行业层"
    theme_match_status = str(payload.get("theme_match_status", "")).strip()
    theme_match_candidates = [str(item).strip() for item in list(payload.get("theme_match_candidates") or []) if str(item).strip()]
    bridge_confidence = str(payload.get("subtheme_bridge_confidence", "")).strip() or "none"
    bridge_top_label = str(payload.get("subtheme_bridge_top_label", "")).strip()
    bridge_items = [dict(item) for item in list(payload.get("subtheme_bridge") or []) if dict(item)]
    bridge_labels = [str(item.get("label", "")).strip() for item in bridge_items if str(item.get("label", "")).strip()]

    rows: List[List[str]] = []
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
    return rows


def _theme_playbook_explainer_lines(playbook: Optional[Mapping[str, Any]]) -> List[str]:
    return [f"{label}：{text}" for label, text in _theme_playbook_summary_rows(playbook)]


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
) -> List[List[str]]:
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
        ["当前框架", f"`{regime}` / `{day_theme}`"],
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
    rows.extend(
        [
            ["空仓怎么做", empty_text],
            ["持仓怎么做", holder_text],
            ["首次仓位", first_position],
            [
                "主要利好",
                (
                    f"当前仍能分出结构性优先级，主线先按 `{day_theme}` 理解。"
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


def _stock_pick_observe_threshold_lines(*, sector_filter: str = "") -> List[str]:
    lines = [
        "## 正式动作阈值",
        "",
        "这段只回答什么情况下，观察名单才会升级成试仓或正式动作。",
        "",
        "- 系统层先看能不能从 `观察为主` 升到 `看好但暂不推荐`：通常至少要有两块非技术支撑站住，或基本面/催化/相对强弱里出现更强的组合信号，而不是只靠单一高分硬撑。",
        "- 执行层再看动作栏有没有真的放行：即使分层已经接近可观察升级，只要当前动作仍是 `暂不出手 / 观察为主 / 先按观察仓`，就还不算正式动作票。",
        "- 默认先看 `技术面 / 相对强弱` 两项：至少一项回到非拖累区，且另一项不再明显恶化，才考虑从观察升级成试仓。",
        "- 如果直接催化仍弱，就不靠基本面高分单独硬升推荐；先等价格、动量和催化里至少两项形成共振。",
        "- 历史样本只给执行边界，不替代当前触发；过去同类形态不差，不等于今天这一笔已经完成确认。",
    ]
    if sector_filter:
        lines.append(f"- 对 `{sector_filter}` 这类主题内排序，先看方向不删，再看个股右侧确认；主题强不自动等于个股买点成熟。")
    return lines


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
    structured_positive = any("结构化事件覆盖" in line and "0%（0/" not in line for line in coverage_lines)
    direct_news_zero = any("高置信直接情报覆盖 0%" in line or "高置信直接新闻覆盖 0%" in line for line in coverage_lines)
    if not (structured_positive and direct_news_zero):
        return ""
    if asset_label == "场外基金":
        return "当前证据更偏结构化事件、基金画像和持仓/基准映射，不是直连情报催化型驱动。"
    if asset_label == "ETF":
        return "当前证据更偏结构化事件、产品画像和持仓/基准映射，不是直连情报催化型驱动。"
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
) -> List[List[str]]:
    rows: List[List[str]] = []
    if headline_lines:
        rows.append(["当前判断", str(headline_lines[0]).strip()])
    if action_lines:
        rows.append(["优先动作", str(action_lines[0]).strip()])
    regime_name = str(dict(regime or {}).get("current_regime", "")).strip()
    if regime_name or day_theme:
        rows.append(["中期背景 / 当天主线", f"{regime_name or '未标注'} / {day_theme or '未标注'}"])
    rows.extend(_theme_playbook_summary_rows(theme_playbook))
    pool_size = int(dict(a_share_watch_meta or {}).get("pool_size") or 0)
    complete_size = int(dict(a_share_watch_meta or {}).get("complete_analysis_size") or 0)
    if pool_size or complete_size:
        rows.append(["A股观察池", f"全市场初筛 `{pool_size}` -> 完整分析 `{complete_size}`。"])
    if quality_lines:
        rows.append(["当前限制", str(quality_lines[0]).strip()])
    return rows[:5]


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
    if asset_label == "场外基金":
        return "当前证据更偏结构化事件、基金画像和持仓/基准映射，不是直接新闻催化型驱动。"
    return "当前证据更偏结构化事件、产品画像和持仓/基准映射，不是直接新闻催化型驱动。"


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
            "label": label,
            "style": f"更适合作为中长期底仓来跟踪{ctx}，允许短线波动，但要持续复核主线、基本面和风险预算。",
            "fit_reason": f"更依赖中长期逻辑{ctx}，而不是一两天的节奏变化。",
            "misfit_reason": f"不适合按纯短线追涨杀跌来理解{ctx}。",
        }
    if "中线" in label:
        return {
            "code": "position_trade",
            "label": label,
            "style": f"更像 1-3 个月的分批配置或波段跟踪{ctx}，不按隔日涨跌去做快进快出。",
            "fit_reason": f"更适合围绕一段完整主线分批拿{ctx}，而不是只看日内波动。",
            "misfit_reason": f"不适合直接当成超短节奏仓{ctx}，也别默认长到长期不复核。",
        }
    if "短线" in label:
        return {
            "code": "short_term",
            "label": label,
            "style": f"更看催化、趋势和执行节奏{ctx}，适合盯右侧确认和止损，不适合当成长线底仓。",
            "fit_reason": f"当前优势更多集中在催化和节奏{ctx}，不在长周期基本面。",
            "misfit_reason": f"不适合当成长线配置仓{ctx}。",
        }
    if "波段" in label:
        return {
            "code": "swing",
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
        (r"龙虎榜/竞价/涨跌停边界", "龙虎榜/开局结构/涨跌停边界"),
        (r"竞价明显低开", "开局明显低开"),
        (r"竞价高开且量比放大", "开局高开且量比放大"),
        (r"竞价", "开局"),
        (r"盘中", "交易时段"),
        (r"隔日涨跌", "短期涨跌"),
        (r"只按隔夜消息", "只按单条消息"),
        (r"纯隔夜交易", "纯超短交易"),
        (r"隔夜交易", "超短交易"),
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
    def _prepend_editor_homepage(markdown_text: str, homepage_markdown: str) -> str:
        homepage = str(homepage_markdown or "").strip()
        if not homepage:
            return markdown_text.rstrip()
        homepage = _sanitize_client_markdown(homepage)
        lines = markdown_text.splitlines()
        if not lines:
            return homepage
        title = lines[0]
        body = "\n".join(lines[1:]).lstrip()
        return "\n".join([title, "", homepage, "", body]).rstrip()

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
        has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in top)
        stock_pick_packet = build_stock_pick_editor_packet(payload)
        summary_rows = _stock_pick_summary_rows(
            day_theme=day_theme,
            regime=regime,
            sector_filter=sector_filter,
            theme_playbook=dict(stock_pick_packet.get("theme_playbook") or {}),
            grouped=grouped,
            coverage_grouped=coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
        )
        compact_observe = not has_actionable

        lines = [
            f"# {'今日个股推荐（详细版）' if has_actionable else '今日个股观察（详细版）'} | {generated_at}",
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 今日动作摘要",
                    lead=(
                        "首页判断已经先给了总结论；这段只继续回答今天能不能动、空仓和持仓分别怎么做，以及仓位先怎么配。"
                    ),
                ),
                "",
            ]
        )
        if not has_actionable:
            lines.extend(_stock_pick_observe_threshold_lines(sector_filter=sector_filter))
            lines.append("")
        coverage = dict(payload.get("stock_pick_coverage") or {})
        coverage_lines = list(coverage.get("lines") or [])
        if coverage_lines:
            structure_driven_coverage = any(
                "结构化事件覆盖" in item
                and (
                    "高置信公司新闻覆盖 0%" in item
                    or "高置信公司级直连情报覆盖 0%" in item
                    or "高置信直接情报覆盖 0%" in item
                )
                for item in coverage_lines
            )
            lines.append(f"**数据完整度：** {coverage.get('note', '未标注')}")
            if structure_driven_coverage:
                lines.append("- 当前证据更偏结构化事件与公告日历，不是直连情报催化型驱动。")
            for item in coverage_lines[:3]:
                lines.append(f"- {item}")
            lines.append("- 覆盖率的分母是当前纳入详细分析的各市场标的，不是全市场扫描池。")
            lines.append("- 新闻热度更看多源共振；单一来源只算提及，不等于热度确认。")
            if structure_driven_coverage:
                lines.append("- 这批当前更依赖结构化事件和公告日历，不等于情报链失效；`0%` 只代表没命中高置信个股直连情报。")
            lines.append("- 相关性/分散度按各市场观察池基准代理，不同市场之间只适合看相对高低，不适合直接横向比较绝对值。")
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
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(stock_pick_packet.get("event_digest") or {}))
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
        what_changed_lines = render_what_changed_section(stock_pick_packet.get("what_changed") or {})
        if what_changed_lines:
            lines.extend(what_changed_lines)
            lines.append("")

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked_items = self._rank_market_items(items, watch_symbols)
            market_has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in ranked_items)
            tracks = _market_recommendation_tracks(ranked_items, watch_symbols)
            track_summary = _market_track_summary_text(tracks)
            scope_text = _market_pick_scope_text(ranked_items[0], generated_at=str(payload.get("generated_at", "")))
            if track_summary:
                lines.append(f"- {market_name}{scope_text}{track_summary}")
            elif not market_has_actionable:
                watch_summary = _market_watch_summary_text(ranked_items, watch_symbols)
                if watch_summary:
                    lines.append(f"- {market_name}{scope_text}{watch_summary}")
            affordable_rows = _affordable_stock_rows(
                coverage_grouped.get(market_name, []),
                watch_symbols,
                actionable_only=has_actionable,
            )
            if market_name == "A股" and affordable_rows:
                label = "低门槛可执行先看" if has_actionable else "低门槛继续跟踪先看"
                lines.append(f"- {market_name}{label}：`{affordable_rows[0][0].split(' (', 1)[0]}`")
            related_etf_rows = _related_etf_rows(coverage_grouped.get(market_name, []), watch_symbols)
            if market_name == "A股" and related_etf_rows:
                lines.append(f"- {market_name}关联ETF平替先看：`{related_etf_rows[0][0].split(' (', 1)[0]}`")

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
        if top and not has_formal:
            lines.extend(["", "## 催化证据来源", ""])
            lines.extend(_stock_pick_shared_evidence_lines(shared_reference_items))
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
                if followup_rows:
                    lines.extend(["", "### 第二批：继续跟踪", ""])
                    lines.extend(["", "- 第一批先看最接近触发的几只；这批是值得继续记在观察名单里的补充对象。"])
                    lines.extend(_table(["标的", "为什么还没进第一批", "现在更该看什么"], followup_rows))
                affordable_rows = _affordable_stock_rows(
                    coverage_grouped.get(market_name, []),
                    watch_symbols,
                    actionable_only=False,
                )
                related_etf_rows = _related_etf_rows(coverage_grouped.get(market_name, []), watch_symbols)
                if market_name == "A股" and (affordable_rows or related_etf_rows):
                    lines.extend(["", "### 第二批：低门槛 / 观察替代", ""])
                    lines.extend(["", "- 方向还值得跟踪，但不想直接扛高价或单票波动时，这批更适合先放在观察清单里。"])
                if market_name == "A股" and affordable_rows:
                    lines.extend(["", "#### 低门槛继续跟踪", ""])
                    lines.extend(_table(["标的", "一手参考", "更适合的周期", "为什么继续看"], affordable_rows))
                if market_name == "A股" and related_etf_rows:
                    lines.extend(["", "#### 关联ETF观察", ""])
                    lines.extend(_table(["关联ETF", "更适合替代哪个方向", "什么时候更适合用它"], related_etf_rows))
                continue

            affordable_rows = _affordable_stock_rows(
                coverage_grouped.get(market_name, []),
                watch_symbols,
                actionable_only=has_actionable,
            )
            related_etf_rows = _related_etf_rows(coverage_grouped.get(market_name, []), watch_symbols)
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
                    )
                    lines.extend(["", "**催化证据来源：**", ""])
                    if evidence_lines:
                        lines.extend(evidence_lines)
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
                            ),
                        ]
                    )

        if compact_observe:
            representative_item = next((item for item in shared_reference_items if not _analysis_is_actionable(item, watch_symbols)), None)
            if representative_item is not None:
                lines.extend(["", "## 代表样本复核卡", ""])
                lines.extend(["", "- 前面先给观察名单和触发器，这里只保留 1 只代表样本，方便复核理由、证据和升级条件。"])
                lines.extend(["", *_observe_representative_card_lines(representative_item, watch_symbols=watch_symbols, day_theme=day_theme)])
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
            for item in blind_spots:
                lines.append(f"- {item}")

        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(stock_pick_packet)
        return self._prepend_editor_homepage(rendered, homepage)

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
        used_reason_lines: Counter[str] = Counter()
        has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in top)
        stock_pick_packet = build_stock_pick_editor_packet(payload)
        summary_rows = _stock_pick_summary_rows(
            day_theme=day_theme,
            regime=regime,
            sector_filter=sector_filter,
            theme_playbook=dict(stock_pick_packet.get("theme_playbook") or {}),
            grouped=grouped,
            coverage_grouped=coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
        )

        lines = [
            f"# {'今日个股推荐' if has_actionable else '今日个股观察'} | {generated_at}",
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 今日动作摘要",
                    lead="首页判断已经先给了总结论；这段只继续回答今天能不能动、空仓和持仓分别怎么做，以及仓位先怎么配。",
                ),
                "",
            ]
        )
        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked_items = self._rank_market_items(items, watch_symbols)
            market_has_actionable = any(_analysis_is_actionable(item, watch_symbols) for item in ranked_items)
            tracks = _market_recommendation_tracks(ranked_items, watch_symbols)
            track_summary = _market_track_summary_text(tracks)
            scope_text = _market_pick_scope_text(ranked_items[0], generated_at=str(payload.get("generated_at", "")))
            if track_summary:
                lines.append(f"- {market_name}{scope_text}{track_summary}")
            elif not market_has_actionable:
                watch_summary = _market_watch_summary_text(ranked_items, watch_symbols)
                if watch_summary:
                    lines.append(f"- {market_name}{scope_text}{watch_summary}")
            affordable_rows = _affordable_stock_rows(coverage_grouped.get(market_name, []), watch_symbols)
            if market_name == "A股" and affordable_rows:
                label = "低门槛可执行先看" if has_actionable else "低门槛继续跟踪先看"
                lines.append(f"- {market_name}{label}：`{affordable_rows[0][0].split(' (', 1)[0]}`")
            related_etf_rows = _related_etf_rows(coverage_grouped.get(market_name, []), watch_symbols)
            if market_name == "A股" and related_etf_rows:
                lines.append(f"- {market_name}关联ETF平替先看：`{related_etf_rows[0][0].split(' (', 1)[0]}`")
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
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(stock_pick_packet.get("event_digest") or {}))
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = render_what_changed_section(stock_pick_packet.get("what_changed") or {})
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
                coverage_grouped.get(market_name, []),
                watch_symbols,
                actionable_only=market_has_actionable,
            )
            related_etf_rows = _related_etf_rows(coverage_grouped.get(market_name, []), watch_symbols)
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
            for item in formal[:2]:
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
        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(stock_pick_packet)
        return self._prepend_editor_homepage(rendered, homepage)

    def render_scan_detailed(
        self,
        analysis: Dict[str, Any],
        *,
        prepend_homepage: bool = True,
        keep_observe_execution: bool = False,
    ) -> str:
        rendered = OpportunityReportRenderer().render_scan(analysis, visuals=analysis.get("visuals"))
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
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
        packet = build_scan_editor_packet(analysis, bucket=_recommendation_bucket(analysis))
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
        sanitized = _sanitize_client_markdown(rewritten)
        if not prepend_homepage:
            return sanitized
        homepage = render_editor_homepage(packet)
        return self._prepend_editor_homepage(sanitized, homepage)

    def render_scan(self, analysis: Dict[str, Any]) -> str:
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        bucket = _recommendation_bucket(analysis)
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
        notes = [str(item).strip() for item in (analysis.get("notes") or []) if str(item).strip()]
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
            f"**升级触发器：** {_pick_client_safe_line(_primary_upgrade_trigger(analysis))}",
            "",
            "## 为什么这么判断",
            "",
            *_section_lead_lines("这段先看八维里哪几项在支撑，哪几项在拖后腿。"),
        ]
        for item in _theme_playbook_explainer_lines(dict(packet.get("theme_playbook") or {}))[:2]:
            lines.append(f"- {item}")
        if _theme_playbook_explainer_lines(dict(packet.get("theme_playbook") or {}))[:2]:
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
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(packet.get("event_digest") or {}))
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = render_what_changed_section(packet.get("what_changed") or {})
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
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", action.get("direction", "观察为主")],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["周期理由", _pick_client_safe_line(horizon.get("fit_reason", horizon.get("style", "先按当前动作、仓位和止损框架理解。")))],
                    ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把当前动作自动理解成另一种更长或更短的打法。"))],
                    *(
                        [["配置视角", _pick_client_safe_line(horizon.get("allocation_view") or str(dict(narrative.get("playbook") or {}).get("allocation", "")))]]
                        if (horizon.get("allocation_view") or str(dict(narrative.get("playbook") or {}).get("allocation", "")).strip())
                        else []
                    ),
                    *(
                        [["交易视角", _pick_client_safe_line(horizon.get("trading_view") or str(dict(narrative.get("playbook") or {}).get("trend", "")))]]
                        if (horizon.get("trading_view") or str(dict(narrative.get("playbook") or {}).get("trend", "")).strip())
                        else []
                    ),
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["介入条件", _pick_client_safe_line(action.get("entry", "等待进一步确认"))],
                    [
                        "触发买点条件",
                        _observe_trigger_condition(
                            analysis,
                            horizon,
                            default_text="先等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
                        ),
                    ],
                    *_present_action_row("关键盯盘价位", _observe_watch_levels(analysis)),
                    *(
                        [
                            ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                            ["预演命令", f"`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`"],
                            *_present_action_row("建议买入区间", _safe_buy_range_text(action)),
                            ["首次仓位", _pick_client_safe_line(action.get("position", "小仓位分批"))],
                            ["加仓节奏", _pick_client_safe_line(action.get("scaling_plan", "确认后再考虑第二笔"))],
                            *_present_action_row("建议减仓区间", action.get("trim_range", "")),
                            ["止损参考", _pick_client_safe_line(action.get("stop", "重新跌破关键支撑就处理"))],
                            *_present_action_row("目标参考", action.get("target", "")),
                        ]
                        if show_precise_execution
                        else [["执行参数", "当前是观察稿，不前置精确仓位、止损和目标模板；先看确认条件和关键位。"]]
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
            score = dimension.get("score")
            max_score = dimension.get("max_score", 100)
            display = "—" if score is None else f"{score}/{max_score}"
            lines.extend(
                [
                    "",
                    f"### {label} {display}",
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
            for item in notes[:3]:
                lines.append(f"- {item}")
        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(packet)
        return self._prepend_editor_homepage(rendered, homepage)

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
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        lines = rendered.splitlines()
        if lines and lines[0].startswith("# "):
            lines[0] = f"# {name} ({symbol}) | 个股详细分析 | {generated_at}"
        updated = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(build_stock_analysis_editor_packet(analysis))
        return self._prepend_editor_homepage(
            _insert_print_page_break_before(updated, "## 一句话结论"),
            homepage,
        )

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
        )
        watch_upgrade_lines = [self._briefing_client_safe_line(item) for item in _briefing_watch_upgrade_lines(named_a_share_watch_candidates)]
        strategy_summary_text = ""
        for item in named_a_share_watch_candidates[:2]:
            strategy_summary_text = _strategy_background_upgrade_text(item)
            if strategy_summary_text:
                break
        if strategy_summary_text:
            summary_rows = [*summary_rows, ["后台置信度", strategy_summary_text]]
        lines = [
            f"# 今日晨报 | {generated_at}",
            "",
        ]
        lines.extend(
            _summary_block_lines(
                summary_rows,
                heading="## 执行摘要",
                lead="这段先把今天的主判断、优先动作、宏观背景和观察池结论压到最前面。",
            )
        )
        lines.extend(
            [
                "",
            "## 今日最重要的判断",
            "",
            ]
        )
        if headline_lines:
            lines.append(headline_lines[0])
            for item in headline_lines[1:3]:
                lines.append("")
                lines.append(item)
        else:
            lines.append("今天更适合先看风险控制，再看进攻节奏。")
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
        lines.extend(["", "## 今天怎么做", ""])
        for item in action_lines[:4]:
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
        alerts = [self._briefing_client_safe_line(item) for item in list(payload.get("alerts") or [])]
        lines.extend(["", "## 今天最重要的风险提醒", ""])
        for item in alerts[:3]:
            lines.append(f"- {item}")
        rendered = "\n".join(lines).rstrip()
        homepage = render_editor_homepage(briefing_packet)
        return self._prepend_editor_homepage(rendered, homepage)

    def render_fund_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        alternatives = list(payload.get("alternatives") or [])
        selection_context = dict(payload.get("selection_context") or {})
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
        theme_packet = build_fund_pick_editor_packet(payload)
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
        if observe_only:
            lines.extend(
                _observe_delivery_threshold_lines(
                    asset_label="场外基金",
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
            lines.extend(
                [
                    f"> 发现方式: {selection_context.get('discovery_mode_label', '未标注')} | 初筛池: {selection_context.get('scan_pool', '—')} | 完整分析: {selection_context.get('passed_pool', '—')}",
                    f"> 主题过滤: {selection_context.get('theme_filter_label', '未指定')} | 风格过滤: {selection_context.get('style_filter_label', '不限')} | 管理人过滤: {selection_context.get('manager_filter_label', '未指定')}",
                    "",
                ]
            )
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
            basis_label = selection_context.get("comparison_basis_label", "对比基准")
            if basis_label == "当日基准版":
                basis_label = "今天首个快照版"
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
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(theme_packet.get("event_digest") or {}))
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = render_what_changed_section(theme_packet.get("what_changed") or {})
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
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
                lines.extend([""])
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
                for item in blind_spots[:3]:
                    lines.append(f"- {item}")
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            return self._prepend_editor_homepage(rendered, homepage)
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
                    *_present_action_row("建议减仓区间", winner.get("action", {}).get("trim_range", "")),
                    ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
                ]
            )
        lines.extend(_table(["项目", "建议"], fund_rows))
        fund_sections = winner.get("fund_sections") or _fund_profile_sections(winner)
        if fund_sections:
            lines.extend(["", *fund_sections])
        if selection_context.get("blind_spots"):
            lines.extend(["", "## 数据限制与说明", ""])
            for item in selection_context.get("blind_spots", [])[:3]:
                lines.append(f"- {item}")
        lines.extend(["", "## 为什么不是另外几只", ""])
        if alternatives:
            for index, item in enumerate(alternatives[:2], start=1):
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
            return self._prepend_editor_homepage(rendered, homepage)
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
        return self._prepend_editor_homepage(rendered, homepage)

    def render_etf_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        alternatives = list(payload.get("alternatives") or [])
        recommendation_tracks = dict(payload.get("recommendation_tracks") or {})
        selection_context = dict(payload.get("selection_context") or {})
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
        theme_packet = build_etf_pick_editor_packet(payload)
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        track_summary = _payload_track_summary_text(recommendation_tracks)
        track_rows = _payload_track_rows(recommendation_tracks)
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
        if display_track_rows:
            summary_prefix_rows: List[List[str]] = []
            if len(display_track_rows) >= 1:
                summary_prefix_rows.append([display_track_rows[0][0], f"{display_track_rows[0][1]}；{display_track_rows[0][3]}"])
            if len(display_track_rows) >= 2:
                summary_prefix_rows.append([display_track_rows[1][0], f"{display_track_rows[1][1]}；{display_track_rows[1][3]}"])
            summary_rows = summary_prefix_rows + summary_rows
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
        if observe_only:
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
            lines.extend(
                [
                    f"> 发现方式: {selection_context.get('discovery_mode_label', '未标注')} | 初筛池: {selection_context.get('scan_pool', '—')} | 完整分析: {selection_context.get('passed_pool', '—')}",
                    f"> 主题过滤: {selection_context.get('theme_filter_label', '未指定')}",
                    "",
                ]
            )
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
            basis_label = selection_context.get("comparison_basis_label", "对比基准")
            if basis_label == "当日基准版":
                basis_label = "今天首个快照版"
            lines.append(
                f"- 分数变化对比的是 `{basis_label} {selection_context.get('comparison_basis_at')}`。"
            )
        if selection_context.get("model_version_warning"):
            lines.append(f"- {selection_context.get('model_version_warning')}")
        lines.extend(_delivery_tier_section(selection_context, asset_label="ETF"))
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
        if display_track_rows:
            lines.extend(["", "## 当前分层建议", ""])
            lines.extend(_table(["层次", "标的", "更适合的周期", "为什么先看"], display_track_rows))
        lines.extend(["", why_heading, ""])
        for item in _theme_playbook_explainer_lines(dict(theme_packet.get("theme_playbook") or {}))[:2]:
            lines.append(f"- {item}")
        for item in winner.get("positives", [])[:4]:
            lines.append(f"- {item}")
        for item in _etf_front_reason_lines(winner):
            lines.append(f"- {item}")
        event_digest_lines = _client_safe_markdown_lines(render_event_digest_section(theme_packet.get("event_digest") or {}))
        if event_digest_lines:
            lines.extend(["", *event_digest_lines])
        what_changed_lines = render_what_changed_section(theme_packet.get("what_changed") or {})
        if what_changed_lines:
            lines.extend(["", *what_changed_lines])
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
                lines.extend([""])
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
                summary_rows = [
                    [display_track_rows[0][0], f"{display_track_rows[0][1]}；{display_track_rows[0][3]}"] if len(display_track_rows) >= 1 else None,
                    [display_track_rows[1][0], f"{display_track_rows[1][1]}；{display_track_rows[1][3]}"] if len(display_track_rows) >= 2 else None,
                    *summary_rows,
                ]
                summary_rows = [row for row in summary_rows if row]
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
                for item in notes:
                    lines.append(f"- {item}")
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            return self._prepend_editor_homepage(rendered, homepage)
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
                    *_present_action_row("建议减仓区间", winner.get("action", {}).get("trim_range", "")),
                    ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
                    *_present_action_row("目标参考", winner.get("action", {}).get("target", "")),
                ]
            )
        if display_track_rows:
            staged_rows: List[List[str]] = []
            if len(display_track_rows) >= 1:
                staged_rows.append([display_track_rows[0][0], f"{display_track_rows[0][1]}；{display_track_rows[0][3]}"])
            if len(display_track_rows) >= 2:
                staged_rows.append([display_track_rows[1][0], f"{display_track_rows[1][1]}；{display_track_rows[1][3]}"])
            etf_rows = staged_rows + etf_rows
        lines.extend(_table(["项目", "建议"], etf_rows))
        fund_sections = winner.get("fund_sections") or _fund_profile_sections(winner)
        if fund_sections:
            lines.extend(["", *fund_sections])
        lines.extend(["", "## 为什么不是另外几只", ""])
        if alternatives:
            for index, item in enumerate(alternatives[:2], start=1):
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
            lines.append("- 今天可进入完整评分且未被硬排除的 ETF 候选只有 1 只，没有形成可并列展开的第二候选。")
            if observe_only:
                lines.append("- 这也是本次只能按观察优先或降级稿处理、不能把它写成正式强推荐的原因之一。")
            else:
                lines.append("- 单候选不等于自动降级，是否按正式推荐理解仍以本页 `交付等级` 为准。")
        if observe_only:
            notes = _client_visible_blind_spot_lines(payload.get("notes") or [], focus_name=str(winner.get("name", "")))
            if notes:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in notes:
                    lines.append(f"- {item}")
            rendered = "\n".join(lines).rstrip()
            homepage = render_editor_homepage(theme_packet)
            return self._prepend_editor_homepage(rendered, homepage)
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
        return self._prepend_editor_homepage(rendered, homepage)
