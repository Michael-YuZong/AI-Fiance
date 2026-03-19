"""Client-facing report renderers."""

from __future__ import annotations

from collections import Counter, defaultdict
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.output.opportunity_report import (
    OpportunityReportRenderer,
    _catalyst_factor_rows,
    _dimension_rows as _detail_dimension_rows,
    _factor_rows,
    _hard_check_inline,
    _hard_check_rows,
    _manager_profile_text,
    _visual_lines,
)
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

    lines = [
        (
            f"**历史相似样本：** {confidence.get('summary', '')}"
            or f"同标的近似样本 `{confidence.get('sample_count', '—')}` 个。"
        ),
        "",
    ]
    rows = [
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
    lines.extend(_table(["指标", "结果"], rows))
    sample_dates = list(confidence.get("sample_dates") or [])
    lines.extend(
        [
            "",
            "- 这层只反映历史相似量价/技术场景的样本置信度，不等于本次总推荐置信度。",
            "- 严格口径会剔除未来窗口重叠样本，避免把一段连续走势重复算成多次命中。",
        ]
    )
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
        return str(value).replace("|", "\\|").replace("\n", "<br>")

    lines = [
        "| " + " | ".join(_escape(header) for header in headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_escape(cell) for cell in row) + " |")
    return lines


def _score(analysis: Mapping[str, Any], dimension: str) -> int:
    value = analysis.get("dimensions", {}).get(dimension, {}).get("score")
    try:
        return int(value or 0)
    except Exception:
        return 0


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
    rating_rank = int(analysis.get("rating", {}).get("rank", 0) or 0)
    if rating_rank >= 3:
        return "正式推荐"
    if watch_symbols and str(analysis.get("symbol", "")) in watch_symbols:
        return "看好但暂不推荐"
    if max(
        _score(analysis, "fundamental"),
        _score(analysis, "relative_strength"),
        _score(analysis, "risk"),
        _score(analysis, "catalyst"),
    ) >= 60:
        return "看好但暂不推荐"
    return "观察为主"


def _bucket_priority(bucket: str) -> int:
    return {
        "正式推荐": 0,
        "看好但暂不推荐": 1,
        "观察为主": 2,
    }.get(bucket, 9)


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


def _evidence_lines(items: Sequence[Mapping[str, Any]], *, max_items: int = 3) -> List[str]:
    lines: List[str] = []
    priority = {
        "结构化事件": 0,
        "龙头公告/业绩": 1,
        "前瞻催化": 2,
        "负面事件": 3,
        "海外映射": 4,
        "政策催化": 5,
    }
    ranked_items = sorted(
        list(items),
        key=lambda item: (
            priority.get(str(item.get("layer", "")).strip(), 9),
            str(item.get("date", "")),
        ),
    )
    for item in ranked_items[:max_items]:
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        layer = str(item.get("layer", "")).strip() or "证据"
        source = str(item.get("source", "")).strip()
        date = str(item.get("date", "")).strip()
        link = str(item.get("link", "")).strip()
        title_text = f"[{title}]({link})" if link else title
        suffix_parts = [part for part in (source, date) if part]
        suffix = f"（{' / '.join(suffix_parts)}）" if suffix_parts else ""
        lines.append(f"- `{layer}`：{title_text}{suffix}")
    return lines


def _analysis_provenance_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    provenance = dict(analysis.get("provenance") or build_analysis_provenance(analysis))
    rows: List[List[str]] = [
        ["分析生成时间", provenance.get("analysis_generated_at", "—")],
        ["行情 as_of", provenance.get("market_data_as_of", "—")],
        ["行情来源", provenance.get("market_data_source", "—")],
        ["分钟级快照 as_of", provenance.get("intraday_as_of", "未启用")],
        ["催化证据 as_of", provenance.get("catalyst_evidence_as_of", "—")],
        ["催化来源", provenance.get("catalyst_sources_text", "—")],
        ["新闻模式", provenance.get("news_mode", "unknown")],
        ["时点边界", provenance.get("point_in_time_note", "默认只使用生成时点前可见信息。")],
    ]
    for item in list(provenance.get("notes") or [])[:2]:
        rows.append(["时点/溯源提醒", str(item)])
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

    lines = [
        f"### {name} ({symbol})",
        "",
    ]
    if bucket == "正式推荐":
        lines.append("为什么能进正式推荐：")
        lines.append("")
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
    lines.extend(
        [
            f"- 介入条件：{_pick_client_safe_line(action.get('entry', '等待进一步确认'))}",
            f"- 首次仓位：{_pick_client_safe_line(action.get('position', '小仓位分批'))}",
            f"- 加仓节奏：{_pick_client_safe_line(action.get('scaling_plan', '确认后再考虑第二笔'))}",
            f"- 止损参考：{_pick_client_safe_line(action.get('stop', '重新跌破关键支撑就处理'))}",
            f"- 组合落单前：{handoff['summary']}",
            f"- 预演命令：`{handoff['command']}`",
        ]
    )
    return lines


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
        reason = str(dimension.get("summary", "")).strip() or str(dimension.get("core_signal", "")).strip()
        rows.append([label, display, reason])
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
                str(factor.get("signal", "")).strip() or "—",
                str(factor.get("detail", "")).strip() or "—",
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
    overview = dict(fund_profile.get("overview") or {})
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

    asset_mix = list(fund_profile.get("asset_mix") or [])
    if asset_mix:
        mix_rows = [
            [str(item.get("资产类型", "—")), f"{item.get('占总资产比例', 0.0):.2f}%"]
            for item in asset_mix[:6]
        ]
        lines.extend(["", "### 资产配置", ""])
        lines.extend(_table(["资产类型", "仓位占比"], mix_rows))

    top_holdings = list(fund_profile.get("top_holdings") or [])
    if top_holdings:
        holding_rows = [
            [
                item.get("股票代码", "—"),
                item.get("股票名称", "—"),
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
    if notes:
        for item in notes[:3]:
            lines.append(f"- {item}")
    else:
        lines.append("- 当前没有额外降级或回退说明。")
    return lines


def _rename_markdown_heading(markdown_text: str, mapping: Mapping[str, str]) -> str:
    if not markdown_text:
        return ""
    lines = markdown_text.splitlines()
    rewritten: List[str] = []
    for line in lines:
        stripped = line.strip()
        rewritten.append(mapping.get(stripped, line))
    return "\n".join(rewritten).rstrip()


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


def _bucket_summary_text(bucket: str, analysis: Mapping[str, Any]) -> str:
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
        if label:
            ctx = f"（{context}）" if context else ""
            return {
                "code": str(structured.get("code", "")).strip(),
                "label": label,
                "style": f"{style}{ctx}" if style else "",
                "fit_reason": f"{fit_reason}{ctx}" if fit_reason else "",
                "misfit_reason": f"{misfit_reason}{ctx}" if misfit_reason else "",
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
        (r"开盘\s*30\s*分钟", "早段"),
        (r"开盘后先观察\s*\d+\s*分钟", "先观察早段延续性"),
        (r"明天开盘前", "明早"),
        (r"盘中", "交易时段"),
        (r"隔日涨跌", "短期涨跌"),
        (r"只按隔夜消息", "只按单条消息"),
        (r"纯隔夜交易", "纯超短交易"),
        (r"隔夜交易", "超短交易"),
    )
    for pattern, repl in replacements:
        line = re.sub(pattern, repl, line)
    return line


class ClientReportRenderer:
    """Render concise client-facing reports from structured payloads."""

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
        return sorted(
            items,
            key=lambda item: (
                _bucket_priority(_recommendation_bucket(item, watch_symbols)),
                -int(item.get("rating", {}).get("rank", 0) or 0),
                -_score(item, "relative_strength"),
                -_score(item, "fundamental"),
            ),
        )

    def render_stock_picks_detailed(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        day_theme = str(payload.get("day_theme", {}).get("label", "未识别"))
        regime = str(payload.get("regime", {}).get("current_regime", "unknown"))
        market_label = str(payload.get("market_label", "全市场")).strip() or "全市场"
        top = list(payload.get("top") or [])
        watch_symbols = {
            str(item.get("symbol", ""))
            for item in (payload.get("watch_positive") or [])
            if str(item.get("symbol", "")).strip()
        }
        grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for item in top:
            grouped[_market_label(str(item.get("asset_type", "")))].append(item)

        lines = [
            f"# 今日个股推荐（详细版） | {generated_at}",
            "",
            "## 今日结论",
            "",
            (
                f"今天按 `{market_label}` 范围筛，背景更接近 `{regime}`，主线偏 `{day_theme}`。"
                " 这不是全市场无差别普涨的环境，更适合分市场只抓少数逻辑、位置和执行条件还能兼顾的标的。"
            ),
            "",
        ]
        coverage = dict(payload.get("stock_pick_coverage") or {})
        coverage_lines = list(coverage.get("lines") or [])
        if coverage_lines:
            lines.append(f"**数据完整度：** {coverage.get('note', '未标注')}")
            for item in coverage_lines[:3]:
                lines.append(f"- {item}")
            lines.append("- 覆盖率的分母是当前纳入详细分析的各市场标的，不是全市场扫描池。")
            lines.append("- 新闻热度更看多源共振；单一来源只算提及，不等于热度确认。")
            lines.append("- 相关性/分散度按各市场观察池基准代理，不同市场之间只适合看相对高低，不适合直接横向比较绝对值。")
            lines.append("")

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked_items = self._rank_market_items(items, watch_symbols)
            best = ranked_items[0]
            best_label = str(best.get("name", best.get("symbol", "")))
            best_bucket = _recommendation_bucket(best, watch_symbols)
            scope_text = _market_pick_scope_text(best, generated_at=str(payload.get("generated_at", "")))
            if best_bucket == "正式推荐":
                lines.append(f"- {market_name}{scope_text}优先看：`{best_label}`")
            else:
                lines.append(f"- {market_name}{scope_text}暂不做正式推荐，优先观察：`{best_label}`")

        lines.extend(
            [
                "",
                "更适合的做法仍然是：`先小仓、等确认、分批做，不把一条观点直接打满。`",
            ]
        )

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked = self._rank_market_items(items, watch_symbols)
            used_reason_lines: Counter[str] = Counter()
            lines.extend(["", f"## {market_name}", ""])

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

            for item in ranked[:3]:
                bucket = _recommendation_bucket(item, watch_symbols)
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
                if visual_lines:
                    lines.extend(visual_lines)
                    lines.append("")
                lines.extend(
                    [
                        *_analysis_section_lines(
                            item,
                            bucket,
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
                lines.extend(_table(["催化子项", "层级", "当前信号", "得分"], _catalyst_factor_rows(catalyst_dimension)))
                if any(str(factor.get("display_score", "")).startswith("-") for factor in catalyst_dimension.get("factors", [])):
                    lines.extend(["", "- 注：催化总分按 0 封底；负面事件会先体现在子项扣分和正文风险提示里。"])
                evidence = list(catalyst_dimension.get("evidence") or [])
                evidence_lines = _evidence_lines(evidence, max_items=2)
                lines.extend(["", "**催化证据来源：**", ""])
                if evidence_lines:
                    lines.extend(evidence_lines)
                else:
                    provenance = dict(item.get("provenance") or build_analysis_provenance(item))
                    lines.append(
                        "当前没有高置信直连催化，先按 `"
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
                lines.extend(_table(["风险子项", "当前信号", "说明", "得分"], _factor_rows(risk_dimension)))
                lines.extend(["", *_signal_confidence_lines(item)])

            watch_items = [item for item in ranked if _recommendation_bucket(item, watch_symbols) != "正式推荐"]
            if watch_items:
                lines.extend(["", "### 看好但暂不推荐", ""])
                for item in watch_items[:3]:
                    positives = _take_diverse_reason_lines(
                        _merge_reason_lines(
                            _top_dimension_reasons(item, top_n=2),
                            list((item.get("narrative") or {}).get("positives") or []),
                            max_items=4,
                        ),
                        used_reason_lines,
                        max_items=2,
                    )
                    cautions = _take_diverse_reason_lines(
                        _merge_reason_lines(
                            _bottom_dimension_reasons(item, top_n=2),
                            list((item.get("narrative") or {}).get("cautions") or []),
                            max_items=4,
                        ),
                        used_reason_lines,
                        max_items=2,
                    )
                    lines.append(f"#### {item.get('name', '—')} ({item.get('symbol', '—')})")
                    lines.append("")
                    lines.append("值得继续看的地方：")
                    lines.append("")
                    for reason in positives[:2]:
                        lines.append(f"- {reason}")
                    lines.extend(["", "今天不放进正式推荐的原因：", ""])
                    for reason in cautions[:2]:
                        lines.append(f"- {reason}")
                    lines.append("")

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

        return "\n".join(lines).rstrip()

    def render_stock_picks(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        day_theme = str(payload.get("day_theme", {}).get("label", "未识别"))
        regime = str(payload.get("regime", {}).get("current_regime", "unknown"))
        market_label = str(payload.get("market_label", "")).strip()
        top = list(payload.get("top") or [])
        watch_symbols = {
            str(item.get("symbol", ""))
            for item in (payload.get("watch_positive") or [])
            if str(item.get("symbol", "")).strip()
        }
        grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
        for item in top:
            grouped[_market_label(str(item.get("asset_type", "")))].append(item)
        used_reason_lines: Counter[str] = Counter()

        lines = [
            f"# 今日个股推荐 | {generated_at}",
            "",
            "## 今日结论",
            "",
            (
                "今天更像结构性机会，不适合把全市场当成同一条主线去追。更合理的是分市场只抓少数几只逻辑、位置和交易条件还能兼顾的标的。"
                if market_label == "全市场"
                else f"今天更适合在 `{day_theme}` / `{regime}` 这个框架下，分市场只抓少数几只逻辑和位置还能兼顾的标的。"
            ),
            "",
        ]
        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked_items = self._rank_market_items(items, watch_symbols)
            best = ranked_items[0]
            best_label = str(best.get("name", best.get("symbol", "")))
            best_bucket = _recommendation_bucket(best, watch_symbols)
            scope_text = _market_pick_scope_text(best, generated_at=str(payload.get("generated_at", "")))
            if best_bucket == "正式推荐":
                lines.append(f"- {market_name}{scope_text}首选：`{best_label}`")
            else:
                lines.append(f"- {market_name}{scope_text}暂不正式推荐，优先观察：`{best_label}`")
        lines.extend(
            [
                "",
                "今天没有哪只票适合一把梭。更合理的做法仍然是：`先小仓，等回踩或确认后再加。`",
            ]
        )

        for market_name in ("A股", "港股", "美股"):
            items = grouped.get(market_name, [])
            if not items:
                continue
            ranked = self._rank_market_items(items, watch_symbols)
            lines.extend(
                [
                    "",
                    f"## {market_name}",
                    "",
                ]
            )
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

            formal = [item for item in ranked if _recommendation_bucket(item, watch_symbols) == "正式推荐"]
            for item in formal[:2]:
                lines.extend(
                    [
                        "",
                        *_analysis_section_lines(
                            item,
                            "正式推荐",
                            used_positive_reasons=used_reason_lines,
                            used_caution_reasons=used_reason_lines,
                            generated_at=str(payload.get("generated_at", "")),
                        ),
                    ]
                )

            watch_items = [item for item in ranked if _recommendation_bucket(item, watch_symbols) != "正式推荐"]
            if watch_items:
                lines.extend(["", "### 看好但暂不推荐", ""])
                for item in watch_items[:2]:
                    lines.extend(
                        _analysis_section_lines(
                            item,
                            "看好但暂不推荐",
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
        return "\n".join(lines).rstrip()

    def render_scan_detailed(self, analysis: Dict[str, Any]) -> str:
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
                "## 分析元数据": "## 证据时点与来源",
            },
        )
        return _inject_scan_reasoning_table(rewritten, analysis)

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
            "## 为什么这么判断",
            "",
        ]
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
        catalyst_dimension = dict(analysis.get("dimensions", {}).get("catalyst") or {})
        evidence_lines = _evidence_lines(list(catalyst_dimension.get("evidence") or []), max_items=3)
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
            ]
        )
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", action.get("direction", "观察为主")],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["周期理由", _pick_client_safe_line(horizon.get("fit_reason", horizon.get("style", "先按当前动作、仓位和止损框架理解。")))],
                    ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把当前动作自动理解成另一种更长或更短的打法。"))],
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`"],
                    ["介入条件", _pick_client_safe_line(action.get("entry", "等待进一步确认"))],
                    ["首次仓位", _pick_client_safe_line(action.get("position", "小仓位分批"))],
                    ["加仓节奏", _pick_client_safe_line(action.get("scaling_plan", "确认后再考虑第二笔"))],
                    ["止损参考", _pick_client_safe_line(action.get("stop", "重新跌破关键支撑就处理"))],
                    ["目标参考", _pick_client_safe_line(action.get("target", "先看前高压力位"))],
                ],
            )
        )
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
        return "\n".join(lines).rstrip()

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
        rendered = self.render_scan_detailed(analysis)
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        lines = rendered.splitlines()
        if lines and lines[0].startswith("# "):
            lines[0] = f"# {name} ({symbol}) | 个股详细分析 | {generated_at}"
        return "\n".join(lines).rstrip()

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
        data_coverage = str(payload.get("data_coverage", "")).strip()
        missing_sources = str(payload.get("missing_sources", "")).strip()
        lines = [
            f"# 今日晨报 | {generated_at}",
            "",
            "## 今日最重要的判断",
            "",
        ]
        if headline_lines:
            lines.append(headline_lines[0])
            for item in headline_lines[1:3]:
                lines.append("")
                lines.append(item)
        else:
            lines.append("今天更适合先看风险控制，再看进攻节奏。")
        lines.extend(["", "## 为什么今天这么判断", ""])
        why_lines = headline_lines[1:4] if len(headline_lines) > 1 else []
        if why_lines:
            for item in why_lines:
                lines.append(f"- {item}")
        else:
            lines.append("- 今天的判断不是看单一涨跌，而是看波动、主线和资金是否真正共振。")
        if macro_items:
            lines.extend(["", "## 宏观领先指标", ""])
            for item in macro_items[:5]:
                lines.append(f"- {item}")
        lines.extend(["", "## 数据完整度", ""])
        lines.append(f"- 本次覆盖：{data_coverage or '未标注'}。")
        lines.append(f"- 当前缺失：{missing_sources or '无'}。")
        detail_lines = [
            item
            for item in quality_lines
            if any(token in item for token in ("代理", "改用", "覆盖", "缺失", "复核", "免费源"))
        ]
        if not detail_lines:
            detail_lines = quality_lines
        for item in detail_lines[:2]:
            lines.append(f"- {item}")
        lines.extend(["", "## 今天怎么做", ""])
        for item in action_lines[:4]:
            lines.append(f"- {item}")
        lines.extend(["", "## 重点观察", ""])
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
        lines.extend(["", "## 今天最值得看的 3 个方向", ""])
        if theme_rows:
            for index, row in enumerate(theme_rows[:3], start=1):
                direction = row[0] if len(row) > 0 else f"方向 {index}"
                catalyst = row[1] if len(row) > 1 else "暂无"
                logic = row[2] if len(row) > 2 else "暂无"
                risk = row[4] if len(row) > 4 else "暂无"
                lines.extend(
                    [
                        f"### {index}. {direction}",
                        "",
                        f"- 为什么值得看：{logic}",
                        f"- 当前催化：{catalyst}",
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
        return "\n".join(lines).rstrip()

    def render_fund_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        alternatives = list(payload.get("alternatives") or [])
        selection_context = dict(payload.get("selection_context") or {})
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
            "## 今日结论",
            "",
            lead_line,
            "",
            f"这不是激进进攻型推荐，而是：**`{winner.get('trade_state', '持有优于追高')}`**",
            "",
            f"这份建议的适用时段：{handoff.get('timing_summary', '先按当前可申赎窗口理解，不把它默认成必须立刻处理。')}",
            "",
        ]
        if horizon:
            lines.extend(
                [
                    f"当前更合适的持有周期：**`{horizon.get('label', '观察期')}`**",
                    "",
                    f"这更像：{horizon.get('style', '')}",
                    "",
                ]
            )
            if horizon.get("fit_reason"):
                lines.append(f"为什么按这个周期看：{horizon.get('fit_reason')}")
                lines.append("")
            if horizon.get("misfit_reason"):
                lines.append(f"现在不适合：{horizon.get('misfit_reason')}")
                lines.append("")
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
        if selection_context.get("coverage_note"):
            lines.append(f"- {selection_context.get('coverage_note')}")
        else:
            lines.append("- 当前没有额外覆盖率备注，默认按已进入完整分析的样本理解。")
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
        lines.extend(
            [
            why_heading,
            "",
            ]
        )
        for item in winner.get("positives", [])[:3]:
            lines.append(f"- {item}")
        lines.extend(["", "## 这只基金为什么是这个分", ""])
        lines.extend(
            _table(
                ["维度", "分数", "为什么是这个分"],
                winner.get("dimension_rows", []),
            )
        )
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
        evidence_lines = _evidence_lines(list(winner.get("evidence") or []), max_items=3)
        lines.extend(["", "## 关键证据", ""])
        if evidence_lines:
            lines.extend(evidence_lines)
        else:
            lines.append("- 当前没有可直接复核的高置信直连证据，催化判断更多依赖结构化事件、基金画像或历史有效信号。")
        lines.extend(["", *_analysis_provenance_lines(winner)])
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["适用打法", _pick_client_safe_line(horizon.get("style", "先按当前动作、仓位和止损框架理解，不把它默认当成长线配置。"))],
                    ["为什么按这个周期看", _pick_client_safe_line(horizon.get("fit_reason", "当前更适合按已有动作和仓位框架理解。"))],
                    ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把它自动理解成另一种更长或更短的打法。"))],
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可申赎窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', 'portfolio whatif buy 标的 最新净值 计划金额')}`"],
                    ["介入条件", _pick_client_safe_line(winner.get("action", {}).get("entry", "等回撤再看"))],
                    ["首次仓位", _pick_client_safe_line(winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2"))],
                    ["加仓节奏", _pick_client_safe_line(winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔"))],
                    ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
                ],
            )
        )
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
        return "\n".join(lines).rstrip()

    def render_etf_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        alternatives = list(payload.get("alternatives") or [])
        selection_context = dict(payload.get("selection_context") or {})
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
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        lead_line = (
            f"如果按{handoff.get('decision_scope', '今天的交易计划')}先排一个观察优先的 ETF 对象，我先看：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
            if observe_only
            else f"如果按{handoff.get('decision_scope', '今天的交易计划')}只看一只 ETF，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
        )
        title = "今日ETF观察" if observe_only else "今日ETF推荐"
        lines = [
            f"# {title} | {generated_at}",
            "",
            "## 今日结论",
            "",
            lead_line,
            "",
            f"这不是无脑追高型推荐，而是：**`{winner.get('trade_state', '持有优于追高')}`**",
            "",
            f"这份建议的适用时段：{handoff.get('timing_summary', '先按当前可交易窗口理解，不把它默认成必须立刻处理。')}",
            "",
        ]
        if horizon:
            lines.extend(
                [
                    f"当前更合适的持有周期：**`{horizon.get('label', '观察期')}`**",
                    "",
                    f"这更像：{horizon.get('style', '')}",
                    "",
                ]
            )
            if horizon.get("fit_reason"):
                lines.append(f"为什么按这个周期看：{horizon.get('fit_reason')}")
                lines.append("")
            if horizon.get("misfit_reason"):
                lines.append(f"现在不适合：{horizon.get('misfit_reason')}")
                lines.append("")
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
        lines.extend(["", why_heading, ""])
        for item in winner.get("positives", [])[:4]:
            lines.append(f"- {item}")
        lines.extend(["", "## 这只ETF为什么是这个分", ""])
        lines.extend(
            _table(
                ["维度", "分数", "为什么是这个分"],
                winner.get("dimension_rows", []),
            )
        )
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
        evidence_lines = _evidence_lines(list(winner.get("evidence") or []), max_items=3)
        lines.extend(["", "## 关键证据", ""])
        if evidence_lines:
            lines.extend(evidence_lines)
        else:
            lines.append("- 当前没有可直接复核的高置信直连证据，催化判断更多依赖结构化事件或历史有效信号。")
        lines.extend(["", *_analysis_provenance_lines(winner)])
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["适用打法", _pick_client_safe_line(horizon.get("style", "先按当前动作、仓位和止损框架理解，不把它默认当成长线配置。"))],
                    ["为什么按这个周期看", _pick_client_safe_line(horizon.get("fit_reason", "当前更适合按已有动作和仓位框架理解。"))],
                    ["现在不适合", _pick_client_safe_line(horizon.get("misfit_reason", "不要把它自动理解成另一种更长或更短的打法。"))],
                    ["适用时段", _pick_client_safe_line(handoff.get("timing_summary", "先按当前可交易窗口理解，不把这条观点默认成必须立刻执行。"))],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', 'portfolio whatif buy 标的 最新价 计划金额')}`"],
                    ["介入条件", _pick_client_safe_line(winner.get("action", {}).get("entry", "等回撤再看"))],
                    ["首次仓位", _pick_client_safe_line(winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2"))],
                    ["加仓节奏", _pick_client_safe_line(winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔"))],
                    ["止损参考", _pick_client_safe_line(winner.get("action", {}).get("stop", "重新跌破关键支撑就处理"))],
                    ["目标参考", _pick_client_safe_line(winner.get("action", {}).get("target", "先看前高压力位"))],
                ],
            )
        )
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
        lines.extend(["## 仓位管理", ""])
        for item in winner.get("positioning_lines", []):
            lines.append(f"- {item}")
        lines.extend(["", "## 组合落单前", ""])
        lines.append(f"- {handoff.get('summary', '先跑组合预演，再决定真实金额。')}")
        lines.append(f"- 命令：`{handoff.get('command', 'portfolio whatif buy 标的 最新价 计划金额')}`")
        notes = [str(item).strip() for item in (payload.get("notes") or []) if str(item).strip()]
        if notes:
            lines.extend(["", "## 数据限制与说明", ""])
            for item in notes[:3]:
                lines.append(f"- {item}")
        return "\n".join(lines).rstrip()
