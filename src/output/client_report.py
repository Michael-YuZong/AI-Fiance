"""Client-facing report renderers."""

from __future__ import annotations

from collections import Counter, defaultdict
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

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
) -> List[str]:
    regime_payload = dict(regime or {})
    regime_name = str(regime_payload.get("current_regime", "")).strip()
    reasoning = [str(item).strip() for item in regime_payload.get("reasoning", []) if str(item).strip()]
    if not regime_name or not reasoning:
        return []
    lines = [heading, ""]
    lines.extend(_section_lead_lines("这段只回答为什么把今天的中期背景判断成这个 regime，不把切换写成无依据结论。"))
    for item in reasoning[:3]:
        lines.append(f"- {item}")
    if day_theme:
        lines.append(f"- 当天主线写成 `{day_theme}`，是这层中期背景里的短线表达，不等于 macro regime 重新切档。")
    return lines


def _section_lead_lines(text: str) -> List[str]:
    line = str(text).strip()
    if not line:
        return []
    return [f"**先看结论：** {line}", ""]


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
    technical = _score(analysis, "technical")
    fundamental = _score(analysis, "fundamental")
    catalyst = _score(analysis, "catalyst")
    relative = _score(analysis, "relative_strength")
    risk = _score(analysis, "risk")

    support_dims = sum(score >= 60 for score in (technical, fundamental, catalyst, relative, risk))
    positive_dims = sum(score >= 60 for score in (fundamental, catalyst, relative, risk))
    elite_positive = max(fundamental, catalyst, relative, risk) >= 80

    def qualified_watch() -> bool:
        return (
            positive_dims >= 2
            or (elite_positive and technical >= 35)
            or (fundamental >= 75 and catalyst >= 30 and technical >= 30)
            or (catalyst >= 60 and relative >= 45 and technical >= 25)
            or (relative >= 70 and technical >= 45)
            or support_dims >= 3
        )

    if rating_rank >= 3:
        return "正式推荐"
    if rating_rank >= 2:
        return "看好但暂不推荐"
    if watch_symbols and str(analysis.get("symbol", "")) in watch_symbols and qualified_watch():
        return "看好但暂不推荐"
    if qualified_watch():
        return "看好但暂不推荐"
    return "观察为主"


def _analysis_is_actionable(
    analysis: Mapping[str, Any],
    watch_symbols: Optional[set[str]] = None,
) -> bool:
    bucket = _recommendation_bucket(analysis, watch_symbols)
    if bucket == "正式推荐":
        return True
    if bucket != "看好但暂不推荐":
        return False

    action = dict(analysis.get("action") or {})
    direction = _pick_client_safe_line(action.get("direction", ""))
    position = _pick_client_safe_line(action.get("position", ""))
    entry = _pick_client_safe_line(action.get("entry", ""))
    combined = " / ".join(part for part in (direction, position, entry) if part)
    non_action_markers = (
        "暂不出手",
        "仅观察仓",
        "先按观察仓",
        "先观察",
        "观察为主",
        "回避",
        "等待更好窗口",
        "触发前先别急着",
    )
    if any(marker in combined for marker in non_action_markers):
        return False
    if "观望" in direction and not any(token in combined for token in ("试仓", "建仓", "小仓", "%", "分批")):
        return False
    return True


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
        rows.append(
            [
                labels[len(rows)] if len(rows) < len(labels) else f"观察 {len(rows) + 1}",
                f"{item.get('name', '—')} ({symbol})",
                _analysis_track_reason(item),
                _pick_client_safe_line(_decision_gate_explanation(item)),
                _pick_client_safe_line(_primary_upgrade_trigger(item)),
                _observe_watch_levels(item) or "先等关键位和动能一起改善。",
            ]
        )
        if len(rows) >= limit:
            break
    return rows


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
) -> List[List[str]]:
    affordable = [
        item
        for item in ClientReportRenderer._rank_market_items(items, watch_symbols)
        if _analysis_is_actionable(item, watch_symbols) and (_cn_stock_lot_cost(item) or float("inf")) <= max_lot_cost
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


def _evidence_lines(
    items: Sequence[Mapping[str, Any]],
    *,
    max_items: int = 3,
    as_of: Any = None,
) -> List[str]:
    lines: List[str] = []
    as_of_stamp = _report_timestamp(as_of)
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
        lines.append(f"- `{layer}`：{title_text}{suffix}")
    return lines


def _analysis_provenance_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    provenance = dict(analysis.get("provenance") or build_analysis_provenance(analysis))
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
        f"**先看结论：** {_analysis_section_takeaway(analysis, bucket)}",
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
    evidence_lines = _evidence_lines(
        list(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []),
        max_items=1,
        as_of=analysis.get("generated_at"),
    )
    watch_levels = _observe_watch_levels(analysis)
    trigger_line = _observe_trigger_condition(
        analysis,
        horizon,
        default_text="等技术确认、相对强弱和时点一起改善后，再决定要不要给买入区间。",
    )
    constraint_hint = _analysis_constraint_hint(analysis)
    if constraint_hint and constraint_hint not in trigger_line:
        trigger_line = _append_sentence(trigger_line, constraint_hint)

    lines = [
        f"### {name} ({symbol}) | {bucket}",
        "",
        f"**先看结论：** {_analysis_section_takeaway(analysis, bucket)}",
        "",
        "为什么继续看它：",
        "",
    ]
    for item in positives[:2]:
        lines.append(f"- {item}")
    lines.extend(["", "为什么现在不升级成正式推荐：", ""])
    for item in cautions[:2]:
        lines.append(f"- {item}")
    lines.extend(_pick_upgrade_lines(analysis))
    lines.extend(["", "下一步怎么盯：", ""])
    if horizon.get("label"):
        watch_profile = _pick_client_safe_line(horizon.get("fit_reason") or horizon.get("style") or "先看确认，不急着按正式动作理解。")
        lines.append(f"- 当前更像{horizon['label']}：{watch_profile}")
    lines.append(f"- 对 `{name}` 来说，触发买点条件是：{trigger_line}")
    if watch_levels:
        lines.append(f"- 关键盯盘价位：{watch_levels}")
    lines.append(f"- 首次仓位：{_pick_client_safe_line(action.get('position', '≤2% 观察仓，或先不出手'))}")
    lines.extend(["", "证据口径：", ""])
    if evidence_lines:
        lines.extend(evidence_lines)
    else:
        provenance = dict(analysis.get("provenance") or build_analysis_provenance(analysis))
        lines.append(
            f"- `{name}` 当前没有高置信直连催化，先按 `"
            + str(provenance.get("catalyst_sources_text", "结构化事件/代理来源"))
            + "` 这层来源理解。"
        )
    return lines


def _analysis_detail_appendix_lines(analysis: Mapping[str, Any]) -> List[str]:
    catalyst_dimension = dict(analysis.get("dimensions", {}).get("catalyst") or {})
    risk_dimension = dict(analysis.get("dimensions", {}).get("risk") or {})
    evidence = list(catalyst_dimension.get("evidence") or [])
    evidence_lines = _evidence_lines(evidence, max_items=2, as_of=analysis.get("generated_at"))
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
    lines.extend(_table(["催化子项", "层级", "当前信号", "得分"], _catalyst_factor_rows(catalyst_dimension)))
    lines.extend(["", "**催化证据来源：**", ""])
    if evidence_lines:
        lines.extend(evidence_lines)
    else:
        provenance = dict(analysis.get("provenance") or build_analysis_provenance(analysis))
        lines.append(
            "当前没有高置信直连催化，先按 `"
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
    lines.extend(_table(["风险子项", "当前信号", "说明", "得分"], _factor_rows(risk_dimension)))
    lines.extend(["", *_signal_confidence_lines(analysis)])
    return lines


def _stock_pick_shared_evidence_lines(items: Sequence[Mapping[str, Any]]) -> List[str]:
    for item in items:
        evidence = list(dict(item.get("dimensions", {}).get("catalyst") or {}).get("evidence") or [])
        evidence_lines = _evidence_lines(evidence, max_items=2, as_of=item.get("generated_at"))
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
    lines.extend(_section_lead_lines("这段只回答哪些判断来自代理层、这些代理能信到什么程度。"))
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
        return _compose_trigger_sentence(phrases, buy_range=buy_range)
    if buy_range and "暂不设" not in buy_range:
        return f"先等价格回到 `{buy_range}` 一带并确认承接，再决定要不要动。"
    if derived_phrases:
        return _compose_trigger_sentence(derived_phrases, buy_range=buy_range)
    constraint = _analysis_constraint_hint(analysis)
    if constraint:
        return _pick_client_safe_line(constraint)
    fit_reason = str(horizon.get("fit_reason", "")).strip()
    if fit_reason:
        return _pick_client_safe_line(f"先等{fit_reason}，再决定要不要升级成可执行方案。")
    return _pick_client_safe_line(default_text)


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
    trigger_text = _pick_client_safe_line(_primary_upgrade_trigger(analysis))
    lines: List[str] = []
    if why_text:
        lines.append(f"- 为什么还不升级：{why_text}")
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
            *_present_action_row("关键盯盘价位", _observe_watch_levels(winner)),
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
    if observe_conflict and direction and direction != current_action:
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
) -> List[str]:
    if not rows:
        return []
    lines = [heading, ""]
    lines.extend(_section_lead_lines(lead))
    lines.extend(_table(["项目", "建议"], rows))
    return lines


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
    direct_news_zero = any("高置信直接新闻覆盖 0%" in line for line in coverage_lines)
    if not (structured_positive and direct_news_zero):
        return ""
    if asset_label == "场外基金":
        return "当前证据更偏结构化事件、基金画像和持仓/基准映射，不是直接新闻催化型驱动。"
    if asset_label == "ETF":
        return "当前证据更偏结构化事件、产品画像和持仓/基准映射，不是直接新闻催化型驱动。"
    return f"当前证据更偏结构化事件与公告日历，不是{asset_label}的新闻催化型驱动；`0%` 只代表没命中高置信个股直连新闻。"


def _briefing_summary_rows(
    *,
    headline_lines: Sequence[str],
    action_lines: Sequence[str],
    regime: Mapping[str, Any],
    day_theme: str,
    a_share_watch_meta: Mapping[str, Any],
    quality_lines: Sequence[str],
) -> List[List[str]]:
    rows: List[List[str]] = []
    if headline_lines:
        rows.append(["当前判断", str(headline_lines[0]).strip()])
    if action_lines:
        rows.append(["优先动作", str(action_lines[0]).strip()])
    regime_name = str(dict(regime or {}).get("current_regime", "")).strip()
    if regime_name or day_theme:
        rows.append(["中期背景 / 当天主线", f"{regime_name or '未标注'} / {day_theme or '未标注'}"])
    pool_size = int(dict(a_share_watch_meta or {}).get("pool_size") or 0)
    complete_size = int(dict(a_share_watch_meta or {}).get("complete_analysis_size") or 0)
    if pool_size or complete_size:
        rows.append(["A股观察池", f"全市场初筛 `{pool_size}` -> 完整分析 `{complete_size}`。"])
    if quality_lines:
        rows.append(["当前限制", str(quality_lines[0]).strip()])
    return rows[:5]


def _briefing_watch_upgrade_lines(candidates: Sequence[Mapping[str, Any]]) -> List[str]:
    rows: List[str] = []
    items = list(candidates or [])
    if not items:
        return rows
    rows.append("当前A股观察池更像全市场方向筛选，不等于今天已经出现正式动作票。")
    for item in items[:2]:
        name = str(item.get("name", item.get("symbol", ""))).strip()
        symbol = str(item.get("symbol", "")).strip()
        label = f"{name} ({symbol})" if symbol else name
        why_text = _decision_gate_explanation(item)
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
    evidence_fallback: str,
    no_alternative_text: str,
) -> List[str]:
    lines: List[str] = []
    lines.extend(["", *_taxonomy_section(winner)])
    lines.extend(["", "## 关键证据", ""])
    evidence_lines = _evidence_lines(list(winner.get("evidence") or []), max_items=2, as_of=winner.get("generated_at"))
    if evidence_lines:
        lines.extend(evidence_lines)
    else:
        lines.append(f"- {evidence_fallback}")
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


def _sanitize_client_markdown(text: str) -> str:
    lines: List[str] = []
    for raw in str(text or "").splitlines():
        stripped = raw.strip()
        if not stripped:
            lines.append(raw)
            continue
        lines.append(_pick_client_safe_line(raw))
    return "\n".join(lines)


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
        summary_rows = _stock_pick_summary_rows(
            day_theme=day_theme,
            regime=regime,
            sector_filter=sector_filter,
            grouped=grouped,
            coverage_grouped=coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
        )
        compact_observe = not has_actionable

        lines = [
            f"# {'今日个股推荐（详细版）' if has_actionable else '今日个股观察（详细版）'} | {generated_at}",
            "",
            "## 今日结论",
            "",
            *_section_lead_lines(
                "先看各市场的短线/中线优先级，再决定要不要往下读单票细节。"
                if has_actionable
                else "先看今天哪些票还值得继续观察，以及要等什么条件才值得动手。"
            ),
            (
                (
                    f"今天按 `{market_label}` 范围筛，背景更接近 `{regime}`，主线偏 `{day_theme}`。"
                    " 这不是全市场无差别普涨的环境，更适合分市场只抓少数逻辑、位置和执行条件还能兼顾的标的。"
                )
                if has_actionable
                else (
                    f"今天按 `{market_label}` 范围筛，背景更接近 `{regime}`，主线偏 `{day_theme}`。"
                    " 但这轮候选还没出现达到正式动作阈值的个股，当前更适合按观察名单理解，不把它包装成可直接下单的推荐稿。"
                )
            ),
            (
                "当前主线更像在支撑方向不删，还不够支撑价格与动量确认都已完成；方向没有被删，但买点也还没被确认。"
                if not has_actionable
                else ""
            ),
            (
                f"当前这份稿更像 `{sector_filter}` 主题内的相对排序，不是跨主题分散候选池。"
                if sector_filter
                else ""
            ),
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 今日动作摘要",
                    lead="这段只回答今天能不能动、空仓和持仓分别怎么做，以及仓位先怎么配。",
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
            structure_driven_coverage = any("结构化事件覆盖" in item and "高置信公司新闻覆盖 0%" in item for item in coverage_lines)
            lines.append(f"**数据完整度：** {coverage.get('note', '未标注')}")
            if structure_driven_coverage:
                lines.append("- 当前证据更偏结构化事件与公告日历，不是新闻催化型驱动。")
            for item in coverage_lines[:3]:
                lines.append(f"- {item}")
            lines.append("- 覆盖率的分母是当前纳入详细分析的各市场标的，不是全市场扫描池。")
            lines.append("- 新闻热度更看多源共振；单一来源只算提及，不等于热度确认。")
            if structure_driven_coverage:
                lines.append("- 这批当前更依赖结构化事件和公告日历，不等于新闻链路失效；`0%` 只代表没命中高置信个股直连新闻。")
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
                lines.append(f"- {market_name}低门槛可执行先看：`{affordable_rows[0][0].split(' (', 1)[0]}`")
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

        has_formal = any(_recommendation_bucket(item, watch_symbols) == "正式推荐" for item in top)
        if top and not has_formal:
            lines.extend(["", "## 催化证据来源", ""])
            lines.extend(_stock_pick_shared_evidence_lines(top))
            lines.extend(["", "## 历史相似样本附注" if not has_actionable else "## 历史相似样本验证", ""])
            if not has_actionable:
                lines.extend(_section_lead_lines("这层只保留边界参考，不单独支撑今天出手。"))
            lines.extend(_stock_pick_shared_signal_confidence_lines(top))

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
                lines.extend(_section_lead_lines("这批只是更值得继续跟踪的观察名单，不代表现在就该出手。"))
                lines.extend(_table(["层次", "标的", "更适合的周期", "为什么继续看"], watch_rows))

            if compact_observe:
                trigger_rows = _market_watch_trigger_rows(ranked, watch_symbols)
                if trigger_rows:
                    lines.extend(["", "### 观察触发器", ""])
                    lines.extend(
                        _table(
                            ["层次", "标的", "为什么继续看", "主要卡点", "升级条件", "关键盯盘价位"],
                            trigger_rows,
                        )
                    )
                continue

            affordable_rows = _affordable_stock_rows(coverage_grouped.get(market_name, []), watch_symbols)
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
                    lines.extend(_table(["层次", "当前判断", "说明"], _catalyst_structure_rows(catalyst_dimension)))
                    lines.append("")
                    lines.extend(_table(["催化子项", "层级", "当前信号", "得分"], _catalyst_factor_rows(catalyst_dimension)))
                    if any(str(factor.get("display_score", "")).startswith("-") for factor in catalyst_dimension.get("factors", [])):
                        lines.extend(["", "- 注：催化总分按 0 封底；负面事件会先体现在子项扣分和正文风险提示里。"])
                    evidence = list(catalyst_dimension.get("evidence") or [])
                    evidence_lines = _evidence_lines(evidence, max_items=2, as_of=item.get("generated_at") or payload.get("generated_at"))
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
                else:
                    lines.extend(
                        [
                            "",
                            *_analysis_watch_card_lines(
                                item,
                                bucket,
                                used_positive_reasons=used_reason_lines,
                                used_caution_reasons=used_reason_lines,
                                generated_at=str(payload.get("generated_at", "")),
                            ),
                        ]
                    )

        if compact_observe:
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

        return "\n".join(lines).rstrip()

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
        summary_rows = _stock_pick_summary_rows(
            day_theme=day_theme,
            regime=regime,
            sector_filter=sector_filter,
            grouped=grouped,
            coverage_grouped=coverage_grouped,
            watch_symbols=watch_symbols,
            has_actionable=has_actionable,
        )

        lines = [
            f"# {'今日个股推荐' if has_actionable else '今日个股观察'} | {generated_at}",
            "",
            "## 今日结论",
            "",
            *_section_lead_lines(
                "先看短线/中线和低门槛入口，不必把整份报告读完才找建议。"
                if has_actionable
                else "先看今天哪些票还值得继续观察，以及接下来应该等什么触发条件。"
            ),
            (
                (
                    "今天更像结构性机会，不适合把全市场当成同一条主线去追。更合理的是分市场只抓少数几只逻辑、位置和交易条件还能兼顾的标的。"
                    if market_label == "全市场"
                    else f"今天更适合在 `{day_theme}` / `{regime}` 这个框架下，分市场只抓少数几只逻辑和位置还能兼顾的标的。"
                )
                if has_actionable
                else (
                    "今天更像观察日，当前没有达到正式动作阈值的个股。更合理的是先列观察对象和触发条件，不把这份稿件写成可直接下单的推荐单。"
                )
            ),
            "",
        ]
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 今日动作摘要",
                    lead="这段只回答今天能不能动、空仓和持仓分别怎么做，以及仓位先怎么配。",
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
                lines.append(f"- {market_name}低门槛可执行先看：`{affordable_rows[0][0].split(' (', 1)[0]}`")
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

            affordable_rows = _affordable_stock_rows(coverage_grouped.get(market_name, []), watch_symbols)
            related_etf_rows = _related_etf_rows(coverage_grouped.get(market_name, []), watch_symbols)
            if market_name == "A股" and market_has_actionable and (affordable_rows or related_etf_rows):
                lines.extend(["", "### 第二批：低门槛 / 关联ETF", ""])
                lines.extend(_section_lead_lines("这批解决的是单价门槛太高或更想先用ETF承接方向时看什么。"))
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
        rewritten = _inject_scan_reasoning_table(rewritten, analysis)
        rewritten = _inject_scan_exec_summary(rewritten, analysis)
        return _sanitize_client_markdown(rewritten)

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
        summary_rows = _single_asset_exec_summary_rows(
            analysis,
            horizon,
            handoff,
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
        catalyst_dimension = dict(analysis.get("dimensions", {}).get("catalyst") or {})
        evidence_lines = _evidence_lines(list(catalyst_dimension.get("evidence") or []), max_items=3, as_of=analysis.get("generated_at"))
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
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`"],
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
                    *_present_action_row("建议买入区间", _safe_buy_range_text(action)),
                    ["首次仓位", _pick_client_safe_line(action.get("position", "小仓位分批"))],
                    ["加仓节奏", _pick_client_safe_line(action.get("scaling_plan", "确认后再考虑第二笔"))],
                    *_present_action_row("建议减仓区间", action.get("trim_range", "")),
                    ["止损参考", _pick_client_safe_line(action.get("stop", "重新跌破关键支撑就处理"))],
                    *_present_action_row("目标参考", action.get("target", "")),
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
        a_share_watch_meta = dict(payload.get("a_share_watch_meta") or {})
        a_share_watch_candidates = list(payload.get("a_share_watch_candidates") or [])
        data_coverage = str(payload.get("data_coverage", "")).strip()
        missing_sources = str(payload.get("missing_sources", "")).strip()
        regime = dict(payload.get("regime") or {})
        day_theme = str(payload.get("day_theme", "")).strip()
        evidence_rows = list(payload.get("evidence_rows") or [])
        summary_rows = _briefing_summary_rows(
            headline_lines=headline_lines,
            action_lines=action_lines,
            regime=regime,
            day_theme=day_theme,
            a_share_watch_meta=a_share_watch_meta,
            quality_lines=quality_lines,
        )
        watch_upgrade_lines = [self._briefing_client_safe_line(item) for item in _briefing_watch_upgrade_lines(a_share_watch_candidates)]
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
        why_lines = headline_lines[1:4] if len(headline_lines) > 1 else []
        if why_lines:
            for item in why_lines:
                lines.append(f"- {item}")
        else:
            lines.append("- 今天的判断不是看单一涨跌，而是看波动、主线和资金是否真正共振。")
        regime_section = _regime_basis_section(regime, day_theme=day_theme)
        if regime_section:
            lines.extend(["", *regime_section])
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
        proxy_section = _proxy_contract_section(
            dict(payload.get("proxy_contract") or {}),
            heading="## 市场代理信号",
        )
        if proxy_section:
            lines.extend(["", *proxy_section])
        if evidence_rows:
            lines.extend(["", "## 证据时点与来源", ""])
            lines.extend(_table(["项目", "说明"], evidence_rows))
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
        if watch_upgrade_lines:
            lines.extend(["", "## A股观察池升级条件", ""])
            lines.extend(_section_lead_lines("这段只回答观察池里的方向为什么还不能升成正式动作，以及接下来该盯什么触发器。"))
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
        return "\n".join(lines).rstrip()

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
        summary_rows = _single_asset_exec_summary_rows(
            winner,
            horizon,
            handoff,
            selection_context=selection_context,
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
            "## 今日结论",
            "",
            lead_line,
            "",
            f"这不是激进进攻型推荐，而是：**`{winner.get('trade_state', '持有优于追高')}`**",
            "",
        ]
        if observe_only:
            lines.extend([_observe_delivery_bridge_text("场外基金"), ""])
        lines.extend(
            [
            f"这份建议的适用时段：{handoff.get('timing_summary', '先按当前可申赎窗口理解，不把它默认成必须立刻处理。')}",
            "",
            ]
        )
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
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 执行摘要",
                    lead="这段只回答能不能申赎、空仓和持仓分别怎么处理、仓位大概多大，以及主要利好利空是什么。",
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
        lines.extend(_section_lead_lines("这段只回答这份稿靠不靠谱、哪些地方是降级或代理。"))
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
        lines.extend(_section_lead_lines("这段只回答这是不是正式成稿，能不能按正式推荐理解。"))
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
        lines.extend(
            [
            why_heading,
            "",
            *_section_lead_lines("这段只看它为什么能进入今天名单，不重复展开全套动作。"),
            ]
        )
        for item in winner.get("positives", [])[:3]:
            lines.append(f"- {item}")
        lines.extend(["", "## 这只基金为什么是这个分", ""])
        lines.extend(_section_lead_lines("这段先看分数结构，不要只盯最高分那一项。"))
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
                    evidence_fallback="当前没有高置信直连证据，摘要判断主要依赖覆盖率、基金画像和现有代理信号。",
                    no_alternative_text="当前可进入完整评分的基金候选不足，暂时没有可并列展开的第二候选。",
                )
            )
            blind_spots = [str(item).strip() for item in selection_context.get("blind_spots", []) if str(item).strip()]
            if blind_spots:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in blind_spots[:3]:
                    lines.append(f"- {item}")
            return "\n".join(lines).rstrip()
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
        evidence_lines = _evidence_lines(list(winner.get("evidence") or []), max_items=3, as_of=winner.get("generated_at") or payload.get("generated_at"))
        lines.extend(["", "## 关键证据", ""])
        if evidence_lines:
            lines.extend(evidence_lines)
        else:
            lines.append("- 当前没有可直接复核的高置信直连证据，催化判断更多依赖结构化事件、基金画像或历史有效信号。")
        lines.extend(["", *_analysis_provenance_lines(winner)])
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _section_lead_lines(
                "今天先看动作和触发条件；没有触发前，不给精确买点。"
                if observe_only
                else "先看动作、买入区间和仓位，再决定要不要往下读细节。"
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
            return "\n".join(lines).rstrip()
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
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        track_summary = _payload_track_summary_text(recommendation_tracks)
        track_rows = _payload_track_rows(recommendation_tracks)
        track_count = len(track_rows)
        if observe_only:
            if track_summary:
                if track_count >= 2:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}一定要给执行入口，我会分成两档：{track_summary}"
                else:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}只能先给一档，我先看：{track_summary}"
            else:
                lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先排一个观察优先的 ETF 对象，我先看：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
        else:
            if track_summary:
                if track_count >= 2:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先分层理解：{track_summary}"
                else:
                    lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}先看这一档：{track_summary}"
            else:
                lead_line = f"如果按{handoff.get('decision_scope', '今天的交易计划')}只看一只 ETF，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
        summary_rows = list(
            _single_asset_exec_summary_rows(
                winner,
                horizon,
                handoff,
                selection_context=selection_context,
                status_label=str(winner.get("trade_state", "")) or ("观察优先" if observe_only else "推荐"),
                default_trigger="先等技术确认、催化覆盖和数据完整度一起改善，再决定要不要给买入区间。",
                default_holder_text="已有仓位先按止损和主线节奏管理，不把当前判断直接当成继续追涨的理由。",
            )
        )
        if track_rows:
            summary_prefix_rows: List[List[str]] = []
            if len(track_rows) >= 1:
                summary_prefix_rows.append(["短线优先", f"{track_rows[0][1]}；{track_rows[0][3]}"])
            if len(track_rows) >= 2:
                summary_prefix_rows.append(["中线优先", f"{track_rows[1][1]}；{track_rows[1][3]}"])
            summary_rows = summary_prefix_rows + summary_rows
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
        ]
        if observe_only:
            lines.extend([_observe_delivery_bridge_text("ETF"), ""])
        lines.extend(
            [
            f"这份建议的适用时段：{handoff.get('timing_summary', '先按当前可交易窗口理解，不把它默认成必须立刻处理。')}",
            "",
            ]
        )
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
        lines.extend(
            [
                *_summary_block_lines(
                    summary_rows,
                    heading="## 执行摘要",
                    lead="这段只回答今天能不能动、空仓和持仓分别怎么做、仓位大概多大，以及短线中线先看谁。",
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
        if track_rows:
            lines.extend(["", "## 当前分层建议", ""])
            lines.extend(_table(["层次", "标的", "更适合的周期", "为什么先看"], track_rows))
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
            if track_rows:
                summary_rows = [
                    ["短线优先", f"{track_rows[0][1]}；{track_rows[0][3]}"] if len(track_rows) >= 1 else None,
                    ["中线优先", f"{track_rows[1][1]}；{track_rows[1][3]}"] if len(track_rows) >= 2 else None,
                    *summary_rows,
                ]
                summary_rows = [row for row in summary_rows if row]
            lines.extend(_table(["项目", "建议"], summary_rows))
            lines.extend(
                _summary_only_explainer_sections(
                    winner,
                    alternatives,
                    evidence_fallback="当前没有高置信直连证据，摘要判断主要依赖结构化事件、覆盖率和已有代理信号。",
                    no_alternative_text="当前可进入完整评分的 ETF 候选不足，暂时没有可并列展开的第二候选。",
                )
            )
            notes = [str(item).strip() for item in (payload.get("notes") or []) if str(item).strip()]
            if notes:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in notes[:3]:
                    lines.append(f"- {item}")
            return "\n".join(lines).rstrip()
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
        evidence_lines = _evidence_lines(list(winner.get("evidence") or []), max_items=3, as_of=winner.get("generated_at") or payload.get("generated_at"))
        lines.extend(["", "## 关键证据", ""])
        if evidence_lines:
            lines.extend(evidence_lines)
        else:
            lines.append("- 当前没有可直接复核的高置信直连证据，催化判断更多依赖结构化事件或历史有效信号。")
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
        if track_rows:
            staged_rows: List[List[str]] = []
            if len(track_rows) >= 1:
                staged_rows.append(["短线优先", f"{track_rows[0][1]}；{track_rows[0][3]}"])
            if len(track_rows) >= 2:
                staged_rows.append(["中线优先", f"{track_rows[1][1]}；{track_rows[1][3]}"])
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
            notes = [str(item).strip() for item in (payload.get("notes") or []) if str(item).strip()]
            if notes:
                lines.extend(["", "## 数据限制与说明", ""])
                for item in notes[:3]:
                    lines.append(f"- {item}")
            return "\n".join(lines).rstrip()
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
