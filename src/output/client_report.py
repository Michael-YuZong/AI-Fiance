"""Client-facing report renderers."""

from __future__ import annotations

from collections import Counter, defaultdict
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.output.opportunity_report import (
    _catalyst_factor_rows,
    _dimension_rows as _detail_dimension_rows,
    _factor_rows,
    _hard_check_inline,
    _hard_check_rows,
)
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


def _signal_confidence_lines(analysis: Mapping[str, Any]) -> List[str]:
    confidence = dict(analysis.get("signal_confidence") or {})
    if not confidence:
        return [
            "**历史相似样本：** 当前还没有输出这层统计。",
            "",
            "- 当前稿件未产出历史相似样本结论，先不要把它当成已有历史命中率验证的建议。",
        ]
    if not confidence.get("available"):
        return [
            "**历史相似样本：** 当前不给这层置信度。",
            "",
            f"- 原因：{confidence.get('reason', '样本或数据置信度不足。')}",
            "- 处理原则：宁可不报，也不拿低置信历史样本给当前建议背书。",
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


def _reference_price_text(asset_type: str, reference_price: Any) -> str:
    try:
        price = float(reference_price)
    except (TypeError, ValueError):
        price = 0.0
    if price > 0:
        return f"{price:.4f}"
    return "最新净值" if asset_type == "cn_fund" else "最新价"


def _portfolio_whatif_handoff(
    *,
    symbol: str,
    action: Mapping[str, Any],
    horizon: Mapping[str, Any],
    asset_type: str = "",
    reference_price: Any = None,
) -> Dict[str, str]:
    code = str(horizon.get("code", "")).strip()
    label = str(horizon.get("label", "观察期")).strip() or "观察期"
    direction = str(action.get("direction", "")).strip()
    trade_action = "sell" if any(token in direction for token in ("卖", "减仓", "止盈", "止损", "回收")) else "buy"
    price_text = _reference_price_text(asset_type, reference_price)
    command = f"portfolio whatif {trade_action} {symbol} {price_text} 计划金额"

    if code == "short_term":
        summary = f"把它当 `{label}` 的交易仓处理：先预演首笔金额落下去后，仓位、执行成本和止损纪律是否还成立。"
    elif code == "swing":
        summary = f"把它当 `{label}` 的波段仓处理：先看首笔落下去后，单票权重、行业暴露和后续第二笔空间还剩多少。"
    elif code == "position_trade":
        summary = f"把它当 `{label}` 的配置仓处理：落单前先看加仓后是否仍在组合风险预算和行业/地区上限内。"
    elif code == "long_term_allocation":
        summary = f"把它当 `{label}` 的底仓处理：先看长期目标权重和风险预算能否承受，而不是只盯一次下单的短期波动。"
    else:
        summary = f"当前更像 `{label}`，先别急着落单；如果你坚持试仓，至少先预演这笔单会不会把组合推过上限。"
    return {"summary": summary, "command": command}


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
    for limit in (1, 2, 999):
        for item in candidates:
            text = str(item).strip()
            if not text or text in picked:
                continue
            if used_counts[text] >= limit:
                continue
            picked.append(text)
            if len(picked) >= max_items:
                break
        if len(picked) >= max_items:
            break

    for item in picked:
        used_counts[item] += 1
    return picked


def _analysis_section_lines(
    analysis: Mapping[str, Any],
    bucket: str,
    *,
    used_positive_reasons: Counter[str] | None = None,
    used_caution_reasons: Counter[str] | None = None,
) -> List[str]:
    name = str(analysis.get("name", ""))
    symbol = str(analysis.get("symbol", ""))
    action = dict(analysis.get("action") or {})
    narrative = dict(analysis.get("narrative") or {})
    horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")))
    handoff = _portfolio_whatif_handoff(
        symbol=symbol,
        action=action,
        horizon=horizon,
        asset_type=str(analysis.get("asset_type", "")),
        reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
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
        lines.append(f"- 持有周期：{horizon['label']}。{horizon['style']}")
        if horizon.get("fit_reason"):
            lines.append(f"- 为什么按这个周期理解：{horizon['fit_reason']}")
        if horizon.get("misfit_reason"):
            lines.append(f"- 现在不适合的打法：{horizon['misfit_reason']}")
    lines.extend(
        [
            f"- 介入条件：{action.get('entry', '等待进一步确认')}",
            f"- 首次仓位：{action.get('position', '小仓位分批')}",
            f"- 加仓节奏：{action.get('scaling_plan', '确认后再考虑第二笔')}",
            f"- 止损参考：{action.get('stop', '重新跌破关键支撑就处理')}",
            f"- 组合落单前：{handoff['summary']}",
            f"- 预演命令：`{handoff['command']}`",
        ]
    )
    return lines


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


def _fund_profile_sections(analysis: Mapping[str, Any]) -> List[str]:
    fund_profile = dict(analysis.get("fund_profile") or {})
    if not fund_profile:
        return []
    overview = dict(fund_profile.get("overview") or {})
    style = dict(fund_profile.get("style") or {})
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

    style_rows = [
        ["风格标签", " / ".join(style.get("tags") or []) or "—"],
        ["仓位画像", style.get("positioning", "—")],
        ["选股方式", style.get("selection", "—")],
        ["风格一致性", style.get("consistency", "—")],
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


def _pick_horizon_profile(action: Mapping[str, Any], trade_state: str = "") -> Dict[str, str]:
    structured = dict(action.get("horizon") or {})
    if structured:
        label = str(structured.get("label", "")).strip().replace("(", "（").replace(")", "）")
        style = str(structured.get("style", "")).strip()
        fit_reason = str(structured.get("fit_reason", "")).strip()
        misfit_reason = str(structured.get("misfit_reason", "")).strip()
        if label:
            return {
                "code": str(structured.get("code", "")).strip(),
                "label": label,
                "style": style,
                "fit_reason": fit_reason,
                "misfit_reason": misfit_reason,
            }

    raw = str(action.get("timeframe", "")).strip()
    trade_state = str(trade_state).strip()
    direction = str(action.get("direction", "")).strip()
    label = raw.replace("(", "（").replace(")", "）") if raw else ""

    if "长线" in label:
        return {
            "code": "long_term_allocation",
            "label": label,
            "style": "更适合作为中长期底仓来跟踪，允许短线波动，但要持续复核主线、基本面和风险预算。",
            "fit_reason": "更依赖中长期逻辑，而不是一两天的节奏变化。",
            "misfit_reason": "不适合按纯短线追涨杀跌来理解。",
        }
    if "中线" in label:
        return {
            "code": "position_trade",
            "label": label,
            "style": "更像 1-3 个月的分批配置或波段跟踪，不按隔日涨跌去做快进快出。",
            "fit_reason": "更适合围绕一段完整主线分批拿，而不是只看日内波动。",
            "misfit_reason": "不适合直接当成超短节奏仓，也别默认长到长期不复核。",
        }
    if "短线" in label:
        return {
            "code": "short_term",
            "label": label,
            "style": "更看催化、趋势和执行节奏，适合盯右侧确认和止损，不适合当成长线底仓。",
            "fit_reason": "当前优势更多集中在催化和节奏，不在长周期基本面。",
            "misfit_reason": "不适合当成长线配置仓。",
        }
    if "波段" in label:
        return {
            "code": "swing",
            "label": label,
            "style": "更适合按几周级别的波段节奏去跟踪，等确认和回踩，不靠单日冲动去追。",
            "fit_reason": "趋势和轮动有基础，但还更依赖未来几周节奏。",
            "misfit_reason": "不适合当长期底仓，也不适合只按隔夜消息去赌超短。",
        }

    fallback_blob = " ".join(part for part in (raw, trade_state, direction) if part)
    if any(token in fallback_blob for token in ("等待", "观察", "回避", "暂不出手")):
        return {
            "code": "watch",
            "label": "观察期",
            "style": "现在先看窗口和确认信号，不建议急着把它定义成短线执行仓或长线配置仓。",
            "fit_reason": "当前信号还没共振到足以支撑正式动作，先观察更稳妥。",
            "misfit_reason": "不适合直接按明确的长线或短线打法去执行。",
        }
    return {}


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
            if best_bucket == "正式推荐":
                lines.append(f"- {market_name}优先看：`{best_label}`")
            else:
                lines.append(f"- {market_name}今天暂不做正式推荐，优先观察：`{best_label}`")

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
            used_positive_reasons: Counter[str] = Counter()
            used_caution_reasons: Counter[str] = Counter()
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
                        *_analysis_section_lines(
                            item,
                            bucket,
                            used_positive_reasons=used_positive_reasons,
                            used_caution_reasons=used_caution_reasons,
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
                if evidence_lines:
                    lines.extend(["", "**催化证据来源：**", ""])
                    lines.extend(evidence_lines)

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
                        used_positive_reasons,
                        max_items=2,
                    )
                    cautions = _take_diverse_reason_lines(
                        _merge_reason_lines(
                            _bottom_dimension_reasons(item, top_n=2),
                            list((item.get("narrative") or {}).get("cautions") or []),
                            max_items=4,
                        ),
                        used_caution_reasons,
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
        used_positive_reasons: Counter[str] = Counter()
        used_caution_reasons: Counter[str] = Counter()

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
            if best_bucket == "正式推荐":
                lines.append(f"- {market_name}首选：`{best_label}`")
            else:
                lines.append(f"- {market_name}暂不正式推荐，优先观察：`{best_label}`")
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
                            used_positive_reasons=used_positive_reasons,
                            used_caution_reasons=used_caution_reasons,
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
                            used_positive_reasons=used_positive_reasons,
                            used_caution_reasons=used_caution_reasons,
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

    def render_scan(self, analysis: Dict[str, Any]) -> str:
        generated_at = str(analysis.get("generated_at", ""))[:10]
        name = str(analysis.get("name", ""))
        symbol = str(analysis.get("symbol", ""))
        bucket = _recommendation_bucket(analysis)
        narrative = dict(analysis.get("narrative") or {})
        action = dict(analysis.get("action") or {})
        horizon = _pick_horizon_profile(action, str(dict(narrative.get("judgment") or {}).get("state", "")))
        handoff = _portfolio_whatif_handoff(
            symbol=symbol,
            action=action,
            horizon=horizon,
            asset_type=str(analysis.get("asset_type", "")),
            reference_price=dict(analysis.get("metrics") or {}).get("last_close"),
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
                    ["周期理由", horizon.get("fit_reason", horizon.get("style", "先按当前动作、仓位和止损框架理解。"))],
                    ["现在不适合", horizon.get("misfit_reason", "不要把当前动作自动理解成另一种更长或更短的打法。")],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', f'portfolio whatif buy {symbol} 最新价 计划金额')}`"],
                    ["介入条件", action.get("entry", "等待进一步确认")],
                    ["首次仓位", action.get("position", "小仓位分批")],
                    ["加仓节奏", action.get("scaling_plan", "确认后再考虑第二笔")],
                    ["止损参考", action.get("stop", "重新跌破关键支撑就处理")],
                    ["目标参考", action.get("target", "先看前高压力位")],
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
        handoff = _portfolio_whatif_handoff(
            symbol=str(winner.get("symbol", "")),
            action=dict(winner.get("action") or {}),
            horizon=horizon,
            asset_type=str(winner.get("asset_type", "")),
            reference_price=winner.get("reference_price"),
        )
        observe_only = bool(selection_context.get("delivery_observe_only"))
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        lead_line = (
            f"今天先给一个观察优先的场外基金对象：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
            if observe_only
            else f"今天如果只推荐一只场外基金，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
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
        lines.extend(["", *_taxonomy_section(winner)])
        if winner.get("score_changes"):
            lines.extend(["", "## 跟今天首个快照版相比", ""])
            for item in winner.get("score_changes", [])[:4]:
                lines.append(
                    f"- `{item.get('label', '维度')}` 从 `{item.get('previous', '—')}` 变到 `{item.get('current', '—')}`：{item.get('reason', '')}"
                )
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["适用打法", horizon.get("style", "先按当前动作、仓位和止损框架理解，不把它默认当成长线配置。")],
                    ["为什么按这个周期看", horizon.get("fit_reason", "当前更适合按已有动作和仓位框架理解。")],
                    ["现在不适合", horizon.get("misfit_reason", "不要把它自动理解成另一种更长或更短的打法。")],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', 'portfolio whatif buy 标的 最新净值 计划金额')}`"],
                    ["介入条件", winner.get("action", {}).get("entry", "等回撤再看")],
                    ["首次仓位", winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2")],
                    ["加仓节奏", winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔")],
                    ["止损参考", winner.get("action", {}).get("stop", "重新跌破关键支撑就处理")],
                ],
            )
        )
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
        handoff = _portfolio_whatif_handoff(
            symbol=str(winner.get("symbol", "")),
            action=dict(winner.get("action") or {}),
            horizon=horizon,
            asset_type=str(winner.get("asset_type", "")),
            reference_price=winner.get("reference_price"),
        )
        observe_only = bool(selection_context.get("delivery_observe_only"))
        why_heading = "## 为什么先看它" if observe_only else "## 为什么推荐它"
        lead_line = (
            f"今天先给一个观察优先的 ETF 对象：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
            if observe_only
            else f"今天如果只推荐一只 ETF，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**"
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
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                    ["持有周期", horizon.get("label", "未单独标注")],
                    ["适用打法", horizon.get("style", "先按当前动作、仓位和止损框架理解，不把它默认当成长线配置。")],
                    ["为什么按这个周期看", horizon.get("fit_reason", "当前更适合按已有动作和仓位框架理解。")],
                    ["现在不适合", horizon.get("misfit_reason", "不要把它自动理解成另一种更长或更短的打法。")],
                    ["组合落单前", handoff.get("summary", "先跑组合预演，再决定真实金额。")],
                    ["预演命令", f"`{handoff.get('command', 'portfolio whatif buy 标的 最新价 计划金额')}`"],
                    ["介入条件", winner.get("action", {}).get("entry", "等回撤再看")],
                    ["首次仓位", winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2")],
                    ["加仓节奏", winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔")],
                    ["止损参考", winner.get("action", {}).get("stop", "重新跌破关键支撑就处理")],
                    ["目标参考", winner.get("action", {}).get("target", "先看前高压力位")],
                ],
            )
        )
        fund_sections = winner.get("fund_sections") or []
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
