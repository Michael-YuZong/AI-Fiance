"""Client-facing report renderers."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.output.opportunity_report import (
    _catalyst_factor_rows,
    _dimension_rows as _detail_dimension_rows,
    _factor_rows,
    _hard_check_inline,
    _hard_check_rows,
)

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


def _dimension_reason_text(dimension: Mapping[str, Any], *, positive: bool) -> str:
    factors = list(dimension.get("factors") or [])
    candidates: List[Tuple[float, str]] = []
    for factor in factors:
        signal = str(factor.get("signal", "")).strip()
        if not signal:
            continue
        if signal in GENERIC_FACTOR_SIGNALS:
            continue
        if signal.startswith("个股相关头条 0 条") or signal.startswith("覆盖源 0 个"):
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
            f"- 介入条件：{action.get('entry', '等待进一步确认')}",
            f"- 首次仓位：{action.get('position', '小仓位分批')}",
            f"- 加仓节奏：{action.get('scaling_plan', '确认后再考虑第二笔')}",
            f"- 止损参考：{action.get('stop', '重新跌破关键支撑就处理')}",
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
        ["成立日期", overview.get("成立日期/规模", "—")],
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


def _bucket_summary_text(bucket: str, analysis: Mapping[str, Any]) -> str:
    if bucket == "正式推荐":
        return "逻辑和执行条件都还在，可以小仓分批，但不适合重仓追高。"
    if bucket == "看好但暂不推荐":
        return "方向没坏，但今天更像观察和等待确认，不适合直接激进出手。"
    return "当前主要价值在观察，不在立即执行。"


class ClientReportRenderer:
    """Render concise client-facing reports from structured payloads."""

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

    def render_briefing(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        headline_lines = list(payload.get("headline_lines") or [])
        action_lines = list(payload.get("action_lines") or [])
        theme_rows = list(payload.get("theme_tracking_rows") or [])
        verification_rows = list(payload.get("verification_rows") or [])
        macro_asset_rows = list(payload.get("macro_asset_rows") or [])
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
        lines.extend(["", "## 今天怎么做", ""])
        for item in action_lines[:4]:
            lines.append(f"- {item}")
        lines.extend(["", "## 今日关键数据", ""])
        key_rows = macro_asset_rows[:5]
        if key_rows:
            lines.extend(_table(["资产", "最新价", "1日", "5日", "20日", "状态", "异常"], key_rows))
        else:
            lines.append("暂无关键资产快照。")
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
        alerts = list(payload.get("alerts") or [])
        lines.extend(["", "## 今天最重要的风险提醒", ""])
        for item in alerts[:3]:
            lines.append(f"- {item}")
        return "\n".join(lines).rstrip()

    def render_fund_pick(self, payload: Dict[str, Any]) -> str:
        generated_at = str(payload.get("generated_at", ""))[:10]
        winner = dict(payload.get("winner") or {})
        alternatives = list(payload.get("alternatives") or [])
        lines = [
            f"# 今日场外基金推荐 | {generated_at}",
            "",
            "## 今日结论",
            "",
            f"今天如果只推荐一只场外基金，我给：**`{winner.get('name', '')} ({winner.get('symbol', '')})`**",
            "",
            f"这不是激进进攻型推荐，而是：**`{winner.get('trade_state', '持有优于追高')}`**",
            "",
            "## 为什么推荐它",
            "",
        ]
        for item in winner.get("positives", [])[:3]:
            lines.append(f"- {item}")
        lines.extend(["", "## 这只基金为什么是这个分", ""])
        lines.extend(
            _table(
                ["维度", "分数", "为什么是这个分"],
                winner.get("dimension_rows", []),
            )
        )
        lines.extend(["", "## 怎么做", ""])
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", winner.get("action", {}).get("direction", "观察为主")],
                    ["介入条件", winner.get("action", {}).get("entry", "等回撤再看")],
                    ["首次仓位", winner.get("action", {}).get("position", "计划仓位的 1/3 - 1/2")],
                    ["加仓节奏", winner.get("action", {}).get("scaling_plan", "确认后再考虑第二笔")],
                    ["止损参考", winner.get("action", {}).get("stop", "重新跌破关键支撑就处理")],
                ],
            )
        )
        if alternatives:
            lines.extend(["", "## 为什么不是另外几只", ""])
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
        lines.extend(
            [
                "## 仓位管理",
                "",
            ]
        )
        for item in winner.get("positioning_lines", []):
            lines.append(f"- {item}")
        return "\n".join(lines).rstrip()
