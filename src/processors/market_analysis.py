"""Shared market-structure analysis for A-share briefing flows."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

import pandas as pd

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.market import fetch_asset_history, format_pct


FOCUS_INDEX_SPECS: tuple[dict[str, str], ...] = (
    {"name": "上证指数", "symbol": "sh000001", "label": "上证指数"},
    {"name": "沪深300", "symbol": "sh000300", "label": "中证核心(沪深300)"},
    {"name": "创业板指", "symbol": "sz399006", "label": "创业板指"},
)

DEFENSIVE_KEYWORDS = ("银行", "煤炭", "公用事业", "电力", "黄金", "红利", "高股息", "石油")
GROWTH_KEYWORDS = ("半导体", "人工智能", "AI", "算力", "机器人", "消费电子", "通信", "软件", "传媒", "新能源")


def _to_float(value: Any) -> float | None:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return None
    return float(number)


def _latest_valid(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.iloc[-1])


def _resample_ohlcv(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    normalized = normalize_ohlcv_frame(frame)
    indexed = normalized.set_index("date")
    resampled = (
        indexed.resample(rule)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "amount": "sum",
            }
        )
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    return normalize_ohlcv_frame(resampled)


def _ma_signal(frame: pd.DataFrame) -> Dict[str, Any]:
    normalized = normalize_ohlcv_frame(frame)
    close = normalized["close"].astype(float)
    price = float(close.iloc[-1])
    ma5 = _latest_valid(close.rolling(5).mean())
    ma20 = _latest_valid(close.rolling(20).mean())
    ma60 = _latest_valid(close.rolling(60).mean())
    ma120 = _latest_valid(close.rolling(120).mean())

    if ma5 is None or ma20 is None or ma60 is None:
        return {"label": "样本不足", "bias": "neutral", "detail": "历史长度不足以判断均线结构。"}

    if price > ma5 > ma20 > ma60:
        label = "多头排列"
        bias = "bullish"
    elif price < ma5 < ma20 < ma60:
        label = "空头排列"
        bias = "bearish"
    elif price >= ma20 and ma20 >= ma60:
        label = "偏强修复"
        bias = "bullish"
    elif price <= ma20 and ma20 <= ma60:
        label = "弱势下行"
        bias = "bearish"
    else:
        label = "均线缠绕"
        bias = "neutral"

    detail = f"MA20 {ma20:.1f} / MA60 {ma60:.1f}"
    if ma120 is not None:
        detail += f" / MA120 {ma120:.1f}"
    return {"label": label, "bias": bias, "detail": detail}


def _macd_signal(frame: pd.DataFrame) -> Dict[str, Any]:
    normalized = normalize_ohlcv_frame(frame)
    if len(normalized.index) < 35:
        return {"label": "样本不足", "bias": "neutral", "detail": "样本不足，无法稳定判断 MACD。"}

    series = TechnicalAnalyzer(normalized).indicator_series()
    dif_series = pd.to_numeric(series["macd_dif"], errors="coerce").dropna()
    dea_series = pd.to_numeric(series["macd_dea"], errors="coerce").dropna()
    hist_series = pd.to_numeric(series["macd_hist"], errors="coerce").dropna()
    if dif_series.empty or dea_series.empty or hist_series.empty:
        return {"label": "样本不足", "bias": "neutral", "detail": "样本不足，无法稳定判断 MACD。"}

    dif = float(dif_series.iloc[-1])
    dea = float(dea_series.iloc[-1])
    hist = float(hist_series.iloc[-1])
    prev_hist = float(hist_series.iloc[-2]) if len(hist_series.index) > 1 else hist

    if dif >= dea and dif >= 0 and dea >= 0:
        label = "水上多头"
        bias = "bullish"
    elif dif >= dea:
        label = "水下修复"
        bias = "bullish"
    elif dif < dea and dif >= 0 and dea >= 0:
        label = "水上回落"
        bias = "bearish"
    else:
        label = "水下走弱"
        bias = "bearish"

    if abs(hist) > abs(prev_hist) * 1.05:
        hist_state = "柱体放大"
    elif abs(hist) < abs(prev_hist) * 0.95:
        hist_state = "柱体收敛"
    else:
        hist_state = "柱体平稳"
    return {"label": label, "bias": bias, "detail": f"DIF {dif:.2f} / DEA {dea:.2f}，{hist_state}"}


def _volume_signal(frame: pd.DataFrame) -> Dict[str, Any]:
    normalized = normalize_ohlcv_frame(frame)
    signal = TechnicalAnalyzer(normalized).volume_analysis()
    ratio = _to_float(signal.get("vol_ratio_20")) or 1.0
    structure = str(signal.get("structure", "量价中性")).strip() or "量价中性"

    if structure in {"放量突破", "放量上攻"} or ratio >= 1.25:
        label = "放量活跃"
        bias = "bullish"
    elif structure in {"放量下跌", "跌破平台"}:
        label = "放量转弱"
        bias = "bearish"
    elif ratio <= 0.8:
        label = "缩量观望"
        bias = "neutral"
    else:
        label = "常态量能"
        bias = "neutral"
    return {"label": label, "bias": bias, "detail": f"{structure}，20日量比 {ratio:.2f}"}


def _index_summary(
    ma_payload: Dict[str, Any],
    weekly_macd: Dict[str, Any],
    monthly_macd: Dict[str, Any],
    volume_payload: Dict[str, Any],
) -> str:
    bullish = sum(
        1 for payload in (ma_payload, weekly_macd, monthly_macd) if str(payload.get("bias")) == "bullish"
    )
    bearish = sum(
        1 for payload in (ma_payload, weekly_macd, monthly_macd) if str(payload.get("bias")) == "bearish"
    )
    if bullish >= 3:
        conclusion = "多头共振"
    elif bearish >= 2:
        conclusion = "偏弱主导"
    elif str(weekly_macd.get("bias")) == "bullish":
        conclusion = "短线修复"
    else:
        conclusion = "等待确认"
    volume_label = str(volume_payload.get("label", "")).strip()
    if volume_label in {"放量活跃", "放量转弱"}:
        return f"{conclusion}，{volume_label}"
    return conclusion


def _turnover_text(value: float | None) -> str:
    if value is None:
        return "N/A"
    if value >= 10000:
        return f"{value / 10000:.2f}万亿"
    return f"{value:.0f}亿"


def _market_structure_amount_to_yi(value: float | None) -> float | None:
    if value is None:
        return None
    # Tushare daily_info is already close to 亿元, while sz_daily_info can return 元 or 万元.
    # A single market snapshot above 10,000,000,000 亿元 is impossible, so treat it as 元.
    if value >= 10_000_000_000:
        return value / 100_000_000
    # Values in the millions/billions are usually 万元 for this endpoint.
    if value >= 1_000_000:
        return value / 10000
    return value


def _best_market_structure_row(rows: Sequence[Mapping[str, Any]], *, preferred: Sequence[str] = ()) -> Dict[str, Any]:
    candidates = [dict(row or {}) for row in rows if isinstance(row, Mapping)]
    if not candidates:
        return {}

    def _rank(row: Mapping[str, Any]) -> tuple[float, float, float]:
        name = str(row.get("ts_name", "") or row.get("ts_code", "")).strip()
        score = 0.0
        if any(token and token in name for token in preferred):
            score += 10.0
        if any(token in name for token in ("A股", "股票", "市场", "主板", "创业板", "科创板")):
            score += 5.0
        amount = _market_structure_amount_to_yi(_to_float(row.get("amount"))) or 0.0
        count = _to_float(row.get("count")) or 0.0
        return (score, amount, count)

    return dict(max(candidates, key=_rank))


def _market_structure_signal(overview: Mapping[str, Any]) -> Dict[str, Any]:
    structure = dict(overview.get("market_structure") or {})
    daily_rows = [dict(row or {}) for row in list(structure.get("daily_info") or []) if isinstance(row, Mapping)]
    sz_rows = [dict(row or {}) for row in list(structure.get("sz_daily_info") or []) if isinstance(row, Mapping)]
    if not daily_rows and not sz_rows:
        return {
            "value": "N/A",
            "label": "结构缺失",
            "detail": "暂未拿到 daily_info / sz_daily_info 交易结构快照。",
            "line": "交易结构快照暂缺，当前先按宽度、量能和轮动判断市场结构。",
            "bias": "neutral",
        }

    daily_row = _best_market_structure_row(daily_rows, preferred=("A股", "股票", "市场"))
    sz_row = _best_market_structure_row(sz_rows, preferred=("A股", "股票", "创业板", "市场"))
    latest_date = str(structure.get("latest_date", "")).strip()
    is_fresh = bool(structure.get("is_fresh"))
    label = "结构快照可用" if is_fresh else "结构快照偏旧"

    segments: List[str] = []
    summary_bits: List[str] = []
    for row in (daily_row, sz_row):
        if not row:
            continue
        name = str(row.get("ts_name", "") or row.get("ts_code", "")).strip() or "市场结构"
        count = _to_float(row.get("count"))
        amount = _market_structure_amount_to_yi(_to_float(row.get("amount")))
        summary = name
        if count is not None:
            summary += f" {int(count)}家"
        if amount is not None:
            summary += f" / 成交{_turnover_text(amount)}"
        segments.append(summary)
        summary_bits.append(name)

    if not segments:
        segments.append("结构快照已接入")
    detail = "；".join(segments)
    if latest_date and not is_fresh:
        detail += f"（最新日期 {latest_date}）"
    line = "交易结构快照：" + "；".join(segments) + "。"
    if latest_date and not is_fresh:
        line += f" 最新日期停在 {latest_date}，当前只作为结构参考，不当成今天 fresh 盘面。"
    return {
        "value": " / ".join(summary_bits) if summary_bits else "结构快照",
        "label": label,
        "detail": detail,
        "line": line,
        "bias": "neutral",
    }


def _analyze_focus_index(
    spec: Mapping[str, str],
    config: Mapping[str, Any],
    domestic_snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    label = str(spec.get("label") or spec.get("name") or spec.get("symbol") or "指数").strip()
    history = normalize_ohlcv_frame(fetch_asset_history(str(spec.get("symbol", "")), "cn_index", dict(config)))
    ma_payload = _ma_signal(history)
    weekly_macd = _macd_signal(_resample_ohlcv(history, "W-FRI"))
    monthly_macd = _macd_signal(_resample_ohlcv(history, "ME"))
    volume_payload = _volume_signal(history)

    latest = _to_float(domestic_snapshot.get("latest"))
    if latest is None:
        latest = float(history["close"].iloc[-1])
    change_pct = _to_float(domestic_snapshot.get("change_pct"))
    if change_pct is None and len(history.index) > 1:
        prev_close = float(history["close"].iloc[-2])
        change_pct = float(latest / prev_close - 1) if prev_close else None

    return {
        "label": label,
        "latest": latest,
        "change_pct": change_pct,
        "ma": ma_payload,
        "weekly_macd": weekly_macd,
        "monthly_macd": monthly_macd,
        "volume": volume_payload,
        "summary": _index_summary(ma_payload, weekly_macd, monthly_macd, volume_payload),
    }


def _prev_limit_avg(pulse: Mapping[str, Any]) -> float | None:
    prev_zt = pulse.get("prev_zt_pool", pd.DataFrame())
    if isinstance(prev_zt, pd.DataFrame) and not prev_zt.empty and "涨跌幅" in prev_zt.columns:
        values = pd.to_numeric(prev_zt["涨跌幅"], errors="coerce").dropna()
        if not values.empty:
            return float(values.mean())
    return None


def _breadth_signal(overview: Mapping[str, Any], pulse: Mapping[str, Any]) -> Dict[str, Any]:
    breadth = dict(overview.get("breadth") or {})
    up_count = int(breadth.get("up_count") or 0)
    down_count = int(breadth.get("down_count") or 0)
    flat_count = int(breadth.get("flat_count") or 0)
    total = up_count + down_count + flat_count
    if total <= 0:
        return {
            "value": "N/A",
            "label": "宽度缺失",
            "detail": "暂未拿到全市场涨跌家数。",
            "line": "市场宽度暂缺，当前只能依赖指数和轮动判断强弱。",
            "bias": "neutral",
        }

    ratio = up_count / max(down_count, 1)
    advance_ratio = up_count / total
    prev_avg = _prev_limit_avg(pulse)
    zt_count = len(pulse.get("zt_pool", pd.DataFrame()).index) if pulse else 0
    dt_count = len(pulse.get("dt_pool", pd.DataFrame()).index) if pulse else 0

    if advance_ratio >= 0.6 and ratio >= 1.5:
        label = "普涨扩散"
        bias = "bullish"
    elif advance_ratio >= 0.52:
        label = "偏强扩散"
        bias = "bullish"
    elif advance_ratio <= 0.4 and ratio <= 0.7:
        label = "退潮偏弱"
        bias = "bearish"
    elif advance_ratio <= 0.48:
        label = "分化偏弱"
        bias = "bearish"
    else:
        label = "分歧中性"
        bias = "neutral"

    detail = f"涨跌比 {ratio:.2f}，涨停 {zt_count} / 跌停 {dt_count}"
    line = f"市场宽度 `{label}`：上涨 {up_count} 家、下跌 {down_count} 家，涨跌比 {ratio:.2f}。"
    if prev_avg is not None:
        line += f" 前一日涨停溢价 {prev_avg:+.2f}%。"
    return {
        "value": f"上涨 {up_count} / 下跌 {down_count}",
        "label": label,
        "detail": detail,
        "line": line,
        "bias": bias,
        "advance_ratio": advance_ratio,
    }


def _turnover_signal(overview: Mapping[str, Any], index_payloads: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    breadth = dict(overview.get("breadth") or {})
    turnover = _to_float(breadth.get("turnover"))
    active_count = sum(1 for payload in index_payloads if str(dict(payload.get("volume") or {}).get("label")) == "放量活跃")
    weak_count = sum(1 for payload in index_payloads if str(dict(payload.get("volume") or {}).get("label")) == "放量转弱")

    if turnover is None:
        return {
            "value": "N/A",
            "label": "量能缺失",
            "detail": "暂未拿到全市场成交额。",
            "line": "成交量能暂缺，当前先按指数结构与宽度判断。",
            "bias": "neutral",
        }

    if turnover >= 18000:
        base = "极活跃"
    elif turnover >= 13000:
        base = "活跃"
    elif turnover >= 9000:
        base = "常态"
    else:
        base = "偏冷"

    if base in {"极活跃", "活跃"} and active_count >= 2:
        label = "量能支撑"
        bias = "bullish"
    elif weak_count >= 1 and base in {"活跃", "极活跃"}:
        label = "放量分歧"
        bias = "bearish"
    elif base == "偏冷":
        label = "缩量博弈"
        bias = "bearish"
    else:
        label = base
        bias = "neutral"

    detail = f"核心指数放量 {active_count}/3，放量转弱 {weak_count}/3"
    line = f"全市场成交额 {_turnover_text(turnover)}，当前量能判断为 `{label}`。"
    return {"value": _turnover_text(turnover), "label": label, "detail": detail, "line": line, "bias": bias}


def _sentiment_signal(overview: Mapping[str, Any], pulse: Mapping[str, Any]) -> Dict[str, Any]:
    zt_count = len(pulse.get("zt_pool", pd.DataFrame()).index) if pulse else 0
    strong_count = len(pulse.get("strong_pool", pd.DataFrame()).index) if pulse else 0
    dt_count = len(pulse.get("dt_pool", pd.DataFrame()).index) if pulse else 0
    prev_avg = _prev_limit_avg(pulse)
    breadth = dict(overview.get("breadth") or {})
    up_count = int(breadth.get("up_count") or 0)
    down_count = int(breadth.get("down_count") or 0)
    flat_count = int(breadth.get("flat_count") or 0)
    total = up_count + down_count + flat_count
    advance_ratio = (up_count / total) if total > 0 else 0.5

    score = 50.0
    score += min(20.0, zt_count * 0.22)
    score += min(14.0, strong_count * 0.45)
    score -= min(24.0, dt_count * 1.2)
    score += (advance_ratio - 0.5) * 30.0
    if prev_avg is not None:
        score += max(-15.0, min(15.0, prev_avg * 3.0))
    score = max(0.0, min(100.0, score))
    score_int = int(round(score))

    if score_int >= 75:
        label = "情绪过热"
        bias = "bullish"
    elif score_int >= 60:
        label = "偏热"
        bias = "bullish"
    elif score_int <= 25:
        label = "情绪冰点"
        bias = "bearish"
    elif score_int <= 40:
        label = "偏冷"
        bias = "bearish"
    else:
        label = "分歧中性"
        bias = "neutral"

    detail = f"涨停 {zt_count} / 强势 {strong_count} / 跌停 {dt_count}"
    line = (
        f"情绪极端指标 `{label}`：当前 {score_int}/100，"
        f"涨停 {zt_count} 家、强势股 {strong_count} 家、跌停 {dt_count} 家。"
    )
    if prev_avg is not None:
        line += f" 昨日涨停溢价 {prev_avg:+.2f}% 作为接力代理。"
    line += " 这是盘口情绪代理，不是社媒或席位原始情绪 feed。"
    return {"value": f"{score_int}/100", "label": label, "detail": detail, "line": line, "bias": bias}


def _clean_rank_names(frame: pd.DataFrame, *, top: bool, limit: int) -> List[str]:
    if frame.empty or "名称" not in frame.columns or "涨跌幅" not in frame.columns:
        return []
    working = frame.copy()
    working["涨跌幅"] = pd.to_numeric(working["涨跌幅"], errors="coerce")
    working = working.dropna(subset=["涨跌幅"])
    if working.empty:
        return []
    ranked = working.sort_values("涨跌幅", ascending=not top).head(limit)
    items: List[str] = []
    for _, row in ranked.iterrows():
        items.append(f"{row['名称']}({row['涨跌幅']:+.2f}%)")
    return items


def _rotation_judgement(leaders: Sequence[str], laggards: Sequence[str]) -> str:
    leader_text = " ".join(leaders)
    laggard_text = " ".join(laggards)
    if any(keyword in leader_text for keyword in DEFENSIVE_KEYWORDS) and any(
        keyword in laggard_text for keyword in GROWTH_KEYWORDS
    ):
        return "防守占优，高低切明显"
    if any(keyword in leader_text for keyword in GROWTH_KEYWORDS) and any(
        keyword in laggard_text for keyword in DEFENSIVE_KEYWORDS
    ):
        return "成长回流，风险偏好修复"
    if leaders and laggards:
        return "轮动加快，板块分化仍大"
    if leaders:
        return "资金集中在领涨方向"
    return "暂未识别到稳定轮动主线"


def _top_categories(frame: pd.DataFrame, column: str, *, limit: int = 3) -> List[str]:
    if frame.empty or column not in frame.columns:
        return []
    series = frame[column].dropna().astype(str).str.strip()
    series = series[series != ""]
    if series.empty:
        return []
    counts = series.value_counts().head(limit)
    return [f"{name}({count})" for name, count in counts.items()]


def _rotation_payload(drivers: Mapping[str, Any], pulse: Mapping[str, Any]) -> Dict[str, Any]:
    industry_spot = drivers.get("industry_spot", pd.DataFrame()) if drivers else pd.DataFrame()
    concept_spot = drivers.get("concept_spot", pd.DataFrame()) if drivers else pd.DataFrame()
    industry_leaders = _clean_rank_names(industry_spot, top=True, limit=3)
    industry_laggards = _clean_rank_names(industry_spot, top=False, limit=2)
    concept_leaders = _clean_rank_names(concept_spot, top=True, limit=3)
    strong_categories = _top_categories(pulse.get("strong_pool", pd.DataFrame()), "所属行业", limit=3) if pulse else []
    zt_categories = _top_categories(pulse.get("zt_pool", pd.DataFrame()), "所属行业", limit=3) if pulse else []

    rows: List[List[str]] = []
    if industry_leaders or industry_laggards:
        rows.append(
            [
                "行业",
                "、".join(industry_leaders) if industry_leaders else "—",
                "、".join(industry_laggards) if industry_laggards else "—",
                _rotation_judgement(industry_leaders, industry_laggards),
            ]
        )
    if concept_leaders:
        rows.append(
            [
                "概念",
                "、".join(concept_leaders),
                "—",
                "概念端热度继续向领涨主题集中。",
            ]
        )
    if strong_categories or zt_categories:
        rows.append(
            [
                "涨停/强势股",
                "、".join(strong_categories or zt_categories),
                "—",
                "短线资金主要围绕强势行业接力。",
            ]
        )

    lines: List[str] = []
    if industry_leaders:
        lines.append("行业轮动靠前: " + "、".join(industry_leaders) + "。")
    if industry_laggards:
        lines.append("行业轮动靠后: " + "、".join(industry_laggards) + "。")
    if concept_leaders:
        lines.append("概念热度靠前: " + "、".join(concept_leaders) + "。")
    if strong_categories:
        lines.append("强势股主要集中在: " + "、".join(strong_categories) + "。")
    elif zt_categories:
        lines.append("涨停分布主要集中在: " + "、".join(zt_categories) + "。")
    if not lines:
        lines.append("当前轮动数据有限，先按指数结构和市场宽度判断主线。")

    return {"rows": rows, "lines": lines}


def _summary_lines(
    index_payloads: Sequence[Mapping[str, Any]],
    breadth_payload: Mapping[str, Any],
    turnover_payload: Mapping[str, Any],
    sentiment_payload: Mapping[str, Any],
    rotation_payload: Mapping[str, Any],
) -> List[str]:
    bullish_count = sum(1 for payload in index_payloads if "多头" in str(payload.get("summary", "")) or "修复" in str(payload.get("summary", "")))
    bearish_count = sum(1 for payload in index_payloads if "偏弱" in str(payload.get("summary", "")))
    lines: List[str] = []

    if bullish_count >= 2:
        lines.append("三大核心指数里至少有两条处在偏强或修复结构，指数层面不算全面转弱。")
    elif bearish_count >= 2:
        lines.append("三大核心指数里至少有两条仍偏弱，反弹更像结构性修复，不宜当成全面牛市启动。")
    else:
        lines.append("核心指数强弱并不统一，当前更像结构性行情而不是全市场同向共振。")

    lines.append(str(breadth_payload.get("line", "")))
    lines.append(str(turnover_payload.get("line", "")))
    lines.append(str(sentiment_payload.get("line", "")))

    rotation_lines = [str(item).strip() for item in rotation_payload.get("lines", []) if str(item).strip()]
    if rotation_lines:
        lines.append(rotation_lines[0])
    lines.append("这里将“中证”按 `沪深300` 作为核心宽基代理；更高弹性的中小盘轮动仍建议结合 `中证1000` 一起看。")
    return [line for line in lines if line]


def build_market_analysis(
    config: Mapping[str, Any],
    overview: Mapping[str, Any],
    pulse: Mapping[str, Any],
    drivers: Mapping[str, Any],
) -> Dict[str, Any]:
    domestic_by_name = {
        str(item.get("name", "")).strip(): dict(item)
        for item in (overview.get("domestic_indices", []) or [])
        if str(item.get("name", "")).strip()
    }

    index_payloads: List[Dict[str, Any]] = []
    index_rows: List[List[str]] = []
    index_lines: List[str] = []
    for spec in FOCUS_INDEX_SPECS:
        try:
            payload = _analyze_focus_index(spec, config, domestic_by_name.get(str(spec["name"]), {}))
            index_payloads.append(payload)
            index_rows.append(
                [
                    payload["label"],
                    f"{float(payload['latest']):.2f}" if payload.get("latest") is not None else "N/A",
                    format_pct(payload.get("change_pct")),
                    str(dict(payload.get("ma") or {}).get("label", "N/A")),
                    str(dict(payload.get("weekly_macd") or {}).get("label", "N/A")),
                    str(dict(payload.get("monthly_macd") or {}).get("label", "N/A")),
                    str(dict(payload.get("volume") or {}).get("label", "N/A")),
                    str(payload.get("summary", "等待确认")),
                ]
            )
            index_lines.append(
                f"{payload['label']}：{payload['summary']}。"
                f" 日线均线 `{payload['ma']['label']}`，周线 MACD `{payload['weekly_macd']['label']}`，"
                f"月线 MACD `{payload['monthly_macd']['label']}`，量能 `{payload['volume']['detail']}`。"
            )
        except Exception as exc:  # noqa: BLE001
            index_rows.append([str(spec["label"]), "N/A", "N/A", "数据缺失", "数据缺失", "数据缺失", "数据缺失", "需人工复核"])
            index_lines.append(f"{spec['label']}：指数历史暂不可用，当前未生成完整技术信号。{exc}")

    breadth_payload = _breadth_signal(overview, pulse)
    turnover_payload = _turnover_signal(overview, index_payloads)
    market_structure_payload = _market_structure_signal(overview)
    sentiment_payload = _sentiment_signal(overview, pulse)
    rotation_payload = _rotation_payload(drivers, pulse)

    market_signal_rows = [
        ["市场宽度", breadth_payload["value"], breadth_payload["label"], breadth_payload["detail"]],
        ["成交量能", turnover_payload["value"], turnover_payload["label"], turnover_payload["detail"]],
        ["交易结构", market_structure_payload["value"], market_structure_payload["label"], market_structure_payload["detail"]],
        ["情绪极端", sentiment_payload["value"], sentiment_payload["label"], sentiment_payload["detail"]],
    ]

    return {
        "index_rows": index_rows,
        "index_lines": index_lines,
        "market_signal_rows": market_signal_rows,
        "market_signal_lines": [
            str(breadth_payload.get("line", "")),
            str(turnover_payload.get("line", "")),
            str(market_structure_payload.get("line", "")),
            str(sentiment_payload.get("line", "")),
        ],
        "rotation_rows": list(rotation_payload.get("rows", [])),
        "rotation_lines": list(rotation_payload.get("lines", [])),
        "summary_lines": _summary_lines(index_payloads, breadth_payload, turnover_payload, sentiment_payload, rotation_payload),
    }
