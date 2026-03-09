"""Unified opportunity discovery and analysis engine."""

from __future__ import annotations

import io
import math
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from src.collectors import (
    ChinaMarketCollector,
    EventsCollector,
    MarketDriversCollector,
    MarketMonitorCollector,
    MarketPulseCollector,
    NewsCollector,
)
from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.regime import RegimeDetector
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import detect_asset_type
from src.utils.data import load_watchlist
from src.utils.market import compute_history_metrics, fetch_asset_history, format_pct, get_asset_context


SECTOR_RULES = [
    (("电网", "电力", "储能", "逆变器", "特高压"), "电网", ["AI算力", "电力需求", "电网设备", "铜铝"]),
    (("黄金", "贵金属"), "黄金", ["黄金", "通胀预期"]),
    (("半导体", "芯片", "通信", "算力", "人工智能", "AI", "软件", "消费电子"), "科技", ["AI算力", "半导体", "成长股估值修复"]),
    (("油", "煤", "能源"), "能源", ["原油", "通胀预期", "能源安全"]),
    (("银行", "红利", "高股息"), "高股息", ["高股息", "防守"]),
    (("医药", "医疗"), "医药", ["医药", "老龄化"]),
    (("消费", "酒", "食品"), "消费", ["内需", "消费修复"]),
    (("军工", "国防"), "军工", ["军工", "地缘风险"]),
    (("有色", "铜", "铝"), "有色", ["铜铝", "顺周期"]),
]

DEFAULT_CHAIN_NODES = ["宏观主线", "行业轮动"]
BENCHMARKS = {
    "cn_etf": ("510300", "cn_etf", "沪深300ETF"),
    "us": ("SPY", "us", "标普500"),
    "hk": ("2800.HK", "hk", "恒生指数ETF"),
    "hk_index": ("2800.HK", "hk", "恒生指数ETF"),
}

MONTHLY_SEASONAL_WINDOWS = {
    "消费": {9, 10, 11},
    "医药": {10, 11, 12},
    "高股息": {4, 5, 6},
    "电网": {3, 4, 5},
    "科技": {6, 7, 8},
    "能源": {9, 10, 11},
    "军工": {7, 8, 9},
    "有色": {2, 3, 4},
}

SENSITIVITY_MAP = {
    "电网": {"rate": -1, "usd": 0, "oil": 1, "cny": 1},
    "科技": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "黄金": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
    "能源": {"rate": 0, "usd": 1, "oil": 1, "cny": -1},
    "高股息": {"rate": 1, "usd": 0, "oil": 0, "cny": 0},
    "医药": {"rate": -1, "usd": 0, "oil": -1, "cny": 1},
    "消费": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "军工": {"rate": 0, "usd": 1, "oil": 1, "cny": -1},
    "有色": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
}


@dataclass
class PoolItem:
    symbol: str
    name: str
    asset_type: str
    region: str
    sector: str
    chain_nodes: List[str]
    source: str
    turnover: float = 0.0
    in_watchlist: bool = False


def _normalize_sector(name: str, fallback: str = "综合") -> tuple[str, List[str]]:
    lowered = name.lower()
    for keywords, sector, chain_nodes in SECTOR_RULES:
        if any(keyword.lower() in lowered for keyword in keywords):
            return sector, list(chain_nodes)
    return fallback, list(DEFAULT_CHAIN_NODES)


def _merge_metadata(symbol: str, asset_type: str, metadata: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    context = get_asset_context(symbol, asset_type, {})
    merged = dict(context.metadata)
    if metadata:
        merged.update(dict(metadata))
    merged.setdefault("symbol", symbol)
    merged.setdefault("asset_type", asset_type)
    merged.setdefault("name", context.name or symbol)
    sector, chain_nodes = _normalize_sector(str(merged.get("name", symbol)), str(merged.get("sector", "综合")))
    merged.setdefault("sector", sector)
    merged.setdefault("chain_nodes", chain_nodes)
    merged.setdefault("region", {"cn_etf": "CN", "hk": "HK", "hk_index": "HK", "us": "US", "futures": "CN"}.get(asset_type, "CN"))
    return merged


def _safe_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _dimension_summary(score: Optional[int], positive: str, neutral: str, negative: str, missing: str) -> str:
    if score is None:
        return missing
    if score >= 70:
        return positive
    if score >= 40:
        return neutral
    return negative


def _normalize_dimension(raw_score: int, available_max: int, target_max: int) -> Optional[int]:
    if available_max <= 0:
        return None
    normalized = int(round(raw_score / available_max * target_max))
    return max(0, min(target_max, normalized))


def _top_positive_signals(factors: Sequence[Dict[str, str]], limit: int = 3) -> str:
    positives = [item["signal"] for item in factors if item.get("awarded", 0) > 0]
    return " · ".join(positives[:limit]) if positives else "当前没有明确亮点"


def _factor_row(name: str, signal: str, awarded: Optional[int], maximum: int, detail: str) -> Dict[str, Any]:
    return {
        "name": name,
        "signal": signal,
        "awarded": awarded if awarded is not None else 0,
        "max": maximum,
        "detail": detail,
        "display_score": "缺失" if awarded is None else f"{awarded}/{maximum}",
    }


def _history_returns(frame: pd.DataFrame) -> pd.Series:
    normalized = normalize_ohlcv_frame(frame)
    return normalized["close"].astype(float).pct_change().dropna()


def _safe_history(symbol: str, asset_type: str, config: Mapping[str, Any], period: str = "3y") -> Optional[pd.DataFrame]:
    try:
        return normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config), period=period))
    except Exception:
        return None


def _monitor_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    return {str(item.get("name", "")): item for item in rows}


def _today_theme(news_report: Mapping[str, Any], monitor_rows: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    items = list(news_report.get("items", []) or [])
    counter: Counter[str] = Counter(str(item.get("category", "")).lower() for item in items if item.get("category"))
    monitor = _monitor_map(monitor_rows)
    brent_5d = float(monitor.get("布伦特原油", {}).get("return_5d", 0.0))
    vix = float(monitor.get("VIX波动率", {}).get("latest", 0.0))
    if counter["energy"] + counter["geopolitics"] >= 2 and (brent_5d >= 0.12 or vix >= 25):
        return {"code": "energy_shock", "label": "能源冲击 + 地缘风险"}
    if counter["fed"] >= 1:
        return {"code": "rate_growth", "label": "利率驱动成长修复"}
    if counter["ai"] + counter["semiconductor"] >= 2:
        return {"code": "ai_semis", "label": "AI / 半导体催化"}
    if counter["china_macro"] + counter["china_macro_domestic"] >= 1:
        return {"code": "china_policy", "label": "中国政策 / 内需确定性"}
    return {"code": "macro_background", "label": "背景宏观主导"}


def build_market_context(
    config: Mapping[str, Any],
    preferred_sources: Optional[Sequence[str]] = None,
    relevant_asset_types: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    china_macro: Dict[str, Any] = {}
    global_proxy: Dict[str, Any] = {}
    monitor_rows: List[Dict[str, Any]] = []
    regime: Dict[str, Any] = {"current_regime": "recovery", "preferred_assets": ["成长股", "顺周期", "港股科技", "铜"]}
    news_report: Dict[str, Any] = {"mode": "proxy", "items": [], "lines": [], "note": ""}
    events: List[Dict[str, Any]] = []
    drivers: Dict[str, Any] = {}
    pulse: Dict[str, Any] = {}
    notes: List[str] = []
    watchlist = load_watchlist()
    try:
        china_macro = load_china_macro_snapshot(dict(config))
    except Exception as exc:
        notes.append(f"中国宏观数据缺失: {exc}")
    try:
        with redirect_stderr(io.StringIO()):
            global_proxy = load_global_proxy_snapshot()
    except Exception as exc:
        notes.append(f"全球代理数据缺失: {exc}")
    try:
        monitor_rows = MarketMonitorCollector(config).collect()
    except Exception as exc:
        notes.append(f"宏观监控数据缺失: {exc}")
    try:
        regime_inputs = derive_regime_inputs(china_macro, global_proxy, monitor_rows)
        regime = RegimeDetector(regime_inputs).detect_regime()
    except Exception as exc:
        notes.append(f"regime 判断失败: {exc}")
    try:
        news_report = NewsCollector(config).collect(
            snapshots=watchlist,
            china_macro=china_macro,
            global_proxy=global_proxy,
            preferred_sources=preferred_sources or (),
            limit=8,
        )
    except Exception as exc:
        notes.append(f"新闻源缺失: {exc}")
    try:
        events = EventsCollector(config).collect(mode="daily")
    except Exception as exc:
        notes.append(f"事件日历缺失: {exc}")
    try:
        drivers = MarketDriversCollector(config).collect()
    except Exception as exc:
        notes.append(f"板块驱动数据缺失: {exc}")
    try:
        pulse = MarketPulseCollector(config).collect()
    except Exception as exc:
        notes.append(f"盘面情绪数据缺失: {exc}")

    day_theme = _today_theme(news_report, monitor_rows)
    selected_asset_types = {str(item) for item in (relevant_asset_types or []) if str(item)}
    watchlist_returns: Dict[str, pd.Series] = {}
    for item in watchlist:
        item_asset_type = str(item.get("asset_type", "cn_etf"))
        if selected_asset_types and item_asset_type not in selected_asset_types:
            continue
        try:
            watchlist_returns[item["symbol"]] = _history_returns(fetch_asset_history(item["symbol"], item["asset_type"], dict(config)))
        except Exception:
            continue
    benchmark_returns: Dict[str, pd.Series] = {}
    for asset_type, (symbol, bench_asset_type, _name) in BENCHMARKS.items():
        if selected_asset_types and asset_type not in selected_asset_types:
            continue
        history = _safe_history(symbol, bench_asset_type, config)
        if history is not None:
            benchmark_returns[asset_type] = history["close"].pct_change().dropna()

    return {
        "china_macro": china_macro,
        "global_proxy": global_proxy,
        "monitor_rows": monitor_rows,
        "regime": regime,
        "day_theme": day_theme,
        "news_report": news_report,
        "events": events,
        "drivers": drivers,
        "pulse": pulse,
        "notes": notes,
        "watchlist": watchlist,
        "watchlist_returns": watchlist_returns,
        "benchmark_returns": benchmark_returns,
    }


def _find_related_news(items: Sequence[Mapping[str, Any]], metadata: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    keys = [str(metadata.get("name", "")), str(metadata.get("sector", "")), *[str(item) for item in metadata.get("chain_nodes", [])]]
    keys = [key.lower() for key in keys if key and key != "综合"]
    related: List[Mapping[str, Any]] = []
    for item in items:
        text = " ".join(
            [
                str(item.get("title", "")),
                str(item.get("category", "")),
                str(item.get("source", "")),
            ]
        ).lower()
        if any(key in text for key in keys):
            related.append(item)
    return related


def _regime_assets(regime: Mapping[str, Any]) -> List[str]:
    return [str(item) for item in regime.get("preferred_assets", [])]


def _theme_alignment(metadata: Mapping[str, Any], day_theme: Mapping[str, str]) -> bool:
    sector = str(metadata.get("sector", ""))
    label = str(day_theme.get("label", ""))
    if "能源" in label or "地缘" in label:
        return sector in {"能源", "电网", "黄金", "高股息"}
    if "利率" in label:
        return sector in {"科技", "消费"}
    if "政策" in label:
        return sector in {"电网", "高股息", "消费"}
    if "AI" in label or "半导体" in label:
        return sector in {"科技"}
    return False


def _current_factor_state(context: Mapping[str, Any]) -> Dict[str, int]:
    monitor = _monitor_map(context.get("monitor_rows", []))
    oil = 1 if float(monitor.get("布伦特原油", {}).get("return_5d", 0.0)) > 0.05 else -1
    rate = -1 if float(monitor.get("美国10Y收益率", {}).get("return_5d", 0.0)) < 0 else 1
    usd = -1 if float(monitor.get("美元指数", {}).get("return_20d", 0.0)) < 0 else 1
    cny = 1 if float(monitor.get("USDCNY", {}).get("return_20d", 0.0)) < 0 else -1
    return {"rate": rate, "usd": usd, "oil": oil, "cny": cny}


def _hard_checks(
    metadata: Mapping[str, Any],
    history: pd.DataFrame,
    metrics: Mapping[str, float],
    technical: Mapping[str, Any],
    context: Mapping[str, Any],
    macro_score: Optional[int],
    correlation_pair: Optional[tuple[str, float]],
) -> tuple[List[Dict[str, str]], List[str], List[str]]:
    checks: List[Dict[str, str]] = []
    exclusion_reasons: List[str] = []
    warnings: List[str] = []
    opportunity_cfg = dict(context.get("config", {})).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("min_turnover", 50_000_000))
    min_history_days = int(opportunity_cfg.get("min_listing_days", 60))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))

    liquidity_ok = float(metrics.get("avg_turnover_20d", 0.0)) >= min_turnover
    checks.append({"name": "流动性", "status": "✅" if liquidity_ok else "❌", "detail": f"日均成交 {metrics.get('avg_turnover_20d', 0.0) / 1e8:.2f} 亿"})
    if not liquidity_ok:
        exclusion_reasons.append("日均成交额低于 5000 万")

    listed_ok = len(history) >= min_history_days
    checks.append({"name": "上市时长", "status": "✅" if listed_ok else "❌", "detail": f"有效历史样本 {len(history)} 个交易日"})
    if not listed_ok:
        exclusion_reasons.append("上市不满 60 个交易日")

    checks.append({"name": "基本面底线", "status": "ℹ️", "detail": "当前以 ETF / 行业代理为主，利润同比底线暂未接入原始财报数据"})
    if price_percentile > 0.90:
        checks.append({"name": "估值极端", "status": "⚠️", "detail": f"价格位置代理分位 {price_percentile:.0%}，接近极端高位"})
        exclusion_reasons.append("价格位置代理已处于极端高位")
        warnings.append("⚠️ 价格位置已在高位区，追高性价比明显下降")
    else:
        checks.append({"name": "估值极端", "status": "✅", "detail": f"价格位置代理分位 {price_percentile:.0%}"})

    checks.append({"name": "解禁压力", "status": "ℹ️", "detail": "解禁日历尚未接入，当前不纳入硬性排除"})

    return_5d = float(metrics.get("return_5d", 0.0))
    close_returns = history["close"].pct_change().dropna()
    limit_down_like = bool(len(close_returns) >= 2 and (close_returns.tail(2) <= -0.095).all())
    trend_ok = return_5d > -0.20 and not limit_down_like
    checks.append({"name": "趋势崩坏", "status": "✅" if trend_ok else "❌", "detail": f"近 5 日 {format_pct(return_5d)}"})
    if not trend_ok:
        exclusion_reasons.append("近 5 日跌幅过大或出现连续跌停式崩坏")

    if correlation_pair:
        peer, corr = correlation_pair
        diversified = abs(corr) <= 0.85
        checks.append({"name": "相关性", "status": "✅" if diversified else "❌", "detail": f"与 {peer} 相关性 {corr:.2f}"})
        if not diversified:
            exclusion_reasons.append(f"与 watchlist 中 {peer} 相关性过高")
    else:
        checks.append({"name": "相关性", "status": "ℹ️", "detail": "相关性代理暂缺，未用于排除"})

    macro_ok = macro_score is None or macro_score > 0
    checks.append({"name": "宏观顺逆风", "status": "✅" if macro_ok else "⚠️", "detail": "按宏观敏感度维度做顺逆风修正"})
    if macro_score == 0:
        exclusion_reasons.append("宏观敏感度完全逆风")
        warnings.append("⚠️ 当前宏观敏感度完全逆风，哪怕其他维度不差也不宜给高评级")

    if float(technical.get("rsi", {}).get("RSI", 0.0)) > 70:
        warnings.append("⚠️ 已进入超买区，追高性价比下降")
    return checks, exclusion_reasons, warnings


def _support_signals(history: pd.DataFrame, technical: Mapping[str, Any]) -> tuple[int, str]:
    close = history["close"].astype(float)
    price = float(close.iloc[-1])
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", price))
    fib = technical.get("fibonacci", {})
    fib_levels = fib.get("levels", {})
    hits: List[str] = []
    if ma60 > 0 and abs(price / ma60 - 1) <= 0.03:
        hits.append("MA60")
    for level_name in ("0.382", "0.500", "0.618"):
        level = float(fib_levels.get(level_name, 0.0))
        if level > 0 and abs(price / level - 1) <= 0.03:
            hits.append(f"斐波那契 {level_name}")
    recent_low = float(close.tail(20).min())
    if recent_low > 0 and abs(price / recent_low - 1) <= 0.05:
        hits.append("前低")
    score = 20 if len(hits) >= 2 else 15 if hits else 0
    detail = " / ".join(hits) if hits else "当前价格未明显贴近 MA60、前低或关键斐波那契支撑"
    return score, detail


def _technical_dimension(history: pd.DataFrame, technical: Mapping[str, Any]) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    macd = technical.get("macd", {})
    dif = float(macd.get("DIF", 0.0))
    dea = float(macd.get("DEA", 0.0))
    if dif > dea:
        awarded = 20 if dif > 0 else 10
        raw += awarded
        available += 20
        signal = "MACD 零轴上方金叉" if dif > 0 else "MACD 零轴下方金叉"
        factors.append(_factor_row("MACD 金叉", signal, awarded, 20, f"DIF {dif:.3f} / DEA {dea:.3f}"))
    else:
        available += 20
        factors.append(_factor_row("MACD 金叉", "MACD 未金叉", 0, 20, f"DIF {dif:.3f} / DEA {dea:.3f}"))

    adx = float(technical.get("dmi", {}).get("ADX", 0.0))
    adx_award = 20 if adx > 35 else 15 if adx > 25 else 0
    raw += adx_award
    available += 20
    factors.append(_factor_row("ADX", f"ADX {adx:.1f}", adx_award, 20, "趋势强度越高，越接近单边趋势"))

    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    if 30 <= rsi <= 50:
        rsi_award = 15
    elif rsi < 30:
        rsi_award = 10
    elif rsi <= 65:
        rsi_award = 8
    else:
        rsi_award = 0
    raw += rsi_award
    available += 15
    factors.append(_factor_row("RSI 位置", f"RSI {rsi:.1f}", rsi_award, 15, "30-50 更像回调未崩的理想区间"))

    support_award, support_detail = _support_signals(history, technical)
    raw += support_award
    available += 20
    factors.append(_factor_row("支撑位", support_detail, support_award, 20, "优先看前低 / MA60 / 斐波那契 0.382-0.618"))

    patterns = list(technical.get("candlestick", []) or [])
    bullish_patterns = [item for item in patterns if item in {"hammer", "inverted_hammer", "marubozu"}]
    candle_award = 10 if bullish_patterns else 0
    raw += candle_award
    available += 10
    factors.append(_factor_row("K线形态", " / ".join(bullish_patterns) if bullish_patterns else "无明显看涨形态", candle_award, 10, "当前只识别最近一根 K 线"))

    vol_ratio = float(technical.get("volume", {}).get("vol_ratio", 1.0))
    latest_return = float(history["close"].pct_change().iloc[-1]) if len(history) > 1 else 0.0
    volume_award = 10 if vol_ratio < 0.7 and latest_return <= 0 else 5 if 0.7 <= vol_ratio <= 1.2 else 0
    raw += volume_award
    available += 10
    factors.append(_factor_row("量能", f"量比 {vol_ratio:.2f}", volume_award, 10, "缩量回调更像抛压衰减，放量上冲更适合确认趋势"))

    ma = technical.get("ma_system", {})
    ma_values = ma.get("mas", {})
    ma5 = float(ma_values.get("MA5", 0.0))
    ma20 = float(ma_values.get("MA20", 0.0))
    ma60 = float(ma_values.get("MA60", 0.0))
    ma_award = 10 if ma5 > ma20 > ma60 else 0
    raw += ma_award
    available += 10
    factors.append(_factor_row("均线", f"MA5 {ma5:.3f} / MA20 {ma20:.3f} / MA60 {ma60:.3f}", ma_award, 10, "多头排列代表中期趋势向上"))

    boll_signal = str(technical.get("bollinger", {}).get("signal", "neutral"))
    boll_award = 5 if boll_signal in {"near_lower", "neutral"} else 0
    raw += boll_award
    available += 5
    factors.append(_factor_row("布林带", boll_signal, boll_award, 5, "中轨或下轨附近更适合观察回调后的承接"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "技术面",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "价格结构到位，技术信号共振较强。", "技术面有亮点，但还没有形成满配共振。", "技术结构仍偏弱，暂不支持激进介入。", "ℹ️ 技术面数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _fundamental_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    metrics: Mapping[str, float],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    percentile_award = 25 if price_percentile < 0.30 else 10 if price_percentile < 0.50 else 0
    raw += percentile_award
    available += 25
    factors.append(_factor_row("估值代理分位", f"价格位置代理 {price_percentile:.0%}", percentile_award, 25, "当前先用价格位置代理估值分位，行业 PE/PB/ROE 仍待补充"))

    flow_award: Optional[int] = None
    flow_detail = "ETF 份额 / 资金流向暂缺"
    if asset_type == "cn_etf":
        try:
            fund_flow = ChinaMarketCollector(config).get_etf_fund_flow(symbol)
            flow_series = pd.to_numeric(fund_flow.get("净流入", pd.Series(dtype=float)), errors="coerce").dropna()
            if flow_series.empty and "净申购份额" in fund_flow.columns:
                flow_series = pd.to_numeric(fund_flow["净申购份额"], errors="coerce").dropna()
            if not flow_series.empty:
                positive_days = int((flow_series.tail(5) > 0).sum())
                flow_award = 10 if positive_days >= 3 else 0
                flow_detail = f"近 5 个可用样本中 {positive_days} 个为净流入/净申购"
        except Exception as exc:
            flow_detail = f"ETF 份额数据缺失: {exc}"
    factors.append(_factor_row("资金承接", "ETF 份额 / 资金流向", flow_award, 10, flow_detail))
    if flow_award is not None:
        raw += flow_award
        available += 10

    factors.append(_factor_row("盈利增速", "缺失", None, 20, "当前未接入对应指数或行业的营收/利润同比"))
    factors.append(_factor_row("ROE", "缺失", None, 20, "当前未接入对应指数或行业的 ROE 数据"))
    factors.append(_factor_row("毛利率", "缺失", None, 15, "当前未接入对应行业毛利率环比数据"))
    factors.append(_factor_row("PEG", "缺失", None, 10, "当前未接入 PEG 代理"))

    score = _normalize_dimension(raw, available, 100)
    summary = _dimension_summary(
        score,
        "估值/资金承接代理偏正面，但当前仍是 ETF/行业代理视角。",
        "基本面代理没有明显便宜或显著昂贵结论。",
        "估值代理偏高，基本面安全边际不足。",
        "ℹ️ 基本面数据缺失，本次评级未纳入完整基本面维度",
    )
    if score is not None and available < 35:
        summary += " 当前仅基于代理因子归一化评分。"
    return {
        "name": "基本面",
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
        "available_max": available,
    }


def _catalyst_dimension(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    related_news = _find_related_news(context.get("news_report", {}).get("items", []), metadata)
    related_events = []
    for event in context.get("events", []):
        text = f"{event.get('title', '')} {event.get('note', '')}"
        if any(keyword in text for keyword in [str(metadata.get("sector", "")), str(metadata.get("name", ""))]):
            related_events.append(event)

    policy_items = [item for item in related_news if str(item.get("category", "")).lower() in {"china_macro", "china_macro_domestic"}]
    policy_award = 30 if policy_items else 0
    raw += policy_award
    available += 30
    factors.append(_factor_row("政策催化", policy_items[0]["title"] if policy_items else "近 7 日未命中直接政策催化", policy_award, 30, "政策原文和一级媒体优先"))

    leader_items = [item for item in related_news if str(item.get("category", "")).lower() in {"china_market_domestic", "earnings"}]
    leader_award = 25 if leader_items else 0
    raw += leader_award
    available += 25
    factors.append(_factor_row("龙头公告/业绩", leader_items[0]["title"] if leader_items else "未命中直接龙头公告", leader_award, 25, "优先看订单、扩产、回购、并购或超预期业绩"))

    overseas_items = [item for item in related_news if str(item.get("category", "")).lower() in {"earnings", "ai", "semiconductor", "fed"}]
    overseas_award = 20 if overseas_items else 0
    raw += overseas_award
    available += 20
    factors.append(_factor_row("海外映射", overseas_items[0]["title"] if overseas_items else "未命中直接海外映射", overseas_award, 20, "重点看海外龙头财报/指引或模型产品催化"))

    density_award = 10 if len(related_news) >= 3 else 0
    raw += density_award
    available += 10
    factors.append(_factor_row("研报/新闻密度", f"相关头条 {len(related_news)} 条", density_award, 10, "当前用一级媒体新闻密度代理"))

    source_count = len({str(item.get("source", "")) for item in related_news if item.get("source")})
    heat_award = 10 if source_count >= 2 else 0
    raw += heat_award
    available += 10
    factors.append(_factor_row("新闻热度", f"覆盖源 {source_count} 个", heat_award, 10, "从少量提及到多源同步，是热度拐点的代理"))

    forward_award = 5 if related_events else 0
    raw += forward_award
    available += 5
    factors.append(_factor_row("前瞻催化", related_events[0]["title"] if related_events else "未来 14 日未命中直接催化事件", forward_award, 5, "当前用本地事件日历代理"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "催化面",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "催化明确，市场有理由重新定价。", "有催化苗头，但强度还不够形成一致预期。", "催化不足，当前更像静态博弈。", "ℹ️ 催化面数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _correlation_to_watchlist(symbol: str, asset_returns: pd.Series, context: Mapping[str, Any]) -> Optional[tuple[str, float]]:
    best_symbol = ""
    best_corr = None
    for peer_symbol, peer_returns in dict(context.get("watchlist_returns", {})).items():
        if peer_symbol == symbol:
            continue
        aligned = pd.concat([asset_returns, peer_returns], axis=1, join="inner").dropna()
        if len(aligned) < 20:
            continue
        corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
        if best_corr is None or abs(corr) > abs(best_corr):
            best_symbol = peer_symbol
            best_corr = corr
    if best_corr is None:
        return None
    return best_symbol, best_corr


def _sector_board_match(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Optional[float]:
    sector = str(metadata.get("sector", ""))
    frame = drivers.get("industry_spot", pd.DataFrame()) if drivers else pd.DataFrame()
    if frame is None or frame.empty:
        return None
    name_col = "板块名称" if "板块名称" in frame.columns else "名称" if "名称" in frame.columns else None
    if name_col is None or "涨跌幅" not in frame.columns:
        return None
    matched = frame[frame[name_col].astype(str).str.contains(sector, na=False)]
    if matched.empty:
        return None
    return float(pd.to_numeric(matched.iloc[0]["涨跌幅"], errors="coerce")) / 100


def _relative_strength_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    metrics: Mapping[str, float],
    asset_returns: pd.Series,
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    benchmark_returns = context.get("benchmark_returns", {}).get(asset_type)
    rel_5d = None
    rel_20d = None
    if benchmark_returns is not None and not benchmark_returns.empty:
        bench_5d = float(benchmark_returns.tail(5).sum())
        bench_20d = float(benchmark_returns.tail(20).sum())
        rel_5d = float(metrics.get("return_5d", 0.0)) - bench_5d
        rel_20d = float(metrics.get("return_20d", 0.0)) - bench_20d
        turn_award = 30 if rel_5d > 0 and rel_20d <= 0 else 20 if rel_20d > 0 else 0
        raw += turn_award
        available += 30
        factors.append(_factor_row("超额拐点", f"相对基准 5日 {format_pct(rel_5d)} / 20日 {format_pct(rel_20d)}", turn_award, 30, "相对基准从负转正更接近轮动切换窗口"))
    else:
        factors.append(_factor_row("超额拐点", "缺失", None, 30, "基准收益序列缺失，未计算超额拐点"))

    board_move = _sector_board_match(metadata, context.get("drivers", {}))
    if board_move is not None:
        breadth_award = 25 if board_move > 0.01 else 10 if board_move > 0 else 0
        raw += breadth_award
        available += 25
        factors.append(_factor_row("板块扩散", f"板块涨跌幅 {format_pct(board_move)}", breadth_award, 25, "板块内部越普涨，越像轮动扩散"))
    else:
        factors.append(_factor_row("板块扩散", "缺失", None, 25, "板块扩散数据缺失"))

    chain_award = 20 if _theme_alignment(metadata, context.get("day_theme", {})) and float(metrics.get("return_5d", 0.0)) < 0.08 else 0
    raw += chain_award
    available += 20
    factors.append(_factor_row("产业链传导", "主线相关但自身尚未极端透支" if chain_award else "当前没有明显下一棒逻辑", chain_award, 20, "主线先启动上下游，再找尚未完全跟涨的方向"))

    preferred_assets = _regime_assets(context.get("regime", {}))
    preferred_key = {
        "电网": "电网基建",
        "科技": "成长股",
        "黄金": "黄金",
        "能源": "顺周期",
        "高股息": "高股息",
    }.get(str(metadata.get("sector", "")), "")
    regime_match = bool(preferred_key and preferred_key in preferred_assets)
    regime_award = 15 if regime_match or _theme_alignment(metadata, context.get("day_theme", {})) else 0
    raw += regime_award
    available += 15
    factors.append(_factor_row("Regime 适配", "与当前 regime / 主线方向一致" if regime_award else "当前 regime 对它没有额外加分", regime_award, 15, "大环境顺风时，轮动更容易持续"))

    ah_award = 10 if asset_type in {"hk", "hk_index"} and float(context.get("global_proxy", {}).get("dxy_20d_change", 0.0)) <= 0 else 0
    raw += ah_award
    available += 10
    factors.append(_factor_row("跨市场比价", "港股估值压力缓和" if ah_award else "该项不适用或暂无明显优势", ah_award, 10, "仅港股相关标的适用"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "相对强弱",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "轮动已经轮到它，具备主线扩散条件。", "相对强弱有改善，但还不是最典型的扩散点。", "轮动还没轮到它，更多是背景观察。", "ℹ️ 相对强弱数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _chips_dimension(symbol: str, asset_type: str, metadata: Mapping[str, Any], context: Mapping[str, Any], config: Mapping[str, Any]) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    factors.append(_factor_row("公募配置", "缺失", None, 30, "基金季报低配/超配仍待补充"))
    factors.append(_factor_row("高管增持", "缺失", None, 25, "ETF 不适用或暂未接入个股增持"))

    if asset_type == "cn_etf":
        try:
            flow = ChinaMarketCollector(config).get_north_south_flow()
            north = flow[flow["资金方向"].astype(str).str.contains("北向", na=False)] if "资金方向" in flow.columns else pd.DataFrame()
            value = float(pd.to_numeric(north.get("成交净买额", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not north.empty else 0.0
            north_award = 20 if value > 0 else 0
            raw += north_award
            available += 20
            factors.append(_factor_row("北向/南向", f"北向净买额约 {value:.2f} 亿", north_award, 20, "当前用全市场方向做代理，而不是单行业拆分"))
        except Exception as exc:
            factors.append(_factor_row("北向/南向", "缺失", None, 20, f"北向/南向数据缺失: {exc}"))
    elif asset_type in {"hk", "hk_index"}:
        factors.append(_factor_row("北向/南向", "港股方向优先看南向", None, 20, "南向分项尚未稳定接入"))
    else:
        factors.append(_factor_row("北向/南向", "该项不适用", None, 20, "当前主要针对权益资产"))

    if asset_type == "cn_etf":
        try:
            flow = ChinaMarketCollector(config).get_etf_fund_flow(symbol)
            series = pd.to_numeric(flow.get("净流入", pd.Series(dtype=float)), errors="coerce").dropna()
            chips_award = 10 if not series.empty and float(series.tail(5).sum()) > 0 else 0
            raw += chips_award
            available += 10
            factors.append(_factor_row("机构资金承接", "ETF 近 5 个样本净流入为正" if chips_award else "ETF 流入没有持续为正", chips_award, 10, "用 ETF 资金流做筹码代理"))
        except Exception as exc:
            factors.append(_factor_row("机构资金承接", "缺失", None, 10, f"ETF 资金流数据缺失: {exc}"))
    else:
        factors.append(_factor_row("机构资金承接", "该项不适用", None, 10, "当前只对 A 股 ETF 接稳定资金流代理"))

    factors.append(_factor_row("机构集中度", "缺失", None, 15, "机构集中度与新进前十数据尚未接入"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "筹码结构",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "聪明钱方向偏正面。", "筹码结构没有形成明确增量共识。", "聪明钱没有明显站在这一边。", "ℹ️ 筹码结构数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _drawdown_percentile(close: pd.Series) -> tuple[float, float]:
    rolling_max = close.cummax()
    drawdown = 1 - close / rolling_max
    current = float(drawdown.iloc[-1])
    percentile = float((drawdown <= current).mean())
    return current, percentile


def _volatility_percentile(close: pd.Series) -> tuple[float, float]:
    returns = close.pct_change().dropna()
    if len(returns) < 40:
        return 0.0, 0.5
    rolling = returns.rolling(20).std().dropna() * math.sqrt(252)
    if rolling.empty:
        return 0.0, 0.5
    current = float(rolling.iloc[-1])
    percentile = float((rolling <= current).mean())
    return current, percentile


def _downside_beta(asset_returns: pd.Series, benchmark_returns: Optional[pd.Series]) -> Optional[float]:
    if benchmark_returns is None or benchmark_returns.empty:
        return None
    aligned = pd.concat([asset_returns, benchmark_returns], axis=1, join="inner").dropna()
    if len(aligned) < 40:
        return None
    downside = aligned[aligned.iloc[:, 1] < 0]
    if len(downside) < 20:
        return None
    variance = float(downside.iloc[:, 1].var())
    if variance == 0:
        return None
    covariance = float(np.cov(downside.iloc[:, 0], downside.iloc[:, 1])[0][1])
    return covariance / variance


def _risk_dimension(symbol: str, asset_type: str, history: pd.DataFrame, asset_returns: pd.Series, context: Mapping[str, Any], correlation_pair: Optional[tuple[str, float]]) -> Dict[str, Any]:
    close = history["close"].astype(float)
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0

    current_dd, dd_pct = _drawdown_percentile(close)
    dd_award = 30 if dd_pct >= 0.70 else 15 if dd_pct >= 0.50 else 0
    raw += dd_award
    available += 30
    factors.append(_factor_row("回撤分位", f"当前回撤 {current_dd:.1%}，历史分位 {dd_pct:.0%}", dd_award, 30, "跌得越充分，风险释放通常越充分"))

    vol, vol_pct = _volatility_percentile(close)
    vol_award = 25 if vol_pct < 0.30 else 10 if vol_pct < 0.50 else 0
    raw += vol_award
    available += 25
    factors.append(_factor_row("波动率", f"20 日年化波动 {vol:.1%}，分位 {vol_pct:.0%}", vol_award, 25, "低波动更像启动前的平静"))

    beta = _downside_beta(asset_returns, context.get("benchmark_returns", {}).get(asset_type))
    if beta is None:
        factors.append(_factor_row("下行 beta", "缺失", None, 20, "基准收益序列不足，未计算 downside beta"))
    else:
        beta_award = 20 if beta < 0.8 else 10 if beta < 1.0 else 0
        raw += beta_award
        available += 20
        factors.append(_factor_row("下行 beta", f"下行 beta {beta:.2f}", beta_award, 20, "大盘跌时它跌得少，风控价值更高"))

    recovery_days = 0
    if len(close) >= 120:
        trailing = close.tail(120).reset_index(drop=True)
        peak = float(trailing.cummax().iloc[-1])
        trough_idx = int((trailing / trailing.cummax() - 1).idxmin())
        if trough_idx < len(trailing) - 1:
            recover = trailing.iloc[trough_idx:]
            recovered = recover[recover >= peak * 0.95]
            recovery_days = int(recovered.index[0] - trough_idx) if not recovered.empty else 999
    recovery_award = 15 if recovery_days and recovery_days < 60 else 0
    raw += recovery_award
    available += 15
    factors.append(_factor_row("回撤恢复", f"近似恢复速度 {recovery_days if recovery_days else '未知'} 日", recovery_award, 15, "恢复越快，韧性越好"))

    if correlation_pair is None:
        factors.append(_factor_row("组合分散", "缺失", None, 10, "watchlist 相关性序列不足"))
    else:
        peer, corr = correlation_pair
        div_award = 10 if abs(corr) < 0.5 else 5 if abs(corr) < 0.75 else 0
        raw += div_award
        available += 10
        factors.append(_factor_row("组合分散", f"与 {peer} 相关性 {corr:.2f}", div_award, 10, "相关性越低，越有真正分散价值"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "风险特征",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "下行空间和组合风险都还可控。", "风险可控但没有特别便宜。", "风险收益比不占优，需更严控节奏。", "ℹ️ 风险特征数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _seasonality_dimension(metadata: Mapping[str, Any], history: pd.DataFrame, context: Mapping[str, Any]) -> Dict[str, Any]:
    dated_history = history.copy()
    dated_history["date"] = pd.to_datetime(dated_history["date"], errors="coerce")
    dated_history = dated_history.dropna(subset=["date"]).set_index("date").sort_index()
    close = dated_history["close"].astype(float)
    monthly = close.resample("ME").last().pct_change().dropna()
    month = datetime.now().month
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0

    if not monthly.empty:
        month_series = monthly[monthly.index.month == month]
        if not month_series.empty:
            win_rate = float((month_series > 0).mean())
            month_award = 30 if win_rate > 0.65 else 10 if win_rate > 0.50 else 0
            raw += month_award
            available += 30
            factors.append(_factor_row("月度胜率", f"同月胜率 {win_rate:.0%}", month_award, 30, "当前用标的自身历史月度胜率做代理"))
        else:
            factors.append(_factor_row("月度胜率", "缺失", None, 30, "样本不足"))
    else:
        factors.append(_factor_row("月度胜率", "缺失", None, 30, "缺少月度历史"))

    sector = str(metadata.get("sector", "综合"))
    in_window = month in MONTHLY_SEASONAL_WINDOWS.get(sector, set())
    window_award = 25 if in_window else 0
    raw += window_award
    available += 25
    factors.append(_factor_row("旺季前置", "位于常见旺季窗口" if in_window else "当前不在典型旺季前置窗口", window_award, 25, "基于行业常见季节性映射"))

    earnings_window = month in {1, 4, 7, 10}
    earnings_award = 20 if earnings_window and sector == "科技" else 0
    raw += earnings_award
    available += 20
    factors.append(_factor_row("财报窗口", "当前处在典型财报月附近" if earnings_award else "当前不是典型财报博弈窗口", earnings_award, 20, "以季度财报窗口做代理"))

    factors.append(_factor_row("指数调整", "缺失", None, 15, "指数纳入/调整日历暂未接入"))

    dividend_award = 10 if sector == "高股息" and month in {4, 5, 6} else 0
    raw += dividend_award
    available += 10
    factors.append(_factor_row("分红窗口", "接近高股息常见抢权窗口" if dividend_award else "当前不是典型分红博弈窗口", dividend_award, 10, "高股息方向更相关"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "季节/日历",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "当前时间窗口相对有利。", "时间窗口中性，没有明显顺风。", "时间窗口不占优，更多靠主线和技术本身。", "ℹ️ 季节/日历数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _macro_dimension(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    vector = SENSITIVITY_MAP.get(str(metadata.get("sector", "综合")), {"rate": 0, "usd": 0, "oil": 0, "cny": 0})
    states = _current_factor_state(context)
    match_count = 0
    active = 0
    details: List[str] = []
    for factor in ("rate", "usd", "oil", "cny"):
        direction = int(vector.get(factor, 0))
        if direction == 0:
            continue
        active += 1
        same = direction == states.get(factor)
        match_count += 1 if same else 0
        details.append(f"{factor} {'顺风' if same else '逆风'}")

    if active == 0:
        score = 20
    elif match_count >= 3:
        score = 40
    elif match_count == 2:
        score = 30
    elif match_count == 1:
        score = 10
    else:
        score = 0
    factors = [
        _factor_row("敏感度向量", " / ".join(details) if details else "当前定义为中性敏感度", score, 40, "利率 / 美元 / 油价 / 人民币 四因子匹配"),
        _factor_row("当前 regime", context.get("regime", {}).get("current_regime", "unknown"), 0, 0, "宏观敏感度是修正因子，不单独决定方向"),
    ]
    return {
        "name": "宏观敏感度",
        "score": score,
        "max_score": 40,
        "summary": "大环境明显配合。" if score >= 30 else "宏观中性。" if score >= 20 else "宏观逆风明显。",
        "factors": factors,
        "core_signal": " · ".join(details[:3]) if details else "宏观中性",
        "missing": False,
        "macro_reverse": score == 0,
    }


def _rating_from_dimensions(dimensions: Mapping[str, Mapping[str, Any]], warnings: Sequence[str]) -> Dict[str, Any]:
    tech = dimensions["technical"]["score"]
    fundamental = dimensions["fundamental"]["score"]
    catalyst = dimensions["catalyst"]["score"]
    relative = dimensions["relative_strength"]["score"]
    risk = dimensions["risk"]["score"]
    macro = dimensions["macro"]["score"]

    def ok(value: Optional[int], threshold: int) -> bool:
        return value is not None and value >= threshold

    rank = 0
    label = "无信号"
    meaning = "没有形成可执行的多维共振。"
    if ok(tech, 70) and ok(fundamental, 60) and ok(catalyst, 50) and ok(risk, 50):
        rank, label, meaning = 4, "强机会", "四维共振，具备建仓计划条件。"
    elif ok(fundamental, 60) and (ok(catalyst, 50) or ok(relative, 60)) and ok(tech, 40):
        rank, label, meaning = 3, "较强机会", "逻辑成立，但还需要一个维度继续确认。"
    elif (ok(tech, 70) and catalyst is not None and catalyst < 30) or (ok(catalyst, 60) and tech is not None and tech < 40):
        rank, label, meaning = 2, "储备机会", "单维度亮灯但还未形成共振。"
    else:
        strong_dims = sum(1 for item in dimensions.values() if item.get("score") is not None and item.get("score", 0) >= 70)
        mid_dims = sum(1 for item in dimensions.values() if item.get("score") is not None and item.get("score", 0) >= 40)
        if strong_dims == 1 and mid_dims <= 2:
            rank, label, meaning = 1, "有信号但不充分", "只有单一维度足够亮，其余不足以支持动作。"

    if dimensions["macro"].get("macro_reverse"):
        rank = min(rank, 2)
        warnings = list(warnings) + ["⚠️ 宏观敏感度完全逆风，评级上限已压到 ⭐⭐"]
    if tech is None:
        rank = min(rank, 2)
        warnings = list(warnings) + ["ℹ️ 技术面数据缺失，评级上限降至 ⭐⭐"]
    if dimensions["fundamental"].get("available_max", 0) < 30:
        rank = min(rank, 3)
        warnings = list(warnings) + ["ℹ️ 基本面当前以代理因子为主，评级上限降至 ⭐⭐⭐"]

    stars = "—" if rank == 0 else "⭐" * rank
    return {"rank": rank, "stars": stars, "label": label, "meaning": meaning, "warnings": list(dict.fromkeys(warnings))}


def _action_plan(
    analysis: Mapping[str, Any],
    history: pd.DataFrame,
    technical: Mapping[str, Any],
) -> Dict[str, str]:
    rating = analysis["rating"]["rank"]
    tech = analysis["dimensions"]["technical"]["score"]
    macro_reverse = analysis["dimensions"]["macro"].get("macro_reverse", False)
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    fib_levels = technical.get("fibonacci", {}).get("levels", {})
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", history["close"].iloc[-1]))
    stop_ref = max(float(fib_levels.get("0.382", 0.0)), float(fib_levels.get("0.500", 0.0)), ma60)
    target_ref = float(history["high"].tail(60).max())
    if rating >= 3 and not macro_reverse:
        direction = "做多"
    elif rating == 2:
        direction = "观望"
    else:
        direction = "回避"
    if rsi > 70:
        entry = "等 RSI 回落到 60 附近且 MACD 不死叉，再考虑分批介入"
    elif tech is not None and tech >= 70:
        entry = "等回踩 MA20 / MA60 或关键斐波那契支撑后企稳，再做首次试探"
    else:
        entry = "先等 MACD 再次转强或站回 MA20，避免在弱趋势里提前出手"
    position = "首次建仓 ≤5%，确认后再加到 10%" if rating >= 3 else "先不超过 5% 试错" if rating == 2 else "暂不出手"
    timeframe = "中线配置(1-3月)" if rating >= 3 else "短线交易(1-2周)" if rating == 2 else "等待更好窗口"
    target = f"先看前高/近 60 日高点 {target_ref:.3f} 附近的承压与突破情况"
    stop = f"跌破 {stop_ref:.3f} 或主线/催化失效时重新评估"
    return {
        "direction": direction,
        "entry": entry,
        "position": position,
        "stop": stop,
        "target": target,
        "timeframe": timeframe,
    }


def analyze_opportunity(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    context: Optional[Mapping[str, Any]] = None,
    metadata_override: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    runtime_context = dict(context or build_market_context(config, relevant_asset_types=[asset_type, "cn_etf", "futures"]))
    runtime_context["config"] = dict(config)
    metadata = _merge_metadata(symbol, asset_type, metadata_override)
    history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config)))
    technical = TechnicalAnalyzer(history).generate_scorecard(dict(config).get("technical", {}))
    metrics = compute_history_metrics(history)
    asset_returns = history["close"].pct_change().dropna()
    correlation_pair = _correlation_to_watchlist(symbol, asset_returns, runtime_context)

    dimensions = {
        "technical": _technical_dimension(history, technical),
        "fundamental": _fundamental_dimension(symbol, asset_type, metadata, metrics, config),
        "catalyst": _catalyst_dimension(metadata, runtime_context),
        "relative_strength": _relative_strength_dimension(symbol, asset_type, metadata, metrics, asset_returns, runtime_context),
        "chips": _chips_dimension(symbol, asset_type, metadata, runtime_context, config),
        "risk": _risk_dimension(symbol, asset_type, history, asset_returns, runtime_context, correlation_pair),
        "seasonality": _seasonality_dimension(metadata, history, runtime_context),
        "macro": _macro_dimension(metadata, runtime_context),
    }
    checks, exclusion_reasons, warnings = _hard_checks(metadata, history, metrics, technical, runtime_context, dimensions["macro"]["score"], correlation_pair)
    rating = _rating_from_dimensions(dimensions, warnings)
    action = _action_plan({"rating": rating, "dimensions": dimensions}, history, technical)
    notes: List[str] = list(runtime_context.get("notes", []))
    if metadata.get("in_watchlist"):
        notes.append("该标的已在 watchlist 中，本次分析更偏复核而不是首次发现。")
    conclusion = (
        f"{dimensions['technical']['summary']} "
        f"{dimensions['fundamental']['summary']} "
        f"{dimensions['catalyst']['summary']}"
    ).strip()
    risks = list(dict.fromkeys(rating["warnings"] + exclusion_reasons))
    if not risks:
        risks = ["当前没有触发额外强风险，但仍需按主线和止损条件执行。"]

    return {
        "symbol": symbol,
        "name": str(metadata.get("name", symbol)),
        "asset_type": asset_type,
        "metadata": metadata,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "regime": runtime_context.get("regime", {}),
        "day_theme": runtime_context.get("day_theme", {}),
        "metrics": metrics,
        "technical_raw": technical,
        "dimensions": dimensions,
        "hard_checks": checks,
        "rating": rating,
        "conclusion": conclusion,
        "action": action,
        "risks": risks,
        "notes": notes,
        "excluded": bool(exclusion_reasons),
        "exclusion_reasons": exclusion_reasons,
        "correlation_pair": correlation_pair,
    }


def build_default_pool(config: Mapping[str, Any], theme_filter: str = "") -> tuple[List[PoolItem], List[str]]:
    warnings: List[str] = []
    watchlist = load_watchlist()
    pool: List[PoolItem] = []
    seen: set[str] = set()
    opportunity_cfg = dict(config).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("min_turnover", 50_000_000))
    max_candidates = int(opportunity_cfg.get("max_scan_candidates", 30))
    lowered_filter = theme_filter.lower().strip()

    try:
        realtime = ChinaMarketCollector(config).get_etf_realtime()
        code_col = "代码" if "代码" in realtime.columns else "基金代码" if "基金代码" in realtime.columns else None
        name_col = "名称" if "名称" in realtime.columns else "基金简称" if "基金简称" in realtime.columns else None
        amount_col = "成交额" if "成交额" in realtime.columns else None
        if code_col and name_col and amount_col:
            frame = realtime.copy()
            frame[amount_col] = pd.to_numeric(frame[amount_col], errors="coerce").fillna(0.0)
            frame = frame[frame[amount_col] >= min_turnover]
            if lowered_filter:
                frame = frame[frame[name_col].astype(str).str.lower().str.contains(lowered_filter, na=False)]
            excluded_keywords = ("债", "货币", "国债", "政金", "现金", "利率", "短融")
            frame = frame[~frame[name_col].astype(str).str.contains("|".join(excluded_keywords), na=False)]
            frame = frame.sort_values(amount_col, ascending=False).head(max_candidates)
            for _, row in frame.iterrows():
                symbol = str(row[code_col])
                if symbol in seen:
                    continue
                name = str(row[name_col])
                sector, chain_nodes = _normalize_sector(name)
                pool.append(
                    PoolItem(
                        symbol=symbol,
                        name=name,
                        asset_type="cn_etf",
                        region="CN",
                        sector=sector,
                        chain_nodes=chain_nodes,
                        source="all_market_etf",
                        turnover=float(row[amount_col]),
                        in_watchlist=any(item["symbol"] == symbol for item in watchlist),
                    )
                )
                seen.add(symbol)
    except Exception as exc:
        warnings.append(f"全市场 ETF 扫描池拉取失败，已回退到 watchlist: {exc}")

    for item in watchlist:
        if lowered_filter and lowered_filter not in str(item.get("name", "")).lower() and lowered_filter not in str(item.get("sector", "")).lower():
            continue
        if item["symbol"] in seen:
            continue
        metadata = _merge_metadata(str(item["symbol"]), str(item.get("asset_type", "cn_etf")), item)
        pool.append(
            PoolItem(
                symbol=str(item["symbol"]),
                name=str(metadata.get("name", item["symbol"])),
                asset_type=str(item.get("asset_type", "cn_etf")),
                region=str(metadata.get("region", "CN")),
                sector=str(metadata.get("sector", "综合")),
                chain_nodes=list(metadata.get("chain_nodes", DEFAULT_CHAIN_NODES)),
                source="watchlist",
                in_watchlist=True,
            )
        )
        seen.add(item["symbol"])

    return pool, warnings


def discover_opportunities(config: Mapping[str, Any], top_n: int = 5, theme_filter: str = "") -> Dict[str, Any]:
    context = build_market_context(config, relevant_asset_types=["cn_etf", "futures"])
    pool, pool_warnings = build_default_pool(config, theme_filter)
    passed = 0
    analyses: List[Dict[str, Any]] = []
    blind_spots: List[str] = list(pool_warnings)
    for item in pool:
        try:
            analysis = analyze_opportunity(
                item.symbol,
                item.asset_type,
                config,
                context=context,
                metadata_override={
                    "name": item.name,
                    "sector": item.sector,
                    "chain_nodes": item.chain_nodes,
                    "region": item.region,
                    "in_watchlist": item.in_watchlist,
                },
            )
        except Exception as exc:
            blind_spots.append(f"{item.symbol} 扫描失败: {exc}")
            continue
        if analysis["excluded"]:
            continue
        passed += 1
        if analysis["rating"]["rank"] > 0:
            analyses.append(analysis)
    analyses.sort(
        key=lambda item: (
            item["rating"]["rank"],
            item["dimensions"]["technical"]["score"] or 0,
            item["dimensions"]["relative_strength"]["score"] or 0,
            item["dimensions"]["catalyst"]["score"] or 0,
        ),
        reverse=True,
    )
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_pool": len(pool),
        "passed_pool": passed,
        "regime": context.get("regime", {}),
        "day_theme": context.get("day_theme", {}),
        "top": analyses[:top_n],
        "blind_spots": blind_spots[:8],
        "theme_filter": theme_filter,
    }


def compare_opportunities(symbols: Sequence[str], config: Mapping[str, Any]) -> Dict[str, Any]:
    asset_types = [detect_asset_type_for_compare(symbol, config) for symbol in symbols]
    context = build_market_context(config, relevant_asset_types=list(dict.fromkeys(asset_types + ["cn_etf", "futures"])))
    rows: List[Dict[str, Any]] = []
    for symbol, asset_type in zip(symbols, asset_types):
        rows.append(analyze_opportunity(symbol, asset_type, config, context=context))
    best = max(
        rows,
        key=lambda item: (
            item["rating"]["rank"],
            sum((dimension.get("score") or 0) for dimension in item["dimensions"].values()),
        ),
    )
    return {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "analyses": rows, "best_symbol": best["symbol"]}


def detect_asset_type_for_compare(symbol: str, config: Mapping[str, Any]) -> str:
    return detect_asset_type(symbol, config)
