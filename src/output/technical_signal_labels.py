"""Shared technical signal labels for charts and report summaries."""

from __future__ import annotations

from typing import Any, Dict, Mapping

import numpy as np
import pandas as pd

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame


SignalBadge = tuple[str, str]


def trim_indicator_series(indicators: Mapping[str, Any], window: int) -> Dict[str, Any]:
    trimmed: Dict[str, Any] = {}
    for key, value in indicators.items():
        if isinstance(value, (pd.Series, pd.DataFrame)):
            trimmed[key] = value.tail(window)
        else:
            trimmed[key] = value
    return trimmed


def build_technical_signal_context(
    history: pd.DataFrame | None,
    *,
    calc_window: int = 120,
    plot_window: int = 22,
) -> Dict[str, Any]:
    if history is None or not isinstance(history, pd.DataFrame) or history.empty:
        return {}
    try:
        normalized = normalize_ohlcv_frame(history.tail(max(calc_window, plot_window)).copy())
        technical = TechnicalAnalyzer(normalized)
        indicators = trim_indicator_series(technical.indicator_series(), plot_window)
        plot_history = normalized.tail(plot_window).reset_index(drop=True)
    except Exception:
        return {}
    return {
        "history": plot_history,
        "indicators": indicators,
        "divergence": technical.divergence_analysis(),
    }


def market_mode_badge(indicators: Mapping[str, Any]) -> SignalBadge:
    adx = float(indicators["adx"].iloc[-1])
    if adx >= 25:
        return ("趋势市", "bull")
    if adx <= 18:
        return ("震荡市", "neutral")
    return ("过渡期", "warn")


def recent_divergence_label(price_series: pd.Series, signal_series: pd.Series) -> SignalBadge | None:
    price = pd.to_numeric(pd.Series(price_series), errors="coerce").reset_index(drop=True)
    signal = pd.to_numeric(pd.Series(signal_series), errors="coerce").reset_index(drop=True)
    size = min(len(price), len(signal))
    if size < 10:
        return None
    price = price.tail(size).reset_index(drop=True)
    signal = signal.tail(size).reset_index(drop=True)
    mid = size // 2
    early_p = price.iloc[:mid]
    late_p = price.iloc[mid:]
    early_s = signal.iloc[:mid]
    late_s = signal.iloc[mid:]
    if early_p.empty or late_p.empty or early_s.empty or late_s.empty:
        return None

    early_hi = int(np.argmax(early_p.to_numpy()))
    late_hi = int(np.argmax(late_p.to_numpy())) + mid
    if price.iloc[late_hi] > price.iloc[early_hi] * 1.005 and signal.iloc[late_hi] < signal.iloc[early_hi] * 0.98:
        return ("顶背离", "bear")

    early_lo = int(np.argmin(early_p.to_numpy()))
    late_lo = int(np.argmin(late_p.to_numpy())) + mid
    if price.iloc[late_lo] < price.iloc[early_lo] * 0.995 and signal.iloc[late_lo] > signal.iloc[early_lo] * 1.02:
        return ("底背离", "bull")
    return None


def divergence_badge_for_indicator(indicator_name: str, divergence: Mapping[str, Any] | None) -> SignalBadge | None:
    if not divergence:
        return None
    signal = str(divergence.get("signal", "") or "")
    kind = str(divergence.get("kind", "") or "")
    indicators = {str(item).upper() for item in divergence.get("indicators", [])}
    if signal not in {"bullish", "bearish"}:
        return None
    if str(indicator_name).upper() not in indicators:
        return None
    return (kind, "bull" if signal == "bullish" else "bear")


def macd_badges(
    indicators: Mapping[str, Any],
    close_series: pd.Series,
    divergence: Mapping[str, Any] | None = None,
) -> list[SignalBadge]:
    dif = float(indicators["macd_dif"].iloc[-1])
    dea = float(indicators["macd_dea"].iloc[-1])
    hist = float(indicators["macd_hist"].iloc[-1])
    prev_hist = float(indicators["macd_hist"].iloc[-2]) if len(indicators["macd_hist"]) >= 2 else hist
    badges: list[SignalBadge] = [market_mode_badge(indicators)]
    badges.append(("多头主导", "bull") if dif >= dea else ("空头主导", "bear"))
    if hist >= 0:
        badges.append(("动能增强", "bull") if hist >= prev_hist else ("多头回落", "warn"))
    else:
        badges.append(("空头放大", "bear") if hist <= prev_hist else ("空头收敛", "warn"))
    shared_divergence = divergence_badge_for_indicator("MACD", divergence)
    fallback_divergence = recent_divergence_label(close_series, indicators["macd_dif"])
    if shared_divergence:
        badges.append(shared_divergence)
    elif fallback_divergence:
        badges.append(fallback_divergence)
    return badges


def kdj_badges(indicators: Mapping[str, Any]) -> list[SignalBadge]:
    k = float(indicators["kdj_k"].iloc[-1])
    d = float(indicators["kdj_d"].iloc[-1])
    j = float(indicators["kdj_j"].iloc[-1])
    badges: list[SignalBadge] = [market_mode_badge(indicators)]
    if max(k, d) >= 80:
        badges.append(("超买区", "warn"))
    elif min(k, d) <= 20:
        badges.append(("超卖区", "warn"))
    else:
        badges.append(("中性区", "neutral"))
    badges.append(("K线上穿D", "bull") if k >= d else ("K线下穿D", "bear"))
    if j > 100 or j < 0:
        badges.append(("波动放大", "warn"))
    return badges


def rsi_badges(
    indicators: Mapping[str, Any],
    close_series: pd.Series,
    divergence: Mapping[str, Any] | None = None,
) -> list[SignalBadge]:
    rsi = float(indicators["rsi"].iloc[-1])
    prev_rsi = float(indicators["rsi"].iloc[-5]) if len(indicators["rsi"]) >= 5 else rsi
    badges: list[SignalBadge] = [market_mode_badge(indicators)]
    if rsi >= 60:
        badges.append(("强势区", "bull"))
    elif rsi <= 40:
        badges.append(("弱势区", "bear"))
    else:
        badges.append(("中性震荡", "neutral"))
    badges.append(("修复中", "bull") if rsi >= prev_rsi else ("走弱中", "bear"))
    shared_divergence = divergence_badge_for_indicator("RSI", divergence)
    fallback_divergence = recent_divergence_label(close_series, indicators["rsi"])
    if shared_divergence:
        badges.append(shared_divergence)
    elif fallback_divergence:
        badges.append(fallback_divergence)
    return badges


def boll_badges(history: pd.DataFrame, indicators: Mapping[str, Any]) -> list[SignalBadge]:
    close = float(history["close"].iloc[-1])
    upper = float(indicators["boll_upper"].iloc[-1])
    lower = float(indicators["boll_lower"].iloc[-1])
    mid = float(indicators["boll_mid"].iloc[-1])
    width = pd.to_numeric(indicators["boll_upper"] - indicators["boll_lower"], errors="coerce")
    latest_width = float(width.iloc[-1])
    median_width = float(width.tail(10).median()) if len(width) >= 5 else latest_width
    badges: list[SignalBadge] = []
    if latest_width <= median_width * 0.92:
        badges.append(("收口", "warn"))
    elif latest_width >= median_width * 1.08:
        badges.append(("开口", "bull"))
    else:
        badges.append(("带宽平稳", "neutral"))
    if close >= upper * 0.995:
        badges.append(("上轨附近", "bull"))
    elif close <= lower * 1.005:
        badges.append(("下轨附近", "bear"))
    elif abs(close - mid) <= max((upper - lower) * 0.15, abs(mid) * 0.01):
        badges.append(("中轨回归", "neutral"))
    else:
        badges.append(("偏离中轨", "warn"))
    badges.append(("上轨突破", "bull") if close > upper else ("未破上轨", "neutral"))
    return badges


def adx_badges(indicators: Mapping[str, Any]) -> list[SignalBadge]:
    adx = float(indicators["adx"].iloc[-1])
    prev_adx = float(indicators["adx"].iloc[-5]) if len(indicators["adx"]) >= 5 else adx
    plus_di = float(indicators["plus_di"].iloc[-1])
    minus_di = float(indicators["minus_di"].iloc[-1])
    badges: list[SignalBadge] = [market_mode_badge(indicators)]
    badges.append(("+DI主导", "bull") if plus_di >= minus_di else ("-DI主导", "bear"))
    if adx >= prev_adx + 1:
        badges.append(("趋势增强", "bull"))
    elif adx <= prev_adx - 1:
        badges.append(("趋势转弱", "warn"))
    else:
        badges.append(("趋势平稳", "neutral"))
    return badges


def obv_badges(
    indicators: Mapping[str, Any],
    close_series: pd.Series,
    divergence: Mapping[str, Any] | None = None,
) -> list[SignalBadge]:
    obv = float(indicators["obv"].iloc[-1])
    obv_ma = float(indicators["obv_ma"].iloc[-1])
    prev_obv = float(indicators["obv"].iloc[-5]) if len(indicators["obv"]) >= 5 else obv
    badges: list[SignalBadge] = [market_mode_badge(indicators)]
    badges.append(("量能确认", "bull") if obv >= obv_ma else ("量能未跟", "bear"))
    badges.append(("吸筹", "bull") if obv >= prev_obv else ("派发", "bear"))
    shared_divergence = divergence_badge_for_indicator("OBV", divergence)
    fallback_divergence = recent_divergence_label(close_series, indicators["obv"])
    if shared_divergence:
        badges.append(shared_divergence)
    elif fallback_divergence:
        badges.append(fallback_divergence)
    return badges


def build_indicator_badge_map(context: Mapping[str, Any]) -> Dict[str, list[SignalBadge]]:
    indicators = dict(context.get("indicators") or {})
    history = context.get("history")
    divergence = dict(context.get("divergence") or {})
    if not indicators or not isinstance(history, pd.DataFrame) or history.empty:
        return {}
    close_series = history["close"].reset_index(drop=True)
    return {
        "macd": macd_badges(indicators, close_series, divergence),
        "kdj": kdj_badges(indicators),
        "rsi": rsi_badges(indicators, close_series, divergence),
        "boll": boll_badges(history, indicators),
        "adx": adx_badges(indicators),
        "obv": obv_badges(indicators, close_series, divergence),
    }


def compact_technical_signal_text(history: pd.DataFrame | None, *, max_items: int = 3) -> str:
    context = build_technical_signal_context(history)
    if not context:
        return ""
    badge_map = build_indicator_badge_map(context)
    if not badge_map:
        return ""

    selected: list[str] = []

    def add(label: str) -> None:
        clean = str(label).strip()
        if clean and clean not in selected:
            selected.append(clean)

    add(badge_map["macd"][0][0])
    for key in ("macd", "rsi", "obv"):
        for label, _tone in badge_map.get(key, []):
            if "背离" in label:
                add(label)
                break
        if len(selected) >= max_items:
            break
    for label, _tone in badge_map.get("macd", [])[1:]:
        if label in {"多头主导", "空头主导", "动能增强", "空头放大", "空头收敛", "多头回落"}:
            add(label)
            break
    for label, _tone in badge_map.get("obv", []):
        if label in {"量能确认", "量能未跟", "吸筹", "派发"}:
            add(label)
            break
    for label, _tone in badge_map.get("adx", []):
        if label in {"趋势增强", "趋势转弱"}:
            add(label)
            break

    if not selected:
        return ""
    return f"当前图形标签：{'、'.join(selected[:max_items])}。"


def compact_technical_trigger_text(history: pd.DataFrame | None, *, max_items: int = 2) -> str:
    context = build_technical_signal_context(history)
    if not context:
        return ""
    badge_map = build_indicator_badge_map(context)
    if not badge_map:
        return ""

    selected: list[str] = []

    def add(label: str) -> None:
        clean = str(label).strip()
        if clean and clean not in selected:
            selected.append(clean)

    market_mode = badge_map.get("macd", [("过渡期", "warn")])[0][0]
    if market_mode == "震荡市":
        add("震荡区间能否脱离")
    elif market_mode == "过渡期":
        add("方向重新拉开")

    for key in ("macd", "rsi", "obv"):
        for label, _tone in badge_map.get(key, []):
            if label == "顶背离":
                add("顶背离修复")
                break
            if label == "底背离":
                add("底背离延续")
                break
        if len(selected) >= max_items:
            break

    macd_mapping = {
        "空头主导": "空头主导缓解",
        "多头主导": "多头主导延续",
        "动能增强": "动能继续增强",
        "空头放大": "空头动能收敛",
        "空头收敛": "空头收敛后转强",
        "多头回落": "多头回落止住",
    }
    for label, _tone in badge_map.get("macd", [])[1:]:
        mapped = macd_mapping.get(label, "")
        if mapped:
            add(mapped)
            break

    obv_mapping = {
        "量能未跟": "量能回补",
        "量能确认": "量能继续确认",
        "吸筹": "吸筹继续",
        "派发": "派发结束",
    }
    for label, _tone in badge_map.get("obv", []):
        mapped = obv_mapping.get(label, "")
        if mapped:
            add(mapped)
            break

    adx_mapping = {
        "趋势增强": "趋势强度继续抬升",
        "趋势转弱": "趋势转弱先止住",
    }
    for label, _tone in badge_map.get("adx", []):
        mapped = adx_mapping.get(label, "")
        if mapped:
            add(mapped)
            break

    if not selected:
        return ""
    return f"技术上先看{'、'.join(selected[:max_items])}。"


def append_technical_trigger_text(
    base_text: str,
    history: pd.DataFrame | None,
    *,
    max_items: int = 2,
) -> str:
    base = str(base_text).strip().rstrip("。；;,， ")
    trigger_text = compact_technical_trigger_text(history, max_items=max_items)
    if not trigger_text:
        return f"{base}。" if base else ""
    trigger_clause = trigger_text.replace("技术上先看", "", 1).strip().rstrip("。；;,， ")
    for prefix in ("先等", "先看", "先", "优先看"):
        if trigger_clause.startswith(prefix):
            trigger_clause = trigger_clause[len(prefix):].strip()
            break
    if not base:
        return f"技术上先看{trigger_clause}。" if trigger_clause else ""
    if trigger_clause and trigger_clause in base:
        return f"{base}。"
    return f"{base}；技术上先看{trigger_clause}。"
