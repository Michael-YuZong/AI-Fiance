"""Scorecard aggregation logic."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


def _icon_from_score(score: int) -> str:
    if score >= 2:
        return "✅"
    if score <= -2:
        return "❌"
    return "⚠️"


def _item_icon(score: int) -> str:
    if score > 0:
        return "✅"
    if score < 0:
        return "❌"
    return "⚠️"


class ScorecardBuilder:
    """Combine technical and valuation inputs into a structured scorecard."""

    def build(
        self,
        symbol: str,
        asset_type: str,
        technical_scorecard: Dict[str, Any],
        price_history: pd.DataFrame,
        valuation_snapshot: Optional[Dict[str, Any]] = None,
        extra_sections: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        technical_section = self._technical_section(technical_scorecard)
        valuation_section = self._valuation_section(price_history, valuation_snapshot)
        sections = list(extra_sections or []) + [technical_section, valuation_section]
        return {
            "symbol": symbol,
            "asset_type": asset_type,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sections": sections,
            "notes": [
                "本输出只做研究辅助，不构成投资建议。",
                "估值面当前仍以价格位置代理为主，后续再接入 PE/PB、PEG、股息率等更完整数据。",
            ],
        }

    def _technical_section(self, technical: Dict[str, Any]) -> Dict[str, Any]:
        items: List[Dict[str, str]] = []
        score = 0

        macd_signal = technical["macd"]["signal"]
        macd_score = 1 if macd_signal == "bullish" else -1
        score += macd_score
        items.append(
            {
                "name": "MACD",
                "icon": _item_icon(macd_score),
                "reason": (
                    f"DIF={technical['macd']['DIF']:.3f}, DEA={technical['macd']['DEA']:.3f}, "
                    f"当前为{'金叉偏多' if macd_signal == 'bullish' else '死叉偏弱'}。"
                ),
            }
        )

        ma_signal = technical["ma_system"]["signal"]
        ma_score = 1 if ma_signal == "bullish" else -1 if ma_signal == "bearish" else 0
        score += ma_score
        items.append(
            {
                "name": "均线系统",
                "icon": _item_icon(ma_score),
                "reason": f"均线排列为 {technical['ma_system']['alignment']}，趋势状态为 {ma_signal}。",
            }
        )

        rsi_signal = technical["rsi"]["signal"]
        rsi_score = 1 if rsi_signal == "oversold" else -1 if rsi_signal == "overbought" else 0
        score += rsi_score
        items.append(
            {
                "name": "RSI",
                "icon": _item_icon(rsi_score),
                "reason": f"RSI={technical['rsi']['RSI']:.1f}，状态为 {rsi_signal}。",
            }
        )

        dmi_signal = technical["dmi"]["signal"]
        dmi_score = 1 if dmi_signal == "bullish_trend" else -1 if dmi_signal == "bearish_trend" else 0
        score += dmi_score
        items.append(
            {
                "name": "DMI/ADX",
                "icon": _item_icon(dmi_score),
                "reason": (
                    f"DI+={technical['dmi']['DI+']:.1f}, DI-={technical['dmi']['DI-']:.1f}, "
                    f"ADX={technical['dmi']['ADX']:.1f}。"
                ),
            }
        )

        volume_signal = technical["volume"]["signal"]
        volume_score = 1 if volume_signal == "heavy_volume" else -1 if volume_signal == "light_volume" else 0
        score += volume_score
        items.append(
            {
                "name": "量能",
                "icon": _item_icon(volume_score),
                "reason": f"量比={technical['volume']['vol_ratio']:.2f}，状态为 {volume_signal}。",
            }
        )

        patterns = technical.get("candlestick", [])
        pattern_reason = "最近一根 K 线未识别出明显形态。" if not patterns else f"最近形态：{', '.join(patterns)}。"
        items.append({"name": "K 线形态", "icon": "⚠️", "reason": pattern_reason})

        overall = _icon_from_score(score)
        summary = {
            "✅": "技术面偏强，趋势和动量信号整体站在多头一侧。",
            "⚠️": "技术面中性，当前没有足够多的单边共振信号。",
            "❌": "技术面偏弱，趋势和动量信号整体承压。",
        }[overall]
        return {"title": "技术面", "overall": overall, "summary": summary, "items": items}

    def _valuation_section(
        self,
        price_history: pd.DataFrame,
        valuation_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        close = price_history["close"].dropna()
        window = close.tail(min(len(close), 750))
        current_price = float(window.iloc[-1])
        high = float(window.max())
        low = float(window.min())
        percentile = float((window <= current_price).mean())
        drawdown = (current_price / high) - 1 if high else 0.0
        rebound_from_low = (current_price / low) - 1 if low else 0.0

        if valuation_snapshot is None:
            valuation_snapshot = {
                "method": "price_position_proxy",
                "price_percentile": percentile,
                "drawdown_from_high": drawdown,
                "rebound_from_low": rebound_from_low,
            }

        score = 0
        items: List[Dict[str, str]] = []

        percentile_score = 1 if percentile <= 0.3 else -1 if percentile >= 0.7 else 0
        score += percentile_score
        items.append(
            {
                "name": "区间分位",
                "icon": _item_icon(percentile_score),
                "reason": f"当前价格处于近样本区间的 {percentile:.0%} 分位。",
            }
        )

        drawdown_score = 1 if drawdown <= -0.15 else -1 if drawdown >= -0.05 else 0
        score += drawdown_score
        items.append(
            {
                "name": "距区间高点",
                "icon": _item_icon(drawdown_score),
                "reason": f"相对区间高点回撤 {drawdown:.1%}。",
            }
        )

        rebound_score = -1 if rebound_from_low >= 0.6 else 0
        score += rebound_score
        items.append(
            {
                "name": "距区间低点",
                "icon": _item_icon(rebound_score),
                "reason": f"相对区间低点反弹 {rebound_from_low:.1%}。",
            }
        )

        overall = _icon_from_score(score)
        summary = {
            "✅": "当前位置偏低，估值代理层面更接近左侧观察区。",
            "⚠️": "当前位置中性，估值代理没有给出明显便宜或拥挤结论。",
            "❌": "当前位置偏高，估值代理提示拥挤或安全边际有限。",
        }[overall]
        return {"title": "估值面", "overall": overall, "summary": summary, "items": items}
