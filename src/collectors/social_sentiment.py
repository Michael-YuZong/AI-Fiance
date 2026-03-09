"""Offline-friendly social sentiment proxy collector."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from src.processors.social_index import SocialIndexCalculator


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


class SocialSentimentCollector:
    """Generate sentiment proxies from price and volume behavior."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        self.config = dict(config or {})
        self.calculator = SocialIndexCalculator()

    def get_xueqiu_hot(self, symbol: str, market_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        bullish_ratio = self._bullish_ratio(market_snapshot, style_bias=0.03)
        mention_growth = self._mention_growth(market_snapshot, scale=0.9)
        engagement = self._engagement_zscore(market_snapshot, scale=1.0)
        return {
            "symbol": symbol,
            "channel": "xueqiu",
            "bullish_ratio": bullish_ratio,
            "mention_growth": mention_growth,
            "engagement_zscore": engagement,
        }

    def get_eastmoney_sentiment(self, symbol: str, market_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        bullish_ratio = self._bullish_ratio(market_snapshot, style_bias=-0.02)
        mention_growth = self._mention_growth(market_snapshot, scale=1.1)
        engagement = self._engagement_zscore(market_snapshot, scale=1.2)
        return {
            "symbol": symbol,
            "channel": "eastmoney",
            "bullish_ratio": bullish_ratio,
            "mention_growth": mention_growth,
            "engagement_zscore": engagement,
        }

    def get_reddit_sentiment(self, symbol: str, market_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        bullish_ratio = self._bullish_ratio(market_snapshot, style_bias=0.0)
        mention_growth = self._mention_growth(market_snapshot, scale=0.7)
        engagement = self._engagement_zscore(market_snapshot, scale=0.8)
        return {
            "symbol": symbol,
            "channel": "reddit",
            "bullish_ratio": bullish_ratio,
            "mention_growth": mention_growth,
            "engagement_zscore": engagement,
        }

    def collect(self, symbol: str, market_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        """Combine channel proxies into one aggregate sentiment snapshot."""
        channels = [
            self.get_xueqiu_hot(symbol, market_snapshot),
            self.get_eastmoney_sentiment(symbol, market_snapshot),
            self.get_reddit_sentiment(symbol, market_snapshot),
        ]
        aggregate_raw = {
            "bullish_ratio": sum(item["bullish_ratio"] for item in channels) / len(channels),
            "mention_growth": sum(item["mention_growth"] for item in channels) / len(channels),
            "engagement_zscore": sum(item["engagement_zscore"] for item in channels) / len(channels),
        }
        aggregate = self.calculator.compute(aggregate_raw)
        aggregate["interpretation"] = self._interpretation(aggregate["sentiment_index"], aggregate["signal"])
        aggregate["method"] = "proxy"
        return {
            "symbol": symbol,
            "channels": channels,
            "aggregate": aggregate,
        }

    def _bullish_ratio(self, market_snapshot: Mapping[str, Any], style_bias: float = 0.0) -> float:
        score = (
            float(market_snapshot.get("return_20d", 0.0)) * 1.6
            + float(market_snapshot.get("return_5d", 0.0)) * 1.0
            + float(market_snapshot.get("return_1d", 0.0)) * 0.6
            + (0.10 if market_snapshot.get("trend") == "多头" else -0.10 if market_snapshot.get("trend") == "空头" else 0.0)
            + style_bias
        )
        return _clamp(0.5 + score, 0.05, 0.95)

    def _mention_growth(self, market_snapshot: Mapping[str, Any], scale: float = 1.0) -> float:
        score = (
            (float(market_snapshot.get("volume_ratio", 1.0)) - 1.0) * 0.9
            + abs(float(market_snapshot.get("return_1d", 0.0))) * 6.0
            + abs(float(market_snapshot.get("return_5d", 0.0))) * 2.0
        )
        return _clamp(score * scale, -1.0, 1.0)

    def _engagement_zscore(self, market_snapshot: Mapping[str, Any], scale: float = 1.0) -> float:
        score = (
            (float(market_snapshot.get("volume_ratio", 1.0)) - 1.0) * 1.8
            + abs(float(market_snapshot.get("return_1d", 0.0))) * 10.0
            + abs(float(market_snapshot.get("return_20d", 0.0))) * 2.0
        )
        return _clamp(score * scale, -3.0, 3.0)

    def _interpretation(self, sentiment_index: float, signal: str) -> str:
        if signal == "contrarian_bearish":
            return f"情绪指数 {sentiment_index:.1f}，讨论热度偏高，需防拥挤交易。"
        if signal == "contrarian_bullish":
            return f"情绪指数 {sentiment_index:.1f}，市场情绪较冷，留意超跌修复。"
        return f"情绪指数 {sentiment_index:.1f}，当前未出现极端一致预期。"
