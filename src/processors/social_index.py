"""Social sentiment index calculation."""

from __future__ import annotations

from typing import Dict


class SocialIndexCalculator:
    """社媒情绪指数计算。"""

    def compute(self, raw_data: Dict[str, float]) -> Dict[str, object]:
        bullish_ratio = float(raw_data.get("bullish_ratio", 0.5))
        mention_growth = float(raw_data.get("mention_growth", 0.0))
        engagement_zscore = float(raw_data.get("engagement_zscore", 0.0))
        index = max(0.0, min(100.0, bullish_ratio * 60 + mention_growth * 20 + (engagement_zscore + 3) * 5))
        signal = "contrarian_bearish" if index > 80 else "contrarian_bullish" if index < 20 else "neutral"
        return {
            "sentiment_index": index,
            "signal": signal,
        }
