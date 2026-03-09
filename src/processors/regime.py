"""Macro regime detection."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd


class RegimeDetector:
    """宏观体制识别与历史相似期匹配。"""

    def __init__(self, macro_data: Dict[str, object]):
        self.data = macro_data

    def detect_regime(self) -> Dict[str, object]:
        scores = {
            "recovery": 0,
            "overheating": 0,
            "stagflation": 0,
            "deflation": 0,
        }
        if self.data.get("pmi", 0) > 50 and self.data.get("pmi_trend") == "rising":
            scores["recovery"] += 2
        if self.data.get("cpi", 0) < 3 and self.data.get("cpi_trend") != "rising":
            scores["recovery"] += 1
        if self.data.get("policy_stance") == "easing":
            scores["recovery"] += 1
            scores["deflation"] += 1
        if self.data.get("credit_impulse") == "expanding":
            scores["recovery"] += 2
        if self.data.get("pmi", 0) > 52 and self.data.get("cpi_trend") == "rising":
            scores["overheating"] += 2
        if self.data.get("pmi", 0) < 50 and self.data.get("cpi", 0) > 2.5:
            scores["stagflation"] += 2
        if self.data.get("pmi", 0) < 50 and self.data.get("cpi_trend") == "falling":
            scores["deflation"] += 2

        best = max(scores, key=scores.get)
        return {
            "current_regime": best,
            "confidence_scores": scores,
            "preferred_assets": self._get_preferred_assets(best),
        }

    def _get_preferred_assets(self, regime: str) -> List[str]:
        mapping = {
            "recovery": ["成长股", "顺周期", "港股科技", "铜"],
            "overheating": ["商品", "资源股", "通胀受益", "短债"],
            "stagflation": ["黄金", "现金", "公用事业", "防御"],
            "deflation": ["长债", "黄金", "防御板块", "高股息"],
        }
        return mapping.get(regime, [])

    def find_historical_analog(self, history_db: pd.DataFrame) -> Dict[str, object]:
        if history_db.empty:
            return {}
        return history_db.iloc[0].to_dict()
