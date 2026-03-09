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
        reasoning: Dict[str, List[str]] = {key: [] for key in scores}
        if self.data.get("pmi", 0) > 50 and self.data.get("pmi_trend") == "rising":
            scores["recovery"] += 2
            reasoning["recovery"].append("PMI 站上 50 且处于回升趋势。")
        if self.data.get("cpi", 0) < 3 and self.data.get("cpi_trend") != "rising":
            scores["recovery"] += 1
            reasoning["recovery"].append("CPI 压力可控，没有明显走高。")
        if self.data.get("policy_stance") == "easing":
            scores["recovery"] += 1
            scores["deflation"] += 1
            reasoning["recovery"].append("政策偏宽松。")
            reasoning["deflation"].append("政策偏宽松，说明需求端仍需支持。")
        if self.data.get("credit_impulse") == "expanding":
            scores["recovery"] += 2
            reasoning["recovery"].append("信用脉冲处于扩张。")
        if self.data.get("pmi", 0) > 52 and self.data.get("cpi_trend") == "rising":
            scores["overheating"] += 2
            reasoning["overheating"].append("PMI 高位且 CPI 继续上行。")
        if self.data.get("pmi", 0) < 50 and self.data.get("cpi", 0) > 2.5:
            scores["stagflation"] += 2
            reasoning["stagflation"].append("增长偏弱但价格压力仍高。")
        if self.data.get("pmi", 0) < 50 and self.data.get("dxy_state") == "strengthening":
            scores["stagflation"] += 1
            reasoning["stagflation"].append("PMI 低于 50 且美元偏强，说明增长承压、外部流动性偏紧。")
        if self.data.get("policy_stance") == "dilemma":
            scores["stagflation"] += 1
            reasoning["stagflation"].append("政策处在两难区间，宽松和稳汇率约束并存。")
        if self.data.get("pmi", 0) < 50 and self.data.get("cpi_trend") == "falling":
            scores["deflation"] += 2
            reasoning["deflation"].append("PMI 低于 50 且 CPI 走弱。")
        if self.data.get("dxy_state") == "strengthening":
            scores["stagflation"] += 1
            reasoning["stagflation"].append("美元走强通常对应全球流动性偏紧。")
        if self.data.get("dxy_state") == "weakening":
            scores["recovery"] += 1
            reasoning["recovery"].append("美元走弱，有利于风险偏好回暖。")

        best = max(scores, key=scores.get)
        return {
            "current_regime": best,
            "scores": scores,
            "reasoning": reasoning.get(best, []),
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
        frame = history_db.copy()
        frame = frame[frame["regime"] == self.detect_regime()["current_regime"]]
        if frame.empty:
            frame = history_db.copy()

        def distance(row: pd.Series) -> float:
            score = abs(float(row.get("pmi", self.data.get("pmi", 50.0))) - float(self.data.get("pmi", 50.0)))
            score += abs(float(row.get("cpi_monthly", self.data.get("cpi", 0.0))) - float(self.data.get("cpi", 0.0)))
            score += 0 if row.get("policy_stance") == self.data.get("policy_stance") else 1
            score += 0 if row.get("credit_impulse") == self.data.get("credit_impulse") else 1
            score += 0 if row.get("dxy_state") == self.data.get("dxy_state") else 0.5
            return score

        frame["distance"] = frame.apply(distance, axis=1)
        best = frame.sort_values("distance").iloc[0]
        return best.to_dict()
