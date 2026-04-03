"""Macro regime detection."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd


class RegimeDetector:
    """宏观体制识别与历史相似期匹配。"""

    def __init__(self, macro_data: Dict[str, object]):
        self.data = macro_data

    def detect_regime(self) -> Dict[str, object]:
        basis_lines = self._basis_lines()
        explicit = self._explicit_regime()
        if explicit is not None:
            regime, reasons = explicit
            return {
                "current_regime": regime,
                "scores": {key: 1 if key == regime else 0 for key in ("recovery", "overheating", "stagflation", "deflation")},
                "reasoning": reasons,
                "basis_lines": basis_lines,
                "preferred_assets": self._get_preferred_assets(regime),
            }

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
        if self.data.get("demand_state") == "improving":
            scores["recovery"] += 1
            reasoning["recovery"].append("新订单和生产分项在改善，景气领先指标偏正面。")
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
        if self.data.get("ppi_trend") == "rising" and self.data.get("ppi", -99) > -1.0:
            scores["recovery"] += 1
            reasoning["recovery"].append("PPI 从底部回升，价格链条和工业利润预期在修复。")
        if self.data.get("pmi", 0) > 52 and self.data.get("cpi_trend") == "rising":
            scores["overheating"] += 2
            reasoning["overheating"].append("PMI 高位且 CPI 继续上行。")
        if self.data.get("ppi", 0) > 1.0 and self.data.get("ppi_trend") == "rising":
            scores["overheating"] += 1
            reasoning["overheating"].append("PPI 已明显转正并继续上行，价格链条偏热。")
        if self.data.get("pmi", 0) < 50 and self.data.get("cpi", 0) > 2.5:
            scores["stagflation"] += 2
            reasoning["stagflation"].append("增长偏弱但价格压力仍高。")
        if self.data.get("pmi", 0) < 50 and (self.data.get("ppi_trend") == "rising" or self.data.get("ppi", 0) > 0):
            scores["stagflation"] += 1
            reasoning["stagflation"].append("PMI 偏弱但 PPI/上游价格没有同步回落，存在成本端压力。")
        if self.data.get("pmi", 0) < 50 and self.data.get("dxy_state") == "strengthening":
            scores["stagflation"] += 1
            reasoning["stagflation"].append("PMI 低于 50 且美元偏强，说明增长承压、外部流动性偏紧。")
        if self.data.get("policy_stance") == "dilemma":
            scores["stagflation"] += 1
            reasoning["stagflation"].append("政策处在两难区间，宽松和稳汇率约束并存。")
        if self.data.get("pmi", 0) < 50 and self.data.get("cpi_trend") == "falling":
            scores["deflation"] += 2
            reasoning["deflation"].append("PMI 低于 50 且 CPI 走弱。")
        if self.data.get("ppi", 0) < 0 and self.data.get("ppi_trend") == "falling":
            scores["deflation"] += 1
            reasoning["deflation"].append("PPI 继续走弱，说明工业价格链条仍在通缩区间。")
        if self.data.get("credit_impulse") == "contracting":
            scores["deflation"] += 1
            reasoning["deflation"].append("信用脉冲收缩，内需修复力度不足。")
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
            "basis_lines": basis_lines,
            "preferred_assets": self._get_preferred_assets(best),
        }

    def _explicit_regime(self) -> tuple[str, List[str]] | None:
        pmi = float(self.data.get("pmi", 50.0))
        cpi = float(self.data.get("cpi", 0.0))
        ppi = float(self.data.get("ppi", 0.0))
        pmi_trend = str(self.data.get("pmi_trend", "stable"))
        cpi_trend = str(self.data.get("cpi_trend", "stable"))
        ppi_trend = str(self.data.get("ppi_trend", "stable"))
        policy_stance = str(self.data.get("policy_stance", "neutral"))
        credit_impulse = str(self.data.get("credit_impulse", "stable"))
        demand_state = str(self.data.get("demand_state", "stable"))
        m1_m2_spread = float(self.data.get("m1_m2_spread", 0.0))
        oil_5d = float(self.data.get("oil_5d_change", 0.0))
        oil_20d = float(self.data.get("oil_20d_change", 0.0))

        if pmi < 50 and (cpi >= 2.0 or oil_20d >= 0.20 or oil_5d >= 0.15):
            return (
                "stagflation",
                [
                    f"PMI {pmi:.1f} 低于 50，增长端偏弱。",
                    f"CPI {cpi:.1f}% 与油价冲击并存（油价 5 日 {oil_5d * 100:+.1f}% / 20 日 {oil_20d * 100:+.1f}%）。",
                    "增长承压但价格/能源冲击仍强，优先按滞涨背景处理。",
                ],
            )

        if pmi < 50 and cpi < 2.0 and ppi <= 0 and policy_stance == "easing":
            return (
                "deflation",
                [
                    f"PMI {pmi:.1f} 低于 50，需求端仍偏弱。",
                    f"CPI {cpi:.1f}%、PPI {ppi:.1f}% 都处在偏弱区间，价格压力不强。",
                    "政策偏宽松，更接近通缩/偏弱环境而不是过热。",
                ],
            )

        if pmi >= 50 and pmi_trend == "rising" and credit_impulse == "expanding":
            return (
                "recovery",
                [
                    f"PMI {pmi:.1f} 站上 50 且延续回升。",
                    f"信用脉冲扩张，M1-M2 剪刀差约 {m1_m2_spread:+.1f} 个百分点，融资环境在改善。",
                    "新订单/生产等领先指标偏正面，更接近温和复苏。",
                ],
            )

        if pmi >= 50 and demand_state == "improving" and ppi_trend == "rising":
            return (
                "recovery",
                [
                    f"PMI {pmi:.1f} 站上 50，新订单领先分项也在改善。",
                    f"PPI {ppi:.1f}% 呈回升趋势，说明工业利润链条开始修复。",
                    "当前更接近温和复苏，而不是滞涨或通缩。",
                ],
            )

        if pmi >= 52 and cpi_trend == "rising" and ppi >= 0:
            return (
                "overheating",
                [
                    f"PMI {pmi:.1f} 处在高位。",
                    f"CPI 趋势继续上行，PPI {ppi:.1f}% 也不弱，价格压力开始抬头。",
                    "景气与通胀共振，更接近过热环境。",
                ],
            )

        return None

    def _get_preferred_assets(self, regime: str) -> List[str]:
        mapping = {
            "recovery": ["成长股", "顺周期", "港股科技", "铜"],
            "overheating": ["商品", "资源股", "通胀受益", "短债"],
            "stagflation": ["黄金", "现金", "公用事业", "防御"],
            "deflation": ["长债", "黄金", "防御板块", "高股息"],
        }
        return mapping.get(regime, [])

    def _basis_lines(self) -> List[str]:
        lines: List[str] = []
        pmi = self.data.get("pmi")
        if pmi is not None:
            pmi_trend = str(self.data.get("pmi_trend", "stable"))
            demand_state = str(self.data.get("demand_state", "stable"))
            trend_label = {"rising": "回升", "falling": "走弱"}.get(pmi_trend, "持平")
            demand_tail = ""
            if demand_state == "improving":
                demand_tail = "，新订单/生产分项偏改善"
            elif demand_state == "weakening":
                demand_tail = "，新订单/生产分项偏走弱"
            lines.append(f"PMI {float(pmi):.1f}，景气端{trend_label}{demand_tail}。")
        cpi = self.data.get("cpi")
        ppi = self.data.get("ppi")
        if cpi is not None or ppi is not None:
            price_parts: List[str] = []
            if cpi is not None:
                price_parts.append(f"CPI {float(cpi):.1f}%")
            if ppi is not None:
                price_parts.append(f"PPI {float(ppi):.1f}%")
            ppi_trend = str(self.data.get("ppi_trend", "stable"))
            price_tail = {"rising": "，价格链条偏修复", "falling": "，价格链条仍偏弱"}.get(ppi_trend, "")
            lines.append("、".join(price_parts) + price_tail + "。")
        credit_impulse = str(self.data.get("credit_impulse", "")).strip()
        m1_m2_spread = self.data.get("m1_m2_spread")
        if credit_impulse or m1_m2_spread is not None:
            credit_label = {
                "expanding": "扩张",
                "contracting": "收缩",
                "stable": "中性",
            }.get(credit_impulse, credit_impulse or "中性")
            spread_text = ""
            if m1_m2_spread is not None:
                spread_text = f"，M1-M2 剪刀差 {float(m1_m2_spread):+.1f}pct"
            lines.append(f"信用脉冲{credit_label}{spread_text}。")
        dxy_state = str(self.data.get("dxy_state", "")).strip()
        if dxy_state:
            dollar_label = {
                "strengthening": "偏强",
                "weakening": "偏弱",
                "stable": "中性",
            }.get(dxy_state, dxy_state)
            lines.append(f"美元{dollar_label}，外部流动性环境同步受影响。")
        return lines

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
            score += abs(float(row.get("ppi", self.data.get("ppi", 0.0))) - float(self.data.get("ppi", 0.0))) * 0.5
            score += 0 if row.get("policy_stance") == self.data.get("policy_stance") else 1
            score += 0 if row.get("credit_impulse") == self.data.get("credit_impulse") else 1
            score += 0 if row.get("dxy_state") == self.data.get("dxy_state") else 0.5
            return score

        frame["distance"] = frame.apply(distance, axis=1)
        best = frame.sort_values("distance").iloc[0]
        return best.to_dict()
