"""Cross-market analytics."""

from __future__ import annotations


class CrossMarketAnalyzer:
    """跨市场联动因子计算。"""

    def copper_gold_ratio(self, copper_price: float, gold_price: float) -> dict:
        ratio = copper_price / gold_price
        signal = "risk_on" if ratio > 0.22 else "risk_off" if ratio < 0.16 else "neutral"
        return {"ratio": ratio, "signal": signal}

    def ah_premium(self, a_price: float, h_price: float, exchange_rate: float) -> dict:
        premium = (a_price / (h_price * exchange_rate) - 1) * 100
        signal = "hk_cheap" if premium > 30 else "fairly_valued" if premium < 10 else "moderate"
        return {"premium_pct": premium, "signal": signal}

    def equity_bond_yield_ratio(self, pe_ratio: float, bond_yield_10y: float) -> dict:
        earnings_yield = 1 / pe_ratio * 100
        ratio = earnings_yield / bond_yield_10y
        signal = "equity_attractive" if ratio > 2.0 else "equity_expensive" if ratio < 1.0 else "neutral"
        return {"ratio": ratio, "signal": signal}

    def yield_curve(self, yield_10y: float, yield_2y: float) -> dict:
        spread = yield_10y - yield_2y
        signal = "inverted" if spread < 0 else "steepening" if spread > 0.5 else "flat"
        return {"spread_bps": spread * 100, "signal": signal}
