"""Portfolio risk analytics."""

from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd
from scipy import stats


class RiskAnalyzer:
    """组合风险分析。"""

    def __init__(self, returns_df: pd.DataFrame, weights: Dict[str, float]):
        self.returns = returns_df.fillna(0.0)
        self.weights = weights
        self.w = np.array([weights.get(column, 0.0) for column in self.returns.columns])
        self.portfolio_returns = (self.returns * self.w).sum(axis=1)

    def correlation_matrix(self) -> pd.DataFrame:
        return self.returns.corr()

    def concentration_alert(self, threshold: float = 0.8) -> List[Dict[str, object]]:
        corr = self.correlation_matrix()
        alerts: List[Dict[str, object]] = []
        columns = list(corr.columns)
        for left in range(len(columns)):
            for right in range(left + 1, len(columns)):
                corr_value = corr.iloc[left, right]
                if abs(corr_value) > threshold:
                    alerts.append(
                        {
                            "pair": (columns[left], columns[right]),
                            "correlation": float(corr_value),
                            "warning": (
                                f"{columns[left]} 和 {columns[right]} 高度正相关 "
                                f"({corr_value:.2f})，分散效果有限。"
                            ),
                        }
                    )
        return alerts

    def var(self, confidence: float = 0.95, method: str = "historical") -> Dict[str, object]:
        if method == "historical":
            var_value = -np.percentile(self.portfolio_returns, (1 - confidence) * 100)
        else:
            mean = self.portfolio_returns.mean()
            sigma = self.portfolio_returns.std()
            var_value = -(mean + stats.norm.ppf(1 - confidence) * sigma)
        return {
            "VaR": float(var_value),
            "confidence": confidence,
            "method": method,
            "interpretation": f"在 {confidence * 100:.0f}% 置信水平下，单日损失大致不超过 {var_value * 100:.2f}%。",
        }

    def cvar(self, confidence: float = 0.95) -> Dict[str, object]:
        var_value = self.var(confidence)["VaR"]
        tail_losses = self.portfolio_returns[self.portfolio_returns < -var_value]
        cvar_value = -tail_losses.mean() if len(tail_losses) > 0 else var_value
        return {
            "CVaR": float(cvar_value),
            "confidence": confidence,
            "interpretation": f"在超过 VaR 的尾部场景中，平均损失约为 {cvar_value * 100:.2f}%。",
        }

    def max_drawdown(self) -> Dict[str, object]:
        cumulative = (1 + self.portfolio_returns).cumprod()
        peak = cumulative.expanding().max()
        drawdown = (cumulative - peak) / peak
        max_dd = float(drawdown.min())
        max_dd_end = drawdown.idxmin()
        return {
            "max_drawdown": max_dd,
            "max_dd_date": str(max_dd_end),
            "interpretation": f"历史最大回撤为 {max_dd * 100:.2f}%。",
        }

    def beta(self, benchmark_returns: pd.Series) -> Dict[str, object]:
        aligned = pd.concat([self.portfolio_returns, benchmark_returns], axis=1).dropna()
        if aligned.empty or aligned.iloc[:, 1].var() == 0:
            beta_value = 0.0
        else:
            beta_value = np.cov(aligned.iloc[:, 0], aligned.iloc[:, 1])[0][1] / aligned.iloc[:, 1].var()
        return {
            "beta": float(beta_value),
            "interpretation": f"组合 Beta 为 {beta_value:.2f}。",
        }

    def stress_test(self, scenario: Dict[str, object]) -> Dict[str, object]:
        shocks = scenario.get("shocks", {})
        impact = sum(self.weights.get(asset, 0.0) * shock for asset, shock in shocks.items())
        return {
            "scenario": scenario.get("name", "unknown"),
            "portfolio_impact": float(impact),
            "interpretation": f"在场景 '{scenario.get('name', 'unknown')}' 下，组合预计变动 {impact * 100:+.2f}%。",
            "asset_breakdown": {
                asset: {
                    "weight": self.weights.get(asset, 0.0),
                    "shock": shock,
                    "contribution": self.weights.get(asset, 0.0) * shock,
                }
                for asset, shock in shocks.items()
            },
        }

    def generate_risk_report(self, benchmark_returns: pd.Series) -> Dict[str, object]:
        return {
            "correlation": self.correlation_matrix().to_dict(),
            "concentration_alerts": self.concentration_alert(),
            "var_95": self.var(0.95),
            "var_99": self.var(0.99),
            "cvar_95": self.cvar(0.95),
            "max_drawdown": self.max_drawdown(),
            "beta": self.beta(benchmark_returns),
            "sharpe": self._sharpe_ratio(),
        }

    def _sharpe_ratio(self, risk_free_rate: float = 0.02) -> Dict[str, float]:
        annual_return = float(self.portfolio_returns.mean() * 252)
        annual_vol = float(self.portfolio_returns.std() * np.sqrt(252))
        sharpe = 0.0 if annual_vol == 0 else (annual_return - risk_free_rate) / annual_vol
        return {
            "sharpe": float(sharpe),
            "annual_return": annual_return,
            "annual_vol": annual_vol,
        }
