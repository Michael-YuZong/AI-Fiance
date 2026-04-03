from __future__ import annotations

import sys

import pandas as pd

from src.commands import risk as risk_module


def test_risk_report_surfaces_portfolio_linkage(monkeypatch, capsys) -> None:
    monkeypatch.setattr(risk_module, "load_config", lambda _path=None: {})

    class _FakeContext:
        def __init__(self):
            self.status = {
                "base_currency": "CNY",
                "total_value": 100.0,
                "holdings": [
                    {"symbol": "561380", "name": "电网ETF", "sector": "电网", "region": "CN", "weight": 0.28},
                    {"symbol": "510300", "name": "沪深300ETF", "sector": "宽基", "region": "CN", "weight": 0.42},
                    {"symbol": "512480", "name": "创新药ETF", "sector": "医药", "region": "CN", "weight": 0.30},
                ],
                "region_exposure": {"CN": 1.0},
                "sector_exposure": {"电网": 0.28, "宽基": 0.42, "医药": 0.30},
            }
            self.weights = {"561380": 0.28, "510300": 0.42, "512480": 0.30}
            self.returns_df = pd.DataFrame({"561380": [0.01, -0.01], "510300": [0.0, 0.01], "512480": [0.02, 0.0]})
            self.benchmark_returns = pd.Series([0.0, 0.0])
            self.coverage_notes = ["样本覆盖正常。"]

    class _FakeRiskAnalyzer:
        def __init__(self, returns_df, weights):  # noqa: ARG002
            pass

        def generate_risk_report(self, benchmark_returns):  # noqa: ARG002
            return {
                "max_drawdown": {"interpretation": "最大回撤可控。"},
                "var_95": {"interpretation": "VaR 95 处于可接受区间。"},
                "var_99": {"interpretation": "VaR 99 仍需留意。"},
                "cvar_95": {"interpretation": "CVaR 95 未触发硬约束。"},
                "rolling_volatility": {"vol_20d": 0.12, "vol_60d": 0.10},
                "sharpe": {"sharpe": 1.1, "annual_return": 0.18, "annual_vol": 0.12},
                "beta": {"beta": 1.05, "interpretation": "Beta 适中。"},
                "correlation": {},
                "concentration_alerts": [{"warning": "561380 和 510300 高度相关 (0.92)，分散效果有限。"}],
            }

    monkeypatch.setattr(risk_module, "PortfolioRepository", lambda: object())
    monkeypatch.setattr(risk_module, "build_portfolio_risk_context", lambda config, repo=None, period="3y": _FakeContext())
    monkeypatch.setattr(risk_module, "RiskAnalyzer", _FakeRiskAnalyzer)
    monkeypatch.setattr(sys, "argv", ["risk", "report"])

    risk_module.main()

    captured = capsys.readouterr()
    assert "# 组合风险报告" in captured.out
    assert "组合联动:" in captured.out
    assert "重复度" in captured.out
    assert "风格与方向:" in captured.out
    assert "组合优先级:" in captured.out
    assert "561380 和 510300 高度相关" in captured.out
