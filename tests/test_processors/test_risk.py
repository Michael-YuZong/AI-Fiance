"""Tests for risk analytics."""

from __future__ import annotations

import pandas as pd

from src.processors.risk import RiskAnalyzer


def test_risk_report_contains_core_sections():
    returns = pd.DataFrame(
        {
            "QQQM": [0.01, -0.02, 0.015, 0.005, -0.01],
            "GLD": [0.002, 0.004, -0.001, 0.003, 0.002],
        }
    )
    weights = {"QQQM": 0.6, "GLD": 0.4}
    benchmark = pd.Series([0.008, -0.01, 0.01, 0.002, -0.004])
    report = RiskAnalyzer(returns, weights).generate_risk_report(benchmark)
    assert "var_95" in report
    assert "beta" in report
    assert "correlation" in report
    assert "rolling_volatility" in report
