"""Tests for Phase 4 risk support helpers."""

from __future__ import annotations

import pandas as pd

from src.processors.risk_support import resolve_stress_scenario, trim_history_period


def test_resolve_stress_scenario_maps_thematic_aliases():
    scenario = {
        "name": "测试场景",
        "description": "科技和黄金同时受冲击。",
        "shocks": {"TECH": -0.08, "GLD": 0.05},
    }
    holdings = [
        {"symbol": "QQQM", "asset_type": "us", "region": "US", "sector": "科技"},
        {"symbol": "AU0", "asset_type": "futures", "region": "CN", "sector": "黄金"},
    ]
    resolved = resolve_stress_scenario(scenario, holdings, config={})
    assert resolved["shocks"]["QQQM"] == -0.08
    assert resolved["shocks"]["AU0"] == 0.05


def test_trim_history_period_keeps_recent_rows():
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=2000, freq="D"),
            "open": range(2000),
            "high": range(1, 2001),
            "low": range(2000),
            "close": range(1, 2001),
            "volume": [1000] * 2000,
        }
    )
    trimmed = trim_history_period(frame, "1y")
    assert len(trimmed) < len(frame)
    assert trimmed["date"].min() >= frame["date"].max() - pd.DateOffset(years=1, days=3)
