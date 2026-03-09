"""Tests for local chart rendering."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.output.analysis_charts import AnalysisChartRenderer


def _sample_analysis() -> dict:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [1.0 + i * 0.004 for i in range(120)],
            "high": [1.02 + i * 0.004 for i in range(120)],
            "low": [0.98 + i * 0.004 for i in range(120)],
            "close": [1.0 + i * 0.004 for i in range(120)],
            "volume": [8_000_000 + i * 1_000 for i in range(120)],
            "amount": [16_000_000 + i * 2_000 for i in range(120)],
        }
    )
    benchmark = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [0.95 + i * 0.002 for i in range(120)],
            "high": [0.97 + i * 0.002 for i in range(120)],
            "low": [0.93 + i * 0.002 for i in range(120)],
            "close": [0.95 + i * 0.002 for i in range(120)],
            "volume": [7_000_000 + i * 800 for i in range(120)],
            "amount": [14_000_000 + i * 1_600 for i in range(120)],
        }
    )
    return {
        "symbol": "561380",
        "name": "电网ETF",
        "generated_at": "2026-03-09 08:00:00",
        "rating": {"stars": "⭐⭐⭐", "label": "较强机会"},
        "history": history,
        "benchmark_name": "沪深300ETF",
        "benchmark_history": benchmark,
        "metrics": {"return_5d": 0.034, "return_20d": 0.087},
        "narrative": {
            "phase": {"label": "强势整理"},
            "judgment": {"direction": "中性偏多", "odds": "中"},
            "summary_lines": ["核心逻辑仍在。", "短期更像强势整理。"],
        },
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "technical_raw": {
            "rsi": {"RSI": 61.4},
            "dmi": {"ADX": 29.7},
            "ma_system": {"mas": {"MA20": 1.34, "MA60": 1.22}},
            "fibonacci": {"levels": {"0.500": 1.27, "0.618": 1.31}},
        },
        "dimensions": {
            "technical": {"score": 82, "max_score": 100},
            "fundamental": {"score": 58, "max_score": 100},
            "catalyst": {"score": 61, "max_score": 100},
            "relative_strength": {"score": 67, "max_score": 100},
            "chips": {"score": 44, "max_score": 100},
            "risk": {"score": 53, "max_score": 100},
            "seasonality": {"score": 38, "max_score": 100},
            "macro": {"score": 30, "max_score": 40},
        },
    }


def test_analysis_chart_renderer_outputs_dashboard(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    visuals = renderer.render(_sample_analysis())
    assert "dashboard" in visuals
    assert "windows" in visuals
    assert "indicators" in visuals
    for key in ("dashboard", "windows", "indicators"):
        image = Path(visuals[key])
        assert image.exists()
        assert image.stat().st_size > 0
