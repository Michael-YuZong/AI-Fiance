"""Tests for the technical indicator engine."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame


def _sample_price_frame(rows: int = 120) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    base = np.linspace(10, 30, rows)
    return pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.3,
            "high": base + 0.5,
            "low": base - 0.8,
            "close": base,
            "volume": np.linspace(1000, 3000, rows),
        }
    )


def test_normalize_ohlcv_frame_accepts_chinese_columns():
    frame = pd.DataFrame(
        {
            "日期": pd.date_range("2025-01-01", periods=35, freq="D"),
            "开盘": np.linspace(9, 12, 35),
            "最高": np.linspace(10, 13, 35),
            "最低": np.linspace(8, 11, 35),
            "收盘": np.linspace(9.5, 12.5, 35),
            "成交量": np.linspace(100, 500, 35),
        }
    )
    normalized = normalize_ohlcv_frame(frame)
    assert list(normalized.columns) == ["date", "open", "high", "low", "close", "volume", "amount"]
    assert len(normalized) == 35


def test_technical_analyzer_generates_bullish_ma_signal():
    analyzer = TechnicalAnalyzer(_sample_price_frame())
    scorecard = analyzer.generate_scorecard()
    assert scorecard["ma_system"]["signal"] == "bullish"
    assert scorecard["macd"]["signal"] in {"bullish", "bearish"}
    assert scorecard["kdj"]["signal"] in {"bullish", "bearish", "neutral"}
    assert scorecard["obv"]["signal"] in {"bullish", "bearish", "neutral"}
    assert scorecard["fibonacci"]["nearest_level"] in {"0.236", "0.382", "0.500", "0.618", "0.786"}
    assert 0 <= scorecard["fibonacci"]["position_pct"] <= 1.2
    assert "candlestick" in scorecard
