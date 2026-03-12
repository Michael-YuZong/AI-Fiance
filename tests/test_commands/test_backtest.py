"""Tests for backtest command helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.commands.backtest import _prepare_rule_frame
from src.processors.technical import TechnicalAnalyzer


def _sample_history(rows: int = 120) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=rows, freq="B")
    base = np.linspace(20, 36, rows)
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.2,
            "high": base + 0.6,
            "low": base - 0.8,
            "close": base,
            "volume": np.linspace(1_000_000, 1_500_000, rows),
            "amount": np.linspace(80_000_000, 120_000_000, rows),
        }
    )
    frame.loc[::7, "close"] -= 0.9
    frame.loc[::5, "close"] += 1.1
    return frame


def test_prepare_rule_frame_reuses_technical_analyzer_series():
    history = _sample_history()
    technical_config = {"macd": {"fast": 12, "slow": 26, "signal": 9}, "rsi": {"period": 14}}
    prepared = _prepare_rule_frame(history, technical_config)
    analyzer = TechnicalAnalyzer(history)
    indicators = analyzer.indicator_series(technical_config)

    assert float(prepared["dif"].iloc[-1]) == pytest.approx(float(indicators["macd_dif"].iloc[-1]), rel=1e-6)
    assert float(prepared["dea"].iloc[-1]) == pytest.approx(float(indicators["macd_dea"].iloc[-1]), rel=1e-6)
    assert float(prepared["rsi"].iloc[-1]) == pytest.approx(float(indicators["rsi"].iloc[-1]), rel=1e-6)
