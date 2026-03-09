"""Tests for the simple backtester."""

from __future__ import annotations

import pandas as pd

from src.processors.backtester import SimpleBacktester


def test_backtester_returns_trade_stats():
    prices = pd.DataFrame(
        {"close": [10, 11, 12, 13, 14, 15]},
        index=pd.date_range("2025-01-01", periods=6, freq="D"),
    )
    backtester = SimpleBacktester(prices)
    result = backtester.run(
        entry_rule=lambda _df, i: i == 1,
        exit_rule=lambda _df, i: i == 4,
        initial_capital=1000,
    )
    assert result["total_trades"] == 1
    assert "warning" in result
