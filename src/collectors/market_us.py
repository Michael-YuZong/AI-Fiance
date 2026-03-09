"""US market data collector."""

from __future__ import annotations

import pandas as pd

from .base import BaseCollector

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


class USMarketCollector(BaseCollector):
    """美股行情采集。"""

    def get_history(self, symbol: str, period: str = "3y", interval: str = "1d") -> pd.DataFrame:
        if yf is None:
            raise RuntimeError("yfinance is not installed")
        return self.cached_call(
            f"us_market:history:{symbol}:{period}:{interval}",
            yf.Ticker(symbol).history,
            period=period,
            interval=interval,
            auto_adjust=False,
        )
