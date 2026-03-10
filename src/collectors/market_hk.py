"""Hong Kong market data collector."""

from __future__ import annotations

from typing import Optional

import pandas as pd

from .base import BaseCollector

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


class HongKongMarketCollector(BaseCollector):
    """港股与港股指数行情。"""

    SYMBOL_MAP = {
        "HSTECH": "^HSTECH",
    }

    def _normalize_symbol(self, symbol: str) -> str:
        if symbol in self.SYMBOL_MAP:
            return self.SYMBOL_MAP[symbol]
        if symbol.isdigit() and len(symbol) == 5:
            return f"{symbol}.HK"
        # yfinance uses 4-digit HK codes (e.g. 0700.HK, not 00700.HK)
        if symbol.upper().endswith(".HK"):
            code = symbol[:-3].lstrip("0") or "0"
            return f"{code.zfill(4)}.HK"
        return symbol

    def get_history(self, symbol: str, period: str = "3y", interval: str = "1d") -> pd.DataFrame:
        if yf is None:
            raise RuntimeError("yfinance is not installed")
        ticker = self._normalize_symbol(symbol)
        return self.cached_call(
            f"hk_market:history:{ticker}:{period}:{interval}",
            yf.Ticker(ticker).history,
            period=period,
            interval=interval,
            auto_adjust=False,
        )
