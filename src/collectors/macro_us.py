"""US macro data collector."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from .base import BaseCollector

try:
    from fredapi import Fred
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    Fred = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    yf = None


class USMacroCollector(BaseCollector):
    """美国宏观经济数据采集。"""

    def __init__(self, config: Optional[dict] = None, api_key: Optional[str] = None) -> None:
        super().__init__(config=config)
        self.api_key = api_key or (self.config.get("api_keys", {}) or {}).get("fred")
        self._fred_client = None

    def _fred(self):
        if Fred is None:
            raise RuntimeError("fredapi is not installed")
        if not self.api_key or str(self.api_key).startswith("YOUR_"):
            raise RuntimeError("FRED API key is not configured")
        if self._fred_client is None:
            self._fred_client = Fred(api_key=self.api_key)
        return self._fred_client

    def _series(self, series_id: str) -> pd.Series:
        return self.cached_call(f"us_macro:fred:{series_id}", self._fred().get_series, series_id)

    def get_fed_funds_rate(self) -> pd.Series:
        return self._series("FEDFUNDS")

    def get_us_cpi(self) -> pd.Series:
        return self._series("CPIAUCSL")

    def get_us_pmi(self) -> pd.Series:
        return self._series("NAPM")

    def get_treasury_10y(self) -> pd.Series:
        return self._series("DGS10")

    def get_treasury_2y(self) -> pd.Series:
        return self._series("DGS2")

    def _ticker_history(self, symbol: str, period: str = "1y") -> pd.DataFrame:
        if yf is None:
            raise RuntimeError("yfinance is not installed")
        ticker = yf.Ticker(symbol)
        return ticker.history(period=period, auto_adjust=False)

    def get_vix(self) -> pd.DataFrame:
        return self.cached_call("us_macro:vix", self._ticker_history, "^VIX")

    def get_dxy(self) -> pd.DataFrame:
        return self.cached_call("us_macro:dxy", self._ticker_history, "DX-Y.NYB")
