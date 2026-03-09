"""Macro asset monitors for briefing generation."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from .base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_yaml

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


class MarketMonitorCollector(BaseCollector):
    """Collect a small set of macro-sensitive asset monitors."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="MarketMonitorCollector")
        self.monitors_path = resolve_project_path(
            self.config.get("market_monitors_file", "config/market_monitors.yaml")
        )

    def collect(self) -> List[Dict[str, Any]]:
        if yf is None:
            return []
        payload = load_yaml(self.monitors_path, default={"monitors": []}) or {"monitors": []}
        rows: List[Dict[str, Any]] = []
        for item in payload.get("monitors", []) or []:
            symbol = str(item.get("symbol", "")).strip()
            if not symbol:
                continue
            try:
                frame = self.cached_call(
                    f"market_monitor:{symbol}",
                    yf.Ticker(symbol).history,
                    period="1mo",
                    interval="1d",
                    auto_adjust=False,
                    ttl_hours=2,
                )
            except Exception:
                continue
            close = self._close_series(frame)
            if close.empty:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "name": str(item.get("name", symbol)),
                    "category": str(item.get("category", "")),
                    "latest": float(close.iloc[-1]),
                    "return_1d": self._period_return(close, 1),
                    "return_5d": self._period_return(close, 5),
                }
            )
        return rows

    def _close_series(self, frame: pd.DataFrame) -> pd.Series:
        if frame.empty:
            return pd.Series(dtype=float)
        return pd.to_numeric(frame["Close"], errors="coerce").dropna()

    def _period_return(self, close: pd.Series, days: int) -> float:
        if len(close) <= days:
            return 0.0
        return float(close.iloc[-1] / close.iloc[-(days + 1)] - 1)
