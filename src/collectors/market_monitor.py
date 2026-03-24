"""Macro asset monitors for briefing generation."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
import time
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
        self.max_stale_hours = float(self.config.get("market_monitor_max_stale_hours", 12))
        self.fetch_timeout_seconds = float(self.config.get("market_monitor_fetch_timeout_seconds", 8))

    def collect(self) -> List[Dict[str, Any]]:
        if yf is None:
            return []
        payload = load_yaml(self.monitors_path, default={"monitors": []}) or {"monitors": []}
        rows: List[Dict[str, Any]] = []
        for item in payload.get("monitors", []) or []:
            symbol = str(item.get("symbol", "")).strip()
            if not symbol:
                continue
            cache_key = f"market_monitor:v2:{symbol}"
            frame = self._load_cache(cache_key, ttl_hours=2)
            stale_age_hours = None
            source_status = "fresh_cache"
            if frame is None:
                stale_frame = self._load_cache(cache_key, ttl_hours=2, allow_stale=True)
                stale_age_hours = self._stale_cache_age_hours(cache_key)
                refresh_issue = "实时刷新失败。"
                try:
                    self.rate_limiter.wait()
                    frame = self._history_with_timeout(symbol)
                    if frame is None or getattr(frame, "empty", False):
                        raise ValueError(f"{self.name} returned empty result for {cache_key}")
                    self._save_cache(cache_key, frame)
                    source_status = "live"
                except TimeoutError:
                    refresh_issue = f"实时刷新超时（>{self.fetch_timeout_seconds:.0f}s）。"
                    if (
                        stale_frame is None
                        or stale_age_hours is None
                        or stale_age_hours > self.max_stale_hours
                    ):
                        continue
                    frame = stale_frame
                    source_status = "stale_cache"
                except Exception:
                    if (
                        stale_frame is None
                        or stale_age_hours is None
                        or stale_age_hours > self.max_stale_hours
                    ):
                        continue
                    frame = stale_frame
                    source_status = "stale_cache"
            close = self._close_series(frame)
            if close.empty:
                continue
            row = {
                "symbol": symbol,
                "name": str(item.get("name", symbol)),
                "category": str(item.get("category", "")),
                "latest": float(close.iloc[-1]),
                "return_1d": self._period_return(close, 1),
                "return_5d": self._period_return(close, 5),
                "return_20d": self._period_return(close, 20),
                "source_status": source_status,
            }
            if close.index.size:
                latest_index = close.index[-1]
                row["as_of"] = str(latest_index.date()) if hasattr(latest_index, "date") else str(latest_index)
            if source_status == "stale_cache" and stale_age_hours is not None:
                row["stale_age_hours"] = round(float(stale_age_hours), 1)
                row["data_warning"] = (
                    f"{refresh_issue.rstrip('。')}，当前使用约 {stale_age_hours:.1f} 小时前的缓存；"
                    "不应把它当成严格实时数值。"
                )
            rows.append(row)
        return rows

    def _history_with_timeout(self, symbol: str) -> pd.DataFrame:
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(
            self._execute_fetcher,
            yf.Ticker(symbol).history,
            period="3mo",
            interval="1d",
            auto_adjust=False,
        )
        try:
            return future.result(timeout=self.fetch_timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"{self.name} refresh timeout for {symbol}") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _stale_cache_age_hours(self, cache_key: str) -> Optional[float]:
        cache_path = self._cache_path(cache_key)
        if not cache_path.exists():
            return None
        return max(0.0, (time.time() - cache_path.stat().st_mtime) / 3600)

    def _close_series(self, frame: pd.DataFrame) -> pd.Series:
        if frame.empty:
            return pd.Series(dtype=float)
        return pd.to_numeric(frame["Close"], errors="coerce").dropna()

    def _period_return(self, close: pd.Series, days: int) -> float:
        if len(close) <= days:
            return 0.0
        return float(close.iloc[-1] / close.iloc[-(days + 1)] - 1)
