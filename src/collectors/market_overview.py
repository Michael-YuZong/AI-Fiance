"""High-level market overview collector for briefing generation — Tushare-first."""

from __future__ import annotations

import io
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from .base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_yaml

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


class MarketOverviewCollector(BaseCollector):
    """Collect index-level snapshots for briefing overview tables.

    国内指数行情优先用 Tushare index_daily，海外指数继续用 yfinance。
    """

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="MarketOverviewCollector")
        self.overview_path = resolve_project_path(
            self.config.get("market_overview_file", "config/market_overview.yaml")
        )

    def collect(self) -> Dict[str, Any]:
        payload = load_yaml(self.overview_path, default={}) or {}
        domestic = self._collect_domestic(payload.get("domestic_indices", []) or [])
        breadth = self._collect_breadth()
        global_rows = self._collect_global(payload.get("global_indices", []) or [])
        return {
            "domestic_indices": domestic,
            "breadth": breadth,
            "global_indices": global_rows,
        }

    def _collect_domestic(self, indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """国内指数行情。Tushare index_daily 优先。"""
        # ── Tushare (primary) ──
        ts_rows = self._ts_domestic_indices(indices)
        if ts_rows:
            return ts_rows

        # ── AKShare (fallback) ──
        if ak is None:
            return []
        spot = self.cached_call(
            "market_overview:domestic_spot:v1",
            self._quiet_fetch,
            ak.stock_zh_index_spot_sina,
            ttl_hours=1,
        )
        rows: List[Dict[str, Any]] = []
        for item in indices:
            code = str(item.get("symbol", "")).strip()
            if not code:
                continue
            matched = spot[spot["代码"].astype(str) == code]
            if matched.empty:
                continue
            current = matched.iloc[0]
            prev_close = pd.to_numeric(pd.Series([current.get("昨收")]), errors="coerce").iloc[0]
            open_price = pd.to_numeric(pd.Series([current.get("今开")]), errors="coerce").iloc[0]
            amount = pd.to_numeric(pd.Series([current.get("成交额")]), errors="coerce").iloc[0]
            latest = pd.to_numeric(pd.Series([current.get("最新价")]), errors="coerce").iloc[0]
            change_pct = pd.to_numeric(pd.Series([current.get("涨跌幅")]), errors="coerce").iloc[0] / 100
            rows.append(
                {
                    "name": str(item.get("name", code)),
                    "symbol": code,
                    "latest": float(latest) if pd.notna(latest) else None,
                    "change_pct": float(change_pct) if pd.notna(change_pct) else None,
                    "amount": float(amount) / 1e8 if pd.notna(amount) else None,
                    "amount_delta": None,
                    "open": float(open_price) if pd.notna(open_price) else None,
                    "prev_close": float(prev_close) if pd.notna(prev_close) else None,
                    "proxy_note": str(item.get("proxy_note", "")).strip(),
                }
            )
        return rows

    def _ts_domestic_indices(self, indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Tushare index_daily — 国内指数日线快照。"""
        rows: List[Dict[str, Any]] = []
        for item in indices:
            code = str(item.get("symbol", "")).strip()
            if not code:
                continue
            for ts_code in self._ts_index_code_candidates(code):
                try:
                    end = datetime.now().strftime("%Y%m%d")
                    start = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
                    raw = self._ts_call("index_daily", ts_code=ts_code, start_date=start, end_date=end)
                    if raw is None or raw.empty:
                        continue
                    raw = raw.sort_values("trade_date", ascending=False)
                    latest_row = raw.iloc[0]
                    prev_row = raw.iloc[1] if len(raw) > 1 else latest_row
                    close = float(latest_row["close"])
                    prev_close = float(prev_row["close"])
                    change_pct = (close / prev_close - 1) if prev_close else None
                    amount = float(latest_row.get("amount", 0)) / 1e4  # 千元→亿
                    rows.append(
                        {
                            "name": str(item.get("name", code)),
                            "symbol": code,
                            "latest": close,
                            "change_pct": change_pct,
                            "amount": amount if amount else None,
                            "amount_delta": None,
                            "open": float(latest_row.get("open", close)),
                            "prev_close": prev_close,
                            "proxy_note": str(item.get("proxy_note", "")).strip(),
                        }
                    )
                    break
                except Exception:
                    continue
        return rows

    def _collect_breadth(self) -> Dict[str, Any]:
        trade_date = self._latest_open_trade_date() or datetime.now().strftime("%Y%m%d")
        try:
            raw = self._ts_call("daily", trade_date=trade_date)
            if raw is not None and not raw.empty:
                change = pd.to_numeric(raw["pct_chg"], errors="coerce").dropna()
                amount = pd.to_numeric(raw["amount"], errors="coerce").dropna()
                return {
                    "up_count": int((change > 0).sum()),
                    "down_count": int((change < 0).sum()),
                    "flat_count": int((change == 0).sum()),
                    "turnover": float(amount.sum()) / 1e5 if not amount.empty else None,  # 千元→亿
                    "trade_date": trade_date,
                    "source": "tushare_daily",
                }
        except Exception:
            pass
        if ak is None:
            return {}
        frame = self.cached_call(
            "market_overview:a_spot_em:v1",
            self._quiet_fetch,
            ak.stock_zh_a_spot_em,
            ttl_hours=1,
        )
        if frame.empty:
            return {}
        change = pd.to_numeric(frame["涨跌幅"], errors="coerce").dropna()
        amount = pd.to_numeric(frame["成交额"], errors="coerce").dropna()
        return {
            "up_count": int((change > 0).sum()),
            "down_count": int((change < 0).sum()),
            "flat_count": int((change == 0).sum()),
            "turnover": float(amount.sum()) / 1e8 if not amount.empty else None,
            "source": "akshare_spot",
        }

    def _collect_global(self, indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if yf is None:
            return []
        rows: List[Dict[str, Any]] = []
        for item in indices:
            symbol = str(item.get("symbol", "")).strip()
            if not symbol:
                continue
            try:
                frame = self.cached_call(
                    f"market_overview:global:{symbol}:v1",
                    yf.Ticker(symbol).history,
                    ttl_hours=2,
                    period="3mo",
                    interval="1d",
                    auto_adjust=False,
                )
            except Exception:
                continue
            if frame.empty or "Close" not in frame.columns:
                continue
            close = pd.to_numeric(frame["Close"], errors="coerce").dropna()
            if close.empty:
                continue
            latest = float(close.iloc[-1])
            change_pct = float(close.iloc[-1] / close.iloc[-2] - 1) if len(close) > 1 else None
            rows.append(
                {
                    "market": str(item.get("market", "")),
                    "name": str(item.get("name", symbol)),
                    "symbol": symbol,
                    "latest": latest,
                    "change_pct": change_pct,
                    "proxy_note": str(item.get("proxy_note", "")).strip(),
                }
            )
        return rows

    def _quiet_fetch(self, fetcher: Any, **kwargs: Any) -> pd.DataFrame:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return fetcher(**kwargs)
