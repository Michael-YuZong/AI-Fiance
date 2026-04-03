"""High-level market overview collector for briefing generation — Tushare-first."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from .base import BaseCollector
from .index_topic import IndexTopicCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_yaml

class MarketOverviewCollector(BaseCollector):
    """Collect index-level snapshots for briefing overview tables.

    国内/海外指数概览统一优先走 Tushare 指数专题主链。
    """

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="MarketOverviewCollector")
        self.overview_path = resolve_project_path(
            self.config.get("market_overview_file", "config/market_overview.yaml")
        )

    def _index_topic_collector(self) -> IndexTopicCollector:
        return IndexTopicCollector(self.config)

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
        ts_rows = self._index_topic_collector().get_domestic_overview_rows(indices)
        return ts_rows if ts_rows else []

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
        return {}

    def _collect_global(self, indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._index_topic_collector().get_global_overview_rows(indices)
