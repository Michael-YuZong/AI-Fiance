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
        market_structure = self._collect_market_structure()
        breadth = dict(market_structure.get("breadth") or {})
        global_rows = self._collect_global(payload.get("global_indices", []) or [])
        return {
            "domestic_indices": domestic,
            "breadth": breadth,
            "market_structure": market_structure,
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

    @staticmethod
    def _safe_float(value: Any, *, scale: float = 1.0) -> Optional[float]:
        num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(num):
            return None
        return float(num) * scale

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(num):
            return None
        return int(num)

    def _collect_market_structure(self) -> Dict[str, Any]:
        trade_date = self._latest_open_trade_date() or datetime.now().strftime("%Y%m%d")
        daily_info = self._collect_daily_info_snapshot(trade_date)
        sz_daily_info = self._collect_sz_daily_info_snapshot(trade_date)
        breadth = self._collect_breadth(trade_date)
        has_any = bool(daily_info.get("rows") or sz_daily_info.get("rows") or breadth)
        disclosure = "daily_info / sz_daily_info 优先走 Tushare 市场结构快照；空结果、权限失败或非当期数据不会伪装成 fresh。"
        return {
            "trade_date": trade_date,
            "as_of": trade_date,
            "latest_date": trade_date if has_any else "",
            "is_fresh": has_any,
            "source": "tushare.daily_info+sz_daily_info",
            "fallback": "none" if has_any else "missing",
            "disclosure": disclosure,
            "breadth": breadth,
            "daily_info": daily_info.get("rows", []),
            "daily_info_snapshot": daily_info,
            "sz_daily_info": sz_daily_info.get("rows", []),
            "sz_daily_info_snapshot": sz_daily_info,
        }

    def _collect_daily_info_snapshot(self, trade_date: str) -> Dict[str, Any]:
        """Collect Tushare market transaction structure snapshot."""
        return self._collect_market_structure_snapshot(
            api_name="daily_info",
            trade_date=trade_date,
            call_kwargs={"trade_date": trade_date, "exchange": "SZ,SH"},
            row_scale=1.0,
            source_label="tushare.daily_info",
            disclosure="daily_info 作为全市场结构快照主源，空表或权限不足不伪装成 fresh。",
        )

    def _collect_sz_daily_info_snapshot(self, trade_date: str) -> Dict[str, Any]:
        """Collect Shenzhen market transaction structure snapshot."""
        return self._collect_market_structure_snapshot(
            api_name="sz_daily_info",
            trade_date=trade_date,
            call_kwargs={"trade_date": trade_date},
            row_scale=1.0,
            source_label="tushare.sz_daily_info",
            disclosure="sz_daily_info 作为深圳市场补充结构快照主源，保持 Tushare 官方输出单位，空表或非当期不伪装成 fresh。",
        )

    def _collect_market_structure_snapshot(
        self,
        *,
        api_name: str,
        trade_date: str,
        call_kwargs: Mapping[str, Any],
        row_scale: float,
        source_label: str,
        disclosure: str,
    ) -> Dict[str, Any]:
        try:
            raw = self._ts_call(api_name, **dict(call_kwargs))
        except Exception:
            raw = None
        if raw is None or raw.empty:
            return {
                "trade_date": trade_date,
                "as_of": trade_date,
                "latest_date": "",
                "is_fresh": False,
                "source": source_label,
                "fallback": "missing",
                "disclosure": disclosure,
                "rows": [],
            }

        frame = raw.copy()
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].astype(str)
        if "ts_code" in frame.columns:
            frame["ts_code"] = frame["ts_code"].astype(str)
        sort_cols = [column for column in ("trade_date", "ts_code", "ts_name") if column in frame.columns]
        if sort_cols:
            frame = frame.sort_values(sort_cols, ascending=[True] * len(sort_cols))
        latest_date = trade_date
        if "trade_date" in frame.columns and not frame["trade_date"].empty:
            latest_date = str(frame["trade_date"].dropna().astype(str).max() or trade_date)
        is_fresh = latest_date == trade_date
        fallback = "none" if is_fresh else "stale"
        rows: List[Dict[str, Any]] = []
        for _, row in frame.iterrows():
            rows.append(
                {
                    "trade_date": str(row.get("trade_date", trade_date) or trade_date),
                    "ts_code": str(row.get("ts_code", "")).strip(),
                    "ts_name": str(row.get("ts_name", "")).strip(),
                    "count": self._safe_int(row.get("count", row.get("com_count"))),
                    "amount": self._safe_float(row.get("amount"), scale=row_scale),
                    "vol": self._safe_float(row.get("vol"), scale=row_scale),
                    "total_share": self._safe_float(row.get("total_share"), scale=row_scale),
                    "total_mv": self._safe_float(row.get("total_mv"), scale=row_scale),
                    "float_share": self._safe_float(row.get("float_share"), scale=row_scale),
                    "float_mv": self._safe_float(row.get("float_mv"), scale=row_scale),
                    "trans_count": self._safe_int(row.get("trans_count")),
                    "pe": self._safe_float(row.get("pe")),
                    "tr": self._safe_float(row.get("tr")),
                    "exchange": str(row.get("exchange", "")).strip(),
                    "source": source_label,
                    "as_of": trade_date,
                    "latest_date": latest_date,
                    "is_fresh": is_fresh,
                    "fallback": fallback,
                    "disclosure": disclosure,
                }
            )
        return {
            "trade_date": trade_date,
            "as_of": trade_date,
            "latest_date": latest_date,
            "is_fresh": is_fresh,
            "source": source_label,
            "fallback": fallback,
            "disclosure": disclosure,
            "rows": rows,
        }

    def _collect_breadth(self, trade_date: Optional[str] = None) -> Dict[str, Any]:
        trade_date = trade_date or self._latest_open_trade_date() or datetime.now().strftime("%Y%m%d")
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
                    "as_of": trade_date,
                    "latest_date": trade_date,
                    "is_fresh": True,
                    "source": "tushare_daily",
                    "fallback": "none",
                    "disclosure": "全市场涨跌家数与成交额优先走 Tushare daily，空表不伪装成 fresh。",
                }
        except Exception:
            pass
        return {}

    def _collect_global(self, indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._index_topic_collector().get_global_overview_rows(indices)
