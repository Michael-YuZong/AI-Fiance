"""China market data collector."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    ak = None


class ChinaMarketCollector(BaseCollector):
    """A 股 ETF 行情与技术数据采集。"""

    def _ak_function(self, name: str) -> Callable[..., Any]:
        if ak is None:
            raise RuntimeError("akshare is not installed")
        func = getattr(ak, name, None)
        if not callable(func):
            raise RuntimeError(f"AKShare function not available: {name}")
        return func

    def _date_str(self, offset_days: int = 0) -> str:
        return (datetime.now() + timedelta(days=offset_days)).strftime("%Y%m%d")

    def get_etf_daily(
        self,
        symbol: str,
        period: str = "daily",
        adjust: str = "qfq",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """ETF 日 K 线。"""
        fetcher = self._ak_function("fund_etf_hist_em")
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()
        return self.cached_call(
            f"cn_market:etf_daily:{symbol}:{period}:{adjust}:{start}:{end}",
            fetcher,
            symbol=symbol,
            period=period,
            start_date=start,
            end_date=end,
            adjust=adjust,
        )

    def get_etf_realtime(self) -> pd.DataFrame:
        """ETF 实时行情。"""
        fetcher = self._ak_function("fund_etf_spot_em")
        return self.cached_call("cn_market:etf_realtime", fetcher, ttl_hours=0)

    def get_etf_fund_flow(self, symbol: str) -> pd.DataFrame:
        """ETF 资金流向。"""
        if ak is None:
            raise RuntimeError("akshare is not installed")
        fetcher = getattr(ak, "fund_etf_fund_daily_em", None)
        if callable(fetcher):
            frame = self.cached_call("cn_market:fund_flow:all", fetcher, ttl_hours=0)
            code_columns = ["基金代码", "代码"]
            for code_column in code_columns:
                if code_column in frame.columns:
                    filtered = frame[frame[code_column].astype(str) == str(symbol)]
                    if not filtered.empty:
                        return filtered.reset_index(drop=True)
        return pd.DataFrame()

    def get_north_south_flow(self) -> pd.DataFrame:
        """北向 / 南向资金净流入。"""
        fetcher = self._ak_function("stock_hsgt_fund_flow_summary_em")
        return self.cached_call("cn_market:north_south_flow", fetcher, ttl_hours=0)

    def get_margin_trading(self) -> pd.DataFrame:
        """融资融券数据。"""
        fetcher = self._ak_function("stock_margin_detail_sse")
        return self.cached_call(
            f"cn_market:margin:{self._date_str()}",
            fetcher,
            date=self._date_str(),
        )

    def get_sector_pe(self, sector: str) -> pd.DataFrame:
        """板块估值数据。"""
        if ak is None:
            raise RuntimeError("akshare is not installed")
        fetcher = getattr(ak, "stock_board_industry_hist_em", None)
        if not callable(fetcher):
            raise RuntimeError("AKShare function not available: stock_board_industry_hist_em")
        return self.cached_call(
            f"cn_market:sector:{sector}",
            fetcher,
            symbol=sector,
            period="日k",
            adjust="qfq",
        )
