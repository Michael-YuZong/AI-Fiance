"""Intraday data collector."""

from __future__ import annotations

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class IntradayCollector(BaseCollector):
    """分钟级分时数据采集。"""

    def get_intraday_chart(self, symbol: str, period: str = "1") -> pd.DataFrame:
        if ak is None:
            raise RuntimeError("akshare is not installed")
        return self.cached_call(
            f"intraday:cn_etf:{symbol}:{period}",
            ak.fund_etf_hist_min_em,
            symbol=symbol,
            period=period,
            adjust="",
            ttl_hours=0,
        )
