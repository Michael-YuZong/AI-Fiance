"""Valuation and ETF metadata collectors."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class ValuationCollector(BaseCollector):
    """Collect ETF scale and NAV-related data."""

    def _require_ak(self):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        return ak

    def get_cn_etf_nav_history(self, symbol: str, days: int = 120) -> pd.DataFrame:
        client = self._require_ak()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        return self.cached_call(
            f"valuation:nav:{symbol}:{start_date}:{end_date}",
            client.fund_etf_fund_info_em,
            fund=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def get_cn_etf_scale(self, symbol: str) -> Optional[dict]:
        client = self._require_ak()
        date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        candidates = []
        try:
            candidates.append(self.cached_call(f"valuation:scale:sse:{date}", client.fund_etf_scale_sse, date=date))
        except Exception:
            pass
        try:
            candidates.append(self.cached_call("valuation:scale:szse", client.fund_etf_scale_szse))
        except Exception:
            pass
        for frame in candidates:
            code_column = "基金代码" if "基金代码" in frame.columns else None
            if code_column is None:
                continue
            filtered = frame[frame[code_column].astype(str) == str(symbol)]
            if not filtered.empty:
                return filtered.iloc[0].to_dict()
        return None
