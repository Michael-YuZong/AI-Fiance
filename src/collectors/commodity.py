"""Commodity data collector."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class CommodityCollector(BaseCollector):
    """商品期货行情采集。"""

    def _require_ak(self):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        return ak

    def _default_date(self, days: int = 0) -> str:
        return (datetime.now() + timedelta(days=days)).strftime("%Y%m%d")

    def get_main_contract(
        self,
        symbol: str,
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        client = self._require_ak()
        start = start_date or self._default_date(-365 * 3)
        end = end_date or self._default_date()
        return self.cached_call(
            f"commodity:main:{symbol}:{start}:{end}",
            client.futures_main_sina,
            symbol=symbol,
            start_date=start,
            end_date=end,
        )

    def get_gold(self) -> pd.DataFrame:
        return self.get_main_contract("AU0")

    def get_crude_oil(self) -> pd.DataFrame:
        return self.get_main_contract("SC0")

    def get_copper(self) -> pd.DataFrame:
        return self.get_main_contract("CU0")

    def get_rebar(self) -> pd.DataFrame:
        return self.get_main_contract("RB0")

    def get_futures_contango(self) -> pd.DataFrame:
        client = self._require_ak()
        return self.cached_call(
            f"commodity:contango:{self._default_date()}",
            client.futures_spot_price,
            date=self._default_date(),
        )
