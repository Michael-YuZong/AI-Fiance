"""China macro data collector."""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    ak = None


class ChinaMacroCollector(BaseCollector):
    """采集中国宏观经济核心指标。"""

    def _ak_function(self, *candidates: str) -> Callable[..., Any]:
        if ak is None:
            raise RuntimeError("akshare is not installed")
        for name in candidates:
            func = getattr(ak, name, None)
            if callable(func):
                return func
        raise RuntimeError(f"AKShare function not available: {', '.join(candidates)}")

    def get_pmi(self) -> pd.DataFrame:
        """制造业 PMI。"""
        fetcher = self._ak_function("macro_china_pmi", "macro_china_pmi_yearly")
        return self.cached_call("china_macro:pmi", fetcher)

    def get_cpi(self) -> pd.DataFrame:
        """CPI 同比。"""
        fetcher = self._ak_function("macro_china_cpi_monthly", "macro_china_cpi_yearly")
        return self.cached_call("china_macro:cpi", fetcher)

    def get_ppi(self) -> pd.DataFrame:
        """PPI 同比。"""
        fetcher = self._ak_function("macro_china_ppi")
        return self.cached_call("china_macro:ppi", fetcher)

    def get_money_supply(self) -> pd.DataFrame:
        """M1/M2 增速。"""
        fetcher = self._ak_function("macro_china_money_supply")
        return self.cached_call("china_macro:money_supply", fetcher)

    def get_social_financing(self) -> pd.DataFrame:
        """社会融资规模。"""
        fetcher = self._ak_function("macro_china_shrzgm")
        return self.cached_call("china_macro:social_financing", fetcher)

    def get_lpr(self) -> pd.DataFrame:
        """LPR 利率。"""
        fetcher = self._ak_function("macro_china_lpr")
        return self.cached_call("china_macro:lpr", fetcher)

    def get_reverse_repo(self) -> pd.DataFrame:
        """逆回购操作。"""
        fetcher = self._ak_function("macro_china_reverse_repo")
        return self.cached_call("china_macro:reverse_repo", fetcher)

    def get_shibor(self) -> pd.DataFrame:
        """SHIBOR 利率。"""
        fetcher = self._ak_function("rate_interbank")
        return self.cached_call(
            "china_macro:shibor",
            fetcher,
            market="上海银行同业拆借市场",
            symbol="Shibor人民币",
            indicator="利率",
        )
