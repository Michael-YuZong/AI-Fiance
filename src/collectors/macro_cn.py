"""China macro data collector — Tushare-first, AKShare fallback."""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    ak = None


class ChinaMacroCollector(BaseCollector):
    """采集中国宏观经济核心指标。Tushare 优先，AKShare 兜底。"""

    def _ak_function(self, *candidates: str) -> Callable[..., Any]:
        if ak is None:
            raise RuntimeError("akshare is not installed")
        for name in candidates:
            func = getattr(ak, name, None)
            if callable(func):
                return func
        raise RuntimeError(f"AKShare function not available: {', '.join(candidates)}")

    # ── PMI ───────────────────────────────────────────────────

    def get_pmi(self) -> pd.DataFrame:
        """制造业 PMI。Tushare cn_pmi 优先。"""
        try:
            raw = self._ts_call("cn_pmi")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        fetcher = self._ak_function("macro_china_pmi", "macro_china_pmi_yearly")
        return self.cached_call("china_macro:pmi", fetcher, prefer_stale=True)

    # ── CPI ───────────────────────────────────────────────────

    def get_cpi(self) -> pd.DataFrame:
        """CPI 同比。Tushare cn_cpi 优先。"""
        try:
            raw = self._ts_call("cn_cpi")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        fetcher = self._ak_function("macro_china_cpi_monthly", "macro_china_cpi_yearly")
        return self.cached_call("china_macro:cpi", fetcher, prefer_stale=True)

    # ── PPI ───────────────────────────────────────────────────

    def get_ppi(self) -> pd.DataFrame:
        """PPI 同比。Tushare cn_ppi 优先。"""
        try:
            raw = self._ts_call("cn_ppi")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        fetcher = self._ak_function("macro_china_ppi")
        return self.cached_call("china_macro:ppi", fetcher, prefer_stale=True)

    # ── M0 / M1 / M2 货币供应 ────────────────────────────────

    def get_money_supply(self) -> pd.DataFrame:
        """M1/M2 增速。Tushare cn_m 优先。"""
        try:
            raw = self._ts_call("cn_m")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        fetcher = self._ak_function("macro_china_money_supply")
        return self.cached_call("china_macro:money_supply", fetcher, prefer_stale=True)

    # ── 社会融资规模 ──────────────────────────────────────────

    def get_social_financing(self) -> pd.DataFrame:
        """社会融资规模。Tushare cn_sf 优先。"""
        try:
            for api_name in ("sf_month", "cn_sf"):
                raw = self._ts_call(api_name)
                if raw is not None and not raw.empty:
                    return raw
        except Exception:
            pass
        fetcher = self._ak_function("macro_china_shrzgm")
        return self.cached_call("china_macro:social_financing", fetcher, prefer_stale=True)

    # ── LPR 利率 ──────────────────────────────────────────────

    def get_lpr(self) -> pd.DataFrame:
        """LPR 利率。Tushare shibor_lpr 优先。"""
        try:
            raw = self._ts_call("shibor_lpr")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        fetcher = self._ak_function("macro_china_lpr")
        return self.cached_call("china_macro:lpr", fetcher, prefer_stale=True)

    # ── 逆回购 ───────────────────────────────────────────────

    def get_reverse_repo(self) -> pd.DataFrame:
        """逆回购操作。"""
        fetcher = self._ak_function("macro_china_reverse_repo")
        return self.cached_call("china_macro:reverse_repo", fetcher, prefer_stale=True)

    # ── SHIBOR ────────────────────────────────────────────────

    def get_shibor(self) -> pd.DataFrame:
        """SHIBOR 利率。Tushare shibor 优先。"""
        try:
            raw = self._ts_call("shibor")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        fetcher = self._ak_function("rate_interbank")
        return self.cached_call(
            "china_macro:shibor",
            fetcher,
            market="上海银行同业拆借市场",
            symbol="Shibor人民币",
            indicator="利率",
        )

    # ── GDP（新增）────────────────────────────────────────────

    def get_gdp(self) -> pd.DataFrame:
        """GDP 数据。Tushare cn_gdp 优先。"""
        try:
            raw = self._ts_call("cn_gdp")
            if raw is not None and not raw.empty:
                return raw
        except Exception:
            pass
        return pd.DataFrame()
