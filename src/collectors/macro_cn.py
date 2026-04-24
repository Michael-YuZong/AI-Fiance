"""China macro data collector — Tushare-first, AKShare fallback."""

from __future__ import annotations

from datetime import datetime
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

    # ── FX 外汇 ───────────────────────────────────────────────

    def get_fx_basic(
        self,
        ts_code: str = "",
        *,
        classify: str = "FX",
        exchange: str = "FXCM",
    ) -> pd.DataFrame:
        """外汇基础信息。Tushare fx_obasic 优先，空表按缺失处理。"""
        ts_code = str(ts_code).strip()
        classify = str(classify).strip()
        exchange = str(exchange).strip()
        cache_key = f"china_macro:fx_basic:{ts_code}:{classify}:{exchange}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if isinstance(cached, pd.DataFrame):
            return cached.copy()

        kwargs: dict[str, Any] = {}
        if ts_code:
            kwargs["ts_code"] = ts_code
        if classify:
            kwargs["classify"] = classify
        if exchange:
            kwargs["exchange"] = exchange
        frame = self._ts_call("fx_obasic", **kwargs)
        annotated = self._annotate_fx_basic_frame(frame, ts_code=ts_code, classify=classify, exchange=exchange)
        if not annotated.empty:
            self._save_cache(cache_key, annotated)
        return annotated

    def get_fx_obasic(
        self,
        ts_code: str = "",
        *,
        classify: str = "FX",
        exchange: str = "FXCM",
    ) -> pd.DataFrame:
        """Alias for :meth:`get_fx_basic` using the official Tushare endpoint name."""
        return self.get_fx_basic(ts_code=ts_code, classify=classify, exchange=exchange)

    def get_fx_daily(
        self,
        ts_code: str = "",
        *,
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """外汇日线行情。Tushare fx_daily 优先，旧日期不伪装 fresh。"""
        ts_code = str(ts_code).strip()
        trade_date = str(trade_date).replace("-", "").strip()
        start_date = str(start_date).replace("-", "").strip()
        end_date = str(end_date).replace("-", "").strip()
        cache_key = f"china_macro:fx_daily:{ts_code}:{trade_date}:{start_date}:{end_date}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if isinstance(cached, pd.DataFrame):
            return cached.copy()

        kwargs: dict[str, Any] = {}
        if ts_code:
            kwargs["ts_code"] = ts_code
        if trade_date:
            kwargs["trade_date"] = trade_date
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        try:
            frame = self._ts_call("fx_daily", **kwargs)
        except Exception:
            return self._empty_fx_daily_frame(
                ts_code=ts_code,
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date,
            )
        annotated = self._annotate_fx_daily_frame(frame, ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        if not annotated.empty:
            self._save_cache(cache_key, annotated)
        return annotated

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

    def _annotate_fx_basic_frame(
        self,
        frame: pd.DataFrame | None,
        *,
        ts_code: str,
        classify: str,
        exchange: str,
    ) -> pd.DataFrame:
        if frame is None:
            frame = pd.DataFrame()
        elif not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)
        else:
            frame = frame.copy()
        disclosure = "外汇基础信息来自 Tushare fx_obasic；空表、权限失败或旧日期均按缺失处理，不伪装成 fresh。"
        latest_date = ""
        frame.attrs["source"] = "tushare.fx_obasic"
        frame.attrs["as_of"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = latest_date
        frame.attrs["is_fresh"] = False
        frame.attrs["fallback"] = "none" if not frame.empty else "missing"
        frame.attrs["disclosure"] = disclosure
        frame.attrs["ts_code"] = ts_code
        frame.attrs["classify"] = classify
        frame.attrs["exchange"] = exchange
        return frame

    def _annotate_fx_daily_frame(
        self,
        frame: pd.DataFrame | None,
        *,
        ts_code: str,
        trade_date: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        if frame is None:
            frame = pd.DataFrame()
        elif not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)
        else:
            frame = frame.copy()
        if not frame.empty and "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(BaseCollector._normalize_date_text)
            frame = frame[frame["trade_date"].astype(str) != ""].copy()
            if not frame.empty:
                frame = frame.sort_values("trade_date").reset_index(drop=True)
        latest_date = ""
        if not frame.empty and "trade_date" in frame.columns:
            latest_date = str(frame["trade_date"].iloc[-1]).strip()
        as_of = datetime.now()
        is_fresh = bool(latest_date and self._is_date_fresh(latest_date, as_of, max_age_days=2))
        disclosure = "外汇日线来自 Tushare fx_daily；空表、权限失败或旧日期均按缺失处理，不伪装成 fresh。"
        frame.attrs["source"] = "tushare.fx_daily"
        frame.attrs["as_of"] = as_of.strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = latest_date
        frame.attrs["is_fresh"] = is_fresh
        frame.attrs["fallback"] = "none" if not frame.empty else "missing"
        frame.attrs["disclosure"] = disclosure
        frame.attrs["ts_code"] = ts_code
        frame.attrs["trade_date"] = trade_date
        frame.attrs["start_date"] = start_date
        frame.attrs["end_date"] = end_date
        return frame

    def _empty_fx_basic_frame(self, *, ts_code: str, classify: str, exchange: str) -> pd.DataFrame:
        frame = pd.DataFrame()
        return self._annotate_fx_basic_frame(frame, ts_code=ts_code, classify=classify, exchange=exchange)

    def _empty_fx_daily_frame(self, *, ts_code: str, trade_date: str, start_date: str, end_date: str) -> pd.DataFrame:
        frame = pd.DataFrame()
        return self._annotate_fx_daily_frame(frame, ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)

    @staticmethod
    def _is_date_fresh(date_text: str, reference_date: datetime, max_age_days: int = 7) -> bool:
        parsed = pd.to_datetime(date_text, errors="coerce")
        if pd.isna(parsed):
            return False
        stamp = pd.Timestamp(parsed)
        if stamp.tzinfo is not None:
            try:
                stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
            except TypeError:
                stamp = stamp.tz_localize(None)
        age_days = (reference_date - stamp.to_pydatetime()).total_seconds() / 86400.0
        return 0 <= age_days <= max_age_days
