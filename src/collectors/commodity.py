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
    """商品期货与黄金现货行情采集。"""

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
        return self.get_sge_daily("Au99.95")

    def get_sge_basic(self, ts_code: str = "") -> pd.DataFrame:
        """上海黄金交易所现货合约基础信息。Tushare sge_basic 优先。"""
        ts_code = str(ts_code).strip()
        cache_key = f"commodity:sge_basic:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if isinstance(cached, pd.DataFrame):
            return cached.copy()

        kwargs = {}
        if ts_code:
            kwargs["ts_code"] = ts_code

        frame = self._ts_call("sge_basic", **kwargs)
        annotated = self._annotate_sge_basic_frame(frame, ts_code=ts_code)
        if not annotated.empty:
            self._save_cache(cache_key, annotated)
        return annotated

    def get_sge_daily(
        self,
        ts_code: str = "",
        *,
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """上海黄金交易所现货合约日线。Tushare sge_daily 优先。"""
        ts_code = str(ts_code).strip()
        trade_date = str(trade_date).replace("-", "").strip()
        start_date = str(start_date).replace("-", "").strip()
        end_date = str(end_date).replace("-", "").strip()
        cache_key = f"commodity:sge_daily:{ts_code}:{trade_date}:{start_date}:{end_date}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if isinstance(cached, pd.DataFrame):
            return cached.copy()

        kwargs = {}
        if ts_code:
            kwargs["ts_code"] = ts_code
        if trade_date:
            kwargs["trade_date"] = trade_date
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        frame = self._ts_call("sge_daily", **kwargs)
        annotated = self._annotate_sge_daily_frame(
            frame,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if not annotated.empty:
            self._save_cache(cache_key, annotated)
        return annotated

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

    def _annotate_sge_basic_frame(self, frame: pd.DataFrame | None, *, ts_code: str) -> pd.DataFrame:
        if frame is None:
            frame = pd.DataFrame()
        elif not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)
        else:
            frame = frame.copy()
        frame.attrs["source"] = "tushare.sge_basic"
        frame.attrs["as_of"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = ""
        frame.attrs["is_fresh"] = False
        frame.attrs["fallback"] = "none" if not frame.empty else "missing"
        frame.attrs["disclosure"] = "黄金现货基础信息来自 Tushare sge_basic；空表、权限失败或缺失均按缺失处理，不伪装成 fresh。"
        frame.attrs["ts_code"] = ts_code
        return frame

    def _annotate_sge_daily_frame(
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
        date_col = next((column for column in ("trade_date", "date", "dt") if column in frame.columns), "")
        if not frame.empty and date_col:
            frame[date_col] = frame[date_col].map(self._normalize_date_text)
            frame = frame[frame[date_col].astype(str) != ""].copy()
            if not frame.empty:
                frame = frame.sort_values(date_col).reset_index(drop=True)
        latest_date = ""
        if not frame.empty and date_col:
            latest_date = str(frame[date_col].iloc[-1]).strip()
        frame.attrs["source"] = "tushare.sge_daily"
        frame.attrs["as_of"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = latest_date
        frame.attrs["is_fresh"] = bool(latest_date and self._is_date_fresh(latest_date, max_age_days=2))
        frame.attrs["fallback"] = "none" if not frame.empty else "missing"
        frame.attrs["disclosure"] = "黄金现货日线来自 Tushare sge_daily；空表、权限失败或旧日期均按缺失处理，不伪装成 fresh。"
        frame.attrs["ts_code"] = ts_code
        frame.attrs["trade_date"] = trade_date
        frame.attrs["start_date"] = start_date
        frame.attrs["end_date"] = end_date
        return frame

    @staticmethod
    def _is_date_fresh(date_text: str, max_age_days: int = 7) -> bool:
        parsed = pd.to_datetime(date_text, errors="coerce")
        if pd.isna(parsed):
            return False
        stamp = pd.Timestamp(parsed)
        if stamp.tzinfo is not None:
            try:
                stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
            except TypeError:
                stamp = stamp.tz_localize(None)
        age_days = (datetime.now() - stamp.to_pydatetime()).total_seconds() / 86400.0
        return 0 <= age_days <= max_age_days
