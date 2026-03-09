"""A-share market pulse collectors for briefing generation."""

from __future__ import annotations

import io
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class MarketPulseCollector(BaseCollector):
    """Collect broad A-share pulse data such as limit-up pools and 龙虎榜."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="MarketPulseCollector")

    def collect(self, reference_date: Optional[datetime] = None) -> Dict[str, Any]:
        if ak is None:
            return {}

        as_of = reference_date or datetime.now()
        zt_info = self._latest_pool("stock_zt_pool_em", as_of)
        prev_zt_info = self._latest_pool("stock_zt_pool_previous_em", as_of)
        strong_info = self._latest_pool("stock_zt_pool_strong_em", as_of)
        dt_info = self._latest_pool("stock_zt_pool_dtgc_em", as_of)

        latest_trade_date = (
            zt_info["date"]
            or prev_zt_info["date"]
            or strong_info["date"]
            or dt_info["date"]
            or as_of.strftime("%Y-%m-%d")
        )
        lhb_detail = self._lhb_detail(latest_trade_date)
        lhb_stats = self._lhb_stats()
        lhb_institution = self._lhb_institution(latest_trade_date)
        lhb_desks = self._lhb_active_desks(latest_trade_date)

        return {
            "market_date": latest_trade_date,
            "zt_pool": zt_info["frame"],
            "prev_zt_pool": prev_zt_info["frame"],
            "strong_pool": strong_info["frame"],
            "dt_pool": dt_info["frame"],
            "lhb_detail": lhb_detail,
            "lhb_stats": lhb_stats,
            "lhb_institution": lhb_institution,
            "lhb_desks": lhb_desks,
        }

    def _latest_pool(self, func_name: str, reference_date: datetime) -> Dict[str, Any]:
        fetcher = getattr(ak, func_name, None)
        if not callable(fetcher):
            return {"date": "", "frame": pd.DataFrame()}

        for offset in range(0, 8):
            target = (reference_date - timedelta(days=offset)).strftime("%Y%m%d")
            cache_key = f"market_pulse:{func_name}:{target}"
            try:
                frame = self.cached_call(
                    cache_key,
                    self._quiet_fetch,
                    fetcher,
                    ttl_hours=2,
                    date=target,
                )
            except Exception:
                continue
            if frame is not None and not frame.empty:
                return {
                    "date": datetime.strptime(target, "%Y%m%d").strftime("%Y-%m-%d"),
                    "frame": frame.reset_index(drop=True),
                }
        return {"date": "", "frame": pd.DataFrame()}

    def _lhb_detail(self, trade_date: str) -> pd.DataFrame:
        fetcher = getattr(ak, "stock_lhb_detail_em", None)
        if not callable(fetcher) or not trade_date:
            return pd.DataFrame()
        end_date = trade_date.replace("-", "")
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        try:
            frame = self.cached_call(
                f"market_pulse:lhb_detail:{start_date}:{end_date}",
                self._quiet_fetch,
                fetcher,
                ttl_hours=6,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()
        if "上榜日" in frame.columns:
            frame = self._latest_rows(frame, "上榜日", trade_date)
        return frame.reset_index(drop=True)

    def _lhb_stats(self) -> pd.DataFrame:
        fetcher = getattr(ak, "stock_lhb_stock_statistic_em", None)
        if not callable(fetcher):
            return pd.DataFrame()
        try:
            frame = self.cached_call(
                "market_pulse:lhb_stock_statistic:1m",
                self._quiet_fetch,
                fetcher,
                ttl_hours=12,
                symbol="近一月",
            )
        except Exception:
            return pd.DataFrame()
        return frame.reset_index(drop=True)

    def _lhb_institution(self, trade_date: str) -> pd.DataFrame:
        fetcher = getattr(ak, "stock_lhb_jgmmtj_em", None)
        if not callable(fetcher) or not trade_date:
            return pd.DataFrame()
        end_date = trade_date.replace("-", "")
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        try:
            frame = self.cached_call(
                f"market_pulse:lhb_institution:{start_date}:{end_date}",
                self._quiet_fetch,
                fetcher,
                ttl_hours=6,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()
        if "上榜日期" in frame.columns:
            frame = self._latest_rows(frame, "上榜日期", trade_date)
        return frame.reset_index(drop=True)

    def _lhb_active_desks(self, trade_date: str) -> pd.DataFrame:
        fetcher = getattr(ak, "stock_lhb_hyyyb_em", None)
        if not callable(fetcher) or not trade_date:
            return pd.DataFrame()
        end_date = trade_date.replace("-", "")
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        try:
            frame = self.cached_call(
                f"market_pulse:lhb_desks:{start_date}:{end_date}",
                self._quiet_fetch,
                fetcher,
                ttl_hours=6,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            return pd.DataFrame()
        if "上榜日" in frame.columns:
            frame = self._latest_rows(frame, "上榜日", trade_date)
        return frame.reset_index(drop=True)

    def _quiet_fetch(self, fetcher: Any, **kwargs: Any) -> pd.DataFrame:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return fetcher(**kwargs)

    def _latest_rows(self, frame: pd.DataFrame, column: str, preferred_date: str) -> pd.DataFrame:
        if frame.empty or column not in frame.columns:
            return frame
        normalized = frame[column].astype(str)
        exact = frame[normalized == preferred_date]
        if not exact.empty:
            return exact
        latest = normalized.max()
        return frame[normalized == latest]
