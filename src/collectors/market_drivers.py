"""Broad market drivers for rotation and capital-flow sections — Tushare-first."""

from __future__ import annotations

import io
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from typing import Any, Dict, Mapping, Optional

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class MarketDriversCollector(BaseCollector):
    """Collect market-wide sector rotation and capital-flow inputs.

    沪深港通资金流向/十大成交股/融资融券 优先用 Tushare，板块涨幅/热门度继续用 AKShare。
    """

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="MarketDriversCollector")

    def collect(self, reference_date: Optional[datetime] = None) -> Dict[str, Any]:
        as_of = reference_date or datetime.now()

        result: Dict[str, Any] = {
            "market_flow": self._market_flow(as_of),
            "northbound_flow": self._ts_northbound_flow(),
            "northbound_top10": self._ts_northbound_top10(),
            "margin_summary": self._ts_margin_summary(),
            "pledge_stat": self._ts_pledge_stat(),
        }

        # AKShare 数据（板块涨幅/概念/热门排名 — Tushare 不覆盖）
        if ak is not None:
            result["industry_fund_flow"] = self._sector_fund_flow("行业资金流")
            result["concept_fund_flow"] = self._sector_fund_flow("概念资金流")
            result["northbound_industry"] = self._northbound_rank("北向资金增持行业板块排行", as_of)
            result["northbound_concept"] = self._northbound_rank("北向资金增持概念板块排行", as_of)
            result["industry_spot"] = self._board_spot("stock_board_industry_name_em")
            result["concept_spot"] = self._board_spot("stock_board_concept_name_em")
            result["hot_rank"] = self._hot_rank()
        else:
            for key in ("industry_fund_flow", "concept_fund_flow", "northbound_industry",
                        "northbound_concept", "industry_spot", "concept_spot", "hot_rank"):
                result.setdefault(key, pd.DataFrame() if key.endswith("spot") or key == "hot_rank" else {})

        return result

    # ── Tushare: 沪深港通资金流向 ─────────────────────────────

    def _ts_northbound_flow(self) -> pd.DataFrame:
        """Tushare moneyflow_hsgt — 北向/南向每日资金流向。"""
        cache_key = "market_drivers:ts_northbound_flow"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None:
            return cached
        start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        raw = self._ts_call("moneyflow_hsgt", start_date=start, end_date=end)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def _ts_northbound_top10(self) -> pd.DataFrame:
        """Tushare hsgt_top10 — 沪深港通十大成交股。"""
        cache_key = "market_drivers:ts_northbound_top10"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached
        for offset in range(0, 6):
            trade_date = (datetime.now() - timedelta(days=offset)).strftime("%Y%m%d")
            raw = self._ts_call("hsgt_top10", trade_date=trade_date)
            if raw is not None and not raw.empty:
                self._save_cache(cache_key, raw)
                return raw
        return pd.DataFrame()

    # ── Tushare: 融资融券 ─────────────────────────────────────

    def _ts_margin_summary(self) -> pd.DataFrame:
        """Tushare margin — 全市场融资融券汇总。"""
        cache_key = "market_drivers:ts_margin_summary"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached
        for offset in range(0, 6):
            trade_date = (datetime.now() - timedelta(days=offset)).strftime("%Y%m%d")
            raw = self._ts_call("margin", trade_date=trade_date)
            if raw is not None and not raw.empty:
                self._save_cache(cache_key, raw)
                return raw
        return pd.DataFrame()

    # ── Tushare: 股权质押 ─────────────────────────────────────

    def _ts_pledge_stat(self) -> pd.DataFrame:
        """Tushare pledge_stat — 大股东股权质押统计。"""
        cache_key = "market_drivers:ts_pledge_stat"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("pledge_stat", end_date=datetime.now().strftime("%Y%m%d"))
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def _market_flow(self, reference_date: datetime) -> Dict[str, Any]:
        fetcher = getattr(ak, "stock_market_fund_flow", None)
        if not callable(fetcher):
            return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": ""}
        try:
            frame = self.cached_call(
                "market_drivers:market_flow",
                self._quiet_fetch,
                fetcher,
                ttl_hours=2,
            )
        except Exception:
            return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": ""}
        latest_date = self._extract_latest_date(frame, "日期")
        return {
            "frame": frame.reset_index(drop=True),
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, reference_date),
        }

    def _northbound_rank(self, symbol: str, reference_date: datetime) -> Dict[str, Any]:
        fetcher = getattr(ak, "stock_hsgt_board_rank_em", None)
        if not callable(fetcher):
            return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": "", "symbol": symbol}
        try:
            frame = self.cached_call(
                f"market_drivers:northbound:{symbol}",
                self._quiet_fetch,
                fetcher,
                ttl_hours=8,
                symbol=symbol,
                indicator="今日",
            )
        except Exception:
            return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": "", "symbol": symbol}
        latest_date = self._extract_latest_date(frame, "报告时间")
        return {
            "frame": frame.reset_index(drop=True),
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, reference_date),
            "symbol": symbol,
        }

    def _board_spot(self, func_name: str) -> pd.DataFrame:
        fetcher = getattr(ak, func_name, None)
        if not callable(fetcher):
            return pd.DataFrame()
        try:
            return self.cached_call(
                f"market_drivers:{func_name}",
                self._quiet_fetch,
                fetcher,
                ttl_hours=1,
            ).reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

    def _hot_rank(self) -> pd.DataFrame:
        fetcher = getattr(ak, "stock_hot_rank_em", None)
        if not callable(fetcher):
            return pd.DataFrame()
        try:
            return self.cached_call(
                "market_drivers:hot_rank",
                self._quiet_fetch,
                fetcher,
                ttl_hours=1,
            ).reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

    def _sector_fund_flow(self, sector_type: str) -> pd.DataFrame:
        fetcher = getattr(ak, "stock_sector_fund_flow_rank", None)
        if not callable(fetcher):
            return pd.DataFrame()
        try:
            return self.cached_call(
                f"market_drivers:sector_fund_flow:{sector_type}",
                self._quiet_fetch,
                fetcher,
                ttl_hours=1,
                indicator="今日",
                sector_type=sector_type,
            ).reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

    def _quiet_fetch(self, fetcher: Any, **kwargs: Any) -> pd.DataFrame:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return fetcher(**kwargs)

    def _extract_latest_date(self, frame: pd.DataFrame, column: str) -> str:
        if frame.empty or column not in frame.columns:
            return ""
        values = frame[column].astype(str).dropna()
        if values.empty:
            return ""
        return str(values.max())

    def _is_fresh(self, date_text: str, reference_date: datetime, max_age_days: int = 7) -> bool:
        if not date_text:
            return False
        normalized = date_text.replace("/", "-")
        try:
            target = datetime.strptime(normalized[:10], "%Y-%m-%d")
        except ValueError:
            return False
        return abs((reference_date.date() - target.date()).days) <= max_age_days
