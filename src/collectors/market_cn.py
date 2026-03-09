"""China market data collector."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

import numpy as np
import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    ak = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


class ChinaMarketCollector(BaseCollector):
    """A 股 ETF 行情与技术数据采集。"""

    def _yahoo_symbol(self, symbol: str) -> str:
        if len(symbol) == 6 and symbol.isdigit():
            suffix = ".SS" if symbol[0] in {"5", "6", "9"} else ".SZ"
            return f"{symbol}{suffix}"
        return symbol

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
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()
        try:
            fetcher = self._ak_function("fund_etf_hist_em")
            return self.cached_call(
                f"cn_market:etf_daily:{symbol}:{period}:{adjust}:{start}:{end}",
                fetcher,
                symbol=symbol,
                period=period,
                start_date=start,
                end_date=end,
                adjust=adjust,
            )
        except Exception as primary_exc:
            if yf is None:
                raise primary_exc
            ticker = self._yahoo_symbol(symbol)
            try:
                return self.cached_call(
                    f"cn_market:yahoo_etf_daily:{ticker}:{period}",
                    yf.Ticker(ticker).history,
                    period="3y" if period == "daily" else period,
                    interval="1d",
                    auto_adjust=False,
                )
            except Exception:
                raise primary_exc

    def get_index_daily(
        self,
        symbol: str,
        period: str = "daily",
        start_date: str = "",
        end_date: str = "",
        proxy_symbol: str = "",
    ) -> pd.DataFrame:
        """A 股指数历史行情；必要时可回退到代表性 ETF 代理。"""
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()
        if proxy_symbol and proxy_symbol != symbol:
            try:
                return self.get_etf_daily(proxy_symbol, period="daily", adjust="qfq", start_date=start, end_date=end)
            except Exception:
                pass
        try:
            fetcher = self._ak_function("index_zh_a_hist")
            return self.cached_call(
                f"cn_market:index_daily:{symbol}:{period}:{start}:{end}",
                fetcher,
                symbol=symbol,
                period=period,
                start_date=start,
                end_date=end,
            )
        except Exception as primary_exc:
            if proxy_symbol and proxy_symbol != symbol:
                try:
                    return self.get_etf_daily(proxy_symbol, period="daily", adjust="qfq", start_date=start, end_date=end)
                except Exception:
                    pass
            raise primary_exc

    def get_open_fund_daily(
        self,
        symbol: str,
        indicator: str = "单位净值走势",
        period: str = "3年",
        proxy_symbol: str = "",
    ) -> pd.DataFrame:
        """开放式基金净值走势，转换为可复用的 OHLCV 结构。"""
        try:
            fetcher = self._ak_function("fund_open_fund_info_em")
            frame = self.cached_call(
                f"cn_market:open_fund:{symbol}:{indicator}:{period}",
                fetcher,
                symbol=symbol,
                indicator=indicator,
                period=period,
            )
            return self._normalize_open_fund_nav(frame)
        except Exception as primary_exc:
            if proxy_symbol and proxy_symbol != symbol:
                try:
                    return self.get_etf_daily(proxy_symbol)
                except Exception:
                    pass
            raise primary_exc

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

    def _normalize_open_fund_nav(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            raise ValueError("Open fund nav frame is empty")

        date_col = next((col for col in frame.columns if col in {"净值日期", "日期", "date"}), None)
        value_col = next((col for col in frame.columns if col in {"单位净值", "累计净值", "净值", "close"}), None)
        if not date_col or not value_col:
            raise ValueError("Open fund nav frame missing required columns")

        nav = frame[[date_col, value_col]].copy()
        nav.columns = ["date", "close"]
        nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
        nav["close"] = pd.to_numeric(nav["close"], errors="coerce")
        nav = nav.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        if nav.empty:
            raise ValueError("Open fund nav frame has no valid rows")

        for column in ("open", "high", "low"):
            nav[column] = nav["close"]
        nav["volume"] = 0.0
        nav["amount"] = np.nan
        return nav[["date", "open", "high", "low", "close", "volume", "amount"]]
