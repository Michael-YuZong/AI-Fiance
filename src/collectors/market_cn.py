"""China market data collector — Tushare-first, AKShare/yfinance fallback."""

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

try:
    import efinance as ef
except ImportError:  # pragma: no cover
    ef = None


class ChinaMarketCollector(BaseCollector):
    """A 股 ETF 行情与技术数据采集。Tushare 优先，AKShare/yfinance 兜底。"""

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

    # ── A 股个股日 K ──────────────────────────────────────────

    def get_stock_daily(
        self,
        symbol: str,
        period: str = "daily",
        adjust: str = "qfq",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """A 股个股日 K 线。Tushare daily + adj_factor 优先。"""
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()

        # ── Tushare (primary) ──
        try:
            frame = self._ts_stock_daily(symbol, start, end, adjust)
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback 1) ──
        try:
            fetcher = self._ak_function("stock_zh_a_hist")
            return self.cached_call(
                f"cn_market:stock_daily:{symbol}:{period}:{adjust}:{start}:{end}",
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
                    f"cn_market:yahoo_stock_daily:{ticker}:{period}",
                    yf.Ticker(ticker).history,
                    period="3y" if period == "daily" else period,
                    interval="1d",
                    auto_adjust=False,
                )
            except Exception:
                raise primary_exc

    def _ts_stock_daily(self, symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame | None:
        """Tushare daily + adj_factor → 前/后复权 OHLCV。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"cn_market:ts_stock_daily:v2:{ts_code}:{start}:{end}:{adjust}"
        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        raw = self._ts_call("daily", ts_code=ts_code, start_date=start, end_date=end)
        if raw is None or raw.empty:
            return None

        if "amount" in raw.columns:
            raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 1000.0

        if adjust in ("qfq", "hfq"):
            adj = self._ts_call("adj_factor", ts_code=ts_code, start_date=start, end_date=end)
            if adj is not None and not adj.empty:
                raw = raw.merge(adj[["trade_date", "adj_factor"]], on="trade_date", how="left")
                raw["adj_factor"] = raw["adj_factor"].ffill().bfill()
                if adjust == "qfq":
                    latest_factor = raw["adj_factor"].iloc[0]  # 按 trade_date 降序
                    ratio = raw["adj_factor"] / latest_factor
                elif adjust == "hfq":
                    ratio = raw["adj_factor"]
                else:
                    ratio = 1.0
                for col in ("open", "high", "low", "close"):
                    raw[col] = raw[col] * ratio

        frame = raw.rename(columns={
            "trade_date": "日期", "open": "开盘", "high": "最高",
            "low": "最低", "close": "收盘", "vol": "成交量", "amount": "成交额",
        })
        frame["日期"] = pd.to_datetime(frame["日期"], format="%Y%m%d")
        frame = frame.sort_values("日期").reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return frame

    # ── ETF 日 K ──────────────────────────────────────────────

    def get_etf_daily(
        self,
        symbol: str,
        period: str = "daily",
        adjust: str = "qfq",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """ETF 日 K 线。Tushare fund_daily 优先。"""
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()

        # ── Tushare (primary) ──
        try:
            frame = self._ts_etf_daily(symbol, start, end, adjust)
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback 1) ──
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
            market_cfg = dict(self.config or {}).get("market", {})
            allow_yahoo_fallback = bool(market_cfg.get("enable_yahoo_fallback_for_cn_etf", False))
            if yf is None or not allow_yahoo_fallback:
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

    def _ts_etf_daily(self, symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame | None:
        """Tushare fund_daily → ETF OHLCV。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"cn_market:ts_etf_daily:v2:{ts_code}:{start}:{end}:{adjust}"
        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        raw = self._ts_call("fund_daily", ts_code=ts_code, start_date=start, end_date=end)
        if raw is None or raw.empty:
            return None

        if "amount" in raw.columns:
            raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 1000.0

        if adjust in ("qfq", "hfq"):
            adj = self._ts_call("adj_factor", ts_code=ts_code, start_date=start, end_date=end)
            if adj is not None and not adj.empty:
                raw = raw.merge(adj[["trade_date", "adj_factor"]], on="trade_date", how="left")
                raw["adj_factor"] = raw["adj_factor"].ffill().bfill()
                if adjust == "qfq":
                    latest_factor = raw["adj_factor"].iloc[0]
                    ratio = raw["adj_factor"] / latest_factor
                else:
                    ratio = raw["adj_factor"]
                for col in ("open", "high", "low", "close"):
                    raw[col] = raw[col] * ratio

        frame = raw.rename(columns={
            "trade_date": "日期", "open": "开盘", "high": "最高",
            "low": "最低", "close": "收盘", "vol": "成交量", "amount": "成交额",
        })
        frame["日期"] = pd.to_datetime(frame["日期"], format="%Y%m%d")
        frame = frame.sort_values("日期").reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return frame

    # ── 指数日 K ──────────────────────────────────────────────

    def get_index_daily(
        self,
        symbol: str,
        period: str = "daily",
        start_date: str = "",
        end_date: str = "",
        proxy_symbol: str = "",
    ) -> pd.DataFrame:
        """A 股指数历史行情。Tushare index_daily 优先。"""
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()

        # ── Tushare (primary) ──
        try:
            frame = self._ts_index_daily(symbol, start, end)
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback) ──
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

    def _ts_index_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """Tushare index_daily。指数代码需要按指数规则尝试 SH/SZ 后缀。"""
        for ts_code in self._ts_index_code_candidates(symbol):
            cache_key = f"cn_market:ts_index_daily:{ts_code}:{start}:{end}"
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

            raw = self._ts_call("index_daily", ts_code=ts_code, start_date=start, end_date=end)
            if raw is None or raw.empty:
                continue

            frame = raw.rename(columns={
                "trade_date": "日期", "open": "开盘", "high": "最高",
                "low": "最低", "close": "收盘", "vol": "成交量", "amount": "成交额",
            })
            frame["日期"] = pd.to_datetime(frame["日期"], format="%Y%m%d")
            frame = frame.sort_values("日期").reset_index(drop=True)
            self._save_cache(cache_key, frame)
            return frame
        return None

    # ── 开放式基金净值 ────────────────────────────────────────

    def get_open_fund_daily(
        self,
        symbol: str,
        indicator: str = "单位净值走势",
        period: str = "3年",
        proxy_symbol: str = "",
    ) -> pd.DataFrame:
        """开放式基金净值走势。Tushare fund_nav 优先。"""
        # ── Tushare (primary) ──
        try:
            frame = self._ts_fund_nav(symbol)
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback) ──
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

    def _ts_fund_nav(self, symbol: str) -> pd.DataFrame | None:
        """Tushare fund_nav → 基金净值历史。"""
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("O", "L", "E"))
        cache_key = f"cn_market:ts_fund_nav:{ts_code}"
        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        raw = self._ts_call("fund_nav", ts_code=ts_code)
        if raw is None or raw.empty:
            return None

        date_col = "nav_date" if "nav_date" in raw.columns else "end_date"
        if date_col not in raw.columns or "unit_nav" not in raw.columns:
            return None
        nav = raw[[date_col, "unit_nav"]].copy()
        nav.columns = ["date", "close"]
        nav["date"] = pd.to_datetime(nav["date"], format="%Y%m%d", errors="coerce")
        nav["close"] = pd.to_numeric(nav["close"], errors="coerce")
        nav = nav.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        if nav.empty:
            return None
        for column in ("open", "high", "low"):
            nav[column] = nav["close"]
        nav["volume"] = 0.0
        nav["amount"] = np.nan
        result = nav[["date", "open", "high", "low", "close", "volume", "amount"]]
        self._save_cache(cache_key, result)
        return result

    # ── 实时行情（Tushare daily_basic 替代实时快照） ───────────

    def get_stock_realtime(self) -> pd.DataFrame:
        """A 股全市场行情快照（含代码、名称、市值、PE、PB 等）。

        Tushare daily_basic 提供更精准的 PE/PB/PS/换手率/市值数据。
        但 daily_basic 是收盘后更新，盘中仍需 akshare 实时行情兜底。
        """
        # ── Tushare daily_basic (primary — 收盘后数据更精准) ──
        try:
            frame = self._ts_daily_basic_snapshot()
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback — 盘中实时) ──
        try:
            fetcher = self._ak_function("stock_zh_a_spot_em")
            return self.cached_call("cn_market:stock_realtime", fetcher, ttl_hours=1)
        except Exception as primary_exc:
            if ef is None:
                raise primary_exc
            try:
                frame = self.cached_call(
                    "cn_market:stock_realtime:efinance",
                    ef.stock.get_realtime_quotes,
                    ttl_hours=1,
                )
                return self._normalize_efinance_stock_realtime(frame)
            except Exception:
                raise primary_exc

    def _ts_daily_basic_snapshot(self) -> pd.DataFrame | None:
        """Tushare 全市场快照。

        ``daily_basic`` 只提供估值/换手/市值等收盘后字段，不包含 ``名称``、``行业``、
        ``成交额``。为了让机会池可以直接用这份快照做 A 股选股，需要额外补齐：

        - ``stock_basic``: ``name`` / ``industry``
        - ``daily``: ``amount``（Tushare 单位为千元，这里统一换算成元）
        """
        cache_key = "cn_market:ts_daily_basic_snapshot:v3"
        cached = self._load_cache(cache_key, ttl_hours=1)
        if cached is not None:
            return cached

        trade_date = self._latest_open_trade_date() or self._date_str()
        raw = self._ts_call("daily_basic", trade_date=trade_date)
        if raw is None or raw.empty:
            # 可能还没收盘，尝试前一个交易日
            raw = self._ts_call("daily_basic", trade_date=self._date_str(-1))
        if raw is None or raw.empty:
            return None

        frame = raw.rename(columns={
            "ts_code": "ts_code_raw",
            "trade_date": "trade_date_raw",
            "close": "最新价",
            "turnover_rate": "换手率",
            "turnover_rate_f": "换手率(自由)",
            "volume_ratio": "量比",
            "pe": "市盈率(动态)",
            "pe_ttm": "市盈率TTM",
            "pb": "市净率",
            "ps": "市销率",
            "ps_ttm": "市销率TTM",
            "dv_ratio": "股息率",
            "dv_ttm": "股息率TTM",
            "total_share": "总股本",
            "float_share": "流通股本",
            "total_mv": "总市值",
            "circ_mv": "流通市值",
        })
        frame["代码"] = frame["ts_code_raw"].apply(self._from_ts_code)
        for column in ("总市值", "流通市值"):
            if column in frame.columns:
                # Tushare daily_basic uses 万元; normalize to 元 so downstream
                # liquidity / market-cap filters are comparable across sources.
                frame[column] = pd.to_numeric(frame[column], errors="coerce") * 10_000.0

        snapshot_trade_date = str(raw["trade_date"].iloc[0])

        stock_basic_cache_key = "cn_market:ts_stock_basic_snapshot:v1"
        stock_basic = self._load_cache(stock_basic_cache_key, ttl_hours=24)
        if stock_basic is None:
            stock_basic = self._ts_call(
                "stock_basic",
                exchange="",
                list_status="L",
                fields="ts_code,name,industry",
            )
            if stock_basic is not None and not stock_basic.empty:
                self._save_cache(stock_basic_cache_key, stock_basic)
        if stock_basic is not None and not stock_basic.empty:
            basics = stock_basic.rename(columns={"name": "名称", "industry": "行业"})
            frame = frame.merge(basics[["ts_code", "名称", "行业"]], left_on="ts_code_raw", right_on="ts_code", how="left")
            frame = frame.drop(columns=["ts_code"])
        if "名称" not in frame.columns:
            frame["名称"] = frame["代码"]
        if "行业" not in frame.columns:
            frame["行业"] = ""

        daily_cache_key = f"cn_market:ts_daily_snapshot:{snapshot_trade_date}:v1"
        daily = self._load_cache(daily_cache_key, ttl_hours=1)
        if daily is None:
            daily = self._ts_call("daily", trade_date=snapshot_trade_date)
            if daily is not None and not daily.empty:
                self._save_cache(daily_cache_key, daily)
        if daily is not None and not daily.empty:
            daily_view = daily[["ts_code", "amount"]].copy()
            # Tushare daily.amount unit is 千元; normalize to 元 to align with the
            # rest of the codebase and liquidity thresholds.
            daily_view["成交额"] = pd.to_numeric(daily_view["amount"], errors="coerce") * 1000.0
            frame = frame.merge(daily_view[["ts_code", "成交额"]], left_on="ts_code_raw", right_on="ts_code", how="left")
            frame = frame.drop(columns=["ts_code"])

        if "成交额" not in frame.columns or frame["成交额"].isna().all():
            circ_mv = pd.to_numeric(frame.get("流通市值"), errors="coerce")
            turnover = pd.to_numeric(frame.get("换手率"), errors="coerce")
            frame["成交额"] = circ_mv * (turnover / 100.0)

        self._save_cache(cache_key, frame)
        return frame

    def get_stock_industry(self, symbol: str) -> str:
        """查询个股所属行业。Tushare stock_basic 优先。"""
        ts_code = self._to_ts_code(symbol)
        try:
            raw = self._ts_call("stock_basic", ts_code=ts_code, fields="ts_code,name,industry")
            if raw is not None and not raw.empty:
                industry = raw.iloc[0].get("industry", "")
                if industry:
                    return str(industry)
        except Exception:
            pass

        # AKShare fallback
        try:
            fetcher = self._ak_function("stock_individual_info_em")
            df = self.cached_call(f"cn_market:stock_info:{symbol}", fetcher, symbol=symbol, ttl_hours=24)
            row = df[df["item"] == "行业"]
            if not row.empty:
                return str(row.iloc[0]["value"])
        except Exception:
            pass
        return ""

    def get_etf_realtime(self) -> pd.DataFrame:
        """ETF 实时行情。"""
        try:
            fetcher = self._ak_function("fund_etf_spot_em")
            return self.cached_call("cn_market:etf_realtime", fetcher, ttl_hours=0)
        except Exception as primary_exc:
            if ef is None:
                raise primary_exc
            try:
                frame = self.cached_call(
                    "cn_market:etf_realtime:efinance",
                    ef.fund.get_realtime_quotes,
                    ttl_hours=0,
                )
                return self._normalize_efinance_etf_realtime(frame)
            except Exception:
                raise primary_exc

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
        """北向 / 南向资金净流入。Tushare moneyflow_hsgt 优先。"""
        # ── Tushare (primary) ──
        try:
            frame = self._ts_north_south_flow()
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback) ──
        fetcher = self._ak_function("stock_hsgt_fund_flow_summary_em")
        raw = self.cached_call("cn_market:north_south_flow:v2", fetcher, ttl_hours=0)
        return self._normalize_north_south_flow_frame(raw, source="akshare")

    def _ts_north_south_flow(self) -> pd.DataFrame | None:
        """Tushare moneyflow_hsgt — 沪深港通资金流向。"""
        cache_key = "cn_market:ts_north_south_flow:v2"
        cached = self._load_cache(cache_key, ttl_hours=1)
        if cached is not None:
            return cached

        start = self._date_str(-30)
        end = self._date_str()
        raw = self._ts_call("moneyflow_hsgt", start_date=start, end_date=end)
        if raw is None or raw.empty:
            return None
        normalized = self._normalize_north_south_flow_frame(raw, source="tushare")
        if normalized.empty:
            return None
        self._save_cache(cache_key, normalized)
        return normalized

    def get_margin_trading(self) -> pd.DataFrame:
        """融资融券汇总数据。Tushare margin 优先。"""
        # ── Tushare (primary) ──
        try:
            frame = self._ts_margin()
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback) ──
        try:
            frame = self._ak_margin_summary()
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass
        return pd.DataFrame()

    def get_share_float(self, symbol: str, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """A股限售股解禁日历。Tushare share_float 优先。"""
        start = start_date or self._date_str()
        end = end_date or self._date_str(90)

        try:
            frame = self._ts_share_float(symbol, start, end)
            if frame is not None:
                return frame
        except Exception:
            pass
        return pd.DataFrame()

    def get_unlock_pressure(self, symbol: str, as_of: str = "", lookahead_days: int = 90) -> dict[str, Any]:
        """Summarize upcoming unlock pressure for an A-share symbol.

        Status policy:
        - ``✅``: no explicit unlock within the lookahead window or only very small unlocks
        - ``⚠️``: moderate unlock pressure in the next 30/90 days
        - ``❌``: large unlock pressure in the next 30 days
        - ``ℹ️``: data source unavailable
        """

        as_of_ts = pd.Timestamp(as_of or datetime.now().strftime("%Y-%m-%d")).normalize()
        start = as_of_ts.strftime("%Y%m%d")
        end = (as_of_ts + timedelta(days=lookahead_days)).strftime("%Y%m%d")
        frame = self._ts_share_float(symbol, start, end)
        if frame is None:
            return {
                "supported": False,
                "status": "ℹ️",
                "detail": "Tushare share_float 当前不可用，解禁压力暂未纳入本轮检查",
                "next_date": "",
                "ratio_30d": None,
                "ratio_90d": None,
                "share_30d": None,
                "share_90d": None,
            }

        if frame.empty:
            return {
                "supported": True,
                "status": "✅",
                "detail": f"未来 {lookahead_days} 日未见明确限售股解禁安排",
                "next_date": "",
                "ratio_30d": 0.0,
                "ratio_90d": 0.0,
                "share_30d": 0.0,
                "share_90d": 0.0,
            }

        future = frame.copy()
        future["float_date"] = pd.to_datetime(future["float_date"], errors="coerce")
        future = future.dropna(subset=["float_date"])
        future = future[future["float_date"] >= as_of_ts].sort_values(["float_date", "float_ratio"], ascending=[True, False])
        if future.empty:
            return {
                "supported": True,
                "status": "✅",
                "detail": f"未来 {lookahead_days} 日未见明确限售股解禁安排",
                "next_date": "",
                "ratio_30d": 0.0,
                "ratio_90d": 0.0,
                "share_30d": 0.0,
                "share_90d": 0.0,
            }

        future["days_until"] = (future["float_date"] - as_of_ts).dt.days
        next_date = future["float_date"].iloc[0]
        next_rows = future[future["float_date"] == next_date]
        window_30 = future[future["days_until"] <= 30]
        window_90 = future[future["days_until"] <= lookahead_days]

        ratio_30 = round(float(pd.to_numeric(window_30.get("float_ratio"), errors="coerce").fillna(0).sum()), 4)
        ratio_90 = round(float(pd.to_numeric(window_90.get("float_ratio"), errors="coerce").fillna(0).sum()), 4)
        share_30 = round(float(pd.to_numeric(window_30.get("float_share"), errors="coerce").fillna(0).sum()), 4)
        share_90 = round(float(pd.to_numeric(window_90.get("float_share"), errors="coerce").fillna(0).sum()), 4)
        next_ratio = round(float(pd.to_numeric(next_rows.get("float_ratio"), errors="coerce").fillna(0).sum()), 4)
        next_share_types = [str(item).strip() for item in next_rows.get("share_type", pd.Series(dtype=str)).tolist() if str(item).strip()]
        next_share_types = list(dict.fromkeys(next_share_types))
        share_type_text = "、".join(next_share_types[:2]) if next_share_types else "未披露解禁类型"

        share_30_yi = share_30 / 100_000_000.0
        if ratio_30 >= 5.0:
            status = "❌"
            detail = (
                f"未来 30 日预计解禁约 {ratio_30:.2f}%（约 {share_30_yi:.2f} 亿股）；"
                f"最近一次在 {next_date.strftime('%Y-%m-%d')}，主要为 {share_type_text}"
            )
        elif ratio_30 >= 1.0:
            status = "⚠️"
            detail = (
                f"未来 30 日预计解禁约 {ratio_30:.2f}%（约 {share_30_yi:.2f} 亿股）；"
                f"最近一次在 {next_date.strftime('%Y-%m-%d')}，主要为 {share_type_text}"
            )
        elif ratio_90 >= 5.0:
            status = "⚠️"
            detail = (
                f"未来 30 日无明显解禁，但未来 {lookahead_days} 日累计约 {ratio_90:.2f}%（最近一次 "
                f"{next_date.strftime('%Y-%m-%d')}，单次约 {next_ratio:.2f}%）"
            )
        else:
            status = "✅"
            detail = (
                f"未来 30 日无明显解禁；最近一次在 {next_date.strftime('%Y-%m-%d')}，单次约 {next_ratio:.2f}%"
            )

        return {
            "supported": True,
            "status": status,
            "detail": detail,
            "next_date": next_date.strftime("%Y-%m-%d"),
            "next_ratio": next_ratio,
            "ratio_30d": ratio_30,
            "ratio_90d": ratio_90,
            "share_30d": share_30,
            "share_90d": share_90,
        }

    def _ts_margin(self) -> pd.DataFrame | None:
        """Tushare margin — 融资融券汇总。"""
        cache_key = "cn_market:ts_margin:v2"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None:
            return cached

        for offset in range(0, 6):
            trade_date = self._date_str(-offset)
            raw = self._ts_call("margin", trade_date=trade_date)
            if raw is not None and not raw.empty:
                normalized = self._normalize_margin_summary_frame(raw, source="tushare")
                if normalized.empty:
                    continue
                self._save_cache(cache_key, normalized)
                return normalized
        return None

    def _ts_share_float(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """Tushare share_float — A股限售股解禁日历。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"cn_market:ts_share_float:v1:{ts_code}:{start}:{end}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call(
            "share_float",
            ts_code=ts_code,
            start_date=start,
            end_date=end,
            fields="ts_code,ann_date,float_date,float_share,float_ratio,holder_name,share_type",
        )
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "ann_date", "float_date", "float_share", "float_ratio", "holder_name", "share_type"])
            self._save_cache(cache_key, empty)
            return empty

        frame = raw.copy()
        for column in ("ann_date", "float_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("float_share", "float_ratio"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values(["float_date", "float_ratio"], ascending=[True, False]).reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return frame

    def _ak_margin_summary(self) -> pd.DataFrame | None:
        """AKShare 融资融券汇总兜底，统一为与 Tushare 一致的汇总列。"""
        sse_fetcher = self._ak_function("stock_margin_sse")
        szse_fetcher = self._ak_function("stock_margin_szse")

        for offset in range(0, 6):
            date_str = self._date_str(offset_days=-offset)
            frames: list[pd.DataFrame] = []

            try:
                sse_raw = self.cached_call(
                    f"cn_market:margin_sse:v2:{date_str}",
                    sse_fetcher,
                    start_date=date_str,
                    end_date=date_str,
                )
                sse_frame = self._normalize_margin_summary_frame(sse_raw, source="akshare_sse", default_date=date_str)
                if not sse_frame.empty:
                    frames.append(sse_frame)
            except Exception:
                pass

            try:
                szse_raw = self.cached_call(
                    f"cn_market:margin_szse:v2:{date_str}",
                    szse_fetcher,
                    date=date_str,
                )
                szse_frame = self._normalize_margin_summary_frame(szse_raw, source="akshare_szse", default_date=date_str)
                if not szse_frame.empty:
                    frames.append(szse_frame)
            except Exception:
                pass

            if frames:
                return pd.concat(frames, ignore_index=True)
        return None

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

    # ── 内部工具方法 ──────────────────────────────────────────

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

    # --- efinance fallback normalization helpers ---

    def _normalize_efinance_stock_realtime(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Map efinance stock realtime columns to akshare-compatible names."""
        if frame is None or frame.empty:
            return pd.DataFrame()
        col_map = {
            "股票代码": "代码",
            "股票名称": "名称",
            "动态市盈率": "市盈率(动态)",
            # 成交额, 总市值, 流通市值 already match
        }
        return frame.rename(columns=col_map)

    def _normalize_efinance_etf_realtime(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Map efinance stock quotes to ETF realtime format, filtering ETF codes."""
        if frame is None or frame.empty:
            return pd.DataFrame()
        col_map = {
            "股票代码": "代码",
            "股票名称": "名称",
            "动态市盈率": "市盈率(动态)",
        }
        result = frame.rename(columns=col_map)
        code_col = "代码"
        if code_col in result.columns:
            result = result[result[code_col].astype(str).str.match(r"^[15]\d{5}$", na=False)]
        return result.reset_index(drop=True)
