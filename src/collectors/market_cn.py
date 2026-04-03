"""China market data collector — Tushare-first, AKShare only for realtime / uncovered side paths."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

import numpy as np
import pandas as pd

from .base import BaseCollector
from .index_topic import IndexTopicCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    ak = None

try:
    import efinance as ef
except ImportError:  # pragma: no cover
    ef = None


class ChinaMarketCollector(BaseCollector):
    """China market collector with Tushare-first research paths."""

    def _retry_tushare_history(self, fetcher: Callable[[], pd.DataFrame | None], attempts: int = 2) -> pd.DataFrame | None:
        last_exc: Exception | None = None
        for _ in range(max(int(attempts), 1)):
            try:
                frame = fetcher()
                if frame is not None and not frame.empty:
                    return frame
            except Exception as exc:  # pragma: no cover - retry branch
                last_exc = exc
        if last_exc is not None:
            raise last_exc
        return None

    def _tag_history_frame(self, frame: pd.DataFrame | None, source: str, label: str) -> pd.DataFrame | None:
        if frame is None:
            return None
        tagged = frame.copy()
        tagged.attrs["history_source"] = source
        tagged.attrs["history_source_label"] = label
        return tagged

    def _ak_function(self, name: str) -> Callable[..., Any]:
        if ak is None:
            raise RuntimeError("akshare is not installed")
        func = getattr(ak, name, None)
        if not callable(func):
            raise RuntimeError(f"AKShare function not available: {name}")
        return func

    def _date_str(self, offset_days: int = 0) -> str:
        return (datetime.now() + timedelta(days=offset_days)).strftime("%Y%m%d")

    def _index_topic_collector(self) -> IndexTopicCollector:
        return IndexTopicCollector(self.config)

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
            frame = self._retry_tushare_history(lambda: self._ts_stock_daily(symbol, start, end, adjust))
            if frame is not None and not frame.empty:
                return self._tag_history_frame(frame, "tushare", "Tushare 日线")
        except Exception:
            pass
        return pd.DataFrame()

    def get_stock_auction(self, symbol: str, trade_date: str = "") -> pd.DataFrame:
        """A股集合竞价快照。Tushare stk_auction 优先。"""
        try:
            frame = self._ts_stock_auction(symbol, trade_date=trade_date or self._latest_open_trade_date())
            if frame is not None:
                return frame
        except Exception:
            pass
        return pd.DataFrame()

    def get_stock_limit(self, symbol: str, trade_date: str = "") -> pd.DataFrame:
        """A股涨跌停边界。Tushare stk_limit 优先。"""
        try:
            frame = self._ts_stock_limit(symbol, trade_date=trade_date or self._latest_open_trade_date())
            if frame is not None:
                return frame
        except Exception:
            pass
        return pd.DataFrame()

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

    def _ts_stock_auction(self, symbol: str, trade_date: str) -> pd.DataFrame | None:
        """Tushare stk_auction — 集合竞价量价快照。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"cn_market:ts_stock_auction:v1:{ts_code}:{trade_date}"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached

        raw = self._ts_call("stk_auction", ts_code=ts_code, trade_date=str(trade_date).replace("-", ""))
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "trade_date", "vol", "price", "amount", "pre_close", "turnover_rate", "volume_ratio", "float_share"])
            self._save_cache(cache_key, empty)
            return empty
        frame = raw.copy()
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(self._normalize_date_text)
        for column in ("vol", "price", "amount", "pre_close", "turnover_rate", "volume_ratio", "float_share"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        self._save_cache(cache_key, frame)
        return frame

    def _ts_stock_limit(self, symbol: str, trade_date: str) -> pd.DataFrame | None:
        """Tushare stk_limit — A股涨跌停边界。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"cn_market:ts_stock_limit:v1:{ts_code}:{trade_date}"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached

        raw = self._ts_call("stk_limit", ts_code=ts_code, trade_date=str(trade_date).replace("-", ""))
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["trade_date", "ts_code", "up_limit", "down_limit"])
            self._save_cache(cache_key, empty)
            return empty
        frame = raw.copy()
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(self._normalize_date_text)
        for column in ("up_limit", "down_limit"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
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
        """ETF 日 K 线。研究主链统一使用 Tushare fund_daily。"""
        start = start_date or self._date_str(-365 * 3)
        end = end_date or self._date_str()

        frame = self._retry_tushare_history(lambda: self._ts_etf_daily(symbol, start, end, adjust))
        if frame is not None and not frame.empty:
            return self._tag_history_frame(frame, "tushare", "Tushare 日线")
        raise RuntimeError(f"Tushare ETF 日线当前不可用：{symbol}")

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
            adj = self._ts_fund_adj_snapshot(ts_code=ts_code, start_date=start, end_date=end)
            if adj is None or getattr(adj, "empty", False):
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

    def get_etf_universe_snapshot(self, trade_date: str = "") -> pd.DataFrame:
        """Tushare-first ETF universe snapshot for the latest open trade date.

        Returns a merged frame of ETF metadata, ETF share size and ``fund_daily(trade_date=...)``
        with yuan-denominated turnover and normalized symbol/date fields.
        """
        basic = self._ts_etf_basic_snapshot()
        if basic is None or getattr(basic, "empty", False):
            basic = self._ts_fund_basic_snapshot("E")
        if basic is None or getattr(basic, "empty", False):
            return pd.DataFrame()
        basic_frame = basic.copy()
        candidate_dates = [str(trade_date).replace("-", "")] if trade_date else list(reversed(self._recent_open_trade_dates()))
        for candidate in candidate_dates:
            latest_trade_date = str(candidate).replace("-", "")
            if not latest_trade_date:
                continue

            cache_key = f"cn_market:ts_etf_universe:v1:{latest_trade_date}"
            cached = self._load_cache(cache_key, ttl_hours=6)
            if cached is not None:
                return cached

            daily = self._ts_call("fund_daily", trade_date=latest_trade_date)
            if daily is None or getattr(daily, "empty", False):
                continue

            share_size = self._ts_etf_share_size_snapshot(trade_date=latest_trade_date)
            if share_size is None or getattr(share_size, "empty", False):
                share_size = None

            daily_frame = daily.copy()
            if "amount" in daily_frame.columns:
                daily_frame["amount"] = pd.to_numeric(daily_frame["amount"], errors="coerce") * 1000.0
            if "trade_date" in daily_frame.columns:
                daily_frame["trade_date"] = daily_frame["trade_date"].map(self._normalize_date_text)

            merged = daily_frame.merge(
                basic_frame,
                on="ts_code",
                how="left",
                suffixes=("", "_basic"),
            )
            if merged.empty:
                continue

            if share_size is not None and not share_size.empty and "ts_code" in share_size.columns:
                share_frame = share_size.copy()
                if "trade_date" in share_frame.columns:
                    share_frame["trade_date"] = share_frame["trade_date"].map(self._normalize_date_text)
                for column in ("total_share", "total_size", "nav", "close"):
                    if column in share_frame.columns:
                        share_frame[column] = pd.to_numeric(share_frame[column], errors="coerce")
                merged = merged.merge(
                    share_frame,
                    on="ts_code",
                    how="left",
                    suffixes=("", "_share"),
                )

            merged["symbol"] = merged["ts_code"].astype(str).str.split(".").str[0]
            if "csname" in merged.columns:
                merged["name"] = merged.get("name", pd.Series("", index=merged.index))
                merged["name"] = merged["name"].where(
                    merged["name"].astype(str).str.strip().ne(""),
                    merged["csname"].astype(str),
                )
            if "extname" in merged.columns:
                merged["name"] = merged["name"].where(
                    merged["name"].astype(str).str.strip().ne(""),
                    merged["extname"].astype(str),
                )
            if "cname" in merged.columns:
                merged["name"] = merged["name"].where(
                    merged["name"].astype(str).str.strip().ne(""),
                    merged["cname"].astype(str),
                )
            if "index_name" in merged.columns:
                merged["benchmark"] = merged.get("benchmark", pd.Series("", index=merged.index))
                merged["benchmark"] = merged["benchmark"].where(
                    merged["benchmark"].astype(str).str.strip().ne(""),
                    merged["index_name"].astype(str),
                )
            if "mgr_name" in merged.columns:
                merged["management"] = merged.get("management", pd.Series("", index=merged.index))
                merged["management"] = merged["management"].where(
                    merged["management"].astype(str).str.strip().ne(""),
                    merged["mgr_name"].astype(str),
                )
            if "etf_type" in merged.columns:
                merged["fund_type"] = merged.get("fund_type", pd.Series("", index=merged.index))
                merged["fund_type"] = merged["fund_type"].where(
                    merged["fund_type"].astype(str).str.strip().ne(""),
                    merged["etf_type"].astype(str),
                )
            if "total_share" in merged.columns and "total_size" in merged.columns:
                merged["ETF总份额"] = pd.to_numeric(merged["total_share"], errors="coerce")
                merged["ETF总规模"] = pd.to_numeric(merged["total_size"], errors="coerce")
            for column in (
                "pre_close",
                "open",
                "high",
                "low",
                "close",
                "change",
                "pct_chg",
                "vol",
                "amount",
                "issue_amount",
                "m_fee",
                "c_fee",
                "min_amount",
            ):
                if column in merged.columns:
                    merged[column] = pd.to_numeric(merged[column], errors="coerce")

            for column in ("found_date", "list_date", "issue_date", "delist_date", "setup_date"):
                if column in merged.columns:
                    merged[column] = merged[column].map(self._normalize_date_text)

            self._save_cache(cache_key, merged)
            return merged
        return pd.DataFrame()

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
            frame = self._index_topic_collector().get_index_history(
                symbol,
                period=period,
                start_date=start,
                end_date=end,
            )
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass
        if proxy_symbol and proxy_symbol != symbol:
            try:
                frame = self.get_etf_daily(proxy_symbol, period="daily", adjust="qfq", start_date=start, end_date=end)
                return self._tag_history_frame(frame, "proxy_etf", f"代理 ETF 日线回退（{proxy_symbol}）")
            except Exception:
                pass
        return pd.DataFrame()

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
                return self._tag_history_frame(frame, "tushare", "Tushare 基金净值")
        except Exception:
            pass

        if proxy_symbol and proxy_symbol != symbol:
            try:
                frame = self.get_etf_daily(proxy_symbol)
                return self._tag_history_frame(frame, "proxy_etf", f"代理 ETF 日线回退（{proxy_symbol}）")
            except Exception:
                pass
        return pd.DataFrame()

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

        bak_daily = self._ts_bak_daily_snapshot(snapshot_trade_date)
        if bak_daily is not None and not bak_daily.empty:
            bak_view = bak_daily.rename(
                columns={
                    "ts_code": "ts_code_bak",
                    "name": "名称(bak)",
                    "industry": "行业(bak)",
                    "vol_ratio": "量比(bak)",
                    "turn_over": "换手率(bak)",
                    "swing": "振幅",
                    "avg_price": "均价",
                    "strength": "强弱度",
                    "activity": "活跃度",
                    "attack": "攻击度",
                    "area": "地域",
                }
            ).copy()
            if "amount" in bak_view.columns:
                bak_view["成交额(bak)"] = pd.to_numeric(bak_view["amount"], errors="coerce") * 10_000.0
            frame = frame.merge(
                bak_view[
                    [
                        "ts_code_bak",
                        *[
                            column
                            for column in (
                                "名称(bak)",
                                "行业(bak)",
                                "量比(bak)",
                                "换手率(bak)",
                                "振幅",
                                "均价",
                                "强弱度",
                                "活跃度",
                                "攻击度",
                                "地域",
                                "成交额(bak)",
                            )
                            if column in bak_view.columns
                        ],
                    ]
                ],
                left_on="ts_code_raw",
                right_on="ts_code_bak",
                how="left",
            )
            if "ts_code_bak" in frame.columns:
                frame = frame.drop(columns=["ts_code_bak"])
            if "名称(bak)" in frame.columns:
                bak_name = frame["名称(bak)"]
                frame["名称"] = frame["名称"].where(
                    frame["名称"].astype(str).str.strip().ne("") & (frame["名称"].astype(str) != frame["代码"].astype(str)),
                    bak_name,
                )
            if "行业(bak)" in frame.columns:
                frame["行业"] = frame["行业"].replace("", pd.NA).fillna(frame["行业(bak)"]).fillna("")

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
            if "成交额(bak)" in frame.columns and not frame["成交额(bak)"].isna().all():
                frame["成交额"] = pd.to_numeric(frame["成交额(bak)"], errors="coerce")
            else:
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
        return pd.DataFrame()

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
        return pd.DataFrame()

    def get_stock_regulatory_risk_snapshot(
        self,
        symbol: str,
        *,
        as_of: str = "",
        lookback_days: int = 30,
        display_name: str = "",
    ) -> dict[str, Any]:
        """Unified Tushare-first stock risk snapshot for ST / high-shock / alert signals."""
        cleaned_symbol = str(symbol).strip()
        ts_code = self._to_ts_code(cleaned_symbol)
        as_of_ts = pd.Timestamp(as_of or datetime.now().strftime("%Y-%m-%d")).normalize()
        as_of_text = f"{as_of_ts.strftime('%Y-%m-%d')} 00:00:00"
        latest_trade_date = ""
        for trade_date in reversed(self._recent_open_trade_dates(lookback_days=max(int(lookback_days), 20))):
            normalized = self._normalize_date_text(trade_date)
            if normalized and normalized <= as_of_ts.strftime("%Y-%m-%d"):
                latest_trade_date = trade_date
                break
        if not latest_trade_date:
            latest_trade_date = as_of_ts.strftime("%Y%m%d")
        latest_trade_text = self._normalize_date_text(latest_trade_date)
        window_start = (as_of_ts - timedelta(days=max(int(lookback_days), 1))).strftime("%Y%m%d")
        window_end = as_of_ts.strftime("%Y%m%d")
        fallback_name_st = str(display_name or "").strip().upper().startswith(("ST", "*ST"))

        components: dict[str, dict[str, Any]] = {}
        active_st = False
        st_events: list[dict[str, Any]] = []
        high_shock_events: list[dict[str, Any]] = []
        alert_events: list[dict[str, Any]] = []

        st_daily_error: BaseException | None = None
        try:
            st_daily_raw = self._ts_stock_st_snapshot(latest_trade_date)
        except Exception as exc:  # noqa: BLE001
            st_daily_raw = None
            st_daily_error = exc
        st_daily_frame = self._normalize_stock_st_snapshot(st_daily_raw, trade_date=latest_trade_date)
        st_daily_match = st_daily_frame[st_daily_frame["ts_code"] == ts_code].reset_index(drop=True) if not st_daily_frame.empty else pd.DataFrame()
        st_daily_diagnosis = "live"
        st_daily_fallback = "none"
        if st_daily_error is not None:
            st_daily_diagnosis = self._tushare_failure_diagnosis(st_daily_error)
        elif st_daily_raw is None:
            st_daily_diagnosis = "unavailable"
        elif st_daily_frame.empty:
            st_daily_diagnosis = "empty"
        if not st_daily_match.empty:
            active_st = True
            row = st_daily_match.iloc[0]
            detail = f"{latest_trade_text} 仍在 `{str(row.get('type_name', '') or row.get('type', 'ST')).strip()}` 名单内"
            status = "❌"
            disclosure = "Tushare stock_st 命中当前 ST 风险警示板名单。"
        elif st_daily_diagnosis == "empty":
            detail = f"{latest_trade_text} 未命中 ST 风险警示板名单"
            status = "✅"
            disclosure = "Tushare stock_st 当前未命中该股票。"
        else:
            st_daily_fallback = "name_prefix_only" if fallback_name_st else "none"
            detail = (
                "名称前缀仍显示 ST / *ST，本轮先按保守口径处理"
                if fallback_name_st
                else "当前未拿到 ST 风险警示板日度名单"
            )
            status = "❌" if fallback_name_st else "ℹ️"
            disclosure = self._blocked_disclosure(st_daily_diagnosis, source="Tushare stock_st")
        components["stock_st"] = {
            "source": "tushare.stock_st",
            "as_of": latest_trade_text,
            "fallback": st_daily_fallback,
            "diagnosis": st_daily_diagnosis,
            "disclosure": disclosure,
            "status": status,
            "detail": detail,
            "active": bool(active_st or (status == "❌" and st_daily_fallback == "name_prefix_only")),
            "items": st_daily_match.to_dict("records") if not st_daily_match.empty else [],
        }

        st_error: BaseException | None = None
        try:
            st_raw = self._ts_st_events(ts_code)
        except Exception as exc:  # noqa: BLE001
            st_raw = None
            st_error = exc
        st_frame = self._normalize_st_events(st_raw)
        st_window = pd.DataFrame()
        st_diagnosis = "live"
        if st_error is not None:
            st_diagnosis = self._tushare_failure_diagnosis(st_error)
        elif st_raw is None:
            st_diagnosis = "unavailable"
        elif st_frame.empty:
            st_diagnosis = "empty"
        if not st_frame.empty:
            st_frame["imp_date_ts"] = pd.to_datetime(st_frame["imp_date"], errors="coerce")
            st_window = st_frame[
                st_frame["imp_date_ts"].notna()
                & (st_frame["imp_date_ts"] <= as_of_ts)
            ].sort_values("imp_date_ts", ascending=False).reset_index(drop=True)
            st_events = st_window.drop(columns=["imp_date_ts"], errors="ignore").to_dict("records")
            if not active_st and not st_window.empty:
                latest = st_window.iloc[0]
                st_type_text = str(latest.get("st_type", "")).strip()
                reason_text = " ".join(
                    [
                        st_type_text,
                        str(latest.get("st_reason", "")).strip(),
                        str(latest.get("st_explain", "")).strip(),
                    ]
                )
                if not any(token in reason_text for token in ("撤销", "摘帽", "解除")):
                    active_st = True
        if st_window.empty:
            st_detail = "近窗口未命中 ST 变更记录" if st_diagnosis == "empty" else "当前未拿到 ST 变更记录"
            st_status = "✅" if st_diagnosis == "empty" else ("❌" if active_st else "ℹ️")
        else:
            latest = st_window.iloc[0]
            st_detail = (
                f"{str(latest.get('imp_date', '')).strip()} 最近一次 ST 变更为 `{str(latest.get('st_type', '')).strip() or '未标注'}`；"
                f"原因：{str(latest.get('st_reason', '')).strip() or '未披露'}"
            )
            st_status = "❌" if active_st else "⚠️"
        components["st"] = {
            "source": "tushare.st",
            "as_of": as_of_text,
            "fallback": "none",
            "diagnosis": st_diagnosis,
            "disclosure": (
                "Tushare st 提供 ST 风险警示板变更原因与实施日期。"
                if st_diagnosis in {"live", "empty"}
                else self._blocked_disclosure(st_diagnosis, source="Tushare st")
            ),
            "status": st_status,
            "detail": st_detail,
            "active": active_st,
            "items": st_events[:5],
        }

        high_shock_error: BaseException | None = None
        try:
            high_shock_raw = self._ts_stock_high_shock(ts_code, window_start, window_end)
        except Exception as exc:  # noqa: BLE001
            high_shock_raw = None
            high_shock_error = exc
        high_shock_frame = self._normalize_stock_regulatory_events(high_shock_raw, date_column="trade_date")
        high_shock_diagnosis = "live"
        if high_shock_error is not None:
            high_shock_diagnosis = self._tushare_failure_diagnosis(high_shock_error)
        elif high_shock_raw is None:
            high_shock_diagnosis = "unavailable"
        elif high_shock_frame.empty:
            high_shock_diagnosis = "empty"
        high_shock_events = high_shock_frame.to_dict("records") if not high_shock_frame.empty else []
        if high_shock_events:
            latest = high_shock_events[0]
            high_shock_status = "⚠️"
            high_shock_detail = (
                f"{str(latest.get('event_date', '')).strip()} 命中严重异常波动："
                f"{str(latest.get('reason', '')).strip() or '未披露原因'}"
            )
        else:
            high_shock_status = "✅" if high_shock_diagnosis == "empty" else "ℹ️"
            high_shock_detail = "近窗口未命中严重异常波动" if high_shock_diagnosis == "empty" else "当前未拿到严重异常波动记录"
        components["stk_high_shock"] = {
            "source": "tushare.stk_high_shock",
            "as_of": as_of_text,
            "fallback": "none",
            "diagnosis": high_shock_diagnosis,
            "disclosure": (
                "Tushare stk_high_shock 提供交易所严重异常波动公告。"
                if high_shock_diagnosis in {"live", "empty"}
                else self._blocked_disclosure(high_shock_diagnosis, source="Tushare stk_high_shock")
            ),
            "status": high_shock_status,
            "detail": high_shock_detail,
            "count": len(high_shock_events),
            "items": high_shock_events[:5],
        }

        alert_error: BaseException | None = None
        try:
            alert_raw = self._ts_stock_alert(ts_code, window_start, window_end)
        except Exception as exc:  # noqa: BLE001
            alert_raw = None
            alert_error = exc
        alert_frame = self._normalize_stock_regulatory_events(alert_raw, date_column="start_date", end_column="end_date")
        alert_diagnosis = "live"
        if alert_error is not None:
            alert_diagnosis = self._tushare_failure_diagnosis(alert_error)
        elif alert_raw is None:
            alert_diagnosis = "unavailable"
        elif alert_frame.empty:
            alert_diagnosis = "empty"
        alert_events = alert_frame.to_dict("records") if not alert_frame.empty else []
        active_alerts = [
            item
            for item in alert_events
            if str(item.get("event_date", "")).strip() <= as_of_ts.strftime("%Y-%m-%d")
            and (not str(item.get("end_date", "")).strip() or str(item.get("end_date", "")).strip() >= as_of_ts.strftime("%Y-%m-%d"))
        ]
        if active_alerts:
            latest = active_alerts[0]
            alert_status = "⚠️"
            alert_detail = (
                f"{str(latest.get('event_date', '')).strip()} 起被列入 `{str(latest.get('type', '')).strip() or '交易所重点提示证券'}`，"
                f"参考截至 {str(latest.get('end_date', '')).strip() or '未披露'}"
            )
        elif alert_events:
            latest = alert_events[0]
            alert_status = "⚠️"
            alert_detail = (
                f"近窗口曾被列入 `{str(latest.get('type', '')).strip() or '交易所重点提示证券'}`，"
                f"起始 {str(latest.get('event_date', '')).strip()}"
            )
        else:
            alert_status = "✅" if alert_diagnosis == "empty" else "ℹ️"
            alert_detail = "近窗口未命中交易所重点提示证券" if alert_diagnosis == "empty" else "当前未拿到交易所重点提示记录"
        components["stk_alert"] = {
            "source": "tushare.stk_alert",
            "as_of": as_of_text,
            "fallback": "none",
            "diagnosis": alert_diagnosis,
            "disclosure": (
                "Tushare stk_alert 提供交易所重点提示证券名单。"
                if alert_diagnosis in {"live", "empty"}
                else self._blocked_disclosure(alert_diagnosis, source="Tushare stk_alert")
            ),
            "status": alert_status,
            "detail": alert_detail,
            "count": len(alert_events),
            "active_count": len(active_alerts),
            "items": alert_events[:5],
        }

        fallback = "name_prefix_only" if components["stock_st"]["fallback"] == "name_prefix_only" else "none"
        diagnoses = {str(component.get("diagnosis", "")) for component in components.values()}
        blocked_diagnoses = {"unavailable", "permission_blocked", "rate_limited", "network_error", "fetch_error"}
        if active_st:
            overall_status = "❌"
            detail = "当前仍处于 ST 风险警示板，直接抬高退市与交易约束风险。"
        elif high_shock_events and active_alerts:
            overall_status = "⚠️"
            detail = "近窗口同时命中严重异常波动与交易所重点提示，短线波动与监管关注都偏高。"
        elif high_shock_events:
            overall_status = "⚠️"
            detail = "近窗口命中过严重异常波动，需要把它当成高波动样本而不是普通强势股。"
        elif active_alerts:
            overall_status = "⚠️"
            detail = "当前仍在交易所重点提示证券名单内，短线执行需要更保守。"
        elif diagnoses.issubset(blocked_diagnoses):
            overall_status = "ℹ️"
            detail = "Tushare 股票风险专题接口当前不可用，本轮不把缺口写成通过。"
        elif diagnoses & blocked_diagnoses:
            overall_status = "ℹ️"
            detail = "股票风险专题部分缺失，本轮只做保守披露，不把缺口写成通过。"
        else:
            overall_status = "✅"
            detail = f"截至 {latest_trade_text} 未命中 ST 风险警示、严重异常波动或交易所重点提示。"

        disclosure_lines = [
            str(component.get("disclosure", "")).strip()
            for component in components.values()
            if str(component.get("disclosure", "")).strip()
        ]
        return {
            "source": "tushare.stock_st+tushare.st+tushare.stk_high_shock+tushare.stk_alert",
            "as_of": as_of_text,
            "fallback": fallback,
            "status": overall_status,
            "detail": detail,
            "disclosure": "；".join(dict.fromkeys(disclosure_lines)),
            "active_st": active_st,
            "high_shock_count": len(high_shock_events),
            "alert_count": len(alert_events),
            "active_alert_count": len(active_alerts),
            "components": components,
        }

    def get_stock_margin_snapshot(
        self,
        symbol: str,
        *,
        as_of: str = "",
        lookback_days: int = 20,
        display_name: str = "",
    ) -> dict[str, Any]:
        """Unified Tushare-first stock margin snapshot for crowding/risk checks."""
        cleaned_symbol = str(symbol).strip()
        ts_code = self._to_ts_code(cleaned_symbol)
        as_of_ts = pd.Timestamp(as_of or datetime.now().strftime("%Y-%m-%d")).normalize()
        as_of_text = f"{as_of_ts.strftime('%Y-%m-%d')} 00:00:00"
        latest_trade_date = ""
        for trade_date in reversed(self._recent_open_trade_dates(lookback_days=max(int(lookback_days), 20))):
            normalized = self._normalize_date_text(trade_date)
            if normalized and normalized <= as_of_ts.strftime("%Y-%m-%d"):
                latest_trade_date = trade_date
                break
        if not latest_trade_date:
            latest_trade_date = as_of_ts.strftime("%Y%m%d")
        latest_trade_text = self._normalize_date_text(latest_trade_date)
        window_start = (as_of_ts - timedelta(days=max(int(lookback_days), 1))).strftime("%Y%m%d")

        margin_error: BaseException | None = None
        try:
            margin_raw = self._ts_margin_detail_snapshot(
                ts_code=ts_code,
                start_date=window_start,
                end_date=latest_trade_date,
            )
        except Exception as exc:  # noqa: BLE001
            margin_raw = None
            margin_error = exc
        margin_frame = self._normalize_margin_detail_frame(margin_raw)
        margin_frame = margin_frame[margin_frame["ts_code"] == ts_code].sort_values("日期").reset_index(drop=True) if not margin_frame.empty else pd.DataFrame()
        diagnosis = "live"
        if margin_error is not None:
            diagnosis = self._tushare_failure_diagnosis(margin_error)
        elif margin_raw is None:
            diagnosis = "unavailable"
        elif margin_frame.empty:
            diagnosis = "empty"

        latest_row: dict[str, Any] = {}
        latest_date = ""
        is_fresh = False
        fin_balance = None
        margin_balance = None
        sec_balance = None
        fin_buy = None
        fin_repay = None
        buy_repay_ratio = None
        five_day_change_pct = None
        three_day_change_pct = None
        crowding_level = "unknown"

        if not margin_frame.empty:
            latest_row = dict(margin_frame.iloc[-1])
            latest_date = str(latest_row.get("日期", "")).strip()
            is_fresh = bool(latest_date and latest_date == latest_trade_text)
            if diagnosis == "live" and not is_fresh:
                diagnosis = "stale"

            def _coerce_latest(key: str) -> float | None:
                series = pd.to_numeric(pd.Series([latest_row.get(key)]), errors="coerce").dropna()
                return None if series.empty else float(series.iloc[0])

            fin_balance = _coerce_latest("融资余额")
            margin_balance = _coerce_latest("融资融券余额")
            sec_balance = _coerce_latest("融券余额")
            fin_buy = _coerce_latest("融资买入额")
            fin_repay = _coerce_latest("融资偿还额")
            if fin_buy is not None and fin_repay is not None and fin_repay > 0:
                buy_repay_ratio = fin_buy / fin_repay

            fin_series = pd.to_numeric(margin_frame.get("融资余额", pd.Series(dtype=float)), errors="coerce").dropna()
            if len(fin_series) >= 2:
                tail_three = fin_series.tail(min(3, len(fin_series)))
                base_three = float(tail_three.iloc[0] or 0.0)
                if base_three > 0:
                    three_day_change_pct = (float(tail_three.iloc[-1]) - base_three) / base_three
                tail_five = fin_series.tail(min(5, len(fin_series)))
                base_five = float(tail_five.iloc[0] or 0.0)
                if base_five > 0:
                    five_day_change_pct = (float(tail_five.iloc[-1]) - base_five) / base_five

            if is_fresh:
                if (five_day_change_pct is not None and five_day_change_pct >= 0.12) or (
                    buy_repay_ratio is not None and buy_repay_ratio >= 1.30
                ):
                    crowding_level = "high"
                elif (five_day_change_pct is not None and five_day_change_pct >= 0.05) or (
                    buy_repay_ratio is not None and buy_repay_ratio >= 1.10
                ):
                    crowding_level = "medium"
                elif five_day_change_pct is not None and five_day_change_pct <= -0.05:
                    crowding_level = "relieved"
                else:
                    crowding_level = "neutral"

        component = {
            "source": "tushare.margin_detail",
            "as_of": latest_date or latest_trade_text,
            "fallback": "none",
            "diagnosis": diagnosis,
            "disclosure": (
                "Tushare margin_detail 提供个股融资融券余额与当日买卖变动。"
                if diagnosis in {"live", "empty", "stale"}
                else self._blocked_disclosure(diagnosis, source="Tushare margin_detail")
            ),
            "status": "matched" if latest_row else "empty" if diagnosis == "empty" else "blocked",
            "detail": (
                f"{latest_date} 融资余额 {fin_balance:,.0f} 元 / 融资买入 {fin_buy:,.0f} 元 / 融资偿还 {fin_repay:,.0f} 元"
                if latest_row and fin_balance is not None and fin_buy is not None and fin_repay is not None
                else "当前未命中可用个股两融明细。"
            ),
            "item": latest_row,
        }

        stock_name = str(display_name or cleaned_symbol).strip() or cleaned_symbol
        if latest_row and is_fresh and crowding_level == "high":
            status = "⚠️"
            detail = (
                f"{stock_name} 两融拥挤度偏高：近 5 个样本融资余额变化 "
                f"{(five_day_change_pct or 0.0):+.1%}，当日融资买入/偿还约 {buy_repay_ratio:.2f}x。"
                if buy_repay_ratio is not None
                else f"{stock_name} 两融拥挤度偏高：近 5 个样本融资余额变化 {(five_day_change_pct or 0.0):+.1%}。"
            )
        elif latest_row and is_fresh and crowding_level == "medium":
            status = "⚠️"
            detail = (
                f"{stock_name} 两融资金仍在升温：近 5 个样本融资余额变化 {(five_day_change_pct or 0.0):+.1%}，"
                "短线需要防融资盘一致性交易放大波动。"
            )
        elif latest_row and is_fresh and crowding_level == "relieved":
            status = "✅"
            detail = f"{stock_name} 两融余额近窗口回落，短线融资盘拥挤度有所释放。"
        elif latest_row and is_fresh:
            status = "✅"
            detail = f"{stock_name} 两融余额未见明显拥挤式抬升，当前更偏中性观察。"
        elif latest_row:
            status = "ℹ️"
            detail = f"{stock_name} 个股两融明细最新停在 {latest_date or '未知'}，当前不按 fresh 命中处理。"
        elif diagnosis in {"permission_blocked", "rate_limited", "network_error", "fetch_error", "unavailable"}:
            status = "ℹ️"
            detail = "个股两融明细当前不可用，本轮不把缺口写成融资盘已经退潮。"
        else:
            status = "ℹ️"
            detail = "当前未命中可稳定使用的个股两融明细。"

        return {
            "source": "tushare.margin_detail",
            "as_of": as_of_text,
            "latest_date": latest_date or latest_trade_text,
            "fallback": "none",
            "diagnosis": diagnosis,
            "status": status,
            "is_fresh": is_fresh,
            "detail": detail,
            "disclosure": component["disclosure"],
            "crowding_level": crowding_level,
            "fin_balance": fin_balance,
            "margin_balance": margin_balance,
            "sec_balance": sec_balance,
            "fin_buy": fin_buy,
            "fin_repay": fin_repay,
            "buy_repay_ratio": buy_repay_ratio,
            "five_day_change_pct": five_day_change_pct,
            "three_day_change_pct": three_day_change_pct,
            "components": {"margin_detail": component},
        }

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

    def _ts_stock_st_snapshot(self, trade_date: str) -> pd.DataFrame | None:
        trade_date = str(trade_date).replace("-", "").strip()
        if not trade_date:
            return None
        cache_key = f"cn_market:ts_stock_st:v1:{trade_date}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("stock_st", trade_date=trade_date)
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "name", "trade_date", "type", "type_name"])
            self._save_cache(cache_key, empty)
            return empty
        self._save_cache(cache_key, raw)
        return raw

    def _normalize_stock_st_snapshot(self, frame: pd.DataFrame | None, trade_date: str = "") -> pd.DataFrame:
        if frame is None:
            return pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=["ts_code", "name", "trade_date", "type", "type_name"])
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        name_col = self._first_existing_column(working, ("name", "名称"))
        type_col = self._first_existing_column(working, ("type", "st_type"))
        type_name_col = self._first_existing_column(working, ("type_name", "st_type_name"))
        if ts_code_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str),
                "name": working.get(name_col, pd.Series("", index=working.index)).astype(str),
                "trade_date": working.get("trade_date", pd.Series(trade_date, index=working.index)).map(self._normalize_date_text),
                "type": working.get(type_col, pd.Series("", index=working.index)).astype(str),
                "type_name": working.get(type_name_col, pd.Series("", index=working.index)).astype(str),
            }
        )
        return normalized.drop_duplicates("ts_code").reset_index(drop=True)

    def _ts_st_events(self, ts_code: str) -> pd.DataFrame | None:
        ts_code = str(ts_code).strip()
        if not ts_code:
            return None
        cache_key = f"cn_market:ts_st:v1:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("st", ts_code=ts_code)
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "name", "pub_date", "imp_date", "st_tpye", "st_reason", "st_explain"])
            self._save_cache(cache_key, empty)
            return empty
        self._save_cache(cache_key, raw)
        return raw

    def _normalize_st_events(self, frame: pd.DataFrame | None) -> pd.DataFrame:
        if frame is None:
            return pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=["ts_code", "name", "pub_date", "imp_date", "st_type", "st_reason", "st_explain"])
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        name_col = self._first_existing_column(working, ("name", "名称"))
        st_type_col = self._first_existing_column(working, ("st_type", "st_tpye", "type"))
        if ts_code_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str),
                "name": working.get(name_col, pd.Series("", index=working.index)).astype(str),
                "pub_date": working.get("pub_date", pd.Series("", index=working.index)).map(self._normalize_date_text),
                "imp_date": working.get("imp_date", pd.Series("", index=working.index)).map(self._normalize_date_text),
                "st_type": working.get(st_type_col, pd.Series("", index=working.index)).astype(str),
                "st_reason": working.get("st_reason", pd.Series("", index=working.index)).astype(str),
                "st_explain": working.get("st_explain", pd.Series("", index=working.index)).astype(str),
            }
        )
        return normalized.drop_duplicates(["ts_code", "imp_date", "st_type"], keep="first").reset_index(drop=True)

    def _ts_stock_high_shock(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        cache_key = f"cn_market:ts_stk_high_shock:v1:{ts_code}:{start_date}:{end_date}"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached
        raw = self._ts_call("stk_high_shock", ts_code=ts_code, start_date=start_date, end_date=end_date)
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "trade_date", "name", "trade_market", "reason", "period"])
            self._save_cache(cache_key, empty)
            return empty
        self._save_cache(cache_key, raw)
        return raw

    def _ts_stock_alert(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        cache_key = f"cn_market:ts_stk_alert:v1:{ts_code}:{start_date}:{end_date}"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached
        raw = self._ts_call("stk_alert", ts_code=ts_code, start_date=start_date, end_date=end_date)
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date", "type"])
            self._save_cache(cache_key, empty)
            return empty
        self._save_cache(cache_key, raw)
        return raw

    def _normalize_stock_regulatory_events(
        self,
        frame: pd.DataFrame | None,
        *,
        date_column: str,
        end_column: str = "",
    ) -> pd.DataFrame:
        if frame is None:
            return pd.DataFrame()
        if frame.empty:
            columns = ["ts_code", "name", "event_date", "market", "reason", "period", "end_date", "type"]
            return pd.DataFrame(columns=columns)
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        name_col = self._first_existing_column(working, ("name", "名称"))
        market_col = self._first_existing_column(working, ("trade_market", "market"))
        reason_col = self._first_existing_column(working, ("reason", "remark"))
        period_col = self._first_existing_column(working, ("period",))
        type_col = self._first_existing_column(working, ("type",))
        if ts_code_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str),
                "name": working.get(name_col, pd.Series("", index=working.index)).astype(str),
                "event_date": working.get(date_column, pd.Series("", index=working.index)).map(self._normalize_date_text),
                "market": working.get(market_col, pd.Series("", index=working.index)).astype(str),
                "reason": working.get(reason_col, pd.Series("", index=working.index)).astype(str),
                "period": working.get(period_col, pd.Series("", index=working.index)).astype(str),
                "end_date": working.get(end_column, pd.Series("", index=working.index)).map(self._normalize_date_text) if end_column else "",
                "type": working.get(type_col, pd.Series("", index=working.index)).astype(str),
            }
        )
        normalized = normalized[normalized["event_date"].astype(str) != ""].copy()
        return normalized.sort_values("event_date", ascending=False).reset_index(drop=True)

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

    def _ts_bak_daily_snapshot(self, trade_date: str) -> pd.DataFrame | None:
        """Tushare bak_daily — 带增强字段的全市场日度快照。"""
        cache_key = f"cn_market:ts_bak_daily_snapshot:v1:{trade_date}"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached

        raw = self._ts_call("bak_daily", trade_date=str(trade_date).replace("-", ""))
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame()
            self._save_cache(cache_key, empty)
            return empty

        frame = raw.copy()
        numeric_columns = (
            "pct_change",
            "close",
            "change",
            "open",
            "high",
            "low",
            "pre_close",
            "vol_ratio",
            "turn_over",
            "swing",
            "vol",
            "amount",
            "total_share",
            "float_share",
            "pe",
            "float_mv",
            "total_mv",
            "avg_price",
            "strength",
            "activity",
            "avg_turnover",
            "attack",
        )
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        self._save_cache(cache_key, frame)
        return frame

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
