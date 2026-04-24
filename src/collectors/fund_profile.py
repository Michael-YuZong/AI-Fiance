"""Open-end fund profile collector — Tushare-first for fund/ETF core facts, AKShare only for uncovered open-end detail."""

from __future__ import annotations

import io
import re
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from .base import BaseCollector
from src.utils.fund_taxonomy import build_standard_fund_taxonomy, infer_fund_sector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


FUND_THEME_RULES = [
    (("科技", "半导体", "芯片", "ai", "人工智能", "软件", "算力", "恒生科技"), ("科技", ["AI算力", "半导体", "成长股估值修复"])),
    (("军工", "国防", "航天", "卫星", "商业航天"), ("军工", ["军工", "地缘风险", "商业航天"])),
    (("黄金", "贵金属"), ("黄金", ["黄金", "通胀预期"])),
    (("电网", "电力", "储能", "特高压"), ("电网", ["AI算力", "电力需求", "电网设备"])),
    (("有色", "铜", "铝", "黄金股"), ("有色", ["铜铝", "顺周期"])),
    (("医药", "医疗", "创新药"), ("医药", ["医药", "老龄化"])),
    (("农业", "农牧", "农林", "粮食", "粮油", "种业", "种植", "农化", "化肥", "农资", "粮食安全", "乡村振兴"), ("农业", ["粮食安全", "种业", "农化"])),
    (("消费", "食品", "饮料", "家电", "零售"), ("消费", ["内需", "消费修复"])),
    (("红利", "高股息", "股息", "银行", "公用事业"), ("高股息", ["高股息", "防守"])),
    (("能源", "原油", "煤炭", "油气"), ("能源", ["原油", "能源安全", "通胀预期"])),
    (
        (
            "沪深300",
            "中证a500",
            "中证500",
            "上证50",
            "上证综指",
            "上证综合",
            "上证指数",
            "恒生指数",
            "恒生中国企业指数",
            "国企指数",
            "恒指",
            "hang seng index",
        ),
        ("宽基", ["宽基", "大盘蓝筹", "内需"]),
    ),
]

_PROCESS_SHARED_FRAME_CACHE: Dict[str, pd.DataFrame] = {}


def _theme_detection_text(text: str) -> str:
    cleaned = str(text or "")
    for noise in (
        "中国人民银行人民币活期存款利率",
        "银行活期存款利率",
        "人民币活期存款利率",
        "活期存款税后利率",
        "活期存款利率",
        "税后",
    ):
        cleaned = cleaned.replace(noise, " ")
    return cleaned


class FundProfileCollector(BaseCollector):
    """场外基金画像数据采集。

    Primary Tushare feeds:
    - fund_basic
    - fund_nav
    - fund_portfolio
    - fund_manager
    - fund_company
    - fund_div

    AKShare remains the richer fallback for:
    - open-end fund overview text fields
    - holdings names / industry allocation when Tushare holdings are absent
    - manager AUM / peer-fund enrichment
    - rating tables

    For ETFs, overview/benchmark/share-size are expected to come from:
    - etf_basic
    - etf_index
    - etf_share_size

    Once those Tushare feeds are available, ETF main-path research should not
    keep a parallel AKShare overview / holdings chain.
    """

    def _ak_function(self, name: str):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        func = getattr(ak, name, None)
        if not callable(func):
            raise RuntimeError(f"AKShare function not available: {name}")
        return func

    def _shared_frame_cache(self, key: str) -> pd.DataFrame | None:
        frame = _PROCESS_SHARED_FRAME_CACHE.get(str(key))
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return frame
        return None

    def _remember_shared_frame(self, key: str, frame: pd.DataFrame) -> pd.DataFrame:
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            _PROCESS_SHARED_FRAME_CACHE[str(key)] = frame
        return frame

    # ── Tushare: 基金基础信息 ─────────────────────────────────

    def get_fund_basic(self, market: str = "O") -> pd.DataFrame:
        """Tushare fund_basic — 公募基金列表与管理人信息。

        market: E=场内, O=场外/开放式, L=LOF
        """
        process_cached = self._shared_frame_cache(f"ts_fund_basic:{market}")
        if process_cached is not None:
            return process_cached
        cache_key = f"fund_profile:ts_fund_basic:{market}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return self._remember_shared_frame(f"ts_fund_basic:{market}", cached)
        raw = self._ts_call("fund_basic", market=market)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return self._remember_shared_frame(f"ts_fund_basic:{market}", raw)
        return pd.DataFrame()

    def get_etf_basic_ts(self, symbol: str = "") -> pd.DataFrame:
        """Tushare etf_basic — ETF 基础信息。"""
        cache_suffix = str(symbol).strip() or "all"
        process_cached = self._shared_frame_cache(f"ts_etf_basic:{cache_suffix}")
        if process_cached is not None:
            return process_cached
        cache_key = f"fund_profile:ts_etf_basic:{cache_suffix}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return self._remember_shared_frame(f"ts_etf_basic:{cache_suffix}", cached)
        raw = self._ts_etf_basic_snapshot()
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        if symbol and "ts_code" in frame.columns:
            resolved = self._resolve_tushare_etf_code(symbol, preferred_markets=("E", "O", "L"))
            bare = str(resolved).split(".")[0]
            matched = frame[frame["ts_code"].astype(str).str.startswith(f"{bare}.", na=False)]
            if matched.empty:
                return pd.DataFrame()
            frame = matched.reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return self._remember_shared_frame(f"ts_etf_basic:{cache_suffix}", frame.reset_index(drop=True))

    def get_etf_index_ts(self, symbol: str = "") -> pd.DataFrame:
        """Tushare etf_index — ETF 基准指数列表。"""
        cache_suffix = str(symbol).strip() or "all"
        process_cached = self._shared_frame_cache(f"ts_etf_index:{cache_suffix}")
        if process_cached is not None:
            return process_cached
        cache_key = f"fund_profile:ts_etf_index:{cache_suffix}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return self._remember_shared_frame(f"ts_etf_index:{cache_suffix}", cached)
        raw = self._ts_etf_index_snapshot()
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        if symbol:
            basic = self.get_etf_basic_ts(symbol)
            index_code = ""
            if not basic.empty and "index_code" in basic.columns:
                index_code = str(basic.iloc[0].get("index_code", "")).strip()
            if not index_code or "ts_code" not in frame.columns:
                return pd.DataFrame()
            matched = frame[frame["ts_code"].astype(str) == index_code]
            if matched.empty:
                return pd.DataFrame()
            frame = matched.reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return self._remember_shared_frame(f"ts_etf_index:{cache_suffix}", frame.reset_index(drop=True))

    def get_etf_share_size_ts(self, symbol: str, trade_date: str = "", start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """Tushare etf_share_size — ETF 份额和规模数据。"""
        cache_suffix = f"{symbol}:{trade_date}:{start_date}:{end_date}".strip(":")
        process_cached = self._shared_frame_cache(f"ts_etf_share_size:{cache_suffix}")
        if process_cached is not None:
            return process_cached
        ts_code = self._resolve_tushare_etf_code(symbol, preferred_markets=("E", "O", "L"))
        trade_date = str(trade_date).replace("-", "").strip()
        start_date = str(start_date).replace("-", "").strip()
        end_date = str(end_date).replace("-", "").strip()
        if not trade_date and not start_date and not end_date:
            recent_open_dates = self._recent_open_trade_dates(lookback_days=21)
            if recent_open_dates:
                end_date = recent_open_dates[-1]
                start_idx = max(len(recent_open_dates) - 7, 0)
                start_date = recent_open_dates[start_idx]
            else:
                trade_date = self._latest_open_trade_date()
        cache_key = f"fund_profile:ts_etf_share_size:{ts_code}:{trade_date}:{start_date}:{end_date}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return self._remember_shared_frame(f"ts_etf_share_size:{cache_suffix}", cached)
        raw = self._ts_etf_share_size_snapshot(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(self._normalize_compact_date)
        for column in ("total_share", "total_size", "nav", "close"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if "trade_date" in frame.columns:
            frame = frame.sort_values("trade_date", ascending=False, na_position="last")
        self._save_cache(cache_key, frame)
        return self._remember_shared_frame(f"ts_etf_share_size:{cache_suffix}", frame.reset_index(drop=True))

    def get_fund_factor_pro_ts(
        self,
        symbol: str,
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """Tushare fund_factor_pro — 场内基金/ETF 技术面因子数据。"""
        cache_suffix = f"{symbol}:{trade_date}:{start_date}:{end_date}".strip(":")
        process_cached = self._shared_frame_cache(f"ts_fund_factor_pro:{cache_suffix}")
        if process_cached is not None:
            return process_cached
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("E", "O", "L"))
        explicit_trade_date = str(trade_date).replace("-", "").strip()
        explicit_start_date = str(start_date).replace("-", "").strip()
        explicit_end_date = str(end_date).replace("-", "").strip()
        trade_date = explicit_trade_date
        start_date = explicit_start_date
        end_date = explicit_end_date
        if not trade_date and not start_date and not end_date:
            recent_open_dates = self._recent_open_trade_dates(lookback_days=14)
            if recent_open_dates:
                end_date = recent_open_dates[-1]
                start_date = recent_open_dates[-2] if len(recent_open_dates) >= 2 else recent_open_dates[-1]
            else:
                trade_date = self._latest_open_trade_date()
        cache_key = f"fund_profile:ts_fund_factor_pro:{ts_code}:{trade_date}:{start_date}:{end_date}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return self._remember_shared_frame(f"ts_fund_factor_pro:{cache_suffix}", cached)
        raw = self._ts_fund_factor_pro_snapshot(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(self._normalize_compact_date)
            frame = frame.sort_values("trade_date", ascending=False, na_position="last")
        elif "date" in frame.columns:
            frame["date"] = frame["date"].map(self._normalize_compact_date)
            frame = frame.sort_values("date", ascending=False, na_position="last")
        for column in frame.columns:
            if column in {"ts_code", "trade_date", "date", "fund_name", "name"}:
                continue
            if frame[column].dtype == object:
                numeric = pd.to_numeric(frame[column], errors="coerce")
                if numeric.notna().any():
                    frame[column] = numeric
        latest_date_text = ""
        if not frame.empty:
            latest_date_text = str(frame.iloc[0].get("trade_date", frame.iloc[0].get("date", ""))).strip()
        latest_date = BaseCollector._normalize_date_text(latest_date_text)
        is_fresh = False
        if explicit_trade_date or explicit_start_date or explicit_end_date:
            is_fresh = not frame.empty
        elif latest_date:
            try:
                latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
            except ValueError:
                latest_dt = None
            if latest_dt is not None:
                is_fresh = abs((datetime.now().date() - latest_dt.date()).days) <= 4
        frame.attrs["source"] = "tushare.fund_factor_pro"
        frame.attrs["as_of"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = latest_date
        frame.attrs["is_fresh"] = is_fresh
        frame.attrs["fallback"] = "none"
        frame.attrs["disclosure"] = "场内基金技术面因子来自 Tushare fund_factor_pro；空表或受限时按缺失处理，不伪装成 fresh。"
        self._save_cache(cache_key, frame)
        return self._remember_shared_frame(f"ts_fund_factor_pro:{cache_suffix}", frame.reset_index(drop=True))

    def get_fund_nav_ts(self, symbol: str) -> pd.DataFrame:
        """Tushare fund_nav — 基金历史净值。"""
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("O", "L", "E"))
        cache_key = f"fund_profile:ts_fund_nav:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_nav", ts_code=ts_code)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def get_fund_portfolio_ts(self, symbol: str) -> pd.DataFrame:
        """Tushare fund_portfolio — 基金季度持仓。"""
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("O", "L", "E"))
        cache_key = f"fund_profile:ts_fund_portfolio:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_portfolio", ts_code=ts_code)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def get_fund_manager_ts(self, symbol: str) -> pd.DataFrame:
        """Tushare fund_manager — 基金经理任职与简历。"""
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("O", "L", "E"))
        cache_key = f"fund_profile:ts_fund_manager:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_manager", ts_code=ts_code)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def get_fund_company_ts(self) -> pd.DataFrame:
        """Tushare fund_company — 基金公司目录。"""
        process_cached = self._shared_frame_cache("ts_fund_company")
        if process_cached is not None:
            return process_cached
        cache_key = "fund_profile:ts_fund_company"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return self._remember_shared_frame("ts_fund_company", cached)
        raw = self._ts_call("fund_company")
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return self._remember_shared_frame("ts_fund_company", raw)
        return pd.DataFrame()

    def get_stock_basic_ts(self) -> pd.DataFrame:
        """Tushare stock_basic — 个股名称映射，用于 ETF 持仓去 AKShare 化。"""
        process_cached = self._shared_frame_cache("ts_stock_basic")
        if process_cached is not None:
            return process_cached
        cache_key = "fund_profile:ts_stock_basic"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return self._remember_shared_frame("ts_stock_basic", cached)
        raw = self._ts_call(
            "stock_basic",
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name",
        )
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return self._remember_shared_frame("ts_stock_basic", raw)
        return pd.DataFrame()

    def get_fund_div_ts(self, symbol: str) -> pd.DataFrame:
        """Tushare fund_div — 基金分红记录。"""
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("O", "L", "E"))
        cache_key = f"fund_profile:ts_fund_div:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_div", ts_code=ts_code)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def get_fund_sales_ratio_ts(self, year: str = "") -> pd.DataFrame:
        """Tushare fund_sales_ratio — 各渠道公募基金销售保有规模占比。"""
        normalized_year = str(year).strip()
        cache_suffix = normalized_year or "latest"
        process_cached = self._shared_frame_cache(f"ts_fund_sales_ratio:{cache_suffix}")
        if process_cached is not None:
            return process_cached
        cache_key = f"fund_profile:ts_fund_sales_ratio:{cache_suffix}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return self._remember_shared_frame(f"ts_fund_sales_ratio:{cache_suffix}", cached)
        kwargs = {"year": normalized_year} if normalized_year else {}
        raw = self._ts_call("fund_sales_ratio", **kwargs)
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = raw.copy()
        if "year" in frame.columns:
            frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
            frame = frame.sort_values("year", ascending=False, na_position="last")
        for column in ("bank", "sec_comp", "fund_comp", "indep_comp", "rests"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        latest_year = self._normalize_sales_ratio_year(frame.iloc[0].get("year") if not frame.empty else "")
        latest_date = f"{latest_year}-12-31" if latest_year else ""
        current_year = datetime.now().year
        is_fresh = bool(latest_year) and current_year - int(latest_year) <= 1
        frame.attrs["source"] = "tushare.fund_sales_ratio"
        frame.attrs["as_of"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = latest_date
        frame.attrs["latest_year"] = latest_year
        frame.attrs["is_fresh"] = is_fresh
        frame.attrs["fallback"] = "none"
        frame.attrs["disclosure"] = "各渠道公募基金销售保有规模占比来自 Tushare fund_sales_ratio；年度更新。空表或旧年不伪装成 fresh。"
        self._save_cache(cache_key, frame)
        return self._remember_shared_frame(f"ts_fund_sales_ratio:{cache_suffix}", frame.reset_index(drop=True))

    def get_overview(self, symbol: str) -> pd.DataFrame:
        fetcher = self._ak_function("fund_overview_em")
        return self.cached_call(
            f"fund_profile:overview:{symbol}",
            fetcher,
            symbol=symbol,
            ttl_hours=24,
            prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
        )

    def get_achievement(self, symbol: str) -> pd.DataFrame:
        fetcher = self._ak_function("fund_individual_achievement_xq")
        return self.cached_call(
            f"fund_profile:achievement:{symbol}",
            fetcher,
            symbol=symbol,
            ttl_hours=12,
            prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
        )

    def get_asset_allocation(self, symbol: str) -> pd.DataFrame:
        fetcher = self._ak_function("fund_individual_detail_hold_xq")
        return self.cached_call(
            f"fund_profile:asset_mix:{symbol}",
            fetcher,
            symbol=symbol,
            ttl_hours=12,
            prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
        )

    def get_portfolio_hold(self, symbol: str, years: Optional[Sequence[str]] = None) -> pd.DataFrame:
        raw_fetcher = self._ak_function("fund_portfolio_hold_em")

        def fetcher(**kwargs) -> pd.DataFrame:
            try:
                return raw_fetcher(**kwargs)
            except Exception as exc:
                if self._is_known_empty_detail_error(exc):
                    return pd.DataFrame()
                raise

        for year in years or self._year_candidates():
            try:
                frame = self.cached_call(
                    f"fund_profile:holdings:{symbol}:{year}",
                    fetcher,
                    symbol=symbol,
                    date=year,
                    ttl_hours=24,
                    prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
                )
            except Exception:
                continue
            latest = self._latest_quarter_frame(frame, "季度")
            if not latest.empty:
                return latest.reset_index(drop=True)
        return pd.DataFrame()

    def get_industry_allocation(self, symbol: str, years: Optional[Sequence[str]] = None) -> pd.DataFrame:
        raw_fetcher = self._ak_function("fund_portfolio_industry_allocation_em")

        def fetcher(**kwargs) -> pd.DataFrame:
            try:
                return raw_fetcher(**kwargs)
            except Exception as exc:
                if self._is_known_empty_detail_error(exc):
                    return pd.DataFrame()
                raise

        for year in years or self._year_candidates():
            try:
                frame = self.cached_call(
                    f"fund_profile:industry:{symbol}:{year}",
                    fetcher,
                    symbol=symbol,
                    date=year,
                    ttl_hours=24,
                    prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
                )
            except Exception:
                continue
            latest = self._latest_cutoff_frame(frame, "截止时间")
            if not latest.empty:
                return latest.reset_index(drop=True)
        return pd.DataFrame()

    def get_manager_directory(self) -> pd.DataFrame:
        process_cached = self._shared_frame_cache("manager_directory")
        if process_cached is not None:
            return process_cached

        def fetcher() -> pd.DataFrame:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                return self._ak_function("fund_manager_em")()

        return self._remember_shared_frame(
            "manager_directory",
            self.cached_call(
            "fund_profile:manager_directory",
            fetcher,
            ttl_hours=24,
            prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
            ),
        )

    def get_rating_table(self) -> pd.DataFrame:
        process_cached = self._shared_frame_cache("rating_all")
        if process_cached is not None:
            return process_cached
        fetcher = self._ak_function("fund_rating_all")
        return self._remember_shared_frame(
            "rating_all",
            self.cached_call(
                "fund_profile:rating_all",
                fetcher,
                ttl_hours=24,
                prefer_stale=bool(getattr(self, "_profile_prefer_stale", False)),
            ),
        )

    def collect_profile(self, symbol: str, asset_type: str = "cn_fund", profile_mode: str = "full") -> Dict[str, Any]:
        notes: List[str] = []
        mode = str(profile_mode or "full").strip().lower() or "full"
        light_mode = mode == "light"
        previous_prefer_stale = bool(getattr(self, "_profile_prefer_stale", False))
        self._profile_prefer_stale = asset_type == "cn_etf"
        try:
            etf_basic_df = self._safe_frame(self.get_etf_basic_ts, symbol) if asset_type == "cn_etf" else pd.DataFrame()
            etf_index_df = self._safe_frame(self.get_etf_index_ts, symbol) if asset_type == "cn_etf" else pd.DataFrame()
            etf_share_size_df = self._safe_frame(self.get_etf_share_size_ts, symbol) if asset_type == "cn_etf" else pd.DataFrame()
            fund_factor_df = self._safe_frame(self.get_fund_factor_pro_ts, symbol) if asset_type in {"cn_etf", "cn_fund"} else pd.DataFrame()
            ts_manager_df = self._safe_frame(self.get_fund_manager_ts, symbol) if not light_mode else pd.DataFrame()
            ts_company_df = self._safe_frame(self.get_fund_company_ts) if not light_mode else pd.DataFrame()
            ts_div_df = self._safe_frame(self.get_fund_div_ts, symbol) if not light_mode else pd.DataFrame()
            ts_holdings_df = self._safe_frame(self.get_fund_portfolio_ts, symbol) if not light_mode else pd.DataFrame()
            has_tushare_holdings = isinstance(ts_holdings_df, pd.DataFrame) and not ts_holdings_df.empty
            stock_basic_df = (
                self._safe_frame(self.get_stock_basic_ts)
                if not light_mode and (asset_type == "cn_etf" or has_tushare_holdings)
                else pd.DataFrame()
            )
            if asset_type == "cn_etf":
                should_load_overview_fallback = (
                    not light_mode
                    and self._should_load_etf_overview_enrichment(
                        symbol,
                        etf_basic_df=etf_basic_df,
                        etf_index_df=etf_index_df,
                        etf_share_size_df=etf_share_size_df,
                    )
                )
                overview_df = self._safe_frame(self.get_overview, symbol) if should_load_overview_fallback else pd.DataFrame()
            else:
                overview_df = self._safe_frame(self.get_overview, symbol)
            achievement_df = self._safe_frame(self.get_achievement, symbol) if not light_mode else pd.DataFrame()
            asset_mix_df = self._safe_frame(self.get_asset_allocation, symbol) if not light_mode else pd.DataFrame()
            ak_holdings_df = pd.DataFrame()
            if not light_mode and not has_tushare_holdings and asset_type == "cn_fund":
                ak_holdings_df = self._safe_frame(self.get_portfolio_hold, symbol)
            industry_df = self._safe_frame(self.get_industry_allocation, symbol) if not light_mode else pd.DataFrame()
            manager_df = self._safe_frame(self.get_manager_directory) if not light_mode else pd.DataFrame()
            rating_df = self._safe_frame(self.get_rating_table) if not light_mode else pd.DataFrame()
        finally:
            self._profile_prefer_stale = previous_prefer_stale

        overview = overview_df.iloc[0].to_dict() if not overview_df.empty else {}
        overview = self._merge_overview_with_tushare(
            overview,
            symbol,
            asset_type=asset_type,
            etf_basic_df=etf_basic_df,
            etf_index_df=etf_index_df,
            etf_share_size_df=etf_share_size_df,
        )
        if not overview:
            notes.append("基金概况缺失")
        achievement = self._achievement_snapshot(achievement_df)
        merged_holdings = self._merge_holdings(ts_holdings_df, ak_holdings_df, stock_basic_df=stock_basic_df)
        top_holdings = self._top_holdings(merged_holdings)
        top_industries = self._top_industries(industry_df)
        asset_mix = self._asset_mix(asset_mix_df)
        if not asset_mix and not asset_mix_df.empty:
            notes.append("基金资产配置口径异常，已降级移除，避免把坏数据当成仓位结论。")
        rating = self._rating_snapshot(rating_df, symbol)
        manager = self._manager_snapshot(ts_manager_df, manager_df, overview)
        company = self._company_snapshot(ts_company_df, overview)
        dividends = self._dividend_snapshot(ts_div_df)
        if manager and not str(overview.get("基金经理人", "")).strip():
            overview["基金经理人"] = str(manager.get("name", "")).strip()
        style = self._derive_style(overview, top_holdings, top_industries, asset_mix, manager, asset_type=asset_type)
        sales_ratio_snapshot = self.get_fund_sales_ratio_snapshot() if asset_type == "cn_fund" else {}

        if not top_holdings:
            notes.append("基金持仓明细缺失")
        if not manager:
            notes.append("基金经理画像缺失")
        if not company:
            notes.append("基金公司画像缺失")
        if not rating:
            notes.append("基金评级缺失")
        etf_snapshot = (
            self._build_etf_snapshot(
                overview,
                etf_basic_df=etf_basic_df,
                etf_index_df=etf_index_df,
                etf_share_size_df=etf_share_size_df,
            )
            if asset_type == "cn_etf"
            else {}
        )
        fund_factor_snapshot = self._build_fund_factor_snapshot(fund_factor_df)

        return {
            "overview": overview,
            "achievement": achievement,
            "top_holdings": top_holdings,
            "industry_allocation": top_industries,
            "asset_allocation": asset_mix,
            "manager": manager,
            "company": company,
            "dividends": dividends,
            "rating": rating,
            "style": style,
            "sales_ratio_snapshot": sales_ratio_snapshot,
            "latest_quarter": str(top_holdings[0].get("季度", "")) if top_holdings else "",
            "etf_snapshot": etf_snapshot,
            "fund_factor_snapshot": fund_factor_snapshot,
            "profile_mode": mode,
            "notes": notes,
        }

    def _should_load_etf_overview_enrichment(
        self,
        symbol: str,
        *,
        etf_basic_df: pd.DataFrame | None = None,
        etf_index_df: pd.DataFrame | None = None,
        etf_share_size_df: pd.DataFrame | None = None,
    ) -> bool:
        """Use legacy ETF overview only as an enrichment lane once Tushare
        already provides the product's core identity.

        This keeps manager/benchmark wording补洞, but retires the old
        "Tushare 空表 -> AKShare overview 重新接管主链" fallback.
        """

        has_tushare_core = False
        if isinstance(etf_basic_df, pd.DataFrame) and not etf_basic_df.empty:
            has_tushare_core = True
        elif self._tushare_etf_basic_row(symbol):
            has_tushare_core = True
        elif self._tushare_fund_basic_row(symbol):
            has_tushare_core = True
        if not has_tushare_core:
            return False

        has_index_snapshot = isinstance(etf_index_df, pd.DataFrame) and not etf_index_df.empty
        has_share_snapshot = isinstance(etf_share_size_df, pd.DataFrame) and not etf_share_size_df.empty
        return not (has_index_snapshot and has_share_snapshot)

    def get_fund_sales_ratio_snapshot(self, year: str = "") -> Dict[str, Any]:
        """标准化 fund_sales_ratio 快照，便于后续直接下沉到 fund_pick / briefing。"""
        frame = self.get_fund_sales_ratio_ts(year=year)
        snapshot = self._build_fund_sales_ratio_snapshot(frame)
        if snapshot:
            return snapshot
        as_of = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        normalized_year = self._normalize_sales_ratio_year(year)
        latest_date = f"{normalized_year}-12-31" if normalized_year else ""
        return {
            "status": "empty",
            "source": "tushare.fund_sales_ratio",
            "as_of": as_of,
            "latest_date": latest_date,
            "latest_year": normalized_year,
            "is_fresh": False,
            "fallback": "none",
            "disclosure": "各渠道公募基金销售保有规模占比来自 Tushare fund_sales_ratio；年度更新。空表或旧年不伪装成 fresh。",
            "channel_mix": [],
        }

    def _build_etf_snapshot(
        self,
        overview: Mapping[str, Any],
        *,
        etf_basic_df: pd.DataFrame | None = None,
        etf_index_df: pd.DataFrame | None = None,
        etf_share_size_df: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        basic_row = etf_basic_df.iloc[0].to_dict() if isinstance(etf_basic_df, pd.DataFrame) and not etf_basic_df.empty else {}
        index_row = etf_index_df.iloc[0].to_dict() if isinstance(etf_index_df, pd.DataFrame) and not etf_index_df.empty else {}
        share_frame = etf_share_size_df.copy() if isinstance(etf_share_size_df, pd.DataFrame) and not etf_share_size_df.empty else pd.DataFrame()

        for source_key, target_key in (
            ("ts_code", "ts_code"),
            ("index_code", "index_code"),
            ("index_name", "index_name"),
            ("mgr_name", "manager_name"),
            ("custod_name", "custodian_name"),
            ("exchange", "exchange"),
            ("list_status", "list_status"),
            ("etf_type", "etf_type"),
        ):
            value = basic_row.get(source_key)
            if value not in (None, "", []):
                snapshot[target_key] = value

        if basic_row.get("list_date") not in (None, ""):
            snapshot["list_date"] = self._normalize_compact_date(basic_row.get("list_date"))
        if basic_row.get("setup_date") not in (None, ""):
            snapshot["setup_date"] = self._normalize_compact_date(basic_row.get("setup_date"))
        management_fee = self._to_float(basic_row.get("mgt_fee"))
        if management_fee is not None and pd.notna(management_fee):
            snapshot["management_fee"] = management_fee

        for source_key, target_key in (
            ("indx_csname", "index_short_name"),
            ("pub_party_name", "index_publisher"),
            ("adj_circle", "index_rebalance_cycle"),
        ):
            value = index_row.get(source_key)
            if value not in (None, "", []):
                snapshot[target_key] = value
        if index_row.get("pub_date") not in (None, ""):
            snapshot["index_publish_date"] = self._normalize_compact_date(index_row.get("pub_date"))
        if index_row.get("base_date") not in (None, ""):
            snapshot["index_base_date"] = self._normalize_compact_date(index_row.get("base_date"))
        base_point = self._to_float(index_row.get("bp"))
        if base_point is not None and pd.notna(base_point):
            snapshot["index_base_point"] = base_point

        if not share_frame.empty:
            if "trade_date" in share_frame.columns:
                share_frame["trade_date"] = share_frame["trade_date"].map(self._normalize_compact_date)
                share_frame = share_frame.sort_values("trade_date", ascending=False, na_position="last")
            latest_row = share_frame.iloc[0].to_dict()
            trade_date = str(latest_row.get("trade_date", "")).strip()
            total_share = self._to_float(latest_row.get("total_share"))
            total_size = self._to_float(latest_row.get("total_size"))
            nav = self._to_float(latest_row.get("nav"))
            close = self._to_float(latest_row.get("close"))
            if trade_date:
                snapshot["share_as_of"] = trade_date
            if total_share is not None and pd.notna(total_share):
                snapshot["total_share"] = total_share
                snapshot["total_share_yi"] = total_share / 10000.0
            if total_size is not None and pd.notna(total_size):
                snapshot["total_size"] = total_size
                snapshot["total_size_yi"] = total_size / 10000.0
            if nav is not None and pd.notna(nav):
                snapshot["nav"] = nav
            if close is not None and pd.notna(close):
                snapshot["close"] = close
            if len(share_frame.index) >= 2:
                previous_row = share_frame.iloc[1].to_dict()
                previous_trade_date = str(previous_row.get("trade_date", "")).strip()
                previous_share = self._to_float(previous_row.get("total_share"))
                previous_size = self._to_float(previous_row.get("total_size"))
                if previous_trade_date:
                    snapshot["previous_share_as_of"] = previous_trade_date
                    snapshot["previous_size_as_of"] = previous_trade_date
                if (
                    total_share is not None
                    and pd.notna(total_share)
                    and previous_share is not None
                    and pd.notna(previous_share)
                ):
                    share_change = total_share - previous_share
                    snapshot["etf_share_change_raw"] = share_change
                    snapshot["etf_share_change"] = share_change / 10000.0
                    if previous_share:
                        snapshot["etf_share_change_pct"] = share_change / previous_share * 100.0
                if (
                    total_size is not None
                    and pd.notna(total_size)
                    and previous_size is not None
                    and pd.notna(previous_size)
                ):
                    size_change = total_size - previous_size
                    snapshot["etf_size_change_raw"] = size_change
                    snapshot["etf_size_change"] = size_change / 10000.0
                    if previous_size:
                        snapshot["etf_size_change_pct"] = size_change / previous_size * 100.0
            snapshot["share_change_text"] = self._format_etf_flow_change(
                snapshot.get("etf_share_change"),
                snapshot.get("etf_share_change_pct"),
                positive_label="净创设",
                negative_label="净赎回",
                neutral_label="基本持平",
                unit="亿份",
                previous_date=snapshot.get("previous_share_as_of"),
            )
            snapshot["size_change_text"] = self._format_etf_flow_change(
                snapshot.get("etf_size_change"),
                snapshot.get("etf_size_change_pct"),
                positive_label="规模扩张",
                negative_label="规模收缩",
                neutral_label="规模基本持平",
                unit="亿元",
                previous_date=snapshot.get("previous_size_as_of"),
            )

        if "index_name" not in snapshot:
            benchmark = str(overview.get("ETF基准指数中文全称", "") or overview.get("业绩比较基准", "")).strip()
            if benchmark:
                snapshot["index_name"] = benchmark
        if "exchange" not in snapshot:
            exchange = str(overview.get("交易所", "")).strip()
            if exchange:
                snapshot["exchange"] = exchange
        return snapshot

    def _build_fund_factor_snapshot(self, factor_df: pd.DataFrame | None) -> Dict[str, Any]:
        if not isinstance(factor_df, pd.DataFrame) or factor_df.empty:
            return {}

        latest_row = factor_df.iloc[0].to_dict()

        def _first_float(*keys: str) -> float | None:
            for key in keys:
                value = self._to_float(latest_row.get(key))
                if value is not None and pd.notna(value):
                    return float(value)
            return None

        latest_date = str(
            latest_row.get("trade_date", latest_row.get("date", factor_df.attrs.get("latest_date", "")))
        ).strip()
        trade_date = self._normalize_compact_date(latest_date)
        close = _first_float("close")
        pct_change = _first_float("pct_change", "pct_chg")
        ma20 = _first_float("ma_bfq_20", "ma20")
        ma60 = _first_float("ma_bfq_60", "ma60")
        macd = _first_float("macd_bfq", "macd")
        rsi6 = _first_float("rsi_bfq_6", "rsi6")

        trend_label = "震荡"
        if close is not None and ma20 is not None and ma60 is not None:
            if close >= ma20 >= ma60:
                trend_label = "趋势偏强"
            elif close >= ma20:
                trend_label = "修复中"
            elif close < ma20 < ma60:
                trend_label = "趋势偏弱"

        momentum_label = ""
        if macd is not None:
            if macd >= 0:
                momentum_label = "动能改善"
            elif macd < 0:
                momentum_label = "动能偏弱"
        if rsi6 is not None:
            if rsi6 >= 65 and momentum_label != "动能改善":
                momentum_label = "动能改善"
            elif rsi6 <= 40 and momentum_label != "动能偏弱":
                momentum_label = "动能偏弱"

        detail_parts: List[str] = []
        if close is not None:
            detail_parts.append(f"收盘 {close:.2f}")
        if ma20 is not None:
            detail_parts.append(f"MA20 {ma20:.2f}")
        if ma60 is not None:
            detail_parts.append(f"MA60 {ma60:.2f}")
        if macd is not None:
            detail_parts.append(f"MACD {macd:+.2f}")
        if rsi6 is not None:
            detail_parts.append(f"RSI6 {rsi6:.1f}")

        signal_strength = "中"
        if pct_change is not None:
            if pct_change >= 3:
                signal_strength = "高"
            elif pct_change < 1:
                signal_strength = "低"

        return {
            "trade_date": trade_date,
            "latest_date": str(factor_df.attrs.get("latest_date", trade_date or "")).strip() or trade_date,
            "as_of": str(factor_df.attrs.get("as_of", "")).strip(),
            "is_fresh": bool(factor_df.attrs.get("is_fresh", False)),
            "source": str(factor_df.attrs.get("source", "tushare.fund_factor_pro")).strip() or "tushare.fund_factor_pro",
            "fallback": str(factor_df.attrs.get("fallback", "none")).strip() or "none",
            "disclosure": str(factor_df.attrs.get("disclosure", "")).strip(),
            "status": "matched",
            "close": close,
            "pct_change": pct_change,
            "ma20": ma20,
            "ma60": ma60,
            "macd": macd,
            "rsi6": rsi6,
            "trend_label": trend_label,
            "momentum_label": momentum_label,
            "signal_strength": signal_strength,
            "detail": " / ".join(detail_parts),
        }

    def _build_fund_sales_ratio_snapshot(self, ratio_df: pd.DataFrame | None) -> Dict[str, Any]:
        if not isinstance(ratio_df, pd.DataFrame) or ratio_df.empty:
            return {}

        latest_row = ratio_df.iloc[0].to_dict()
        latest_year = self._normalize_sales_ratio_year(
            latest_row.get("year", ratio_df.attrs.get("latest_year", ""))
        )
        latest_date = str(ratio_df.attrs.get("latest_date", "")).strip() or (f"{latest_year}-12-31" if latest_year else "")
        as_of = str(ratio_df.attrs.get("as_of", "")).strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source = str(ratio_df.attrs.get("source", "tushare.fund_sales_ratio")).strip() or "tushare.fund_sales_ratio"
        fallback = str(ratio_df.attrs.get("fallback", "none")).strip() or "none"
        disclosure = str(ratio_df.attrs.get("disclosure", "")).strip()
        is_fresh = bool(ratio_df.attrs.get("is_fresh", False))

        channel_specs = [
            ("bank", "商业银行"),
            ("sec_comp", "证券公司"),
            ("fund_comp", "基金公司直销"),
            ("indep_comp", "独立基金销售机构"),
            ("rests", "其他"),
        ]
        channel_mix: List[Dict[str, Any]] = []
        for key, label in channel_specs:
            value = self._to_float(latest_row.get(key))
            if value is None or pd.isna(value):
                continue
            channel_mix.append({"channel": label, "ratio": round(float(value), 2), "key": key})
        channel_mix.sort(key=lambda item: item.get("ratio", 0.0), reverse=True)

        if not channel_mix:
            return {}

        lead = channel_mix[0]
        total = sum(float(item.get("ratio", 0.0)) for item in channel_mix)
        summary = f"{latest_year or '最新'}年渠道保有结构：{lead['channel']}占比最高，约 {lead['ratio']:.2f}% 。"
        if len(channel_mix) >= 2:
            second = channel_mix[1]
            summary += f" 其次是 {second['channel']} 约 {second['ratio']:.2f}% 。"

        return {
            "status": "matched",
            "source": source,
            "as_of": as_of,
            "latest_date": latest_date,
            "latest_year": latest_year,
            "is_fresh": is_fresh,
            "fallback": fallback,
            "disclosure": disclosure,
            "total_ratio": round(total, 2),
            "lead_channel": lead["channel"],
            "lead_ratio": round(float(lead["ratio"]), 2),
            "channel_mix": channel_mix,
            "summary": summary.strip(),
        }

    def _format_etf_flow_change(
        self,
        change_value: Any,
        change_pct: Any,
        *,
        positive_label: str,
        negative_label: str,
        neutral_label: str,
        unit: str,
        previous_date: Any = "",
    ) -> str:
        change = self._to_float(change_value)
        pct = self._to_float(change_pct)
        if change is None or pd.isna(change):
            return ""
        if change > 0:
            label = positive_label
        elif change < 0:
            label = negative_label
        else:
            label = neutral_label
        pct_text = ""
        if pct is not None and pd.notna(pct):
            pct_text = f" ({pct:+.2f}%)"
        basis_text = f"，较 {str(previous_date).strip()}" if str(previous_date).strip() else ""
        return f"{label} {change:+.2f}{unit}{pct_text}{basis_text}"

    def _merge_overview_with_tushare(
        self,
        overview: Dict[str, Any],
        symbol: str,
        asset_type: str = "cn_fund",
        etf_basic_df: pd.DataFrame | None = None,
        etf_index_df: pd.DataFrame | None = None,
        etf_share_size_df: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        if asset_type == "cn_etf":
            etf_basic = etf_basic_df.iloc[0].to_dict() if isinstance(etf_basic_df, pd.DataFrame) and not etf_basic_df.empty else self._tushare_etf_basic_row(symbol)
            if etf_basic:
                merged = self._overview_from_tushare_etf_basic(etf_basic)
            else:
                basic = self._tushare_fund_basic_row(symbol)
                merged = self._overview_from_tushare_basic(basic) if basic else {}
            if isinstance(etf_index_df, pd.DataFrame) and not etf_index_df.empty:
                merged = self._merge_etf_index_overview(merged, etf_index_df.iloc[0].to_dict())
            if isinstance(etf_share_size_df, pd.DataFrame) and not etf_share_size_df.empty:
                merged = self._merge_etf_share_size_overview(merged, etf_share_size_df.iloc[0].to_dict())
        else:
            basic = self._tushare_fund_basic_row(symbol)
            merged = self._overview_from_tushare_basic(basic) if basic else {}
        if not overview:
            return merged
        if not merged:
            return dict(overview)

        for key, value in dict(overview).items():
            if merged.get(key) in (None, "", "—"):
                if value not in (None, "", "—"):
                    merged[key] = value
        return merged

    def _overview_from_tushare_etf_basic(self, basic: Dict[str, Any]) -> Dict[str, Any]:
        overview: Dict[str, Any] = {}
        if not basic:
            return overview

        ts_code = str(basic.get("ts_code", "")).strip()
        csname = str(basic.get("csname", "") or basic.get("extname", "") or basic.get("cname", "")).strip()
        extname = str(basic.get("extname", "")).strip()
        cname = str(basic.get("cname", "")).strip()
        index_code = str(basic.get("index_code", "")).strip()
        index_name = str(basic.get("index_name", "")).strip()
        setup_date = self._normalize_compact_date(basic.get("setup_date"))
        list_date = self._normalize_compact_date(basic.get("list_date"))
        etf_type = str(basic.get("etf_type", "")).strip()
        mgr_name = str(basic.get("mgr_name", "")).strip()
        custod_name = str(basic.get("custod_name", "")).strip()
        mgt_fee = self._to_float(basic.get("mgt_fee"))
        exchange = str(basic.get("exchange", "")).strip()

        if csname:
            overview["基金简称"] = csname
        if ts_code:
            overview["基金代码"] = ts_code.split(".")[0]
        if extname:
            overview["ETF扩位简称"] = extname
        if cname:
            overview["基金中文全称"] = cname
        if index_code:
            overview["ETF基准指数代码"] = index_code
        if index_name:
            overview["ETF基准指数中文全称"] = index_name
            overview["业绩比较基准"] = index_name
            overview["跟踪标的"] = index_name
        if setup_date:
            overview["成立日期"] = setup_date
        if list_date:
            overview["上市日期"] = list_date
        if mgr_name:
            overview["基金管理人"] = mgr_name
        if custod_name:
            overview["基金托管人"] = custod_name
        if etf_type:
            overview["ETF类型"] = etf_type
            overview.setdefault("基金类型", f"ETF / {etf_type}")
        if exchange:
            overview["交易所"] = exchange
        if mgt_fee is not None and pd.notna(mgt_fee):
            overview["管理费率"] = f"{mgt_fee:.2f}%（每年）"
        return overview

    def _merge_etf_index_overview(self, overview: Dict[str, Any], index_row: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(overview)
        if not index_row:
            return merged
        index_name = str(index_row.get("indx_name", "") or index_row.get("index_name", "")).strip()
        index_short = str(index_row.get("indx_csname", "")).strip()
        publisher = str(index_row.get("pub_party_name", "")).strip()
        pub_date = self._normalize_compact_date(index_row.get("pub_date"))
        base_date = self._normalize_compact_date(index_row.get("base_date"))
        adj_circle = str(index_row.get("adj_circle", "")).strip()
        bp = self._to_float(index_row.get("bp"))

        if index_name and not merged.get("ETF基准指数中文全称"):
            merged["ETF基准指数中文全称"] = index_name
        if index_short and not merged.get("ETF基准指数简称"):
            merged["ETF基准指数简称"] = index_short
        if publisher and not merged.get("ETF基准指数发布机构"):
            merged["ETF基准指数发布机构"] = publisher
        if pub_date and not merged.get("ETF基准指数发布日期"):
            merged["ETF基准指数发布日期"] = pub_date
        if base_date and not merged.get("ETF基准指数基日"):
            merged["ETF基准指数基日"] = base_date
        if adj_circle and not merged.get("ETF基准指数调样周期"):
            merged["ETF基准指数调样周期"] = adj_circle
        if bp is not None and not merged.get("ETF基准指数基点"):
            merged["ETF基准指数基点"] = bp
        return merged

    def _merge_etf_share_size_overview(self, overview: Dict[str, Any], share_row: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(overview)
        if not share_row:
            return merged
        trade_date = str(share_row.get("trade_date", "")).strip()
        total_share = self._to_float(share_row.get("total_share"))
        total_size = self._to_float(share_row.get("total_size"))
        nav = self._to_float(share_row.get("nav"))
        close = self._to_float(share_row.get("close"))
        etf_name = str(share_row.get("etf_name", "")).strip()
        exchange = str(share_row.get("exchange", "")).strip()

        if trade_date:
            merged["ETF份额规模日期"] = trade_date
        if etf_name and not merged.get("基金简称"):
            merged["基金简称"] = etf_name
        if total_share is not None and pd.notna(total_share):
            merged["ETF总份额"] = f"{total_share:.2f}万份"
        if total_size is not None and pd.notna(total_size):
            merged["ETF总规模"] = f"{total_size:.2f}万元"
            merged.setdefault("净资产规模", f"{total_size:.2f}万元（截止至：{trade_date}）" if trade_date else f"{total_size:.2f}万元")
        if nav is not None and pd.notna(nav):
            merged["ETF份额净值"] = f"{nav:.4f}"
        if close is not None and pd.notna(close):
            merged["ETF收盘价"] = f"{close:.4f}"
        if exchange and not merged.get("交易所"):
            merged["交易所"] = exchange
        return merged

    def _overview_from_tushare_basic(self, basic: Dict[str, Any]) -> Dict[str, Any]:
        overview: Dict[str, Any] = {}
        if not basic:
            return overview

        fund_type = str(basic.get("fund_type", "")).strip()
        invest_type = str(basic.get("invest_type", "")).strip()
        type_text = fund_type or invest_type
        if fund_type and invest_type and invest_type not in fund_type:
            type_text = f"{fund_type} / {invest_type}"

        found_date = self._normalize_compact_date(basic.get("found_date"))
        issue_date = self._normalize_compact_date(basic.get("issue_date"))
        list_date = self._normalize_compact_date(basic.get("list_date"))
        issue_amount = self._to_float(basic.get("issue_amount"))
        founding_text = found_date or ""
        launch_scale_text = f"{issue_amount:.4f}亿份" if issue_amount is not None and pd.notna(issue_amount) else ""

        if str(basic.get("name", "")).strip():
            overview["基金简称"] = str(basic.get("name", "")).strip()
        ts_code = str(basic.get("ts_code", "")).split(".")[0]
        if ts_code:
            overview["基金代码"] = ts_code
        if type_text:
            overview["基金类型"] = type_text
        if str(basic.get("management", "")).strip():
            overview["基金管理人"] = str(basic.get("management", "")).strip()
        custodian = str(basic.get("custodian", "") or basic.get("trustee", "")).strip()
        if custodian:
            overview["基金托管人"] = custodian
        if issue_date:
            overview["发行日期"] = issue_date
        if founding_text:
            overview["成立日期"] = founding_text
            overview["成立日期/规模"] = f"{founding_text} / {launch_scale_text}" if launch_scale_text else founding_text
        if launch_scale_text:
            overview["首发规模"] = launch_scale_text
        benchmark = str(basic.get("benchmark", "")).strip()
        if benchmark:
            overview["业绩比较基准"] = benchmark
            overview["跟踪标的"] = benchmark
        if list_date:
            overview["上市日期"] = list_date
        if basic.get("m_fee") not in (None, ""):
            overview["管理费率"] = f"{float(basic.get('m_fee')):.2f}%（每年）"
        if basic.get("c_fee") not in (None, ""):
            overview["托管费率"] = f"{float(basic.get('c_fee')):.2f}%（每年）"
        return overview

    def _tushare_fund_basic_row(self, symbol: str) -> Dict[str, Any]:
        bare_symbol = str(symbol).split(".")[0]
        for market in ("E", "O", "L"):
            frame = self.get_fund_basic(market)
            if frame.empty or "ts_code" not in frame.columns:
                continue
            matched = frame[frame["ts_code"].astype(str).str.startswith(f"{bare_symbol}.", na=False)]
            if not matched.empty:
                return matched.iloc[0].to_dict()
        return {}

    def _tushare_etf_basic_row(self, symbol: str) -> Dict[str, Any]:
        bare_symbol = str(symbol).split(".")[0]
        frame = self.get_etf_basic_ts(symbol)
        if frame.empty or "ts_code" not in frame.columns:
            return {}
        matched = frame[frame["ts_code"].astype(str).str.startswith(f"{bare_symbol}.", na=False)]
        if not matched.empty:
            return matched.iloc[0].to_dict()
        return {}

    def _normalize_compact_date(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            return ""
        if text.isdigit() and len(text) == 8:
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        return text

    def _normalize_sales_ratio_year(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            return ""
        if text.isdigit() and len(text) == 4:
            return text
        if text.isdigit() and len(text) == 8:
            return text[:4]
        try:
            year = int(float(text))
        except (TypeError, ValueError):
            return ""
        return f"{year:04d}"

    def _safe_frame(self, method, *args) -> pd.DataFrame:  # noqa: ANN001
        try:
            frame = method(*args)
            return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _year_candidates(self) -> List[str]:
        year = datetime.now().year
        return [str(year - offset) for offset in range(0, 4)]

    def _is_known_empty_detail_error(self, exc: Exception) -> bool:
        text = str(exc)
        return any(
            token in text
            for token in (
                "Length mismatch",
                "Excel file format cannot be determined",
                "CERTIFICATE_VERIFY_FAILED",
                "'data'",
            )
        )

    def _latest_quarter_frame(self, frame: pd.DataFrame, column: str) -> pd.DataFrame:
        if frame.empty or column not in frame.columns:
            return pd.DataFrame()
        scored = frame.copy()
        scored["_quarter_score"] = scored[column].astype(str).map(self._quarter_score)
        latest = scored["_quarter_score"].max()
        if pd.isna(latest):
            return pd.DataFrame()
        return scored[scored["_quarter_score"] == latest].drop(columns="_quarter_score")

    def _latest_cutoff_frame(self, frame: pd.DataFrame, column: str) -> pd.DataFrame:
        if frame.empty or column not in frame.columns:
            return pd.DataFrame()
        scored = frame.copy()
        scored["_cutoff"] = pd.to_datetime(scored[column], errors="coerce")
        latest = scored["_cutoff"].max()
        if pd.isna(latest):
            return pd.DataFrame()
        return scored[scored["_cutoff"] == latest].drop(columns="_cutoff")

    def _quarter_score(self, value: str) -> float:
        match = re.search(r"(\d{4})年(\d)季度", str(value))
        if not match:
            return float("-inf")
        return float(int(match.group(1)) * 10 + int(match.group(2)))

    def _achievement_snapshot(self, frame: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        if frame.empty:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        period_col = "周期" if "周期" in frame.columns else None
        if not period_col:
            return result
        for _, row in frame.iterrows():
            period = str(row.get(period_col, "")).strip()
            if not period or period in result:
                continue
            result[period] = {
                "return_pct": self._to_float(row.get("本产品区间收益")),
                "max_drawdown_pct": self._to_float(row.get("本产品最大回撒")),
                "peer_rank": str(row.get("周期收益同类排名", "")).strip(),
            }
        return result

    def _top_holdings(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        result: List[Dict[str, Any]] = []
        sorted_frame = frame.copy()
        if "占净值比例" in sorted_frame.columns:
            sorted_frame["占净值比例"] = pd.to_numeric(sorted_frame["占净值比例"], errors="coerce")
            sorted_frame = sorted_frame.sort_values("占净值比例", ascending=False)
        for _, row in sorted_frame.head(10).iterrows():
            result.append(
                {
                    "股票代码": str(row.get("股票代码", "")).strip(),
                    "股票名称": str(row.get("股票名称", "")).strip(),
                    "占净值比例": self._to_float(row.get("占净值比例")),
                    "持股数": self._to_float(row.get("持股数")),
                    "持仓市值": self._to_float(row.get("持仓市值")),
                    "季度": str(row.get("季度", "")).strip(),
                }
            )
        return result

    def _merge_holdings(self, ts_frame: pd.DataFrame, ak_frame: pd.DataFrame, *, stock_basic_df: pd.DataFrame | None = None) -> pd.DataFrame:
        ts_norm = self._normalize_tushare_holdings(ts_frame, stock_basic_df=stock_basic_df)
        ak_norm = self._normalize_ak_holdings(ak_frame)
        if ts_norm.empty:
            return ak_norm
        if ak_norm.empty:
            return ts_norm

        merged = ts_norm.merge(
            ak_norm,
            on="股票代码",
            how="left",
            suffixes=("", "_ak"),
        )
        for column in ("股票名称", "持股数", "季度"):
            ak_column = f"{column}_ak"
            if ak_column in merged.columns:
                merged[column] = merged[column].replace("", pd.NA).fillna(merged[ak_column])
                merged = merged.drop(columns=[ak_column])
        if "持仓市值_ak" in merged.columns:
            merged["持仓市值"] = pd.to_numeric(merged["持仓市值"], errors="coerce").fillna(
                pd.to_numeric(merged["持仓市值_ak"], errors="coerce")
            )
            merged = merged.drop(columns=["持仓市值_ak"])
        if "占净值比例_ak" in merged.columns:
            merged["占净值比例"] = pd.to_numeric(merged["占净值比例"], errors="coerce").fillna(
                pd.to_numeric(merged["占净值比例_ak"], errors="coerce")
            )
            merged = merged.drop(columns=["占净值比例_ak"])
        return merged

    def _normalize_tushare_holdings(self, frame: pd.DataFrame, *, stock_basic_df: pd.DataFrame | None = None) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()
        symbol_col = "symbol" if "symbol" in frame.columns else "stk_code" if "stk_code" in frame.columns else None
        ratio_col = "stk_mkv_ratio" if "stk_mkv_ratio" in frame.columns else "mkv_ratio" if "mkv_ratio" in frame.columns else None
        value_col = "mkv" if "mkv" in frame.columns else "stk_mkv" if "stk_mkv" in frame.columns else None
        amount_col = "amount" if "amount" in frame.columns else "stk_amount" if "stk_amount" in frame.columns else None
        end_col = "end_date" if "end_date" in frame.columns else "report_date" if "report_date" in frame.columns else None
        ann_col = "ann_date" if "ann_date" in frame.columns else None
        if not symbol_col:
            return pd.DataFrame()

        working = frame.copy()
        if end_col:
            working["_end_date"] = pd.to_datetime(working[end_col], format="%Y%m%d", errors="coerce")
            latest = working["_end_date"].max()
            if pd.notna(latest):
                working = working[working["_end_date"] == latest]
        elif ann_col:
            working["_ann_date"] = pd.to_datetime(working[ann_col], format="%Y%m%d", errors="coerce")
            latest = working["_ann_date"].max()
            if pd.notna(latest):
                working = working[working["_ann_date"] == latest]

        normalized = pd.DataFrame()
        normalized["股票代码"] = working[symbol_col].fillna("").astype(str).str.split(".").str[0]
        normalized["股票名称"] = ""
        if isinstance(stock_basic_df, pd.DataFrame) and not stock_basic_df.empty:
            name_map = (
                stock_basic_df.assign(_symbol=stock_basic_df.get("symbol", pd.Series(dtype=str)).fillna("").astype(str).str.strip())
                .loc[lambda df: df["_symbol"] != "", ["_symbol", "name"]]
                .drop_duplicates("_symbol")
                .set_index("_symbol")["name"]
                .to_dict()
            )
            if name_map:
                normalized["股票名称"] = normalized["股票代码"].map(lambda code: str(name_map.get(str(code).strip(), "")).strip())
        normalized["占净值比例"] = pd.to_numeric(working[ratio_col], errors="coerce") if ratio_col else pd.NA
        normalized["持股数"] = pd.to_numeric(working[amount_col], errors="coerce") if amount_col else pd.NA
        normalized["持仓市值"] = pd.to_numeric(working[value_col], errors="coerce") if value_col else pd.NA
        if end_col:
            normalized["季度"] = working[end_col].map(self._normalize_compact_date)
        elif ann_col:
            normalized["季度"] = working[ann_col].map(self._normalize_compact_date)
        else:
            normalized["季度"] = ""
        normalized = normalized[normalized["股票代码"] != ""]
        return normalized.sort_values("占净值比例", ascending=False, na_position="last").reset_index(drop=True)

    def _normalize_ak_holdings(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()
        normalized = pd.DataFrame()
        normalized["股票代码"] = frame.get("股票代码", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        normalized["股票名称"] = frame.get("股票名称", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        normalized["占净值比例"] = pd.to_numeric(frame.get("占净值比例", pd.Series(dtype=float)), errors="coerce")
        normalized["持股数"] = pd.to_numeric(frame.get("持股数", pd.Series(dtype=float)), errors="coerce")
        normalized["持仓市值"] = pd.to_numeric(frame.get("持仓市值", pd.Series(dtype=float)), errors="coerce")
        normalized["季度"] = frame.get("季度", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        normalized = normalized[normalized["股票代码"] != ""]
        return normalized.sort_values("占净值比例", ascending=False, na_position="last").reset_index(drop=True)

    def _top_industries(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        result: List[Dict[str, Any]] = []
        sorted_frame = frame.copy()
        if "占净值比例" in sorted_frame.columns:
            sorted_frame["占净值比例"] = pd.to_numeric(sorted_frame["占净值比例"], errors="coerce")
            sorted_frame = sorted_frame.sort_values("占净值比例", ascending=False)
        for _, row in sorted_frame.head(8).iterrows():
            result.append(
                {
                    "行业类别": str(row.get("行业类别", "")).strip(),
                    "占净值比例": self._to_float(row.get("占净值比例")),
                    "市值": self._to_float(row.get("市值")),
                    "截止时间": str(row.get("截止时间", "")).strip(),
                }
            )
        return result

    def _asset_mix(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        result: List[Dict[str, Any]] = []
        for _, row in frame.iterrows():
            ratio = (
                self._to_float(row.get("仓位占比"))
                if "仓位占比" in row
                else None
            )
            if ratio is None:
                ratio = self._to_float(row.get("占总资产比例"))
            if ratio is None:
                ratio = self._to_float(row.get("占净值比例"))
            result.append(
                {
                    "资产类型": str(row.get("资产类型", "")).strip(),
                    "仓位占比": ratio,
                }
            )
        valid_ratios = [float(item["仓位占比"]) for item in result if item.get("仓位占比") is not None]
        if any(ratio < 0 or ratio > 100 for ratio in valid_ratios):
            return []
        # 商品/联接产品可能因保证金或口径差异略高于 100%，但大幅超出通常是供应商坏数据。
        if len(valid_ratios) >= 2 and sum(valid_ratios) > 120:
            return []
        return result

    def _manager_snapshot(self, ts_frame: pd.DataFrame, ak_frame: pd.DataFrame, overview: Dict[str, Any]) -> Dict[str, Any]:
        manager_name = str(overview.get("基金经理人", "")).strip()
        if not manager_name:
            manager_name = self._derive_tushare_manager_names(ts_frame)
        manager_names = [name.strip() for name in re.split(r"[,，、/]+", manager_name) if name.strip()]
        if not manager_names:
            return {}

        active_ts = self._active_tushare_manager_rows(ts_frame, manager_names)
        matched_ak = self._matched_ak_manager_rows(ak_frame, manager_names)

        primary_ts = active_ts.iloc[0].to_dict() if not active_ts.empty else {}
        primary_ak = matched_ak.iloc[0].to_dict() if not matched_ak.empty else {}
        begin_date = self._normalize_compact_date(primary_ts.get("begin_date"))
        end_date = self._normalize_compact_date(primary_ts.get("end_date"))
        ann_date = self._normalize_compact_date(primary_ts.get("ann_date"))

        snapshot: Dict[str, Any] = {
            "name": "、".join(manager_names),
            "company": str(primary_ak.get("所属公司", "") or overview.get("基金管理人", "")).strip(),
            "tenure_days": self._to_float(primary_ak.get("累计从业时间")),
            "aum_billion": self._to_float(primary_ak.get("现任基金资产总规模")),
            "best_return_pct": self._to_float(primary_ak.get("现任基金最佳回报")),
            "current_fund_count": int(matched_ak["现任基金代码"].astype(str).nunique()) if not matched_ak.empty and "现任基金代码" in matched_ak.columns else len(manager_names),
            "peer_funds": list(dict.fromkeys(matched_ak.get("现任基金", pd.Series(dtype=str)).astype(str).tolist())) if not matched_ak.empty else [],
            "begin_date": begin_date,
            "end_date": end_date,
            "ann_date": ann_date,
            "education": str(primary_ts.get("edu", "")).strip(),
            "nationality": str(primary_ts.get("nationality", "")).strip(),
            "gender": str(primary_ts.get("gender", "")).strip(),
            "birth_year": str(primary_ts.get("birth_year", "")).strip(),
            "resume": str(primary_ts.get("resume", "")).strip(),
        }
        return {key: value for key, value in snapshot.items() if value not in (None, "", [])}

    def _derive_tushare_manager_names(self, frame: pd.DataFrame) -> str:
        if frame.empty or "name" not in frame.columns:
            return ""
        active = self._active_tushare_manager_rows(frame, [])
        if active.empty:
            active = frame.copy()
        names = list(dict.fromkeys(active.get("name", pd.Series(dtype=str)).astype(str).str.strip().tolist()))
        names = [name for name in names if name and name.lower() != "nan"]
        return "、".join(names[:3])

    def _active_tushare_manager_rows(self, frame: pd.DataFrame, manager_names: Sequence[str]) -> pd.DataFrame:
        if frame.empty or "name" not in frame.columns:
            return pd.DataFrame()
        working = frame.copy()
        if manager_names:
            working = working[working["name"].astype(str).isin(manager_names)]
        if "ann_date" in working.columns:
            working["_ann_date"] = pd.to_datetime(working["ann_date"], format="%Y%m%d", errors="coerce")
        if "begin_date" in working.columns:
            working["_begin_date"] = pd.to_datetime(working["begin_date"], format="%Y%m%d", errors="coerce")
        if "end_date" in working.columns:
            end_text = working["end_date"].fillna("").astype(str).str.strip()
            active_mask = end_text.eq("") | end_text.str.lower().eq("nan")
            active_rows = working[active_mask]
            if not active_rows.empty:
                working = active_rows
        order_columns = [column for column in ("_ann_date", "_begin_date") if column in working.columns]
        if order_columns:
            working = working.sort_values(order_columns, ascending=False, na_position="last")
        return working.reset_index(drop=True)

    def _matched_ak_manager_rows(self, frame: pd.DataFrame, manager_names: Sequence[str]) -> pd.DataFrame:
        if frame.empty or "姓名" not in frame.columns or not manager_names:
            return pd.DataFrame()
        matched = frame[frame["姓名"].astype(str).isin(manager_names)].copy()
        if matched.empty:
            return matched
        if "现任基金资产总规模" in matched.columns:
            matched["现任基金资产总规模"] = pd.to_numeric(matched["现任基金资产总规模"], errors="coerce")
            matched = matched.sort_values("现任基金资产总规模", ascending=False, na_position="last")
        return matched.reset_index(drop=True)

    def _company_snapshot(self, frame: pd.DataFrame, overview: Dict[str, Any]) -> Dict[str, Any]:
        company_name = str(overview.get("基金管理人", "")).strip()
        if not company_name or frame.empty:
            return {}
        name_col = "name" if "name" in frame.columns else "company" if "company" in frame.columns else None
        if not name_col:
            return {}
        matched = frame[frame[name_col].astype(str).eq(company_name)].copy()
        if matched.empty:
            return {}
        row = matched.iloc[0]
        snapshot = {
            "name": company_name,
            "short_name": str(row.get("short_name", "") or row.get("shortname", "")).strip(),
            "province": str(row.get("province", "")).strip(),
            "city": str(row.get("city", "")).strip(),
            "website": str(row.get("website", "")).strip(),
            "phone": str(row.get("phone", "")).strip(),
            "office": str(row.get("office", "")).strip(),
            "chairman": str(row.get("chairman", "")).strip(),
            "general_manager": str(row.get("manager", "")).strip(),
            "employees": self._to_float(row.get("employees")),
            "registered_capital": self._to_float(row.get("reg_capital")),
        }
        return {key: value for key, value in snapshot.items() if value not in (None, "", [])}

    def _dividend_snapshot(self, frame: pd.DataFrame) -> Dict[str, Any]:
        if frame.empty:
            return {}
        working = frame.copy()
        sort_col = None
        for candidate in ("pay_date", "ex_date", "ann_date", "record_date"):
            if candidate in working.columns:
                working[f"_{candidate}"] = pd.to_datetime(working[candidate], format="%Y%m%d", errors="coerce")
                sort_col = f"_{candidate}"
                break
        if sort_col:
            working = working.sort_values(sort_col, ascending=False, na_position="last")

        rows: List[Dict[str, Any]] = []
        for _, row in working.head(3).iterrows():
            rows.append(
                {
                    "ann_date": self._normalize_compact_date(row.get("ann_date")),
                    "record_date": self._normalize_compact_date(row.get("record_date")),
                    "ex_date": self._normalize_compact_date(row.get("ex_date")),
                    "pay_date": self._normalize_compact_date(row.get("pay_date")),
                    "div_cash": self._to_float(row.get("div_cash")),
                    "base_unit": str(row.get("base_unit", "")).strip(),
                    "ear_distr": self._to_float(row.get("ear_distr")),
                    "progress": str(row.get("progress", "")).strip(),
                }
            )
        latest = rows[0] if rows else {}
        return {
            "count": len(frame),
            "latest_ann_date": latest.get("ann_date", ""),
            "latest_ex_date": latest.get("ex_date", ""),
            "latest_pay_date": latest.get("pay_date", ""),
            "latest_div_cash": latest.get("div_cash"),
            "rows": rows,
        }

    def _rating_snapshot(self, frame: pd.DataFrame, symbol: str) -> Dict[str, Any]:
        if frame.empty or "代码" not in frame.columns:
            return {}
        matched = frame[frame["代码"].astype(str) == str(symbol)]
        if matched.empty:
            return {}
        row = matched.iloc[0]
        return {
            "five_star_count": self._to_float(row.get("5星评级家数")),
            "shanghai": self._to_float(row.get("上海证券")),
            "zhaoshang": self._to_float(row.get("招商证券")),
            "jiaan": self._to_float(row.get("济安金信")),
            "morningstar": self._to_float(row.get("晨星评级")),
            "category": str(row.get("类型", "")).strip(),
            "fee": self._to_float(row.get("手续费")),
        }

    def _derive_style(
        self,
        overview: Dict[str, Any],
        top_holdings: List[Dict[str, Any]],
        top_industries: List[Dict[str, Any]],
        asset_mix: List[Dict[str, Any]],
        manager: Dict[str, Any],
        *,
        asset_type: str = "cn_fund",
    ) -> Dict[str, Any]:
        fund_name = str(overview.get("基金简称", "")).strip()
        fund_type = str(overview.get("基金类型", "")).strip()
        benchmark_note = str(overview.get("业绩比较基准", "")).strip() or "未披露业绩比较基准"
        tracking_target = str(overview.get("跟踪标的", "")).strip()
        passive_text = f"{fund_name} {fund_type}".lower()
        is_passive = any(token in passive_text for token in ("指数", "etf", "联接"))
        commodity_like = any(token in f"{fund_type} {benchmark_note} {tracking_target}".lower() for token in ("商品", "期货", "原油", "黄金", "贵金属", "能源化工", "现货"))
        core_text_parts = [
            fund_name,
            fund_type,
            benchmark_note,
            tracking_target,
        ]
        secondary_text_parts = [
            " ".join(item.get("行业类别", "") for item in top_industries),
            " ".join(item.get("股票名称", "") for item in top_holdings),
        ]
        sector, chain_nodes = self._infer_theme(" ".join(core_text_parts))
        secondary_sector, secondary_chain_nodes = self._infer_theme(" ".join(secondary_text_parts))
        if sector == "综合" or (is_passive and sector == "科技" and secondary_sector not in {"", "综合", "科技"}):
            sector, chain_nodes = secondary_sector, secondary_chain_nodes
        if sector == "综合":
            fallback_text = " ".join([*core_text_parts, *secondary_text_parts, " ".join(manager.get("peer_funds", []))])
            sector, chain_nodes = self._infer_theme(fallback_text)
        stock_ratio = self._asset_ratio(asset_mix, "股票")
        cash_ratio = self._asset_ratio(asset_mix, "现金")
        top5 = sum(item.get("占净值比例") or 0.0 for item in top_holdings[:5])
        tags: List[str] = []
        if sector != "综合":
            tags.append(f"{sector}主题")
        if is_passive:
            tags.append("被动跟踪")
            if commodity_like:
                tags.append("商品/期货跟踪")
        elif stock_ratio >= 80:
            tags.append("高仓位主动")
        elif stock_ratio >= 60:
            tags.append("偏股进攻")
        elif stock_ratio > 0:
            tags.append("仓位灵活")
        if cash_ratio >= 15:
            tags.append("保证金/备付结构" if commodity_like else "保留机动仓位")
        if not is_passive and top5 >= 40:
            tags.append("高集中选股")
        elif not is_passive and top5 >= 25:
            tags.append("中等集中")
        if not is_passive and self._manager_style_consistent(manager.get("peer_funds", []), sector):
            tags.append("风格稳定")

        if is_passive and commodity_like:
            positioning = "这类商品/期货 ETF 更看跟踪合约、展期损益、保证金与申赎效率，不以基金经理主观择时择股为核心。"
        elif is_passive:
            positioning = "这类基金更看跟踪标的暴露、跟踪误差和申赎效率，不以基金经理主观择时择股为核心。"
        elif stock_ratio >= 80:
            positioning = f"股票仓位约 {stock_ratio:.1f}% ，整体是高仓位进攻框架。"
        elif stock_ratio > 0:
            positioning = f"股票仓位约 {stock_ratio:.1f}% ，仓位并不保守。"
        else:
            positioning = "当前仓位信息不足，无法稳定判断进攻/防守倾向。"
        if cash_ratio >= 15:
            if commodity_like:
                positioning += f" 当前约 {cash_ratio:.1f}% 的现金/保证金更多反映合约保证金与备付结构，不等于主观空仓。"
            else:
                positioning += f" 同时保留约 {cash_ratio:.1f}% 现金，机动性不低。"

        if is_passive and commodity_like:
            selection = "核心不是基金经理主动选股，而是跟踪对应商品/期货指数及其合约结构、展期和保证金安排。"
        elif is_passive:
            if sector != "综合":
                selection = f"核心不是基金经理主动选股，而是跟踪 `{sector}` 暴露及其对应基准。"
            else:
                selection = "核心不是基金经理主动选股，而是跟踪对应指数/标的本身。"
        elif top5 >= 40:
            selection = f"前五大重仓合计约 {top5:.1f}% ，选股集中度较高，本质上是在买基金经理的高 conviction 组合。"
        elif top5 > 0:
            selection = f"前五大重仓合计约 {top5:.1f}% ，持仓集中度中等，更像主题内的主动均衡配置。"
        else:
            selection = "当前没有拿到稳定的前十大持仓，选股风格暂时无法下强结论。"

        if is_passive and commodity_like:
            consistency = "这类产品更重要的是跟踪误差、展期成本、保证金结构和流动性，基金经理风格漂移不是核心变量。"
        elif is_passive:
            consistency = "这类产品更重要的是跟踪误差、费率和标的暴露是否清晰，基金经理风格漂移不是核心变量。"
        elif manager:
            consistency = (
                f"经理当前在管约 {manager.get('current_fund_count', 0)} 只产品，"
                f"在管规模约 {manager.get('aum_billion', 0.0):.2f} 亿。"
            )
            if "风格稳定" in tags:
                consistency += " 从在管产品命名和重仓暴露看，风格一致性较强。"
        else:
            consistency = "基金经理画像缺失，无法评估风格一致性。"

        summary = "这只基金更像在买"
        if sector != "综合":
            summary += f"`{sector}`方向"
            if is_passive:
                summary += "的被动暴露"
        else:
            summary += "对应指数/标的本身" if is_passive else "基金经理的主动选股框架"
        if tags:
            summary += "，当前标签是 `" + " / ".join(tags) + "`。"
        else:
            summary += "。"

        taxonomy_sector_hint = " ".join([sector, *chain_nodes, *secondary_text_parts]) if is_passive else sector

        return {
            "sector": sector,
            "chain_nodes": chain_nodes,
            "tags": tags,
            "taxonomy": build_standard_fund_taxonomy(
                name=fund_name,
                fund_type=fund_type,
                invest_type=fund_type,
                benchmark=benchmark_note,
                tracking_target=tracking_target,
                asset_type=asset_type or "cn_fund",
                sector_hint=taxonomy_sector_hint,
                is_passive=is_passive,
                commodity_like=commodity_like,
            ),
            "summary": summary,
            "positioning": positioning,
            "selection": selection,
            "consistency": consistency,
            "benchmark_note": benchmark_note,
            "top5_concentration": round(top5, 2),
            "stock_ratio": round(stock_ratio, 2),
            "cash_ratio": round(cash_ratio, 2),
        }

    def _infer_theme(self, text: str) -> tuple[str, List[str]]:
        return infer_fund_sector(str(text))

    def _manager_style_consistent(self, peer_funds: Iterable[str], sector: str) -> bool:
        peer_text = " ".join(str(item) for item in peer_funds).lower()
        if not peer_text or sector == "综合":
            return False
        for keywords, payload in FUND_THEME_RULES:
            if payload[0] == sector:
                return sum(1 for keyword in keywords if keyword.lower() in peer_text) >= 1
        return False

    def _asset_ratio(self, rows: Iterable[Dict[str, Any]], label: str) -> float:
        for row in rows:
            if str(row.get("资产类型", "")).strip() == label:
                return float(row.get("仓位占比") or 0.0)
        return 0.0

    def _to_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        return float(match.group(0)) if match else None
