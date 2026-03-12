"""Base collector with caching, retrying, and rate limiting."""

from __future__ import annotations

import hashlib
import os
import pickle
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence

import pandas as pd

from src.utils.config import resolve_project_path
from src.utils.logger import logger
from src.utils.retry import RateLimiter

try:
    import tushare as ts
except ImportError:  # pragma: no cover
    ts = None

_tushare_api_instance: Any = None
_tushare_rate_limiter = RateLimiter(min_interval_seconds=0.35)
DIRECT_DATA_HOST_SUFFIXES = (
    ".eastmoney.com",
    ".jin10.com",
    ".tushare.pro",
    ".cninfo.com.cn",
)


def _ensure_direct_data_no_proxy() -> None:
    """Bypass system HTTP proxies for domestic market-data domains.

    On macOS, ``requests`` can inherit system proxies through urllib, even when
    the shell environment is clean. For CN market-data endpoints this adds an
    unnecessary local-proxy hop and has shown up as intermittent
    ``ProxyError(... RemoteDisconnected ...)`` failures. We keep these domains
    on direct/TUN paths and let collector-level retry/cache handle upstream
    throttling separately.
    """

    existing = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    items = [item.strip() for item in existing.split(",") if item.strip()]
    merged: list[str] = []
    seen: set[str] = set()
    for item in [*items, *DIRECT_DATA_HOST_SUFFIXES, "localhost", "127.0.0.1"]:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append(normalized)
    joined = ",".join(merged)
    os.environ["NO_PROXY"] = joined
    os.environ["no_proxy"] = joined


class BaseCollector:
    """Common data collector helpers."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, name: Optional[str] = None) -> None:
        _ensure_direct_data_no_proxy()
        self.config = config or {}
        self.name = name or self.__class__.__name__
        storage = self.config.get("storage", {})
        cache_dir = storage.get("cache_dir", "data/cache")
        self.cache_dir = resolve_project_path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl_hours = int(storage.get("cache_ttl_hours", 4))
        self.rate_limiter = RateLimiter(min_interval_seconds=0.5)

    def _tushare_pro(self) -> Any:
        """Return a cached tushare pro_api instance, or None if unavailable."""
        global _tushare_api_instance
        if ts is None:
            return None
        if _tushare_api_instance is not None:
            return _tushare_api_instance
        token = (self.config.get("api_keys") or {}).get("tushare", "")
        if not token or token == "YOUR_TUSHARE_TOKEN":
            return None
        ts.set_token(token)
        _tushare_api_instance = ts.pro_api()
        return _tushare_api_instance

    def _ts_call(self, api_name: str, **kwargs: Any) -> Any:
        """Call a Tushare pro API with rate limiting. Returns DataFrame or None."""
        pro = self._tushare_pro()
        if pro is None:
            return None
        _tushare_rate_limiter.wait()
        method = getattr(pro, api_name, None)
        if not callable(method):
            logger.warning(f"Tushare API not found: {api_name}")
            return None
        return method(**kwargs)

    @staticmethod
    def _to_ts_code(symbol: str) -> str:
        """Convert 6-digit A-share symbol to Tushare ts_code format (e.g. 000001 → 000001.SZ)."""
        symbol = symbol.strip()
        if "." in symbol:
            return symbol
        if len(symbol) == 6 and symbol.isdigit():
            suffix = "SH" if symbol[0] in ("5", "6", "9") else "SZ"
            return f"{symbol}.{suffix}"
        return symbol

    @staticmethod
    def _from_ts_code(ts_code: str) -> str:
        """Convert Tushare ts_code to bare 6-digit symbol (e.g. 000001.SZ → 000001)."""
        return ts_code.split(".")[0] if "." in ts_code else ts_code

    @staticmethod
    def _ts_index_code_candidates(symbol: str) -> list[str]:
        """Return plausible Tushare index codes for a bare CN index symbol.

        A-share指数代码和股票代码的交易所规则不同，不能直接复用 ``_to_ts_code``。
        常见情况：

        - ``000300`` / ``000905`` / ``000688`` → ``.SH``
        - ``399001`` / ``399006`` → ``.SZ``
        """
        symbol = str(symbol).strip()
        if not symbol:
            return []
        if "." in symbol:
            return [symbol]
        if len(symbol) == 6 and symbol.isdigit():
            if symbol.startswith("399"):
                return [f"{symbol}.SZ", f"{symbol}.SH"]
            return [f"{symbol}.SH", f"{symbol}.SZ"]
        return [symbol]

    def _ts_fund_basic_snapshot(self, market: str) -> Any:
        """Return cached Tushare fund_basic snapshot for the given market."""
        cache_key = f"base:ts_fund_basic:{market}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_basic", market=market)
        if raw is not None and not getattr(raw, "empty", False):
            self._save_cache(cache_key, raw)
            return raw
        return None

    def _resolve_tushare_fund_code(
        self,
        symbol: str,
        preferred_markets: Sequence[str] = ("E", "O", "L"),
    ) -> str:
        """Resolve a bare fund code into a Tushare ``ts_code`` when possible."""
        symbol = str(symbol).strip()
        if not symbol or "." in symbol:
            return symbol
        if not (len(symbol) == 6 and symbol.isdigit()):
            return symbol

        for market in preferred_markets:
            frame = self._ts_fund_basic_snapshot(market)
            if frame is None or getattr(frame, "empty", False):
                continue
            ts_codes = frame.get("ts_code")
            if ts_codes is None:
                continue
            matched = frame[ts_codes.astype(str).str.startswith(f"{symbol}.", na=False)]
            if not matched.empty:
                return str(matched.iloc[0]["ts_code"])

        # Exchange-traded funds commonly use SH/SZ suffixes and can still be
        # queried with the stock-style converter when fund_basic is unavailable.
        return self._to_ts_code(symbol)

    def _latest_open_trade_date(self, lookback_days: int = 14, exchange: str = "SSE") -> str:
        """Return the latest open trade date from Tushare trade_cal when available."""
        end_date = datetime.now().strftime("%Y%m%d")
        # ``trade_cal`` is small; cache by the current end date to avoid repeated scans.
        cache_key = f"base:ts_trade_cal:{exchange}:{end_date}:{lookback_days}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is None:
            from datetime import timedelta

            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
            raw = self._ts_call("trade_cal", exchange=exchange, start_date=start_date, end_date=end_date)
            if raw is None or getattr(raw, "empty", False):
                return ""
            cached = raw
            self._save_cache(cache_key, raw)
        frame = cached.copy()
        if "is_open" not in frame.columns or "cal_date" not in frame.columns:
            return ""
        open_days = frame[pd.to_numeric(frame["is_open"], errors="coerce") == 1]
        if open_days.empty:
            return ""
        return str(open_days["cal_date"].max())

    @staticmethod
    def _first_existing_column(frame: pd.DataFrame, candidates: Sequence[str]) -> str | None:
        """Return the first matching column name from ``candidates``."""
        return next((column for column in candidates if column in frame.columns), None)

    @staticmethod
    def _normalize_date_text(value: Any) -> str:
        """Normalize common Tushare/AKShare date formats to ``YYYY-MM-DD``."""
        if value is None or pd.isna(value):
            return ""
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return ""
        if text.isdigit() and len(text) == 8:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return ""
        return parsed.strftime("%Y-%m-%d")

    def _normalize_north_south_flow_frame(self, frame: pd.DataFrame, source: str = "") -> pd.DataFrame:
        """Normalize north/south flow into a date-level yuan-denominated frame."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        desired = [
            "日期",
            "沪股通净流入",
            "深股通净流入",
            "北向资金净流入",
            "港股通(沪)净流入",
            "港股通(深)净流入",
            "南向资金净流入",
        ]

        source_key = str(source).lower()

        def _numeric_series(raw_frame: pd.DataFrame, candidates: Sequence[str], scale: float = 1.0) -> pd.Series:
            column = self._first_existing_column(raw_frame, candidates)
            if column is None:
                return pd.Series(0.0, index=raw_frame.index, dtype=float)
            series = pd.to_numeric(raw_frame[column], errors="coerce")
            return series.fillna(0.0) * scale

        if {"日期", "北向资金净流入", "南向资金净流入"}.issubset(frame.columns):
            normalized = frame.copy()
        elif {"trade_date", "north_money", "south_money"}.issubset(frame.columns):
            scale = 1_000_000.0  # Tushare moneyflow_hsgt is reported in 百万元.
            normalized = pd.DataFrame(
                {
                    "日期": frame["trade_date"].map(self._normalize_date_text),
                    "沪股通净流入": _numeric_series(frame, ("hgt",), scale=scale),
                    "深股通净流入": _numeric_series(frame, ("sgt",), scale=scale),
                    "北向资金净流入": _numeric_series(frame, ("north_money",), scale=scale),
                    "港股通(沪)净流入": _numeric_series(frame, ("ggt_ss",), scale=scale),
                    "港股通(深)净流入": _numeric_series(frame, ("ggt_sz",), scale=scale),
                    "南向资金净流入": _numeric_series(frame, ("south_money",), scale=scale),
                }
            )
        elif {"资金方向", "成交净买额"}.issubset(frame.columns):
            date_col = self._first_existing_column(frame, ("交易日", "日期"))
            if date_col is None:
                return pd.DataFrame()
            working = frame.copy()
            working["日期"] = working[date_col].map(self._normalize_date_text)
            working["板块"] = working.get("板块", pd.Series("", index=working.index)).astype(str)
            working["资金方向"] = working.get("资金方向", pd.Series("", index=working.index)).astype(str)
            amount = pd.to_numeric(working["成交净买额"], errors="coerce").fillna(0.0)
            if source_key.startswith("ak"):
                amount = amount * 100_000_000.0  # AKShare summary is reported in 亿元.
            working["成交净买额"] = amount

            rows: list[dict[str, float | str]] = []
            for trade_date, group in working.groupby("日期", sort=True):
                if not trade_date:
                    continue
                hgt = float(group.loc[group["板块"].str.fullmatch("沪股通", na=False), "成交净买额"].sum())
                sgt = float(group.loc[group["板块"].str.fullmatch("深股通", na=False), "成交净买额"].sum())
                ggt_sh = float(group.loc[group["板块"].str.fullmatch("港股通\\(沪\\)", na=False), "成交净买额"].sum())
                ggt_sz = float(group.loc[group["板块"].str.fullmatch("港股通\\(深\\)", na=False), "成交净买额"].sum())
                north = float(group.loc[group["资金方向"].str.contains("北向", na=False), "成交净买额"].sum())
                south = float(group.loc[group["资金方向"].str.contains("南向", na=False), "成交净买额"].sum())
                rows.append(
                    {
                        "日期": trade_date,
                        "沪股通净流入": hgt,
                        "深股通净流入": sgt,
                        "北向资金净流入": north if north else hgt + sgt,
                        "港股通(沪)净流入": ggt_sh,
                        "港股通(深)净流入": ggt_sz,
                        "南向资金净流入": south if south else ggt_sh + ggt_sz,
                    }
                )
            normalized = pd.DataFrame(rows)
        else:
            date_col = self._first_existing_column(frame, ("trade_date", "交易日", "日期"))
            if date_col is None:
                return pd.DataFrame()
            scale = 100_000_000.0 if source_key.startswith("ak") else 1.0
            normalized = pd.DataFrame(
                {
                    "日期": frame[date_col].map(self._normalize_date_text),
                    "沪股通净流入": _numeric_series(frame, ("沪股通净流入", "沪股通", "hgt"), scale=scale),
                    "深股通净流入": _numeric_series(frame, ("深股通净流入", "深股通", "sgt"), scale=scale),
                    "北向资金净流入": _numeric_series(frame, ("北向资金净流入", "北向资金", "north_money"), scale=scale),
                    "港股通(沪)净流入": _numeric_series(frame, ("港股通(沪)净流入", "港股通(沪)", "ggt_ss"), scale=scale),
                    "港股通(深)净流入": _numeric_series(frame, ("港股通(深)净流入", "港股通(深)", "ggt_sz"), scale=scale),
                    "南向资金净流入": _numeric_series(frame, ("南向资金净流入", "南向资金", "south_money"), scale=scale),
                }
            )

        for column in desired[1:]:
            if column not in normalized.columns:
                normalized[column] = 0.0
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        normalized["日期"] = normalized["日期"].map(self._normalize_date_text)
        normalized = normalized[normalized["日期"].astype(str) != ""]
        normalized = normalized.dropna(subset=["日期"]).sort_values("日期").drop_duplicates("日期", keep="last")
        return normalized[desired].reset_index(drop=True)

    def _normalize_margin_summary_frame(
        self,
        frame: pd.DataFrame,
        source: str = "",
        default_date: str = "",
    ) -> pd.DataFrame:
        """Normalize margin summary into exchange-level yuan-denominated rows."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        desired = [
            "日期",
            "交易所",
            "融资余额",
            "融资买入额",
            "融资偿还额",
            "融券余额",
            "融资融券余额",
        ]
        source_key = str(source).lower()

        def _numeric_series(raw_frame: pd.DataFrame, candidates: Sequence[str], scale: float = 1.0) -> pd.Series:
            column = self._first_existing_column(raw_frame, candidates)
            if column is None:
                return pd.Series(pd.NA, index=raw_frame.index, dtype="Float64")
            series = pd.to_numeric(raw_frame[column], errors="coerce")
            return series.astype("Float64") * scale

        if {"日期", "交易所", "融资余额"}.issubset(frame.columns):
            normalized = frame.copy()
        elif {"trade_date", "exchange_id", "rzye"}.issubset(frame.columns):
            exchange_map = {"SSE": "上交所", "SZSE": "深交所"}
            normalized = pd.DataFrame(
                {
                    "日期": frame["trade_date"].map(self._normalize_date_text),
                    "交易所": frame["exchange_id"].astype(str).map(lambda value: exchange_map.get(value, value)),
                    "融资余额": _numeric_series(frame, ("rzye",)),
                    "融资买入额": _numeric_series(frame, ("rzmre",)),
                    "融资偿还额": _numeric_series(frame, ("rzche",)),
                    "融券余额": _numeric_series(frame, ("rqye",)),
                    "融资融券余额": _numeric_series(frame, ("rzrqye",)),
                }
            )
        elif "信用交易日期" in frame.columns and "融资余额" in frame.columns:
            normalized = pd.DataFrame(
                {
                    "日期": frame["信用交易日期"].map(self._normalize_date_text),
                    "交易所": "上交所",
                    "融资余额": _numeric_series(frame, ("融资余额",)),
                    "融资买入额": _numeric_series(frame, ("融资买入额",)),
                    "融资偿还额": _numeric_series(frame, ("融资偿还额",)),
                    "融券余额": _numeric_series(frame, ("融券余额", "融券余量金额")),
                    "融资融券余额": _numeric_series(frame, ("融资融券余额",)),
                }
            )
        else:
            date_text = self._normalize_date_text(default_date)
            money_scale = 100_000_000.0 if source_key.startswith("ak") else 1.0
            exchange_name = "深交所" if "szse" in source_key else "上交所"
            normalized = pd.DataFrame(
                {
                    "日期": pd.Series(date_text, index=frame.index),
                    "交易所": exchange_name,
                    "融资余额": _numeric_series(frame, ("融资余额", "rzye"), scale=money_scale),
                    "融资买入额": _numeric_series(frame, ("融资买入额", "rzmre"), scale=money_scale),
                    "融资偿还额": _numeric_series(frame, ("融资偿还额", "rzche"), scale=money_scale),
                    "融券余额": _numeric_series(frame, ("融券余额", "融券余量金额", "rqye"), scale=money_scale),
                    "融资融券余额": _numeric_series(frame, ("融资融券余额", "rzrqye"), scale=money_scale),
                }
            )

        normalized["日期"] = normalized["日期"].map(self._normalize_date_text)
        normalized["交易所"] = normalized["交易所"].astype(str)
        normalized = normalized[normalized["日期"].astype(str) != ""]
        normalized = normalized.dropna(subset=["日期", "交易所"])
        for column in desired[2:]:
            if column not in normalized.columns:
                normalized[column] = pd.Series(pd.NA, index=normalized.index, dtype="Float64")
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        return normalized[desired].sort_values(["日期", "交易所"]).reset_index(drop=True)

    def _normalize_hsgt_top10_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize Tushare hsgt_top10 rows into a stable schema."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        desired = ["日期", "代码", "名称", "市场", "排名", "收盘价", "涨跌幅", "成交额", "净买额", "买入额", "卖出额"]

        if {"日期", "代码", "名称", "市场", "净买额"}.issubset(frame.columns):
            normalized = frame.copy()
        elif {"trade_date", "ts_code", "name"}.issubset(frame.columns):
            market_map = {
                "1": "沪股通",
                "2": "港股通(沪)",
                "3": "深股通",
                "4": "港股通(深)",
            }
            normalized = pd.DataFrame(
                {
                    "日期": frame["trade_date"].map(self._normalize_date_text),
                    "代码": frame["ts_code"].astype(str).map(self._from_ts_code),
                    "名称": frame["name"].astype(str),
                    "市场": frame.get("market_type", pd.Series("", index=frame.index)).astype(str).map(
                        lambda value: market_map.get(value, value)
                    ),
                    "排名": pd.to_numeric(frame.get("rank"), errors="coerce"),
                    "收盘价": pd.to_numeric(frame.get("close"), errors="coerce"),
                    "涨跌幅": pd.to_numeric(frame.get("change"), errors="coerce"),
                    "成交额": pd.to_numeric(frame.get("amount"), errors="coerce"),
                    "净买额": pd.to_numeric(frame.get("net_amount"), errors="coerce"),
                    "买入额": pd.to_numeric(frame.get("buy"), errors="coerce"),
                    "卖出额": pd.to_numeric(frame.get("sell"), errors="coerce"),
                }
            )
        else:
            return pd.DataFrame()

        normalized["日期"] = normalized["日期"].map(self._normalize_date_text)
        normalized = normalized[normalized["日期"].astype(str) != ""]
        for column in desired[4:]:
            if column not in normalized.columns:
                normalized[column] = pd.NA
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        return normalized[desired].sort_values(["日期", "市场", "排名"], na_position="last").reset_index(drop=True)

    def _normalize_pledge_stat_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize Tushare pledge_stat rows into a stable schema."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        desired = [
            "截止日期",
            "代码",
            "名称",
            "质押次数",
            "无限售质押股数(万股)",
            "限售质押股数(万股)",
            "总股本",
            "质押比例",
        ]

        if {"截止日期", "代码", "名称", "质押比例"}.issubset(frame.columns):
            normalized = frame.copy()
        elif {"end_date", "ts_code"}.issubset(frame.columns):
            normalized = pd.DataFrame(
                {
                    "截止日期": frame["end_date"].map(self._normalize_date_text),
                    "代码": frame["ts_code"].astype(str).map(self._from_ts_code),
                    "名称": frame.get("name", pd.Series("", index=frame.index)).astype(str),
                    "质押次数": pd.to_numeric(frame.get("pledge_count"), errors="coerce"),
                    "无限售质押股数(万股)": pd.to_numeric(frame.get("unrest_pledge"), errors="coerce"),
                    "限售质押股数(万股)": pd.to_numeric(frame.get("rest_pledge"), errors="coerce"),
                    "总股本": pd.to_numeric(frame.get("total_share"), errors="coerce"),
                    "质押比例": pd.to_numeric(frame.get("pledge_ratio"), errors="coerce"),
                }
            )
        else:
            return pd.DataFrame()

        normalized["截止日期"] = normalized["截止日期"].map(self._normalize_date_text)
        normalized = normalized[normalized["截止日期"].astype(str) != ""]
        for column in desired[3:]:
            if column not in normalized.columns:
                normalized[column] = pd.NA
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        return normalized[desired].sort_values(["截止日期", "质押比例"], ascending=[True, False]).reset_index(drop=True)

    def _cache_path(self, cache_key: str) -> Path:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{self.name.lower()}_{digest}.pkl"

    def _load_cache(self, cache_key: str, ttl_hours: Optional[int] = None, allow_stale: bool = False) -> Any:
        cache_path = self._cache_path(cache_key)
        if not cache_path.exists():
            return None
        effective_ttl = ttl_hours if ttl_hours is not None else self.cache_ttl_hours
        max_age_seconds = effective_ttl * 3600
        if not allow_stale and effective_ttl >= 0 and time.time() - cache_path.stat().st_mtime > max_age_seconds:
            return None
        with cache_path.open("rb") as handle:
            return pickle.load(handle)

    def _save_cache(self, cache_key: str, payload: Any) -> None:
        cache_path = self._cache_path(cache_key)
        with cache_path.open("wb") as handle:
            pickle.dump(payload, handle)

    @staticmethod
    def _looks_like_resolution_error(exc: BaseException) -> bool:
        text = str(exc)
        markers = (
            "NameResolutionError",
            "Failed to resolve",
            "nodename nor servname provided",
            "Temporary failure in name resolution",
            "Could not resolve host",
            "curl: (6)",
            "ProxyError",
            "RemoteDisconnected",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def _is_nonrecoverable_fetch_error(fetcher: Callable[..., Any], exc: BaseException) -> bool:
        if BaseCollector._looks_like_resolution_error(exc):
            return True
        fetcher_name = getattr(fetcher, "__name__", "")
        text = str(exc)
        if fetcher_name == "history" and "'NoneType' object is not subscriptable" in text:
            return True
        return False

    def _execute_fetcher(
        self,
        fetcher: Callable[..., Any],
        *args,
        attempts: int = 3,
        backoff_seconds: float = 1.0,
        backoff_multiplier: float = 2.0,
        **kwargs,
    ) -> Any:
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            try:
                return fetcher(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if self._is_nonrecoverable_fetch_error(fetcher, exc):
                    raise
                if attempt >= attempts:
                    raise
                sleep_seconds = backoff_seconds * (backoff_multiplier ** (attempt - 1))
                logger.warning(
                    f"Retrying _execute_fetcher after error on attempt {attempt}/{attempts}: {exc}"
                )
                time.sleep(sleep_seconds)
        if last_error is not None:
            raise last_error
        return None

    def cached_call(
        self,
        cache_key: str,
        fetcher: Callable[..., Any],
        *args,
        ttl_hours: Optional[int] = None,
        use_cache: bool = True,
        prefer_stale: bool = False,
        **kwargs,
    ) -> Any:
        """Return cached result when possible, otherwise fetch and cache."""
        if use_cache:
            cached = self._load_cache(cache_key, ttl_hours=ttl_hours)
            if cached is not None:
                logger.info(f"{self.name} cache hit: {cache_key}")
                return cached
            stale_cached = self._load_cache(cache_key, ttl_hours=ttl_hours, allow_stale=True)
            if prefer_stale and stale_cached is not None:
                logger.info(f"{self.name} stale cache hit: {cache_key}")
                return stale_cached
        else:
            stale_cached = None

        self.rate_limiter.wait()
        try:
            result = self._execute_fetcher(
                fetcher,
                *args,
                **kwargs,
            )
        except Exception as exc:
            if stale_cached is not None:
                logger.warning(f"{self.name} fetch failed for {cache_key}; using stale cache: {exc}")
                return stale_cached
            raise
        if result is None or getattr(result, "empty", False):
            if stale_cached is not None:
                logger.warning(f"{self.name} returned empty result for {cache_key}; using stale cache")
                return stale_cached
            raise ValueError(f"{self.name} returned empty result for {cache_key}")
        if use_cache:
            self._save_cache(cache_key, result)
        return result
