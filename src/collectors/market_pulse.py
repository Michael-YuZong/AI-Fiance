"""A-share market pulse collectors for briefing generation — Tushare-first."""

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
        as_of = reference_date or datetime.now()

        zt_info = self._ts_limit_pool("U", as_of)
        dt_info = self._ts_limit_pool("D", as_of)
        strong_info = self._derive_strong_pool(zt_info)
        prev_zt_info = self._derive_prev_zt_pool(zt_info["date"])

        if zt_info["frame"].empty and ak is not None:
            zt_info = self._latest_pool("stock_zt_pool_em", as_of)
        if prev_zt_info["frame"].empty and ak is not None:
            prev_zt_info = self._latest_pool("stock_zt_pool_previous_em", as_of)
        if strong_info["frame"].empty and ak is not None:
            strong_info = self._latest_pool("stock_zt_pool_strong_em", as_of)
        if dt_info["frame"].empty and ak is not None:
            dt_info = self._latest_pool("stock_zt_pool_dtgc_em", as_of)

        latest_trade_date = (
            zt_info["date"]
            or prev_zt_info["date"]
            or strong_info["date"]
            or dt_info["date"]
            or as_of.strftime("%Y-%m-%d")
        )

        # 龙虎榜 — Tushare 优先
        lhb_detail = self._ts_top_list(latest_trade_date)
        lhb_institution = self._ts_top_inst(latest_trade_date)

        # AKShare 兜底（如果 Tushare 数据为空）
        if lhb_detail.empty and ak is not None:
            lhb_detail = self._lhb_detail(latest_trade_date)
        if lhb_institution.empty and ak is not None:
            lhb_institution = self._lhb_institution(latest_trade_date)

        lhb_stats = self._ts_lhb_stats(latest_trade_date)
        if lhb_stats.empty and ak is not None:
            lhb_stats = self._lhb_stats()

        lhb_desks = self._ts_lhb_active_desks(latest_trade_date)
        if lhb_desks.empty and ak is not None:
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

    # ── Tushare: 龙虎榜 ──────────────────────────────────────

    def _ts_top_list(self, trade_date: str) -> pd.DataFrame:
        """Tushare top_list — 龙虎榜每日明细。"""
        if not trade_date:
            return pd.DataFrame()
        date_str = trade_date.replace("-", "")
        cache_key = f"market_pulse:ts_top_list:{date_str}"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached
        raw = self._ts_call("top_list", trade_date=date_str)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def _ts_top_inst(self, trade_date: str) -> pd.DataFrame:
        """Tushare top_inst — 龙虎榜机构交易明细。"""
        if not trade_date:
            return pd.DataFrame()
        date_str = trade_date.replace("-", "")
        cache_key = f"market_pulse:ts_top_inst:{date_str}"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached
        raw = self._ts_call("top_inst", trade_date=date_str)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def _ts_limit_pool(self, limit_type: str, reference_date: datetime) -> Dict[str, Any]:
        label = {"U": "zt", "D": "dt"}.get(limit_type, limit_type.lower())
        for trade_date in reversed(self._recent_open_trade_dates(lookback_days=14)):
            cache_key = f"market_pulse:ts_limit_pool:{label}:{trade_date}:v1"
            cached = self._load_cache(cache_key, ttl_hours=2)
            if cached is not None:
                return {
                    "date": self._normalize_date_text(trade_date),
                    "frame": cached.reset_index(drop=True),
                }
            try:
                raw = self._ts_call("limit_list_d", trade_date=trade_date, limit_type=limit_type)
            except Exception:
                raw = None
            normalized = self._normalize_limit_list(raw, trade_date=trade_date, limit_type=limit_type)
            if not normalized.empty:
                self._save_cache(cache_key, normalized)
                return {
                    "date": self._normalize_date_text(trade_date),
                    "frame": normalized.reset_index(drop=True),
                }
        return {"date": "", "frame": pd.DataFrame()}

    def _normalize_limit_list(
        self,
        frame: pd.DataFrame | None,
        trade_date: str = "",
        limit_type: str = "U",
    ) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        name_col = self._first_existing_column(working, ("name", "名称"))
        industry_col = self._first_existing_column(working, ("industry", "所属行业"))
        pct_col = self._first_existing_column(working, ("pct_chg", "change_pct", "涨跌幅"))
        up_stat_col = self._first_existing_column(working, ("up_stat", "涨停统计", "连板数"))
        limit_times_col = self._first_existing_column(working, ("limit_times", "连板数"))
        if ts_code_col is None:
            return pd.DataFrame()

        if industry_col is None:
            stock_basic = self._ts_call("stock_basic", exchange="", list_status="L", fields="ts_code,industry")
            if stock_basic is not None and not stock_basic.empty:
                working = working.merge(stock_basic[["ts_code", "industry"]], left_on=ts_code_col, right_on="ts_code", how="left")
                industry_col = "industry"

        normalized = pd.DataFrame(
            {
                "代码": working[ts_code_col].astype(str).map(self._from_ts_code),
                "名称": working.get(name_col, pd.Series("", index=working.index)).astype(str),
                "所属行业": working.get(industry_col, pd.Series("", index=working.index)).astype(str),
                "涨跌幅": pd.to_numeric(working.get(pct_col, pd.Series(pd.NA, index=working.index)), errors="coerce"),
                "连板数": self._extract_limit_times(working, up_stat_col, limit_times_col),
                "涨停统计": working.get(up_stat_col, pd.Series("", index=working.index)).astype(str),
                "交易日": working.get("trade_date", pd.Series(trade_date, index=working.index)).map(self._normalize_date_text),
                "状态": "涨停" if limit_type == "U" else "跌停",
            }
        )
        return normalized.drop_duplicates("代码").reset_index(drop=True)

    def _extract_limit_times(
        self,
        frame: pd.DataFrame,
        up_stat_col: str | None,
        limit_times_col: str | None,
    ) -> pd.Series:
        if limit_times_col is not None:
            series = pd.to_numeric(frame[limit_times_col], errors="coerce")
            if not series.isna().all():
                return series
        if up_stat_col is None:
            return pd.Series(pd.NA, index=frame.index, dtype="Float64")
        parsed = frame[up_stat_col].astype(str).str.extract(r"(?P<count>\d+)")[["count"]]
        return pd.to_numeric(parsed["count"], errors="coerce")

    def _derive_strong_pool(self, zt_info: Dict[str, Any]) -> Dict[str, Any]:
        frame = zt_info.get("frame", pd.DataFrame())
        if frame is None or frame.empty:
            return {"date": zt_info.get("date", ""), "frame": pd.DataFrame()}
        working = frame.copy()
        if "连板数" not in working.columns:
            return {"date": zt_info.get("date", ""), "frame": pd.DataFrame()}
        strong = working[pd.to_numeric(working["连板数"], errors="coerce").fillna(0) > 1].reset_index(drop=True)
        return {"date": zt_info.get("date", ""), "frame": strong}

    def _derive_prev_zt_pool(self, trade_date: str) -> Dict[str, Any]:
        if not trade_date:
            return {"date": "", "frame": pd.DataFrame()}
        recent = [date for date in self._recent_open_trade_dates(lookback_days=20) if self._normalize_date_text(date) < trade_date]
        if not recent:
            return {"date": trade_date, "frame": pd.DataFrame()}
        prev_trade_date = recent[-1]
        try:
            prev_raw = self._ts_call("limit_list_d", trade_date=prev_trade_date, limit_type="U")
        except Exception:
            prev_raw = None
        prev_limit = self._normalize_limit_list(prev_raw, trade_date=prev_trade_date, limit_type="U")
        if prev_limit.empty:
            return {"date": trade_date, "frame": pd.DataFrame()}
        try:
            daily = self._ts_call("daily", trade_date=trade_date.replace("-", ""))
        except Exception:
            daily = None
        if daily is None or daily.empty:
            try:
                daily = self._ts_call("daily", trade_date=trade_date)
            except Exception:
                daily = None
        if daily is None or daily.empty:
            return {"date": trade_date, "frame": prev_limit}
        daily_view = daily[["ts_code", "pct_chg"]].copy()
        daily_view["代码"] = daily_view["ts_code"].astype(str).map(self._from_ts_code)
        daily_view = daily_view.rename(columns={"pct_chg": "涨跌幅"})
        merged = prev_limit.drop(columns=["涨跌幅"], errors="ignore").merge(daily_view[["代码", "涨跌幅"]], on="代码", how="left")
        return {"date": trade_date, "frame": merged.reset_index(drop=True)}

    def _ts_lhb_stats(self, trade_date: str) -> pd.DataFrame:
        if not trade_date:
            return pd.DataFrame()
        cache_key = f"market_pulse:ts_lhb_stats:{trade_date}:v1"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        dates = [date for date in self._recent_open_trade_dates(lookback_days=45) if self._normalize_date_text(date) <= trade_date][-22:]
        rows: List[pd.DataFrame] = []
        for date in dates:
            try:
                frame = self._ts_top_list(self._normalize_date_text(date))
            except Exception:
                frame = pd.DataFrame()
            if frame is None or frame.empty:
                continue
            rows.append(frame)
        if not rows:
            return pd.DataFrame()

        merged = pd.concat(rows, ignore_index=True)
        name_col = self._first_existing_column(merged, ("name", "名称"))
        ts_code_col = self._first_existing_column(merged, ("ts_code", "代码"))
        if name_col is None or ts_code_col is None:
            return pd.DataFrame()
        grouped = (
            merged.assign(名称=merged[name_col].astype(str), 代码=merged[ts_code_col].astype(str).map(self._from_ts_code))
            .groupby(["代码", "名称"], as_index=False)
            .size()
            .rename(columns={"size": "上榜次数"})
            .sort_values("上榜次数", ascending=False)
            .reset_index(drop=True)
        )
        self._save_cache(cache_key, grouped)
        return grouped

    def _ts_lhb_active_desks(self, trade_date: str) -> pd.DataFrame:
        if not trade_date:
            return pd.DataFrame()
        detail = self._ts_top_list(trade_date)
        if detail is None or detail.empty:
            return pd.DataFrame()
        desk_col = self._first_existing_column(detail, ("exalter", "营业部名称"))
        net_col = self._first_existing_column(detail, ("net_buy", "净买额", "总买卖净额"))
        if desk_col is None or net_col is None:
            return pd.DataFrame()
        desks = (
            detail.assign(
                营业部名称=detail[desk_col].astype(str),
                总买卖净额=pd.to_numeric(detail[net_col], errors="coerce"),
                上榜日=trade_date,
            )
            .dropna(subset=["营业部名称", "总买卖净额"])
            .groupby("营业部名称", as_index=False)["总买卖净额"]
            .sum()
            .sort_values("总买卖净额", ascending=False)
            .reset_index(drop=True)
        )
        desks["上榜日"] = trade_date
        return desks

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
                    prefer_stale=True,
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
                prefer_stale=True,
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
                prefer_stale=True,
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
                prefer_stale=True,
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
                prefer_stale=True,
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
