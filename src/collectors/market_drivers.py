"""Broad market drivers for rotation and capital-flow sections — Tushare-first."""

from __future__ import annotations

import io
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from typing import Any, Dict, Mapping, Optional, Sequence

import pandas as pd

from .base import BaseCollector
from .market_cn import ChinaMarketCollector

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
        cn_market = ChinaMarketCollector(self.config)

        result: Dict[str, Any] = {
            "market_flow": self._market_flow(as_of),
            "northbound_flow": cn_market.get_north_south_flow(),
            "northbound_top10": self._ts_northbound_top10(),
            "margin_summary": cn_market.get_margin_trading(),
            "pledge_stat": self._ts_pledge_stat(),
        }

        result["industry_fund_flow"] = self._ts_sector_fund_flow("industry")
        if result["industry_fund_flow"].empty:
            result["industry_fund_flow"] = self._sector_fund_flow("行业资金流")

        result["concept_fund_flow"] = self._ts_sector_fund_flow("concept")
        if result["concept_fund_flow"].empty:
            result["concept_fund_flow"] = self._sector_fund_flow("概念资金流")

        result["northbound_industry"] = self._ts_northbound_industry(as_of)
        if result["northbound_industry"]["frame"].empty:
            result["northbound_industry"] = self._northbound_rank("北向资金增持行业板块排行", as_of)

        # Tushare 目前更适合稳定补行业级北向持仓变化，概念级无可靠一对一映射时回退为空/AK。
        result["northbound_concept"] = self._empty_rank_report()
        if ak is not None:
            fallback_concept = self._northbound_rank("北向资金增持概念板块排行", as_of)
            if not fallback_concept["frame"].empty:
                result["northbound_concept"] = fallback_concept

        result["industry_spot"] = self._ts_board_spot("industry")
        if result["industry_spot"].empty:
            result["industry_spot"] = self._board_spot("stock_board_industry_name_em")

        result["concept_spot"] = self._ts_board_spot("concept")
        if result["concept_spot"].empty:
            result["concept_spot"] = self._board_spot("stock_board_concept_name_em")

        result["hot_rank"] = self._ts_hot_rank()
        if result["hot_rank"].empty:
            result["hot_rank"] = self._hot_rank()

        return result

    # ── Tushare: 沪深港通资金流向 ─────────────────────────────

    def _ts_northbound_flow(self) -> pd.DataFrame:
        """Tushare moneyflow_hsgt — 北向/南向每日资金流向。"""
        cache_key = "market_drivers:ts_northbound_flow:v2"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None:
            return cached
        start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        raw = self._ts_call("moneyflow_hsgt", start_date=start, end_date=end)
        if raw is not None and not raw.empty:
            normalized = self._normalize_north_south_flow_frame(raw, source="tushare")
            if not normalized.empty:
                self._save_cache(cache_key, normalized)
                return normalized
        return pd.DataFrame()

    def _ts_northbound_top10(self) -> pd.DataFrame:
        """Tushare hsgt_top10 — 沪深港通十大成交股。"""
        cache_key = "market_drivers:ts_northbound_top10:v2"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached
        for offset in range(0, 6):
            trade_date = (datetime.now() - timedelta(days=offset)).strftime("%Y%m%d")
            raw = self._ts_call("hsgt_top10", trade_date=trade_date)
            if raw is not None and not raw.empty:
                normalized = self._normalize_hsgt_top10_frame(raw)
                if normalized.empty:
                    continue
                self._save_cache(cache_key, normalized)
                return normalized
        return pd.DataFrame()

    # ── Tushare: 融资融券 ─────────────────────────────────────

    def _ts_margin_summary(self) -> pd.DataFrame:
        """Tushare margin — 全市场融资融券汇总。"""
        cache_key = "market_drivers:ts_margin_summary:v2"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached
        for offset in range(0, 6):
            trade_date = (datetime.now() - timedelta(days=offset)).strftime("%Y%m%d")
            raw = self._ts_call("margin", trade_date=trade_date)
            if raw is not None and not raw.empty:
                normalized = self._normalize_margin_summary_frame(raw, source="tushare")
                if normalized.empty:
                    continue
                self._save_cache(cache_key, normalized)
                return normalized
        return pd.DataFrame()

    # ── Tushare: 股权质押 ─────────────────────────────────────

    def _ts_pledge_stat(self) -> pd.DataFrame:
        """Tushare pledge_stat — 大股东股权质押统计。"""
        cache_key = "market_drivers:ts_pledge_stat:v2"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("pledge_stat", end_date=datetime.now().strftime("%Y%m%d"))
        if raw is not None and not raw.empty:
            normalized = self._normalize_pledge_stat_frame(raw)
            if not normalized.empty:
                self._save_cache(cache_key, normalized)
                return normalized
        return pd.DataFrame()

    def _market_flow(self, reference_date: datetime) -> Dict[str, Any]:
        ts_report = self._ts_market_flow(reference_date)
        if not ts_report["frame"].empty:
            return ts_report

        fetcher = getattr(ak, "stock_market_fund_flow", None)
        if not callable(fetcher):
            return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": ""}
        try:
            frame = self.cached_call(
                "market_drivers:market_flow",
                self._quiet_fetch,
                fetcher,
                ttl_hours=2,
                prefer_stale=True,
            )
        except Exception:
            return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": ""}
        latest_date = self._extract_latest_date(frame, "日期")
        return {
            "frame": frame.reset_index(drop=True),
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, reference_date),
        }

    def _ts_market_flow(self, reference_date: datetime) -> Dict[str, Any]:
        """Aggregate Tushare moneyflow into the market-flow shape used by briefing."""
        cache_key = "market_drivers:ts_market_flow"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None and not cached.empty:
            latest_date = self._extract_latest_date(cached, "日期")
            return {
                "frame": cached.reset_index(drop=True),
                "latest_date": latest_date,
                "is_fresh": self._is_fresh(latest_date, reference_date),
            }

        for offset in range(0, 8):
            trade_date = self._latest_open_trade_date(lookback_days=14) if offset == 0 else ""
            if not trade_date:
                trade_date = (datetime.now() - timedelta(days=offset)).strftime("%Y%m%d")
            elif offset > 0:
                trade_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=offset)).strftime("%Y%m%d")

            raw = self._ts_call("moneyflow", trade_date=trade_date)
            if raw is None or raw.empty:
                continue

            numeric_cols = [
                "buy_sm_amount", "sell_sm_amount", "buy_md_amount", "sell_md_amount",
                "buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount",
            ]
            frame = raw.copy()
            for col in numeric_cols:
                if col in frame.columns:
                    frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)

            small_amt = float((frame["buy_sm_amount"] - frame["sell_sm_amount"]).sum()) * 10_000.0
            medium_amt = float((frame["buy_md_amount"] - frame["sell_md_amount"]).sum()) * 10_000.0
            big_amt = float((frame["buy_lg_amount"] - frame["sell_lg_amount"]).sum()) * 10_000.0
            super_amt = float((frame["buy_elg_amount"] - frame["sell_elg_amount"]).sum()) * 10_000.0
            main_amt = big_amt + super_amt

            daily = self._ts_call("daily", trade_date=trade_date)
            total_turnover = 0.0
            if daily is not None and not daily.empty and "amount" in daily.columns:
                total_turnover = float(pd.to_numeric(daily["amount"], errors="coerce").fillna(0.0).sum()) * 1000.0
            if total_turnover <= 0:
                total_turnover = sum(
                    float(frame[col].sum()) for col in numeric_cols if col in frame.columns
                ) * 10_000.0 / 2.0

            row = pd.DataFrame(
                [
                    {
                        "日期": datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d"),
                        "主力净流入-净额": main_amt,
                        "主力净流入-净占比": (main_amt / total_turnover * 100.0) if total_turnover else None,
                        "超大单净流入-净额": super_amt,
                        "大单净流入-净额": big_amt,
                        "中单净流入-净额": medium_amt,
                        "小单净流入-净额": small_amt,
                        "成交额": total_turnover if total_turnover else None,
                    }
                ]
            )
            self._save_cache(cache_key, row)
            latest_date = str(row.iloc[0]["日期"])
            return {
                "frame": row,
                "latest_date": latest_date,
                "is_fresh": self._is_fresh(latest_date, reference_date),
            }

        return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": ""}

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
                prefer_stale=True,
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

    def _empty_rank_report(self) -> Dict[str, Any]:
        return {"frame": pd.DataFrame(), "is_fresh": False, "latest_date": "", "symbol": ""}

    def _ts_board_spot(self, board_type: str) -> pd.DataFrame:
        cache_key = f"market_drivers:ts_board_spot:{board_type}:v1"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None:
            return cached

        board_rows = self._ts_board_index_rows(board_type)
        if board_rows.empty:
            return pd.DataFrame()

        trade_date = self._latest_open_trade_date(lookback_days=14)
        if not trade_date:
            return pd.DataFrame()

        try:
            daily = self._ts_call("ths_daily", trade_date=trade_date)
        except Exception:
            daily = None
        if daily is None or daily.empty:
            return pd.DataFrame()

        working = daily.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code", "指数代码"))
        if ts_code_col is None:
            return pd.DataFrame()
        working["ts_code"] = working[ts_code_col].astype(str)
        working = working.merge(board_rows[["ts_code", "名称", "板块类型"]], on="ts_code", how="inner")
        if working.empty:
            return pd.DataFrame()

        normalized = pd.DataFrame(
            {
                "代码": working["ts_code"].astype(str),
                "名称": working["名称"].astype(str),
                "板块类型": working["板块类型"].astype(str),
                "涨跌幅": pd.to_numeric(
                    working.get("pct_change", working.get("change_pct", working.get("涨跌幅"))),
                    errors="coerce",
                ),
                "成交额": pd.to_numeric(
                    working.get("amount", working.get("成交额")),
                    errors="coerce",
                ) * 1000.0,
                "日期": working.get("trade_date", pd.Series(trade_date, index=working.index)).map(self._normalize_date_text),
            }
        )
        normalized = normalized.dropna(subset=["涨跌幅"]).sort_values("涨跌幅", ascending=False).reset_index(drop=True)
        self._save_cache(cache_key, normalized)
        return normalized

    def _ts_board_index_rows(self, board_type: str) -> pd.DataFrame:
        cache_key = f"market_drivers:ts_board_index_rows:{board_type}:v1"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        type_map = {"industry": "I", "concept": "N"}
        type_code = type_map.get(board_type, "")
        raw = None
        for kwargs in (
            {"exchange": "A", "type": type_code},
            {"type": type_code},
            {"exchange": "A"},
            {},
        ):
            filtered_kwargs = {key: value for key, value in kwargs.items() if value}
            try:
                raw = self._ts_call("ths_index", **filtered_kwargs)
            except Exception:
                raw = None
            if raw is not None and not raw.empty:
                break
        if raw is None or raw.empty:
            return pd.DataFrame()

        frame = raw.copy()
        ts_code_col = self._first_existing_column(frame, ("ts_code", "code"))
        name_col = self._first_existing_column(frame, ("name", "名称"))
        if ts_code_col is None or name_col is None:
            return pd.DataFrame()
        frame["ts_code"] = frame[ts_code_col].astype(str)
        frame["名称"] = frame[name_col].astype(str)
        raw_type_col = self._first_existing_column(frame, ("type", "板块类型"))
        if raw_type_col is not None:
            frame["raw_type"] = frame[raw_type_col].astype(str)
            if type_code:
                frame = frame[frame["raw_type"].eq(type_code)]
        frame["板块类型"] = board_type
        result = frame[["ts_code", "名称", "板块类型"]].drop_duplicates("ts_code").reset_index(drop=True)
        self._save_cache(cache_key, result)
        return result

    def _ts_sector_fund_flow(self, board_type: str) -> pd.DataFrame:
        api_name = "moneyflow_ind_ths" if board_type == "industry" else "moneyflow_cnt_ths"
        cache_key = f"market_drivers:ts_sector_fund_flow:{board_type}:v1"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None:
            return cached

        for trade_date in reversed(self._recent_open_trade_dates(lookback_days=14)):
            try:
                raw = self._ts_call(api_name, trade_date=trade_date)
            except Exception:
                raw = None
            normalized = self._normalize_ths_moneyflow(raw, trade_date=trade_date)
            if not normalized.empty:
                self._save_cache(cache_key, normalized)
                return normalized
        return pd.DataFrame()

    def _normalize_ths_moneyflow(self, frame: pd.DataFrame | None, trade_date: str = "") -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        working = frame.copy()
        name_col = self._first_existing_column(working, ("name", "名称", "板块名称", "概念名称", "行业"))
        if name_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "名称": working[name_col].astype(str),
                "行业": working[name_col].astype(str),
                "涨跌幅": pd.to_numeric(
                    working.get("pct_change", working.get("change_pct", working.get("涨跌幅"))),
                    errors="coerce",
                ),
                "今日主力净流入-净额": pd.to_numeric(
                    working.get("net_amount", working.get("主力净流入-净额", working.get("今日主力净流入-净额"))),
                    errors="coerce",
                ) * 10_000.0,
                "今日主力净流入-净占比": pd.to_numeric(
                    working.get("net_amount_rate", working.get("主力净流入-净占比", working.get("今日主力净流入-净占比"))),
                    errors="coerce",
                ),
                "今日超大单净流入-净额": pd.to_numeric(
                    working.get("buy_elg_amount", working.get("超大单净流入-净额", working.get("今日超大单净流入-净额"))),
                    errors="coerce",
                ) * 10_000.0,
                "今日大单净流入-净额": pd.to_numeric(
                    working.get("buy_lg_amount", working.get("大单净流入-净额", working.get("今日大单净流入-净额"))),
                    errors="coerce",
                ) * 10_000.0,
                "日期": working.get("trade_date", pd.Series(trade_date, index=working.index)).map(self._normalize_date_text),
            }
        )
        if normalized["今日超大单净流入-净额"].isna().all():
            normalized["今日超大单净流入-净额"] = pd.to_numeric(
                working.get("elg_net_amount", working.get("super_net_amount")),
                errors="coerce",
            ) * 10_000.0
        if normalized["今日大单净流入-净额"].isna().all():
            normalized["今日大单净流入-净额"] = pd.to_numeric(
                working.get("lg_net_amount", working.get("big_net_amount")),
                errors="coerce",
            ) * 10_000.0
        return normalized.dropna(subset=["名称"]).reset_index(drop=True)

    def _ts_northbound_industry(self, reference_date: datetime) -> Dict[str, Any]:
        cache_key = "market_drivers:ts_northbound_industry:v1"
        cached = self._load_cache(cache_key, ttl_hours=8)
        if cached is not None and not cached.empty:
            latest_date = self._extract_latest_date(cached, "报告时间")
            return {
                "frame": cached.reset_index(drop=True),
                "latest_date": latest_date,
                "is_fresh": self._is_fresh(latest_date, reference_date),
                "symbol": "北向资金增持行业板块排行",
            }

        trade_dates = self._recent_open_trade_dates(lookback_days=14)
        if len(trade_dates) < 2:
            return self._empty_rank_report()

        latest = trade_dates[-1]
        previous = trade_dates[-2]
        try:
            current = self._ts_call("hk_hold", trade_date=latest)
        except Exception:
            current = None
        try:
            previous_frame = self._ts_call("hk_hold", trade_date=previous)
        except Exception:
            previous_frame = None
        if current is None or current.empty or previous_frame is None or previous_frame.empty:
            return self._empty_rank_report()

        current_working = self._normalize_hk_hold_frame(current)
        previous_working = self._normalize_hk_hold_frame(previous_frame)
        if current_working.empty or previous_working.empty:
            return self._empty_rank_report()

        merged = current_working.merge(
            previous_working[["ts_code", "持股量"]].rename(columns={"持股量": "前日持股量"}),
            on="ts_code",
            how="left",
        )
        merged["持股变动"] = merged["持股量"] - merged["前日持股量"].fillna(0.0)

        try:
            daily = self._ts_call("daily", trade_date=latest)
        except Exception:
            daily = None
        try:
            stock_basic = self._ts_call("stock_basic", exchange="", list_status="L", fields="ts_code,industry")
        except Exception:
            stock_basic = None
        if daily is None or daily.empty or stock_basic is None or stock_basic.empty:
            return self._empty_rank_report()

        daily_view = daily[["ts_code", "close"]].copy()
        daily_view["close"] = pd.to_numeric(daily_view["close"], errors="coerce")
        industry_view = stock_basic[["ts_code", "industry"]].copy()
        industry_view["industry"] = industry_view["industry"].astype(str)
        merged = merged.merge(daily_view, on="ts_code", how="left").merge(industry_view, on="ts_code", how="left")
        merged["北向资金今日增持估计-市值"] = pd.to_numeric(merged["持股变动"], errors="coerce") * pd.to_numeric(
            merged["close"], errors="coerce"
        )
        ranked = (
            merged.dropna(subset=["industry", "北向资金今日增持估计-市值"])
            .groupby("industry", dropna=False)["北向资金今日增持估计-市值"]
            .sum()
            .reset_index()
            .rename(columns={"industry": "名称"})
            .sort_values("北向资金今日增持估计-市值", ascending=False)
            .reset_index(drop=True)
        )
        if ranked.empty:
            return self._empty_rank_report()
        ranked["报告时间"] = self._normalize_date_text(latest)
        self._save_cache(cache_key, ranked)
        latest_date = self._extract_latest_date(ranked, "报告时间")
        return {
            "frame": ranked,
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, reference_date),
            "symbol": "北向资金增持行业板块排行",
        }

    def _normalize_hk_hold_frame(self, frame: pd.DataFrame | None) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        vol_col = self._first_existing_column(working, ("vol", "持股数量", "持股量"))
        if ts_code_col is None or vol_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str),
                "持股量": pd.to_numeric(working[vol_col], errors="coerce"),
            }
        )
        return normalized.dropna(subset=["ts_code", "持股量"]).reset_index(drop=True)

    def _ts_hot_rank(self) -> pd.DataFrame:
        cache_key = "market_drivers:ts_hot_rank:v1"
        cached = self._load_cache(cache_key, ttl_hours=1)
        if cached is not None:
            return cached

        trade_date = self._latest_open_trade_date(lookback_days=14)
        if not trade_date:
            return pd.DataFrame()

        raw = None
        for kwargs in (
            {"trade_date": trade_date, "market": "A"},
            {"trade_date": trade_date},
            {},
        ):
            try:
                raw = self._ts_call("ths_hot", **kwargs)
            except Exception:
                raw = None
            if raw is not None and not raw.empty:
                break
        if raw is None or raw.empty:
            return pd.DataFrame()

        working = raw.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        rank_col = self._first_existing_column(working, ("rank", "排名", "当前排名"))
        hot_col = self._first_existing_column(working, ("hot", "热度"))
        if ts_code_col is None or rank_col is None:
            return pd.DataFrame()

        try:
            stock_basic = self._ts_call("stock_basic", exchange="", list_status="L", fields="ts_code,name")
        except Exception:
            stock_basic = None
        try:
            daily = self._ts_call("daily", trade_date=trade_date)
        except Exception:
            daily = None
        if stock_basic is None or stock_basic.empty or daily is None or daily.empty:
            return pd.DataFrame()
        basics = stock_basic[["ts_code", "name"]].rename(columns={"name": "股票名称"})
        daily_view = daily[["ts_code", "pct_chg"]].rename(columns={"pct_chg": "涨跌幅"})

        selected_columns = [ts_code_col, rank_col] + ([hot_col] if hot_col else [])
        normalized = working[selected_columns].copy()
        rename_map = {ts_code_col: "ts_code", rank_col: "排名"}
        if hot_col:
            rename_map[hot_col] = "热度"
        normalized = normalized.rename(columns=rename_map)
        normalized["排名"] = pd.to_numeric(normalized["排名"], errors="coerce")
        normalized = normalized.merge(basics, on="ts_code", how="left").merge(daily_view, on="ts_code", how="left")
        normalized["名称"] = normalized["股票名称"]
        normalized["代码"] = normalized["ts_code"].astype(str).map(self._from_ts_code)
        normalized = normalized.dropna(subset=["排名"]).sort_values("排名").reset_index(drop=True)
        self._save_cache(cache_key, normalized)
        return normalized

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
                prefer_stale=True,
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
                prefer_stale=True,
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
                prefer_stale=True,
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
