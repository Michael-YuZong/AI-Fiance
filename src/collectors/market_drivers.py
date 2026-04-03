"""Broad market drivers for rotation and capital-flow sections — Tushare-first."""

from __future__ import annotations

import io
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from typing import Any, Dict, Mapping, Optional, Sequence

import pandas as pd

from .base import BaseCollector
from .industry_index import IndustryIndexCollector
from .market_cn import ChinaMarketCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class MarketDriversCollector(BaseCollector):
    """Collect market-wide sector rotation and capital-flow inputs.

    沪深港通资金流向/十大成交股/融资融券/板块涨幅/热门度优先用 Tushare。
    仅在 Tushare 未覆盖到的北向概念/少数补充维度上保留 AKShare fallback。
    """

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="MarketDriversCollector")

    def collect(self, reference_date: Optional[datetime] = None) -> Dict[str, Any]:
        as_of = reference_date or datetime.now()
        cn_market = ChinaMarketCollector(self.config)
        as_of_text = as_of.strftime("%Y-%m-%d %H:%M:%S")

        result: Dict[str, Any] = {
            "as_of": as_of_text,
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

        # Tushare 已覆盖行业/概念/热榜主链；AKShare 这里只保留给未覆盖的北向概念补充维度。
        result["northbound_concept"] = self._empty_rank_report()
        if ak is not None:
            fallback_concept = self._northbound_rank("北向资金增持概念板块排行", as_of)
            if not fallback_concept["frame"].empty:
                result["northbound_concept"] = fallback_concept

        industry_framework = IndustryIndexCollector(self.config).collect_market_snapshot(as_of)
        sw_industry_report = dict(industry_framework.get("sw_industry_report") or {})
        ci_industry_report = dict(industry_framework.get("ci_industry_report") or {})
        sw_industry_spot = sw_industry_report.get("frame", pd.DataFrame())
        ci_industry_spot = ci_industry_report.get("frame", pd.DataFrame())
        result["sw_industry_spot"] = sw_industry_spot if isinstance(sw_industry_spot, pd.DataFrame) else pd.DataFrame()
        result["sw_industry_report"] = sw_industry_report
        result["ci_industry_spot"] = ci_industry_spot if isinstance(ci_industry_spot, pd.DataFrame) else pd.DataFrame()
        result["ci_industry_report"] = ci_industry_report

        industry_spot = result["sw_industry_spot"]
        industry_spot_report = sw_industry_report
        if industry_spot.empty and not result["ci_industry_spot"].empty:
            industry_spot = result["ci_industry_spot"]
            industry_spot_report = ci_industry_report
        if industry_spot.empty:
            industry_spot = self._ts_board_spot("industry")
            industry_spot_report = {
                **self._volatile_frame_report(
                    industry_spot,
                    as_of,
                    default_date=self._latest_open_trade_date(lookback_days=14),
                    source="tushare.ths_daily+tushare.ths_index",
                ),
                "fallback": "ths_board_spot",
                "disclosure": (
                    "申万/中信行业指数当前不可用，临时回退到同花顺行业盘面；"
                    "AKShare 行业板块旧主路径已退场，不再作为默认回退。"
                ),
            }
        result["industry_spot"] = industry_spot
        result["industry_spot_report"] = industry_spot_report

        concept_spot = self._ts_board_spot("concept")
        result["concept_spot"] = concept_spot
        concept_spot_report = self._volatile_frame_report(
            concept_spot,
            as_of,
            default_date=self._latest_open_trade_date(lookback_days=14),
            source="concept_spot",
        )
        if concept_spot.empty:
            concept_spot_report = {
                **concept_spot_report,
                "fallback": "none",
                "disclosure": (
                    "同花顺概念盘面当前不可用；AKShare 概念板块旧主路径已退场，不再作为默认回退。"
                ),
            }
        else:
            concept_spot_report["fallback"] = "none"
        result["concept_spot_report"] = concept_spot_report

        hot_rank = self._ts_hot_rank()
        result["hot_rank"] = hot_rank
        hot_rank_report = self._volatile_frame_report(
            hot_rank,
            as_of,
            default_date=self._latest_open_trade_date(lookback_days=14),
            source="hot_rank",
        )
        if hot_rank.empty:
            hot_rank_report = {
                **hot_rank_report,
                "fallback": "none",
                "disclosure": (
                    "同花顺热榜当前不可用；AKShare 热榜旧主路径已退场，不再作为默认回退。"
                ),
            }
        else:
            hot_rank_report["fallback"] = "none"
        result["hot_rank_report"] = hot_rank_report

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
        board_rows = self._ts_board_index_rows(board_type)
        if board_rows.empty:
            return pd.DataFrame()

        trade_date = self._latest_open_trade_date(lookback_days=14)
        if not trade_date:
            return pd.DataFrame()
        use_intraday_cache = trade_date != datetime.now().strftime("%Y%m%d")
        if use_intraday_cache:
            cached = self._load_cache(cache_key, ttl_hours=2)
            if cached is not None:
                return cached

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
        if use_intraday_cache:
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

    def _ts_ths_member_by_stock(self, ts_code: str) -> pd.DataFrame | None:
        ts_code = str(ts_code).strip()
        if not ts_code:
            return None
        cache_key = f"market_drivers:ths_member:con_code:{ts_code}:v1"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("ths_member", con_code=ts_code)
        if raw is None:
            return None
        if raw.empty:
            empty = pd.DataFrame(columns=["ts_code", "con_code", "con_name", "weight", "in_date", "out_date", "is_new"])
            self._save_cache(cache_key, empty)
            return empty
        self._save_cache(cache_key, raw)
        return raw

    def _normalize_ths_member_frame(self, frame: pd.DataFrame | None) -> pd.DataFrame:
        if frame is None:
            return pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=["ts_code", "con_code", "con_name", "weight", "in_date", "out_date", "is_new"])
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        con_code_col = self._first_existing_column(working, ("con_code",))
        name_col = self._first_existing_column(working, ("con_name", "name"))
        if ts_code_col is None or con_code_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str),
                "con_code": working[con_code_col].astype(str),
                "con_name": working.get(name_col, pd.Series("", index=working.index)).astype(str),
                "weight": pd.to_numeric(working.get("weight", pd.Series(pd.NA, index=working.index)), errors="coerce"),
                "in_date": working.get("in_date", pd.Series("", index=working.index)).map(self._normalize_date_text),
                "out_date": working.get("out_date", pd.Series("", index=working.index)).map(self._normalize_date_text),
                "is_new": working.get("is_new", pd.Series("", index=working.index)).astype(str),
            }
        )
        normalized = normalized.drop_duplicates(["ts_code", "con_code"], keep="first").reset_index(drop=True)
        return normalized

    def _ts_board_daily_snapshot(self, trade_date: str) -> pd.DataFrame:
        trade_date = str(trade_date).replace("-", "").strip()
        if not trade_date:
            return pd.DataFrame()
        cache_key = f"market_drivers:ths_daily:board_snapshot:{trade_date}:v1"
        cached = self._load_cache(cache_key, ttl_hours=2)
        if cached is not None:
            return cached
        raw = self._ts_call("ths_daily", trade_date=trade_date)
        if raw is None or raw.empty:
            return pd.DataFrame()
        working = raw.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        if ts_code_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str),
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
        normalized = normalized.drop_duplicates("ts_code", keep="last").reset_index(drop=True)
        self._save_cache(cache_key, normalized)
        return normalized

    def get_stock_theme_membership(
        self,
        symbol: str,
        *,
        reference_date: Optional[datetime] = None,
        limit: int = 5,
    ) -> Dict[str, Any]:
        as_of = reference_date or datetime.now()
        as_of_text = as_of.strftime("%Y-%m-%d %H:%M:%S")
        trade_date = self._latest_open_trade_date(lookback_days=14)
        latest_date = self._normalize_date_text(trade_date)
        ts_code = self._to_ts_code(symbol)
        diagnosis = "live"
        member_error: BaseException | None = None
        try:
            member_raw = self._ts_ths_member_by_stock(ts_code)
        except Exception as exc:  # noqa: BLE001
            member_raw = None
            member_error = exc
        member_frame = self._normalize_ths_member_frame(member_raw)
        if member_error is not None:
            diagnosis = self._tushare_failure_diagnosis(member_error)
        elif member_raw is None:
            diagnosis = "unavailable"
        elif member_frame.empty:
            diagnosis = "empty"

        if member_frame.empty:
            disclosure = (
                "Tushare ths_member 当前未命中该股票的主题/行业成分。"
                if diagnosis == "empty"
                else self._blocked_disclosure(diagnosis, source="Tushare ths_member")
            )
            return {
                "symbol": str(symbol).strip(),
                "ts_code": ts_code,
                "as_of": as_of_text,
                "latest_date": latest_date,
                "is_fresh": False,
                "source": "tushare.ths_member+tushare.ths_index+tushare.ths_daily",
                "fallback": "none",
                "diagnosis": diagnosis,
                "disclosure": disclosure,
                "status": "empty" if diagnosis == "empty" else "blocked",
                "items": [],
            }

        board_frames = [self._ts_board_index_rows("concept"), self._ts_board_index_rows("industry")]
        if any(not frame.empty for frame in board_frames):
            board_rows = pd.concat(board_frames, ignore_index=True)
        else:
            board_rows = pd.DataFrame(columns=["ts_code", "名称", "板块类型"])
        if "ts_code" not in board_rows.columns:
            board_rows = pd.DataFrame(columns=["ts_code", "名称", "板块类型"])
        else:
            board_rows = board_rows.drop_duplicates("ts_code", keep="first")
        board_daily = self._ts_board_daily_snapshot(trade_date)
        if "ts_code" not in board_daily.columns:
            board_daily = pd.DataFrame(columns=["ts_code", "涨跌幅", "成交额", "日期"])
        working = member_frame.merge(board_rows, on="ts_code", how="left").merge(board_daily, on="ts_code", how="left")
        working["板块名称"] = working.get("名称", pd.Series("", index=working.index))
        working["板块名称"] = working["板块名称"].where(~working["板块名称"].isna(), "")
        working["板块名称"] = working["板块名称"].astype(str).str.strip()
        working.loc[working["板块名称"].str.lower().isin({"nan", "none"}), "板块名称"] = ""
        working["板块类型"] = working.get("板块类型", pd.Series("", index=working.index))
        working["板块类型"] = working["板块类型"].where(~working["板块类型"].isna(), "")
        working["板块类型"] = working["板块类型"].astype(str).str.strip().replace({"concept": "concept", "industry": "industry", "": "unknown"})
        working["日期"] = working.get("日期", pd.Series(latest_date, index=working.index)).astype(str)
        working["涨跌幅"] = pd.to_numeric(working.get("涨跌幅", pd.Series(pd.NA, index=working.index)), errors="coerce")
        working["成交额"] = pd.to_numeric(working.get("成交额", pd.Series(pd.NA, index=working.index)), errors="coerce")
        working = working.sort_values(
            by=["涨跌幅", "成交额", "板块名称"],
            ascending=[False, False, True],
            na_position="last",
        ).reset_index(drop=True)

        items: list[dict[str, Any]] = []
        for _, row in working.head(max(int(limit), 1) * 2).iterrows():
            board_name = str(row.get("板块名称", "")).strip()
            if board_name.lower() in {"nan", "none"}:
                board_name = ""
            if not board_name:
                continue
            move = row.get("涨跌幅")
            if pd.notna(move):
                move_float = float(move)
                strength = "高" if move_float >= 3 else "中" if move_float >= 1 else "低"
            else:
                move_float = None
                strength = "中"
            items.append(
                {
                    "board_code": str(row.get("ts_code", "")).strip(),
                    "board_name": board_name,
                    "board_type": str(row.get("板块类型", "unknown")).strip() or "unknown",
                    "board_type_label": "概念" if str(row.get("板块类型", "")).strip() == "concept" else "行业" if str(row.get("板块类型", "")).strip() == "industry" else "题材",
                    "pct_change": move_float,
                    "amount": None if pd.isna(row.get("成交额")) else float(row.get("成交额")),
                    "trade_date": str(row.get("日期", "")).strip() or latest_date,
                    "is_new": str(row.get("is_new", "")).strip(),
                    "signal_strength": strength,
                    "source": "Tushare ths_member / ths_daily / ths_index",
                }
            )
            if len(items) >= max(int(limit), 1):
                break

        last_signal_date = next((str(item.get("trade_date", "")).strip() for item in items if str(item.get("trade_date", "")).strip()), latest_date)
        return {
            "symbol": str(symbol).strip(),
            "ts_code": ts_code,
            "as_of": as_of_text,
            "latest_date": last_signal_date,
            "is_fresh": self._is_fresh(last_signal_date, as_of, max_age_days=0) if last_signal_date else False,
            "source": "tushare.ths_member+tushare.ths_index+tushare.ths_daily",
            "fallback": "none",
            "diagnosis": diagnosis,
            "disclosure": "Tushare ths_member 按股票反查同花顺行业/概念成分，并结合 ths_daily 判断当日主题强弱。",
            "status": "matched" if items else "empty",
            "items": items,
        }

    def get_stock_capital_flow_snapshot(
        self,
        symbol: str,
        *,
        reference_date: Optional[datetime] = None,
        display_name: str = "",
        sector: str = "",
        chain_nodes: Sequence[str] = (),
    ) -> Dict[str, Any]:
        """Unified direct/proxy capital-flow snapshot for an A-share stock."""
        as_of = reference_date or datetime.now()
        as_of_text = as_of.strftime("%Y-%m-%d %H:%M:%S")
        latest_trade_date = self._latest_open_trade_date(lookback_days=14) or as_of.strftime("%Y%m%d")
        latest_trade_text = self._normalize_date_text(latest_trade_date) or as_of.strftime("%Y-%m-%d")
        ts_code = self._to_ts_code(symbol)
        window_start = (pd.Timestamp(latest_trade_text) - timedelta(days=10)).strftime("%Y%m%d")
        blocked_diagnoses = {"unavailable", "permission_blocked", "rate_limited", "network_error", "fetch_error"}

        def _fmt_amount(value: Any) -> str:
            numeric = pd.to_numeric(pd.Series([value]), errors="coerce").dropna()
            if numeric.empty:
                return "缺失"
            return f"{float(numeric.iloc[0]) / 100_000_000.0:.2f}亿"

        direct_error: BaseException | None = None
        try:
            direct_raw = self._ts_stock_moneyflow_snapshot(
                ts_code=ts_code,
                start_date=window_start,
                end_date=latest_trade_date,
            )
        except Exception as exc:  # noqa: BLE001
            direct_raw = None
            direct_error = exc
        direct_frame = self._normalize_stock_moneyflow_frame(direct_raw)
        direct_frame = direct_frame[direct_frame["ts_code"] == ts_code].reset_index(drop=True) if not direct_frame.empty else pd.DataFrame()
        direct_diagnosis = "live"
        if direct_error is not None:
            direct_diagnosis = self._tushare_failure_diagnosis(direct_error)
        elif direct_raw is None:
            direct_diagnosis = "unavailable"
        elif direct_frame.empty:
            direct_diagnosis = "empty"
        direct_latest_date = ""
        direct_row: Dict[str, Any] = {}
        direct_5d_main_flow = None
        direct_is_fresh = False
        if not direct_frame.empty:
            direct_frame = direct_frame.sort_values("日期").reset_index(drop=True)
            direct_latest_date = str(direct_frame.iloc[-1].get("日期", "")).strip()
            direct_is_fresh = bool(direct_latest_date and direct_latest_date == latest_trade_text)
            if not direct_is_fresh and direct_diagnosis == "live":
                direct_diagnosis = "stale"
            direct_row = direct_frame.iloc[-1].to_dict()
            main_flow_series = pd.to_numeric(direct_frame.get("主力净流入-净额", pd.Series(dtype=float)), errors="coerce").dropna()
            if not main_flow_series.empty:
                direct_5d_main_flow = float(main_flow_series.tail(5).sum())

        needs_proxy_context = not (direct_row and direct_is_fresh)
        board_row: Dict[str, Any] = {}
        board_type = ""
        board_source = ""
        board_name = ""
        board_latest_date = ""
        board_is_fresh = False
        theme_report: Dict[str, Any] = {
            "source": "tushare.ths_member",
            "latest_date": latest_trade_text,
            "fallback": "skipped_direct_moneyflow",
            "diagnosis": "skipped",
            "disclosure": "个股 moneyflow 当期已命中，本轮未再额外拉取主题成员代理。",
            "status": "skipped",
            "items": [],
        }
        theme_items: list[dict[str, Any]] = []

        def _match_board_row(frame: pd.DataFrame, names: Sequence[str]) -> Dict[str, Any]:
            if frame.empty:
                return {}
            name_col = self._first_existing_column(frame, ("名称", "行业", "板块名称", "概念名称"))
            if name_col is None:
                return {}
            cleaned_names = [str(item).strip() for item in names if str(item).strip()]
            for candidate in cleaned_names:
                exact = frame[frame[name_col].astype(str).str.strip() == candidate]
                if not exact.empty:
                    return dict(exact.iloc[0])
            for candidate in cleaned_names:
                if len(candidate) < 4:
                    continue
                for _, row in frame.iterrows():
                    row_name = str(row.get(name_col, "")).strip()
                    if not row_name:
                        continue
                    if candidate in row_name or row_name in candidate:
                        return dict(row)
            return {}

        if needs_proxy_context:
            theme_report = self.get_stock_theme_membership(symbol, reference_date=as_of, limit=3)
            theme_items = list(theme_report.get("items") or [])
            industry_frame = self._ts_sector_fund_flow("industry")
            concept_frame = self._ts_sector_fund_flow("concept")

            concept_candidates = [
                str(item.get("board_name", "")).strip()
                for item in theme_items
                if str(item.get("board_type", "")).strip() == "concept"
            ]
            industry_candidates = [
                str(item.get("board_name", "")).strip()
                for item in theme_items
                if str(item.get("board_type", "")).strip() == "industry"
            ]
            if sector:
                industry_candidates.append(str(sector).strip())
            industry_candidates.extend(str(item).strip() for item in chain_nodes if str(item).strip())
            concept_candidates.extend(str(item).strip() for item in chain_nodes if str(item).strip())

            board_row = _match_board_row(concept_frame, concept_candidates)
            board_type = "concept"
            board_source = "tushare.moneyflow_cnt_ths"
            if not board_row:
                board_row = _match_board_row(industry_frame, industry_candidates)
                board_type = "industry"
                board_source = "tushare.moneyflow_ind_ths"
            board_name = str(board_row.get("名称", board_row.get("行业", ""))).strip() if board_row else ""
            board_latest_date = str(board_row.get("日期", "")).strip() if board_row else ""
            board_is_fresh = bool(board_latest_date and board_latest_date == latest_trade_text)
        board_component = {
            "source": board_source or "tushare.moneyflow_ind_ths+tushare.moneyflow_cnt_ths",
            "as_of": board_latest_date or latest_trade_text,
            "fallback": "theme_membership" if theme_items else ("metadata" if needs_proxy_context else "skipped_direct_moneyflow"),
            "diagnosis": (
                "live"
                if board_is_fresh
                else "stale"
                if board_row
                else "empty"
                if needs_proxy_context
                else "skipped"
            ),
            "disclosure": (
                f"{'概念' if board_type == 'concept' else '行业'}资金流当前命中 `{board_name}`。"
                if board_row
                else "行业/概念资金流当前未命中该股票的直接主题映射。"
                if needs_proxy_context
                else "个股 moneyflow 当期已命中，本轮未再额外拉取行业/概念资金流代理。"
            ),
            "status": "matched" if board_row else "empty" if needs_proxy_context else "skipped",
            "detail": (
                f"{board_name} 主力净{'流入' if float(board_row.get('今日主力净流入-净额') or 0) >= 0 else '流出'} "
                f"{_fmt_amount(board_row.get('今日主力净流入-净额'))}"
                if board_row
                else "当前未命中相关行业/概念资金流。"
                if needs_proxy_context
                else "个股资金流已直接命中，当期判断优先使用个股主力净流入。"
            ),
            "item": dict(board_row) if board_row else {},
        }

        components = {
            "stock_moneyflow": {
                "source": "tushare.moneyflow",
                "as_of": direct_latest_date or latest_trade_text,
                "fallback": "none",
                "diagnosis": direct_diagnosis,
                "disclosure": (
                    "Tushare moneyflow 提供个股大小单/主力净流入。"
                    if direct_diagnosis in {"live", "empty", "stale"}
                    else self._blocked_disclosure(direct_diagnosis, source="Tushare moneyflow")
                ),
                "status": "matched" if direct_row else "empty" if direct_diagnosis == "empty" else "blocked",
                "detail": (
                    f"{direct_latest_date} 主力净{'流入' if float(direct_row.get('主力净流入-净额') or 0) >= 0 else '流出'} "
                    f"{_fmt_amount(direct_row.get('主力净流入-净额'))}"
                    if direct_row
                    else "当前未命中可用个股主力资金流。"
                ),
                "item": direct_row,
            },
            "board_flow": board_component,
            "theme_membership": {
                "source": str(theme_report.get("source", "tushare.ths_member")).strip() or "tushare.ths_member",
                "as_of": str(theme_report.get("latest_date", latest_trade_text)).strip() or latest_trade_text,
                "fallback": str(theme_report.get("fallback", "none")).strip() or "none",
                "diagnosis": str(theme_report.get("diagnosis", "empty")).strip() or "empty",
                "disclosure": str(theme_report.get("disclosure", "")).strip() or "同花顺主题成员当前未命中。",
                "status": str(theme_report.get("status", "empty")).strip() or "empty",
                "detail": (
                    f"已命中 {len(theme_items)} 个同花顺行业/概念成员。"
                    if theme_items
                    else "当前未命中同花顺主题成员。"
                ),
                "items": theme_items[:3],
            },
        }

        disclosure_lines = [
            str(component.get("disclosure", "")).strip()
            for component in components.values()
            if str(component.get("disclosure", "")).strip()
        ]
        direct_main_flow = direct_row.get("主力净流入-净额") if direct_row else None
        board_main_flow = board_row.get("今日主力净流入-净额") if board_row else None
        direct_main_ratio = direct_row.get("主力净流入-净占比") if direct_row else None
        board_main_ratio = board_row.get("今日主力净流入-净占比") if board_row else None

        if direct_row and direct_is_fresh:
            status = "matched"
            diagnosis = "live"
            fallback = "none"
            detail = (
                f"个股主力资金当期已命中：{latest_trade_text} 主力净{'流入' if float(direct_main_flow or 0) >= 0 else '流出'} "
                f"{_fmt_amount(direct_main_flow)}；近 5 日累计 {_fmt_amount(direct_5d_main_flow)}。"
            )
            is_fresh = True
        elif board_row and board_is_fresh:
            status = "proxy"
            diagnosis = "proxy"
            fallback = "sector_or_concept_flow"
            detail = (
                f"个股主力资金当前未命中 fresh，先看{'概念' if board_type == 'concept' else '行业'}代理："
                f"`{board_name}` 主力净{'流入' if float(board_main_flow or 0) >= 0 else '流出'} {_fmt_amount(board_main_flow)}。"
            )
            is_fresh = True
        elif direct_row:
            status = "stale"
            diagnosis = direct_diagnosis
            fallback = "none"
            detail = f"个股资金流最新停在 {direct_latest_date or '未知'}，当前不按 fresh 命中处理。"
            is_fresh = False
        elif {str(component.get('diagnosis', '')) for component in components.values()}.issubset(blocked_diagnoses):
            status = "blocked"
            diagnosis = "blocked"
            fallback = "none"
            detail = "股票资金流专题当前不可用，本轮不把缺口写成主力已经明确承接。"
            is_fresh = False
        elif board_row:
            status = "proxy"
            diagnosis = "stale"
            fallback = "sector_or_concept_flow"
            detail = f"相关行业/概念资金流最新停在 {board_latest_date or '未知'}，当前先按旧代理披露，不写成当期主力承接。"
            is_fresh = False
        else:
            status = "empty"
            diagnosis = "empty"
            fallback = "none"
            detail = "当前未命中可稳定使用的个股或行业/概念资金流。"
            is_fresh = False

        return {
            "symbol": str(symbol).strip(),
            "ts_code": ts_code,
            "source": "tushare.moneyflow+tushare.ths_member+tushare.moneyflow_ind_ths+tushare.moneyflow_cnt_ths",
            "as_of": as_of_text,
            "latest_date": direct_latest_date or board_latest_date or latest_trade_text,
            "fallback": fallback,
            "diagnosis": diagnosis,
            "status": status,
            "is_fresh": is_fresh,
            "disclosure": "；".join(dict.fromkeys(disclosure_lines)),
            "detail": detail,
            "direct_main_flow": None if direct_main_flow is None or pd.isna(direct_main_flow) else float(direct_main_flow),
            "direct_main_ratio": None if direct_main_ratio is None or pd.isna(direct_main_ratio) else float(direct_main_ratio),
            "direct_5d_main_flow": direct_5d_main_flow,
            "board_name": board_name,
            "board_type": board_type if board_row else "",
            "board_main_flow": None if board_main_flow is None or pd.isna(board_main_flow) else float(board_main_flow),
            "board_main_ratio": None if board_main_ratio is None or pd.isna(board_main_ratio) else float(board_main_ratio),
            "components": components,
        }

    def get_stock_broker_recommend_snapshot(
        self,
        symbol: str,
        *,
        reference_date: Optional[datetime] = None,
        display_name: str = "",
    ) -> Dict[str, Any]:
        """Unified sell-side monthly recommendation snapshot for an A-share stock."""
        as_of = reference_date or datetime.now()
        as_of_text = as_of.strftime("%Y-%m-%d %H:%M:%S")
        current_month = as_of.strftime("%Y%m")
        current_month_label = self._normalize_month_text(current_month)
        ts_code = self._to_ts_code(symbol)
        stock_name = str(display_name).strip()

        current_report = self._broker_recommend_month_report(ts_code, current_month)
        current_diagnosis = str(current_report["diagnosis"]).strip() or "unavailable"
        if current_diagnosis not in {"live", "empty", "unavailable"}:
            diagnosis = str(current_report["diagnosis"]).strip() or "unavailable"
            return {
                "symbol": str(symbol).strip(),
                "display_name": stock_name,
                "ts_code": ts_code,
                "source": "tushare.broker_recommend",
                "as_of": as_of_text,
                "latest_date": current_month_label,
                "fallback": "none",
                "diagnosis": diagnosis,
                "status": "blocked",
                "is_fresh": False,
                "disclosure": self._blocked_disclosure(diagnosis, source="Tushare broker_recommend"),
                "detail": "卖方月度金股专题当前不可用，本轮不把缺口误写成零覆盖或低拥挤。",
                "latest_month": current_month,
                "latest_broker_count": 0,
                "previous_month": "",
                "previous_broker_count": 0,
                "broker_delta": None,
                "consecutive_months": 0,
                "coverage_level": "",
                "crowding_level": "",
                "brokers": [],
                "months": [current_report],
            }

        month_reports = [current_report]
        history_reports: list[dict[str, Any]] = []
        month_cursor = pd.Timestamp(as_of.year, as_of.month, 1)
        for offset in range(1, 4):
            month_key = (month_cursor - pd.DateOffset(months=offset)).strftime("%Y%m")
            report = self._broker_recommend_month_report(ts_code, month_key)
            month_reports.append(report)
            if report["diagnosis"] == "live" and report["broker_count"] > 0:
                history_reports.append(report)

        current_count = int(current_report["broker_count"] or 0)
        latest_report = current_report if current_count > 0 else (history_reports[0] if history_reports else {})
        latest_month = str(latest_report.get("month", "")).strip()
        latest_month_label = self._normalize_month_text(latest_month)
        latest_count = int(latest_report.get("broker_count") or 0)
        latest_index = next((idx for idx, report in enumerate(month_reports) if str(report.get("month", "")).strip() == latest_month), -1)
        previous_report = month_reports[latest_index + 1] if 0 <= latest_index < len(month_reports) - 1 else {}
        previous_month = str(previous_report.get("month", "")).strip()
        previous_diagnosis = str(previous_report.get("diagnosis", "")).strip()
        previous_count = int(previous_report.get("broker_count") or 0) if previous_diagnosis in {"live", "empty"} else 0
        broker_delta = latest_count - previous_count if previous_month and previous_diagnosis in {"live", "empty"} else None
        consecutive_months = 0
        if latest_month and latest_index >= 0:
            for report in month_reports[latest_index:]:
                if int(report.get("broker_count") or 0) <= 0:
                    break
                consecutive_months += 1

        coverage_level = (
            "high" if latest_count >= 5
            else "medium" if latest_count >= 3
            else "low" if latest_count >= 1
            else ""
        )
        crowding_level = (
            "high" if latest_count >= 6 or consecutive_months >= 4
            else "medium" if latest_count >= 4 or consecutive_months >= 3
            else "low" if latest_count >= 2
            else ""
        )
        is_fresh = bool(latest_month and latest_month == current_month and latest_count > 0)

        if latest_count > 0:
            broker_bits = list(latest_report.get("brokers") or [])
            broker_brief = " / ".join(broker_bits[:4])
            if is_fresh:
                detail = f"{latest_month_label} 命中 {latest_count} 家券商月度金股推荐"
                if broker_delta is not None:
                    if broker_delta > 0:
                        detail += f"，较上月增加 {broker_delta} 家"
                    elif broker_delta < 0:
                        detail += f"，较上月减少 {abs(broker_delta)} 家"
                    else:
                        detail += "，与上月持平"
                if consecutive_months >= 2:
                    detail += f"，已连续 {consecutive_months} 个月进入月度金股名单"
                if broker_brief:
                    detail += f"；当前券商包括 {broker_brief}"
                status = "matched"
                diagnosis = "live"
                fallback = "none"
                disclosure = "Tushare broker_recommend 提供券商月度金股/推荐名单；这里只把它当卖方共识热度与拥挤度参考，不替代公司公告或订单证据。"
            else:
                detail = f"卖方月度金股最新停在 {latest_month_label}，当前不按本月 fresh 共识处理；最近一次命中 {latest_count} 家券商推荐。"
                if broker_brief:
                    detail += f" 当前可见券商包括 {broker_brief}。"
                status = "stale"
                diagnosis = "stale"
                fallback = "previous_month"
                disclosure = "Tushare broker_recommend 当前只命中历史月份名单；本轮只把它当旧共识参考，不写成本月 fresh 卖方升温。"
        else:
            blocked_history = next(
                (
                    str(report.get("diagnosis", "")).strip()
                    for report in month_reports
                    if str(report.get("diagnosis", "")).strip() not in {"live", "empty"}
                ),
                "",
            )
            if blocked_history:
                status = "blocked"
                diagnosis = blocked_history
                fallback = "none"
                disclosure = self._blocked_disclosure(blocked_history, source="Tushare broker_recommend")
                detail = "卖方月度金股专题当前不可用，本轮不把缺口误写成零覆盖或低拥挤。"
            else:
                status = "empty"
                diagnosis = "empty"
                fallback = "none"
                disclosure = "Tushare broker_recommend 当前未命中这只股票的近月券商金股名单，本轮不把空结果写成卖方已明确看空。"
                detail = "近 4 个月未命中券商月度金股推荐。"

        return {
            "symbol": str(symbol).strip(),
            "display_name": stock_name,
            "ts_code": ts_code,
            "source": "tushare.broker_recommend",
            "as_of": as_of_text,
            "latest_date": latest_month_label or current_month_label,
            "fallback": fallback,
            "diagnosis": diagnosis,
            "status": status,
            "is_fresh": is_fresh,
            "disclosure": disclosure,
            "detail": detail,
            "latest_month": latest_month,
            "latest_broker_count": latest_count,
            "previous_month": previous_month,
            "previous_broker_count": previous_count,
            "broker_delta": broker_delta,
            "consecutive_months": consecutive_months,
            "coverage_level": coverage_level,
            "crowding_level": crowding_level,
            "brokers": list(latest_report.get("brokers") or []),
            "months": month_reports,
        }

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
        trade_date = self._latest_open_trade_date(lookback_days=14)
        if not trade_date:
            return pd.DataFrame()
        use_intraday_cache = trade_date != datetime.now().strftime("%Y%m%d")
        if use_intraday_cache:
            cached = self._load_cache(cache_key, ttl_hours=1)
            if cached is not None:
                return cached

        raw = None
        for kwargs in ({}, {"is_new": "Y"}):
            try:
                raw = self._ts_call("ths_hot", **kwargs)
            except Exception:
                raw = None
            if raw is not None and not raw.empty:
                break
        if raw is None or raw.empty:
            return pd.DataFrame()

        working = raw.copy()
        if "ts_code" not in working.columns:
            return pd.DataFrame()

        # ``ths_hot`` 在当前权限下不稳定支持 ``market=A`` / ``trade_date=...`` 过滤；
        # 实盘更稳定的方式是拉全量热榜，再在本地筛 A 股并截取最新交易日快照。
        code_series = working["ts_code"].astype(str)
        a_share_mask = code_series.str.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", na=False)
        if "data_type" in working.columns:
            data_type = working["data_type"].astype(str)
            hot_like = data_type.str.contains("热股|A股|沪深", case=False, regex=True, na=False)
            a_share_mask = a_share_mask & hot_like
        working = working[a_share_mask].copy()
        if working.empty:
            return pd.DataFrame()

        if "trade_date" in working.columns:
            working["trade_date"] = working["trade_date"].map(self._normalize_date_text)
            valid_dates = working["trade_date"][working["trade_date"] != ""]
            if not valid_dates.empty:
                requested_date = self._normalize_date_text(trade_date)
                all_dates = valid_dates.astype(str).tolist()
                eligible_dates = [date for date in all_dates if date <= requested_date]
                latest_date = max(eligible_dates) if eligible_dates else max(all_dates)
                working = working[working["trade_date"].astype(str) == latest_date].copy()
        if working.empty:
            return pd.DataFrame()

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
        normalized["日期"] = working.get("trade_date", pd.Series(trade_date, index=working.index)).map(self._normalize_date_text)
        if "pct_change" in working.columns:
            normalized["原始涨跌幅"] = pd.to_numeric(working["pct_change"], errors="coerce")
        if "rank_time" in working.columns:
            normalized["rank_time"] = working["rank_time"].astype(str)
        normalized = normalized.merge(basics, on="ts_code", how="left").merge(daily_view, on="ts_code", how="left")
        if "原始涨跌幅" in normalized.columns:
            normalized["涨跌幅"] = pd.to_numeric(normalized["涨跌幅"], errors="coerce").fillna(normalized["原始涨跌幅"])
        normalized["名称"] = normalized["股票名称"]
        normalized["代码"] = normalized["ts_code"].astype(str).map(self._from_ts_code)
        normalized = normalized.dropna(subset=["排名"])
        if "rank_time" in normalized.columns:
            normalized = normalized.sort_values(["代码", "rank_time"], ascending=[True, False]).drop_duplicates("代码", keep="first")
        normalized = normalized.sort_values("排名").reset_index(drop=True)
        for helper_col in ("rank_time", "原始涨跌幅"):
            if helper_col in normalized.columns:
                normalized = normalized.drop(columns=[helper_col])
        if use_intraday_cache:
            self._save_cache(cache_key, normalized)
        return normalized

    def _broker_recommend_month_report(self, ts_code: str, month: str) -> Dict[str, Any]:
        month_text = self._normalize_month_text(month)
        raw_error: BaseException | None = None
        try:
            raw = self._ts_broker_recommend_snapshot(month=month)
        except Exception as exc:  # noqa: BLE001
            raw = None
            raw_error = exc

        diagnosis = "live"
        if raw_error is not None:
            diagnosis = self._tushare_failure_diagnosis(raw_error)
        elif raw is None:
            diagnosis = "unavailable"

        frame = self._normalize_broker_recommend_frame(raw)
        if not frame.empty:
            frame = frame[frame["ts_code"] == ts_code].reset_index(drop=True)
        broker_count = int(frame["broker"].nunique()) if not frame.empty else 0
        if diagnosis == "live" and broker_count <= 0:
            diagnosis = "empty"

        brokers = sorted({str(item).strip() for item in frame.get("broker", pd.Series(dtype=str)).tolist() if str(item).strip()})
        return {
            "month": str(month).strip(),
            "month_label": month_text,
            "source": "tushare.broker_recommend",
            "as_of": month_text,
            "fallback": "none",
            "diagnosis": diagnosis,
            "disclosure": (
                f"{month_text} 当前命中 {broker_count} 家券商月度金股推荐。"
                if broker_count > 0
                else "当前月份未命中该股票的券商月度金股推荐。"
                if diagnosis == "empty"
                else self._blocked_disclosure(diagnosis, source="Tushare broker_recommend")
            ),
            "status": "matched" if broker_count > 0 else "empty" if diagnosis == "empty" else "blocked",
            "broker_count": broker_count,
            "brokers": brokers,
            "frame": frame,
        }

    def _normalize_broker_recommend_frame(self, frame: pd.DataFrame | None) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        working = frame.copy()
        ts_code_col = self._first_existing_column(working, ("ts_code", "code"))
        month_col = self._first_existing_column(working, ("month", "月份"))
        broker_col = self._first_existing_column(working, ("broker", "券商"))
        name_col = self._first_existing_column(working, ("name", "股票名称", "股票简称"))
        if ts_code_col is None or month_col is None or broker_col is None:
            return pd.DataFrame()
        normalized = pd.DataFrame(
            {
                "ts_code": working[ts_code_col].astype(str).str.strip(),
                "month": working[month_col].astype(str).str.replace("-", "", regex=False).str.slice(0, 6),
                "broker": working[broker_col].astype(str).str.strip(),
                "name": working[name_col].astype(str).str.strip() if name_col else "",
            }
        )
        normalized = normalized[
            normalized["ts_code"].astype(str).str.strip().ne("")
            & normalized["broker"].astype(str).str.strip().ne("")
            & normalized["month"].astype(str).str.fullmatch(r"\d{6}", na=False)
        ]
        return normalized.drop_duplicates(["month", "broker", "ts_code"]).reset_index(drop=True)

    @staticmethod
    def _normalize_month_text(value: Any) -> str:
        text = str(value or "").strip().replace("-", "")
        if len(text) == 6 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}"
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return ""
        return parsed.strftime("%Y-%m")

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
                use_cache=False,
                prefer_stale=False,
                allow_stale_on_error=False,
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
                use_cache=False,
                prefer_stale=False,
                allow_stale_on_error=False,
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

    def _volatile_frame_report(
        self,
        frame: pd.DataFrame,
        reference_date: datetime,
        *,
        date_column: str = "日期",
        default_date: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        working = frame.reset_index(drop=True) if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        if working.empty:
            return {
                "frame": working,
                "latest_date": "",
                "is_fresh": False,
                "as_of": reference_date.strftime("%Y-%m-%d %H:%M:%S"),
                "source": source,
                "diagnosis": "empty_or_blocked",
            }
        latest_date = self._extract_latest_date(working, date_column)
        normalized_default = self._normalize_date_text(default_date)
        if not latest_date and normalized_default:
            latest_date = normalized_default
            if not working.empty and date_column not in working.columns:
                working = working.copy()
                working[date_column] = normalized_default
        return {
            "frame": working,
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, reference_date, max_age_days=0),
            "as_of": reference_date.strftime("%Y-%m-%d %H:%M:%S"),
            "source": source,
            "diagnosis": "",
        }

    def _is_fresh(self, date_text: str, reference_date: datetime, max_age_days: int = 7) -> bool:
        if not date_text:
            return False
        normalized = date_text.replace("/", "-")
        try:
            target = datetime.strptime(normalized[:10], "%Y-%m-%d")
        except ValueError:
            return False
        return abs((reference_date.date() - target.date()).days) <= max_age_days
