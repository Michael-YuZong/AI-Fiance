"""Valuation, index metadata, and financial proxy collectors — Tushare-first."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional, Sequence

import pandas as pd

from .base import BaseCollector
from .index_topic import IndexTopicCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

try:
    import baostock as bs
except ImportError:  # pragma: no cover
    bs = None


class ValuationCollector(BaseCollector):
    """Collect ETF scale, index valuation snapshots, and financial proxies.

    Tushare 优先（daily_basic / fina_indicator / index_topic 主链），AKShare 仅保留未覆盖侧路。
    """

    def _require_ak(self):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        return ak

    def _index_topic_collector(self) -> IndexTopicCollector:
        return IndexTopicCollector(self.config)

    # ── ETF NAV ──────────────────────────────────────────────

    def get_cn_etf_nav_history(self, symbol: str, days: int = 120) -> pd.DataFrame:
        """ETF 净值历史。Tushare fund_nav 优先。"""
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("E", "O", "L"))

        # Tushare
        try:
            raw = self._ts_call("fund_nav", ts_code=ts_code)
            if raw is not None and not raw.empty:
                date_col = "nav_date" if "nav_date" in raw.columns else "end_date"
                if date_col not in raw.columns:
                    raise ValueError("fund_nav missing nav_date/end_date")
                raw[date_col] = pd.to_datetime(raw[date_col], format="%Y%m%d", errors="coerce")
                mask = raw[date_col] >= pd.to_datetime(start_date, format="%Y%m%d")
                filtered = raw[mask].sort_values(date_col).reset_index(drop=True)
                if not filtered.empty:
                    return filtered
        except Exception:
            pass
        return pd.DataFrame()

    def get_cn_etf_scale(self, symbol: str) -> Optional[dict]:
        """ETF 份额/规模快照。Tushare etf_share_size 优先。"""
        ts_code = self._resolve_tushare_etf_code(symbol, preferred_markets=("E", "O", "L"))
        recent_open_dates = self._recent_open_trade_dates(lookback_days=21)
        trade_date = ""
        start_date = ""
        end_date = ""
        if recent_open_dates:
            end_date = recent_open_dates[-1]
            start_date = recent_open_dates[max(len(recent_open_dates) - 7, 0)]
        else:
            trade_date = self._latest_open_trade_date()

        raw = self._ts_etf_share_size_snapshot(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if raw is None or getattr(raw, "empty", False):
            return None

        frame = raw.copy()
        if "ts_code" in frame.columns:
            frame = frame[frame["ts_code"].astype(str) == ts_code]
        if frame.empty:
            return None
        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(self._normalize_date_text)
            frame = frame.sort_values("trade_date", ascending=False, na_position="last")
        latest = frame.iloc[0].to_dict()
        for column in ("total_share", "total_size", "nav", "close"):
            if column in latest:
                latest[column] = pd.to_numeric(pd.Series([latest.get(column)]), errors="coerce").iloc[0]
        return latest

    # ── 指数估值快照 ─────────────────────────────────────────

    def get_cn_index_snapshot(self, keywords: Sequence[str]) -> Optional[Dict[str, Any]]:
        """Find a CSI/CNI index valuation snapshot by keyword heuristics."""
        return self._index_topic_collector().get_cn_index_snapshot(keywords)

    def get_cn_index_value_history(self, index_code: str) -> pd.DataFrame:
        """Fetch CSI/CNI index valuation history."""
        return self._index_topic_collector().get_cn_index_value_history(index_code)

    # ── 指数成分权重 ─────────────────────────────────────────

    def get_cn_index_constituent_weights(self, index_code: str, top_n: int = 10) -> pd.DataFrame:
        """Fetch the latest index constituent weights. Tushare index_weight 优先。"""
        return self._index_topic_collector().get_cn_index_constituent_weights(index_code, top_n=top_n)

    # ── 个股财务代理指标 ─────────────────────────────────────

    def get_cn_stock_financial_proxy(self, symbol: str) -> Dict[str, Any]:
        """Fetch the latest single-stock financial proxy metrics.

        Tushare fina_indicator 优先，daily_basic 只补充基础估值字段。
        """
        # ── Tushare daily_basic (补充 PE/PB) ──
        try:
            basic = self._ts_daily_basic_for_stock(symbol)
        except Exception:
            basic = {}

        # ── Tushare fina_indicator (primary) ──
        try:
            result = self._tushare_stock_financial(symbol)
            if result:
                merged = dict(basic)
                merged.update(result)
                return merged
        except Exception:
            pass

        # 如果只有 daily_basic 有数据
        if basic:
            return basic

        raise RuntimeError("No stock financial proxy source available")

    def _tushare_stock_financial(self, symbol: str) -> Dict[str, Any]:
        """Tushare fina_indicator — 直接获取 ROE/毛利率/营收增速等高阶指标。

        2000 积分只能单只循环拉取，不能全市场一次性下载。
        """
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_fina_indicator:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        raw = self._ts_call("fina_indicator", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        # 取最新一期
        if "end_date" in raw.columns:
            raw = raw.sort_values("end_date", ascending=False)
        latest = raw.iloc[0]

        def _safe_float(val: Any) -> Optional[float]:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        result: Dict[str, Any] = {
            "report_date": str(latest.get("end_date", "")),
            "roe": _safe_float(latest.get("roe")),
            "roe_dt": _safe_float(latest.get("roe_dt")),
            "gross_margin": _safe_float(latest.get("grossprofit_margin")),
            "revenue_yoy": _safe_float(latest.get("or_yoy")),  # 营业收入同比
            "profit_yoy": _safe_float(latest.get("netprofit_yoy")),  # 净利同比
            "profit_dedt_yoy": _safe_float(latest.get("dt_netprofit_yoy")),  # 扣非净利同比
            "debt_to_assets": _safe_float(latest.get("debt_to_assets")),
            "current_ratio": _safe_float(latest.get("current_ratio")),
            "eps": _safe_float(latest.get("eps")),
            "bps": _safe_float(latest.get("bps")),
            "cfps": _safe_float(latest.get("cfps")),
            "op_income_yoy": _safe_float(latest.get("op_yoy")),  # 营业利润同比
            "netprofit_margin": _safe_float(latest.get("netprofit_margin")),  # 净利率
        }
        # 去除所有 None 值
        result = {k: v for k, v in result.items() if v is not None}
        if result:
            result["report_date"] = str(latest.get("end_date", ""))
            self._save_cache(cache_key, result)
        return result

    def _ts_daily_basic_for_stock(self, symbol: str) -> Dict[str, Any]:
        """从 Tushare daily_basic 获取个股最新 PE/PB/PS/换手率/市值。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_daily_basic:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached

        raw = self._ts_call("daily_basic", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        latest = raw.sort_values("trade_date", ascending=False).iloc[0]

        def _sf(val: Any) -> Optional[float]:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        result = {}
        pe = _sf(latest.get("pe_ttm"))
        if pe is not None:
            result["pe_ttm"] = pe
        pb = _sf(latest.get("pb"))
        if pb is not None:
            result["pb"] = pb
        ps = _sf(latest.get("ps_ttm"))
        if ps is not None:
            result["ps_ttm"] = ps
        dv = _sf(latest.get("dv_ttm"))
        if dv is not None:
            result["dv_ratio"] = dv
        mv = _sf(latest.get("total_mv"))
        if mv is not None:
            result["total_mv"] = mv * 10_000.0
        turnover = _sf(latest.get("turnover_rate_f"))
        if turnover is not None:
            result["turnover_rate"] = turnover

        if result:
            self._save_cache(cache_key, result)
        return result

    # ── 业绩预告 / 快报 ──────────────────────────────────────

    def get_cn_stock_forecast(self, symbol: str) -> Dict[str, Any]:
        """Tushare forecast — 业绩预告。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_forecast:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("forecast", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        latest = raw.sort_values("ann_date", ascending=False).iloc[0]
        result = {
            "ann_date": str(latest.get("ann_date", "")),
            "end_date": str(latest.get("end_date", "")),
            "type": str(latest.get("type", "")),
            "change_reason": str(latest.get("change_reason", "")),
            "net_profit_min": latest.get("net_profit_min"),
            "net_profit_max": latest.get("net_profit_max"),
        }
        self._save_cache(cache_key, result)
        return result

    def get_cn_stock_express(self, symbol: str) -> Dict[str, Any]:
        """Tushare express — 业绩快报。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_express:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("express", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        latest = raw.sort_values("ann_date", ascending=False).iloc[0]
        result = {
            "ann_date": str(latest.get("ann_date", "")),
            "end_date": str(latest.get("end_date", "")),
            "revenue": latest.get("revenue"),
            "operate_profit": latest.get("operate_profit"),
            "total_profit": latest.get("total_profit"),
            "n_income": latest.get("n_income"),
            "revenue_yoy": latest.get("yoy_sales"),
            "profit_yoy": latest.get("yoy_net_profit"),
        }
        self._save_cache(cache_key, result)
        return result

    def get_cn_stock_disclosure_dates(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare disclosure_date — 财报披露计划。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_disclosure_date:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("disclosure_date", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "end_date", "pre_date", "actual_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        frame = frame.sort_values(["end_date", "pre_date", "actual_date"], ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_holder_trades(self, symbol: str, start_date: str = "", end_date: str = "") -> list[Dict[str, Any]]:
        """Tushare stk_holdertrade — 大股东/高管增减持。"""
        ts_code = self._to_ts_code(symbol)
        end = end_date or datetime.now().strftime("%Y%m%d")
        start = start_date or (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
        cache_key = f"valuation:ts_stk_holdertrade:{ts_code}:{start}:{end}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("stk_holdertrade", ts_code=ts_code, start_date=start, end_date=end)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        if "ann_date" in frame.columns:
            frame["ann_date"] = frame["ann_date"].map(self._normalize_date_text)
        for column in ("change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values("ann_date", ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_dividend(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare dividend — 分红送转。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_dividend:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("dividend", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("end_date", "ann_date", "record_date", "ex_date", "pay_date", "div_listdate", "imp_ann_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("cash_div", "cash_div_tax", "stk_div", "stk_bo_rate", "stk_co_rate"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values(["ann_date", "end_date"], ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_repurchase(self, symbol: str, start_date: str = "", end_date: str = "") -> list[Dict[str, Any]]:
        """Tushare repurchase — 回购进展。"""
        ts_code = self._to_ts_code(symbol)
        end = end_date or datetime.now().strftime("%Y%m%d")
        start = start_date or (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        cache_key = f"valuation:ts_repurchase:{ts_code}:{start}:{end}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("repurchase", ts_code=ts_code, start_date=start, end_date=end)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "end_date", "exp_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("vol", "amount", "high_limit", "low_limit"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values("ann_date", ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_top10_holders(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare top10_holders — 前十大股东。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_top10_holders:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        raw = self._ts_call("top10_holders", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "end_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values(["end_date", "hold_ratio"], ascending=[False, False]).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_top10_floatholders(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare top10_floatholders — 前十大流通股东。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_top10_floatholders:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        raw = self._ts_call("top10_floatholders", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "end_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values(["end_date", "hold_float_ratio"], ascending=[False, False]).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_pledge_stat(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare pledge_stat — 股权质押统计。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_pledge_stat:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        raw = self._ts_call("pledge_stat", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        if "end_date" in frame.columns:
            frame["end_date"] = frame["end_date"].map(self._normalize_date_text)
        for column in ("pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values("end_date", ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_pledge_detail(self, symbol: str, start_date: str = "", end_date: str = "") -> list[Dict[str, Any]]:
        """Tushare pledge_detail — 股权质押明细。"""
        ts_code = self._to_ts_code(symbol)
        end = end_date or datetime.now().strftime("%Y%m%d")
        start = start_date or (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")
        cache_key = f"valuation:ts_pledge_detail:{ts_code}:{start}:{end}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        raw = self._ts_call("pledge_detail", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "start_date", "end_date", "release_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("pledge_amount", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if "ann_date" in frame.columns:
            frame["ann_date_ts"] = pd.to_datetime(frame["ann_date"], errors="coerce")
            frame = frame.sort_values(["ann_date_ts", "p_total_ratio"], ascending=[False, False]).drop(columns=["ann_date_ts"])
        records = frame.reset_index(drop=True).to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def _normalize_cyq_perf_frame(self, frame: pd.DataFrame | None) -> pd.DataFrame:
        columns = [
            "ts_code",
            "trade_date",
            "his_low",
            "his_high",
            "cost_5pct",
            "cost_15pct",
            "cost_50pct",
            "cost_85pct",
            "cost_95pct",
            "weight_avg",
            "winner_rate",
        ]
        if frame is None:
            return pd.DataFrame(columns=columns)
        if frame.empty:
            return pd.DataFrame(columns=columns)
        working = frame.copy()
        if "ts_code" not in working.columns or "trade_date" not in working.columns:
            return pd.DataFrame(columns=columns)
        normalized = pd.DataFrame(
            {
                "ts_code": working["ts_code"].astype(str),
                "trade_date": working["trade_date"].map(self._normalize_date_text),
            }
        )
        for column in columns[2:]:
            normalized[column] = pd.to_numeric(working.get(column), errors="coerce")
        normalized = normalized.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
        return normalized

    def _normalize_cyq_chips_frame(self, frame: pd.DataFrame | None) -> pd.DataFrame:
        columns = ["ts_code", "trade_date", "price", "percent"]
        if frame is None:
            return pd.DataFrame(columns=columns)
        if frame.empty:
            return pd.DataFrame(columns=columns)
        working = frame.copy()
        if "ts_code" not in working.columns or "trade_date" not in working.columns:
            return pd.DataFrame(columns=columns)
        normalized = pd.DataFrame(
            {
                "ts_code": working["ts_code"].astype(str),
                "trade_date": working["trade_date"].map(self._normalize_date_text),
                "price": pd.to_numeric(working.get("price"), errors="coerce"),
                "percent": pd.to_numeric(working.get("percent"), errors="coerce"),
            }
        )
        normalized = normalized.dropna(subset=["trade_date", "price", "percent"]).sort_values(
            ["trade_date", "percent", "price"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        return normalized

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").dropna()
        if numeric.empty:
            return None
        return float(numeric.iloc[0])

    @staticmethod
    def _normalize_cyq_percent(value: Any) -> float | None:
        percent = ValuationCollector._coerce_float(value)
        if percent is None:
            return None
        if abs(percent) <= 1.0:
            percent *= 100.0
        return percent

    def get_cn_stock_chip_snapshot(
        self,
        symbol: str,
        *,
        as_of: str = "",
        lookback_days: int = 90,
        current_price: float | None = None,
    ) -> Dict[str, Any]:
        """Unified Tushare-first stock chip snapshot for cyq_perf / cyq_chips."""
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
        window_start = (as_of_ts - timedelta(days=max(int(lookback_days), 30))).strftime("%Y%m%d")
        window_end = latest_trade_date
        latest_trade_ts = pd.Timestamp(latest_trade_text) if latest_trade_text else as_of_ts
        near_start = (latest_trade_ts - timedelta(days=10)).strftime("%Y%m%d")

        perf_error: BaseException | None = None
        try:
            # We only consume the latest chip-performance row, so prefer a narrow
            # recent window first. Fall back to the broader lookback only when the
            # near window is empty, keeping the output contract unchanged while
            # reducing first-run latency for multi-symbol scans.
            perf_raw = self._ts_cyq_perf_snapshot(ts_code=ts_code, start_date=near_start, end_date=window_end)
            if perf_raw is None or getattr(perf_raw, "empty", False):
                perf_raw = self._ts_cyq_perf_snapshot(ts_code=ts_code, start_date=window_start, end_date=window_end)
        except Exception as exc:  # noqa: BLE001
            perf_raw = None
            perf_error = exc
        perf_frame = self._normalize_cyq_perf_frame(perf_raw)
        perf_frame = perf_frame[perf_frame["ts_code"] == ts_code].reset_index(drop=True)
        perf_diagnosis = "live"
        if perf_error is not None:
            perf_diagnosis = self._tushare_failure_diagnosis(perf_error)
        elif perf_raw is None:
            perf_diagnosis = "unavailable"
        elif perf_frame.empty:
            perf_diagnosis = "empty"

        perf_row = perf_frame.iloc[-1].to_dict() if not perf_frame.empty else {}
        chip_trade_date = str(perf_row.get("trade_date", "")).replace("-", "") or latest_trade_date

        chips_error: BaseException | None = None
        try:
            chips_raw = self._ts_cyq_chips_snapshot(ts_code=ts_code, trade_date=chip_trade_date)
        except Exception as exc:  # noqa: BLE001
            chips_raw = None
            chips_error = exc
        chips_frame = self._normalize_cyq_chips_frame(chips_raw)
        chips_frame = chips_frame[chips_frame["ts_code"] == ts_code].reset_index(drop=True)
        chips_diagnosis = "live"
        if chips_error is not None:
            chips_diagnosis = self._tushare_failure_diagnosis(chips_error)
        elif chips_raw is None:
            chips_diagnosis = "unavailable"
        elif chips_frame.empty:
            chips_diagnosis = "empty"

        perf_trade_text = str(perf_row.get("trade_date", "")).strip()
        winner_rate_pct = self._normalize_cyq_percent(perf_row.get("winner_rate"))
        avg_cost = self._coerce_float(perf_row.get("weight_avg"))
        cost_15pct = self._coerce_float(perf_row.get("cost_15pct"))
        cost_50pct = self._coerce_float(perf_row.get("cost_50pct"))
        cost_85pct = self._coerce_float(perf_row.get("cost_85pct"))
        current_price_value = self._coerce_float(current_price)

        chips_latest = chips_frame[chips_frame["trade_date"] == perf_trade_text].copy() if perf_trade_text else chips_frame.copy()
        if chips_latest.empty and not chips_frame.empty:
            latest_distribution_date = str(chips_frame["trade_date"].max()).strip()
            chips_latest = chips_frame[chips_frame["trade_date"] == latest_distribution_date].copy()
        distribution_trade_text = str(chips_latest["trade_date"].iloc[0]).strip() if not chips_latest.empty else ""
        chips_latest = chips_latest.sort_values(["percent", "price"], ascending=[False, True]).reset_index(drop=True)

        peak_price = None
        peak_percent = None
        above_price_pct = None
        near_price_pct = None
        if not chips_latest.empty:
            peak_row = chips_latest.iloc[0]
            peak_price = float(peak_row["price"])
            peak_percent = float(peak_row["percent"])
            if current_price_value and current_price_value > 0:
                above_price_pct = float(chips_latest.loc[chips_latest["price"] > current_price_value, "percent"].sum())
                near_price_pct = float(
                    chips_latest.loc[(chips_latest["price"] / current_price_value - 1.0).abs() <= 0.03, "percent"].sum()
                )

        blocked_diagnoses = {"permission_blocked", "rate_limited", "network_error", "fetch_error", "unavailable"}
        if perf_row or not chips_latest.empty:
            status = "matched"
        elif perf_diagnosis in blocked_diagnoses or chips_diagnosis in blocked_diagnoses:
            status = "blocked"
        else:
            status = "empty"

        matched_dates = [date for date in (perf_trade_text, distribution_trade_text) if date]
        latest_date = max(matched_dates, default=latest_trade_text)
        is_fresh = status == "matched" and latest_date == latest_trade_text
        if status == "matched" and is_fresh:
            diagnosis = "live"
        elif status == "matched":
            diagnosis = "stale"
        elif perf_diagnosis in blocked_diagnoses:
            diagnosis = perf_diagnosis
        elif chips_diagnosis in blocked_diagnoses:
            diagnosis = chips_diagnosis
        else:
            diagnosis = "empty"
        price_vs_avg = (current_price_value / avg_cost - 1.0) if current_price_value and avg_cost else None

        if status == "matched" and not is_fresh:
            detail = (
                f"最新可用筹码日期停在 {latest_date}，当前不按 fresh 命中处理；"
                "先只保留缺口披露，不把旧筹码直接写成今天的资金确认。"
            )
        elif status == "matched" and current_price_value and avg_cost:
            above_text = f"，上方套牢盘约 {above_price_pct:.1f}%" if above_price_pct is not None else ""
            if above_price_pct is not None and above_price_pct >= 60:
                detail = f"现价 {current_price_value:.2f} 元仍低于加权平均成本 {avg_cost:.2f} 元{above_text}，筹码压力偏重。"
            elif winner_rate_pct is not None and winner_rate_pct >= 65 and price_vs_avg >= 0:
                detail = f"现价 {current_price_value:.2f} 元高于加权平均成本 {avg_cost:.2f} 元 {price_vs_avg:.1%}{above_text}，多数筹码已进入盈利区。"
            elif near_price_pct is not None and near_price_pct >= 30:
                detail = f"现价 {current_price_value:.2f} 元附近筹码约 {near_price_pct:.1f}%，主筹码开始在现价附近换手。"
            else:
                detail = f"现价 {current_price_value:.2f} 元相对加权平均成本 {avg_cost:.2f} 元约 {price_vs_avg:+.1%}{above_text}，筹码分布仍在拉锯。"
        elif status == "matched" and avg_cost:
            winner_text = f" / 胜率 {winner_rate_pct:.1f}%" if winner_rate_pct is not None else ""
            detail = f"最新加权平均成本约 {avg_cost:.2f} 元{winner_text}，需结合现价判断筹码承压位置。"
        elif status == "empty":
            detail = "Tushare cyq_perf / cyq_chips 当前未返回可用筹码数据。"
        else:
            blocked = perf_diagnosis if perf_diagnosis in blocked_diagnoses else chips_diagnosis
            detail = self._blocked_disclosure(blocked, source="Tushare cyq_perf / cyq_chips")

        return {
            "symbol": cleaned_symbol,
            "ts_code": ts_code,
            "as_of": as_of_text,
            "latest_date": latest_date,
            "is_fresh": is_fresh,
            "source": "tushare.cyq_perf+tushare.cyq_chips",
            "fallback": "none",
            "diagnosis": diagnosis,
            "disclosure": "Tushare cyq_perf / cyq_chips 用于刻画平均成本、胜率和成本密集区；权限失败、空表或频控时只按缺失处理，不把空结果写成筹码轻松。",  # noqa: E501
            "status": status,
            "detail": detail,
            "current_price": current_price_value,
            "winner_rate_pct": winner_rate_pct,
            "weight_avg": avg_cost,
            "cost_15pct": cost_15pct,
            "cost_50pct": cost_50pct,
            "cost_85pct": cost_85pct,
            "price_vs_weight_avg_pct": price_vs_avg,
            "above_price_pct": above_price_pct,
            "near_price_pct": near_price_pct,
            "peak_price": peak_price,
            "peak_percent": peak_percent,
            "components": {
                "cyq_perf": {
                    "source": "tushare.cyq_perf",
                    "as_of": perf_trade_text or latest_trade_text,
                    "fallback": "none",
                    "diagnosis": perf_diagnosis,
                    "disclosure": (
                        "Tushare cyq_perf 已命中最新筹码胜率和平均成本快照。"
                        if perf_row
                        else (
                            "Tushare cyq_perf 当前未返回该股票的筹码快照。"
                            if perf_diagnosis == "empty"
                            else self._blocked_disclosure(perf_diagnosis, source="Tushare cyq_perf")
                        )
                    ),
                    "status": "matched" if perf_row else "empty" if perf_diagnosis == "empty" else "blocked",
                    "detail": (
                        (
                            f"最新胜率 {winner_rate_pct:.1f}% / 加权平均成本 {avg_cost:.2f} 元"
                            if winner_rate_pct is not None and avg_cost is not None
                            else f"最新加权平均成本 {avg_cost:.2f} 元"
                            if avg_cost is not None
                            else f"最新胜率 {winner_rate_pct:.1f}%"
                            if winner_rate_pct is not None
                            else "当前未拿到可用筹码胜率/平均成本快照"
                        )
                        if perf_row
                        else "当前未拿到可用筹码胜率/平均成本快照"
                    ),
                    "item": perf_row,
                },
                "cyq_chips": {
                    "source": "tushare.cyq_chips",
                    "as_of": distribution_trade_text or latest_trade_text,
                    "fallback": "none",
                    "diagnosis": chips_diagnosis,
                    "disclosure": (
                        "Tushare cyq_chips 已命中最新成本分布。"
                        if not chips_latest.empty
                        else (
                            "Tushare cyq_chips 当前未返回该股票的筹码分布。"
                            if chips_diagnosis == "empty"
                            else self._blocked_disclosure(chips_diagnosis, source="Tushare cyq_chips")
                        )
                    ),
                    "status": "matched" if not chips_latest.empty else "empty" if chips_diagnosis == "empty" else "blocked",
                    "detail": (
                        (
                            f"主筹码密集区 {peak_price:.2f} 元 / 单价位占比 {peak_percent:.1f}% / 现价附近筹码 {near_price_pct:.1f}%"
                            if peak_price is not None and peak_percent is not None and near_price_pct is not None
                            else f"主筹码密集区 {peak_price:.2f} 元 / 单价位占比 {peak_percent:.1f}%"
                            if peak_price is not None and peak_percent is not None
                            else "当前未拿到可用成本分布"
                        )
                        if not chips_latest.empty
                        else "当前未拿到可用成本分布"
                    ),
                    "items": chips_latest.head(10).to_dict("records"),
                },
            },
        }

    # ── 指数聚合财务 ─────────────────────────────────────────

    def get_cn_index_financial_proxies(self, index_code: str, top_n: int = 5) -> Dict[str, Any]:
        """Aggregate weighted financial proxies from top constituents."""
        constituents = self.get_cn_index_constituent_weights(index_code=index_code, top_n=top_n)
        if constituents.empty:
            return {}

        total_weight = float(pd.to_numeric(constituents["weight"], errors="coerce").fillna(0.0).sum())
        if total_weight <= 0:
            return {}

        rows: list[Dict[str, Any]] = []
        for _, row in constituents.iterrows():
            symbol = str(row.get("symbol", "")).strip()
            if not symbol:
                continue
            try:
                snapshot = self.get_cn_stock_financial_proxy(symbol)
            except Exception:
                continue
            snapshot["weight"] = float(row.get("weight", 0.0))
            snapshot["symbol"] = symbol
            snapshot["name"] = str(row.get("name", symbol))
            rows.append(snapshot)

        if not rows:
            return {
                "top_concentration": float(pd.to_numeric(constituents["weight"], errors="coerce").fillna(0.0).sum()),
                "coverage_weight": 0.0,
                "coverage_count": 0,
            }

        metrics = {
            "pe_ttm": self._weighted_harmonic_average(rows, "pe_ttm"),
            "ps_ttm": self._weighted_harmonic_average(rows, "ps_ttm"),
            "revenue_yoy": self._weighted_average(rows, "revenue_yoy"),
            "profit_yoy": self._weighted_average(rows, "profit_yoy"),
            "roe": self._weighted_average(rows, "roe"),
            "gross_margin": self._weighted_average(rows, "gross_margin"),
            "dv_ratio": self._weighted_average(rows, "dv_ratio"),
        }
        report_dates = [str(item.get("report_date", "")).strip() for item in rows if str(item.get("report_date", "")).strip()]
        coverage_weight = float(sum(float(item.get("weight", 0.0)) for item in rows))
        return {
            **metrics,
            "top_concentration": float(pd.to_numeric(constituents["weight"], errors="coerce").fillna(0.0).sum()),
            "coverage_weight": coverage_weight,
            "coverage_ratio": coverage_weight / total_weight if total_weight else 0.0,
            "coverage_count": len(rows),
            "report_date": max(report_dates) if report_dates else "",
            "constituents": constituents.to_dict("records"),
        }

    def get_weighted_stock_financial_proxies(
        self,
        holdings: Sequence[Dict[str, Any]],
        symbol_key: str = "symbol",
        name_key: str = "name",
        weight_key: str = "weight",
        top_n: int = 5,
    ) -> Dict[str, Any]:
        """Aggregate weighted financial proxies from an arbitrary holdings list."""
        normalized_rows: list[Dict[str, Any]] = []
        for raw in list(holdings)[:top_n]:
            symbol = str(raw.get(symbol_key, "")).strip()
            weight = pd.to_numeric(pd.Series([raw.get(weight_key)]), errors="coerce").iloc[0]
            if not symbol or pd.isna(weight) or float(weight) <= 0:
                continue
            normalized_rows.append(
                {
                    "symbol": symbol,
                    "name": str(raw.get(name_key, symbol)).strip() or symbol,
                    "weight": float(weight),
                }
            )
        if not normalized_rows:
            return {}

        total_weight = float(sum(float(item["weight"]) for item in normalized_rows))
        if total_weight <= 0:
            return {}

        rows: list[Dict[str, Any]] = []
        for row in normalized_rows:
            try:
                snapshot = self.get_cn_stock_financial_proxy(row["symbol"])
            except Exception:
                continue
            snapshot["weight"] = float(row["weight"])
            snapshot["symbol"] = row["symbol"]
            snapshot["name"] = row["name"]
            rows.append(snapshot)

        if not rows:
            return {
                "top_concentration": total_weight,
                "coverage_weight": 0.0,
                "coverage_ratio": 0.0,
                "coverage_count": 0,
                "constituents": normalized_rows,
            }

        metrics = {
            "pe_ttm": self._weighted_harmonic_average(rows, "pe_ttm"),
            "ps_ttm": self._weighted_harmonic_average(rows, "ps_ttm"),
            "revenue_yoy": self._weighted_average(rows, "revenue_yoy"),
            "profit_yoy": self._weighted_average(rows, "profit_yoy"),
            "roe": self._weighted_average(rows, "roe"),
            "gross_margin": self._weighted_average(rows, "gross_margin"),
            "dv_ratio": self._weighted_average(rows, "dv_ratio"),
        }
        report_dates = [str(item.get("report_date", "")).strip() for item in rows if str(item.get("report_date", "")).strip()]
        coverage_weight = float(sum(float(item.get("weight", 0.0)) for item in rows))
        return {
            **metrics,
            "top_concentration": total_weight,
            "coverage_weight": coverage_weight,
            "coverage_ratio": coverage_weight / total_weight if total_weight else 0.0,
            "coverage_count": len(rows),
            "report_date": max(report_dates) if report_dates else "",
            "constituents": normalized_rows,
        }

    def get_weighted_market_financial_proxies(
        self,
        holdings: Sequence[Dict[str, Any]],
        *,
        asset_type: str,
        symbol_key: str = "symbol",
        name_key: str = "name",
        weight_key: str = "weight",
        top_n: int = 5,
    ) -> Dict[str, Any]:
        """Aggregate weighted fundamentals for HK/US holdings via yfinance."""
        normalized_rows: list[Dict[str, Any]] = []
        for raw in list(holdings)[:top_n]:
            symbol = str(raw.get(symbol_key, "")).strip()
            weight = pd.to_numeric(pd.Series([raw.get(weight_key)]), errors="coerce").iloc[0]
            if not symbol or pd.isna(weight) or float(weight) <= 0:
                continue
            normalized_rows.append(
                {
                    "symbol": symbol,
                    "name": str(raw.get(name_key, symbol)).strip() or symbol,
                    "weight": float(weight),
                }
            )
        if not normalized_rows:
            return {}

        total_weight = float(sum(float(item["weight"]) for item in normalized_rows))
        if total_weight <= 0:
            return {}

        rows: list[Dict[str, Any]] = []
        for row in normalized_rows:
            try:
                snapshot = self.get_yf_fundamental(row["symbol"], asset_type)
            except Exception:
                continue
            if not snapshot:
                continue
            snapshot["weight"] = float(row["weight"])
            snapshot["symbol"] = row["symbol"]
            snapshot["name"] = row["name"]
            rows.append(snapshot)

        if not rows:
            return {
                "top_concentration": total_weight,
                "coverage_weight": 0.0,
                "coverage_ratio": 0.0,
                "coverage_count": 0,
                "constituents": normalized_rows,
            }

        coverage_weight = float(sum(float(item.get("weight", 0.0)) for item in rows))
        return {
            "pe_ttm": self._weighted_harmonic_average(rows, "pe_ttm"),
            "ps_ttm": self._weighted_harmonic_average(rows, "ps_ttm"),
            "revenue_yoy": self._weighted_average(rows, "revenue_yoy"),
            "roe": self._weighted_average(rows, "roe"),
            "gross_margin": self._weighted_average(rows, "gross_margin"),
            "top_concentration": total_weight,
            "coverage_weight": coverage_weight,
            "coverage_ratio": coverage_weight / total_weight if total_weight else 0.0,
            "coverage_count": len(rows),
            "constituents": normalized_rows,
        }

    # ── HK/US yfinance 估值 ──────────────────────────────────

    def get_yf_fundamental(self, symbol: str, asset_type: str) -> Dict[str, Any]:
        """Fetch fundamental metrics for HK/US stocks via yfinance Ticker.info."""
        if yf is None:
            return {}
        ticker = self._yf_ticker(symbol, asset_type)
        if not ticker:
            return {}
        try:
            info = self.cached_call(
                f"valuation:yf_fundamental:{ticker}",
                lambda: yf.Ticker(ticker).info,
                ttl_hours=12,
            )
        except Exception:
            return {}
        if not isinstance(info, dict):
            return {}
        result: Dict[str, Any] = {}
        pe = info.get("trailingPE")
        if pe is not None:
            try:
                result["pe_ttm"] = float(pe)
            except (ValueError, TypeError):
                pass
        ps = info.get("priceToSalesTrailing12Months")
        if ps is not None:
            try:
                result["ps_ttm"] = float(ps)
            except (ValueError, TypeError):
                pass
        roe = info.get("returnOnEquity")
        if roe is not None:
            try:
                result["roe"] = float(roe) * 100  # decimal → percentage
            except (ValueError, TypeError):
                pass
        rev_growth = info.get("revenueGrowth")
        if rev_growth is not None:
            try:
                result["revenue_yoy"] = float(rev_growth) * 100
            except (ValueError, TypeError):
                pass
        gross = info.get("grossMargins")
        if gross is not None:
            try:
                result["gross_margin"] = float(gross) * 100
            except (ValueError, TypeError):
                pass
        peg = info.get("trailingPegRatio")
        if peg is not None:
            try:
                result["peg"] = float(peg)
            except (ValueError, TypeError):
                pass
        return result

    def _yf_ticker(self, symbol: str, asset_type: str) -> str:
        """Convert symbol to yfinance ticker format."""
        if asset_type == "hk":
            if symbol.upper().endswith(".HK"):
                code = symbol[:-3].lstrip("0") or "0"
                return f"{code.zfill(4)}.HK"
            if symbol.isdigit():
                code = symbol.lstrip("0") or "0"
                return f"{code.zfill(4)}.HK"
            return symbol
        if asset_type == "us":
            return symbol.upper().replace(".US", "")
        return symbol

    def _eastmoney_symbol(self, symbol: str) -> str:
        if symbol.startswith(("6", "9")):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"

    # ── 工具方法 ─────────────────────────────────────────────

    def _weighted_average(self, rows: Iterable[Dict[str, Any]], field: str) -> Optional[float]:
        pairs: list[tuple[float, float]] = []
        for row in rows:
            value = pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0]
            weight = pd.to_numeric(pd.Series([row.get("weight")]), errors="coerce").iloc[0]
            if pd.isna(value) or pd.isna(weight) or float(weight) <= 0:
                continue
            pairs.append((float(value), float(weight)))
        if not pairs:
            return None
        total_weight = sum(weight for _, weight in pairs)
        if total_weight <= 0:
            return None
        return sum(value * weight for value, weight in pairs) / total_weight

    def _weighted_harmonic_average(self, rows: Iterable[Dict[str, Any]], field: str) -> Optional[float]:
        numer = 0.0
        denom = 0.0
        for row in rows:
            value = pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0]
            weight = pd.to_numeric(pd.Series([row.get("weight")]), errors="coerce").iloc[0]
            if pd.isna(value) or pd.isna(weight):
                continue
            value_num = float(value)
            weight_num = float(weight)
            if value_num <= 0 or weight_num <= 0:
                continue
            numer += weight_num
            denom += weight_num / value_num
        if numer <= 0 or denom <= 0:
            return None
        return numer / denom

    def _parse_stock_financial_frame(self, frame: pd.DataFrame) -> Dict[str, Any]:
        if frame is None or frame.empty:
            return {}
        if {"metric_name", "value"}.issubset(frame.columns):
            return self._parse_long_financial_frame(frame)
        return self._parse_wide_financial_frame(frame)

    def _parse_long_financial_frame(self, frame: pd.DataFrame) -> Dict[str, Any]:
        normalized = frame.copy()
        if "report_date" in normalized.columns:
            normalized["report_date"] = pd.to_datetime(normalized["report_date"], errors="coerce")
            normalized = normalized.sort_values("report_date", ascending=False)
            latest_date = normalized["report_date"].dropna().iloc[0] if normalized["report_date"].notna().any() else pd.NaT
            if pd.notna(latest_date):
                normalized = normalized[normalized["report_date"] == latest_date]
        report_date = ""
        if "report_date" in normalized.columns and normalized["report_date"].notna().any():
            report_date = normalized["report_date"].dropna().iloc[0].strftime("%Y-%m-%d")

        metric_col = normalized["metric_name"].astype(str)
        value_col = pd.to_numeric(normalized.get("value", pd.Series(dtype=float)), errors="coerce")
        yoy_col = pd.to_numeric(normalized.get("yoy", pd.Series(dtype=float)), errors="coerce")

        return {
            "report_date": report_date,
            "revenue_yoy": self._pick_metric(metric_col, yoy_col, ("营业总收入", "营业收入", "营收")),
            "profit_yoy": self._pick_metric(metric_col, yoy_col, ("归母净利润", "净利润", "扣非净利润")),
            "roe": self._pick_metric(metric_col, value_col, ("净资产收益率", "ROE", "净资收益率")),
            "gross_margin": self._pick_metric(metric_col, value_col, ("销售毛利率", "毛利率")),
        }

    def _parse_wide_financial_frame(self, frame: pd.DataFrame) -> Dict[str, Any]:
        normalized = frame.copy()
        date_col = self._first_existing_column(normalized, ("报告期", "REPORT_DATE", "日期", "date"))
        if date_col:
            normalized["_report_date"] = pd.to_datetime(normalized[date_col], errors="coerce")
            normalized = normalized.sort_values("_report_date", ascending=False)
        latest = normalized.iloc[0]
        report_date = ""
        if "_report_date" in normalized.columns and pd.notna(latest.get("_report_date")):
            report_date = pd.to_datetime(latest["_report_date"]).strftime("%Y-%m-%d")

        return {
            "report_date": report_date,
            "revenue_yoy": self._row_value(latest, ("营业总收入同比增长", "营业收入同比增长", "营收同比增长", "TOTALOPERATEREVETZ")),
            "profit_yoy": self._row_value(latest, ("归母净利润同比增长", "净利润同比增长", "扣非净利润同比增长", "PARENTNETPROFITTZ")),
            "roe": self._row_value(latest, ("净资产收益率", "净资产收益率加权", "加权净资产收益率", "ROEJQ")),
            "gross_margin": self._row_value(latest, ("销售毛利率", "毛利率", "XSMLL")),
        }

    def _pick_metric(self, metric_names: pd.Series, values: pd.Series, keywords: Sequence[str]) -> Optional[float]:
        lowered_keywords = [str(item).lower() for item in keywords]
        for metric_name, value in zip(metric_names, values):
            label = str(metric_name).strip().lower()
            if any(keyword in label for keyword in lowered_keywords) and pd.notna(value):
                return float(value)
        return None

    def _row_value(self, row: pd.Series, candidates: Sequence[str]) -> Optional[float]:
        for candidate in candidates:
            if candidate in row.index:
                value = pd.to_numeric(pd.Series([row.get(candidate)]), errors="coerce").iloc[0]
                if pd.notna(value):
                    return float(value)
        lowered_map = {str(column).lower(): column for column in row.index}
        for candidate in candidates:
            matched = lowered_map.get(str(candidate).lower())
            if matched is None:
                continue
            value = pd.to_numeric(pd.Series([row.get(matched)]), errors="coerce").iloc[0]
            if pd.notna(value):
                return float(value)
        return None

    def _first_existing_column(self, frame: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
        for candidate in candidates:
            if candidate in frame.columns:
                return candidate
        lowered = {str(column).lower(): column for column in frame.columns}
        for candidate in candidates:
            matched = lowered.get(str(candidate).lower())
            if matched:
                return matched
        return None
