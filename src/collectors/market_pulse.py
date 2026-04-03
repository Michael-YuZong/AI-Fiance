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
        as_of_text = as_of.strftime("%Y-%m-%d %H:%M:%S")

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
            "as_of": as_of_text,
            "market_date": latest_trade_date,
            "zt_pool": zt_info["frame"],
            "zt_pool_report": self._pool_report(zt_info, as_of, source="zt_pool"),
            "prev_zt_pool": prev_zt_info["frame"],
            "prev_zt_pool_report": self._pool_report(prev_zt_info, as_of, source="prev_zt_pool"),
            "strong_pool": strong_info["frame"],
            "strong_pool_report": self._pool_report(strong_info, as_of, source="strong_pool"),
            "dt_pool": dt_info["frame"],
            "dt_pool_report": self._pool_report(dt_info, as_of, source="dt_pool"),
            "lhb_detail": lhb_detail,
            "lhb_stats": lhb_stats,
            "lhb_institution": lhb_institution,
            "lhb_desks": lhb_desks,
        }

    def get_stock_board_action_snapshot(
        self,
        symbol: str,
        *,
        reference_date: Optional[datetime] = None,
        display_name: str = "",
        current_price: float | None = None,
    ) -> Dict[str, Any]:
        """Unified 龙虎榜 / 竞价 / 涨跌停边界 snapshot for a single stock."""
        from .market_cn import ChinaMarketCollector

        as_of = reference_date or datetime.now()
        as_of_text = as_of.strftime("%Y-%m-%d %H:%M:%S")
        latest_trade_date = self._latest_open_trade_date(lookback_days=14) or as_of.strftime("%Y%m%d")
        latest_trade_text = self._normalize_date_text(latest_trade_date) or as_of.strftime("%Y-%m-%d")
        ts_code = self._to_ts_code(symbol)
        stock_name = str(display_name or symbol).strip() or str(symbol).strip()

        def _coerce_float(value: Any) -> float | None:
            series = pd.to_numeric(pd.Series([value]), errors="coerce").dropna()
            return None if series.empty else float(series.iloc[0])

        def _fmt_amount(value: Any) -> str:
            numeric = _coerce_float(value)
            return "缺失" if numeric is None else f"{numeric / 100_000_000.0:.2f}亿"

        def _filter_stock_rows(frame: pd.DataFrame) -> pd.DataFrame:
            if frame is None or frame.empty:
                return pd.DataFrame()
            working = frame.copy()
            if "ts_code" in working.columns:
                return working[working["ts_code"].astype(str) == ts_code].reset_index(drop=True)
            if "代码" in working.columns:
                return working[working["代码"].astype(str) == str(symbol).strip()].reset_index(drop=True)
            return pd.DataFrame()

        components: Dict[str, Dict[str, Any]] = {}

        lhb_error: BaseException | None = None
        try:
            lhb_detail = self._ts_top_list(latest_trade_text)
        except Exception as exc:  # noqa: BLE001
            lhb_detail = pd.DataFrame()
            lhb_error = exc
        lhb_rows = _filter_stock_rows(lhb_detail)
        lhb_diagnosis = "live"
        if lhb_error is not None:
            lhb_diagnosis = self._tushare_failure_diagnosis(lhb_error)
        elif lhb_rows.empty and (lhb_detail is None or lhb_detail.empty):
            lhb_diagnosis = "empty"
        lhb_reason_col = self._first_existing_column(lhb_rows, ("reason", "上榜原因"))
        lhb_net_col = self._first_existing_column(lhb_rows, ("net_amount", "净买额", "总买卖净额"))
        lhb_amount_col = self._first_existing_column(lhb_rows, ("amount", "成交额"))
        lhb_reason = str(lhb_rows.iloc[0].get(lhb_reason_col, "")).strip() if not lhb_rows.empty and lhb_reason_col else ""
        lhb_net_amount = None
        if not lhb_rows.empty and lhb_net_col:
            net_series = pd.to_numeric(lhb_rows[lhb_net_col], errors="coerce").dropna()
            if not net_series.empty:
                lhb_net_amount = float(net_series.sum())
        lhb_amount = None
        if not lhb_rows.empty and lhb_amount_col:
            amount_series = pd.to_numeric(lhb_rows[lhb_amount_col], errors="coerce").dropna()
            if not amount_series.empty:
                lhb_amount = float(amount_series.max())
        components["top_list"] = {
            "source": "tushare.top_list",
            "as_of": latest_trade_text,
            "fallback": "none",
            "diagnosis": lhb_diagnosis,
            "disclosure": (
                "Tushare top_list 提供龙虎榜日度明细。"
                if lhb_diagnosis == "live"
                else "Tushare top_list 当前未命中该股票。"
                if lhb_diagnosis == "empty"
                else self._blocked_disclosure(lhb_diagnosis, source="Tushare top_list")
            ),
            "status": "matched" if not lhb_rows.empty else "empty" if lhb_diagnosis == "empty" else "blocked",
            "detail": (
                f"龙虎榜净买额 {_fmt_amount(lhb_net_amount)}；原因：{lhb_reason or '未披露'}"
                if not lhb_rows.empty
                else "当前未命中龙虎榜明细。"
            ),
            "items": lhb_rows.to_dict("records")[:3],
        }

        inst_error: BaseException | None = None
        try:
            inst_detail = self._ts_top_inst(latest_trade_text)
        except Exception as exc:  # noqa: BLE001
            inst_detail = pd.DataFrame()
            inst_error = exc
        inst_rows = _filter_stock_rows(inst_detail)
        inst_diagnosis = "live"
        if inst_error is not None:
            inst_diagnosis = self._tushare_failure_diagnosis(inst_error)
        elif inst_rows.empty and (inst_detail is None or inst_detail.empty):
            inst_diagnosis = "empty"
        inst_net_amount = None
        if not inst_rows.empty:
            inst_net_col = self._first_existing_column(inst_rows, ("net_buy", "net_amount", "净买额", "净买入额"))
            if inst_net_col:
                net_series = pd.to_numeric(inst_rows[inst_net_col], errors="coerce").dropna()
                if not net_series.empty:
                    inst_net_amount = float(net_series.sum())
            if inst_net_amount is None:
                buy_col = self._first_existing_column(inst_rows, ("buy", "买入额"))
                sell_col = self._first_existing_column(inst_rows, ("sell", "卖出额"))
                if buy_col and sell_col:
                    buy_series = pd.to_numeric(inst_rows[buy_col], errors="coerce").dropna()
                    sell_series = pd.to_numeric(inst_rows[sell_col], errors="coerce").dropna()
                    if not buy_series.empty and not sell_series.empty:
                        inst_net_amount = float(buy_series.sum() - sell_series.sum())
        components["top_inst"] = {
            "source": "tushare.top_inst",
            "as_of": latest_trade_text,
            "fallback": "none",
            "diagnosis": inst_diagnosis,
            "disclosure": (
                "Tushare top_inst 提供龙虎榜机构席位明细。"
                if inst_diagnosis == "live"
                else "Tushare top_inst 当前未命中该股票。"
                if inst_diagnosis == "empty"
                else self._blocked_disclosure(inst_diagnosis, source="Tushare top_inst")
            ),
            "status": "matched" if not inst_rows.empty else "empty" if inst_diagnosis == "empty" else "blocked",
            "detail": (
                f"机构席位净买额 {_fmt_amount(inst_net_amount)}"
                if not inst_rows.empty
                else "当前未命中机构席位明细。"
            ),
            "items": inst_rows.to_dict("records")[:3],
        }

        cn_market = ChinaMarketCollector(self.config)
        auction_error: BaseException | None = None
        try:
            auction_frame = cn_market.get_stock_auction(str(symbol), trade_date=latest_trade_date)
        except Exception as exc:  # noqa: BLE001
            auction_frame = pd.DataFrame()
            auction_error = exc
        auction_rows = _filter_stock_rows(auction_frame)
        auction_diagnosis = "live"
        if auction_error is not None:
            auction_diagnosis = self._tushare_failure_diagnosis(auction_error)
        elif auction_rows.empty and (auction_frame is None or auction_frame.empty):
            auction_diagnosis = "empty"
        auction_gap_pct = None
        auction_volume_ratio = None
        auction_turnover_rate = None
        if not auction_rows.empty:
            row = dict(auction_rows.iloc[-1])
            auction_price = _coerce_float(row.get("price"))
            pre_close = _coerce_float(row.get("pre_close"))
            if auction_price is not None and pre_close not in (None, 0):
                auction_gap_pct = auction_price / pre_close - 1.0
            auction_volume_ratio = _coerce_float(row.get("volume_ratio"))
            auction_turnover_rate = _coerce_float(row.get("turnover_rate"))
        components["auction"] = {
            "source": "tushare.stk_auction",
            "as_of": latest_trade_text,
            "fallback": "none",
            "diagnosis": auction_diagnosis,
            "disclosure": (
                "Tushare stk_auction 提供集合竞价量价快照。"
                if auction_diagnosis == "live"
                else "Tushare stk_auction 当前未命中该股票。"
                if auction_diagnosis == "empty"
                else self._blocked_disclosure(auction_diagnosis, source="Tushare stk_auction")
            ),
            "status": "matched" if not auction_rows.empty else "empty" if auction_diagnosis == "empty" else "blocked",
            "detail": (
                f"竞价涨跌幅 {(auction_gap_pct or 0.0):+.2%} / 量比 {auction_volume_ratio:.2f}"
                if auction_gap_pct is not None and auction_volume_ratio is not None
                else "当前未命中可用集合竞价快照。"
            ),
            "items": auction_rows.to_dict("records")[:1],
        }

        limit_error: BaseException | None = None
        try:
            limit_frame = cn_market.get_stock_limit(str(symbol), trade_date=latest_trade_date)
        except Exception as exc:  # noqa: BLE001
            limit_frame = pd.DataFrame()
            limit_error = exc
        limit_rows = _filter_stock_rows(limit_frame)
        limit_diagnosis = "live"
        if limit_error is not None:
            limit_diagnosis = self._tushare_failure_diagnosis(limit_error)
        elif limit_rows.empty and (limit_frame is None or limit_frame.empty):
            limit_diagnosis = "empty"
        up_limit = None
        down_limit = None
        up_limit_gap_pct = None
        down_limit_gap_pct = None
        if not limit_rows.empty:
            row = dict(limit_rows.iloc[-1])
            up_limit = _coerce_float(row.get("up_limit"))
            down_limit = _coerce_float(row.get("down_limit"))
            if current_price is not None and up_limit not in (None, 0):
                up_limit_gap_pct = (up_limit - float(current_price)) / up_limit
            if current_price is not None and down_limit not in (None, 0):
                down_limit_gap_pct = (float(current_price) - down_limit) / float(current_price) if float(current_price) else None
        components["limit_price"] = {
            "source": "tushare.stk_limit",
            "as_of": latest_trade_text,
            "fallback": "none",
            "diagnosis": limit_diagnosis,
            "disclosure": (
                "Tushare stk_limit 提供个股涨跌停边界。"
                if limit_diagnosis == "live"
                else "Tushare stk_limit 当前未命中该股票。"
                if limit_diagnosis == "empty"
                else self._blocked_disclosure(limit_diagnosis, source="Tushare stk_limit")
            ),
            "status": "matched" if not limit_rows.empty else "empty" if limit_diagnosis == "empty" else "blocked",
            "detail": (
                f"涨停价 {up_limit:.2f} / 跌停价 {down_limit:.2f}"
                if up_limit is not None and down_limit is not None
                else "当前未命中涨跌停边界。"
            ),
            "items": limit_rows.to_dict("records")[:1],
        }

        zt_error: BaseException | None = None
        dt_error: BaseException | None = None
        try:
            zt_info = self._ts_limit_pool("U", as_of)
        except Exception as exc:  # noqa: BLE001
            zt_info = {"date": "", "frame": pd.DataFrame()}
            zt_error = exc
        try:
            dt_info = self._ts_limit_pool("D", as_of)
        except Exception as exc:  # noqa: BLE001
            dt_info = {"date": "", "frame": pd.DataFrame()}
            dt_error = exc
        strong_info = self._derive_strong_pool(zt_info)
        zt_rows = _filter_stock_rows(zt_info.get("frame", pd.DataFrame()))
        strong_rows = _filter_stock_rows(strong_info.get("frame", pd.DataFrame()))
        dt_rows = _filter_stock_rows(dt_info.get("frame", pd.DataFrame()))
        zt_hit = not zt_rows.empty
        strong_hit = not strong_rows.empty
        dt_hit = not dt_rows.empty
        limit_times = None
        if strong_hit:
            limit_times = _coerce_float(strong_rows.iloc[0].get("连板数"))
        elif zt_hit:
            limit_times = _coerce_float(zt_rows.iloc[0].get("连板数"))
        pool_date = str(strong_info.get("date") or zt_info.get("date") or dt_info.get("date") or "").strip()
        pool_diagnosis = "live" if pool_date == latest_trade_text else "stale" if pool_date else "empty"
        if zt_error is not None and dt_error is not None and not (zt_hit or strong_hit or dt_hit):
            pool_diagnosis = self._tushare_failure_diagnosis(zt_error)
        components["limit_pool"] = {
            "source": "tushare.limit_list_d",
            "as_of": pool_date or latest_trade_text,
            "fallback": "none",
            "diagnosis": pool_diagnosis,
            "disclosure": (
                "Tushare limit_list_d 提供涨停/跌停与连板池。"
                if pool_diagnosis in {"live", "empty", "stale"}
                else self._blocked_disclosure(pool_diagnosis, source="Tushare limit_list_d")
            ),
            "status": "matched" if (zt_hit or strong_hit or dt_hit) else "empty" if pool_diagnosis == "empty" else "blocked",
            "detail": (
                f"{stock_name} {'位于跌停池' if dt_hit else '位于强势连板池' if strong_hit else '位于涨停池' if zt_hit else '当前未命中涨跌停/连板池'}"
            ),
            "items": [*strong_rows.to_dict("records")[:1], *dt_rows.to_dict("records")[:1], *zt_rows.to_dict("records")[:1]],
        }

        positive_bits: List[str] = []
        negative_bits: List[str] = []
        if lhb_net_amount is not None and lhb_net_amount > 0:
            positive_bits.append("龙虎榜净买额为正")
        elif lhb_net_amount is not None and lhb_net_amount < 0:
            negative_bits.append("龙虎榜净卖额为负")
        if inst_net_amount is not None and inst_net_amount > 0:
            positive_bits.append("机构席位净买额为正")
        elif inst_net_amount is not None and inst_net_amount < 0:
            negative_bits.append("机构席位净卖额为负")
        if strong_hit:
            positive_bits.append("命中强势连板池")
        elif zt_hit:
            positive_bits.append("命中涨停池")
        if dt_hit:
            negative_bits.append("命中跌停池")
        if auction_gap_pct is not None and auction_gap_pct >= 0.02 and (auction_volume_ratio or 0) >= 1.2:
            positive_bits.append("竞价高开且量比放大")
        elif auction_gap_pct is not None and auction_gap_pct <= -0.02:
            negative_bits.append("竞价明显低开")
        if up_limit_gap_pct is not None and up_limit_gap_pct <= 0.005:
            positive_bits.append("收盘已贴近涨停边界")
        if down_limit_gap_pct is not None and down_limit_gap_pct <= 0.01:
            negative_bits.append("收盘已贴近跌停边界")

        disclosure_lines = [
            str(component.get("disclosure", "")).strip()
            for component in components.values()
            if str(component.get("disclosure", "")).strip()
        ]
        latest_dates = [
            str(component.get("as_of", "")).strip()
            for component in components.values()
            if str(component.get("as_of", "")).strip()
        ]
        latest_date = max(latest_dates) if latest_dates else latest_trade_text
        is_fresh = bool(latest_date and latest_date[:10] == latest_trade_text[:10])
        if positive_bits and negative_bits:
            status = "⚠️"
            detail = f"{stock_name} 打板情绪信号分化：{'；'.join(positive_bits[:2])}，但同时 {'；'.join(negative_bits[:2])}。"
        elif negative_bits:
            status = "⚠️"
            detail = f"{stock_name} 打板/情绪风险偏高：{'；'.join(negative_bits[:3])}。"
        elif positive_bits:
            status = "✅"
            detail = f"{stock_name} 打板/微观结构偏正面：{'；'.join(positive_bits[:3])}。"
        elif any(component.get("status") == "matched" for component in components.values()):
            status = "ℹ️"
            detail = f"{stock_name} 已接入龙虎榜/竞价/涨跌停边界，但当前未命中明确打板专题信号。"
        elif any(str(component.get("diagnosis", "")) in {"permission_blocked", "rate_limited", "network_error", "fetch_error", "unavailable"} for component in components.values()):
            status = "ℹ️"
            detail = "打板专题接口当前不可用，本轮不把缺口写成没有龙虎榜或情绪风险。"
        else:
            status = "ℹ️"
            detail = "当前未命中可稳定使用的打板专题信号。"

        return {
            "source": "tushare.top_list+tushare.top_inst+tushare.stk_auction+tushare.stk_limit+tushare.limit_list_d",
            "as_of": as_of_text,
            "latest_date": latest_date,
            "fallback": "none",
            "status": status,
            "is_fresh": is_fresh,
            "detail": detail,
            "disclosure": "；".join(dict.fromkeys(disclosure_lines)),
            "has_positive_signal": bool(positive_bits),
            "has_negative_signal": bool(negative_bits),
            "positive_bits": positive_bits,
            "negative_bits": negative_bits,
            "lhb_reason": lhb_reason,
            "lhb_net_amount": lhb_net_amount,
            "inst_net_amount": inst_net_amount,
            "auction_gap_pct": auction_gap_pct,
            "auction_volume_ratio": auction_volume_ratio,
            "auction_turnover_rate": auction_turnover_rate,
            "up_limit": up_limit,
            "down_limit": down_limit,
            "up_limit_gap_pct": up_limit_gap_pct,
            "down_limit_gap_pct": down_limit_gap_pct,
            "in_zt_pool": zt_hit,
            "in_strong_pool": strong_hit,
            "in_dt_pool": dt_hit,
            "limit_times": limit_times,
            "components": components,
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
        today = reference_date.strftime("%Y%m%d")
        for trade_date in reversed(self._recent_open_trade_dates(lookback_days=14)):
            cache_key = f"market_pulse:ts_limit_pool:{label}:{trade_date}:v1"
            use_intraday_cache = trade_date != today
            if use_intraday_cache:
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
                if use_intraday_cache:
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

        today = reference_date.strftime("%Y%m%d")
        for offset in range(0, 8):
            target = (reference_date - timedelta(days=offset)).strftime("%Y%m%d")
            cache_key = f"market_pulse:{func_name}:{target}"
            try:
                frame = self.cached_call(
                    cache_key,
                    self._quiet_fetch,
                    fetcher,
                    ttl_hours=2,
                    use_cache=(target != today),
                    prefer_stale=False,
                    allow_stale_on_error=False,
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

    def _pool_report(self, info: Dict[str, Any], reference_date: datetime, *, source: str = "") -> Dict[str, Any]:
        frame = info.get("frame", pd.DataFrame())
        latest_date = self._normalize_date_text(info.get("date"))
        return {
            "frame": frame.reset_index(drop=True) if isinstance(frame, pd.DataFrame) else pd.DataFrame(),
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, reference_date, max_age_days=0),
            "as_of": reference_date.strftime("%Y-%m-%d %H:%M:%S"),
            "source": source,
        }

    def _is_fresh(self, date_text: str, reference_date: datetime, max_age_days: int = 7) -> bool:
        if not date_text:
            return False
        normalized = str(date_text).replace("/", "-")
        try:
            target = datetime.strptime(normalized[:10], "%Y-%m-%d")
        except ValueError:
            return False
        return abs((reference_date.date() - target.date()).days) <= max_age_days
