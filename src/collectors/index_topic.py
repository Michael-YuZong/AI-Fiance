"""Shared Tushare-first index-topic collector for mature research pipelines."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from .base import BaseCollector


GENERIC_INDEX_KEYWORDS = {
    "科技",
    "成长",
    "价值",
    "红利",
    "消费",
    "医药",
    "金融",
    "地产",
    "周期",
    "制造",
    "材料",
}

BROAD_BENCHMARK_KEYWORDS = {
    "中证a500",
    "a500",
    "沪深300",
    "中证500",
    "中证800",
    "中证1000",
    "中证2000",
    "上证50",
    "中证a50",
    "中证a100",
    "中证全指",
    "创业板",
    "科创50",
}

INDEX_BASIC_MARKETS = ("CSI", "SSE", "SZSE", "CICC", "SW", "OTH")
MAX_EXACT_SNAPSHOT_LOOKUPS = 1
MAX_PROXY_SNAPSHOT_LOOKUPS = 3

GLOBAL_INDEX_SYMBOL_MAP = {
    "^GSPC": "SPX",
    "^IXIC": "IXIC",
    "^DJI": "DJI",
    "^GDAXI": "GDAXI",
    "^FTSE": "FTSE",
    "^N225": "N225",
    "^HSI": "HSI",
    "3033.HK": "HKTECH",
    "HKTECH": "HKTECH",
    "SPX": "SPX",
    "IXIC": "IXIC",
    "DJI": "DJI",
    "GDAXI": "GDAXI",
    "FTSE": "FTSE",
    "N225": "N225",
    "HSI": "HSI",
    "XIN9": "XIN9",
}


def _normalize_index_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    for token in ("收益率", "价格指数", "指数", " ", "\t", "\n", "*", "×", "+", "/", "-", "（", "）", "(", ")", "·"):
        text = text.replace(token, "")
    return text


def _keyword_specificity(value: Any) -> int:
    normalized = _normalize_index_label(value)
    if not normalized:
        return 0
    return 1 if normalized in GENERIC_INDEX_KEYWORDS else len(normalized)


def _is_broad_benchmark_keyword(value: Any) -> bool:
    normalized = _normalize_index_label(value)
    if not normalized:
        return False
    return any(token in normalized for token in BROAD_BENCHMARK_KEYWORDS)


def _signal_strength(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "中"
    if float(numeric) >= 3:
        return "高"
    if float(numeric) >= 1:
        return "中"
    return "低"


class IndexTopicCollector(BaseCollector):
    """Shared index/industry topic collector for Tushare index-topic mainline."""

    _INDEX_BASIC_FRAME: pd.DataFrame | None = None

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="IndexTopicCollector")

    def get_cn_index_snapshot(
        self,
        keywords: Sequence[str],
        *,
        reference_date: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve index metadata + latest daily-basic snapshot by keyword heuristics."""
        cleaned = [str(item).strip() for item in keywords if str(item).strip()]
        if not cleaned:
            return None
        as_of = reference_date or datetime.now()
        frame = self._index_basic_frame()
        if frame.empty:
            return None

        explicit_code_entries: List[tuple[str, str]] = []
        for item in cleaned:
            normalized_code = self._normalize_index_code(item)
            if normalized_code and "." in normalized_code:
                explicit_code_entries.append((str(item).strip(), normalized_code))
        for raw_code, normalized_code in explicit_code_entries:
            matched = frame[frame["ts_code"].astype(str).str.strip() == normalized_code]
            if matched.empty:
                continue
            snapshot = self._snapshot_from_basic_row(
                matched.iloc[0],
                matched_keywords=[raw_code],
                as_of=as_of,
                exact_match=True,
                match_quality="exact_code",
                display_label="真实指数估值",
                match_note="指数主链已按指数代码直接命中，不再退回主题关键词代理。",
            )
            snapshot["explicit_code_match"] = True
            return snapshot

        normalized_keywords = [
            (_normalize_index_label(item), index, _keyword_specificity(item), item)
            for index, item in enumerate(cleaned)
            if item not in {raw for raw, _ in explicit_code_entries}
        ]
        ranked_exact: list[tuple[tuple[int, int, int, int, int], Dict[str, Any]]] = []
        ranked_proxy: list[tuple[tuple[int, int, int, int, int], Dict[str, Any]]] = []

        for _, row in frame.iterrows():
            candidate = self._index_candidate_from_basic_row(row, normalized_keywords)
            if not candidate:
                continue
            rank_key = candidate.pop("_rank_key")
            if bool(candidate.get("exact_match")):
                ranked_exact.append((rank_key, candidate))
            else:
                ranked_proxy.append((rank_key, candidate))

        if ranked_exact:
            ranked_exact.sort(key=lambda item: item[0])
            selected = self._best_snapshot_candidate(
                [candidate for _, candidate in ranked_exact],
                as_of=as_of,
                max_lookups=MAX_EXACT_SNAPSHOT_LOOKUPS,
            )
            if selected:
                if selected.get("pe_ttm") is None:
                    selected["match_quality"] = "exact_no_pe"
                    selected["display_label"] = "真实指数估值"
                    proxy_candidate = self._best_snapshot_candidate(
                        [candidate for _, candidate in sorted(ranked_proxy, key=lambda item: item[0])],
                        as_of=as_of,
                        max_lookups=MAX_PROXY_SNAPSHOT_LOOKUPS,
                        require_pe=True,
                    )
                    if proxy_candidate:
                        selected["fallback_proxy_name"] = proxy_candidate["index_name"]
                        selected["match_note"] = (
                            f"指数主链已命中 `{selected['index_name']}`，但当前缺少可用滚动PE；"
                            "为避免错配，不再回退到其他主题指数代理。"
                        )
                    else:
                        selected["match_note"] = "指数主链已命中基准指数，但当前缺少可用滚动PE。"
                    return selected
                selected["match_note"] = "指数主链已匹配到与目标基准高度一致的指数名称。"
                return selected

        if explicit_code_entries:
            preferred_label = max(
                [
                    item
                    for item in cleaned
                    if item not in {raw for raw, _ in explicit_code_entries}
                    and _keyword_specificity(item) > 1
                ],
                key=_keyword_specificity,
                default="",
            )
            if preferred_label:
                normalized_code = explicit_code_entries[0][1]
                return {
                    "index_code": normalized_code,
                    "index_name": preferred_label,
                    "pe_ttm": None,
                    "pb": None,
                    "turnover_rate": None,
                    "matched_keywords": [preferred_label, explicit_code_entries[0][0]],
                    "match_quality": "explicit_code_unmatched",
                    "display_label": "真实指数框架",
                    "match_note": (
                        f"指数主链已命中 `{normalized_code}` 这类明确指数代码锚点，"
                        "但当前 index_basic 主表未收录；为避免错配，不再回退到其他主题指数代理。"
                    ),
                    "source": "tushare.index_basic+tushare.index_dailybasic",
                    "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                    "latest_date": "",
                    "is_fresh": False,
                    "fallback": "none",
                    "disclosure": (
                        "指数估值主链优先走 Tushare index_basic + index_dailybasic；"
                        "命中明确指数代码但主表未收录时，只保留真实指数框架名，不伪造 fresh 或错误代理。"
                    ),
                }

        broad_keywords = [item for item in cleaned if _is_broad_benchmark_keyword(item)]
        if broad_keywords:
            benchmark_label = max(broad_keywords, key=_keyword_specificity)
            return {
                "index_code": "",
                "index_name": benchmark_label,
                "pe_ttm": None,
                "pb": None,
                "turnover_rate": None,
                "matched_keywords": [benchmark_label],
                "match_quality": "benchmark_no_proxy",
                "display_label": "真实指数估值",
                "match_note": (
                    f"指数主链未直接命中 `{benchmark_label}`；"
                    "为避免错配，不再回退到其他主题指数代理。"
                ),
                "source": "tushare.index_basic+tushare.index_dailybasic",
                "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_date": "",
                "is_fresh": False,
                "fallback": "none",
                "disclosure": (
                    "指数估值主链优先走 Tushare index_basic + index_dailybasic；"
                    "命不中或缺少PE时不伪造 fresh 命中，也不回退到不相干主题指数。"
                ),
            }
        if ranked_proxy:
            ranked_proxy.sort(key=lambda item: item[0])
            selected = self._best_snapshot_candidate(
                [candidate for _, candidate in ranked_proxy],
                as_of=as_of,
                max_lookups=MAX_PROXY_SNAPSHOT_LOOKUPS,
                require_pe=True,
            )
            if selected:
                selected["match_note"] = "指数主链未直接命中精确基准，当前使用最接近的主题指数代理。"
                return selected
        return None

    def get_cn_index_value_history(self, index_code: str, *, lookback_days: int = 365 * 8) -> pd.DataFrame:
        """Return index daily-basic history for valuation percentile calculation."""
        ts_code = self._normalize_index_code(index_code)
        if not ts_code:
            return pd.DataFrame()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=max(int(lookback_days), 30))).strftime("%Y%m%d")
        cache_key = f"index_topic:index_dailybasic_history:{ts_code}:{start_date}:{end_date}:v1"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        raw = self._ts_call("index_dailybasic", ts_code=ts_code, start_date=start_date, end_date=end_date)
        if raw is None or raw.empty:
            return pd.DataFrame()
        frame = pd.DataFrame(
            {
                "日期": raw["trade_date"].map(self._normalize_date_text),
                "PE滚动": pd.to_numeric(raw.get("pe_ttm", raw.get("pe")), errors="coerce"),
                "市净率": pd.to_numeric(raw.get("pb"), errors="coerce"),
                "换手率": pd.to_numeric(raw.get("turnover_rate"), errors="coerce"),
            }
        )
        frame = frame.dropna(how="all", subset=["PE滚动", "市净率", "换手率"]).sort_values("日期").reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return frame

    def get_index_history_snapshot(
        self,
        index_code: str,
        *,
        period: str = "weekly",
        reference_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Return a compact summary for index daily/weekly/monthly history."""
        as_of = reference_date or datetime.now()
        cleaned_period = str(period or "weekly").strip().lower()
        api_name = {
            "daily": "index_daily",
            "weekly": "index_weekly",
            "monthly": "index_monthly",
        }.get(cleaned_period, "index_weekly")
        lookback_days = {
            "index_daily": 90,
            "index_weekly": 365 * 3,
            "index_monthly": 365 * 8,
        }.get(api_name, 365 * 3)
        ts_code = self._normalize_index_code(index_code)
        if not ts_code:
            return {
                "period": cleaned_period,
                "source": f"tushare.{api_name}",
                "source_label": {
                    "index_daily": "Tushare 指数日线",
                    "index_weekly": "Tushare 指数周线",
                    "index_monthly": "Tushare 指数月线",
                }.get(api_name, "Tushare 指数行情"),
                "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_date": "",
                "is_fresh": False,
                "fallback": "none",
                "status": "empty",
                "trend_label": "缺失",
                "momentum_label": "缺失",
                "signal_strength": "低",
                "summary": "历史行情缺失，先不把趋势写死。",
                "disclosure": "指数历史行情优先走 Tushare 指数专题主链；缺失时不把旧链伪装成 fresh。",
            }
        frame = self.get_index_history(ts_code, period=cleaned_period, start_date=(as_of - timedelta(days=lookback_days)).strftime("%Y%m%d"), end_date=as_of.strftime("%Y%m%d"))
        if frame.empty:
            return {
                "period": cleaned_period,
                "source": f"tushare.{api_name}",
                "source_label": {
                    "index_daily": "Tushare 指数日线",
                    "index_weekly": "Tushare 指数周线",
                    "index_monthly": "Tushare 指数月线",
                }.get(api_name, "Tushare 指数行情"),
                "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_date": "",
                "is_fresh": False,
                "fallback": "none",
                "status": "empty",
                "trend_label": "缺失",
                "momentum_label": "缺失",
                "signal_strength": "低",
                "summary": "历史行情缺失，先不把趋势写死。",
                "disclosure": "指数历史行情优先走 Tushare 指数专题主链；缺失时不把旧链伪装成 fresh。",
            }
        working = frame.copy()
        if "日期" in working.columns:
            working = working.sort_values("日期").reset_index(drop=True)
        latest_row = working.iloc[-1]
        prev_row = working.iloc[-2] if len(working) > 1 else latest_row
        first_row = working.iloc[0]
        latest_close = pd.to_numeric(pd.Series([latest_row.get("收盘")]), errors="coerce").iloc[0]
        prev_close = pd.to_numeric(pd.Series([prev_row.get("收盘")]), errors="coerce").iloc[0]
        first_close = pd.to_numeric(pd.Series([first_row.get("收盘")]), errors="coerce").iloc[0]
        window_change_pct = None
        recent_change_pct = None
        if pd.notna(latest_close) and pd.notna(first_close) and float(first_close):
            window_change_pct = float(latest_close) / float(first_close) - 1.0
        if pd.notna(latest_close) and pd.notna(prev_close) and float(prev_close):
            recent_change_pct = float(latest_close) / float(prev_close) - 1.0

        trend_label = "震荡"
        if window_change_pct is not None:
            if window_change_pct >= 0.08 and (recent_change_pct is None or recent_change_pct >= 0):
                trend_label = "趋势偏强"
            elif window_change_pct <= -0.08 and (recent_change_pct is None or recent_change_pct <= 0):
                trend_label = "趋势偏弱"
            elif abs(window_change_pct) < 0.02:
                trend_label = "震荡"
            elif window_change_pct > 0:
                trend_label = "修复中"
            else:
                trend_label = "承压震荡"

        momentum_label = "动能中性"
        if recent_change_pct is not None:
            if recent_change_pct >= 0.02:
                momentum_label = "动能偏强"
            elif recent_change_pct >= 0.005:
                momentum_label = "动能改善"
            elif recent_change_pct <= -0.02:
                momentum_label = "动能偏弱"
            elif recent_change_pct < 0:
                momentum_label = "动能承压"

        summary_parts: List[str] = []
        unit_label = {"index_daily": "日", "index_weekly": "周", "index_monthly": "月"}.get(api_name, "期")
        if window_change_pct is not None:
            summary_parts.append(f"近 {len(working)}{unit_label} {window_change_pct:+.2%}")
        if recent_change_pct is not None:
            summary_parts.append(f"最近一{unit_label} {recent_change_pct:+.2%}")
        summary_parts.append(trend_label)
        if momentum_label and momentum_label != "动能中性":
            summary_parts.append(momentum_label)
        return {
            "period": cleaned_period,
            "source": f"tushare.{api_name}",
            "source_label": {
                "index_daily": "Tushare 指数日线",
                "index_weekly": "Tushare 指数周线",
                "index_monthly": "Tushare 指数月线",
            }.get(api_name, "Tushare 指数行情"),
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "latest_date": self._normalize_date_text(latest_row.get("日期")),
            "is_fresh": bool(working.attrs.get("history_is_fresh", self._is_fresh(latest_row.get("日期"), as_of, max_age_days=4 if api_name == "index_daily" else 10))),
            "fallback": "none",
            "status": "matched",
            "latest_close": None if pd.isna(latest_close) else float(latest_close),
            "window_change_pct": window_change_pct,
            "recent_change_pct": recent_change_pct,
            "trend_label": trend_label,
            "momentum_label": momentum_label,
            "signal_strength": _signal_strength(abs((window_change_pct or 0.0) * 100.0)),
            "summary": "，".join(summary_parts),
            "disclosure": "指数历史行情优先走 Tushare 指数专题主链；缺失时不把旧链伪装成 fresh。",
        }

    def get_cn_index_constituent_weights(
        self,
        index_code: str,
        *,
        top_n: int = 10,
        reference_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Return latest index constituent weights from Tushare index_weight."""
        as_of = reference_date or datetime.now()
        ts_code = self._normalize_index_code(index_code)
        if not ts_code:
            return pd.DataFrame()
        cache_key = f"index_topic:index_weight:{ts_code}:v1"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            frame = cached.copy()
            self._attach_frame_contract(
                frame,
                as_of=as_of,
                latest_date=str(frame.attrs.get("latest_date", "")),
                source="tushare.index_weight",
                fallback="none",
                disclosure="指数成分和权重来自 Tushare index_weight；为空或受限时按缺失处理，不伪装成 fresh。",
            )
            return frame

        raw = self._ts_call("index_weight", index_code=ts_code)
        if raw is None or raw.empty:
            return pd.DataFrame()
        latest_date = str(raw.get("trade_date", pd.Series("", index=raw.index)).max() or "")
        if latest_date:
            raw = raw[raw["trade_date"].astype(str) == latest_date]
        frame = pd.DataFrame(
            {
                "symbol": raw["con_code"].apply(self._from_ts_code),
                "name": raw.get("con_name", raw["con_code"].apply(self._from_ts_code)),
                "weight": pd.to_numeric(raw["weight"], errors="coerce"),
            }
        )
        frame = frame.dropna(subset=["weight"]).sort_values("weight", ascending=False)
        if top_n > 0:
            frame = frame.head(top_n)
        frame = frame.reset_index(drop=True)
        frame.attrs["latest_date"] = self._normalize_date_text(latest_date)
        self._attach_frame_contract(
            frame,
            as_of=as_of,
            latest_date=self._normalize_date_text(latest_date),
            source="tushare.index_weight",
            fallback="none",
            disclosure="指数成分和权重来自 Tushare index_weight；为空或受限时按缺失处理，不伪装成 fresh。",
        )
        self._save_cache(cache_key, frame)
        return frame

    def get_index_history(
        self,
        symbol: str,
        *,
        period: str = "daily",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """Return normalized CN index history from Tushare daily/weekly/monthly APIs."""
        cleaned = self._clean_cn_index_symbol(symbol)
        if not cleaned:
            return pd.DataFrame()
        start = str(start_date or (datetime.now() - timedelta(days=365 * 3)).strftime("%Y%m%d")).replace("-", "")
        end = str(end_date or datetime.now().strftime("%Y%m%d")).replace("-", "")
        api_name = {
            "daily": "index_daily",
            "weekly": "index_weekly",
            "monthly": "index_monthly",
        }.get(str(period).strip().lower(), "index_daily")
        for ts_code in self._ts_index_code_candidates(cleaned):
            cache_key = f"index_topic:{api_name}:{ts_code}:{start}:{end}:v1"
            cached = self._load_cache(cache_key)
            if cached is not None:
                return self._with_history_contract(cached.copy(), api_name)
            raw = self._ts_call(api_name, ts_code=ts_code, start_date=start, end_date=end)
            if raw is None or raw.empty:
                continue
            frame = raw.rename(
                columns={
                    "trade_date": "日期",
                    "open": "开盘",
                    "high": "最高",
                    "low": "最低",
                    "close": "收盘",
                    "vol": "成交量",
                    "amount": "成交额",
                }
            )
            frame["日期"] = pd.to_datetime(frame["日期"], format="%Y%m%d")
            if "成交额" in frame.columns:
                frame["成交额"] = pd.to_numeric(frame["成交额"], errors="coerce") * 1000.0
            frame = frame.sort_values("日期").reset_index(drop=True)
            self._save_cache(cache_key, frame)
            return self._with_history_contract(frame, api_name)
        return pd.DataFrame()

    def get_domestic_overview_rows(
        self,
        indices: Sequence[Mapping[str, Any]],
        *,
        reference_date: Optional[datetime] = None,
        period: str = "daily",
    ) -> List[Dict[str, Any]]:
        """Return latest domestic index snapshot rows for briefing overview."""
        return self.get_index_period_overview_rows(indices, reference_date=reference_date, period=period)

    def get_index_period_overview_rows(
        self,
        indices: Sequence[Mapping[str, Any]],
        *,
        reference_date: Optional[datetime] = None,
        period: str = "daily",
    ) -> List[Dict[str, Any]]:
        """Return latest domestic index snapshot rows for a given history period."""
        as_of = reference_date or datetime.now()
        normalized_period = str(period or "daily").strip().lower()
        period_days = {
            "daily": 10,
            "weekly": 365 * 3,
            "monthly": 365 * 5,
        }.get(normalized_period, 10)
        rows: List[Dict[str, Any]] = []
        for item in indices:
            symbol = self._clean_cn_index_symbol(item.get("symbol", ""))
            if not symbol:
                continue
            history_snapshots: Dict[str, Dict[str, Any]] = {}
            if normalized_period == "daily":
                for extra_period in ("weekly", "monthly"):
                    history_snapshots[extra_period] = self.get_index_history_snapshot(
                        symbol,
                        period=extra_period,
                        reference_date=as_of,
                    )
            frame = self.get_index_history(
                symbol,
                period=normalized_period,
                start_date=(as_of - timedelta(days=period_days)).strftime("%Y%m%d"),
                end_date=as_of.strftime("%Y%m%d"),
            )
            if frame.empty:
                continue
            latest_row = frame.iloc[-1]
            prev_row = frame.iloc[-2] if len(frame) > 1 else latest_row
            latest = pd.to_numeric(pd.Series([latest_row.get("收盘")]), errors="coerce").iloc[0]
            prev_close = pd.to_numeric(pd.Series([prev_row.get("收盘")]), errors="coerce").iloc[0]
            amount = pd.to_numeric(pd.Series([latest_row.get("成交额")]), errors="coerce").iloc[0]
            change_pct = (float(latest) / float(prev_close) - 1.0) if pd.notna(latest) and pd.notna(prev_close) and float(prev_close) else None
            rows.append(
                {
                    "name": str(item.get("name", symbol)).strip() or symbol,
                    "symbol": symbol,
                    "latest": None if pd.isna(latest) else float(latest),
                    "change_pct": change_pct,
                    "amount": None if pd.isna(amount) else float(amount) / 1e8,
                    "amount_delta": None,
                    "open": pd.to_numeric(pd.Series([latest_row.get("开盘")]), errors="coerce").iloc[0],
                    "prev_close": None if pd.isna(prev_close) else float(prev_close),
                    "proxy_note": str(item.get("proxy_note", "")).strip(),
                    "source": f"tushare.{str(frame.attrs.get('history_api', f'index_{normalized_period}')).strip()}",
                    "as_of": frame.attrs.get("history_as_of", ""),
                    "is_fresh": bool(frame.attrs.get("history_is_fresh", False)),
                    "fallback": str(frame.attrs.get("history_fallback", "none")),
                    "disclosure": "国内指数概览优先走 Tushare 指数历史主链，缺失时不把旧行情伪装成 fresh。",
                    "weekly_summary": str(history_snapshots.get("weekly", {}).get("summary", "")).strip(),
                    "weekly_trend_label": str(history_snapshots.get("weekly", {}).get("trend_label", "")).strip(),
                    "weekly_momentum_label": str(history_snapshots.get("weekly", {}).get("momentum_label", "")).strip(),
                    "weekly_source_label": str(history_snapshots.get("weekly", {}).get("source_label", "")).strip(),
                    "weekly_latest_date": str(history_snapshots.get("weekly", {}).get("latest_date", "")).strip(),
                    "weekly_is_fresh": bool(history_snapshots.get("weekly", {}).get("is_fresh", False)),
                    "monthly_summary": str(history_snapshots.get("monthly", {}).get("summary", "")).strip(),
                    "monthly_trend_label": str(history_snapshots.get("monthly", {}).get("trend_label", "")).strip(),
                    "monthly_momentum_label": str(history_snapshots.get("monthly", {}).get("momentum_label", "")).strip(),
                    "monthly_source_label": str(history_snapshots.get("monthly", {}).get("source_label", "")).strip(),
                    "monthly_latest_date": str(history_snapshots.get("monthly", {}).get("latest_date", "")).strip(),
                    "monthly_is_fresh": bool(history_snapshots.get("monthly", {}).get("is_fresh", False)),
                }
            )
        return rows

    def get_global_overview_rows(
        self,
        indices: Sequence[Mapping[str, Any]],
        *,
        reference_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Return latest global index snapshot rows from Tushare index_global."""
        as_of = reference_date or datetime.now()
        end_date = as_of.strftime("%Y%m%d")
        start_date = (as_of - timedelta(days=21)).strftime("%Y%m%d")
        rows: List[Dict[str, Any]] = []
        for item in indices:
            raw_symbol = str(item.get("symbol", "")).strip()
            ts_code = GLOBAL_INDEX_SYMBOL_MAP.get(raw_symbol.upper(), GLOBAL_INDEX_SYMBOL_MAP.get(raw_symbol, ""))
            if not ts_code:
                continue
            cache_key = f"index_topic:index_global:{ts_code}:{start_date}:{end_date}:v1"
            cached = self._load_cache(cache_key, ttl_hours=6)
            frame = cached
            if frame is None:
                frame = self._ts_call("index_global", ts_code=ts_code, start_date=start_date, end_date=end_date)
                if frame is not None and not getattr(frame, "empty", False):
                    self._save_cache(cache_key, frame)
            if frame is None or frame.empty:
                continue
            working = frame.sort_values("trade_date")
            latest_row = working.iloc[-1]
            latest = pd.to_numeric(pd.Series([latest_row.get("close")]), errors="coerce").iloc[0]
            pct_chg = pd.to_numeric(pd.Series([latest_row.get("pct_chg")]), errors="coerce").iloc[0]
            rows.append(
                {
                    "market": str(item.get("market", "")),
                    "name": str(item.get("name", raw_symbol)).strip() or raw_symbol,
                    "symbol": raw_symbol,
                    "ts_code": ts_code,
                    "latest": None if pd.isna(latest) else float(latest),
                    "change_pct": None if pd.isna(pct_chg) else float(pct_chg) / 100.0,
                    "proxy_note": str(item.get("proxy_note", "")).strip(),
                    "source": "tushare.index_global",
                    "as_of": self._normalize_date_text(latest_row.get("trade_date")),
                    "latest_date": self._normalize_date_text(latest_row.get("trade_date")),
                    "is_fresh": self._is_fresh(str(latest_row.get("trade_date", "")), as_of, max_age_days=4),
                    "fallback": "none",
                    "disclosure": "国际主要指数来自 Tushare index_global；缺失或延迟时按缺失处理，不再写成 yfinance 主链。",
                }
            )
        return rows

    def get_index_technical_snapshot(
        self,
        index_code: str,
        *,
        reference_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Return latest index technical-factor snapshot from idx_factor_pro."""
        as_of = reference_date or datetime.now()
        ts_code = self._normalize_index_code(index_code)
        if not ts_code:
            return {}
        try:
            row = self._latest_idx_factor_row(ts_code, as_of=as_of)
        except Exception as exc:  # noqa: BLE001
            diagnosis = self._tushare_failure_diagnosis(exc)
            return {
                "index_code": ts_code,
                "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_date": "",
                "is_fresh": False,
                "source": "tushare.idx_factor_pro",
                "fallback": "none",
                "diagnosis": diagnosis,
                "status": "blocked",
                "disclosure": self._blocked_disclosure(diagnosis, source="Tushare idx_factor_pro"),
            }
        if row is None:
            return {
                "index_code": ts_code,
                "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_date": "",
                "is_fresh": False,
                "source": "tushare.idx_factor_pro",
                "fallback": "none",
                "diagnosis": "empty",
                "status": "empty",
                "disclosure": "指数技术面因子来自 Tushare idx_factor_pro；空表时按缺失处理，不伪装成 fresh。",
            }
        close = pd.to_numeric(pd.Series([row.get("close")]), errors="coerce").iloc[0]
        ma20 = pd.to_numeric(pd.Series([row.get("ma_bfq_20")]), errors="coerce").iloc[0]
        ma60 = pd.to_numeric(pd.Series([row.get("ma_bfq_60")]), errors="coerce").iloc[0]
        macd = pd.to_numeric(pd.Series([row.get("macd_bfq")]), errors="coerce").iloc[0]
        rsi6 = pd.to_numeric(pd.Series([row.get("rsi_bfq_6")]), errors="coerce").iloc[0]
        pct_change = pd.to_numeric(pd.Series([row.get("pct_change")]), errors="coerce").iloc[0]

        trend_label = "震荡"
        if pd.notna(close) and pd.notna(ma20) and pd.notna(ma60):
            if float(close) >= float(ma20) >= float(ma60):
                trend_label = "趋势偏强"
            elif float(close) >= float(ma20):
                trend_label = "修复中"
            elif float(close) < float(ma20) < float(ma60):
                trend_label = "趋势偏弱"
            else:
                trend_label = "承压震荡"
        momentum_label = "动能中性"
        if pd.notna(macd) and pd.notna(rsi6):
            if float(macd) > 0 and float(rsi6) >= 60:
                momentum_label = "动能偏强"
            elif float(macd) > 0 or float(rsi6) >= 55:
                momentum_label = "动能改善"
            elif float(macd) < 0 and float(rsi6) <= 45:
                momentum_label = "动能偏弱"
        detail_parts: List[str] = []
        if pd.notna(close) and pd.notna(ma20):
            detail_parts.append(f"收盘 {float(close):.2f} / MA20 {float(ma20):.2f}")
        if pd.notna(ma60):
            detail_parts.append(f"MA60 {float(ma60):.2f}")
        if pd.notna(macd):
            detail_parts.append(f"MACD {float(macd):+.2f}")
        if pd.notna(rsi6):
            detail_parts.append(f"RSI6 {float(rsi6):.1f}")
        return {
            "index_code": ts_code,
            "trade_date": self._normalize_date_text(row.get("trade_date")),
            "latest_date": self._normalize_date_text(row.get("trade_date")),
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "is_fresh": self._is_fresh(str(row.get("trade_date", "")), as_of, max_age_days=3),
            "source": "tushare.idx_factor_pro",
            "fallback": "none",
            "diagnosis": "live",
            "status": "matched",
            "pct_change": None if pd.isna(pct_change) else float(pct_change),
            "trend_label": trend_label,
            "momentum_label": momentum_label,
            "signal_strength": _signal_strength(pct_change),
            "detail": " / ".join(detail_parts),
            "disclosure": "指数技术面因子来自 Tushare idx_factor_pro；权限失败、空表或频控时按缺失处理，不伪装成 fresh。",
        }

    def get_index_bundle(
        self,
        *,
        index_code: str = "",
        keywords: Sequence[str] = (),
        top_n: int = 5,
        reference_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Return combined index snapshot + technicals + top weights."""
        as_of = reference_date or datetime.now()
        snapshot = self.get_cn_index_snapshot([*(keywords or []), index_code], reference_date=as_of) or {}
        resolved_code = self._normalize_index_code(index_code or snapshot.get("index_code", ""))
        technical = self.get_index_technical_snapshot(resolved_code, reference_date=as_of) if resolved_code else {}
        weights = self.get_cn_index_constituent_weights(resolved_code, top_n=top_n, reference_date=as_of) if resolved_code else pd.DataFrame()
        history_snapshots = {
            period: self.get_index_history_snapshot(resolved_code, period=period, reference_date=as_of)
            for period in ("weekly", "monthly")
        } if resolved_code else {}
        history_is_fresh = any(bool(item.get("is_fresh")) for item in history_snapshots.values())
        return {
            "index_snapshot": snapshot,
            "technical_snapshot": technical,
            "constituent_weights": weights,
            "history_snapshots": history_snapshots,
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "source": "tushare.index_basic+tushare.index_dailybasic+tushare.idx_factor_pro+tushare.index_weight+tushare.index_weekly+tushare.index_monthly",
            "fallback": "none",
            "is_fresh": bool(snapshot.get("is_fresh")) or bool(technical.get("is_fresh")) or history_is_fresh,
            "disclosure": (
                "指数专题主链优先走 Tushare index_basic / index_dailybasic / idx_factor_pro / index_weight / index_weekly / index_monthly；"
                "任一子接口为空或受限时只按缺失处理，不伪造 fresh。"
            ),
        }

    def _snapshot_from_basic_row(
        self,
        row: Mapping[str, Any],
        *,
        matched_keywords: Sequence[str],
        as_of: datetime,
        exact_match: bool,
        match_quality: str,
        display_label: str,
        match_note: str,
    ) -> Dict[str, Any]:
        code = self._normalize_index_code(row.get("ts_code", ""))
        dailybasic, dailybasic_diagnosis = self._latest_index_dailybasic_snapshot(code, as_of=as_of)
        pe_ttm = dailybasic.get("pe_ttm")
        if pe_ttm is None:
            pe_ttm = dailybasic.get("pe")
        fallback = "none"
        is_fresh = bool(dailybasic.get("is_fresh", False)) if dailybasic else False
        latest_date = str(dailybasic.get("trade_date", "")) if dailybasic else ""
        if dailybasic_diagnosis != "live":
            fallback = "index_basic_only"
        return {
            "index_code": code,
            "index_name": str(row.get("name", "")).strip(),
            "fullname": str(row.get("fullname", "")).strip(),
            "market": str(row.get("market", "")).strip(),
            "publisher": str(row.get("publisher", "")).strip(),
            "category": str(row.get("category", "")).strip(),
            "index_type": str(row.get("index_type", "")).strip(),
            "list_date": self._normalize_date_text(row.get("list_date")),
            "matched_keywords": list(matched_keywords),
            "match_quality": match_quality,
            "display_label": display_label,
            "exact_match": exact_match,
            "pe_ttm": None if pe_ttm is None else float(pe_ttm),
            "pb": dailybasic.get("pb"),
            "turnover_rate": dailybasic.get("turnover_rate"),
            "total_mv": dailybasic.get("total_mv"),
            "float_mv": dailybasic.get("float_mv"),
            "latest_date": latest_date,
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "is_fresh": is_fresh,
            "source": "tushare.index_basic+tushare.index_dailybasic",
            "fallback": fallback,
            "match_note": match_note,
            "disclosure": (
                "指数估值主链优先走 Tushare index_basic + index_dailybasic；"
                "权限失败、空表或频控时不伪造 fresh，也不把不相干主题指数当成主链。"
            ),
        }

    def _best_snapshot_candidate(
        self,
        candidates: Sequence[Mapping[str, Any]],
        *,
        as_of: datetime,
        max_lookups: int,
        require_pe: bool = False,
    ) -> Dict[str, Any]:
        lookups = 0
        for candidate in candidates:
            if lookups >= max(int(max_lookups), 1):
                break
            lookups += 1
            snapshot = self._snapshot_from_basic_row(
                candidate,
                matched_keywords=candidate.get("matched_keywords", ()),
                as_of=as_of,
                exact_match=bool(candidate.get("exact_match")),
                match_quality=str(candidate.get("match_quality", "")),
                display_label=str(candidate.get("display_label", "")),
                match_note=str(candidate.get("match_note", "")),
            )
            if require_pe and snapshot.get("pe_ttm") is None:
                continue
            return snapshot
        return {}

    def _index_candidate_from_basic_row(
        self,
        row: Mapping[str, Any],
        normalized_keywords: Sequence[tuple[str, int, int, str]],
    ) -> Dict[str, Any]:
        name = str(row.get("name", "")).strip()
        fullname = str(row.get("fullname", "")).strip()
        code = self._normalize_index_code(row.get("ts_code", ""))
        if not name or not code:
            return {}
        names = [name, fullname]
        lowered_names = [item.lower() for item in names if item]
        normalized_names = [_normalize_index_label(item) for item in names if item]
        matched_keywords = [
            raw
            for normalized, _, _, raw in normalized_keywords
            if normalized
            and any(
                normalized == normalized_name
                or normalized in normalized_name
                or normalized_name in normalized
                for normalized_name in normalized_names
            )
            or any(raw.lower() in lowered for lowered in lowered_names)
        ]
        if not matched_keywords:
            return {}

        exact_match = any(
            _normalize_index_label(keyword) in {normalized for normalized in normalized_names if normalized}
            for keyword in matched_keywords
        )
        specifics = [_keyword_specificity(keyword) for keyword in matched_keywords]
        best_specificity = max(specifics) if specifics else 0
        total_specificity = sum(specifics)
        keyword_index = min(index for normalized, index, _, raw in normalized_keywords if raw in matched_keywords)

        penalty = 0
        if name.endswith("R"):
            penalty += 1
        return {
            "_rank_key": (-best_specificity, -total_specificity, keyword_index, penalty, len(name)),
            "ts_code": code,
            "index_code": code,
            "index_name": name,
            "name": name,
            "fullname": fullname,
            "market": str(row.get("market", "")).strip(),
            "publisher": str(row.get("publisher", "")).strip(),
            "category": str(row.get("category", "")).strip(),
            "index_type": str(row.get("index_type", "")).strip(),
            "list_date": row.get("list_date"),
            "matched_keywords": list(matched_keywords),
            "exact_match": exact_match,
            "match_quality": "exact" if exact_match else "theme_proxy",
            "display_label": "真实指数估值" if exact_match else "指数估值代理",
            "match_note": "",
        }

    def _index_basic_frame(self) -> pd.DataFrame:
        cached = self.__class__._INDEX_BASIC_FRAME
        if cached is not None and not cached.empty:
            return cached
        cache_key = "index_topic:index_basic:ALL:v1"
        cached_disk = self._load_cache(cache_key, ttl_hours=24)
        if cached_disk is not None:
            self.__class__._INDEX_BASIC_FRAME = cached_disk
            return cached_disk
        rows: List[pd.DataFrame] = []
        for market in INDEX_BASIC_MARKETS:
            try:
                raw = self._ts_call("index_basic", market=market)
            except Exception:
                raw = None
            if raw is None or raw.empty:
                continue
            rows.append(raw.copy())
        if not rows:
            return pd.DataFrame()
        frame = pd.concat(rows, ignore_index=True).drop_duplicates(subset=["ts_code"]).reset_index(drop=True)
        self.__class__._INDEX_BASIC_FRAME = frame
        self._save_cache(cache_key, frame)
        return frame

    def _latest_index_dailybasic_snapshot(
        self,
        index_code: str,
        *,
        as_of: datetime,
    ) -> tuple[Dict[str, Any], str]:
        ts_code = self._normalize_index_code(index_code)
        if not ts_code:
            return ({}, "empty")
        cache_key = f"index_topic:index_dailybasic_latest:{ts_code}:v1"
        cached = self._load_cache(cache_key, ttl_hours=12)
        raw = cached
        diagnosis = "live"
        if raw is None:
            try:
                start_date = (as_of - timedelta(days=21)).strftime("%Y%m%d")
                raw = self._ts_call("index_dailybasic", ts_code=ts_code, start_date=start_date, end_date=as_of.strftime("%Y%m%d"))
                if raw is not None and not raw.empty:
                    self._save_cache(cache_key, raw)
            except Exception as exc:  # noqa: BLE001
                return ({}, self._tushare_failure_diagnosis(exc))
        if raw is None or raw.empty:
            return ({}, "empty")
        latest = raw.sort_values("trade_date", ascending=False).iloc[0]
        snapshot = {
            "trade_date": self._normalize_date_text(latest.get("trade_date")),
            "total_mv": self._safe_float(latest.get("total_mv")),
            "float_mv": self._safe_float(latest.get("float_mv")),
            "turnover_rate": self._safe_float(latest.get("turnover_rate")),
            "turnover_rate_f": self._safe_float(latest.get("turnover_rate_f")),
            "pe": self._safe_float(latest.get("pe")),
            "pe_ttm": self._safe_float(latest.get("pe_ttm")),
            "pb": self._safe_float(latest.get("pb")),
        }
        snapshot["is_fresh"] = self._is_fresh(latest.get("trade_date"), as_of, max_age_days=4)
        return (snapshot, diagnosis)

    def _latest_idx_factor_row(self, index_code: str, *, as_of: datetime) -> Mapping[str, Any] | None:
        ts_code = self._normalize_index_code(index_code)
        if not ts_code:
            return None
        cache_key = f"index_topic:idx_factor_pro:{ts_code}:v1"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None and not cached.empty:
            return cached.sort_values("trade_date", ascending=False).iloc[0].to_dict()
        dates = [
            date
            for date in self._recent_open_trade_dates(lookback_days=14)
            if str(date).strip() and str(date).strip() <= as_of.strftime("%Y%m%d")
        ]
        for trade_date in reversed(dates):
            raw = self._ts_call("idx_factor_pro", ts_code=ts_code, trade_date=trade_date)
            if raw is None or raw.empty:
                continue
            self._save_cache(cache_key, raw)
            return raw.iloc[0].to_dict()
        return None

    def _normalize_index_code(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if text in GLOBAL_INDEX_SYMBOL_MAP:
            return GLOBAL_INDEX_SYMBOL_MAP[text]
        if text.upper() in GLOBAL_INDEX_SYMBOL_MAP:
            return GLOBAL_INDEX_SYMBOL_MAP[text.upper()]
        cleaned = self._clean_cn_index_symbol(text)
        if "." in cleaned:
            return cleaned
        candidates = self._ts_index_code_candidates(cleaned)
        return candidates[0] if candidates else cleaned

    @staticmethod
    def _clean_cn_index_symbol(value: Any) -> str:
        text = str(value or "").strip()
        lowered = text.lower()
        if len(lowered) == 8 and lowered[:2] in {"sh", "sz"} and lowered[2:].isdigit():
            return lowered[2:]
        return text

    @staticmethod
    def _is_fresh(date_text: Any, reference_date: datetime, max_age_days: int = 7) -> bool:
        normalized = BaseCollector._normalize_date_text(date_text)
        if not normalized:
            return False
        try:
            target = datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError:
            return False
        return abs((reference_date.date() - target.date()).days) <= max_age_days

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric):
            return None
        return float(numeric)

    def _with_history_contract(self, frame: pd.DataFrame, api_name: str) -> pd.DataFrame:
        if frame.empty:
            return frame
        latest_date = ""
        if "日期" in frame.columns and not frame.empty:
            latest_date = pd.to_datetime(frame["日期"].iloc[-1]).strftime("%Y-%m-%d")
        frame.attrs["history_api"] = api_name
        frame.attrs["history_source"] = "tushare"
        label = {
            "index_daily": "Tushare 指数日线",
            "index_weekly": "Tushare 指数周线",
            "index_monthly": "Tushare 指数月线",
        }.get(api_name, "Tushare 指数行情")
        frame.attrs["history_source_label"] = label
        frame.attrs["history_as_of"] = latest_date
        frame.attrs["history_is_fresh"] = self._is_fresh(latest_date, datetime.now(), max_age_days=4 if api_name == "index_daily" else 10)
        frame.attrs["history_fallback"] = "none"
        frame.attrs["history_disclosure"] = "指数历史行情优先走 Tushare 指数专题主链；缺失时不把旧链伪装成 fresh。"
        return frame

    def _attach_frame_contract(
        self,
        frame: pd.DataFrame,
        *,
        as_of: datetime,
        latest_date: str,
        source: str,
        fallback: str,
        disclosure: str,
    ) -> None:
        frame.attrs["source"] = source
        frame.attrs["as_of"] = as_of.strftime("%Y-%m-%d %H:%M:%S")
        frame.attrs["latest_date"] = latest_date
        frame.attrs["is_fresh"] = bool(not frame.empty and latest_date and self._is_fresh(latest_date, as_of, max_age_days=7))
        frame.attrs["fallback"] = fallback
        frame.attrs["disclosure"] = disclosure
