"""Standardized SW/CI industry and index snapshots for research pipelines."""

from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from .base import BaseCollector


_LEVEL_LABELS = {
    "L1": "一级",
    "L2": "二级",
    "L3": "三级",
}

_MATCH_STOPWORDS = (
    "收益率",
    "指数",
    "指数增强",
    "etf",
    "联接",
    "基金",
    "中证",
    "国证",
    "沪深",
    "a股",
    "策略",
    "精选",
    "主题",
    "行业",
)


def _normalize_index_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[\s\-/+*×（）()·,，；;:：]", "", text)
    for token in _MATCH_STOPWORDS:
        text = text.replace(token, "")
    return text


class IndustryIndexCollector(BaseCollector):
    """Shared Tushare-first industry/index framework collector."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="IndustryIndexCollector")

    def collect_market_snapshot(self, reference_date: Optional[datetime] = None) -> Dict[str, Any]:
        as_of = reference_date or datetime.now()
        sw_report = self._family_market_report("sw", as_of=as_of, preferred_level="L2")
        ci_report = self._family_market_report("ci", as_of=as_of, preferred_level="")
        return {
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "sw_industry_spot": sw_report["frame"],
            "sw_industry_report": sw_report,
            "ci_industry_spot": ci_report["frame"],
            "ci_industry_report": ci_report,
        }

    def get_stock_industry_snapshot(
        self,
        symbol: str,
        *,
        reference_date: Optional[datetime] = None,
        limit: int = 6,
    ) -> Dict[str, Any]:
        as_of = reference_date or datetime.now()
        ts_code = self._to_ts_code(symbol)
        sw_report = self._stock_membership_report("sw", ts_code=ts_code, as_of=as_of)
        ci_report = self._stock_membership_report("ci", ts_code=ts_code, as_of=as_of)
        items = list(sw_report.get("items") or []) + list(ci_report.get("items") or [])
        items = items[: max(int(limit), 1)]
        latest_dates = [str(item.get("trade_date", "")).strip() for item in items if str(item.get("trade_date", "")).strip()]
        blocked = {"permission_blocked", "rate_limited", "network_error", "fetch_error", "unavailable"}
        diagnoses = {
            str(sw_report.get("diagnosis", "")).strip(),
            str(ci_report.get("diagnosis", "")).strip(),
        }
        if items:
            diagnosis = "live"
            status = "matched"
        elif diagnoses & blocked:
            diagnosis = next((item for item in diagnoses if item in blocked), "unavailable")
            status = "blocked"
        else:
            diagnosis = "empty"
            status = "empty"
        latest_date = max(latest_dates) if latest_dates else ""
        return {
            "symbol": str(symbol).strip(),
            "ts_code": ts_code,
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "latest_date": latest_date,
            "is_fresh": self._is_fresh(latest_date, as_of, max_age_days=0) if latest_date else False,
            "source": "tushare.index_member_all+tushare.sw_daily+tushare.ci_index_member+tushare.ci_daily",
            "fallback": "none",
            "diagnosis": diagnosis,
            "status": status,
            "disclosure": (
                "申万/中信行业框架按股票代码反查标准行业层级，并结合对应行业指数日行情判断当期强弱；"
                "权限失败、空表或频控时只按缺失处理，不把空结果写成 fresh 命中。"
            ),
            "items": items,
            "families": {
                "sw": sw_report,
                "ci": ci_report,
            },
        }

    def get_etf_industry_snapshot(
        self,
        metadata: Mapping[str, Any],
        *,
        fund_profile: Optional[Mapping[str, Any]] = None,
        reference_date: Optional[datetime] = None,
        limit: int = 4,
    ) -> Dict[str, Any]:
        as_of = reference_date or datetime.now()
        code_candidates = self._etf_index_code_candidates(metadata, fund_profile)
        name_candidates = self._etf_index_name_candidates(metadata, fund_profile)
        sw_items, sw_fallback = self._direct_index_items("sw", code_candidates=code_candidates, name_candidates=name_candidates, as_of=as_of)
        ci_items, ci_fallback = self._direct_index_items("ci", code_candidates=code_candidates, name_candidates=name_candidates, as_of=as_of)
        items = [*sw_items, *ci_items]
        latest_dates = [str(item.get("trade_date", "")).strip() for item in items if str(item.get("trade_date", "")).strip()]
        fallback = "none"
        if not items and sw_fallback != "none":
            fallback = sw_fallback
        elif not items and ci_fallback != "none":
            fallback = ci_fallback
        elif items and "name_match" in {sw_fallback, ci_fallback}:
            fallback = "name_match"
        return {
            "symbol": str(metadata.get("symbol", "")).strip(),
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "latest_date": max(latest_dates) if latest_dates else "",
            "is_fresh": self._is_fresh(max(latest_dates), as_of, max_age_days=0) if latest_dates else False,
            "source": "tushare.sw_daily+tushare.index_classify+tushare.ci_daily",
            "fallback": fallback,
            "diagnosis": "live" if items else "empty",
            "status": "matched" if items else "empty",
            "disclosure": (
                "ETF 标准行业/指数框架优先按跟踪指数代码或指数名称匹配申万/中信行业指数日行情；"
                "匹配不到时不强行伪造行业归属。"
            ),
            "items": items[: max(int(limit), 1)],
        }

    def _family_market_report(
        self,
        family: str,
        *,
        as_of: datetime,
        preferred_level: str = "",
    ) -> Dict[str, Any]:
        latest_trade_date = self._latest_trade_date_for(as_of)
        diagnosis = "live"
        error: BaseException | None = None
        try:
            frame = self._family_daily_frame(family, latest_trade_date, preferred_level=preferred_level)
        except Exception as exc:  # noqa: BLE001
            frame = pd.DataFrame()
            error = exc
        if error is not None:
            diagnosis = self._tushare_failure_diagnosis(error)
        elif frame.empty:
            diagnosis = "empty"
        family_label = "申万" if family == "sw" else "中信"
        source = (
            "tushare.sw_daily+tushare.index_classify"
            if family == "sw"
            else "tushare.ci_daily"
        )
        disclosure = (
            f"{family_label}行业指数日行情当前可用时，直接用标准行业指数主链替代模糊板块快照。"
            if diagnosis in {"live", "empty"}
            else self._blocked_disclosure(diagnosis, source=f"Tushare {family_label}行业指数")
        )
        return self._frame_report(
            frame,
            as_of=as_of,
            latest_date=self._normalize_date_text(latest_trade_date),
            source=source,
            fallback="none",
            diagnosis=diagnosis,
            disclosure=disclosure,
        )

    def _stock_membership_report(
        self,
        family: str,
        *,
        ts_code: str,
        as_of: datetime,
    ) -> Dict[str, Any]:
        api_name = "index_member_all" if family == "sw" else "ci_index_member"
        family_label = "申万" if family == "sw" else "中信"
        cache_key = f"industry_index:{family}:member:{ts_code}:v1"
        cached = self._load_cache(cache_key, ttl_hours=12)
        raw = cached
        error: BaseException | None = None
        if raw is None:
            try:
                raw = self._ts_call(api_name, ts_code=ts_code, is_new="Y")
                if raw is not None:
                    self._save_cache(cache_key, raw)
            except Exception as exc:  # noqa: BLE001
                raw = None
                error = exc
        diagnosis = "live"
        if error is not None:
            diagnosis = self._tushare_failure_diagnosis(error)
        elif raw is None:
            diagnosis = "unavailable"
        elif getattr(raw, "empty", False):
            diagnosis = "empty"
        items = self._membership_items_from_raw(family, raw, as_of=as_of)
        latest_dates = [str(item.get("trade_date", "")).strip() for item in items if str(item.get("trade_date", "")).strip()]
        return {
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "latest_date": max(latest_dates) if latest_dates else "",
            "is_fresh": self._is_fresh(max(latest_dates), as_of, max_age_days=0) if latest_dates else False,
            "source": (
                "tushare.index_member_all+tushare.sw_daily"
                if family == "sw"
                else "tushare.ci_index_member+tushare.ci_daily"
            ),
            "fallback": "none",
            "diagnosis": diagnosis,
            "status": "matched" if items else ("blocked" if diagnosis not in {"live", "empty"} else "empty"),
            "disclosure": (
                f"Tushare {family_label}行业成分按股票代码回填标准行业层级，再结合对应行业指数日行情判断强弱。"
                if diagnosis in {"live", "empty"}
                else self._blocked_disclosure(diagnosis, source=f"Tushare {family_label}行业成分")
            ),
            "items": items,
        }

    def _membership_items_from_raw(
        self,
        family: str,
        raw: pd.DataFrame | None,
        *,
        as_of: datetime,
    ) -> List[Dict[str, Any]]:
        if raw is None or raw.empty:
            return []
        row = raw.iloc[0]
        lookup = self._family_daily_lookup(family, self._latest_trade_date_for(as_of))
        items: List[Dict[str, Any]] = []
        family_label = "申万" if family == "sw" else "中信"
        for code_key, name_key, level in (
            ("l3_code", "l3_name", "L3"),
            ("l2_code", "l2_name", "L2"),
            ("l1_code", "l1_name", "L1"),
        ):
            index_code = str(row.get(code_key, "")).strip()
            index_name = str(row.get(name_key, "")).strip()
            if not index_code or not index_name:
                continue
            matched = lookup.get(index_code, {})
            pct_change = matched.get("pct_change")
            items.append(
                {
                    "family": family,
                    "family_label": family_label,
                    "level": level,
                    "level_label": _LEVEL_LABELS.get(level, level),
                    "index_code": index_code,
                    "index_name": index_name,
                    "pct_change": pct_change,
                    "amount": matched.get("amount"),
                    "trade_date": matched.get("trade_date", ""),
                    "signal_strength": self._signal_strength(pct_change),
                    "framework_source": f"{family_label}{_LEVEL_LABELS.get(level, level)}行业",
                    "source": (
                        "Tushare index_member_all / sw_daily"
                        if family == "sw"
                        else "Tushare ci_index_member / ci_daily"
                    ),
                }
            )
        return items

    def _direct_index_items(
        self,
        family: str,
        *,
        code_candidates: Sequence[str],
        name_candidates: Sequence[str],
        as_of: datetime,
    ) -> tuple[List[Dict[str, Any]], str]:
        lookup = self._family_daily_lookup(family, self._latest_trade_date_for(as_of))
        family_frame = self._family_daily_frame(family, self._latest_trade_date_for(as_of), preferred_level="")
        family_label = "申万" if family == "sw" else "中信"
        for code in code_candidates:
            cleaned = str(code).strip()
            if not cleaned:
                continue
            matched = lookup.get(cleaned)
            if matched:
                return ([self._matched_index_item(matched, family=family, family_label=family_label)], "code_match")
        normalized_names = [
            (_normalize_index_text(item), str(item).strip())
            for item in name_candidates
            if _normalize_index_text(item)
        ]
        if family_frame.empty or not normalized_names:
            return ([], "none")
        best_row: pd.Series | None = None
        best_score = -1
        for _, row in family_frame.iterrows():
            row_name = str(row.get("名称", "")).strip()
            row_norm = _normalize_index_text(row_name)
            if not row_norm:
                continue
            for candidate_norm, _candidate_raw in normalized_names:
                if candidate_norm == row_norm:
                    score = 1000 + len(candidate_norm)
                elif candidate_norm and (candidate_norm in row_norm or row_norm in candidate_norm):
                    score = len(candidate_norm)
                else:
                    continue
                if score > best_score:
                    best_score = score
                    best_row = row
        if best_row is None:
            return ([], "none")
        return ([self._matched_index_item(best_row.to_dict(), family=family, family_label=family_label)], "name_match")

    def _matched_index_item(
        self,
        row: Mapping[str, Any],
        *,
        family: str,
        family_label: str,
    ) -> Dict[str, Any]:
        pct_change = pd.to_numeric(pd.Series([row.get("涨跌幅")]), errors="coerce").iloc[0]
        return {
            "family": family,
            "family_label": family_label,
            "level": str(row.get("行业层级", "")).strip(),
            "level_label": _LEVEL_LABELS.get(str(row.get("行业层级", "")).strip(), str(row.get("行业层级", "")).strip()),
            "index_code": str(row.get("指数代码", "") or row.get("代码", "")).strip(),
            "index_name": str(row.get("名称", "")).strip(),
            "pct_change": None if pd.isna(pct_change) else float(pct_change),
            "amount": pd.to_numeric(pd.Series([row.get("成交额")]), errors="coerce").iloc[0],
            "trade_date": str(row.get("日期", "")).strip(),
            "signal_strength": self._signal_strength(None if pd.isna(pct_change) else float(pct_change)),
            "framework_source": str(row.get("框架来源", "")).strip() or f"{family_label}行业",
            "source": f"Tushare {family_label}行业指数日行情",
        }

    def _family_daily_lookup(self, family: str, trade_date: str) -> Dict[str, Dict[str, Any]]:
        frame = self._family_daily_frame(family, trade_date, preferred_level="")
        if frame.empty:
            return {}
        lookup: Dict[str, Dict[str, Any]] = {}
        for _, row in frame.iterrows():
            code = str(row.get("指数代码", "") or row.get("代码", "")).strip()
            if not code:
                continue
            lookup[code] = {
                "pct_change": pd.to_numeric(pd.Series([row.get("涨跌幅")]), errors="coerce").iloc[0],
                "amount": pd.to_numeric(pd.Series([row.get("成交额")]), errors="coerce").iloc[0],
                "trade_date": str(row.get("日期", "")).strip(),
            }
        return lookup

    def _family_daily_frame(self, family: str, trade_date: str, *, preferred_level: str = "") -> pd.DataFrame:
        if family == "sw":
            return self._sw_daily_frame(trade_date, preferred_level=preferred_level)
        return self._ci_daily_frame(trade_date)

    def _sw_daily_frame(self, trade_date: str, *, preferred_level: str = "") -> pd.DataFrame:
        cache_key = f"industry_index:sw_daily:{trade_date}:{preferred_level or 'ALL'}:v1"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached
        raw = self._ts_call("sw_daily", trade_date=str(trade_date).replace("-", ""))
        if raw is None or raw.empty:
            return pd.DataFrame()
        classify = self._sw_classify_frame()
        working = raw.copy()
        if classify is not None and not classify.empty:
            working = working.merge(classify, left_on="ts_code", right_on="index_code", how="left")
        if preferred_level:
            working = working[working.get("level", pd.Series("", index=working.index)).astype(str) == preferred_level]
        frame = pd.DataFrame(
            {
                "代码": working["ts_code"].astype(str),
                "指数代码": working["ts_code"].astype(str),
                "名称": working.get("name", working.get("industry_name", pd.Series("", index=working.index))).astype(str),
                "板块名称": working.get("name", working.get("industry_name", pd.Series("", index=working.index))).astype(str),
                "涨跌幅": pd.to_numeric(working.get("pct_change", pd.Series(pd.NA, index=working.index)), errors="coerce"),
                "成交额": pd.to_numeric(working.get("amount", pd.Series(pd.NA, index=working.index)), errors="coerce") * 10_000.0,
                "日期": working.get("trade_date", pd.Series(trade_date, index=working.index)).map(self._normalize_date_text),
                "行业层级": working.get("level", pd.Series("", index=working.index)).astype(str),
                "框架来源": working.get("level", pd.Series("", index=working.index)).astype(str).map(
                    lambda value: f"申万{_LEVEL_LABELS.get(value, value)}行业" if value else "申万行业"
                ),
                "分类体系": "SW2021",
            }
        )
        frame = frame.dropna(subset=["涨跌幅"]).sort_values("涨跌幅", ascending=False).reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return frame

    def _ci_daily_frame(self, trade_date: str) -> pd.DataFrame:
        cache_key = f"industry_index:ci_daily:{trade_date}:v1"
        cached = self._load_cache(cache_key, ttl_hours=6)
        if cached is not None:
            return cached
        raw = self._ts_call("ci_daily", trade_date=str(trade_date).replace("-", ""))
        if raw is None or raw.empty:
            return pd.DataFrame()
        name_col = self._first_existing_column(raw, ("name", "industry_name", "指数名称"))
        frame = pd.DataFrame(
            {
                "代码": raw["ts_code"].astype(str),
                "指数代码": raw["ts_code"].astype(str),
                "名称": raw.get(name_col, pd.Series("", index=raw.index)).astype(str),
                "板块名称": raw.get(name_col, pd.Series("", index=raw.index)).astype(str),
                "涨跌幅": pd.to_numeric(raw.get("pct_change", pd.Series(pd.NA, index=raw.index)), errors="coerce"),
                "成交额": pd.to_numeric(raw.get("amount", pd.Series(pd.NA, index=raw.index)), errors="coerce") * 10_000.0,
                "日期": raw.get("trade_date", pd.Series(trade_date, index=raw.index)).map(self._normalize_date_text),
                "行业层级": raw.get("level", pd.Series("", index=raw.index)).astype(str),
                "框架来源": "中信行业",
                "分类体系": "CI",
            }
        )
        frame = frame.dropna(subset=["涨跌幅"]).sort_values("涨跌幅", ascending=False).reset_index(drop=True)
        self._save_cache(cache_key, frame)
        return frame

    def _sw_classify_frame(self) -> pd.DataFrame:
        cache_key = "industry_index:sw_classify:SW2021:v1"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        rows: List[pd.DataFrame] = []
        for level in ("L1", "L2", "L3"):
            raw = self._ts_call("index_classify", level=level, src="SW2021")
            if raw is None or raw.empty:
                continue
            rows.append(raw.copy())
        if not rows:
            return pd.DataFrame()
        frame = pd.concat(rows, ignore_index=True)
        self._save_cache(cache_key, frame)
        return frame

    def _etf_index_code_candidates(
        self,
        metadata: Mapping[str, Any],
        fund_profile: Optional[Mapping[str, Any]],
    ) -> List[str]:
        overview = dict((fund_profile or {}).get("overview") or {})
        etf_snapshot = dict((fund_profile or {}).get("etf_snapshot") or {})
        candidates = [
            metadata.get("index_code"),
            metadata.get("benchmark_symbol"),
            etf_snapshot.get("index_code"),
            overview.get("ETF基准指数代码"),
        ]
        return [
            str(item).strip()
            for item in candidates
            if str(item or "").strip()
        ]

    def _etf_index_name_candidates(
        self,
        metadata: Mapping[str, Any],
        fund_profile: Optional[Mapping[str, Any]],
    ) -> List[str]:
        overview = dict((fund_profile or {}).get("overview") or {})
        etf_snapshot = dict((fund_profile or {}).get("etf_snapshot") or {})
        candidates = [
            metadata.get("index_name"),
            metadata.get("benchmark_name"),
            metadata.get("benchmark"),
            etf_snapshot.get("index_name"),
            overview.get("ETF基准指数中文全称"),
            overview.get("业绩比较基准"),
            " ".join(str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()),
            metadata.get("sector"),
        ]
        deduped: List[str] = []
        for item in candidates:
            text = str(item or "").strip()
            if text and text not in deduped:
                deduped.append(text)
        return deduped

    def _latest_trade_date_for(self, as_of: datetime) -> str:
        reference = pd.Timestamp(as_of).normalize().strftime("%Y%m%d")
        dates = [
            date
            for date in self._recent_open_trade_dates(lookback_days=14)
            if str(date).strip() and str(date).strip() <= reference
        ]
        return dates[-1] if dates else reference

    def _frame_report(
        self,
        frame: pd.DataFrame,
        *,
        as_of: datetime,
        latest_date: str,
        source: str,
        fallback: str,
        diagnosis: str,
        disclosure: str,
    ) -> Dict[str, Any]:
        working = frame.reset_index(drop=True) if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        normalized_date = self._normalize_date_text(latest_date)
        return {
            "frame": working,
            "latest_date": normalized_date,
            "is_fresh": bool(not working.empty and normalized_date and self._is_fresh(normalized_date, as_of, max_age_days=0)),
            "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"),
            "source": source,
            "fallback": fallback,
            "diagnosis": diagnosis,
            "disclosure": disclosure,
        }

    @staticmethod
    def _is_fresh(date_text: str, reference_date: datetime, max_age_days: int = 7) -> bool:
        if not date_text:
            return False
        normalized = str(date_text).replace("/", "-")
        try:
            target = datetime.strptime(normalized[:10], "%Y-%m-%d")
        except ValueError:
            return False
        return abs((reference_date.date() - target.date()).days) <= max_age_days

    @staticmethod
    def _signal_strength(pct_change: Any) -> str:
        value = pd.to_numeric(pd.Series([pct_change]), errors="coerce").iloc[0]
        if pd.isna(value):
            return "中"
        if float(value) >= 3:
            return "高"
        if float(value) >= 1:
            return "中"
        return "低"
