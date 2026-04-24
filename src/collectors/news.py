"""RSS and offline fallback headlines for briefing generation."""

from __future__ import annotations

from datetime import datetime, timedelta
import html
import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import quote_plus, urlparse

import feedparser
import requests

import pandas as pd

from src.collectors.base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_yaml

GENERIC_HEADLINE_TITLE_KEYS = (
    "global market headlines",
    "breaking stock market news",
    "market headlines",
    "stock price & latest news",
    "stock quote price and forecast",
    "historical prices and data",
)


def _format_pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _clean_source_name(value: str) -> str:
    source = value.strip()
    replacements = {
        "Bloomberg Link": "Bloomberg",
        "Reuters.com": "Reuters",
    }
    return replacements.get(source, source)


def _clean_title(title: str, source: str) -> str:
    cleaned = title.strip()
    if source == "Bloomberg" and cleaned.lower().startswith("bloomberg link"):
        cleaned = cleaned[len("Bloomberg Link") :].strip(" -:")
    if cleaned.lower() in {"bloomberg link", "bloomberg", "reuters"}:
        return ""
    return cleaned


def _is_generic_headline_title(title: str) -> bool:
    cleaned = str(title).strip().lower()
    if not cleaned:
        return True
    return any(token in cleaned for token in GENERIC_HEADLINE_TITLE_KEYS)


def _normalized_title_key(title: str) -> str:
    cleaned = str(title).strip().lower()
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s*[-|·]\s*(reuters|bloomberg|financial times|ft|财联社|证券时报)\s*$", "", cleaned)
    cleaned = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _parse_news_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    stamp = pd.Timestamp(parsed)
    if stamp.tzinfo is not None:
        try:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        except TypeError:
            stamp = stamp.tz_localize(None)
    return stamp.to_pydatetime()


def _freshness_bucket(age_days: Optional[float]) -> str:
    if age_days is None:
        return "unknown"
    if age_days <= 1.5:
        return "fresh"
    if age_days <= 3.5:
        return "recent"
    return "stale"


def _value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


def _keyword_relevance(text: str, keywords: Sequence[str]) -> int:
    haystack = str(text).lower()
    score = 0
    for keyword in keywords:
        token = str(keyword).strip().lower()
        if not token:
            continue
        if token in haystack:
            score += 2 if " " in token else 1
    return score


def _contains_any(text: str, keywords: Sequence[str]) -> bool:
    haystack = str(text).lower()
    return any(str(keyword).strip().lower() in haystack for keyword in keywords if str(keyword).strip())


SOURCE_DOMAIN_HINTS = {
    "Reuters": "site:reuters.com",
    "Bloomberg": "site:bloomberg.com",
    "Financial Times": "site:ft.com",
    "Business Wire": "site:businesswire.com",
    "PR Newswire": "site:prnewswire.com",
    "GlobeNewswire": "site:globenewswire.com",
    "HKEXnews": "site:hkexnews.hk",
    "SEC": "site:sec.gov",
    "Investor Relations": "\"Investor Relations\"",
    "CNINFO": "site:cninfo.com.cn",
    "巨潮资讯": "site:cninfo.com.cn",
    "SSE": "site:sse.com.cn",
    "上交所": "site:sse.com.cn",
    "SZSE": "site:szse.cn",
    "深交所": "site:szse.cn",
    "财联社": "site:cls.cn",
    "证券时报": "site:stcn.com",
}

FIRST_PARTY_SOURCE_HINTS = (
    "cninfo",
    "巨潮",
    "sse",
    "上交所",
    "szse",
    "深交所",
    "hkexnews",
    "sec.gov",
    "sec",
    "investor relations",
    "company ir",
    "investor relations",
)

A_SHARE_FIRST_PARTY_SOURCES = (
    "CNINFO",
    "巨潮资讯",
    "SSE",
    "上交所",
    "SZSE",
    "深交所",
    "Investor Relations",
)

A_SHARE_INTELLIGENCE_TOKENS = (
    "公告",
    "年报",
    "业绩",
    "业绩预告",
    "业绩说明会",
    "中标",
    "订单",
    "合同",
    "回购",
    "分红",
    "减持",
    "解禁",
    "定增",
    "重组",
    "股权激励",
    "互动易",
    "问询回复",
    "回复函",
    "问询函",
    "路演纪要",
    "投资者关系",
    "官网",
)

A_SHARE_DIRECT_SOURCE_HINTS = (
    "CNINFO",
    "巨潮资讯",
    "SSE",
    "上交所",
    "Investor Relations",
    "IR",
)

STRUCTURED_STOCK_INTELLIGENCE_APIS = (
    "forecast",
    "express",
    "dividend",
    "stk_holdertrade",
    "stk_surv",
    "disclosure_date",
    "irm_qa_sh",
    "irm_qa_sz",
)

# Shared market-intelligence lanes often describe a theme through event words
# ("ASCO"/"FDA"/"版号"/"特高压") instead of the product-facing theme label.
# Bridge those two vocabularies here so shared context does not depend on
# exact ETF/theme wording appearing in Tushare headlines.
MARKET_INTELLIGENCE_THEME_BRIDGES = (
    {
        "triggers": ("创新药", "港股医药", "港股创新药", "医药", "医疗", "生物医药", "制药", "cro", "cxo", "biotech", "pharma"),
        "terms": (
            "创新药",
            "港股医药",
            "生物医药",
            "医药",
            "医疗",
            "制药",
            "药企",
            "新药",
            "药监局",
            "临床",
            "FDA",
            "ASCO",
            "ESMO",
            "license-out",
            "授权",
            "首付款",
            "里程碑",
        ),
    },
    {
        "triggers": ("通信", "通信设备", "光通信", "光模块", "cpo"),
        "terms": ("通信", "通信设备", "光通信", "光模块", "CPO", "光纤", "交换机", "服务器"),
    },
    {
        "triggers": ("电网", "电力", "公用事业", "特高压"),
        "terms": ("电网", "电力", "公用事业", "特高压", "配电网", "电力设备", "变压器", "虚拟电厂"),
    },
    {
        "triggers": ("游戏", "电竞"),
        "terms": ("游戏", "版号", "手游", "端游", "电竞", "小游戏", "出海游戏"),
    },
)

IRM_QA_STRUCTURED_APIS = ("irm_qa_sh", "irm_qa_sz")

IRM_PLATFORM_LINKS = {
    "irm_qa_sh": "https://sns.sseinfo.com/",
    "irm_qa_sz": "https://irm.cninfo.com.cn/",
}

OFFICIAL_SEARCH_DOMAIN_MAP = {
    "cninfo.com.cn": ("CNINFO", "CNINFO::search", "official_search_fallback"),
    "sse.com.cn": ("SSE", "SSE::search", "official_search_fallback"),
    "query.sse.com.cn": ("SSE", "SSE::search", "official_search_fallback"),
    "szse.cn": ("SZSE", "SZSE::search", "official_search_fallback"),
}

IR_SEARCH_TOKENS = (
    "investor relations",
    "投资者关系",
    "/ir",
    "/irm",
    "ir.",
    "irm.",
    "ir/",
)


def _contains_cjk(value: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in value)


def _contains_ascii_alpha(value: str) -> bool:
    return any(char.isascii() and char.isalpha() for char in value)


def _site_domain(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = parsed.netloc.strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_ir_link(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return any(token in text for token in IR_SEARCH_TOKENS)


class NewsCollector(BaseCollector):
    """Collect market-facing headlines from RSS and offline market proxies."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="NewsCollector")
        self.feeds_path = resolve_project_path(self.config.get("news_feeds_file", "config/news_feeds.yaml"))

    def _structured_stock_intelligence_api_names(self) -> tuple[str, ...]:
        configured = self.config.get("structured_stock_intelligence_apis")
        if not configured:
            return STRUCTURED_STOCK_INTELLIGENCE_APIS
        if isinstance(configured, str):
            items = [item.strip() for item in configured.split(",") if item.strip()]
        else:
            items = [str(item).strip() for item in list(configured or []) if str(item).strip()]
        selected = tuple(item for item in items if item in STRUCTURED_STOCK_INTELLIGENCE_APIS)
        return selected or STRUCTURED_STOCK_INTELLIGENCE_APIS

    def collect(
        self,
        snapshots: Optional[Iterable[Any]] = None,
        china_macro: Optional[Mapping[str, Any]] = None,
        global_proxy: Optional[Mapping[str, Any]] = None,
        limit: int = 6,
        preferred_sources: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Return live headlines when available, otherwise fall back to proxy narratives."""
        rows = list(snapshots or [])
        feeds = load_yaml(self.feeds_path, default={"feeds": []}) or {"feeds": []}
        preferences = feeds.get("preferences", {}) or {}
        configured_preferred = [str(item) for item in preferences.get("preferred_sources", []) if str(item).strip()]
        configured_required = [str(item) for item in preferences.get("required_sources", []) if str(item).strip()]
        preferred = list(configured_preferred)
        if preferred_sources:
            for item in preferred_sources:
                if item not in preferred:
                    preferred.append(str(item))
        max_items_per_feed = int(preferences.get("max_items_per_feed", 2))
        live_items: List[Dict[str, str]] = []
        errors: List[str] = []

        for feed in feeds.get("feeds", []) or []:
            url = str(feed.get("url", "")).strip()
            if not url:
                continue
            try:
                parsed = self.cached_call(
                    f"news:rss:{url}",
                    self._fetch_feed,
                    url,
                    ttl_hours=2,
                    prefer_stale=True,
                )
            except Exception as exc:
                errors.append(str(feed.get("name", feed.get("category", "news"))))
                continue
            for entry in parsed.entries[:max_items_per_feed]:
                source = getattr(entry, "source", {})
                if isinstance(source, Mapping):
                    source_name = _clean_source_name(str(source.get("title", "")).strip())
                else:
                    source_name = _clean_source_name(str(source or "").strip())
                configured_source = _clean_source_name(str(feed.get("source", "")).strip())
                title = _clean_title(str(getattr(entry, "title", "")).strip(), source_name or configured_source)
                if not title:
                    continue
                live_items.append(
                    {
                        "category": str(feed.get("category", "market")),
                        "title": title,
                        "source": source_name
                        or _clean_source_name(
                            str(getattr(entry, "publisher", "") or configured_source or feed.get("name", "")).strip()
                        ),
                        "configured_source": configured_source,
                        "must_include": bool(feed.get("must_include", False)),
                        "published_at": str(getattr(entry, "published", "") or getattr(entry, "updated", "")).strip(),
                        "link": str(getattr(entry, "link", "")).strip(),
                    }
                )
        live_items = self._filter_candidate_items(live_items, recent_days=14)
        live_items = self._rank_items(live_items, preferred_sources=preferred)
        selected_items = self._diversify_items(live_items, limit)

        if selected_items:
            required_present = self._present_sources(selected_items)
            missing_required = [
                item for item in configured_required if not any(item.lower() in source for source in required_present)
            ]
            return {
                "mode": "live",
                "items": selected_items,
                "all_items": live_items,
                "lines": self._live_lines(selected_items),
                "source_list": sorted(required_present),
                "note": self._live_note(preferred, missing_required),
            }

        return {
            "mode": "proxy",
            "items": [],
            "all_items": [],
            "lines": self._fallback_lines(rows, dict(china_macro or {}), dict(global_proxy or {})),
            "note": "实时 RSS 暂不可用，已回退到本地宏观与市场代理主线。"
            + (f" 当前有 {len(errors)} 个新闻源未连通。" if errors else ""),
        }

    def search_by_keywords(
        self,
        keywords: Sequence[str],
        preferred_sources: Optional[Sequence[str]] = None,
        limit: int = 6,
        recent_days: int = 7,
        query_cap: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """Search live RSS by asset/topic keywords for sector-specific catalysts."""
        topic_search_enabled = bool(
            self.config.get("news_topic_search_enabled", dict(self.config).get("news", {}).get("topic_search_enabled", True))
        )
        if not topic_search_enabled:
            return []
        cleaned = self._normalize_topic_keywords(keywords)
        if not cleaned:
            return []

        feeds = load_yaml(self.feeds_path, default={"preferences": {}}) or {"preferences": {}}
        preferences = feeds.get("preferences", {}) or {}
        configured_preferred = [str(item) for item in preferences.get("preferred_sources", []) if str(item).strip()]
        preferred = list(configured_preferred)
        if preferred_sources:
            for item in preferred_sources:
                value = str(item).strip()
                if value and value not in preferred:
                    preferred.append(value)

        effective_query_cap = query_cap if query_cap is not None else self.config.get("news_topic_query_cap", 4)
        effective_query_cap = max(1, int(effective_query_cap or 4))
        items: List[Dict[str, str]] = []
        for label, url in self._topic_queries(cleaned, preferred, recent_days)[:effective_query_cap]:
            try:
                parsed = self.cached_call(
                    f"news:topic:{url}",
                    self._fetch_feed,
                    url,
                    ttl_hours=2,
                    prefer_stale=True,
                )
            except Exception:
                continue
            for entry in parsed.entries[:4]:
                source = getattr(entry, "source", {})
                source_name = (
                    _clean_source_name(str(source.get("title", "")).strip())
                    if isinstance(source, Mapping)
                    else _clean_source_name(str(source or "").strip())
                )
                title = _clean_title(str(getattr(entry, "title", "")).strip(), source_name)
                if not title:
                    continue
                items.append(
                    {
                        "category": label,
                        "title": title,
                        "source": source_name or label,
                        "configured_source": source_name or label,
                        "must_include": False,
                        "published_at": str(getattr(entry, "published", "") or getattr(entry, "updated", "")).strip(),
                        "link": str(getattr(entry, "link", "")).strip(),
                    }
                )

        items, ranking_reference_time = self._filter_topic_search_items(items, recent_days=recent_days)
        ranked = self._rank_items(items, preferred, query_keywords=cleaned, reference_time=ranking_reference_time)
        return self._diversify_items(ranked, limit)

    def search_by_keyword_groups(
        self,
        keyword_groups: Sequence[Sequence[str]],
        preferred_sources: Optional[Sequence[str]] = None,
        limit: int = 6,
        recent_days: int = 7,
        query_cap_per_group: Optional[int] = None,
        total_query_cap: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """Search multiple query groups and merge the best live items.

        This is meant for theme/sector catalyst retrieval where a single flat
        keyword list can easily miss the most useful query permutations.
        """

        merged: List[Dict[str, str]] = []
        seen_groups: set[tuple[str, ...]] = set()
        remaining_budget = max(int(total_query_cap or 0), 0) if total_query_cap is not None else None
        for group in keyword_groups:
            cleaned_group = tuple(self._normalize_topic_keywords(group))
            if not cleaned_group or cleaned_group in seen_groups:
                continue
            if remaining_budget is not None and remaining_budget <= 0:
                break
            seen_groups.add(cleaned_group)
            per_group_query_cap = query_cap_per_group
            if remaining_budget is not None:
                if per_group_query_cap is None:
                    per_group_query_cap = remaining_budget
                else:
                    per_group_query_cap = min(int(per_group_query_cap or 0), remaining_budget)
            try:
                hits = self._call_search_by_keywords(
                    cleaned_group,
                    preferred_sources=preferred_sources,
                    limit=max(limit, 4),
                    recent_days=recent_days,
                    query_cap=per_group_query_cap,
                )
            except Exception:
                continue
            merged.extend(hits)
            if remaining_budget is not None and per_group_query_cap is not None:
                remaining_budget -= max(int(per_group_query_cap or 0), 0)
        merged, ranking_reference_time = self._filter_topic_search_items(merged, recent_days=recent_days)
        ranked = self._rank_items(
            merged,
            list(preferred_sources or []),
            query_keywords=[item for group in seen_groups for item in group],
            reference_time=ranking_reference_time,
        )
        return self._diversify_items(ranked, limit)

    def get_stock_news(self, symbol: str, limit: int = 10) -> List[Dict[str, str]]:
        """Fetch per-stock intelligence from official direct sources, structured disclosures and search fallback."""
        configured_limit = int(self.config.get("stock_news_limit", limit) or limit)
        limit = max(1, min(limit, configured_limit))
        runtime_mode = str(self.config.get("stock_news_runtime_mode", "full") or "full").strip().lower()
        if runtime_mode not in {"full", "focused", "finalist", "structured_only"}:
            runtime_mode = "full"
        profile = self._stock_identity(symbol)
        if not profile:
            return []
        keywords = [profile["symbol"], profile["ts_code"], profile["name"]]
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        preferred_sources = self._stock_preferred_sources()
        direct_items: List[Dict[str, str]] = []
        structured_items: List[Dict[str, str]] = []
        tushare_news_items: List[Dict[str, str]] = []
        search_items: List[Dict[str, str]] = []

        try:
            structured_items.extend(self._structured_stock_intelligence(profile, start_date=start, end_date=end, limit=max(limit, 6)))
        except Exception:
            pass

        if runtime_mode != "structured_only":
            try:
                direct_items.extend(self._official_stock_intelligence(profile, start_date=start, end_date=end, limit=max(limit, 6)))
            except Exception:
                pass

        primary_items = self._filter_candidate_items([*direct_items, *structured_items], recent_days=90)
        if runtime_mode != "structured_only" and self._needs_stock_tushare_news_backfill(primary_items, limit=limit):
            for api_name in ("news", "major_news"):
                try:
                    frame = self._ts_call(api_name, start_date=start, end_date=end)
                except Exception:
                    frame = None
                tushare_news_items.extend(self._normalize_tushare_stock_news(frame, keywords, limit=limit))
                if len(tushare_news_items) >= limit:
                    break

        primary_items = self._filter_candidate_items([*direct_items, *structured_items, *tushare_news_items], recent_days=90)
        if runtime_mode != "structured_only" and self._needs_stock_search_backfill(primary_items, limit=limit):
            try:
                search_items.extend(self._call_search_stock_intelligence(profile, limit=max(limit, 6), runtime_mode=runtime_mode))
            except Exception:
                pass

        items = [*direct_items, *structured_items, *tushare_news_items, *search_items]
        ranked = self._rank_items(
            self._filter_candidate_items(items, recent_days=None),
            preferred_sources=preferred_sources + ["Tushare"],
            query_keywords=keywords,
        )
        diversified = self._diversify_items(ranked, limit)
        diversified = self._ensure_structured_lane_coverage(diversified, ranked, limit=limit)
        return self._ensure_irm_lane_coverage(diversified, ranked, limit=limit)

    def get_stk_surv(
        self,
        symbol: str,
        *,
        limit: int = 20,
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> Dict[str, Any]:
        """Return a Tushare机构调研快照 for downstream event consumption.

        The snapshot keeps the same freshness contract as the other structured
        collectors: empty / blocked / stale rows are never disguised as fresh.
        """
        configured_limit = max(1, int(limit or 1))
        profile = self._stock_identity(symbol) or {}
        ts_code = str(profile.get("ts_code", "")).strip() or self._to_ts_code(symbol)
        name = str(profile.get("name", "")).strip() or str(symbol).strip() or self._from_ts_code(ts_code)
        bare_symbol = str(profile.get("symbol", "")).strip() or self._from_ts_code(ts_code)
        if not ts_code:
            return self._empty_stk_surv_snapshot(symbol=bare_symbol, ts_code="", name=name)

        normalized_trade_date = str(trade_date).replace("-", "").strip()
        normalized_start_date = str(start_date).replace("-", "").strip()
        normalized_end_date = str(end_date).replace("-", "").strip()

        cache_key = (
            f"news:stk_surv:{ts_code}:{normalized_trade_date}:{normalized_start_date}:{normalized_end_date}:{configured_limit}"
        )

        def _fetch_snapshot() -> Dict[str, Any]:
            try:
                kwargs: Dict[str, Any] = {"ts_code": ts_code}
                if normalized_trade_date:
                    kwargs["trade_date"] = normalized_trade_date
                if normalized_start_date:
                    kwargs["start_date"] = normalized_start_date
                if normalized_end_date:
                    kwargs["end_date"] = normalized_end_date
                frame = self._ts_call(
                    "stk_surv",
                    **kwargs,
                )
            except TypeError:
                try:
                    kwargs = {"ts_code": ts_code}
                    if normalized_start_date:
                        kwargs["start_date"] = normalized_start_date
                    if normalized_end_date:
                        kwargs["end_date"] = normalized_end_date
                    frame = self._ts_call(
                        "stk_surv",
                        **kwargs,
                    )
                except Exception:
                    frame = None
            except Exception:
                frame = None
            return self._annotate_stk_surv_snapshot(
                frame,
                symbol=bare_symbol,
                ts_code=ts_code,
                name=name,
                limit=configured_limit,
            )

        try:
            snapshot = self.cached_call(cache_key, _fetch_snapshot, ttl_hours=12, prefer_stale=True)
        except Exception:
            snapshot = self._empty_stk_surv_snapshot(symbol=bare_symbol, ts_code=ts_code, name=name)
        if not isinstance(snapshot, Mapping):
            return self._empty_stk_surv_snapshot(symbol=bare_symbol, ts_code=ts_code, name=name)
        return dict(snapshot)

    def _ensure_irm_lane_coverage(
        self,
        selected_items: Sequence[Dict[str, str]],
        ranked_items: Sequence[Dict[str, str]],
        *,
        limit: int,
    ) -> List[Dict[str, str]]:
        chosen = [dict(item) for item in selected_items if isinstance(item, Mapping)]
        if int(limit) < 2:
            return chosen
        if any(str(item.get("configured_source", "")).startswith("Tushare::irm_qa_") for item in chosen):
            return chosen
        irm_items = [
            dict(item)
            for item in ranked_items
            if isinstance(item, Mapping) and str(item.get("configured_source", "")).startswith("Tushare::irm_qa_")
        ]
        if not irm_items:
            return chosen
        best_irm = irm_items[0]
        best_key = (
            str(best_irm.get("title", "")).strip(),
            str(best_irm.get("source", "")).strip(),
        )
        if any((str(item.get("title", "")).strip(), str(item.get("source", "")).strip()) == best_key for item in chosen):
            return chosen
        trimmed = chosen[: max(int(limit) - 1, 0)]
        return [*trimmed, best_irm]

    def _ensure_structured_lane_coverage(
        self,
        selected_items: Sequence[Dict[str, str]],
        ranked_items: Sequence[Dict[str, str]],
        *,
        limit: int,
    ) -> List[Dict[str, str]]:
        chosen = [dict(item) for item in selected_items if isinstance(item, Mapping)]
        if int(limit) < 2:
            return chosen

        def _item_key(item: Mapping[str, Any]) -> tuple[str, str]:
            return (
                str(item.get("title", "")).strip(),
                str(item.get("source", "")).strip(),
            )

        def _is_structured(item: Mapping[str, Any]) -> bool:
            return str(item.get("source_note", "")).strip() == "structured_disclosure" or str(item.get("configured_source", "")).startswith("Tushare::")

        structured_ranked: List[Dict[str, str]] = []
        seen_structured: set[tuple[str, str]] = set()
        for item in ranked_items:
            if not isinstance(item, Mapping) or not _is_structured(item):
                continue
            key = _item_key(item)
            if key in seen_structured:
                continue
            seen_structured.add(key)
            structured_ranked.append(dict(item))
        if not structured_ranked:
            return chosen

        target_structured = 2 if int(limit) >= 5 and len(structured_ranked) >= 2 else 1
        chosen_keys = {_item_key(item) for item in chosen}
        chosen_structured = sum(1 for item in chosen if _is_structured(item))
        missing = [item for item in structured_ranked if _item_key(item) not in chosen_keys]

        while chosen_structured < target_structured and missing:
            candidate = missing.pop(0)
            if len(chosen) < int(limit):
                chosen.append(candidate)
                chosen_keys.add(_item_key(candidate))
                chosen_structured += 1
                continue
            replace_idx = next((idx for idx in range(len(chosen) - 1, -1, -1) if not _is_structured(chosen[idx])), None)
            if replace_idx is None:
                break
            removed_key = _item_key(chosen[replace_idx])
            chosen[replace_idx] = candidate
            chosen_keys.discard(removed_key)
            chosen_keys.add(_item_key(candidate))
            chosen_structured = sum(1 for item in chosen if _is_structured(item))

        ranked_order = {
            _item_key(item): idx
            for idx, item in enumerate(ranked_items)
            if isinstance(item, Mapping)
        }
        return sorted(chosen, key=lambda item: ranked_order.get(_item_key(item), len(ranked_order)))

    def get_market_intelligence(
        self,
        keywords: Sequence[str],
        *,
        limit: int = 6,
        recent_days: int = 14,
    ) -> List[Dict[str, str]]:
        """Fetch broad market intelligence from structured Tushare news endpoints.

        This is a non-search fallback for briefing-like workflows where RSS/search
        can be thin or unstable. It should surface market-wide, macro and theme
        headlines without depending on Google News query feeds.
        """
        cleaned_keywords = [str(item).strip() for item in keywords if str(item).strip()]
        match_keywords = self._expand_market_intelligence_keywords(cleaned_keywords)
        if not match_keywords:
            return []
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=max(recent_days, 3))).strftime("%Y%m%d")
        items: List[Dict[str, str]] = []
        for api_name in ("major_news", "news"):
            frame = self._cached_market_intelligence_frame(api_name, start_date=start, end_date=end)
            items.extend(
                self._normalize_tushare_market_news(
                    api_name,
                    frame,
                    keywords=match_keywords,
                    limit=max(limit, 6),
                )
            )
            if len(items) >= limit:
                break
        ranked = self._rank_items(
            self._filter_candidate_items(items, recent_days=recent_days),
            preferred_sources=["Tushare"],
            query_keywords=match_keywords,
        )
        return self._diversify_items(ranked, limit)

    def _cached_market_intelligence_frame(
        self,
        api_name: str,
        *,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame | None:
        cache_key = f"news:market_intelligence:{api_name}:{start_date}:{end_date}"
        try:
            return self.cached_call(
                cache_key,
                self._ts_call,
                api_name,
                start_date=start_date,
                end_date=end_date,
                ttl_hours=2,
                prefer_stale=True,
            )
        except Exception:
            return None

    def _official_stock_intelligence(
        self,
        profile: Mapping[str, Any],
        *,
        start_date: str,
        end_date: str,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        items.extend(self._cninfo_direct_intelligence(profile, start_date=start_date, end_date=end_date, limit=limit))
        items.extend(self._sse_direct_intelligence(profile, start_date=start_date, end_date=end_date, limit=limit))
        return items

    def _cninfo_direct_intelligence(
        self,
        profile: Mapping[str, Any],
        *,
        start_date: str,
        end_date: str,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        ts_code = str(profile.get("ts_code", "")).strip()
        name = str(profile.get("name", "")).strip()
        symbol = str(profile.get("symbol", "")).strip() or self._from_ts_code(ts_code)
        if not ts_code or not name:
            return []
        cache_key = f"news:cninfo_direct:{ts_code}:{start_date}:{end_date}:{limit}"
        try:
            rows = self.cached_call(
                cache_key,
                self._fetch_cninfo_direct_announcements,
                profile,
                start_date,
                end_date,
                limit,
                ttl_hours=2,
                prefer_stale=True,
            )
        except Exception:
            return []
        items: List[Dict[str, str]] = []
        for row in rows or []:
            title = self._clean_cninfo_title(
                row.get("announcementTitle")
                or row.get("announcementtitle")
                or row.get("title")
                or ""
            )
            if not title:
                continue
            text = " ".join([title, str(row.get("secName", "")), str(row.get("secCode", ""))]).strip()
            if name not in text and symbol not in text:
                continue
            published_at = self._normalize_cninfo_datetime(
                row.get("announcementTime")
                or row.get("announcementtime")
                or row.get("announcementDate")
                or row.get("announcementdate")
            )
            link = self._cninfo_link(row.get("adjunctUrl") or row.get("adjuncturl") or row.get("link") or "")
            note = self._official_announcement_hint(
                row.get("announcementTypeName")
                or row.get("announcementType")
                or row.get("announcementtypename")
                or row.get("announcementtype")
                or row.get("category")
                or ""
            )
            items.append(
                {
                    "category": "stock_announcement",
                    "title": title,
                    "source": "CNINFO",
                    "configured_source": "CNINFO::direct",
                    "source_note": "official_direct",
                    "must_include": False,
                    "published_at": published_at,
                    "link": link,
                    "note": note,
                }
            )
        return items

    def _fetch_cninfo_direct_announcements(
        self,
        profile: Mapping[str, Any],
        start_date: str,
        end_date: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        ts_code = str(profile.get("ts_code", "")).strip()
        symbol = str(profile.get("symbol", "")).strip() or self._from_ts_code(ts_code)
        if not symbol:
            return []
        column = "sse" if symbol.startswith(("6", "9")) else "szse"
        org_id = self._cninfo_org_id(symbol)
        if not org_id:
            return []
        response = requests.post(
            "https://www.cninfo.com.cn/new/hisAnnouncement/query",
            data={
                "pageNum": 1,
                "pageSize": max(int(limit), 10),
                "tabName": "fulltext",
                "plate": "",
                "stock": f"{symbol},{org_id}",
                "searchkey": "",
                "secid": "",
                "category": "",
                "trade": "",
                "column": column,
                "seDate": f"{self._normalize_cninfo_date(start_date)}~{self._normalize_cninfo_date(end_date)}",
                "sortName": "announcementTime",
                "sortType": "desc",
                "isHLtitle": "true",
            },
            timeout=4,
            headers={
                "User-Agent": "investment-agent/0.6",
                "Referer": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        response.raise_for_status()
        data = response.json()
        announcements = data.get("announcements") or data.get("classifiedAnnouncements") or []
        if not isinstance(announcements, list):
            return []
        return [item for item in announcements if isinstance(item, Mapping)]

    def _sse_direct_intelligence(
        self,
        profile: Mapping[str, Any],
        *,
        start_date: str,
        end_date: str,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        ts_code = str(profile.get("ts_code", "")).strip()
        symbol = str(profile.get("symbol", "")).strip() or self._from_ts_code(ts_code)
        if not symbol or not symbol.startswith(("6", "9")):
            return []
        cache_key = f"news:sse_direct:{symbol}:{start_date}:{end_date}:{limit}"
        try:
            rows = self.cached_call(
                cache_key,
                self._fetch_sse_direct_announcements,
                profile,
                start_date,
                end_date,
                limit,
                ttl_hours=2,
                prefer_stale=True,
            )
        except Exception:
            return []
        items: List[Dict[str, str]] = []
        for row in rows or []:
            title = self._clean_cninfo_title(
                row.get("TITLE") or row.get("title") or row.get("bulletinTitle") or row.get("announcementTitle") or ""
            )
            if not title:
                continue
            published_at = self._normalize_cninfo_date(row.get("SSEDATE") or row.get("bulletinDate") or row.get("announcementTime") or row.get("date"))
            link = self._sse_link(
                row.get("URL") or row.get("url"),
                row.get("bulletinId") or row.get("SEQ") or row.get("bulletinid"),
                row.get("SSEDATE") or row.get("bulletinDate") or row.get("date"),
            )
            note = self._official_announcement_hint(
                row.get("BULLETIN_TYPE")
                or row.get("BULLETIN_TYPE_NAME")
                or row.get("bulletinType")
                or row.get("bulletinTypeName")
                or row.get("EXTWTFL")
                or row.get("extwtfl")
                or ""
            )
            items.append(
                {
                    "category": "stock_announcement",
                    "title": title,
                    "source": "SSE",
                    "configured_source": "SSE::direct",
                    "source_note": "official_direct",
                    "must_include": False,
                    "published_at": published_at,
                    "link": link,
                    "note": note,
                }
            )
        return items

    def _fetch_sse_direct_announcements(
        self,
        profile: Mapping[str, Any],
        start_date: str,
        end_date: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        ts_code = str(profile.get("ts_code", "")).strip()
        symbol = str(profile.get("symbol", "")).strip() or self._from_ts_code(ts_code)
        if not symbol:
            return []
        stock_type = 2 if symbol.startswith(("688", "689")) else 1
        response = requests.get(
            "https://query.sse.com.cn/security/stock/queryCompanyBulletinNew.do",
            params={
                "jsonCallBack": "jsonpCallback",
                "isPagination": "true",
                "START_DATE": self._normalize_cninfo_date(start_date),
                "END_DATE": self._normalize_cninfo_date(end_date),
                "SECURITY_CODE": symbol,
                "TITLE": "",
                "BULLETIN_TYPE": "",
                "stockType": stock_type,
                "pageHelp.pageSize": max(int(limit), 10),
                "pageHelp.pageNo": 1,
                "pageHelp.beginPage": 1,
                "pageHelp.endPage": 1,
                "pageHelp.cacheSize": 1,
            },
            timeout=4,
            headers={
                "User-Agent": "investment-agent/0.6",
                "Referer": "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
        )
        response.raise_for_status()
        payload = self._parse_json_like(response.text)
        if not isinstance(payload, Mapping):
            return []
        page_help = payload.get("pageHelp") or {}
        rows = page_help.get("data") or payload.get("result") or []
        flattened: List[Dict[str, Any]] = []
        for item in rows:
            if isinstance(item, Mapping):
                flattened.append(dict(item))
                continue
            if isinstance(item, list):
                for nested in item:
                    if isinstance(nested, Mapping):
                        flattened.append(dict(nested))
        return flattened

    def _cninfo_org_id(self, symbol: str) -> str:
        catalog = self._cninfo_stock_catalog()
        row = catalog.get(symbol) or {}
        return str(row.get("orgId", "")).strip()

    def _cninfo_stock_catalog(self) -> Dict[str, Dict[str, Any]]:
        cache_key = "news:cninfo_stock_catalog"
        try:
            payload = self.cached_call(
                cache_key,
                self._fetch_cninfo_stock_catalog,
                ttl_hours=24,
                prefer_stale=True,
            )
        except Exception:
            return {}
        catalog: Dict[str, Dict[str, Any]] = {}
        for row in payload or []:
            if not isinstance(row, Mapping):
                continue
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            catalog[code] = dict(row)
        return catalog

    def _fetch_cninfo_stock_catalog(self) -> List[Dict[str, Any]]:
        response = requests.get(
            "https://www.cninfo.com.cn/new/data/szse_stock.json",
            timeout=4,
            headers={"User-Agent": "investment-agent/0.6"},
        )
        response.raise_for_status()
        payload = response.json() or {}
        rows = payload.get("stockList") or []
        return [dict(item) for item in rows if isinstance(item, Mapping)]

    def _parse_json_like(self, text: str) -> Any:
        stripped = text.strip()
        if not stripped:
            return {}
        if stripped.startswith("{") or stripped.startswith("["):
            return json.loads(stripped)
        match = re.match(r"^[^(]+\((.*)\)\s*;?\s*$", stripped, re.S)
        if match:
            return json.loads(match.group(1))
        raise ValueError("Unsupported JSON-like payload")

    def _needs_stock_tushare_news_backfill(self, items: Sequence[Dict[str, str]], *, limit: int) -> bool:
        del limit
        return not self._has_recent_primary_stock_intelligence(items)

    def _needs_stock_search_backfill(self, items: Sequence[Dict[str, str]], *, limit: int) -> bool:
        del limit
        return not self._has_recent_primary_stock_intelligence(items)

    def _has_recent_primary_stock_intelligence(self, items: Sequence[Dict[str, str]]) -> bool:
        reference_time = datetime.now()
        for item in items:
            published_at = _parse_news_timestamp(item.get("published_at"))
            if published_at is not None:
                age_days = (reference_time - published_at).total_seconds() / 86400.0
                if age_days < -1 or age_days > 35:
                    continue
            elif str(item.get("freshness_bucket", "")).strip() not in {"fresh", "recent"}:
                continue
            if str(item.get("source_note", "")).strip() in {"official_direct", "official_site_search", "structured_disclosure"}:
                return True
            if str(item.get("configured_source", "")).startswith("Tushare::"):
                return True
            if _contains_any(
                " ".join(
                    [
                        str(item.get("source", "")),
                        str(item.get("configured_source", "")),
                        str(item.get("category", "")),
                    ]
                ),
                FIRST_PARTY_SOURCE_HINTS,
            ):
                return True
        return False

    def _structured_stock_intelligence(
        self,
        profile: Mapping[str, Any],
        *,
        start_date: str,
        end_date: str,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        ts_code = str(profile.get("ts_code", "")).strip()
        name = str(profile.get("name", "")).strip()
        symbol = str(profile.get("symbol", "")).strip() or self._from_ts_code(ts_code)
        if not ts_code:
            return []
        items: List[Dict[str, str]] = []
        for api_name in self._structured_stock_intelligence_api_names():
            frame = self._structured_stock_intelligence_frame(
                api_name,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
            )
            items.extend(
                self._normalize_tushare_structured_stock_intelligence(
                    api_name,
                    frame,
                    name=name,
                    symbol=symbol,
                    ts_code=ts_code,
                    limit=limit,
                )
            )
            if len(items) >= limit * 2:
                break
        return items

    def _structured_stock_intelligence_frame(
        self,
        api_name: str,
        *,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame | None:
        if api_name in IRM_QA_STRUCTURED_APIS:
            suffix = str(ts_code).split(".")[-1].upper()
            if api_name == "irm_qa_sh" and suffix != "SH":
                return None
            if api_name == "irm_qa_sz" and suffix != "SZ":
                return None
            try:
                return self._ts_irm_qa_snapshot(
                    api_name,
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception:
                return None
        if api_name == "stk_surv":
            try:
                return self._ts_call(api_name, ts_code=ts_code, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    return self._ts_call(api_name, ts_code=ts_code)
                except Exception:
                    return None
            except Exception:
                return None
        try:
            return self._ts_call(api_name, ts_code=ts_code, start_date=start_date, end_date=end_date)
        except TypeError:
            try:
                return self._ts_call(api_name, ts_code=ts_code)
            except Exception:
                return None
        except Exception:
            return None

    def _stock_identity(self, symbol: str) -> Dict[str, str]:
        ts_code = self._to_ts_code(symbol)
        try:
            frame = self._ts_call("stock_basic", ts_code=ts_code, fields="ts_code,symbol,name")
        except Exception:
            frame = None
        if frame is None or frame.empty:
            return {}
        row = frame.iloc[0]
        company_website = ""
        ir_website = ""
        try:
            company_frame = self._ts_call("stock_company", ts_code=ts_code)
        except Exception:
            company_frame = None
        if company_frame is not None and not company_frame.empty:
            company_row = company_frame.iloc[0]
            for key in ("ir_website",):
                value = str(company_row.get(key, "")).strip()
                if value:
                    ir_website = value
                    break
            for key in ("website", "web_site", "url", "homepage"):
                value = str(company_row.get(key, "")).strip()
                if value:
                    company_website = value
                    break
            if not ir_website and _is_ir_link(company_website):
                ir_website = company_website
        return {
            "symbol": str(row.get("symbol", symbol)).strip() or symbol,
            "ts_code": str(row.get("ts_code", ts_code)).strip() or ts_code,
            "name": str(row.get("name", "")).strip(),
            "company_website": company_website,
            "ir_website": ir_website,
        }

    def _normalize_tushare_stock_news(
        self,
        frame: pd.DataFrame | None,
        keywords: Sequence[str],
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        if frame is None or frame.empty:
            return []

        title_col = self._first_existing_column(frame, ("title", "标题", "subject", "新闻标题"))
        content_col = self._first_existing_column(frame, ("content", "summary", "内容", "正文"))
        source_col = self._first_existing_column(frame, ("src", "media", "来源", "新闻来源"))
        time_col = self._first_existing_column(frame, ("pub_time", "datetime", "发布时间", "时间", "date"))
        link_col = self._first_existing_column(frame, ("url", "link", "新闻链接"))
        if title_col is None:
            return []

        pattern_keywords = [str(item).strip() for item in keywords if str(item).strip()]
        items: List[Dict[str, str]] = []
        working = frame.copy()
        combined_text = working[title_col].astype(str)
        if content_col is not None:
            combined_text = combined_text + " " + working[content_col].astype(str)
        mask = pd.Series(False, index=working.index)
        for keyword in pattern_keywords:
            mask = mask | combined_text.str.contains(keyword, case=False, na=False)
        filtered = working[mask].copy()
        if filtered.empty:
            return []

        if time_col is not None:
            filtered["_published_at"] = filtered[time_col].map(self._normalize_date_text)
            filtered = filtered.sort_values("_published_at", ascending=False)
        for _, row in filtered.head(limit).iterrows():
            title = str(row.get(title_col, "")).strip()
            if not title:
                continue
            items.append(
                {
                    "category": "stock_announcement",
                    "title": title,
                    "source": str(row.get(source_col, "Tushare")).strip() if source_col else "Tushare",
                    "configured_source": "Tushare",
                    "source_note": "tushare_news",
                    "must_include": False,
                    "published_at": str(row.get("_published_at", "")) if "_published_at" in row else (
                        self._normalize_date_text(row.get(time_col)) if time_col else ""
                    ),
                    "link": str(row.get(link_col, "")).strip() if link_col else "",
                }
            )
        return items

    def _normalize_tushare_market_news(
        self,
        api_name: str,
        frame: pd.DataFrame | None,
        *,
        keywords: Sequence[str],
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        if frame is None or frame.empty:
            return []

        title_col = self._first_existing_column(frame, ("title", "标题", "subject", "新闻标题"))
        content_col = self._first_existing_column(frame, ("content", "summary", "内容", "正文"))
        source_col = self._first_existing_column(frame, ("src", "media", "来源", "新闻来源"))
        time_col = self._first_existing_column(frame, ("pub_time", "datetime", "发布时间", "时间", "date"))
        link_col = self._first_existing_column(frame, ("url", "link", "新闻链接"))
        if title_col is None:
            return []

        pattern_keywords = [str(item).strip() for item in keywords if str(item).strip()]
        if not pattern_keywords:
            return []
        working = frame.copy()
        combined_text = working[title_col].astype(str)
        if content_col is not None:
            combined_text = combined_text + " " + working[content_col].astype(str)
        mask = pd.Series(False, index=working.index)
        for keyword in pattern_keywords:
            mask = mask | combined_text.str.contains(keyword, case=False, na=False, regex=False)
        filtered = working[mask].copy()
        if filtered.empty:
            return []

        if time_col is not None:
            filtered["_published_at"] = filtered[time_col].map(self._normalize_date_text)
            filtered = filtered.sort_values("_published_at", ascending=False)
        items: List[Dict[str, str]] = []
        for _, row in filtered.head(limit).iterrows():
            title = str(row.get(title_col, "")).strip()
            if not title:
                continue
            items.append(
                {
                    "category": "market_intelligence",
                    "title": title,
                    "source": str(row.get(source_col, "Tushare")).strip() if source_col else "Tushare",
                    "configured_source": f"Tushare::{api_name}",
                    "source_note": "tushare_market_news",
                    "must_include": False,
                    "published_at": str(row.get("_published_at", "")) if "_published_at" in row else (
                        self._normalize_date_text(row.get(time_col)) if time_col else ""
                    ),
                    "link": str(row.get(link_col, "")).strip() if link_col else "",
                }
            )
        return items

    def _normalize_tushare_structured_stock_intelligence(
        self,
        api_name: str,
        frame: pd.DataFrame | None,
        *,
        name: str,
        symbol: str,
        ts_code: str,
        limit: int = 10,
    ) -> List[Dict[str, str]]:
        if frame is None or frame.empty:
            return []

        working = frame.copy()
        time_col = self._first_existing_column(
            working,
            ("pub_time", "ann_date", "actual_date", "end_date", "pub_date", "trade_date", "modify_date", "pre_date"),
        )
        if time_col is not None:
            working["_published_at"] = working[time_col].map(self._normalize_date_text)
            working = working.sort_values("_published_at", ascending=False)

        items: List[Dict[str, str]] = []
        for _, row in working.head(limit).iterrows():
            title = self._structured_intelligence_title(api_name, row, name=name)
            if not title:
                continue
            fallback_link = self._structured_intelligence_link(api_name, symbol=symbol, ts_code=ts_code)
            note = self._structured_intelligence_note(api_name, row)
            item = {
                "category": "stock_structured_intelligence",
                "title": title,
                "source": "Tushare",
                "configured_source": f"Tushare::{api_name}",
                "source_note": "structured_disclosure",
                "must_include": False,
                "published_at": str(row.get("_published_at", "")) if "_published_at" in row else (
                    self._normalize_date_text(row.get(time_col)) if time_col else ""
                ),
                "link": fallback_link,
            }
            if note:
                item["note"] = note
            items.append(item)
        return items

    def _structured_intelligence_link(self, api_name: str, *, symbol: str, ts_code: str) -> str:
        if api_name in IRM_PLATFORM_LINKS:
            return IRM_PLATFORM_LINKS[api_name]
        return self._structured_disclosure_fallback_link(symbol=symbol, ts_code=ts_code)

    def _structured_disclosure_fallback_link(self, *, symbol: str, ts_code: str) -> str:
        code = str(symbol or "").strip() or self._from_ts_code(ts_code)
        if not code:
            return ""
        return f"https://www.cninfo.com.cn/new/disclosure/detail?stockCode={code}"

    def _structured_intelligence_note(self, api_name: str, row: Mapping[str, Any]) -> str:
        if api_name in IRM_QA_STRUCTURED_APIS:
            return "投资者关系/路演纪要"
        return self._official_announcement_hint(
            row.get("announcementTypeName")
            or row.get("announcementType")
            or row.get("announcementtypename")
            or row.get("announcementtype")
            or row.get("category")
            or ""
        )

    def _compact_irm_text(self, value: Any, *, limit: int) -> str:
        text = html.unescape(str(value or "")).strip()
        if not text:
            return ""
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"^尊敬的投资者[，,：:\s]*", "", text)
        text = re.sub(r"^(?:尊敬的)?董秘[，,：:\s]*", "", text)
        text = re.sub(r"^投资者您好[，,：:\s]*", "", text)
        text = re.sub(r"^您好[，,：:\s]*", "", text)
        text = re.sub(r"^你好[，,：:\s]*", "", text)
        text = re.sub(r"^请问[，,：:\s]*", "", text)
        text = re.sub(r"感谢(?:您|你)[^。；;，,]{0,10}关注[。；;，,!\s]*", "", text)
        text = re.sub(r"\s+", " ", text)
        text = text.strip(" ，,。；;:：")
        if not text:
            return ""
        if len(text) <= limit:
            return text
        return text[: max(limit - 1, 1)].rstrip(" ，,。；;:：") + "…"

    def _structured_intelligence_title(self, api_name: str, row: Mapping[str, Any], *, name: str) -> str:
        prefix = f"{name}" if name else "公司"
        if api_name == "forecast":
            summary = str(row.get("summary", "")).strip()
            type_label = str(row.get("type", "")).strip()
            parts = [part for part in [type_label, summary] if part]
            return f"{prefix}业绩预告：{'；'.join(parts)}" if parts else f"{prefix}披露业绩预告"
        if api_name == "express":
            revenue = row.get("revenue")
            profit = row.get("n_income") or row.get("net_profit")
            yoy = row.get("yoy_net_profit") or row.get("yoy_sales")
            detail_parts = []
            if revenue not in (None, "", "nan"):
                detail_parts.append(f"营收 {revenue}")
            if profit not in (None, "", "nan"):
                detail_parts.append(f"净利 {profit}")
            if yoy not in (None, "", "nan"):
                detail_parts.append(f"同比 {yoy}")
            return f"{prefix}业绩快报：{'；'.join(detail_parts)}" if detail_parts else f"{prefix}披露业绩快报"
        if api_name == "dividend":
            proc = str(row.get("div_proc", "")).strip()
            cash = row.get("cash_div_tax")
            stock = row.get("stk_div")
            detail_parts = []
            if proc:
                detail_parts.append(proc)
            if cash not in (None, "", "nan"):
                detail_parts.append(f"现金分红 {cash}")
            if stock not in (None, "", "nan"):
                detail_parts.append(f"送转 {stock}")
            return f"{prefix}分红方案：{'；'.join(detail_parts)}" if detail_parts else f"{prefix}更新分红方案"
        if api_name == "stk_holdertrade":
            holder = str(row.get("holder_name", "")).strip()
            change = str(row.get("in_de", "")).strip()
            ratio = row.get("change_ratio")
            detail_parts = []
            if holder:
                detail_parts.append(holder)
            if change:
                detail_parts.append(change)
            if ratio not in (None, "", "nan"):
                detail_parts.append(f"比例 {ratio}")
            return f"{prefix}股东变动：{'；'.join(detail_parts)}" if detail_parts else f"{prefix}披露股东变动"
        if api_name == "disclosure_date":
            pre_date = self._normalize_date_text(row.get("pre_date"))
            actual_date = self._normalize_date_text(row.get("actual_date"))
            modify_date = self._normalize_date_text(row.get("modify_date"))
            detail_parts = []
            if pre_date:
                detail_parts.append(f"预约 {pre_date}")
            if actual_date:
                detail_parts.append(f"实际 {actual_date}")
            if modify_date:
                detail_parts.append(f"更新 {modify_date}")
            return f"{prefix}披露日历：{'；'.join(detail_parts)}" if detail_parts else f"{prefix}更新披露日历"
        if api_name == "stk_surv":
            surv_date = self._normalize_date_text(row.get("surv_date") or row.get("trade_date") or row.get("ann_date"))
            rece_mode = str(row.get("rece_mode", "")).strip()
            rece_org = str(row.get("rece_org", "")).strip()
            org_type = str(row.get("org_type", "")).strip()
            detail_parts = []
            if surv_date:
                detail_parts.append(surv_date)
            if rece_mode:
                detail_parts.append(rece_mode)
            if rece_org:
                detail_parts.append(rece_org)
            elif org_type:
                detail_parts.append(org_type)
            return f"{prefix}机构调研：{'；'.join(detail_parts)}" if detail_parts else f"{prefix}机构调研"
        if api_name in IRM_QA_STRUCTURED_APIS:
            question = self._compact_irm_text(row.get("q") or row.get("question"), limit=16)
            answer = self._compact_irm_text(row.get("a") or row.get("answer") or row.get("reply"), limit=18)
            industry = self._compact_irm_text(row.get("industry"), limit=12)
            detail_parts = []
            if question:
                detail_parts.append(question)
            if answer:
                detail_parts.append(f"回复称 {answer}")
            elif industry:
                detail_parts.append(f"涉及 {industry}")
            return f"{prefix}互动平台问答：{'；'.join(detail_parts)}" if detail_parts else f"{prefix}互动平台问答"
        return ""

    def _stk_surv_title(self, row: Mapping[str, Any], *, name: str) -> str:
        return self._structured_intelligence_title("stk_surv", row, name=name)

    def _normalize_tushare_stk_surv_items(
        self,
        frame: pd.DataFrame | None,
        *,
        name: str,
        symbol: str,
        ts_code: str,
        limit: int = 20,
    ) -> List[Dict[str, str]]:
        if frame is None or frame.empty:
            return []

        working = frame.copy()
        date_col = self._first_existing_column(working, ("surv_date", "trade_date", "ann_date", "pub_date", "date"))
        if date_col is not None:
            working["_published_at"] = working[date_col].map(self._normalize_date_text)
            working = working.sort_values("_published_at", ascending=False)

        items: List[Dict[str, str]] = []
        for _, row in working.head(limit).iterrows():
            title = self._stk_surv_title(row, name=name)
            if not title:
                continue
            latest_date = str(row.get("_published_at", "")).strip() if "_published_at" in row else (
                self._normalize_date_text(row.get(date_col)) if date_col else ""
            )
            lead_detail = self._compact_irm_text(
                "；".join(
                    [
                        str(row.get("fund_visitors", "")).strip(),
                        str(row.get("rece_place", "")).strip(),
                        str(row.get("rece_mode", "")).strip(),
                        str(row.get("rece_org", "")).strip(),
                        str(row.get("org_type", "")).strip(),
                        str(row.get("comp_rece", "")).strip(),
                        str(row.get("content", "")).strip(),
                    ]
                ),
                limit=80,
            )
            disclosure = "stk_surv 机构调研快照来自 Tushare；空表、权限失败或旧日期均按缺失处理，不伪装成 fresh。"
            item = {
                "category": "stock_structured_intelligence",
                "title": title,
                "source": "Tushare",
                "configured_source": "Tushare::stk_surv",
                "source_note": "structured_disclosure",
                "must_include": False,
                "published_at": latest_date,
                "link": self._structured_disclosure_fallback_link(symbol=symbol, ts_code=ts_code),
                "note": "投资者关系/路演纪要",
                "lead_detail": lead_detail or "投资者关系/路演纪要",
                "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "latest_date": latest_date,
                "fallback": "none",
                "disclosure": disclosure,
            }
            item["is_fresh"] = bool(latest_date and self._is_snapshot_fresh(latest_date, datetime.now(), max_age_days=14))
            items.append(item)
        return items

    def _annotate_stk_surv_snapshot(
        self,
        frame: pd.DataFrame | None,
        *,
        symbol: str,
        ts_code: str,
        name: str,
        limit: int,
    ) -> Dict[str, Any]:
        items = self._normalize_tushare_stk_surv_items(frame, name=name, symbol=symbol, ts_code=ts_code, limit=limit)
        latest_date = ""
        if items:
            latest_date = max(
                (str(item.get("latest_date", "")).strip() for item in items if str(item.get("latest_date", "")).strip()),
                default="",
            )
        as_of = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        disclosure = "stk_surv 以 Tushare 机构调研快照为准；空表、权限失败或旧日期均按缺失处理，不伪装成 fresh。"
        is_fresh = bool(latest_date and self._is_snapshot_fresh(latest_date, datetime.now(), max_age_days=14))
        return {
            "symbol": symbol,
            "ts_code": ts_code,
            "name": name,
            "source": "tushare.stk_surv",
            "as_of": as_of,
            "latest_date": latest_date,
            "is_fresh": is_fresh,
            "fallback": "none" if items else "missing",
            "disclosure": disclosure,
            "items": items,
        }

    def _empty_stk_surv_snapshot(self, *, symbol: str, ts_code: str, name: str) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "ts_code": ts_code,
            "name": name,
            "source": "tushare.stk_surv",
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "latest_date": "",
            "is_fresh": False,
            "fallback": "missing",
            "disclosure": "stk_surv 当前无可用快照，空表、权限失败或旧日期均按缺失处理，不伪装成 fresh。",
            "items": [],
        }

    @staticmethod
    def _is_snapshot_fresh(date_text: str, reference_date: datetime, max_age_days: int = 7) -> bool:
        parsed = _parse_news_timestamp(date_text)
        if parsed is None:
            return False
        age_days = (reference_date - parsed).total_seconds() / 86400.0
        return 0 <= age_days <= max_age_days

    def _official_announcement_hint(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        normalized = re.sub(r"\s+", " ", text)
        if any(token in normalized for token in ("业绩说明会", "投资者关系", "调研纪要", "路演纪要", "活动记录表", "电话会", "互动易", "互动平台", "投资者问答", "e互动")):
            return "投资者关系/路演纪要"
        if any(token in normalized for token in ("问询函", "回复函", "问询回复", "监管函", "关注函", "工作函")):
            return "问询/回复函"
        if any(token in normalized for token in ("业绩预告", "年报", "中报", "季报", "快报", "财务")):
            return normalized
        if any(token in normalized for token in ("中标", "订单", "合同", "签约")):
            return "中标/订单"
        if any(token in normalized for token in ("回购", "增持", "减持", "解禁", "并购", "重组", "分红", "定增", "融资", "新品", "发布", "投产", "扩产")):
            return normalized
        return normalized

    def _clean_cninfo_title(self, value: Any) -> str:
        title = html.unescape(str(value or "")).strip()
        if not title:
            return ""
        title = re.sub(r"</?em>", "", title, flags=re.IGNORECASE)
        return re.sub(r"\s+", " ", title).strip()

    def _normalize_cninfo_date(self, value: Any) -> str:
        text = self._normalize_date_text(value)
        return text or str(value or "").strip()

    def _normalize_cninfo_datetime(self, value: Any) -> str:
        if value in (None, ""):
            return ""
        if isinstance(value, (int, float)):
            if value > 10_000_000_000:
                return datetime.fromtimestamp(float(value) / 1000.0).isoformat(timespec="seconds")
            return datetime.fromtimestamp(float(value)).isoformat(timespec="seconds")
        parsed = _parse_news_timestamp(value)
        if parsed is not None:
            return parsed.isoformat(timespec="seconds")
        text = self._normalize_date_text(value)
        return f"{text}T00:00:00" if text else str(value).strip()

    def _cninfo_link(self, value: Any) -> str:
        link = str(value or "").strip()
        if not link:
            return ""
        if link.startswith("http://") or link.startswith("https://"):
            return link
        return f"https://www.cninfo.com.cn{link}"

    def _sse_link(self, value: Any, bulletin_id: Any = None, bulletin_date: Any = None) -> str:
        link = str(value or "").strip()
        if link:
            if link.startswith("http://") or link.startswith("https://"):
                return link
            if link.startswith("/"):
                return f"https://www.sse.com.cn{link}"
        bulletin = str(bulletin_id or "").strip()
        date = str(bulletin_date or "").strip()
        if not bulletin or not date:
            return ""
        return (
            "https://star.sse.com.cn/star/en/infodisclosure/announcements/index_detail.shtml"
            f"?SEQ={bulletin}&DATA_TIME={date}"
        )

    def _stock_preferred_sources(self) -> List[str]:
        return [*A_SHARE_FIRST_PARTY_SOURCES, "证券时报", "财联社", "Reuters", "Bloomberg"]

    def _stock_query_groups(self, profile: Mapping[str, Any], *, query_cap: int | None = None) -> List[List[str]]:
        name = str(profile.get("name", "")).strip()
        symbol = str(profile.get("symbol", "")).strip()
        groups: List[List[str]] = []
        if not name:
            return groups
        groups.append([name, symbol] if symbol else [name])
        for token in A_SHARE_INTELLIGENCE_TOKENS:
            groups.append([name, token])
        if query_cap is None:
            return groups
        return groups[: max(int(query_cap or 0), 0)]

    def _stock_official_query_groups(self, profile: Mapping[str, Any], *, query_cap: int | None = None) -> List[List[str]]:
        name = str(profile.get("name", "")).strip()
        symbol = str(profile.get("symbol", "")).strip()
        if not name:
            return []
        groups: List[List[str]] = []
        symbol_groups: List[List[str]] = []
        for token in ("公告", "业绩预告", "业绩说明会", "互动易", "问询回复", "路演纪要", "投资者关系", "官网"):
            groups.append([name, token])
            if symbol:
                symbol_groups.append([name, symbol, token])
        groups.extend(symbol_groups)
        if query_cap is None:
            return groups
        return groups[: max(int(query_cap or 0), 0)]

    def _classify_stock_search_item(self, item: Mapping[str, Any], profile: Mapping[str, Any]) -> Dict[str, str]:
        row = dict(item or {})
        if not str(row.get("title", "")).strip():
            return {}
        source = str(row.get("source", "")).strip()
        configured_source = str(row.get("configured_source", "")).strip() or source
        link = str(row.get("link", "")).strip()
        domain = _site_domain(link)
        lane = OFFICIAL_SEARCH_DOMAIN_MAP.get(domain)
        if lane:
            row["source"] = lane[0]
            row["configured_source"] = lane[1]
            row["source_note"] = lane[2]
            return row
        company_sites = {
            _site_domain(profile.get("company_website")),
            _site_domain(profile.get("ir_website")),
        }
        company_sites.discard("")
        if domain and any(domain == site or domain.endswith(f".{site}") or site.endswith(f".{domain}") for site in company_sites):
            row["configured_source"] = "Investor Relations::search"
            row["source_note"] = "official_site_search"
            if not source:
                row["source"] = "Investor Relations"
            return row
        link_blob = " ".join([source, configured_source, link, str(row.get("title", ""))])
        if _is_ir_link(link_blob):
            row["configured_source"] = "Investor Relations::search"
            row["source_note"] = "official_site_search"
            if not source:
                row["source"] = "Investor Relations"
            return row
        row.setdefault("source_note", "search_fallback")
        return row

    def _search_stock_intelligence(
        self,
        profile: Mapping[str, Any],
        *,
        limit: int = 10,
        runtime_mode: str = "full",
    ) -> List[Dict[str, str]]:
        runtime_mode = str(runtime_mode or "full").strip().lower()
        if runtime_mode not in {"full", "focused", "finalist"}:
            runtime_mode = "full"
        if runtime_mode == "focused":
            official_query_cap = int(self.config.get("stock_news_official_query_cap", 2) or 2)
            generic_query_cap = int(self.config.get("stock_news_search_query_cap", 1) or 1)
            recent_days = int(self.config.get("stock_news_search_recent_days", 14) or 14)
            search_limit = min(max(limit, 3), 4)
        elif runtime_mode == "finalist":
            official_query_cap = int(self.config.get("stock_news_finalist_official_query_cap", 2) or 2)
            generic_query_cap = int(self.config.get("stock_news_finalist_search_query_cap", 1) or 1)
            recent_days = int(self.config.get("stock_news_finalist_search_recent_days", 21) or 21)
            search_limit = min(max(limit, 4), 5)
        else:
            official_query_cap = int(self.config.get("stock_news_official_query_cap", 0) or 0) or None
            generic_query_cap = int(self.config.get("stock_news_search_query_cap", 0) or 0) or None
            recent_days = int(self.config.get("stock_news_search_recent_days", 21) or 21)
            search_limit = max(limit, 6)
        official_groups = self._stock_official_query_groups(profile, query_cap=official_query_cap)
        generic_groups = self._stock_query_groups(profile, query_cap=generic_query_cap)
        official_group_keys = {
            tuple(token.strip() for token in group if str(token).strip())
            for group in official_groups
        }
        generic_groups = [
            group
            for group in generic_groups
            if tuple(token.strip() for token in group if str(token).strip()) not in official_group_keys
        ]
        if not official_groups and not generic_groups:
            return []
        hits: List[Dict[str, str]] = []
        if official_groups:
            official_total_query_cap = max(1, int(official_query_cap or 0)) if official_query_cap is not None else None
            hits.extend(
                self._call_search_by_keyword_groups(
                    official_groups,
                    preferred_sources=[*A_SHARE_DIRECT_SOURCE_HINTS, *self._stock_site_search_hints(profile)],
                    limit=search_limit,
                    recent_days=recent_days,
                    query_cap_per_group=1,
                    total_query_cap=official_total_query_cap,
                )
            )
        classified_official_hits = [self._classify_stock_search_item(item, profile) for item in hits]
        classified_official_hits = [
            item for item in classified_official_hits if str(item.get("title", "")).strip()
        ]
        official_like = [
            item
            for item in classified_official_hits
            if str(item.get("source_note", "")).startswith("official_")
        ]
        if len(official_like) < min(limit, 2) and generic_groups:
            generic_total_query_cap = max(1, int(generic_query_cap or 0)) if generic_query_cap is not None else None
            hits.extend(
                self._call_search_by_keyword_groups(
                    generic_groups,
                    preferred_sources=self._stock_preferred_sources() + self._stock_site_search_hints(profile),
                    limit=search_limit,
                    recent_days=recent_days,
                    query_cap_per_group=1,
                    total_query_cap=generic_total_query_cap,
                )
            )

        normalized: List[Dict[str, str]] = []
        for item in hits:
            row = self._classify_stock_search_item(item, profile)
            if not str(row.get("title", "")).strip():
                continue
            row.setdefault("category", "stock_live_intelligence")
            row.setdefault("configured_source", row.get("source") or row.get("configured_source") or "live")
            row.setdefault("source_note", "search_fallback")
            normalized.append(row)
        return normalized

    def _call_search_by_keywords(
        self,
        keywords: Sequence[str],
        *,
        preferred_sources: Optional[Sequence[str]] = None,
        limit: int = 6,
        recent_days: int = 7,
        query_cap: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        try:
            return self.search_by_keywords(
                keywords,
                preferred_sources=preferred_sources,
                limit=limit,
                recent_days=recent_days,
                query_cap=query_cap,
            )
        except TypeError:
            return self.search_by_keywords(
                keywords,
                preferred_sources=preferred_sources,
                limit=limit,
                recent_days=recent_days,
            )

    def _call_search_by_keyword_groups(
        self,
        keyword_groups: Sequence[Sequence[str]],
        *,
        preferred_sources: Optional[Sequence[str]] = None,
        limit: int = 6,
        recent_days: int = 7,
        query_cap_per_group: Optional[int] = None,
        total_query_cap: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        try:
            return self.search_by_keyword_groups(
                keyword_groups,
                preferred_sources=preferred_sources,
                limit=limit,
                recent_days=recent_days,
                query_cap_per_group=query_cap_per_group,
                total_query_cap=total_query_cap,
            )
        except TypeError:
            return self.search_by_keyword_groups(
                keyword_groups,
                preferred_sources=preferred_sources,
                limit=limit,
                recent_days=recent_days,
            )

    def _call_search_stock_intelligence(
        self,
        profile: Mapping[str, Any],
        *,
        limit: int = 10,
        runtime_mode: str = "full",
    ) -> List[Dict[str, str]]:
        try:
            return self._search_stock_intelligence(profile, limit=limit, runtime_mode=runtime_mode)
        except TypeError:
            return self._search_stock_intelligence(profile, limit=limit)

    def _stock_site_search_hints(self, profile: Mapping[str, Any]) -> List[str]:
        hints: List[str] = []
        for value in (profile.get("company_website"), profile.get("ir_website")):
            domain = _site_domain(value)
            if domain:
                hint = f"site:{domain}"
                if hint not in hints:
                    hints.append(hint)
        return hints

    def _fetch_feed(self, url: str) -> feedparser.FeedParserDict:
        response = requests.get(
            url,
            timeout=4,
            headers={"User-Agent": "investment-agent/0.6"},
        )
        response.raise_for_status()
        parsed = feedparser.parse(response.content)
        if getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", []):
            raise ValueError("RSS parse failed")
        return parsed

    def _live_lines(self, items: List[Dict[str, str]]) -> List[str]:
        return [
            f"[{item['category']}] {item['title']}" + (f" ({item['source']})" if item.get("source") else "")
            for item in items
        ]

    def _live_note(self, preferred: Sequence[str], missing_required: Sequence[str]) -> str:
        note = "情报主线当前来自 RSS 聚合与搜索回退，按配置抓取最近条目。"
        if preferred:
            note += " 优先源: " + " / ".join(preferred[:4]) + "。"
        if missing_required:
            note += " 未命中必带源: " + " / ".join(missing_required[:3]) + "。"
        return note

    def _rank_items(
        self,
        items: Sequence[Dict[str, str]],
        preferred_sources: Sequence[str],
        query_keywords: Optional[Sequence[str]] = None,
        reference_time: Optional[datetime] = None,
    ) -> List[Dict[str, str]]:
        preferred_lower = [item.lower() for item in preferred_sources]
        query_terms = [str(item).strip() for item in (query_keywords or []) if str(item).strip()]
        deduped: List[Dict[str, str]] = []
        seen_titles = set()
        reference_time = reference_time or datetime.now()
        for item in items:
            title = item.get("title", "")
            normalized_title = _normalized_title_key(title)
            if not title or normalized_title in seen_titles:
                continue
            seen_titles.add(normalized_title)
            deduped.append(item)

        def _source_tier(item: Dict[str, str]) -> int:
            source = " ".join(
                [
                    str(item.get("source", "")),
                    str(item.get("configured_source", "")),
                    str(item.get("category", "")),
                ]
            ).lower()
            if any(token in source for token in FIRST_PARTY_SOURCE_HINTS):
                return 2
            if any(name in source for name in preferred_lower):
                return 1
            return 0

        def _score(item: Dict[str, str]) -> tuple[int, int, int, int, float, int, str]:
            source = (item.get("source") or item.get("configured_source") or "").lower()
            priority = 0
            if item.get("must_include"):
                priority += 4
            if any(name in source for name in preferred_lower):
                priority += 3
            if item.get("configured_source") and item["configured_source"].lower() in source:
                priority += 1
            relevance = _keyword_relevance(
                " ".join(
                    [
                        str(item.get("title", "")),
                        str(item.get("source", "")),
                        str(item.get("configured_source", "")),
                    ]
                ),
                query_terms,
            )
            published_at = _parse_news_timestamp(item.get("published_at"))
            if published_at is None:
                age_days = 999.0
                freshness_rank = 0
                freshness_priority = -1000
            else:
                age_days = max((reference_time - published_at).total_seconds() / 86400.0, 0.0)
                freshness_rank = {
                    "fresh": 3,
                    "recent": 2,
                    "stale": 1,
                    "unknown": 0,
                }.get(_freshness_bucket(age_days), 0)
                freshness_priority = freshness_rank * 100 - min(int(round(age_days * 10)), 99)
            return (
                -relevance,
                -freshness_priority,
                -_source_tier(item),
                -priority,
                age_days,
                len(item.get("title", "")),
                item.get("category", ""),
            )

        return sorted(deduped, key=_score)

    def _latest_published_reference_time(self, items: Sequence[Dict[str, str]]) -> Optional[datetime]:
        published_times = [
            parsed
            for parsed in (_parse_news_timestamp(item.get("published_at")) for item in items)
            if parsed is not None
        ]
        if not published_times:
            return None
        return max(published_times)

    def _filter_topic_search_items(
        self,
        items: Sequence[Dict[str, str]],
        *,
        recent_days: int,
    ) -> tuple[List[Dict[str, str]], Optional[datetime]]:
        filtered = self._filter_candidate_items(items, recent_days=recent_days)
        if filtered:
            return filtered, None
        reference_time = self._latest_published_reference_time(items)
        if reference_time is None:
            return [], None
        return self._filter_candidate_items(items, recent_days=recent_days, reference_time=reference_time), reference_time

    def _filter_candidate_items(
        self,
        items: Sequence[Dict[str, str]],
        *,
        recent_days: Optional[int] = None,
        reference_time: Optional[datetime] = None,
    ) -> List[Dict[str, str]]:
        filtered: List[Dict[str, str]] = []
        as_of = reference_time or datetime.now()
        max_age_days = None if recent_days is None else max(int(recent_days), 1) + 1
        for item in items:
            title = str(item.get("title", "")).strip()
            if not title or _is_generic_headline_title(title):
                continue
            published_at = _parse_news_timestamp(item.get("published_at"))
            if published_at is not None and max_age_days is not None:
                age_days = (as_of - published_at).total_seconds() / 86400.0
                if age_days > max_age_days or age_days < -1:
                    continue
            enriched = dict(item)
            if published_at is not None:
                age_days = max((as_of - published_at).total_seconds() / 86400.0, 0.0)
                enriched["published_at"] = published_at.isoformat(timespec="seconds")
                enriched["age_days"] = round(age_days, 2)
                enriched["freshness_bucket"] = _freshness_bucket(age_days)
            else:
                enriched.setdefault("freshness_bucket", "unknown")
            filtered.append(enriched)
        return filtered

    def _diversify_items(self, items: Sequence[Dict[str, str]], limit: int) -> List[Dict[str, str]]:
        selected: List[Dict[str, str]] = []
        used_sources: set[str] = set()

        for item in items:
            source = (item.get("source") or item.get("configured_source") or "").strip().lower()
            if source and source in used_sources:
                continue
            selected.append(item)
            if source:
                used_sources.add(source)
            if len(selected) >= limit:
                return selected

        for item in items:
            if item in selected:
                continue
            selected.append(item)
            if len(selected) >= limit:
                break
        return selected

    def _present_sources(self, items: Sequence[Dict[str, str]]) -> set[str]:
        result = set()
        for item in items:
            source = (item.get("source") or item.get("configured_source") or "").strip().lower()
            if source:
                result.add(source)
        return result

    def _normalize_topic_keywords(self, keywords: Sequence[str]) -> List[str]:
        cleaned: List[str] = []
        for keyword in keywords:
            value = str(keyword).strip()
            if not value or value.lower() in {"etf", "index", "fund", "市场", "行情"}:
                continue
            if len(value) <= 1:
                continue
            if value not in cleaned:
                cleaned.append(value)
        return cleaned[:8]

    def _expand_market_intelligence_keywords(self, keywords: Sequence[str]) -> List[str]:
        expanded: List[str] = []
        seen: set[str] = set()

        def _append(value: Any) -> None:
            token = str(value).strip()
            normalized = token.lower()
            if not token or normalized in seen:
                return
            seen.add(normalized)
            expanded.append(token)

        base_keywords = self._normalize_topic_keywords(keywords)
        for keyword in base_keywords:
            _append(keyword)
        if not expanded:
            return []

        query_blob = " ".join(item.lower() for item in expanded)
        for rule in MARKET_INTELLIGENCE_THEME_BRIDGES:
            triggers = tuple(str(item).strip().lower() for item in rule.get("triggers", ()) if str(item).strip())
            if not triggers or not any(trigger in query_blob for trigger in triggers):
                continue
            for term in rule.get("terms", ()):
                _append(term)
        return expanded[:20]

    def _topic_queries(
        self,
        keywords: Sequence[str],
        preferred_sources: Sequence[str],
        recent_days: int,
    ) -> List[tuple[str, str]]:
        base_terms = list(keywords[:4])
        zh_terms = [term for term in base_terms if _contains_cjk(term)]
        en_terms = [term for term in base_terms if _contains_ascii_alpha(term)]

        locales: List[tuple[str, str, str, List[str]]] = []
        if zh_terms or not en_terms:
            locales.append(("zh-CN", "CN", "CN:zh-Hans", zh_terms or base_terms[:2]))
        if en_terms:
            locales.append(("en-US", "US", "US:en", en_terms or base_terms[:2]))

        queries: List[tuple[str, str]] = []
        seen: set[str] = set()
        for hl, gl, ceid, terms in locales:
            cleaned_terms = [term for term in terms if term][:2]
            if not cleaned_terms:
                continue
            broad_query = " ".join(cleaned_terms) + f" when:{recent_days}d"
            url = self._google_news_search_url(broad_query, hl=hl, gl=gl, ceid=ceid)
            if url not in seen:
                queries.append(("topic_search", url))
                seen.add(url)

            anchor_terms = cleaned_terms[:2]
            combined_anchor = " ".join(anchor_terms).strip()
            named_sources: List[tuple[str, str]] = []
            site_sources: List[tuple[str, str]] = []
            for source in preferred_sources[:5]:
                source_name = str(source).strip()
                if not source_name:
                    continue
                if source_name.lower().startswith("site:"):
                    domain = source_name[5:].strip()
                    if domain:
                        site_sources.append((source_name, f"site:{domain}"))
                    continue
                domain = SOURCE_DOMAIN_HINTS.get(source_name)
                if domain:
                    named_sources.append((source_name, domain))

            # Cover each preferred lane once before expanding anchor variants.
            for source_name, domain_query in [*named_sources, *site_sources[:3]]:
                if not combined_anchor:
                    continue
                combined_query = f"{domain_query} {combined_anchor} when:{recent_days}d"
                combined_url = self._google_news_search_url(combined_query, hl=hl, gl=gl, ceid=ceid)
                if combined_url in seen:
                    continue
                queries.append((source_name, combined_url))
                seen.add(combined_url)

            for anchor in anchor_terms[:1]:
                for source_name, domain_query in [*named_sources, *site_sources[:3]]:
                    scoped_query = f"{domain_query} {anchor} when:{recent_days}d"
                    scoped_url = self._google_news_search_url(scoped_query, hl=hl, gl=gl, ceid=ceid)
                    if scoped_url in seen:
                        continue
                    queries.append((source_name, scoped_url))
                    seen.add(scoped_url)
        return queries

    def _google_news_search_url(self, query: str, hl: str = "zh-CN", gl: str = "CN", ceid: str = "CN:zh-Hans") -> str:
        return (
            "https://news.google.com/rss/search?q="
            + quote_plus(query)
            + f"&hl={hl}&gl={gl}&ceid={ceid}"
        )

    def _fallback_lines(
        self,
        snapshots: List[Any],
        china_macro: Mapping[str, Any],
        global_proxy: Mapping[str, Any],
    ) -> List[str]:
        lines: List[str] = []

        tech = [item for item in snapshots if _value(item, "sector") == "科技"]
        gold = [item for item in snapshots if _value(item, "sector") == "黄金"]
        domestic = [item for item in snapshots if _value(item, "region") == "CN"]
        offshore = [item for item in snapshots if _value(item, "region") in {"US", "HK"}]

        tech_1d = self._avg(tech, "return_1d")
        gold_1d = self._avg(gold, "return_1d")
        domestic_1d = self._avg(domestic, "return_1d")
        offshore_1d = self._avg(offshore, "return_1d")
        pmi = float(china_macro.get("pmi", 50.0))
        cpi = float(china_macro.get("cpi_monthly", 0.0))
        pmi_prev = float(china_macro.get("pmi_prev", pmi))
        vix = float(global_proxy.get("vix", 0.0))
        dxy_change = float(global_proxy.get("dxy_20d_change", 0.0))

        if gold_1d > tech_1d + 0.01:
            lines.append(
                f"[能源与地缘] 防守资产相对更强，今天更要盯原油、黄金和美元是否形成联动，市场可能在交易地缘或通胀扰动。"
            )
        elif tech_1d > gold_1d + 0.01:
            lines.append(
                "[能源与地缘] 风险资产相对更稳，说明能源和地缘风险暂未主导盘面，主线仍更偏成长和风险偏好修复。"
            )
        else:
            lines.append(
                "[能源与地缘] 目前能源与避险方向没有形成绝对主线，盘中重点看原油链和黄金是否突然放量接管叙事。"
            )

        if vix >= 22 or dxy_change > 0.02:
            lines.append(
                f"[国际局势] 波动率/美元代理偏强，外盘更像在交易避险与流动性收紧，跨市场仓位需要更重视回撤控制。"
            )
        elif vix and vix <= 18 and dxy_change <= 0:
            lines.append(
                "[国际局势] 波动率和美元代理都不强，外部环境对风险资产的压制暂时有限。"
            )
        else:
            lines.append(
                "[国际局势] 外部风险处于可跟踪但未失控状态，重点看今晚外盘是否把短线波动放大成新主线。"
            )

        pmi_trend = "回升" if pmi >= pmi_prev else "回落"
        if pmi < 50:
            lines.append(
                f"[中国宏观] PMI {pmi:.1f}、景气仍在荣枯线下，晨报更偏向寻找逆势走强和政策托底方向。"
            )
        else:
            lines.append(
                f"[中国宏观] PMI {pmi:.1f}、较前值 {pmi_trend}，国内主线更适合看顺周期和成长扩散。"
            )
        lines.append(
            f"[通胀与流动性] CPI 月率 {cpi:.1f}%，结合 LPR 和风险偏好变化，今天重点观察市场是在交易复苏还是交易通胀。"
        )

        if domestic_1d > offshore_1d + 0.01:
            lines.append(
                f"[资金风格] 国内方向相对更稳（国内 {_format_pct(domestic_1d)} vs 海外 {_format_pct(offshore_1d)}），本土确定性资产更容易获得关注。"
            )
        elif offshore_1d > domestic_1d + 0.01:
            lines.append(
                f"[资金风格] 海外弹性方向相对更强（国内 {_format_pct(domestic_1d)} vs 海外 {_format_pct(offshore_1d)}），说明资金更愿意博弈外盘成长或离岸修复。"
            )
        else:
            lines.append("[资金风格] 国内和海外强弱差不大，今天更可能是结构轮动，而不是单边总攻。")

        return lines[:6]

    def _avg(self, rows: List[Any], key: str) -> float:
        if not rows:
            return 0.0
        return sum(float(_value(item, key, 0.0)) for item in rows) / len(rows)
