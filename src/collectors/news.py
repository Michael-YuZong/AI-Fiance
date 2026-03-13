"""RSS and offline fallback headlines for briefing generation."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import quote_plus

import feedparser
import requests

from src.collectors.base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_yaml

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


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
    "财联社": "site:cls.cn",
    "证券时报": "site:stcn.com",
}


def _contains_cjk(value: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in value)


def _contains_ascii_alpha(value: str) -> bool:
    return any(char.isascii() and char.isalpha() for char in value)


class NewsCollector(BaseCollector):
    """Collect market-facing headlines from RSS and offline market proxies."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="NewsCollector")
        self.feeds_path = resolve_project_path(self.config.get("news_feeds_file", "config/news_feeds.yaml"))

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

        items: List[Dict[str, str]] = []
        for label, url in self._topic_queries(cleaned, preferred, recent_days):
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
            for entry in parsed.entries[:2]:
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

        ranked = self._rank_items(items, preferred, query_keywords=cleaned)
        return self._diversify_items(ranked, limit)

    def get_stock_news(self, symbol: str, limit: int = 10) -> List[Dict[str, str]]:
        """Fetch per-stock news from akshare (A-share only)."""
        if ak is None:
            return []
        fetcher = getattr(ak, "stock_news_em", None)
        if not callable(fetcher):
            return []
        try:
            frame = self.cached_call(
                f"news:stock:{symbol}",
                fetcher,
                symbol=symbol,
                ttl_hours=2,
                prefer_stale=True,
            )
        except Exception:
            return []
        if frame is None or frame.empty:
            return []
        title_col = next((c for c in frame.columns if "新闻标题" in c or "title" in c.lower()), None)
        source_col = next((c for c in frame.columns if "新闻来源" in c or "来源" in c or "source" in c.lower()), None)
        time_col = next((c for c in frame.columns if "发布时间" in c or "时间" in c or "date" in c.lower()), None)
        if not title_col:
            return []
        items: List[Dict[str, str]] = []
        for _, row in frame.head(limit).iterrows():
            title = str(row.get(title_col, "")).strip()
            if not title:
                continue
            items.append({
                "category": "stock_announcement",
                "title": title,
                "source": str(row.get(source_col, "东方财富")).strip() if source_col else "东方财富",
                "configured_source": "东方财富",
                "must_include": False,
                "published_at": self._normalize_date_text(row.get(time_col)) if time_col else "",
                "link": str(row.get("新闻链接", "")).strip() if "新闻链接" in frame.columns else "",
            })
        return items

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
        note = "新闻主线来自 RSS 聚合，按配置抓取最近条目。"
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
    ) -> List[Dict[str, str]]:
        preferred_lower = [item.lower() for item in preferred_sources]
        query_terms = [str(item).strip() for item in (query_keywords or []) if str(item).strip()]
        deduped: List[Dict[str, str]] = []
        seen_titles = set()
        for item in items:
            title = item.get("title", "")
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            deduped.append(item)

        def _score(item: Dict[str, str]) -> tuple[int, int, int, str]:
            source = (item.get("source") or item.get("configured_source") or "").lower()
            score = 0
            if item.get("must_include"):
                score += 4
            if any(name in source for name in preferred_lower):
                score += 3
            if item.get("configured_source") and item["configured_source"].lower() in source:
                score += 1
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
            return (-score, -relevance, len(item.get("title", "")), item.get("category", ""))

        return sorted(deduped, key=_score)

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
        if en_terms or any(source in {"Reuters", "Bloomberg", "Financial Times"} for source in preferred_sources):
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
            for source in preferred_sources[:5]:
                domain = SOURCE_DOMAIN_HINTS.get(source)
                if not domain:
                    continue
                for anchor in anchor_terms:
                    scoped_query = f"{domain} {anchor} when:{recent_days}d"
                    scoped_url = self._google_news_search_url(scoped_query, hl=hl, gl=gl, ceid=ceid)
                    if scoped_url in seen:
                        continue
                    queries.append((source, scoped_url))
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
