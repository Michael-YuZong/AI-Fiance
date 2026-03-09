"""RSS and offline fallback headlines for briefing generation."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import feedparser
import requests

from src.collectors.base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_yaml
from src.utils.market import format_pct


def _value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


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
                )
            except Exception as exc:
                errors.append(str(feed.get("name", feed.get("category", "news"))))
                continue
            for entry in parsed.entries[:max_items_per_feed]:
                source = getattr(entry, "source", {})
                if isinstance(source, Mapping):
                    source_name = str(source.get("title", "")).strip()
                else:
                    source_name = str(source or "").strip()
                live_items.append(
                    {
                        "category": str(feed.get("category", "market")),
                        "title": str(getattr(entry, "title", "")).strip(),
                        "source": source_name or str(getattr(entry, "publisher", "") or feed.get("source", "") or feed.get("name", "")).strip(),
                        "configured_source": str(feed.get("source", "")).strip(),
                        "must_include": bool(feed.get("must_include", False)),
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
                "lines": self._live_lines(selected_items),
                "source_list": sorted(required_present),
                "note": self._live_note(preferred, missing_required),
            }

        return {
            "mode": "proxy",
            "items": [],
            "lines": self._fallback_lines(rows, dict(china_macro or {}), dict(global_proxy or {})),
            "note": "实时 RSS 暂不可用，已回退到本地宏观与市场代理主线。"
            + (f" 当前有 {len(errors)} 个新闻源未连通。" if errors else ""),
        }

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
    ) -> List[Dict[str, str]]:
        preferred_lower = [item.lower() for item in preferred_sources]
        deduped: List[Dict[str, str]] = []
        seen_titles = set()
        for item in items:
            title = item.get("title", "")
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            deduped.append(item)

        def _score(item: Dict[str, str]) -> tuple[int, int, str]:
            source = (item.get("source") or item.get("configured_source") or "").lower()
            score = 0
            if item.get("must_include"):
                score += 4
            if any(name in source for name in preferred_lower):
                score += 3
            if item.get("configured_source") and item["configured_source"].lower() in source:
                score += 1
            return (-score, len(item.get("title", "")), item.get("category", ""))

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
                f"[资金风格] 国内方向相对更稳（国内 {format_pct(domestic_1d)} vs 海外 {format_pct(offshore_1d)}），本土确定性资产更容易获得关注。"
            )
        elif offshore_1d > domestic_1d + 0.01:
            lines.append(
                f"[资金风格] 海外弹性方向相对更强（国内 {format_pct(domestic_1d)} vs 海外 {format_pct(offshore_1d)}），说明资金更愿意博弈外盘成长或离岸修复。"
            )
        else:
            lines.append("[资金风格] 国内和海外强弱差不大，今天更可能是结构轮动，而不是单边总攻。")

        return lines[:6]

    def _avg(self, rows: List[Any], key: str) -> float:
        if not rows:
            return 0.0
        return sum(float(_value(item, key, 0.0)) for item in rows) / len(rows)
