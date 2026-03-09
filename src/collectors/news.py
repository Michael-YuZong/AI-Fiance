"""RSS and offline fallback headlines for briefing generation."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional

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
    ) -> Dict[str, Any]:
        """Return live headlines when available, otherwise fall back to proxy narratives."""
        rows = list(snapshots or [])
        feeds = load_yaml(self.feeds_path, default={"feeds": []}) or {"feeds": []}
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
            for entry in parsed.entries[:2]:
                source = getattr(entry, "source", {})
                if isinstance(source, Mapping):
                    source_name = str(source.get("title", "")).strip()
                else:
                    source_name = str(source or "").strip()
                live_items.append(
                    {
                        "category": str(feed.get("category", "market")),
                        "title": str(getattr(entry, "title", "")).strip(),
                        "source": source_name or str(getattr(entry, "publisher", "") or feed.get("name", "")).strip(),
                        "link": str(getattr(entry, "link", "")).strip(),
                    }
                )
                if len(live_items) >= limit:
                    break
            if len(live_items) >= limit:
                break

        if live_items:
            return {
                "mode": "live",
                "items": live_items[:limit],
                "lines": self._live_lines(live_items[:limit]),
                "note": "新闻主线来自 RSS 聚合，按配置抓取最近条目。",
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
            timeout=6,
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
