"""Tests for news collector live fallback behavior."""

from __future__ import annotations

import json
import feedparser

from src.collectors.news import NewsCollector


def test_news_collector_falls_back_to_proxy_lines():
    collector = NewsCollector({"news_feeds_file": "config/does_not_exist.yaml"})
    report = collector.collect(
        snapshots=[
            {"sector": "科技", "region": "US", "return_1d": -0.02},
            {"sector": "黄金", "region": "US", "return_1d": 0.01},
            {"sector": "电网", "region": "CN", "return_1d": 0.005},
        ],
        china_macro={"pmi": 49.0, "pmi_prev": 49.2, "cpi_monthly": 0.4},
        global_proxy={},
    )
    assert report["lines"]
    assert any("能源与地缘" in line for line in report["lines"])


def test_news_collector_prioritizes_preferred_source(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: ["Reuters"]
  max_items_per_feed: 2
feeds:
  - category: "global_macro"
    name: "Reuters Feed"
    source: "Reuters"
    must_include: true
    url: "https://example.com/reuters"
  - category: "global_macro"
    name: "Other Feed"
    source: "Other"
    url: "https://example.com/other"
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if "reuters" in cache_key:
            return feedparser.parse(
                """<rss><channel><item><title>Reuters headline</title><link>https://example.com/r</link><source>Reuters</source></item></channel></rss>"""
            )
        return feedparser.parse(
            """<rss><channel><item><title>Other headline</title><link>https://example.com/o</link><source>Other</source></item></channel></rss>"""
        )

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    report = collector.collect(preferred_sources=["Reuters"])
    assert report["mode"] == "live"
    assert "Reuters" in report["lines"][0]
    assert "优先源" in report["note"]
    assert len(report["all_items"]) == 2


def test_news_collector_search_by_keywords_returns_ranked_items(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters", "财联社"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if "reuters" in cache_key.lower():
            return feedparser.parse(
                """<rss><channel><item><title>Defense orders rise</title><link>https://example.com/r</link><source>Reuters</source></item></channel></rss>"""
            )
        return feedparser.parse(
            """<rss><channel><item><title>军工订单加速</title><link>https://example.com/c</link><source>财联社</source></item></channel></rss>"""
        )

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector.search_by_keywords(["军工", "订单"], preferred_sources=["Reuters"], limit=4)
    assert items
    assert any("Defense orders rise" in item["title"] for item in items)


def test_news_collector_search_by_keywords_uses_english_alias_queries_for_foreign_sources(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})
    cache_keys = []

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        cache_keys.append(cache_key)
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    collector.search_by_keywords(["腾讯控股", "Tencent"], preferred_sources=["Reuters"], limit=4, recent_days=30)
    assert any("hl=en-US" in cache_key and "Tencent" in cache_key for cache_key in cache_keys)


def test_news_collector_search_by_keywords_skips_en_locale_for_cjk_only_terms(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})
    cache_keys = []

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        cache_keys.append(cache_key)
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    collector.search_by_keywords(["华电新能", "电网"], preferred_sources=["Reuters"], limit=4, recent_days=30)
    assert cache_keys
    assert not any("hl=en-US" in cache_key for cache_key in cache_keys)


def test_news_collector_search_by_keywords_supports_sec_and_ir_source_hints(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Investor Relations", "SEC"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})
    cache_keys = []

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        cache_keys.append(cache_key)
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    collector.search_by_keywords(["Coinbase", "earnings", "results"], preferred_sources=["Investor Relations", "SEC"], limit=4, recent_days=30)
    assert any("sec.gov" in cache_key.lower() for cache_key in cache_keys)
    assert any("Investor+Relations" in cache_key or "Investor%20Relations" in cache_key for cache_key in cache_keys)


def test_news_collector_search_by_keywords_supports_company_site_hints(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})
    cache_keys = []

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        cache_keys.append(cache_key)
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    collector.search_by_keywords(
        ["贵州茅台", "投资者关系"],
        preferred_sources=["site:ir.example.com"],
        limit=4,
        recent_days=30,
    )
    assert any("site%3Air.example.com" in cache_key or "site:ir.example.com" in cache_key for cache_key in cache_keys)


def test_news_collector_search_by_keywords_supports_cn_exchange_and_cninfo_hints(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["CNINFO", "SSE", "SZSE"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})
    cache_keys = []

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        cache_keys.append(cache_key)
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    collector.search_by_keywords(["农发种业", "年报", "业绩"], preferred_sources=["CNINFO", "SSE", "SZSE"], limit=4, recent_days=30)
    joined = "\n".join(cache_keys).lower()
    assert "cninfo.com.cn" in joined
    assert "sse.com.cn" in joined
    assert "szse.cn" in joined


def test_news_collector_search_by_keywords_ranks_more_relevant_same_source_item_first(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if "reuters.com" in cache_key.lower():
            return feedparser.parse(
                """
<rss><channel>
  <item><title>China's largest provincial economy vows to reshape industry with AI - Reuters</title><link>https://example.com/generic</link><source>Reuters</source></item>
  <item><title>Trump administration debates allowing Tencent to keep its gaming stakes, FT reports - Reuters</title><link>https://example.com/tencent</link><source>Reuters</source></item>
</channel></rss>
""".strip()
            )
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector.search_by_keywords(["Tencent", "gaming stakes"], preferred_sources=["Reuters"], limit=4, recent_days=30)
    assert items
    assert "gaming stakes" in items[0]["title"].lower()


def test_news_collector_prefers_fresher_item_when_relevance_is_similar(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if "reuters.com" in cache_key.lower():
            return feedparser.parse(
                """
<rss><channel>
  <item>
    <title>Tencent gaming business outlook improves - Reuters</title>
    <link>https://example.com/old</link>
    <source>Reuters</source>
    <pubDate>Mon, 23 Mar 2026 08:00:00 GMT</pubDate>
  </item>
  <item>
    <title>Tencent gaming business outlook improves on new release cycle - Reuters</title>
    <link>https://example.com/new</link>
    <source>Reuters</source>
    <pubDate>Sun, 29 Mar 2026 08:00:00 GMT</pubDate>
  </item>
</channel></rss>
""".strip()
            )
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector.search_by_keywords(["Tencent", "gaming"], preferred_sources=["Reuters"], limit=4, recent_days=14)
    assert items
    assert "new release cycle" in items[0]["title"].lower()
    assert items[0]["freshness_bucket"] in {"fresh", "recent"}


def test_news_collector_dedupes_near_duplicate_titles_with_source_suffix(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if "reuters.com" in cache_key.lower():
            return feedparser.parse(
                """
<rss><channel>
  <item><title>Tencent gaming outlook improves - Reuters</title><link>https://example.com/1</link><source>Reuters</source></item>
  <item><title>Tencent gaming outlook improves</title><link>https://example.com/2</link><source>Reuters</source></item>
</channel></rss>
""".strip()
            )
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector.search_by_keywords(["Tencent", "gaming"], preferred_sources=["Reuters"], limit=4, recent_days=14)
    assert len(items) == 1


def test_news_collector_search_by_keyword_groups_merges_multiple_query_buckets(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_search(self, keywords, preferred_sources=None, limit=6, recent_days=7):  # noqa: ANN001, ARG001
        joined = " ".join(keywords)
        if "半导体 芯片" in joined:
            return [{"title": "China chip equipment demand rises - Reuters", "source": "Reuters", "category": "topic_search"}]
        if "台积电 capex" in joined:
            return [{"title": "TSMC raises capex on AI demand - Reuters", "source": "Reuters", "category": "topic_search"}]
        return []

    monkeypatch.setattr(NewsCollector, "search_by_keywords", fake_search)
    items = collector.search_by_keyword_groups(
        [["半导体", "芯片"], ["台积电", "capex"]],
        preferred_sources=["Reuters"],
        limit=4,
        recent_days=30,
    )
    assert len(items) == 2
    assert any("TSMC raises capex" in item["title"] for item in items)


def test_news_collector_search_by_keywords_filters_generic_or_stale_market_pages(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if "reuters.com" in cache_key.lower():
            return feedparser.parse(
                """
<rss><channel>
  <item><title>Global Market Headlines | Breaking Stock Market News - Reuters</title><link>https://example.com/generic</link><pubDate>Mon, 06 Jun 2016 02:15:43 GMT</pubDate><source>Reuters</source></item>
  <item><title>PBOC signals ample liquidity support for domestic demand - Reuters</title><link>https://example.com/policy</link><pubDate>Sun, 29 Mar 2026 02:15:43 GMT</pubDate><source>Reuters</source></item>
</channel></rss>
""".strip()
            )
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector.search_by_keywords(["流动性", "内需"], preferred_sources=["Reuters"], limit=4, recent_days=7)
    assert len(items) == 1
    assert "Global Market Headlines" not in items[0]["title"]
    assert "ample liquidity support" in items[0]["title"]


def test_news_collector_search_by_keywords_prioritizes_first_party_over_media(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["CNINFO", "SSE", "SZSE"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        return feedparser.parse(
            """
<rss><channel>
  <item>
    <title>农发种业披露年报，利润继续改善</title>
    <link>https://example.com/cninfo</link>
    <source>CNINFO</source>
    <pubDate>Sun, 30 Mar 2026 08:00:00 GMT</pubDate>
  </item>
  <item>
    <title>农发种业年报解读：媒体关注盈利修复</title>
    <link>https://example.com/media</link>
    <source>证券时报</source>
    <pubDate>Sun, 30 Mar 2026 08:00:00 GMT</pubDate>
  </item>
</channel></rss>
""".strip()
        )

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    monkeypatch.setattr(
        collector,
        "_topic_queries",
        lambda keywords, preferred_sources, recent_days: [("topic_search", "https://example.com/feed")],  # noqa: ARG005
    )
    items = collector.search_by_keywords(["农发种业", "年报", "业绩"], preferred_sources=["CNINFO", "SSE", "SZSE"], limit=4, recent_days=30)
    assert items
    assert items[0]["source"] == "CNINFO"
    assert "年报" in items[0]["title"]


def test_news_collector_search_by_keywords_prefers_materially_fresher_direct_hit_over_older_first_party_notice(
    tmp_path, monkeypatch
):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["CNINFO", "SSE", "SZSE"]
  required_sources: []
  max_items_per_feed: 2
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path)})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        return feedparser.parse(
            """
<rss><channel>
  <item>
    <title>农发种业披露年报，利润继续改善</title>
    <link>https://example.com/cninfo-old</link>
    <source>CNINFO</source>
    <pubDate>Mon, 23 Mar 2026 08:00:00 GMT</pubDate>
  </item>
  <item>
    <title>农发种业利润改善，市场聚焦新品种推广节奏</title>
    <link>https://example.com/media-fresh</link>
    <source>证券时报</source>
    <pubDate>Sun, 30 Mar 2026 08:00:00 GMT</pubDate>
  </item>
</channel></rss>
""".strip()
        )

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    monkeypatch.setattr(
        collector,
        "_topic_queries",
        lambda keywords, preferred_sources, recent_days: [("topic_search", "https://example.com/feed")],  # noqa: ARG005
    )
    items = collector.search_by_keywords(["农发种业", "利润", "改善"], preferred_sources=["CNINFO", "SSE", "SZSE"], limit=4, recent_days=14)
    assert items
    assert items[0]["source"] == "证券时报"
    assert items[0]["freshness_bucket"] in {"fresh", "recent"}


def test_news_collector_search_by_keywords_can_be_disabled(tmp_path, monkeypatch):
    config_path = tmp_path / "news.yaml"
    config_path.write_text(
        """
preferences:
  preferred_sources: ["Reuters"]
feeds: []
""".strip(),
        encoding="utf-8",
    )
    collector = NewsCollector({"news_feeds_file": str(config_path), "news_topic_search_enabled": False})

    def fail_cached_call(*args, **kwargs):  # noqa: ANN001, ARG001
        raise AssertionError("cached_call should not run when topic search is disabled")

    monkeypatch.setattr(collector, "cached_call", fail_cached_call)
    items = collector.search_by_keywords(["黄金", "避险"], preferred_sources=["Reuters"], limit=4, recent_days=30)
    assert items == []


def test_news_collector_get_stock_news_prefers_tushare(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        if api_name == "stock_basic":
            return __import__("pandas").DataFrame([{"ts_code": "600519.SH", "symbol": "600519", "name": "贵州茅台"}])
        if api_name == "news":
            return __import__("pandas").DataFrame(
                [
                    {
                        "title": "贵州茅台发布年报，分红继续提升",
                        "src": "证券时报",
                        "pub_time": "20260310",
                        "url": "https://example.com/mt",
                    },
                    {
                        "title": "新能源板块震荡",
                        "src": "财联社",
                        "pub_time": "20260310",
                        "url": "https://example.com/other",
                    },
                ]
            )
        return __import__("pandas").DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "search_by_keywords", lambda *args, **kwargs: [])
    items = collector.get_stock_news("600519", limit=5)
    assert items
    assert items[0]["title"] == "贵州茅台发布年报，分红继续提升"
    assert items[0]["source"] == "证券时报"


def test_news_collector_stock_identity_includes_company_website(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        import pandas as pd

        if api_name == "stock_basic":
            return pd.DataFrame([{"ts_code": "600519.SH", "symbol": "600519", "name": "贵州茅台"}])
        if api_name == "stock_company":
            return pd.DataFrame([{"website": "https://ir.example.com"}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    profile = collector._stock_identity("600519")

    assert profile["company_website"] == "https://ir.example.com"
    assert collector._stock_site_search_hints(profile) == ["site:ir.example.com"]


def test_news_collector_get_stock_news_merges_first_party_search_lane(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_search_by_keywords(keywords, preferred_sources=None, limit=6, recent_days=7):  # noqa: ANN001, ARG001
        return [
            {
                "category": "topic_search",
                "title": "贵州茅台公告年报，回款和分红节奏改善",
                "source": "CNINFO",
                "configured_source": "CNINFO",
                "must_include": False,
                "published_at": "2026-03-30T08:00:00",
                "link": "https://example.com/cninfo",
            }
        ]

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        if api_name == "stock_basic":
            return __import__("pandas").DataFrame([{"ts_code": "600519.SH", "symbol": "600519", "name": "贵州茅台"}])
        if api_name == "news":
            return __import__("pandas").DataFrame(
                [
                    {
                        "title": "贵州茅台发布年报，分红继续提升",
                        "src": "证券时报",
                        "pub_time": "20260310",
                        "url": "https://example.com/mt",
                    }
                ]
            )
        return __import__("pandas").DataFrame()

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "search_by_keywords", fake_search_by_keywords)
    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    items = collector.get_stock_news("600519", limit=5)
    assert items
    assert items[0]["source"] == "CNINFO"
    assert "公告年报" in items[0]["title"]


def test_news_collector_search_stock_intelligence_uses_company_site_hint(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})
    captured = {}

    def fake_search_by_keyword_groups(keyword_groups, preferred_sources=None, limit=6, recent_days=7):  # noqa: ANN001
        captured["preferred_sources"] = list(preferred_sources or [])
        return [
            {
                "category": "stock_live_intelligence",
                "title": "贵州茅台公司官网更新投资者关系信息",
                "source": "公司官网",
                "configured_source": "company_site",
                "must_include": False,
                "published_at": "2026-03-31T10:00:00",
                "link": "https://ir.example.com/news/1",
            }
        ]

    monkeypatch.setattr(collector, "search_by_keyword_groups", fake_search_by_keyword_groups)

    items = collector._search_stock_intelligence(
        {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台", "company_website": "https://ir.example.com"},
        limit=5,
    )

    assert any(str(item).startswith("site:ir.example.com") for item in captured["preferred_sources"])
    assert items
    assert items[0]["source_note"] == "official_site_search"


def test_news_collector_stock_query_groups_cover_company_level_intelligence_tokens() -> None:
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    groups = collector._stock_query_groups({"symbol": "600519", "name": "贵州茅台"})

    assert ["贵州茅台", "回购"] in groups
    assert ["贵州茅台", "定增"] in groups
    assert ["贵州茅台", "重组"] in groups
    assert ["贵州茅台", "股权激励"] in groups
    assert ["贵州茅台", "互动易"] in groups
    assert ["贵州茅台", "投资者关系"] in groups
    assert ["贵州茅台", "官网"] in groups


def test_news_collector_stock_official_query_groups_cover_ir_and_disclosure_tokens() -> None:
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    groups = collector._stock_official_query_groups({"symbol": "600519", "name": "贵州茅台"})

    assert ["贵州茅台", "投资者关系"] in groups
    assert ["贵州茅台", "官网"] in groups
    assert ["贵州茅台", "600519", "业绩说明会"] in groups


def test_news_collector_classifies_stock_search_item_from_company_ir_site() -> None:
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    row = collector._classify_stock_search_item(
        {
            "title": "贵州茅台投资者关系活动记录表",
            "source": "",
            "configured_source": "topic_search",
            "link": "https://ir.kweichowmoutai.com/cmscontent/123.html",
        },
        {
            "symbol": "600519",
            "name": "贵州茅台",
            "company_website": "https://www.kweichowmoutai.com/",
            "ir_website": "https://ir.kweichowmoutai.com/",
        },
    )

    assert row["configured_source"] == "Investor Relations::search"
    assert row["source_note"] == "official_site_search"


def test_news_collector_get_stock_news_merges_structured_tushare_intelligence(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        import pandas as pd

        if api_name == "forecast":
            return pd.DataFrame(
                [
                    {
                        "ann_date": "20260330",
                        "type": "预增",
                        "summary": "净利润同比明显改善",
                    }
                ]
            )
        if api_name == "dividend":
            return pd.DataFrame(
                [
                    {
                        "ann_date": "20260329",
                        "div_proc": "董事会预案",
                        "cash_div_tax": 27.6,
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "search_by_keywords", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    items = collector.get_stock_news("600519", limit=5)

    assert items
    assert any("业绩预告" in item["title"] for item in items)
    assert any("分红方案" in item["title"] for item in items)
    assert all(item["source"] == "Tushare" for item in items)
    assert all(item["link"] == "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519" for item in items)


def test_news_collector_get_stock_news_merges_tushare_irm_qa_items(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "002122", "ts_code": "002122.SZ", "name": "汇洲智能"}

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001, ARG001
        import pandas as pd

        return pd.DataFrame()

    def fake_irm_snapshot(api_name: str, **kwargs: object):  # noqa: ANN001
        import pandas as pd

        assert api_name == "irm_qa_sz"
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260402",
                    "q": "公司是否和幻方量化有合作？",
                    "a": "您好，经核查，公司及控股公司与幻方量化无合作关系，具体信息请以公开披露为准。",
                    "pub_time": "2026-04-02 15:02:17",
                    "industry": "制造业",
                }
            ]
        )

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_search_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr(collector, "_ts_irm_qa_snapshot", fake_irm_snapshot)

    items = collector.get_stock_news("002122", limit=5)

    assert items
    irm_item = next(item for item in items if item["configured_source"] == "Tushare::irm_qa_sz")
    assert "互动平台问答" in irm_item["title"]
    assert irm_item["note"] == "投资者关系/路演纪要"
    assert irm_item["link"] == "https://irm.cninfo.com.cn/"


def test_news_collector_get_stock_news_keeps_irm_lane_under_tight_limit(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml", "stock_news_limit": 2})

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "002122", "ts_code": "002122.SZ", "name": "汇洲智能"}

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(
        collector,
        "_official_stock_intelligence",
        lambda *args, **kwargs: [  # noqa: ARG005
            {
                "category": "stock_announcement",
                "title": "汇洲智能披露股东大会决议公告",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "source_note": "official_direct",
                "published_at": "2026-04-02T09:30:00",
                "link": "https://www.cninfo.com.cn/example1",
            },
            {
                "category": "stock_announcement",
                "title": "汇洲智能披露补充公告",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "source_note": "official_direct",
                "published_at": "2026-04-01T09:30:00",
                "link": "https://www.cninfo.com.cn/example2",
            },
        ],
    )
    monkeypatch.setattr(
        collector,
        "_structured_stock_intelligence",
        lambda *args, **kwargs: [  # noqa: ARG005
            {
                "category": "stock_structured_intelligence",
                "title": "汇洲智能互动平台问答：公司是否和幻方量化有合作；回复称经核查无合作关系…",
                "source": "Tushare",
                "configured_source": "Tushare::irm_qa_sz",
                "source_note": "structured_disclosure",
                "note": "投资者关系/路演纪要",
                "published_at": "2026-03-20T15:02:17",
                "link": "https://irm.cninfo.com.cn/",
            }
        ],
    )
    monkeypatch.setattr(collector, "_search_stock_intelligence", lambda *args, **kwargs: [])

    items = collector.get_stock_news("002122", limit=2)

    assert len(items) == 2
    assert any(item["configured_source"] == "Tushare::irm_qa_sz" for item in items)


def test_news_collector_get_stock_news_respects_configured_structured_api_subset(monkeypatch):
    collector = NewsCollector(
        {
            "news_feeds_file": "config/news_feeds.yaml",
            "structured_stock_intelligence_apis": ["forecast", "dividend"],
        }
    )
    called: list[str] = []

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        import pandas as pd

        called.append(api_name)
        if api_name == "forecast":
            return pd.DataFrame([{"ann_date": "20260330", "type": "预增", "summary": "净利润同比明显改善"}])
        if api_name == "dividend":
            return pd.DataFrame([{"ann_date": "20260329", "div_proc": "董事会预案", "cash_div_tax": 27.6}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "search_by_keywords", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    items = collector.get_stock_news("600519", limit=5)

    assert items
    assert "forecast" in called
    assert "dividend" in called
    assert "express" not in called
    assert "stk_holdertrade" not in called
    assert "disclosure_date" not in called


def test_news_collector_get_stock_news_skips_tushare_news_backfill_when_direct_or_structured_lane_is_recent(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})
    called: list[str] = []

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_structured(*args, **kwargs):  # noqa: ANN001, ARG001
        return [
            {
                "title": "贵州茅台分红方案：董事会预案；现金分红 27.6",
                "summary": "董事会预案；现金分红 27.6",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
                "source": "Tushare",
                "configured_source": "Tushare dividend",
                "source_note": "structured_disclosure",
                "published_at": "2026-03-31 09:00:00",
                "freshness_bucket": "fresh",
                "category": "company_notice",
            }
        ]

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        called.append(api_name)
        import pandas as pd

        return pd.DataFrame()

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_structured_stock_intelligence", fake_structured)
    monkeypatch.setattr(collector, "_search_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    items = collector.get_stock_news("600519", limit=5)

    assert items
    assert "news" not in called
    assert "major_news" not in called


def test_news_collector_get_stock_news_structured_lane_survives_without_media_news(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600313", "ts_code": "600313.SH", "name": "农发种业"}

    def fake_ts_call(api_name: str, **kwargs: object):  # noqa: ANN001
        import pandas as pd

        if api_name == "stk_holdertrade":
            return pd.DataFrame(
                [
                    {
                        "ann_date": "20260331",
                        "holder_name": "控股股东",
                        "in_de": "增持",
                        "change_ratio": 0.8,
                    }
                ]
            )
        if api_name == "disclosure_date":
            return pd.DataFrame(
                [
                    {
                        "pre_date": "20260330",
                        "actual_date": "20260331",
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "search_by_keywords", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    items = collector.get_stock_news("600313", limit=5)

    assert items
    assert any("股东变动" in item["title"] for item in items)
    assert any("披露日历" in item["title"] for item in items)


def test_news_collector_get_stock_news_skips_search_when_recent_structured_intelligence_is_sufficient(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})
    calls = {"search": 0}

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_structured(profile, *, start_date, end_date, limit):  # noqa: ANN001, ARG001
        return [
            {
                "category": "stock_structured_intelligence",
                "title": "贵州茅台业绩预告：净利润同比明显改善",
                "source": "Tushare",
                "configured_source": "Tushare::forecast",
                "source_note": "structured_disclosure",
                "must_include": False,
                "published_at": "2026-03-31T09:30:00",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
            },
            {
                "category": "stock_structured_intelligence",
                "title": "贵州茅台分红方案：董事会预案；现金分红 27.6",
                "source": "Tushare",
                "configured_source": "Tushare::dividend",
                "source_note": "structured_disclosure",
                "must_include": False,
                "published_at": "2026-03-30T09:30:00",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
            },
        ]

    def fake_search(*args, **kwargs):  # noqa: ANN001, ARG001
        calls["search"] += 1
        return []

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_structured_stock_intelligence", fake_structured)
    monkeypatch.setattr(collector, "_search_stock_intelligence", fake_search)

    items = collector.get_stock_news("600519", limit=5)

    assert items
    assert any("业绩预告" in item["title"] for item in items)
    assert calls["search"] == 0


def test_news_collector_get_stock_news_prefers_cninfo_direct_and_skips_search_when_recent_direct_is_sufficient(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})
    calls = {"search": 0}

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_direct(profile, *, start_date, end_date, limit):  # noqa: ANN001, ARG001
        return [
            {
                "category": "stock_announcement",
                "title": "贵州茅台2026年一季报预告",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "must_include": False,
                "published_at": "2026-03-31T09:30:00",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
            }
        ]

    def fake_search(*args, **kwargs):  # noqa: ANN001, ARG001
        calls["search"] += 1
        return []

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", fake_direct)
    monkeypatch.setattr(collector, "_ts_call", lambda *args, **kwargs: __import__("pandas").DataFrame())
    monkeypatch.setattr(collector, "_search_stock_intelligence", fake_search)

    items = collector.get_stock_news("600519", limit=5)

    assert items
    assert items[0]["source"] == "CNINFO"
    assert items[0]["configured_source"] == "CNINFO::direct"
    assert calls["search"] == 0


def test_cninfo_direct_intelligence_preserves_announcement_type_hint(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})
    profile = {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    monkeypatch.setattr(
        collector,
        "cached_call",
        lambda cache_key, fetcher, *args, **kwargs: [
            {
                "announcementTitle": "关于举办投资者关系活动的公告",
                "secName": "贵州茅台",
                "secCode": "600519",
                "announcementTime": "2026-03-31 09:30:00",
                "announcementTypeName": "投资者关系活动记录表",
                "adjunctUrl": "/finalpage/2026-03-31/123.PDF",
            }
        ],
    )

    items = collector._cninfo_direct_intelligence(profile, start_date="20260301", end_date="20260331", limit=5)

    assert items
    assert items[0]["source"] == "CNINFO"
    assert items[0]["source_note"] == "official_direct"
    assert items[0]["note"] == "投资者关系/路演纪要"


def test_news_collector_get_stock_news_falls_back_to_search_when_direct_lane_fails_and_primary_items_are_thin(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600313", "ts_code": "600313.SH", "name": "农发种业"}

    def fail_direct(*args, **kwargs):  # noqa: ANN001, ARG001
        raise RuntimeError("cninfo unavailable")

    def fake_search(profile, *, limit):  # noqa: ANN001, ARG001
        return [
            {
                "category": "stock_live_intelligence",
                "title": "农发种业互动易回应制种扩张节奏",
                "source": "财联社",
                "configured_source": "财联社",
                "must_include": False,
                "published_at": "2026-03-31T10:00:00",
                "link": "https://example.com/seed",
            }
        ]

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_official_stock_intelligence", fail_direct)
    monkeypatch.setattr(collector, "_ts_call", lambda *args, **kwargs: __import__("pandas").DataFrame())
    monkeypatch.setattr(collector, "_search_stock_intelligence", fake_search)

    items = collector.get_stock_news("600313", limit=5)

    assert items
    assert any("互动易" in item["title"] for item in items)


def test_news_collector_get_stock_news_structured_only_runtime_skips_direct_and_search(monkeypatch):
    collector = NewsCollector(
        {
            "news_feeds_file": "config/news_feeds.yaml",
            "stock_news_runtime_mode": "structured_only",
            "structured_stock_intelligence_apis": ["forecast", "irm_qa_sh"],
        }
    )
    calls = {"direct": 0, "search": 0}

    def fake_stock_identity(symbol: str):  # noqa: ARG001
        return {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"}

    def fake_structured(profile, *, start_date, end_date, limit):  # noqa: ANN001, ARG001
        return [
            {
                "category": "stock_structured_intelligence",
                "title": "贵州茅台业绩预告：净利润同比明显改善",
                "source": "Tushare",
                "configured_source": "Tushare::forecast",
                "source_note": "structured_disclosure",
                "must_include": False,
                "published_at": "2026-04-03T09:30:00",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
            }
        ]

    def fake_direct(*args, **kwargs):  # noqa: ANN001, ARG001
        calls["direct"] += 1
        return []

    def fake_search(*args, **kwargs):  # noqa: ANN001, ARG001
        calls["search"] += 1
        return []

    monkeypatch.setattr(collector, "_stock_identity", fake_stock_identity)
    monkeypatch.setattr(collector, "_structured_stock_intelligence", fake_structured)
    monkeypatch.setattr(collector, "_official_stock_intelligence", fake_direct)
    monkeypatch.setattr(collector, "_search_stock_intelligence", fake_search)

    items = collector.get_stock_news("600519", limit=5)

    assert items
    assert any(item["configured_source"] == "Tushare::forecast" for item in items)
    assert calls["direct"] == 0
    assert calls["search"] == 0


def test_news_collector_fetch_sse_direct_announcements_parses_official_jsonp(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    class _FakeResponse:
        status_code = 200
        text = "jsonpCallback(" + json.dumps(
            {
                "pageHelp": {
                    "data": [
                        [
                            {
                                "TITLE": "贵州茅台重大事项公告",
                                "SSEDATE": "2026-03-31",
                                "SECURITY_CODE": "600519",
                                "SECURITY_NAME": "贵州茅台",
                                "URL": "/disclosure/listedinfo/announcement/c/new/2026-03-31/600519_demo.pdf",
                            }
                        ]
                    ]
                }
            },
            ensure_ascii=False,
        ) + ")"

        def raise_for_status(self) -> None:
            return None

    def fake_get(url: str, **kwargs):  # noqa: ANN001
        assert "queryCompanyBulletinNew.do" in url
        assert kwargs["params"]["SECURITY_CODE"] == "600519"
        assert kwargs["params"]["stockType"] == 1
        return _FakeResponse()

    monkeypatch.setattr("src.collectors.news.requests.get", fake_get)
    rows = collector._fetch_sse_direct_announcements(
        {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"},
        "20260301",
        "20260331",
        5,
    )

    assert rows
    assert rows[0]["TITLE"] == "贵州茅台重大事项公告"
    assert rows[0]["URL"].endswith("600519_demo.pdf")


def test_news_collector_sse_direct_items_carry_official_lane(monkeypatch):
    collector = NewsCollector({"news_feeds_file": "config/news_feeds.yaml"})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001, ARG001
        return [
            {
                "TITLE": "贵州茅台重大事项公告",
                "SSEDATE": "2026-03-31",
                "URL": "/disclosure/listedinfo/announcement/c/new/2026-03-31/600519_demo.pdf",
            }
        ]

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector._sse_direct_intelligence(
        {"symbol": "600519", "ts_code": "600519.SH", "name": "贵州茅台"},
        start_date="20260301",
        end_date="20260331",
        limit=5,
    )

    assert items
    assert items[0]["source"] == "SSE"
    assert items[0]["configured_source"] == "SSE::direct"
    assert items[0]["source_note"] == "official_direct"
    assert items[0]["link"] == "https://www.sse.com.cn/disclosure/listedinfo/announcement/c/new/2026-03-31/600519_demo.pdf"
