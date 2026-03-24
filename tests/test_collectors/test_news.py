"""Tests for news collector live fallback behavior."""

from __future__ import annotations

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
  <item><title>PBOC signals ample liquidity support for domestic demand - Reuters</title><link>https://example.com/policy</link><pubDate>Sat, 21 Mar 2026 02:15:43 GMT</pubDate><source>Reuters</source></item>
</channel></rss>
""".strip()
            )
        return feedparser.parse("<rss><channel></channel></rss>")

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    items = collector.search_by_keywords(["流动性", "内需"], preferred_sources=["Reuters"], limit=4, recent_days=7)
    assert len(items) == 1
    assert "Global Market Headlines" not in items[0]["title"]
    assert "ample liquidity support" in items[0]["title"]


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
    items = collector.get_stock_news("600519", limit=5)
    assert items
    assert items[0]["title"] == "贵州茅台发布年报，分红继续提升"
    assert items[0]["source"] == "证券时报"
