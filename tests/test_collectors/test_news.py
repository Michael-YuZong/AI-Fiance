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
