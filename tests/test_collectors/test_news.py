"""Tests for news collector live fallback behavior."""

from __future__ import annotations

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
