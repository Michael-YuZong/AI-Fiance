"""Tests for social sentiment proxy collection."""

from __future__ import annotations

from src.collectors.social_sentiment import SocialSentimentCollector


def test_social_sentiment_collect_returns_aggregate():
    collector = SocialSentimentCollector()
    payload = collector.collect(
        "561380",
        {
            "return_1d": 0.01,
            "return_5d": 0.03,
            "return_20d": 0.12,
            "volume_ratio": 1.4,
            "trend": "多头",
        },
    )
    assert payload["symbol"] == "561380"
    assert payload["aggregate"]["method"] == "proxy"
    assert 0 <= payload["aggregate"]["sentiment_index"] <= 100
    assert payload["aggregate"]["confidence_label"] in {"中", "高"}
    assert payload["aggregate"]["limitations"]
    assert "真实发帖/阅读/评论抓取" in payload["aggregate"]["limitations"][0]


def test_social_sentiment_missing_inputs_lowers_confidence() -> None:
    payload = SocialSentimentCollector().collect(
        "561380",
        {
            "return_1d": 0.01,
            "trend": "多头",
        },
    )

    assert payload["aggregate"]["confidence_label"] == "低"
    assert "量能比" in payload["aggregate"]["limitations"][1]
