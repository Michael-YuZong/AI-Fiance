"""Tests for ad-hoc intelligence command helpers."""

from __future__ import annotations

from src.commands.intel import (
    _prune_theme_noise_items,
    _render_intel_markdown,
    build_news_report_from_intel_payload,
    collect_market_aware_intel_news_report,
    collect_intel_payload,
)


class _FakeLookupCollector:
    def __init__(self, _config):
        pass

    def resolve_best(self, _query: str):
        return {
            "symbol": "512400",
            "name": "有色金属ETF",
            "asset_type": "cn_etf",
            "match_type": "alias",
        }


class _FakeNewsCollector:
    def __init__(self, config):
        self.config = dict(config)

    def get_market_intelligence(self, keywords, *, limit=6, recent_days=7):
        assert keywords
        return [
            {
                "category": "market_news",
                "title": "有色金属板块关注度升温",
                "source": "Tushare",
                "configured_source": "Tushare::major_news",
                "published_at": "2026-04-05 09:30:00",
                "link": "https://example.com/a",
            }
        ]

    def search_by_keyword_groups(self, groups, *, preferred_sources=None, limit=6, recent_days=7):
        assert groups
        return [
            {
                "category": "topic_search",
                "title": "铜价上行带动有色链活跃",
                "source": "财联社",
                "configured_source": "财联社",
                "published_at": "2026-04-05 10:00:00",
                "link": "https://example.com/b",
            }
        ]

    def get_stock_news(self, symbol, limit=10):
        assert symbol == "512400"
        return [
            {
                "category": "stock_news",
                "title": "有色金属ETF资金持续流入",
                "source": "证券时报",
                "configured_source": "证券时报",
                "published_at": "2026-04-05 10:30:00",
                "link": "https://example.com/c",
            }
        ]

    def get_stk_surv(self, symbol, limit=20):
        assert symbol == "512400"
        return {
            "items": [
                {
                    "category": "stk_surv",
                    "title": "机构调研讨论资源品景气",
                    "source": "Tushare",
                    "configured_source": "Tushare::stk_surv",
                    "published_at": "2026-04-05 11:00:00",
                    "link": "https://example.com/d",
                }
            ]
        }

    def _filter_candidate_items(self, items, recent_days=7):
        return list(items)

    def _rank_items(self, items, preferred_sources=None, query_keywords=None):
        return list(items)

    def _diversify_items(self, items, limit):
        return list(items)[:limit]

    def _present_sources(self, items):
        return {str(item.get("source") or "").strip() for item in items if str(item.get("source") or "").strip()}


def test_collect_intel_payload_merges_search_market_and_structured_hits(monkeypatch) -> None:
    monkeypatch.setattr("src.commands.intel.AssetLookupCollector", _FakeLookupCollector)
    monkeypatch.setattr("src.commands.intel.NewsCollector", _FakeNewsCollector)

    payload = collect_intel_payload(
        "收集有色金属ETF相关情报",
        config={},
        limit=6,
        recent_days=5,
        structured_only=False,
    )

    assert payload["symbol"] == "512400"
    assert payload["asset_name"] == "有色金属ETF"
    assert payload["market_hits_count"] == 1
    assert payload["search_hits_count"] == 1
    assert payload["stock_hits_count"] == 1
    assert payload["structured_hits_count"] == 1
    assert len(payload["items"]) == 4
    assert payload["cluster_count"] >= 4
    assert payload["items"][0]["source_tier"] in {"structured", "primary_media", "aggregator"}
    assert payload["items"][0]["theme_bucket"]
    assert payload["source_tiers"]
    assert payload["grouped_items"]


def test_collect_intel_payload_restores_default_feeds_from_client_final_light_config(monkeypatch) -> None:
    seen: dict[str, str] = {}

    class _CaptureNewsCollector(_FakeNewsCollector):
        def __init__(self, config):
            super().__init__(config)
            seen["news_feeds_file"] = str(self.config.get("news_feeds_file", ""))

    monkeypatch.setattr("src.commands.intel.AssetLookupCollector", _FakeLookupCollector)
    monkeypatch.setattr("src.commands.intel.NewsCollector", _CaptureNewsCollector)

    payload = collect_intel_payload(
        "收集有色金属ETF相关情报",
        config={"news_feeds_file": "config/news_feeds.empty.yaml"},
        limit=6,
        recent_days=5,
        structured_only=False,
    )

    assert seen["news_feeds_file"] == "config/news_feeds.yaml"
    assert payload["search_hits_count"] == 1


def test_render_intel_markdown_surfaces_boundary_and_links() -> None:
    markdown = _render_intel_markdown(
        {
            "query": "有色金属",
            "generated_at": "2026-04-05 11:30:00",
            "structured_only": False,
            "recent_days": 7,
            "symbol": "512400",
            "asset_name": "有色金属ETF",
            "asset_type": "cn_etf",
            "items": [
                {
                    "category": "topic_search",
                    "title": "铜价上行带动有色链活跃",
                    "source": "财联社",
                    "published_at": "2026-04-05 10:00:00",
                    "link": "https://example.com/b",
                }
            ],
            "surv_snapshot": {
                "items": [
                    {
                        "category": "stk_surv",
                        "title": "机构调研讨论资源品景气",
                        "source": "Tushare",
                        "published_at": "2026-04-05 11:00:00",
                        "link": "https://example.com/d",
                    }
                ]
            },
            "source_list": ["财联社", "Tushare"],
            "query_terms": ["有色金属"],
            "market_hits_count": 1,
            "search_hits_count": 1,
            "stock_hits_count": 0,
            "structured_hits_count": 1,
            "note": "已命中 RSS/topic search。",
            "disclosure": "这是一份自由情报快照，不走 final / release_check / report_guard。",
        }
    )

    assert "# 情报快照" in markdown
    assert "## 关键情报" in markdown
    assert "[铜价上行带动有色链活跃](https://example.com/b)" in markdown
    assert "## 结构化辅助" in markdown
    assert "不走 final / release_check / report_guard" in markdown


def test_prune_theme_noise_items_drops_stock_diagnosis_noise() -> None:
    filtered = _prune_theme_noise_items(
        [
            {
                "category": "topic_search",
                "title": "个股诊断：南都电源：储能板块走弱，业绩不佳，股价加速下跌，关注支撑位。C盛龙：次新股，有色金属板块，股价冲高回落，参考洛阳钼业估值偏高",
                "source": "第一财经",
            },
            {
                "category": "market_intelligence",
                "title": "中国有色金属工业协会硅业分会赴包头开展硅产业深度调研",
                "source": "同花顺",
            },
        ],
        query_terms=["有色金属"],
    )

    assert len(filtered) == 1
    assert filtered[0]["title"] == "中国有色金属工业协会硅业分会赴包头开展硅产业深度调研"


def test_build_news_report_from_intel_payload_preserves_live_contract() -> None:
    report = build_news_report_from_intel_payload(
        {
            "items": [
                {
                    "title": "有色协会调研更新",
                    "source": "同花顺",
                    "link": "https://example.com/a",
                }
            ],
            "ranked_items": [
                {
                    "title": "有色协会调研更新",
                    "source": "同花顺",
                    "link": "https://example.com/a",
                },
                {
                    "title": "铜价修复带动情绪回暖",
                    "source": "新浪财经",
                    "link": "https://example.com/b",
                },
            ],
            "source_list": ["同花顺", "新浪财经"],
            "grouped_items": [{"label": "产业/公司", "count": 1, "items": []}],
            "source_tiers": [{"tier": "aggregator", "label": "聚合转述", "count": 1}],
            "cluster_count": 1,
            "note": "已命中 RSS/topic search。",
            "disclosure": "自由情报快照",
        },
        note_prefix="briefing intel: ",
    )

    assert report["mode"] == "live"
    assert report["lines"] == ["有色协会调研更新"]
    assert report["source_list"] == ["同花顺", "新浪财经"]
    assert report["grouped_items"][0]["label"] == "产业/公司"
    assert report["source_tiers"][0]["label"] == "聚合转述"
    assert report["cluster_count"] == 1
    assert report["summary_lines"]
    assert report["lead_summary"].startswith("主题聚类：")
    assert report["note"] == "briefing intel: 已命中 RSS/topic search。"
    assert report["disclosure"] == "自由情报快照"


def test_collect_market_aware_intel_news_report_prioritizes_major_event_hypothesis_from_proxy_summary() -> None:
    seen: list[str] = []

    def fake_collect(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001,ARG001
        seen.append(query)
        if query == "美伊 停火 中东 风险偏好 财联社 Reuters":
            return {
                "mode": "live",
                "items": [
                    {
                        "title": "美伊停火带动全球风险偏好修复",
                        "source": "财联社",
                        "link": "https://example.com/ceasefire",
                        "published_at": "2026-04-08 10:00:00",
                    }
                ],
                "all_items": [
                    {
                        "title": "美伊停火带动全球风险偏好修复",
                        "source": "财联社",
                        "link": "https://example.com/ceasefire",
                        "published_at": "2026-04-08 10:00:00",
                    }
                ],
                "lines": ["美伊停火带动全球风险偏好修复"],
                "source_list": ["财联社"],
                "note": "命中地缘缓和快讯",
                "disclosure": "共享情报快照",
            }
        return {"mode": "proxy", "items": [], "all_items": [], "lines": [], "source_list": [], "note": "", "disclosure": "共享情报快照"}

    report = collect_market_aware_intel_news_report(
        "A股 主线 情报",
        config={},
        baseline_report={
            "mode": "proxy",
            "items": [],
            "summary_lines": ["国际局势与风险偏好正在主导盘面"],
            "lines": ["能源与地缘未主导盘面"],
            "note": "proxy summary",
        },
        collect_fn=fake_collect,
        note_prefix="共享 intel",
    )

    assert seen[0] == "美伊 停火 中东 风险偏好 财联社 Reuters"
    assert report["items"][0]["title"] == "美伊停火带动全球风险偏好修复"
    assert "市场级大事假设优先补搜" in report["note"]


def test_collect_market_aware_intel_news_report_keeps_existing_sufficient_linked_items() -> None:
    existing = {
        "mode": "live",
        "items": [
            {"title": "已有外链 1", "source": "财联社", "link": "https://example.com/a"},
            {"title": "已有外链 2", "source": "Reuters", "link": "https://example.com/b"},
        ],
        "all_items": [
            {"title": "已有外链 1", "source": "财联社", "link": "https://example.com/a"},
            {"title": "已有外链 2", "source": "Reuters", "link": "https://example.com/b"},
        ],
        "lines": ["已有外链 1", "已有外链 2"],
        "source_list": ["财联社", "Reuters"],
        "note": "现有情报已足够",
        "disclosure": "共享情报快照",
    }

    def fail_collect(*args, **kwargs):  # noqa: ANN001,ARG001
        raise AssertionError("should not collect again when linked evidence already suffices")

    report = collect_market_aware_intel_news_report(
        "A股 主线 情报",
        config={},
        baseline_report=existing,
        collect_fn=fail_collect,
    )

    assert report == existing
