"""Tests for ETF pick command fallbacks."""

from __future__ import annotations

import time

from src.commands.etf_pick import (
    _etf_discovery_runtime_overrides,
    _client_final_runtime_overrides,
    _backfill_etf_news_report,
    _detail_markdown,
    _hydrate_selected_etf_profiles,
    _market_event_rows,
    _etf_news_query_groups,
    _payload_from_analyses,
    _promote_observation_candidates,
    _select_pick_analyses,
    _watchlist_fallback_payload,
)


def _analysis(symbol: str, name: str, rank: int) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "generated_at": "2026-03-13 15:00:00",
        "excluded": False,
        "rating": {"rank": rank, "label": "有信号但不充分"},
        "dimensions": {
            "technical": {"score": 40, "summary": "技术一般"},
            "fundamental": {"score": 30, "summary": "基本面一般"},
            "catalyst": {"score": 10, "summary": "催化偏弱", "coverage": {"news_mode": "proxy", "degraded": True}},
            "relative_strength": {"score": 35, "summary": "相对强弱一般"},
            "chips": {"score": 50, "summary": "筹码中性"},
            "risk": {"score": 45, "summary": "风险一般"},
            "seasonality": {"score": 0, "summary": "时点一般"},
            "macro": {"score": 12, "summary": "宏观中性"},
        },
    }


def test_watchlist_fallback_payload_uses_only_cn_etf(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.commands.etf_pick.load_watchlist",
        lambda: [
            {"symbol": "QQQM", "name": "Invesco NASDAQ 100 ETF", "asset_type": "us", "sector": "科技"},
            {"symbol": "513120", "name": "港股创新药ETF", "asset_type": "cn_etf", "sector": "医药"},
            {"symbol": "561380", "name": "电网ETF", "asset_type": "cn_etf", "sector": "电网"},
        ],
    )
    monkeypatch.setattr("src.commands.etf_pick.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005

    def fake_analyze(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001, ARG001
        assert asset_type == "cn_etf"
        return _analysis(symbol, metadata_override.get("name", symbol), 1 if symbol == "513120" else 0)

    monkeypatch.setattr("src.commands.etf_pick.analyze_opportunity", fake_analyze)

    payload = _watchlist_fallback_payload({}, top_n=5, theme_filter="")

    assert payload["discovery_mode"] == "watchlist_fallback"
    assert payload["scan_pool"] == 2
    assert payload["passed_pool"] == 2
    assert payload["data_coverage"]["total"] == 2
    assert len(payload["coverage_analyses"]) == 2
    assert [item["symbol"] for item in payload["top"]] == ["513120"]
    assert any("回退到 ETF watchlist" in item for item in payload["blind_spots"])


def test_client_final_runtime_overrides_apply_lightweight_etf_profile_by_default() -> None:
    config, notes = _client_final_runtime_overrides(
        {"opportunity": {"analysis_workers": 4}},
        client_final=True,
    )

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["opportunity"]["analysis_workers"] == 2
    assert config["opportunity"]["max_scan_candidates"] == 12
    assert config["skip_fund_profile"] is False
    assert config["news_topic_search_enabled"] is True
    assert config["news_feeds_file"] == "config/news_feeds.briefing_light.yaml"
    assert config["etf_fund_profile_mode"] == "light"
    assert config["etf_news_backfill_timeout_seconds"] == 12
    assert config["news_topic_query_cap"] == 4
    assert any("跨市场代理" in item for item in notes)
    assert any("候选池" in item for item in notes)
    assert any("轻量候选画像" in item for item in notes)
    assert any("基金画像链" in item for item in notes)
    assert any("轻量非空新闻源配置" in item for item in notes)
    assert any("受控 ETF 主题情报回填" in item for item in notes)


def test_etf_discovery_runtime_overrides_restore_light_profile_chain() -> None:
    config, notes = _etf_discovery_runtime_overrides({"skip_fund_profile": True, "news_topic_search_enabled": True})

    assert config["skip_fund_profile"] is False
    assert config["etf_fund_profile_mode"] == "light"
    assert config["news_topic_search_enabled"] is False
    assert config["etf_full_reanalysis_limit"] == 1
    assert any("轻量基金画像链" in item for item in notes)
    assert any("默认启用 `light` 画像" in item for item in notes)
    assert any("不在全候选阶段逐只跑主题扩搜" in item for item in notes)


def test_etf_news_query_groups_prefer_tracked_index_over_relative_strength_benchmark() -> None:
    groups = _etf_news_query_groups(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "benchmark_name": "沪深300ETF",
            "metadata": {
                "tracked_index_name": "中证半导体材料设备主题指数",
                "sector": "科技",
                "chain_nodes": ["半导体"],
            },
        }
    )

    assert groups[0][0] == "中证半导体材料设备主题指数"
    assert not any("沪深300ETF" in item for group in groups for item in group)


def test_backfill_etf_news_report_does_not_stop_on_three_unlinked_items(monkeypatch) -> None:
    analysis = {
        "name": "华夏上证科创板半导体材料设备主题ETF",
        "symbol": "588170",
        "asset_type": "cn_etf",
        "benchmark_name": "科创半导体材料设备",
        "metadata": {
            "tracked_index_name": "上证科创板半导体材料设备主题指数",
            "index_framework_label": "科创半导体材料设备",
            "sector": "半导体",
        },
        "news_report": {
            "items": [
                {"title": "已有摘要 1", "source": "旧摘要"},
                {"title": "已有摘要 2", "source": "旧摘要"},
                {"title": "已有摘要 3", "source": "旧摘要"},
            ]
        },
    }

    monkeypatch.setattr(
        "src.commands.etf_pick._timed_value",
        lambda loader, fallback, timeout_seconds: loader(),  # noqa: ARG005
    )

    class _FakeNewsCollector:
        def __init__(self, config):  # noqa: D401, ANN001
            self.config = config

        def get_market_intelligence(self, keywords, limit=6, recent_days=7):  # noqa: ANN001, ARG002
            return []

        def search_by_keyword_groups(self, keyword_groups, preferred_sources=None, limit=6, recent_days=5):  # noqa: ANN001, ARG002
            return [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长",
                    "source": "财联社",
                    "link": "https://example.com/semi",
                    "date": "2026-04-02",
                }
            ]

        def _filter_candidate_items(self, items, recent_days=7):  # noqa: ANN001, ARG002
            return list(items)

        def _rank_items(self, items, preferred_sources=None, query_keywords=None):  # noqa: ANN001, ARG002
            return list(items)

        def _diversify_items(self, items, limit):  # noqa: ANN001
            return list(items)[:limit]

        def _live_lines(self, items):  # noqa: ANN001
            return [item.get("title", "") for item in items]

        def _present_sources(self, items):  # noqa: ANN001
            return [item.get("source", "") for item in items]

    monkeypatch.setattr("src.commands.etf_pick.NewsCollector", _FakeNewsCollector)

    report = _backfill_etf_news_report(analysis, config={})

    assert len(report["items"]) == 4
    assert any(item.get("link") == "https://example.com/semi" for item in report["items"])


def test_client_final_runtime_overrides_preserves_topic_search_when_enabled() -> None:
    config, notes = _client_final_runtime_overrides(
        {"news_topic_search_enabled": True},
        client_final=True,
    )

    assert config["news_topic_search_enabled"] is True
    assert not any("默认不强开 ETF 主题扩搜慢链" in item for item in notes)


def test_client_final_runtime_overrides_respect_explicit_config_path() -> None:
    config, notes = _client_final_runtime_overrides(
        {"opportunity": {"analysis_workers": 4}},
        client_final=True,
        explicit_config_path="config/custom.yaml",
    )

    assert config["opportunity"]["analysis_workers"] == 4
    assert "max_scan_candidates" not in config.get("opportunity", {})
    assert "market_context" not in config
    assert "news_topic_search_enabled" not in config
    assert "news_feeds_file" not in config
    assert notes == []


def test_watchlist_fallback_payload_analyzes_candidates_in_parallel(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.commands.etf_pick.load_watchlist",
        lambda: [
            {"symbol": "513120", "name": "港股创新药ETF", "asset_type": "cn_etf", "sector": "医药"},
            {"symbol": "561380", "name": "电网ETF", "asset_type": "cn_etf", "sector": "电网"},
            {"symbol": "512480", "name": "半导体ETF", "asset_type": "cn_etf", "sector": "科技"},
        ],
    )
    monkeypatch.setattr("src.commands.etf_pick.build_market_context", lambda config, relevant_asset_types=None: {"runtime_caches": {}})  # noqa: ARG005

    def fake_analyze(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001, ARG001
        time.sleep(0.08)
        return _analysis(symbol, metadata_override.get("name", symbol), 1)

    monkeypatch.setattr("src.commands.etf_pick.analyze_opportunity", fake_analyze)

    start = time.perf_counter()
    payload = _watchlist_fallback_payload({"opportunity": {"analysis_workers": 3}}, top_n=5, theme_filter="")
    elapsed = time.perf_counter() - start

    assert payload["scan_pool"] == 3
    assert payload["passed_pool"] == 3
    assert len(payload["top"]) == 3
    assert elapsed < 0.20


def test_watchlist_fallback_payload_keeps_correlation_only_exclusions_for_observe_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.commands.etf_pick.load_watchlist",
        lambda: [
            {"symbol": "588200", "name": "科创芯片ETF", "asset_type": "cn_etf", "sector": "半导体"},
            {"symbol": "512480", "name": "半导体ETF", "asset_type": "cn_etf", "sector": "半导体"},
        ],
    )
    monkeypatch.setattr("src.commands.etf_pick.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005

    def fake_analyze(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001, ARG001
        analysis = _analysis(symbol, metadata_override.get("name", symbol), 0)
        analysis["excluded"] = True
        analysis["exclusion_reasons"] = [f"与 watchlist 中 {'512480' if symbol == '588200' else '588200'} 相关性过高"]
        analysis["narrative"] = {"judgment": {"state": "观察为主"}}
        return analysis

    monkeypatch.setattr("src.commands.etf_pick.analyze_opportunity", fake_analyze)

    payload = _watchlist_fallback_payload({"opportunity": {"analysis_workers": 1}}, top_n=5, theme_filter="半导体")
    selected = _select_pick_analyses(payload, top_n=5)

    assert payload["scan_pool"] == 2
    assert payload["passed_pool"] == 2
    assert payload["top"] == []
    assert len(payload["coverage_analyses"]) == 2
    assert [item["symbol"] for item in selected] == ["588200", "512480"]
    assert any("主题内候选彼此高相关" in item for item in payload["blind_spots"])


def test_hydrate_selected_etf_profiles_refreshes_index_framework_rows(monkeypatch) -> None:
    analysis = _analysis("588200", "科创芯片ETF", 0)
    analysis["asset_type"] = "cn_etf"
    analysis["metadata"] = {"symbol": "588200", "name": "科创芯片ETF", "asset_type": "cn_etf", "sector": "科技"}

    monkeypatch.setattr(
        "src.commands.etf_pick.FundProfileCollector.collect_profile",
        lambda self, symbol, asset_type="cn_etf", profile_mode="full": {  # noqa: ARG005
            "overview": {"跟踪标的": "上证科创板芯片指数", "ETF基准指数代码": "000685.SH"},
            "etf_snapshot": {"index_name": "上证科创板芯片指数", "index_code": "000685.SH"},
        },
    )
    monkeypatch.setattr(
        "src.commands.etf_pick.refresh_etf_analysis_report_fields",
        lambda analysis, config=None: {  # noqa: ARG005
            **analysis,
            "metadata": {**dict(analysis.get("metadata") or {}), "benchmark_name": "上证科创板芯片指数"},
            "benchmark_name": "上证科创板芯片指数",
            "benchmark_symbol": "000685.SH",
            "market_event_rows": [
                ["2026-04-02", "跟踪指数框架：科创芯片ETF 跟踪 上证科创板芯片指数", "跟踪指数/框架", "中", "上证科创板芯片指数", "", "标准指数框架", "先按标准指数框架理解。"]
            ],
        },
    )

    rows = _hydrate_selected_etf_profiles([analysis], config={}, limit=1)

    assert rows[0]["benchmark_symbol"] == "000685.SH"
    assert rows[0]["market_event_rows"][0][1] == "跟踪指数框架：科创芯片ETF 跟踪 上证科创板芯片指数"


def test_payload_from_analyses_exposes_short_and_medium_tracks() -> None:
    short = _analysis("159981", "能源化工ETF", 2)
    short["action"] = {
        "timeframe": "短线交易(1-2周)",
        "horizon": {
            "code": "short_term",
            "label": "短线交易（3-10日）",
            "fit_reason": "更适合看短催化和右侧确认。",
        },
    }
    short["narrative"] = {"judgment": {"state": "观望偏多"}, "positives": ["方向没坏。"]}
    medium = _analysis("510880", "红利ETF", 2)
    medium["action"] = {
        "timeframe": "中线配置(1-3月)",
        "horizon": {
            "code": "position_trade",
            "label": "中线配置（1-3月）",
            "fit_reason": "更适合按一段完整主线分批拿。",
        },
    }
    medium["narrative"] = {"judgment": {"state": "持有优于追高"}, "positives": ["配置价值还在。"]}

    payload = _payload_from_analyses([short, medium], {})

    assert payload["recommendation_tracks"]["short_term"]["symbol"] == "159981"
    assert payload["recommendation_tracks"]["medium_term"]["symbol"] == "510880"


def test_payload_from_analyses_uses_shared_dimension_summary_for_catalyst() -> None:
    short = _analysis("159981", "能源化工ETF", 1)
    short["action"] = {"timeframe": "等待更好窗口", "horizon": {"code": "watch", "label": "观察期"}}
    short["narrative"] = {"judgment": {"state": "观察为主"}}
    short["dimensions"]["catalyst"] = {
        "score": 23,
        "max_score": 100,
        "summary": "催化不足，当前更像静态博弈。",
        "factors": [
            {"name": "政策催化", "display_score": "0/30"},
            {"name": "产品/跟踪方向催化", "display_score": "12/12"},
            {"name": "研报/新闻密度", "display_score": "10/10"},
            {"name": "新闻热度", "display_score": "10/10"},
        ],
    }

    payload = _payload_from_analyses([short], {})

    catalyst_row = next(row for row in payload["winner"]["dimension_rows"] if row[0] == "催化面")
    assert catalyst_row[2] == "直接催化偏弱，舆情关注度尚可，因此当前更像静态博弈。"


def test_payload_from_analyses_uses_client_safe_coverage_summary_when_catalyst_evidence_missing() -> None:
    short = _analysis("159981", "能源化工ETF", 1)
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
    }

    payload = _payload_from_analyses([short], {})

    assert payload["winner"]["evidence"] == [
        {"title": "当前可前置的一手情报有限，判断更多参考结构化事件和行业线索。", "source": "覆盖率摘要"}
    ]


def test_payload_from_analyses_preserves_news_report_when_direct_evidence_missing() -> None:
    short = _analysis("512400", "南方中证申万有色金属ETF", 1)
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
        "theme_news": [],
    }
    short["news_report"] = {
        "items": [
            {
                "title": "有色金属板块走强，工业金属价格继续修复",
                "source": "财联社",
                "published_at": "2026-04-01 14:00:00",
                "link": "https://example.com/nonferrous",
            }
        ]
    }

    payload = _payload_from_analyses([short], {})

    assert payload["winner"]["evidence"] == []
    assert payload["winner"]["news_report"]["items"][0]["title"] == "有色金属板块走强，工业金属价格继续修复"


def test_payload_from_analyses_backfills_etf_news_report_when_missing(monkeypatch) -> None:
    short = _analysis("512400", "南方中证申万有色金属ETF", 1)
    short["benchmark_name"] = "中证申万有色金属指数"
    short["metadata"] = {"sector": "有色", "chain_nodes": ["铜", "工业金属"]}
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
        "theme_news": [],
    }

    class FakeNewsCollector:
        def __init__(self, config):  # noqa: ANN001
            self.config = dict(config or {})

        def get_market_intelligence(self, keywords, limit=4, recent_days=7):  # noqa: ANN001, ARG002
            return []

        def search_by_keyword_groups(self, query_groups, preferred_sources=None, limit=4, recent_days=5):  # noqa: ANN001, ARG002
            assert any("中证申万有色金属指数" in " ".join(group) for group in query_groups)
            return [
                {
                    "title": "有色金属板块走强，铜价修复带动相关 ETF 活跃",
                    "source": "财联社",
                    "published_at": "2026-04-01 14:30:00",
                    "link": "https://example.com/nonferrous-etf",
                }
            ]

        def _filter_candidate_items(self, items, recent_days=7):  # noqa: ANN001, ARG002
            return list(items)

        def _rank_items(self, items, preferred_sources=None, query_keywords=None):  # noqa: ANN001, ARG002
            return list(items)

        def _diversify_items(self, items, limit):  # noqa: ANN001
            return list(items)[:limit]

        def _live_lines(self, items):  # noqa: ANN001
            return [str(dict(items[0]).get("title", ""))]

        def _present_sources(self, items):  # noqa: ANN001
            return {str(dict(items[0]).get("source", ""))}

    monkeypatch.setattr("src.commands.etf_pick.NewsCollector", FakeNewsCollector)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    assert payload["winner"]["news_report"]["items"][0]["title"] == "有色金属板块走强，铜价修复带动相关 ETF 活跃"
    assert payload["winner"]["news_report"]["items"][0]["link"] == "https://example.com/nonferrous-etf"


def test_payload_from_analyses_filters_quote_noise_etf_news_and_keeps_theme_intelligence(monkeypatch) -> None:
    short = _analysis("588170", "华夏上证科创板半导体材料设备主题ETF", 1)
    short["benchmark_name"] = "上证科创板半导体材料设备主题指数"
    short["metadata"] = {
        "tracked_index_name": "上证科创板半导体材料设备主题指数",
        "sector": "科技",
        "chain_nodes": ["半导体"],
    }
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
        "theme_news": [],
    }

    class FakeNewsCollector:
        def __init__(self, config):  # noqa: ANN001
            self.config = dict(config or {})

        def get_market_intelligence(self, keywords, limit=4, recent_days=7):  # noqa: ANN001, ARG002
            assert "上证科创板半导体材料设备主题指数" in " ".join(keywords)
            return [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长",
                    "source": "Tushare",
                    "published_at": "2026-04-02 14:28:00",
                    "link": "https://example.com/semi-capex",
                }
            ]

        def search_by_keyword_groups(self, query_groups, preferred_sources=None, limit=4, recent_days=3):  # noqa: ANN001, ARG002
            assert any("AI算力" in " ".join(group) or "半导体材料设备" in " ".join(group) for group in query_groups)
            return [
                {
                    "title": "科创半导体设备ETF鹏华（589020）开盘涨0.00%，重仓股中微公司跌0.49%",
                    "source": "新浪财经",
                    "published_at": "2026-04-02 09:35:59",
                    "link": "https://example.com/etf-quote-noise",
                },
                {
                    "title": "AI算力扩张拉动半导体设备需求，机构继续看多国产替代",
                    "source": "证券时报",
                    "published_at": "2026-04-02 10:20:00",
                    "link": "https://example.com/ai-semiconductor",
                },
            ]

        def _filter_candidate_items(self, items, recent_days=7):  # noqa: ANN001, ARG002
            return list(items)

        def _rank_items(self, items, preferred_sources=None, query_keywords=None):  # noqa: ANN001, ARG002
            return list(items)

        def _diversify_items(self, items, limit):  # noqa: ANN001
            return list(items)[:limit]

        def _live_lines(self, items):  # noqa: ANN001
            return [str(dict(item).get("title", "")).strip() for item in items]

        def _present_sources(self, items):  # noqa: ANN001
            return {str(dict(item).get("source", "")).strip() for item in items}

    monkeypatch.setattr("src.commands.etf_pick.NewsCollector", FakeNewsCollector)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    titles = [str(dict(item).get("title", "")).strip() for item in payload["winner"]["news_report"]["items"]]
    assert "SEMI：未来四年12英寸晶圆厂设备支出持续增长" in titles
    assert "AI算力扩张拉动半导体设备需求，机构继续看多国产替代" in titles
    assert not any("开盘涨0.00%" in title for title in titles)


def test_payload_from_analyses_filters_etf_net_subscription_noise_but_keeps_mixed_theme_titles(monkeypatch) -> None:
    short = _analysis("588170", "华夏上证科创板半导体材料设备主题ETF", 1)
    short["benchmark_name"] = "上证科创板半导体材料设备主题指数"
    short["metadata"] = {
        "tracked_index_name": "上证科创板半导体材料设备主题指数",
        "sector": "科技",
        "chain_nodes": ["半导体"],
    }
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
        "theme_news": [],
    }

    class FakeNewsCollector:
        def __init__(self, config):  # noqa: ANN001
            self.config = dict(config or {})

        def get_market_intelligence(self, keywords, limit=6, recent_days=7):  # noqa: ANN001, ARG002
            return []

        def search_by_keyword_groups(self, query_groups, preferred_sources=None, limit=6, recent_days=3):  # noqa: ANN001, ARG002
            return [
                {
                    "title": "半导体产业链集体下挫，资金逆势加码，半导体设备ETF易方达（159558）半日净申购达1300万份",
                    "source": "财联社",
                    "published_at": "2026-04-02 14:35:00",
                    "link": "https://example.com/net-sub-noise",
                },
                {
                    "title": "三星西安晶圆厂制程升级正式量产，半导体设备需求继续受益",
                    "source": "新浪财经",
                    "published_at": "2026-04-02 10:20:00",
                    "link": "https://example.com/wafer-fab-theme",
                },
            ]

        def _filter_candidate_items(self, items, recent_days=7):  # noqa: ANN001, ARG002
            return list(items)

        def _rank_items(self, items, preferred_sources=None, query_keywords=None):  # noqa: ANN001, ARG002
            return list(items)

        def _diversify_items(self, items, limit):  # noqa: ANN001
            return list(items)[:limit]

        def _live_lines(self, items):  # noqa: ANN001
            return [str(dict(item).get("title", "")).strip() for item in items]

        def _present_sources(self, items):  # noqa: ANN001
            return {str(dict(item).get("source", "")).strip() for item in items}

    monkeypatch.setattr("src.commands.etf_pick.NewsCollector", FakeNewsCollector)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    titles = [str(dict(item).get("title", "")).strip() for item in payload["winner"]["news_report"]["items"]]
    assert "三星西安晶圆厂制程升级正式量产，半导体设备需求继续受益" in titles
    assert not any("半日净申购达1300万份" in title for title in titles)


def test_payload_from_analyses_filters_generic_market_headline_without_theme_specificity(monkeypatch) -> None:
    short = _analysis("588170", "华夏上证科创板半导体材料设备主题ETF", 1)
    short["benchmark_name"] = "上证科创板半导体材料设备主题指数"
    short["metadata"] = {
        "tracked_index_name": "上证科创板半导体材料设备主题指数",
        "sector": "科技",
        "chain_nodes": ["半导体"],
    }
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
        "theme_news": [],
    }

    class FakeNewsCollector:
        def __init__(self, config):  # noqa: ANN001
            self.config = dict(config or {})

        def get_market_intelligence(self, keywords, limit=6, recent_days=7):  # noqa: ANN001, ARG002
            return []

        def search_by_keyword_groups(self, query_groups, preferred_sources=None, limit=6, recent_days=3):  # noqa: ANN001, ARG002
            return [
                {
                    "title": "市场回暖信号显现，四月份关注三个方向 - 财富号",
                    "source": "财富号",
                    "published_at": "2026-04-02 10:20:00",
                    "link": "https://example.com/generic-market",
                },
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长",
                    "source": "亿欧网",
                    "published_at": "2026-04-02 06:40:05",
                    "link": "https://example.com/semi-capex",
                },
            ]

        def _filter_candidate_items(self, items, recent_days=7):  # noqa: ANN001, ARG002
            return list(items)

        def _rank_items(self, items, preferred_sources=None, query_keywords=None):  # noqa: ANN001, ARG002
            return list(items)

        def _diversify_items(self, items, limit):  # noqa: ANN001
            return list(items)[:limit]

        def _live_lines(self, items):  # noqa: ANN001
            return [str(dict(item).get("title", "")).strip() for item in items]

        def _present_sources(self, items):  # noqa: ANN001
            return {str(dict(item).get("source", "")).strip() for item in items}

    monkeypatch.setattr("src.commands.etf_pick.NewsCollector", FakeNewsCollector)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    titles = [str(dict(item).get("title", "")).strip() for item in payload["winner"]["news_report"]["items"]]
    assert "SEMI：未来四年12英寸晶圆厂设备支出持续增长" in titles
    assert not any("市场回暖信号显现" in title for title in titles)


def test_payload_from_analyses_preserves_market_event_rows_when_news_is_thin() -> None:
    short = _analysis("512400", "南方中证申万有色金属ETF", 1)
    short["market_event_rows"] = [
        [
            "2026-04-01",
            "A股行业走强：有色（+2.65%）；领涨 紫金矿业",
            "A股行业/盘面",
            "高",
            "有色",
            "",
            "主线增强",
            "偏利多，先看 `有色` 能否继续扩散。",
        ]
    ]

    payload = _payload_from_analyses([short], {})

    assert payload["winner"]["market_event_rows"][0][1] == "A股行业走强：有色（+2.65%）；领涨 紫金矿业"


def test_payload_from_analyses_does_not_inject_empty_intelligence_summary_when_market_event_rows_exist() -> None:
    short = _analysis("512400", "南方中证申万有色金属ETF", 1)
    short["dimensions"]["catalyst"] = {
        "score": 10,
        "max_score": 100,
        "summary": "催化偏弱",
        "factors": [],
        "coverage": {"news_mode": "proxy", "degraded": True},
        "evidence": [],
        "theme_news": [],
    }
    short["news_report"] = {"items": []}
    short["market_event_rows"] = [
        [
            "2026-04-01",
            "A股行业走强：有色（+2.65%）；领涨 紫金矿业",
            "A股行业/盘面",
            "高",
            "有色",
            "",
            "主线增强",
            "偏利多，先看 `有色` 能否继续扩散。",
        ]
    ]

    payload = _payload_from_analyses([short], {})

    assert payload["winner"]["evidence"] == []
    assert payload["winner"]["market_event_rows"][0][1] == "A股行业走强：有色（+2.65%）；领涨 紫金矿业"


def test_market_event_rows_falls_back_to_relative_strength_board_move() -> None:
    short = _analysis("512400", "南方中证申万有色金属ETF", 1)
    short["generated_at"] = "2026-04-01 21:47:47"
    short["metadata"] = {"sector": "有色"}
    short["dimensions"]["relative_strength"]["summary"] = "相对强弱有改善。板块涨跌幅 +2.65%"

    rows = _market_event_rows(short)

    assert rows[0][1] == "主题/盘面跟踪：有色（板块涨跌幅 +2.65%）"
    assert rows[0][2] == "相对强弱/盘面"


def test_payload_from_analyses_marks_etf_chips_as_auxiliary_dimension() -> None:
    short = _analysis("159981", "能源化工ETF", 1)

    payload = _payload_from_analyses([short], {})

    chips_row = next(row for row in payload["winner"]["dimension_rows"] if row[0] == "筹码结构（辅助项）")
    assert chips_row[1] == "辅助项"
    assert "主排序不直接使用这项" in chips_row[2]


def test_payload_from_analyses_prefers_etf_with_index_structure_and_positive_share_flow() -> None:
    strong = _analysis("563360", "A500ETF华泰柏瑞", 1)
    strong["asset_type"] = "cn_etf"
    strong["metadata"] = {
        "index_framework_label": "中证A500指数",
        "industry_framework_label": "申万一级行业·宽基",
        "index_top_weight_sum": 18.2,
        "index_top_constituent_name": "宁德时代",
        "etf_share_change": 2.58,
        "etf_share_change_pct": 0.84,
        "share_as_of": "2026-04-01",
    }
    strong["fund_profile"] = {
        "etf_snapshot": {
            "index_name": "中证A500指数",
            "share_as_of": "2026-04-01",
            "etf_share_change": 2.58,
            "etf_share_change_pct": 0.84,
        }
    }

    weak = _analysis("512400", "有色金属ETF", 1)
    weak["asset_type"] = "cn_etf"
    weak["metadata"] = {
        "benchmark_name": "中证申万有色金属指数",
        "etf_share_change": -3.20,
        "etf_share_change_pct": -1.15,
    }

    payload = _payload_from_analyses([weak, strong], {})

    assert payload["winner"]["symbol"] == "563360"
    assert any("跟踪指数 `中证A500指数`" in line for line in payload["winner"]["positives"])
    assert any("净创设 `+2.58 亿份`" in line for line in payload["winner"]["positives"])


def test_payload_from_analyses_surfaces_etf_fusion_reason_when_share_flow_and_external_intel_align() -> None:
    strong = _analysis("563360", "A500ETF华泰柏瑞", 1)
    strong["asset_type"] = "cn_etf"
    strong["metadata"] = {
        "index_framework_label": "中证A500指数",
        "etf_share_change": 2.58,
        "etf_share_change_pct": 0.84,
        "share_as_of": "2026-04-01",
    }
    strong["fund_profile"] = {
        "etf_snapshot": {
            "index_name": "中证A500指数",
            "share_as_of": "2026-04-01",
            "etf_share_change": 2.58,
            "etf_share_change_pct": 0.84,
        }
    }
    strong["news_report"] = {
        "items": [
            {
                "title": "中证A500成分权重调整后资金继续回流",
                "source": "财联社",
                "link": "https://example.com/a500",
            }
        ]
    }

    payload = _payload_from_analyses([strong], {})

    assert any("产品层和情报层开始同向" in line for line in payload["winner"]["positives"])
    assert any("外部情报已接上" in line for line in payload["winner"]["positives"])


def test_payload_from_analyses_surfaces_single_day_share_snapshot_when_flow_delta_missing() -> None:
    single_day = _analysis("563360", "A500ETF华泰柏瑞", 1)
    single_day["asset_type"] = "cn_etf"
    single_day["metadata"] = {
        "index_framework_label": "中证A500指数",
        "industry_framework_label": "申万一级行业·宽基",
    }
    single_day["fund_profile"] = {
        "etf_snapshot": {
            "index_name": "中证A500指数",
            "share_as_of": "2026-04-01",
            "total_share_yi": 308.72,
        }
    }

    payload = _payload_from_analyses([single_day], {})

    assert any("份额快照已接上" in line for line in payload["winner"]["positives"])
    assert any("先不把申赎方向写死" in line for line in payload["winner"]["positives"])
    assert any("份额快照仍只有单日口径" in line for line in payload["selection_context"]["delivery_notes"])


def test_payload_from_analyses_surfaces_fund_factor_snapshot_reason() -> None:
    single_day = _analysis("563360", "A500ETF华泰柏瑞", 1)
    single_day["asset_type"] = "cn_etf"
    single_day["fund_profile"] = {
        "fund_factor_snapshot": {
            "trend_label": "趋势偏强",
            "momentum_label": "动能改善",
            "latest_date": "2026-04-01",
        }
    }

    payload = _payload_from_analyses([single_day], {})

    assert any("场内基金技术因子显示 `趋势偏强` / `动能改善`" in line for line in payload["winner"]["positives"])


def test_etf_news_query_groups_include_index_constituent_and_holdings() -> None:
    groups = _etf_news_query_groups(
        {
            "name": "A500ETF华泰柏瑞",
            "asset_type": "cn_etf",
            "metadata": {
                "tracked_index_name": "中证A500指数",
                "industry_framework_label": "申万一级行业·宽基",
                "index_top_constituent_name": "宁德时代",
                "chain_nodes": ["宽基"],
            },
            "fund_profile": {
                "top_holdings": [
                    {"股票名称": "贵州茅台"},
                    {"股票名称": "招商银行"},
                ]
            },
        }
    )

    flattened = [" ".join(group) for group in groups]
    assert any("宁德时代" in item for item in flattened)
    assert any("贵州茅台" in item for item in flattened)


def test_payload_from_analyses_keeps_regime_context() -> None:
    short = _analysis("159981", "能源化工ETF", 1)

    payload = _payload_from_analyses(
        [short],
        {},
        regime={
            "current_regime": "recovery",
            "reasoning": ["PMI 回到 50 上方。", "信用脉冲边际改善。"],
        },
        day_theme={"label": "能源冲击 + 地缘风险"},
    )

    assert payload["regime"]["current_regime"] == "recovery"
    assert payload["day_theme"]["label"] == "能源冲击 + 地缘风险"


def test_payload_from_analyses_keeps_provenance_fields_for_renderer() -> None:
    short = _analysis("159981", "能源化工ETF", 1)
    short["history"] = {"stub": True}
    short["intraday"] = {"enabled": False}
    short["metadata"] = {"history_source_label": "Tushare 日线"}
    short["benchmark_name"] = "沪深300ETF"
    short["benchmark_symbol"] = "510300"
    short["dimensions"]["relative_strength"] = {
        "score": 35,
        "summary": "相对强弱一般",
        "benchmark_name": "沪深300ETF",
        "benchmark_symbol": "510300",
    }
    short["provenance"] = {
        "analysis_generated_at": "2026-03-13 15:00",
        "market_data_as_of": "2026-03-13",
        "relative_benchmark_name": "沪深300ETF",
        "relative_benchmark_symbol": "510300",
        "news_mode": "proxy",
    }

    payload = _payload_from_analyses([short], {})

    assert payload["winner"]["generated_at"] == "2026-03-13 15:00:00"
    assert payload["winner"]["provenance"]["market_data_as_of"] == "2026-03-13"
    assert payload["winner"]["dimensions"]["relative_strength"]["benchmark_symbol"] == "510300"
    assert payload["winner"]["benchmark_symbol"] == "510300"


def test_payload_from_analyses_keeps_portfolio_overlap_summary_for_renderer() -> None:
    short = _analysis("159981", "能源化工ETF", 1)
    short["portfolio_overlap_summary"] = {
        "summary_line": "这条建议和现有组合最重的行业同线，更像同一主线延伸。",
        "overlap_label": "同一行业主线加码",
        "conflict_label": "暂未看到明显主线冲突",
    }

    payload = _payload_from_analyses([short], {})

    assert payload["winner"]["portfolio_overlap_summary"]["overlap_label"] == "同一行业主线加码"


def test_select_pick_analyses_falls_back_to_observation_candidates() -> None:
    watch = _analysis("513120", "港股创新药ETF", 0)
    watch["narrative"] = {"judgment": {"state": "观察为主"}}
    watch["dimensions"]["fundamental"]["score"] = 68
    weak = _analysis("159870", "化工ETF", 0)
    weak["narrative"] = {"judgment": {"state": "无信号"}}
    weak["dimensions"]["fundamental"]["score"] = 20

    rows = _select_pick_analyses({"top": [], "coverage_analyses": [weak, watch]}, top_n=5)

    assert [item["symbol"] for item in rows] == ["513120"]


def test_select_pick_analyses_prefers_full_history_candidates_over_fallback() -> None:
    fallback = _analysis("512000", "券商ETF华宝", 2)
    fallback["history_fallback_mode"] = True
    fallback["narrative"] = {"judgment": {"state": "观察为主"}}
    full = _analysis("513120", "港股创新药ETF广发", 1)
    full["history_fallback_mode"] = False
    full["narrative"] = {"judgment": {"state": "观察为主"}}

    rows = _select_pick_analyses({"top": [fallback, full]}, top_n=5)

    assert [item["symbol"] for item in rows[:2]] == ["513120", "512000"]


def test_promote_observation_candidates_reuses_existing_coverage_rows() -> None:
    watch = _analysis("512480", "半导体ETF", 0)
    watch["narrative"] = {"judgment": {"state": "观察为主"}}
    watch["dimensions"]["fundamental"]["score"] = 66

    payload = _promote_observation_candidates(
        {
            "top": [],
            "coverage_analyses": [watch],
            "blind_spots": ["原始说明"],
            "discovery_mode": "tushare_universe",
        },
        top_n=5,
        reason="全市场 ETF 扫描未形成正向入围，本次改按观察级候选继续排序，不再直接丢掉已有覆盖样本。",
    )

    assert [item["symbol"] for item in payload["top"]] == ["512480"]
    assert payload["blind_spots"][-1].startswith("全市场 ETF 扫描未形成正向入围")


def test_payload_from_analyses_softly_prefers_portfolio_style_complement_within_same_score_band() -> None:
    repeat = _analysis("512000", "券商ETF华宝", 1)
    repeat["dimensions"]["relative_strength"]["score"] = 36
    repeat["portfolio_overlap_summary"] = {
        "overlap_label": "同一行业主线加码",
        "style_conflict_label": "同风格延伸",
        "same_sector_weight": 0.29,
        "same_region_weight": 0.92,
    }

    complement = _analysis("159611", "电力ETF", 1)
    complement["dimensions"]["relative_strength"]["score"] = 35
    complement["portfolio_overlap_summary"] = {
        "overlap_label": "重复度较低",
        "style_conflict_label": "风格补位",
        "same_sector_weight": 0.0,
        "same_region_weight": 0.10,
    }

    payload = _payload_from_analyses([repeat, complement], {})

    assert payload["winner"]["symbol"] == "159611"


def test_detail_markdown_softens_winner_reason_when_candidates_share_same_rank_bucket(monkeypatch) -> None:
    first = _analysis("563360", "A500ETF华泰柏瑞", 1)
    first["narrative"] = {"judgment": {"state": "观察为主"}}
    second = _analysis("159352", "A500ETF南方", 1)
    second["narrative"] = {"judgment": {"state": "观察为主"}}
    monkeypatch.setattr(
        "src.commands.etf_pick.OpportunityReportRenderer.render_scan",
        lambda self, payload: "# stub",  # noqa: ARG005
    )

    detail = _detail_markdown(
        [first, second],
        "563360",
        selection_context={"delivery_observe_only": True},
    )

    assert "当前排在候选首位" in detail
    assert "评级与综合排序分最优" not in detail
