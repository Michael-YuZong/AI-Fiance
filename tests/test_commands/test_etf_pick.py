"""Tests for ETF pick command fallbacks."""

from __future__ import annotations

from datetime import datetime
import time
from types import SimpleNamespace

from src.commands.etf_pick import (
    CLIENT_FINAL_BUNDLE_CONTRACT_VERSION,
    _etf_discovery_runtime_overrides,
    _client_final_runtime_overrides,
    _backfill_etf_news_report,
    _curate_etf_news_items,
    _detail_markdown,
    _detail_output_path,
    _hydrate_selected_etf_profiles,
    _analysis_matches_preferred_sector,
    _market_event_rows,
    _etf_news_query_groups,
    _payload_from_analyses,
    _persist_etf_pick_internal,
    _promote_observation_candidates,
    _rank_key,
    _select_pick_analyses,
    _selection_context,
    _sanitize_etf_analysis_news_payload,
    _watchlist_fallback_payload,
    main,
)
from src.output.editor_payload import build_etf_pick_editor_packet


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
    assert payload["top"] == []
    assert any("回退到 ETF watchlist" in item for item in payload["blind_spots"])


def test_client_final_runtime_overrides_apply_lightweight_etf_profile_by_default() -> None:
    config, notes = _client_final_runtime_overrides(
        {"opportunity": {"analysis_workers": 4}},
        client_final=True,
    )

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["opportunity"]["analysis_workers"] == 2
    assert config["opportunity"]["max_scan_candidates"] == 18
    assert config["skip_fund_profile"] is False
    assert config["news_topic_search_enabled"] is True
    assert config["news_feeds_file"] == "config/news_feeds.briefing_light.yaml"
    assert config["etf_fund_profile_mode"] == "light"
    assert config["etf_news_backfill_timeout_seconds"] == 12
    assert config["news_topic_query_cap"] == 4
    assert any("跨市场代理与 market monitor 慢链" in item for item in notes)
    assert any("候选池" in item for item in notes)
    assert any("轻量候选画像" in item for item in notes)
    assert any("基金画像链" in item for item in notes)
    assert any("轻量非空新闻源配置" in item for item in notes)
    assert any("受控 ETF 主题情报回填" in item for item in notes)


def test_etf_discovery_runtime_overrides_restore_light_profile_chain() -> None:
    config, notes = _etf_discovery_runtime_overrides({"skip_fund_profile": True, "news_topic_search_enabled": True})

    assert config["skip_fund_profile"] is False
    assert config["etf_fund_profile_mode"] == "light"
    assert config["news_topic_search_enabled"] is True
    assert config["skip_catalyst_dynamic_search_runtime"] is True
    assert any("轻量基金画像链" in item for item in notes)
    assert any("默认启用 `light` 画像" in item for item in notes)
    assert any("动态主题扩搜" in item for item in notes)


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
                {"title": "半导体材料设备景气观察 1", "source": "旧摘要"},
                {"title": "晶圆厂设备资本开支跟踪 2", "source": "旧摘要"},
                {"title": "国产替代链条梳理 3", "source": "旧摘要"},
            ]
        },
    }

    monkeypatch.setattr(
        "src.commands.etf_pick._timed_value",
        lambda loader, fallback, timeout_seconds: loader(),  # noqa: ARG005
    )
    seen: dict[str, str] = {}

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        seen["query"] = query
        seen["symbol"] = explicit_symbol
        seen["note_prefix"] = note_prefix
        return {
            "mode": "live",
            "items": [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长",
                    "source": "财联社",
                    "link": "https://example.com/semi",
                    "published_at": "2026-04-02",
                }
            ],
            "all_items": [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长",
                    "source": "财联社",
                    "link": "https://example.com/semi",
                    "published_at": "2026-04-02",
                },
                {
                    "title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣",
                    "source": "财联社",
                    "link": "https://example.com/ai-entertainment",
                    "published_at": "2026-04-02",
                }
            ],
            "lines": ["SEMI：未来四年12英寸晶圆厂设备支出持续增长"],
            "source_list": ["财联社"],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

    report = _backfill_etf_news_report(analysis, config={})

    assert len(report["items"]) == 3
    assert any(item.get("link") == "https://example.com/semi" for item in report["items"])
    assert all("AI艺人库" not in str(item.get("title", "")) for item in report["all_items"])
    assert "上证科创板半导体材料设备主题指数" in seen["query"]
    assert seen["symbol"] == "588170"
    assert seen["note_prefix"] == "ETF 外部情报"


def test_curate_etf_news_items_rejects_generic_a_share_briefing() -> None:
    analysis = {
        "name": "国泰中证半导体材料设备主题ETF",
        "symbol": "159516",
        "asset_type": "cn_etf",
        "benchmark_name": "中证半导体材料设备主题指数",
        "metadata": {
            "tracked_index_name": "中证半导体材料设备主题指数",
            "sector": "科技",
            "chain_nodes": ["半导体"],
            "index_top_constituent_name": "北方华创",
        },
    }

    curated = _curate_etf_news_items(
        [
            {
                "title": "【早报】消费领域多个利好来袭，多家A股公司被证监会立案",
                "source": "财联社",
                "link": "https://example.com/generic",
            },
            {
                "title": "半导体材料设备国产替代订单预期继续升温",
                "source": "证券时报",
                "link": "https://example.com/semis",
            },
        ],
        analysis,
    )

    assert [item["link"] for item in curated] == ["https://example.com/semis"]


def test_curate_etf_news_items_rejects_ai_application_news_for_cpo_etf() -> None:
    analysis = {
        "name": "国泰中证全指通信设备ETF",
        "symbol": "515880",
        "asset_type": "cn_etf",
        "benchmark_name": "中证全指通信设备指数",
        "metadata": {
            "tracked_index_name": "中证全指通信设备指数",
            "sector": "通信",
            "chain_nodes": ["通信设备", "光模块", "CPO", "AI算力"],
            "index_top_constituent_name": "新易盛",
        },
    }

    curated = _curate_etf_news_items(
        [
            {
                "title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣",
                "source": "财联社",
                "link": "https://example.com/ai-entertainment",
            },
            {
                "title": "午评：创业板指涨0.63% CPO概念大涨",
                "source": "证券时报",
                "link": "https://example.com/cpo",
            },
            {
                "title": "国务院部署AI、算力、6G、卫星互联网等方向",
                "source": "财联社",
                "link": "https://example.com/compute-policy",
            },
        ],
        analysis,
    )

    assert [item["link"] for item in curated] == [
        "https://example.com/cpo",
        "https://example.com/compute-policy",
    ]


def test_sanitize_etf_analysis_news_payload_filters_sidecar_news_fields() -> None:
    analysis = {
        "name": "国泰中证全指通信设备ETF",
        "symbol": "515880",
        "asset_type": "cn_etf",
        "metadata": {
            "tracked_index_name": "中证全指通信设备指数",
            "sector": "通信",
            "chain_nodes": ["通信设备", "光模块", "CPO", "AI算力"],
        },
        "news_report": {
            "items": [
                {"title": "午评：创业板指涨0.63% CPO概念大涨", "source": "证券时报"},
                {"title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣", "source": "财联社"},
            ],
            "all_items": [
                {"title": "午评：创业板指涨0.63% CPO概念大涨", "source": "证券时报"},
                {"title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣", "source": "财联社"},
            ],
            "lines": ["bad old line"],
        },
        "dimensions": {
            "catalyst": {
                "theme_news": [
                    {"title": "午评：创业板指涨0.63% CPO概念大涨", "source": "证券时报"},
                    {"title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣", "source": "财联社"},
                ]
            }
        },
    }

    sanitized = _sanitize_etf_analysis_news_payload(analysis)

    assert [item["title"] for item in sanitized["news_report"]["items"]] == ["午评：创业板指涨0.63% CPO概念大涨"]
    assert all("AI艺人库" not in item["title"] for item in sanitized["news_report"]["all_items"])
    assert sanitized["news_report"]["lines"] == ["午评：创业板指涨0.63% CPO概念大涨 (证券时报)"]
    assert [item["title"] for item in sanitized["dimensions"]["catalyst"]["theme_news"]] == ["午评：创业板指涨0.63% CPO概念大涨"]


def test_backfill_etf_news_report_filters_existing_all_items_before_early_return() -> None:
    analysis = {
        "name": "国泰中证全指通信设备ETF",
        "symbol": "515880",
        "asset_type": "cn_etf",
        "metadata": {
            "tracked_index_name": "中证全指通信设备指数",
            "sector": "通信",
            "chain_nodes": ["通信设备", "光模块", "CPO", "AI算力"],
        },
        "news_report": {
            "items": [
                {"title": "午评：创业板指涨0.63% CPO概念大涨", "source": "证券时报", "link": "https://example.com/cpo"},
                {"title": "国务院部署AI、算力、6G、卫星互联网等方向", "source": "财联社", "link": "https://example.com/policy"},
            ],
            "all_items": [
                {"title": "午评：创业板指涨0.63% CPO概念大涨", "source": "证券时报", "link": "https://example.com/cpo"},
                {"title": "国务院部署AI、算力、6G、卫星互联网等方向", "source": "财联社", "link": "https://example.com/policy"},
                {"title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣", "source": "财联社", "link": "https://example.com/ai-entertainment"},
            ],
        },
    }

    report = _backfill_etf_news_report(analysis, config={})

    assert [item["link"] for item in report["all_items"]] == [
        "https://example.com/cpo",
        "https://example.com/policy",
    ]


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

    assert config["opportunity"]["analysis_workers"] == 2
    assert config["opportunity"]["max_scan_candidates"] == 18
    assert config["market_context"]["skip_global_proxy"] is True
    assert config["news_topic_search_enabled"] is True
    assert config["news_feeds_file"] == "config/news_feeds.briefing_light.yaml"
    assert any("显式配置文件" in item for item in notes)


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
    selected = _select_pick_analyses(payload, top_n=5)

    assert payload["scan_pool"] == 3
    assert payload["passed_pool"] == 3
    assert payload["top"] == []
    assert len(selected) == 3
    assert elapsed < 0.20


def test_watchlist_fallback_payload_skips_excluded_candidates(monkeypatch) -> None:
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
        analysis["exclusion_reasons"] = ["日均成交额低于 5000 万"]
        return analysis

    monkeypatch.setattr("src.commands.etf_pick.analyze_opportunity", fake_analyze)

    payload = _watchlist_fallback_payload({"opportunity": {"analysis_workers": 1}}, top_n=5, theme_filter="半导体")
    selected = _select_pick_analyses(payload, top_n=5)

    assert payload["scan_pool"] == 2
    assert payload["passed_pool"] == 0
    assert payload["top"] == []
    assert payload["coverage_analyses"] == []
    assert selected == []


def test_rank_key_prefers_direct_day_theme_alignment_within_same_bucket() -> None:
    direct = _analysis("588200", "科创芯片ETF", 0)
    direct["metadata"] = {"sector": "科技", "benchmark": "上证科创板芯片指数"}
    direct["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    direct["dimensions"]["relative_strength"]["score"] = 70

    sidechain = _analysis("561380", "电网ETF", 0)
    sidechain["metadata"] = {"sector": "电网", "benchmark": "恒生A股电网设备指数", "chain_nodes": ["AI算力", "电力需求"]}
    sidechain["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    sidechain["dimensions"]["relative_strength"]["score"] = 72

    assert _rank_key(direct) > _rank_key(sidechain)


def test_rank_key_prefers_today_mainline_alignment_over_offtheme_defensive_rank() -> None:
    direct = _analysis("515880", "通信ETF", 1)
    direct["metadata"] = {"sector": "通信", "benchmark": "中证全指通信设备指数", "chain_nodes": ["CPO", "光模块"]}
    direct["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    direct["dimensions"]["technical"]["score"] = 36
    direct["dimensions"]["relative_strength"]["score"] = 58
    direct["dimensions"]["catalyst"]["score"] = 18

    defensive = _analysis("159937", "博时黄金ETF", 2)
    defensive["metadata"] = {"sector": "黄金", "benchmark": "黄金9999"}
    defensive["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    defensive["dimensions"]["technical"]["score"] = 38
    defensive["dimensions"]["fundamental"]["score"] = 71
    defensive["dimensions"]["relative_strength"]["score"] = 40
    defensive["dimensions"]["risk"]["score"] = 55

    assert _rank_key(direct) > _rank_key(defensive)


def test_rank_key_prefers_explicit_chain_carrier_over_holdings_inferred_wrapper() -> None:
    wrapper = _analysis("159363", "创业板人工智能ETF", 1)
    wrapper["asset_type"] = "cn_etf"
    wrapper["metadata"] = {
        "sector": "通信",
        "tracked_index_name": "创业板人工智能指数",
        "primary_chain": "CPO/光模块",
        "theme_directness": "direct",
        "theme_role": "AI硬件主链",
        "evidence_keywords": ["CPO", "光模块", "通信设备"],
    }
    wrapper["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    wrapper["dimensions"]["technical"]["score"] = 42
    wrapper["dimensions"]["relative_strength"]["score"] = 62
    wrapper["dimensions"]["catalyst"]["score"] = 16

    direct = _analysis("515880", "通信ETF", 1)
    direct["asset_type"] = "cn_etf"
    direct["metadata"] = {
        "sector": "通信",
        "tracked_index_name": "中证全指通信设备指数",
        "primary_chain": "CPO/光模块",
        "theme_directness": "direct",
        "theme_role": "AI硬件主链",
        "evidence_keywords": ["CPO", "光模块", "通信设备"],
    }
    direct["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    direct["dimensions"]["technical"]["score"] = 40
    direct["dimensions"]["relative_strength"]["score"] = 58
    direct["dimensions"]["catalyst"]["score"] = 16

    assert _rank_key(direct) > _rank_key(wrapper)


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


def test_hydrate_selected_etf_profiles_does_not_rerun_full_analysis(monkeypatch) -> None:
    analysis = _analysis("513120", "港股创新药ETF", 3)
    analysis["asset_type"] = "cn_etf"

    monkeypatch.setattr(
        "src.commands.etf_pick.FundProfileCollector.collect_profile",
        lambda self, symbol, asset_type="cn_etf", profile_mode="full": {"overview": {"基金经理人": "测试经理"}},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.commands.etf_pick.refresh_etf_analysis_report_fields",
        lambda row, config=None: {**dict(row), "refreshed": True},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.commands.etf_pick.analyze_opportunity",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full reanalysis should not run")),  # noqa: ARG005
    )

    rows = _hydrate_selected_etf_profiles([analysis], config={"skip_catalyst_dynamic_search_runtime": True}, limit=1)

    assert rows[0]["fund_profile"]["overview"]["基金经理人"] == "测试经理"
    assert rows[0]["refreshed"] is True


def test_payload_from_analyses_exposes_three_recommendation_tracks() -> None:
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
    third = _analysis("513120", "港股创新药ETF", 2)
    third["action"] = {
        "timeframe": "波段跟踪(2-6周)",
        "horizon": {
            "code": "swing",
            "label": "波段跟踪（2-6周）",
            "fit_reason": "更适合按主线延续和催化兑现去跟。",
        },
    }
    third["narrative"] = {"judgment": {"state": "右侧改善中"}, "positives": ["主线延续还在。"]}

    payload = _payload_from_analyses([short, medium, third], {})

    assert payload["recommendation_tracks"]["short_term"]["symbol"] == "159981"
    assert payload["recommendation_tracks"]["medium_term"]["symbol"] == "510880"
    assert payload["recommendation_tracks"]["third_term"]["symbol"] == "513120"
    assert payload["winner"]["rating"]["rank"] == 2


def test_payload_from_analyses_keeps_recommendation_tracks_inside_preferred_sectors() -> None:
    cpo = _analysis("515880", "通信ETF", 3)
    cpo["metadata"] = {"sector": "通信", "chain_nodes": ["通信设备", "CPO"]}
    cpo["action"] = {"horizon": {"code": "swing", "label": "波段跟踪（2-6周）"}}

    gold = _analysis("518880", "黄金ETF", 3)
    gold["metadata"] = {"sector": "黄金", "chain_nodes": ["贵金属"]}
    gold["action"] = {"horizon": {"code": "position_trade", "label": "中线配置（1-3月）"}}

    semi = _analysis("159516", "半导体材料设备ETF", 2)
    semi["metadata"] = {"sector": "半导体", "chain_nodes": ["半导体设备"]}
    semi["action"] = {"horizon": {"code": "swing", "label": "波段跟踪（2-6周）"}}

    chip = _analysis("588200", "科创芯片ETF", 1)
    chip["metadata"] = {"sector": "半导体", "chain_nodes": ["芯片"]}
    chip["action"] = {"horizon": {"code": "watch", "label": "观察期"}}

    payload = _payload_from_analyses(
        [cpo, gold, semi, chip],
        {"preferred_sectors": ["科技", "半导体", "通信", "宽基"]},
    )

    assert [item["symbol"] for item in payload["recommendation_tracks"].values()] == ["515880", "159516", "588200"]


def test_analysis_matches_preferred_sector_uses_taxonomy_profile_aliases() -> None:
    analysis = _analysis("515880", "通信ETF", 3)
    analysis["metadata"] = {
        "sector": "信息技术",
        "chain_nodes": ["网络基础设施"],
        "taxonomy": {
            "theme_profile": {
                "theme_family": "硬科技",
                "primary_chain": "CPO/光模块",
                "theme_role": "AI硬件主链",
                "preferred_sector_aliases": ["科技", "AI硬件", "通信", "CPO", "光模块"],
                "evidence_keywords": ["CPO", "光模块", "AI算力"],
                "mainline_tags": ["AI硬件链"],
            }
        },
    }

    assert _analysis_matches_preferred_sector(analysis, ["科技"])
    assert _analysis_matches_preferred_sector(analysis, ["光模块"])
    assert not _analysis_matches_preferred_sector(analysis, ["黄金"])


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


def test_payload_from_analyses_preserves_theme_background_catalyst_summary() -> None:
    short = _analysis("515880", "通信ETF", 1)
    short["dimensions"]["catalyst"] = {
        "score": 7,
        "max_score": 100,
        "summary": "已命中 ETF/指数暴露方向的主题级 live 情报，说明背景催化不是空白；但还缺直接、强、可执行的新增催化，先按背景支持处理。",
        "factors": [
            {"name": "政策催化", "display_score": "0/30"},
            {"name": "产品/跟踪方向催化", "display_score": "12/12"},
            {"name": "研报/新闻密度", "display_score": "10/10"},
            {"name": "新闻热度", "display_score": "10/10"},
        ],
    }

    payload = _payload_from_analyses([short], {})

    catalyst_row = next(row for row in payload["winner"]["dimension_rows"] if row[0] == "催化面")
    assert "背景催化不是空白" in catalyst_row[2]
    assert "还缺直接、强、可执行" in catalyst_row[2]


def test_payload_from_analyses_frontloads_top_holding_disclosure_calendar(monkeypatch) -> None:
    class FakeValuationCollector:
        def __init__(self, config):  # noqa: ANN001
            self.config = config

        def get_cn_stock_disclosure_dates(self, symbol):  # noqa: ANN001
            assert symbol == "300502"
            return [
                {
                    "end_date": "20260331",
                    "pre_date": "20260424",
                    "actual_date": "",
                    "ann_date": "",
                },
                {
                    "end_date": "20251231",
                    "pre_date": "20260424",
                    "actual_date": "",
                    "ann_date": "",
                }
            ]

    monkeypatch.setattr("src.commands.etf_pick.ValuationCollector", FakeValuationCollector)
    analysis = _analysis("515880", "通信ETF", 1)
    analysis["asset_type"] = "cn_etf"
    analysis["generated_at"] = "2026-04-23 15:30:00"
    analysis["fund_profile"] = {
        "top_holdings": [
            {
                "股票代码": "300502",
                "股票名称": "新易盛",
                "占净值比例": 15.09,
            }
        ]
    }
    analysis["news_report"] = {
        "items": [
            {
                "title": "午评：创业板指涨0.63% CPO概念大涨 - 证券时报",
                "source": "证券时报",
                "published_at": "2026-04-21T07:04:00",
                "link": "https://example.com/cpo",
            }
        ]
    }

    payload = _payload_from_analyses(
        [analysis],
        {},
        config={"etf_holding_disclosure_timeout_seconds": 1, "etf_holding_disclosure_lookahead_days": 7},
    )
    rows = payload["winner"]["market_event_rows"]
    editor_packet = build_etf_pick_editor_packet(payload)

    assert any("新易盛(300502) 预约披露 2025年年报 / 2026年一季报" in row[1] for row in rows)
    assert editor_packet["event_digest"]["lead_layer"] == "财报"
    assert "新易盛" in editor_packet["homepage"]["news_lines"][0]
    assert "新易盛" in "\n".join(editor_packet["homepage"]["news_lines"])


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

    seen: dict[str, str] = {}

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        seen["query"] = query
        seen["symbol"] = explicit_symbol
        seen["note_prefix"] = note_prefix
        return {
            "mode": "live",
            "items": [
                {
                    "title": "有色金属板块走强，铜价修复带动相关 ETF 活跃",
                    "source": "财联社",
                    "published_at": "2026-04-01 14:30:00",
                    "link": "https://example.com/nonferrous-etf",
                }
            ],
            "all_items": [
                {
                    "title": "有色金属板块走强，铜价修复带动相关 ETF 活跃",
                    "source": "财联社",
                    "published_at": "2026-04-01 14:30:00",
                    "link": "https://example.com/nonferrous-etf",
                }
            ],
            "lines": ["有色金属板块走强，铜价修复带动相关 ETF 活跃"],
            "source_list": ["财联社"],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    assert payload["winner"]["news_report"]["items"][0]["title"] == "有色金属板块走强，铜价修复带动相关 ETF 活跃"
    assert payload["winner"]["news_report"]["items"][0]["link"] == "https://example.com/nonferrous-etf"
    assert "中证申万有色金属指数" in seen["query"]
    assert seen["symbol"] == "512400"
    assert seen["note_prefix"] == "ETF 外部情报"


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

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        assert "上证科创板半导体材料设备主题指数" in query
        assert explicit_symbol == "588170"
        assert note_prefix == "ETF 外部情报"
        return {
            "mode": "live",
            "items": [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长",
                    "source": "Tushare",
                    "published_at": "2026-04-02 14:28:00",
                    "link": "https://example.com/semi-capex",
                },
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
            ],
            "all_items": [],
            "lines": [],
            "source_list": [],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

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

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        assert explicit_symbol == "588170"
        return {
            "mode": "live",
            "items": [
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
            ],
            "all_items": [],
            "lines": [],
            "source_list": [],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

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

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        return {
            "mode": "live",
            "items": [
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
            ],
            "all_items": [],
            "lines": [],
            "source_list": [],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    titles = [str(dict(item).get("title", "")).strip() for item in payload["winner"]["news_report"]["items"]]
    assert "SEMI：未来四年12英寸晶圆厂设备支出持续增长" in titles
    assert not any("市场回暖信号显现" in title for title in titles)


def test_payload_from_analyses_filters_unrelated_etf_news_even_if_source_is_primary_media(monkeypatch) -> None:
    short = _analysis("159980", "大成有色金属期货ETF", 1)
    short["benchmark_name"] = "上海期货交易所有色金属期货价格指数"
    short["metadata"] = {
        "tracked_index_name": "上海期货交易所有色金属期货价格指数",
        "sector": "有色",
        "chain_nodes": ["铜铝", "顺周期", "有色金属"],
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

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        return {
            "mode": "live",
            "items": [
                {
                    "title": "【早报】消费领域，多个利好来袭；多家A股公司被证监会立案 - 财联社",
                    "source": "财联社",
                    "published_at": "2026-04-03T23:29:17",
                    "link": "https://example.com/generic-early",
                },
                {
                    "title": "张雪机车双冠点燃A股摩托板块，终端订单排至5-7月 - 财联社",
                    "source": "财联社",
                    "published_at": "2026-03-31T08:25:00",
                    "link": "https://example.com/motorcycle",
                },
                {
                    "title": "新材料行业月报：几内亚考虑收紧铝土矿供应，铝价中枢或继续抬升",
                    "source": "新浪财经",
                    "published_at": "2026-03-30T23:30:00",
                    "link": "https://example.com/bauxite",
                },
            ],
            "all_items": [],
            "lines": [],
            "source_list": [],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    titles = [str(dict(item).get("title", "")).strip() for item in payload["winner"]["news_report"]["items"]]
    assert "新材料行业月报：几内亚考虑收紧铝土矿供应，铝价中枢或继续抬升" in titles
    assert not any("消费领域，多个利好来袭" in title for title in titles)
    assert not any("张雪机车双冠点燃A股摩托板块" in title for title in titles)


def test_payload_from_analyses_filters_precious_metal_news_for_industrial_nonferrous_etf(monkeypatch) -> None:
    short = _analysis("159980", "大成有色金属期货ETF", 1)
    short["benchmark_name"] = "上海期货交易所有色金属期货价格指数"
    short["metadata"] = {
        "tracked_index_name": "上海期货交易所有色金属期货价格指数",
        "sector": "有色",
        "chain_nodes": ["铜铝", "顺周期", "有色金属"],
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

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ANN001, ARG001
        return {
            "mode": "live",
            "items": [
                {
                    "title": "金银暴跌，是短期波动，还是长期趋势的转折？",
                    "source": "第一财经",
                    "published_at": "2026-04-02T09:37:18",
                    "link": "https://example.com/gold-silver",
                },
                {
                    "title": "几内亚铝土矿供应扰动延续，氧化铝价格中枢抬升",
                    "source": "新浪财经",
                    "published_at": "2026-03-30T23:30:00",
                    "link": "https://example.com/bauxite",
                },
            ],
            "all_items": [],
            "lines": [],
            "source_list": [],
            "note": "shared intel",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.commands.etf_pick.collect_intel_news_report", fake_collect_intel_news_report)

    payload = _payload_from_analyses([short], {}, config={"news_topic_search_enabled": True})

    titles = [str(dict(item).get("title", "")).strip() for item in payload["winner"]["news_report"]["items"]]
    assert "几内亚铝土矿供应扰动延续，氧化铝价格中枢抬升" in titles
    assert not any("金银暴跌" in title for title in titles)


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


def test_payload_from_analyses_keeps_winner_fund_profile_and_theme_playbook() -> None:
    winner = _analysis("159928", "汇添富中证主要消费ETF", 1)
    winner["asset_type"] = "cn_etf"
    winner["fund_profile"] = {
        "overview": {
            "基金管理人": "汇添富基金",
            "基金经理人": "过蓓蓓",
            "业绩比较基准": "中证主要消费指数",
        }
    }
    winner["theme_playbook"] = {
        "key": "sector::consumer_discretionary",
        "label": "消费",
        "playbook_level": "sector",
    }

    payload = _payload_from_analyses([winner], {})

    assert payload["winner"]["fund_profile"]["overview"]["基金管理人"] == "汇添富基金"
    assert payload["winner"]["theme_playbook"]["key"] == "sector::consumer_discretionary"


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


def test_select_pick_analyses_does_not_promote_risk_only_defensive_etf_as_observe_fallback() -> None:
    cpo = _analysis("515880", "通信ETF", 0)
    cpo["metadata"] = {"sector": "通信", "benchmark": "中证全指通信设备指数", "chain_nodes": ["CPO", "光模块"]}
    cpo["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    cpo["dimensions"]["technical"]["score"] = 36
    cpo["dimensions"]["fundamental"]["score"] = 22
    cpo["dimensions"]["catalyst"]["score"] = 18
    cpo["dimensions"]["relative_strength"]["score"] = 58
    cpo["dimensions"]["risk"]["score"] = 42

    grid = _analysis("561380", "电网ETF", 0)
    grid["metadata"] = {"sector": "电网", "benchmark": "恒生A股电网设备指数", "chain_nodes": ["电网设备", "电力需求"]}
    grid["day_theme"] = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}
    grid["dimensions"]["technical"]["score"] = 18
    grid["dimensions"]["fundamental"]["score"] = 28
    grid["dimensions"]["catalyst"]["score"] = 0
    grid["dimensions"]["relative_strength"]["score"] = 24
    grid["dimensions"]["risk"]["score"] = 78

    rows = _select_pick_analyses({"top": [], "coverage_analyses": [grid, cpo]}, top_n=5)

    assert [item["symbol"] for item in rows] == ["515880"]


def test_select_pick_analyses_prefers_full_history_candidates_over_fallback() -> None:
    fallback = _analysis("512000", "券商ETF华宝", 2)
    fallback["history_fallback_mode"] = True
    fallback["narrative"] = {"judgment": {"state": "观察为主"}}
    full = _analysis("513120", "港股创新药ETF广发", 1)
    full["history_fallback_mode"] = False
    full["narrative"] = {"judgment": {"state": "观察为主"}}

    rows = _select_pick_analyses({"top": [fallback, full]}, top_n=5)

    assert [item["symbol"] for item in rows[:2]] == ["513120", "512000"]


def test_select_pick_analyses_expands_sparse_top_with_coverage_rows() -> None:
    winner = _analysis("512890", "红利低波", 3)
    winner["asset_type"] = "cn_etf"
    winner["narrative"] = {"judgment": {"state": "回调更优"}}

    innovation = _analysis("513120", "港股创新药ETF", 0)
    innovation["asset_type"] = "cn_etf"
    innovation["narrative"] = {"judgment": {"state": "观察为主"}}
    innovation["dimensions"]["technical"]["score"] = 39
    innovation["dimensions"]["fundamental"]["score"] = 44
    innovation["dimensions"]["relative_strength"]["score"] = 51

    rows = _select_pick_analyses(
        {
            "top": [winner],
            "coverage_analyses": [winner, innovation],
            "watch_positive": [],
        },
        top_n=3,
    )

    assert [item["symbol"] for item in rows[:2]] == ["512890", "513120"]


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


def test_selection_context_keeps_discovery_preferred_sectors() -> None:
    context = _selection_context(
        discovery_mode="tushare_universe",
        scan_pool=18,
        passed_pool=18,
        theme_filter="",
        preferred_sectors=["科技", "半导体", "通信", "通信"],
        coverage={"total": 18},
    )

    assert context["preferred_sectors"] == ["科技", "半导体", "通信"]
    assert context["preferred_sector_label"] == "科技 / 半导体 / 通信"
    assert context["theme_filter_label"] == "未指定"


def test_selection_context_downgrades_delivery_when_direct_news_is_zero() -> None:
    context = _selection_context(
        discovery_mode="tushare_universe",
        scan_pool=18,
        passed_pool=18,
        coverage={"total": 18, "structured_rate": 0.5, "direct_news_rate": 0.0},
        delivery_tier={
            "code": "standard_recommendation",
            "label": "标准推荐稿",
            "observe_only": False,
            "summary_only": False,
            "notes": [
                "原始交付说明",
                "这份稿件仍可作为正式推荐框架下的单只优先对象。",
                "新闻/事件覆盖存在局部降级，但当前优先标的自身仍有可执行证据，本次继续按正式推荐框架处理。",
            ],
        },
    )

    assert context["delivery_tier_code"] == "degraded_observation"
    assert context["delivery_tier_label"] == "降级观察稿"
    assert context["delivery_observe_only"] is True
    assert "原始交付说明" in context["delivery_notes"]
    assert not any("正式推荐框架" in item for item in context["delivery_notes"])
    assert "今天先给一个观察优先对象，不按正式买入稿理解。" in context["delivery_notes"]
    assert any("高置信直接新闻覆盖仍为 0" in item for item in context["delivery_notes"])


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
        selection_context={"delivery_observe_only": True, "preferred_sector_label": "科技 / 半导体 / 通信"},
    )

    assert "当前排在候选首位" in detail
    assert "评级与综合排序分最优" not in detail
    assert "- 偏好主题: `科技 / 半导体 / 通信`" in detail


def test_persist_etf_pick_internal_writes_ascii_sidecar_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.commands.etf_pick.resolve_project_path", lambda relative: tmp_path / relative)
    today = datetime.now().strftime("%Y-%m-%d")
    payload = {
        "generated_at": f"{today} 15:00:00",
        "selection_context": {"scan_pool": 8, "passed_pool": 3},
        "winner": {"symbol": "512400"},
    }
    detail_path = _detail_output_path(payload["generated_at"], "有色")

    written_detail_path, payload_path = _persist_etf_pick_internal(detail_path, "# internal\n", payload)
    expected_payload = {
        **payload,
        "_client_final_bundle_contract_version": CLIENT_FINAL_BUNDLE_CONTRACT_VERSION,
    }

    assert written_detail_path == detail_path
    assert payload_path.exists()
    assert payload_path.read_text(encoding="utf-8") == __import__("json").dumps(expected_payload, ensure_ascii=False, indent=2) + "\n"
    assert detail_path.name.isascii()


def test_detail_output_path_uses_ascii_slug_for_chinese_theme(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.commands.etf_pick.resolve_project_path", lambda relative: tmp_path / relative)

    path = _detail_output_path("2026-04-11 15:00:00", "半导体")

    assert path.name.isascii()
    assert path.name.startswith("etf_pick_theme_")
    assert path.name.endswith("_2026-04-11_internal_detail.md")


def test_main_client_final_always_redoes_discovery(monkeypatch, capsys, tmp_path) -> None:
    selection_calls = []

    monkeypatch.setattr(
        "src.commands.etf_pick.build_parser",
        lambda: SimpleNamespace(parse_args=lambda: SimpleNamespace(theme="有色", top=1, config="", client_final=True)),
    )
    monkeypatch.setattr("src.commands.etf_pick.ensure_report_task_registered", lambda report_type: None)  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.setup_logger", lambda level: None)  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.load_config", lambda path=None: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.commands.etf_pick._client_final_runtime_overrides",
        lambda config, client_final, explicit_config_path='': (dict(config), []),  # noqa: ARG005
    )
    monkeypatch.setattr("src.commands.etf_pick._etf_discovery_runtime_overrides", lambda config: (dict(config), []))
    monkeypatch.setattr(
        "src.commands.etf_pick.discover_opportunities",
        lambda *args, **kwargs: {
            "top": [_analysis("512400", "有色金属ETF", 1)],
            "coverage_analyses": [_analysis("512400", "有色金属ETF", 1)],
            "watch_positive": [],
            "scan_pool": 1,
            "passed_pool": 1,
            "runtime_context": {},
            "preferred_sectors": ["科技", "半导体", "通信"],
        },
    )
    monkeypatch.setattr("src.commands.etf_pick._attach_strategy_background_confidence", lambda rows: list(rows))  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.attach_portfolio_overlap_summaries", lambda rows, config: list(rows))  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick._hydrate_selected_etf_profiles", lambda analyses, config=None, limit=3: list(analyses))  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.attach_visuals_to_analyses", lambda analyses, render_theme_variants=False: None)  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.grade_pick_delivery", lambda **kwargs: {"code": "observe", "label": "观察稿", "observe_only": True, "summary_only": True, "notes": []})  # noqa: ARG005
    def fake_selection_context(**kwargs):
        selection_calls.append(kwargs)
        return {
            "delivery_observe_only": True,
            "delivery_notes": [],
            "preferred_sectors": list(kwargs.get("preferred_sectors") or []),
        }

    monkeypatch.setattr("src.commands.etf_pick._selection_context", fake_selection_context)
    monkeypatch.setattr("src.commands.etf_pick.summarize_pick_coverage", lambda analyses: {"total": len(list(analyses or []))})  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.summarize_factor_contracts_from_analyses", lambda analyses, sample_limit=16: {})  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick._detail_markdown", lambda analyses, winner_symbol, selection_context=None: "# internal\n")  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick._persist_etf_pick_internal", lambda detail_path, rendered, payload: (detail_path, tmp_path / 'payload.json'))  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick._export_etf_pick_client_final", lambda **kwargs: ("# client\n", {"markdown": tmp_path / "final.md"}))  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.exported_bundle_lines", lambda bundle: ["bundle exported"])  # noqa: ARG005

    main()

    captured = capsys.readouterr().out
    assert "# client" in captured
    assert "bundle exported" in captured
    assert selection_calls
    assert selection_calls[-1]["preferred_sectors"] == ["科技", "半导体", "通信"]


def test_main_client_final_reranks_after_hydration(monkeypatch, capsys, tmp_path) -> None:
    exported_payload = {}

    monkeypatch.setattr(
        "src.commands.etf_pick.build_parser",
        lambda: SimpleNamespace(parse_args=lambda: SimpleNamespace(theme="", top=2, config="", client_final=True)),
    )
    monkeypatch.setattr("src.commands.etf_pick.ensure_report_task_registered", lambda report_type: None)  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.setup_logger", lambda level: None)  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.load_config", lambda path=None: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.commands.etf_pick._client_final_runtime_overrides",
        lambda config, client_final, explicit_config_path='': (dict(config), []),  # noqa: ARG005
    )
    monkeypatch.setattr("src.commands.etf_pick._etf_discovery_runtime_overrides", lambda config: (dict(config), []))
    broad = _analysis("159363", "创业板人工智能ETF", 3)
    broad["asset_type"] = "cn_etf"
    broad["narrative"] = {"judgment": {"state": "观察为主"}}
    direct = _analysis("515880", "通信ETF", 1)
    direct["asset_type"] = "cn_etf"
    direct["narrative"] = {"judgment": {"state": "观察为主"}}
    monkeypatch.setattr(
        "src.commands.etf_pick.discover_opportunities",
        lambda *args, **kwargs: {
            "top": [broad, direct],
            "coverage_analyses": [broad, direct],
            "watch_positive": [],
            "scan_pool": 2,
            "passed_pool": 2,
            "runtime_context": {},
            "preferred_sectors": ["科技", "通信"],
        },
    )
    monkeypatch.setattr("src.commands.etf_pick._attach_strategy_background_confidence", lambda rows: list(rows))  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.attach_portfolio_overlap_summaries", lambda rows, config: list(rows))  # noqa: ARG005
    monkeypatch.setattr(
        "src.commands.etf_pick._hydrate_selected_etf_profiles",
        lambda analyses, config=None, limit=3: [  # noqa: ARG005
            {**dict(analyses[0]), "rating": {"rank": 1, "label": "有信号但不充分"}},
            {**dict(analyses[1]), "rating": {"rank": 3, "label": "较强机会"}},
        ],
    )
    monkeypatch.setattr("src.commands.etf_pick.attach_visuals_to_analyses", lambda analyses, render_theme_variants=False: None)  # noqa: ARG005
    monkeypatch.setattr(
        "src.commands.etf_pick.grade_pick_delivery",
        lambda **kwargs: {"code": "observe", "label": "观察稿", "observe_only": True, "summary_only": True, "notes": []},
    )  # noqa: ARG005
    monkeypatch.setattr(
        "src.commands.etf_pick._selection_context",
        lambda **kwargs: {"delivery_observe_only": True, "delivery_notes": [], "preferred_sectors": list(kwargs.get("preferred_sectors") or [])},
    )
    monkeypatch.setattr("src.commands.etf_pick.summarize_pick_coverage", lambda analyses: {"total": len(list(analyses or []))})  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick.summarize_factor_contracts_from_analyses", lambda analyses, sample_limit=16: {})  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick._detail_markdown", lambda analyses, winner_symbol, selection_context=None: "# internal\n")  # noqa: ARG005
    monkeypatch.setattr("src.commands.etf_pick._persist_etf_pick_internal", lambda detail_path, rendered, payload: (detail_path, tmp_path / 'payload.json'))  # noqa: ARG005

    def fake_export(**kwargs):
        exported_payload["winner_symbol"] = str(dict(kwargs.get("payload") or {}).get("winner", {}).get("symbol", ""))
        return "# client\n", {"markdown": tmp_path / "final.md"}

    monkeypatch.setattr("src.commands.etf_pick._export_etf_pick_client_final", fake_export)
    monkeypatch.setattr("src.commands.etf_pick.exported_bundle_lines", lambda bundle: ["bundle exported"])  # noqa: ARG005

    main()

    captured = capsys.readouterr().out
    assert "# client" in captured
    assert exported_payload["winner_symbol"] == "515880"
