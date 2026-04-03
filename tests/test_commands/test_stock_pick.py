"""Tests for stock-pick command helpers."""

from __future__ import annotations

import json
from pathlib import Path

from src.commands.stock_pick import (
    _attach_featured_visuals,
    _client_final_runtime_overrides,
    _internal_detail_stem,
    _market_final_stem,
    _preview_runtime_overrides,
    _run_market,
    _watch_positive_candidates,
    build_parser,
    enrich_payload_with_score_history,
)
from src.utils.config import load_config


def _sample_payload(score: int, signal: str, generated_at: str) -> dict:
    return {
        "generated_at": generated_at,
        "data_coverage": {"news_mode": "live", "degraded": False},
        "top": [
            {
                "symbol": "00700.HK",
                "name": "腾讯控股",
                "rating": {"rank": 3},
                "dimensions": {
                    "technical": {"score": 62, "core_signal": "趋势未坏", "factors": [{"name": "趋势", "display_score": "20/20", "signal": "MA20 上方"}]},
                    "fundamental": {"score": 61, "core_signal": "估值中性", "factors": [{"name": "估值", "display_score": "20/30", "signal": "PE 21x"}]},
                    "catalyst": {
                        "score": score,
                        "core_signal": signal,
                        "factors": [
                            {"name": "海外映射", "display_score": "20/20" if score >= 75 else "0/20", "signal": signal},
                            {"name": "负面事件", "display_score": "信息项" if score >= 75 else "-15", "signal": "近 30 日未命中明确稀释/监管负面" if score >= 75 else "Review risk"},
                        ],
                    },
                    "relative_strength": {"score": 47, "core_signal": "相对中性", "factors": [{"name": "超额拐点", "display_score": "15/30", "signal": "5日跑赢"}]},
                    "chips": {"score": None, "core_signal": "缺失", "factors": [{"name": "北向/南向", "display_score": "缺失", "signal": "缺失"}]},
                    "risk": {
                        "score": 50,
                        "core_signal": "波动偏高",
                        "factors": [
                            {"name": "回撤分位", "display_score": "15/30", "signal": "当前回撤 12%"},
                            {"name": "波动率分位", "display_score": "10/25", "signal": "年化波动偏高"},
                        ],
                    },
                    "seasonality": {"score": 40, "core_signal": "中性", "factors": [{"name": "月度胜率", "display_score": "10/30", "signal": "同月胜率一般"}]},
                    "macro": {"score": 10, "core_signal": "轻度逆风", "factors": [{"name": "敏感度向量", "display_score": "10/40", "signal": "rate 逆风"}]},
                },
            }
        ],
    }


def test_enrich_payload_with_score_history_adds_dimension_change_reason(tmp_path: Path):
    snapshot_path = tmp_path / "stock_pick_score_history.json"
    first = enrich_payload_with_score_history(
        _sample_payload(55, "催化不足", "2026-03-09 20:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    assert first["top"][0]["score_changes"] == []
    assert first["is_daily_baseline"] is True
    assert first["baseline_snapshot_at"] == "2026-03-09 20:00:00"

    second = enrich_payload_with_score_history(
        _sample_payload(55, "催化不足", "2026-03-10 20:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    assert second["top"][0]["score_changes"] == []
    assert second["is_daily_baseline"] is True
    assert second["baseline_snapshot_at"] == "2026-03-10 20:00:00"

    third = enrich_payload_with_score_history(
        _sample_payload(75, "海外映射增强", "2026-03-10 22:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    changes = third["top"][0]["score_changes"]
    assert len(changes) == 1
    assert third["is_daily_baseline"] is False
    assert third["comparison_basis_label"] == "当日基准版"
    assert third["comparison_basis_at"] == "2026-03-10 20:00:00"
    assert changes[0]["dimension"] == "catalyst"
    assert changes[0]["previous"] == 55
    assert changes[0]["current"] == 75
    assert "海外映射" in changes[0]["reason"]
    assert "负面事件" in changes[0]["reason"]


def test_enrich_payload_with_score_history_warns_when_model_version_changes(tmp_path: Path):
    snapshot_path = tmp_path / "stock_pick_score_history.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "hk:*": {
                    "latest": {},
                    "daily_baselines": {
                        "2026-03-10": {
                            "generated_at": "2026-03-10 09:00:00",
                            "model_version": "old-model-version",
                            "items": {
                                "00700.HK": {
                                    "name": "腾讯控股",
                                    "rating_rank": 3,
                                    "dimensions": {
                                        "technical": {"score": 62, "core_signal": "趋势未坏", "factors": {}},
                                        "fundamental": {"score": 61, "core_signal": "估值中性", "factors": {}},
                                        "catalyst": {"score": 55, "core_signal": "催化不足", "factors": {}},
                                        "relative_strength": {"score": 47, "core_signal": "相对中性", "factors": {}},
                                        "chips": {"score": None, "core_signal": "缺失", "factors": {}},
                                        "risk": {"score": 50, "core_signal": "波动偏高", "factors": {}},
                                        "seasonality": {"score": 40, "core_signal": "中性", "factors": {}},
                                        "macro": {"score": 10, "core_signal": "轻度逆风", "factors": {}},
                                    },
                                }
                            },
                        }
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = enrich_payload_with_score_history(
        _sample_payload(75, "海外映射增强", "2026-03-10 20:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    assert payload["is_daily_baseline"] is False
    assert payload["comparison_basis_at"] == "2026-03-10 09:00:00"
    assert "old-model-version" in payload["model_version_warning"]


def test_enrich_payload_with_score_history_applies_catalyst_fallback_when_news_degraded(tmp_path: Path):
    snapshot_path = tmp_path / "stock_pick_score_history.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "hk:*": {
                    "latest": {
                        "generated_at": "2026-03-10 20:00:00",
                        "model_version": "stock-pick-2026-03-11-indicator-sanity-v6",
                        "items": {
                            "00700.HK": {
                                "name": "腾讯控股",
                                "rating_rank": 3,
                                "dimensions": {
                                    "technical": {"score": 62, "core_signal": "趋势未坏", "factors": {}},
                                    "fundamental": {"score": 61, "core_signal": "估值中性", "factors": {}},
                                    "catalyst": {"score": 55, "core_signal": "财报窗口+公司新闻", "factors": {}},
                                    "relative_strength": {"score": 47, "core_signal": "相对中性", "factors": {}},
                                    "chips": {"score": None, "core_signal": "缺失", "factors": {}},
                                    "risk": {"score": 50, "core_signal": "波动偏高", "factors": {}},
                                    "seasonality": {"score": 40, "core_signal": "中性", "factors": {}},
                                    "macro": {"score": 10, "core_signal": "轻度逆风", "factors": {}},
                                },
                            }
                        },
                    },
                    "daily_baselines": {},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    degraded_payload = _sample_payload(11, "信息不足", "2026-03-11 20:00:00")
    degraded_payload["data_coverage"] = {"news_mode": "proxy", "degraded": True}
    degraded_payload["top"][0]["dimensions"]["catalyst"]["coverage"] = {
        "news_mode": "proxy",
        "high_confidence_company_news": False,
        "structured_event": False,
        "forward_event": False,
        "degraded": True,
    }

    payload = enrich_payload_with_score_history(
        degraded_payload,
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    catalyst = payload["top"][0]["dimensions"]["catalyst"]
    assert catalyst["score"] > 11
    assert catalyst["coverage"]["fallback_applied"] is True
    assert any(f["name"] == "历史催化回退" for f in catalyst["factors"])


def test_enrich_payload_with_score_history_prefers_full_coverage_population(tmp_path: Path):
    snapshot_path = tmp_path / "stock_pick_score_history.json"
    payload = _sample_payload(55, "催化不足", "2026-03-10 20:00:00")
    payload["top"][0]["asset_type"] = "hk"
    extra = json.loads(json.dumps(payload["top"][0], ensure_ascii=False))
    extra["symbol"] = "300750"
    extra["name"] = "宁德时代"
    extra["asset_type"] = "cn_stock"
    extra["dimensions"]["catalyst"]["coverage"] = {
        "news_mode": "live",
        "high_confidence_company_news": False,
        "structured_event": True,
        "forward_event": False,
        "degraded": False,
    }
    payload["coverage_analyses"] = [payload["top"][0], extra]

    enriched = enrich_payload_with_score_history(
        payload,
        market="all",
        sector_filter="",
        snapshot_path=snapshot_path,
    )

    coverage = enriched["stock_pick_coverage"]
    assert coverage["total"] == 2
    assert any("A股 结构化事件覆盖 100%" in item for item in coverage["lines"])
    assert any("港股 结构化事件覆盖 0%" in item for item in coverage["lines"])


def test_build_parser_defaults_to_cn_market() -> None:
    parser = build_parser()

    args = parser.parse_args([])

    assert args.market == "cn"


def test_client_final_runtime_overrides_apply_lightweight_stock_profile_by_default() -> None:
    config, notes = _client_final_runtime_overrides(
        {},
        client_final=True,
    )

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["market_context"]["skip_market_drivers"] is True
    assert config["news_topic_search_enabled"] is False
    assert config["news_feeds_file"] == "config/news_feeds.empty.yaml"
    assert config["opportunity"]["analysis_workers"] == 2
    assert config["opportunity"]["stock_max_scan_candidates"] == 18
    assert config["structured_stock_intelligence_apis"] == ["forecast", "express", "dividend", "irm_qa_sh", "irm_qa_sz"]
    assert config["stock_news_runtime_mode"] == "structured_only"
    assert config["stock_news_limit"] == 6
    assert config["skip_analysis_proxy_signals_runtime"] is True
    assert config["skip_signal_confidence_runtime"] is True
    assert config["stock_pool_skip_industry_lookup_runtime"] is True
    assert any("跨市场代理" in item for item in notes)
    assert any("板块驱动" in item for item in notes)
    assert any("主题新闻扩搜" in item for item in notes)
    assert any("轻量新闻源配置" in item for item in notes)
    assert any("收窄个股分析并发" in item for item in notes)
    assert any("收窄个股候选池" in item for item in notes)
    assert any("结构化情报源" in item for item in notes)
    assert any("结构化快链" in item for item in notes)
    assert any("单票情报条数上限" in item for item in notes)
    assert any("情绪代理" in item for item in notes)


def test_client_final_runtime_overrides_respect_explicit_stock_config_path() -> None:
    config, notes = _client_final_runtime_overrides(
        {"news_topic_search_enabled": True},
        client_final=True,
        explicit_config_path="config/custom.yaml",
    )

    assert config["news_topic_search_enabled"] is True
    assert "market_context" not in config
    assert "news_feeds_file" not in config
    assert notes == []


def test_preview_runtime_overrides_apply_lightweight_preview_profile_by_default() -> None:
    config, notes = _preview_runtime_overrides({})

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["market_context"]["skip_market_drivers"] is True
    assert config["opportunity"]["analysis_workers"] == 2
    assert config["opportunity"]["stock_max_scan_candidates"] == 15
    assert config["news_topic_search_enabled"] is False
    assert config["structured_stock_intelligence_apis"] == ["forecast", "express", "dividend", "irm_qa_sh", "irm_qa_sz"]
    assert config["stock_news_runtime_mode"] == "structured_only"
    assert config["stock_news_limit"] == 6
    assert config["skip_analysis_proxy_signals_runtime"] is True
    assert config["skip_signal_confidence_runtime"] is True
    assert config["stock_pool_skip_industry_lookup_runtime"] is True
    assert any("跨市场代理" in item for item in notes)
    assert any("分析并发" in item for item in notes)
    assert any("候选池" in item for item in notes)
    assert any("主题新闻扩搜" in item for item in notes)
    assert any("结构化情报" in item for item in notes)
    assert any("结构化快链" in item for item in notes)
    assert any("单票情报条数上限" in item for item in notes)
    assert any("情绪代理" in item for item in notes)


def test_preview_runtime_overrides_respect_explicit_stock_config_path() -> None:
    config, notes = _preview_runtime_overrides(
        {"opportunity": {"analysis_workers": 5}},
        explicit_config_path="config/custom.yaml",
    )

    assert config["opportunity"]["analysis_workers"] == 5
    assert "market_context" not in config
    assert notes == []


def test_repo_stock_pick_fast_profile_keeps_lightweight_runtime_contract() -> None:
    config = load_config("config/config.stock_pick_fast.yaml")

    assert config["news_feeds_file"] == "config/news_feeds.empty.yaml"
    assert config["news_topic_search_enabled"] is False
    assert config["structured_stock_intelligence_apis"] == ["forecast", "express", "dividend", "irm_qa_sh", "irm_qa_sz"]
    assert config["stock_news_runtime_mode"] == "structured_only"
    assert config["stock_news_limit"] == 6
    assert config["skip_analysis_proxy_signals_runtime"] is True
    assert config["skip_signal_confidence_runtime"] is True
    assert config["stock_pool_skip_industry_lookup_runtime"] is True
    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["opportunity"]["analysis_workers"] == 2
    assert config["opportunity"]["stock_max_scan_candidates"] == 15


def test_sector_filtered_final_paths_keep_scope_in_filename() -> None:
    assert _internal_detail_stem("cn", "2026-03-22 13:36:00", "黄金") == "stock_picks_cn_黄金_2026-03-22_internal_detail"
    assert _market_final_stem("cn", "2026-03-22 13:36:00", "黄金 / 贵金属") == "stock_picks_cn_黄金_贵金属_2026-03-22_final"


def test_run_market_reuses_shared_context(monkeypatch) -> None:
    shared_context = {"regime": {"current_regime": "deflation"}}
    captured: dict = {}

    def fake_discover(config, top_n=20, market="all", sector_filter="", context=None):  # noqa: ANN001
        captured["context"] = context
        captured["market"] = market
        return {
            "generated_at": "2026-03-17 18:00:00",
            "market": market,
            "data_coverage": {"news_mode": "live", "degraded": False},
            "top": [],
            "coverage_analyses": [],
            "watch_positive": [],
        }

    monkeypatch.setattr("src.commands.stock_pick.discover_stock_opportunities", fake_discover)
    monkeypatch.setattr("src.commands.stock_pick.enrich_payload_with_score_history", lambda payload, market, sector_filter: payload)  # noqa: ARG005,E501
    monkeypatch.setattr("src.commands.stock_pick._attach_featured_visuals", lambda payload: payload)

    payload = _run_market({}, "cn", 5, "", context=shared_context)

    assert payload["market"] == "cn"
    assert captured["market"] == "cn"
    assert captured["context"] is shared_context


def test_run_market_client_final_reenriches_finalists_after_light_discovery(monkeypatch) -> None:
    shared_context = {"regime": {"current_regime": "deflation"}}
    captured: dict = {}

    def fake_discover(config, top_n=20, market="all", sector_filter="", context=None):  # noqa: ANN001
        captured["discover_config"] = dict(config)
        captured["discover_context"] = context
        return {
            "generated_at": "2026-04-03 10:00:00",
            "market": market,
            "data_coverage": {"news_mode": "proxy", "degraded": True},
            "top": [
                {
                    "symbol": "300750",
                    "name": "宁德时代",
                    "asset_type": "cn_stock",
                    "metadata": {"sector": "新能源", "chain_nodes": ["锂电"]},
                }
            ],
            "coverage_analyses": [
                {
                    "symbol": "300750",
                    "name": "宁德时代",
                    "asset_type": "cn_stock",
                    "metadata": {"sector": "新能源", "chain_nodes": ["锂电"]},
                }
            ],
            "watch_positive": [],
        }

    def fake_analyze(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001
        captured["analyze_config"] = dict(config)
        captured["analyze_context"] = context
        captured["metadata_override"] = dict(metadata_override or {})
        return {
            "symbol": symbol,
            "name": "宁德时代",
            "asset_type": asset_type,
            "generated_at": "2026-04-03 10:05:00",
            "metadata": {"sector": "新能源", "chain_nodes": ["锂电"]},
            "rating": {"rank": 2, "label": "储备机会", "stars": "⭐"},
            "dimensions": {
                "technical": {"score": 42},
                "fundamental": {"score": 88},
                "catalyst": {"score": 44},
                "relative_strength": {"score": 51},
                "chips": {"score": 25},
                "risk": {"score": 61},
                "seasonality": {"score": 0},
                "macro": {"score": 20},
            },
            "excluded": False,
        }

    monkeypatch.setattr("src.commands.stock_pick.discover_stock_opportunities", fake_discover)
    monkeypatch.setattr("src.commands.stock_pick.analyze_opportunity", fake_analyze)
    monkeypatch.setattr("src.commands.stock_pick.enrich_payload_with_score_history", lambda payload, market, sector_filter: payload)  # noqa: ARG005,E501
    monkeypatch.setattr("src.commands.stock_pick.attach_portfolio_overlap_summaries", lambda rows, config: rows)  # noqa: ARG005
    monkeypatch.setattr("src.commands.stock_pick._attach_featured_visuals", lambda payload: payload)

    payload = _run_market({}, "cn", 5, "", context=shared_context, enrich_finalists=True)

    assert captured["discover_context"] is shared_context
    assert captured["discover_config"]["skip_cn_stock_chip_snapshot_runtime"] is True
    assert captured["discover_config"]["skip_cn_stock_direct_news_runtime"] is True
    assert captured["discover_config"]["skip_cn_stock_unlock_pressure_runtime"] is True
    assert captured["discover_config"]["skip_cn_stock_pledge_risk_runtime"] is True
    assert "skip_cn_stock_chip_snapshot_runtime" not in captured["analyze_config"]
    assert captured["metadata_override"]["sector"] == "新能源"
    assert payload["top"][0]["dimensions"]["fundamental"]["score"] == 88


def test_watch_positive_candidates_softly_prefer_portfolio_style_complement() -> None:
    repeat = _sample_payload(55, "催化不足", "2026-03-10 20:00:00")["top"][0]
    repeat["symbol"] = "600406"
    repeat["name"] = "国电南瑞"
    repeat["rating"] = {"rank": 1}
    repeat["dimensions"]["fundamental"]["score"] = 66
    repeat["portfolio_overlap_summary"] = {
        "overlap_label": "同一行业主线加码",
        "style_conflict_label": "同风格延伸",
        "same_sector_weight": 0.31,
        "same_region_weight": 0.78,
    }

    complement = _sample_payload(55, "催化不足", "2026-03-10 20:00:00")["top"][0]
    complement["symbol"] = "000651"
    complement["name"] = "格力电器"
    complement["rating"] = {"rank": 1}
    complement["dimensions"]["fundamental"]["score"] = 64
    complement["portfolio_overlap_summary"] = {
        "overlap_label": "重复度较低",
        "style_conflict_label": "风格补位",
        "same_sector_weight": 0.0,
        "same_region_weight": 0.12,
    }

    rows = _watch_positive_candidates([repeat, complement])

    assert [item["symbol"] for item in rows[:2]] == ["000651", "600406"]


def test_attach_featured_visuals_backfills_top_and_watch_from_richer_coverage_item(monkeypatch) -> None:
    top_item = {
        "asset_type": "cn_stock",
        "symbol": "601899",
        "name": "紫金矿业",
        "generated_at": "2026-03-31 10:00:00",
        "rating": {"rank": 1},
        "dimensions": {
            "technical": {"score": 35},
            "fundamental": {"score": 83},
            "catalyst": {"score": 0},
            "relative_strength": {"score": 27},
            "risk": {"score": 25},
        },
    }
    watch_item = json.loads(json.dumps(top_item, ensure_ascii=False))
    coverage_item = json.loads(json.dumps(top_item, ensure_ascii=False))
    coverage_item["history"] = "full-history"
    coverage_item["technical_raw"] = {"ma_system": {"mas": {"MA20": 1.0}}}
    payload = {
        "top": [top_item],
        "watch_positive": [watch_item],
        "coverage_analyses": [coverage_item],
    }

    def fake_attach(analyses):  # noqa: ANN001
        for analysis in analyses:
            if analysis.get("history") == "full-history":
                analysis["visuals"] = {"dashboard": "/tmp/601899_dashboard.png"}

    monkeypatch.setattr("src.commands.stock_pick.attach_visuals_to_analyses", fake_attach)

    enriched = _attach_featured_visuals(payload)

    assert enriched["top"][0]["visuals"]["dashboard"] == "/tmp/601899_dashboard.png"
    assert enriched["watch_positive"][0]["visuals"]["dashboard"] == "/tmp/601899_dashboard.png"
    assert enriched["coverage_analyses"][0]["visuals"]["dashboard"] == "/tmp/601899_dashboard.png"
