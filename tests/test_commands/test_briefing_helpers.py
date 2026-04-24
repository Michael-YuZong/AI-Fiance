"""Tests for briefing helper proxy disclosures."""

from __future__ import annotations

from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pandas as pd

import src.commands.briefing as briefing_module
from src.commands.briefing import (
    _appendix_derivative_lines,
    _appendix_allocation_rows,
    _action_lines,
    _briefing_a_share_watch_rows,
    _briefing_evidence_rows,
    _briefing_news_backfill_groups,
    _build_evening_payload,
    _client_final_runtime_overrides,
    _build_noon_payload,
    _calendar_review_trigger_lines,
    _load_briefing_global_proxy,
    _load_same_day_stock_pick_market_event_rows,
    _briefing_shared_market_context,
    _build_market_payload,
    _briefing_internal_dir,
    _compact_validation_lines,
    _compact_headline_lines,
    _coverage_metadata,
    _core_event_lines,
    _domestic_overview_rows,
    _export_briefing_client_final,
    _export_pdf,
    _flow_lines,
    _load_same_day_briefing,
    _market_event_rows,
    _merge_quality_lines,
    _noon_action_lines,
    _monitor_alerts,
    _portfolio_lines,
    _portfolio_priority_action_line,
    _portfolio_review_queue,
    _portfolio_table_rows,
    _primary_narrative,
    _review_queue_transition_lines,
    _persist_briefing,
    _persist_briefing_payload,
    _quality_lines,
    _source_quality_lines,
    _style_rows,
    _sentiment_lines,
    _theme_information_environment,
    _timed_collect,
    _tomorrow_action_lines,
    _render_briefing_charts,
    _watchlist_review_trigger_lines,
    _yesterday_review_summary_lines,
    build_parser,
)


def test_flow_lines_include_proxy_confidence_and_limitations(monkeypatch) -> None:
    class _FakeFlowCollector:
        def __init__(self, _config):
            pass

        def collect(self, snapshots):
            assert snapshots
            return {
                "lines": ["成长与黄金的相对强弱接近。"],
                "confidence_label": "中",
                "limitations": ["这是相对强弱代理，不是机构申购赎回原始数据。"],
            }

    monkeypatch.setattr("src.commands.briefing.GlobalFlowCollector", _FakeFlowCollector)

    lines = _flow_lines([SimpleNamespace(symbol="561380")], {})

    assert any("代理置信度 `中`" in item for item in lines)
    assert any("限制：" in item for item in lines)


def test_client_final_runtime_overrides_apply_lightweight_briefing_profile_by_default() -> None:
    config, notes = _client_final_runtime_overrides(
        {
            "briefing_snapshot_timeout_seconds": 15,
            "briefing_collector_timeout_seconds": 15,
        },
        client_final=True,
    )

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["briefing_snapshot_timeout_seconds"] == 8
    assert config["briefing_collector_timeout_seconds"] == 8
    assert config["briefing_signal_timeout_seconds"] == 10
    assert config["news_topic_search_enabled"] is False
    assert config["briefing_search_backfill_enabled"] is True
    assert config["briefing_backfill_timeout_seconds"] == 12
    assert config["briefing_a_share_watch_timeout_seconds"] == 20
    assert config["news_feeds_file"] == "config/news_feeds.briefing_light.yaml"
    assert any("跨市场代理" in item for item in notes)
    assert any("快照超时阈值" in item for item in notes)
    assert any("采集超时阈值" in item for item in notes)
    assert any("关键情报" in item for item in notes)
    assert any("主题新闻扩搜" in item for item in notes)
    assert any("query-group 搜索回填" in item for item in notes)
    assert any("轻量新闻源配置" in item for item in notes)


def test_client_final_runtime_overrides_respect_explicit_briefing_config_path() -> None:
    config, notes = _client_final_runtime_overrides(
        {"briefing_snapshot_timeout_seconds": 15},
        client_final=True,
        explicit_config_path="config/custom.yaml",
    )

    assert config["briefing_snapshot_timeout_seconds"] == 15
    assert "market_context" not in config
    assert "news_topic_search_enabled" not in config
    assert "news_feeds_file" not in config
    assert notes == []


def test_briefing_a_share_watch_rows_uses_structured_fast_runtime(monkeypatch) -> None:
    captured = {}

    def fake_discover_stock_opportunities(config, **kwargs):
        captured["config"] = dict(config or {})
        captured["kwargs"] = dict(kwargs or {})
        return {
            "coverage_analyses": [],
            "top": [],
            "scan_pool": 0,
            "passed_pool": 0,
            "candidate_limit": kwargs.get("max_candidates", 0),
        }

    monkeypatch.setattr(briefing_module, "discover_stock_opportunities", fake_discover_stock_opportunities)

    _briefing_a_share_watch_rows({"briefing_a_share_top_n": 5}, shared_context={})

    effective = captured["config"]
    assert effective["news_topic_search_enabled"] is False
    assert effective["stock_news_runtime_mode"] == "structured_only"
    assert effective["structured_stock_intelligence_apis"] == ["forecast", "express", "dividend", "stk_surv", "irm_qa_sh", "irm_qa_sz"]
    assert effective["skip_catalyst_dynamic_search_runtime"] is True
    assert effective["skip_cn_stock_direct_news_runtime"] is True
    assert effective["skip_analysis_proxy_signals_runtime"] is True
    assert effective["skip_signal_confidence_runtime"] is True
    assert effective["skip_index_topic_bundle_runtime"] is True
    assert effective["skip_cn_stock_unlock_pressure_runtime"] is True
    assert effective["skip_cn_stock_pledge_risk_runtime"] is True
    assert effective["skip_cn_stock_regulatory_risk_runtime"] is True
    assert effective["skip_cn_stock_chip_snapshot_runtime"] is True
    assert effective["skip_cn_stock_capital_flow_runtime"] is True
    assert effective["skip_cn_stock_margin_runtime"] is True
    assert effective["skip_cn_stock_board_action_runtime"] is True
    assert effective["stock_pool_skip_industry_lookup_runtime"] is True
    assert effective["opportunity"]["analysis_workers"] == 3
    assert captured["kwargs"]["top_n"] == 6
    assert captured["kwargs"]["max_candidates"] == 8


def test_briefing_export_pdf_prefers_fast_client_export(monkeypatch, tmp_path) -> None:
    calls = {}

    def _fake_markdown_to_html(markdown_text, title, source_dir=None):
        calls["html"] = {"markdown": markdown_text, "title": title, "source_dir": source_dir}
        return "<html><body>ok</body></html>"

    def _fake_export(markdown_text, html_path, pdf_path):
        calls["export"] = {"markdown": markdown_text, "html_path": html_path, "pdf_path": pdf_path}
        pdf_path.write_bytes(b"%PDF-1.4 test")

    monkeypatch.setattr("src.output.client_export.markdown_to_html", _fake_markdown_to_html)
    monkeypatch.setattr("src.output.client_export._export_pdf", _fake_export)

    pdf_path = tmp_path / "briefing.pdf"
    _export_pdf("# briefing", pdf_path)

    assert calls["html"]["title"] == "briefing"
    assert calls["html"]["source_dir"] == tmp_path
    assert calls["export"]["html_path"] == pdf_path.with_suffix(".html")
    assert pdf_path.exists()


def test_render_briefing_charts_uses_svg_assets(monkeypatch, tmp_path) -> None:
    class _FakeRenderer:
        def __init__(self) -> None:
            self.enabled = True
            self.output_dir = tmp_path

        def _asset_path(self, base: str, kind: str) -> Path:
            return self.output_dir / f"{base}_{kind}.svg"

        def _render_windows(self, analysis, history, path) -> None:  # noqa: ARG002
            path.write_text("<svg/>", encoding="utf-8")

        def _render_indicators(self, analysis, history, path) -> None:  # noqa: ARG002
            path.write_text("<svg/>", encoding="utf-8")

    monkeypatch.setattr("src.output.analysis_charts.AnalysisChartRenderer", _FakeRenderer)
    snapshot = SimpleNamespace(
        symbol="561380",
        name="电网ETF",
        history=pd.DataFrame(
            {
                "date": pd.date_range("2026-03-01", periods=5, freq="B"),
                "open": [1, 1, 1, 1, 1],
                "high": [1, 1, 1, 1, 1],
                "low": [1, 1, 1, 1, 1],
                "close": [1, 1, 1, 1, 1],
                "volume": [1, 1, 1, 1, 1],
                "amount": [1, 1, 1, 1, 1],
            }
        ),
        technical={},
    )

    charts = _render_briefing_charts([snapshot])

    key = "电网ETF (561380)"
    assert charts[key]["windows"].endswith(".svg")
    assert charts[key]["indicators"].endswith(".svg")


def test_export_briefing_client_final_uses_client_renderer_for_market_mode(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_finalize_client_markdown(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return {"markdown": kwargs["markdown_path"]}

    monkeypatch.setattr(briefing_module, "finalize_client_markdown", fake_finalize_client_markdown)

    payload = {
        "generated_at": "2026-03-12 08:30:00",
        "news_report": {
            "items": [
                {
                    "category": "china_market_domestic",
                    "title": "国家电网发布新一轮特高压招标",
                    "source": "CNINFO",
                    "published_at": "2026-03-12 07:20:00",
                    "link": "https://example.com/grid",
                    "freshness_bucket": "fresh",
                }
            ]
        },
        "headline_lines": ["今天更像结构性行情。", "风险偏好没有全面回暖。", "更适合先看主线确认。"],
        "action_lines": ["先小仓。", "等确认。"],
        "theme_tracking_rows": [["新能源", "产业链订单改善", "景气方向还在。", "中线配置", "需要等量价确认。", "当前更多是背景储备和信息环境支持，不等于直接催化已兑现。"]],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": ["制造业 PMI 50.1，较前值回升；新订单 50.8、生产 51.7。"],
        "regime": {"current_regime": "recovery", "reasoning": ["PMI 回到 50 上方。"]},
        "day_theme": "中国政策 / 内需确定性",
        "data_coverage": "中国宏观 | Watchlist 行情 | RSS新闻",
        "missing_sources": "跨市场代理",
        "evidence_rows": [
            ["分析生成时间", "2026-03-12 08:30:00"],
            ["A股观察池来源", "Tushare 优先全市场初筛；初筛 `60` 只，完整分析 `8` 只，候选上限 `16` 只。"],
        ],
        "quality_lines": ["本次新闻覆盖源: Reuters / 财联社。"],
        "proxy_contract": {},
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]],
        "a_share_watch_lines": ["A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。"],
        "alerts": [],
    }

    client_markdown, bundle = _export_briefing_client_final(
        mode="market",
        payload=payload,
        rendered="# 全市场行情简报\n\n## 1. 市场结论\n\n- 原始详细稿占位。\n",
        detail_path=tmp_path / "market_briefing_2026-03-12.md",
    )

    assert bundle["markdown"].name == "market_briefing_2026-03-12_client_final.md"
    assert "# 今日晨报 | 2026-03-12" in client_markdown
    assert "## 先看执行" in client_markdown
    assert "## 今日A股观察池" in client_markdown
    assert "# 全市场行情简报" not in client_markdown
    assert captured["client_markdown"] == client_markdown


def test_backfill_briefing_news_report_returns_unchanged_when_existing_items_are_sufficient() -> None:
    report = {
        "items": [
            {"title": "A", "source": "Reuters", "link": "https://example.com/a"},
            {"title": "B", "source": "Bloomberg", "link": "https://example.com/b"},
            {"title": "C", "source": "财联社"},
        ],
        "note": "已有情报",
    }

    result = briefing_module._backfill_briefing_news_report(
        report,
        config={},
        narrative={"theme": "broad_market_repair"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()},
    )

    assert result == report


def test_briefing_news_backfill_groups_include_active_theme_and_geopolitical_queries() -> None:
    groups = _briefing_news_backfill_groups(
        narrative={"theme": "broad_market_repair"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        snapshots=[
            SimpleNamespace(symbol="513120", name="港股创新药ETF", sector="医药", return_1d=0.0724),
            SimpleNamespace(symbol="515070", name="人工智能ETF", sector="科技", return_1d=0.0330),
            SimpleNamespace(symbol="512400", name="有色金属ETF", sector="有色", return_1d=0.0217),
        ],
    )

    flat = " | ".join(" / ".join(group) for group in groups[:8])
    assert "创新药" in flat
    assert "智谱" in flat
    assert "新易盛" in flat
    assert "中东" in flat


def test_backfill_briefing_news_report_uses_shared_intel_backfill(monkeypatch) -> None:
    calls = []

    monkeypatch.setattr(
        briefing_module,
        "_briefing_news_backfill_groups",
        lambda **kwargs: [["黄金", "避险"], ["科技", "修复"]],
    )

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ARG001
        calls.append(
            {
                "query": query,
                "structured_only": structured_only,
                "limit": limit,
                "recent_days": recent_days,
                "note_prefix": note_prefix,
            }
        )
        if query == "黄金 避险 科技 修复":
            return {
                "mode": "live",
                "items": [
                    {
                        "title": "黄金避险回填",
                        "link": "https://example.com/gold",
                        "source": "财联社",
                        "published_at": "2026-03-31 09:00:00",
                    }
                ],
                "all_items": [
                    {
                        "title": "黄金避险回填",
                        "link": "https://example.com/gold",
                        "source": "财联社",
                        "published_at": "2026-03-31 09:00:00",
                    }
                ],
                "source_list": ["财联社"],
                "note": "共享 intel 已命中黄金避险。",
                "disclosure": "共享 intel disclosure",
            }
        if query == "科技 修复":
            return {
                "mode": "live",
                "items": [
                    {
                        "title": "科技修复回填",
                        "link": "https://example.com/tech",
                        "source": "Reuters",
                        "published_at": "2026-03-31 10:00:00",
                    }
                ],
                "all_items": [
                    {
                        "title": "科技修复回填",
                        "link": "https://example.com/tech",
                        "source": "Reuters",
                        "published_at": "2026-03-31 10:00:00",
                    }
                ],
                "source_list": ["Reuters"],
                "note": "共享 intel 已命中科技修复。",
                "disclosure": "共享 intel disclosure",
            }
        return {
            "mode": "live",
            "items": [],
            "all_items": [],
            "source_list": [],
            "note": "共享 intel 当前空表。",
            "disclosure": "共享 intel disclosure",
        }

    monkeypatch.setattr(briefing_module, "collect_intel_news_report", fake_collect_intel_news_report)
    monkeypatch.setattr(
        briefing_module,
        "NewsCollector",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("backfill should not instantiate NewsCollector")),
    )

    result = briefing_module._backfill_briefing_news_report(
        {"items": [], "lines": []},
        config={"briefing_search_backfill_enabled": True},
        narrative={"theme": "gold_defense"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()},
    )

    assert [item["query"] for item in calls] == ["黄金 避险 科技 修复", "科技 修复"]
    assert all(call["structured_only"] is False for call in calls)
    assert result["mode"] == "live"
    assert [item["title"] for item in result["items"]] == ["黄金避险回填", "科技修复回填"]
    assert result["source_list"] == ["Reuters", "财联社"]
    assert "briefing 共享 intel 回填" in result["note"]
    assert "共享 intel 已命中黄金避险" in result["note"]
    assert "共享 intel 已命中科技修复" in result["note"]
    assert result["disclosure"] == "共享 intel disclosure"
    assert result["fallback"] == "intel_shared_upstream"
    assert result["as_of"] == "2026-03-31 10:00:00"
    assert result["summary_lines"]
    assert any("主题聚类" in line for line in result["summary_lines"])
    assert any("来源分层" in line for line in result["summary_lines"])


def test_backfill_briefing_news_report_prioritizes_geopolitics_when_proxy_lines_hint_major_event(monkeypatch) -> None:
    calls = []

    monkeypatch.setattr(
        briefing_module,
        "_briefing_news_backfill_groups",
        lambda **kwargs: [
            ["智谱 大模型 A股", "AI 应用 国产模型"],
            ["中东 停火 风险偏好", "ceasefire middle east market"],
        ],
    )

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ARG001
        calls.append(query)
        if "美伊 停火" in query or "中东 停火 风险偏好" in query:
            return {
                "mode": "live",
                "items": [
                    {
                        "title": "美伊接受停火提议，国际油价回落、风险资产走强",
                        "link": "https://example.com/ceasefire",
                        "source": "财联社",
                        "published_at": "2026-04-08 07:25:00",
                        "theme_bucket": "政策/宏观",
                        "source_tier": "primary_media",
                        "source_tier_label": "主流媒体",
                    }
                ],
                "all_items": [],
                "source_list": ["财联社"],
                "note": "地缘缓和主线已命中。",
                "disclosure": "共享 intel disclosure",
            }
        return {
            "mode": "live",
            "items": [],
            "all_items": [],
            "source_list": [],
            "note": "共享 intel 当前空表。",
            "disclosure": "共享 intel disclosure",
        }

    monkeypatch.setattr(briefing_module, "collect_intel_news_report", fake_collect_intel_news_report)

    result = briefing_module._backfill_briefing_news_report(
        {
            "mode": "proxy",
            "items": [],
            "lines": [
                "[能源与地缘] 风险资产相对更稳，说明能源和地缘风险暂未主导盘面。",
                "[国际局势] 外部风险处于可跟踪但未失控状态。",
            ],
        },
        config={"briefing_search_backfill_enabled": True},
        narrative={"theme": "ai_semis"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()},
    )

    assert calls[0] == "美伊 停火 中东 风险偏好 财联社 Reuters"
    assert result["items"][0]["title"].startswith("美伊接受停火提议")
    assert result["mode"] == "live"


def test_backfill_briefing_news_report_stops_after_primary_combined_query_when_sufficient(monkeypatch) -> None:
    calls = []

    monkeypatch.setattr(
        briefing_module,
        "_briefing_news_backfill_groups",
        lambda **kwargs: [["黄金", "避险"], ["科技", "修复"], ["A股", "轮动"]],
    )

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ARG001
        calls.append(query)
        return {
            "mode": "live",
            "items": [
                {
                    "title": "黄金避险回填",
                    "link": "https://example.com/gold",
                    "source": "财联社",
                    "published_at": "2026-03-31 09:00:00",
                    "theme_bucket": "价格/供需",
                    "source_tier": "primary_media",
                    "source_tier_label": "主流媒体",
                },
                {
                    "title": "科技修复回填",
                    "link": "https://example.com/tech",
                    "source": "Reuters",
                    "published_at": "2026-03-31 10:00:00",
                    "theme_bucket": "产业/公司",
                    "source_tier": "primary_media",
                    "source_tier_label": "主流媒体",
                },
                {
                    "title": "A股轮动回填",
                    "link": "https://example.com/market",
                    "source": "Tushare",
                    "published_at": "2026-03-31 10:30:00",
                    "theme_bucket": "资金/交易",
                    "source_tier": "structured",
                    "source_tier_label": "结构化",
                },
                {
                    "title": "宏观快讯回填",
                    "link": "https://example.com/macro",
                    "source": "中证网",
                    "published_at": "2026-03-31 11:00:00",
                    "theme_bucket": "政策/宏观",
                    "source_tier": "primary_media",
                    "source_tier_label": "主流媒体",
                },
            ],
            "all_items": [],
            "source_list": ["Reuters", "Tushare", "中证网", "财联社"],
            "note": "主合并查询已足够。",
            "disclosure": "共享 intel disclosure",
        }

    monkeypatch.setattr(briefing_module, "collect_intel_news_report", fake_collect_intel_news_report)

    result = briefing_module._backfill_briefing_news_report(
        {"items": [], "lines": []},
        config={"briefing_search_backfill_enabled": True},
        narrative={"theme": "gold_defense"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()},
    )

    assert calls == ["黄金 避险 科技 修复"]
    assert len(result["items"]) == 4
    assert result["cluster_count"] == 4
    assert any("主题聚类" in line for line in result["summary_lines"])


def test_backfill_briefing_news_report_uses_structured_only_when_backfill_search_disabled(monkeypatch) -> None:
    calls = []

    monkeypatch.setattr(
        briefing_module,
        "_briefing_news_backfill_groups",
        lambda **kwargs: [["宏观", "政策"]],
    )

    def fake_collect_intel_news_report(query, *, config, explicit_symbol="", limit=6, recent_days=7, structured_only=False, note_prefix=""):  # noqa: ARG001
        calls.append(
            {
                "query": query,
                "structured_only": structured_only,
                "note_prefix": note_prefix,
            }
        )
        return {
            "mode": "proxy" if structured_only else "live",
            "items": [
                {
                    "title": "结构化情报回填",
                    "link": "https://example.com/structured",
                    "source": "Tushare",
                    "published_at": "2026-03-31 11:00:00",
                }
            ],
            "all_items": [
                {
                    "title": "结构化情报回填",
                    "link": "https://example.com/structured",
                    "source": "Tushare",
                    "published_at": "2026-03-31 11:00:00",
                }
            ],
            "source_list": ["Tushare"],
            "note": "结构化情报命中。",
            "disclosure": "structured disclosure",
        }

    monkeypatch.setattr(briefing_module, "collect_intel_news_report", fake_collect_intel_news_report)

    result = briefing_module._backfill_briefing_news_report(
        {"items": [], "lines": []},
        config={"briefing_search_backfill_enabled": False},
        narrative={"theme": "broad_market_repair"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()},
    )

    assert calls == [{"query": "宏观 政策", "structured_only": True, "note_prefix": "briefing 共享 intel 回填"}]
    assert result["mode"] == "live"
    assert result["items"][0]["title"] == "结构化情报回填"
    assert result["fallback"] == "intel_shared_upstream"
    assert result["disclosure"] == "structured disclosure"


def test_briefing_news_backfill_groups_include_concepts_hot_names_and_pulse() -> None:
    groups = briefing_module._briefing_news_backfill_groups(
        narrative={"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame([{"板块名称": "半导体"}]),
            "concept_spot": pd.DataFrame(
                [
                    {"名称": "创新药", "涨跌幅": 6.2},
                    {"名称": "智谱AI", "涨跌幅": 4.8},
                ]
            ),
            "hot_rank": pd.DataFrame(
                [
                    {"股票名称": "新易盛", "涨跌幅": 12.1},
                    {"股票名称": "中际旭创", "涨跌幅": 8.4},
                ]
            ),
        },
        pulse={"zt_pool": pd.DataFrame([{"所属行业": "医药生物"}])},
    )

    flattened = " | ".join(" / ".join(group) for group in groups)
    assert "创新药 A股 盘面" in flattened
    assert "智谱AI A股 盘面" in flattened
    assert "新易盛 A股 大涨" in flattened
    assert "医药生物" in flattened and "A股 涨停" in flattened


def test_sentiment_lines_include_proxy_confidence_and_limitations(monkeypatch) -> None:
    class _FakeSentimentCollector:
        def __init__(self, _config):
            pass

        def collect(self, symbol, market_snapshot):
            return {
                "symbol": symbol,
                "aggregate": {
                    "interpretation": "情绪指数 61.0，当前未出现极端一致预期。",
                    "confidence_label": "高",
                    "limitations": ["这是价格和量能推导出的情绪代理，不是真实社媒抓取。"],
                },
            }

    snapshots = [
        SimpleNamespace(symbol="561380", signal_score=80.0, return_20d=0.12, return_1d=0.01, return_5d=0.03, volume_ratio=1.2, trend="多头"),
        SimpleNamespace(symbol="GLD", signal_score=20.0, return_20d=-0.02, return_1d=-0.01, return_5d=-0.02, volume_ratio=0.8, trend="空头"),
    ]
    monkeypatch.setattr("src.commands.briefing.SocialSentimentCollector", _FakeSentimentCollector)

    lines = _sentiment_lines(snapshots, {})

    assert any("代理置信度 `高`" in item for item in lines)
    assert any("限制：" in item for item in lines)


def test_briefing_internal_dir_and_same_day_loader_use_internal_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(briefing_module, "resolve_project_path", lambda path="": tmp_path / str(path))
    monkeypatch.setattr(briefing_module, "_export_pdf", lambda markdown_text, pdf_path: None)  # noqa: ARG005

    detail_path = _persist_briefing("# demo", "daily")
    expected_date = briefing_module.datetime.now().strftime("%Y-%m-%d")

    assert detail_path == tmp_path / f"reports/briefings/internal/daily_briefing_{expected_date}.md"
    assert _briefing_internal_dir() == tmp_path / "reports/briefings/internal"
    assert _load_same_day_briefing("daily") == "# demo"


def test_briefing_payload_sidecar_uses_internal_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(briefing_module, "resolve_project_path", lambda path="": tmp_path / str(path))
    monkeypatch.setattr(briefing_module, "_export_pdf", lambda markdown_text, pdf_path: None)  # noqa: ARG005

    detail_path = _persist_briefing("# demo", "daily")
    payload_path = _persist_briefing_payload(
        detail_path,
        {
            "generated_at": "2026-04-05 23:00:00",
            "title": "每日晨报",
            "a_share_watch_candidates": [{"symbol": "300274"}],
        },
    )

    assert payload_path.name.endswith("_payload.json")
    loaded_payload = __import__("json").loads(payload_path.read_text(encoding="utf-8"))
    assert detail_path.read_text(encoding="utf-8") == "# demo"
    assert loaded_payload["title"] == "每日晨报"
    assert loaded_payload["generated_at"] == "2026-04-05 23:00:00"


def test_briefing_coverage_and_quality_disclose_stale_monitor_rows() -> None:
    monitor_rows = [{"name": "布伦特原油", "data_warning": "实时刷新失败", "stale_age_hours": 36.0}]

    coverage, missing = _coverage_metadata({"items": []}, [], [], "", monitor_rows)
    quality = _quality_lines({"items": []}, {"lines": []}, monitor_rows)

    assert "宏观资产监控" in coverage
    assert "宏观资产监控(实时刷新)" in missing
    assert any("陈旧缓存回退" in item for item in quality)


def test_merge_quality_lines_prioritizes_runtime_notes_and_deduplicates() -> None:
    lines = _merge_quality_lines(
        [
            "本轮 `client-final` 已自动关闭晨报全局主题新闻扩搜，优先使用结构化事件和已命中的新闻线索。",
            "本轮 `client-final` 已自动切到轻量新闻源配置，避免晨报被全局新闻拉取慢链拖住。",
        ],
        [
            "本轮 `client-final` 已自动切到轻量新闻源配置，避免晨报被全局新闻拉取慢链拖住。",
            "ℹ️ HSTECH 当前使用 `3033.HK` 作为行情代理。",
        ],
    )

    assert lines[0].startswith("本轮 `client-final` 已自动关闭晨报全局主题新闻扩搜")
    assert lines[1].startswith("本轮 `client-final` 已自动切到轻量新闻源配置")
    assert lines[2].startswith("ℹ️ HSTECH 当前使用")
    assert len(lines) == 3


def test_monitor_alerts_disclose_missing_or_stale_macro_monitor_data() -> None:
    assert any("未能完成实时刷新" in item for item in _monitor_alerts([]))
    stale_alerts = _monitor_alerts([{"name": "布伦特原油", "data_warning": "实时刷新失败"}])
    assert any("布伦特原油" in item for item in stale_alerts)


def test_collect_monitor_rows_honors_skip_flag(monkeypatch) -> None:
    called = {"value": False}

    class _FakeMonitorCollector:
        def __init__(self, _config):
            pass

        def collect(self):
            called["value"] = True
            return [{"name": "布伦特原油"}]

    monkeypatch.setattr("src.commands.briefing.MarketMonitorCollector", _FakeMonitorCollector)

    assert briefing_module._collect_monitor_rows({"market_context": {"skip_market_monitor": True}}) == []
    assert called["value"] is False


def test_load_briefing_global_proxy_honors_skip_flag() -> None:
    rows, note = _load_briefing_global_proxy({"market_context": {"skip_global_proxy": True}})

    assert rows == {}
    assert "已按运行配置关闭" in note


def test_load_briefing_global_proxy_disabled_by_default() -> None:
    rows, note = _load_briefing_global_proxy({})

    assert rows == {}
    assert "默认关闭" in note


def test_theme_information_environment_stays_cautious_for_aligned_theme() -> None:
    line = _theme_information_environment(True, "短线交易 / 中线配置")

    assert "直接催化" not in line
    assert "已验证" in line


def test_appendix_allocation_rows_does_not_mark_aggressive_without_vix() -> None:
    rows = _appendix_allocation_rows({"theme": "macro_background"}, [])
    applicable_row = next(row for row in rows if row[0] == "当日适用")

    assert applicable_row[1] == "观察"
    assert "波动代理缺失" in applicable_row[4]


def test_portfolio_table_rows_use_thesis_core_assumption_and_event_monitor(monkeypatch) -> None:
    class _FakePortfolioRepo:
        def list_holdings(self):
            return [{"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1}]

    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "561380"
            return {
                "core_assumption": "电网投资提升",
                "thesis_state_snapshot": {
                    "state": "升级",
                    "trigger": "事件完成消化",
                    "summary": "当前事件已完成消化。",
                },
                "event_digest_snapshot": {
                    "status": "已消化",
                    "lead_layer": "公告",
                    "thesis_scope": "thesis变化",
                    "lead_title": "国电南瑞中标项目",
                    "changed_what": "已经下沉到公司级执行。",
                },
            }

    monkeypatch.setattr("src.commands.briefing.PortfolioRepository", lambda: _FakePortfolioRepo())
    monkeypatch.setattr("src.commands.briefing.ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr("src.commands.briefing.fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr(
        "src.commands.briefing.compute_history_metrics",
        lambda history: {"last_close": 2.23},
    )

    rows = _portfolio_table_rows({})

    assert rows == [["561380", "多", "2.100", "2.230", "+6.19%", "电网投资提升", "thesis变化 / 升级(事件完成消化) / 公告已消化"]]


def test_portfolio_lines_highlight_priority_thesis_reviews(monkeypatch) -> None:
    class _FakePortfolioRepo:
        def list_holdings(self):
            return [
                {"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1},
                {"symbol": "512480", "asset_type": "cn_etf", "cost_basis": 1.0},
            ]

        def build_status(self, latest_prices):
            return {
                "total_value": 200000.0,
                "base_currency": "CNY",
                "holdings": [
                    {"symbol": "561380", "weight": 0.28, "pnl": -0.09},
                    {"symbol": "512480", "weight": 0.08, "pnl": 0.03},
                ],
                "region_exposure": {"CN": 1.0},
                "sector_exposure": {"电网": 0.28, "半导体": 0.08},
            }

        def rebalance_suggestions(self, latest_prices):
            return []

    class _FakeThesisRepo:
        def get(self, symbol):
            if symbol == "561380":
                return {
                    "core_assumption": "电网投资提升",
                    "thesis_state_snapshot": {
                        "state": "待复核",
                        "trigger": "事件边界待复核",
                        "summary": "当前事件边界已退回待复核。",
                    },
                    "event_digest_snapshot": {"status": "待复核", "lead_layer": "政策", "lead_detail": "政策影响层：配套细则", "importance": "high", "importance_reason": "必须前置复核，因为政策细则可能改写景气 / 资金偏好。", "thesis_scope": "待确认", "impact_summary": "景气 / 资金偏好"},
                }
            if symbol == "512480":
                return {
                    "core_assumption": "半导体景气修复",
                    "event_digest_snapshot": {"status": "已消化", "lead_layer": "财报", "lead_detail": "财报摘要：盈利/指引上修", "importance": "high", "importance_reason": "优先前置，因为公司级财报已经直接改写盈利 / 估值。", "thesis_scope": "thesis变化", "impact_summary": "盈利 / 估值"},
                }
            return None

        def load_review_queue(self):
            return {
                "history": {
                    "561380": {
                        "report_followup": {
                            "status": "待更新正式稿",
                            "reason": "最近复查已完成，但当前正式稿仍停留在旧版本；下一步应补新的 final / client-final。",
                            "reports": [
                                {
                                    "report_type": "scan",
                                    "generated_at": "2026-03-29 09:00:00",
                                    "markdown": "reports/scans/etfs/final/scan_561380_2026-03-29_client_final.md",
                                }
                            ],
                        }
                    }
                }
            }

    monkeypatch.setattr("src.commands.briefing.PortfolioRepository", lambda: _FakePortfolioRepo())
    monkeypatch.setattr("src.commands.briefing.ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr("src.commands.briefing.fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr(
        "src.commands.briefing.compute_history_metrics",
        lambda history: {"last_close": 2.23},
    )

    lines = _portfolio_lines({})

    assert any("Thesis 覆盖 2/2 个持仓" in item for item in lines)
    assert any("组合联动:" in item for item in lines)
    assert any("风格与方向:" in item for item in lines)
    assert any("优先复查 thesis" in item and "561380（高）" in item and "待复核" in item and "当前事件边界已退回待复核" in item and "待确认" in item and "政策影响层：配套细则" in item and "事件优先级 高" in item and "事件待复核" in item for item in lines)
    assert any("正式稿跟进: 561380 当前是 `待更新正式稿`" in item for item in lines)


def test_portfolio_lines_put_missing_thesis_into_review_queue(monkeypatch) -> None:
    class _FakePortfolioRepo:
        def list_holdings(self):
            return [{"symbol": "000001", "asset_type": "cn_stock", "cost_basis": 10.0}]

        def build_status(self, latest_prices):
            return {
                "total_value": 50000.0,
                "base_currency": "CNY",
                "holdings": [{"symbol": "000001", "weight": 0.18, "pnl": -0.01}],
                "region_exposure": {"CN": 1.0},
                "sector_exposure": {"银行": 0.18},
            }

        def rebalance_suggestions(self, latest_prices):
            return []

    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "000001"
            return None

    monkeypatch.setattr("src.commands.briefing.PortfolioRepository", lambda: _FakePortfolioRepo())
    monkeypatch.setattr("src.commands.briefing.ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr("src.commands.briefing.fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr("src.commands.briefing.compute_history_metrics", lambda history: {"last_close": 10.2})

    lines = _portfolio_lines({})

    assert any("Thesis 覆盖 0/1 个持仓" in item for item in lines)
    assert any("优先复查 thesis" in item and "000001（高）" in item and "还没有绑定 thesis" in item for item in lines)


def test_portfolio_review_queue_sorts_high_priority_holdings(monkeypatch) -> None:
    class _FakePortfolioRepo:
        def list_holdings(self):
            return [
                {"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1},
                {"symbol": "000001", "asset_type": "cn_stock", "cost_basis": 10.0},
            ]

        def build_status(self, latest_prices):
            return {
                "total_value": 150000.0,
                "base_currency": "CNY",
                "holdings": [
                    {"symbol": "561380", "weight": 0.28, "pnl": -0.09},
                    {"symbol": "000001", "weight": 0.18, "pnl": -0.01},
                ],
                "region_exposure": {"CN": 1.0},
                "sector_exposure": {"电网": 0.28, "银行": 0.18},
            }

    class _FakeThesisRepo:
        def get(self, symbol):
            if symbol == "561380":
                return {"event_digest_snapshot": {"status": "待复核", "lead_layer": "政策"}}
            return None

    monkeypatch.setattr("src.commands.briefing.PortfolioRepository", lambda: _FakePortfolioRepo())
    monkeypatch.setattr("src.commands.briefing.ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr("src.commands.briefing.fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr("src.commands.briefing.compute_history_metrics", lambda history: {"last_close": 2.23})

    queue = _portfolio_review_queue({})

    assert [item["symbol"] for item in queue[:2]] == ["561380", "000001"]
    assert queue[0]["priority"] == "高"
    assert queue[1]["has_thesis"] is False


def test_watchlist_review_trigger_lines_follow_review_queue_priority() -> None:
    snapshots = [
        SimpleNamespace(symbol="561380", name="电网ETF"),
        SimpleNamespace(symbol="512480", name="半导体ETF"),
    ]

    lines = _watchlist_review_trigger_lines(
        snapshots,
        [
            {"symbol": "561380", "priority": "高", "thesis_state": "削弱", "thesis_state_trigger": "事件边界待复核", "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。", "thesis_scope": "待确认", "event_detail": "政策影响层：配套细则", "event_importance_label": "高", "event_importance_reason": "必须前置复核，因为政策细则可能改写盈利 / 景气。", "impact_summary": "盈利 / 景气", "summary": "事件边界待复核，仓位较重", "event_monitor_label": "事件待复核"},
            {"symbol": "512480", "priority": "低", "summary": "事件层已消化", "event_monitor_label": "财报已消化"},
        ],
    )

    assert len(lines) == 1
    assert "watchlist 触发 thesis 复查" in lines[0]
    assert "561380" in lines[0]
    assert "削弱" in lines[0]
    assert "事件边界待复核" in lines[0]
    assert "当前事件边界已退回待复核" in lines[0]
    assert "政策影响层：配套细则" in lines[0]
    assert "事件优先级 高" in lines[0]
    assert "盈利 / 景气" in lines[0]
    assert "事件待复核" in lines[0]


def test_review_queue_transition_lines_record_and_render_queue_history(monkeypatch) -> None:
    class _FakeThesisRepo:
        def record_review_queue(self, queue, *, source="", as_of=""):
            assert queue
            assert source == "briefing_daily"
            assert as_of == "2026-03-29 07:30:00"
            return {
                "new_entries": [
                    {
                        "symbol": "561380",
                        "priority": "高",
                        "recommended_action": "重跑 scan",
                        "event_detail": "政策影响层：配套细则",
                        "thesis_state_trigger": "事件边界待复核",
                        "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。",
                        "event_importance_label": "高",
                        "impact_summary": "盈利 / 景气",
                        "thesis_scope": "待确认",
                    }
                ],
                "resolved_entries": [],
                "stale_high_priority": [],
            }

    monkeypatch.setattr("src.commands.briefing.ThesisRepository", lambda: _FakeThesisRepo())

    lines = _review_queue_transition_lines(
        [{"symbol": "561380", "priority": "高"}],
        source="briefing_daily",
        as_of="2026-03-29 07:30:00",
    )

    assert lines == [
        "今日新进复查队列: 561380（高，建议重跑 scan，焦点：政策影响层：配套细则；事件边界待复核；当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。；事件优先级 高；盈利 / 景气；待确认）。"
    ]


def test_calendar_review_trigger_lines_match_event_titles_to_review_queue() -> None:
    lines = _calendar_review_trigger_lines(
        [
            {"time": "09:30", "title": "电网ETF 跟踪会", "note": "观察政策兑现"},
            {"time": "14:00", "title": "半导体行业会", "note": "仅做背景跟踪"},
        ],
        [
            {"symbol": "561380", "priority": "高", "recommended_action": "重跑 scan"},
            {"symbol": "512480", "priority": "低", "recommended_action": "复查 thesis"},
        ],
        [
            SimpleNamespace(symbol="561380", name="电网ETF"),
            SimpleNamespace(symbol="512480", name="半导体ETF"),
        ],
    )

    assert lines == ["事件日历触发 thesis 复查：09:30 电网ETF 跟踪会 命中 电网ETF(561380)，建议先重跑 scan。"]


def test_calendar_review_trigger_lines_match_event_layer_when_symbol_absent() -> None:
    lines = _calendar_review_trigger_lines(
        [
            {"time": "09:30", "title": "国务院稳增长政策发布会", "note": "披露项目节奏和投资安排"},
        ],
        [
            {
                "symbol": "561380",
                "priority": "高",
                "recommended_action": "重跑 scan",
                "event_layer": "政策",
                "event_detail": "政策影响层：配套细则",
                "thesis_state_trigger": "事件边界待复核",
                "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。",
                "event_importance_label": "高",
                "event_importance_reason": "必须前置复核，因为政策细则可能改写盈利 / 景气。",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
            }
        ],
        [
            SimpleNamespace(symbol="561380", name="电网ETF"),
        ],
    )

    assert lines == [
        "事件日历触发 thesis 复查：09:30 国务院稳增长政策发布会 属于 `政策` 层，电网ETF(561380) 当前也卡在同层事件，建议先重跑 scan；当前焦点：政策影响层：配套细则；事件边界待复核；当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。；事件优先级 高；盈利 / 景气；待确认；必须前置复核，因为政策细则可能改写盈利 / 景气。。"
    ]


def test_portfolio_priority_action_line_lifts_review_line_to_action_section() -> None:
    line = _portfolio_priority_action_line(
        [
            "组合市值约 200000.00 CNY。",
            "优先复查 thesis: 561380（高） 事件边界待复核；事件待复核。",
        ]
    )

    assert line.startswith("今天优先复查 thesis:")
    assert "561380（高）" in line


def test_build_noon_payload_prepends_calendar_review_triggers(monkeypatch) -> None:
    monkeypatch.setattr("src.commands.briefing._load_same_day_briefing", lambda mode="daily": "")
    monkeypatch.setattr("src.commands.briefing._parse_prior_verification_rows", lambda markdown: [])
    monkeypatch.setattr("src.commands.briefing._parse_prior_headline", lambda markdown: "")
    monkeypatch.setattr("src.commands.briefing._evaluate_prior_verification", lambda prior_rows, snapshots, monitor_rows: [])
    monkeypatch.setattr("src.commands.briefing._domestic_overview_rows", lambda overview, pulse: ([], []))
    monkeypatch.setattr("src.commands.briefing._style_rows", lambda overview, industry_spot: [])
    monkeypatch.setattr("src.commands.briefing._industry_rank_rows", lambda drivers, narrative, news_report: [])
    monkeypatch.setattr("src.commands.briefing._watchlist_change_lines", lambda snapshots, morning_md: [])
    monkeypatch.setattr("src.commands.briefing._watchlist_review_trigger_lines", lambda snapshots, review_queue: [])
    monkeypatch.setattr("src.commands.briefing._noon_strategy_adjustment", lambda *args, **kwargs: [])
    monkeypatch.setattr("src.commands.briefing._noon_action_lines", lambda *args, **kwargs: [])
    monkeypatch.setattr("src.commands.briefing._noon_verification_rows", lambda snapshots, monitor_rows: [])
    monkeypatch.setattr("src.commands.briefing._workflow_event_rows", lambda events: [])
    monkeypatch.setattr("src.commands.briefing._portfolio_lines", lambda config, review_queue=None: [])
    monkeypatch.setattr("src.commands.briefing._portfolio_table_rows", lambda config: [])

    payload = _build_noon_payload(
        [],
        [],
        {},
        {},
        {},
        {},
        [],
        [],
        {},
        review_queue=[],
        review_queue_transition_lines=[],
        review_queue_action_lines=[],
        calendar_review_trigger_lines=["事件日历触发 thesis 复查：09:30 电网ETF 跟踪会 命中 电网ETF(561380)，建议先重跑 scan。"],
    )

    assert payload["watchlist_change_lines"] == ["事件日历触发 thesis 复查：09:30 电网ETF 跟踪会 命中 电网ETF(561380)，建议先重跑 scan。"]


def test_build_evening_payload_prepends_calendar_review_triggers(monkeypatch) -> None:
    monkeypatch.setattr("src.commands.briefing._load_same_day_briefing", lambda mode="daily": "")
    monkeypatch.setattr("src.commands.briefing._parse_prior_verification_rows", lambda markdown: [])
    monkeypatch.setattr("src.commands.briefing._parse_prior_headline", lambda markdown: "")
    monkeypatch.setattr("src.commands.briefing._evaluate_prior_verification", lambda prior_rows, snapshots, monitor_rows: [])
    monkeypatch.setattr("src.commands.briefing._domestic_overview_rows", lambda overview, pulse: ([], []))
    monkeypatch.setattr("src.commands.briefing._style_rows", lambda overview, industry_spot: [])
    monkeypatch.setattr("src.commands.briefing._industry_rank_rows", lambda drivers, narrative, news_report: [])
    monkeypatch.setattr("src.commands.briefing._macro_asset_rows", lambda monitor_rows, anomaly_report: [])
    monkeypatch.setattr("src.commands.briefing._catalyst_rows", lambda news_report, narrative: [])
    monkeypatch.setattr("src.commands.briefing._capital_flow_lines", lambda pulse, drivers, liquidity_lines, snapshots: [])
    monkeypatch.setattr("src.commands.briefing._watchlist_change_lines", lambda snapshots, morning_md: [])
    monkeypatch.setattr("src.commands.briefing._watchlist_review_trigger_lines", lambda snapshots, review_queue: [])
    monkeypatch.setattr("src.commands.briefing._evening_hit_rate_summary", lambda eval_rows: [])
    monkeypatch.setattr("src.commands.briefing._evening_narrative_review", lambda *args, **kwargs: [])
    monkeypatch.setattr("src.commands.briefing._core_event_lines", lambda news_report, catalyst_rows: [])
    monkeypatch.setattr("src.commands.briefing._tomorrow_outlook_lines", lambda narrative, snapshots, monitor_rows, overnight_rows: [])
    monkeypatch.setattr("src.commands.briefing._tomorrow_verification_rows", lambda snapshots, monitor_rows, narrative: [])
    monkeypatch.setattr("src.commands.briefing._tomorrow_action_lines", lambda eval_rows, snapshots, narrative: [])
    monkeypatch.setattr("src.commands.briefing._portfolio_lines", lambda config, review_queue=None: [])
    monkeypatch.setattr("src.commands.briefing._portfolio_table_rows", lambda config: [])
    monkeypatch.setattr("src.commands.briefing._appendix_technical_rows", lambda snapshots: [])
    monkeypatch.setattr("src.commands.briefing._lhb_lines", lambda pulse: [])
    monkeypatch.setattr("src.commands.briefing._flow_lines", lambda snapshots, config: [])
    monkeypatch.setattr("src.commands.briefing._sentiment_lines", lambda snapshots, config: [])
    monkeypatch.setattr("src.commands.briefing._render_briefing_charts", lambda snapshots: [])

    payload = _build_evening_payload(
        [],
        [],
        {},
        {},
        {},
        {},
        {},
        [],
        {},
        {},
        [],
        [],
        review_queue=[],
        review_queue_transition_lines=[],
        review_queue_action_lines=[],
        calendar_review_trigger_lines=["事件日历触发 thesis 复查：09:30 电网ETF 跟踪会 命中 电网ETF(561380)，建议先重跑 scan。"],
    )

    assert payload["watchlist_change_lines"] == ["事件日历触发 thesis 复查：09:30 电网ETF 跟踪会 命中 电网ETF(561380)，建议先重跑 scan。"]


def test_appendix_derivative_lines_do_not_render_fake_zero_vix() -> None:
    lines = _appendix_derivative_lines({"theme": "energy_shock"}, [])

    assert any("VIX/外盘波动代理缺失" in item for item in lines)
    assert not any("VIX 0.0" in item for item in lines)


def test_briefing_a_share_watch_rows_use_full_market_disclosure(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        briefing_module,
        "discover_stock_opportunities",
        lambda config, top_n=8, market="cn", context=None, max_candidates=None, attach_signal_confidence=True: captured.update({"context": context, "max_candidates": max_candidates, "attach_signal_confidence": attach_signal_confidence}) or {  # noqa: ARG005
            "scan_pool": 1,
            "passed_pool": 1,
            "blind_spots": ["部分样本缺少完整事件覆盖。"],
            "top": [
                {
                    "symbol": "300750",
                    "name": "宁德时代",
                    "metadata": {"sector": "新能源"},
                    "rating": {"label": "较强机会", "rank": 3},
                    "action": {"position": "首次建仓 ≤3%"},
                    "narrative": {"judgment": {"state": "持有优于追高"}},
                }
            ],
            "coverage_analyses": [
                {
                    "dimensions": {"risk": {"score": 60}, "relative_strength": {"score": 80}, "technical": {"score": 70}},
                }
            ],
        },
    )

    rows, lines, meta, candidates = _briefing_a_share_watch_rows({})

    assert rows == [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]]
    assert any("Tushare 优先" in item for item in lines)
    assert any("初筛池 `1`" in item for item in lines)
    assert meta["enabled"] is True
    assert meta["mode"] == "tushare_priority_full_market_prescreen"
    assert meta["pool_size"] == 1
    assert meta["shortlist_size"] == 1
    assert meta["complete_analysis_size"] == 1
    assert meta["report_top_n"] == 1
    assert meta["blind_spot"] == "部分样本缺少完整事件覆盖。"
    assert "factor_contract" in meta
    assert candidates[0]["symbol"] == "300750"
    assert captured["context"] is None
    assert captured["max_candidates"] == 8
    assert captured["attach_signal_confidence"] is False


def test_briefing_a_share_watch_rows_reuses_shared_context(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        briefing_module,
        "discover_stock_opportunities",
        lambda config, top_n=8, market="cn", context=None, max_candidates=None, attach_signal_confidence=True: captured.update({"context": context, "max_candidates": max_candidates, "attach_signal_confidence": attach_signal_confidence}) or {  # noqa: ARG005
            "scan_pool": 0,
            "passed_pool": 0,
            "blind_spots": [],
            "top": [],
            "coverage_analyses": [],
            "candidate_limit": max_candidates,
        },
    )

    shared_context = _briefing_shared_market_context(
        {},
        china_macro={"pmi": 50.1},
        global_proxy={"dxy_20d_change": -0.01},
        monitor_rows=[{"name": "VIX波动率", "latest": 18.0}],
        regime_result={"current_regime": "recovery", "preferred_assets": ["成长股"]},
        news_report={"items": [{"category": "fed"}]},
        drivers={"industry_spot": pd.DataFrame()},
        pulse={"zt_pool": pd.DataFrame()},
        events=[{"title": "财报窗口"}],
    )

    _briefing_a_share_watch_rows({}, shared_context=shared_context)

    assert captured["context"] == shared_context
    assert captured["context"]["regime"]["current_regime"] == "recovery"
    assert captured["context"]["day_theme"]["code"] == "macro_background"
    assert captured["max_candidates"] == 8
    assert captured["attach_signal_confidence"] is False


def test_briefing_a_share_watch_rows_discloses_candidate_limit(monkeypatch) -> None:
    monkeypatch.setattr(
        briefing_module,
        "discover_stock_opportunities",
        lambda config, top_n=8, market="cn", context=None, max_candidates=None, attach_signal_confidence=True: {  # noqa: ARG005
            "scan_pool": 2,
            "passed_pool": 1,
            "blind_spots": [],
            "top": [
                {
                    "symbol": "300750",
                    "name": "宁德时代",
                    "metadata": {"sector": "新能源"},
                    "rating": {"label": "较强机会", "rank": 3},
                    "action": {"position": "首次建仓 ≤3%"},
                    "narrative": {"judgment": {"state": "持有优于追高"}},
                }
            ],
            "coverage_analyses": [{"dimensions": {"risk": {"score": 60}}}],
            "candidate_limit": max_candidates,
        },
    )

    rows, lines, meta, _ = _briefing_a_share_watch_rows({})

    assert rows[0][1] == "宁德时代 (300750)"
    assert any("候选上限 `8`" in item for item in lines)
    assert meta["candidate_limit"] == 8


def test_briefing_a_share_watch_rows_softly_prefers_portfolio_style_complement(monkeypatch) -> None:
    monkeypatch.setattr(
        briefing_module,
        "discover_stock_opportunities",
        lambda config, top_n=8, market="cn", context=None, max_candidates=None, attach_signal_confidence=True: {  # noqa: ARG005
            "scan_pool": 2,
            "passed_pool": 2,
            "blind_spots": [],
            "top": [
                {
                    "symbol": "600406",
                    "name": "国电南瑞",
                    "metadata": {"sector": "电网"},
                    "rating": {"label": "观察", "rank": 1},
                    "action": {"position": "先观察"},
                    "narrative": {"judgment": {"state": "观察为主"}},
                    "strategy_background_confidence": {"status": "stable"},
                    "dimensions": {
                        "technical": {"score": 56},
                        "fundamental": {"score": 58},
                        "relative_strength": {"score": 54},
                        "risk": {"score": 52},
                    },
                },
                {
                    "symbol": "000651",
                    "name": "格力电器",
                    "metadata": {"sector": "家电"},
                    "rating": {"label": "观察", "rank": 1},
                    "action": {"position": "先观察"},
                    "narrative": {"judgment": {"state": "观察为主"}},
                    "strategy_background_confidence": {"status": "stable"},
                    "dimensions": {
                        "technical": {"score": 56},
                        "fundamental": {"score": 57},
                        "relative_strength": {"score": 54},
                        "risk": {"score": 53},
                    },
                },
            ],
            "coverage_analyses": [
                {"dimensions": {"risk": {"score": 60}}},
                {"dimensions": {"risk": {"score": 58}}},
            ],
            "candidate_limit": max_candidates,
        },
    )

    monkeypatch.setattr(
        briefing_module,
        "attach_portfolio_overlap_summaries",
        lambda items, config: [  # noqa: ARG005
            {
                **dict(item),
                "portfolio_overlap_summary": (
                    {"style_conflict_label": "同一行业主线加码", "overlap_label": "同一行业主线加码"}
                    if str(item.get("symbol")) == "600406"
                    else {"style_conflict_label": "风格补位", "overlap_label": "重复度较低"}
                ),
            }
            for item in items
        ],
    )

    rows, _, _, candidates = _briefing_a_share_watch_rows({})

    assert rows[0][1] == "格力电器 (000651)"
    assert candidates[0]["symbol"] == "000651"


def test_briefing_evidence_rows_include_watch_pool_and_point_in_time_note() -> None:
    rows = _briefing_evidence_rows(
        generated_at="2026-03-23 08:30:00",
        narrative={"label": "中国政策 / 内需确定性"},
        regime_result={"current_regime": "recovery"},
        data_coverage="中国宏观 | Watchlist 行情 | RSS新闻",
        missing_sources="跨市场代理",
        a_share_watch_meta={"pool_size": 16, "complete_analysis_size": 5, "candidate_limit": 16},
        proxy_contract={
            "market_flow": {"interpretation": "高股息相对成长更抗跌。", "confidence_label": "中"},
            "social_sentiment": {"covered": 2, "total": 3},
        },
    )

    payload = {row[0]: row[1] for row in rows}
    assert payload["A股观察池来源"].startswith("Tushare 优先全市场初筛")
    assert payload["时点边界"].startswith("默认只使用生成时点前可见")


def test_timed_collect_timeout_returns_fallback_and_warning() -> None:
    before_non_daemon = [thread.name for thread in threading.enumerate() if not thread.daemon]
    result, warning = _timed_collect(
        "市场驱动",
        lambda: (time.sleep(0.05), {"ok": True})[1],
        fallback={},
        timeout_seconds=0.01,
    )
    time.sleep(0.005)
    after_non_daemon = [thread.name for thread in threading.enumerate() if not thread.daemon]

    assert result == {}
    assert "市场驱动 拉取超时" in warning
    assert after_non_daemon == before_non_daemon


def test_briefing_daily_action_helpers_avoid_portfolio_whatif_handoff() -> None:
    snapshot = SimpleNamespace(
        symbol="561380",
        name="电网ETF",
        asset_type="cn_etf",
        sector="电网",
        signal_score=5,
        return_1d=0.012,
        return_20d=0.11,
        latest_price=2.234,
        trend="多头",
        technical={"rsi": {"RSI": 61.0}},
    )
    narrative = {"theme": "china_policy", "label": "电网投资主线"}

    daily_lines = _action_lines([snapshot], narrative, [])
    noon_lines = _noon_action_lines([], [snapshot], narrative, [], {}, {})
    tomorrow_lines = _tomorrow_action_lines([["验证", "条件", "实际", "✅"]], [snapshot], narrative)

    assert not any("portfolio whatif buy 561380 2.2340 计划金额" in item for item in daily_lines)
    assert any("优先观察方向" in item for item in daily_lines)
    assert any("portfolio whatif buy 561380 2.2340 计划金额" in item for item in noon_lines)
    assert any("portfolio whatif buy 561380 2.2340 计划金额" in item for item in tomorrow_lines)


def test_build_noon_payload_prepends_review_queue_action_lines(monkeypatch) -> None:
    monkeypatch.setattr(briefing_module, "_load_same_day_briefing", lambda mode: "")

    payload = _build_noon_payload(
        snapshots=[],
        monitor_rows=[],
        overview={},
        pulse={},
        drivers={},
        narrative={},
        watchlist_rows=[],
        events=[],
        config={},
        review_queue=[],
        review_queue_transition_lines=[],
        review_queue_action_lines=["研究动作 1: 561380（高）先重跑 scan；命令：`python -m src.commands.scan 561380`。"],
        calendar_review_trigger_lines=["事件日历触发 thesis 复查：09:30 国务院稳增长政策发布会 属于 `政策` 层，电网ETF(561380) 当前也卡在同层事件，建议先重跑 scan。"],
    )

    assert payload["afternoon_action_lines"][0].startswith("研究动作 1:")
    assert payload["watchlist_change_lines"][0].startswith("事件日历触发 thesis 复查：")


def test_build_evening_payload_prepends_review_queue_action_lines(monkeypatch) -> None:
    monkeypatch.setattr(briefing_module, "_load_same_day_briefing", lambda mode: "")

    payload = _build_evening_payload(
        snapshots=[],
        monitor_rows=[],
        overview={},
        pulse={},
        drivers={},
        narrative={},
        news_report={},
        watchlist_rows=[],
        config={},
        anomaly_report={},
        overnight_rows=[],
        liquidity_lines=[],
        review_queue=[],
        review_queue_transition_lines=[],
        review_queue_action_lines=["研究动作 1: 561380（高）先重跑 scan；命令：`python -m src.commands.scan 561380`。"],
        calendar_review_trigger_lines=["事件日历触发 thesis 复查：09:30 国务院稳增长政策发布会 属于 `政策` 层，电网ETF(561380) 当前也卡在同层事件，建议先重跑 scan。"],
    )

    assert payload["tomorrow_action_lines"][0].startswith("研究动作 1:")
    assert payload["watchlist_change_lines"][0].startswith("事件日历触发 thesis 复查：")


def test_build_evening_payload_surfaces_client_final_contract_fields(monkeypatch) -> None:
    monkeypatch.setattr(briefing_module, "_load_same_day_briefing", lambda mode: "")
    monkeypatch.setattr(briefing_module, "_parse_prior_verification_rows", lambda markdown: [])
    monkeypatch.setattr(briefing_module, "_parse_prior_headline", lambda markdown: "")
    monkeypatch.setattr(briefing_module, "_evaluate_prior_verification", lambda prior_rows, snapshots, monitor_rows: [])
    monkeypatch.setattr(briefing_module, "_domestic_overview_rows", lambda overview, pulse: ([], []))
    monkeypatch.setattr(briefing_module, "_style_rows", lambda overview, industry_spot: [])
    monkeypatch.setattr(briefing_module, "_industry_rank_rows", lambda drivers, narrative, news_report: [])
    monkeypatch.setattr(briefing_module, "_macro_asset_rows", lambda monitor_rows, anomaly_report: [])
    monkeypatch.setattr(briefing_module, "_catalyst_rows", lambda news_report, narrative: [])
    monkeypatch.setattr(briefing_module, "_capital_flow_lines", lambda pulse, drivers, liquidity_lines, snapshots: [])
    monkeypatch.setattr(briefing_module, "_theme_tracking_rows", lambda narrative, drivers: [["AI硬件链", "CPO 承接", "主线还在", "短线观察", "高位波动", "信息环境仍偏支持"]])
    monkeypatch.setattr(briefing_module, "_theme_tracking_lines", lambda narrative, rows, mode: ["与主线一致性: AI硬件链仍在前排。"])
    monkeypatch.setattr(briefing_module, "_watchlist_change_lines", lambda snapshots, morning_md: [])
    monkeypatch.setattr(briefing_module, "_watchlist_review_trigger_lines", lambda snapshots, review_queue: [])
    monkeypatch.setattr(briefing_module, "_evening_hit_rate_summary", lambda eval_rows: ["全日验证命中率: 1/1。"])
    monkeypatch.setattr(briefing_module, "_evening_narrative_review", lambda *args, **kwargs: ["AI 算力主线今天虽有分歧，但前排承接还在。", "创新药更像次主线补涨。"])
    monkeypatch.setattr(briefing_module, "_core_event_lines", lambda news_report, catalyst_rows: ["核心事件 1"])
    monkeypatch.setattr(briefing_module, "_tomorrow_outlook_lines", lambda narrative, snapshots, monitor_rows, overnight_rows: ["明天先按结构性行情理解，不把今天收盘强弱直接外推成全面 risk-on。"])
    monkeypatch.setattr(briefing_module, "_tomorrow_verification_rows", lambda snapshots, monitor_rows, narrative: [["1", "主线验证", "前排继续放量", "提高风险暴露", "回到观察"]])
    monkeypatch.setattr(briefing_module, "_tomorrow_action_lines", lambda eval_rows, snapshots, narrative: ["明天先看 AI 算力和创新药谁先继续拿到量价确认。"])
    monkeypatch.setattr(briefing_module, "_portfolio_lines", lambda config, review_queue=None: [])
    monkeypatch.setattr(briefing_module, "_portfolio_table_rows", lambda config: [])
    monkeypatch.setattr(briefing_module, "_appendix_technical_rows", lambda snapshots: [])
    monkeypatch.setattr(briefing_module, "_lhb_lines", lambda pulse: [])
    monkeypatch.setattr(briefing_module, "_flow_lines", lambda snapshots, config: [])
    monkeypatch.setattr(briefing_module, "_sentiment_lines", lambda snapshots, config: [])
    monkeypatch.setattr(briefing_module, "_render_briefing_charts", lambda snapshots: [])

    payload = _build_evening_payload(
        snapshots=[],
        monitor_rows=[],
        overview={},
        pulse={},
        drivers={},
        narrative={"label": "科技主线修复"},
        news_report={"items": [{"title": "AI 算力继续活跃"}]},
        watchlist_rows=[],
        config={},
        anomaly_report={},
        overnight_rows=[],
        liquidity_lines=[],
        review_queue=[],
        review_queue_transition_lines=[],
        review_queue_action_lines=[],
        calendar_review_trigger_lines=[],
        generated_at="2026-04-22 16:10:00",
        data_coverage="中国宏观 | 主题跟踪 | A股观察池",
        missing_sources="无",
        macro_items=["PMI 继续在扩张区间。", "信用脉冲边际改善。", "当前更匹配的资产偏好: 科技成长。"],
        regime_result={"current_regime": "recovery", "reasoning": ["PMI 回到 50 上方。", "价格链条边际修复。"]},
        proxy_contract={"market_flow": {"interpretation": "成长风格重新占优", "confidence_label": "中"}},
        evidence_rows=[["分析生成时间", "2026-04-22 16:10:00"]],
        quality_lines=["本次新闻覆盖源: 财联社 / 路透。"],
        alerts=["高位主线分歧仍需继续验证。"],
        a_share_watch_rows=[["1", "新易盛 (300502)", "通信", "观察为主", "等待确认", "首次建仓 ≤3%"]],
        a_share_watch_lines=["A 股观察池来自 Tushare 优先全市场初筛。", "当前先按观察池管理。"],
        a_share_watch_meta={"pool_size": 24, "complete_analysis_size": 6},
        a_share_watch_candidates=[],
        market_analysis={
            "index_rows": [["上证指数", "3400", "+0.2%", "修复中", "周线改善", "月线修复", "常态量能", "等待确认"]],
            "market_signal_rows": [["市场宽度", "上涨 3000 / 下跌 1800", "偏强", "涨跌比改善"]],
            "rotation_rows": [["行业", "通信", "银行", "成长回流"]],
        },
    )

    assert payload["headline_lines"][0] == "AI 算力主线今天虽有分歧，但前排承接还在。"
    assert "明天先按结构性行情理解，不把今天收盘强弱直接外推成全面 risk-on。" in payload["headline_lines"]
    assert payload["action_lines"][0] == "明天先看 AI 算力和创新药谁先继续拿到量价确认。"
    assert payload["verification_rows"][0][1] == "主线验证"
    assert payload["a_share_watch_rows"][0][1] == "新易盛 (300502)"
    assert payload["theme_tracking_rows"][0][0] == "AI硬件链"


def test_action_lines_use_weekend_wording_and_theme_anchor(monkeypatch) -> None:
    class _WeekendDateTime:
        @classmethod
        def now(cls):
            return pd.Timestamp("2026-03-28 10:00:00").to_pydatetime()

    monkeypatch.setattr(briefing_module, "datetime", _WeekendDateTime)
    snapshots = [
        SimpleNamespace(
            symbol="513120",
            name="港股创新药ETF",
            asset_type="cn_etf",
            sector="创新药",
            latest_price=1.2,
            return_1d=0.03,
            return_5d=0.08,
            return_20d=0.12,
            volume_ratio=1.1,
            trend="多头",
            signal_score=6,
            summary="弹性更强。",
            note="",
            notes="",
            technical={"rsi": {"RSI": 55.0}},
        ),
        SimpleNamespace(
            symbol="561380",
            name="电网ETF",
            asset_type="cn_etf",
            sector="电网",
            latest_price=2.2,
            return_1d=0.01,
            return_5d=0.04,
            return_20d=0.10,
            volume_ratio=1.0,
            trend="多头",
            signal_score=5,
            summary="更贴近主线。",
            note="",
            notes="电网/公用事业承接",
            technical={"rsi": {"RSI": 52.0}},
        ),
    ]

    lines = _action_lines(snapshots, {"theme": "power_utilities", "label": "电网/公用事业"}, [])

    assert lines[0].startswith("周末先按")
    assert any("优先观察方向: 561380" in item for item in lines)
    assert any("下个交易时段先看开盘后前 30 分钟" in item for item in lines)


def test_action_lines_use_next_session_wording_after_market_open(monkeypatch) -> None:
    class _AfternoonDateTime:
        @classmethod
        def now(cls):
            return pd.Timestamp("2026-04-01 14:00:00").to_pydatetime()

    monkeypatch.setattr(briefing_module, "datetime", _AfternoonDateTime)
    snapshot = SimpleNamespace(
        symbol="561380",
        name="电网ETF",
        asset_type="cn_etf",
        sector="电网",
        latest_price=2.2,
        return_1d=0.01,
        return_5d=0.04,
        return_20d=0.10,
        volume_ratio=1.0,
        trend="多头",
        signal_score=5,
        summary="更贴近主线。",
        note="",
        notes="电网/公用事业承接",
        technical={"rsi": {"RSI": 52.0}},
    )

    lines = _action_lines([snapshot], {"theme": "power_utilities", "label": "电网/公用事业"}, [])

    assert any("下个交易时段先看开盘后前 30 分钟" in item for item in lines)
    assert not any(item.startswith("观察节奏: 先看开盘 30 分钟") for item in lines)


def test_compact_validation_lines_do_not_count_missing_cross_market_as_pass() -> None:
    narrative = {"theme": "power_utilities"}
    pulse = {
        "zt_pool": pd.DataFrame([{"所属行业": "电力"}]),
        "strong_pool": pd.DataFrame([{"所属行业": "电网"}]),
    }

    lines = _compact_validation_lines(narrative, [], pulse, cross_market_available=False)

    assert "跨市场 ⚠️" in lines[0]
    assert "通过 2/2 项（跨市场待补）" in lines[0]


def test_liquidity_lines_suppress_extreme_north_south_values(monkeypatch) -> None:
    class _FakeCollector:
        def __init__(self, _config):
            pass

        def get_north_south_flow(self):
            return pd.DataFrame(
                [
                    {
                        "日期": "2026-03-28",
                        "北向资金净流入": 2445.68 * 1e8,
                        "南向资金净流入": 532.53 * 1e8,
                    }
                ]
            )

        def get_margin_trading(self):
            return pd.DataFrame()

    monkeypatch.setattr(briefing_module, "ChinaMarketCollector", _FakeCollector)

    lines = briefing_module._liquidity_lines({})

    assert any("北向资金读数异常偏大" in item for item in lines)
    assert any("南向资金读数异常偏大" in item for item in lines)
    assert not any("2445.68亿" in item for item in lines)
    assert not any("532.53亿" in item for item in lines)


def test_core_event_lines_do_not_fallback_to_proxy_catalyst_rows() -> None:
    lines = _core_event_lines(
        {"items": []},
        [["主题推演", "—", "代理逻辑", "观察"]],
    )

    assert lines == ["当前没有可直接复核的核心事件；以下判断更多依赖主题推演和盘面代理，不把它们当成已验证事件。"]


def test_core_event_lines_filter_ai_entertainment_noise_when_not_market_relevant() -> None:
    lines = _core_event_lines(
        {
            "items": [
                {
                    "title": "又上热搜！爱奇艺回应“AI艺人库” 多位明星已紧急辟谣",
                    "source": "财联社",
                    "category": "china_market_domestic",
                },
                {
                    "title": "午评：创业板指涨0.63% CPO概念大涨",
                    "source": "证券时报",
                    "category": "china_market_domestic",
                },
            ]
        },
        [],
    )

    joined = "\n".join(lines)
    assert "爱奇艺" not in joined
    assert "AI艺人库" not in joined
    assert "CPO概念大涨" in joined


def test_source_quality_lines_explain_displayed_vs_total_sources() -> None:
    lines = _source_quality_lines(
        {
            "items": [
                {"source": "Reuters"},
                {"source": "Bloomberg"},
                {"source": "财联社"},
                {"source": "证券时报"},
                {"source": "CNBC"},
                {"source": "路透中文"},
            ]
        }
    )

    assert "正文展示前 4 类代表源" in lines[1]


def test_style_rows_do_not_render_missing_market_data_as_flat() -> None:
    rows = _style_rows({"domestic_indices": []}, pd.DataFrame())

    assert rows[0][1] == "中证1000 —"
    assert rows[0][2] == "沪深300 —"
    assert rows[0][3] == "待补"
    assert rows[1][1] == "创业板指 —"
    assert rows[1][2] == "上证指数 —"
    assert rows[1][3] == "待补"


def test_domestic_overview_rows_do_not_render_zero_limit_counts_when_pulse_missing() -> None:
    _, lines = _domestic_overview_rows({"domestic_indices": [], "breadth": {}}, {"zt_pool": pd.DataFrame(), "dt_pool": pd.DataFrame()})

    assert any("先不把 `0 家` 解读成情绪冰点" in item for item in lines)


def test_domestic_overview_rows_include_weekly_and_monthly_index_structure_lines() -> None:
    overview = {
        "domestic_indices": [
            {
                "name": "沪深300",
                "latest": 3800.0,
                "change_pct": 0.012,
                "amount": 1200.0,
                "amount_delta": 0.15,
                "weekly_summary": "近 156周 +12.00%，最近一周 +1.10%，修复中，动能改善",
                "monthly_summary": "近 36月 +18.00%，最近一月 +4.20%，趋势偏强，动能偏强",
            }
        ],
        "breadth": {},
    }

    _, lines = _domestic_overview_rows(overview, {"zt_pool": pd.DataFrame(), "dt_pool": pd.DataFrame()})

    assert any("沪深300：周线" in item for item in lines)
    assert any("月线" in item for item in lines)


def test_market_event_rows_use_explicit_event_calendar_instead_of_news_items() -> None:
    rows = _market_event_rows(
        [
            {
                "time": "21:30",
                "title": "国家统计局 PMI",
                "note": "前值 50.4 / 关注内需链",
                "importance": "high",
            }
        ],
        {"theme": "broad_market_repair"},
    )

    assert rows == [[rows[0][0], "国家统计局 PMI", "前值 50.4 / 关注内需链", "高", "宽基、券商、顺周期"]]
    assert rows[0][0].endswith("21:30")


def test_market_event_rows_prepend_a_share_movers_and_geopolitical_rows() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame([{"名称": "通信", "涨跌幅": 3.6, "领涨股票": "华工科技", "领涨股票-涨跌幅": 9.21}]),
            "concept_spot": pd.DataFrame([{"名称": "创新药", "涨跌幅": 7.8, "领涨股票": "药明康德", "领涨股票-涨跌幅": 5.61}]),
            "hot_rank": pd.DataFrame([{"股票名称": "新易盛", "涨跌幅": 12.5}]),
        },
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame()},
        news_report={
            "items": [
                {
                    "title": "US and Iran ceasefire hopes ease oil jitters",
                    "source": "Reuters",
                    "published_at": "2026-04-01T08:00:00",
                    "link": "https://example.com/iran",
                    "category": "geopolitics",
                }
            ]
        },
        a_share_watch_candidates=[],
        snapshots=[
            briefing_module.BriefingSnapshot(
                symbol="513120",
                name="港股创新药ETF",
                asset_type="cn_etf",
                region="CN",
                sector="医药",
                latest_price=1.215,
                return_1d=0.0025,
                return_5d=0.051,
                return_20d=0.0538,
                volume_ratio=1.1,
                trend="多头",
                signal_score=2,
                summary="",
                note="",
            )
        ],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "A股概念领涨：创新药（+7.80%）；领涨 药明康德（+5.61%）" in joined
    assert "A股热股前排：新易盛" in joined
    assert "观察资产走强：港股创新药ETF" in joined
    assert "地缘缓和" in joined
    assert "https://example.com/iran" in joined
    assert "偏利多，先看 `创新药/医药` 能否继续拿到价格与成交确认。" in joined
    assert "A股行业走强：通信（+3.60%）；领涨 华工科技（+9.21%）" not in joined
    assert joined.index("A股概念领涨：创新药") < joined.index("观察资产走强：港股创新药ETF")
    assert joined.index("A股热股前排：新易盛") < joined.index("观察资产走强：港股创新药ETF")


def test_market_event_rows_reserve_slots_for_specific_a_share_and_external_signals() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame(
                [
                    {"名称": "通信", "涨跌幅": 3.6, "领涨股票": "华工科技", "领涨股票-涨跌幅": 9.21},
                    {"名称": "电网设备", "涨跌幅": 3.2, "领涨股票": "平高电气", "领涨股票-涨跌幅": 8.13},
                ]
            ),
            "concept_spot": pd.DataFrame(
                [
                    {"名称": "创新药", "涨跌幅": 7.8, "领涨股票": "药明康德", "领涨股票-涨跌幅": 5.61},
                    {"名称": "智谱AI", "涨跌幅": 6.4, "领涨股票": "思美传媒", "领涨股票-涨跌幅": 10.01},
                ]
            ),
            "hot_rank": pd.DataFrame(
                [
                    {"股票名称": "新易盛", "涨跌幅": 12.5},
                    {"股票名称": "中际旭创", "涨跌幅": 8.4},
                ]
            ),
        },
        pulse={
            "market_date": "2026-04-01",
            "zt_pool": pd.DataFrame(
                [
                    {"名称": "津药药业", "所属行业": "化学制药", "涨跌幅": 10.08},
                    {"名称": "哈三联", "所属行业": "化学制药", "涨跌幅": 10.01},
                    {"名称": "塞力医疗", "所属行业": "化学制药", "涨跌幅": 10.00},
                    {"名称": "中京电子", "所属行业": "通信设备", "涨跌幅": 10.00},
                    {"名称": "武汉凡谷", "所属行业": "通信设备", "涨跌幅": 10.00},
                    {"名称": "剑桥科技", "所属行业": "通信设备", "涨跌幅": 10.00},
                ]
            ),
            "strong_pool": pd.DataFrame(
                [
                    {"名称": "力诺药包", "涨跌幅": 20.01},
                    {"名称": "星辉环材", "涨跌幅": 20.00},
                ]
            ),
        },
        news_report={
            "items": [
                {
                    "title": "US and Iran ceasefire hopes ease oil jitters",
                    "source": "Reuters",
                    "published_at": "2026-04-01T08:00:00",
                    "link": "https://example.com/iran",
                    "category": "geopolitics",
                }
            ]
        },
        snapshots=[
            briefing_module.BriefingSnapshot(
                symbol="513120",
                name="港股创新药ETF",
                asset_type="cn_etf",
                region="CN",
                sector="医药",
                latest_price=1.215,
                return_1d=0.0025,
                return_5d=0.051,
                return_20d=0.0538,
                volume_ratio=1.1,
                trend="多头",
                signal_score=2,
                summary="",
                note="",
            )
        ],
        a_share_watch_candidates=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert len(rows) == 8
    assert "A股概念领涨：创新药" in joined
    assert "A股概念领涨：智谱AI" in joined
    assert "A股热股前排：新易盛" in joined
    assert "A股热股前排：中际旭创" in joined
    assert "地缘缓和" in joined
    assert "A股行业走强：通信" not in joined
    assert joined.index("A股概念领涨：创新药") < joined.index("A股涨停集中：化学制药")
    assert joined.index("A股热股前排：新易盛") < joined.index("A股涨停集中：化学制药")
    assert joined.index("US and Iran ceasefire hopes ease oil jitters") < joined.index("A股涨停集中：化学制药")


def test_market_event_rows_synthesize_theme_rows_when_concept_and_hot_rank_are_empty() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame(),
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
        pulse={
            "market_date": "2026-04-01",
            "zt_pool": pd.DataFrame(
                [
                    {"名称": "津药药业", "所属行业": "化学制药", "涨跌幅": 10.08},
                    {"名称": "哈三联", "所属行业": "化学制药", "涨跌幅": 10.01},
                    {"名称": "塞力医疗", "所属行业": "化学制药", "涨跌幅": 10.00},
                ]
            ),
            "strong_pool": pd.DataFrame(),
        },
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "symbol": "300308",
                "name": "中际旭创",
                "trade_state": "观察为主",
                "metadata": {"sector": "科技"},
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "A股主题活跃：创新药/医药（涨停集中：化学制药 3家）" in joined
    assert "A股主题跟踪：AI算力/光模块（观察池前排 中际旭创）" in joined
    assert joined.index("A股主题活跃：创新药/医药") < joined.index("A股涨停集中：化学制药")


def test_market_event_rows_include_a_share_watch_market_event_rows_from_new_tushare_signals() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "symbol": "300308",
                "name": "中际旭创",
                "trade_state": "观察为主",
                "metadata": {"sector": "科技"},
                "market_event_rows": [
                    ["2026-04-01", "A股概念成员：中际旭创 属于 AI算力（+6.40%）", "同花顺主题成分", "高", "AI算力", "", "主线归因", "偏利多，`中际旭创` 属于 `AI算力` 链路。"],
                    ["2026-04-01", "交易所重点提示：中际旭创 当前仍在重点提示证券名单", "交易所风险专题", "中", "中际旭创", "", "风险提示", "偏谨慎，先按高波动样本管理节奏。"],
                    ["2026-04-01", "筹码确认：中际旭创 胜率约 70.2%，现价已回到平均成本上方", "筹码分布专题", "中", "中际旭创", "", "筹码确认", "偏利多，真实筹码分布开始配合价格修复。"],
                ],
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "A股概念成员：中际旭创 属于 AI算力（+6.40%）" in joined
    assert "交易所重点提示：中际旭创 当前仍在重点提示证券名单" in joined
    assert "筹码确认：中际旭创 胜率约 70.2%" in joined


def test_market_event_rows_include_a_share_watch_market_event_rows_from_p1_stock_signals() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "symbol": "300308",
                "name": "中际旭创",
                "trade_state": "观察为主",
                "metadata": {"sector": "科技"},
                "market_event_rows": [
                    ["2026-04-01", "个股资金流确认：中际旭创 当日主力净流入 1.60亿", "个股资金流向专题", "高", "中际旭创", "", "资金承接", "偏利多，个股主力资金开始给出直接承接。"],
                    ["2026-04-01", "两融拥挤提示：中际旭创 当前融资盘升温明显", "两融专题", "高", "中际旭创", "", "两融拥挤", "偏谨慎，融资盘一致性交易会放大短线波动。"],
                    ["2026-04-01", "打板信号确认：中际旭创 龙虎榜净买入/竞价高开", "龙虎榜/打板专题", "中", "中际旭创", "", "龙虎榜确认", "偏利多，微观交易结构开始配合。"],
                ],
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "个股资金流确认：中际旭创 当日主力净流入 1.60亿" in joined
    assert "两融拥挤提示：中际旭创 当前融资盘升温明显" in joined
    assert "打板信号确认：中际旭创 龙虎榜净买入/竞价高开" in joined


def test_market_event_rows_include_a_share_watch_broker_recommend_signal() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "symbol": "300308",
                "name": "中际旭创",
                "trade_state": "观察为主",
                "metadata": {"sector": "科技"},
                "market_event_rows": [
                    ["2026-04-01", "卖方共识升温：中际旭创 本月获 4 家券商金股推荐", "卖方共识专题", "中", "中际旭创", "", "卖方共识升温", "偏利多，卖方月度金股覆盖开始抬升，但这里只当共识热度参考。"],
                ],
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "卖方共识升温：中际旭创 本月获 4 家券商金股推荐" in joined


def test_market_event_rows_promote_a_share_watch_irm_evidence() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "symbol": "300308",
                "name": "中际旭创",
                "trade_state": "观察为主",
                "metadata": {"sector": "科技"},
                "market_event_rows": [],
                "dimensions": {
                    "catalyst": {
                        "evidence": [
                            {
                                "title": "中际旭创互动平台问答：回复称 800G 光模块需求仍在放量",
                                "source": "互动易",
                                "configured_source": "Tushare::irm_qa_sz",
                                "published_at": "2026-04-01T11:00:00",
                                "link": "https://irm.cninfo.com.cn/",
                                "note": "投资者关系/路演纪要",
                            }
                        ]
                    }
                },
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "中际旭创互动平台问答：回复称 800G 光模块需求仍在放量" in joined
    assert "互动易/投资者关系" in joined
    assert "管理层口径确认" in joined


def test_market_event_rows_accept_reuse_only_watch_candidates() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "name": "",
                "symbol": "",
                "briefing_reuse_only": True,
                "metadata": {"sector": "A股观察池"},
                "market_event_rows": [
                    ["2026-04-03 19:25:13.478024", "卖方共识非当期：宁德时代 最新券商金股仍停在 2026-03", "卖方共识专题", "低", "宁德时代", "", "卖方共识观察", "卖方月度金股最新停在 2026-03。"],
                ],
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "卖方共识非当期：宁德时代 最新券商金股仍停在 2026-03" in joined


def test_load_same_day_stock_pick_market_event_rows_parses_key_evidence(tmp_path, monkeypatch) -> None:
    target = tmp_path / "reports" / "stock_picks" / "final" / "stock_picks_cn_2026-04-03_final.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join(
            [
                "# stock pick",
                "",
                "## 关键证据",
                "",
                "- `市场情报`：卖方共识非当期：宁德时代 最新券商金股仍停在 2026-03（卖方共识专题 / 2026-04-03 19:25:13.478024）；信号类型：`卖方共识观察`；信号强弱：`低`；主要影响：`宁德时代`；结论：卖方月度金股最新停在 2026-03。",
                "",
                "## 其他章节",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(briefing_module, "resolve_project_path", lambda path: target)

    rows = _load_same_day_stock_pick_market_event_rows("2026-04-03")

    assert rows == [
        [
            "2026-04-03 19:25:13.478024",
            "卖方共识非当期：宁德时代 最新券商金股仍停在 2026-03",
            "卖方共识专题",
            "低",
            "宁德时代",
            "",
            "卖方共识观察",
            "卖方月度金股最新停在 2026-03。",
        ]
    ]


def test_load_same_day_stock_pick_market_event_rows_prioritizes_company_and_broker_signals(tmp_path, monkeypatch) -> None:
    target = tmp_path / "reports" / "stock_picks" / "final" / "stock_picks_cn_2026-04-03_final.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join(
            [
                "# stock pick",
                "",
                "## 关键证据",
                "",
                "- `市场情报`：标准行业框架：宁德时代 属于 申万二级行业·电池（申万行业框架 / 2026-04-03 19:25:13.478024）；信号类型：`标准行业归因`；信号强弱：`低`；主要影响：`电池`；结论：行业承压。",
                "- `市场情报`：指数技术面：人工智能 趋势偏弱（相关指数/框架 / 2026-04-03 19:25:13.478024）；信号类型：`行业/指数映射`；信号强弱：`低`；主要影响：`人工智能`；结论：指数偏弱。",
                "- `市场情报`：卖方共识非当期：宁德时代 最新券商金股仍停在 2026-03（卖方共识专题 / 2026-04-03 19:25:13.478024）；信号类型：`卖方共识观察`；信号强弱：`低`；主要影响：`宁德时代`；结论：卖方月度金股最新停在 2026-03。",
                "- `市场情报`：中际旭创互动平台问答：回复称 800G 光模块需求仍在放量（互动易/投资者关系 / 2026-04-03 19:25:13.478024）；信号类型：`管理层口径确认`；信号强弱：`中`；主要影响：`中际旭创`；结论：管理层口径偏中性偏利多。",
                "",
                "## 其他章节",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(briefing_module, "resolve_project_path", lambda path: target)

    rows = _load_same_day_stock_pick_market_event_rows("2026-04-03", limit=2)

    assert len(rows) == 2
    assert rows[0][2] == "互动易/投资者关系"
    assert rows[1][2] == "卖方共识专题"


def test_market_event_rows_accept_reuse_only_company_source_rows() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[
            {
                "name": "",
                "symbol": "",
                "briefing_reuse_only": True,
                "metadata": {"sector": "A股观察池"},
                "market_event_rows": [
                    ["2026-04-03 19:25:13.478024", "中际旭创互动平台问答：回复称 800G 光模块需求仍在放量", "互动易/投资者关系", "中", "中际旭创", "https://irm.cninfo.com.cn/", "管理层口径确认", "管理层口径偏中性偏利多。"],
                ],
            }
        ],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "中际旭创互动平台问答：回复称 800G 光模块需求仍在放量" in joined
    assert "互动易/投资者关系" in joined


def test_market_event_rows_prioritize_standard_industry_framework_when_available() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame(
                [
                    {
                        "名称": "通信设备",
                        "涨跌幅": 3.6,
                        "领涨股票": "华工科技",
                        "领涨股票-涨跌幅": 9.21,
                        "框架来源": "申万二级行业",
                    }
                ]
            ),
            "industry_spot_report": {
                "frame": pd.DataFrame(
                    [
                        {
                            "名称": "通信设备",
                            "涨跌幅": 3.6,
                            "领涨股票": "华工科技",
                            "领涨股票-涨跌幅": 9.21,
                            "框架来源": "申万二级行业",
                        }
                    ]
                ),
                "is_fresh": True,
                "source": "tushare.sw_daily+tushare.index_classify",
            },
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[],
    )

    assert rows[0][1] == "A股申万二级行业走强：通信设备（+3.60%）；领涨 华工科技（+9.21%）"
    assert rows[0][2] == "申万二级行业/盘面"


def test_market_event_rows_do_not_label_negative_industry_move_as_strength() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame(
                [
                    {"名称": "通信设备", "涨跌幅": 3.19, "框架来源": "申万二级行业"},
                    {"名称": "饰品", "涨跌幅": -1.51, "框架来源": "申万二级行业"},
                ]
            ),
            "industry_spot_report": {
                "frame": pd.DataFrame(
                    [
                        {"名称": "通信设备", "涨跌幅": 3.19, "框架来源": "申万二级行业"},
                        {"名称": "饰品", "涨跌幅": -1.51, "框架来源": "申万二级行业"},
                    ]
                ),
                "is_fresh": True,
                "source": "tushare.sw_daily+tushare.index_classify",
            },
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
        pulse={"market_date": "2026-04-03", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        a_share_watch_candidates=[],
    )

    titles = [str(row[1]) for row in rows]
    assert any("A股申万二级行业走强：通信设备（+3.19%）" in title for title in titles)
    assert not any("A股申万二级行业走强：饰品（-1.51%）" in title for title in titles)


def test_market_event_rows_include_watch_standard_industry_framework_rows() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()},
        pulse={"market_date": "2026-04-01", "zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()},
        news_report={"items": []},
        snapshots=[],
        a_share_watch_candidates=[
            {
                "name": "中际旭创",
                "symbol": "300308",
                "trade_state": "观察为主",
                "metadata": {"sector": "科技"},
                "market_event_rows": [
                    [
                        "2026-04-01",
                        "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）",
                        "申万行业框架",
                        "高",
                        "通信设备",
                        "",
                        "标准行业归因",
                        "偏利多，`中际旭创` 先按 `申万二级行业` 的 `通信设备` 去理解。",
                    ]
                ],
            }
        ],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）" in joined
    assert "申万行业框架" in joined


def test_market_event_rows_include_zt_pool_and_strong_pool_signals() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame(),
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
        pulse={
            "market_date": "2026-04-01",
            "zt_pool": pd.DataFrame(
                [
                    {"名称": "津药药业", "所属行业": "化学制药", "涨跌幅": 10.08},
                    {"名称": "哈三联", "所属行业": "化学制药", "涨跌幅": 10.01},
                    {"名称": "塞力医疗", "所属行业": "化学制药", "涨跌幅": 10.00},
                ]
            ),
            "strong_pool": pd.DataFrame(
                [
                    {"名称": "华工科技", "涨跌幅": 9.21},
                ]
            ),
        },
        news_report={"items": []},
        a_share_watch_candidates=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "A股涨停集中：化学制药（3家）" in joined
    assert "A股强势股池：华工科技（+9.21%）" in joined
    assert "医药催化" in joined
    assert "海外科技映射" in joined


def test_market_event_rows_suppress_stale_a_share_snapshots() -> None:
    rows = _market_event_rows(
        [],
        {"theme": "macro_background"},
        drivers={
            "industry_spot": pd.DataFrame([{"名称": "通信", "涨跌幅": 3.6, "领涨股票": "华工科技", "领涨股票-涨跌幅": 9.21}]),
            "industry_spot_report": {
                "frame": pd.DataFrame([{"名称": "通信", "涨跌幅": 3.6, "领涨股票": "华工科技", "领涨股票-涨跌幅": 9.21}]),
                "is_fresh": False,
            },
            "concept_spot": pd.DataFrame([{"名称": "创新药", "涨跌幅": 7.8, "领涨股票": "药明康德", "领涨股票-涨跌幅": 5.61}]),
            "concept_spot_report": {
                "frame": pd.DataFrame([{"名称": "创新药", "涨跌幅": 7.8, "领涨股票": "药明康德", "领涨股票-涨跌幅": 5.61}]),
                "is_fresh": False,
            },
            "hot_rank": pd.DataFrame([{"股票名称": "新易盛", "涨跌幅": 12.5}]),
            "hot_rank_report": {
                "frame": pd.DataFrame([{"股票名称": "新易盛", "涨跌幅": 12.5}]),
                "is_fresh": False,
            },
        },
        pulse={
            "market_date": "2026-04-01",
            "zt_pool": pd.DataFrame([{"所属行业": "化学制药"}]),
            "zt_pool_report": {"frame": pd.DataFrame([{"所属行业": "化学制药"}]), "is_fresh": False},
        },
        news_report={
            "items": [
                {
                    "title": "US and Iran ceasefire hopes ease oil jitters",
                    "source": "Reuters",
                    "published_at": "2026-04-01T08:00:00",
                    "link": "https://example.com/iran",
                    "category": "geopolitics",
                }
            ]
        },
        a_share_watch_candidates=[],
        snapshots=[],
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "A股概念领涨" not in joined
    assert "A股行业走强" not in joined
    assert "A股热股前排" not in joined
    assert "A股涨停集中" not in joined
    assert "地缘缓和" in joined


def test_market_event_rows_shift_past_intraday_time_to_next_session(monkeypatch) -> None:
    class _AfternoonDateTime:
        @classmethod
        def now(cls):
            return pd.Timestamp("2026-04-01 14:00:00").to_pydatetime()

    monkeypatch.setattr(briefing_module, "datetime", _AfternoonDateTime)
    rows = _market_event_rows(
        [
            {
                "time": "09:30",
                "title": "国家统计局 PMI",
                "note": "前值 50.4 / 关注内需链",
                "importance": "high",
            }
        ],
        {"theme": "broad_market_repair"},
    )

    assert rows == [["下个交易日 09:30", "国家统计局 PMI", "前值 50.4 / 关注内需链", "高", "宽基、券商、顺周期"]]


def test_yesterday_review_summary_lines_use_natural_fallback_text() -> None:
    lines = _yesterday_review_summary_lines([])

    assert lines[-1] == "框架修正: 暂无自动回顾记录。"


def test_briefing_parser_accepts_market_mode() -> None:
    args = build_parser().parse_args(["market"])
    assert args.mode == "market"


def test_primary_narrative_can_identify_broad_market_repair() -> None:
    snapshots = [
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=0.012, return_5d=0.031),
        SimpleNamespace(symbol="QQQM", sector="科技", return_1d=0.001, return_5d=0.01),
        SimpleNamespace(symbol="HSTECH", sector="科技", return_1d=-0.002, return_5d=0.0),
    ]
    monitor_rows = [
        {"name": "美国10Y收益率", "return_1d": -0.015},
        {"name": "美元指数", "return_5d": -0.003},
        {"name": "VIX波动率", "latest": 18.0},
    ]
    drivers = {"industry_spot": pd.DataFrame([{"名称": "银行"}]), "concept_spot": pd.DataFrame()}
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()}
    news_report = {"items": [{"category": "fed"}]}
    regime = {"current_regime": "recovery", "preferred_assets": ["宽基", "成长股"]}
    a_share_watch_meta = {"sector_counts": {"宽基": 1, "银行": 1, "电网": 1}}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime, a_share_watch_meta)

    assert narrative["theme"] == "broad_market_repair"
    assert narrative["label"] == "宽基修复"
    headline_lines = _compact_headline_lines(narrative, {"pmi": 50.1}, monitor_rows, {})
    assert "背景框架" in headline_lines[1]
    assert "交易主线候选" in headline_lines[1]


def test_primary_narrative_can_identify_power_utilities_separately_from_policy() -> None:
    snapshots = [
        SimpleNamespace(symbol="561380", sector="电网", return_1d=0.015, return_5d=0.04),
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=0.001, return_5d=0.01),
        SimpleNamespace(symbol="510880", sector="高股息", return_1d=0.004, return_5d=0.012),
    ]
    monitor_rows = [
        {"name": "VIX波动率", "latest": 19.0},
        {"name": "美元指数", "return_5d": 0.001},
    ]
    drivers = {"industry_spot": pd.DataFrame([{"名称": "电力设备"}, {"名称": "公用事业"}]), "concept_spot": pd.DataFrame()}
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()}
    news_report = {"items": [{"category": "china_macro"}]}
    regime = {"current_regime": "deflation", "preferred_assets": ["电网", "高股息"]}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime)

    assert narrative["theme"] == "power_utilities"
    assert narrative["label"] == "电网/公用事业"


def test_primary_narrative_does_not_misclassify_utilities_strength_as_dividend_defense() -> None:
    snapshots = [
        SimpleNamespace(symbol="561380", sector="电网", return_1d=0.018, return_5d=0.052),
        SimpleNamespace(symbol="510880", sector="高股息", return_1d=0.001, return_5d=0.008),
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=0.003, return_5d=0.011),
        SimpleNamespace(symbol="QQQM", sector="科技", return_1d=0.006, return_5d=0.018),
    ]
    monitor_rows = [
        {"name": "VIX波动率", "latest": 18.5},
        {"name": "美元指数", "return_5d": -0.001},
    ]
    drivers = {
        "industry_spot": pd.DataFrame([{"名称": "逆变器"}, {"名称": "储能"}, {"名称": "公用事业"}]),
        "concept_spot": pd.DataFrame(),
    }
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()}
    news_report = {"items": [{"category": "china_macro"}]}
    regime = {"current_regime": "deflation", "preferred_assets": ["电网", "高股息"]}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime)

    assert narrative["theme"] == "power_utilities"
    assert narrative["scores"]["power_utilities"] > narrative["scores"]["dividend_defense"]


def test_primary_narrative_prefers_power_utilities_over_dividend_when_no_bank_keywords() -> None:
    snapshots = [
        SimpleNamespace(symbol="561380", sector="电网", return_1d=0.012, return_5d=0.035),
        SimpleNamespace(symbol="510880", sector="高股息", return_1d=0.004, return_5d=0.015),
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=-0.002, return_5d=0.005),
    ]
    monitor_rows = [
        {"name": "VIX波动率", "latest": 21.0},
        {"name": "美元指数", "return_5d": 0.001},
    ]
    drivers = {
        "industry_spot": pd.DataFrame([{"名称": "储能"}, {"名称": "逆变器"}, {"名称": "公用事业"}]),
        "concept_spot": pd.DataFrame(),
    }
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()}
    news_report = {"items": []}
    regime = {"current_regime": "deflation", "preferred_assets": ["电网", "高股息"]}
    a_share_watch_meta = {"sector_counts": {"高股息": 3, "电网": 2}}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime, a_share_watch_meta)

    assert narrative["theme"] == "power_utilities"
    assert narrative["label"] == "电网/公用事业"


def test_primary_narrative_uses_a_share_watch_sector_counts_to_boost_theme() -> None:
    snapshots = [
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=0.0, return_5d=0.01),
        SimpleNamespace(symbol="QQQM", sector="科技", return_1d=0.005, return_5d=0.02),
    ]
    monitor_rows = [
        {"name": "美国10Y收益率", "return_1d": -0.005},
        {"name": "美元指数", "return_5d": -0.001},
        {"name": "VIX波动率", "latest": 20.0},
    ]
    drivers = {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()}
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()}
    news_report = {"items": []}
    regime = {"current_regime": "recovery", "preferred_assets": ["宽基"]}
    a_share_watch_meta = {"sector_counts": {"宽基": 2, "银行": 1}}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime, a_share_watch_meta)

    assert narrative["theme"] == "broad_market_repair"
    assert narrative["scores"]["broad_market_repair"] > narrative["scores"]["rate_growth"]


def test_primary_narrative_prefers_hard_tech_label_for_ai_hardware_chain() -> None:
    snapshots = [
        SimpleNamespace(symbol="QQQM", sector="科技", return_1d=0.012, return_5d=0.028),
        SimpleNamespace(symbol="HSTECH", sector="科技", return_1d=0.006, return_5d=0.012),
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=0.001, return_5d=0.008),
    ]
    monitor_rows = [
        {"name": "美国10Y收益率", "return_1d": -0.006},
        {"name": "美元指数", "return_5d": -0.002},
        {"name": "VIX波动率", "latest": 18.0},
    ]
    drivers = {
        "industry_spot": pd.DataFrame([{"名称": "半导体"}, {"名称": "光模块"}, {"名称": "液冷服务器"}]),
        "concept_spot": pd.DataFrame(),
    }
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame()}
    news_report = {"items": [{"category": "ai"}, {"category": "semiconductor"}]}
    regime = {"current_regime": "recovery", "preferred_assets": ["成长股"]}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime)

    assert narrative["theme"] == "ai_semis"
    assert narrative["label"] == "硬科技 / AI硬件链"


def test_primary_narrative_uses_chip_proxy_strength_to_override_generic_repair() -> None:
    snapshots = [
        SimpleNamespace(symbol="QQQM", sector="科技", return_1d=0.009, return_5d=0.024),
        SimpleNamespace(symbol="HSTECH", sector="科技", return_1d=0.004, return_5d=0.012),
        SimpleNamespace(symbol="510210", sector="宽基", return_1d=0.012, return_5d=0.031),
        SimpleNamespace(symbol="588200", sector="科技", return_1d=0.018, return_5d=0.112),
        SimpleNamespace(symbol="512480", sector="科技", return_1d=0.017, return_5d=0.104),
        SimpleNamespace(symbol="515070", sector="科技", return_1d=0.021, return_5d=0.110),
    ]
    monitor_rows = [
        {"name": "美国10Y收益率", "return_1d": -0.004},
        {"name": "美元指数", "return_5d": -0.002},
        {"name": "VIX波动率", "latest": 18.5},
    ]
    drivers = {
        "industry_spot": pd.DataFrame([{"名称": "证券Ⅱ"}, {"名称": "半导体"}, {"名称": "电池"}]),
        "concept_spot": pd.DataFrame(),
    }
    pulse = {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame([{"所属行业": "光学光电"}, {"所属行业": "通信服务"}])}
    news_report = {"items": [{"category": "china_macro"}, {"category": "semiconductor"}]}
    regime = {"current_regime": "recovery", "preferred_assets": ["宽基", "成长股"]}

    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime)

    assert narrative["theme"] == "ai_semis"
    assert narrative["scores"]["ai_semis"] > narrative["scores"]["broad_market_repair"]


def test_build_market_payload_includes_market_analysis_blocks(monkeypatch) -> None:
    monkeypatch.setattr(
        briefing_module,
        "build_market_analysis",
        lambda config, overview, pulse, drivers: {  # noqa: ARG005
            "index_rows": [["上证指数", "3400.00", "+0.20%", "偏强修复", "周线金叉", "月线修复", "常态量能", "等待确认"]],
            "index_lines": ["上证指数：偏强修复。"],
            "market_signal_rows": [["市场宽度", "上涨 2800 / 下跌 2100", "分歧中性", "涨跌比 1.33"]],
            "market_signal_lines": ["市场宽度 `分歧中性`。"],
            "rotation_rows": [["行业", "银行(+1.40%)", "半导体(-0.80%)", "防守占优，高低切明显"]],
            "rotation_lines": ["行业轮动靠前: 银行(+1.40%)。"],
            "summary_lines": ["核心指数强弱并不统一，当前更像结构性行情而不是全市场同向共振。"],
        },
    )

    payload = _build_market_payload(
        config={},
        narrative={"summary": "当前主线候选: `宽基修复`。", "background_regime": "通缩/偏弱"},
        china_macro={"pmi": 50.1},
        regime_result={"current_regime": "deflation", "reasoning": [], "preferred_assets": ["宽基"]},
        overview={"domestic_indices": [], "breadth": {}},
        pulse={"zt_pool": pd.DataFrame(), "dt_pool": pd.DataFrame()},
        drivers={"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame()},
        news_report={"items": []},
        monitor_rows=[],
        snapshots=[],
        anomaly_report={},
        liquidity_lines=[],
        overnight_rows=[],
        watchlist_rows=[],
        a_share_watch_rows=[],
        a_share_watch_lines=[],
        a_share_watch_meta={},
        a_share_watch_candidates=[],
        data_coverage="中国宏观 | 全市场快照",
        missing_sources="无",
        macro_items=[],
        alerts=[],
        quality_lines=[],
        proxy_contract={},
        evidence_rows=[],
    )

    assert payload["index_signal_rows"][0][0] == "上证指数"
    assert payload["market_signal_rows"][0][0] == "市场宽度"
    assert payload["rotation_rows"][0][0] == "行业"
    assert payload["headline_lines"][0].startswith("核心指数强弱并不统一")
