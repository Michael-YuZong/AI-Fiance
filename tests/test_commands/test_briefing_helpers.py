"""Tests for briefing helper proxy disclosures."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import src.commands.briefing as briefing_module
from src.commands.briefing import (
    _appendix_derivative_lines,
    _action_lines,
    _briefing_a_share_watch_rows,
    _briefing_shared_market_context,
    _briefing_internal_dir,
    _compact_headline_lines,
    _coverage_metadata,
    _flow_lines,
    _load_same_day_briefing,
    _noon_action_lines,
    _monitor_alerts,
    _primary_narrative,
    _persist_briefing,
    _quality_lines,
    _sentiment_lines,
    _tomorrow_action_lines,
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


def test_briefing_coverage_and_quality_disclose_stale_monitor_rows() -> None:
    monitor_rows = [{"name": "布伦特原油", "data_warning": "实时刷新失败", "stale_age_hours": 36.0}]

    coverage, missing = _coverage_metadata({"items": []}, [], [], "", monitor_rows)
    quality = _quality_lines({"items": []}, {"lines": []}, monitor_rows)

    assert "宏观资产监控" in coverage
    assert "宏观资产监控(实时刷新)" in missing
    assert any("陈旧缓存回退" in item for item in quality)


def test_monitor_alerts_disclose_missing_or_stale_macro_monitor_data() -> None:
    assert any("未能完成实时刷新" in item for item in _monitor_alerts([]))
    stale_alerts = _monitor_alerts([{"name": "布伦特原油", "data_warning": "实时刷新失败"}])
    assert any("布伦特原油" in item for item in stale_alerts)


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

    rows, lines, meta = _briefing_a_share_watch_rows({})

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
    assert captured["context"] is None
    assert captured["max_candidates"] == 16
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
    assert captured["context"]["day_theme"]["code"] == "rate_growth"
    assert captured["max_candidates"] == 16
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

    rows, lines, meta = _briefing_a_share_watch_rows({})

    assert rows[0][1] == "宁德时代 (300750)"
    assert any("候选上限 `16`" in item for item in lines)
    assert meta["candidate_limit"] == 16


def test_briefing_action_helpers_include_portfolio_whatif_handoff() -> None:
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

    assert any("portfolio whatif buy 561380 2.2340 计划金额" in item for item in daily_lines)
    assert any("portfolio whatif buy 561380 2.2340 计划金额" in item for item in noon_lines)
    assert any("portfolio whatif buy 561380 2.2340 计划金额" in item for item in tomorrow_lines)


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
