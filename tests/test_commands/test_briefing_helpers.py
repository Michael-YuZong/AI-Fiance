"""Tests for briefing helper proxy disclosures."""

from __future__ import annotations

from types import SimpleNamespace

import src.commands.briefing as briefing_module
from src.commands.briefing import (
    _appendix_derivative_lines,
    _action_lines,
    _briefing_a_share_watch_rows,
    _briefing_internal_dir,
    _coverage_metadata,
    _flow_lines,
    _load_same_day_briefing,
    _noon_action_lines,
    _monitor_alerts,
    _persist_briefing,
    _quality_lines,
    _sentiment_lines,
    _tomorrow_action_lines,
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

    assert detail_path == tmp_path / "reports/briefings/internal/daily_briefing_2026-03-13.md"
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
    pool = [
        SimpleNamespace(
            symbol="300750",
            name="宁德时代",
            asset_type="cn_stock",
            region="CN",
            sector="新能源",
            chain_nodes=["新能源"],
            in_watchlist=False,
            metadata={"bak_strength": 90, "bak_activity": 80, "bak_attack": 70},
            turnover=1_000_000_000,
        )
    ]
    monkeypatch.setattr(briefing_module, "build_stock_pool", lambda config, market="cn", max_candidates=60: (pool, ["部分样本缺少完整事件覆盖。"]))  # noqa: ARG005
    monkeypatch.setattr(briefing_module, "build_market_context", lambda config, relevant_asset_types=None: {"regime": {}, "day_theme": {}, "notes": []})  # noqa: ARG005
    monkeypatch.setattr(
        briefing_module,
        "analyze_opportunity",
        lambda symbol, asset_type, config, context=None, metadata_override=None: {  # noqa: ARG005
            "symbol": symbol,
            "name": "宁德时代",
            "metadata": {"sector": "新能源"},
            "rating": {"label": "较强机会", "rank": 3},
            "action": {"position": "首次建仓 ≤3%"},
            "narrative": {"judgment": {"state": "持有优于追高"}},
            "dimensions": {"risk": {"score": 60}, "relative_strength": {"score": 80}, "technical": {"score": 70}},
            "excluded": False,
        },
    )

    rows, lines, meta = _briefing_a_share_watch_rows({})

    assert rows == [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]]
    assert any("Tushare 优先" in item for item in lines)
    assert any("初筛池 `1`" in item for item in lines)
    assert meta == {
        "enabled": True,
        "mode": "tushare_priority_full_market_prescreen",
        "pool_size": 1,
        "shortlist_size": 1,
        "complete_analysis_size": 1,
        "report_top_n": 1,
        "blind_spot": "部分样本缺少完整事件覆盖。",
    }


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
