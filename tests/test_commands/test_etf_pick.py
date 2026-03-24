"""Tests for ETF pick command fallbacks."""

from __future__ import annotations

import time

from src.commands.etf_pick import _payload_from_analyses, _watchlist_fallback_payload


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


def test_payload_from_analyses_marks_etf_chips_as_auxiliary_dimension() -> None:
    short = _analysis("159981", "能源化工ETF", 1)

    payload = _payload_from_analyses([short], {})

    chips_row = next(row for row in payload["winner"]["dimension_rows"] if row[0] == "筹码结构（辅助项）")
    assert chips_row[1] == "辅助项"
    assert "主排序不直接使用这项" in chips_row[2]


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
