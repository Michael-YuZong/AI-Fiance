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
