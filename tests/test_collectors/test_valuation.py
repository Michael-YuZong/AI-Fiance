"""Tests for valuation collector Tushare-first helpers."""

from __future__ import annotations

import pandas as pd

from src.collectors.valuation import ValuationCollector


def test_valuation_index_weight_uses_index_code_candidates(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "index_weight"
        if kwargs.get("index_code") == "000300.SH":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "con_code": "600519.SH", "weight": 5.0, "con_name": "贵州茅台"},
                    {"trade_date": "20260310", "con_code": "300750.SZ", "weight": 4.0, "con_name": "宁德时代"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_cn_index_constituent_weights("000300", top_n=2)
    assert list(frame["symbol"]) == ["600519", "300750"]


def test_valuation_etf_nav_resolves_tushare_fund_code(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "fund_basic":
            return pd.DataFrame([{"ts_code": "510300.SH", "name": "沪深300ETF华泰柏瑞"}])
        if api_name == "fund_nav":
            assert kwargs.get("ts_code") == "510300.SH"
            return pd.DataFrame([{"end_date": "20260310", "unit_nav": 3.0}])
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_cn_etf_nav_history("510300", days=30)
    assert not frame.empty
    assert str(frame.iloc[-1]["end_date"].date()) == "2026-03-10"


def test_cn_stock_financial_proxy_merges_daily_basic_into_fina_indicator(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_tushare_stock_financial",
        lambda symbol: {  # noqa: ARG005
            "report_date": "2025-12-31",
            "roe": 15.0,
            "gross_margin": 28.0,
        },
    )
    monkeypatch.setattr(
        collector,
        "_ts_daily_basic_for_stock",
        lambda symbol: {  # noqa: ARG005
            "pe_ttm": 24.5,
            "pb": 3.1,
            "ps_ttm": 2.8,
        },
    )
    result = collector.get_cn_stock_financial_proxy("300750")
    assert result["report_date"] == "2025-12-31"
    assert result["roe"] == 15.0
    assert result["pe_ttm"] == 24.5
    assert result["pb"] == 3.1
