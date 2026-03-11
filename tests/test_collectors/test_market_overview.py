"""Tests for market overview collector Tushare-first paths."""

from __future__ import annotations

import pandas as pd

from src.collectors.market_overview import MarketOverviewCollector


def test_market_overview_breadth_prefers_tushare(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")
    monkeypatch.setattr(
        collector,
        "_ts_call",
        lambda api_name, **kwargs: pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "20260310", "pct_chg": 1.0, "amount": 1000.0},
                {"ts_code": "000002.SZ", "trade_date": "20260310", "pct_chg": -0.5, "amount": 2000.0},
                {"ts_code": "000003.SZ", "trade_date": "20260310", "pct_chg": 0.0, "amount": 3000.0},
            ]
        )
        if api_name == "daily"
        else pd.DataFrame(),
    )

    breadth = collector._collect_breadth()
    assert breadth["source"] == "tushare_daily"
    assert breadth["up_count"] == 1
    assert breadth["down_count"] == 1
    assert breadth["flat_count"] == 1
    assert breadth["turnover"] == 0.06


def test_market_overview_domestic_indices_try_sh_before_sz_for_000_prefix(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "index_daily"
        if kwargs.get("ts_code") == "000300.SH":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "open": 10.0, "high": 10.1, "low": 9.8, "close": 10.0, "amount": 10_000.0},
                    {"trade_date": "20260309", "open": 9.7, "high": 9.9, "low": 9.6, "close": 9.8, "amount": 9_000.0},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    rows = collector._ts_domestic_indices([{"symbol": "000300", "name": "沪深300"}])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "000300"
    assert rows[0]["latest"] == 10.0
