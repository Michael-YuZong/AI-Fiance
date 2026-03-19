"""Tests for market pulse collector Tushare-first pools."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.collectors.market_pulse import MarketPulseCollector


def test_market_pulse_limit_pool_prefers_tushare(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "limit_list_d":
            assert kwargs.get("limit_type") == "U"
            return pd.DataFrame(
                [
                    {"ts_code": "300750.SZ", "name": "宁德时代", "industry": "新能源设备", "pct_chg": 9.99, "up_stat": "2/2"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    info = collector._ts_limit_pool("U", datetime(2026, 3, 10))
    frame = info["frame"]
    assert info["date"] == "2026-03-10"
    assert frame.loc[0, "名称"] == "宁德时代"
    assert frame.loc[0, "所属行业"] == "新能源设备"
    assert float(frame.loc[0, "连板数"]) == 2.0


def test_market_pulse_prev_zt_pool_uses_previous_limit_and_current_daily(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260309", "20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "limit_list_d":
            return pd.DataFrame(
                [
                    {"ts_code": "300750.SZ", "name": "宁德时代", "industry": "新能源设备", "pct_chg": 9.99, "up_stat": "1/1"},
                ]
            )
        if api_name == "daily":
            return pd.DataFrame([{"ts_code": "300750.SZ", "pct_chg": 3.5}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    info = collector._derive_prev_zt_pool("2026-03-10")
    frame = info["frame"]
    assert info["date"] == "2026-03-10"
    assert float(frame.loc[0, "涨跌幅"]) == 3.5


def test_market_pulse_lhb_stats_aggregates_tushare_top_list(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_recent_open_trade_dates",
        lambda *args, **kwargs: ["20260307", "20260308", "20260309", "20260310"],
    )

    def fake_top_list(trade_date: str) -> pd.DataFrame:
        if trade_date in {"2026-03-09", "2026-03-10"}:
            return pd.DataFrame(
                [
                    {"ts_code": "300750.SZ", "name": "宁德时代"},
                    {"ts_code": "600519.SH", "name": "贵州茅台"},
                ]
            )
        return pd.DataFrame([{"ts_code": "300750.SZ", "name": "宁德时代"}])

    monkeypatch.setattr(collector, "_ts_top_list", fake_top_list)
    frame = collector._ts_lhb_stats("2026-03-10")
    assert not frame.empty
    assert frame.loc[0, "名称"] == "宁德时代"
    assert int(frame.loc[0, "上榜次数"]) == 4
