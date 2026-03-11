"""Tests for market drivers collector Tushare-first aggregation."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.collectors.market_drivers import MarketDriversCollector


def test_market_drivers_market_flow_prefers_tushare_moneyflow(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "moneyflow":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260310",
                        "buy_sm_amount": 100.0,
                        "sell_sm_amount": 90.0,
                        "buy_md_amount": 110.0,
                        "sell_md_amount": 100.0,
                        "buy_lg_amount": 140.0,
                        "sell_lg_amount": 120.0,
                        "buy_elg_amount": 150.0,
                        "sell_elg_amount": 130.0,
                    }
                ]
            )
        if api_name == "daily":
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260310", "amount": 1_000.0}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector._market_flow(datetime(2026, 3, 10))
    frame = report["frame"]
    assert not frame.empty
    assert report["latest_date"] == "2026-03-10"
    assert float(frame.iloc[0]["超大单净流入-净额"]) == 200_000.0
    assert float(frame.iloc[0]["大单净流入-净额"]) == 200_000.0
    assert float(frame.iloc[0]["主力净流入-净额"]) == 400_000.0


def test_market_drivers_top10_normalizes_tushare_fields(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "hsgt_top10"
        assert kwargs.get("trade_date") == "20260310"
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260310",
                    "ts_code": "600519.SH",
                    "name": "贵州茅台",
                    "close": 1500.0,
                    "change": 2.5,
                    "rank": 1,
                    "market_type": 1,
                    "amount": 3_200_000_000.0,
                    "net_amount": 500_000_000.0,
                    "buy": 1_850_000_000.0,
                    "sell": 1_350_000_000.0,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr("src.collectors.market_drivers.datetime", type("FrozenDateTime", (), {
        "now": staticmethod(lambda: datetime(2026, 3, 10)),
        "strptime": staticmethod(datetime.strptime),
    }))
    frame = collector._ts_northbound_top10()
    assert not frame.empty
    assert frame.loc[0, "日期"] == "2026-03-10"
    assert frame.loc[0, "代码"] == "600519"
    assert frame.loc[0, "市场"] == "沪股通"
    assert float(frame.loc[0, "净买额"]) == 500_000_000.0


def test_market_drivers_pledge_stat_normalizes_tushare_fields(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "pledge_stat"
        assert kwargs.get("end_date") == "20260310"
        return pd.DataFrame(
            [
                {
                    "end_date": "20260310",
                    "ts_code": "300750.SZ",
                    "name": "宁德时代",
                    "pledge_count": 2,
                    "unrest_pledge": 150.0,
                    "rest_pledge": 30.0,
                    "total_share": 2_448_907.12,
                    "pledge_ratio": 1.2,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr("src.collectors.market_drivers.datetime", type("FrozenDateTime", (), {"now": staticmethod(lambda: datetime(2026, 3, 10))}))
    frame = collector._ts_pledge_stat()
    assert not frame.empty
    assert frame.loc[0, "截止日期"] == "2026-03-10"
    assert frame.loc[0, "代码"] == "300750"
    assert float(frame.loc[0, "质押比例"]) == 1.2
