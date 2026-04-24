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


def test_market_overview_breadth_returns_empty_without_tushare_fallback(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")
    monkeypatch.setattr(collector, "_ts_call", lambda api_name, **kwargs: pd.DataFrame() if api_name == "daily" else pd.DataFrame())

    breadth = collector._collect_breadth()

    assert breadth == {}


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


def test_market_overview_domestic_indices_return_empty_without_tushare_rows(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type("FakeIndexTopic", (), {"get_domestic_overview_rows": staticmethod(lambda indices: [])})(),
    )

    rows = collector._collect_domestic([{"symbol": "000300", "name": "沪深300"}])

    assert rows == []


def test_market_overview_global_indices_use_index_topic_mainline(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_global_overview_rows": staticmethod(
                    lambda indices: [
                        {
                            "market": "美股",
                            "name": "标普500",
                            "symbol": "^GSPC",
                            "latest": 5050.0,
                            "change_pct": 0.01,
                            "source": "tushare.index_global",
                        }
                    ]
                )
            },
        )(),
    )

    rows = collector._collect_global([{"symbol": "^GSPC", "market": "美股", "name": "标普500"}])

    assert len(rows) == 1
    assert rows[0]["symbol"] == "^GSPC"
    assert rows[0]["source"] == "tushare.index_global"


def test_market_overview_collects_market_structure_snapshots(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")
    monkeypatch.setattr(
        "src.collectors.market_overview.load_yaml",
        lambda *args, **kwargs: {"domestic_indices": [], "global_indices": []},
    )
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_domestic_overview_rows": staticmethod(lambda indices: []),
                "get_global_overview_rows": staticmethod(lambda indices: []),
            },
        )(),
    )

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "daily":
            return pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "trade_date": "20260310", "pct_chg": 1.0, "amount": 1000.0},
                    {"ts_code": "000002.SZ", "trade_date": "20260310", "pct_chg": -0.5, "amount": 2000.0},
                ]
            )
        if api_name == "daily_info":
            assert kwargs.get("trade_date") == "20260310"
            assert kwargs.get("exchange") == "SZ,SH"
            return pd.DataFrame(
                [
                    {
                        "trade_date": "20260310",
                        "ts_code": "SZ_MARKET",
                        "ts_name": "深圳市场",
                        "com_count": 2200,
                        "amount": 4363.01,
                        "vol": 3811.75,
                        "total_share": 21657.12,
                        "total_mv": 236813.99,
                        "float_share": 17674.90,
                        "float_mv": 184009.17,
                        "trans_count": 830.0,
                        "pe": 25.46,
                        "tr": 2.18,
                        "exchange": "SZ",
                    }
                ]
            )
        if api_name == "sz_daily_info":
            assert kwargs.get("trade_date") == "20260310"
            return pd.DataFrame(
                [
                    {
                        "trade_date": "20260310",
                        "ts_code": "SZ_A",
                        "ts_name": "深圳A股",
                        "count": 1500,
                        "amount": 1234.5,
                        "vol": 678.9,
                        "total_share": 111.0,
                        "total_mv": 222.0,
                        "float_share": 99.0,
                        "float_mv": 188.0,
                        "trans_count": 456.0,
                        "pe": 20.5,
                        "tr": 3.2,
                        "exchange": "SZ",
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    payload = collector.collect()

    assert payload["breadth"]["source"] == "tushare_daily"
    assert payload["market_structure"]["source"] == "tushare.daily_info+sz_daily_info"
    assert payload["market_structure"]["as_of"] == "20260310"
    assert payload["market_structure"]["latest_date"] == "20260310"
    assert payload["market_structure"]["is_fresh"] is True
    assert payload["market_structure"]["daily_info"]
    assert payload["market_structure"]["sz_daily_info"]
    assert payload["market_structure"]["daily_info_snapshot"]["rows"][0]["source"] == "tushare.daily_info"
    assert payload["market_structure"]["sz_daily_info_snapshot"]["rows"][0]["source"] == "tushare.sz_daily_info"
    assert payload["market_structure"]["sz_daily_info_snapshot"]["rows"][0]["amount"] == 1234.5


def test_market_overview_market_structure_marks_stale_rows(monkeypatch, tmp_path):
    collector = MarketOverviewCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_ts_call",
        lambda api_name, **kwargs: pd.DataFrame(
                [
                    {
                        "trade_date": "20260309",
                        "ts_code": "SZ_MARKET",
                        "ts_name": "深圳市场",
                        "com_count": 2200,
                        "amount": 1000.0,
                    }
                ]
        ),
    )
    stale = collector._collect_market_structure_snapshot(
        api_name="daily_info",
        trade_date="20260310",
        call_kwargs={"trade_date": "20260310", "exchange": "SZ,SH"},
        row_scale=1.0,
        source_label="tushare.daily_info",
        disclosure="test disclosure",
    )

    assert stale["latest_date"] == "20260309"
    assert stale["is_fresh"] is False
    assert stale["fallback"] == "stale"
