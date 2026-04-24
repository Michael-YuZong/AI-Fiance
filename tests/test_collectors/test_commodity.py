"""Tests for commodity collector."""

from __future__ import annotations

import pandas as pd

import src.collectors.commodity as commodity_module
from src.collectors.commodity import CommodityCollector


class _FakeAk:
    @staticmethod
    def futures_main_sina(*, symbol: str, start_date: str, end_date: str):  # noqa: ARG003
        return pd.DataFrame(
            [
                {"date": "2026-03-10", "open": 10.0, "close": 10.2},
            ]
        )

    @staticmethod
    def futures_spot_price(*, date: str):  # noqa: ARG003
        return pd.DataFrame(
            [
                {"date": "2026-03-10", "symbol": "AU0", "price": 600.0},
            ]
        )


def test_commodity_get_sge_basic_returns_tushare_annotated_snapshot(monkeypatch, tmp_path):
    collector = CommodityCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "sge_basic":
            assert kwargs.get("ts_code") == "Au99.95"
            return pd.DataFrame(
                [
                    {"ts_code": "Au99.95", "name": "Au99.95", "exchange": "SGE", "product": "gold"},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    frame = collector.get_sge_basic("Au99.95")

    assert not frame.empty
    assert frame.attrs["source"] == "tushare.sge_basic"
    assert frame.attrs["latest_date"] == ""
    assert frame.attrs["is_fresh"] is False
    assert frame.attrs["fallback"] == "none"
    assert "sge_basic" in frame.attrs["disclosure"]
    assert frame.iloc[0]["ts_code"] == "Au99.95"


def test_commodity_get_sge_daily_returns_normalized_tushare_snapshot(monkeypatch, tmp_path):
    collector = CommodityCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "sge_daily":
            assert kwargs.get("ts_code") == "Au99.95"
            return pd.DataFrame(
                [
                    {"ts_code": "Au99.95", "trade_date": "20240102", "open": 600.0, "high": 605.0, "low": 598.0, "close": 604.0},
                    {"ts_code": "Au99.95", "trade_date": "20240101", "open": 595.0, "high": 601.0, "low": 593.0, "close": 600.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    frame = collector.get_sge_daily("Au99.95")

    assert not frame.empty
    assert list(frame["trade_date"]) == ["2024-01-01", "2024-01-02"]
    assert frame.attrs["source"] == "tushare.sge_daily"
    assert frame.attrs["latest_date"] == "2024-01-02"
    assert frame.attrs["is_fresh"] is False
    assert frame.attrs["fallback"] == "none"
    assert "sge_daily" in frame.attrs["disclosure"]

    gold = collector.get_gold()
    assert not gold.empty
    assert gold.attrs["source"] == "tushare.sge_daily"
    assert gold.attrs["latest_date"] == "2024-01-02"


def test_commodity_main_contract_keeps_akshare_path(monkeypatch, tmp_path):
    monkeypatch.setattr(commodity_module, "ak", _FakeAk)
    collector = CommodityCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    frame = collector.get_main_contract("AU0")

    assert not frame.empty
    assert float(frame.iloc[0]["open"]) == 10.0
