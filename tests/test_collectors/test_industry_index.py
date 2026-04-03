"""Tests for standardized Tushare industry/index collector."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.collectors.industry_index import IndustryIndexCollector


def test_collect_market_snapshot_builds_sw_report_contract(monkeypatch, tmp_path) -> None:
    collector = IndustryIndexCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14: ["20260401"])  # noqa: ARG005

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "sw_daily":
            assert kwargs.get("trade_date") == "20260401"
            return pd.DataFrame(
                [
                    {"ts_code": "801737.SI", "name": "通信设备", "pct_change": 3.6, "amount": 123.4, "trade_date": "20260401"},
                    {"ts_code": "801730.SI", "name": "电网设备", "pct_change": 2.8, "amount": 98.7, "trade_date": "20260401"},
                ]
            )
        if api_name == "index_classify":
            level = kwargs.get("level")
            if level == "L2":
                return pd.DataFrame(
                    [
                        {"index_code": "801737.SI", "industry_name": "通信设备", "level": "L2"},
                        {"index_code": "801730.SI", "industry_name": "电网设备", "level": "L2"},
                    ]
                )
            return pd.DataFrame()
        if api_name == "ci_daily":
            return pd.DataFrame()
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    snapshot = collector.collect_market_snapshot(datetime(2026, 4, 1, 18, 0, 0))

    report = snapshot["sw_industry_report"]
    assert snapshot["sw_industry_spot"].iloc[0]["名称"] == "通信设备"
    assert report["latest_date"] == "2026-04-01"
    assert report["is_fresh"] is True
    assert report["source"] == "tushare.sw_daily+tushare.index_classify"
    assert report["fallback"] == "none"
    assert report["diagnosis"] == "live"


def test_get_stock_industry_snapshot_joins_membership_with_daily(monkeypatch, tmp_path) -> None:
    collector = IndustryIndexCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14: ["20260401"])  # noqa: ARG005

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "index_member_all":
            return pd.DataFrame(
                [
                    {
                        "index_code": "801730.SI",
                        "index_name": "通信",
                        "con_code": "300308.SZ",
                        "l1_code": "801700.SI",
                        "l1_name": "信息技术",
                        "l2_code": "801730.SI",
                        "l2_name": "通信设备",
                        "l3_code": "801731.SI",
                        "l3_name": "通信传输设备",
                    }
                ]
            )
        if api_name == "ci_index_member":
            return pd.DataFrame(
                [
                    {
                        "con_code": "300308.SZ",
                        "l1_code": "CI005001.WI",
                        "l1_name": "TMT",
                        "l2_code": "CI005026.WI",
                        "l2_name": "通信设备",
                    }
                ]
            )
        if api_name == "sw_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "801700.SI", "name": "信息技术", "pct_change": 1.2, "amount": 100.0, "trade_date": "20260401"},
                    {"ts_code": "801730.SI", "name": "通信设备", "pct_change": 3.6, "amount": 130.0, "trade_date": "20260401"},
                    {"ts_code": "801731.SI", "name": "通信传输设备", "pct_change": 4.2, "amount": 88.0, "trade_date": "20260401"},
                ]
            )
        if api_name == "ci_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "CI005001.WI", "name": "TMT", "pct_change": 0.8, "amount": 56.0, "trade_date": "20260401"},
                    {"ts_code": "CI005026.WI", "name": "通信设备", "pct_change": 2.9, "amount": 42.0, "trade_date": "20260401"},
                ]
            )
        if api_name == "index_classify":
            level = kwargs.get("level")
            if level == "L1":
                return pd.DataFrame([{"index_code": "801700.SI", "industry_name": "信息技术", "level": "L1"}])
            if level == "L2":
                return pd.DataFrame([{"index_code": "801730.SI", "industry_name": "通信设备", "level": "L2"}])
            if level == "L3":
                return pd.DataFrame([{"index_code": "801731.SI", "industry_name": "通信传输设备", "level": "L3"}])
            return pd.DataFrame()
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    snapshot = collector.get_stock_industry_snapshot("300308", reference_date=datetime(2026, 4, 1, 18, 0, 0))

    assert snapshot["status"] == "matched"
    assert snapshot["is_fresh"] is True
    assert snapshot["fallback"] == "none"
    assert any(item["family"] == "sw" and item["index_name"] == "通信设备" for item in snapshot["items"])
    assert any(item["family"] == "ci" and item["index_name"] == "通信设备" for item in snapshot["items"])


def test_get_etf_industry_snapshot_matches_index_name_without_faking(monkeypatch, tmp_path) -> None:
    collector = IndustryIndexCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14: ["20260401"])  # noqa: ARG005

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "sw_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "801050.SI", "name": "有色金属", "pct_change": 2.65, "amount": 88.0, "trade_date": "20260401"},
                ]
            )
        if api_name == "index_classify":
            if kwargs.get("level") == "L2":
                return pd.DataFrame([{"index_code": "801050.SI", "industry_name": "有色金属", "level": "L2"}])
            return pd.DataFrame()
        if api_name == "ci_daily":
            return pd.DataFrame()
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    snapshot = collector.get_etf_industry_snapshot(
        {
            "symbol": "512400",
            "asset_type": "cn_etf",
            "name": "南方中证申万有色金属ETF",
            "index_name": "中证申万有色金属指数",
        },
        reference_date=datetime(2026, 4, 1, 18, 0, 0),
    )

    assert snapshot["status"] == "matched"
    assert snapshot["fallback"] == "name_match"
    assert snapshot["is_fresh"] is True
    assert snapshot["items"][0]["index_name"] == "有色金属"
