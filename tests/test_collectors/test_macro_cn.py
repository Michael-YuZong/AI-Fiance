"""Tests for China macro collector."""

from __future__ import annotations

import pandas as pd

import src.collectors.macro_cn as macro_cn_module
from src.collectors.macro_cn import ChinaMacroCollector


class _FakeAk:
    @staticmethod
    def macro_china_pmi():
        return pd.DataFrame({"月份": ["2026-01"], "PMI": [50.2]})


def test_get_pmi_returns_dataframe(monkeypatch, tmp_path):
    monkeypatch.setattr(macro_cn_module, "ak", _FakeAk)
    collector = ChinaMacroCollector(config={"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 1}})
    result = collector.get_pmi()
    assert not result.empty
    assert "PMI" in result.columns


def test_get_fx_basic_returns_tushare_annotated_snapshot(monkeypatch, tmp_path):
    collector = ChinaMacroCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "fx_obasic":
            assert kwargs.get("classify") == "FX"
            assert kwargs.get("exchange") == "FXCM"
            return pd.DataFrame(
                [
                    {"ts_code": "USDCNH.FXCM", "name": "美元兑人民币", "classify": "FX", "exchange": "FXCM"},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_fx_basic()

    assert not frame.empty
    assert frame.attrs["source"] == "tushare.fx_obasic"
    assert frame.attrs["latest_date"] == ""
    assert frame.attrs["is_fresh"] is False
    assert frame.attrs["fallback"] == "none"
    assert "fx_obasic" in frame.attrs["disclosure"]
    assert frame.iloc[0]["ts_code"] == "USDCNH.FXCM"


def test_get_fx_daily_returns_normalized_tushare_snapshot(monkeypatch, tmp_path):
    collector = ChinaMacroCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "fx_daily":
            assert kwargs.get("ts_code") == "USDCNH.FXCM"
            return pd.DataFrame(
                [
                    {"ts_code": "USDCNH.FXCM", "trade_date": "20240102", "open": 7.10, "high": 7.20, "low": 7.05, "close": 7.15},
                    {"ts_code": "USDCNH.FXCM", "trade_date": "20240101", "open": 7.00, "high": 7.12, "low": 6.98, "close": 7.08},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_fx_daily("USDCNH.FXCM")

    assert not frame.empty
    assert list(frame["trade_date"]) == ["2024-01-01", "2024-01-02"]
    assert frame.attrs["source"] == "tushare.fx_daily"
    assert frame.attrs["latest_date"] == "2024-01-02"
    assert frame.attrs["is_fresh"] is False
    assert frame.attrs["fallback"] == "none"
    assert "fx_daily" in frame.attrs["disclosure"]


def test_get_fx_daily_returns_missing_snapshot_when_tushare_raises(monkeypatch, tmp_path):
    collector = ChinaMacroCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:  # noqa: ARG001
        if api_name == "fx_daily":
            raise Exception("您的IP数量超限，最大数量为2个！")
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_fx_daily("USDCNH.FXCM")

    assert frame.empty
    assert frame.attrs["source"] == "tushare.fx_daily"
    assert frame.attrs["latest_date"] == ""
    assert frame.attrs["is_fresh"] is False
    assert frame.attrs["fallback"] == "missing"
    assert "fx_daily" in frame.attrs["disclosure"]
