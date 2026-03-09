"""Tests for CN ETF market collector fallbacks."""

from __future__ import annotations

import pandas as pd

from src.collectors.market_cn import ChinaMarketCollector


class _FakeTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def history(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Open": [1.0, 1.1],
                "High": [1.1, 1.2],
                "Low": [0.9, 1.0],
                "Close": [1.05, 1.15],
                "Volume": [1000, 1200],
            }
        )


class _FakeYFinance:
    def Ticker(self, symbol: str) -> _FakeTicker:  # noqa: N802
        return _FakeTicker(symbol)


def test_market_cn_falls_back_to_yahoo_when_ak_unavailable(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(RuntimeError("ak failed")))
    monkeypatch.setattr("src.collectors.market_cn.yf", _FakeYFinance())
    frame = collector.get_etf_daily("512400")
    assert not frame.empty


def test_market_cn_maps_exchange_suffix():
    collector = ChinaMarketCollector({})
    assert collector._yahoo_symbol("512400") == "512400.SS"
    assert collector._yahoo_symbol("159980") == "159980.SZ"
