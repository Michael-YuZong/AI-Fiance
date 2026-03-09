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


def test_market_cn_normalizes_open_fund_nav(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    def fake_cached_call(_cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        return fetcher(*args, **kwargs)

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    monkeypatch.setattr(
        collector,
        "_ak_function",
        lambda name: (
            (lambda **_: pd.DataFrame({"净值日期": ["2026-03-07", "2026-03-08"], "单位净值": ["1.01", "1.02"]}))
            if name == "fund_open_fund_info_em"
            else (_ for _ in ()).throw(RuntimeError("unexpected"))
        ),
    )
    frame = collector.get_open_fund_daily("022365")
    assert list(frame.columns) == ["date", "open", "high", "low", "close", "volume", "amount"]
    assert float(frame["close"].iloc[-1]) == 1.02


def test_market_cn_index_daily_falls_back_to_proxy_etf(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(RuntimeError("ak failed")))
    monkeypatch.setattr(collector, "get_etf_daily", lambda symbol, **_: pd.DataFrame({"date": ["2026-03-08"], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [0], "amount": [0]}))
    frame = collector.get_index_daily("000300", proxy_symbol="510330")
    assert not frame.empty
