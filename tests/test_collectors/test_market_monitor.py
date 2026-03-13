from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import src.collectors.market_monitor as market_monitor_module
from src.collectors.market_monitor import MarketMonitorCollector


def test_market_monitor_skips_overly_stale_cache_on_refresh_failure(monkeypatch) -> None:
    collector = MarketMonitorCollector({"market_monitor_max_stale_hours": 12})
    stale_frame = pd.DataFrame({"Close": [80.0, 82.0, 86.0]})

    monkeypatch.setattr(
        market_monitor_module,
        "load_yaml",
        lambda *_args, **_kwargs: {"monitors": [{"symbol": "BZ=F", "name": "布伦特原油", "category": "energy"}]},
    )
    monkeypatch.setattr(market_monitor_module, "yf", SimpleNamespace(Ticker=lambda _symbol: SimpleNamespace(history=lambda **_kwargs: None)))
    monkeypatch.setattr(
        collector,
        "_load_cache",
        lambda _cache_key, ttl_hours=None, allow_stale=False: None if not allow_stale else stale_frame,
    )
    monkeypatch.setattr(collector, "_stale_cache_age_hours", lambda _cache_key: 36.0)
    monkeypatch.setattr(collector.rate_limiter, "wait", lambda: None)
    monkeypatch.setattr(collector, "_execute_fetcher", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("Too Many Requests")))

    assert collector.collect() == []


def test_market_monitor_recent_stale_cache_keeps_disclosure(monkeypatch) -> None:
    collector = MarketMonitorCollector({"market_monitor_max_stale_hours": 12})
    stale_frame = pd.DataFrame({"Close": [80.0, 82.0, 86.0]})

    monkeypatch.setattr(
        market_monitor_module,
        "load_yaml",
        lambda *_args, **_kwargs: {"monitors": [{"symbol": "BZ=F", "name": "布伦特原油", "category": "energy"}]},
    )
    monkeypatch.setattr(market_monitor_module, "yf", SimpleNamespace(Ticker=lambda _symbol: SimpleNamespace(history=lambda **_kwargs: None)))
    monkeypatch.setattr(
        collector,
        "_load_cache",
        lambda _cache_key, ttl_hours=None, allow_stale=False: None if not allow_stale else stale_frame,
    )
    monkeypatch.setattr(collector, "_stale_cache_age_hours", lambda _cache_key: 6.0)
    monkeypatch.setattr(collector.rate_limiter, "wait", lambda: None)
    monkeypatch.setattr(collector, "_execute_fetcher", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("Too Many Requests")))

    rows = collector.collect()

    assert len(rows) == 1
    assert rows[0]["source_status"] == "stale_cache"
    assert rows[0]["stale_age_hours"] == 6.0
    assert "实时刷新失败" in rows[0]["data_warning"]
