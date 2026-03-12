from __future__ import annotations

import os
import time

import pandas as pd

from src.collectors.base import BaseCollector


def test_cached_call_uses_stale_cache_when_fetch_fails(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    collector._save_cache("demo:key", {"value": 42})
    cache_path = collector._cache_path("demo:key")
    old_time = time.time() - 3600
    os.utime(cache_path, (old_time, old_time))

    def broken_fetcher():  # noqa: ANN202
        raise RuntimeError("network down")

    result = collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    assert result == {"value": 42}


def test_cached_call_uses_stale_cache_when_fetch_returns_empty(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    collector._save_cache("demo:key", {"value": 7})
    cache_path = collector._cache_path("demo:key")
    old_time = time.time() - 3600
    os.utime(cache_path, (old_time, old_time))

    result = collector.cached_call("demo:key", lambda: pd.DataFrame(), ttl_hours=0)
    assert result == {"value": 7}


def test_cached_call_fast_falls_back_to_stale_cache_on_resolution_error(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    collector._save_cache("demo:key", {"value": 9})
    cache_path = collector._cache_path("demo:key")
    old_time = time.time() - 3600
    os.utime(cache_path, (old_time, old_time))
    calls = {"count": 0}

    def broken_fetcher():  # noqa: ANN202
        calls["count"] += 1
        raise RuntimeError("HTTPSConnectionPool(host='news.google.com', port=443): Failed to resolve 'news.google.com'")

    result = collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    assert result == {"value": 9}
    assert calls["count"] == 1


def test_cached_call_does_not_retry_resolution_error_without_stale_cache(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"count": 0}

    def broken_fetcher():  # noqa: ANN202
        calls["count"] += 1
        raise RuntimeError("NameResolutionError: Failed to resolve host")

    try:
        collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    except RuntimeError as exc:
        assert "Failed to resolve host" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected resolution error to bubble up")

    assert calls["count"] == 1


def test_cached_call_does_not_retry_yfinance_history_none_type_failure(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"count": 0}

    def history():  # noqa: ANN202
        calls["count"] += 1
        raise TypeError("'NoneType' object is not subscriptable")

    try:
        collector.cached_call("demo:key", history, ttl_hours=0)
    except TypeError as exc:
        assert "NoneType" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected yfinance-style history failure to bubble up")

    assert calls["count"] == 1


def test_base_collector_sets_no_proxy_for_domestic_data_domains(tmp_path, monkeypatch):
    monkeypatch.setenv("NO_PROXY", "example.com")
    monkeypatch.delenv("no_proxy", raising=False)

    BaseCollector({"storage": {"cache_dir": str(tmp_path)}}, name="TestCollector")

    for key in ("NO_PROXY", "no_proxy"):
        value = os.environ.get(key, "")
        assert "example.com" in value
        assert ".eastmoney.com" in value
        assert ".jin10.com" in value
        assert ".tushare.pro" in value
        assert ".cninfo.com.cn" in value
