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
