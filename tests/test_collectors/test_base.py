from __future__ import annotations

import os
import time

import pandas as pd

import src.collectors.base as base_module
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


def test_load_cache_drops_corrupt_pickle_and_returns_none(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    cache_path = collector._cache_path("demo:corrupt")
    cache_path.write_bytes(b"not-a-valid-pickle")

    payload = collector._load_cache("demo:corrupt", ttl_hours=24)

    assert payload is None
    assert cache_path.exists() is False


def test_cached_call_can_disable_stale_fallback_on_error(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    collector._save_cache("demo:key", {"value": 42})
    cache_path = collector._cache_path("demo:key")
    old_time = time.time() - 3600
    os.utime(cache_path, (old_time, old_time))

    def broken_fetcher():  # noqa: ANN202
        raise RuntimeError("network down")

    try:
        collector.cached_call("demo:key", broken_fetcher, ttl_hours=0, allow_stale_on_error=False)
    except RuntimeError as exc:
        assert "network down" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected stale fallback to stay disabled")


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


def test_cached_call_does_not_retry_known_parser_failure(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"count": 0}

    def broken_fetcher():  # noqa: ANN202
        calls["count"] += 1
        raise ValueError("Excel file format cannot be determined, you must specify an engine manually.")

    try:
        collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    except ValueError as exc:
        assert "Excel file format" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected parser failure to bubble up")

    assert calls["count"] == 1


def test_cached_call_does_not_retry_length_mismatch_parser_failure(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"count": 0}

    def broken_fetcher():  # noqa: ANN202
        calls["count"] += 1
        raise ValueError("Length mismatch: Expected axis has 0 elements, new values have 13 elements")

    try:
        collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    except ValueError as exc:
        assert "Length mismatch" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected length mismatch failure to bubble up")

    assert calls["count"] == 1


def test_cached_call_does_not_retry_known_runtime_wrapper_failure(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"count": 0}

    def broken_fetcher():  # noqa: ANN202
        calls["count"] += 1
        raise TypeError("exceptions must derive from BaseException")

    try:
        collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    except TypeError as exc:
        assert "BaseException" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected wrapper failure to bubble up")

    assert calls["count"] == 1


def test_cached_call_does_not_retry_rate_limit_failure(tmp_path):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"count": 0}

    def broken_fetcher():  # noqa: ANN202
        calls["count"] += 1
        raise RuntimeError("Too Many Requests. Rate limited. Try after a while.")

    try:
        collector.cached_call("demo:key", broken_fetcher, ttl_hours=0)
    except RuntimeError as exc:
        assert "Too Many Requests" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected rate limit failure to bubble up")

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


def test_tushare_pro_uses_configured_timeout(tmp_path, monkeypatch):
    class _FakeTs:
        def __init__(self) -> None:
            self.token = None
            self.timeout = None

        def set_token(self, token):  # noqa: ANN001
            self.token = token

        def pro_api(self, timeout=30):  # noqa: ANN001
            self.timeout = timeout
            return object()

    fake_ts = _FakeTs()
    monkeypatch.setattr(base_module, "ts", fake_ts)
    monkeypatch.setattr(base_module, "_tushare_api_instance", None)
    monkeypatch.setattr(base_module, "_tushare_api_timeout", None)

    collector = BaseCollector(
        {
            "storage": {"cache_dir": str(tmp_path)},
            "api_keys": {"tushare": "demo-token"},
            "tushare_timeout_seconds": 9,
        },
        name="TestCollector",
    )

    collector._tushare_pro()

    assert fake_ts.token == "demo-token"
    assert fake_ts.timeout == 9


def test_tushare_pro_defaults_to_faster_repo_timeout(tmp_path, monkeypatch):
    class _FakeTs:
        def __init__(self) -> None:
            self.token = None
            self.timeout = None

        def set_token(self, token):  # noqa: ANN001
            self.token = token

        def pro_api(self, timeout=30):  # noqa: ANN001
            self.timeout = timeout
            return object()

    fake_ts = _FakeTs()
    monkeypatch.setattr(base_module, "ts", fake_ts)
    monkeypatch.setattr(base_module, "_tushare_api_instance", None)
    monkeypatch.setattr(base_module, "_tushare_api_timeout", None)

    collector = BaseCollector(
        {
            "storage": {"cache_dir": str(tmp_path)},
            "api_keys": {"tushare": "demo-token"},
        },
        name="TestCollector",
    )

    collector._tushare_pro()

    assert fake_ts.token == "demo-token"
    assert fake_ts.timeout == 8


def test_resolve_tushare_etf_code_prefers_etf_basic_snapshot(tmp_path, monkeypatch):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    monkeypatch.setattr(
        collector,
        "_ts_etf_basic_snapshot",
        lambda: pd.DataFrame(
            [
                {"ts_code": "510300.OF", "list_status": "L"},
                {"ts_code": "510300.SH"},
                {"ts_code": "510880.SH"},
            ]
        ),
    )
    monkeypatch.setattr(collector, "_ts_fund_basic_snapshot", lambda market: pd.DataFrame())  # noqa: ARG005

    assert collector._resolve_tushare_etf_code("510300") == "510300.SH"


def test_ts_fund_adj_snapshot_uses_configured_args(tmp_path, monkeypatch):
    collector = BaseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}}, name="TestCollector")
    calls = {"api": "", "kwargs": {}}

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        calls["api"] = api_name
        calls["kwargs"] = dict(kwargs)
        if api_name == "fund_adj":
            return pd.DataFrame([{"trade_date": "20260310", "adj_factor": 1.0}])
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector._ts_fund_adj_snapshot(ts_code="510300.SH", start_date="2026-03-01", end_date="2026-03-10")

    assert not frame.empty
    assert calls["api"] == "fund_adj"
    assert calls["kwargs"]["ts_code"] == "510300.SH"
    assert calls["kwargs"]["start_date"] == "20260301"
    assert calls["kwargs"]["end_date"] == "20260310"
