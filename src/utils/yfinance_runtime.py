"""Shared yfinance runtime cleanup helpers."""

from __future__ import annotations


def disable_yfinance_sqlite_caches() -> None:
    try:
        import yfinance.cache as yf_cache
    except Exception:
        return

    for manager_name in ("_TzDBManager", "_CookieDBManager", "_ISINDBManager"):
        manager = getattr(yf_cache, manager_name, None)
        close_db = getattr(manager, "close_db", None)
        if callable(close_db):
            try:
                close_db()
            except Exception:
                pass
        if manager is not None and hasattr(manager, "_db"):
            try:
                manager._db = None
            except Exception:
                pass

    dummy_specs = (
        ("_TzCacheManager", "_tz_cache", "_TzCacheDummy"),
        ("_CookieCacheManager", "_Cookie_cache", "_CookieCacheDummy"),
        ("_ISINCacheManager", "_isin_cache", "_ISINCacheDummy"),
    )
    for manager_name, cache_attr, dummy_name in dummy_specs:
        manager = getattr(yf_cache, manager_name, None)
        dummy_cls = getattr(yf_cache, dummy_name, None)
        if manager is None or dummy_cls is None:
            continue
        try:
            setattr(manager, cache_attr, dummy_cls())
        except Exception:
            pass


def close_yfinance_runtime_caches() -> None:
    try:
        import yfinance.cache as yf_cache
    except Exception:
        return

    for manager_name in ("_TzDBManager", "_CookieDBManager", "_ISINDBManager"):
        manager = getattr(yf_cache, manager_name, None)
        close_db = getattr(manager, "close_db", None)
        if callable(close_db):
            try:
                close_db()
            except Exception:
                pass
        if manager is not None and hasattr(manager, "_db"):
            try:
                manager._db = None
            except Exception:
                pass
    disable_yfinance_sqlite_caches()
