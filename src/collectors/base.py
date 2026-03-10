"""Base collector with caching, retrying, and rate limiting."""

from __future__ import annotations

import hashlib
import pickle
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from src.utils.config import resolve_project_path
from src.utils.logger import logger
from src.utils.retry import RateLimiter, retry

try:
    import tushare as ts
except ImportError:  # pragma: no cover
    ts = None

_tushare_api_instance: Any = None
_tushare_rate_limiter = RateLimiter(min_interval_seconds=0.35)


class BaseCollector:
    """Common data collector helpers."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, name: Optional[str] = None) -> None:
        self.config = config or {}
        self.name = name or self.__class__.__name__
        storage = self.config.get("storage", {})
        cache_dir = storage.get("cache_dir", "data/cache")
        self.cache_dir = resolve_project_path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl_hours = int(storage.get("cache_ttl_hours", 4))
        self.rate_limiter = RateLimiter(min_interval_seconds=0.5)

    def _tushare_pro(self) -> Any:
        """Return a cached tushare pro_api instance, or None if unavailable."""
        global _tushare_api_instance
        if ts is None:
            return None
        if _tushare_api_instance is not None:
            return _tushare_api_instance
        token = (self.config.get("api_keys") or {}).get("tushare", "")
        if not token or token == "YOUR_TUSHARE_TOKEN":
            return None
        ts.set_token(token)
        _tushare_api_instance = ts.pro_api()
        return _tushare_api_instance

    def _ts_call(self, api_name: str, **kwargs: Any) -> Any:
        """Call a Tushare pro API with rate limiting. Returns DataFrame or None."""
        pro = self._tushare_pro()
        if pro is None:
            return None
        _tushare_rate_limiter.wait()
        method = getattr(pro, api_name, None)
        if not callable(method):
            logger.warning(f"Tushare API not found: {api_name}")
            return None
        return method(**kwargs)

    @staticmethod
    def _to_ts_code(symbol: str) -> str:
        """Convert 6-digit A-share symbol to Tushare ts_code format (e.g. 000001 → 000001.SZ)."""
        symbol = symbol.strip()
        if "." in symbol:
            return symbol
        if len(symbol) == 6 and symbol.isdigit():
            suffix = "SH" if symbol[0] in ("5", "6", "9") else "SZ"
            return f"{symbol}.{suffix}"
        return symbol

    @staticmethod
    def _from_ts_code(ts_code: str) -> str:
        """Convert Tushare ts_code to bare 6-digit symbol (e.g. 000001.SZ → 000001)."""
        return ts_code.split(".")[0] if "." in ts_code else ts_code

    def _cache_path(self, cache_key: str) -> Path:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{self.name.lower()}_{digest}.pkl"

    def _load_cache(self, cache_key: str, ttl_hours: Optional[int] = None) -> Any:
        cache_path = self._cache_path(cache_key)
        if not cache_path.exists():
            return None
        effective_ttl = ttl_hours if ttl_hours is not None else self.cache_ttl_hours
        max_age_seconds = effective_ttl * 3600
        if effective_ttl >= 0 and time.time() - cache_path.stat().st_mtime > max_age_seconds:
            return None
        with cache_path.open("rb") as handle:
            return pickle.load(handle)

    def _save_cache(self, cache_key: str, payload: Any) -> None:
        cache_path = self._cache_path(cache_key)
        with cache_path.open("wb") as handle:
            pickle.dump(payload, handle)

    @retry()
    def _execute_fetcher(self, fetcher: Callable[..., Any], *args, **kwargs) -> Any:
        return fetcher(*args, **kwargs)

    def cached_call(
        self,
        cache_key: str,
        fetcher: Callable[..., Any],
        *args,
        ttl_hours: Optional[int] = None,
        use_cache: bool = True,
        **kwargs,
    ) -> Any:
        """Return cached result when possible, otherwise fetch and cache."""
        if use_cache:
            cached = self._load_cache(cache_key, ttl_hours=ttl_hours)
            if cached is not None:
                logger.info(f"{self.name} cache hit: {cache_key}")
                return cached

        self.rate_limiter.wait()
        result = self._execute_fetcher(fetcher, *args, **kwargs)
        if result is None or getattr(result, "empty", False):
            raise ValueError(f"{self.name} returned empty result for {cache_key}")
        if use_cache:
            self._save_cache(cache_key, result)
        return result
