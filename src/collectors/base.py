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
