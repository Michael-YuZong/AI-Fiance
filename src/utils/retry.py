"""Retry and rate limiting helpers."""

from __future__ import annotations

import threading
import time
from functools import wraps
from typing import Callable, Tuple, Type

from .logger import logger


class RateLimiter:
    """Simple process-local rate limiter."""

    def __init__(self, min_interval_seconds: float = 0.0) -> None:
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._last_called = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_called
            remaining = self.min_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
            self._last_called = time.monotonic()


def retry(
    attempts: int = 3,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
) -> Callable:
    """Retry decorated function with exponential backoff."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:  # type: ignore[misc]
                    last_error = exc
                    if attempt >= attempts:
                        raise
                    sleep_seconds = backoff_seconds * (backoff_multiplier ** (attempt - 1))
                    logger.warning(
                        f"Retrying {func.__name__} after error on attempt {attempt}/{attempts}: {exc}"
                    )
                    time.sleep(sleep_seconds)
            if last_error is not None:
                raise last_error
            return None

        return wrapper

    return decorator
