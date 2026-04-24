"""Shared concurrency helpers for bounded runtime loaders."""

from __future__ import annotations

import threading
from typing import Any, Callable, TypeVar


_MISSING = object()
T = TypeVar("T")


def run_with_timeout(
    loader: Callable[[], T],
    *,
    timeout_seconds: float,
    fallback: Any = _MISSING,
    timeout_exc: BaseException | None = None,
    thread_name: str = "timed_loader",
) -> T | Any:
    """Run a loader in a daemon thread and return/raise on timeout.

    This is used for network-bound runtime fetchers where we prefer a bounded
    foreground wait and are willing to let the background attempt die with the
    process rather than block command exit.
    """

    timeout = float(timeout_seconds or 0)
    if timeout <= 0:
        return loader()

    state: dict[str, Any] = {"value": fallback, "error": None}

    def _runner() -> None:
        try:
            state["value"] = loader()
        except BaseException as exc:  # pragma: no cover - surfaced after join
            state["error"] = exc

    worker = threading.Thread(target=_runner, name=thread_name, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        if timeout_exc is not None:
            raise timeout_exc
        if fallback is _MISSING:
            raise TimeoutError(f"{thread_name} timeout after {timeout:.1f}s")
        return fallback
    if state["error"] is not None:
        raise state["error"]
    return state["value"]
