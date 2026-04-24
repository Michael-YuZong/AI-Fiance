from __future__ import annotations

import threading
import time

import pytest

from src.utils.concurrency import run_with_timeout


def test_run_with_timeout_returns_quickly_on_timeout() -> None:
    blocker = threading.Event()
    started = time.monotonic()

    value = run_with_timeout(
        lambda: blocker.wait(30.0),
        timeout_seconds=0.05,
        fallback="fallback",
        thread_name="test_timeout_loader",
    )

    elapsed = time.monotonic() - started
    assert value == "fallback"
    assert elapsed < 0.5


def test_run_with_timeout_raises_custom_timeout_error() -> None:
    blocker = threading.Event()

    with pytest.raises(TimeoutError, match="custom timeout"):
        run_with_timeout(
            lambda: blocker.wait(30.0),
            timeout_seconds=0.05,
            timeout_exc=TimeoutError("custom timeout"),
            thread_name="test_timeout_error",
        )


def test_run_with_timeout_reraises_loader_error() -> None:
    with pytest.raises(RuntimeError, match="boom"):
        run_with_timeout(
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            timeout_seconds=0.05,
            thread_name="test_loader_error",
        )
