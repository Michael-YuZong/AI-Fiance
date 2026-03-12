from __future__ import annotations

import numpy as np
import pandas as pd

from src.processors.signal_confidence import build_signal_confidence


def _cyclical_history(periods: int = 720) -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=periods)
    base = np.linspace(20, 52, periods)
    wave = 2.8 * np.sin(np.arange(periods) / 18.0) + 1.5 * np.sin(np.arange(periods) / 7.0)
    close = base + wave
    open_ = close * (1 - 0.004)
    high = close * 1.012
    low = close * 0.988
    volume = 1_000_000 + (np.sin(np.arange(periods) / 9.0) + 1.2) * 350_000
    amount = close * volume
    return pd.DataFrame(
        {
            "date": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "amount": amount,
        }
    )


def test_signal_confidence_requires_sufficient_history() -> None:
    history = _cyclical_history(periods=180)
    result = build_signal_confidence(history, asset_type="cn_stock")
    assert result["available"] is False
    assert "历史样本不足" in result["reason"]


def test_signal_confidence_rejects_fallback_history() -> None:
    history = _cyclical_history()
    result = build_signal_confidence(history, asset_type="cn_stock", history_fallback=True)
    assert result["available"] is False
    assert "历史降级快照" in result["reason"]


def test_signal_confidence_builds_same_symbol_statistics() -> None:
    history = _cyclical_history()
    result = build_signal_confidence(
        history,
        asset_type="cn_stock",
        stop_loss_pct="-8%",
        target_pct=0.12,
    )
    assert result["available"] is True
    assert result["sample_count"] >= 12
    assert 0 <= result["win_rate_20d"] <= 1
    assert result["latest_sample_date"] < str(history["date"].iloc[-20].date())
    assert result["confidence_label"] in {"高", "中高", "中", "低"}
