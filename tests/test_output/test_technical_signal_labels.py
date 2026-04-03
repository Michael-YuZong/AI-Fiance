from __future__ import annotations

import pandas as pd

from src.output.technical_signal_labels import (
    append_technical_trigger_text,
    build_indicator_badge_map,
    build_technical_signal_context,
    compact_technical_signal_text,
    compact_technical_trigger_text,
)


def _sample_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [1.0 + i * 0.004 for i in range(120)],
            "high": [1.02 + i * 0.004 for i in range(120)],
            "low": [0.98 + i * 0.004 for i in range(120)],
            "close": [1.0 + i * 0.004 for i in range(120)],
            "volume": [8_000_000 + i * 1_000 for i in range(120)],
            "amount": [16_000_000 + i * 2_000 for i in range(120)],
        }
    )


def test_build_technical_signal_context_returns_badge_inputs() -> None:
    context = build_technical_signal_context(_sample_history())
    assert "history" in context
    assert "indicators" in context
    assert "divergence" in context
    badge_map = build_indicator_badge_map(context)
    assert set(badge_map) == {"macd", "kdj", "rsi", "boll", "adx", "obv"}
    assert badge_map["macd"]


def test_compact_technical_signal_text_uses_shared_badges() -> None:
    text = compact_technical_signal_text(_sample_history())
    assert text.startswith("当前图形标签：")
    assert any(token in text for token in ("趋势市", "震荡市", "过渡期"))


def test_compact_technical_trigger_text_returns_execution_hint() -> None:
    text = compact_technical_trigger_text(_sample_history())
    assert text.startswith("技术上先看")
    assert any(token in text for token in ("方向重新拉开", "震荡区间", "量能", "主导"))


def test_append_technical_trigger_text_appends_hint_without_dropping_base_text() -> None:
    text = append_technical_trigger_text("先等回踩关键支撑不破。", _sample_history())
    assert "先等回踩关键支撑不破" in text
    assert "技术上先看" in text
    assert "技术上先看先" not in text
