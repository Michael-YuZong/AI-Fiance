"""Tests for local chart rendering."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from src.output.analysis_charts import AnalysisChartRenderer


def _sample_analysis() -> dict:
    history = pd.DataFrame(
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
    benchmark = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [0.95 + i * 0.002 for i in range(120)],
            "high": [0.97 + i * 0.002 for i in range(120)],
            "low": [0.93 + i * 0.002 for i in range(120)],
            "close": [0.95 + i * 0.002 for i in range(120)],
            "volume": [7_000_000 + i * 800 for i in range(120)],
            "amount": [14_000_000 + i * 1_600 for i in range(120)],
        }
    )
    return {
        "symbol": "561380",
        "name": "电网ETF",
        "generated_at": "2026-03-09 08:00:00",
        "rating": {"stars": "⭐⭐⭐", "label": "较强机会"},
        "history": history,
        "benchmark_name": "沪深300ETF",
        "benchmark_history": benchmark,
        "metrics": {"return_5d": 0.034, "return_20d": 0.087},
        "narrative": {
            "phase": {"label": "强势整理"},
            "judgment": {"direction": "中性偏多", "odds": "中"},
            "summary_lines": ["核心逻辑仍在。", "短期更像强势整理。"],
        },
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "technical_raw": {
            "rsi": {"RSI": 61.4},
            "dmi": {"ADX": 29.7},
            "ma_system": {"mas": {"MA20": 1.34, "MA60": 1.22}},
            "fibonacci": {"levels": {"0.500": 1.27, "0.618": 1.31}},
        },
        "dimensions": {
            "technical": {"score": 82, "max_score": 100},
            "fundamental": {"score": 58, "max_score": 100},
            "catalyst": {"score": 61, "max_score": 100},
            "relative_strength": {"score": 67, "max_score": 100},
            "chips": {"score": 44, "max_score": 100},
            "risk": {"score": 53, "max_score": 100},
            "seasonality": {"score": 38, "max_score": 100},
            "macro": {"score": 30, "max_score": 40},
        },
    }


def test_analysis_chart_renderer_outputs_dashboard(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    visuals = renderer.render(_sample_analysis())
    assert "dashboard" in visuals
    assert "windows" in visuals
    assert "indicators" in visuals
    for key in ("dashboard", "windows", "indicators"):
        image = Path(visuals[key])
        assert image.exists()
        assert image.stat().st_size > 0


def test_analysis_chart_renderer_skips_visuals_for_history_fallback(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    analysis["history_fallback_mode"] = True
    visuals = renderer.render(analysis)
    assert visuals == {}


def test_draw_candles_adds_bodies_and_wicks(tmp_path: Path):
    matplotlib = pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    history = _sample_analysis()["history"].tail(8).copy()
    fig, ax = plt.subplots(figsize=(6, 3))
    renderer._draw_candles(ax, history, width=0.7)
    assert len(ax.patches) == len(history)
    assert len(ax.collections) >= 1
    plt.close(fig)


def test_format_date_axis_uses_sparse_date_labels_for_short_window(tmp_path: Path):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    history = _sample_analysis()["history"].tail(22).copy()
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.plot(history["date"], history["close"])
    renderer._format_date_axis(ax, history["date"])
    fig.canvas.draw()
    labels = [tick.get_text() for tick in ax.get_xticklabels() if tick.get_text()]
    assert labels
    assert len(labels) <= 4
    assert all(re.fullmatch(r"\d{2}-\d{2}", label) for label in labels)
    plt.close(fig)


def test_draw_short_window_panel_uses_ma5_and_ma10(tmp_path: Path):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    history = _sample_analysis()["history"].tail(22).copy()
    fig, ax = plt.subplots(figsize=(6, 3))
    renderer._draw_short_window_panel(ax, analysis, history, "近1月均线节奏")
    fig.canvas.draw()

    labels = [tick.get_text() for tick in ax.get_xticklabels() if tick.get_text()]
    legend_labels = [text.get_text() for text in ax.get_legend().get_texts()]

    assert len(labels) <= 4
    assert "收盘线" in legend_labels
    assert "MA5" in legend_labels
    assert "MA10" in legend_labels
    assert "K线" not in ax.get_title()
    plt.close(fig)


def test_build_price_levels_exposes_support_and_resistance(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    analysis["action"] = {
        "stop": "跌破 1.210 重评",
        "target": "先看前高 1.420",
    }
    levels = renderer._build_price_levels(
        analysis,
        analysis["history"].tail(40).copy(),
        price=float(analysis["history"]["close"].iloc[-1]),
        support_low=1.24,
        support_high=1.28,
    )
    labels = [item[0] for item in levels]
    assert "支撑下沿" in labels
    assert "支撑上沿" in labels
    assert any(label in labels for label in ("目标压力", "前高压力", "近端压力"))


def test_visible_price_levels_hides_far_target_from_short_panel(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    levels = [
        ("支撑下沿", 1.420, "support"),
        ("支撑上沿", 1.590, "support"),
        ("近端压力", 1.565, "resistance"),
        ("目标压力", 1.784, "resistance"),
        ("止损参考", 1.400, "stop"),
    ]
    filtered = renderer._visible_price_levels(levels, price=1.544, max_distance_pct=0.08)
    labels = [item[0] for item in filtered]
    assert "近端压力" in labels
    assert "目标压力" not in labels
    assert "止损参考" in labels


def test_extract_price_hint_prefers_price_over_day_count(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    value = renderer._extract_price_hint("先看前高/近 60 日高点 1.442 附近", reference_price=1.335)
    assert value == 1.442


def test_indicator_summary_text_keeps_adx_and_obv_metrics(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    indicators = renderer._indicator_series(_sample_analysis()["history"].tail(120).copy())
    summary = renderer._indicator_summary_text(indicators)
    assert "ADX" in summary
    assert "DMI" in summary
    assert "OBV" in summary


def test_draw_boll_panel_uses_three_item_clean_legend(tmp_path: Path):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    history = _sample_analysis()["history"].tail(22).copy()
    indicators = renderer._trim_indicator_series(renderer._indicator_series(_sample_analysis()["history"].tail(120).copy()), 22)
    fig, ax = plt.subplots(figsize=(6, 3))
    renderer._draw_boll_panel(ax, history, indicators)
    fig.canvas.draw()
    labels = [text.get_text() for text in ax.get_legend().get_texts()]
    assert labels == ["收盘价", "中轨", "波动带"]
    plt.close(fig)


def test_draw_adx_panel_shows_adx_and_dmi_series(tmp_path: Path):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    indicators = renderer._trim_indicator_series(renderer._indicator_series(_sample_analysis()["history"].tail(120).copy()), 22)
    fig, ax = plt.subplots(figsize=(6, 3))
    renderer._draw_adx_panel(ax, indicators)
    fig.canvas.draw()
    labels = [text.get_text() for text in ax.get_legend().get_texts()]
    assert labels == ["ADX", "+DI", "-DI"]
    assert "ADX / DMI" in ax.get_title(loc="left")
    plt.close(fig)


def test_draw_obv_panel_shows_obv_and_ma_series(tmp_path: Path):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    close_series = _sample_analysis()["history"].tail(22).reset_index(drop=True)["close"]
    indicators = renderer._trim_indicator_series(renderer._indicator_series(_sample_analysis()["history"].tail(120).copy()), 22)
    fig, ax = plt.subplots(figsize=(6, 3))
    renderer._draw_obv_panel(ax, indicators, close_series)
    fig.canvas.draw()
    labels = [text.get_text() for text in ax.get_legend().get_texts()]
    assert labels == ["OBV", "OBV MA20"]
    assert "OBV | 今日" in ax.get_title(loc="left")
    plt.close(fig)


def test_market_mode_badge_uses_adx_thresholds(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    assert renderer._market_mode_badge({"adx": pd.Series([31.0])}) == ("趋势市", "bull")
    assert renderer._market_mode_badge({"adx": pd.Series([17.0])}) == ("震荡市", "neutral")
    assert renderer._market_mode_badge({"adx": pd.Series([21.0])}) == ("过渡期", "warn")


def test_recent_divergence_label_detects_bearish_divergence(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    price = pd.Series([10.0, 11.0, 12.0, 13.0, 12.5, 12.3, 13.2, 14.1, 15.0, 14.2])
    signal = pd.Series([30.0, 32.0, 35.0, 40.0, 38.0, 37.0, 36.0, 35.0, 34.0, 33.0])
    assert renderer._recent_divergence_label(price, signal) == ("顶背离", "bear")


def test_recent_divergence_label_detects_bullish_divergence(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    price = pd.Series([15.0, 14.0, 13.0, 12.0, 11.0, 12.0, 11.0, 10.0, 9.0, 10.0])
    signal = pd.Series([20.0, 18.0, 16.0, 14.0, 12.0, 15.0, 16.0, 17.0, 18.0, 19.0])
    assert renderer._recent_divergence_label(price, signal) == ("底背离", "bull")


def test_obv_badges_mark_confirmation_and_distribution(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    indicators = {
        "adx": pd.Series([27.0, 28.0, 29.0, 30.0, 31.0]),
        "obv": pd.Series([120.0, 118.0, 116.0, 110.0, 105.0]),
        "obv_ma": pd.Series([100.0, 101.0, 102.0, 103.0, 106.0]),
    }
    close_series = pd.Series([10.0, 10.4, 10.8, 11.2, 11.6])
    labels = [item[0] for item in renderer._obv_badges(indicators, close_series)]
    assert "趋势市" in labels
    assert "量能未跟" in labels
    assert "派发" in labels


def test_boll_badges_mark_squeeze_and_mid_reversion(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    history = pd.DataFrame({"close": [10.0, 10.1, 10.0, 10.05, 10.02]})
    indicators = {
        "boll_upper": pd.Series([11.6, 11.2, 10.9, 10.6, 10.4]),
        "boll_lower": pd.Series([8.4, 8.8, 9.1, 9.4, 9.6]),
        "boll_mid": pd.Series([10.0, 10.0, 10.0, 10.0, 10.0]),
    }
    labels = [item[0] for item in renderer._boll_badges(history, indicators)]
    assert "收口" in labels
    assert "中轨回归" in labels


def test_macd_badges_prefer_shared_divergence_signal(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    indicators = {
        "adx": pd.Series([30.0, 31.0, 32.0, 33.0, 34.0]),
        "macd_dif": pd.Series([0.20, 0.24, 0.28, 0.30, 0.26]),
        "macd_dea": pd.Series([0.18, 0.22, 0.25, 0.29, 0.28]),
        "macd_hist": pd.Series([0.02, 0.02, 0.03, 0.01, -0.02]),
    }
    close_series = pd.Series([10.0, 10.4, 10.8, 11.2, 11.5])
    divergence = {"signal": "bearish", "kind": "顶背离", "indicators": ["MACD", "RSI"]}
    labels = [item[0] for item in renderer._macd_badges(indicators, close_series, divergence)]
    assert "顶背离" in labels


def test_divergence_badge_for_indicator_requires_matching_indicator(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    divergence = {"signal": "bullish", "kind": "底背离", "indicators": ["RSI"]}
    assert renderer._divergence_badge_for_indicator("MACD", divergence) is None
    assert renderer._divergence_badge_for_indicator("RSI", divergence) == ("底背离", "bull")
