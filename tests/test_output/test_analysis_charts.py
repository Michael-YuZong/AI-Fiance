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
        assert image.suffix == ".svg"


def test_analysis_chart_renderer_writes_theme_variant_svgs(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path), theme="institutional", render_theme_variants=True)
    visuals = renderer.render(_sample_analysis())
    dashboard = Path(visuals["dashboard"])
    assert "<svg" in dashboard.read_text(encoding="utf-8")
    for theme_name in ("terminal", "abyss-gold", "institutional", "clinical", "erdtree", "neo-brutal"):
        variant = tmp_path / f"{dashboard.stem}.theme-{theme_name}{dashboard.suffix}"
        assert variant.exists()
        assert variant.stat().st_size > 0


def test_terminal_theme_variant_uses_terminal_header_palette(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path), theme="institutional", render_theme_variants=True)
    visuals = renderer.render(_sample_analysis())
    dashboard = Path(visuals["dashboard"])
    terminal_variant = tmp_path / f"{dashboard.stem}.theme-terminal{dashboard.suffix}"
    svg = terminal_variant.read_text(encoding="utf-8")

    assert "#202d3d" in svg
    assert "#edf3fb" not in svg


def test_analysis_chart_renderer_skips_theme_variants_by_default(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path), theme="institutional")
    visuals = renderer.render(_sample_analysis())
    dashboard = Path(visuals["dashboard"])
    assert dashboard.exists()
    for theme_name in ("terminal", "abyss-gold", "institutional", "clinical", "erdtree", "neo-brutal"):
        variant = tmp_path / f"{dashboard.stem}.theme-{theme_name}{dashboard.suffix}"
        assert not variant.exists()


def test_analysis_chart_renderer_defaults_to_institutional_theme(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AI_FINANCE_REPORT_THEME", raising=False)
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    assert renderer.theme == "institutional"
    assert renderer._PANEL == "#05080c"
    assert renderer._PAPER == "#000000"


def test_institutional_theme_uses_dark_gold_header_cards(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path), theme="institutional")
    assert renderer._header_panel_fill() == "#15120e"
    assert renderer._header_panel_alpha() == pytest.approx(0.96)
    assert renderer._header_panel_edge_alpha() == pytest.approx(0.88)
    assert renderer._header_shadow_alpha() == pytest.approx(0.12)
    assert renderer._header_title_color() == "#ffe082"
    score_fill = renderer._score_card_fill()
    assert isinstance(score_fill, tuple)
    assert len(score_fill) == 4
    assert score_fill[-1] == pytest.approx(0.08)


def test_apply_theme_updates_variant_specific_header_palette(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path), theme="institutional")
    renderer._apply_theme("terminal")
    assert renderer.theme == "terminal"
    assert renderer._header_panel_fill() == "#202d3d"
    assert renderer._header_title_color() == "#eff6ff"


def test_clinical_theme_header_palette_stays_distinct_from_institutional(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path), theme="clinical")
    assert renderer._header_panel_fill() == "#ffffff"
    assert renderer._header_title_color() == "#1d1d1f"


def test_analysis_chart_renderer_uses_report_theme_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "abyss-gold")
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    assert renderer.theme == "abyss-gold"
    assert renderer._PANEL == "#181512"
    assert renderer._LAST_PRICE_BOX == "#2b2218"


def test_analysis_chart_renderer_supports_light_report_theme_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "clinical")
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    assert renderer.theme == "clinical"
    assert renderer._PANEL == "#f5f8fd"
    assert renderer._PAPER == "#fbfbfd"


def test_analysis_chart_renderer_returns_snapshot_card_for_history_fallback(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    analysis["history_fallback_mode"] = True
    visuals = renderer.render(analysis)
    assert visuals["mode"] == "snapshot_fallback"
    assert "dashboard" in visuals
    assert "note" in visuals
    dashboard = Path(visuals["dashboard"])
    assert dashboard.exists()
    assert dashboard.suffix == ".svg"
    svg = dashboard.read_text(encoding="utf-8")
    assert "当前快照 / 关键位" in svg
    assert "降级说明 / 当前应读什么" in svg
    assert "近3月价格结构 / K线" not in svg
    assert "相对强弱 / 归一化走势" not in svg


def test_header_context_lines_wraps_long_summary_into_two_lines(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    analysis["narrative"]["summary_lines"] = [
        "总体来看，阳光电源的核心逻辑在于背景宏观主导背景下的电气设备暴露仍有配置价值，同时短线还需要等价格和资金重新共振确认。"
    ]
    lines = renderer._header_context_lines(analysis)
    assert 1 <= len(lines) <= 2
    assert all(line.strip() for line in lines)
    assert sum("行情" in line for line in lines) <= 1


def test_dashboard_header_uses_single_summary_card_layout(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    visuals = renderer.render(_sample_analysis())
    dashboard = Path(visuals["dashboard"])
    svg = dashboard.read_text(encoding="utf-8")
    assert "研判概览" not in svg
    assert "当前判断" not in svg
    assert "机会评级" not in svg
    assert "信号等级" not in svg
    assert "阶段" in svg
    assert "方向" in svg
    assert "电网设备" in svg
    assert "较强机会" in svg


def test_dashboard_header_renders_signal_badges_inside_header(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    renderer._header_market_snapshot = lambda analysis: {
        "price_value": "28.580",
        "change_value": "-7.09%",
        "change_color": "#18c48f",
        "metrics": [("MACD", "0.496"), ("KDJ", "29.6"), ("RSI(14)", "47.5"), ("ADX", "29.7")],
        "badges": [("修复中", "bull"), ("空头主导", "bear")],
    }
    visuals = renderer.render(_sample_analysis())
    svg = Path(visuals["dashboard"]).read_text(encoding="utf-8")

    assert "修复中" in svg
    assert "空头主导" in svg


def test_header_signal_rows_keep_summary_text_inside_compact_right_card(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    rows = renderer._header_signal_rows(
        phase_value="震荡整理中的修复阶段",
        direction_value="中性偏多但仍待确认",
        action_value="等待右侧确认，不追高",
        theme_value="能源 / 资源 / 地缘 / 周期",
    )

    assert len(rows) == 3
    assert rows[0][0] > 0.0
    assert rows[-1][0] > 0.0
    assert rows[0][2].count("/") >= 1
    assert rows[1][2].endswith("...") or len(rows[1][2]) <= 10
    assert rows[2][2].endswith("...") or len(rows[2][2]) <= 10


def test_dashboard_header_splits_name_and_symbol_hierarchy(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    visuals = renderer.render(_sample_analysis())
    dashboard = Path(visuals["dashboard"])
    svg = dashboard.read_text(encoding="utf-8")

    assert "电网ETF" in svg
    assert "561380" in svg
    assert "电网ETF (561380)" not in svg


def test_dashboard_header_prefers_theme_playbook_label_over_day_theme(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    analysis["day_theme"] = {"label": "背景宏观主导"}
    analysis["theme_playbook"] = {
        "label": "光伏主链",
        "hard_sector_label": "电力设备 / 新能源设备",
        "playbook_level": "theme",
    }
    cards = renderer._header_summary_cards(analysis)
    theme_card = next(item for item in cards if item["label"] == "主线")

    assert theme_card["value"] == "光伏主链"
    assert "光伏主链" in renderer._headline_note(analysis)
    assert "背景宏观主导" not in renderer._headline_note(analysis)
    assert "主线: 光伏主链" in renderer._footer_text(analysis)


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


def test_visible_price_levels_keeps_nearest_resistance_when_none_is_within_range(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    levels = [
        ("止损参考", 1.558, "stop"),
        ("反压下沿", 1.978, "resistance"),
        ("反压上沿", 2.032, "resistance"),
    ]
    filtered = renderer._visible_price_levels(levels, price=1.694, max_distance_pct=0.10)
    labels = [item[0] for item in filtered]
    assert "止损参考" in labels
    assert "反压下沿" in labels


def test_build_price_levels_relabels_broken_support_zone_as_resistance(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    analysis = _sample_analysis()
    levels = renderer._build_price_levels(
        analysis,
        analysis["history"].tail(40).copy(),
        price=1.694,
        support_low=1.978,
        support_high=2.032,
    )
    labels = [item[0] for item in levels]
    assert "反压下沿" in labels
    assert "反压上沿" in labels
    assert "支撑下沿" not in labels


def test_resolve_level_label_y_spreads_dense_price_labels(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    occupied = [29.923, 29.775]
    label_y = renderer._resolve_level_label_y(
        29.890,
        occupied,
        min_gap=0.12,
        y_min=27.0,
        y_max=36.5,
        prefer_direction=-1,
    )
    assert all(abs(label_y - used) >= 0.12 for used in occupied[:-1])


def test_apply_price_ylim_adds_extra_headroom_for_badges(tmp_path: Path):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    history = _sample_analysis()["history"].tail(40).copy()
    history["high"] = [30.4 + idx * 0.01 for idx in range(len(history))]
    history["low"] = [27.8 + idx * 0.01 for idx in range(len(history))]
    levels = [("支撑上沿", 29.923, "support"), ("止损参考", 27.756, "stop"), ("目标压力", 36.490, "resistance")]
    fig, ax = plt.subplots(figsize=(6, 4))
    renderer._apply_price_ylim(ax, history, levels)
    lower, upper = ax.get_ylim()
    assert upper >= 36.490 + (36.490 - 27.756) * 0.10
    assert lower <= 27.756 - (36.490 - 27.756) * 0.10
    plt.close(fig)


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


def test_dashboard_svg_header_uses_explanatory_chip_labels(tmp_path: Path):
    renderer = AnalysisChartRenderer(output_dir=str(tmp_path))
    visuals = renderer.render(_sample_analysis())
    dashboard_svg = Path(visuals["dashboard"]).read_text(encoding="utf-8")
    assert "当前判断" not in dashboard_svg
    assert "研判概览" not in dashboard_svg
    assert "机会评级" not in dashboard_svg
    assert "3星" in dashboard_svg
    assert "信号等级" not in dashboard_svg
    assert "较强机会" in dashboard_svg
    assert "MACD" in dashboard_svg
    assert "KDJ" in dashboard_svg
    assert "RSI(14)" in dashboard_svg
    assert "ADX" in dashboard_svg
    assert "阶段" in dashboard_svg
    assert "强势整理" in dashboard_svg
    assert "观察为主" in dashboard_svg
    assert "电网设备" in dashboard_svg
    assert "当前价" in dashboard_svg
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
