"""Chart rendering for single-asset analysis reports."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd
from matplotlib import font_manager

from src.output.technical_signal_labels import (
    adx_badges as _shared_adx_badges,
    boll_badges as _shared_boll_badges,
    build_technical_signal_context,
    divergence_badge_for_indicator as _shared_divergence_badge_for_indicator,
    kdj_badges as _shared_kdj_badges,
    macd_badges as _shared_macd_badges,
    market_mode_badge as _shared_market_mode_badge,
    obv_badges as _shared_obv_badges,
    recent_divergence_label as _shared_recent_divergence_label,
    rsi_badges as _shared_rsi_badges,
    trim_indicator_series as _shared_trim_indicator_series,
)
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import resolve_project_path

try:  # pragma: no cover - rendering dependency
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    from matplotlib import transforms
    from matplotlib.patches import FancyBboxPatch, Rectangle
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    matplotlib = None
    mdates = None
    transforms = None
    FancyBboxPatch = None
    Rectangle = None
    plt = None


DIMENSION_LABELS = [
    ("technical", "技术面"),
    ("fundamental", "基本面"),
    ("catalyst", "催化面"),
    ("relative_strength", "相对强弱"),
    ("chips", "筹码结构"),
    ("risk", "风险特征"),
    ("seasonality", "季节/日历"),
    ("macro", "宏观敏感度"),
]


class AnalysisChartRenderer:
    """Render chart assets for a single analysis."""

    _UP_BODY = "#de7c5a"
    _UP_EDGE = "#bb6546"
    _DOWN_BODY = "#2f6f8f"
    _DOWN_EDGE = "#1f4f68"
    _WICK = "#7b8794"
    _MA20 = "#e6a35c"
    _MA60 = "#4f8f93"
    _GRID = "#e7dccd"
    _PANEL = "#fbf2e6"
    _PAPER = "#f6ecdf"
    _TEXT = "#1a1a1a"
    _MUTED = "#374151"
    _SOFT_BAR = "#efe3d2"
    _CARD_BG = "#fff8ef"
    _CARD_EDGE = "#ead8c2"
    _VOLUME_UP = "#de7c5a"
    _VOLUME_DOWN = "#5f879c"
    _SUPPORT = "#33a39a"
    _RESISTANCE = "#ef7f63"
    _STOP = "#f59e0b"
    _BOLL_BAND = "#d9e5ef"
    _BOLL_EDGE = "#8ea5b8"
    _ADX = "#4b5563"
    _PLUS_DI = "#1f9d8d"
    _MINUS_DI = "#d97757"
    _OBV = "#315f7d"
    _OBV_MA = "#c99547"

    def __init__(self, output_dir: str = "reports/assets") -> None:
        self.output_dir = resolve_project_path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.enabled = plt is not None
        if self.enabled:
            self._configure_style()

    def render(self, analysis: Mapping[str, Any]) -> Dict[str, str]:
        if not self.enabled:
            return {}

        history = analysis.get("history")
        if not isinstance(history, pd.DataFrame) or history.empty:
            return {}

        symbol = str(analysis.get("symbol", "asset"))
        stamp = str(analysis.get("generated_at", "")).replace(":", "-").replace(" ", "_")
        base = f"{symbol}_{stamp[:19] or 'latest'}"
        dashboard_path = self.output_dir / f"{base}_dashboard.png"
        windows_path = self.output_dir / f"{base}_windows.png"
        indicators_path = self.output_dir / f"{base}_indicators.png"
        if self._is_history_fallback(analysis):
            return {}
        self._render_dashboard(analysis, history.copy(), dashboard_path)
        self._render_windows(analysis, history.copy(), windows_path)
        self._render_indicators(analysis, history.copy(), indicators_path)
        return {
            "dashboard": str(dashboard_path.resolve()),
            "windows": str(windows_path.resolve()),
            "indicators": str(indicators_path.resolve()),
        }

    def _configure_style(self) -> None:
        font_candidates = [
            "Songti SC",
            "STSong",
            "Songti TC",
            "Noto Serif CJK SC",
            "Source Han Serif SC",
            "Times New Roman",
            "Georgia",
            "Palatino",
            "DejaVu Serif",
        ]
        available_fonts = {font.name for font in font_manager.fontManager.ttflist}
        chosen = [font for font in font_candidates if font in available_fonts]
        plt.style.use("default")
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = ["PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", "Arial Unicode MS", "DejaVu Sans"]
        plt.rcParams["font.serif"] = chosen or ["DejaVu Serif"]
        plt.rcParams["axes.unicode_minus"] = False
        plt.rcParams["figure.facecolor"] = self._PAPER
        plt.rcParams["axes.facecolor"] = self._PANEL
        plt.rcParams["savefig.facecolor"] = self._PAPER
        plt.rcParams["axes.edgecolor"] = "#d1d5db"
        plt.rcParams["grid.color"] = self._GRID
        plt.rcParams["grid.alpha"] = 0.24
        plt.rcParams["grid.linewidth"] = 0.6
        plt.rcParams["axes.titleweight"] = "bold"
        plt.rcParams["axes.titlesize"] = 12.5
        plt.rcParams["axes.labelsize"] = 10.5
        plt.rcParams["axes.labelcolor"] = self._MUTED
        plt.rcParams["axes.titlecolor"] = self._TEXT
        plt.rcParams["xtick.color"] = self._MUTED
        plt.rcParams["ytick.color"] = self._MUTED
        plt.rcParams["lines.solid_capstyle"] = "round"
        plt.rcParams["lines.solid_joinstyle"] = "round"
        plt.rcParams["patch.antialiased"] = True
        plt.rcParams["path.simplify"] = True
        plt.rcParams["legend.frameon"] = False

    def _render_dashboard(self, analysis: Mapping[str, Any], history: pd.DataFrame, path: Path) -> None:
        prepared = self._prepare_history(history)
        symbol = str(analysis.get("symbol", "asset"))
        benchmark = analysis.get("benchmark_history")
        benchmark_prepared = self._prepare_history(benchmark.copy()) if isinstance(benchmark, pd.DataFrame) and not benchmark.empty else None
        technical = dict(analysis.get("technical_raw", {}))
        ma = technical.get("ma_system", {}).get("mas", {})
        fib = technical.get("fibonacci", {}).get("levels", {})
        price = float(prepared["close"].iloc[-1])
        ma20 = prepared["close"].rolling(20).mean()
        ma60 = prepared["close"].rolling(60).mean()
        support_low = self._first_positive(float(fib.get("0.500", 0.0)), float(ma.get("MA60", 0.0)), float(prepared["low"].tail(30).min()))
        support_high = self._first_positive(float(fib.get("0.618", 0.0)), float(ma.get("MA20", 0.0)), support_low)

        fig = plt.figure(figsize=(15, 8.9), dpi=170)
        fig.subplots_adjust(top=0.96, bottom=0.08, left=0.06, right=0.94)
        grid = fig.add_gridspec(3, 2, height_ratios=[0.30, 0.98, 0.60], hspace=0.18, wspace=0.22)
        ax_header = fig.add_subplot(grid[0, :])
        gs_price = grid[1, 0].subgridspec(2, 1, height_ratios=[4.2, 1.0], hspace=0.04)
        ax_price = fig.add_subplot(gs_price[0, 0])
        ax_vol = fig.add_subplot(gs_price[1, 0], sharex=ax_price)
        ax_relative = fig.add_subplot(grid[1, 1])
        ax_scores = fig.add_subplot(grid[2, :])

        self._draw_dashboard_header(ax_header, analysis)
        self._draw_price_panel(ax_price, ax_vol, analysis, prepared, ma20, ma60, price, support_low, support_high)
        self._draw_relative_panel(ax_relative, analysis, prepared, benchmark_prepared)
        self._draw_score_panel(ax_scores, analysis)
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _render_windows(self, analysis: Mapping[str, Any], history: pd.DataFrame, path: Path) -> None:
        prepared = self._prepare_history(history)
        if prepared.empty:
            return
        technical = dict(analysis.get("technical_raw", {}))
        ma = technical.get("ma_system", {}).get("mas", {})
        fib = technical.get("fibonacci", {}).get("levels", {})
        support_low = self._first_positive(float(fib.get("0.500", 0.0)), float(ma.get("MA60", 0.0)), float(prepared["low"].tail(30).min()))
        support_high = self._first_positive(float(fib.get("0.618", 0.0)), float(ma.get("MA20", 0.0)), support_low)

        fig = plt.figure(figsize=(14, 7.0), dpi=170)
        fig.subplots_adjust(top=0.95, bottom=0.08, left=0.07, right=0.95, hspace=0.32)
        grid = fig.add_gridspec(2, 1, height_ratios=[1.16, 1.0])
        ax_3m = fig.add_subplot(grid[0, 0])
        ax_1m = fig.add_subplot(grid[1, 0])

        self._draw_window_panel(ax_3m, analysis, prepared.tail(66).copy(), "近3月走势", support_low=support_low, support_high=support_high)
        self._draw_short_window_panel(ax_1m, analysis, prepared.tail(22).copy(), "近1月均线节奏", support_low=support_low, support_high=support_high)

        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _render_indicators(self, analysis: Mapping[str, Any], history: pd.DataFrame, path: Path) -> None:
        prepared = self._prepare_history(history)
        if prepared.empty:
            return

        context = build_technical_signal_context(prepared, calc_window=120, plot_window=22)
        if not context:
            return
        plot_window = context["history"]
        indicators = context["indicators"]
        divergence = context["divergence"]

        fig = plt.figure(figsize=(14, 10.1), dpi=170)
        fig.subplots_adjust(top=0.94, bottom=0.07, left=0.08, right=0.97, hspace=0.30, wspace=0.20)
        grid = fig.add_gridspec(3, 2)
        axes = [fig.add_subplot(grid[i, j]) for i in range(3) for j in range(2)]

        close_series = plot_window["close"].reset_index(drop=True)

        self._draw_macd_panel(axes[0], indicators, close_series, divergence)
        self._draw_kdj_panel(axes[1], indicators)
        self._draw_rsi_panel(axes[2], indicators, close_series, divergence)
        self._draw_boll_panel(axes[3], plot_window, indicators)
        self._draw_adx_panel(axes[4], indicators)
        self._draw_obv_panel(axes[5], indicators, close_series, divergence)

        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _prepare_history(self, history: Optional[pd.DataFrame]) -> pd.DataFrame:
        if history is None or not isinstance(history, pd.DataFrame) or history.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
        frame = history.copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.dropna(subset=["date"]).sort_values("date")
        for column in ("open", "high", "low", "close", "volume", "amount"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return frame.tail(180).reset_index(drop=True)

    def _draw_price_panel(
        self,
        ax: Any,
        ax_vol: Any,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        ma20: pd.Series,
        ma60: pd.Series,
        price: float,
        support_low: float,
        support_high: float,
    ) -> None:
        panel_history = history.tail(72).copy()
        panel_ma20 = ma20.tail(len(panel_history))
        panel_ma60 = ma60.tail(len(panel_history))
        level_guides = self._visible_price_levels(
            self._build_price_levels(analysis, panel_history, price=price, support_low=support_low, support_high=support_high),
            price=price,
            max_distance_pct=0.10,
        )
        volume = panel_history.get("volume")
        self._style_axis(ax)
        self._style_volume_axis(ax_vol)
        self._draw_candles(ax, panel_history, width=0.62)
        if isinstance(volume, pd.Series) and volume.notna().any():
            self._draw_volume_panel(ax_vol, panel_history)
        ax.plot(panel_history["date"], panel_ma20, color=self._MA20, linewidth=1.85, alpha=0.98, label="MA20", zorder=4)
        ax.plot(panel_history["date"], panel_ma60, color=self._MA60, linewidth=1.85, alpha=0.98, label="MA60", zorder=4)
        if support_low > 0 and support_high > 0 and max(support_low, support_high) >= panel_history["low"].min() * 0.95:
            lower = min(support_low, support_high)
            upper = max(support_low, support_high)
            ax.axhspan(lower, upper, color="#dff3ec", alpha=0.20, zorder=1)
        self._draw_price_levels(ax, panel_history, level_guides)
        self._apply_price_ylim(ax, panel_history, level_guides)
        ax.scatter(panel_history["date"].iloc[-1], price, color="#9f2f1f", s=30, zorder=6)
        ax.annotate(
            f"{price:.3f}",
            xy=(panel_history["date"].iloc[-1], price),
            xytext=(10, 8),
            textcoords="offset points",
            fontsize=8.7,
            color="#8b2a1a",
            bbox={"boxstyle": "round,pad=0.24,rounding_size=0.12", "fc": "#fff7ed", "ec": "#fed7aa"},
        )
        ax.set_title("近3月价格结构 / K线", loc="left", fontsize=12.5, color=self._TEXT, pad=10)
        ax.legend(loc="upper left", ncol=2, fontsize=8.0)
        ax.tick_params(axis="x", which="both", labelbottom=False)
        self._format_date_axis(ax_vol, panel_history["date"])
        ax.set_ylabel("价格")
        ax_vol.set_ylabel("")

    def _draw_volume_panel(self, ax_vol: Any, history: pd.DataFrame) -> None:
        if history.empty or "volume" not in history.columns:
            return
        volume = pd.to_numeric(history["volume"], errors="coerce").fillna(0.0)
        if volume.max() <= 0:
            return
        dates = pd.to_datetime(history["date"])
        colors = [self._VOLUME_UP if close >= open_ else self._VOLUME_DOWN for open_, close in zip(history["open"], history["close"])]
        ax_vol.bar(dates, volume, width=0.76, color=colors, alpha=0.86, align="center")
        ax_vol.set_ylim(0, volume.max() * 1.18)
        ax_vol.set_yticks([])
        ax_vol.grid(False)
        ax_vol.spines["top"].set_visible(False)
        ax_vol.spines["right"].set_visible(False)
        ax_vol.spines["left"].set_visible(False)
        ax_vol.spines["bottom"].set_color("#d1d5db")

    def _draw_relative_panel(
        self,
        ax: Any,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        benchmark_history: Optional[pd.DataFrame],
    ) -> None:
        base = history.tail(120).copy()
        base["norm"] = base["close"] / float(base["close"].iloc[0]) * 100
        self._style_axis(ax)
        ax.plot(base["date"], base["norm"], color="#3f46e8", linewidth=2.65, alpha=0.97, label=str(analysis.get("symbol", "标的")))
        if benchmark_history is not None and not benchmark_history.empty:
            bench = benchmark_history.tail(120).copy()
            merged = pd.merge(base[["date", "norm"]], bench[["date", "close"]], on="date", how="inner")
            if not merged.empty:
                merged["bench_norm"] = merged["close"] / float(merged["close"].iloc[0]) * 100
                ax.plot(merged["date"], merged["bench_norm"], color="#97a6bf", linewidth=1.7, linestyle=(0, (4, 2)), alpha=0.80, label=str(analysis.get("benchmark_name", "基准")))
        ax.axhline(100, color="#cbd5e1", linewidth=1)
        ax.fill_between(base["date"], 100, base["norm"], where=base["norm"] >= 100, color="#ede9fe", alpha=0.12)
        ax.fill_between(base["date"], 100, base["norm"], where=base["norm"] < 100, color="#e0f2fe", alpha=0.08)
        ax.set_title("相对强弱 / 归一化走势", loc="left", fontsize=12.5, color=self._TEXT, pad=10)
        self._format_date_axis(ax, base["date"])
        ax.set_ylabel("")

    def _draw_score_panel(self, ax: Any, analysis: Mapping[str, Any]) -> None:
        ax.set_facecolor(self._PANEL)
        ax.set_axis_off()
        ax.set_title("八维评分", loc="left", fontsize=12.0, color=self._TEXT, pad=8)

        cards = []
        for key, label in DIMENSION_LABELS:
            dimension = analysis.get("dimensions", {}).get(key, {})
            score = dimension.get("score")
            max_score = dimension.get("max_score", 100) or 100
            normalized = 0.0 if score is None else float(score) / float(max_score) * 100
            cards.append(
                {
                    "label": label,
                    "normalized": normalized,
                    "raw": "缺失" if score is None else f"{score}/{max_score}",
                    "color": self._score_color(normalized),
                }
            )

        cols = 4
        rows = 2
        left_margin = 0.02
        right_margin = 0.02
        top_margin = 0.10
        bottom_margin = 0.06
        h_gap = 0.022
        v_gap = 0.10
        card_w = (1 - left_margin - right_margin - h_gap * (cols - 1)) / cols
        card_h = (1 - top_margin - bottom_margin - v_gap * (rows - 1)) / rows

        for idx, card in enumerate(cards):
            row = idx // cols
            col = idx % cols
            x0 = left_margin + col * (card_w + h_gap)
            y0 = 1 - top_margin - (row + 1) * card_h - row * v_gap

            rect = Rectangle(
                (x0, y0),
                card_w,
                card_h,
                transform=ax.transAxes,
                facecolor=self._CARD_BG,
                edgecolor=self._CARD_EDGE,
                linewidth=1.0,
                zorder=1,
            )
            ax.add_patch(rect)

            ax.text(
                x0 + 0.04 * card_w,
                y0 + 0.74 * card_h,
                card["label"],
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=8.6,
                color=self._MUTED,
                zorder=3,
            )
            ax.text(
                x0 + 0.04 * card_w,
                y0 + 0.46 * card_h,
                card["raw"],
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=10.4,
                fontweight="bold",
                color=self._TEXT,
                zorder=3,
            )

            track_x = x0 + 0.04 * card_w
            track_y = y0 + 0.16 * card_h
            track_w = 0.92 * card_w
            track_h = 0.14 * card_h

            track = Rectangle(
                (track_x, track_y),
                track_w,
                track_h,
                transform=ax.transAxes,
                facecolor=self._SOFT_BAR,
                edgecolor="none",
                zorder=1,
            )
            fill = Rectangle(
                (track_x, track_y),
                track_w * max(0.0, min(card["normalized"], 100.0)) / 100.0,
                track_h,
                transform=ax.transAxes,
                facecolor=card["color"],
                edgecolor="none",
                zorder=2,
            )
            ax.add_patch(track)
            ax.add_patch(fill)

            ax.text(
                track_x + track_w,
                track_y + track_h + 0.05 * card_h,
                f"{card['normalized']:.0f}",
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=7.6,
                color=self._MUTED,
                zorder=3,
            )

    def _draw_dashboard_header(self, ax: Any, analysis: Mapping[str, Any]) -> None:
        ax.set_axis_off()
        if FancyBboxPatch is not None:
            bg = FancyBboxPatch(
                (0.0, 0.02),
                1.0,
                0.92,
                boxstyle="round,pad=0.012,rounding_size=0.03",
                transform=ax.transAxes,
                facecolor="#fbf4ea",
                edgecolor="#e7d7c3",
                linewidth=0.85,
                zorder=0,
            )
            ax.add_patch(bg)

        title = f"{analysis.get('name', analysis.get('symbol', 'asset'))} ({analysis.get('symbol', 'asset')})"
        narrative = analysis.get("narrative", {})
        chips_top = [
            self._rating_badge(analysis),
            str(analysis.get("rating", {}).get("label", "未评级")),
            str(narrative.get("phase", {}).get("label", "状态未识别")),
        ]
        chips_bottom = self._headline_note(analysis).split(" | ")

        ax.text(0.02, 0.72, title, transform=ax.transAxes, ha="left", va="center", fontsize=12.8, fontweight="bold", color=self._TEXT)
        self._draw_chip_row(ax, 0.02, 0.39, chips_top, fill="#fffaf3", edge="#e7d7c3", text_color=self._MUTED)
        self._draw_chip_row(ax, 0.02, 0.13, chips_bottom, fill="#f8efe3", edge="#ead8c2", text_color=self._MUTED)

    def _draw_chip_row(
        self,
        ax: Any,
        x0: float,
        y0: float,
        texts: list[str],
        *,
        fill: str,
        edge: str,
        text_color: str,
    ) -> None:
        cursor = x0
        for text in texts:
            clean = str(text).strip()
            if not clean:
                continue
            est_w = min(max(0.056 + 0.0086 * len(clean), 0.082), 0.255)
            if FancyBboxPatch is not None:
                chip = FancyBboxPatch(
                    (cursor, y0),
                    est_w,
                    0.175,
                    boxstyle="round,pad=0.012,rounding_size=0.06",
                    transform=ax.transAxes,
                    facecolor=fill,
                    edgecolor=edge,
                    linewidth=0.72,
                    zorder=1,
                )
                ax.add_patch(chip)
            ax.text(cursor + 0.013, y0 + 0.0875, clean, transform=ax.transAxes, ha="left", va="center", fontsize=7.35, color=text_color, zorder=2)
            cursor += est_w + 0.015
            if cursor > 0.92:
                break

    def _draw_window_panel(
        self,
        ax: Any,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        title: str,
        *,
        support_low: float = 0.0,
        support_high: float = 0.0,
    ) -> None:
        if history.empty:
            return
        self._style_axis(ax)
        close = history["close"]
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        level_guides = self._visible_price_levels(
            self._build_price_levels(
            analysis,
            history,
            price=float(close.iloc[-1]),
            support_low=support_low,
            support_high=support_high,
            ),
            price=float(close.iloc[-1]),
            max_distance_pct=0.10,
        )
        self._draw_candles(ax, history, width=0.70)
        self._draw_price_levels(ax, history, level_guides)
        self._apply_price_ylim(ax, history, level_guides)
        ax.plot(history["date"], ma10, color="#e18b5a", linewidth=1.75, alpha=0.96, label="MA10", zorder=4)
        ax.plot(history["date"], ma20, color="#56898b", linewidth=1.75, alpha=0.96, label="MA20", zorder=4)
        ax.set_title(f"{title} / K线", loc="left", fontsize=12.6, color=self._TEXT, pad=10)
        ax.legend(loc="upper left", frameon=False, fontsize=7.8, ncol=3)
        self._format_date_axis(ax, history["date"])
        ax.set_ylabel("价格")

    def _draw_short_window_panel(
        self,
        ax: Any,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        title: str,
        *,
        support_low: float = 0.0,
        support_high: float = 0.0,
    ) -> None:
        if history.empty:
            return
        self._style_axis(ax)
        close = history["close"]
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        level_guides = self._visible_price_levels(
            self._build_price_levels(
            analysis,
            history,
            price=float(close.iloc[-1]),
            support_low=support_low,
            support_high=support_high,
            ),
            price=float(close.iloc[-1]),
            max_distance_pct=0.08,
        )
        ax.plot(history["date"], close, color=self._DOWN_EDGE, linewidth=2.2, alpha=0.97, label="收盘线", zorder=3)
        ax.plot(history["date"], ma5, color="#d97757", linewidth=1.75, alpha=0.96, label="MA5", zorder=4)
        ax.plot(history["date"], ma10, color="#5f8f8c", linewidth=1.75, alpha=0.96, label="MA10", zorder=4)
        ax.fill_between(history["date"], ma5, ma10, color="#fdebd3", alpha=0.18, zorder=2)
        self._draw_price_levels(ax, history, level_guides)
        self._apply_price_ylim(ax, history, level_guides)
        latest_close = float(close.iloc[-1])
        ax.scatter(history["date"].iloc[-1], latest_close, color="#9f2f1f", s=28, zorder=5)
        ax.annotate(
            f"{latest_close:.3f}",
            xy=(history["date"].iloc[-1], latest_close),
            xytext=(8, 8),
            textcoords="offset points",
            fontsize=8.3,
            color="#8b2a1a",
            bbox={"boxstyle": "round,pad=0.25,rounding_size=0.14", "fc": "#fce7dc", "ec": "#f3c7b4"},
        )
        ax.set_title(f"{title} / 收盘线 + 均线", loc="left", fontsize=12.3, color=self._TEXT, pad=10)
        ax.legend(loc="upper left", frameon=False, fontsize=7.8, ncol=3)
        self._format_date_axis(ax, history["date"])
        ax.set_ylabel("价格")

    def _indicator_series(self, history: pd.DataFrame) -> Dict[str, Any]:
        normalized = normalize_ohlcv_frame(history)
        return TechnicalAnalyzer(normalized).indicator_series()

    def _trim_indicator_series(self, indicators: Mapping[str, Any], window: int) -> Dict[str, Any]:
        return _shared_trim_indicator_series(indicators, window)

    def _draw_macd_panel(self, ax: Any, indicators: Mapping[str, Any], close_series: pd.Series, divergence: Mapping[str, Any] | None = None) -> None:
        self._style_axis(ax)
        ax.bar(indicators["date"], indicators["macd_hist"], color=["#9fd4c7" if x >= 0 else "#e8b2a8" for x in indicators["macd_hist"]], alpha=0.90, label="柱体")
        ax.plot(indicators["date"], indicators["macd_dif"], color="#4f78d0", linewidth=1.5, label="DIF")
        ax.plot(indicators["date"], indicators["macd_dea"], color="#dc8751", linewidth=1.5, label="DEA")
        self._mark_crosses(ax, indicators["date"], indicators["macd_dif"], indicators["macd_dea"])
        ax.axhline(0, color="#94a3b8", linewidth=1)
        ax.set_title(
            f"MACD | DIF {float(indicators['macd_dif'].iloc[-1]):.3f}  DEA {float(indicators['macd_dea'].iloc[-1]):.3f}  HIST {float(indicators['macd_hist'].iloc[-1]):.3f}",
            loc="left",
            fontsize=11.6,
            pad=10,
            fontweight="semibold",
        )
        ax.legend(loc="upper left", fontsize=6.3, ncol=3, columnspacing=0.85, handletextpad=0.40)
        self._draw_signal_badges(ax, self._macd_badges(indicators, close_series, divergence))
        self._format_date_axis(ax, indicators["date"])

    def _draw_kdj_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["kdj_k"], color="#5073c6", linewidth=1.35, label="K")
        ax.plot(indicators["date"], indicators["kdj_d"], color="#dd8a52", linewidth=1.35, label="D")
        ax.plot(indicators["date"], indicators["kdj_j"], color="#8a63d2", linewidth=1.15, alpha=0.88, label="J")
        self._mark_crosses(ax, indicators["date"], indicators["kdj_k"], indicators["kdj_d"])
        ax.axhline(80, color="#f59e0b", linewidth=1, linestyle="--")
        ax.axhline(20, color="#10b981", linewidth=1, linestyle="--")
        ax.set_title(
            f"KDJ | K {float(indicators['kdj_k'].iloc[-1]):.1f}  D {float(indicators['kdj_d'].iloc[-1]):.1f}  J {float(indicators['kdj_j'].iloc[-1]):.1f}",
            loc="left",
            fontsize=11.6,
            pad=10,
            fontweight="semibold",
        )
        ax.legend(loc="upper left", fontsize=6.3, ncol=3, columnspacing=0.85, handletextpad=0.40)
        self._draw_signal_badges(ax, self._kdj_badges(indicators))
        self._format_date_axis(ax, indicators["date"])

    def _draw_rsi_panel(self, ax: Any, indicators: Mapping[str, Any], close_series: pd.Series, divergence: Mapping[str, Any] | None = None) -> None:
        self._style_axis(ax)
        ax.fill_between(indicators["date"], 30, 70, color="#f1e7d8", alpha=0.74)
        ax.plot(indicators["date"], indicators["rsi"], color="#d46a6a", linewidth=1.65)
        ax.axhline(70, color="#f59e0b", linewidth=1, linestyle="--")
        ax.axhline(30, color="#10b981", linewidth=1, linestyle="--")
        ax.set_ylim(0, 100)
        ax.set_title(f"RSI | 今日 {float(indicators['rsi'].iloc[-1]):.1f}", loc="left", fontsize=11.6, pad=10, fontweight="semibold")
        self._draw_signal_badges(ax, self._rsi_badges(indicators, close_series, divergence))
        self._format_date_axis(ax, indicators["date"])

    def _draw_boll_panel(self, ax: Any, history: pd.DataFrame, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.fill_between(
            indicators["date"],
            indicators["boll_lower"],
            indicators["boll_upper"],
            color=self._BOLL_BAND,
            alpha=0.18,
            label="波动带",
        )
        ax.plot(indicators["date"], indicators["boll_upper"], color="#cf8b86", linewidth=0.95, alpha=0.62, linestyle=(0, (4, 2)))
        ax.plot(indicators["date"], indicators["boll_lower"], color="#80aea1", linewidth=0.95, alpha=0.62, linestyle=(0, (4, 2)))
        ax.plot(indicators["date"], indicators["boll_mid"], color="#c99547", linewidth=1.55, label="中轨")
        ax.plot(indicators["date"], history["close"], color=self._DOWN_EDGE, linewidth=2.2, label="收盘价")
        self._apply_price_ylim(ax, history, [])
        latest_close = float(history["close"].iloc[-1])
        ax.scatter(indicators["date"].iloc[-1], latest_close, color="#9f2f1f", s=20, zorder=5)
        ax.set_title(
            f"BOLL | 收 {float(history['close'].iloc[-1]):.3f}  中 {float(indicators['boll_mid'].iloc[-1]):.3f}",
            loc="left",
            fontsize=11.6,
            pad=10,
            fontweight="semibold",
        )
        handles, labels = ax.get_legend_handles_labels()
        order = []
        for preferred in ("收盘价", "中轨", "波动带"):
            if preferred in labels:
                order.append(labels.index(preferred))
        ax.legend([handles[i] for i in order], [labels[i] for i in order], loc="upper left", fontsize=6.3, ncol=3, columnspacing=0.85, handletextpad=0.40)
        self._draw_signal_badges(ax, self._boll_badges(history, indicators))
        self._format_date_axis(ax, indicators["date"])

    def _draw_adx_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["adx"], color=self._ADX, linewidth=1.65, label="ADX")
        ax.plot(indicators["date"], indicators["plus_di"], color=self._PLUS_DI, linewidth=1.45, label="+DI")
        ax.plot(indicators["date"], indicators["minus_di"], color=self._MINUS_DI, linewidth=1.45, label="-DI")
        ax.axhline(25, color="#94a3b8", linewidth=1, linestyle="--", alpha=0.75)
        ax.set_title(
            f"ADX / DMI | ADX {float(indicators['adx'].iloc[-1]):.1f}  +DI {float(indicators['plus_di'].iloc[-1]):.1f}  -DI {float(indicators['minus_di'].iloc[-1]):.1f}",
            loc="left",
            fontsize=11.6,
            pad=10,
            fontweight="semibold",
        )
        ax.legend(loc="upper left", fontsize=6.3, ncol=3, columnspacing=0.85, handletextpad=0.40)
        self._draw_signal_badges(ax, self._adx_badges(indicators))
        self._format_date_axis(ax, indicators["date"])

    def _draw_obv_panel(self, ax: Any, indicators: Mapping[str, Any], close_series: pd.Series, divergence: Mapping[str, Any] | None = None) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["obv"], color=self._OBV, linewidth=1.8, label="OBV")
        ax.plot(indicators["date"], indicators["obv_ma"], color=self._OBV_MA, linewidth=1.45, label="OBV MA20")
        ax.axhline(float(indicators["obv"].iloc[0]), color="#cbd5e1", linewidth=0.9, linestyle="--", alpha=0.72)
        latest_obv = float(indicators["obv"].iloc[-1])
        latest_ma = float(indicators["obv_ma"].iloc[-1])
        ax.set_title(
            f"OBV | 今日 {self._format_obv_value(latest_obv)}  MA20 {self._format_obv_value(latest_ma)}",
            loc="left",
            fontsize=11.6,
            pad=10,
            fontweight="semibold",
        )
        ax.legend(loc="upper left", fontsize=6.3, ncol=2, columnspacing=0.85, handletextpad=0.40)
        ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
        ax.yaxis.get_offset_text().set_size(7.0)
        self._draw_signal_badges(ax, self._obv_badges(indicators, close_series, divergence))
        self._format_date_axis(ax, indicators["date"])

    def _draw_signal_badges(self, ax: Any, badges: list[tuple[str, str]]) -> None:
        if not badges:
            return
        cursor = 0.995
        y = 1.02
        for text, tone in badges[:4][::-1]:
            clean = str(text).strip()
            if not clean:
                continue
            width = min(max(0.050 + 0.0084 * len(clean), 0.075), 0.24)
            x = cursor - width
            if x < 0.45:
                break
            fc, ec, tc = self._signal_badge_colors(tone)
            if FancyBboxPatch is not None:
                chip = FancyBboxPatch(
                    (x, y),
                    width,
                    0.11,
                    boxstyle="round,pad=0.010,rounding_size=0.05",
                    transform=ax.transAxes,
                    facecolor=fc,
                    edgecolor=ec,
                    linewidth=0.65,
                    zorder=7,
                    clip_on=False,
                )
                ax.add_patch(chip)
            ax.text(
                x + 0.012,
                y + 0.055,
                clean,
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=6.6,
                color=tc,
                zorder=8,
                clip_on=False,
            )
            cursor = x - 0.010

    def _signal_badge_colors(self, tone: str) -> tuple[str, str, str]:
        if tone == "bull":
            return "#edf9f2", "#8fd0a8", "#146c43"
        if tone == "bear":
            return "#fff0ee", "#f0aaa0", "#9f2f1f"
        if tone == "warn":
            return "#fff8e8", "#efc97c", "#8a5a12"
        return "#f5efe5", "#e3d4c0", "#5b6470"

    def _market_mode_badge(self, indicators: Mapping[str, Any]) -> tuple[str, str]:
        return _shared_market_mode_badge(indicators)

    def _recent_divergence_label(self, price_series: pd.Series, signal_series: pd.Series) -> tuple[str, str] | None:
        return _shared_recent_divergence_label(price_series, signal_series)

    def _divergence_badge_for_indicator(self, indicator_name: str, divergence: Mapping[str, Any] | None) -> tuple[str, str] | None:
        return _shared_divergence_badge_for_indicator(indicator_name, divergence)

    def _macd_badges(self, indicators: Mapping[str, Any], close_series: pd.Series, divergence: Mapping[str, Any] | None = None) -> list[tuple[str, str]]:
        return _shared_macd_badges(indicators, close_series, divergence)

    def _kdj_badges(self, indicators: Mapping[str, Any]) -> list[tuple[str, str]]:
        return _shared_kdj_badges(indicators)

    def _rsi_badges(self, indicators: Mapping[str, Any], close_series: pd.Series, divergence: Mapping[str, Any] | None = None) -> list[tuple[str, str]]:
        return _shared_rsi_badges(indicators, close_series, divergence)

    def _boll_badges(self, history: pd.DataFrame, indicators: Mapping[str, Any]) -> list[tuple[str, str]]:
        return _shared_boll_badges(history, indicators)

    def _adx_badges(self, indicators: Mapping[str, Any]) -> list[tuple[str, str]]:
        return _shared_adx_badges(indicators)

    def _obv_badges(self, indicators: Mapping[str, Any], close_series: pd.Series, divergence: Mapping[str, Any] | None = None) -> list[tuple[str, str]]:
        return _shared_obv_badges(indicators, close_series, divergence)

    def _mark_crosses(self, ax: Any, dates: pd.Series, series_a: pd.Series, series_b: pd.Series) -> None:
        date_series = pd.to_datetime(pd.Series(dates), errors="coerce").reset_index(drop=True)
        a = pd.to_numeric(pd.Series(series_a), errors="coerce").reset_index(drop=True)
        b = pd.to_numeric(pd.Series(series_b), errors="coerce").reset_index(drop=True)
        if date_series.empty or a.empty or b.empty:
            return

        golden_x: list[pd.Timestamp] = []
        golden_y: list[float] = []
        death_x: list[pd.Timestamp] = []
        death_y: list[float] = []

        for idx in range(1, min(len(date_series), len(a), len(b))):
            prev_date = date_series.iloc[idx - 1]
            curr_date = date_series.iloc[idx]
            prev_a = a.iloc[idx - 1]
            curr_a = a.iloc[idx]
            prev_b = b.iloc[idx - 1]
            curr_b = b.iloc[idx]
            if pd.isna(prev_date) or pd.isna(curr_date) or any(pd.isna(v) for v in (prev_a, curr_a, prev_b, curr_b)):
                continue

            prev_diff = float(prev_a - prev_b)
            curr_diff = float(curr_a - curr_b)
            is_golden = prev_diff <= 0 < curr_diff
            is_death = prev_diff >= 0 > curr_diff
            if not is_golden and not is_death:
                continue

            denom = curr_diff - prev_diff
            alpha = 0.5 if abs(denom) < 1e-12 else float(np.clip((-prev_diff) / denom, 0.0, 1.0))
            cross_x = prev_date + (curr_date - prev_date) * alpha
            cross_y = float(prev_a + (curr_a - prev_a) * alpha)

            if is_golden:
                golden_x.append(cross_x)
                golden_y.append(cross_y)
            else:
                death_x.append(cross_x)
                death_y.append(cross_y)

        if golden_x:
            ax.scatter(golden_x, golden_y, color="#16a34a", marker="^", s=54, zorder=6, label="_nolegend_")
        if death_x:
            ax.scatter(death_x, death_y, color="#dc2626", marker="v", s=54, zorder=6, label="_nolegend_")

    def _draw_candles(self, ax: Any, history: pd.DataFrame, *, width: float) -> None:
        if history.empty or Rectangle is None or mdates is None:
            return
        dates = mdates.date2num(pd.to_datetime(history["date"]).dt.to_pydatetime())
        widths = np.diff(dates)
        candle_width = float(np.nanmedian(widths)) * width if len(widths) else width
        if not np.isfinite(candle_width) or candle_width <= 0:
            candle_width = width
        wick_colors = []
        wick_lows = []
        wick_highs = []
        wick_dates = []
        for date_num, row in zip(dates, history.itertuples(index=False)):
            open_price = float(row.open)
            close_price = float(row.close)
            high = float(row.high)
            low = float(row.low)
            bullish = close_price >= open_price
            face = self._UP_BODY if bullish else self._DOWN_BODY
            edge = self._UP_EDGE if bullish else self._DOWN_EDGE
            wick_dates.append(date_num)
            wick_lows.append(low)
            wick_highs.append(high)
            wick_colors.append(edge if bullish else self._WICK)
            body_bottom = min(open_price, close_price)
            body_height = abs(close_price - open_price)
            if body_height < 1e-6:
                body_height = max((high - low) * 0.08, max(abs(close_price), 1.0) * 0.0015)
                body_bottom = close_price - body_height / 2
            rect = Rectangle(
                (date_num - candle_width / 2, body_bottom),
                candle_width,
                body_height,
                facecolor=face,
                edgecolor=edge,
                linewidth=1.0,
                alpha=0.92,
                zorder=3,
            )
            ax.add_patch(rect)
        ax.vlines(wick_dates, wick_lows, wick_highs, colors=wick_colors, linewidth=0.95, alpha=0.95, zorder=2)
        ax.set_xlim(dates[0] - candle_width * 1.4, dates[-1] + candle_width * 1.8)

    def _draw_volume_overlay(self, ax: Any, history: pd.DataFrame) -> None:
        if history.empty or "volume" not in history.columns or mdates is None:
            return
        volume = pd.to_numeric(history["volume"], errors="coerce").fillna(0.0)
        if volume.max() <= 0:
            return
        ax_vol = ax.inset_axes([0.0, 0.01, 1.0, 0.20], sharex=ax)
        ax_vol.set_zorder(1)
        ax_vol.set_facecolor("none")
        dates = pd.to_datetime(history["date"])
        colors = [self._VOLUME_UP if close >= open_ else self._VOLUME_DOWN for open_, close in zip(history["open"], history["close"])]
        ax_vol.bar(dates, volume, width=0.76, color=colors, alpha=0.84, align="center")
        ax_vol.set_ylim(0, volume.max() * 1.25)
        ax_vol.set_yticks([])
        ax_vol.tick_params(axis="x", which="both", bottom=False, labelbottom=False, length=0)
        for spine in ax_vol.spines.values():
            spine.set_visible(False)
        ax_vol.grid(False)

    def _build_price_levels(
        self,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        *,
        price: float,
        support_low: float,
        support_high: float,
    ) -> list[tuple[str, float, str]]:
        levels: list[tuple[str, float, str]] = []
        history = history.copy()
        technical = dict(analysis.get("technical_raw", {}) or {})
        fibonacci = dict(technical.get("fibonacci", {}) or {})
        swing_high = float(fibonacci.get("swing_high") or 0.0)
        recent_high = float(pd.to_numeric(history.get("high"), errors="coerce").dropna().tail(20).max() or 0.0)
        recent_low = float(pd.to_numeric(history.get("low"), errors="coerce").dropna().tail(20).min() or 0.0)
        stop_level = self._extract_price_hint(str(dict(analysis.get("action") or {}).get("stop", "")), reference_price=price)
        target_level = self._extract_price_hint(str(dict(analysis.get("action") or {}).get("target", "")), reference_price=price)
        tolerance = max(abs(price), 1.0) * 0.004

        def add(label: str, value: float, tone: str) -> None:
            if not np.isfinite(value) or value <= 0:
                return
            for existing_label, existing_value, existing_tone in levels:
                if abs(existing_value - value) <= tolerance:
                    if existing_tone == tone:
                        return
                    if "上沿" in label or "下沿" in label:
                        return
                    if label == existing_label:
                        return
            levels.append((label, float(value), tone))

        if support_low > 0:
            add("支撑下沿", support_low, "support")
        if support_high > 0 and abs(support_high - support_low) > tolerance:
            add("支撑上沿", support_high, "support")
        if not levels and recent_low > 0 and recent_low < price:
            add("前低支撑", recent_low, "support")
        if stop_level and stop_level < price:
            add("止损参考", stop_level, "stop")
        if target_level and target_level > price:
            add("目标压力", target_level, "resistance")
        if recent_high > price * 1.01:
            add("近端压力", recent_high, "resistance")
        if swing_high > price * 1.02:
            add("前高压力", swing_high, "resistance")

        supports = sorted([item for item in levels if item[2] in {"support", "stop"}], key=lambda item: item[1], reverse=True)[:3]
        resistances = sorted([item for item in levels if item[2] == "resistance"], key=lambda item: item[1])[:2]
        return sorted([*supports, *resistances], key=lambda item: item[1])

    def _draw_price_levels(self, ax: Any, history: pd.DataFrame, levels: list[tuple[str, float, str]]) -> None:
        if not levels or transforms is None or history.empty:
            return
        value_range = float(pd.to_numeric(history.get("high"), errors="coerce").max() - pd.to_numeric(history.get("low"), errors="coerce").min())
        min_gap = max(value_range * 0.035, max(abs(float(history["close"].iloc[-1])), 1.0) * 0.004)
        placed: list[float] = []
        trans = transforms.blended_transform_factory(ax.transAxes, ax.transData)
        for label, value, tone in levels:
            if tone == "support":
                color = self._SUPPORT
                linestyle = (0, (4, 2))
            elif tone == "stop":
                color = self._STOP
                linestyle = (0, (2, 2))
            else:
                color = self._RESISTANCE
                linestyle = (0, (6, 2))
            ax.axhline(value, color=color, linewidth=0.9, linestyle=linestyle, alpha=0.65, zorder=1.15)
            label_y = value
            for placed_y in placed:
                if abs(label_y - placed_y) < min_gap:
                    label_y = placed_y + min_gap
            placed.append(label_y)
            ax.text(
                1.01,
                label_y,
                f"{label} {value:.3f}",
                transform=trans,
                ha="left",
                va="center",
                fontsize=8.0,
                color=self._PAPER,
                fontweight="bold",
                bbox={"boxstyle": "round,pad=0.20,rounding_size=0.14", "fc": color, "ec": "none", "alpha": 0.92},
                zorder=6,
                clip_on=False,
            )

    def _style_axis(self, ax: Any) -> None:
        ax.set_facecolor(self._PANEL)
        ax.grid(axis="y", linestyle="--", linewidth=0.65, alpha=0.24)
        ax.grid(axis="x", linestyle="-", linewidth=0.35, alpha=0.06)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#d1d5db")
        ax.spines["bottom"].set_color("#d1d5db")
        ax.tick_params(axis="both", which="major", labelsize=8.3, length=0, pad=4)

    def _style_volume_axis(self, ax: Any) -> None:
        ax.set_facecolor(self._PANEL)
        ax.grid(False)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["bottom"].set_color("#d1d5db")
        ax.tick_params(axis="both", which="major", labelsize=8.0, length=0, pad=3)

    def _visible_price_levels(
        self,
        levels: list[tuple[str, float, str]],
        *,
        price: float,
        max_distance_pct: float,
    ) -> list[tuple[str, float, str]]:
        if price <= 0:
            return levels
        filtered: list[tuple[str, float, str]] = []
        for label, value, tone in levels:
            distance_pct = abs(value - price) / price
            if tone == "stop":
                filtered.append((label, value, tone))
                continue
            if distance_pct <= max_distance_pct:
                filtered.append((label, value, tone))
        supports = [item for item in filtered if item[2] in {"support", "stop"}]
        resistances = [item for item in filtered if item[2] == "resistance"]
        supports = sorted(supports, key=lambda item: item[1], reverse=True)[:3]
        resistances = sorted(resistances, key=lambda item: item[1])[:2]
        return sorted([*supports, *resistances], key=lambda item: item[1])

    def _apply_price_ylim(self, ax: Any, history: pd.DataFrame, levels: list[tuple[str, float, str]]) -> None:
        if history.empty:
            return
        highs = pd.to_numeric(history.get("high"), errors="coerce").dropna()
        lows = pd.to_numeric(history.get("low"), errors="coerce").dropna()
        values: list[float] = []
        if not highs.empty:
            values.append(float(highs.max()))
        if not lows.empty:
            values.append(float(lows.min()))
        values.extend(float(value) for _, value, _ in levels if np.isfinite(value) and value > 0)
        if len(values) < 2:
            return
        upper = max(values)
        lower = min(values)
        span = max(upper - lower, max(abs(upper), 1.0) * 0.04)
        padding = span * 0.08
        ax.set_ylim(lower - padding, upper + padding)

    def _format_date_axis(self, ax: Any, dates: Optional[pd.Series] = None) -> None:
        if mdates is None:
            ax.tick_params(axis="x", rotation=0)
            return

        date_series = pd.Series(dtype="datetime64[ns]")
        span_days = 0
        if dates is not None and len(dates):
            date_series = pd.to_datetime(pd.Series(dates), errors="coerce").dropna().drop_duplicates()
            if not date_series.empty:
                span_days = max(int((date_series.iloc[-1] - date_series.iloc[0]).days), 1)

        if not date_series.empty:
            if span_days <= 35:
                max_labels = 4
                formatter = mdates.DateFormatter("%m-%d")
            elif span_days <= 120:
                max_labels = 5
                formatter = mdates.DateFormatter("%m-%d")
            else:
                max_labels = 6
                formatter = mdates.DateFormatter("%m-%d")
            tick_dates = self._sample_tick_dates(date_series, max_labels)
            ax.set_xticks(list(tick_dates.dt.to_pydatetime()))
            ax.xaxis.set_major_formatter(formatter)
        else:
            locator = mdates.AutoDateLocator(minticks=4, maxticks=5, interval_multiples=True)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        ax.tick_params(axis="x", rotation=0)

    def _extend_right_gutter(self, ax: Any, history: pd.DataFrame, *, ratio: float, min_days: int) -> Any:
        if history.empty or mdates is None:
            return 1.0
        dates = pd.to_datetime(history["date"], errors="coerce").dropna()
        if dates.empty:
            return 1.0
        first = dates.iloc[0]
        last = dates.iloc[-1]
        span_days = max(int((last - first).days), max(len(dates) - 1, 1))
        pad_days = max(int(span_days * ratio), min_days)
        left_pad = max(int(span_days * 0.02), 1)
        anchor = last + pd.Timedelta(days=max(pad_days - 1, min_days // 2 or 1))
        ax.set_xlim(mdates.date2num(first - pd.Timedelta(days=left_pad)), mdates.date2num(last + pd.Timedelta(days=pad_days)))
        return anchor

    def _extract_price_hint(self, text: str, *, reference_price: float = 0.0) -> float:
        matches = re.findall(r"(-?\d+(?:\.\d+)?)", str(text or ""))
        if not matches:
            return 0.0
        values: list[float] = []
        for item in matches:
            try:
                values.append(float(item))
            except ValueError:
                continue
        if not values:
            return 0.0
        if reference_price > 0:
            plausible = [value for value in values if reference_price * 0.4 <= value <= reference_price * 2.5]
            if plausible:
                return min(plausible, key=lambda value: abs(value - reference_price))
        decimal_like = [value for value in values if abs(value) < 20 and not float(value).is_integer()]
        if decimal_like:
            return decimal_like[-1]
        return values[-1]

    def _is_history_fallback(self, analysis: Mapping[str, Any]) -> bool:
        if bool(analysis.get("history_fallback_mode")):
            return True
        metadata = dict(analysis.get("metadata") or {})
        return bool(metadata.get("history_fallback"))

    def _sample_tick_dates(self, date_series: pd.Series, max_labels: int) -> pd.Series:
        clean = pd.to_datetime(pd.Series(date_series), errors="coerce").dropna().drop_duplicates().reset_index(drop=True)
        if clean.empty:
            return clean
        if len(clean) <= max_labels:
            return clean
        indices = np.linspace(0, len(clean) - 1, num=max_labels, dtype=int)
        indices = sorted(set(int(index) for index in indices))
        return clean.iloc[indices].reset_index(drop=True)

    def _price_panel_note(self, analysis: Mapping[str, Any]) -> str:
        technical = analysis.get("technical_raw", {})
        rsi = float(technical.get("rsi", {}).get("RSI", 0.0))
        adx = float(technical.get("dmi", {}).get("ADX", 0.0))
        return f"RSI {rsi:.1f} · ADX {adx:.1f} · 阶段 {analysis.get('narrative', {}).get('phase', {}).get('label', '未识别')}"

    def _headline_note(self, analysis: Mapping[str, Any]) -> str:
        narrative = analysis.get("narrative", {})
        judgment = narrative.get("judgment", {})
        day_theme = analysis.get("day_theme", {}).get("label", "未识别")
        return " | ".join(
            [
                f"方向 {judgment.get('direction', '未识别')}",
                str(judgment.get("trade_state", "未识别")),
                f"主线 {day_theme}",
            ]
        )

    def _rating_badge(self, analysis: Mapping[str, Any]) -> str:
        rating = analysis.get("rating", {})
        rank = rating.get("rank")
        if isinstance(rank, int) and rank > 0:
            return f"{rank}星"
        stars = str(rating.get("stars", "")).strip()
        if stars:
            return f"{stars.count('⭐') or len(stars)}星"
        return "未评级"

    def _relative_panel_note(self, analysis: Mapping[str, Any]) -> str:
        metrics = analysis.get("metrics", {})
        rel_5d = metrics.get("return_5d")
        rel_20d = metrics.get("return_20d")
        benchmark = str(analysis.get("benchmark_name", "基准") or "基准")
        regime = str(analysis.get("regime", {}).get("current_regime", "unknown") or "unknown")
        return f"蓝线为标的，灰虚线为 {benchmark}；近5日 {self._fmt_pct(rel_5d)}，近20日 {self._fmt_pct(rel_20d)}，Regime {regime}"

    def _footer_text(self, analysis: Mapping[str, Any]) -> str:
        narrative = analysis.get("narrative", {})
        day_theme = analysis.get("day_theme", {}).get("label", "未识别")
        summary_lines = narrative.get("summary_lines", [])
        summary = summary_lines[0] if summary_lines else "结论待补充"
        if len(summary) > 48:
            summary = f"{summary[:48]}..."
        return f"主线: {day_theme} | 核心: {summary}"

    def _score_color(self, value: float) -> str:
        if value >= 70:
            return "#2f8f83"
        if value >= 45:
            return "#d49a56"
        return "#c9716f"

    def _fmt_pct(self, value: Optional[float]) -> str:
        if value is None:
            return "缺失"
        return f"{float(value) * 100:+.2f}%"

    def _first_positive(self, *values: float) -> float:
        for value in values:
            if value and value > 0:
                return value
        return 0.0

    def _indicator_summary_text(self, indicators: Mapping[str, Any]) -> str:
        adx = float(indicators["adx"].iloc[-1])
        plus_di = float(indicators["plus_di"].iloc[-1])
        minus_di = float(indicators["minus_di"].iloc[-1])
        obv = float(indicators["obv"].iloc[-1])
        obv_display = f"{obv/10000:.1f}万" if abs(obv) >= 10000 else f"{obv:.0f}"
        return f"趋势 ADX {adx:.1f} | DMI +DI {plus_di:.1f} / -DI {minus_di:.1f} | 量能 OBV {obv_display}"

    def _format_obv_value(self, value: float) -> str:
        abs_value = abs(value)
        if abs_value >= 100000000:
            return f"{value / 100000000:.2f}亿"
        if abs_value >= 10000:
            return f"{value / 10000:.1f}万"
        return f"{value:.0f}"

    def _draw_panel_note(self, ax: Any, text: str) -> None:
        if not str(text).strip():
            return
        ax.text(
            0.0,
            1.015,
            str(text).strip(),
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=7.7,
            color=self._MUTED,
            clip_on=False,
        )
