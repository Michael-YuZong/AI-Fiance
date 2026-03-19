"""Chart rendering for single-asset analysis reports."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd
from matplotlib import font_manager

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import resolve_project_path

try:  # pragma: no cover - rendering dependency
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    from matplotlib import transforms
    from matplotlib.patches import Rectangle
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    matplotlib = None
    mdates = None
    transforms = None
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

    _UP_BODY = "#d97757"
    _UP_EDGE = "#b45309"
    _DOWN_BODY = "#4b798c"
    _DOWN_EDGE = "#1f4e5f"
    _WICK = "#7c6b58"
    _MA20 = "#cf6a3c"
    _MA60 = "#2d6f73"
    _GRID = "#e8dccf"
    _PANEL = "#fffaf2"
    _PAPER = "#f6efe4"
    _TEXT = "#233042"
    _MUTED = "#6b7280"
    _VOLUME_UP = "#efcfbc"
    _VOLUME_DOWN = "#c9d8df"
    _SUPPORT = "#5c8d74"
    _RESISTANCE = "#c26a55"
    _STOP = "#b45309"

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
        plt.style.use("seaborn-v0_8-whitegrid")
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = ["PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", "Arial Unicode MS", "DejaVu Sans"]
        plt.rcParams["font.serif"] = chosen or ["DejaVu Serif"]
        plt.rcParams["axes.unicode_minus"] = False
        plt.rcParams["figure.facecolor"] = self._PAPER
        plt.rcParams["axes.facecolor"] = self._PANEL
        plt.rcParams["savefig.facecolor"] = self._PAPER
        plt.rcParams["axes.edgecolor"] = "#dacdbc"
        plt.rcParams["grid.color"] = self._GRID
        plt.rcParams["grid.alpha"] = 0.7
        plt.rcParams["grid.linewidth"] = 0.7
        plt.rcParams["axes.titleweight"] = "bold"
        plt.rcParams["axes.titlesize"] = 13
        plt.rcParams["axes.labelsize"] = 11
        plt.rcParams["axes.labelcolor"] = self._MUTED
        plt.rcParams["axes.titlecolor"] = self._TEXT
        plt.rcParams["xtick.color"] = "#6d6256"
        plt.rcParams["ytick.color"] = "#6d6256"
        plt.rcParams["lines.solid_capstyle"] = "round"
        plt.rcParams["lines.solid_joinstyle"] = "round"
        plt.rcParams["patch.antialiased"] = True
        plt.rcParams["path.simplify"] = True

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

        fig = plt.figure(figsize=(14, 10), dpi=160)
        fig.subplots_adjust(top=0.82, bottom=0.10, left=0.07, right=0.97)
        grid = fig.add_gridspec(2, 2, height_ratios=[1.2, 1], hspace=0.28, wspace=0.18)
        ax_price = fig.add_subplot(grid[0, 0])
        ax_relative = fig.add_subplot(grid[0, 1])
        ax_scores = fig.add_subplot(grid[1, :])

        self._draw_price_panel(ax_price, analysis, prepared, ma20, ma60, price, support_low, support_high)
        self._draw_relative_panel(ax_relative, analysis, prepared, benchmark_prepared)
        self._draw_score_panel(ax_scores, analysis)

        narrative = analysis.get("narrative", {})
        title = f"{analysis.get('name', symbol)} ({symbol})"
        subtitle = " | ".join(
            [
                self._rating_badge(analysis),
                str(analysis.get("rating", {}).get("label", "未评级")),
                str(narrative.get("phase", {}).get("label", "状态未识别")),
            ]
        )
        fig.suptitle(title, x=0.07, y=0.982, ha="left", fontsize=18, fontweight="bold", color="#1f2937")
        fig.text(0.07, 0.936, subtitle, ha="left", fontsize=9.2, color="#6b7280")
        fig.text(
            0.97,
            0.936,
            self._headline_note(analysis),
            ha="right",
            va="center",
            fontsize=8.0,
            color="#374151",
        )
        fig.text(
            0.07,
            0.03,
            self._footer_text(analysis),
            ha="left",
            va="bottom",
            fontsize=9,
            color="#4b5563",
        )
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

        fig = plt.figure(figsize=(14, 6), dpi=160)
        fig.subplots_adjust(top=0.82, bottom=0.14, left=0.07, right=0.97, wspace=0.18)
        grid = fig.add_gridspec(1, 2)
        ax_3m = fig.add_subplot(grid[0, 0])
        ax_1m = fig.add_subplot(grid[0, 1])

        self._draw_window_panel(ax_3m, analysis, prepared.tail(66).copy(), "近3月走势", support_low=support_low, support_high=support_high)
        self._draw_short_window_panel(ax_1m, analysis, prepared.tail(22).copy(), "近1月均线节奏", support_low=support_low, support_high=support_high)

        fig.suptitle(
            f"{analysis.get('name', analysis.get('symbol', 'asset'))} | 阶段走势",
            x=0.07,
            y=0.965,
            ha="left",
            fontsize=16,
            fontweight="bold",
            color="#1f2937",
        )
        fig.text(
            0.07,
            0.905,
            "左侧看中期 K 线节奏，右侧只保留近1月收盘线与 MA5 / MA10，避免短窗 K 线过密影响判断。",
            ha="left",
            fontsize=8.5,
            color="#6b7280",
        )
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _render_indicators(self, analysis: Mapping[str, Any], history: pd.DataFrame, path: Path) -> None:
        prepared = self._prepare_history(history)
        if prepared.empty:
            return

        calc_window = prepared.tail(120).copy()
        plot_window = calc_window.tail(22).copy()
        indicators = self._indicator_series(calc_window)
        indicators = self._trim_indicator_series(indicators, 22)

        fig = plt.figure(figsize=(14, 11), dpi=160)
        fig.subplots_adjust(top=0.87, bottom=0.08, left=0.08, right=0.97, hspace=0.34, wspace=0.22)
        grid = fig.add_gridspec(3, 2)
        axes = [fig.add_subplot(grid[i, j]) for i in range(3) for j in range(2)]

        self._draw_macd_panel(axes[0], indicators)
        self._draw_kdj_panel(axes[1], indicators)
        self._draw_rsi_panel(axes[2], indicators)
        self._draw_adx_panel(axes[3], indicators)
        self._draw_boll_panel(axes[4], plot_window, indicators)
        self._draw_obv_panel(axes[5], indicators)

        fig.suptitle(
            f"{analysis.get('name', analysis.get('symbol', 'asset'))} | 技术指标总览（近1月）",
            x=0.08,
            y=0.982,
            ha="left",
            fontsize=16,
            fontweight="bold",
            color="#1f2937",
        )
        fig.text(
            0.08,
            0.935,
            "仅展示近1月走势；标题直接给今日值，并在 MACD / KDJ 面板标注金叉死叉位置。",
            ha="left",
            fontsize=8.6,
            color="#6b7280",
        )
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
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        ma20: pd.Series,
        ma60: pd.Series,
        price: float,
        support_low: float,
        support_high: float,
    ) -> None:
        panel_history = history.tail(90).copy()
        panel_ma20 = ma20.tail(len(panel_history))
        panel_ma60 = ma60.tail(len(panel_history))
        level_guides = self._build_price_levels(analysis, panel_history, price=price, support_low=support_low, support_high=support_high)
        volume = panel_history.get("volume")
        self._style_axis(ax)
        self._draw_candles(ax, panel_history, width=0.62)
        if isinstance(volume, pd.Series) and volume.notna().any():
            self._draw_volume_overlay(ax, panel_history)
        ax.plot(panel_history["date"], panel_ma20, color=self._MA20, linewidth=1.7, alpha=0.98, label="MA20", zorder=4)
        ax.plot(panel_history["date"], panel_ma60, color=self._MA60, linewidth=1.7, alpha=0.98, label="MA60", zorder=4)
        if support_low > 0 and support_high > 0:
            lower = min(support_low, support_high)
            upper = max(support_low, support_high)
            ax.axhspan(lower, upper, color="#e7c59a", alpha=0.18, label="支撑观察区", zorder=1)
        self._draw_price_levels(ax, panel_history, level_guides)
        ax.scatter(panel_history["date"].iloc[-1], price, color="#9f2f1f", s=30, zorder=6)
        ax.annotate(
            f"{price:.3f}",
            xy=(panel_history["date"].iloc[-1], price),
            xytext=(10, 8),
            textcoords="offset points",
            fontsize=8.7,
            color="#8b2a1a",
            bbox={"boxstyle": "round,pad=0.28,rounding_size=0.15", "fc": "#fce7dc", "ec": "#f3c7b4"},
        )
        ax.set_title("价格结构 / K线", loc="left", fontsize=13, color=self._TEXT)
        ax.text(
            0.02,
            0.04,
            self._price_panel_note(analysis),
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=8.5,
            color="#43505f",
            bbox={"boxstyle": "round,pad=0.38,rounding_size=0.2", "fc": "#fff6eb", "ec": "#ecd1b3"},
        )
        ax.legend(loc="upper right", frameon=False, ncol=4, fontsize=8.3)
        self._format_date_axis(ax, panel_history["date"])
        ax.set_ylabel("价格")

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
        ax.plot(base["date"], base["norm"], color="#8b5cf6", linewidth=2.0, alpha=0.95, label=str(analysis.get("symbol", "标的")))
        if benchmark_history is not None and not benchmark_history.empty:
            bench = benchmark_history.tail(120).copy()
            merged = pd.merge(base[["date", "norm"]], bench[["date", "close"]], on="date", how="inner")
            if not merged.empty:
                merged["bench_norm"] = merged["close"] / float(merged["close"].iloc[0]) * 100
                ax.plot(merged["date"], merged["bench_norm"], color="#7d8591", linewidth=1.7, linestyle=(0, (4, 2)), alpha=0.9, label=str(analysis.get("benchmark_name", "基准")))
        ax.axhline(100, color="#c8d0d9", linewidth=1)
        ax.fill_between(base["date"], 100, base["norm"], where=base["norm"] >= 100, color="#eadcff", alpha=0.18)
        ax.fill_between(base["date"], 100, base["norm"], where=base["norm"] < 100, color="#dbeafe", alpha=0.12)
        ax.set_title("相对强弱 / 归一化走势", loc="left", fontsize=13, color=self._TEXT)
        ax.text(
            0.02,
            0.04,
            self._relative_panel_note(analysis),
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=8.5,
            color="#43505f",
            bbox={"boxstyle": "round,pad=0.38,rounding_size=0.2", "fc": "#f4efff", "ec": "#ddd1fb"},
        )
        ax.legend(loc="upper right", frameon=False, fontsize=8.5)
        self._format_date_axis(ax, base["date"])
        ax.set_ylabel("起点=100")

    def _draw_score_panel(self, ax: Any, analysis: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        labels = []
        values = []
        raw_labels = []
        for key, label in DIMENSION_LABELS:
            dimension = analysis.get("dimensions", {}).get(key, {})
            score = dimension.get("score")
            max_score = dimension.get("max_score", 100) or 100
            normalized = 0.0 if score is None else float(score) / float(max_score) * 100
            labels.append(label)
            values.append(normalized)
            raw_labels.append("缺失" if score is None else f"{score}/{max_score}")
        colors = [self._score_color(value) for value in values]
        ax.barh(labels, values, color=colors, edgecolor=self._PAPER, height=0.58, linewidth=1.0)
        ax.set_xlim(0, 100)
        ax.set_title("八维评分", loc="left", fontsize=13, color=self._TEXT)
        ax.set_xlabel("归一化得分")
        for idx, (value, raw) in enumerate(zip(values, raw_labels)):
            ax.text(min(value + 1.5, 98), idx, raw, va="center", fontsize=9, color=self._TEXT)
        ax.grid(axis="x", linestyle="--", alpha=0.45)
        ax.invert_yaxis()

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
        level_guides = self._build_price_levels(
            analysis,
            history,
            price=float(close.iloc[-1]),
            support_low=support_low,
            support_high=support_high,
        )
        self._draw_candles(ax, history, width=0.70)
        self._draw_price_levels(ax, history, level_guides)
        ax.plot(history["date"], ma10, color="#e18b5a", linewidth=1.35, alpha=0.95, label="MA10", zorder=4)
        ax.plot(history["date"], ma20, color="#56898b", linewidth=1.35, alpha=0.95, label="MA20", zorder=4)
        ax.set_title(f"{title} / K线", loc="left", fontsize=13, color=self._TEXT)
        ax.legend(loc="upper left", frameon=False, fontsize=8.2, ncol=3)
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
        level_guides = self._build_price_levels(
            analysis,
            history,
            price=float(close.iloc[-1]),
            support_low=support_low,
            support_high=support_high,
        )
        ax.plot(history["date"], close, color=self._DOWN_EDGE, linewidth=1.8, alpha=0.96, label="收盘线", zorder=3)
        ax.plot(history["date"], ma5, color="#d97757", linewidth=1.45, alpha=0.96, label="MA5", zorder=4)
        ax.plot(history["date"], ma10, color="#5f8f8c", linewidth=1.45, alpha=0.96, label="MA10", zorder=4)
        ax.fill_between(history["date"], ma5, ma10, color="#eadfcc", alpha=0.16, zorder=2)
        self._draw_price_levels(ax, history, level_guides)
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
        ax.set_title(f"{title} / 收盘线 + 均线", loc="left", fontsize=13, color=self._TEXT)
        ax.legend(loc="upper left", frameon=False, fontsize=8.2, ncol=3)
        self._format_date_axis(ax, history["date"])
        ax.set_ylabel("价格")

    def _indicator_series(self, history: pd.DataFrame) -> Dict[str, Any]:
        normalized = normalize_ohlcv_frame(history)
        return TechnicalAnalyzer(normalized).indicator_series()

    def _trim_indicator_series(self, indicators: Mapping[str, Any], window: int) -> Dict[str, Any]:
        trimmed: Dict[str, Any] = {}
        for key, value in indicators.items():
            if isinstance(value, (pd.Series, pd.DataFrame)):
                trimmed[key] = value.tail(window)
            else:
                trimmed[key] = value
        return trimmed

    def _draw_macd_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.bar(indicators["date"], indicators["macd_hist"], color=["#8fb8a8" if x >= 0 else "#e8aaa1" for x in indicators["macd_hist"]], alpha=0.68, label="柱体")
        ax.plot(indicators["date"], indicators["macd_dif"], color="#4f78d0", linewidth=1.55, label="DIF")
        ax.plot(indicators["date"], indicators["macd_dea"], color="#dc8751", linewidth=1.55, label="DEA")
        self._mark_crosses(ax, indicators["date"], indicators["macd_dif"], indicators["macd_dea"])
        ax.axhline(0, color="#94a3b8", linewidth=1)
        ax.set_title(
            f"MACD | DIF {float(indicators['macd_dif'].iloc[-1]):.3f}  DEA {float(indicators['macd_dea'].iloc[-1]):.3f}  HIST {float(indicators['macd_hist'].iloc[-1]):.3f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=4)
        self._format_date_axis(ax, indicators["date"])

    def _draw_kdj_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["kdj_k"], color="#5073c6", linewidth=1.35, label="K")
        ax.plot(indicators["date"], indicators["kdj_d"], color="#dd8a52", linewidth=1.35, label="D")
        ax.plot(indicators["date"], indicators["kdj_j"], color="#8a63d2", linewidth=1.2, label="J")
        self._mark_crosses(ax, indicators["date"], indicators["kdj_k"], indicators["kdj_d"])
        ax.axhline(80, color="#f59e0b", linewidth=1, linestyle="--")
        ax.axhline(20, color="#10b981", linewidth=1, linestyle="--")
        ax.set_title(
            f"KDJ | K {float(indicators['kdj_k'].iloc[-1]):.1f}  D {float(indicators['kdj_d'].iloc[-1]):.1f}  J {float(indicators['kdj_j'].iloc[-1]):.1f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=5)
        self._format_date_axis(ax, indicators["date"])

    def _draw_rsi_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["rsi"], color="#d46a6a", linewidth=1.6)
        ax.axhline(70, color="#f59e0b", linewidth=1, linestyle="--")
        ax.axhline(30, color="#10b981", linewidth=1, linestyle="--")
        ax.set_ylim(0, 100)
        ax.set_title(f"RSI | 今日 {float(indicators['rsi'].iloc[-1]):.1f}", loc="left", fontsize=12.0)
        self._format_date_axis(ax, indicators["date"])

    def _draw_adx_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["adx"], color="#394152", linewidth=1.6, label="ADX")
        ax.plot(indicators["date"], indicators["plus_di"], color="#5aaa88", linewidth=1.2, label="+DI")
        ax.plot(indicators["date"], indicators["minus_di"], color="#df7e7e", linewidth=1.2, label="-DI")
        ax.axhline(25, color="#94a3b8", linewidth=1, linestyle="--")
        ax.set_title(
            f"ADX / DMI | ADX {float(indicators['adx'].iloc[-1]):.1f}  +DI {float(indicators['plus_di'].iloc[-1]):.1f}  -DI {float(indicators['minus_di'].iloc[-1]):.1f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=3)
        self._format_date_axis(ax, indicators["date"])

    def _draw_boll_panel(self, ax: Any, history: pd.DataFrame, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], history["close"], color=self._DOWN_EDGE, linewidth=1.7, label="收盘价")
        ax.plot(indicators["date"], indicators["boll_upper"], color="#d88f8b", linewidth=1.05, label="上轨")
        ax.plot(indicators["date"], indicators["boll_mid"], color="#d0a15a", linewidth=1.05, label="中轨")
        ax.plot(indicators["date"], indicators["boll_lower"], color="#79aa9a", linewidth=1.05, label="下轨")
        ax.fill_between(indicators["date"], indicators["boll_lower"], indicators["boll_upper"], color="#dbe7f2", alpha=0.22)
        ax.set_title(
            f"BOLL | 收 {float(history['close'].iloc[-1]):.3f}  上 {float(indicators['boll_upper'].iloc[-1]):.3f}  中 {float(indicators['boll_mid'].iloc[-1]):.3f}  下 {float(indicators['boll_lower'].iloc[-1]):.3f}",
            loc="left",
            fontsize=11.4,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=4)
        self._format_date_axis(ax, indicators["date"])

    def _draw_obv_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax)
        ax.plot(indicators["date"], indicators["obv"], color="#8365d4", linewidth=1.7, label="OBV")
        ax.plot(indicators["date"], indicators["obv_ma"], color="#8a9098", linewidth=1.15, label="OBV MA20")
        ax.set_title(
            f"OBV | 今日 {float(indicators['obv'].iloc[-1]):.0f}  MA20 {float(indicators['obv_ma'].iloc[-1]):.0f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8)
        self._format_date_axis(ax, indicators["date"])

    def _mark_crosses(self, ax: Any, dates: pd.Series, series_a: pd.Series, series_b: pd.Series) -> None:
        diff = (series_a - series_b).fillna(0)
        prev = diff.shift(1)
        golden = (prev <= 0) & (diff > 0)
        death = (prev >= 0) & (diff < 0)
        if golden.any():
            ax.scatter(dates[golden], series_a[golden], color="#16a34a", marker="^", s=48, zorder=6, label="金叉")
        if death.any():
            ax.scatter(dates[death], series_a[death], color="#dc2626", marker="v", s=48, zorder=6, label="死叉")

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
                linewidth=0.9,
                alpha=0.92,
                zorder=3,
            )
            ax.add_patch(rect)
        ax.vlines(wick_dates, wick_lows, wick_highs, colors=wick_colors, linewidth=0.85, alpha=0.95, zorder=2)
        ax.set_xlim(dates[0] - candle_width * 1.4, dates[-1] + candle_width * 1.8)

    def _draw_volume_overlay(self, ax: Any, history: pd.DataFrame) -> None:
        if history.empty or "volume" not in history.columns or mdates is None:
            return
        volume = pd.to_numeric(history["volume"], errors="coerce").fillna(0.0)
        if volume.max() <= 0:
            return
        ax_vol = ax.twinx()
        ax_vol.set_zorder(0)
        ax_vol.patch.set_alpha(0.0)
        dates = pd.to_datetime(history["date"])
        colors = [self._VOLUME_UP if close >= open_ else self._VOLUME_DOWN for open_, close in zip(history["open"], history["close"])]
        ax_vol.bar(dates, volume, width=0.8, color=colors, alpha=0.28, align="center")
        ax_vol.set_ylim(0, volume.max() * 4.8)
        ax_vol.set_yticks([])
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
            for existing_label, existing_value, _ in levels:
                if abs(existing_value - value) <= tolerance:
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
        text_transform = transforms.blended_transform_factory(ax.transAxes, ax.transData)
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
            ax.axhline(value, color=color, linewidth=0.95, linestyle=linestyle, alpha=0.7, zorder=1.15)
            label_y = value
            for placed_y in placed:
                if abs(label_y - placed_y) < min_gap:
                    label_y = placed_y + min_gap
            placed.append(label_y)
            ax.text(
                0.995,
                label_y,
                f"{label} {value:.3f}",
                transform=text_transform,
                ha="right",
                va="bottom",
                fontsize=7.8,
                color=color,
                bbox={"boxstyle": "round,pad=0.22,rounding_size=0.14", "fc": self._PANEL, "ec": color, "alpha": 0.9},
                zorder=6,
            )

    def _style_axis(self, ax: Any) -> None:
        ax.set_facecolor(self._PANEL)
        ax.grid(axis="y", linestyle="--", linewidth=0.7, alpha=0.42)
        ax.grid(axis="x", linestyle="-", linewidth=0.4, alpha=0.10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#ddcfbe")
        ax.spines["bottom"].set_color("#ddcfbe")
        ax.tick_params(axis="both", which="major", labelsize=8.2, length=0, pad=4)

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
                formatter = mdates.DateFormatter("%Y-%m-%d")
            tick_dates = self._sample_tick_dates(date_series, max_labels)
            ax.set_xticks(list(tick_dates.dt.to_pydatetime()))
            ax.xaxis.set_major_formatter(formatter)
        else:
            locator = mdates.AutoDateLocator(minticks=4, maxticks=5, interval_multiples=True)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        ax.tick_params(axis="x", rotation=0)

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
        return f"RSI {rsi:.1f} | ADX {adx:.1f} | 当前阶段: {analysis.get('narrative', {}).get('phase', {}).get('label', '未识别')}"

    def _headline_note(self, analysis: Mapping[str, Any]) -> str:
        narrative = analysis.get("narrative", {})
        judgment = narrative.get("judgment", {})
        day_theme = analysis.get("day_theme", {}).get("label", "未识别")
        return " | ".join(
            [
                f"方向 {judgment.get('direction', '未识别')}",
                f"赔率 {judgment.get('odds', '未识别')}",
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
        return f"近5日 {self._fmt_pct(rel_5d)} | 近20日 {self._fmt_pct(rel_20d)} | Regime {analysis.get('regime', {}).get('current_regime', 'unknown')}"

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
            return "#0f766e"
        if value >= 45:
            return "#d97706"
        return "#b91c1c"

    def _fmt_pct(self, value: Optional[float]) -> str:
        if value is None:
            return "缺失"
        return f"{float(value) * 100:+.2f}%"

    def _first_positive(self, *values: float) -> float:
        for value in values:
            if value and value > 0:
                return value
        return 0.0
