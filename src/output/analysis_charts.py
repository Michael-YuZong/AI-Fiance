"""Chart rendering for single-asset analysis reports."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd
from matplotlib import font_manager

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import resolve_project_path

try:  # pragma: no cover - rendering dependency
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    matplotlib = None
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
        plt.rcParams["font.family"] = "serif"
        plt.rcParams["font.serif"] = chosen or ["DejaVu Serif"]
        plt.rcParams["axes.unicode_minus"] = False
        plt.rcParams["figure.facecolor"] = "#f5f0e8"
        plt.rcParams["axes.facecolor"] = "#fcfaf6"
        plt.rcParams["savefig.facecolor"] = "#f5f0e8"
        plt.rcParams["axes.edgecolor"] = "#d9d1c5"
        plt.rcParams["grid.color"] = "#e8dfd1"
        plt.rcParams["axes.titleweight"] = "bold"
        plt.rcParams["axes.titlesize"] = 13
        plt.rcParams["axes.labelsize"] = 11

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

        fig = plt.figure(figsize=(14, 6), dpi=160)
        fig.subplots_adjust(top=0.82, bottom=0.14, left=0.07, right=0.97, wspace=0.18)
        grid = fig.add_gridspec(1, 2)
        ax_3m = fig.add_subplot(grid[0, 0])
        ax_1m = fig.add_subplot(grid[0, 1])

        self._draw_window_panel(ax_3m, prepared.tail(66).copy(), "近3月走势")
        self._draw_window_panel(ax_1m, prepared.tail(22).copy(), "近1月走势")

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
            "左侧看中期节奏，右侧看短线加速或回落。适用于所有标的的统一价格窗口。",
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
        ax.plot(history["date"], history["close"], color="#164e63", linewidth=2.3, label="收盘价")
        ax.plot(history["date"], ma20, color="#ea580c", linewidth=1.6, label="MA20")
        ax.plot(history["date"], ma60, color="#0f766e", linewidth=1.6, label="MA60")
        if support_low > 0 and support_high > 0:
            lower = min(support_low, support_high)
            upper = max(support_low, support_high)
            ax.axhspan(lower, upper, color="#f59e0b", alpha=0.12, label="支撑观察区")
        ax.scatter(history["date"].iloc[-1], price, color="#b91c1c", s=38, zorder=5)
        ax.annotate(
            f"{price:.3f}",
            xy=(history["date"].iloc[-1], price),
            xytext=(10, 8),
            textcoords="offset points",
            fontsize=9,
            color="#991b1b",
            bbox={"boxstyle": "round,pad=0.25", "fc": "#fee2e2", "ec": "#fecaca"},
        )
        ax.set_title("价格结构", loc="left", fontsize=13, color="#111827")
        ax.text(
            0.02,
            0.04,
            self._price_panel_note(analysis),
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=8.5,
            color="#374151",
            bbox={"boxstyle": "round,pad=0.35", "fc": "#fff7ed", "ec": "#fed7aa"},
        )
        ax.legend(loc="upper right", frameon=False, ncol=4, fontsize=8.5)
        ax.tick_params(axis="x", rotation=20)
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
        ax.plot(base["date"], base["norm"], color="#7c3aed", linewidth=2.2, label=str(analysis.get("symbol", "标的")))
        if benchmark_history is not None and not benchmark_history.empty:
            bench = benchmark_history.tail(120).copy()
            merged = pd.merge(base[["date", "norm"]], bench[["date", "close"]], on="date", how="inner")
            if not merged.empty:
                merged["bench_norm"] = merged["close"] / float(merged["close"].iloc[0]) * 100
                ax.plot(merged["date"], merged["bench_norm"], color="#6b7280", linewidth=1.8, linestyle="--", label=str(analysis.get("benchmark_name", "基准")))
        ax.axhline(100, color="#cbd5e1", linewidth=1)
        ax.set_title("相对强弱 / 归一化走势", loc="left", fontsize=13, color="#111827")
        ax.text(
            0.02,
            0.04,
            self._relative_panel_note(analysis),
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=8.5,
            color="#374151",
            bbox={"boxstyle": "round,pad=0.35", "fc": "#eef2ff", "ec": "#c7d2fe"},
        )
        ax.legend(loc="upper right", frameon=False, fontsize=8.5)
        ax.tick_params(axis="x", rotation=20)
        ax.set_ylabel("起点=100")

    def _draw_score_panel(self, ax: Any, analysis: Mapping[str, Any]) -> None:
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
        ax.barh(labels, values, color=colors, edgecolor="#f5f0e8", height=0.58)
        ax.set_xlim(0, 100)
        ax.set_title("八维评分", loc="left", fontsize=13, color="#111827")
        ax.set_xlabel("归一化得分")
        for idx, (value, raw) in enumerate(zip(values, raw_labels)):
            ax.text(min(value + 1.5, 98), idx, raw, va="center", fontsize=9, color="#111827")
        ax.grid(axis="x", linestyle="--", alpha=0.5)
        ax.invert_yaxis()

    def _draw_window_panel(self, ax: Any, history: pd.DataFrame, title: str) -> None:
        if history.empty:
            return
        close = history["close"]
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ax.plot(history["date"], close, color="#164e63", linewidth=2.2, label="收盘价")
        ax.plot(history["date"], ma10, color="#ea580c", linewidth=1.4, label="MA10")
        ax.plot(history["date"], ma20, color="#0f766e", linewidth=1.4, label="MA20")
        ax.fill_between(history["date"], close.min(), close.max(), where=close >= ma20.fillna(close), color="#dcfce7", alpha=0.08)
        ax.set_title(title, loc="left", fontsize=13, color="#111827")
        ax.legend(loc="upper left", frameon=False, fontsize=8.5, ncol=3)
        ax.tick_params(axis="x", rotation=20)
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
        ax.bar(indicators["date"], indicators["macd_hist"], color=["#0f766e" if x >= 0 else "#b91c1c" for x in indicators["macd_hist"]], alpha=0.45, label="柱体")
        ax.plot(indicators["date"], indicators["macd_dif"], color="#2563eb", linewidth=1.6, label="DIF")
        ax.plot(indicators["date"], indicators["macd_dea"], color="#f97316", linewidth=1.6, label="DEA")
        self._mark_crosses(ax, indicators["date"], indicators["macd_dif"], indicators["macd_dea"])
        ax.axhline(0, color="#94a3b8", linewidth=1)
        ax.set_title(
            f"MACD | DIF {float(indicators['macd_dif'].iloc[-1]):.3f}  DEA {float(indicators['macd_dea'].iloc[-1]):.3f}  HIST {float(indicators['macd_hist'].iloc[-1]):.3f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=4)

    def _draw_kdj_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        ax.plot(indicators["date"], indicators["kdj_k"], color="#2563eb", linewidth=1.4, label="K")
        ax.plot(indicators["date"], indicators["kdj_d"], color="#f97316", linewidth=1.4, label="D")
        ax.plot(indicators["date"], indicators["kdj_j"], color="#7c3aed", linewidth=1.2, label="J")
        self._mark_crosses(ax, indicators["date"], indicators["kdj_k"], indicators["kdj_d"])
        ax.axhline(80, color="#f59e0b", linewidth=1, linestyle="--")
        ax.axhline(20, color="#10b981", linewidth=1, linestyle="--")
        ax.set_title(
            f"KDJ | K {float(indicators['kdj_k'].iloc[-1]):.1f}  D {float(indicators['kdj_d'].iloc[-1]):.1f}  J {float(indicators['kdj_j'].iloc[-1]):.1f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=5)

    def _draw_rsi_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        ax.plot(indicators["date"], indicators["rsi"], color="#dc2626", linewidth=1.7)
        ax.axhline(70, color="#f59e0b", linewidth=1, linestyle="--")
        ax.axhline(30, color="#10b981", linewidth=1, linestyle="--")
        ax.set_ylim(0, 100)
        ax.set_title(f"RSI | 今日 {float(indicators['rsi'].iloc[-1]):.1f}", loc="left", fontsize=12.0)

    def _draw_adx_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        ax.plot(indicators["date"], indicators["adx"], color="#111827", linewidth=1.7, label="ADX")
        ax.plot(indicators["date"], indicators["plus_di"], color="#10b981", linewidth=1.2, label="+DI")
        ax.plot(indicators["date"], indicators["minus_di"], color="#ef4444", linewidth=1.2, label="-DI")
        ax.axhline(25, color="#94a3b8", linewidth=1, linestyle="--")
        ax.set_title(
            f"ADX / DMI | ADX {float(indicators['adx'].iloc[-1]):.1f}  +DI {float(indicators['plus_di'].iloc[-1]):.1f}  -DI {float(indicators['minus_di'].iloc[-1]):.1f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=3)

    def _draw_boll_panel(self, ax: Any, history: pd.DataFrame, indicators: Mapping[str, Any]) -> None:
        ax.plot(indicators["date"], history["close"], color="#164e63", linewidth=1.8, label="收盘价")
        ax.plot(indicators["date"], indicators["boll_upper"], color="#ef4444", linewidth=1.1, label="上轨")
        ax.plot(indicators["date"], indicators["boll_mid"], color="#f59e0b", linewidth=1.1, label="中轨")
        ax.plot(indicators["date"], indicators["boll_lower"], color="#10b981", linewidth=1.1, label="下轨")
        ax.fill_between(indicators["date"], indicators["boll_lower"], indicators["boll_upper"], color="#dbeafe", alpha=0.18)
        ax.set_title(
            f"BOLL | 收 {float(history['close'].iloc[-1]):.3f}  上 {float(indicators['boll_upper'].iloc[-1]):.3f}  中 {float(indicators['boll_mid'].iloc[-1]):.3f}  下 {float(indicators['boll_lower'].iloc[-1]):.3f}",
            loc="left",
            fontsize=11.4,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=4)

    def _draw_obv_panel(self, ax: Any, indicators: Mapping[str, Any]) -> None:
        ax.plot(indicators["date"], indicators["obv"], color="#7c3aed", linewidth=1.8, label="OBV")
        ax.plot(indicators["date"], indicators["obv_ma"], color="#6b7280", linewidth=1.2, label="OBV MA20")
        ax.set_title(
            f"OBV | 今日 {float(indicators['obv'].iloc[-1]):.0f}  MA20 {float(indicators['obv_ma'].iloc[-1]):.0f}",
            loc="left",
            fontsize=12.0,
        )
        ax.legend(loc="upper left", frameon=False, fontsize=8)

    def _mark_crosses(self, ax: Any, dates: pd.Series, series_a: pd.Series, series_b: pd.Series) -> None:
        diff = (series_a - series_b).fillna(0)
        prev = diff.shift(1)
        golden = (prev <= 0) & (diff > 0)
        death = (prev >= 0) & (diff < 0)
        if golden.any():
            ax.scatter(dates[golden], series_a[golden], color="#16a34a", marker="^", s=48, zorder=6, label="金叉")
        if death.any():
            ax.scatter(dates[death], series_a[death], color="#dc2626", marker="v", s=48, zorder=6, label="死叉")

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
