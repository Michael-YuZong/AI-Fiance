"""Chart rendering for single-asset analysis reports."""

from __future__ import annotations

import os
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
from src.output.theme_playbook import build_theme_playbook_context
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import resolve_project_path

try:  # pragma: no cover - rendering dependency
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.patheffects as path_effects
    from matplotlib.path import Path as MplPath
    from matplotlib import transforms
    from matplotlib.patches import FancyBboxPatch, PathPatch, Rectangle
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    matplotlib = None
    mdates = None
    path_effects = None
    MplPath = None
    transforms = None
    FancyBboxPatch = None
    PathPatch = None
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


_DEFAULT_CHART_THEME = "institutional"


def _normalize_chart_theme(theme: str | None = None) -> str:
    candidate = (theme or os.getenv("AI_FINANCE_REPORT_THEME") or _DEFAULT_CHART_THEME).strip().lower()
    if candidate not in {"terminal", "abyss-gold", "institutional", "clinical", "erdtree", "neo-brutal"}:
        return _DEFAULT_CHART_THEME
    return candidate


def _render_chart_theme_variants_enabled() -> bool:
    candidate = str(os.getenv("AI_FINANCE_CHART_THEME_VARIANTS", "")).strip().lower()
    return candidate in {"1", "true", "yes", "all", "full"}


class AnalysisChartRenderer:
    """Render chart assets for a single analysis."""

    _DEFAULT_EXTENSION = ".svg"

    _THEME_PRESETS: Dict[str, Dict[str, str]] = {
        "terminal": {
            "_UP_BODY": "#ff8f8f",
            "_UP_EDGE": "#ff727f",
            "_DOWN_BODY": "#4ec9b0",
            "_DOWN_EDGE": "#33ae98",
            "_WICK": "#7e8ba0",
            "_MA20": "#f4c56a",
            "_MA60": "#8db7ff",
            "_MA10": "#f29f67",
            "_MA5": "#ffb570",
            "_GRID": "#334154",
            "_PANEL": "#1b2330",
            "_PAPER": "#121822",
            "_TEXT": "#eef6ff",
            "_MUTED": "#a4afbf",
            "_SOFT_BAR": "#263142",
            "_CARD_BG": "#1e2735",
            "_CARD_EDGE": "#314055",
            "_VOLUME_UP": "#ff8f8f",
            "_VOLUME_DOWN": "#4ec9b0",
            "_SUPPORT": "#59d0c2",
            "_SUPPORT_ZONE": "#173c39",
            "_RESISTANCE": "#ff9b7f",
            "_STOP": "#f4c56a",
            "_BOLL_BAND": "#223246",
            "_BOLL_EDGE": "#60748c",
            "_BOLL_UPPER": "#ff9b7f",
            "_BOLL_LOWER": "#59d0c2",
            "_BOLL_MID": "#f4c56a",
            "_ADX": "#d6dde8",
            "_PLUS_DI": "#59d0c2",
            "_MINUS_DI": "#ff9b7f",
            "_OBV": "#8db7ff",
            "_OBV_MA": "#f4c56a",
            "_AXIS_EDGE": "#314055",
            "_RELATIVE_LINE": "#8db7ff",
            "_BENCHMARK": "#8d99aa",
            "_REFERENCE_LINE": "#4b5a72",
            "_REL_FILL_POS": "#1e3c46",
            "_REL_FILL_NEG": "#2c2538",
            "_HEADER_BG": "#182230",
            "_HEADER_EDGE": "#314055",
            "_CHIP_TOP_FILL": "#202c3d",
            "_CHIP_TOP_EDGE": "#314055",
            "_CHIP_BOTTOM_FILL": "#1a2433",
            "_CHIP_BOTTOM_EDGE": "#2b3749",
            "_LAST_PRICE_DOT": "#ffb570",
            "_LAST_PRICE_TEXT": "#fff3d6",
            "_LAST_PRICE_BOX": "#2d2332",
            "_LAST_PRICE_BOX_EDGE": "#8d5b5b",
            "_SHORT_FILL": "#26343d",
            "_MACD_POS_HIST": "#59d0c2",
            "_MACD_NEG_HIST": "#ff9b7f",
            "_MACD_DIF": "#8db7ff",
            "_MACD_DEA": "#f4c56a",
            "_ZERO_LINE": "#4b5a72",
            "_KDJ_K": "#8db7ff",
            "_KDJ_D": "#f4c56a",
            "_KDJ_J": "#b99eff",
            "_OVERBOUGHT": "#f4c56a",
            "_OVERSOLD": "#59d0c2",
            "_RSI_BAND": "#2a3345",
            "_RSI_LINE": "#ff8f8f",
            "_BADGE_BULL_FILL": "#163a37",
            "_BADGE_BULL_EDGE": "#2c8e85",
            "_BADGE_BULL_TEXT": "#a7fff4",
            "_BADGE_BEAR_FILL": "#3a2327",
            "_BADGE_BEAR_EDGE": "#9b565b",
            "_BADGE_BEAR_TEXT": "#ffd7da",
            "_BADGE_WARN_FILL": "#3a321e",
            "_BADGE_WARN_EDGE": "#b48b34",
            "_BADGE_WARN_TEXT": "#ffe39f",
            "_BADGE_NEUTRAL_FILL": "#242d39",
            "_BADGE_NEUTRAL_EDGE": "#445063",
            "_BADGE_NEUTRAL_TEXT": "#d6dde8",
            "_CROSS_GOLD": "#59d0c2",
            "_CROSS_DEATH": "#ff8f8f",
            "_SCORE_HIGH": "#59d0c2",
            "_SCORE_MED": "#f4c56a",
            "_SCORE_LOW": "#ff8f8f",
            "_LEVEL_TEXT": "#eef6ff",
        },
        "abyss-gold": {
            "_UP_BODY": "#b86a73",
            "_UP_EDGE": "#9b565b",
            "_DOWN_BODY": "#5d947c",
            "_DOWN_EDGE": "#4a7a66",
            "_WICK": "#8f8579",
            "_MA20": "#c5a059",
            "_MA60": "#7e9d95",
            "_MA10": "#d2ae66",
            "_MA5": "#e0bb72",
            "_GRID": "#312a24",
            "_PANEL": "#181512",
            "_PAPER": "#0f1013",
            "_TEXT": "#f3e7ca",
            "_MUTED": "#aba091",
            "_SOFT_BAR": "#26211d",
            "_CARD_BG": "#1c1916",
            "_CARD_EDGE": "#3a3028",
            "_VOLUME_UP": "#b86a73",
            "_VOLUME_DOWN": "#5d947c",
            "_SUPPORT": "#6fa08c",
            "_SUPPORT_ZONE": "#192822",
            "_RESISTANCE": "#c98473",
            "_STOP": "#c5a059",
            "_BOLL_BAND": "#231f1b",
            "_BOLL_EDGE": "#6d655a",
            "_BOLL_UPPER": "#b8877e",
            "_BOLL_LOWER": "#6b9787",
            "_BOLL_MID": "#c5a059",
            "_ADX": "#cdc4b6",
            "_PLUS_DI": "#6fa08c",
            "_MINUS_DI": "#b86a73",
            "_OBV": "#9eb7af",
            "_OBV_MA": "#c5a059",
            "_AXIS_EDGE": "#3a3028",
            "_RELATIVE_LINE": "#d8c89f",
            "_BENCHMARK": "#857a6e",
            "_REFERENCE_LINE": "#5f564d",
            "_REL_FILL_POS": "#20261f",
            "_REL_FILL_NEG": "#2b1f20",
            "_HEADER_BG": "#161411",
            "_HEADER_EDGE": "#3a3028",
            "_CHIP_TOP_FILL": "#1c1a16",
            "_CHIP_TOP_EDGE": "#3a3028",
            "_CHIP_BOTTOM_FILL": "#181612",
            "_CHIP_BOTTOM_EDGE": "#332a23",
            "_LAST_PRICE_DOT": "#f0d49a",
            "_LAST_PRICE_TEXT": "#fff4dc",
            "_LAST_PRICE_BOX": "#2b2218",
            "_LAST_PRICE_BOX_EDGE": "#7f6849",
            "_SHORT_FILL": "#2a251f",
            "_MACD_POS_HIST": "#6fa08c",
            "_MACD_NEG_HIST": "#b86a73",
            "_MACD_DIF": "#d8c89f",
            "_MACD_DEA": "#c5a059",
            "_ZERO_LINE": "#5f564d",
            "_KDJ_K": "#d8c89f",
            "_KDJ_D": "#c5a059",
            "_KDJ_J": "#9e87ba",
            "_OVERBOUGHT": "#c5a059",
            "_OVERSOLD": "#6fa08c",
            "_RSI_BAND": "#26211d",
            "_RSI_LINE": "#b86a73",
            "_BADGE_BULL_FILL": "#1b2a24",
            "_BADGE_BULL_EDGE": "#4f7b6c",
            "_BADGE_BULL_TEXT": "#d6efe6",
            "_BADGE_BEAR_FILL": "#2d1f21",
            "_BADGE_BEAR_EDGE": "#8d4e55",
            "_BADGE_BEAR_TEXT": "#f0d4d7",
            "_BADGE_WARN_FILL": "#332a1d",
            "_BADGE_WARN_EDGE": "#a07b39",
            "_BADGE_WARN_TEXT": "#f3d79f",
            "_BADGE_NEUTRAL_FILL": "#211d18",
            "_BADGE_NEUTRAL_EDGE": "#4a4037",
            "_BADGE_NEUTRAL_TEXT": "#d8cfc2",
            "_CROSS_GOLD": "#6fa08c",
            "_CROSS_DEATH": "#b86a73",
            "_SCORE_HIGH": "#6fa08c",
            "_SCORE_MED": "#c5a059",
            "_SCORE_LOW": "#b86a73",
            "_LEVEL_TEXT": "#fff4dc",
        },
        "institutional": {
            "_UP_BODY": "#ff7d7d",
            "_UP_EDGE": "#ff6060",
            "_DOWN_BODY": "#39ff14",
            "_DOWN_EDGE": "#25d50c",
            "_WICK": "#8c96a4",
            "_MA20": "#ffbf00",
            "_MA60": "#8fc5ff",
            "_MA10": "#ffd65a",
            "_MA5": "#ffe082",
            "_GRID": "#1d232b",
            "_PANEL": "#05080c",
            "_PAPER": "#000000",
            "_TEXT": "#f3f6fb",
            "_MUTED": "#a4adb9",
            "_SOFT_BAR": "#11161d",
            "_CARD_BG": "#06090d",
            "_CARD_EDGE": "#222831",
            "_VOLUME_UP": "#ff7d7d",
            "_VOLUME_DOWN": "#39ff14",
            "_SUPPORT": "#39ff14",
            "_SUPPORT_ZONE": "#0b2410",
            "_RESISTANCE": "#ffbf00",
            "_STOP": "#ffd65a",
            "_BOLL_BAND": "#0f141b",
            "_BOLL_EDGE": "#4f5967",
            "_BOLL_UPPER": "#ff7d7d",
            "_BOLL_LOWER": "#39ff14",
            "_BOLL_MID": "#ffbf00",
            "_ADX": "#d6dbe2",
            "_PLUS_DI": "#39ff14",
            "_MINUS_DI": "#ff7d7d",
            "_OBV": "#8fc5ff",
            "_OBV_MA": "#ffbf00",
            "_AXIS_EDGE": "#222831",
            "_RELATIVE_LINE": "#ffbf00",
            "_BENCHMARK": "#6f7a87",
            "_REFERENCE_LINE": "#39414b",
            "_REL_FILL_POS": "#11240f",
            "_REL_FILL_NEG": "#231516",
            "_HEADER_BG": "#05080c",
            "_HEADER_EDGE": "#222831",
            "_CHIP_TOP_FILL": "#0b1015",
            "_CHIP_TOP_EDGE": "#222831",
            "_CHIP_BOTTOM_FILL": "#090d11",
            "_CHIP_BOTTOM_EDGE": "#1a2028",
            "_LAST_PRICE_DOT": "#ffbf00",
            "_LAST_PRICE_TEXT": "#fff1be",
            "_LAST_PRICE_BOX": "#23190a",
            "_LAST_PRICE_BOX_EDGE": "#6b5412",
            "_SHORT_FILL": "#161411",
            "_MACD_POS_HIST": "#39ff14",
            "_MACD_NEG_HIST": "#ff7d7d",
            "_MACD_DIF": "#8fc5ff",
            "_MACD_DEA": "#ffbf00",
            "_ZERO_LINE": "#39414b",
            "_KDJ_K": "#8fc5ff",
            "_KDJ_D": "#ffbf00",
            "_KDJ_J": "#c4a0ff",
            "_OVERBOUGHT": "#ffbf00",
            "_OVERSOLD": "#39ff14",
            "_RSI_BAND": "#12171d",
            "_RSI_LINE": "#ff7d7d",
            "_BADGE_BULL_FILL": "#0f2710",
            "_BADGE_BULL_EDGE": "#1c8f13",
            "_BADGE_BULL_TEXT": "#b8ffab",
            "_BADGE_BEAR_FILL": "#2a1212",
            "_BADGE_BEAR_EDGE": "#c64747",
            "_BADGE_BEAR_TEXT": "#ffd0d0",
            "_BADGE_WARN_FILL": "#2a220b",
            "_BADGE_WARN_EDGE": "#8f6f11",
            "_BADGE_WARN_TEXT": "#ffe082",
            "_BADGE_NEUTRAL_FILL": "#11161d",
            "_BADGE_NEUTRAL_EDGE": "#3b4654",
            "_BADGE_NEUTRAL_TEXT": "#d6dbe2",
            "_CROSS_GOLD": "#39ff14",
            "_CROSS_DEATH": "#ff7d7d",
            "_SCORE_HIGH": "#39ff14",
            "_SCORE_MED": "#ffbf00",
            "_SCORE_LOW": "#ff7d7d",
            "_LEVEL_TEXT": "#fff4cc",
        },
        "clinical": {
            "_UP_BODY": "#e03a3e",
            "_UP_EDGE": "#c92f34",
            "_DOWN_BODY": "#2aa876",
            "_DOWN_EDGE": "#218960",
            "_WICK": "#7b8ba0",
            "_MA20": "#0071e3",
            "_MA60": "#4f8ef7",
            "_MA10": "#78aef9",
            "_MA5": "#9dc4ff",
            "_GRID": "#d6deea",
            "_PANEL": "#f5f8fd",
            "_PAPER": "#fbfbfd",
            "_TEXT": "#1d1d1f",
            "_MUTED": "#67768c",
            "_SOFT_BAR": "#e8eef6",
            "_CARD_BG": "#ffffff",
            "_CARD_EDGE": "#d4dfec",
            "_VOLUME_UP": "#e03a3e",
            "_VOLUME_DOWN": "#2aa876",
            "_SUPPORT": "#0071e3",
            "_SUPPORT_ZONE": "#dfeeff",
            "_RESISTANCE": "#e03a3e",
            "_STOP": "#d28a16",
            "_BOLL_BAND": "#edf3fa",
            "_BOLL_EDGE": "#97a9be",
            "_BOLL_UPPER": "#e03a3e",
            "_BOLL_LOWER": "#2aa876",
            "_BOLL_MID": "#0071e3",
            "_ADX": "#31465f",
            "_PLUS_DI": "#0071e3",
            "_MINUS_DI": "#e03a3e",
            "_OBV": "#4f8ef7",
            "_OBV_MA": "#0071e3",
            "_AXIS_EDGE": "#c8d4e2",
            "_RELATIVE_LINE": "#0071e3",
            "_BENCHMARK": "#8d9daf",
            "_REFERENCE_LINE": "#b8c6d5",
            "_REL_FILL_POS": "#ddecff",
            "_REL_FILL_NEG": "#f8dddf",
            "_HEADER_BG": "#ffffff",
            "_HEADER_EDGE": "#d4dfec",
            "_CHIP_TOP_FILL": "#ffffff",
            "_CHIP_TOP_EDGE": "#d4dfec",
            "_CHIP_BOTTOM_FILL": "#f3f7fc",
            "_CHIP_BOTTOM_EDGE": "#d9e3ef",
            "_LAST_PRICE_DOT": "#0071e3",
            "_LAST_PRICE_TEXT": "#ffffff",
            "_LAST_PRICE_BOX": "#0c4f9e",
            "_LAST_PRICE_BOX_EDGE": "#0c4f9e",
            "_SHORT_FILL": "#e8f1fb",
            "_MACD_POS_HIST": "#2aa876",
            "_MACD_NEG_HIST": "#e03a3e",
            "_MACD_DIF": "#0071e3",
            "_MACD_DEA": "#d28a16",
            "_ZERO_LINE": "#b8c6d5",
            "_KDJ_K": "#0071e3",
            "_KDJ_D": "#d28a16",
            "_KDJ_J": "#7a6df0",
            "_OVERBOUGHT": "#d28a16",
            "_OVERSOLD": "#2aa876",
            "_RSI_BAND": "#edf3fa",
            "_RSI_LINE": "#e03a3e",
            "_BADGE_BULL_FILL": "#e3f0ff",
            "_BADGE_BULL_EDGE": "#0071e3",
            "_BADGE_BULL_TEXT": "#0c4f9e",
            "_BADGE_BEAR_FILL": "#ffe5e6",
            "_BADGE_BEAR_EDGE": "#e03a3e",
            "_BADGE_BEAR_TEXT": "#8c1f23",
            "_BADGE_WARN_FILL": "#fff1d8",
            "_BADGE_WARN_EDGE": "#d28a16",
            "_BADGE_WARN_TEXT": "#8a5d08",
            "_BADGE_NEUTRAL_FILL": "#f2f5fa",
            "_BADGE_NEUTRAL_EDGE": "#b9c7d8",
            "_BADGE_NEUTRAL_TEXT": "#31465f",
            "_CROSS_GOLD": "#0071e3",
            "_CROSS_DEATH": "#e03a3e",
            "_SCORE_HIGH": "#0071e3",
            "_SCORE_MED": "#d28a16",
            "_SCORE_LOW": "#e03a3e",
            "_LEVEL_TEXT": "#0c4f9e",
        },
        "erdtree": {
            "_UP_BODY": "#9e1a1a",
            "_UP_EDGE": "#851616",
            "_DOWN_BODY": "#6a845d",
            "_DOWN_EDGE": "#586f4d",
            "_WICK": "#8b7d6e",
            "_MA20": "#d4af37",
            "_MA60": "#8f9b72",
            "_MA10": "#c39a2f",
            "_MA5": "#e0bd58",
            "_GRID": "#e3d8c3",
            "_PANEL": "#f8f1e5",
            "_PAPER": "#fdfbf7",
            "_TEXT": "#3e352c",
            "_MUTED": "#827464",
            "_SOFT_BAR": "#eee4d5",
            "_CARD_BG": "#fffaf1",
            "_CARD_EDGE": "#dfd0b5",
            "_VOLUME_UP": "#9e1a1a",
            "_VOLUME_DOWN": "#6a845d",
            "_SUPPORT": "#d4af37",
            "_SUPPORT_ZONE": "#f7edd0",
            "_RESISTANCE": "#9e1a1a",
            "_STOP": "#b98915",
            "_BOLL_BAND": "#f3eadb",
            "_BOLL_EDGE": "#b09d80",
            "_BOLL_UPPER": "#9e1a1a",
            "_BOLL_LOWER": "#6a845d",
            "_BOLL_MID": "#d4af37",
            "_ADX": "#56483c",
            "_PLUS_DI": "#6a845d",
            "_MINUS_DI": "#9e1a1a",
            "_OBV": "#7f6d42",
            "_OBV_MA": "#d4af37",
            "_AXIS_EDGE": "#d7c8ae",
            "_RELATIVE_LINE": "#d4af37",
            "_BENCHMARK": "#9b8b77",
            "_REFERENCE_LINE": "#c4b598",
            "_REL_FILL_POS": "#edf0e2",
            "_REL_FILL_NEG": "#f6e1dd",
            "_HEADER_BG": "#fffaf1",
            "_HEADER_EDGE": "#dfd0b5",
            "_CHIP_TOP_FILL": "#fff8ed",
            "_CHIP_TOP_EDGE": "#dfd0b5",
            "_CHIP_BOTTOM_FILL": "#f6eee0",
            "_CHIP_BOTTOM_EDGE": "#decfb4",
            "_LAST_PRICE_DOT": "#d4af37",
            "_LAST_PRICE_TEXT": "#fff7df",
            "_LAST_PRICE_BOX": "#8f6912",
            "_LAST_PRICE_BOX_EDGE": "#8f6912",
            "_SHORT_FILL": "#f1e6d3",
            "_MACD_POS_HIST": "#6a845d",
            "_MACD_NEG_HIST": "#9e1a1a",
            "_MACD_DIF": "#7f6d42",
            "_MACD_DEA": "#d4af37",
            "_ZERO_LINE": "#c4b598",
            "_KDJ_K": "#7f6d42",
            "_KDJ_D": "#d4af37",
            "_KDJ_J": "#8d70c7",
            "_OVERBOUGHT": "#d4af37",
            "_OVERSOLD": "#6a845d",
            "_RSI_BAND": "#f3eadb",
            "_RSI_LINE": "#9e1a1a",
            "_BADGE_BULL_FILL": "#eef2e5",
            "_BADGE_BULL_EDGE": "#6a845d",
            "_BADGE_BULL_TEXT": "#46593c",
            "_BADGE_BEAR_FILL": "#f8e5e0",
            "_BADGE_BEAR_EDGE": "#9e1a1a",
            "_BADGE_BEAR_TEXT": "#6c1414",
            "_BADGE_WARN_FILL": "#fff1d7",
            "_BADGE_WARN_EDGE": "#d4af37",
            "_BADGE_WARN_TEXT": "#7c5b0b",
            "_BADGE_NEUTRAL_FILL": "#f6efe4",
            "_BADGE_NEUTRAL_EDGE": "#c7b89d",
            "_BADGE_NEUTRAL_TEXT": "#56483c",
            "_CROSS_GOLD": "#d4af37",
            "_CROSS_DEATH": "#9e1a1a",
            "_SCORE_HIGH": "#6a845d",
            "_SCORE_MED": "#d4af37",
            "_SCORE_LOW": "#9e1a1a",
            "_LEVEL_TEXT": "#7c5b0b",
        },
        "neo-brutal": {
            "_UP_BODY": "#ff5a5f",
            "_UP_EDGE": "#000000",
            "_DOWN_BODY": "#00c853",
            "_DOWN_EDGE": "#000000",
            "_WICK": "#000000",
            "_MA20": "#00a1d6",
            "_MA60": "#ff8ba7",
            "_MA10": "#004bde",
            "_MA5": "#000000",
            "_GRID": "#d7d7df",
            "_PANEL": "#ffffff",
            "_PAPER": "#ffffff",
            "_TEXT": "#000000",
            "_MUTED": "#4f4f4f",
            "_SOFT_BAR": "#f6f6fb",
            "_CARD_BG": "#ffffff",
            "_CARD_EDGE": "#000000",
            "_VOLUME_UP": "#ff5a5f",
            "_VOLUME_DOWN": "#00c853",
            "_SUPPORT": "#00a1d6",
            "_SUPPORT_ZONE": "#d9f6ff",
            "_RESISTANCE": "#ff8ba7",
            "_STOP": "#000000",
            "_BOLL_BAND": "#f7f7fb",
            "_BOLL_EDGE": "#000000",
            "_BOLL_UPPER": "#ff5a5f",
            "_BOLL_LOWER": "#00c853",
            "_BOLL_MID": "#00a1d6",
            "_ADX": "#000000",
            "_PLUS_DI": "#00a1d6",
            "_MINUS_DI": "#ff5a5f",
            "_OBV": "#004bde",
            "_OBV_MA": "#ff8ba7",
            "_AXIS_EDGE": "#000000",
            "_RELATIVE_LINE": "#00a1d6",
            "_BENCHMARK": "#666666",
            "_REFERENCE_LINE": "#bcbcc8",
            "_REL_FILL_POS": "#dcf6ff",
            "_REL_FILL_NEG": "#ffe1ea",
            "_HEADER_BG": "#ffffff",
            "_HEADER_EDGE": "#000000",
            "_CHIP_TOP_FILL": "#ffffff",
            "_CHIP_TOP_EDGE": "#000000",
            "_CHIP_BOTTOM_FILL": "#fafafc",
            "_CHIP_BOTTOM_EDGE": "#000000",
            "_LAST_PRICE_DOT": "#00a1d6",
            "_LAST_PRICE_TEXT": "#000000",
            "_LAST_PRICE_BOX": "#ffde59",
            "_LAST_PRICE_BOX_EDGE": "#000000",
            "_SHORT_FILL": "#eefbff",
            "_MACD_POS_HIST": "#00c853",
            "_MACD_NEG_HIST": "#ff5a5f",
            "_MACD_DIF": "#00a1d6",
            "_MACD_DEA": "#ff8ba7",
            "_ZERO_LINE": "#000000",
            "_KDJ_K": "#00a1d6",
            "_KDJ_D": "#ff8ba7",
            "_KDJ_J": "#6b5cff",
            "_OVERBOUGHT": "#ff8ba7",
            "_OVERSOLD": "#00c853",
            "_RSI_BAND": "#f7f7fb",
            "_RSI_LINE": "#ff5a5f",
            "_BADGE_BULL_FILL": "#dcf6ff",
            "_BADGE_BULL_EDGE": "#000000",
            "_BADGE_BULL_TEXT": "#000000",
            "_BADGE_BEAR_FILL": "#ffe1ea",
            "_BADGE_BEAR_EDGE": "#000000",
            "_BADGE_BEAR_TEXT": "#000000",
            "_BADGE_WARN_FILL": "#fff1a8",
            "_BADGE_WARN_EDGE": "#000000",
            "_BADGE_WARN_TEXT": "#000000",
            "_BADGE_NEUTRAL_FILL": "#ffffff",
            "_BADGE_NEUTRAL_EDGE": "#000000",
            "_BADGE_NEUTRAL_TEXT": "#000000",
            "_CROSS_GOLD": "#00a1d6",
            "_CROSS_DEATH": "#ff5a5f",
            "_SCORE_HIGH": "#00a1d6",
            "_SCORE_MED": "#ff8ba7",
            "_SCORE_LOW": "#ff5a5f",
            "_LEVEL_TEXT": "#000000",
        },
    }

    def __init__(
        self,
        output_dir: str = "reports/assets",
        theme: str | None = None,
        *,
        render_theme_variants: bool | None = None,
    ) -> None:
        self.output_dir = resolve_project_path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.enabled = plt is not None
        self.theme = _normalize_chart_theme(theme)
        self.render_theme_variants = (
            _render_chart_theme_variants_enabled()
            if render_theme_variants is None
            else bool(render_theme_variants)
        )
        if self.enabled:
            self._apply_theme(self.theme)
            self._configure_style()

    def _apply_theme(self, theme_name: str) -> None:
        normalized = _normalize_chart_theme(theme_name)
        self.theme = normalized
        preset = self._THEME_PRESETS[normalized]
        for key, value in preset.items():
            setattr(self, key, value)

    def _asset_path(self, base: str, kind: str, *, theme: str | None = None) -> Path:
        suffix = self._DEFAULT_EXTENSION
        if theme:
            return self.output_dir / f"{base}_{kind}.theme-{_normalize_chart_theme(theme)}{suffix}"
        return self.output_dir / f"{base}_{kind}{suffix}"

    def render(self, analysis: Mapping[str, Any]) -> Dict[str, str]:
        if not self.enabled:
            return {}

        history = analysis.get("history")
        if not isinstance(history, pd.DataFrame) or history.empty:
            return {}

        symbol = str(analysis.get("symbol", "asset"))
        stamp = str(analysis.get("generated_at", "")).replace(":", "-").replace(" ", "_")
        base = f"{symbol}_{stamp[:19] or 'latest'}"
        if self._is_history_fallback(analysis):
            dashboard_path = self._asset_path(base, "dashboard")
            self._render_snapshot_dashboard(analysis, history.copy(), dashboard_path)
            if not self.render_theme_variants:
                return {
                    "dashboard": str(dashboard_path.resolve()),
                    "mode": "snapshot_fallback",
                    "note": self._fallback_visual_note(analysis),
                }
            active_theme = self.theme
            for variant_theme in self._THEME_PRESETS:
                variant_dashboard = self._asset_path(base, "dashboard", theme=variant_theme)
                if variant_theme == active_theme:
                    if dashboard_path != variant_dashboard:
                        variant_dashboard.write_bytes(dashboard_path.read_bytes())
                    continue
                self._apply_theme(variant_theme)
                self._configure_style()
                self._render_snapshot_dashboard(analysis, history.copy(), variant_dashboard)
            self._apply_theme(active_theme)
            self._configure_style()
            return {
                "dashboard": str(dashboard_path.resolve()),
                "mode": "snapshot_fallback",
                "note": self._fallback_visual_note(analysis),
            }
        dashboard_path = self._asset_path(base, "dashboard")
        windows_path = self._asset_path(base, "windows")
        indicators_path = self._asset_path(base, "indicators")
        self._render_dashboard(analysis, history.copy(), dashboard_path)
        self._render_windows(analysis, history.copy(), windows_path)
        self._render_indicators(analysis, history.copy(), indicators_path)

        if not self.render_theme_variants:
            return {
                "dashboard": str(dashboard_path.resolve()),
                "windows": str(windows_path.resolve()),
                "indicators": str(indicators_path.resolve()),
            }

        active_theme = self.theme
        for variant_theme in self._THEME_PRESETS:
            if variant_theme == active_theme:
                variant_dashboard = self._asset_path(base, "dashboard", theme=variant_theme)
                variant_windows = self._asset_path(base, "windows", theme=variant_theme)
                variant_indicators = self._asset_path(base, "indicators", theme=variant_theme)
                if dashboard_path != variant_dashboard:
                    variant_dashboard.write_bytes(dashboard_path.read_bytes())
                if windows_path != variant_windows:
                    variant_windows.write_bytes(windows_path.read_bytes())
                if indicators_path != variant_indicators:
                    variant_indicators.write_bytes(indicators_path.read_bytes())
                continue
            self._apply_theme(variant_theme)
            self._configure_style()
            self._render_dashboard(analysis, history.copy(), self._asset_path(base, "dashboard", theme=variant_theme))
            self._render_windows(analysis, history.copy(), self._asset_path(base, "windows", theme=variant_theme))
            self._render_indicators(analysis, history.copy(), self._asset_path(base, "indicators", theme=variant_theme))
        self._apply_theme(active_theme)
        self._configure_style()
        return {
            "dashboard": str(dashboard_path.resolve()),
            "windows": str(windows_path.resolve()),
            "indicators": str(indicators_path.resolve()),
        }

    def _fallback_visual_note(self, analysis: Mapping[str, Any]) -> str:
        return "完整日线当前不可用，图表已降级为本地快照卡；图形只用于方向参考，不当成完整历史复盘。"

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
        plt.rcParams["figure.facecolor"] = "none"
        plt.rcParams["axes.facecolor"] = "none"
        plt.rcParams["savefig.facecolor"] = "none"
        plt.rcParams["savefig.edgecolor"] = "none"
        plt.rcParams["axes.edgecolor"] = self._AXIS_EDGE
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

        fig = plt.figure(figsize=(14.2, 10.25), dpi=170)
        fig.subplots_adjust(top=0.972, bottom=0.058, left=0.038, right=0.962)
        grid = fig.add_gridspec(3, 2, height_ratios=[0.52, 1.04, 0.60], hspace=0.21, wspace=0.20)
        header_grid = grid[0, :].subgridspec(1, 2, width_ratios=[0.70, 0.30], wspace=0.05)
        ax_header_left = fig.add_subplot(header_grid[0, 0])
        ax_header_right = fig.add_subplot(header_grid[0, 1])
        gs_price = grid[1, 0].subgridspec(2, 1, height_ratios=[4.9, 1.05], hspace=0.04)
        ax_price = fig.add_subplot(gs_price[0, 0])
        ax_vol = fig.add_subplot(gs_price[1, 0], sharex=ax_price)
        ax_relative = fig.add_subplot(grid[1, 1])
        ax_scores = fig.add_subplot(grid[2, :])

        self._draw_dashboard_header(ax_header_left, ax_header_right, analysis)
        self._draw_price_panel(ax_price, ax_vol, analysis, prepared, ma20, ma60, price, support_low, support_high)
        self._draw_relative_panel(ax_relative, analysis, prepared, benchmark_prepared)
        self._draw_score_panel(ax_scores, analysis)
        fig.savefig(path, bbox_inches="tight", transparent=True)
        plt.close(fig)

    def _render_snapshot_dashboard(self, analysis: Mapping[str, Any], history: pd.DataFrame, path: Path) -> None:
        prepared = self._prepare_history(history)
        if prepared.empty:
            return
        technical = dict(analysis.get("technical_raw", {}))
        ma = technical.get("ma_system", {}).get("mas", {})
        fib = technical.get("fibonacci", {}).get("levels", {})
        price = float(prepared["close"].iloc[-1])
        support_low = self._first_positive(float(fib.get("0.500", 0.0)), float(ma.get("MA60", 0.0)), float(prepared["low"].tail(10).min()))
        support_high = self._first_positive(float(fib.get("0.618", 0.0)), float(ma.get("MA20", 0.0)), support_low)

        fig = plt.figure(figsize=(14.2, 9.2), dpi=170)
        fig.subplots_adjust(top=0.972, bottom=0.060, left=0.040, right=0.960)
        grid = fig.add_gridspec(3, 2, height_ratios=[0.56, 0.72, 0.66], hspace=0.23, wspace=0.20)
        header_grid = grid[0, :].subgridspec(1, 2, width_ratios=[0.70, 0.30], wspace=0.05)
        ax_header_left = fig.add_subplot(header_grid[0, 0])
        ax_header_right = fig.add_subplot(header_grid[0, 1])
        ax_snapshot = fig.add_subplot(grid[1, 0])
        ax_status = fig.add_subplot(grid[1, 1])
        ax_scores = fig.add_subplot(grid[2, :])

        self._draw_dashboard_header(ax_header_left, ax_header_right, analysis)
        self._draw_snapshot_price_card(ax_snapshot, analysis, prepared, price, support_low, support_high)
        self._draw_snapshot_status_card(ax_status, analysis, prepared)
        self._draw_score_panel(ax_scores, analysis)
        fig.savefig(path, bbox_inches="tight", transparent=True)
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

        fig.savefig(path, bbox_inches="tight", transparent=True)
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

        fig = plt.figure(figsize=(14.4, 10.7), dpi=170)
        fig.subplots_adjust(top=0.936, bottom=0.064, left=0.072, right=0.966, hspace=0.36, wspace=0.24)
        grid = fig.add_gridspec(3, 2)
        axes = [fig.add_subplot(grid[i, j]) for i in range(3) for j in range(2)]

        close_series = plot_window["close"].reset_index(drop=True)

        self._draw_macd_panel(axes[0], indicators, close_series, divergence)
        self._draw_kdj_panel(axes[1], indicators)
        self._draw_rsi_panel(axes[2], indicators, close_series, divergence)
        self._draw_boll_panel(axes[3], plot_window, indicators)
        self._draw_adx_panel(axes[4], indicators)
        self._draw_obv_panel(axes[5], indicators, close_series, divergence)

        fig.savefig(path, bbox_inches="tight", transparent=True)
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

    def _is_light_theme(self) -> bool:
        return self.theme in {"clinical", "erdtree", "neo-brutal"}

    def _panel_surface(self, tone: str = "soft") -> str:
        if not self._is_light_theme():
            return "none"
        if tone == "main":
            return getattr(self, "_CARD_BG", self._HEADER_BG)
        if tone == "muted":
            return getattr(self, "_SOFT_BAR", self._PANEL)
        return self._PANEL

    def _apply_line_depth(self, artist: Any, *, emphasis: str = "normal") -> Any:
        if path_effects is None or not self._is_light_theme():
            return artist
        linewidth = float(getattr(artist, "get_linewidth", lambda: 1.6)())
        stroke_width = linewidth + (1.25 if emphasis == "primary" else 0.75)
        stroke_alpha = 0.12 if self.theme != "neo-brutal" else 0.22
        artist.set_path_effects(
            [
                path_effects.Stroke(linewidth=stroke_width, foreground=(0.0, 0.0, 0.0, stroke_alpha)),
                path_effects.Normal(),
            ]
        )
        return artist

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
        self._style_axis(ax, panel_tone="main")
        self._style_volume_axis(ax_vol, panel_tone="muted")
        self._draw_candles(ax, panel_history, width=0.70)
        if isinstance(volume, pd.Series) and volume.notna().any():
            self._draw_volume_panel(ax_vol, panel_history)
        ma20_line = ax.plot(
            panel_history["date"],
            panel_ma20,
            color=self._MA20,
            linewidth=self._dashboard_line(1.85),
            alpha=0.98,
            label="MA20",
            zorder=4,
        )[0]
        ma60_line = ax.plot(
            panel_history["date"],
            panel_ma60,
            color=self._MA60,
            linewidth=self._dashboard_line(1.85),
            alpha=0.98,
            label="MA60",
            zorder=4,
        )[0]
        self._apply_line_depth(ma20_line, emphasis="primary")
        self._apply_line_depth(ma60_line, emphasis="primary")
        if support_low > 0 and support_high > 0 and max(support_low, support_high) >= panel_history["low"].min() * 0.95:
            lower = min(support_low, support_high)
            upper = max(support_low, support_high)
            ax.axhspan(lower, upper, color=self._SUPPORT_ZONE, alpha=0.28, zorder=1)
        self._apply_price_ylim(ax, panel_history, level_guides)
        self._extend_right_gutter(ax, panel_history, ratio=0.23, min_days=22)
        placed_levels = self._draw_price_levels(ax, panel_history, level_guides)
        ax.scatter(panel_history["date"].iloc[-1], price, color=self._LAST_PRICE_DOT, s=self._dashboard_marker(30), zorder=6)
        self._draw_current_price_callout(ax, panel_history, price, placed_levels)
        ax.set_title("近3月价格结构 / K线", loc="left", fontsize=self._dashboard_font(12.5), color=self._TEXT, pad=12)
        ax.legend(loc="upper left", ncol=2, fontsize=self._dashboard_font(8.0))
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
        ax_vol.spines["bottom"].set_color(self._AXIS_EDGE)

    def _draw_relative_panel(
        self,
        ax: Any,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        benchmark_history: Optional[pd.DataFrame],
    ) -> None:
        base = history.tail(120).copy()
        base["norm"] = base["close"] / float(base["close"].iloc[0]) * 100
        self._style_axis(ax, panel_tone="soft")
        relative_line = ax.plot(
            base["date"],
            base["norm"],
            color=self._RELATIVE_LINE,
            linewidth=self._dashboard_line(2.65),
            alpha=0.97,
            label=str(analysis.get("symbol", "标的")),
        )[0]
        self._apply_line_depth(relative_line, emphasis="primary")
        if benchmark_history is not None and not benchmark_history.empty:
            bench = benchmark_history.tail(120).copy()
            merged = pd.merge(base[["date", "norm"]], bench[["date", "close"]], on="date", how="inner")
            if not merged.empty:
                merged["bench_norm"] = merged["close"] / float(merged["close"].iloc[0]) * 100
                bench_line = ax.plot(
                    merged["date"],
                    merged["bench_norm"],
                    color=self._BENCHMARK,
                    linewidth=self._dashboard_line(1.7),
                    linestyle=(0, (4, 2)),
                    alpha=0.80,
                    label=str(analysis.get("benchmark_name", "基准")),
                )[0]
                self._apply_line_depth(bench_line)
        ax.axhline(100, color=self._REFERENCE_LINE, linewidth=self._dashboard_line(1.0))
        ax.fill_between(base["date"], 100, base["norm"], where=base["norm"] >= 100, color=self._REL_FILL_POS, alpha=0.24)
        ax.fill_between(base["date"], 100, base["norm"], where=base["norm"] < 100, color=self._REL_FILL_NEG, alpha=0.20)
        ax.set_title("相对强弱 / 归一化走势", loc="left", fontsize=self._dashboard_font(12.5), color=self._TEXT, pad=12)
        self._format_date_axis(ax, base["date"])
        ax.set_ylabel("")

    def _draw_score_panel(self, ax: Any, analysis: Mapping[str, Any]) -> None:
        ax.set_facecolor(self._panel_surface("soft"))
        ax.set_axis_off()
        ax.set_title("八维评分", loc="left", fontsize=self._dashboard_font(12.8), color=self._TEXT, pad=12)

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
        left_margin = 0.018
        right_margin = 0.018
        top_margin = 0.10
        bottom_margin = 0.055
        h_gap = 0.036
        v_gap = 0.15
        card_w = (1 - left_margin - right_margin - h_gap * (cols - 1)) / cols
        card_h = (1 - top_margin - bottom_margin - v_gap * (rows - 1)) / rows

        for idx, card in enumerate(cards):
            row = idx // cols
            col = idx % cols
            x0 = left_margin + col * (card_w + h_gap)
            y0 = 1 - top_margin - (row + 1) * card_h - row * v_gap

            card_face = self._score_card_fill()
            card_edge = self._score_card_edge()
            if FancyBboxPatch is not None:
                rect = FancyBboxPatch(
                    (x0, y0),
                    card_w,
                    card_h,
                    boxstyle="round,pad=0.006,rounding_size=0.028",
                    transform=ax.transAxes,
                    facecolor=card_face,
                    edgecolor=card_edge,
                    linewidth=self._dashboard_line(0.88),
                    zorder=1,
                )
            else:
                rect = Rectangle(
                    (x0, y0),
                    card_w,
                    card_h,
                    transform=ax.transAxes,
                    facecolor=card_face,
                    edgecolor=card_edge,
                    linewidth=self._dashboard_line(0.88),
                    zorder=1,
                )
            ax.add_patch(rect)

            ax.text(
                x0 + 0.05 * card_w,
                y0 + 0.79 * card_h,
                card["label"],
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=self._dashboard_font(8.9),
                color=self._MUTED,
                zorder=3,
            )
            ax.text(
                x0 + 0.05 * card_w,
                y0 + 0.52 * card_h,
                card["raw"],
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=self._dashboard_font(12.2),
                fontweight="semibold",
                color=self._TEXT,
                zorder=3,
            )

            track_x = x0 + 0.05 * card_w
            track_y = y0 + 0.15 * card_h
            track_w = 0.90 * card_w
            track_h = 0.132 * card_h

            track = Rectangle(
                (track_x, track_y),
                track_w,
                track_h,
                transform=ax.transAxes,
                facecolor=self._score_track_fill(),
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
                y0 + 0.50 * card_h,
                f"{card['normalized']:.0f}",
                transform=ax.transAxes,
                ha="right",
                va="center",
                fontsize=self._dashboard_font(11.0),
                color=self._TEXT,
                zorder=3,
            )

    def _draw_snapshot_price_card(
        self,
        ax: Any,
        analysis: Mapping[str, Any],
        history: pd.DataFrame,
        price: float,
        support_low: float,
        support_high: float,
    ) -> None:
        ax.set_axis_off()
        if Rectangle is not None:
            panel = Rectangle(
                (0.0, 0.0),
                1.0,
                1.0,
                transform=ax.transAxes,
                facecolor=self._panel_surface("main"),
                edgecolor=self._CARD_EDGE,
                linewidth=1.0,
                zorder=0,
            )
            ax.add_patch(panel)

        latest = history.iloc[-1]
        day_low = float(pd.to_numeric(latest.get("low"), errors="coerce") or price)
        day_high = float(pd.to_numeric(latest.get("high"), errors="coerce") or price)
        day_open = float(pd.to_numeric(latest.get("open"), errors="coerce") or price)
        range_low = min(day_low, support_low if support_low > 0 else day_low, price)
        range_high = max(day_high, support_high if support_high > 0 else day_high, price)
        if range_high <= range_low:
            range_high = range_low + max(abs(range_low) * 0.02, 1.0)
        level_guides = self._visible_price_levels(
            self._build_price_levels(analysis, history.tail(12).copy(), price=price, support_low=support_low, support_high=support_high),
            price=price,
            max_distance_pct=0.12,
        )

        ax.text(0.04, 0.90, "当前快照 / 关键位", transform=ax.transAxes, ha="left", va="center", fontsize=12.2, color=self._TEXT, fontweight="bold")
        ax.text(0.04, 0.78, f"现价 {price:.3f}", transform=ax.transAxes, ha="left", va="center", fontsize=22, color=self._TEXT, fontweight="semibold")
        ax.text(
            0.04,
            0.67,
            f"开 {day_open:.3f}  高 {day_high:.3f}  低 {day_low:.3f}",
            transform=ax.transAxes,
            ha="left",
            va="center",
            fontsize=9.2,
            color=self._MUTED,
        )

        line_y = 0.46
        left_x = 0.08
        right_x = 0.92
        ax.plot([left_x, right_x], [line_y, line_y], transform=ax.transAxes, color=self._REFERENCE_LINE, linewidth=2.2, solid_capstyle="round", zorder=2)
        ax.text(left_x, line_y - 0.09, f"{range_low:.3f}", transform=ax.transAxes, ha="center", va="center", fontsize=8.8, color=self._MUTED)
        ax.text(right_x, line_y - 0.09, f"{range_high:.3f}", transform=ax.transAxes, ha="center", va="center", fontsize=8.8, color=self._MUTED)

        def _mark_level(value: float, label: str, color: str, *, dy: float) -> None:
            ratio = max(0.0, min(1.0, (value - range_low) / (range_high - range_low)))
            x = left_x + (right_x - left_x) * ratio
            ax.plot([x, x], [line_y - 0.035, line_y + 0.035], transform=ax.transAxes, color=color, linewidth=1.4, zorder=3)
            ax.text(
                x,
                line_y + dy,
                f"{label} {value:.3f}",
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=8.1,
                color=color,
                bbox={"boxstyle": "round,pad=0.18,rounding_size=0.08", "fc": self._panel_surface("muted"), "ec": color},
                zorder=4,
            )

        price_ratio = max(0.0, min(1.0, (price - range_low) / (range_high - range_low)))
        price_x = left_x + (right_x - left_x) * price_ratio
        ax.scatter([price_x], [line_y], transform=ax.transAxes, color=self._LAST_PRICE_DOT, s=42, zorder=5)
        ax.text(
            price_x,
            line_y + 0.12,
            "当前价",
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=8.2,
            color=self._LAST_PRICE_TEXT,
            bbox={"boxstyle": "round,pad=0.18,rounding_size=0.08", "fc": self._LAST_PRICE_BOX, "ec": self._LAST_PRICE_BOX_EDGE},
            zorder=6,
        )
        for index, (label, value, level_type) in enumerate(level_guides[:4]):
            color = self._RESISTANCE if level_type == "resistance" else self._STOP if level_type == "stop" else self._SUPPORT
            _mark_level(float(value), label, color, dy=(-0.17 if index % 2 else 0.17))

        metadata = dict(analysis.get("metadata") or {})
        history_source_label = str(metadata.get("history_source_label", "") or "本地实时快照占位").strip()
        ax.text(
            0.04,
            0.15,
            f"只展示快照区间与关键位；来源 {history_source_label}。",
            transform=ax.transAxes,
            ha="left",
            va="center",
            fontsize=8.8,
            color=self._MUTED,
        )

    def _draw_snapshot_status_card(self, ax: Any, analysis: Mapping[str, Any], history: pd.DataFrame) -> None:
        ax.set_axis_off()
        if Rectangle is not None:
            panel = Rectangle(
                (0.0, 0.0),
                1.0,
                1.0,
                transform=ax.transAxes,
                facecolor=self._panel_surface("main"),
                edgecolor=self._CARD_EDGE,
                linewidth=1.0,
                zorder=0,
            )
            ax.add_patch(panel)
        ax.text(0.04, 0.90, "降级说明 / 当前应读什么", transform=ax.transAxes, ha="left", va="center", fontsize=12.2, color=self._TEXT, fontweight="bold")
        bullets = [
            "完整日线当前不可用，这张图不展示近3月K线或相对强弱曲线。",
            "优先读当前价、区间高低点、反压/止损参考和八维评分。",
            "需要趋势和相对强弱确认时，应等完整日线恢复后再升级判断。",
        ]
        action = dict(analysis.get("action") or {})
        trigger = str(action.get("entry", "") or "").strip()
        if trigger:
            bullets.append(f"当前更合理的触发条件：{trigger}")
        history_date = pd.to_datetime(history.get("date"), errors="coerce").dropna()
        bullets.append(f"快照时点 {history_date.iloc[-1].date()}" if not history_date.empty else "快照时点未标注")
        y = 0.76
        for idx, bullet in enumerate(bullets[:5]):
            ax.text(
                0.06,
                y,
                f"• {bullet}",
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=9.0,
                color=self._TEXT if idx < 3 else self._MUTED,
            )
            y -= 0.14

    def _draw_dashboard_header(self, ax_left: Any, ax_right: Any, analysis: Mapping[str, Any]) -> None:
        ax_left.set_axis_off()
        ax_right.set_axis_off()
        name = str(analysis.get("name", analysis.get("symbol", "asset")) or analysis.get("symbol", "asset")).strip()
        symbol = str(analysis.get("symbol", "asset") or "asset").strip()
        summary_cards = self._header_summary_cards(analysis)
        context_lines = self._header_context_lines(analysis)
        self._draw_header_summary_card(
            ax_left,
            ax_right,
            analysis=analysis,
            name=name,
            symbol=symbol,
            cards=summary_cards,
            context_lines=context_lines,
        )

    def _draw_header_summary_card(
        self,
        ax_left: Any,
        ax_right: Any,
        *,
        analysis: Mapping[str, Any],
        name: str,
        symbol: str,
        cards: list[dict[str, str]],
        context_lines: list[str],
    ) -> None:
        value_by_label = {str(card.get("label", "")).strip(): str(card.get("value", "")).strip() for card in cards}
        rating_value = value_by_label.get("机会评级", "待评级")
        signal_value = value_by_label.get("信号等级", "待复核")
        phase_value = value_by_label.get("阶段", "待识别阶段")
        direction_value = value_by_label.get("方向", "方向待识别")
        action_value = value_by_label.get("当前动作", "观察为主")
        theme_value = value_by_label.get("主线", "暂无主线")
        snapshot = self._header_market_snapshot(analysis)
        self._draw_header_identity_card(
            ax_left,
            x0=0.0,
            y0=0.0,
            card_w=1.0,
            card_h=1.0,
            name=name,
            symbol=symbol,
            context_lines=context_lines,
            snapshot=snapshot,
        )
        self._draw_header_signal_card(
            ax_right,
            x0=0.0,
            y0=0.0,
            card_w=1.0,
            card_h=1.0,
            rating_value=rating_value,
            signal_value=signal_value,
            phase_value=phase_value,
            direction_value=direction_value,
            action_value=action_value,
            theme_value=theme_value,
            badges=list(snapshot.get("badges") or []),
        )

    def _draw_header_identity_card(
        self,
        ax: Any,
        *,
        x0: float,
        y0: float,
        card_w: float,
        card_h: float,
        name: str,
        symbol: str,
        context_lines: list[str],
        snapshot: Mapping[str, Any],
    ) -> None:
        pad_x = 0.064
        title_color = self._header_title_color()
        muted_color = self._header_muted_color()
        value_color = self._header_value_color()
        card_fill = self._header_panel_fill()
        card_edge = self._header_panel_edge()
        divider = self._header_rule_color()
        price_value = str(snapshot.get("price_value", "--")).strip()
        change_value = str(snapshot.get("change_value", "--")).strip()
        change_color = str(snapshot.get("change_color", value_color))
        metrics = list(snapshot.get("metrics") or [])
        self._draw_header_card_frame(ax, x0=x0, y0=y0, card_w=card_w, card_h=card_h, fill=card_fill, edge=card_edge)
        ax.text(
            x0 + pad_x,
            y0 + card_h * 0.77,
            name,
            transform=ax.transAxes,
            ha="left",
            va="center",
            fontsize=self._dashboard_font(16.1),
            fontweight="bold",
            color=title_color,
            zorder=3,
        )
        ax.text(
            x0 + card_w - pad_x,
            y0 + card_h * 0.75,
            price_value,
            transform=ax.transAxes,
            ha="right",
            va="center",
            fontsize=self._dashboard_font(18.4),
            fontweight="bold",
            color=change_color,
            zorder=3,
        )
        ax.text(
            x0 + card_w - pad_x,
            y0 + card_h * 0.58,
            change_value,
            transform=ax.transAxes,
            ha="right",
            va="center",
            fontsize=self._dashboard_font(10.9),
            fontweight="semibold",
            color=change_color,
            zorder=3,
        )

        chip_w = min(max(0.080 + 0.0105 * len(symbol), 0.12), 0.21)
        chip_h = 0.088
        chip_x = x0 + pad_x
        chip_y = y0 + card_h * 0.49
        if PathPatch is not None and MplPath is not None:
            symbol_chip = PathPatch(
                self._rounded_rect_path(
                    ax=ax,
                    x0=chip_x,
                    y0=chip_y,
                    width=chip_w,
                    height=chip_h,
                    rx_factor=0.13,
                    ry_factor=0.26,
                    min_rx=0.012,
                    min_ry=0.018,
                    max_rx_ratio=0.16,
                    max_ry_ratio=0.36,
                ),
                transform=ax.transAxes,
                facecolor=self._header_chip_fill_rgba(),
                edgecolor=self._header_chip_edge_rgba(),
                linewidth=self._dashboard_line(0.8),
                antialiased=True,
                joinstyle="round",
                capstyle="round",
                zorder=2,
            )
            ax.add_patch(symbol_chip)
        elif FancyBboxPatch is not None:
            symbol_chip = FancyBboxPatch(
                (chip_x, chip_y),
                chip_w,
                chip_h,
                boxstyle="round,pad=0.005,rounding_size=0.020",
                transform=ax.transAxes,
                facecolor=self._header_chip_fill_rgba(),
                edgecolor=self._header_chip_edge_rgba(),
                linewidth=self._dashboard_line(0.8),
                zorder=2,
            )
            ax.add_patch(symbol_chip)
        ax.text(
            chip_x + chip_w * 0.5,
            chip_y + chip_h * 0.5,
            symbol,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=self._dashboard_font(10.9),
            fontweight="semibold",
            color=muted_color,
            zorder=3,
        )

        if context_lines:
            base_y = y0 + card_h * 0.365
            for idx, line in enumerate(context_lines[:2]):
                ax.text(
                    x0 + pad_x,
                    base_y - idx * 0.084,
                    line,
                    transform=ax.transAxes,
                    ha="left",
                    va="center",
                    fontsize=self._dashboard_font(8.3),
                    color=muted_color,
                    zorder=3,
                )
        divider_y = y0 + card_h * 0.205
        ax.plot(
            [x0 + pad_x, x0 + card_w - pad_x],
            [divider_y, divider_y],
            transform=ax.transAxes,
            color=divider,
            linewidth=self._dashboard_line(0.88),
            alpha=0.62,
            zorder=2,
        )

        metric_left = x0 + pad_x
        metric_right = x0 + card_w - pad_x
        metric_count = max(min(len(metrics[:4]), 4), 1)
        metric_step = (metric_right - metric_left) / float(metric_count) if metric_right > metric_left else 0.17
        label_y = y0 + 0.108
        value_y = y0 + 0.044
        metric_positions = tuple((metric_left + metric_step * idx, label_y, value_y) for idx in range(metric_count))
        for idx, (label, value) in enumerate(metrics[:4]):
            col_x, label_y, value_y = metric_positions[idx]
            ax.text(
                col_x,
                label_y,
                label,
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=self._dashboard_font(7.7),
                color=muted_color,
                zorder=3,
            )
            ax.text(
                col_x,
                value_y,
                value,
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=self._dashboard_font(12.0),
                fontweight="semibold",
                color=value_color,
                zorder=3,
            )

    def _draw_header_signal_card(
        self,
        ax: Any,
        *,
        x0: float,
        y0: float,
        card_w: float,
        card_h: float,
        rating_value: str,
        signal_value: str,
        phase_value: str,
        direction_value: str,
        action_value: str,
        theme_value: str,
        badges: list[tuple[str, str]] | None = None,
    ) -> None:
        muted_color = self._header_muted_color()
        card_fill = self._header_panel_fill()
        card_edge = self._header_panel_edge()
        value_color = self._header_value_color()
        _, _, rating_accent = self._summary_card_palette(self._status_tone(signal_value))
        self._draw_header_card_frame(ax, x0=x0, y0=y0, card_w=card_w, card_h=card_h, fill=card_fill, edge=card_edge)
        left_panel_x = x0 + card_w * 0.07
        left_panel_y = y0 + card_h * 0.13
        left_panel_w = card_w * 0.34
        left_panel_h = card_h * 0.74
        if FancyBboxPatch is not None:
            panel = FancyBboxPatch(
                (left_panel_x, left_panel_y),
                left_panel_w,
                left_panel_h,
                boxstyle="round,pad=0.008,rounding_size=0.035",
                transform=ax.transAxes,
                facecolor=self._header_chip_fill_rgba(),
                edgecolor=self._header_chip_edge_rgba(),
                linewidth=self._dashboard_line(0.82),
                zorder=2,
            )
            ax.add_patch(panel)
        left_block_x = left_panel_x + left_panel_w * 0.5
        ax.text(
            left_block_x,
            left_panel_y + left_panel_h * 0.64,
            rating_value,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=self._dashboard_font(17.2),
            fontweight="bold",
            color=rating_accent,
            zorder=3,
        )
        ax.text(
            left_block_x,
            left_panel_y + left_panel_h * 0.32,
            signal_value,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=self._dashboard_font(10.8),
            fontweight="semibold",
            color=rating_accent,
            zorder=3,
        )

        badge_items: list[tuple[str, str, float]] = []
        for text, tone in (badges or [])[:2]:
            width = min(max(card_w * (0.15 + 0.013 * len(str(text).strip())), card_w * 0.21), card_w * 0.26)
            badge_items.append((str(text).strip(), tone, width))
        if badge_items:
            badge_x = x0 + card_w * 0.50
            badge_y_positions = [0.69, 0.53]
            for idx, (text, tone, width) in enumerate(badge_items):
                self._draw_header_pill(
                    ax,
                    text=text,
                    tone=tone,
                    x=badge_x,
                    y=y0 + card_h * badge_y_positions[min(idx, len(badge_y_positions) - 1)],
                    width=width,
                    height=card_h * 0.108,
                    align="left",
                    fontsize=self._dashboard_font(10.4),
                    clip_on=True,
                )

        label_x = x0 + card_w * 0.50
        value_x = x0 + card_w * 0.70
        for row_y_rel, label, value, value_fontsize in self._header_signal_rows(
            phase_value=phase_value,
            direction_value=direction_value,
            action_value=action_value,
            theme_value=theme_value,
        ):
            ax.text(
                label_x,
                y0 + card_h * row_y_rel,
                label,
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=self._dashboard_font(8.1),
                color=muted_color,
                zorder=3,
                clip_on=True,
            )
            ax.text(
                value_x,
                y0 + card_h * row_y_rel,
                value,
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=min(self._dashboard_font(self._header_compact_value_fontsize(value)), self._dashboard_font(value_fontsize)),
                fontweight="medium",
                color=value_color,
                zorder=3,
                clip_on=True,
            )

    def _header_market_snapshot(self, analysis: Mapping[str, Any]) -> dict[str, Any]:
        history = analysis.get("history")
        if not isinstance(history, pd.DataFrame) or history.empty:
            return {
                "price_value": "--",
                "change_value": "--",
                "change_color": self._header_value_color(),
                "metrics": [("MACD", "--"), ("KDJ", "--"), ("RSI(14)", "--"), ("ADX", "--")],
                "badges": [],
            }
        frame = self._prepare_history(history)
        if frame.empty:
            return {
                "price_value": "--",
                "change_value": "--",
                "change_color": self._header_value_color(),
                "metrics": [("MACD", "--"), ("KDJ", "--"), ("RSI(14)", "--"), ("ADX", "--")],
                "badges": [],
            }
        close = pd.to_numeric(frame["close"], errors="coerce").dropna()
        price = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close) >= 2 else price
        change_pct = 0.0 if prev == 0 else (price / prev - 1.0) * 100.0
        change_color = self._header_change_color(change_pct)
        metric_1_value = "--"
        metric_2_value = "--"
        metric_3_value = "--"
        metric_4_value = "--"
        badges: list[tuple[str, str]] = []
        try:
            context = build_technical_signal_context(frame.copy(), calc_window=min(120, len(frame)), plot_window=min(22, len(frame)))
        except Exception:
            context = None
        if context:
            indicators = context["indicators"]
            close_series = context["history"]["close"].reset_index(drop=True)
            divergence = context["divergence"]
            metric_1_value = f"{float(indicators['macd_dif'].iloc[-1]):.3f}"
            metric_2_value = f"{float(indicators['kdj_k'].iloc[-1]):.1f}"
            metric_3_value = f"{float(indicators['rsi'].iloc[-1]):.1f}"
            metric_4_value = f"{float(indicators['adx'].iloc[-1]):.1f}"
            raw_badges = []
            raw_badges.extend(self._macd_badges(indicators, close_series, divergence))
            raw_badges.extend(self._rsi_badges(indicators, close_series, divergence))
            raw_badges.extend(self._kdj_badges(indicators))
            if divergence:
                for indicator_name in ("macd", "rsi", "kdj"):
                    badge = self._divergence_badge_for_indicator(indicator_name, divergence)
                    if badge is not None:
                        raw_badges.append(badge)
            priority_tokens = {
                "背离": 100,
                "金叉": 90,
                "死叉": 88,
                "突破": 82,
                "放量": 78,
                "修复": 76,
                "主导": 74,
                "拐点": 72,
            }
            prioritized: list[tuple[int, int, str, str]] = []
            seen: set[str] = set()
            for idx, (text, tone) in enumerate(raw_badges):
                clean = str(text).strip()
                if not clean or clean in seen or clean in {"趋势市", "震荡市", "过渡期", "中性震荡", "中性区"}:
                    continue
                seen.add(clean)
                score = 0
                for token, value in priority_tokens.items():
                    if token in clean:
                        score = max(score, value)
                if tone == "bull":
                    score += 6
                elif tone == "bear":
                    score += 4
                prioritized.append((score, -idx, clean, tone))
            for _, _, clean, tone in sorted(prioritized, reverse=True):
                badges.append((clean, tone))
                if len(badges) >= 2:
                    break
        return {
            "price_value": f"{price:.3f}",
            "change_value": f"{change_pct:+.2f}%",
            "change_color": change_color,
            "metrics": [
                ("MACD", metric_1_value),
                ("KDJ", metric_2_value),
                ("RSI(14)", metric_3_value),
                ("ADX", metric_4_value),
            ],
            "badges": badges,
        }

    def _draw_header_card_frame(
        self,
        ax: Any,
        *,
        x0: float,
        y0: float,
        card_w: float,
        card_h: float,
        fill: str,
        edge: str,
    ) -> None:
        face = fill
        edge_color = edge
        if matplotlib is not None:
            face = matplotlib.colors.to_rgba(fill, self._header_panel_alpha())
            edge_color = matplotlib.colors.to_rgba(edge, self._header_panel_edge_alpha())
        if PathPatch is not None and MplPath is not None:
            card = PathPatch(
                self._header_card_path(ax=ax, x0=x0, y0=y0, card_w=card_w, card_h=card_h),
                transform=ax.transAxes,
                facecolor=face,
                edgecolor=edge_color,
                linewidth=0.72,
                antialiased=True,
                joinstyle="round",
                capstyle="round",
                zorder=1,
            )
            if path_effects is not None:
                card.set_path_effects(
                    [
                        path_effects.SimplePatchShadow(
                            offset=(0.0, -1.1),
                            shadow_rgbFace=(0.0, 0.0, 0.0),
                            alpha=self._header_shadow_alpha(),
                            rho=0.97,
                        ),
                        path_effects.Normal(),
                    ]
            )
            ax.add_patch(card)
        elif FancyBboxPatch is not None:
            card = FancyBboxPatch(
                (x0, y0),
                card_w,
                card_h,
                boxstyle="round,pad=0.010,rounding_size=0.070",
                transform=ax.transAxes,
                facecolor=face,
                edgecolor=edge_color,
                linewidth=0.72,
                antialiased=True,
                joinstyle="round",
                capstyle="round",
                zorder=1,
            )
            if path_effects is not None:
                card.set_path_effects(
                    [
                        path_effects.SimplePatchShadow(
                            offset=(0.0, -1.1),
                            shadow_rgbFace=(0.0, 0.0, 0.0),
                            alpha=self._header_shadow_alpha(),
                            rho=0.97,
                        ),
                        path_effects.Normal(),
                    ]
                )
            ax.add_patch(card)
        elif Rectangle is not None:
            card = Rectangle(
                (x0, y0),
                card_w,
                card_h,
                transform=ax.transAxes,
                facecolor=face,
                edgecolor=edge_color,
                linewidth=1.15,
                zorder=1,
            )
            ax.add_patch(card)

    def _header_card_path(
        self,
        *,
        ax: Any,
        x0: float,
        y0: float,
        card_w: float,
        card_h: float,
    ) -> Any:
        return self._rounded_rect_path(
            ax=ax,
            x0=x0,
            y0=y0,
            width=card_w,
            height=card_h,
            rx_factor=0.11,
            ry_factor=0.115,
            min_rx=0.020,
            min_ry=0.055,
            max_rx_ratio=0.10,
            max_ry_ratio=0.22,
        )

    def _rounded_rect_path(
        self,
        *,
        ax: Any,
        x0: float,
        y0: float,
        width: float,
        height: float,
        rx_factor: float,
        ry_factor: float,
        min_rx: float,
        min_ry: float,
        max_rx_ratio: float,
        max_ry_ratio: float,
    ) -> Any:
        if MplPath is None:
            return None
        fig = getattr(ax, "figure", None)
        ax_pos = ax.get_position() if hasattr(ax, "get_position") else None
        axis_w = 1.0
        axis_h = 1.0
        if fig is not None and ax_pos is not None:
            try:
                axis_w = max(float(ax_pos.width) * float(fig.get_figwidth()), 1e-6)
                axis_h = max(float(ax_pos.height) * float(fig.get_figheight()), 1e-6)
            except Exception:
                axis_w = 1.0
                axis_h = 1.0
        aspect = axis_h / axis_w if axis_w > 0 else 1.0
        box_w = width
        box_h = height
        ry = min(box_h * ry_factor, box_h * max_ry_ratio)
        ry = max(ry, min_ry)
        rx = min(box_w * rx_factor * aspect, box_w * max_rx_ratio)
        rx = max(rx, min_rx)
        kappa = 0.5522847498
        cx = rx * kappa
        cy = ry * kappa
        x1 = x0 + box_w
        y1 = y0 + box_h
        vertices = [
            (x0 + rx, y0),
            (x1 - rx, y0),
            (x1 - rx + cx, y0),
            (x1, y0 + ry - cy),
            (x1, y0 + ry),
            (x1, y1 - ry),
            (x1, y1 - ry + cy),
            (x1 - rx + cx, y1),
            (x1 - rx, y1),
            (x0 + rx, y1),
            (x0 + rx - cx, y1),
            (x0, y1 - ry + cy),
            (x0, y1 - ry),
            (x0, y0 + ry),
            (x0, y0 + ry - cy),
            (x0 + rx - cx, y0),
            (x0 + rx, y0),
            (0.0, 0.0),
        ]
        codes = [
            MplPath.MOVETO,
            MplPath.LINETO,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.LINETO,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.LINETO,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.LINETO,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.CURVE4,
            MplPath.CLOSEPOLY,
        ]
        return MplPath(vertices, codes)

    def _draw_header_pill(
        self,
        ax: Any,
        *,
        text: str,
        tone: str,
        x: float,
        y: float,
        width: float | None = None,
        height: float = 0.114,
        align: str = "center",
        fontsize: float = 9.2,
        clip_on: bool = True,
    ) -> None:
        clean = str(text).strip()
        if not clean:
            return
        width = width or min(max(0.075 + 0.0100 * len(clean), 0.12), 0.22)
        fc, ec, tc = self._signal_badge_colors(tone)
        if PathPatch is not None and MplPath is not None:
            chip = PathPatch(
                self._rounded_rect_path(
                    ax=ax,
                    x0=x,
                    y0=y,
                    width=width,
                    height=height,
                    rx_factor=0.14,
                    ry_factor=0.32,
                    min_rx=0.012,
                    min_ry=0.018,
                    max_rx_ratio=0.18,
                    max_ry_ratio=0.40,
                ),
                transform=ax.transAxes,
                facecolor=fc,
                edgecolor=ec,
                linewidth=0.78,
                antialiased=True,
                joinstyle="round",
                capstyle="round",
                zorder=3,
                clip_on=clip_on,
            )
            ax.add_patch(chip)
        elif FancyBboxPatch is not None:
            chip = FancyBboxPatch(
                (x, y),
                width,
                height,
                boxstyle="round,pad=0.005,rounding_size=0.020",
                transform=ax.transAxes,
                facecolor=fc,
                edgecolor=ec,
                linewidth=0.78,
                zorder=3,
                clip_on=clip_on,
            )
            ax.add_patch(chip)
        elif Rectangle is not None:
            chip = Rectangle(
                (x, y),
                width,
                height,
                transform=ax.transAxes,
                facecolor=fc,
                edgecolor=ec,
                linewidth=0.78,
                zorder=3,
                clip_on=clip_on,
            )
            ax.add_patch(chip)
        ax.text(
            x + (width * 0.5 if align == "center" else 0.016),
            y + height * 0.5,
            clean,
            transform=ax.transAxes,
            ha="center" if align == "center" else "left",
            va="center",
            fontsize=fontsize,
            fontweight="semibold",
            color=tc,
            zorder=4,
            clip_on=clip_on,
        )

    def _draw_header_card_grid(self, ax: Any, cards: list[dict[str, str]], *, context_lines: int = 1) -> None:
        left = 0.032
        content_right = 0.968
        bottom = 0.05
        top = 0.54 if context_lines >= 2 else 0.60
        section_gap = 0.028
        panel_h = top - bottom
        primary_w = 0.56
        secondary_w = content_right - left - primary_w - section_gap
        primary_cards = [cards[idx] for idx in (0, 2, 4) if idx < len(cards)]
        secondary_cards = [cards[idx] for idx in (1, 3, 5) if idx < len(cards)]

        if primary_cards and secondary_cards:
            self._draw_header_summary_panel(
                ax,
                title="当前判断",
                cards=primary_cards,
                x0=left,
                y0=bottom,
                panel_w=primary_w,
                panel_h=panel_h,
            )
            self._draw_header_summary_panel(
                ax,
                title="研判概览",
                cards=secondary_cards,
                x0=left + primary_w + section_gap,
                y0=bottom,
                panel_w=secondary_w,
                panel_h=panel_h,
            )
            return

        v_gap = 0.026
        cols = 2
        rows = 3
        h_gap = section_gap
        right = 1 - content_right
        card_w = (1 - left - right - h_gap * (cols - 1)) / cols
        card_h = (panel_h - v_gap * (rows - 1)) / rows
        for idx, card in enumerate(cards[: cols * rows]):
            row = idx // cols
            col = idx % cols
            x0 = left + col * (card_w + h_gap)
            y0 = top - (row + 1) * card_h - row * v_gap
            self._draw_header_primary_card(ax, card, x0=x0, y0=y0, card_w=card_w, card_h=card_h)

    def _draw_header_primary_card(
        self,
        ax: Any,
        card: Mapping[str, str],
        *,
        x0: float,
        y0: float,
        card_w: float,
        card_h: float,
    ) -> None:
        base_fill = getattr(self, "_CARD_BG", self._HEADER_BG)
        _, edge, accent = self._summary_card_palette(str(card.get("tone", "neutral")))
        if Rectangle is not None:
            panel = Rectangle(
                (x0, y0),
                card_w,
                card_h,
                transform=ax.transAxes,
                facecolor=base_fill,
                edgecolor=edge,
                linewidth=1.02,
                zorder=1,
            )
            ax.add_patch(panel)
            accent_bar = Rectangle(
                (x0 + card_w - 0.0062, y0),
                0.0062,
                card_h,
                transform=ax.transAxes,
                facecolor=accent,
                edgecolor="none",
                zorder=2,
            )
            ax.add_patch(accent_bar)
        label = str(card.get("label", "")).strip()
        value = str(card.get("value", "")).strip()
        has_both = bool(label and value)
        label_y = y0 + card_h * (0.78 if has_both else 0.50)
        value_y = y0 + card_h * (0.34 if has_both else 0.50)
        if label:
            ax.text(
                x0 + 0.018,
                label_y,
                label,
                transform=ax.transAxes,
                ha="left",
                va="center",
                fontsize=6.5,
                color=self._MUTED,
                zorder=3,
            )
        if value:
            ax.text(
                x0 + card_w * 0.5,
                value_y,
                value,
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=self._header_value_fontsize(value),
                fontweight="medium",
                color=self._TEXT,
                zorder=3,
            )

    def _draw_header_summary_panel(
        self,
        ax: Any,
        title: str,
        cards: list[dict[str, str]],
        *,
        x0: float,
        y0: float,
        panel_w: float,
        panel_h: float,
    ) -> None:
        panel_fill = self._header_panel_fill()
        panel_edge = self._header_panel_edge()
        title_color = self._header_title_color()
        muted_color = self._header_muted_color()
        value_color = self._header_value_color()
        if FancyBboxPatch is not None:
            panel = FancyBboxPatch(
                (x0, y0),
                panel_w,
                panel_h,
                boxstyle="round,pad=0.006,rounding_size=0.028",
                transform=ax.transAxes,
                facecolor=panel_fill,
                edgecolor=panel_edge,
                linewidth=1.05 if self._is_light_theme() else 0.96,
                alpha=0.97,
                zorder=1,
            )
            ax.add_patch(panel)
        elif Rectangle is not None:
            panel = Rectangle(
                (x0, y0),
                panel_w,
                panel_h,
                transform=ax.transAxes,
                facecolor=panel_fill,
                edgecolor=panel_edge,
                linewidth=1.05 if self._is_light_theme() else 0.96,
                alpha=0.97,
                zorder=1,
            )
            ax.add_patch(panel)

        ax.text(
            x0 + 0.028,
            y0 + panel_h * 0.865,
            title,
            transform=ax.transAxes,
            ha="left",
            va="center",
            fontsize=10.9,
            fontweight="semibold",
            color=title_color,
            zorder=3,
        )
        ax.plot(
            [x0 + 0.024, x0 + panel_w - 0.024],
            [y0 + panel_h * 0.76, y0 + panel_h * 0.76],
            transform=ax.transAxes,
            color=self._header_rule_color(),
            linewidth=0.8,
            alpha=0.7 if self._is_light_theme() else 0.45,
            zorder=2,
        )

        inner_top = y0 + panel_h * 0.69
        inner_bottom = y0 + panel_h * 0.10
        row_gap = 0.034
        row_h = (inner_top - inner_bottom - row_gap * (len(cards) - 1)) / max(len(cards), 1)

        for idx, card in enumerate(cards):
            row_y = inner_top - (idx + 1) * row_h - idx * row_gap
            _, _, accent = self._summary_card_palette(str(card.get("tone", "neutral")))
            if idx > 0:
                ax.plot(
                    [x0 + 0.024, x0 + panel_w - 0.024],
                    [row_y + row_h + row_gap * 0.44, row_y + row_h + row_gap * 0.44],
                    transform=ax.transAxes,
                    color=self._header_rule_color(),
                    linewidth=0.72,
                    alpha=0.34,
                    zorder=1.9,
                )
            if FancyBboxPatch is not None:
                accent_chip = FancyBboxPatch(
                    (x0 + 0.028, row_y + row_h * 0.21),
                    0.010,
                    row_h * 0.58,
                    boxstyle="round,pad=0.001,rounding_size=0.004",
                    transform=ax.transAxes,
                    facecolor=accent,
                    edgecolor="none",
                    zorder=2,
                )
                ax.add_patch(accent_chip)
            elif Rectangle is not None:
                accent_bar = Rectangle(
                    (x0 + 0.028, row_y + row_h * 0.21),
                    0.010,
                    row_h * 0.58,
                    transform=ax.transAxes,
                    facecolor=accent,
                    edgecolor="none",
                    zorder=2,
                )
                ax.add_patch(accent_bar)
            label = str(card.get("label", "")).strip()
            value = str(card.get("value", "")).strip()
            center_y = row_y + row_h * 0.5
            if label:
                ax.text(
                    x0 + 0.053,
                    center_y,
                    label,
                    transform=ax.transAxes,
                    ha="left",
                    va="center",
                    fontsize=8.5,
                    color=muted_color,
                    zorder=3,
                )
            if value:
                ax.text(
                    x0 + panel_w - 0.030,
                    center_y,
                    value,
                    transform=ax.transAxes,
                    ha="right",
                    va="center",
                    fontsize=self._header_compact_value_fontsize(value) + 2.6,
                    fontweight="semibold",
                    color=value_color,
                    zorder=3,
                )

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
        self._style_axis(ax, panel_tone="main")
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
        ma10_line = ax.plot(history["date"], ma10, color=self._MA10, linewidth=1.75, alpha=0.96, label="MA10", zorder=4)[0]
        ma20_line = ax.plot(history["date"], ma20, color=self._MA60, linewidth=1.75, alpha=0.96, label="MA20", zorder=4)[0]
        self._apply_line_depth(ma10_line)
        self._apply_line_depth(ma20_line, emphasis="primary")
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
        self._style_axis(ax, panel_tone="soft")
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
        close_line = ax.plot(history["date"], close, color=self._DOWN_EDGE, linewidth=2.2, alpha=0.97, label="收盘线", zorder=3)[0]
        ma5_line = ax.plot(history["date"], ma5, color=self._MA5, linewidth=1.75, alpha=0.96, label="MA5", zorder=4)[0]
        ma10_line = ax.plot(history["date"], ma10, color=self._MA10, linewidth=1.75, alpha=0.96, label="MA10", zorder=4)[0]
        self._apply_line_depth(close_line, emphasis="primary")
        self._apply_line_depth(ma5_line)
        self._apply_line_depth(ma10_line)
        ax.fill_between(history["date"], ma5, ma10, color=self._SHORT_FILL, alpha=0.28, zorder=2)
        self._draw_price_levels(ax, history, level_guides)
        self._apply_price_ylim(ax, history, level_guides)
        latest_close = float(close.iloc[-1])
        ax.scatter(history["date"].iloc[-1], latest_close, color=self._LAST_PRICE_DOT, s=28, zorder=5)
        ax.annotate(
            f"{latest_close:.3f}",
            xy=(history["date"].iloc[-1], latest_close),
            xytext=(8, 8),
            textcoords="offset points",
            fontsize=8.3,
            color=self._LAST_PRICE_TEXT,
            bbox={"boxstyle": "round,pad=0.25,rounding_size=0.14", "fc": self._LAST_PRICE_BOX, "ec": self._LAST_PRICE_BOX_EDGE},
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
        self._style_axis(ax, panel_tone="soft")
        ax.bar(
            indicators["date"],
            indicators["macd_hist"],
            color=[self._MACD_POS_HIST if x >= 0 else self._MACD_NEG_HIST for x in indicators["macd_hist"]],
            alpha=0.90,
            label="柱体",
        )
        dif_line = ax.plot(indicators["date"], indicators["macd_dif"], color=self._MACD_DIF, linewidth=1.5, label="DIF")[0]
        dea_line = ax.plot(indicators["date"], indicators["macd_dea"], color=self._MACD_DEA, linewidth=1.5, label="DEA")[0]
        self._apply_line_depth(dif_line, emphasis="primary")
        self._apply_line_depth(dea_line)
        self._mark_crosses(ax, indicators["date"], indicators["macd_dif"], indicators["macd_dea"])
        ax.axhline(0, color=self._ZERO_LINE, linewidth=1)
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
        self._style_axis(ax, panel_tone="soft")
        k_line = ax.plot(indicators["date"], indicators["kdj_k"], color=self._KDJ_K, linewidth=1.35, label="K")[0]
        d_line = ax.plot(indicators["date"], indicators["kdj_d"], color=self._KDJ_D, linewidth=1.35, label="D")[0]
        j_line = ax.plot(indicators["date"], indicators["kdj_j"], color=self._KDJ_J, linewidth=1.15, alpha=0.88, label="J")[0]
        self._apply_line_depth(k_line, emphasis="primary")
        self._apply_line_depth(d_line)
        self._apply_line_depth(j_line)
        self._mark_crosses(ax, indicators["date"], indicators["kdj_k"], indicators["kdj_d"])
        ax.axhline(80, color=self._OVERBOUGHT, linewidth=1, linestyle="--")
        ax.axhline(20, color=self._OVERSOLD, linewidth=1, linestyle="--")
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
        self._style_axis(ax, panel_tone="soft")
        ax.fill_between(indicators["date"], 30, 70, color=self._RSI_BAND, alpha=0.82)
        rsi_line = ax.plot(indicators["date"], indicators["rsi"], color=self._RSI_LINE, linewidth=1.65)[0]
        self._apply_line_depth(rsi_line, emphasis="primary")
        ax.axhline(70, color=self._OVERBOUGHT, linewidth=1, linestyle="--")
        ax.axhline(30, color=self._OVERSOLD, linewidth=1, linestyle="--")
        ax.set_ylim(0, 100)
        ax.set_title(f"RSI | 今日 {float(indicators['rsi'].iloc[-1]):.1f}", loc="left", fontsize=11.6, pad=10, fontweight="semibold")
        self._draw_signal_badges(ax, self._rsi_badges(indicators, close_series, divergence))
        self._format_date_axis(ax, indicators["date"])

    def _draw_boll_panel(self, ax: Any, history: pd.DataFrame, indicators: Mapping[str, Any]) -> None:
        self._style_axis(ax, panel_tone="soft")
        ax.fill_between(
            indicators["date"],
            indicators["boll_lower"],
            indicators["boll_upper"],
            color=self._BOLL_BAND,
            alpha=0.18,
            label="波动带",
        )
        upper_line = ax.plot(indicators["date"], indicators["boll_upper"], color=self._BOLL_UPPER, linewidth=0.95, alpha=0.62, linestyle=(0, (4, 2)))[0]
        lower_line = ax.plot(indicators["date"], indicators["boll_lower"], color=self._BOLL_LOWER, linewidth=0.95, alpha=0.62, linestyle=(0, (4, 2)))[0]
        mid_line = ax.plot(indicators["date"], indicators["boll_mid"], color=self._BOLL_MID, linewidth=1.55, label="中轨")[0]
        close_line = ax.plot(indicators["date"], history["close"], color=self._DOWN_EDGE, linewidth=2.2, label="收盘价")[0]
        self._apply_line_depth(upper_line)
        self._apply_line_depth(lower_line)
        self._apply_line_depth(mid_line)
        self._apply_line_depth(close_line, emphasis="primary")
        self._apply_price_ylim(ax, history, [])
        latest_close = float(history["close"].iloc[-1])
        ax.scatter(indicators["date"].iloc[-1], latest_close, color=self._LAST_PRICE_DOT, s=20, zorder=5)
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
        self._style_axis(ax, panel_tone="soft")
        adx_line = ax.plot(indicators["date"], indicators["adx"], color=self._ADX, linewidth=1.65, label="ADX")[0]
        plus_line = ax.plot(indicators["date"], indicators["plus_di"], color=self._PLUS_DI, linewidth=1.45, label="+DI")[0]
        minus_line = ax.plot(indicators["date"], indicators["minus_di"], color=self._MINUS_DI, linewidth=1.45, label="-DI")[0]
        self._apply_line_depth(adx_line, emphasis="primary")
        self._apply_line_depth(plus_line)
        self._apply_line_depth(minus_line)
        ax.axhline(25, color=self._ZERO_LINE, linewidth=1, linestyle="--", alpha=0.75)
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
        self._style_axis(ax, panel_tone="soft")
        obv_line = ax.plot(indicators["date"], indicators["obv"], color=self._OBV, linewidth=1.8, label="OBV")[0]
        obv_ma_line = ax.plot(indicators["date"], indicators["obv_ma"], color=self._OBV_MA, linewidth=1.45, label="OBV MA20")[0]
        self._apply_line_depth(obv_line, emphasis="primary")
        self._apply_line_depth(obv_ma_line)
        ax.axhline(float(indicators["obv"].iloc[0]), color=self._REFERENCE_LINE, linewidth=0.9, linestyle="--", alpha=0.72)
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
        cursor = 0.988
        y = 1.012
        for text, tone in badges[:3][::-1]:
            clean = str(text).strip()
            if not clean:
                continue
            width = min(max(0.046 + 0.0076 * len(clean), 0.080), 0.19)
            x = cursor - width
            if x < 0.52:
                break
            fc, ec, tc = self._signal_badge_colors(tone)
            if FancyBboxPatch is not None:
                chip = FancyBboxPatch(
                    (x, y),
                    width,
                    0.082,
                    boxstyle="round,pad=0.006,rounding_size=0.024",
                    transform=ax.transAxes,
                    facecolor=fc,
                    edgecolor=ec,
                    linewidth=0.68,
                    zorder=7,
                    clip_on=False,
                )
                ax.add_patch(chip)
            ax.text(
                x + width * 0.5,
                y + 0.041,
                clean,
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=6.6,
                color=tc,
                zorder=8,
                clip_on=False,
            )
            cursor = x - 0.010

    def _signal_badge_colors(self, tone: str) -> tuple[str, str, str]:
        if tone == "bull":
            return self._BADGE_BULL_FILL, self._BADGE_BULL_EDGE, self._BADGE_BULL_TEXT
        if tone == "bear":
            return self._BADGE_BEAR_FILL, self._BADGE_BEAR_EDGE, self._BADGE_BEAR_TEXT
        if tone == "warn":
            return self._BADGE_WARN_FILL, self._BADGE_WARN_EDGE, self._BADGE_WARN_TEXT
        return self._BADGE_NEUTRAL_FILL, self._BADGE_NEUTRAL_EDGE, self._BADGE_NEUTRAL_TEXT

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
            ax.scatter(golden_x, golden_y, color=self._CROSS_GOLD, marker="^", s=54, zorder=6, label="_nolegend_")
        if death_x:
            ax.scatter(death_x, death_y, color=self._CROSS_DEATH, marker="v", s=54, zorder=6, label="_nolegend_")

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

        zone_low = 0.0
        zone_high = 0.0
        if support_low > 0 and support_high > 0:
            zone_low = min(support_low, support_high)
            zone_high = max(support_low, support_high)
        elif support_low > 0:
            zone_low = support_low
            zone_high = support_low
        elif support_high > 0:
            zone_low = support_high
            zone_high = support_high

        if zone_low > 0:
            if zone_low > price + tolerance:
                add("反压下沿", zone_low, "resistance")
                if zone_high > 0 and abs(zone_high - zone_low) > tolerance:
                    add("反压上沿", zone_high, "resistance")
            else:
                add("支撑下沿", zone_low, "support")
                if zone_high > 0 and abs(zone_high - zone_low) > tolerance:
                    add("支撑上沿", zone_high, "support")
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

    def _draw_price_levels(self, ax: Any, history: pd.DataFrame, levels: list[tuple[str, float, str]]) -> list[float]:
        if not levels or transforms is None or history.empty:
            return []
        value_range = float(pd.to_numeric(history.get("high"), errors="coerce").max() - pd.to_numeric(history.get("low"), errors="coerce").min())
        min_gap = max(value_range * 0.048, max(abs(float(history["close"].iloc[-1])), 1.0) * 0.0065)
        y_min, y_max = ax.get_ylim()
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
            ax.axhline(value, color=color, linewidth=1.02, linestyle=linestyle, alpha=0.76, zorder=1.15)
            prefer_direction = 1 if tone == "resistance" else -1
            label_y = self._resolve_level_label_y(
                float(value),
                placed,
                min_gap=min_gap,
                y_min=y_min,
                y_max=y_max,
                prefer_direction=prefer_direction,
            )
            self._draw_price_level_badge(ax, trans=trans, label_text=f"{label} {value:.3f}", label_y=label_y, fill=color)
        return placed

    def _draw_current_price_callout(
        self,
        ax: Any,
        history: pd.DataFrame,
        price: float,
        placed_levels: list[float],
    ) -> None:
        if transforms is None or history.empty:
            return
        value_range = float(pd.to_numeric(history.get("high"), errors="coerce").max() - pd.to_numeric(history.get("low"), errors="coerce").min())
        min_gap = max(value_range * 0.048, max(abs(float(history["close"].iloc[-1])), 1.0) * 0.0065)
        y_min, y_max = ax.get_ylim()
        prefer_direction = 1 if (y_max - price) >= (price - y_min) else -1
        label_y = self._resolve_level_label_y(
            float(price),
            placed_levels,
            min_gap=min_gap,
            y_min=y_min,
            y_max=y_max,
            prefer_direction=prefer_direction,
        )
        trans = transforms.blended_transform_factory(ax.transAxes, ax.transData)
        self._draw_price_level_badge(
            ax,
            trans=trans,
            label_text=f"当前价 {price:.3f}",
            label_y=label_y,
            fill=self._LAST_PRICE_BOX,
            edge=self._LAST_PRICE_BOX_EDGE,
            text_color=self._LAST_PRICE_TEXT,
            x=1.012,
        )

    def _draw_price_level_badge(
        self,
        ax: Any,
        *,
        trans: Any,
        label_text: str,
        label_y: float,
        fill: str,
        edge: str | None = None,
        text_color: str | None = None,
        x: float = 1.004,
    ) -> None:
        ax.text(
            x,
            label_y,
            label_text,
            transform=trans,
            ha="left",
            va="center",
            fontsize=self._dashboard_font(8.5),
            color=text_color or self._LEVEL_TEXT,
            fontweight="bold",
            bbox={
                "boxstyle": "round,pad=0.24,rounding_size=0.14",
                "fc": fill,
                "ec": edge or "none",
                "alpha": 0.94,
            },
            zorder=6,
            clip_on=False,
        )

    def _resolve_level_label_y(
        self,
        value: float,
        occupied: list[float],
        *,
        min_gap: float,
        y_min: float,
        y_max: float,
        prefer_direction: int,
    ) -> float:
        lower_bound = y_min + min_gap * 0.45
        upper_bound = y_max - min_gap * 0.45
        direction = 1 if prefer_direction >= 0 else -1
        label_y = min(max(float(value), lower_bound), upper_bound)
        candidate_offsets = [0.0]
        for step in range(1, 24):
            candidate_offsets.extend([direction * min_gap * step, -direction * min_gap * step])
        for offset in candidate_offsets:
            candidate = min(max(float(value) + offset, lower_bound), upper_bound)
            if all(abs(candidate - used) >= min_gap for used in occupied):
                label_y = candidate
                break
            if abs(candidate - float(value)) < abs(label_y - float(value)):
                label_y = candidate
        occupied.append(label_y)
        return label_y

    def _dashboard_font(self, base: float) -> float:
        return round(float(base) * 1.14, 2)

    def _dashboard_line(self, base: float) -> float:
        return round(float(base) * 1.12, 2)

    def _dashboard_marker(self, base: float) -> float:
        return round(float(base) * 1.20, 2)

    def _style_axis(self, ax: Any, *, panel_tone: str = "soft") -> None:
        ax.set_facecolor(self._panel_surface(panel_tone))
        ax.grid(axis="y", linestyle="--", linewidth=self._dashboard_line(0.65), alpha=0.14 if self._is_light_theme() else 0.24)
        ax.grid(axis="x", linestyle=(0, (2, 4)) if self._is_light_theme() else "-", linewidth=self._dashboard_line(0.35), alpha=0.04 if self._is_light_theme() else 0.06)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(self._AXIS_EDGE)
        ax.spines["bottom"].set_color(self._AXIS_EDGE)
        ax.tick_params(axis="both", which="major", labelsize=self._dashboard_font(8.3), length=0, pad=5)

    def _style_volume_axis(self, ax: Any, *, panel_tone: str = "muted") -> None:
        ax.set_facecolor(self._panel_surface(panel_tone))
        ax.grid(False)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["bottom"].set_color(self._AXIS_EDGE)
        ax.tick_params(axis="both", which="major", labelsize=self._dashboard_font(8.0), length=0, pad=4)

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
        if not any(item[2] in {"support", "stop"} for item in filtered):
            support_candidates = sorted(
                [item for item in levels if item[2] in {"support", "stop"}],
                key=lambda item: abs(item[1] - price),
            )
            if support_candidates:
                filtered.append(support_candidates[0])
        if not any(item[2] == "resistance" for item in filtered):
            resistance_candidates = sorted(
                [item for item in levels if item[2] == "resistance"],
                key=lambda item: abs(item[1] - price),
            )
            if resistance_candidates:
                filtered.append(resistance_candidates[0])
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
        padding = span * 0.12
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
        ax.set_xlim(mdates.date2num(first - pd.Timedelta(days=left_pad)), mdates.date2num(last + pd.Timedelta(days=pad_days)))
        return last + pd.Timedelta(days=max(pad_days - 1, min_days // 2 or 1))

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

    def _analysis_theme_context(self, analysis: Mapping[str, Any]) -> dict[str, Any]:
        cached = dict(analysis.get("theme_playbook") or {})
        if str(cached.get("label", "")).strip():
            return cached
        metadata = dict(analysis.get("metadata") or {})
        narrative = dict(analysis.get("narrative") or {})
        return dict(
            build_theme_playbook_context(
                metadata,
                analysis.get("name"),
                analysis.get("symbol"),
                analysis.get("notes"),
                dict(analysis.get("day_theme") or {}).get("label"),
                narrative.get("headline"),
                narrative.get("summary_lines"),
                narrative.get("playbook"),
            )
            or {}
        )

    def _analysis_theme_label(self, analysis: Mapping[str, Any]) -> str:
        theme_context = self._analysis_theme_context(analysis)
        playbook_label = self._dashboard_label(theme_context.get("label"), fallback="")
        if str(theme_context.get("theme_match_status", "")).strip() == "hard_sector_guarded":
            for candidate in list(theme_context.get("theme_match_candidates") or []):
                candidate_label = self._dashboard_label(candidate, fallback="")
                if candidate_label:
                    return candidate_label
        hard_sector = self._dashboard_label(theme_context.get("hard_sector_label"), fallback="")
        day_theme = self._dashboard_label(dict(analysis.get("day_theme") or {}).get("label"), fallback="")
        return playbook_label or hard_sector or day_theme or "暂无主线"

    def _headline_note(self, analysis: Mapping[str, Any]) -> str:
        narrative = analysis.get("narrative", {})
        judgment = narrative.get("judgment", {})
        day_theme = self._analysis_theme_label(analysis)
        return " | ".join(
            [
                f"方向 {self._dashboard_label(judgment.get('direction'), fallback='方向待识别')}",
                f"当前动作 {self._trade_state_chip_text(analysis)}",
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
        return "待评级"

    def _header_summary_cards(self, analysis: Mapping[str, Any]) -> list[dict[str, str]]:
        narrative = dict(analysis.get("narrative") or {})
        judgment = dict(narrative.get("judgment") or {})
        phase = self._dashboard_label(dict(narrative.get("phase") or {}).get("label"), fallback="待识别阶段")
        direction = self._dashboard_label(judgment.get("direction"), fallback="方向待识别")
        rating_label = self._dashboard_label(dict(analysis.get("rating") or {}).get("label"), fallback="信号待复核")
        trade_state = self._trade_state_chip_text(analysis)
        day_theme = self._analysis_theme_label(analysis)
        return [
            {"label": "机会评级", "value": self._rating_badge(analysis), "tone": self._rating_tone(analysis)},
            {"label": "信号等级", "value": rating_label, "tone": self._status_tone(rating_label)},
            {"label": "阶段", "value": phase, "tone": self._status_tone(phase)},
            {"label": "方向", "value": direction, "tone": self._status_tone(direction)},
            {"label": "当前动作", "value": trade_state, "tone": self._status_tone(trade_state)},
            {"label": "主线", "value": day_theme, "tone": self._status_tone(day_theme)},
        ]

    def _header_context_line(self, analysis: Mapping[str, Any]) -> str:
        summary_lines = list(dict(analysis.get("narrative") or {}).get("summary_lines") or [])
        summary = str(summary_lines[0]).strip() if summary_lines else ""
        if summary:
            summary = re.sub(r"\s+", " ", summary)
            if len(summary) > 52:
                summary = f"{summary[:52]}..."
        else:
            summary = f"{self._dashboard_label(dict(dict(analysis.get('narrative') or {}).get('phase') or {}).get('label'), fallback='待识别阶段')} · {self._trade_state_chip_text(analysis)}"
        as_of = ""
        history = analysis.get("history")
        if isinstance(history, pd.DataFrame) and not history.empty and "date" in history.columns:
            latest = pd.to_datetime(history["date"], errors="coerce").dropna()
            if not latest.empty:
                as_of = f"行情 {latest.iloc[-1].date()}"
        return " · ".join(part for part in (summary, as_of) if part)

    def _header_context_lines(self, analysis: Mapping[str, Any]) -> list[str]:
        summary_lines = list(dict(analysis.get("narrative") or {}).get("summary_lines") or [])
        summary = str(summary_lines[0]).strip() if summary_lines else ""
        summary = re.sub(r"\s+", " ", summary)
        if not summary:
            summary = self._header_context_line(analysis).split(" · ")[0].strip()
        as_of = ""
        history = analysis.get("history")
        if isinstance(history, pd.DataFrame) and not history.empty and "date" in history.columns:
            latest = pd.to_datetime(history["date"], errors="coerce").dropna()
            if not latest.empty:
                as_of = f"行情 {latest.iloc[-1].date()}"
        lines = self._split_header_summary_text(summary, max_len=32, max_lines=2)
        if as_of:
            if lines:
                lines = lines[:1]
            lines.append(as_of)
        trimmed = [re.sub(r"\s+", " ", item).strip() for item in lines[:2] if str(item).strip()]
        return trimmed

    def _header_value_fontsize(self, text: str) -> float:
        clean = str(text or "").strip()
        if len(clean) <= 6:
            return 13.0
        if len(clean) <= 10:
            return 11.7
        if len(clean) <= 14:
            return 10.3
        if len(clean) <= 18:
            return 9.4
        return 8.6

    def _header_compact_value_fontsize(self, text: str) -> float:
        clean = str(text or "").strip()
        if len(clean) <= 6:
            return 12.4
        if len(clean) <= 10:
            return 11.2
        if len(clean) <= 14:
            return 10.2
        if len(clean) <= 18:
            return 9.4
        return 8.5

    def _header_inline_value(self, text: str, *, limit: int = 14) -> str:
        clean = re.sub(r"\s+", " ", str(text or "").strip())
        if not clean:
            return "--"
        if len(clean) <= limit:
            return clean
        return f"{clean[:limit].rstrip()}..."

    def _header_signal_rows(
        self,
        *,
        phase_value: str,
        direction_value: str,
        action_value: str,
        theme_value: str,
    ) -> tuple[tuple[float, str, str, float], ...]:
        return (
            (
                0.42,
                "阶段 / 方向",
                f"{self._header_inline_value(phase_value, limit=8)} / {self._header_inline_value(direction_value, limit=5)}",
                9.2,
            ),
            (
                0.26,
                "当前动作",
                self._header_inline_value(action_value, limit=10),
                9.1,
            ),
            (
                0.11,
                "主线",
                self._header_inline_value(theme_value, limit=10),
                9.0,
            ),
        )

    def _split_header_summary_text(self, text: str, *, max_len: int = 32, max_lines: int = 2) -> list[str]:
        clean = re.sub(r"\s+", " ", str(text or "").strip())
        if not clean:
            return []
        if len(clean) <= max_len:
            return [clean]
        pieces = re.split(r"(?<=[，。；：,.!?])", clean)
        lines: list[str] = []
        current = ""
        for piece in pieces:
            piece = piece.strip()
            if not piece:
                continue
            candidate = f"{current}{piece}" if current else piece
            if len(candidate) <= max_len or not current:
                current = candidate
                continue
            lines.append(current)
            current = piece
            if len(lines) >= max_lines - 1:
                break
        if current and len(lines) < max_lines:
            lines.append(current)
        if not lines:
            lines = [clean[:max_len]]
        if len(lines) > max_lines:
            lines = lines[:max_lines]
        if len("".join(lines)) < len(clean):
            lines[-1] = f"{lines[-1][:max_len - 1].rstrip()}..."
        return [line.strip() for line in lines if line.strip()]

    def _header_change_color(self, change_pct: float) -> str:
        if change_pct > 0:
            if self.theme == "institutional":
                return "#d85a67"
            if self.theme == "clinical":
                return "#d94753"
            return "#ff5a5f"
        if change_pct < 0:
            if self.theme == "institutional":
                return "#1f9b84"
            if self.theme == "clinical":
                return "#198a75"
            return "#18c48f"
        return self._header_value_color()

    def _header_panel_fill(self) -> str:
        if self.theme == "institutional":
            return "#15120e"
        if self.theme == "terminal":
            return "#202d3d"
        if self.theme == "abyss-gold":
            return "#242019"
        if self.theme == "clinical":
            return "#ffffff"
        if self.theme == "erdtree":
            return "#fffaf3"
        if self.theme == "neo-brutal":
            return "#ffffff"
        return getattr(self, "_CARD_BG", self._HEADER_BG)

    def _header_panel_alpha(self) -> float:
        if self.theme == "institutional":
            return 0.96
        if self.theme == "terminal":
            return 0.68
        if self.theme == "abyss-gold":
            return 0.82
        if self.theme in {"clinical", "erdtree"}:
            return 0.96
        if self.theme == "neo-brutal":
            return 0.94
        return 0.86

    def _header_panel_edge_alpha(self) -> float:
        if self.theme == "institutional":
            return 0.88
        if self.theme == "terminal":
            return 0.32
        if self.theme == "abyss-gold":
            return 0.52
        if self.theme in {"clinical", "erdtree"}:
            return 0.72
        if self.theme == "neo-brutal":
            return 1.0
        return 0.58

    def _header_shadow_alpha(self) -> float:
        if self.theme == "institutional":
            return 0.12
        if self.theme == "terminal":
            return 0.06
        if self.theme == "abyss-gold":
            return 0.10
        if self.theme in {"clinical", "erdtree"}:
            return 0.06
        if self.theme == "neo-brutal":
            return 0.0
        return 0.08

    def _header_panel_edge(self) -> str:
        if self.theme == "institutional":
            return "#6e5420"
        if self.theme == "terminal":
            return "#4a5b72"
        if self.theme == "abyss-gold":
            return "#5b4b3b"
        if self.theme == "clinical":
            return "#d4dfec"
        if self.theme == "erdtree":
            return "#dfd0b5"
        if self.theme == "neo-brutal":
            return "#000000"
        return self._HEADER_EDGE if self._is_light_theme() else self._CARD_EDGE

    def _header_rule_color(self) -> str:
        if self.theme == "institutional":
            return "#5f4a1c"
        if self.theme == "terminal":
            return "#44566e"
        if self.theme == "abyss-gold":
            return "#524538"
        if self.theme == "clinical":
            return "#d9e3ef"
        if self.theme == "erdtree":
            return "#e5d8bf"
        if self.theme == "neo-brutal":
            return "#000000"
        return self._GRID

    def _header_chip_fill(self) -> str:
        if self.theme == "institutional":
            return "#241d15"
        if self.theme == "terminal":
            return "#2a3648"
        if self.theme == "abyss-gold":
            return "#332920"
        if self.theme == "clinical":
            return "#edf3fb"
        if self.theme == "erdtree":
            return "#f3eadc"
        if self.theme == "neo-brutal":
            return "#f3f3f3"
        return self._panel_surface("soft")

    def _header_chip_edge(self) -> str:
        if self.theme == "institutional":
            return "#705b2a"
        if self.theme == "terminal":
            return "#4a5b72"
        if self.theme == "abyss-gold":
            return "#63523f"
        if self.theme == "clinical":
            return "#d4dfec"
        if self.theme == "erdtree":
            return "#dfd0b5"
        if self.theme == "neo-brutal":
            return "#000000"
        return self._HEADER_EDGE

    def _header_title_color(self) -> str:
        if self.theme == "institutional":
            return "#ffe082"
        if self.theme == "terminal":
            return "#eff6ff"
        if self.theme == "abyss-gold":
            return "#f3e7ca"
        return self._TEXT

    def _header_value_color(self) -> str:
        if self.theme == "institutional":
            return "#f4ead4"
        if self.theme == "terminal":
            return "#eef6ff"
        if self.theme == "abyss-gold":
            return "#f5ead4"
        return self._TEXT

    def _header_muted_color(self) -> str:
        if self.theme == "institutional":
            return "#b6a37f"
        if self.theme == "terminal":
            return "#aeb9ca"
        if self.theme == "abyss-gold":
            return "#b7ac9d"
        if self.theme == "clinical":
            return "#6f7f95"
        if self.theme == "erdtree":
            return "#7f7160"
        if self.theme == "neo-brutal":
            return "#3f3f3f"
        return self._MUTED

    def _header_chip_fill_rgba(self) -> Any:
        fill = self._header_chip_fill()
        if matplotlib is not None and self.theme == "institutional":
            return matplotlib.colors.to_rgba(fill, 0.98)
        return fill

    def _header_chip_edge_rgba(self) -> Any:
        edge = self._header_chip_edge()
        if matplotlib is not None and self.theme == "institutional":
            return matplotlib.colors.to_rgba(edge, 0.94)
        return edge

    def _score_card_fill(self) -> Any:
        if matplotlib is not None and self.theme == "institutional":
            return matplotlib.colors.to_rgba("#344151", 0.08)
        if matplotlib is not None and self.theme == "terminal":
            return matplotlib.colors.to_rgba("#2b3647", 0.10)
        return self._panel_surface("main")

    def _score_card_edge(self) -> Any:
        if matplotlib is not None and self.theme == "institutional":
            return matplotlib.colors.to_rgba("#738298", 0.22)
        if matplotlib is not None and self.theme == "terminal":
            return matplotlib.colors.to_rgba("#66778f", 0.26)
        return self._CARD_EDGE

    def _score_track_fill(self) -> Any:
        if matplotlib is not None and self.theme == "institutional":
            return matplotlib.colors.to_rgba("#1a212b", 0.72)
        if matplotlib is not None and self.theme == "terminal":
            return matplotlib.colors.to_rgba("#202938", 0.76)
        return self._SOFT_BAR

    def _summary_card_palette(self, tone: str) -> tuple[str, str, str]:
        if tone == "bull":
            return self._BADGE_BULL_FILL, self._BADGE_BULL_EDGE, self._BADGE_BULL_TEXT
        if tone == "bear":
            return self._BADGE_BEAR_FILL, self._BADGE_BEAR_EDGE, self._BADGE_BEAR_TEXT
        if tone == "warn":
            return self._BADGE_WARN_FILL, self._BADGE_WARN_EDGE, self._BADGE_WARN_TEXT
        return self._BADGE_NEUTRAL_FILL, self._BADGE_NEUTRAL_EDGE, self._BADGE_NEUTRAL_TEXT

    def _rating_tone(self, analysis: Mapping[str, Any]) -> str:
        rank = int(dict(analysis.get("rating") or {}).get("rank", 0) or 0)
        if rank >= 3:
            return "bull"
        if rank == 2:
            return "neutral"
        if rank == 1:
            return "warn"
        return "neutral"

    def _status_tone(self, text: str) -> str:
        clean = str(text or "").strip()
        if any(token in clean for token in ("偏多", "做多", "持有优于追高", "较强", "强势", "趋势市", "修复中", "确认")):
            return "bull"
        if any(token in clean for token in ("偏空", "空头", "回避", "下行", "跌破", "风险释放前不宜激进")):
            return "bear"
        if any(token in clean for token in ("观察", "无信号", "待识别", "待复核", "不充分", "整理", "中性")):
            return "warn"
        return "neutral"

    def _trade_state_chip_text(self, analysis: Mapping[str, Any]) -> str:
        narrative = dict(analysis.get("narrative") or {})
        judgment = dict(narrative.get("judgment") or {})
        action = dict(analysis.get("action") or {})
        for value in (
            judgment.get("trade_state"),
            judgment.get("state"),
            analysis.get("trade_state"),
            action.get("direction"),
            analysis.get("horizon_label"),
            action.get("timeframe"),
        ):
            clean = self._dashboard_label(value, fallback="")
            if clean:
                return clean
        return "观察为主"

    def _dashboard_label(self, value: Any, *, fallback: str) -> str:
        clean = str(value or "").strip()
        if not clean or clean.lower() in {"unknown", "none", "nan"} or clean == "未识别":
            return fallback
        return clean

    def _relative_panel_note(self, analysis: Mapping[str, Any]) -> str:
        metrics = analysis.get("metrics", {})
        rel_5d = metrics.get("return_5d")
        rel_20d = metrics.get("return_20d")
        benchmark = str(analysis.get("benchmark_name", "基准") or "基准")
        regime = str(analysis.get("regime", {}).get("current_regime", "unknown") or "unknown")
        return f"蓝线为标的，灰虚线为 {benchmark}；近5日 {self._fmt_pct(rel_5d)}，近20日 {self._fmt_pct(rel_20d)}，Regime {regime}"

    def _footer_text(self, analysis: Mapping[str, Any]) -> str:
        narrative = analysis.get("narrative", {})
        day_theme = self._analysis_theme_label(analysis)
        summary_lines = narrative.get("summary_lines", [])
        summary = summary_lines[0] if summary_lines else "结论待补充"
        if len(summary) > 48:
            summary = f"{summary[:48]}..."
        return f"主线: {day_theme} | 核心: {summary}"

    def _score_color(self, value: float) -> str:
        if value >= 70:
            return self._SCORE_HIGH
        if value >= 45:
            return self._SCORE_MED
        return self._SCORE_LOW

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
