"""Client-facing markdown export helpers."""

from __future__ import annotations

import base64
import html
import mimetypes
import os
import re
import signal
import subprocess
import tempfile
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import unquote, urlparse


_EDGE_BINARY = Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_REPORT_THEME = "institutional"
_DEFAULT_PRINT_REPORT_THEME = "clinical"
_REPORT_THEME_STORAGE_KEY = "ai-finance-report-theme"
_REPORT_THEME_PRESETS: Dict[str, tuple[str, Dict[str, str]]] = {
    "terminal": (
        "硬核终端",
        {
            "--page-bg": "#141922",
            "--page-bg-2": "#1d2330",
            "--panel-bg": "rgba(31, 38, 51, 0.96)",
            "--panel-bg-strong": "rgba(24, 30, 42, 0.98)",
            "--panel-soft": "rgba(38, 46, 62, 0.94)",
            "--heading": "#eef6ff",
            "--text": "#d6dde8",
            "--text-soft": "#a4afbf",
            "--muted": "#7e8ba0",
            "--accent": "#59d0c2",
            "--accent-strong": "#8ae8db",
            "--accent-soft": "rgba(89, 208, 194, 0.18)",
            "--accent-2": "#f4c56a",
            "--link": "#8db7ff",
            "--rule": "rgba(126, 139, 160, 0.2)",
            "--rule-strong": "rgba(126, 139, 160, 0.34)",
            "--row-stripe": "rgba(255, 255, 255, 0.025)",
            "--code-bg": "rgba(89, 208, 194, 0.12)",
            "--code-ink": "#a7fff4",
            "--code-border": "rgba(89, 208, 194, 0.3)",
            "--shadow": "rgba(0, 0, 0, 0.42)",
            "--hero-glow": "rgba(89, 208, 194, 0.2)",
            "--positive": "#ff7b8a",
            "--negative": "#42c39a",
            "--warning": "#f4c56a",
            "--panel-radius": "12px",
            "--section-shadow": "0 4px 6px -1px rgba(0, 0, 0, 0.14), 0 10px 24px -14px rgba(0, 0, 0, 0.24)",
            "--small-shadow": "0 2px 4px -1px rgba(0, 0, 0, 0.12), 0 8px 18px -14px rgba(0, 0, 0, 0.2)",
        },
    ),
    "abyss-gold": (
        "深渊暗金",
        {
            "--page-bg": "#0f1013",
            "--page-bg-2": "#181512",
            "--panel-bg": "rgba(24, 22, 20, 0.96)",
            "--panel-bg-strong": "rgba(20, 18, 17, 0.98)",
            "--panel-soft": "rgba(34, 30, 26, 0.95)",
            "--heading": "#f3e7ca",
            "--text": "#cdc4b6",
            "--text-soft": "#aba091",
            "--muted": "#8f8579",
            "--accent": "#c5a059",
            "--accent-strong": "#f0d49a",
            "--accent-soft": "rgba(197, 160, 89, 0.16)",
            "--accent-2": "#4f7b6c",
            "--link": "#d8c89f",
            "--rule": "rgba(197, 160, 89, 0.16)",
            "--rule-strong": "rgba(197, 160, 89, 0.3)",
            "--row-stripe": "rgba(197, 160, 89, 0.04)",
            "--code-bg": "rgba(197, 160, 89, 0.12)",
            "--code-ink": "#f3d79f",
            "--code-border": "rgba(197, 160, 89, 0.28)",
            "--shadow": "rgba(0, 0, 0, 0.52)",
            "--hero-glow": "rgba(197, 160, 89, 0.12)",
            "--positive": "#ba5f63",
            "--negative": "#678b76",
            "--warning": "#c5a059",
            "--panel-radius": "12px",
            "--section-shadow": "0 4px 6px -1px rgba(0, 0, 0, 0.16), 0 10px 24px -14px rgba(0, 0, 0, 0.24)",
            "--small-shadow": "0 2px 4px -1px rgba(0, 0, 0, 0.12), 0 8px 18px -14px rgba(0, 0, 0, 0.18)",
        },
    ),
    "institutional": (
        "机构终端",
        {
            "--page-bg": "#000000",
            "--page-bg-2": "#07090c",
            "--panel-bg": "rgba(7, 10, 14, 0.78)",
            "--panel-bg-strong": "rgba(5, 7, 10, 0.88)",
            "--panel-soft": "rgba(12, 17, 24, 0.68)",
            "--heading": "#ffd65a",
            "--text": "#d6dbe2",
            "--text-soft": "#a4adb9",
            "--muted": "#818b97",
            "--accent": "#ffbf00",
            "--accent-strong": "#ffe082",
            "--accent-soft": "rgba(255, 191, 0, 0.14)",
            "--accent-2": "#39ff14",
            "--link": "#8fc5ff",
            "--rule": "rgba(255, 255, 255, 0.10)",
            "--rule-strong": "rgba(255, 191, 0, 0.28)",
            "--row-stripe": "rgba(255, 255, 255, 0.015)",
            "--code-bg": "rgba(255, 191, 0, 0.1)",
            "--code-ink": "#ffe082",
            "--code-border": "rgba(255, 191, 0, 0.2)",
            "--shadow": "rgba(0, 0, 0, 0.58)",
            "--hero-glow": "rgba(255, 191, 0, 0.08)",
            "--hero-sheen": "rgba(255, 214, 90, 0.035)",
            "--positive": "#ff5f72",
            "--negative": "#48d48a",
            "--warning": "#ffbf00",
            "--panel-radius": "14px",
            "--figure-bg": "linear-gradient(180deg, rgba(255, 214, 90, 0.045), rgba(255, 255, 255, 0.01) 28%)",
            "--section-shadow": "0 10px 30px -22px rgba(0, 0, 0, 0.62), 0 24px 60px -42px rgba(0, 0, 0, 0.48)",
            "--figure-shadow": "0 18px 44px -34px rgba(0, 0, 0, 0.62), 0 10px 18px -18px rgba(255, 191, 0, 0.08)",
            "--small-shadow": "0 10px 24px -24px rgba(0, 0, 0, 0.52)",
            "--card-bg": "linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.015))",
            "--blockquote-bg": "linear-gradient(180deg, rgba(255, 191, 0, 0.075), rgba(255, 255, 255, 0.015))",
            "--details-bg": "linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.015))",
            "--details-summary-bg": "linear-gradient(90deg, rgba(255, 191, 0, 0.16), rgba(255, 255, 255, 0.02))",
        },
    ),
    "clinical": (
        "学术实验室",
        {
            "--page-bg": "#fbfbfd",
            "--page-bg-2": "#eef2f7",
            "--panel-bg": "rgba(255, 255, 255, 0.9)",
            "--panel-bg-strong": "rgba(245, 248, 253, 0.96)",
            "--panel-soft": "rgba(241, 245, 251, 0.95)",
            "--heading": "#1d1d1f",
            "--text": "#243040",
            "--text-soft": "#4e5f73",
            "--muted": "#86868b",
            "--accent": "#0071e3",
            "--accent-strong": "#005bc4",
            "--accent-soft": "rgba(0, 113, 227, 0.1)",
            "--accent-2": "#3d8bff",
            "--link": "#005fcc",
            "--rule": "rgba(0, 0, 0, 0.06)",
            "--rule-strong": "rgba(0, 113, 227, 0.18)",
            "--row-stripe": "rgba(0, 113, 227, 0.035)",
            "--code-bg": "rgba(0, 113, 227, 0.1)",
            "--code-ink": "#0c4f9e",
            "--code-border": "rgba(0, 113, 227, 0.12)",
            "--shadow": "rgba(15, 35, 66, 0.08)",
            "--hero-glow": "rgba(0, 113, 227, 0.1)",
            "--positive": "#e03a3e",
            "--negative": "#0f8a5f",
            "--warning": "#d28a16",
            "--panel-radius": "12px",
            "--color-scheme": "light",
            "--toolbar-bg": "rgba(255, 255, 255, 0.84)",
            "--button-active-top": "rgba(0, 113, 227, 0.08)",
            "--button-active-inset": "rgba(255, 255, 255, 0.74)",
            "--hero-sheen": "rgba(0, 113, 227, 0.035)",
            "--figure-bg": "linear-gradient(180deg, rgba(0, 113, 227, 0.035), rgba(255, 255, 255, 0.65) 32%)",
            "--table-bg": "rgba(255, 255, 255, 0.88)",
            "--table-head-bg": "linear-gradient(180deg, rgba(0, 113, 227, 0.08), rgba(0, 113, 227, 0.13))",
            "--blockquote-bg": "linear-gradient(180deg, rgba(0, 113, 227, 0.035), rgba(255, 255, 255, 0.9))",
            "--ordered-bg": "linear-gradient(180deg, rgba(0, 113, 227, 0.04), rgba(255, 255, 255, 0.88))",
            "--details-bg": "linear-gradient(180deg, rgba(0, 113, 227, 0.035), rgba(255, 255, 255, 0.9))",
            "--details-summary-bg": "linear-gradient(90deg, rgba(0, 113, 227, 0.12), rgba(255, 255, 255, 0.65))",
            "--link-underline": "rgba(0, 113, 227, 0.28)",
            "--section-shadow": "0 1px 2px rgba(0, 0, 0, 0.04), 0 4px 12px rgba(0, 0, 0, 0.03), 0 12px 24px rgba(0, 0, 0, 0.02)",
            "--figure-shadow": "0 1px 2px rgba(0, 0, 0, 0.03), 0 6px 16px rgba(15, 35, 66, 0.04), 0 12px 24px rgba(15, 35, 66, 0.03)",
            "--small-shadow": "0 1px 2px rgba(0, 0, 0, 0.03), 0 4px 12px rgba(0, 0, 0, 0.025), 0 12px 24px rgba(0, 0, 0, 0.02)",
            "--table-rule": "rgba(0, 0, 0, 0.05)",
            "--table-hover": "rgba(0, 0, 0, 0.02)",
            "--figure-caption": "#86868b",
            "--pill-bull-bg": "rgba(224, 58, 62, 0.1)",
            "--pill-bull-fg": "#e03a3e",
            "--pill-bull-border": "rgba(224, 58, 62, 0.12)",
            "--pill-bear-bg": "rgba(15, 138, 95, 0.1)",
            "--pill-bear-fg": "#0f8a5f",
            "--pill-bear-border": "rgba(15, 138, 95, 0.12)",
            "--pill-warn-bg": "rgba(210, 138, 22, 0.12)",
            "--pill-warn-fg": "#b96d00",
            "--pill-warn-border": "rgba(210, 138, 22, 0.14)",
            "--pill-neutral-bg": "rgba(29, 29, 31, 0.06)",
            "--pill-neutral-fg": "#4e5f73",
            "--pill-neutral-border": "rgba(29, 29, 31, 0.07)",
            "--page-pattern": "linear-gradient(transparent, transparent)",
        },
    ),
    "erdtree": (
        "黄金树微光",
        {
            "--page-bg": "#fdfbf7",
            "--page-bg-2": "#f4ecdc",
            "--panel-bg": "rgba(255, 250, 242, 0.93)",
            "--panel-bg-strong": "rgba(249, 243, 233, 0.98)",
            "--panel-soft": "rgba(245, 238, 224, 0.96)",
            "--heading": "#3e352c",
            "--text": "#4b4034",
            "--text-soft": "#6b5e50",
            "--muted": "#8b7d6e",
            "--accent": "#d4af37",
            "--accent-strong": "#8f6912",
            "--accent-soft": "rgba(212, 175, 55, 0.1)",
            "--accent-2": "#6a7a58",
            "--link": "#8c5e00",
            "--rule": "rgba(62, 53, 44, 0.06)",
            "--rule-strong": "rgba(212, 175, 55, 0.18)",
            "--row-stripe": "rgba(212, 175, 55, 0.05)",
            "--code-bg": "rgba(212, 175, 55, 0.1)",
            "--code-ink": "#7f5a00",
            "--code-border": "rgba(212, 175, 55, 0.12)",
            "--shadow": "rgba(73, 53, 23, 0.1)",
            "--hero-glow": "rgba(212, 175, 55, 0.12)",
            "--positive": "#b24a4a",
            "--negative": "#6a845d",
            "--warning": "#d4af37",
            "--panel-radius": "12px",
            "--color-scheme": "light",
            "--toolbar-bg": "rgba(253, 249, 240, 0.9)",
            "--button-active-top": "rgba(212, 175, 55, 0.1)",
            "--button-active-inset": "rgba(255, 247, 231, 0.72)",
            "--hero-sheen": "rgba(212, 175, 55, 0.05)",
            "--figure-bg": "linear-gradient(180deg, rgba(212, 175, 55, 0.06), rgba(255, 251, 244, 0.78) 32%)",
            "--table-bg": "rgba(255, 251, 244, 0.88)",
            "--table-head-bg": "linear-gradient(180deg, rgba(212, 175, 55, 0.1), rgba(212, 175, 55, 0.16))",
            "--blockquote-bg": "linear-gradient(180deg, rgba(212, 175, 55, 0.05), rgba(255, 249, 238, 0.9))",
            "--ordered-bg": "linear-gradient(180deg, rgba(212, 175, 55, 0.06), rgba(255, 251, 244, 0.9))",
            "--details-bg": "linear-gradient(180deg, rgba(212, 175, 55, 0.05), rgba(255, 249, 238, 0.9))",
            "--details-summary-bg": "linear-gradient(90deg, rgba(212, 175, 55, 0.14), rgba(255, 248, 232, 0.68))",
            "--link-underline": "rgba(140, 94, 0, 0.28)",
            "--section-shadow": "0 1px 2px rgba(62, 53, 44, 0.05), 0 4px 12px rgba(87, 64, 30, 0.04), 0 12px 24px rgba(87, 64, 30, 0.03)",
            "--figure-shadow": "0 1px 2px rgba(62, 53, 44, 0.05), 0 6px 16px rgba(87, 64, 30, 0.05), 0 12px 24px rgba(87, 64, 30, 0.03)",
            "--small-shadow": "0 1px 2px rgba(62, 53, 44, 0.04), 0 4px 12px rgba(87, 64, 30, 0.03), 0 12px 24px rgba(87, 64, 30, 0.02)",
            "--table-rule": "rgba(62, 53, 44, 0.05)",
            "--table-hover": "rgba(62, 53, 44, 0.02)",
            "--figure-caption": "#8b7d6e",
            "--pill-bull-bg": "rgba(178, 74, 74, 0.11)",
            "--pill-bull-fg": "#9b3f3f",
            "--pill-bull-border": "rgba(178, 74, 74, 0.13)",
            "--pill-bear-bg": "rgba(106, 132, 93, 0.12)",
            "--pill-bear-fg": "#546a49",
            "--pill-bear-border": "rgba(106, 132, 93, 0.14)",
            "--pill-warn-bg": "rgba(212, 175, 55, 0.12)",
            "--pill-warn-fg": "#8f6912",
            "--pill-warn-border": "rgba(212, 175, 55, 0.14)",
            "--pill-neutral-bg": "rgba(62, 53, 44, 0.06)",
            "--pill-neutral-fg": "#6b5e50",
            "--pill-neutral-border": "rgba(62, 53, 44, 0.07)",
            "--page-pattern": "linear-gradient(transparent, transparent)",
        },
    ),
    "neo-brutal": (
        "高能波普",
        {
            "--page-bg": "#ffffff",
            "--page-bg-2": "#f7f7fb",
            "--panel-bg": "#ffffff",
            "--panel-bg-strong": "#ffffff",
            "--panel-soft": "#fafafc",
            "--heading": "#000000",
            "--text": "#111111",
            "--text-soft": "#202020",
            "--muted": "#565656",
            "--accent": "#00a1d6",
            "--accent-strong": "#000000",
            "--accent-soft": "rgba(0, 161, 214, 0.14)",
            "--accent-2": "#ff8ba7",
            "--link": "#004bde",
            "--rule": "#000000",
            "--rule-strong": "#000000",
            "--row-stripe": "rgba(255, 139, 167, 0.07)",
            "--code-bg": "rgba(255, 139, 167, 0.12)",
            "--code-ink": "#000000",
            "--code-border": "#000000",
            "--shadow": "rgba(0, 0, 0, 0.95)",
            "--hero-glow": "rgba(0, 161, 214, 0.07)",
            "--positive": "#ff5a5f",
            "--negative": "#00a36c",
            "--warning": "#ff8ba7",
            "--panel-radius": "10px",
            "--color-scheme": "light",
            "--toolbar-bg": "rgba(255, 255, 255, 0.94)",
            "--button-active-top": "rgba(255, 139, 167, 0.22)",
            "--button-active-inset": "rgba(255, 255, 255, 0.0)",
            "--hero-sheen": "rgba(255, 139, 167, 0.08)",
            "--figure-bg": "linear-gradient(180deg, rgba(255, 139, 167, 0.12), rgba(255, 255, 255, 0.72) 28%)",
            "--table-bg": "#ffffff",
            "--table-head-bg": "linear-gradient(180deg, rgba(0, 161, 214, 0.12), rgba(255, 139, 167, 0.22))",
            "--blockquote-bg": "linear-gradient(180deg, rgba(255, 139, 167, 0.1), rgba(255, 255, 255, 0.95))",
            "--ordered-bg": "linear-gradient(180deg, rgba(0, 161, 214, 0.08), rgba(255, 255, 255, 0.96))",
            "--details-bg": "linear-gradient(180deg, rgba(255, 139, 167, 0.08), rgba(255, 255, 255, 0.96))",
            "--details-summary-bg": "linear-gradient(90deg, rgba(0, 161, 214, 0.18), rgba(255, 139, 167, 0.2))",
            "--link-underline": "rgba(0, 75, 222, 0.35)",
            "--section-shadow": "6px 6px 0px #000000",
            "--figure-shadow": "6px 6px 0px #000000",
            "--small-shadow": "4px 4px 0px #000000",
            "--table-rule": "rgba(0, 0, 0, 0.09)",
            "--table-hover": "rgba(0, 0, 0, 0.02)",
            "--figure-caption": "#565656",
            "--pill-bull-bg": "rgba(255, 90, 95, 0.14)",
            "--pill-bull-fg": "#000000",
            "--pill-bull-border": "#000000",
            "--pill-bear-bg": "rgba(0, 163, 108, 0.14)",
            "--pill-bear-fg": "#000000",
            "--pill-bear-border": "#000000",
            "--pill-warn-bg": "rgba(255, 139, 167, 0.18)",
            "--pill-warn-fg": "#000000",
            "--pill-warn-border": "#000000",
            "--pill-neutral-bg": "rgba(0, 0, 0, 0.06)",
            "--pill-neutral-fg": "#000000",
            "--pill-neutral-border": "#000000",
            "--page-pattern": "repeating-radial-gradient(circle at 0 0, rgba(0, 0, 0, 0.04) 0 1.2px, transparent 1.2px 18px)",
        },
    ),
}


def _normalize_report_theme(theme: str | None = None) -> str:
    candidate = (theme or os.getenv("AI_FINANCE_REPORT_THEME") or _DEFAULT_REPORT_THEME).strip().lower()
    if candidate not in _REPORT_THEME_PRESETS:
        return _DEFAULT_REPORT_THEME
    return candidate


def _render_theme_css() -> str:
    blocks: List[str] = []
    for theme_name, (_, variables) in _REPORT_THEME_PRESETS.items():
        lines = [f"body.theme-{theme_name} {{"]
        for key, value in variables.items():
            lines.append(f"  {key}: {value};")
        lines.append("}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _normalize_print_report_theme(default_theme: str | None = None) -> str:
    candidate = (os.getenv("AI_FINANCE_PRINT_THEME") or "").strip().lower()
    if not candidate or candidate in {"light", "default", "clinical"}:
        return "light"
    if candidate in {"same", "screen", "follow-screen"}:
        return (default_theme or _normalize_report_theme()).strip().lower()
    if candidate in _REPORT_THEME_PRESETS:
        return candidate
    return "light"


def _render_light_print_css() -> str:
    return """
@media print {
  body {
    padding: 0;
    background: #ffffff !important;
    color: #111111 !important;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
    --page-bg: #ffffff;
    --page-bg-2: #ffffff;
    --panel-bg: #ffffff;
    --panel-bg-strong: #ffffff;
    --panel-soft: #ffffff;
    --heading: #111111;
    --text: #16181d;
    --text-soft: #313843;
    --muted: #5b6574;
    --accent: #0b57d0;
    --accent-strong: #0b57d0;
    --accent-soft: rgba(11, 87, 208, 0.08);
    --accent-2: #d93025;
    --link: #0b57d0;
    --rule: rgba(17, 17, 17, 0.08);
    --rule-strong: rgba(11, 87, 208, 0.18);
    --row-stripe: rgba(17, 17, 17, 0.025);
    --code-bg: rgba(11, 87, 208, 0.06);
    --code-ink: #0b57d0;
    --code-border: rgba(11, 87, 208, 0.12);
    --shadow: rgba(0, 0, 0, 0);
    --hero-glow: rgba(0, 0, 0, 0);
    --positive: #d93025;
    --negative: #188038;
    --warning: #b26a00;
    --section-shadow: none;
    --small-shadow: none;
    --table-bg: #ffffff;
    --table-head-bg: rgba(11, 87, 208, 0.08);
    --blockquote-bg: #f8faff;
    --ordered-bg: #f8faff;
    --details-bg: #fafbfd;
    --details-summary-bg: rgba(11, 87, 208, 0.06);
    --figure-bg: transparent;
    --figure-shadow: none;
    --table-rule: rgba(17, 17, 17, 0.08);
    --table-hover: transparent;
    --figure-caption: #5b6574;
    --pill-bull-bg: rgba(217, 48, 37, 0.08);
    --pill-bull-fg: #d93025;
    --pill-bull-border: rgba(217, 48, 37, 0.16);
    --pill-bear-bg: rgba(24, 128, 56, 0.08);
    --pill-bear-fg: #188038;
    --pill-bear-border: rgba(24, 128, 56, 0.16);
    --pill-warn-bg: rgba(178, 106, 0, 0.08);
    --pill-warn-fg: #9a5b00;
    --pill-warn-border: rgba(178, 106, 0, 0.16);
    --pill-neutral-bg: rgba(17, 17, 17, 0.05);
    --pill-neutral-fg: #495364;
    --pill-neutral-border: rgba(17, 17, 17, 0.08);
    --page-pattern: linear-gradient(transparent, transparent);
  }

  .report-shell {
    display: block;
  }

  .report-sidebar,
  .report-toolbar {
    display: none;
  }

  section.report-hero,
  section.report-section {
    background: #ffffff !important;
    box-shadow: none !important;
    border: 1px solid rgba(17, 17, 17, 0.06);
    break-inside: auto !important;
    page-break-inside: auto !important;
    overflow: visible !important;
  }

  figure,
  table,
  blockquote,
  details.report-details,
  .report-summary-card,
  .report-callout {
    background: transparent !important;
    box-shadow: none;
  }
}
""".strip()


def _render_themed_print_css(print_theme_name: str) -> str:
    _, base_variables = _REPORT_THEME_PRESETS.get(print_theme_name, _REPORT_THEME_PRESETS[_DEFAULT_PRINT_REPORT_THEME])
    variables = dict(base_variables)
    variables["--shadow"] = "rgba(0, 0, 0, 0)"
    variables["--hero-glow"] = "rgba(0, 0, 0, 0)"
    variables["--section-shadow"] = "none"
    variables["--small-shadow"] = "none"
    variables["--figure-shadow"] = "none"
    variables["--page-pattern"] = "linear-gradient(transparent, transparent)"
    if print_theme_name in {"terminal", "abyss-gold", "institutional", "neo-brutal"}:
        variables.setdefault("--color-scheme", "dark")
    else:
        variables.setdefault("--color-scheme", "light")
    variable_lines = "\n".join(f"    {key}: {value};" for key, value in variables.items())
    return f"""
@media print {{
  body {{
    padding: 0;
    background:
      var(--page-pattern, linear-gradient(transparent, transparent)),
      linear-gradient(180deg, var(--page-bg), var(--page-bg-2)) !important;
    color: var(--text) !important;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
{variable_lines}
  }}

  .report-shell {{
    display: block;
  }}

  .report-sidebar,
  .report-toolbar {{
    display: none;
  }}

  section.report-hero,
  section.report-section {{
    background: var(--section-bg, var(--panel-bg)) !important;
    box-shadow: none !important;
    border: 1px solid var(--rule);
    break-inside: auto !important;
    page-break-inside: auto !important;
    overflow: visible !important;
    backdrop-filter: none !important;
    -webkit-backdrop-filter: none !important;
  }}

  section.report-figure-block,
  .report-summary-card,
  .report-callout,
  details.report-details {{
    background: var(--panel-soft) !important;
    box-shadow: none !important;
  }}

  figure.report-figure img,
  figure.report-figure img[data-vector-chart="true"] {{
    background: var(--figure-bg, transparent) !important;
    border-color: var(--rule) !important;
    box-shadow: none !important;
  }}

  table,
  thead th,
  tbody tr:nth-child(even) td,
  blockquote {{
    box-shadow: none !important;
  }}
}}
""".strip()


def _render_print_css(default_theme: str) -> str:
    print_theme_name = _normalize_print_report_theme(default_theme)
    if print_theme_name == "light":
        return _render_light_print_css()
    return _render_themed_print_css(print_theme_name)


def _render_html_style(default_theme: str) -> str:
    return _HTML_STYLE.replace("__PRINT_CSS__", _render_print_css(default_theme))


_HTML_STYLE = (
    """
@page {
  size: A4;
  margin: 12mm 10mm 12mm 10mm;
}

:root {
  color-scheme: var(--color-scheme, dark);
  --body-size: 14px;
  --prose-max-width: 760px;
  --section-gap: 34px;
  --section-padding-y: 28px;
  --section-padding-x: 28px;
  --table-cell-y: 10px;
  --table-cell-x: 16px;
  --table-font-size: 13px;
  --card-gap: 18px;
}

html, body {
  min-height: 100%;
  font-family: "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
  color: var(--text);
  line-height: 1.68;
  font-size: var(--body-size);
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
  font-kerning: normal;
  text-size-adjust: 100%;
  font-variant-numeric: tabular-nums lining-nums;
}

"""
    + _render_theme_css()
    + """

body {
  max-width: 1480px;
  margin: 0 auto;
  padding: 14px 18px 28px 18px;
  background:
    var(--page-pattern, linear-gradient(transparent, transparent)),
    radial-gradient(circle at top right, var(--hero-glow), transparent 38%),
    linear-gradient(180deg, var(--page-bg), var(--page-bg-2));
}

.report-shell {
  display: grid;
  grid-template-columns: 224px minmax(0, 1fr) 178px;
  gap: 22px;
  align-items: start;
}

.report-sidebar {
  position: sticky;
  top: 12px;
  align-self: start;
}

.report-sidebar-left,
.report-sidebar-right {
  min-width: 0;
}

main.markdown-body {
  display: flex;
  flex-direction: column;
  gap: var(--section-gap);
  padding: 1mm 0 4mm 0;
  min-width: 0;
}

.report-outline {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 14px;
  background: var(--panel-bg);
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.6 + 4px);
  box-shadow: var(--small-shadow, 0 12px 24px -22px var(--shadow));
}

.report-outline-title {
  font-size: 11px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--muted);
  padding: 2px 2px 6px 2px;
}

.report-outline-list {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.report-toc-link {
  display: block;
  padding: 9px 12px;
  border: 1px solid var(--rule);
  border-left: 3px solid transparent;
  background: var(--panel-soft);
  color: var(--text-soft);
  text-decoration: none;
  border-radius: calc(var(--panel-radius) * 0.45 + 1px);
  transition: border-color 0.16s ease, background 0.16s ease, color 0.16s ease, transform 0.16s ease;
}

.report-toc-link:hover,
.report-toc-link:focus-visible {
  border-color: var(--rule-strong);
  background: linear-gradient(180deg, var(--accent-soft), rgba(255, 255, 255, 0.02));
  color: var(--heading);
  outline: none;
  transform: translateX(2px);
}

.report-toc-link.level-1 {
  font-weight: 600;
  color: var(--heading);
}

.report-toc-link.level-2 {
  padding-left: 12px;
}

.report-toc-link.level-3 {
  padding-left: 20px;
  font-size: 12px;
}

.report-toc-link.is-current {
  border-color: var(--rule-strong);
  border-left-color: var(--accent);
  background: var(--toc-active-bg, var(--accent-soft));
  color: var(--toc-active-color, var(--heading));
  font-weight: 700;
  box-shadow: inset 2px 0 0 var(--accent);
}

.report-toolbar {
  display: flex;
  justify-content: flex-start;
  align-items: stretch;
  margin: 0;
  position: sticky;
  top: 12px;
  z-index: 5;
}

.report-theme-switcher {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 7px;
  width: 100%;
  padding: 10px;
  background: var(--toolbar-bg, rgba(8, 10, 14, 0.72));
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.55 + 2px);
  box-shadow: var(--small-shadow, 0 16px 32px -28px var(--shadow));
  backdrop-filter: blur(10px);
}

.report-theme-switcher::before {
  content: "Theme";
  padding: 0 2px 4px 2px;
  color: var(--muted);
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-size: 10px;
}

.report-theme-button {
  appearance: none;
  border: 1px solid transparent;
  width: 100%;
  text-align: left;
  padding: 8px 11px;
  border-radius: calc(var(--panel-radius) * 0.35 + 3px);
  background: transparent;
  color: var(--text-soft);
  font: inherit;
  font-size: 11px;
  cursor: pointer;
  transition: background 0.16s ease, border-color 0.16s ease, color 0.16s ease;
}

.report-theme-button:hover,
.report-theme-button:focus-visible {
  background: var(--accent-soft);
  border-color: var(--rule-strong);
  color: var(--heading);
  outline: none;
}

.report-theme-button.is-active {
  background: linear-gradient(180deg, var(--button-active-top, rgba(255, 255, 255, 0.06)), var(--accent-soft));
  border-color: var(--rule-strong);
  color: var(--accent-strong);
  box-shadow: inset 0 0 0 1px var(--button-active-inset, rgba(255, 255, 255, 0.02));
}

@media (max-width: 1220px) {
  .report-shell {
    grid-template-columns: 1fr;
  }

  .report-sidebar {
    position: static;
  }

  .report-sidebar-left {
    order: 0;
  }

  main.markdown-body {
    order: 1;
  }

  .report-sidebar-right {
    order: 2;
  }

  .report-theme-switcher {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    align-items: center;
  }

  .report-theme-switcher::before {
    grid-column: 1 / -1;
  }
}

section.report-hero,
section.report-section {
  position: relative;
  overflow: hidden;
  padding: var(--section-padding-y) var(--section-padding-x);
  background: var(--section-bg, var(--panel-bg));
  border: 1px solid var(--rule);
  border-radius: var(--panel-radius);
  box-shadow: var(--section-shadow, 0 4px 6px -1px rgba(0, 0, 0, 0.08), 0 12px 24px -16px var(--shadow));
  backdrop-filter: blur(14px) saturate(115%);
  -webkit-backdrop-filter: blur(14px) saturate(115%);
  break-inside: avoid;
  page-break-inside: avoid;
}

section.report-hero::after,
section.report-section::after {
  content: "";
  position: absolute;
  inset: 0 auto auto 0;
  width: 100%;
  height: 1px;
  background: linear-gradient(90deg, transparent, var(--rule-strong), transparent);
  opacity: 0.88;
}

section.report-hero {
  padding-top: calc(var(--section-padding-y) + 2px);
  background:
    linear-gradient(180deg, var(--hero-sheen, rgba(255, 255, 255, 0.015)), transparent 28%),
    var(--hero-bg, var(--section-bg, var(--panel-bg)));
}

section.report-section > h2:first-child,
section.report-hero > h1:first-child {
  margin-top: 0;
}

h1, h2, h3, h4 {
  font-family: "SF Pro Display", "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
  color: var(--heading);
  margin-top: 1.05em;
  margin-bottom: 0.45em;
  page-break-after: avoid;
  letter-spacing: 0.01em;
}

h1 {
  position: relative;
  font-size: 28px;
  line-height: 1.24;
  padding-bottom: 12px;
  margin-bottom: 0.8em;
}

h1::after {
  content: "";
  position: absolute;
  left: 0;
  bottom: 0;
  width: 112px;
  height: 3px;
  border-radius: 999px;
  background: linear-gradient(90deg, var(--accent), transparent);
}

h2 {
  font-size: 22px;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 0 10px 0;
  border-bottom: 1px solid var(--rule);
}

h2::before {
  content: "";
  width: 7px;
  height: 1.7em;
  border-radius: 999px;
  background: linear-gradient(180deg, var(--accent), var(--accent-2));
  flex: 0 0 auto;
}

h3 {
  font-size: 18px;
  font-weight: 600;
  color: var(--accent-strong);
  padding-bottom: 4px;
  border-bottom: 1px dashed var(--rule);
}

h4 {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-soft);
  margin-top: 0.95em;
  margin-bottom: 0.35em;
}

p, li, blockquote {
  font-family: "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
  line-height: 1.76;
}

table, th, td, code, pre {
  font-family: "JetBrains Mono", "SFMono-Regular", "Menlo", "Consolas", "PingFang SC", "Microsoft YaHei", monospace;
}

p {
  margin: 0.55em 0 0.95em;
  color: var(--text);
  text-align: left;
  letter-spacing: 0.01em;
  word-break: break-word;
}

section.report-hero > p,
section.report-hero > blockquote,
section.report-hero > details.report-details,
section.report-section > p,
section.report-section > blockquote,
section.report-section > details.report-details {
  width: min(100%, var(--prose-max-width));
  max-width: var(--prose-max-width);
  margin-left: 0;
  margin-right: auto;
}

h2 + p,
h2 + blockquote,
h2 + ol,
h2 + ul,
h2 + table {
  margin-top: 0.9em;
}

strong {
  font-weight: 700;
  color: var(--accent-strong);
}

em {
  font-style: italic;
  color: var(--accent-2);
}

mark {
  background: linear-gradient(180deg, var(--accent-soft), var(--accent-soft));
  color: var(--heading);
  padding: 0.06em 0.28em;
  border-radius: 6px;
}

code {
  background: var(--code-bg);
  color: var(--code-ink);
  padding: 0.14em 0.42em;
  border-radius: calc(var(--panel-radius) * 0.4 + 2px);
  border: 1px solid var(--code-border);
  font-size: 0.94em;
}

.report-pill {
  display: inline-flex;
  align-items: center;
  padding: 0.18em 0.58em;
  margin: 0 0.08em;
  border-radius: 999px;
  font-size: 0.92em;
  font-weight: 600;
  line-height: 1.25;
  letter-spacing: 0.015em;
  border: 1px solid transparent;
  vertical-align: baseline;
  white-space: nowrap;
}

.report-pill.is-bull {
  background: var(--pill-bull-bg, var(--accent-soft));
  color: var(--pill-bull-fg, var(--positive));
  border-color: var(--pill-bull-border, transparent);
}

.report-pill.is-bear {
  background: var(--pill-bear-bg, rgba(255, 107, 107, 0.14));
  color: var(--pill-bear-fg, var(--negative));
  border-color: var(--pill-bear-border, transparent);
}

.report-pill.is-warn {
  background: var(--pill-warn-bg, rgba(255, 191, 0, 0.14));
  color: var(--pill-warn-fg, var(--warning));
  border-color: var(--pill-warn-border, transparent);
}

.report-pill.is-neutral {
  background: var(--pill-neutral-bg, rgba(255, 255, 255, 0.08));
  color: var(--pill-neutral-fg, var(--text-soft));
  border-color: var(--pill-neutral-border, transparent);
}

pre {
  background: var(--panel-soft);
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.7 + 1px);
  padding: 12px 14px;
  overflow: hidden;
}

img {
  width: 100%;
  max-width: 100%;
  height: auto;
  display: block;
  margin: 8px auto 18px auto;
}

picture.report-picture {
  display: block;
  width: 100%;
}

figure.report-figure {
  margin: 14px auto 22px auto;
  text-align: center;
  width: 100%;
}

figure.report-figure img {
  width: 100%;
  max-width: 100%;
  max-height: none;
  object-fit: contain;
  box-sizing: border-box;
  border-radius: calc(var(--panel-radius) * 0.9);
  border: 1px solid var(--rule);
  background: var(--figure-bg, transparent);
  box-shadow: var(--figure-shadow, 0 16px 30px -28px var(--shadow));
}

figure.report-figure img[data-vector-chart="true"] {
  padding: 10px 10px 8px 10px;
  box-sizing: border-box;
  background: var(--figure-bg, transparent);
}

figure.report-figure figcaption {
  margin-top: 6px;
  font-size: 11px;
  color: var(--figure-caption, var(--muted));
  letter-spacing: 0.04em;
}

h3 + figure.report-figure,
h4 + figure.report-figure {
  margin-top: 8px;
}

section.report-figure-block {
  margin: 4px 0 22px 0;
  padding: 18px 18px 10px 18px;
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.9);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.012));
  box-shadow: var(--small-shadow, 0 12px 22px -24px var(--shadow));
}

section.report-figure-block > h3,
section.report-figure-block > h4 {
  margin-top: 0;
  margin-bottom: 0.45em;
  border-bottom: 0;
}

section.report-figure-block > figure.report-figure {
  margin-top: 10px;
}

table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  margin: 14px 0 18px 0;
  font-size: var(--table-font-size);
  border: 1px solid var(--table-rule, var(--rule));
  border-radius: calc(var(--panel-radius) * 0.72 + 1px);
  overflow: hidden;
  background: var(--table-bg, rgba(255, 255, 255, 0.012));
}

th, td {
  padding: var(--table-cell-y) var(--table-cell-x);
  vertical-align: top;
  border: 0;
  border-bottom: 1px solid var(--table-rule, var(--rule));
  line-height: 1.66;
}

thead th {
  background: var(--table-head-bg, linear-gradient(180deg, rgba(255, 255, 255, 0.06), var(--accent-soft)));
  color: var(--heading);
  font-weight: 700;
  letter-spacing: 0.02em;
}

th.cell-num,
td.cell-num {
  text-align: right;
}

td.cell-num {
  font-family: "DIN Alternate", "Inter", "JetBrains Mono", "SFMono-Regular", "Menlo", monospace;
  font-weight: 600;
  color: var(--heading);
  white-space: nowrap;
}

th.cell-text,
td.cell-text {
  text-align: left;
}

tbody tr:nth-child(even) td {
  background: var(--row-stripe);
}

@media screen {
  tbody tr:hover td {
    background: var(--table-hover, transparent);
  }
}

tbody tr:last-child td {
  border-bottom: 0;
}

.report-summary-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: var(--card-gap);
  margin: 14px 0 18px 0;
}

.report-summary-card {
  position: relative;
  padding: 18px 18px 16px 18px;
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.72 + 2px);
  background: var(--card-bg, var(--panel-soft));
  box-shadow: var(--small-shadow, 0 12px 22px -24px var(--shadow));
  overflow: hidden;
}

.report-summary-card::before {
  content: "";
  position: absolute;
  left: 14px;
  top: 0;
  width: 68px;
  height: 3px;
  border-radius: 999px;
  background: linear-gradient(90deg, var(--accent), transparent);
}

.report-summary-key {
  margin: 0 0 8px 0;
  font-size: 11px;
  letter-spacing: 0.08em;
  color: var(--muted);
  text-transform: uppercase;
}

.report-summary-value {
  margin: 0;
  color: var(--heading);
  font-family: "DIN Alternate", "Inter", "JetBrains Mono", "SFMono-Regular", "Menlo", monospace;
  font-size: 15px;
  font-weight: 600;
  line-height: 1.58;
}

blockquote {
  margin: 14px 0;
  padding: 14px 18px;
  color: var(--text-soft);
  background: var(--blockquote-bg, var(--panel-soft));
  border-left: 4px solid var(--accent);
  border-radius: calc(var(--panel-radius) * 0.75);
  box-shadow: var(--small-shadow, 0 12px 22px -24px var(--shadow));
}

ul, ol {
  margin: 0.4em 0 1em;
  padding-left: 0;
}

ul {
  list-style: none;
}

ul > li {
  position: relative;
  margin: 0 0 8px 0;
  padding-left: 16px;
}

ul > li::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0.72em;
  width: 6px;
  height: 6px;
  border-radius: 999px;
  background: var(--accent);
  box-shadow: 0 0 0 4px var(--accent-soft);
}

ol {
  counter-reset: report-counter;
  list-style: none;
}

ol > li {
  counter-increment: report-counter;
  position: relative;
  margin: 0 0 10px 0;
  padding: 12px 14px 12px 48px;
  background: var(--ordered-bg, var(--panel-soft));
  border: 1px solid var(--rule);
  border-left: 3px solid var(--accent);
  border-radius: calc(var(--panel-radius) * 0.75 + 1px);
  box-shadow: var(--small-shadow, 0 12px 24px -24px var(--shadow));
}

ol > li::before {
  content: counter(report-counter) ".";
  position: absolute;
  left: 14px;
  top: 11px;
  font-family: "JetBrains Mono", "SFMono-Regular", monospace;
  font-weight: 700;
  color: var(--accent);
}

li strong:first-child {
  display: inline-block;
  margin-right: 0.18em;
}

a {
  color: var(--link);
  text-decoration-color: var(--link-underline, rgba(141, 183, 255, 0.35));
}

hr {
  border: 0;
  border-top: 1px solid var(--rule);
  margin: 18px 0;
}

details.report-details {
  margin: 14px 0 16px 0;
  padding: 0;
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.75 + 2px);
  background: var(--details-bg, var(--panel-soft));
  box-shadow: var(--small-shadow, 0 12px 22px -24px var(--shadow));
  overflow: hidden;
}

details.report-details > summary {
  cursor: pointer;
  list-style: none;
  padding: 10px 14px;
  font-size: 14px;
  font-weight: 600;
  color: var(--accent-strong);
  background: var(--details-summary-bg, linear-gradient(90deg, var(--accent-soft), rgba(255, 255, 255, 0.01)));
  border-bottom: 1px solid var(--rule);
}

details.report-details > summary::-webkit-details-marker {
  display: none;
}

details.report-details > summary::before {
  content: "▸";
  display: inline-block;
  margin-right: 8px;
  color: var(--accent);
  transition: transform 0.18s ease;
}

details.report-details[open] > summary::before {
  transform: rotate(90deg);
}

details.report-details > *:not(summary) {
  margin-left: 14px;
  margin-right: 14px;
}

details.report-details > *:last-child {
  margin-bottom: 14px;
}

.report-page-break {
  height: 0;
  margin: 0;
  padding: 0;
  border: 0;
  break-before: page;
  page-break-before: always;
}

@media screen {
  .report-page-break {
    display: none;
  }
}

__PRINT_CSS__
""".strip()
)
_THEME_SWITCHER_SCRIPT = (
    """
<script>
(() => {
  const storageKey = "__STORAGE_KEY__";
  const body = document.body;
  if (!body) {
    return;
  }
  const buttons = Array.from(document.querySelectorAll("[data-report-theme]"));
  const themedImages = Array.from(document.querySelectorAll("[data-theme-switchable='true']"));
  const tocLinks = Array.from(document.querySelectorAll(".report-toc-link"));
  const supported = new Set(buttons.map((button) => button.getAttribute("data-report-theme")));
  const applyTheme = (theme) => {
    if (!supported.has(theme)) {
      return;
    }
    Array.from(body.classList)
      .filter((name) => name.startsWith("theme-"))
      .forEach((name) => body.classList.remove(name));
    body.classList.add(`theme-${theme}`);
    body.dataset.reportTheme = theme;
    buttons.forEach((button) => {
      const active = button.getAttribute("data-report-theme") === theme;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    themedImages.forEach((image) => {
      const themedSrc = image.getAttribute(`data-theme-src-${theme}`);
      if (!themedSrc) {
        return;
      }
      if (image.getAttribute("src") !== themedSrc) {
        image.setAttribute("src", themedSrc);
      }
    });
  };

  const defaultTheme = body.dataset.defaultTheme || "__DEFAULT_THEME__";
  let storedTheme = defaultTheme;
  try {
    storedTheme = localStorage.getItem(storageKey) || defaultTheme;
  } catch (_error) {
    storedTheme = defaultTheme;
  }
  if (!supported.has(storedTheme)) {
    storedTheme = defaultTheme;
  }
  applyTheme(storedTheme);
  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const theme = button.getAttribute("data-report-theme");
      if (!theme) {
        return;
      }
      applyTheme(theme);
      try {
        localStorage.setItem(storageKey, theme);
      } catch (_error) {
        // Ignore storage failures in local file contexts.
      }
    });
  });

  const setCurrentToc = (id) => {
    if (!id) {
      return;
    }
    tocLinks.forEach((link) => {
      const href = link.getAttribute("href") || "";
      const targetId = href.startsWith("#") ? decodeURIComponent(href.slice(1)) : "";
      link.classList.toggle("is-current", targetId === id);
    });
  };

  const tocTargets = tocLinks
    .map((link) => {
      const href = link.getAttribute("href") || "";
      const id = href.startsWith("#") ? decodeURIComponent(href.slice(1)) : "";
      const target = id ? document.getElementById(id) : null;
      if (!target) {
        return null;
      }
      return { id, target };
    })
    .filter(Boolean);

  if (tocTargets.length) {
    const initialId = decodeURIComponent((window.location.hash || "").replace(/^#/, "")) || tocTargets[0].id;
    setCurrentToc(initialId);
    if ("IntersectionObserver" in window) {
      let activeId = initialId;
      const observer = new IntersectionObserver(
        (entries) => {
          const visible = entries
            .filter((entry) => entry.isIntersecting)
            .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
          if (!visible.length) {
            return;
          }
          const nextId = visible[0].target.getAttribute("id");
          if (nextId && nextId !== activeId) {
            activeId = nextId;
            setCurrentToc(activeId);
          }
        },
        {
          rootMargin: "-18% 0px -70% 0px",
          threshold: [0.05, 0.2, 0.6],
        }
      );
      tocTargets.forEach(({ target }) => observer.observe(target));
    }
    window.addEventListener("hashchange", () => {
      const currentId = decodeURIComponent((window.location.hash || "").replace(/^#/, ""));
      if (currentId) {
        setCurrentToc(currentId);
      }
    });
  }
})();
</script>
"""
    .replace("__STORAGE_KEY__", _REPORT_THEME_STORAGE_KEY)
    .replace("__DEFAULT_THEME__", _DEFAULT_REPORT_THEME)
)

_IMAGE_LINE_RE = re.compile(r"^!\[([^\]]*)\]\(([^)]+)\)$")
_THEME_VARIANT_RE = re.compile(r"^(?P<base>.+)\.theme-(?P<theme>[a-z0-9-]+)$")
_INLINE_STATUS_PATTERNS = [
    ("bull", ("看多", "做多", "通过", "较强机会", "强势整理", "持有优于追高", "等右侧确认", "温和复苏", "趋势市")),
    ("bear", ("回避", "偏回避", "偏空", "下行", "空头", "跌破", "未通过", "风险释放前不宜激进")),
    ("warn", ("观察", "无信号", "暂不出手", "待确认", "待复核", "待识别", "有信号但不充分", "降级观察稿")),
]
_NUMERIC_CELL_RE = re.compile(
    r"^[<>~≈±]?\s*[+-]?\d[\d,]*(?:\.\d+)?(?:/\d[\d,]*(?:\.\d+)?)?(?:%|pct|bp|x|倍|亿|万|元|点|天|周|月|年|星|万份|亿份)?$"
)


@lru_cache(maxsize=512)
def _image_data_uri(path_str: str) -> str | None:
    path = Path(path_str)
    if not path.exists() or not path.is_file():
        return None
    mime_type, _ = mimetypes.guess_type(path.name)
    mime_type = mime_type or "application/octet-stream"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _resolve_report_asset_fallback(candidate: Path) -> Path | None:
    parts = list(candidate.parts)
    if "assets" in parts:
        asset_suffix = Path(*parts[parts.index("assets") :])
        fallback = (_PROJECT_ROOT / "reports" / asset_suffix).resolve()
        if fallback.exists() and fallback.is_file():
            return fallback
    if "reports" in parts:
        report_suffix = Path(*parts[parts.index("reports") :])
        fallback = (_PROJECT_ROOT / report_suffix).resolve()
        if fallback.exists() and fallback.is_file():
            return fallback
    return None


def _resolve_local_image_path(src: str, source_dir: Path | None = None) -> Path | None:
    candidate = src.strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme in {"http", "https", "data"}:
        return None
    if parsed.scheme == "file":
        file_path = Path(unquote(parsed.path))
        if file_path.exists() and file_path.is_file():
            return file_path
        return _resolve_report_asset_fallback(file_path)

    raw_path = Path(unquote(candidate))
    search_candidates: List[Path] = []
    if raw_path.is_absolute():
        search_candidates.append(raw_path.resolve())
    else:
        if source_dir is not None:
            search_candidates.append((source_dir / raw_path).resolve())
        search_candidates.append(raw_path.resolve())

    for resolved in search_candidates:
        if resolved.exists() and resolved.is_file():
            return resolved

    for probe in search_candidates + [raw_path]:
        fallback = _resolve_report_asset_fallback(probe)
        if fallback is not None:
            return fallback
    return search_candidates[0] if search_candidates else raw_path


def _embed_image_src(src: str, source_dir: Path | None = None) -> str:
    local_path = _resolve_local_image_path(src, source_dir)
    if local_path is None:
        return src
    return _image_data_uri(str(local_path)) or src


def _theme_variant_paths(src: str, source_dir: Path | None = None) -> Dict[str, Path]:
    local_path = _resolve_local_image_path(src, source_dir)
    if local_path is None:
        return {}
    stem = local_path.stem
    base_stem = stem
    match = _THEME_VARIANT_RE.match(stem)
    if match:
        base_stem = match.group("base")
    base_path = local_path.with_name(f"{base_stem}{local_path.suffix}")
    variants: Dict[str, Path] = {}
    for theme_name in _REPORT_THEME_PRESETS:
        variant = base_path.with_name(f"{base_stem}.theme-{theme_name}{base_path.suffix}")
        if variant.exists() and variant.is_file():
            variants[theme_name] = variant
    return variants


def _themed_image_sources(src: str, source_dir: Path | None = None) -> Dict[str, str]:
    variants = _theme_variant_paths(src, source_dir)
    embedded: Dict[str, str] = {}
    for theme_name, path in variants.items():
        embedded_src = _image_data_uri(str(path))
        if embedded_src:
            embedded[theme_name] = embedded_src
    return embedded


def _preferred_print_image_src(themed_sources: Dict[str, str], fallback_src: str, *, screen_theme_name: str) -> str:
    print_theme_name = _normalize_print_report_theme(screen_theme_name)
    preferred_order: List[str] = []
    if print_theme_name != "light":
        preferred_order.append(print_theme_name)
    for theme_name in (_DEFAULT_PRINT_REPORT_THEME, "erdtree"):
        if theme_name not in preferred_order:
            preferred_order.append(theme_name)
    for theme_name in preferred_order:
        themed_src = themed_sources.get(theme_name)
        if themed_src:
            return themed_src
    return fallback_src


def _build_img_tag(alt: str, src: str, source_dir: Path | None = None) -> str:
    theme_name = _normalize_report_theme()
    local_path = _resolve_local_image_path(src, source_dir)
    themed_sources = _themed_image_sources(src, source_dir)
    embedded_src = themed_sources.get(theme_name) or _embed_image_src(src, source_dir)
    attrs = [
        f'src="{html.escape(embedded_src, quote=True)}"',
        f'alt="{html.escape(alt, quote=True)}"',
        'loading="eager"',
        'decoding="sync"',
    ]
    if themed_sources:
        attrs.append('data-theme-switchable="true"')
        for variant_theme, variant_src in themed_sources.items():
            attrs.append(
                f'data-theme-src-{html.escape(variant_theme, quote=True)}="{html.escape(variant_src, quote=True)}"'
            )
    source_text = str(local_path or src).lower()
    if source_text.endswith(".svg"):
        attrs.append('data-vector-chart="true"')
    img_tag = "<img " + " ".join(attrs) + " />"
    print_src = _preferred_print_image_src(themed_sources, embedded_src, screen_theme_name=theme_name)
    if print_src != embedded_src:
        return (
            '<picture class="report-picture">'
            f'<source media="print" srcset="{html.escape(print_src, quote=True)}" />'
            f"{img_tag}"
            "</picture>"
        )
    return img_tag


def _inline_status_tone(text: str) -> str | None:
    clean = str(text or "").strip()
    if not clean or len(clean) > 18:
        return None
    if re.search(r"[\\/]|python\b|src\.|config\.|--|\.ya?ml|\.py", clean, flags=re.IGNORECASE):
        return None
    for tone, tokens in _INLINE_STATUS_PATTERNS:
        if any(token in clean for token in tokens):
            return tone
    return None


def _render_inline_code_or_pill(content: str) -> str:
    raw = html.unescape(content).strip()
    safe = html.escape(raw, quote=False)
    tone = _inline_status_tone(raw)
    if tone:
        tone_class = "neutral" if tone == "warn" and "中性" in raw else tone
        if tone == "warn" and "观察" not in raw and "无信号" not in raw and "待" not in raw and "降级" not in raw:
            tone_class = "warn"
        elif tone == "warn":
            tone_class = "neutral"
        return f'<span class="report-pill is-{tone_class}">{safe}</span>'
    return f"<code>{safe}</code>"


def _looks_numeric_cell(text: str) -> bool:
    clean = re.sub(r"`([^`]+)`", r"\1", str(text or "")).strip()
    clean = clean.replace("＋", "+").replace("－", "-").replace("，", ",")
    if not clean or len(clean) > 24:
        return False
    return bool(_NUMERIC_CELL_RE.fullmatch(clean))


def _table_column_classes(header: List[str], body: List[List[str]]) -> List[str]:
    classes: List[str] = []
    for col_idx in range(len(header)):
        samples = [row[col_idx].strip() for row in body if col_idx < len(row) and row[col_idx].strip()]
        numeric_hits = sum(1 for sample in samples if _looks_numeric_cell(sample))
        classes.append("cell-num" if samples and numeric_hits >= max(1, len(samples) // 2 + len(samples) % 2) else "cell-text")
    return classes


def _looks_summary_table(header: List[str], body: List[List[str]]) -> bool:
    if len(header) != 2:
        return False
    normalized = [str(cell).strip() for cell in header]
    if normalized != ["项目", "建议"]:
        return False
    if not body or len(body) > 12:
        return False
    summary_keys = {"当前建议", "当前动作", "交付等级", "置信度", "适用周期", "观察优先", "补充观察", "适用时段", "主要利好", "主要利空"}
    first_col = {str(row[0]).strip() for row in body if row}
    return bool(first_col & summary_keys)


def _render_summary_table(body: List[List[str]], source_dir: Path | None = None) -> str:
    cards: List[str] = []
    for row in body:
        if len(row) < 2:
            continue
        key = _format_inline(row[0], source_dir)
        value = _format_inline(row[1], source_dir)
        cards.append(
            '<article class="report-summary-card">'
            f'<div class="report-summary-key">{key}</div>'
            f'<div class="report-summary-value">{value}</div>'
            "</article>"
        )
    if not cards:
        return ""
    return '<section class="report-summary-grid">' + "".join(cards) + "</section>"


def _format_inline(text: str, source_dir: Path | None = None) -> str:
    escaped = html.escape(text, quote=False)
    code_spans: List[str] = []
    escaped = re.sub(
        r"!\[([^\]]*)\]\(([^)]+)\)",
        lambda match: _build_img_tag(
            html.unescape(match.group(1)),
            html.unescape(match.group(2)),
            source_dir,
        ),
        escaped,
    )
    escaped = re.sub(
        r"`([^`]+)`",
        lambda match: (
            code_spans.append(_render_inline_code_or_pill(match.group(1))) or f"@@CODESPAN{len(code_spans) - 1}@@"
        ),
        escaped,
    )
    escaped = re.sub(r"==([^=\n]+)==", r"<mark>\1</mark>", escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<em>\1</em>", escaped)
    escaped = re.sub(r"(?<!_)_([^_\n]+)_(?!_)", r"<em>\1</em>", escaped)
    escaped = re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        lambda match: (
            f'<a href="{html.escape(html.unescape(match.group(2)), quote=True)}">'
            f"{match.group(1)}</a>"
        ),
        escaped,
    )
    for index, rendered_code in enumerate(code_spans):
        escaped = escaped.replace(f"@@CODESPAN{index}@@", rendered_code)
    return escaped


def _render_image_block(line: str, source_dir: Path | None = None) -> str | None:
    match = _IMAGE_LINE_RE.match(line.strip())
    if not match:
        return None
    alt, src = match.groups()
    caption = f"<figcaption>{html.escape(alt, quote=False)}</figcaption>" if alt else ""
    return (
        '<figure class="report-figure" data-autofit="true">'
        f"{_build_img_tag(alt, src, source_dir)}"
        f"{caption}"
        "</figure>"
    )


def _next_nonempty_line(lines: List[str], start: int) -> tuple[int, str] | None:
    index = start
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped:
            return index, stripped
        index += 1
    return None


def _render_table(table_lines: Iterable[str], source_dir: Path | None = None) -> str:
    rows = [line.strip() for line in table_lines if line.strip()]
    if len(rows) < 2:
        return ""
    parsed = [[cell.strip() for cell in _split_markdown_table_row(row)] for row in rows]
    header = parsed[0]
    body = parsed[2:]
    if _looks_summary_table(header, body):
        return _render_summary_table(body, source_dir)
    column_classes = _table_column_classes(header, body)
    head_html = "".join(
        f'<th class="{column_classes[index]}">{_format_inline(cell, source_dir)}</th>'
        for index, cell in enumerate(header)
    )
    body_html = []
    for row in body:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        elif len(row) > len(header):
            row = row[: len(header) - 1] + [" | ".join(row[len(header) - 1 :])]
        body_html.append(
            "<tr>"
            + "".join(
                f'<td class="{column_classes[index]}">{_format_inline(cell, source_dir)}</td>'
                for index, cell in enumerate(row)
            )
            + "</tr>"
        )
    return "<table><thead><tr>" + head_html + "</tr></thead><tbody>" + "".join(body_html) + "</tbody></table>"


def _split_markdown_table_row(row: str) -> List[str]:
    body = row.strip().strip("|")
    cells: List[str] = []
    current: List[str] = []
    escaped = False
    for char in body:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == "|":
            cells.append("".join(current))
            current = []
            continue
        current.append(char)
    if escaped:
        current.append("\\")
    cells.append("".join(current))
    return cells


def _render_theme_switcher(active_theme: str) -> str:
    buttons = []
    for theme_name, (label, _) in _REPORT_THEME_PRESETS.items():
        active = theme_name == active_theme
        state_class = " is-active" if active else ""
        buttons.append(
            '<button type="button" '
            f'class="report-theme-button{state_class}" '
            f'data-report-theme="{html.escape(theme_name, quote=True)}" '
            f'aria-pressed="{"true" if active else "false"}">'
            f"{html.escape(label, quote=False)}"
            "</button>"
        )
    return (
        '<div class="report-toolbar">'
        '<div class="report-theme-switcher" role="group" aria-label="报告主题切换">'
        + "".join(buttons)
        + "</div></div>"
    )


def _render_report_outline(headings: List[dict[str, str]]) -> str:
    toc_items = []
    for heading in headings:
        level = int(heading.get("level", "2") or 2)
        if level > 3:
            continue
        anchor_id = str(heading.get("id", "")).strip()
        label = str(heading.get("label", "")).strip()
        if not anchor_id or not label:
            continue
        toc_items.append(
            f'<a class="report-toc-link level-{level}" href="#{html.escape(anchor_id, quote=True)}">'
            f"{html.escape(label, quote=False)}</a>"
        )
    if not toc_items:
        return ""
    return (
        '<aside class="report-sidebar report-sidebar-left">'
        '<div class="report-outline">'
        '<div class="report-outline-title">目录</div>'
        '<nav class="report-outline-list" aria-label="报告目录">'
        + "".join(toc_items)
        + "</nav></div></aside>"
    )


def _wrap_report_sections(parts: List[str]) -> str:
    if not parts:
        return ""

    hero_parts: List[str] = []
    sections: List[str] = []
    current_section: List[str] = []

    def _flush_section() -> None:
        if current_section:
            sections.append('<section class="report-section">' + "\n".join(current_section) + "</section>")
            current_section.clear()

    for part in parts:
        if re.match(r"<h2(?:\s+[^>]*)?>", part):
            _flush_section()
            current_section.append(part)
            continue
        if current_section:
            current_section.append(part)
            continue
        hero_parts.append(part)

    _flush_section()

    wrapped: List[str] = []
    if hero_parts:
        wrapped.append('<section class="report-hero">' + "\n".join(hero_parts) + "</section>")
    wrapped.extend(sections)
    return "\n".join(wrapped)


def markdown_to_html(markdown_text: str, title: str, *, source_dir: Path | None = None) -> str:
    theme_name = _normalize_report_theme()
    lines = markdown_text.splitlines()
    parts: List[str] = []
    headings: List[dict[str, str]] = []
    heading_index = 0

    def _next_heading_id(label: str, level: int) -> str:
        nonlocal heading_index
        heading_index += 1
        anchor_id = f"report-section-{heading_index}"
        headings.append({"id": anchor_id, "label": label.strip(), "level": str(level)})
        return anchor_id

    index = 0
    while index < len(lines):
        line = lines[index].rstrip()
        stripped = line.strip()
        if not stripped:
            index += 1
            continue

        if stripped == "---":
            parts.append("<hr />")
            index += 1
            continue

        if stripped == "<details>":
            parts.append('<details class="report-details">')
            index += 1
            continue

        if stripped == "</details>":
            parts.append("</details>")
            index += 1
            continue

        if stripped.startswith("<summary>") and stripped.endswith("</summary>"):
            summary_text = stripped[len("<summary>") : -len("</summary>")].strip()
            parts.append(f"<summary>{_format_inline(summary_text, source_dir)}</summary>")
            index += 1
            continue

        image_html = _render_image_block(stripped, source_dir)
        if image_html:
            parts.append(image_html)
            index += 1
            continue

        if stripped.startswith("|") and index + 1 < len(lines) and lines[index + 1].strip().startswith("|"):
            table_lines = []
            while index < len(lines) and lines[index].strip().startswith("|"):
                table_lines.append(lines[index])
                index += 1
            parts.append(_render_table(table_lines, source_dir))
            continue

        if stripped.startswith("#### "):
            heading_text = stripped[5:]
            heading_id = _next_heading_id(heading_text, 4)
            next_line = _next_nonempty_line(lines, index + 1)
            if next_line:
                next_index, next_stripped = next_line
                image_html = _render_image_block(next_stripped, source_dir)
                if image_html:
                    parts.append(
                        '<section class="report-figure-block">'
                        f'<h4 id="{html.escape(heading_id, quote=True)}">{_format_inline(heading_text, source_dir)}</h4>'
                        f"{image_html}"
                        "</section>"
                    )
                    index = next_index + 1
                    continue
            parts.append(f'<h4 id="{html.escape(heading_id, quote=True)}">{_format_inline(heading_text, source_dir)}</h4>')
            index += 1
            continue
        if stripped.startswith("### "):
            heading_text = stripped[4:]
            heading_id = _next_heading_id(heading_text, 3)
            next_line = _next_nonempty_line(lines, index + 1)
            if next_line:
                next_index, next_stripped = next_line
                image_html = _render_image_block(next_stripped, source_dir)
                if image_html:
                    parts.append(
                        '<section class="report-figure-block">'
                        f'<h3 id="{html.escape(heading_id, quote=True)}">{_format_inline(heading_text, source_dir)}</h3>'
                        f"{image_html}"
                        "</section>"
                    )
                    index = next_index + 1
                    continue
            parts.append(f'<h3 id="{html.escape(heading_id, quote=True)}">{_format_inline(heading_text, source_dir)}</h3>')
            index += 1
            continue
        if stripped.startswith("## "):
            heading_text = stripped[3:]
            heading_id = _next_heading_id(heading_text, 2)
            parts.append(f'<h2 id="{html.escape(heading_id, quote=True)}">{_format_inline(heading_text, source_dir)}</h2>')
            index += 1
            continue
        if stripped.startswith("# "):
            heading_text = stripped[2:]
            heading_id = _next_heading_id(heading_text, 1)
            parts.append(f'<h1 id="{html.escape(heading_id, quote=True)}">{_format_inline(heading_text, source_dir)}</h1>')
            index += 1
            continue

        if stripped.startswith("- "):
            items = []
            while index < len(lines) and lines[index].strip().startswith("- "):
                items.append(lines[index].strip()[2:])
                index += 1
            parts.append("<ul>" + "".join(f"<li>{_format_inline(item, source_dir)}</li>" for item in items) + "</ul>")
            continue

        if re.match(r"\d+\.\s+", stripped):
            items = []
            while index < len(lines) and re.match(r"\d+\.\s+", lines[index].strip()):
                items.append(re.sub(r"^\d+\.\s+", "", lines[index].strip()))
                index += 1
            parts.append("<ol>" + "".join(f"<li>{_format_inline(item, source_dir)}</li>" for item in items) + "</ol>")
            continue

        block: List[str] = [stripped]
        index += 1
        while index < len(lines):
            probe = lines[index].strip()
            if (
                not probe
                or probe.startswith("#")
                or probe.startswith("|")
                or probe.startswith("- ")
                or re.match(r"\d+\.\s+", probe)
                or probe == "---"
                or probe == "<details>"
                or probe == "</details>"
                or (probe.startswith("<summary>") and probe.endswith("</summary>"))
                or _IMAGE_LINE_RE.match(probe) is not None
            ):
                break
            block.append(probe)
            index += 1
        parts.append(f"<p>{_format_inline(' '.join(block), source_dir)}</p>")

    body = _wrap_report_sections(parts)
    toolbar = _render_theme_switcher(theme_name)
    outline = _render_report_outline(headings)
    return (
        "<!doctype html>\n"
        '<html lang="zh-CN">\n'
        "<head>\n"
        '<meta charset="utf-8" />\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1" />\n'
        f"<title>{html.escape(title)}</title>\n"
        "<style>\n"
        f"{_render_html_style(theme_name)}\n"
        "</style>\n"
        "</head>\n"
        f'<body class="report-body theme-{html.escape(theme_name, quote=True)}" '
        f'data-default-theme="{html.escape(theme_name, quote=True)}">\n'
        '<div class="report-shell">\n'
        f"{outline}\n"
        '<main class="markdown-body">\n'
        f"{body}\n"
        "</main>\n"
        '<aside class="report-sidebar report-sidebar-right">\n'
        f"{toolbar}\n"
        "</aside>\n"
        "</div>\n"
        f"{_THEME_SWITCHER_SCRIPT}\n"
        "</body>\n"
        "</html>\n"
    )


def _export_pdf(markdown_text: str, html_path: Path, pdf_path: Path) -> None:
    def _cleanup_stale_edge_pdf_exports(target_path: Path) -> None:
        target = f"--print-to-pdf={target_path}"
        try:
            subprocess.run(
                ["/usr/bin/pkill", "-f", target],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass

    def _terminate_edge_process(process: subprocess.Popen, *, force: bool = False) -> None:
        if process.poll() is not None:
            return
        try:
            if hasattr(os, "killpg") and getattr(process, "pid", 0):
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
                return
        except ProcessLookupError:
            return
        except Exception:
            pass
        try:
            if force:
                process.kill()
            else:
                process.terminate()
        except Exception:
            pass

    try:
        pdf_path.unlink()
    except FileNotFoundError:
        pass

    if _EDGE_BINARY.exists():
        _cleanup_stale_edge_pdf_exports(pdf_path)
        process: subprocess.Popen | None = None
        try:
            with tempfile.TemporaryDirectory(prefix="edge-export-", dir="/tmp") as user_data_dir:
                env = dict(os.environ)
                env.setdefault("MPLCONFIGDIR", "/tmp/ai-finance-mpl")
                cmd = [
                    str(_EDGE_BINARY),
                    "--headless=new",
                    "--disable-gpu",
                    "--disable-background-networking",
                    "--disable-component-update",
                    "--disable-domain-reliability",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--no-pdf-header-footer",
                    f"--user-data-dir={user_data_dir}",
                    "--allow-file-access-from-files",
                    "--run-all-compositor-stages-before-draw",
                    "--virtual-time-budget=5000",
                    f"--print-to-pdf={pdf_path}",
                    str(html_path),
                ]
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=env,
                    start_new_session=True,
                )
                deadline = time.monotonic() + 45.0
                last_pdf_size = -1
                stable_pdf_ticks = 0
                while True:
                    return_code = process.poll()
                    if return_code is not None:
                        if return_code != 0:
                            raise subprocess.CalledProcessError(return_code, cmd)
                        break

                    if pdf_path.exists():
                        pdf_size = pdf_path.stat().st_size
                        if pdf_size > 0 and pdf_size == last_pdf_size:
                            stable_pdf_ticks += 1
                        else:
                            last_pdf_size = pdf_size
                            stable_pdf_ticks = 0
                        if pdf_size > 0 and stable_pdf_ticks >= 2:
                            _terminate_edge_process(process)
                            try:
                                process.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                _terminate_edge_process(process, force=True)
                                process.wait(timeout=3)
                            return

                    if time.monotonic() >= deadline:
                        if pdf_path.exists() and pdf_path.stat().st_size > 0:
                            _terminate_edge_process(process)
                            try:
                                process.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                _terminate_edge_process(process, force=True)
                                process.wait(timeout=3)
                            return
                        _terminate_edge_process(process, force=True)
                        process.wait(timeout=3)
                        raise subprocess.TimeoutExpired(cmd=cmd, timeout=45)
                    time.sleep(0.25)
            return
        except Exception:
            if process is not None:
                _terminate_edge_process(process, force=True)
                try:
                    process.wait(timeout=3)
                except Exception:
                    pass
            pass

    try:
        from src.output.briefing_pdf import render_briefing_pdf

        render_briefing_pdf(markdown_text, pdf_path)
        return
    except Exception:
        pass

    raise RuntimeError("PDF 导出失败：既没有可用的 Microsoft Edge，也没有可用的 fpdf。")


def _rewrite_local_report_asset_paths(markdown_text: str, markdown_dir: Path) -> str:
    base = Path(markdown_dir).resolve()

    def _replace(match: re.Match[str]) -> str:
        prefix, raw_path, suffix = match.group(1), match.group(2), match.group(3)
        parsed = urlparse(raw_path)
        if parsed.scheme in {"http", "https", "data"}:
            return match.group(0)
        try:
            resolved = _resolve_local_image_path(raw_path, source_dir=base)
        except OSError:
            return match.group(0)
        if resolved is None or not resolved.exists() or not resolved.is_file():
            candidate = Path(unquote(parsed.path if parsed.scheme == "file" else raw_path)).expanduser()
            if candidate.is_absolute() and "reports" in candidate.parts and "assets" in candidate.parts:
                try:
                    relative = os.path.relpath(candidate, base)
                except ValueError:
                    return match.group(0)
                return f"{prefix}{relative}{suffix}"
            return match.group(0)
        try:
            relative = os.path.relpath(resolved, base)
        except ValueError:
            return match.group(0)
        return f"{prefix}{relative}{suffix}"

    return re.sub(r"(!?\[[^\]]*\]\()([^)\s]+)(\))", _replace, markdown_text)


def _is_report_asset_path(path: Path) -> bool:
    try:
        path.resolve().relative_to((_PROJECT_ROOT / "reports" / "assets").resolve())
        return True
    except ValueError:
        return False


def _extract_local_report_assets(markdown_text: str, source_dir: Path) -> set[Path]:
    assets: set[Path] = set()
    for match in re.finditer(r"(!?\[[^\]]*\]\()([^)\s]+)(\))", markdown_text):
        raw_path = match.group(2)
        parsed = urlparse(raw_path)
        if parsed.scheme in {"http", "https", "data"}:
            continue
        try:
            resolved = _resolve_local_image_path(raw_path, source_dir=source_dir)
        except OSError:
            continue
        if resolved is None or not resolved.exists() or not resolved.is_file() or not _is_report_asset_path(resolved):
            continue
        assets.add(resolved.resolve())
        for variant in _theme_variant_paths(str(resolved), source_dir=source_dir).values():
            if variant.exists() and variant.is_file():
                assets.add(variant.resolve())
    return assets


def _prune_superseded_local_report_assets(previous_markdown: str, current_markdown: str, markdown_dir: Path) -> None:
    previous_assets = _extract_local_report_assets(previous_markdown, markdown_dir)
    current_assets = _extract_local_report_assets(current_markdown, markdown_dir)
    stale_assets = sorted(previous_assets - current_assets)
    for asset in stale_assets:
        try:
            asset.unlink(missing_ok=True)
        except OSError:
            continue


def export_markdown_bundle(markdown_text: str, markdown_path: Path, *, allow_unreviewed_final: bool = False) -> Dict[str, Path]:
    """Persist markdown and export same-style HTML/PDF bundle."""
    if "final" in markdown_path.parts and not allow_unreviewed_final:
        raise RuntimeError("禁止直接写入 final 目录；请先通过外部评审门禁，再使用 report_guard 导出成稿。")
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    previous_markdown = markdown_path.read_text(encoding="utf-8") if markdown_path.exists() else ""
    normalized_markdown = _rewrite_local_report_asset_paths(markdown_text, markdown_path.parent)
    markdown_path.write_text(normalized_markdown, encoding="utf-8")
    if previous_markdown:
        _prune_superseded_local_report_assets(previous_markdown, normalized_markdown, markdown_path.parent)

    html_path = markdown_path.with_suffix(".html")
    html_path.write_text(
        markdown_to_html(normalized_markdown, markdown_path.stem, source_dir=markdown_path.parent),
        encoding="utf-8",
    )

    pdf_path = markdown_path.with_suffix(".pdf")
    _export_pdf(normalized_markdown, html_path, pdf_path)
    return {
        "markdown": markdown_path,
        "html": html_path,
        "pdf": pdf_path,
    }
