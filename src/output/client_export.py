"""Client-facing markdown export helpers."""

from __future__ import annotations

import base64
import html
import mimetypes
import os
import re
import subprocess
import tempfile
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import unquote, urlparse


_EDGE_BINARY = Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_REPORT_THEME = "terminal"
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
            "--positive": "#4ec9b0",
            "--negative": "#ff8f8f",
            "--warning": "#f4c56a",
            "--panel-radius": "22px",
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
            "--positive": "#5d947c",
            "--negative": "#9b565b",
            "--warning": "#c5a059",
            "--panel-radius": "24px",
        },
    ),
    "institutional": (
        "机构终端",
        {
            "--page-bg": "#000000",
            "--page-bg-2": "#07090c",
            "--panel-bg": "rgba(6, 8, 11, 0.98)",
            "--panel-bg-strong": "rgba(4, 5, 7, 0.99)",
            "--panel-soft": "rgba(11, 15, 20, 0.97)",
            "--heading": "#ffd65a",
            "--text": "#d6dbe2",
            "--text-soft": "#a4adb9",
            "--muted": "#818b97",
            "--accent": "#ffbf00",
            "--accent-strong": "#ffe082",
            "--accent-soft": "rgba(255, 191, 0, 0.14)",
            "--accent-2": "#39ff14",
            "--link": "#8fc5ff",
            "--rule": "rgba(255, 255, 255, 0.07)",
            "--rule-strong": "rgba(255, 191, 0, 0.22)",
            "--row-stripe": "rgba(255, 255, 255, 0.015)",
            "--code-bg": "rgba(255, 191, 0, 0.1)",
            "--code-ink": "#ffe082",
            "--code-border": "rgba(255, 191, 0, 0.2)",
            "--shadow": "rgba(0, 0, 0, 0.58)",
            "--hero-glow": "rgba(255, 191, 0, 0.08)",
            "--positive": "#39ff14",
            "--negative": "#ff6b6b",
            "--warning": "#ffbf00",
            "--panel-radius": "0px",
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


_HTML_STYLE = (
    """
@page {
  size: A4;
  margin: 12mm 10mm 12mm 10mm;
}

:root {
  color-scheme: dark;
}

html, body {
  min-height: 100%;
  font-family: "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
  color: var(--text);
  line-height: 1.68;
  font-size: 13px;
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
  max-width: 1080px;
  margin: 0 auto;
  padding: 14px 18px 28px 18px;
  background:
    radial-gradient(circle at top right, var(--hero-glow), transparent 38%),
    linear-gradient(180deg, var(--page-bg), var(--page-bg-2));
}

main.markdown-body {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 1mm 0 4mm 0;
}

.report-toolbar {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  margin: 0 0 14px 0;
  position: sticky;
  top: 8px;
  z-index: 5;
}

.report-theme-switcher {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 7px;
  background: rgba(8, 10, 14, 0.72);
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.7 + 4px);
  box-shadow: 0 16px 32px -28px var(--shadow);
  backdrop-filter: blur(10px);
}

.report-theme-switcher::before {
  content: "Theme";
  padding: 0 8px 0 4px;
  color: var(--muted);
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-size: 10px;
}

.report-theme-button {
  appearance: none;
  border: 1px solid transparent;
  padding: 7px 11px;
  border-radius: calc(var(--panel-radius) * 0.5 + 4px);
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
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.06), var(--accent-soft));
  border-color: var(--rule-strong);
  color: var(--accent-strong);
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.02);
}

section.report-hero,
section.report-section {
  position: relative;
  overflow: hidden;
  padding: 18px 20px 20px 20px;
  background: linear-gradient(180deg, var(--panel-bg), var(--panel-bg-strong));
  border: 1px solid var(--rule);
  border-radius: var(--panel-radius);
  box-shadow: 0 22px 36px -30px var(--shadow);
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
  padding-top: 22px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.015), transparent 28%),
    linear-gradient(180deg, var(--panel-bg), var(--panel-bg-strong));
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
  font-size: 27px;
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
  font-size: 18px;
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
  font-size: 14px;
  color: var(--accent-strong);
  padding-bottom: 4px;
  border-bottom: 1px dashed var(--rule);
}

h4 {
  font-size: 13px;
  color: var(--text-soft);
  margin-top: 0.95em;
  margin-bottom: 0.35em;
}

p, li, blockquote {
  font-family: "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
}

table, th, td, code, pre {
  font-family: "JetBrains Mono", "SFMono-Regular", "Menlo", "Consolas", "PingFang SC", "Microsoft YaHei", monospace;
}

p {
  margin: 0.5em 0 0.85em;
  color: var(--text);
  text-align: left;
  letter-spacing: 0.01em;
  word-break: break-word;
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

pre {
  background: var(--panel-soft);
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.7 + 2px);
  padding: 10px 12px;
  overflow: hidden;
}

img {
  width: 100%;
  max-width: 100%;
  height: auto;
  display: block;
  margin: 10px auto 16px auto;
}

figure.report-figure {
  margin: 10px auto 16px auto;
  text-align: center;
  width: 100%;
}

figure.report-figure img {
  width: 100%;
  max-width: 100%;
  max-height: none;
  object-fit: contain;
  border-radius: calc(var(--panel-radius) * 0.75);
  border: 1px solid var(--rule);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.02), transparent 30%);
  box-shadow: 0 16px 30px -28px var(--shadow);
}

figure.report-figure figcaption {
  margin-top: 6px;
  font-size: 11px;
  color: var(--muted);
}

h3 + figure.report-figure,
h4 + figure.report-figure {
  margin-top: 8px;
}

section.report-figure-block {
  margin: 0 0 14px 0;
}

section.report-figure-block > h3,
section.report-figure-block > h4 {
  margin-bottom: 0.25em;
}

section.report-figure-block > figure.report-figure {
  margin-top: 6px;
}

table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  margin: 14px 0 18px 0;
  font-size: 12px;
  border: 1px solid var(--rule);
  border-radius: calc(var(--panel-radius) * 0.72 + 2px);
  overflow: hidden;
  background: rgba(255, 255, 255, 0.012);
}

th, td {
  padding: 8px 10px;
  vertical-align: top;
  border: 0;
  border-bottom: 1px solid var(--rule);
}

thead th {
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.06), var(--accent-soft));
  color: var(--heading);
  font-weight: 700;
  letter-spacing: 0.02em;
}

tbody tr:nth-child(even) td {
  background: var(--row-stripe);
}

tbody tr:last-child td {
  border-bottom: 0;
}

blockquote {
  margin: 14px 0;
  padding: 12px 16px;
  color: var(--text-soft);
  background: linear-gradient(180deg, var(--panel-soft), rgba(255, 255, 255, 0.01));
  border-left: 4px solid var(--accent);
  border-radius: calc(var(--panel-radius) * 0.75);
  box-shadow: 0 12px 22px -24px var(--shadow);
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
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.015));
  border: 1px solid var(--rule);
  border-left: 3px solid var(--accent);
  border-radius: calc(var(--panel-radius) * 0.75 + 2px);
  box-shadow: 0 12px 24px -24px var(--shadow);
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
  text-decoration-color: rgba(141, 183, 255, 0.35);
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
  border-radius: calc(var(--panel-radius) * 0.75 + 4px);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.012));
  box-shadow: 0 12px 22px -24px var(--shadow);
  overflow: hidden;
}

details.report-details > summary {
  cursor: pointer;
  list-style: none;
  padding: 10px 14px;
  font-size: 14px;
  font-weight: 600;
  color: var(--accent-strong);
  background: linear-gradient(90deg, var(--accent-soft), rgba(255, 255, 255, 0.01));
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

@media print {
  body {
    padding: 0;
    background: var(--page-bg);
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }

  .report-toolbar {
    display: none;
  }

  section.report-hero,
  section.report-section {
    box-shadow: none;
  }
}
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
})();
</script>
"""
    .replace("__STORAGE_KEY__", _REPORT_THEME_STORAGE_KEY)
    .replace("__DEFAULT_THEME__", _DEFAULT_REPORT_THEME)
)

_IMAGE_LINE_RE = re.compile(r"^!\[([^\]]*)\]\(([^)]+)\)$")


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


def _build_img_tag(alt: str, src: str, source_dir: Path | None = None) -> str:
    embedded_src = _embed_image_src(src, source_dir)
    return (
        f'<img src="{html.escape(embedded_src, quote=True)}" '
        f'alt="{html.escape(alt, quote=True)}" loading="eager" decoding="sync" />'
    )


def _format_inline(text: str, source_dir: Path | None = None) -> str:
    escaped = html.escape(text, quote=False)
    escaped = re.sub(
        r"!\[([^\]]*)\]\(([^)]+)\)",
        lambda match: _build_img_tag(
            html.unescape(match.group(1)),
            html.unescape(match.group(2)),
            source_dir,
        ),
        escaped,
    )
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
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
    head_html = "".join(f"<th>{_format_inline(cell, source_dir)}</th>" for cell in header)
    body_html = []
    for row in body:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        elif len(row) > len(header):
            row = row[: len(header) - 1] + [" | ".join(row[len(header) - 1 :])]
        body_html.append(
            "<tr>" + "".join(f"<td>{_format_inline(cell, source_dir)}</td>" for cell in row) + "</tr>"
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
        if part.startswith("<h2>"):
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
            next_line = _next_nonempty_line(lines, index + 1)
            if next_line:
                next_index, next_stripped = next_line
                image_html = _render_image_block(next_stripped, source_dir)
                if image_html:
                    parts.append(
                        '<section class="report-figure-block">'
                        f"<h4>{_format_inline(stripped[5:], source_dir)}</h4>"
                        f"{image_html}"
                        "</section>"
                    )
                    index = next_index + 1
                    continue
            parts.append(f"<h4>{_format_inline(stripped[5:], source_dir)}</h4>")
            index += 1
            continue
        if stripped.startswith("### "):
            next_line = _next_nonempty_line(lines, index + 1)
            if next_line:
                next_index, next_stripped = next_line
                image_html = _render_image_block(next_stripped, source_dir)
                if image_html:
                    parts.append(
                        '<section class="report-figure-block">'
                        f"<h3>{_format_inline(stripped[4:], source_dir)}</h3>"
                        f"{image_html}"
                        "</section>"
                    )
                    index = next_index + 1
                    continue
            parts.append(f"<h3>{_format_inline(stripped[4:], source_dir)}</h3>")
            index += 1
            continue
        if stripped.startswith("## "):
            parts.append(f"<h2>{_format_inline(stripped[3:], source_dir)}</h2>")
            index += 1
            continue
        if stripped.startswith("# "):
            parts.append(f"<h1>{_format_inline(stripped[2:], source_dir)}</h1>")
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
    return (
        "<!doctype html>\n"
        '<html lang="zh-CN">\n'
        "<head>\n"
        '<meta charset="utf-8" />\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1" />\n'
        f"<title>{html.escape(title)}</title>\n"
        "<style>\n"
        f"{_HTML_STYLE}\n"
        "</style>\n"
        "</head>\n"
        f'<body class="report-body theme-{html.escape(theme_name, quote=True)}" '
        f'data-default-theme="{html.escape(theme_name, quote=True)}">\n'
        f"{toolbar}\n"
        '<main class="markdown-body">\n'
        f"{body}\n"
        "</main>\n"
        f"{_THEME_SWITCHER_SCRIPT}\n"
        "</body>\n"
        "</html>\n"
    )


def _export_pdf(markdown_text: str, html_path: Path, pdf_path: Path) -> None:
    try:
        pdf_path.unlink()
    except FileNotFoundError:
        pass

    if _EDGE_BINARY.exists():
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
                            process.terminate()
                            try:
                                process.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=3)
                            return

                    if time.monotonic() >= deadline:
                        if pdf_path.exists() and pdf_path.stat().st_size > 0:
                            process.terminate()
                            try:
                                process.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=3)
                            return
                        process.kill()
                        process.wait(timeout=3)
                        raise subprocess.TimeoutExpired(cmd=cmd, timeout=45)
                    time.sleep(0.25)
            return
        except Exception:
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
            return match.group(0)
        try:
            relative = os.path.relpath(resolved, base)
        except ValueError:
            return match.group(0)
        return f"{prefix}{relative}{suffix}"

    return re.sub(r"(!?\[[^\]]*\]\()([^)\s]+)(\))", _replace, markdown_text)


def export_markdown_bundle(markdown_text: str, markdown_path: Path, *, allow_unreviewed_final: bool = False) -> Dict[str, Path]:
    """Persist markdown and export same-style HTML/PDF bundle."""
    if "final" in markdown_path.parts and not allow_unreviewed_final:
        raise RuntimeError("禁止直接写入 final 目录；请先通过外部评审门禁，再使用 report_guard 导出成稿。")
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_markdown = _rewrite_local_report_asset_paths(markdown_text, markdown_path.parent)
    markdown_path.write_text(normalized_markdown, encoding="utf-8")

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
