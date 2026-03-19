"""Client-facing markdown export helpers."""

from __future__ import annotations

import base64
import html
import mimetypes
import os
import re
import subprocess
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import unquote, urlparse


_EDGE_BINARY = Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge")
_HTML_STYLE = """
@page {
  size: A4;
  margin: 12mm 10mm 12mm 10mm;
}

:root {
  --paper: #fffdf8;
  --paper-soft: #fbf6ef;
  --paper-strong: #f5ede1;
  --ink: #1f2937;
  --ink-soft: #4b5563;
  --muted: #6b7280;
  --accent: #a85d2f;
  --accent-strong: #7c3f1b;
  --accent-soft: #efd7c1;
  --accent-wash: #fff3e7;
  --rule: #dfd2c2;
  --table-stripe: #fdf8f2;
  --shadow: rgba(124, 63, 27, 0.08);
  --highlight: #fff1bf;
}

html, body {
  font-family: "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
  color: var(--ink);
  line-height: 1.62;
  font-size: 13px;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
  font-kerning: normal;
  text-size-adjust: 100%;
  background: var(--paper);
}

body {
  max-width: 1000px;
  margin: 0 auto;
  padding: 0;
}

main.markdown-body {
  padding: 2mm 0 4mm 0;
}

h1, h2, h3, h4 {
  font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Source Han Serif SC", serif;
  color: #172033;
  margin-top: 1.1em;
  margin-bottom: 0.45em;
  page-break-after: avoid;
  letter-spacing: 0.02em;
}

h1 {
  font-size: 25px;
  line-height: 1.28;
  border-bottom: 2px solid var(--accent-soft);
  padding-bottom: 10px;
  margin-top: 0.1em;
  margin-bottom: 0.75em;
}

h2 {
  font-size: 18px;
  display: flex;
  align-items: center;
  gap: 10px;
  border-left: 0;
  padding: 8px 12px;
  border-radius: 10px;
  background: linear-gradient(90deg, rgba(239, 215, 193, 0.42), rgba(239, 215, 193, 0.08));
  box-shadow: inset 0 0 0 1px rgba(223, 210, 194, 0.7);
}

h2::before {
  content: "";
  width: 6px;
  height: 1.8em;
  border-radius: 999px;
  background: linear-gradient(180deg, var(--accent), var(--accent-soft));
  flex: 0 0 auto;
}

h3 {
  font-size: 15px;
  color: var(--accent-strong);
  border-bottom: 1px dashed rgba(168, 93, 47, 0.28);
  padding-bottom: 3px;
}

h4 {
  font-size: 13px;
  color: var(--ink-soft);
  margin-top: 0.9em;
  margin-bottom: 0.3em;
}

p, li, td, th, blockquote {
  font-family: "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
}

code, pre {
  font-family: "SFMono-Regular", "Menlo", "Consolas", monospace;
}

p {
  margin: 0.5em 0 0.8em;
  color: var(--ink);
  text-align: left;
  letter-spacing: 0.01em;
  word-break: break-word;
}

h2 + p,
h2 + blockquote,
h2 + ol,
h2 + ul {
  margin-top: 0.75em;
}

strong {
  font-weight: 700;
  color: var(--accent-strong);
  padding: 0 0.12em;
  background: linear-gradient(180deg, transparent 58%, rgba(239, 215, 193, 0.85) 58%);
}

em {
  font-style: italic;
  color: #765b2a;
}

mark {
  background: linear-gradient(180deg, rgba(255, 241, 191, 0.9), rgba(255, 241, 191, 0.9));
  color: #4c330f;
  padding: 0.02em 0.22em;
  border-radius: 4px;
}

code {
  background: #f7f0e7;
  color: #8a461a;
  padding: 0.12em 0.36em;
  border-radius: 6px;
  border: 1px solid rgba(223, 210, 194, 0.95);
  font-size: 0.94em;
}

pre {
  background: var(--paper-soft);
  border: 1px solid var(--rule);
  border-radius: 10px;
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
  border-radius: 8px;
  box-shadow: 0 10px 24px -24px rgba(31, 41, 55, 0.35);
}

figure.report-figure figcaption {
  margin-top: 4px;
  font-size: 11px;
  color: var(--muted);
}

h3 + figure.report-figure,
h4 + figure.report-figure {
  margin-top: 6px;
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
  border-collapse: collapse;
  margin: 12px 0 16px 0;
  font-size: 12px;
  box-shadow: inset 0 0 0 1px rgba(223, 210, 194, 0.8);
}

th, td {
  border: 1px solid var(--rule);
  padding: 7px 9px;
  vertical-align: top;
}

th {
  background: linear-gradient(180deg, #f7f1e7, #f1e6d9);
  font-weight: 600;
  color: #3f3025;
}

tbody tr:nth-child(even) td {
  background: var(--table-stripe);
}

blockquote {
  margin: 12px 0;
  padding: 10px 14px;
  color: var(--ink-soft);
  background: linear-gradient(180deg, #fff9f2, #faf4ec);
  border-left: 5px solid var(--accent-soft);
  border-radius: 10px;
  box-shadow: 0 8px 18px -18px var(--shadow);
}

ul, ol {
  margin: 0.35em 0 1em;
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
}

ol {
  counter-reset: report-counter;
  list-style: none;
}

ol > li {
  counter-increment: report-counter;
  position: relative;
  margin: 0 0 10px 0;
  padding: 10px 12px 10px 46px;
  background: linear-gradient(180deg, rgba(251, 246, 239, 0.92), rgba(255, 255, 255, 0.92));
  border-left: 3px solid rgba(168, 93, 47, 0.32);
  border-radius: 12px;
  box-shadow: 0 10px 18px -20px var(--shadow);
}

ol > li::before {
  content: counter(report-counter) ".";
  position: absolute;
  left: 14px;
  top: 10px;
  font-family: "Avenir Next", "Helvetica Neue", "PingFang SC", sans-serif;
  font-weight: 700;
  color: var(--accent);
}

li strong:first-child {
  display: inline-block;
  margin-right: 0.18em;
}

a {
  color: #0f4c81;
  text-decoration-color: rgba(15, 76, 129, 0.35);
}

hr {
  border: 0;
  border-top: 1px solid var(--rule);
  margin: 18px 0;
}

details.report-details {
  margin: 12px 0 16px 0;
  padding: 0;
  border: 1px solid rgba(223, 210, 194, 0.95);
  border-radius: 12px;
  background: linear-gradient(180deg, rgba(251, 246, 239, 0.92), rgba(255, 255, 255, 0.96));
  box-shadow: 0 10px 18px -20px var(--shadow);
  overflow: hidden;
}

details.report-details > summary {
  cursor: pointer;
  list-style: none;
  padding: 10px 14px;
  font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Source Han Serif SC", serif;
  font-size: 15px;
  color: var(--accent-strong);
  background: linear-gradient(90deg, rgba(239, 215, 193, 0.38), rgba(239, 215, 193, 0.08));
  border-bottom: 1px solid rgba(223, 210, 194, 0.9);
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
""".strip()

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


def _resolve_local_image_path(src: str, source_dir: Path | None = None) -> Path | None:
    candidate = src.strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme in {"http", "https", "data"}:
        return None
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))

    resolved = Path(unquote(candidate))
    if not resolved.is_absolute() and source_dir is not None:
        resolved = (source_dir / resolved).resolve()
    return resolved


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
    parsed = [[cell.strip() for cell in row.strip("|").split("|")] for row in rows]
    header = parsed[0]
    body = parsed[2:]
    head_html = "".join(f"<th>{_format_inline(cell, source_dir)}</th>" for cell in header)
    body_html = []
    for row in body:
        body_html.append(
            "<tr>" + "".join(f"<td>{_format_inline(cell, source_dir)}</td>" for cell in row) + "</tr>"
        )
    return "<table><thead><tr>" + head_html + "</tr></thead><tbody>" + "".join(body_html) + "</tbody></table>"


def markdown_to_html(markdown_text: str, title: str, *, source_dir: Path | None = None) -> str:
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
            ):
                break
            block.append(probe)
            index += 1
        parts.append(f"<p>{_format_inline(' '.join(block), source_dir)}</p>")

    body = "\n".join(parts)
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
        "<body>\n"
        '<main class="markdown-body">\n'
        f"{body}\n"
        "</main>\n"
        "</body>\n"
        "</html>\n"
    )


def _export_pdf(markdown_text: str, html_path: Path, pdf_path: Path) -> None:
    if _EDGE_BINARY.exists():
        try:
            with tempfile.TemporaryDirectory(prefix="edge-export-", dir="/tmp") as user_data_dir:
                env = dict(os.environ)
                env.setdefault("MPLCONFIGDIR", "/tmp/ai-finance-mpl")
                subprocess.run(
                    [
                        str(_EDGE_BINARY),
                        "--headless=new",
                        "--disable-gpu",
                        f"--user-data-dir={user_data_dir}",
                        "--allow-file-access-from-files",
                        "--run-all-compositor-stages-before-draw",
                        "--virtual-time-budget=5000",
                        f"--print-to-pdf={pdf_path}",
                        str(html_path),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=180,
                    env=env,
                )
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


def export_markdown_bundle(markdown_text: str, markdown_path: Path, *, allow_unreviewed_final: bool = False) -> Dict[str, Path]:
    """Persist markdown and export same-style HTML/PDF bundle."""
    if "final" in markdown_path.parts and not allow_unreviewed_final:
        raise RuntimeError("禁止直接写入 final 目录；请先通过外部评审门禁，再使用 report_guard 导出成稿。")
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown_text, encoding="utf-8")

    html_path = markdown_path.with_suffix(".html")
    html_path.write_text(
        markdown_to_html(markdown_text, markdown_path.stem, source_dir=markdown_path.parent),
        encoding="utf-8",
    )

    pdf_path = markdown_path.with_suffix(".pdf")
    _export_pdf(markdown_text, html_path, pdf_path)
    return {
        "markdown": markdown_path,
        "html": html_path,
        "pdf": pdf_path,
    }
