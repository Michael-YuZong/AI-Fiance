"""Client-facing markdown export helpers."""

from __future__ import annotations

import html
import re
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List


_EDGE_BINARY = Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge")
_HTML_STYLE = """
@page {
  size: A4;
  margin: 12mm 10mm 12mm 10mm;
}

html, body {
  font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "PingFang SC", "Hiragino Sans GB", "Arial Unicode MS", serif;
  color: #1f2937;
  line-height: 1.55;
  font-size: 13px;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}

body {
  max-width: 1000px;
  margin: 0 auto;
  padding: 0;
}

h1, h2, h3, h4 {
  font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "PingFang SC", serif;
  color: #111827;
  margin-top: 1.1em;
  margin-bottom: 0.45em;
  page-break-after: avoid;
}

h1 {
  font-size: 24px;
  border-bottom: 2px solid #d6c9b8;
  padding-bottom: 8px;
}

h2 {
  font-size: 18px;
  border-left: 4px solid #c08457;
  padding-left: 10px;
}

h3 {
  font-size: 15px;
}

h4 {
  font-size: 13px;
  color: #374151;
  margin-top: 0.9em;
  margin-bottom: 0.3em;
}

p, li, td, th, blockquote {
  font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "PingFang SC", serif;
}

code, pre {
  font-family: "SFMono-Regular", "Menlo", "Consolas", monospace;
}

img {
  max-width: 100%;
  height: auto;
  display: block;
  margin: 10px auto 16px auto;
  page-break-inside: avoid;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin: 12px 0 16px 0;
  font-size: 12px;
}

th, td {
  border: 1px solid #d7d2c9;
  padding: 6px 8px;
  vertical-align: top;
}

th {
  background: #f6f1e8;
  font-weight: 600;
}

blockquote {
  margin: 12px 0;
  padding: 8px 12px;
  color: #4b5563;
  background: #f9f6f0;
  border-left: 4px solid #d6c9b8;
}

ul, ol {
  padding-left: 20px;
}
""".strip()


def _format_inline(text: str) -> str:
    escaped = html.escape(text, quote=False)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', escaped)
    return escaped


def _render_table(table_lines: Iterable[str]) -> str:
    rows = [line.strip() for line in table_lines if line.strip()]
    if len(rows) < 2:
        return ""
    parsed = [[cell.strip() for cell in row.strip("|").split("|")] for row in rows]
    header = parsed[0]
    body = parsed[2:]
    head_html = "".join(f"<th>{_format_inline(cell)}</th>" for cell in header)
    body_html = []
    for row in body:
        body_html.append("<tr>" + "".join(f"<td>{_format_inline(cell)}</td>" for cell in row) + "</tr>")
    return "<table><thead><tr>" + head_html + "</tr></thead><tbody>" + "".join(body_html) + "</tbody></table>"


def markdown_to_html(markdown_text: str, title: str) -> str:
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

        if stripped.startswith("|") and index + 1 < len(lines) and lines[index + 1].strip().startswith("|"):
            table_lines = []
            while index < len(lines) and lines[index].strip().startswith("|"):
                table_lines.append(lines[index])
                index += 1
            parts.append(_render_table(table_lines))
            continue

        if stripped.startswith("#### "):
            parts.append(f"<h4>{_format_inline(stripped[5:])}</h4>")
            index += 1
            continue
        if stripped.startswith("### "):
            parts.append(f"<h3>{_format_inline(stripped[4:])}</h3>")
            index += 1
            continue
        if stripped.startswith("## "):
            parts.append(f"<h2>{_format_inline(stripped[3:])}</h2>")
            index += 1
            continue
        if stripped.startswith("# "):
            parts.append(f"<h1>{_format_inline(stripped[2:])}</h1>")
            index += 1
            continue

        if stripped.startswith("- "):
            items = []
            while index < len(lines) and lines[index].strip().startswith("- "):
                items.append(lines[index].strip()[2:])
                index += 1
            parts.append("<ul>" + "".join(f"<li>{_format_inline(item)}</li>" for item in items) + "</ul>")
            continue

        if re.match(r"\d+\.\s+", stripped):
            items = []
            while index < len(lines) and re.match(r"\d+\.\s+", lines[index].strip()):
                items.append(re.sub(r"^\d+\.\s+", "", lines[index].strip()))
                index += 1
            parts.append("<ol>" + "".join(f"<li>{_format_inline(item)}</li>" for item in items) + "</ol>")
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
        parts.append(f"<p>{_format_inline(' '.join(block))}</p>")

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
    try:
        from src.output.briefing_pdf import render_briefing_pdf

        render_briefing_pdf(markdown_text, pdf_path)
        return
    except Exception:
        pass

    if not _EDGE_BINARY.exists():
        raise RuntimeError("PDF 导出失败：既没有可用的 fpdf，也没有可用的 Microsoft Edge。")

    subprocess.run(
        [
            str(_EDGE_BINARY),
            "--headless=new",
            "--disable-gpu",
            "--allow-file-access-from-files",
            f"--print-to-pdf={pdf_path}",
            str(html_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )


def export_markdown_bundle(markdown_text: str, markdown_path: Path, *, allow_unreviewed_final: bool = False) -> Dict[str, Path]:
    """Persist markdown and export same-style HTML/PDF bundle."""
    if "final" in markdown_path.parts and not allow_unreviewed_final:
        raise RuntimeError("禁止直接写入 final 目录；请先通过外部评审门禁，再使用 report_guard 导出成稿。")
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown_text, encoding="utf-8")

    html_path = markdown_path.with_suffix(".html")
    html_path.write_text(markdown_to_html(markdown_text, markdown_path.stem), encoding="utf-8")

    pdf_path = markdown_path.with_suffix(".pdf")
    _export_pdf(markdown_text, html_path, pdf_path)
    return {
        "markdown": markdown_path,
        "html": html_path,
        "pdf": pdf_path,
    }
