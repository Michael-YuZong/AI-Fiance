"""PDF renderer for daily/weekly briefing reports.

Uses fpdf2 with Songti SC (Chinese) + Georgia (English serif) fonts.
Parses the briefing markdown and renders tables, images, and styled text
into a print-ready A4 PDF.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple

from fpdf import FPDF


_SONGTI_PATH = "/System/Library/Fonts/Supplemental/Songti.ttc"
_GEORGIA_PATH = "/System/Library/Fonts/Supplemental/Georgia.ttf"
_GEORGIA_BOLD_PATH = "/System/Library/Fonts/Supplemental/Georgia Bold.ttf"

# Colours (warm parchment theme matching pdf-export.css)
_BG = (245, 240, 232)
_TEXT = (31, 41, 55)
_H1_BORDER = (214, 201, 184)
_H2_ACCENT = (192, 132, 87)
_TH_BG = (246, 241, 232)
_TD_BORDER = (215, 210, 201)
_QUOTE_BG = (249, 246, 240)
_QUOTE_BORDER = (214, 201, 184)
_MUTED = (107, 114, 128)


class BriefingPDF(FPDF):
    """Custom FPDF subclass with briefing-specific helpers."""

    def __init__(self) -> None:
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=14)
        self._register_fonts()
        self.set_margins(12, 12, 12)

    def _register_fonts(self) -> None:
        if Path(_SONGTI_PATH).exists():
            self.add_font("Songti", "", _SONGTI_PATH)
        if Path(_GEORGIA_PATH).exists():
            self.add_font("Georgia", "", _GEORGIA_PATH)
        if Path(_GEORGIA_BOLD_PATH).exists():
            self.add_font("Georgia", "B", _GEORGIA_BOLD_PATH)

    def header(self) -> None:
        pass

    def footer(self) -> None:
        self.set_y(-10)
        self.set_font("Georgia", "", 8)
        self.set_text_color(*_MUTED)
        self.cell(0, 5, f"Page {self.page_no()}/{{nb}}", align="C")


def render_briefing_pdf(markdown: str, output_path: Path) -> None:
    """Parse briefing markdown and render to PDF."""
    pdf = BriefingPDF()
    pdf.alias_nb_pages()
    pdf.add_page()

    lines = markdown.split("\n")
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        stripped = line.strip()

        # Skip <details>/<summary> HTML tags (expand everything for PDF)
        if stripped in ("<details>", "</details>"):
            idx += 1
            continue
        if stripped.startswith("<summary>") and stripped.endswith("</summary>"):
            title = stripped.replace("<summary>", "").replace("</summary>", "")
            _render_h3(pdf, title)
            idx += 1
            continue

        # H1
        if stripped.startswith("# ") and not stripped.startswith("## "):
            _render_h1(pdf, stripped[2:])
            idx += 1
            continue

        # H2
        if stripped.startswith("## "):
            _render_h2(pdf, stripped[3:])
            idx += 1
            continue

        # H4
        if stripped.startswith("#### "):
            _render_h4(pdf, stripped[5:])
            idx += 1
            continue

        # H3
        if stripped.startswith("### "):
            _render_h3(pdf, stripped[4:])
            idx += 1
            continue

        # Blockquote
        if stripped.startswith("> "):
            block_lines: List[str] = []
            while idx < len(lines) and lines[idx].strip().startswith("> "):
                block_lines.append(lines[idx].strip()[2:])
                idx += 1
            _render_blockquote(pdf, "\n".join(block_lines))
            continue

        # Table
        if stripped.startswith("|") and idx + 1 < len(lines) and _is_separator(lines[idx + 1].strip()):
            table_lines: List[str] = []
            while idx < len(lines) and lines[idx].strip().startswith("|"):
                table_lines.append(lines[idx].strip())
                idx += 1
            _render_table(pdf, table_lines)
            continue

        # Image
        img_match = re.match(r"!\[([^\]]*)\]\(([^)]+)\)", stripped)
        if img_match:
            _render_image(pdf, img_match.group(2), img_match.group(1))
            idx += 1
            continue

        # Bold paragraph (like **行业涨跌幅 TOP/BOTTOM 5**)
        if stripped.startswith("**") and stripped.endswith("**"):
            _render_bold_line(pdf, stripped[2:-2])
            idx += 1
            continue

        # Bullet list item
        if stripped.startswith("- "):
            _render_bullet(pdf, stripped[2:])
            idx += 1
            continue

        # Regular paragraph
        if stripped:
            _render_paragraph(pdf, stripped)
            idx += 1
            continue

        # Empty line
        idx += 1

    pdf.output(str(output_path))


def _is_separator(line: str) -> bool:
    return bool(re.match(r"^\|[\s\-:|]+\|$", line))


def _set_body_font(pdf: BriefingPDF, size: float = 10, bold: bool = False) -> None:
    font = "Songti" if _has_cjk_likelihood() else "Georgia"
    if font == "Songti":
        pdf.set_font("Songti", "", size)
    else:
        pdf.set_font("Georgia", "B" if bold else "", size)
    pdf.set_text_color(*_TEXT)


def _has_cjk_likelihood() -> bool:
    return Path(_SONGTI_PATH).exists()


def _render_h1(pdf: BriefingPDF, text: str) -> None:
    pdf.set_font("Songti", "", 20)
    pdf.set_text_color(17, 24, 39)
    pdf.ln(2)
    pdf.cell(0, 10, _clean(text), new_x="LMARGIN", new_y="NEXT")
    # Underline
    pdf.set_draw_color(*_H1_BORDER)
    pdf.set_line_width(0.6)
    y = pdf.get_y()
    pdf.line(pdf.l_margin, y, pdf.w - pdf.r_margin, y)
    pdf.ln(4)


def _render_h2(pdf: BriefingPDF, text: str) -> None:
    _ensure_space(pdf, 14)
    pdf.set_font("Songti", "", 15)
    pdf.set_text_color(17, 24, 39)
    pdf.ln(5)
    x = pdf.get_x()
    y = pdf.get_y()
    pdf.set_fill_color(*_H2_ACCENT)
    pdf.rect(x, y, 1.2, 7, "F")
    pdf.set_x(x + 4)
    pdf.cell(0, 7, _clean(text), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)


def _render_h3(pdf: BriefingPDF, text: str) -> None:
    _ensure_space(pdf, 12)
    pdf.set_font("Songti", "", 12)
    pdf.set_text_color(17, 24, 39)
    pdf.ln(3)
    pdf.cell(0, 6, _clean(text), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)


def _render_h4(pdf: BriefingPDF, text: str) -> None:
    _ensure_space(pdf, 10)
    pdf.set_font("Songti", "", 10.5)
    pdf.set_text_color(55, 65, 81)
    pdf.ln(2)
    pdf.cell(0, 5, _clean(text), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)


def _render_blockquote(pdf: BriefingPDF, text: str) -> None:
    pdf.ln(2)
    x = pdf.l_margin
    y = pdf.get_y()
    w = pdf.w - pdf.l_margin - pdf.r_margin

    # Render text to measure height
    pdf.set_font("Songti", "", 9)
    pdf.set_text_color(*_MUTED)
    text_cleaned = _clean(text)
    line_h = 4.5
    # Estimate height
    lines_needed = pdf.multi_cell(w - 10, line_h, text_cleaned, dry_run=True, output="LINES")
    block_h = len(lines_needed) * line_h + 6

    _ensure_space(pdf, block_h)
    y = pdf.get_y()

    # Background
    pdf.set_fill_color(*_QUOTE_BG)
    pdf.rect(x, y, w, block_h, "F")
    # Left border
    pdf.set_fill_color(*_QUOTE_BORDER)
    pdf.rect(x, y, 1.2, block_h, "F")

    pdf.set_xy(x + 5, y + 3)
    pdf.multi_cell(w - 10, line_h, text_cleaned)
    pdf.set_y(y + block_h + 2)


def _render_table(pdf: BriefingPDF, table_lines: List[str]) -> None:
    if len(table_lines) < 3:
        return

    header_cells = _parse_row(table_lines[0])
    data_rows = [_parse_row(line) for line in table_lines[2:] if not _is_separator(line)]
    n_cols = len(header_cells)
    if n_cols == 0:
        return

    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
    col_widths = _compute_col_widths(header_cells, data_rows, usable_w, pdf)
    row_h = 6

    def _draw_header() -> None:
        pdf.set_font("Songti", "", 8.5)
        pdf.set_fill_color(*_TH_BG)
        pdf.set_draw_color(*_TD_BORDER)
        pdf.set_text_color(17, 24, 39)
        pdf.set_x(pdf.l_margin)
        for i, cell in enumerate(header_cells):
            w = col_widths[i] if i < len(col_widths) else col_widths[-1]
            pdf.cell(w, row_h, _clean(cell)[:40], border=1, fill=True)
        pdf.ln()

    _ensure_space(pdf, row_h * 2)
    _draw_header()

    # Data rows — use simple single-line cell approach with truncation
    pdf.set_font("Songti", "", 8)
    pdf.set_text_color(*_TEXT)
    for row in data_rows:
        # Precompute row height
        cell_texts = []
        max_lines = 1
        for i, cell in enumerate(row):
            w = col_widths[i] if i < len(col_widths) else col_widths[-1]
            text = _clean(cell)
            cell_texts.append(text)
            lines_needed = pdf.multi_cell(w - 2, row_h, text, dry_run=True, output="LINES")
            max_lines = max(max_lines, len(lines_needed) if lines_needed else 1)

        actual_h = row_h * max_lines

        # Check if we need a new page; if so, re-draw header
        if pdf.get_y() + actual_h > pdf.h - pdf.b_margin:
            pdf.add_page()
            _draw_header()
            pdf.set_font("Songti", "", 8)
            pdf.set_text_color(*_TEXT)

        y_before = pdf.get_y()
        x = pdf.l_margin
        for i, text in enumerate(cell_texts):
            w = col_widths[i] if i < len(col_widths) else col_widths[-1]
            # Draw cell border rect
            pdf.set_draw_color(*_TD_BORDER)
            pdf.rect(x, y_before, w, actual_h)
            # Render text clipped inside the cell
            pdf.set_xy(x + 1, y_before + 0.5)
            with pdf.local_context():
                pdf.set_auto_page_break(auto=False)
                pdf.multi_cell(w - 2, row_h, text)
            x += w
        pdf.set_xy(pdf.l_margin, y_before + actual_h)

    pdf.set_x(pdf.l_margin)
    pdf.ln(3)


def _render_image(pdf: BriefingPDF, path: str, alt: str) -> None:
    img_path = Path(path)
    if not img_path.exists():
        return
    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
    max_h = pdf.h - pdf.t_margin - pdf.b_margin - 3  # max image height on a page

    # Compute actual rendered height from pixel dimensions
    img_h = 60  # fallback
    render_w = usable_w
    render_h: Optional[float] = None
    try:
        from PIL import Image as PILImage
        with PILImage.open(str(img_path)) as im:
            px_w, px_h = im.size
            # Scale to usable width, compute proportional height in mm
            img_h = usable_w * px_h / px_w
            # If taller than a page, constrain by height instead
            if img_h > max_h:
                render_h = max_h
                render_w = max_h * px_w / px_h
                img_h = max_h
    except Exception:
        pass

    _ensure_space(pdf, img_h + 3)
    try:
        if render_h is not None:
            # Center the width-constrained image
            x_offset = pdf.l_margin + (usable_w - render_w) / 2
            pdf.image(str(img_path), x=x_offset, w=render_w, h=render_h)
        else:
            pdf.image(str(img_path), x=pdf.l_margin, w=render_w)
    except Exception:
        pdf.set_font("Songti", "", 9)
        pdf.set_text_color(*_MUTED)
        pdf.cell(0, 5, f"[图片加载失败: {alt}]", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)


def _render_bold_line(pdf: BriefingPDF, text: str) -> None:
    _ensure_space(pdf, 14)
    pdf.ln(2)
    pdf.set_font("Songti", "", 10)
    pdf.set_text_color(*_TEXT)
    pdf.cell(0, 5, _clean(text), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)


def _render_bullet(pdf: BriefingPDF, text: str) -> None:
    pdf.set_font("Songti", "", 9.5)
    pdf.set_text_color(*_TEXT)
    indent = 7
    bullet_x = pdf.l_margin + 3
    text_x = pdf.l_margin + indent
    w = pdf.w - pdf.r_margin - text_x
    if w < 20:
        w = 20
    pdf.set_x(bullet_x)
    pdf.cell(4, 5, "-")
    pdf.set_x(text_x)
    cleaned = _inline_format(_clean(text))
    pdf.multi_cell(w, 5, cleaned)


def _render_paragraph(pdf: BriefingPDF, text: str) -> None:
    pdf.set_font("Songti", "", 10)
    pdf.set_text_color(*_TEXT)
    pdf.set_x(pdf.l_margin)
    cleaned = _inline_format(_clean(text))
    pdf.multi_cell(0, 5, cleaned)
    pdf.ln(1)


def _clean(text: str) -> str:
    """Strip markdown inline formatting for plain-text rendering."""
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = text.replace("\\|", "|")
    # Replace emoji with text equivalents for font compatibility
    text = text.replace("\u2705", "[OK]")   # ✅
    text = text.replace("\u274c", "[X]")    # ❌
    text = text.replace("\u26a0\ufe0f", "[!]")  # ⚠️
    text = text.replace("\u26a0", "[!]")    # ⚠
    text = text.replace("\u2139\ufe0f", "[i]")  # ℹ️
    text = text.replace("\u2139", "[i]")    # ℹ
    return text.strip()


def _inline_format(text: str) -> str:
    """Additional formatting cleanup."""
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    return text


def _parse_row(line: str) -> List[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [p.strip() for p in line.split("|")]


def _compute_col_widths(
    headers: List[str],
    rows: List[List[str]],
    total_w: float,
    pdf: BriefingPDF,
) -> List[float]:
    n = len(headers)
    if n == 0:
        return []

    # Estimate widths based on content length
    max_lens = [len(_clean(h)) for h in headers]
    for row in rows[:10]:
        for i, cell in enumerate(row):
            if i < n:
                max_lens[i] = max(max_lens[i], min(len(_clean(cell)), 50))

    total_chars = sum(max_lens) or 1
    widths = [max(total_w * (l / total_chars), 12) for l in max_lens]

    # Normalize to fit exactly
    scale = total_w / sum(widths)
    return [w * scale for w in widths]


def _ensure_space(pdf: BriefingPDF, needed_mm: float) -> None:
    if pdf.get_y() + needed_mm > pdf.h - pdf.b_margin:
        pdf.add_page()
