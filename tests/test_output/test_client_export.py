from pathlib import Path

import pytest

from src.output.client_export import markdown_to_html


def test_markdown_to_html_renders_h4() -> None:
    markdown = "# 标题\n\n### 三级标题\n\n#### 四级标题\n\n正文"
    rendered = markdown_to_html(markdown, "demo")
    assert "<h3>三级标题</h3>" in rendered
    assert "<h4>四级标题</h4>" in rendered
    assert "<p>正文</p>" in rendered


def test_briefing_pdf_accepts_h4(tmp_path: Path) -> None:
    pytest.importorskip("fpdf")
    from src.output.briefing_pdf import render_briefing_pdf

    markdown = "# 标题\n\n### 三级标题\n\n#### 四级标题\n\n- 条目一\n- 条目二\n"
    output = tmp_path / "h4.pdf"
    render_briefing_pdf(markdown, output)
    assert output.exists()
    assert output.stat().st_size > 0
