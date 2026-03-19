from pathlib import Path

import pytest

from src.output.client_export import markdown_to_html


_PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\xf8\xff\xff?"
    b"\x00\x05\xfe\x02\xfeA\xd9\x8f\xb3\x00\x00\x00\x00IEND\xaeB`\x82"
)


def test_markdown_to_html_renders_h4() -> None:
    markdown = "# 标题\n\n### 三级标题\n\n#### 四级标题\n\n正文"
    rendered = markdown_to_html(markdown, "demo")
    assert "<h3>三级标题</h3>" in rendered
    assert "<h4>四级标题</h4>" in rendered
    assert "<p>正文</p>" in rendered


def test_markdown_to_html_renders_images(tmp_path: Path) -> None:
    image_path = tmp_path / "demo.png"
    image_path.write_bytes(_PNG_BYTES)
    markdown = "# 标题\n\n![图表](demo.png)\n"
    rendered = markdown_to_html(markdown, "demo", source_dir=tmp_path)
    assert '<figure class="report-figure" data-autofit="true">' in rendered
    assert 'src="data:image/png;base64,' in rendered
    assert 'alt="图表"' in rendered
    assert "<figcaption>图表</figcaption>" in rendered


def test_markdown_to_html_uses_consistent_image_style() -> None:
    markdown = "# 标题\n\n![图表](/tmp/demo.png)\n"
    rendered = markdown_to_html(markdown, "demo")
    assert 'figure.report-figure img' in rendered
    assert "width: 100%;" in rendered
    assert "max-height: none;" in rendered
    assert "fitReportFigures" not in rendered


def test_markdown_to_html_preserves_details_blocks() -> None:
    markdown = "<details>\n<summary>分维度详解（点击展开）</summary>\n\n### 技术面\n\n正文\n\n</details>\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<details class="report-details">' in rendered
    assert "<summary>分维度详解（点击展开）</summary>" in rendered
    assert "<h3>技术面</h3>" in rendered
    assert "<p>正文</p>" in rendered
    assert "</details>" in rendered


def test_markdown_to_html_wraps_heading_and_image_block() -> None:
    markdown = "### 总览看板\n\n![图表](/tmp/demo.png)\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<section class="report-figure-block">' in rendered
    assert "<h3>总览看板</h3>" in rendered
    assert '<figure class="report-figure" data-autofit="true">' in rendered


def test_markdown_to_html_keeps_remote_image_urls() -> None:
    markdown = "# 标题\n\n![图表](https://example.com/demo.png)\n"
    rendered = markdown_to_html(markdown, "demo")
    assert 'src="https://example.com/demo.png"' in rendered


def test_markdown_to_html_renders_emphasis_and_highlight() -> None:
    markdown = "普通 **加粗** *斜体* ==高亮== `代码`"
    rendered = markdown_to_html(markdown, "demo")
    assert "<strong>加粗</strong>" in rendered
    assert "<em>斜体</em>" in rendered
    assert "<mark>高亮</mark>" in rendered
    assert "<code>代码</code>" in rendered


def test_markdown_to_html_uses_report_theme_styles() -> None:
    rendered = markdown_to_html("1. **标签**：内容", "demo")
    assert "ol > li" in rendered
    assert "background: linear-gradient(180deg, rgba(251, 246, 239, 0.92)" in rendered
    assert "li strong:first-child" in rendered


def test_briefing_pdf_accepts_h4(tmp_path: Path) -> None:
    pytest.importorskip("fpdf")
    from src.output.briefing_pdf import render_briefing_pdf

    markdown = "# 标题\n\n### 三级标题\n\n#### 四级标题\n\n- 条目一\n- 条目二\n"
    output = tmp_path / "h4.pdf"
    render_briefing_pdf(markdown, output)
    assert output.exists()
    assert output.stat().st_size > 0
