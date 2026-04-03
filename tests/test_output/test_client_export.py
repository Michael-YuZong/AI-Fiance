import os
import subprocess
from pathlib import Path

import pytest

import src.output.client_export as client_export
from src.output.client_export import _export_pdf, export_markdown_bundle, markdown_to_html


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


def test_markdown_to_html_preserves_escaped_pipes_inside_table_cells() -> None:
    markdown = (
        "| 项目 | 说明 |\n"
        "| --- | --- |\n"
        "| 数据覆盖 | 中国宏观 \\| Watchlist 行情 \\| RSS新闻 |\n"
    )
    rendered = markdown_to_html(markdown, "demo")
    assert "<table>" in rendered
    assert "中国宏观 | Watchlist 行情 | RSS新闻" in rendered
    assert rendered.count("<td>") == 2


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
    assert 'class="report-body theme-terminal"' in rendered
    assert 'body.theme-abyss-gold {' in rendered
    assert 'body.theme-institutional {' in rendered
    assert 'data-report-theme="terminal"' in rendered
    assert 'data-report-theme="abyss-gold"' in rendered
    assert 'font-variant-numeric: tabular-nums lining-nums;' in rendered
    assert "ol > li" in rendered
    assert "report-theme-switcher" in rendered
    assert "li strong:first-child" in rendered


def test_markdown_to_html_wraps_top_level_sections_into_cards() -> None:
    markdown = "# 标题\n\n一句摘要\n\n## 宏观面\n\n内容A\n\n## 动作建议\n\n内容B\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<section class="report-hero">' in rendered
    assert rendered.count('<section class="report-section">') == 2
    assert "<h2>宏观面</h2>" in rendered
    assert "<h2>动作建议</h2>" in rendered


def test_markdown_to_html_honors_theme_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "abyss-gold")
    rendered = markdown_to_html("# 标题", "demo")
    assert 'class="report-body theme-abyss-gold"' in rendered
    assert 'data-default-theme="abyss-gold"' in rendered


def test_briefing_pdf_accepts_h4(tmp_path: Path) -> None:
    pytest.importorskip("fpdf")
    from src.output.briefing_pdf import render_briefing_pdf

    markdown = "# 标题\n\n### 三级标题\n\n#### 四级标题\n\n- 条目一\n- 条目二\n"
    output = tmp_path / "h4.pdf"
    render_briefing_pdf(markdown, output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_export_pdf_returns_as_soon_as_pdf_is_stably_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    html_path = tmp_path / "demo.html"
    pdf_path = tmp_path / "demo.pdf"
    edge_path = tmp_path / "Microsoft Edge"
    html_path.write_text("<html><body>demo</body></html>", encoding="utf-8")
    edge_path.write_text("", encoding="utf-8")

    class _FakeProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.terminated = False
            self.killed = False
            self.poll_calls = 0

        def poll(self) -> int | None:
            self.poll_calls += 1
            if self.poll_calls == 2:
                pdf_path.write_bytes(b"%PDF-1.4 demo")
            return self.returncode

        def terminate(self) -> None:
            self.terminated = True
            self.returncode = 0

        def wait(self, timeout: float | None = None) -> int:  # noqa: ARG002
            self.returncode = 0
            return 0

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9

    fake_process = _FakeProcess()
    monkeypatch.setattr(client_export, "_EDGE_BINARY", edge_path)
    monkeypatch.setattr(client_export.subprocess, "Popen", lambda *args, **kwargs: fake_process)
    monkeypatch.setattr(client_export.time, "sleep", lambda _seconds: None)

    _export_pdf("# demo", html_path, pdf_path)

    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 0
    assert fake_process.terminated is True
    assert fake_process.killed is False
    assert fake_process.poll_calls >= 3


def test_export_markdown_bundle_rewrites_local_report_asset_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reports_dir = tmp_path / "reports"
    asset_dir = reports_dir / "assets"
    asset_dir.mkdir(parents=True)
    image_path = asset_dir / "demo.png"
    image_path.write_bytes(_PNG_BYTES)
    target = reports_dir / "etf_picks" / "final" / "demo.md"

    monkeypatch.setattr(client_export, "_export_pdf", lambda markdown_text, html_path, pdf_path: pdf_path.write_bytes(b"%PDF-1.4 demo"))

    bundle = export_markdown_bundle(f"![图表]({image_path})", target, allow_unreviewed_final=True)

    markdown = bundle["markdown"].read_text(encoding="utf-8")
    assert "../../assets/demo.png" in markdown
    assert str(image_path) not in markdown


def test_markdown_to_html_resolves_report_assets_when_preview_dir_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = tmp_path / "project"
    asset_dir = project_root / "reports" / "assets"
    asset_dir.mkdir(parents=True)
    image_path = asset_dir / "demo.png"
    image_path.write_bytes(_PNG_BYTES)

    preview_dir = tmp_path / "preview"
    preview_dir.mkdir()
    monkeypatch.setattr(client_export, "_PROJECT_ROOT", project_root)

    rendered = markdown_to_html("![图表](../../assets/demo.png)", "demo", source_dir=preview_dir)

    assert 'src="data:image/png;base64,' in rendered
    assert "<figcaption>图表</figcaption>" in rendered


def test_export_markdown_bundle_rewrites_relative_report_asset_paths_for_moved_preview(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_root = tmp_path / "project"
    asset_dir = project_root / "reports" / "assets"
    asset_dir.mkdir(parents=True)
    image_path = asset_dir / "demo.png"
    image_path.write_bytes(_PNG_BYTES)
    target = tmp_path / "preview" / "demo.md"

    monkeypatch.setattr(client_export, "_PROJECT_ROOT", project_root)
    monkeypatch.setattr(client_export, "_export_pdf", lambda markdown_text, html_path, pdf_path: pdf_path.write_bytes(b"%PDF-1.4 demo"))

    bundle = export_markdown_bundle("![图表](../../assets/demo.png)", target, allow_unreviewed_final=True)

    markdown = bundle["markdown"].read_text(encoding="utf-8")
    expected = os.path.relpath(image_path, target.parent)
    assert expected in markdown
    assert "../../assets/demo.png" not in markdown


def test_export_pdf_ignores_stale_existing_pdf(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    html_path = tmp_path / "demo.html"
    pdf_path = tmp_path / "demo.pdf"
    edge_path = tmp_path / "Microsoft Edge"
    html_path.write_text("<html><body>demo</body></html>", encoding="utf-8")
    edge_path.write_text("", encoding="utf-8")
    pdf_path.write_bytes(b"%PDF-1.4 stale")

    class _FakeProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.terminated = False
            self.poll_calls = 0

        def poll(self) -> int | None:
            self.poll_calls += 1
            if self.poll_calls == 2:
                pdf_path.write_bytes(b"%PDF-1.4 fresh")
            return self.returncode

        def terminate(self) -> None:
            self.terminated = True
            self.returncode = 0

        def wait(self, timeout: float | None = None) -> int:  # noqa: ARG002
            self.returncode = 0
            return 0

        def kill(self) -> None:
            self.returncode = -9

    fake_process = _FakeProcess()
    monkeypatch.setattr(client_export, "_EDGE_BINARY", edge_path)
    monkeypatch.setattr(client_export.subprocess, "Popen", lambda *args, **kwargs: fake_process)
    monkeypatch.setattr(client_export.time, "sleep", lambda _seconds: None)

    _export_pdf("# demo", html_path, pdf_path)

    assert pdf_path.read_bytes() == b"%PDF-1.4 fresh"
    assert fake_process.terminated is True
    assert fake_process.poll_calls >= 3
