import os
import subprocess
import base64
from pathlib import Path

import pytest

import src.output.client_export as client_export
from src.output.client_export import _export_pdf, export_markdown_bundle, markdown_to_html
from src.output.opportunity_report import _visual_lines


_PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\xf8\xff\xff?"
    b"\x00\x05\xfe\x02\xfeA\xd9\x8f\xb3\x00\x00\x00\x00IEND\xaeB`\x82"
)
_SVG_TEXT = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10">'
    '<line x1="1" y1="9" x2="9" y2="1" stroke="#ffffff" />'
    "</svg>"
)


def test_markdown_to_html_renders_h4() -> None:
    markdown = "# 标题\n\n### 三级标题\n\n#### 四级标题\n\n正文"
    rendered = markdown_to_html(markdown, "demo")
    assert '<h3 id="report-section-2">三级标题</h3>' in rendered
    assert '<h4 id="report-section-3">四级标题</h4>' in rendered
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
    assert rendered.count("<td ") == 2


def test_markdown_to_html_renders_images(tmp_path: Path) -> None:
    image_path = tmp_path / "demo.png"
    image_path.write_bytes(_PNG_BYTES)
    markdown = "# 标题\n\n![图表](demo.png)\n"
    rendered = markdown_to_html(markdown, "demo", source_dir=tmp_path)
    assert '<figure class="report-figure" data-autofit="true">' in rendered
    assert 'src="data:image/png;base64,' in rendered
    assert 'alt="图表"' in rendered
    assert "<figcaption>图表</figcaption>" in rendered


def test_markdown_to_html_renders_switchable_vector_chart_variants(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "demo_dashboard.svg"
    base.write_text(_SVG_TEXT, encoding="utf-8")
    for theme_name in ("terminal", "abyss-gold", "institutional", "clinical", "erdtree", "neo-brutal"):
        (tmp_path / f"demo_dashboard.theme-{theme_name}.svg").write_text(_SVG_TEXT, encoding="utf-8")
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "institutional")
    rendered = markdown_to_html("# 标题\n\n![图表](demo_dashboard.svg)\n", "demo", source_dir=tmp_path)
    assert 'data-theme-switchable="true"' in rendered
    assert 'data-theme-src-terminal="data:image/svg+xml;base64,' in rendered
    assert 'data-theme-src-abyss-gold="data:image/svg+xml;base64,' in rendered
    assert 'data-theme-src-institutional="data:image/svg+xml;base64,' in rendered
    assert 'data-theme-src-clinical="data:image/svg+xml;base64,' in rendered
    assert 'data-theme-src-erdtree="data:image/svg+xml;base64,' in rendered
    assert 'data-theme-src-neo-brutal="data:image/svg+xml;base64,' in rendered
    assert 'data-vector-chart="true"' in rendered


def test_markdown_to_html_uses_light_print_chart_variant_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "demo_dashboard.svg"
    base.write_text('<svg xmlns="http://www.w3.org/2000/svg"><text>base</text></svg>', encoding="utf-8")
    (tmp_path / "demo_dashboard.theme-institutional.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg"><text>institutional</text></svg>',
        encoding="utf-8",
    )
    clinical_svg = '<svg xmlns="http://www.w3.org/2000/svg"><text>clinical</text></svg>'
    clinical_path = tmp_path / "demo_dashboard.theme-clinical.svg"
    clinical_path.write_text(clinical_svg, encoding="utf-8")
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "institutional")

    rendered = markdown_to_html("# 标题\n\n![图表](demo_dashboard.svg)\n", "demo", source_dir=tmp_path)
    clinical_uri = "data:image/svg+xml;base64," + base64.b64encode(clinical_svg.encode("utf-8")).decode("ascii")

    assert '<picture class="report-picture">' in rendered
    assert f'<source media="print" srcset="{clinical_uri}" />' in rendered


def test_markdown_to_html_honors_abyss_gold_print_theme_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "demo_dashboard.svg"
    base.write_text('<svg xmlns="http://www.w3.org/2000/svg"><text>base</text></svg>', encoding="utf-8")
    abyss_svg = '<svg xmlns="http://www.w3.org/2000/svg"><text>abyss</text></svg>'
    (tmp_path / "demo_dashboard.theme-abyss-gold.svg").write_text(abyss_svg, encoding="utf-8")
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "institutional")
    monkeypatch.setenv("AI_FINANCE_PRINT_THEME", "abyss-gold")

    rendered = markdown_to_html("# 标题\n\n![图表](demo_dashboard.svg)\n", "demo", source_dir=tmp_path)
    abyss_uri = "data:image/svg+xml;base64," + base64.b64encode(abyss_svg.encode("utf-8")).decode("ascii")

    assert "background:\n      var(--page-pattern, linear-gradient(transparent, transparent)),\n      linear-gradient(180deg, var(--page-bg), var(--page-bg-2)) !important;" in rendered
    assert "--page-bg: #0f1013;" in rendered
    assert "--panel-bg: rgba(24, 22, 20, 0.96);" in rendered
    assert f'<source media="print" srcset="{abyss_uri}" />' in rendered


def test_markdown_to_html_uses_consistent_image_style() -> None:
    markdown = "# 标题\n\n![图表](/tmp/demo.png)\n"
    rendered = markdown_to_html(markdown, "demo")
    assert 'figure.report-figure img' in rendered
    assert "width: 100%;" in rendered
    assert "max-height: none;" in rendered
    assert "box-sizing: border-box;" in rendered
    assert "picture.report-picture" in rendered
    assert "fitReportFigures" not in rendered
    assert 'img[data-vector-chart="true"]' in rendered


def test_markdown_to_html_preserves_details_blocks() -> None:
    markdown = "<details>\n<summary>分维度详解（点击展开）</summary>\n\n### 技术面\n\n正文\n\n</details>\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<details class="report-details">' in rendered
    assert "<summary>分维度详解（点击展开）</summary>" in rendered
    assert '<h3 id="report-section-1">技术面</h3>' in rendered
    assert "<p>正文</p>" in rendered
    assert "</details>" in rendered


def test_markdown_to_html_wraps_heading_and_image_block() -> None:
    markdown = "### 总览看板\n\n![图表](/tmp/demo.png)\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<section class="report-figure-block">' in rendered
    assert '<h3 id="report-section-1">总览看板</h3>' in rendered
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


def test_markdown_to_html_renders_status_pills_for_stateful_inline_tokens() -> None:
    markdown = "当前建议 `观察为主`，不是 `较强机会`，更不是 `回避`。"
    rendered = markdown_to_html(markdown, "demo")
    assert 'class="report-pill is-neutral"' in rendered
    assert 'class="report-pill is-bull"' in rendered
    assert 'class="report-pill is-bear"' in rendered


def test_markdown_to_html_keeps_command_code_as_code() -> None:
    markdown = "命令：`python -m src.commands.scan 513090 --config config/config.etf_pick_fast.yaml`"
    rendered = markdown_to_html(markdown, "demo")
    assert "<code>python -m src.commands.scan 513090 --config config/config.etf_pick_fast.yaml</code>" in rendered


def test_markdown_to_html_right_aligns_numeric_table_columns() -> None:
    markdown = (
        "| 维度 | 得分 | 备注 |\n"
        "| --- | --- | --- |\n"
        "| 技术面 | 28/100 | 偏弱 |\n"
        "| 基本面 | 69/100 | 中性 |\n"
    )
    rendered = markdown_to_html(markdown, "demo")
    assert '<th class="cell-num">得分</th>' in rendered
    assert '<td class="cell-num">28/100</td>' in rendered


def test_markdown_to_html_upgrades_execution_summary_table_to_cards() -> None:
    markdown = (
        "| 项目 | 建议 |\n"
        "| --- | --- |\n"
        "| 当前建议 | 观察为主 |\n"
        "| 交付等级 | 降级观察稿 |\n"
    )
    rendered = markdown_to_html(markdown, "demo")
    assert '<section class="report-summary-grid">' in rendered
    assert '<article class="report-summary-card">' in rendered
    assert "当前建议" in rendered
    assert "观察为主" in rendered
    assert "<table>" not in rendered


def test_markdown_to_html_uses_report_theme_styles() -> None:
    rendered = markdown_to_html("1. **标签**：内容", "demo")
    assert 'class="report-body theme-institutional"' in rendered
    assert 'body.theme-abyss-gold {' in rendered
    assert 'body.theme-clinical {' in rendered
    assert 'body.theme-erdtree {' in rendered
    assert 'body.theme-neo-brutal {' in rendered
    assert 'body.theme-institutional {' in rendered
    assert 'data-report-theme="terminal"' in rendered
    assert 'data-report-theme="abyss-gold"' in rendered
    assert 'data-report-theme="clinical"' in rendered
    assert 'data-report-theme="erdtree"' in rendered
    assert 'data-report-theme="neo-brutal"' in rendered
    assert 'font-variant-numeric: tabular-nums lining-nums;' in rendered
    assert "ol > li" in rendered
    assert "report-theme-switcher" in rendered
    assert "li strong:first-child" in rendered
    assert ".report-pill" in rendered
    assert "tbody tr:hover td" in rendered
    assert ".report-summary-grid" in rendered
    assert ".report-shell" in rendered
    assert ".report-outline" in rendered
    assert ".report-toc-link" in rendered
    assert ".report-toc-link.is-current" in rendered
    assert "IntersectionObserver" in rendered
    assert "--prose-max-width: 760px;" in rendered
    assert "font-size: var(--body-size);" in rendered
    assert "section.report-hero > ul" not in rendered
    assert "section.report-section > ul" not in rendered
    assert "margin-left: 0;" in rendered


def test_markdown_to_html_uses_light_print_styles() -> None:
    rendered = markdown_to_html("# 标题\n\n正文", "demo")
    assert "@media print {" in rendered
    assert "background: #ffffff !important;" in rendered
    assert "color: #111111 !important;" in rendered
    assert "--page-bg: #ffffff;" in rendered
    assert "--panel-bg: #ffffff;" in rendered
    assert "border: 1px solid rgba(17, 17, 17, 0.06);" in rendered
    assert "break-inside: auto !important;" in rendered
    assert "page-break-inside: auto !important;" in rendered


def test_markdown_to_html_wraps_top_level_sections_into_cards() -> None:
    markdown = "# 标题\n\n一句摘要\n\n## 宏观面\n\n内容A\n\n## 动作建议\n\n内容B\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<section class="report-hero">' in rendered
    assert rendered.count('<section class="report-section">') == 2
    assert '<h2 id="report-section-2">宏观面</h2>' in rendered
    assert '<h2 id="report-section-3">动作建议</h2>' in rendered


def test_markdown_to_html_renders_left_outline_and_right_theme_rail() -> None:
    markdown = "# 标题\n\n一句摘要\n\n## 宏观面\n\n内容A\n\n### 关键证据\n\n内容B\n"
    rendered = markdown_to_html(markdown, "demo")
    assert '<div class="report-shell">' in rendered
    assert '<aside class="report-sidebar report-sidebar-left">' in rendered
    assert '<aside class="report-sidebar report-sidebar-right">' in rendered
    assert '<div class="report-outline-title">目录</div>' in rendered
    assert 'href="#report-section-1"' in rendered
    assert 'href="#report-section-2"' in rendered
    assert 'href="#report-section-3"' in rendered
    assert 'class="report-toc-link level-2"' in rendered
    assert 'class="report-toc-link level-3"' in rendered


def test_markdown_to_html_honors_theme_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "abyss-gold")
    rendered = markdown_to_html("# 标题", "demo")
    assert 'class="report-body theme-abyss-gold"' in rendered
    assert 'data-default-theme="abyss-gold"' in rendered


def test_markdown_to_html_honors_light_theme_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_FINANCE_REPORT_THEME", "clinical")
    rendered = markdown_to_html("# 标题", "demo")
    assert 'class="report-body theme-clinical"' in rendered
    assert 'data-default-theme="clinical"' in rendered
    assert "color-scheme: var(--color-scheme, dark);" in rendered


def test_visual_lines_keep_snapshot_fallback_dashboard() -> None:
    lines = _visual_lines(
        {
            "dashboard": "/tmp/demo_dashboard.svg",
            "mode": "snapshot_fallback",
            "note": "完整日线当前不可用，图表已降级为本地快照卡。",
        }
    )
    joined = "\n".join(lines)
    assert "## 图表速览" in joined
    assert "### 降级快照卡" in joined
    assert "![分析看板](/tmp/demo_dashboard.svg)" in joined


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
            self.pid = 43210

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
    monkeypatch.setattr(client_export.subprocess, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(client_export.os, "killpg", lambda pid, sig: fake_process.kill() if sig == client_export.signal.SIGKILL else fake_process.terminate())
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


def test_export_markdown_bundle_prunes_superseded_report_assets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reports_dir = tmp_path / "reports"
    asset_dir = reports_dir / "assets"
    asset_dir.mkdir(parents=True)
    old_asset = asset_dir / "demo_old.svg"
    old_variant = asset_dir / "demo_old.theme-clinical.svg"
    new_asset = asset_dir / "demo_new.svg"
    new_variant = asset_dir / "demo_new.theme-clinical.svg"
    old_asset.write_text(_SVG_TEXT, encoding="utf-8")
    old_variant.write_text(_SVG_TEXT, encoding="utf-8")
    new_asset.write_text(_SVG_TEXT, encoding="utf-8")
    new_variant.write_text(_SVG_TEXT, encoding="utf-8")
    target = reports_dir / "etf_picks" / "final" / "demo.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("![旧图](../../assets/demo_old.svg)", encoding="utf-8")

    monkeypatch.setattr(client_export, "_PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(client_export, "_export_pdf", lambda markdown_text, html_path, pdf_path: pdf_path.write_bytes(b"%PDF-1.4 demo"))

    export_markdown_bundle("![新图](../../assets/demo_new.svg)", target, allow_unreviewed_final=True)

    assert not old_asset.exists()
    assert not old_variant.exists()
    assert new_asset.exists()
    assert new_variant.exists()


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
            self.pid = 43211

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
    monkeypatch.setattr(client_export.subprocess, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(client_export.os, "killpg", lambda pid, sig: fake_process.kill() if sig == client_export.signal.SIGKILL else fake_process.terminate())
    monkeypatch.setattr(client_export.subprocess, "Popen", lambda *args, **kwargs: fake_process)
    monkeypatch.setattr(client_export.time, "sleep", lambda _seconds: None)

    _export_pdf("# demo", html_path, pdf_path)

    assert pdf_path.read_bytes() == b"%PDF-1.4 fresh"
    assert fake_process.terminated is True
    assert fake_process.poll_calls >= 3


def test_export_pdf_disables_browser_header_footer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    html_path = tmp_path / "demo.html"
    pdf_path = tmp_path / "demo.pdf"
    edge_path = tmp_path / "Microsoft Edge"
    html_path.write_text("<html><body>demo</body></html>", encoding="utf-8")
    edge_path.write_text("", encoding="utf-8")

    captured_cmd: list[str] = []

    class _FakeProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.poll_calls = 0
            self.pid = 43212

        def poll(self) -> int | None:
            self.poll_calls += 1
            if self.poll_calls == 2:
                pdf_path.write_bytes(b"%PDF-1.4 demo")
            return self.returncode

        def terminate(self) -> None:
            self.returncode = 0

        def wait(self, timeout: float | None = None) -> int:  # noqa: ARG002
            self.returncode = 0
            return 0

        def kill(self) -> None:
            self.returncode = -9

    def _fake_popen(cmd, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        captured_cmd.extend(cmd)
        return _FakeProcess()

    monkeypatch.setattr(client_export, "_EDGE_BINARY", edge_path)
    monkeypatch.setattr(client_export.subprocess, "run", lambda *args, **kwargs: None)
    monkeypatch.setattr(client_export.os, "killpg", lambda pid, sig: None)
    monkeypatch.setattr(client_export.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(client_export.time, "sleep", lambda _seconds: None)

    _export_pdf("# demo", html_path, pdf_path)

    assert "--no-pdf-header-footer" in captured_cmd
