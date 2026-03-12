from __future__ import annotations

from pathlib import Path

import pytest

from src.commands import report_guard
from src.commands.report_guard import ReportGuardError, export_reviewed_markdown_bundle, review_path_for
from src.output import client_export
from src.output.client_export import export_markdown_bundle


SCAN_DETAIL_MARKDOWN = "\n".join(
    [
        "# demo",
        "",
        "## 为什么这么判断",
        "",
        "- 理由一",
        "",
        "## 硬检查",
        "",
        "| 项目 | 状态 |",
        "| --- | --- |",
        "| 流动性 | ✅ |",
        "",
        "## 分维度详解",
        "",
        "### 技术面",
    ]
)


@pytest.fixture()
def isolated_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    def _resolve(path: str | Path = "") -> Path:
        path_text = str(path)
        if not path_text:
            return tmp_path
        return tmp_path / path_text

    monkeypatch.setattr(report_guard, "resolve_project_path", _resolve)
    monkeypatch.setattr(
        client_export,
        "export_markdown_bundle",
        lambda markdown_text, markdown_path, **kwargs: {
            "markdown": markdown_path,
            "html": markdown_path.with_suffix(".html"),
            "pdf": markdown_path.with_suffix(".pdf"),
        },
    )
    return tmp_path


def test_report_guard_blocks_missing_review_file(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    with pytest.raises(ReportGuardError, match="外部评审未完成"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_blocks_non_pass_review(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        "\n".join(
            [
                "## 一句话总评",
                "还不能发",
                "## 主要问题",
                "- 存在实质问题",
                "## 独立答案",
                "- 保守处理",
                "## 收敛结论",
                "- 状态：BLOCKED",
                "- 无新的 P0/P1：否",
                "- 允许作为成稿交付：否",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="外部评审尚未通过"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_allows_passed_review_and_writes_manifest(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        "\n".join(
            [
                "## 一句话总评",
                "已接近可发",
                "## 主要问题",
                "- 无新的实质问题",
                "## 独立答案",
                "- 结论一致",
                "## 收敛结论",
                "- 状态：PASS",
                "- 无新的 P0/P1：是",
                "- 允许作为成稿交付：是",
            ]
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="scan",
        markdown_text=SCAN_DETAIL_MARKDOWN,
        markdown_path=target,
        release_findings=[],
        extra_manifest={"symbol": "159981"},
    )

    assert bundle["review"] == review
    assert bundle["manifest"].exists()
    assert "159981" in bundle["manifest"].read_text(encoding="utf-8")


def test_client_export_blocks_direct_write_to_final(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="禁止直接写入 final 目录"):
        export_markdown_bundle("# test", tmp_path / "reports/scans/etfs/final/demo.md")


def test_report_guard_blocks_summary_like_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        "\n".join(
            [
                "## 一句话总评",
                "可发",
                "## 主要问题",
                "- 无新的实质问题",
                "## 独立答案",
                "- 结论一致",
                "## 收敛结论",
                "- 状态：PASS",
                "- 无新的 P0/P1：是",
                "- 允许作为成稿交付：是",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="成稿必须是详细解释版"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text="# 摘要版\n\n只有一句结论。",
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_accepts_detailed_retrospect_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/retrospects/final/portfolio_review_2026-03_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        "\n".join(
            [
                "## 一句话总评",
                "可发",
                "## 主要问题",
                "- 无新的实质问题",
                "## 独立答案",
                "- 结论一致",
                "## 收敛结论",
                "- 状态：PASS",
                "- 无新的 P0/P1：是",
                "- 允许作为成稿交付：是",
            ]
        ),
        encoding="utf-8",
    )
    markdown = "\n".join(
        [
            "# 决策回溯",
            "",
            "## 原始决策",
            "- 记录了一笔买入。",
            "",
            "## 为什么当时会做这个决定",
            "- 因为当时信号偏 bullish。",
            "",
            "## 后验路径",
            "- 5 日后收益 +6.0%。",
            "",
            "## 复盘结论",
            "- 结果判断：结果偏正。",
        ]
    )
    bundle = export_reviewed_markdown_bundle(
        report_type="retrospect",
        markdown_text=markdown,
        markdown_path=target,
        release_findings=[],
    )
    assert bundle["markdown"] == target
