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


def test_report_guard_accepts_stock_analysis_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/stock_analysis/final/stock_analysis_META_2026-03-12_final.md"
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
            "# Meta (META) | 个股详细分析 | 2026-03-12",
            "",
            "## 为什么这么判断",
            "- 理由一",
            "",
            "## 硬检查",
            "| 项目 | 状态 | 说明 |",
            "| --- | --- | --- |",
            "| 流动性 | ✅ | 充足 |",
            "",
            "## 分维度详解",
            "",
            "### 技术面 55/100",
        ]
    )
    bundle = export_reviewed_markdown_bundle(
        report_type="stock_analysis",
        markdown_text=markdown,
        markdown_path=target,
        release_findings=[],
    )
    assert bundle["markdown"] == target


def test_report_guard_accepts_etf_pick_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/etf_picks/final/etf_pick_2026-03-12_final.md"
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
            "# 今日ETF推荐 | 2026-03-12",
            "",
            "## 数据完整度",
            "- 覆盖正常",
            "- 覆盖率的分母是今天进入完整分析的 3 只 ETF。",
            "",
            "## 交付等级",
            "- 当前交付等级：标准推荐稿。",
            "- 这份 ETF 稿件仍按正式推荐框架编排，但执行上仍要遵守仓位和止损。",
            "- 当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 6 只，再对其中 3 只做完整分析。",
            "",
            "## 为什么推荐它",
            "- 理由一",
            "- 理由二",
            "- 理由三",
            "",
            "## 这只ETF为什么是这个分",
            "| 维度 | 分数 | 为什么是这个分 |",
            "| --- | --- | --- |",
            "| 技术面 | 52/100 | 不适合追高 |",
            "",
            "## 标准化分类",
            "| 维度 | 结果 |",
            "| --- | --- |",
            "| 产品形态 | ETF |",
            "| 载体角色 | 场内ETF |",
            "| 管理方式 | 被动跟踪 |",
            "| 暴露类型 | 商品 |",
            "| 主方向 | 能源 |",
            "| 份额类别 | 未分级 |",
            "",
            "## 关键证据",
            "- [证据1](https://example.com)",
            "",
            "## 为什么不是另外几只",
            "### 1. 红利ETF (510880)",
            "- 今天弹性更弱。",
        ]
    )
    bundle = export_reviewed_markdown_bundle(
        report_type="etf_pick",
        markdown_text=markdown,
        markdown_path=target,
        release_findings=[],
    )
    assert bundle["markdown"] == target


def test_report_guard_accepts_observe_etf_pick_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/etf_picks/final/etf_pick_2026-03-13_final.md"
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
            "# 今日ETF观察 | 2026-03-13",
            "",
            "## 数据完整度",
            "- 覆盖存在降级",
            "- 覆盖率的分母是今天进入完整分析的 2 只 ETF。",
            "",
            "## 交付等级",
            "- 当前交付等级：降级观察稿。",
            "- 这是一份 ETF 观察优先稿，不按正式推荐稿理解。",
            "- 当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 4 只，再对其中 2 只做完整分析。",
            "",
            "## 为什么先看它",
            "- 理由一",
            "- 理由二",
            "- 理由三",
            "",
            "## 这只ETF为什么是这个分",
            "| 维度 | 分数 | 为什么是这个分 |",
            "| --- | --- | --- |",
            "| 技术面 | 52/100 | 不适合追高 |",
            "",
            "## 标准化分类",
            "| 维度 | 结果 |",
            "| --- | --- |",
            "| 产品形态 | ETF |",
            "| 载体角色 | 场内ETF |",
            "| 管理方式 | 被动跟踪 |",
            "| 暴露类型 | 医药 |",
            "| 主方向 | 医药 |",
            "| 份额类别 | 未分级 |",
            "",
            "## 关键证据",
            "- [证据1](https://example.com)",
            "",
            "## 为什么不是另外几只",
            "### 1. 红利ETF (510880)",
            "- 今天弹性更弱。",
        ]
    )
    bundle = export_reviewed_markdown_bundle(
        report_type="etf_pick",
        markdown_text=markdown,
        markdown_path=target,
        release_findings=[],
    )
    assert bundle["markdown"] == target
