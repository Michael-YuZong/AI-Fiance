from __future__ import annotations

from pathlib import Path

import pytest

from src.commands import report_guard
from src.commands.report_guard import ReportGuardError, export_reviewed_markdown_bundle, exported_bundle_lines, review_path_for
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

SCAN_THEME_BOUNDARY_MARKDOWN = "\n".join(
    [
        "# demo",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 主题边界 | 当前先按 `信息技术` 行业层理解，`先进封装 / HBM / Chiplet / AI算力` 这几条线还在打架，不适合硬落单一细主题。 |",
        "",
        "## 为什么这么判断",
        "",
        "- 主题边界：当前先按 `信息技术` 行业层理解，不把几条同时活跃的细分线硬写成单一主题。",
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

SCAN_SUBTHEME_BRIDGE_MARKDOWN = "\n".join(
    [
        "# demo",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 细分观察 | 当前仍先按 `信息技术` 行业层理解，但可优先留意 `AI算力`；没有更多确认前，不把它写成确定主线。 |",
        "",
        "## 为什么这么判断",
        "",
        "- 细分观察：当前更偏向 `AI算力`，但还没有足够证据把行业层稿件直接改成已确认细主题。",
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

SCAN_EVENT_DIGEST_MARKDOWN = "\n".join(
    [
        "# demo",
        "",
        "## 为什么这么判断",
        "",
        "- 理由一",
        "",
        "## 事件消化",
        "",
        "- 事件状态：`已消化`。",
        "- 事件分层：`公告`；当前优先级：`高`。",
        "- 事件细分：公告类型：中标/订单",
        "- 影响层与性质：更直接影响 `盈利 / 景气`；当前更像 `thesis变化`。",
        "- 优先级判断：优先前置，因为公司级执行事件已开始改写 `盈利 / 景气`。",
        "- 这件事改变了什么：把研究重点推进到公司/产品级执行层，后面更该看订单、回购、增减持或项目节奏会不会真的兑现。",
        "- 当前前置事件：新易盛公告：800G 光模块新品发布（证券时报 / 2026-03-28）",
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

SCAN_WHAT_CHANGED_MARKDOWN = "\n".join(
    [
        "# demo",
        "",
        "## 为什么这么判断",
        "",
        "- 理由一",
        "",
        "## What Changed",
        "",
        "- 上次怎么看：核心假设是 `800G 光模块放量兑现`；验证指标看 `订单和毛利率同步改善`；预期周期是 `1-3个月`。",
        "- 这次什么变了：事件状态从 `待补充` 升到 `已消化`，前置层从 `行业主题事件` 切到 `公告`。",
        "- 当前事件理解：公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
        "- 结论变化：`升级`；当前更像 `中期逻辑未坏 / 观察为主 / 观察稿`。",
        "",
        "## 当前更合适的动作",
        "",
        "- 先观察。",
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

STRATEGY_VALIDATE_MARKDOWN = "\n".join(
    [
        "# Strategy Validation",
        "",
        "## 动作卡片",
        "",
        "> **继续观察｜暂不可用，不要当成稳定策略**",
        ">",
        "> 当前 batch 仍在观察。",
        ">",
        "> 当前动作：继续扩大样本。",
        "",
        "## 当前结论",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 当前状态 | 继续观察。 |",
        "| 可用性 | 暂不可用，不要当成稳定策略。 |",
        "",
        "## 这套策略是什么",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 是不是具体策略 | 是，但当前是窄版研究策略。 |",
        "| 这份报告在回答什么 | 它在看这套固定打分逻辑历史上是否稳定。 |",
        "",
        "## 这次到底看出来什么",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 一句话结论 | 当前 batch 仍在观察。 |",
        "| 现在能不能用 | 现在不能把它当成稳定可用策略。 |",
        "",
        "## 执行摘要",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 当前判断 | 当前 batch 仍在观察。 |",
        "| 主要问题 | 平均超额收益偏弱。 |",
        "| 下一步 | 继续扩大样本。 |",
        "",
        "## 总体结果",
        "",
        "- hit rate: `50.0%`",
        "- 平均超额收益: `-1.0%`",
        "- 平均成本后方向收益: `-1.5%`",
        "",
        "## Out-Of-Sample Validation",
        "",
        "- 当前 holdout 已开始承压。",
        "",
        "## Rollback Gate",
        "",
        "- 当前 baseline 已进入观察。",
    ]
)

STRATEGY_EXPERIMENT_MARKDOWN = "\n".join(
    [
        "# Strategy Experiment",
        "",
        "## 动作卡片",
        "",
        "> **继续保留 baseline｜暂不切换。**",
        ">",
        "> 当前 baseline 继续保留。",
        ">",
        "> 当前动作：继续扩大样本。",
        "",
        "## 当前结论",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 当前状态 | 继续保留 baseline。 |",
        "| 切换建议 | 暂不切换。 |",
        "",
        "## 这套策略是什么",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 是不是具体策略 | 是，但当前是窄版研究策略。 |",
        "| 这份报告在回答什么 | 它在比较 baseline 和几种预定义权重变体。 |",
        "",
        "## 这次到底看出来什么",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 一句话结论 | 当前 baseline 继续保留。 |",
        "| 现在能不能切换 | 现在不能切换，应继续保留 baseline。 |",
        "",
        "## 执行摘要",
        "",
        "| 项目 | 结论 |",
        "| --- | --- |",
        "| 当前判断 | 当前 baseline 继续保留。 |",
        "| 主要问题 | challenger 还没有稳定跑赢。 |",
        "| 下一步 | 继续扩大样本。 |",
        "",
        "## Promotion Gate",
        "",
        "- 当前 challenger 仍未过 gate。",
        "",
        "## Rollback Gate",
        "",
        "- 当前 baseline 仍可 hold。",
        "",
        "## 变体对比",
        "",
        "| variant | validated | oos | xsec | hit rate | avg excess | avg net directional | avg drawdown | dominant attribution |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        "| baseline | 8 | stable | stable | 50.0% | -1.0% | -1.5% | -5.0% | weight_misallocation |",
    ]
)


def _review_text(
    *,
    status: str,
    no_p1: str,
    allow_delivery: str,
    round_value: int = 1,
    previous_round: int | None = None,
    converged: str = "否",
    continue_next: str = "是",
    summary: str = "可审",
    issues: list[str] | None = None,
    independent: list[str] | None = None,
    zero_prompt: list[str] | None = None,
    convergence_extra: list[str] | None = None,
    structural_reviewer: str = "gpt-5.4 / reviewer_structural",
    divergent_reviewer: str = "gpt-5.4-mini / reviewer_divergent",
) -> str:
    lines = [
        "## 一句话总评",
        summary,
        "## 主要问题",
        *(issues or ["- 无新的实质问题"]),
        "## 独立答案",
        *(independent or ["- 结论一致"]),
        "## 零提示发散审",
        *(zero_prompt or ["- 零提示复核后，没有新的高优先级问题。"]),
        "## 收敛结论",
        f"- round：{round_value}",
    ]
    if previous_round is not None:
        lines.append(f"- previous_round：{previous_round}")
    lines.extend(
        [
            f"- 状态：{status}",
            f"- 无新的 P0/P1：{no_p1}",
            f"- 本轮是否收敛：{converged}",
            f"- 是否建议继续下一轮：{continue_next}",
            f"- 允许作为成稿交付：{allow_delivery}",
            f"- 结构审执行者：{structural_reviewer}",
            f"- 发散审执行者：{divergent_reviewer}",
        ]
    )
    if convergence_extra:
        lines.extend(convergence_extra)
    return "\n".join(lines)


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
        _review_text(
            status="BLOCKED",
            no_p1="否",
            allow_delivery="否",
            round_value=1,
            converged="否",
            continue_next="是",
            summary="还不能发",
            issues=["- 存在实质问题"],
            independent=["- 保守处理"],
            zero_prompt=["- 不看模板时，第一反应仍是风险没有收口。"],
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


def test_report_guard_blocks_review_missing_zero_prompt_divergence_section(isolated_reports: Path) -> None:
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
                "- round：1",
                "- 状态：PASS",
                "- 无新的 P0/P1：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
                "- 允许作为成稿交付：是",
                "- 结构审执行者：gpt-5.4 / reviewer_structural",
                "- 发散审执行者：gpt-5.4-mini / reviewer_divergent",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="零提示发散审"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_blocks_missing_split_review_roles(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
            structural_reviewer="",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="结构审执行者"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_allows_round1_pass_when_sections_only_state_no_new_findings(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/briefings/final/daily_briefing_2026-03-23_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            issues=["- 无新的实质问题。"],
            independent=["- 结论一致。"],
            zero_prompt=["- 零提示二审未发现新的实质性问题。"],
            convergence_extra=["- closed_items：无"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="briefing",
        markdown_text="\n".join(
            [
                "# 今日晨报 | 2026-03-23",
                "",
                "## 为什么今天这么判断",
                "",
                "- 原因一",
                "",
                "## 宏观领先指标",
                "",
                "- 指标一",
                "",
                "## 数据完整度",
                "",
                "- 覆盖完整",
                "",
                "## 今天怎么做",
                "",
                "- 先观察",
                "",
                "## 重点观察",
                "",
                "- 方向一",
                "",
                "## 今日A股观察池",
                "",
                "| 排名 | 标的 |",
                "| --- | --- |",
                "| 1 | 演示标的 |",
            ]
        ),
        markdown_path=target,
        release_findings=[],
        extra_manifest={
            "market_snapshot_contract": {
                "contract_version": "briefing.market_snapshot.v1",
                "stale_detected": False,
                "warning_line": "",
            }
        },
    )

    assert bundle["review"] == review


def test_report_guard_blocks_same_split_review_role(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
            structural_reviewer="gpt-5.4 / reviewer_shared",
            divergent_reviewer="gpt-5.4 / reviewer_shared",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="不能是同一个 reviewer"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_blocks_conflicting_pass_review_text(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="正文仍写着未关闭问题。",
            issues=["- 还有问题", "### P1-1 关闭状态：未关闭"],
            independent=["- 结论还没完全收口"],
            zero_prompt=["- 零提示再看，仍然先担心未关闭的阻塞项。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="正文与收敛结论冲突"):
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
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="已接近可发",
            zero_prompt=["- 只看成稿时，没有再发现新的高优先级问题。"],
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


def test_report_guard_blocks_incomplete_catalyst_web_review_template(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_512480_2026-03-26_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )
    catalyst_review = isolated_reports / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review.md"
    catalyst_review.parent.mkdir(parents=True, exist_ok=True)
    catalyst_review.write_text(
        "\n".join(
            [
                "# Catalyst Web Review | scan | 2026-03-26",
                "",
                "## 1. 半导体ETF (512480)",
                "",
                "### 复核结论",
                "",
                "- 结论：待补",
                "",
                "### 关键证据",
                "",
                "- 待补",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="催化联网复核尚未完成"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
            extra_manifest={"editor_artifacts": {"catalyst_web_review": str(catalyst_review)}},
        )


def test_report_guard_allows_completed_catalyst_web_review(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_512480_2026-03-26_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )
    catalyst_review = isolated_reports / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review.md"
    catalyst_review.parent.mkdir(parents=True, exist_ok=True)
    catalyst_review.write_text(
        "\n".join(
            [
                "# Catalyst Web Review | scan | 2026-03-26",
                "",
                "## 1. 半导体ETF (512480)",
                "",
                "### 复核结论",
                "",
                "- 结论：只有主题级催化",
                "",
                "### 关键证据",
                "",
                "- 证券时报 2026-03-25：半导体设备链继续讨论扩产。",
                "",
                "### 影响判断",
                "",
                "- 不足以把观察稿升级为推荐稿，但不能再写成“零催化”。",
                "",
                "### 边界",
                "",
                "- 主题新闻不等于公司直接催化。",
            ]
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="scan",
        markdown_text=SCAN_DETAIL_MARKDOWN,
        markdown_path=target,
        release_findings=[],
        extra_manifest={"editor_artifacts": {"catalyst_web_review": str(catalyst_review)}},
    )

    assert bundle["review"] == review


def test_report_guard_blocks_missing_event_digest_section(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="事件消化合同错配"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "event_digest_contract": {
                    "status": "已消化",
                    "lead_layer": "公告",
                    "lead_detail": "公告类型：中标/订单",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "thesis变化",
                    "importance_reason": "优先前置，因为公司级执行事件已开始改写 `盈利 / 景气`。",
                    "changed_what": "把研究重点推进到公司级执行层。",
                }
            },
        )


def test_report_guard_allows_event_digest_section_when_contract_present(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="scan",
        markdown_text=SCAN_EVENT_DIGEST_MARKDOWN,
        markdown_path=target,
        release_findings=[],
        extra_manifest={
            "event_digest_contract": {
                "status": "已消化",
                "lead_layer": "公告",
                "lead_detail": "公告类型：中标/订单",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "thesis变化",
                "importance_reason": "优先前置，因为公司级执行事件已开始改写 `盈利 / 景气`。",
                "changed_what": "把研究重点推进到公司级执行层。",
            }
        },
    )

    assert bundle["review"] == review


def test_report_guard_blocks_missing_event_digest_deep_fields(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="影响层"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_EVENT_DIGEST_MARKDOWN.replace(
                "- 影响层与性质：更直接影响 `盈利 / 景气`；当前更像 `thesis变化`。\n",
                "- 影响层与性质：当前更像 `thesis变化`。\n",
            ),
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "event_digest_contract": {
                    "status": "已消化",
                    "lead_layer": "公告",
                    "lead_detail": "公告类型：中标/订单",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "thesis变化",
                    "importance_reason": "优先前置，因为公司级执行事件已开始改写 `盈利 / 景气`。",
                    "changed_what": "把研究重点推进到公司级执行层。",
                }
            },
        )


def test_report_guard_blocks_missing_event_digest_priority_reason(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="优先级判断"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_EVENT_DIGEST_MARKDOWN.replace(
                "- 优先级判断：优先前置，因为公司级执行事件已开始改写 `盈利 / 景气`。\n",
                "",
            ),
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "event_digest_contract": {
                    "status": "已消化",
                    "lead_layer": "公告",
                    "lead_detail": "公告类型：中标/订单",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "thesis变化",
                    "importance_reason": "优先前置，因为公司级执行事件已开始改写 `盈利 / 景气`。",
                    "changed_what": "把研究重点推进到公司级执行层。",
                }
            },
        )


def test_report_guard_blocks_missing_what_changed_current_understanding(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="当前事件理解"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_WHAT_CHANGED_MARKDOWN.replace(
                "- 当前事件理解：公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`\n",
                "",
            ),
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "what_changed_contract": {
                    "previous_view": "核心假设是 `800G 光模块放量兑现`。",
                    "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                    "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
                    "conclusion_label": "升级",
                }
            },
        )


def test_report_guard_allows_what_changed_current_understanding_when_present(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="scan",
        markdown_text=SCAN_WHAT_CHANGED_MARKDOWN,
        markdown_path=target,
        release_findings=[],
        extra_manifest={
            "what_changed_contract": {
                "previous_view": "核心假设是 `800G 光模块放量兑现`。",
                "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
                "conclusion_label": "升级",
            }
        },
    )

    assert bundle["review"] == review


def test_report_guard_blocks_missing_what_changed_state_trigger(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="状态触发"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_WHAT_CHANGED_MARKDOWN,
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "what_changed_contract": {
                    "previous_view": "核心假设是 `800G 光模块放量兑现`。",
                    "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                    "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
                    "conclusion_label": "升级",
                    "state_trigger": "事件完成消化",
                }
            },
        )


def test_report_guard_blocks_missing_what_changed_state_summary(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_300502_2026-03-29_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="状态解释"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_WHAT_CHANGED_MARKDOWN.replace(
                "- 结论变化：`升级`；当前更像 `中期逻辑未坏 / 观察为主 / 观察稿`。\n",
                "- 结论变化：`升级`；当前更像 `中期逻辑未坏 / 观察为主 / 观察稿`；触发：事件完成消化。\n",
            ),
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "what_changed_contract": {
                    "previous_view": "核心假设是 `800G 光模块放量兑现`。",
                    "change_summary": "事件状态从 `待补充` 升到 `已消化`。",
                    "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `thesis变化`",
                    "conclusion_label": "升级",
                    "state_trigger": "事件完成消化",
                    "state_summary": "当前事件已完成消化并更新主导事件，thesis 可以按更高确定性理解。",
                }
            },
        )


def test_report_guard_blocks_theme_conflict_markdown_missing_boundary_hint(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_515000_2026-03-28_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="主题 playbook 合同错配"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "theme_playbook_contract": {
                    "label": "信息技术",
                    "playbook_level": "sector",
                    "theme_match_status": "ambiguous_conflict",
                    "theme_match_candidates": ["先进封装 / HBM / Chiplet", "AI算力"],
                }
            },
        )


def test_report_guard_allows_theme_conflict_markdown_with_boundary_hint(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_515000_2026-03-28_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="scan",
        markdown_text=SCAN_THEME_BOUNDARY_MARKDOWN,
        markdown_path=target,
        release_findings=[],
        extra_manifest={
            "theme_playbook_contract": {
                "label": "信息技术",
                "playbook_level": "sector",
                "theme_match_status": "ambiguous_conflict",
                "theme_match_candidates": ["先进封装 / HBM / Chiplet", "AI算力"],
            }
        },
    )

    assert bundle["review"] == review


def test_report_guard_blocks_bridge_markdown_missing_subtheme_hint(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159852_2026-03-28_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="主题 playbook 合同错配"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "theme_playbook_contract": {
                    "label": "信息技术",
                    "playbook_level": "sector",
                    "subtheme_bridge_confidence": "high",
                    "subtheme_bridge_top_label": "AI算力",
                }
            },
        )


def test_report_guard_allows_bridge_markdown_with_subtheme_hint(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159852_2026-03-28_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示复核后，没有新的高优先级问题。"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="scan",
        markdown_text=SCAN_SUBTHEME_BRIDGE_MARKDOWN,
        markdown_path=target,
        release_findings=[],
        extra_manifest={
            "theme_playbook_contract": {
                "label": "信息技术",
                "playbook_level": "sector",
                "subtheme_bridge_confidence": "high",
                "subtheme_bridge_top_label": "AI算力",
            }
        },
    )

    assert bundle["review"] == review


def test_client_export_blocks_direct_write_to_final(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="禁止直接写入 final 目录"):
        export_markdown_bundle("# test", tmp_path / "reports/scans/etfs/final/demo.md")


def test_report_guard_blocks_summary_like_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/etfs/final/scan_159981_2026-03-11_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示再审，没有新的阻塞项。"],
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
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示再审，没有新的阻塞项。"],
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
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
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


def test_report_guard_accepts_strategy_validation_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/strategy/validate/final/strategy_validate_600519_2026-03-23_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示再审，没有新的阻塞项。"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="strategy",
        markdown_text=STRATEGY_VALIDATE_MARKDOWN,
        markdown_path=target,
        release_findings=[],
    )

    assert bundle["markdown"] == target


def test_report_guard_accepts_strategy_experiment_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/strategy/experiment/final/strategy_experiment_600519_2026-03-23_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            zero_prompt=["- 零提示再审，没有新的阻塞项。"],
        ),
        encoding="utf-8",
    )

    bundle = export_reviewed_markdown_bundle(
        report_type="strategy",
        markdown_text=STRATEGY_EXPERIMENT_MARKDOWN,
        markdown_path=target,
        release_findings=[],
    )

    assert bundle["markdown"] == target


def test_report_guard_accepts_etf_pick_markdown(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/etf_picks/final/etf_pick_2026-03-12_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
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
            "## 基金画像",
            "| 项目 | 内容 |",
            "| --- | --- |",
            "| 基金类型 | 商品型 |",
            "| 基金公司 | 建信基金 |",
            "| 基金经理 | 朱金钰、亢豆 |",
            "| 成立日期 | 2019-12-13 |",
            "| 业绩比较基准 | 易盛郑商所能源化工指数A收益率 |",
            "",
            "## 关键证据",
            "- [证据1](https://example.com)；信号：资金回流确认；强弱：中强；结论：偏利多，说明当前商品主线仍有配置支撑。",
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
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
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
            "## 基金画像",
            "| 项目 | 内容 |",
            "| --- | --- |",
            "| 基金类型 | 股票型 / 被动指数型 |",
            "| 基金公司 | 广发基金 |",
            "| 基金经理 | 刘杰 |",
            "| 成立日期 | 2022-07-01 |",
            "| 业绩比较基准 | 中证香港创新药指数收益率(人民币计价) |",
            "",
            "## 关键证据",
            "- [证据1](https://example.com)；信号：防守资产相对占优；强弱：中等；结论：偏中性，当前更适合作为观察对象而不是立即升级推荐。",
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


def test_report_guard_blocks_observe_stock_pick_missing_feature_retention_sections(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/stock_picks/final/stock_picks_cn_2026-04-01_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
        ),
        encoding="utf-8",
    )
    markdown = "\n".join(
        [
            "# 今日个股观察 | 2026-04-01",
            "",
            "| 报告定位 | 观察稿 |",
            "",
            "## 今日动作摘要",
            "- 当前无正式推荐。",
            "",
            "## 催化证据来源",
            "- `结构化事件`：[测试证据](https://example.com)",
            "",
            "## 历史相似样本附注",
            "- 非重叠样本 `12` 个。",
            "",
            "## A股",
            "- 继续观察。",
            "",
            "升级条件",
            "- 放量站稳。",
            "",
            "关键盯盘价位",
            "- 20 日线。",
        ]
    )

    with pytest.raises(ReportGuardError, match="第二批：继续跟踪"):
        export_reviewed_markdown_bundle(
            report_type="stock_pick",
            markdown_text=markdown,
            markdown_path=target,
            release_findings=[],
        )


def test_exported_bundle_lines_includes_html_between_markdown_and_pdf() -> None:
    lines = exported_bundle_lines(
        {
            "markdown": Path("/tmp/demo.md"),
            "html": Path("/tmp/demo.html"),
            "pdf": Path("/tmp/demo.pdf"),
        }
    )

    assert lines == [
        "[client markdown] /tmp/demo.md",
        "[client html] /tmp/demo.html",
        "[client pdf] /tmp/demo.pdf",
    ]


def test_report_guard_blocks_single_round_pass_with_actionable_findings(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/final/scan_300308_2026-03-21_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="形式上想放行，但正文还有问题。",
            issues=["- `P2`：解释链还没收口。"],
            zero_prompt=["- 零提示再看，最先冒出来的问题仍然是解释链断裂。"],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="缺少回修闭环"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_blocks_multi_round_pass_without_close_or_carry_evidence(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/final/scan_300308_2026-03-22_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=2,
            previous_round=1,
            converged="是",
            continue_next="否",
            summary="看起来想收敛，但没写闭环证据。",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="收敛证据不足"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_blocks_multi_round_pass_with_only_placeholder_handoff(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/scans/final/scan_300308_2026-03-23_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=2,
            previous_round=1,
            converged="是",
            continue_next="否",
            summary="看起来已经收敛，但闭环字段只有占位词。",
            convergence_extra=[
                "- carried_p0_p1：无",
                "- closed_items：无",
            ],
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReportGuardError, match="收敛证据不足"):
        export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=SCAN_DETAIL_MARKDOWN,
            markdown_path=target,
            release_findings=[],
        )


def test_report_guard_blocks_briefing_stale_market_snapshot_signals(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/briefings/final/daily_briefing_2026-04-01_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            issues=["- 无新的实质问题。"],
            independent=["- 结论一致。"],
            zero_prompt=["- 零提示二审未发现新的实质性问题。"],
            convergence_extra=["- closed_items：无"],
        ),
        encoding="utf-8",
    )

    markdown = "\n".join(
        [
            "# 今日晨报 | 2026-04-01",
            "",
            "## 为什么今天这么判断",
            "",
            "- A股轮动快照未通过 freshness 校验，本轮先不把旧盘面当成今天主线依据。",
            "",
            "- A股热股前排：首航新能（+20.01%）",
            "",
            "## 宏观领先指标",
            "",
            "- 指标一",
            "",
            "## 数据完整度",
            "",
            "- 覆盖完整",
            "",
            "## 今天怎么做",
            "",
            "- 先观察",
            "",
            "## 重点观察",
            "",
            "- 方向一",
            "",
            "## 今日A股观察池",
            "",
            "| 排名 | 标的 |",
            "| --- | --- |",
            "| 1 | 演示标的 |",
        ]
        )

    with pytest.raises(ReportGuardError, match="仍前置了盘面信号"):
        export_reviewed_markdown_bundle(
            report_type="briefing",
            markdown_text=markdown,
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "market_snapshot_contract": {
                    "contract_version": "briefing.market_snapshot.v1",
                    "stale_detected": True,
                    "warning_line": "A股轮动快照未通过 freshness 校验，本轮先不把旧盘面当成今天主线依据。",
                }
            },
        )


def test_report_guard_blocks_briefing_without_linked_external_signal_items(isolated_reports: Path) -> None:
    target = isolated_reports / "reports/briefings/final/daily_briefing_2026-04-02_client_final.md"
    review = review_path_for(target)
    review.parent.mkdir(parents=True, exist_ok=True)
    review.write_text(
        _review_text(
            status="PASS",
            no_p1="是",
            allow_delivery="是",
            round_value=1,
            converged="是",
            continue_next="否",
            summary="可发",
            issues=["- 无新的实质问题。"],
            independent=["- 结论一致。"],
            zero_prompt=["- 零提示二审未发现新的实质性问题。"],
            convergence_extra=["- closed_items：无"],
        ),
        encoding="utf-8",
    )

    markdown = "\n".join(
        [
            "# 今日晨报 | 2026-04-02",
            "",
            "## 为什么今天这么判断",
            "",
            "- 先看强修复，但还不是全面反转确认。",
            "",
            "## 今日情报看板",
            "",
            "- A股主题活跃：创新药/医药（涨停集中：化学制药 6 家）",
            "- A股主题跟踪：AI算力/光模块（观察池前排 中际旭创）",
            "- A股热股前排：新易盛（+8.31%）",
            "",
            "## 宏观领先指标",
            "",
            "- PMI 回到荣枯线上方。",
            "",
            "## 数据完整度",
            "",
            "- 当前只保留盘面句，没有外部情报链接。",
            "",
            "## 今天怎么做",
            "",
            "- 先盯医药和算力主线的延续性，再看量能能否继续温和放大。",
            "",
            "## 重点观察",
            "",
            "- 创新药",
            "- AI算力",
            "",
            "## 今日A股观察池",
            "",
            "| 排名 | 标的 |",
            "| --- | --- |",
            "| 1 | 新易盛 |",
        ]
    )

    with pytest.raises(ReportGuardError, match="没有任何可点击外部情报"):
        export_reviewed_markdown_bundle(
            report_type="briefing",
            markdown_text=markdown,
            markdown_path=target,
            release_findings=[],
            extra_manifest={
                "market_snapshot_contract": {
                    "contract_version": "briefing.market_snapshot.v1",
                    "stale_detected": False,
                    "warning_line": "",
                }
            },
        )
