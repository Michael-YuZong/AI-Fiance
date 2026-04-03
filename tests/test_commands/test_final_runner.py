from __future__ import annotations

from pathlib import Path

import pytest

import src.commands.final_runner as final_runner


def test_finalize_client_markdown_writes_detail_and_passes_detail_source(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_export_reviewed_markdown_bundle(**kwargs):
        captured.update(kwargs)
        return {
            "markdown": Path(kwargs["markdown_path"]),
            "html": Path(kwargs["markdown_path"]).with_suffix(".html"),
            "pdf": Path(kwargs["markdown_path"]).with_suffix(".pdf"),
        }

    monkeypatch.setattr(final_runner, "export_reviewed_markdown_bundle", fake_export_reviewed_markdown_bundle)
    monkeypatch.setattr(
        final_runner,
        "review_path_for",
        lambda markdown_path: tmp_path / "reports/reviews/demo/final" / f"{Path(markdown_path).stem}__external_review.md",
    )
    monkeypatch.setattr(
        final_runner,
        "ensure_external_review_scaffold",
        lambda **kwargs: Path(kwargs["review_path"]).parent.mkdir(parents=True, exist_ok=True) or Path(kwargs["review_path"]).write_text(
            "\n".join(
                [
                    "## 一句话总评",
                    "可发",
                    "## 主要问题",
                    "- 无新的实质问题。",
                    "## 独立答案",
                    "- 结论一致。",
                    "## 零提示发散审",
                    "- 无新的实质性发散问题。",
                    "## 收敛结论",
                    "- round：1",
                    "- 状态：PASS",
                    "- 无新的 P0/P1：是",
                    "- 本轮是否收敛：是",
                    "- 是否建议继续下一轮：否",
                    "- 允许作为成稿交付：是",
                    "- 结构审执行者：`reviewer_a`",
                    "- 发散审执行者：`reviewer_b`",
                ]
            ),
            encoding="utf-8",
        )
        or Path(kwargs["review_path"]),
    )
    monkeypatch.setattr(final_runner, "maybe_autoclose_external_review", lambda **kwargs: False)

    detail_path = tmp_path / "reports/demo/internal/detail.md"
    markdown_path = tmp_path / "reports/demo/final/client_final.md"

    bundle = final_runner.finalize_client_markdown(
        report_type="scan",
        client_markdown="# client",
        markdown_path=markdown_path,
        detail_markdown="# detail",
        detail_path=detail_path,
        extra_manifest={"symbol": "300750"},
        release_checker=lambda markdown, source_text: captured.update({"checked": (markdown, source_text)}) or [],
    )

    assert detail_path.read_text(encoding="utf-8") == "# detail"
    assert captured["checked"] == ("# client", "# detail")
    assert captured["report_type"] == "scan"
    assert captured["extra_manifest"]["symbol"] == "300750"
    assert captured["extra_manifest"]["detail_source"] == str(detail_path)
    assert bundle["markdown"] == markdown_path


def test_finalize_client_markdown_wraps_report_guard_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        final_runner,
        "export_reviewed_markdown_bundle",
        lambda **kwargs: (_ for _ in ()).throw(final_runner.ReportGuardError("blocked")),
    )
    monkeypatch.setattr(
        final_runner,
        "review_path_for",
        lambda markdown_path: tmp_path / "reports/reviews/demo/final" / f"{Path(markdown_path).stem}__external_review.md",
    )
    monkeypatch.setattr(final_runner, "ensure_external_review_scaffold", lambda **kwargs: Path(kwargs["review_path"]))
    monkeypatch.setattr(final_runner, "maybe_autoclose_external_review", lambda **kwargs: False)

    with pytest.raises(SystemExit, match="blocked"):
        final_runner.finalize_client_markdown(
            report_type="scan",
            client_markdown="# client",
            markdown_path=tmp_path / "reports/demo/final/client_final.md",
            detail_markdown="# detail",
            detail_path=tmp_path / "reports/demo/internal/detail.md",
        )


def test_finalize_client_markdown_scaffolds_missing_review(monkeypatch, tmp_path: Path) -> None:
    scaffold_calls: dict[str, object] = {}

    monkeypatch.setattr(
        final_runner,
        "review_path_for",
        lambda markdown_path: tmp_path / "reports/reviews/demo/final" / f"{Path(markdown_path).stem}__external_review.md",
    )
    monkeypatch.setattr(
        final_runner,
        "ensure_external_review_scaffold",
        lambda **kwargs: scaffold_calls.update(kwargs) or Path(kwargs["review_path"]).parent.mkdir(parents=True, exist_ok=True) or Path(kwargs["review_path"]).write_text("scaffold", encoding="utf-8") or Path(kwargs["review_path"]),
    )
    monkeypatch.setattr(final_runner, "maybe_autoclose_external_review", lambda **kwargs: False)
    monkeypatch.setattr(
        final_runner,
        "export_reviewed_markdown_bundle",
        lambda **kwargs: (_ for _ in ()).throw(final_runner.ReportGuardError("外部评审尚未通过")),
    )

    with pytest.raises(SystemExit, match="已生成外审模板"):
        final_runner.finalize_client_markdown(
            report_type="briefing",
            client_markdown="# client",
            markdown_path=tmp_path / "reports/demo/final/client_final.md",
            detail_markdown="# detail",
            detail_path=tmp_path / "reports/demo/internal/detail.md",
            scaffold_generated_by="briefing daily --client-final",
        )

    assert scaffold_calls["scaffold_generated_by"] == "briefing daily --client-final"


def test_finalize_client_markdown_rewrites_local_asset_paths_before_release_check(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_export_reviewed_markdown_bundle(**kwargs):
        captured["markdown_text"] = kwargs["markdown_text"]
        return {
            "markdown": Path(kwargs["markdown_path"]),
            "html": Path(kwargs["markdown_path"]).with_suffix(".html"),
            "pdf": Path(kwargs["markdown_path"]).with_suffix(".pdf"),
        }

    monkeypatch.setattr(final_runner, "export_reviewed_markdown_bundle", fake_export_reviewed_markdown_bundle)
    monkeypatch.setattr(
        final_runner,
        "review_path_for",
        lambda markdown_path: tmp_path / "reports/reviews/demo/final" / f"{Path(markdown_path).stem}__external_review.md",
    )
    monkeypatch.setattr(
        final_runner,
        "ensure_external_review_scaffold",
        lambda **kwargs: Path(kwargs["review_path"]).parent.mkdir(parents=True, exist_ok=True) or Path(kwargs["review_path"]).write_text(
            "\n".join(
                [
                    "## 一句话总评",
                    "可发",
                    "## 主要问题",
                    "- 无新的实质问题。",
                    "## 独立答案",
                    "- 结论一致。",
                    "## 零提示发散审",
                    "- 无新的实质性发散问题。",
                    "## 收敛结论",
                    "- round：1",
                    "- 状态：PASS",
                    "- 无新的 P0/P1：是",
                    "- 本轮是否收敛：是",
                    "- 是否建议继续下一轮：否",
                    "- 允许作为成稿交付：是",
                    "- 结构审执行者：`reviewer_a`",
                    "- 发散审执行者：`reviewer_b`",
                ]
            ),
            encoding="utf-8",
        )
        or Path(kwargs["review_path"]),
    )
    monkeypatch.setattr(final_runner, "maybe_autoclose_external_review", lambda **kwargs: False)

    markdown_path = tmp_path / "reports/demo/final/client_final.md"
    absolute_asset = str((tmp_path / "reports/assets/demo.png").resolve())

    final_runner.finalize_client_markdown(
        report_type="scan",
        client_markdown=f"![图表]({absolute_asset})",
        markdown_path=markdown_path,
        detail_markdown="# detail",
        detail_path=tmp_path / "reports/demo/internal/detail.md",
        release_checker=lambda markdown, source_text: captured.update({"checked_markdown": markdown}) or [],
    )

    assert "/reports/assets/" not in str(captured["checked_markdown"])
    assert "../assets/demo.png" in str(captured["checked_markdown"])
    assert "../assets/demo.png" in str(captured["markdown_text"])


def test_finalize_client_markdown_writes_editor_sidecars_and_manifest_paths(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_export_reviewed_markdown_bundle(**kwargs):
        captured.update(kwargs)
        return {
            "markdown": Path(kwargs["markdown_path"]),
            "html": Path(kwargs["markdown_path"]).with_suffix(".html"),
            "pdf": Path(kwargs["markdown_path"]).with_suffix(".pdf"),
        }

    monkeypatch.setattr(final_runner, "export_reviewed_markdown_bundle", fake_export_reviewed_markdown_bundle)
    monkeypatch.setattr(
        final_runner,
        "review_path_for",
        lambda markdown_path: tmp_path / "reports/reviews/demo/final" / f"{Path(markdown_path).stem}__external_review.md",
    )
    monkeypatch.setattr(
        final_runner,
        "ensure_external_review_scaffold",
        lambda **kwargs: Path(kwargs["review_path"]).parent.mkdir(parents=True, exist_ok=True) or Path(kwargs["review_path"]).write_text(
            "\n".join(
                [
                    "## 一句话总评",
                    "可发",
                    "## 主要问题",
                    "- 无新的实质问题。",
                    "## 独立答案",
                    "- 结论一致。",
                    "## 零提示发散审",
                    "- 无新的实质性发散问题。",
                    "## 收敛结论",
                    "- round：1",
                    "- 状态：PASS",
                    "- 无新的 P0/P1：是",
                    "- 本轮是否收敛：是",
                    "- 是否建议继续下一轮：否",
                    "- 允许作为成稿交付：是",
                    "- 结构审执行者：`reviewer_a`",
                    "- 发散审执行者：`reviewer_b`",
                ]
            ),
            encoding="utf-8",
        )
        or Path(kwargs["review_path"]),
    )
    monkeypatch.setattr(final_runner, "maybe_autoclose_external_review", lambda **kwargs: False)

    detail_path = tmp_path / "reports/demo/internal/client_internal_detail.md"
    payload_path = final_runner.internal_sidecar_path(detail_path, "editor_payload.json")
    prompt_path = final_runner.internal_sidecar_path(detail_path, "editor_prompt.md")

    final_runner.finalize_client_markdown(
        report_type="scan",
        client_markdown="# client",
        markdown_path=tmp_path / "reports/demo/final/client_final.md",
        detail_markdown="# detail",
        detail_path=detail_path,
        text_sidecars={"editor_prompt": (prompt_path, "# prompt")},
        json_sidecars={"editor_payload": (payload_path, {"report_type": "scan", "packet_version": "editor-v1"})},
    )

    assert prompt_path.read_text(encoding="utf-8") == "# prompt"
    assert '"packet_version": "editor-v1"' in payload_path.read_text(encoding="utf-8")
    assert captured["extra_manifest"]["editor_artifacts"]["editor_prompt"] == str(prompt_path)
    assert captured["extra_manifest"]["editor_artifacts"]["editor_payload"] == str(payload_path)


def test_finalize_client_markdown_preserves_completed_catalyst_web_review(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_export_reviewed_markdown_bundle(**kwargs):
        captured.update(kwargs)
        return {
            "markdown": Path(kwargs["markdown_path"]),
            "html": Path(kwargs["markdown_path"]).with_suffix(".html"),
            "pdf": Path(kwargs["markdown_path"]).with_suffix(".pdf"),
        }

    monkeypatch.setattr(final_runner, "export_reviewed_markdown_bundle", fake_export_reviewed_markdown_bundle)
    monkeypatch.setattr(
        final_runner,
        "review_path_for",
        lambda markdown_path: tmp_path / "reports/reviews/demo/final" / f"{Path(markdown_path).stem}__external_review.md",
    )
    monkeypatch.setattr(
        final_runner,
        "ensure_external_review_scaffold",
        lambda **kwargs: Path(kwargs["review_path"]).parent.mkdir(parents=True, exist_ok=True) or Path(kwargs["review_path"]).write_text(
            "\n".join(
                [
                    "## 一句话总评",
                    "可发",
                    "## 主要问题",
                    "- 无新的实质问题。",
                    "## 独立答案",
                    "- 结论一致。",
                    "## 零提示发散审",
                    "- 无新的实质性发散问题。",
                    "## 收敛结论",
                    "- round：1",
                    "- 状态：PASS",
                    "- 无新的 P0/P1：是",
                    "- 本轮是否收敛：是",
                    "- 是否建议继续下一轮：否",
                    "- 允许作为成稿交付：是",
                    "- 结构审执行者：`reviewer_a`",
                    "- 发散审执行者：`reviewer_b`",
                ]
            ),
            encoding="utf-8",
        )
        or Path(kwargs["review_path"]),
    )
    monkeypatch.setattr(final_runner, "maybe_autoclose_external_review", lambda **kwargs: False)

    detail_path = tmp_path / "reports/demo/internal/client_internal_detail.md"
    catalyst_path = final_runner.internal_sidecar_path(detail_path, "catalyst_web_review.md")
    catalyst_path.parent.mkdir(parents=True, exist_ok=True)
    completed_review = "\n".join(
        [
            "# Catalyst Web Review | scan | 2026-03-26",
            "",
            "## 1. 半导体ETF (512480)",
            "",
            "### 复核结论",
            "",
            "- 结论：只有主题级催化",
        ]
    )
    catalyst_path.write_text(completed_review, encoding="utf-8")

    final_runner.finalize_client_markdown(
        report_type="scan",
        client_markdown="# client",
        markdown_path=tmp_path / "reports/demo/final/client_final.md",
        detail_markdown="# detail",
        detail_path=detail_path,
        text_sidecars={"catalyst_web_review": (catalyst_path, "# scaffold")},
    )

    assert catalyst_path.read_text(encoding="utf-8") == completed_review
