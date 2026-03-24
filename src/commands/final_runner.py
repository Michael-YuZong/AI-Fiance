"""Shared client-final export helpers for command entrypoints."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.commands.report_guard import ReportGuardError, export_reviewed_markdown_bundle, review_path_for
from src.reporting.review_scaffold import ensure_external_review_scaffold, maybe_autoclose_external_review


ReleaseCheckFn = Callable[[str, str], Sequence[str]]


def write_detail_markdown(detail_path: Path, markdown_text: str) -> Path:
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.write_text(markdown_text, encoding="utf-8")
    return detail_path


def finalize_client_markdown(
    *,
    report_type: str,
    client_markdown: str,
    markdown_path: Path,
    detail_markdown: str,
    detail_path: Path,
    extra_manifest: Mapping[str, Any] | None = None,
    release_checker: ReleaseCheckFn | None = None,
    report_kind: str = "",
    scaffold_generated_by: str = "",
) -> dict[str, Path]:
    written_detail = write_detail_markdown(detail_path, detail_markdown)
    review_path = review_path_for(markdown_path)
    scaffold_created = False
    if not review_path.exists():
        ensure_external_review_scaffold(
            review_path=review_path,
            markdown_path=markdown_path,
            report_type=report_type,
            report_kind=report_kind,
            detail_source=written_detail,
            scaffold_generated_by=scaffold_generated_by,
        )
        scaffold_created = True
    else:
        maybe_autoclose_external_review(
            review_path=review_path,
            markdown_path=markdown_path,
            report_type=report_type,
            report_kind=report_kind,
            detail_source=written_detail,
            scaffold_generated_by=scaffold_generated_by,
        )
    findings = list(release_checker(client_markdown, detail_markdown)) if release_checker else []
    try:
        return export_reviewed_markdown_bundle(
            report_type=report_type,
            markdown_text=client_markdown,
            markdown_path=markdown_path,
            release_findings=findings,
            extra_manifest={
                **dict(extra_manifest or {}),
                "detail_source": str(written_detail),
            },
        )
    except ReportGuardError as exc:
        if scaffold_created:
            raise SystemExit(
                f"{exc}\n已生成外审模板：`{review_path}`。先完成 Pass A / Pass B，并把收敛结论更新到 PASS 后，再重跑同一命令。"
            )
        raise SystemExit(str(exc))
