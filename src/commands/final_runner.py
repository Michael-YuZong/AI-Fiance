"""Shared client-final export helpers for command entrypoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.commands.report_guard import ReportGuardError, export_reviewed_markdown_bundle, review_path_for
from src.output.catalyst_web_review import preserve_existing_catalyst_web_review
from src.output.client_export import _rewrite_local_report_asset_paths
from src.reporting.review_scaffold import ensure_external_review_scaffold, maybe_autoclose_external_review


ReleaseCheckFn = Callable[[str, str], Sequence[str]]


def write_detail_markdown(detail_path: Path, markdown_text: str) -> Path:
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.write_text(markdown_text, encoding="utf-8")
    return detail_path


def internal_sidecar_path(detail_path: Path, filename: str) -> Path:
    stem = detail_path.stem
    if stem.endswith("_internal_detail"):
        stem = stem[: -len("_internal_detail")]
    return detail_path.with_name(f"{stem}_{filename}")


def _write_text_sidecars(sidecars: Mapping[str, tuple[Path, str]] | None) -> dict[str, Path]:
    written: dict[str, Path] = {}
    for key, payload in dict(sidecars or {}).items():
        path, content = payload
        path.parent.mkdir(parents=True, exist_ok=True)
        text = str(content)
        if str(key) == "catalyst_web_review":
            text = preserve_existing_catalyst_web_review(path, text)
        path.write_text(text, encoding="utf-8")
        written[str(key)] = path
    return written


def _write_json_sidecars(sidecars: Mapping[str, tuple[Path, Any]] | None) -> dict[str, Path]:
    written: dict[str, Path] = {}
    for key, payload in dict(sidecars or {}).items():
        path, content = payload
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        written[str(key)] = path
    return written


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
    text_sidecars: Mapping[str, tuple[Path, str]] | None = None,
    json_sidecars: Mapping[str, tuple[Path, Any]] | None = None,
) -> dict[str, Path]:
    written_detail = write_detail_markdown(detail_path, detail_markdown)
    written_text_sidecars = _write_text_sidecars(text_sidecars)
    written_json_sidecars = _write_json_sidecars(json_sidecars)
    release_check_base = markdown_path.parent.parent if markdown_path.parent.name == "final" else markdown_path.parent
    normalized_client_markdown = _rewrite_local_report_asset_paths(client_markdown, release_check_base)
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
    findings = list(release_checker(normalized_client_markdown, detail_markdown)) if release_checker else []
    try:
        return export_reviewed_markdown_bundle(
            report_type=report_type,
            markdown_text=normalized_client_markdown,
            markdown_path=markdown_path,
            release_findings=findings,
            extra_manifest={
                **dict(extra_manifest or {}),
                "detail_source": str(written_detail),
                **(
                    {
                        "editor_artifacts": {
                            **{key: str(path) for key, path in written_text_sidecars.items()},
                            **{key: str(path) for key, path in written_json_sidecars.items()},
                        }
                    }
                    if (written_text_sidecars or written_json_sidecars)
                    else {}
                ),
            },
        )
    except ReportGuardError as exc:
        if scaffold_created:
            raise SystemExit(
                f"{exc}\n已生成外审模板：`{review_path}`。先完成 Pass A / Pass B，并把收敛结论更新到 PASS 后，再重跑同一命令。"
            )
        raise SystemExit(str(exc))
