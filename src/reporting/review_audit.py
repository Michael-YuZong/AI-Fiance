"""Governance audits for round-based external review records."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from src.commands.report_guard import manifest_path_for
from src.reporting.review_ledger import ReviewRecord, collect_review_records
from src.reporting.review_record_utils import canonicalize_sections, split_sections

_REQUIRED_ROUND_SECTIONS = (
    "结论",
    "主要问题",
    "框架外问题",
    "零提示发散审",
    "建议沉淀",
    "收敛结论",
)

_SOLIDIFICATION_KEYS = (
    "prompt",
    "hard rule",
    "guard",
    "workflow",
    "test",
    "fixture",
    "lesson",
    "backlog",
)
_FACTOR_CONTRACT_REPORT_TYPES = {"stock_pick", "etf_pick", "fund_pick", "briefing"}
_PROXY_CONTRACT_REPORT_TYPES = {"stock_pick", "etf_pick", "fund_pick", "briefing"}


@dataclass(frozen=True)
class ReviewAuditFinding:
    path: str
    series_id: str
    round: int | None
    severity: str
    category: str
    title: str
    detail: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def _meaningful_lines(text: str) -> List[str]:
    result: List[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line in {"`go`", "`hold`", "`blocked`", "无", "无新增阻塞项"}:
            continue
        result.append(line)
    return result


def _has_actionable_content(text: str) -> bool:
    lines = _meaningful_lines(text)
    if not lines:
        return False
    normalized = " ".join(lines)
    empty_markers = ("无新增", "没有新的", "无新的", "不适用", "已满足")
    return not any(marker in normalized for marker in empty_markers)


def _solidification_categories(text: str) -> List[str]:
    lowered = text.lower()
    categories = [key for key in _SOLIDIFICATION_KEYS if key in lowered]
    return list(dict.fromkeys(categories))


def _latest_by_series(records: Iterable[ReviewRecord]) -> Dict[str, ReviewRecord]:
    latest: Dict[str, ReviewRecord] = {}
    for record in records:
        current = latest.get(record.series_id)
        current_round = current.round if current and current.round is not None else -1
        record_round = record.round if record.round is not None else -1
        if current is None or record_round >= current_round:
            latest[record.series_id] = record
    return latest


def _records_by_series(records: Iterable[ReviewRecord]) -> Dict[str, List[ReviewRecord]]:
    grouped: Dict[str, List[ReviewRecord]] = {}
    for record in records:
        grouped.setdefault(record.series_id, []).append(record)
    for key in grouped:
        grouped[key] = sorted(grouped[key], key=lambda row: row.round if row.round is not None else -1)
    return grouped


def _manifest_payload_for_record(record: ReviewRecord) -> Dict[str, Any]:
    target_ref = str(record.review_target_ref or record.review_target or "").strip()
    if not target_ref:
        return {}
    target_path = Path(target_ref)
    if not target_path.exists():
        return {}
    try:
        manifest_path = manifest_path_for(target_path)
    except Exception:
        manifest_path = None
    if (manifest_path is None or not manifest_path.exists()) and "reports" in target_path.parts:
        parts = list(target_path.parts)
        reports_index = parts.index("reports")
        reports_root = Path(*parts[: reports_index + 1])
        relative = target_path.relative_to(reports_root)
        stem = relative.with_suffix("")
        manifest_path = reports_root / "reviews" / stem.parent / f"{stem.name}__release_manifest.json"
    if manifest_path is None or not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _audit_round_structure(record: ReviewRecord, sections: Mapping[str, str]) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    if record.round is None:
        return findings
    missing = [name for name in _REQUIRED_ROUND_SECTIONS if not sections.get(name, "").strip()]
    if missing:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="round_contract",
                title="缺少必需外审段落",
                detail="该 round 记录缺少必需段落：" + " / ".join(missing),
            )
        )
    if record.round and record.round > 1 and record.previous_round is None:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="round_contract",
                title="缺少 previous_round",
                detail="第 2 轮及之后的审稿记录必须显式写出 previous_round。",
            )
        )
    return findings


def _audit_solidification(record: ReviewRecord, sections: Mapping[str, str]) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    if record.round is None:
        return findings
    issues_text = sections.get("主要问题", "")
    divergent_text = sections.get("框架外问题", "")
    solidification_text = sections.get("建议沉淀", "")
    needs_solidification = _has_actionable_content(issues_text) or _has_actionable_content(divergent_text)
    if needs_solidification and not solidification_text.strip():
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="solidification",
                title="finding 没有沉淀去向",
                detail="该 round 有主要问题或框架外问题，但缺少“建议沉淀”段落。",
            )
        )
        return findings
    if needs_solidification:
        categories = _solidification_categories(solidification_text)
        if not categories:
            findings.append(
                ReviewAuditFinding(
                    path=record.path,
                    series_id=record.series_id,
                    round=record.round,
                    severity="P2",
                    category="solidification",
                    title="建议沉淀缺少明确类别",
                    detail="该 round 写了建议沉淀，但没有显式落到 prompt / guard / tests / backlog 等去向。",
                )
            )
    return findings


def _audit_series_consistency(series_id: str, records: List[ReviewRecord]) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    if not records:
        return findings

    baseline_target = records[0].review_target_ref or records[0].review_target
    baseline_prompt = records[0].review_prompt_ref or records[0].review_prompt
    expected_round = records[0].round if records[0].round is not None else None

    for index, record in enumerate(records):
        current_target = record.review_target_ref or record.review_target
        current_prompt = record.review_prompt_ref or record.review_prompt
        if baseline_target and current_target and current_target != baseline_target:
            findings.append(
                ReviewAuditFinding(
                    path=record.path,
                    series_id=series_id,
                    round=record.round,
                    severity="P1",
                    category="series_consistency",
                    title="同一审稿序列的 target 漂移",
                    detail=f"该记录的 review_target 已从 `{baseline_target}` 漂移到 `{current_target}`。",
                )
            )
        if baseline_prompt and current_prompt and current_prompt != baseline_prompt:
            findings.append(
                ReviewAuditFinding(
                    path=record.path,
                    series_id=series_id,
                    round=record.round,
                    severity="P1",
                    category="series_consistency",
                    title="同一审稿序列的 prompt 漂移",
                    detail=f"该记录的 review_prompt 已从 `{baseline_prompt}` 漂移到 `{current_prompt}`。",
                )
            )
        if expected_round is not None and record.round is not None and index > 0:
            expected_round += 1
            if record.round != expected_round:
                findings.append(
                    ReviewAuditFinding(
                        path=record.path,
                        series_id=series_id,
                        round=record.round,
                        severity="P1",
                        category="series_consistency",
                        title="round 序号不连续",
                        detail=f"该序列预期下一轮是 {expected_round}，实际记录为 {record.round}。",
                    )
                )
                expected_round = record.round
            if record.previous_round != records[index - 1].round:
                findings.append(
                    ReviewAuditFinding(
                        path=record.path,
                        series_id=series_id,
                        round=record.round,
                        severity="P1",
                        category="series_consistency",
                        title="previous_round 与上一轮不一致",
                        detail=(
                            f"该记录写的是 previous_round={record.previous_round}，"
                            f"但实际上一轮是 {records[index - 1].round}。"
                        ),
                    )
                )

    latest = records[-1]
    if latest.status == "PASS" and latest.recommend_continue == "是":
        findings.append(
            ReviewAuditFinding(
                path=latest.path,
                series_id=series_id,
                round=latest.round,
                severity="P1",
                category="series_consistency",
                title="PASS 记录仍要求继续下一轮",
                detail="收敛状态已标记 PASS，但是否建议继续下一轮仍为“是”。",
            )
        )
    if latest.status in {"BLOCKED", "IN_REVIEW"} and latest.recommend_continue == "否":
        findings.append(
            ReviewAuditFinding(
                path=latest.path,
                series_id=series_id,
                round=latest.round,
                severity="P2",
                category="series_consistency",
                title="未通过记录没有继续审稿信号",
                detail="最新状态仍为 BLOCKED / IN_REVIEW，但是否建议继续下一轮为“否”。",
            )
        )
    return findings


def _audit_manifest_factor_contract(record: ReviewRecord) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    manifest_payload = _manifest_payload_for_record(record)
    if not manifest_payload:
        return findings
    report_type = str(manifest_payload.get("report_type", "")).strip()
    if report_type not in _FACTOR_CONTRACT_REPORT_TYPES:
        return findings
    artifacts = dict(manifest_payload.get("artifacts") or {})
    factor_contract = dict(artifacts.get("factor_contract") or {})
    if not factor_contract:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="manifest 缺少 factor_contract",
                detail=f"`{report_type}` 的 manifest 已生成，但没有写入 factor_contract 摘要，无法在 review audit 里追踪强因子状态。",
            )
        )
    return findings


def _audit_manifest_proxy_contract(record: ReviewRecord) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    manifest_payload = _manifest_payload_for_record(record)
    if not manifest_payload:
        return findings
    report_type = str(manifest_payload.get("report_type", "")).strip()
    if report_type not in _PROXY_CONTRACT_REPORT_TYPES:
        return findings
    artifacts = dict(manifest_payload.get("artifacts") or {})
    proxy_contract = dict(artifacts.get("proxy_contract") or {})
    if not proxy_contract:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="manifest 缺少 proxy_contract",
                detail=f"`{report_type}` 的 manifest 已生成，但没有写入 proxy_contract 摘要，无法在 review audit 里追踪代理信号的置信度和降级影响。",
            )
        )
    return findings


def build_review_audit(root: Path) -> Dict[str, Any]:
    records = collect_review_records(root)
    audited_records = [record for record in records if record.protocol == "structured_round"]
    skipped_legacy_records = [record for record in records if record.protocol != "structured_round"]
    findings: List[ReviewAuditFinding] = []

    for record in audited_records:
        sections = canonicalize_sections(split_sections(Path(record.path).read_text(encoding="utf-8")))
        findings.extend(_audit_round_structure(record, sections))
        findings.extend(_audit_solidification(record, sections))
        findings.extend(_audit_manifest_factor_contract(record))
        findings.extend(_audit_manifest_proxy_contract(record))

    for series_id, series_records in _records_by_series(audited_records).items():
        findings.extend(_audit_series_consistency(series_id, series_records))

    severity_counts: Dict[str, int] = {}
    category_counts: Dict[str, int] = {}
    for finding in findings:
        severity_counts[finding.severity] = severity_counts.get(finding.severity, 0) + 1
        category_counts[finding.category] = category_counts.get(finding.category, 0) + 1

    latest = _latest_by_series(records)
    active_findings = [
        finding
        for finding in findings
        if latest.get(finding.series_id) and latest[finding.series_id].path == finding.path
    ]

    return {
        "root": str(root),
        "summary": {
            "total_records": len(records),
            "audited_records": len(audited_records),
            "skipped_legacy_records": len(skipped_legacy_records),
            "total_series": len(latest),
            "total_findings": len(findings),
            "active_findings": len(active_findings),
            "series_with_findings": len({finding.series_id for finding in findings}),
        },
        "severity_counts": severity_counts,
        "category_counts": category_counts,
        "findings": [finding.to_dict() for finding in findings],
        "active_findings": [finding.to_dict() for finding in active_findings],
        "latest_records": [record.to_dict() for record in latest.values()],
        "skipped_legacy_records": [record.to_dict() for record in skipped_legacy_records],
    }


def render_review_audit_markdown(audit: Mapping[str, Any]) -> str:
    summary = audit.get("summary", {})
    active_findings = audit.get("active_findings", [])
    lines = [
        "# External Review Audit",
        "",
        f"- root: `{audit.get('root', '')}`",
        f"- total records: `{summary.get('total_records', 0)}`",
        f"- audited structured-round records: `{summary.get('audited_records', 0)}`",
        f"- skipped legacy records: `{summary.get('skipped_legacy_records', 0)}`",
        f"- total series: `{summary.get('total_series', 0)}`",
        f"- total findings: `{summary.get('total_findings', 0)}`",
        f"- active findings: `{summary.get('active_findings', 0)}`",
        "",
    ]

    severity_counts = audit.get("severity_counts", {})
    if severity_counts:
        lines.extend(["## Severity", ""])
        for key, value in sorted(severity_counts.items()):
            lines.append(f"- {key}: `{value}`")
        lines.append("")

    category_counts = audit.get("category_counts", {})
    if category_counts:
        lines.extend(["## Categories", ""])
        for key, value in sorted(category_counts.items()):
            lines.append(f"- {key}: `{value}`")
        lines.append("")

    lines.extend(
        [
            "## Active Findings",
            "",
            "| severity | category | series | round | title | file |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for finding in active_findings:
        lines.append(
            "| {severity} | {category} | `{series_id}` | {round} | {title} | `{path}` |".format(
                severity=finding.get("severity", ""),
                category=finding.get("category", ""),
                series_id=finding.get("series_id", ""),
                round=finding.get("round", ""),
                title=str(finding.get("title", "")).replace("|", "/"),
                path=finding.get("path", ""),
            )
        )
    if not active_findings:
        lines.append("| — | — | — | — | no active findings | — |")
    lines.append("")
    return "\n".join(lines)


def render_review_audit_json(audit: Mapping[str, Any]) -> str:
    return json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True)
