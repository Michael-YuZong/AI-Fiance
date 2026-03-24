"""Governance audits for round-based external review records."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from src.commands.report_guard import manifest_path_for
from src.reporting.review_ledger import ReviewRecord, collect_review_records
from src.reporting.review_record_utils import (
    bullet_block_items,
    canonicalize_sections,
    has_actionable_content,
    parse_bullet_mapping,
    split_sections,
)

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


def _audit_split_review_roles(record: ReviewRecord, sections: Mapping[str, str]) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    if record.round is None:
        return findings
    convergence = parse_bullet_mapping(sections.get("收敛结论", "").splitlines())
    structural = str(convergence.get("结构审执行者", "")).strip()
    divergent = str(convergence.get("发散审执行者", "")).strip()
    if not structural or not divergent:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="round_contract",
                title="缺少分阶段外审执行者",
                detail="收敛结论缺少 `结构审执行者` 或 `发散审执行者`，无法证明结构审和发散审已分离执行。",
            )
        )
        return findings
    if structural.casefold() == divergent.casefold():
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="round_contract",
                title="结构审与发散审使用了同一执行者",
                detail="当前 review 记录里 `结构审执行者` 与 `发散审执行者` 相同，不满足双 reviewer / 双子 agent 外审合同。",
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
    needs_solidification = has_actionable_content(issues_text) or has_actionable_content(divergent_text)
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


def _round_has_actionable_findings(sections: Mapping[str, str]) -> bool:
    return any(
        has_actionable_content(sections.get(title, ""))
        for title in ("主要问题", "框架外问题", "零提示发散审")
    )


def _round_requires_followup(sections: Mapping[str, str]) -> bool:
    return any(
        has_actionable_content(sections.get(title, ""))
        for title in ("主要问题", "框架外问题")
    )


def _audit_round_handoff(
    *,
    series_id: str,
    current: ReviewRecord,
    next_record: ReviewRecord,
    current_sections: Mapping[str, str],
    next_sections: Mapping[str, str],
) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    if not _round_requires_followup(current_sections):
        return findings

    convergence_text = next_sections.get("收敛结论", "")
    carried_items = bullet_block_items(convergence_text, "carried_p0_p1")
    closed_items = bullet_block_items(convergence_text, "closed_items")
    if not carried_items and not closed_items:
        findings.append(
            ReviewAuditFinding(
                path=next_record.path,
                series_id=series_id,
                round=next_record.round,
                severity="P1",
                category="series_consistency",
                title="上一轮问题没有在下一轮闭环登记",
                detail=(
                    f"上一轮 `{current.path}` 仍有 actionable finding，"
                    "但这一轮的 `carried_p0_p1 / closed_items` 都为空，无法确认问题是继续携带还是已关闭。"
                ),
            )
        )
    return findings


def _audit_pass_convergence(series_id: str, latest: ReviewRecord, sections: Mapping[str, str]) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    if latest.status != "PASS":
        return findings

    if latest.converged and latest.converged != "是":
        findings.append(
            ReviewAuditFinding(
                path=latest.path,
                series_id=series_id,
                round=latest.round,
                severity="P1",
                category="series_consistency",
                title="PASS 记录没有显式收敛",
                detail="最新记录已标为 PASS，但 `本轮是否收敛` 不是“是”。",
            )
        )

    actionable = _round_has_actionable_findings(sections)
    if actionable:
        findings.append(
            ReviewAuditFinding(
                path=latest.path,
                series_id=series_id,
                round=latest.round,
                severity="P1",
                category="series_consistency",
                title="PASS 记录正文仍有 actionable finding",
                detail="最新记录已标为 PASS，但 `主要问题 / 框架外问题 / 零提示发散审` 里仍有需要回修的实质问题。",
            )
        )
        if latest.round == 1:
            findings.append(
                ReviewAuditFinding(
                    path=latest.path,
                    series_id=series_id,
                    round=latest.round,
                    severity="P1",
                    category="series_consistency",
                    title="单轮 PASS 缺少回修闭环",
                    detail="当前只有 round 1 就直接 PASS，但正文仍有 actionable finding，没有形成“修正 -> 再审”的闭环证据。",
                )
            )
    return findings


def _audit_series_consistency(
    series_id: str,
    records: List[ReviewRecord],
    sections_by_path: Mapping[str, Mapping[str, str]],
) -> List[ReviewAuditFinding]:
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

        if index > 0:
            findings.extend(
                _audit_round_handoff(
                    series_id=series_id,
                    current=records[index - 1],
                    next_record=record,
                    current_sections=sections_by_path.get(records[index - 1].path, {}),
                    next_sections=sections_by_path.get(record.path, {}),
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
    latest_sections = sections_by_path.get(latest.path, {})
    if _round_requires_followup(latest_sections):
        findings.append(
            ReviewAuditFinding(
                path=latest.path,
                series_id=series_id,
                round=latest.round,
                severity="P1",
                category="series_consistency",
                title="主要问题还没进入下一轮闭环",
                detail="最新一轮的 `主要问题 / 框架外问题` 仍有实质问题，但当前序列停在这一轮，尚未看到下一轮回修记录。",
            )
        )
    findings.extend(_audit_pass_convergence(series_id, latest, latest_sections))
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
    sections_by_path: Dict[str, Mapping[str, str]] = {}

    for record in audited_records:
        sections = canonicalize_sections(split_sections(Path(record.path).read_text(encoding="utf-8")))
        sections_by_path[record.path] = sections
        findings.extend(_audit_round_structure(record, sections))
        findings.extend(_audit_split_review_roles(record, sections))
        findings.extend(_audit_solidification(record, sections))
        findings.extend(_audit_manifest_factor_contract(record))
        findings.extend(_audit_manifest_proxy_contract(record))

    for series_id, series_records in _records_by_series(audited_records).items():
        findings.extend(_audit_series_consistency(series_id, series_records, sections_by_path))

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
