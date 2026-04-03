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
_THEME_PLAYBOOK_CONTRACT_REPORT_TYPES = {"scan", "stock_analysis", "stock_pick", "etf_pick", "fund_pick", "briefing"}
_EVENT_DIGEST_CONTRACT_REPORT_TYPES = {"scan", "stock_analysis", "stock_pick", "etf_pick", "fund_pick", "briefing"}
_WHAT_CHANGED_CONTRACT_REPORT_TYPES = {"scan", "stock_analysis", "stock_pick", "etf_pick", "fund_pick", "briefing"}
_CATALYST_WEB_REVIEW_REPORT_TYPES = {"scan", "stock_analysis", "stock_pick", "etf_pick", "fund_pick", "briefing"}


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


def _audit_manifest_theme_playbook_contract(record: ReviewRecord) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    manifest_payload = _manifest_payload_for_record(record)
    if not manifest_payload:
        return findings
    report_type = str(manifest_payload.get("report_type", "")).strip()
    if report_type not in _THEME_PLAYBOOK_CONTRACT_REPORT_TYPES:
        return findings
    artifacts = dict(manifest_payload.get("artifacts") or {})
    contract = dict(artifacts.get("theme_playbook_contract") or {})
    if not contract:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="manifest 缺少 theme_playbook_contract",
                detail=f"`{report_type}` 的 manifest 已生成，但没有写入 theme_playbook_contract，无法在 review audit 里追踪主题边界、行业层 fallback 和细分观察合同。",
            )
        )
        return findings
    playbook_level = str(contract.get("playbook_level") or "").strip()
    label = str(contract.get("label") or "").strip()
    if playbook_level not in {"theme", "sector"} or not label:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="theme_playbook_contract 信息不完整",
                detail="manifest 已写入 theme_playbook_contract，但缺少 `playbook_level` 或 `label`，无法还原正文里的主题边界来源。",
            )
        )
    theme_match_status = str(contract.get("theme_match_status") or "").strip()
    theme_match_candidates = [
        str(item).strip()
        for item in list(contract.get("theme_match_candidates") or [])
        if str(item).strip()
    ]
    if playbook_level == "sector" and theme_match_status == "ambiguous_conflict" and not theme_match_candidates:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="theme_playbook_contract 缺少冲突候选主题",
                detail="manifest 把当前 playbook 标成 `ambiguous_conflict`，但没有写出 `theme_match_candidates`，后续无法核对正文为何退回行业层。",
            )
        )
    bridge_confidence = str(contract.get("subtheme_bridge_confidence") or "").strip()
    bridge_top_label = str(contract.get("subtheme_bridge_top_label") or "").strip()
    if bridge_confidence in {"high", "medium"} and not bridge_top_label:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="theme_playbook_contract 缺少下钻主线",
                detail="manifest 已声明行业层 bridge 置信度较高，但没有写出 `subtheme_bridge_top_label`，无法回看正文里的细分观察指向了什么。",
            )
        )
    return findings


def _audit_manifest_event_digest_contract(record: ReviewRecord) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    manifest_payload = _manifest_payload_for_record(record)
    if not manifest_payload:
        return findings
    report_type = str(manifest_payload.get("report_type", "")).strip()
    if report_type not in _EVENT_DIGEST_CONTRACT_REPORT_TYPES:
        return findings
    artifacts = dict(manifest_payload.get("artifacts") or {})
    contract = dict(artifacts.get("event_digest_contract") or {})
    if not contract:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="manifest 缺少 event_digest_contract",
                detail=f"`{report_type}` 的 manifest 已生成，但没有写入 event_digest_contract，无法在 review audit 里追踪事件消化状态、分层和研究解释。",
            )
        )
        return findings
    status = str(contract.get("status") or "").strip()
    changed_what = str(contract.get("changed_what") or "").strip()
    if status not in {"待补充", "待复核", "已消化"} or not changed_what:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="event_digest_contract 信息不完整",
                detail="manifest 已写入 event_digest_contract，但缺少合法 `status` 或 `changed_what`，无法回看这次事件到底改变了什么。",
            )
        )
    lead_layer = str(contract.get("lead_layer") or "").strip()
    if lead_layer not in {"财报", "公告", "政策", "新闻", "行业主题事件"}:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="event_digest_contract 缺少有效事件分层",
                detail="manifest 已写入 event_digest_contract，但 `lead_layer` 缺失或不在正式分层枚举中，后续无法回看系统到底把这次事件归到哪一层。",
            )
        )
    lead_detail = str(contract.get("lead_detail") or "").strip()
    impact_summary = str(contract.get("impact_summary") or "").strip()
    thesis_scope = str(contract.get("thesis_scope") or "").strip()
    importance_reason = str(contract.get("importance_reason") or "").strip()
    if any((lead_detail, impact_summary, thesis_scope, importance_reason)) and not all((lead_detail, impact_summary, thesis_scope, importance_reason)):
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="event_digest_contract 深度字段不完整",
                detail="manifest 已开始写入事件细分/影响层/事件性质/优先级判断，但这些深度字段没有同时落齐，后续无法稳定回看这次事件到底影响盈利、估值、景气还是资金偏好，以及为什么它该前置或先不升级。",
            )
        )
    return findings


def _audit_manifest_what_changed_contract(record: ReviewRecord) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    manifest_payload = _manifest_payload_for_record(record)
    if not manifest_payload:
        return findings
    report_type = str(manifest_payload.get("report_type", "")).strip()
    if report_type not in _WHAT_CHANGED_CONTRACT_REPORT_TYPES:
        return findings
    artifacts = dict(manifest_payload.get("artifacts") or {})
    contract = dict(artifacts.get("what_changed_contract") or {})
    if not contract:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="manifest 缺少 what_changed_contract",
                detail=f"`{report_type}` 的 manifest 已生成，但没有写入 what_changed_contract，无法在 review audit 里回看连续研究对“上次怎么看 / 这次什么变了 / 结论变化”的正式合同。",
            )
        )
        return findings
    previous_view = str(contract.get("previous_view") or "").strip()
    change_summary = str(contract.get("change_summary") or "").strip()
    conclusion_label = str(contract.get("conclusion_label") or "").strip()
    if not previous_view or not change_summary or not conclusion_label:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="what_changed_contract 信息不完整",
                detail="manifest 已写入 what_changed_contract，但缺少 `previous_view` / `change_summary` / `conclusion_label`，无法回看这次连续研究到底继承了什么旧判断、变化了什么、结论是否升级或降级。",
            )
        )
    current_event_understanding = str(contract.get("current_event_understanding") or "").strip()
    if not current_event_understanding:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="what_changed_contract 缺少当前事件理解",
                detail="manifest 已写入 what_changed_contract，但没有记录“当前事件理解”，后续无法回看这次变化更像什么事件、影响盈利/估值/景气/资金偏好的哪一层，以及它更像噪音还是 thesis 变化。",
            )
        )
    state_trigger = str(contract.get("state_trigger") or "").strip()
    if not state_trigger:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="what_changed_contract 缺少状态触发",
                detail="manifest 已写入 what_changed_contract，但没有记录这次为什么升级、削弱、维持或待复核；后续无法回看连续研究的状态机原因，只能看到结果标签。",
            )
        )
    state_summary = str(contract.get("state_summary") or "").strip()
    if state_trigger and not state_summary:
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P2",
                category="manifest_contract",
                title="what_changed_contract 缺少状态解释",
                detail="manifest 已写入状态触发，但没有记录状态解释；后续只能看到“为什么变了”的标签，看不到这次连续研究到底如何解释升级、削弱或待复核。",
            )
        )
    return findings


def _audit_manifest_catalyst_web_review(record: ReviewRecord) -> List[ReviewAuditFinding]:
    findings: List[ReviewAuditFinding] = []
    manifest_payload = _manifest_payload_for_record(record)
    if not manifest_payload:
        return findings
    report_type = str(manifest_payload.get("report_type", "")).strip()
    if report_type not in _CATALYST_WEB_REVIEW_REPORT_TYPES:
        return findings
    artifacts = dict(manifest_payload.get("artifacts") or {})
    editor_artifacts = dict(artifacts.get("editor_artifacts") or {})
    review_ref = str(editor_artifacts.get("catalyst_web_review") or "").strip()
    if not review_ref:
        return findings
    review_path = Path(review_ref)
    if not review_path.exists():
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="manifest_contract",
                title="manifest 指向的催化联网复核文件缺失",
                detail=f"`{report_type}` 的 manifest 指向了 `{review_path}`，但文件不存在。",
            )
        )
        return findings
    text = review_path.read_text(encoding="utf-8")
    if any(token in text for token in ("- 结论：待补", "\n- 待补\n")):
        findings.append(
            ReviewAuditFinding(
                path=record.path,
                series_id=record.series_id,
                round=record.round,
                severity="P1",
                category="manifest_contract",
                title="催化联网复核仍停留在待补模板",
                detail=f"`{review_path}` 仍然保留 `待补` 模板内容，说明 suspected_search_gap 尚未完成独立联网复核。",
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
        findings.extend(_audit_manifest_theme_playbook_contract(record))
        findings.extend(_audit_manifest_event_digest_contract(record))
        findings.extend(_audit_manifest_what_changed_contract(record))
        findings.extend(_audit_manifest_catalyst_web_review(record))

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
