"""Structured indexing for round-based external reviews."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from .review_record_utils import (
    clean_text,
    decision_from_sections,
    extract_link,
    normalize_status,
    normalize_yes_no,
    parse_bullet_mapping,
    round_from_text,
    series_id_for,
    split_sections,
    top_metadata,
)


@dataclass(frozen=True)
class ReviewRecord:
    path: str
    series_id: str
    title: str
    protocol: str
    round: int | None
    previous_round: int | None
    review_target: str
    review_target_ref: str
    review_prompt: str
    review_prompt_ref: str
    review_mode: str
    decision: str
    status: str
    new_p0_p1: str
    previous_round_closed: str
    converged: str
    recommend_continue: str
    allow_delivery: str
    allow_implementation: str
    sections: tuple[str, ...]
    metadata: Dict[str, str]
    convergence: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def parse_review_record(path: Path) -> ReviewRecord:
    text = path.read_text(encoding="utf-8")
    sections = split_sections(text)
    metadata = top_metadata(text)
    convergence = parse_bullet_mapping(sections.get("收敛结论", "").splitlines())
    title = ""
    for line in text.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            break

    review_target_label, review_target_ref = extract_link(metadata.get("审稿对象", metadata.get("review_target", "")))
    review_prompt_label, review_prompt_ref = extract_link(metadata.get("适用 prompt", metadata.get("review_prompt", "")))
    previous_round_value = convergence.get("previous_round", metadata.get("previous_round", ""))

    round_value = round_from_text(convergence.get("round", "")) or round_from_text(path.stem)
    previous_round = round_from_text(previous_round_value)
    decision = decision_from_sections(sections)
    has_structured_convergence = bool(sections.get("收敛结论", "").strip())
    if has_structured_convergence:
        protocol = "structured_round"
    elif round_value is not None:
        protocol = "legacy_round_note"
    else:
        protocol = "legacy_unstructured"

    return ReviewRecord(
        path=str(path),
        series_id=series_id_for(path),
        title=title,
        protocol=protocol,
        round=round_value,
        previous_round=previous_round,
        review_target=review_target_label or clean_text(metadata.get("审稿对象", metadata.get("review_target", ""))),
        review_target_ref=review_target_ref,
        review_prompt=review_prompt_label or clean_text(metadata.get("适用 prompt", metadata.get("review_prompt", ""))),
        review_prompt_ref=review_prompt_ref,
        review_mode=clean_text(metadata.get("审稿方式", "")),
        decision=decision,
        status=normalize_status(convergence.get("状态", "")),
        new_p0_p1=normalize_yes_no(convergence.get("本轮新增 P0/P1", "")),
        previous_round_closed=normalize_yes_no(convergence.get("上一轮 P0/P1 是否已关闭", "")),
        converged=normalize_yes_no(convergence.get("本轮是否收敛", "")),
        recommend_continue=normalize_yes_no(convergence.get("是否建议继续下一轮", "")),
        allow_delivery=normalize_yes_no(convergence.get("允许作为成稿交付", "")),
        allow_implementation=normalize_yes_no(convergence.get("是否允许开始实现", "")),
        sections=tuple(sections.keys()),
        metadata=dict(metadata),
        convergence=dict(convergence),
    )


def collect_review_records(root: Path) -> List[ReviewRecord]:
    if not root.exists():
        return []
    return [parse_review_record(path) for path in sorted(root.rglob("*.md"))]


def _latest_by_series(records: Iterable[ReviewRecord]) -> List[ReviewRecord]:
    latest: Dict[str, ReviewRecord] = {}
    for record in records:
        current = latest.get(record.series_id)
        current_round = current.round if current and current.round is not None else -1
        record_round = record.round if record.round is not None else -1
        if current is None or record_round >= current_round:
            latest[record.series_id] = record
    return sorted(latest.values(), key=lambda row: (row.series_id, row.round if row.round is not None else -1))


def build_review_ledger(root: Path) -> Dict[str, Any]:
    records = collect_review_records(root)
    latest_records = _latest_by_series(records)
    prompt_counts: Dict[str, int] = {}
    status_counts: Dict[str, int] = {}
    for row in latest_records:
        prompt_key = row.review_prompt_ref or row.review_prompt or "unknown"
        prompt_counts[prompt_key] = prompt_counts.get(prompt_key, 0) + 1
        status_key = row.status or "unknown"
        status_counts[status_key] = status_counts.get(status_key, 0) + 1

    active_loops = [
        row
        for row in latest_records
        if row.protocol == "structured_round"
        and (
            row.status == "BLOCKED"
            or row.recommend_continue == "是"
            or (row.converged and row.converged != "是")
            or (row.round is not None and row.status not in {"", "PASS"})
        )
    ]
    converged_series = [
        row
        for row in latest_records
        if row.protocol == "structured_round"
        and row.status == "PASS"
        and row.recommend_continue != "是"
        and (not row.converged or row.converged == "是")
        and row.round is not None
    ]

    return {
        "root": str(root),
        "summary": {
            "total_records": len(records),
            "total_series": len(latest_records),
            "latest_pass_series": sum(1 for row in latest_records if row.status == "PASS"),
            "latest_blocked_series": sum(1 for row in latest_records if row.status == "BLOCKED"),
            "converged_series": len(converged_series),
            "active_series": len(active_loops),
        },
        "latest_status_counts": status_counts,
        "prompt_usage_counts": dict(sorted(prompt_counts.items())),
        "latest_records": [row.to_dict() for row in latest_records],
        "active_loops": [row.to_dict() for row in active_loops],
        "records": [row.to_dict() for row in records],
    }


def render_review_ledger_markdown(ledger: Mapping[str, Any]) -> str:
    summary = ledger.get("summary", {})
    latest_records = ledger.get("latest_records", [])
    prompt_usage = ledger.get("prompt_usage_counts", {})
    active_loops = ledger.get("active_loops", [])

    lines = [
        "# External Review Ledger",
        "",
        f"- root: `{ledger.get('root', '')}`",
        f"- records: `{summary.get('total_records', 0)}`",
        f"- series: `{summary.get('total_series', 0)}`",
        f"- converged latest series: `{summary.get('converged_series', 0)}`",
        f"- active latest series: `{summary.get('active_series', 0)}`",
        "",
        "## Latest By Series",
        "",
        "| series | round | status | converged | continue | target |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in latest_records:
        lines.append(
            "| {series} | {round} | {status} | {converged} | {cont} | `{target}` |".format(
                series=row.get("series_id", ""),
                round=row.get("round", ""),
                status=row.get("status", "") or "unknown",
                converged=row.get("converged", "") or "-",
                cont=row.get("recommend_continue", "") or "-",
                target=row.get("review_target_ref", "") or row.get("review_target", ""),
            )
        )

    lines.extend(["", "## Prompt Usage", "", "| prompt | latest series |", "| --- | --- |"])
    for prompt, count in prompt_usage.items():
        lines.append(f"| `{prompt}` | {count} |")

    lines.extend(["", "## Active Loops", ""])
    if not active_loops:
        lines.append("- none")
    else:
        for row in active_loops:
            lines.append(
                "- `{series}` round `{round}` | status `{status}` | target `{target}`".format(
                    series=row.get("series_id", ""),
                    round=row.get("round", ""),
                    status=row.get("status", "") or "unknown",
                    target=row.get("review_target_ref", "") or row.get("review_target", ""),
                )
            )
    return "\n".join(lines).strip() + "\n"


def render_review_ledger_json(ledger: Mapping[str, Any]) -> str:
    return json.dumps(ledger, ensure_ascii=False, indent=2) + "\n"
