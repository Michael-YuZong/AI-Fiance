"""Queue helpers for catalyst web-review sidecars."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping

from src.output.catalyst_web_review import (
    catalyst_web_review_has_completed_conclusion,
    is_catalyst_web_review_template,
    load_catalyst_web_review,
)


@dataclass(frozen=True)
class CatalystReviewTask:
    report_type: str
    subject: str
    generated_at: str
    items: int
    status: str
    payload_path: str
    prompt_path: str
    review_path: str
    completed_items: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _task_status(payload: Mapping[str, Any], review_path: Path) -> tuple[str, int]:
    items = list(payload.get("items") or [])
    if not items:
        return "empty", 0
    if not review_path.exists():
        return "missing_review", 0
    review_text = review_path.read_text(encoding="utf-8")
    if "当前没有命中 `待 AI 联网复核` 的条目。" in review_text:
        return "empty", 0
    if is_catalyst_web_review_template(review_text):
        return "pending_template", 0
    lookup = load_catalyst_web_review(review_path)
    completed_items = sum(1 for item in items if dict(lookup.get(str(item.get("symbol") or "").strip()) or {}).get("completed"))
    if completed_items >= len(items) and catalyst_web_review_has_completed_conclusion(review_text):
        return "completed", completed_items
    if completed_items > 0:
        return "partial", completed_items
    return "pending", 0


def build_catalyst_review_queue(root: Path) -> Dict[str, Any]:
    tasks: List[CatalystReviewTask] = []
    for payload_path in sorted(root.rglob("*_catalyst_web_review_payload.json")):
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        prompt_path = payload_path.with_name(payload_path.name.replace("_payload.json", "_prompt.md"))
        review_path = payload_path.with_name(payload_path.name.replace("_payload.json", ".md"))
        status, completed_items = _task_status(payload, review_path)
        tasks.append(
            CatalystReviewTask(
                report_type=str(payload.get("report_type") or "").strip(),
                subject=str(payload.get("subject") or "").strip(),
                generated_at=str(payload.get("generated_at") or "").strip(),
                items=len(list(payload.get("items") or [])),
                status=status,
                payload_path=str(payload_path),
                prompt_path=str(prompt_path),
                review_path=str(review_path),
                completed_items=completed_items,
            )
        )
    summary: Dict[str, int] = {}
    for task in tasks:
        summary[task.status] = summary.get(task.status, 0) + 1
    return {
        "root": str(root),
        "summary": {
            "total_tasks": len(tasks),
            "pending_tasks": sum(1 for task in tasks if task.status in {"missing_review", "pending_template", "pending", "partial"}),
            "status_counts": summary,
        },
        "tasks": [task.to_dict() for task in tasks],
    }


def render_catalyst_review_queue_markdown(queue: Mapping[str, Any], *, status_filter: str = "all") -> str:
    tasks = list(queue.get("tasks") or [])
    if status_filter != "all":
        tasks = [task for task in tasks if str(task.get("status")) == status_filter]
    lines = [
        "# Catalyst Web Review Queue",
        "",
        f"- root: `{queue.get('root', '')}`",
        f"- total_tasks: `{dict(queue.get('summary') or {}).get('total_tasks', 0)}`",
        f"- pending_tasks: `{dict(queue.get('summary') or {}).get('pending_tasks', 0)}`",
        "",
    ]
    if not tasks:
        lines.append("- 当前没有符合筛选条件的催化联网复核任务。")
        return "\n".join(lines)
    lines.extend(
        [
            "| status | report_type | subject | items | completed | review | prompt |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for task in tasks:
        lines.append(
            "| {status} | {report_type} | {subject} | {items} | {completed_items}/{items} | {review_path} | {prompt_path} |".format(
                **task
            )
        )
    return "\n".join(lines)


def render_catalyst_review_queue_json(queue: Mapping[str, Any]) -> str:
    return json.dumps(queue, ensure_ascii=False, indent=2)


def next_pending_task(queue: Mapping[str, Any]) -> Dict[str, Any] | None:
    for task in list(queue.get("tasks") or []):
        if str(task.get("status")) in {"missing_review", "pending_template", "pending", "partial"}:
            return dict(task)
    return None
