"""Inspect and surface pending catalyst web-review tasks."""

from __future__ import annotations

import argparse

from src.reporting.catalyst_review_queue import (
    build_catalyst_review_queue,
    next_pending_task,
    render_catalyst_review_queue_json,
    render_catalyst_review_queue_markdown,
)
from src.utils.config import resolve_project_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List and inspect pending catalyst web-review tasks.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List catalyst web-review tasks")
    list_parser.add_argument("--root", default="reports", help="Directory containing report sidecars")
    list_parser.add_argument("--status", default="all", choices=["all", "missing_review", "pending_template", "pending", "partial", "completed", "empty"], help="Optional status filter")
    list_parser.add_argument("--json-out", default="", help="Optional JSON output path")
    list_parser.add_argument("--markdown-out", default="", help="Optional Markdown output path")

    next_parser = subparsers.add_parser("next", help="Show the next pending catalyst web-review task")
    next_parser.add_argument("--root", default="reports", help="Directory containing report sidecars")
    next_parser.add_argument("--with-prompt", action="store_true", help="Also print the prompt markdown for the next task")

    return parser


def main() -> None:
    args = build_parser().parse_args()
    root = resolve_project_path(args.root)
    queue = build_catalyst_review_queue(root)

    if args.command == "list":
        markdown = render_catalyst_review_queue_markdown(queue, status_filter=args.status)
        print(markdown, end="")
        if args.json_out:
            target = resolve_project_path(args.json_out)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(render_catalyst_review_queue_json(queue), encoding="utf-8")
        if args.markdown_out:
            target = resolve_project_path(args.markdown_out)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(markdown, encoding="utf-8")
        return

    if args.command == "next":
        task = next_pending_task(queue)
        if not task:
            print("当前没有待处理的 catalyst web review 任务。")
            return
        lines = [
            "# Next Catalyst Web Review Task",
            "",
            f"- status: `{task['status']}`",
            f"- report_type: `{task['report_type']}`",
            f"- subject: `{task['subject']}`",
            f"- items: `{task['completed_items']}/{task['items']}`",
            f"- review: `{task['review_path']}`",
            f"- prompt: `{task['prompt_path']}`",
            f"- payload: `{task['payload_path']}`",
            "",
        ]
        if args.with_prompt:
            prompt_path = resolve_project_path(task["prompt_path"])
            if prompt_path.exists():
                lines.extend(["## Prompt", "", prompt_path.read_text(encoding="utf-8").rstrip()])
        print("\n".join(lines).rstrip())


if __name__ == "__main__":
    main()
