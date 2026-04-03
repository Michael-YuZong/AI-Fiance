"""CLI wrapper for the portable external review audit."""

from __future__ import annotations

import argparse
from pathlib import Path

from .review_audit import build_review_audit, render_review_audit_json, render_review_audit_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit round-based external review records.")
    parser.add_argument("--root", default="reports/reviews", help="Directory containing review markdown files")
    parser.add_argument("--json-out", default="", help="Optional JSON output path")
    parser.add_argument("--markdown-out", default="", help="Optional Markdown output path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    root = Path(args.root)
    audit = build_review_audit(root)
    markdown = render_review_audit_markdown(audit)
    print(markdown, end="")

    if args.json_out:
        target = Path(args.json_out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render_review_audit_json(audit), encoding="utf-8")

    if args.markdown_out:
        target = Path(args.markdown_out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(markdown, encoding="utf-8")


if __name__ == "__main__":
    main()
