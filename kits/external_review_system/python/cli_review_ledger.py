"""CLI wrapper for the portable external review ledger."""

from __future__ import annotations

import argparse
from pathlib import Path

from .review_ledger import build_review_ledger, render_review_ledger_json, render_review_ledger_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a structured index for external review rounds.")
    parser.add_argument("--root", default="reports/reviews", help="Directory containing round-based review markdown files")
    parser.add_argument("--json-out", default="", help="Optional JSON output path")
    parser.add_argument("--markdown-out", default="", help="Optional Markdown output path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    root = Path(args.root)
    ledger = build_review_ledger(root)
    markdown = render_review_ledger_markdown(ledger)
    print(markdown, end="")

    if args.json_out:
        target = Path(args.json_out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render_review_ledger_json(ledger), encoding="utf-8")

    if args.markdown_out:
        target = Path(args.markdown_out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(markdown, encoding="utf-8")


if __name__ == "__main__":
    main()
