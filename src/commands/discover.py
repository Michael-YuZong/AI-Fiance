"""Opportunity discovery command."""

from __future__ import annotations

import argparse

from src.output import OpportunityReportRenderer
from src.processors.opportunity_engine import discover_opportunities
from src.utils.config import load_config
from src.utils.logger import setup_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan ETF/watchlist pools and surface multi-factor opportunities.")
    parser.add_argument("theme", nargs="?", default="", help="Optional theme filter, e.g. 科技 / 电网 / 黄金")
    parser.add_argument("--top", type=int, default=5, help="Number of candidates to show")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    setup_logger("ERROR")
    config = load_config(args.config or None)
    payload = discover_opportunities(config, top_n=args.top, theme_filter=args.theme.strip())
    print(OpportunityReportRenderer().render_discovery(payload))


if __name__ == "__main__":
    main()
