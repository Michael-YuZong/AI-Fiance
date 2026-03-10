"""Stock pick command — scan stock universe and surface top individual stock picks."""

from __future__ import annotations

import argparse

from src.output import OpportunityReportRenderer
from src.processors.opportunity_engine import discover_stock_opportunities
from src.utils.config import load_config
from src.utils.logger import setup_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan stock universe and surface top individual stock picks.")
    parser.add_argument("--market", default="all", choices=["cn", "hk", "us", "all"], help="Market scope: cn (A-share), hk, us, or all")
    parser.add_argument("--sector", default="", help="Sector filter, e.g. 科技 / 消费 / 医药")
    parser.add_argument("--top", type=int, default=20, help="Number of top picks to show")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    setup_logger("ERROR")
    config = load_config(args.config or None)
    payload = discover_stock_opportunities(config, top_n=args.top, market=args.market, sector_filter=args.sector.strip())
    print(OpportunityReportRenderer().render_stock_picks(payload))


if __name__ == "__main__":
    main()
