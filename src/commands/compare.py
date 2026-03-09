"""Asset comparison command."""

from __future__ import annotations

import argparse

from src.output import OpportunityReportRenderer
from src.processors.opportunity_engine import compare_opportunities
from src.utils.config import load_config
from src.utils.logger import setup_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare multiple assets with the same eight-dimensional scoring engine.")
    parser.add_argument("symbols", nargs="+", help="Two or more asset symbols")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if len(args.symbols) < 2:
        raise SystemExit("compare 至少需要两个标的代码")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    payload = compare_opportunities(args.symbols[:2], config)
    print(OpportunityReportRenderer().render_compare(payload))


if __name__ == "__main__":
    main()
