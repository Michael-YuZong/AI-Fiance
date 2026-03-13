"""Policy deep-dive command."""

from __future__ import annotations

import argparse

from src.output.policy_report import PolicyReportRenderer
from src.processors.policy_engine import PolicyEngine
from src.storage.portfolio import PortfolioRepository
from src.utils.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze a policy keyword or URL.")
    parser.add_argument("target", help="Policy keyword or URL")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    load_config(args.config or None)
    engine = PolicyEngine()
    try:
        context = engine.load_context(args.target)
    except Exception:
        context = engine.load_context(args.target if not args.target.startswith("http") else args.target.split("/")[-1])
    holdings = PortfolioRepository().list_holdings()
    payload = engine.analyze_context(context, holdings)
    print(PolicyReportRenderer().render(payload))


if __name__ == "__main__":
    main()
