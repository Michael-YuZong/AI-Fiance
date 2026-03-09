"""Asset opportunity analysis command."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Dict, Tuple

from src.output import AnalysisChartRenderer, OpportunityReportRenderer
from src.processors.opportunity_engine import analyze_opportunity, build_market_context
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.logger import setup_logger


def run_scan(symbol: str, config_path: str = "") -> Tuple[str, Dict[str, object]]:
    config = load_config(config_path or None)
    setup_logger("ERROR")
    asset_type = detect_asset_type(symbol, config)
    context = build_market_context(config, relevant_asset_types=[asset_type, "cn_etf", "futures"])
    analysis = analyze_opportunity(symbol, asset_type, config, context=context)
    visuals = AnalysisChartRenderer().render(analysis)
    analysis["visuals"] = visuals
    report = OpportunityReportRenderer().render_scan(analysis, visuals=visuals)
    _persist_scan_report(symbol, report)
    return report, analysis


def _persist_scan_report(symbol: str, report: str) -> None:
    reports_dir = resolve_project_path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    filename = f"scan_{safe_symbol}_{datetime.now().strftime('%Y-%m-%d')}.md"
    path = reports_dir / filename
    path.write_text(report, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze a single asset with eight-dimensional opportunity scoring.")
    parser.add_argument("symbol", help="Asset symbol")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    report, _ = run_scan(args.symbol, args.config)
    print(report)


if __name__ == "__main__":
    main()
