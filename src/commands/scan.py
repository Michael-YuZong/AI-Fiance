"""Asset opportunity analysis command."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle
from src.commands.release_check import check_generic_client_report
from src.output import AnalysisChartRenderer, ClientReportRenderer, OpportunityReportRenderer
from src.processors.opportunity_engine import analyze_opportunity, build_market_context
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.logger import setup_logger


def run_scan(symbol: str, config_path: str = "", today_mode: bool = False) -> Tuple[str, Dict[str, object]]:
    config = load_config(config_path or None)
    setup_logger("ERROR")
    asset_type = detect_asset_type(symbol, config)
    context = build_market_context(config, relevant_asset_types=[asset_type, "cn_etf", "futures"])
    analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=today_mode)
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


def _client_output_path(symbol: str, asset_type: str, generated_at: str) -> Path:
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    date_str = generated_at[:10] or datetime.now().strftime("%Y-%m-%d")
    if asset_type == "cn_etf":
        base = resolve_project_path("reports/scans/etfs/final")
    elif asset_type == "cn_fund":
        base = resolve_project_path("reports/scans/funds/final")
    else:
        base = resolve_project_path("reports/scans/final")
    return base / f"scan_{safe_symbol}_{date_str}_client_final.md"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze a single asset with eight-dimensional opportunity scoring.")
    parser.add_argument("symbol", help="Asset symbol")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--today", action="store_true", help="Add intraday/today snapshot on top of the default daily scan.")
    parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("scan")
    report, analysis = run_scan(args.symbol, args.config, today_mode=args.today)
    if not args.client_final:
        print(report)
        return

    client_markdown = ClientReportRenderer().render_scan(analysis)
    findings = check_generic_client_report(client_markdown, "scan")
    try:
        bundle = export_reviewed_markdown_bundle(
            report_type="scan",
            markdown_text=client_markdown,
            markdown_path=_client_output_path(args.symbol, str(analysis.get("asset_type", "")), str(analysis.get("generated_at", ""))),
            release_findings=findings,
            extra_manifest={
                "symbol": str(args.symbol),
                "asset_type": str(analysis.get("asset_type", "")),
            },
        )
    except ReportGuardError as exc:
        raise SystemExit(str(exc))
    print(client_markdown)
    print(f"\n[client markdown] {bundle['markdown']}")
    print(f"[client pdf] {bundle['pdf']}")


if __name__ == "__main__":
    main()
