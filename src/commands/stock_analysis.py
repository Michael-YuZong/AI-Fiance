"""Single-stock detailed analysis command."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple

from src.commands.final_runner import finalize_client_markdown
from src.commands.report_guard import ensure_report_task_registered, exported_bundle_lines
from src.commands.release_check import check_generic_client_report
from src.output import AnalysisChartRenderer, ClientReportRenderer, OpportunityReportRenderer
from src.processors.opportunity_engine import _attach_signal_confidence, analyze_opportunity, build_market_context
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.logger import setup_logger


def run_stock_analysis(symbol: str, config_path: str = "", today_mode: bool = False) -> Tuple[str, Dict[str, object]]:
    config = load_config(config_path or None)
    setup_logger("ERROR")
    asset_type = detect_asset_type(symbol, config)
    if asset_type not in {"cn_stock", "hk", "us"}:
        raise SystemExit(f"`{symbol}` 当前识别为 `{asset_type}`，不属于个股分析对象。")
    context = build_market_context(config, relevant_asset_types=[asset_type, "cn_stock", "hk", "us"])
    analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=today_mode)
    _attach_signal_confidence([analysis], config, limit=1)
    visuals = AnalysisChartRenderer().render(analysis)
    analysis["visuals"] = visuals
    report = OpportunityReportRenderer().render_scan(analysis, visuals=visuals)
    return report, analysis


def _client_output_path(symbol: str, generated_at: str) -> Path:
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    date_str = generated_at[:10]
    return resolve_project_path(f"reports/stock_analysis/final/stock_analysis_{safe_symbol}_{date_str}_final.md")


def _detail_output_path(symbol: str, generated_at: str) -> Path:
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    date_str = generated_at[:10]
    return resolve_project_path(f"reports/stock_analysis/internal/stock_analysis_{safe_symbol}_{date_str}_internal_detail.md")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a detailed analysis report for a single stock.")
    parser.add_argument("symbol", help="Stock symbol, e.g. 300750 / 0700.HK / META")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--today", action="store_true", help="Add intraday/today snapshot on top of the default daily scan.")
    parser.add_argument("--client-final", action="store_true", help="Render and persist customer-facing final markdown/pdf")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("stock_analysis")
    report, analysis = run_stock_analysis(args.symbol, args.config, today_mode=args.today)
    if not args.client_final:
        print(report)
        return

    detail_path = _detail_output_path(args.symbol, str(analysis.get("generated_at", "")))
    client_markdown = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    bundle = finalize_client_markdown(
        report_type="stock_analysis",
        client_markdown=client_markdown,
        markdown_path=_client_output_path(args.symbol, str(analysis.get("generated_at", ""))),
        detail_markdown=report,
        detail_path=detail_path,
        extra_manifest={
            "symbol": str(args.symbol),
            "asset_type": str(analysis.get("asset_type", "")),
        },
        release_checker=lambda markdown, source_text: check_generic_client_report(markdown, "stock_analysis", source_text=source_text),
    )
    print(client_markdown)
    for index, line in enumerate(exported_bundle_lines(bundle)):
        print(f"\n{line}" if index == 0 else line)


if __name__ == "__main__":
    main()
