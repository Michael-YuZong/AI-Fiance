"""Risk management command."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from src.output.risk_report import RiskReportRenderer
from src.processors.risk import RiskAnalyzer
from src.processors.risk_support import (
    build_portfolio_risk_context,
    find_stress_scenario,
    load_stress_scenarios,
    resolve_stress_scenario,
)
from src.storage.portfolio import PortfolioRepository
from src.utils.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Portfolio risk management command.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    report_parser = subparsers.add_parser("report", help="Generate a full portfolio risk report")
    report_parser.add_argument("--period", default="3y", help="Risk lookback, e.g. 3y or 12m")

    correlation_parser = subparsers.add_parser("correlation", help="Show holdings correlation matrix")
    correlation_parser.add_argument("--period", default="3y", help="Risk lookback, e.g. 3y or 12m")

    stress_parser = subparsers.add_parser("stress", help="Run a predefined stress scenario")
    stress_parser.add_argument("scenario", help='Scenario name, e.g. "美股崩盘"')
    stress_parser.add_argument("--period", default="3y", help="Risk lookback, e.g. 3y or 12m")

    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _format_pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _build_correlation_payload(matrix: pd.DataFrame) -> tuple[List[str], List[List[str]]]:
    if matrix.empty:
        return [], []
    headers = ["标的"] + [str(column) for column in matrix.columns]
    rows: List[List[str]] = []
    for index, row in matrix.iterrows():
        rows.append([str(index)] + [f"{float(value):+.2f}" for value in row.values])
    return headers, rows


def _build_limit_alerts(status: Dict[str, Any], report: Dict[str, Any], config: Dict[str, Any]) -> List[str]:
    risk_limits = config.get("risk_limits", {})
    alerts: List[str] = []

    single_position_max = float(risk_limits.get("single_position_max", 0.30))
    single_sector_max = float(risk_limits.get("single_sector_max", 0.40))
    single_region_max = float(risk_limits.get("single_region_max", 0.50))
    portfolio_beta_max = float(risk_limits.get("portfolio_beta_max", 1.5))

    for row in status.get("holdings", []):
        if float(row["weight"]) > single_position_max:
            alerts.append(
                f"{row['symbol']} 仓位 {row['weight'] * 100:.1f}% 超过单一标的上限 {single_position_max * 100:.0f}%。"
            )
    for sector, weight in status.get("sector_exposure", {}).items():
        if float(weight) > single_sector_max:
            alerts.append(f"行业 {sector} 暴露 {weight * 100:.1f}% 超过上限 {single_sector_max * 100:.0f}%。")
    for region, weight in status.get("region_exposure", {}).items():
        if float(weight) > single_region_max:
            alerts.append(f"地区 {region} 暴露 {weight * 100:.1f}% 超过上限 {single_region_max * 100:.0f}%。")
    if float(report.get("beta", {}).get("beta", 0.0)) > portfolio_beta_max:
        alerts.append(
            f"组合 Beta {report['beta']['beta']:.2f} 超过上限 {portfolio_beta_max:.2f}，需考虑降低风险暴露。"
        )
    return alerts


def _summary_lines(context: Any) -> List[str]:
    status = context.status
    holdings = status.get("holdings", [])
    if not holdings:
        return ["当前没有持仓记录。"]
    return [
        f"总市值约 {status['total_value']:.2f} {status['base_currency']}，共 {len(holdings)} 个持仓。",
        "地区暴露: "
        + " / ".join(
            f"{region} {weight * 100:.1f}%"
            for region, weight in sorted(status.get("region_exposure", {}).items(), key=lambda item: item[1], reverse=True)
        ),
        "行业暴露: "
        + " / ".join(
            f"{sector} {weight * 100:.1f}%"
            for sector, weight in sorted(status.get("sector_exposure", {}).items(), key=lambda item: item[1], reverse=True)
        ),
    ]


def _build_metric_lines(report: Dict[str, Any]) -> List[str]:
    rolling = report.get("rolling_volatility", {})
    sharpe = report.get("sharpe", {})
    return [
        report["max_drawdown"]["interpretation"],
        report["var_95"]["interpretation"],
        report["var_99"]["interpretation"],
        report["cvar_95"]["interpretation"],
        f"20 日年化波动约 {rolling.get('vol_20d', 0.0) * 100:.2f}%，60 日约 {rolling.get('vol_60d', 0.0) * 100:.2f}%。",
        f"夏普比率 {sharpe.get('sharpe', 0.0):.2f}，年化收益 {sharpe.get('annual_return', 0.0) * 100:.2f}%，年化波动 {sharpe.get('annual_vol', 0.0) * 100:.2f}%。",
        report["beta"]["interpretation"],
    ]


def _build_stress_payload(context: Any, analyzer: RiskAnalyzer, config: Dict[str, Any], scenario_name: str) -> Dict[str, Any]:
    scenarios = load_stress_scenarios(config)
    scenario = find_stress_scenario(scenario_name, scenarios)
    if scenario is None:
        raise ValueError(
            "未找到该压力场景，可用场景: " + " / ".join(str(item.get("name", "")) for item in scenarios)
        )

    resolved = resolve_stress_scenario(scenario, context.status.get("holdings", []), config)
    if not resolved["shocks"]:
        raise ValueError("该场景没有映射到当前持仓，无法做组合压力估算。")

    result = analyzer.stress_test({"name": resolved["name"], "shocks": resolved["shocks"]})
    stress_rows: List[List[str]] = []
    for symbol, item in result.get("asset_breakdown", {}).items():
        stress_rows.append(
            [
                symbol,
                f"{item['weight'] * 100:.1f}%",
                _format_pct(float(item["shock"])),
                _format_pct(float(item["contribution"])),
                resolved["mappings"].get(symbol, symbol),
            ]
        )

    return {
        "title": f"压力测试: {resolved['name']}",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary_lines": _summary_lines(context),
        "metric_lines": [],
        "limit_alerts": [],
        "concentration_alerts": [],
        "correlation_headers": [],
        "correlation_rows": [],
        "stress_lines": [
            str(scenario.get("description", "")),
            result["interpretation"],
            "原始冲击设定: "
            + " / ".join(f"{key}={_format_pct(float(value))}" for key, value in scenario.get("shocks", {}).items()),
        ],
        "stress_rows": stress_rows,
        "coverage_notes": context.coverage_notes,
    }


def _build_full_payload(context: Any, report: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    correlation_headers, correlation_rows = _build_correlation_payload(pd.DataFrame(report["correlation"]))
    concentration_alerts = [item["warning"] for item in report.get("concentration_alerts", [])]
    return {
        "title": "组合风险报告",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary_lines": _summary_lines(context),
        "metric_lines": _build_metric_lines(report),
        "limit_alerts": _build_limit_alerts(context.status, report, config),
        "concentration_alerts": concentration_alerts,
        "correlation_headers": correlation_headers,
        "correlation_rows": correlation_rows,
        "stress_lines": [],
        "stress_rows": [],
        "coverage_notes": context.coverage_notes,
    }


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    context = build_portfolio_risk_context(config, repo=PortfolioRepository(), period=getattr(args, "period", "3y"))

    if not context.status.get("holdings"):
        print("# 组合风险")
        print("")
        print("当前没有持仓记录。")
        return

    if not context.weights:
        print("# 组合风险")
        print("")
        print("持仓存在，但没有足够历史数据完成风险测算。")
        for note in context.coverage_notes:
            print(f"- {note}")
        return

    analyzer = RiskAnalyzer(context.returns_df[list(context.weights)], context.weights)

    if args.subcommand == "stress":
        payload = _build_stress_payload(context, analyzer, config, args.scenario)
        print(RiskReportRenderer().render(payload))
        return

    report = analyzer.generate_risk_report(context.benchmark_returns)
    if args.subcommand == "correlation":
        correlation_headers, correlation_rows = _build_correlation_payload(pd.DataFrame(report["correlation"]))
        payload = {
            "title": "持仓相关性",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary_lines": _summary_lines(context),
            "metric_lines": [],
            "limit_alerts": [],
            "concentration_alerts": [item["warning"] for item in report.get("concentration_alerts", [])],
            "correlation_headers": correlation_headers,
            "correlation_rows": correlation_rows,
            "stress_lines": [],
            "stress_rows": [],
            "coverage_notes": context.coverage_notes,
        }
        print(RiskReportRenderer().render(payload))
        return

    payload = _build_full_payload(context, report, config)
    print(RiskReportRenderer().render(payload))


if __name__ == "__main__":
    main()
