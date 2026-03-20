"""Portfolio command."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from src.commands.release_check import check_generic_client_report
from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle
from src.output import DecisionRetrospectReportRenderer
from src.processors.factor_meta import summarize_factor_contracts_from_analysis
from src.processors.horizon import build_trade_plan_horizon
from src.processors.opportunity_engine import analyze_opportunity, build_market_context, summarize_proxy_contracts_from_analyses
from src.processors.portfolio_actions import (
    build_trade_decision_snapshot,
    build_trade_plan,
    estimate_execution_profile,
)
from src.processors.decision_review import build_monthly_decision_review
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.market import compute_history_metrics, fetch_asset_history, get_asset_context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Portfolio management command.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    subparsers.add_parser("status", help="Show current portfolio status")

    log_parser = subparsers.add_parser("log", help="Record a buy or sell trade by amount")
    log_parser.add_argument("action", choices=["buy", "sell"])
    log_parser.add_argument("symbol")
    log_parser.add_argument("price", type=float)
    log_parser.add_argument("amount", type=float, help="Trade amount in currency")
    log_parser.add_argument("--name", default="")
    log_parser.add_argument("--asset-type", default="")
    log_parser.add_argument("--basis", choices=["rule", "subjective", "emergency"], default="rule")
    log_parser.add_argument("--note", default="")

    whatif_parser = subparsers.add_parser("whatif", help="Preview portfolio impact before a trade")
    whatif_parser.add_argument("action", choices=["buy", "sell"])
    whatif_parser.add_argument("symbol")
    whatif_parser.add_argument("price", type=float)
    whatif_parser.add_argument("amount", type=float, help="Trade amount in currency")
    whatif_parser.add_argument("--asset-type", default="")
    whatif_parser.add_argument("--period", default="3y", help="Risk lookback, e.g. 3y or 12m")

    target_parser = subparsers.add_parser("set-target", help="Set target weight for a holding")
    target_parser.add_argument("symbol")
    target_parser.add_argument("weight", type=float)

    rebalance_parser = subparsers.add_parser("rebalance", help="Suggest rebalance actions")
    rebalance_parser.add_argument("--threshold", type=float, default=0.05)

    thesis_parser = subparsers.add_parser("thesis", help="Manage investment thesis")
    thesis_subparsers = thesis_parser.add_subparsers(dest="thesis_command", required=True)
    thesis_subparsers.add_parser("list", help="List all thesis records")
    thesis_get_parser = thesis_subparsers.add_parser("get", help="Get thesis by symbol")
    thesis_get_parser.add_argument("symbol")
    thesis_set_parser = thesis_subparsers.add_parser("set", help="Create or update thesis")
    thesis_set_parser.add_argument("symbol")
    thesis_set_parser.add_argument("--core", required=True)
    thesis_set_parser.add_argument("--validation", required=True)
    thesis_set_parser.add_argument("--stop", required=True)
    thesis_set_parser.add_argument("--period", required=True)
    thesis_check_parser = thesis_subparsers.add_parser("check", help="Check thesis health")
    thesis_check_parser.add_argument("symbol", nargs="?")
    thesis_delete_parser = thesis_subparsers.add_parser("delete", help="Delete thesis")
    thesis_delete_parser.add_argument("symbol")

    review_parser = subparsers.add_parser("review", help="Review monthly decisions")
    review_parser.add_argument("month", help="Month in YYYY-MM format")
    review_parser.add_argument("--symbol", default="", help="Optional symbol filter")
    review_parser.add_argument("--lookahead", type=int, default=20, help="Forward review window in trading days")
    review_parser.add_argument("--stop-pct", type=float, default=0.08, help="Standard stop loss percent for retrospective scoring")
    review_parser.add_argument("--target-pct", type=float, default=0.15, help="Standard target percent for retrospective scoring")
    review_parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")

    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _load_latest_prices(config: dict, repo: PortfolioRepository) -> dict:
    prices = {}
    for holding in repo.list_holdings():
        try:
            history = fetch_asset_history(holding["symbol"], holding["asset_type"], config)
            prices[holding["symbol"]] = compute_history_metrics(history)["last_close"]
        except Exception:
            prices[holding["symbol"]] = float(holding.get("cost_basis", 0.0))
    return prices


def _status_lines(status: dict) -> List[str]:
    lines = [f"# 组合状态", "", f"- 总市值: `{status['total_value']:.2f} {status['base_currency']}`", ""]
    if not status["holdings"]:
        lines.append("当前没有持仓记录。")
        return lines

    lines.extend(["## 持仓", "", "| 标的 | 数量 | 成本 | 最新价 | 市值 | 权重 | 浮盈亏 |", "| --- | --- | --- | --- | --- | --- | --- |"])
    for row in status["holdings"]:
        lines.append(
            f"| {row['symbol']} ({row['name']}) | {row['quantity']:.4f} | {row['cost_basis']:.4f} | "
            f"{row['latest_price']:.4f} | {row['market_value']:.2f} | {row['weight'] * 100:.1f}% | {row['pnl']:+.2f} |"
        )
    lines.extend(["", "## 暴露", ""])
    for region, weight in sorted(status["region_exposure"].items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- 地区 {region}: {weight * 100:.1f}%")
    for sector, weight in sorted(status["sector_exposure"].items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- 行业 {sector}: {weight * 100:.1f}%")
    return lines


def _trade_signal_snapshot(symbol: str, asset_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    try:
        history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
        metrics = compute_history_metrics(history)
        technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
        dmi = dict(technical.get("dmi") or {})
        volume = dict(technical.get("volume") or {})
        return {
            "return_20d": metrics["return_20d"],
            "price_percentile_1y": metrics["price_percentile_1y"],
            "ma_signal": technical["ma_system"]["signal"],
            "macd_signal": technical["macd"]["signal"],
            "rsi": dict(technical.get("rsi") or {}).get("RSI"),
            "adx": dmi.get("ADX"),
            "plus_di": dmi.get("DI+"),
            "minus_di": dmi.get("DI-"),
            "volume_signal": volume.get("signal"),
            "volume_structure": volume.get("structure"),
        }
    except Exception:
        return {}


def _trade_logging_snapshots(
    *,
    action: str,
    symbol: str,
    asset_type: str,
    price: float,
    amount: float,
    config: Dict[str, Any],
    thesis_repo: ThesisRepository,
) -> Dict[str, Dict[str, Any]]:
    thesis_snapshot = dict(thesis_repo.get(symbol) or {})
    try:
        history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
        metrics = compute_history_metrics(history)
        technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
        dmi = dict(technical.get("dmi") or {})
        volume = dict(technical.get("volume") or {})
        signal_snapshot = {
            "return_20d": metrics["return_20d"],
            "price_percentile_1y": metrics["price_percentile_1y"],
            "ma_signal": technical["ma_system"]["signal"],
            "macd_signal": technical["macd"]["signal"],
            "rsi": dict(technical.get("rsi") or {}).get("RSI"),
            "adx": dmi.get("ADX"),
            "plus_di": dmi.get("DI+"),
            "minus_di": dmi.get("DI-"),
            "volume_signal": volume.get("signal"),
            "volume_structure": volume.get("structure"),
        }
        factor_contract: Dict[str, Any] = {}
        proxy_contract: Dict[str, Any] = {}
        try:
            context = build_market_context(config, relevant_asset_types=[asset_type])
            analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=False)
            factor_contract = summarize_factor_contracts_from_analysis(analysis)
            proxy_contract = summarize_proxy_contracts_from_analyses(
                [analysis],
                market_proxy=dict(dict(analysis.get("proxy_signals") or {}).get("market_flow") or {}),
            )
        except Exception:
            factor_contract = {}
            proxy_contract = {}
        decision_snapshot = build_trade_decision_snapshot(
            symbol=symbol,
            asset_type=asset_type,
            config=config,
            history=history,
            thesis=thesis_snapshot,
            factor_contract=factor_contract,
            proxy_contract=proxy_contract,
        )
        execution_snapshot = estimate_execution_profile(
            asset_type=asset_type,
            amount=amount,
            price=price,
            metrics=metrics,
            risk_limits=config.get("risk_limits", {}),
        )
        decision_snapshot["horizon"] = build_trade_plan_horizon(
            thesis=thesis_snapshot,
            action=action,
            projected_weight=0.0,
            suggested_max_weight=1.0,
            execution=execution_snapshot,
            signal_snapshot=signal_snapshot,
        )
    except Exception as exc:
        signal_snapshot = {}
        decision_snapshot = {
            "recorded_at": datetime.now().isoformat(timespec="seconds"),
            "market_data_as_of": "",
            "market_data_source": symbol,
            "history_window": "3y",
            "thesis_snapshot_at": str(thesis_snapshot.get("updated_at") or thesis_snapshot.get("created_at") or ""),
            "notes": [f"行情/技术快照获取失败，当前只保留了最基础的交易记录：{exc}"],
            "horizon": build_trade_plan_horizon(
                thesis=thesis_snapshot,
                action=action,
                projected_weight=0.0,
                suggested_max_weight=1.0,
                execution={},
                signal_snapshot={},
            ),
        }
        execution_snapshot = {
            "execution_mode": "未知",
            "tradability_label": "数据不足",
            "avg_turnover_20d": 0.0,
            "participation_rate": None,
            "slippage_bps": 0.0,
            "estimated_slippage_cost": 0.0,
            "fee_rate": 0.0,
            "estimated_fee_cost": 0.0,
            "estimated_total_cost": 0.0,
            "quantity": amount / price if price else 0.0,
            "liquidity_note": "行情历史不可用，未能估算可成交性和执行成本。",
            "execution_note": "后续复盘时只能看结果路径，不能据此评估当时的冲击成本。",
            "max_participation_limit": float(config.get("risk_limits", {}).get("max_trade_participation", 0.05)),
        }

    return {
        "signal_snapshot": signal_snapshot,
        "thesis_snapshot": thesis_snapshot,
        "decision_snapshot": decision_snapshot,
        "execution_snapshot": execution_snapshot,
    }


def _whatif_lines(plan: Dict[str, Any]) -> List[str]:
    execution = dict(plan.get("execution") or {})
    decision = dict(plan.get("decision_snapshot") or {})
    alerts = list(plan.get("alerts") or [])
    participation = execution.get("participation_rate")
    participation_text = "—" if participation is None else f"{float(participation) * 100:.2f}%"
    return [
        "# 交易预演",
        "",
        f"- 动作: `{plan['action']}`",
        f"- 标的: `{plan['symbol']}` ({plan['name']})",
        f"- 预演金额: `{plan['amount']:.2f}`",
        f"- 假设成交价: `{plan['price']:.4f}`",
        "",
        "## 一句话结论",
        f"- {plan['headline']}",
        "",
        "## 周期判断",
        f"- 当前更适合按 `{dict(plan.get('horizon') or {}).get('label', '观察期')}` 理解。",
        f"- 为什么按这个周期看: {dict(plan.get('horizon') or {}).get('fit_reason', '当前周期未单独标注。')}",
        f"- 现在不适合: {dict(plan.get('horizon') or {}).get('misfit_reason', '当前不建议自动切到另一种更长或更短的打法。')}",
        "",
        "## 组合与风险预算",
        f"- 当前权重约 `{plan['current_weight'] * 100:.1f}%`，预演后约 `{plan['projected_weight'] * 100:.1f}%`。",
        f"- 当前更合理的单票上限约为 `{plan['suggested_max_weight'] * 100:.1f}%`。",
        f"- 行业 `{plan['current_sector']}` 预演后暴露约 `{plan['projected_sector_weight'] * 100:.1f}%`。",
        f"- 地区 `{plan['current_region']}` 预演后暴露约 `{plan['projected_region_weight'] * 100:.1f}%`。",
        f"- 组合年化波动预估: `{plan['current_risk']['annual_vol'] * 100:.2f}% -> {plan['projected_risk']['annual_vol'] * 100:.2f}%`。",
        f"- 组合 Beta 预估: `{plan['current_risk']['beta']:.2f} -> {plan['projected_risk']['beta']:.2f}`。",
        "",
        "## 执行成本与可成交性",
        f"- 执行模式: `{execution.get('execution_mode', '—')}`，可成交性: `{execution.get('tradability_label', '—')}`。",
        f"- 近20日日均成交额约 `{float(execution.get('avg_turnover_20d', 0.0)) / 1e8:.2f} 亿`，订单参与率约 `{participation_text}`。",
        f"- 预估滑点 `{float(execution.get('slippage_bps', 0.0)):.1f} bps`，滑点成本约 `{float(execution.get('estimated_slippage_cost', 0.0)):.2f}`。",
        f"- 显性费用率约 `{float(execution.get('fee_rate', 0.0)) * 100:.2f}%`，费用约 `{float(execution.get('estimated_fee_cost', 0.0)):.2f}`。",
        f"- 预估总成本约 `{float(execution.get('estimated_total_cost', 0.0)):.2f}`。",
        f"- {execution.get('liquidity_note', '')}",
        f"- {execution.get('execution_note', '')}",
        "",
        "## 时点与证据快照",
        f"- 记录时间: `{decision.get('recorded_at', '—')}`",
        f"- 行情 as_of: `{decision.get('market_data_as_of', '—')}`",
        f"- 行情来源: `{decision.get('market_data_source', '—')}`",
        f"- 历史窗口: `{decision.get('history_window', '—')}`",
        f"- Thesis 快照时间: `{decision.get('thesis_snapshot_at') or '—'}`",
        "",
        "## 风险提示",
    ] + [f"- {item}" for item in alerts or ["当前没有触发新的硬约束告警。"]] + ["", "## 备注"] + [
        f"- {item}" for item in (decision.get("notes", []) or [])
    ]


def _review_output_path(month: str, symbol: str = "") -> Path:
    safe_month = str(month).replace("/", "-")
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    stem = f"portfolio_review_{safe_month}"
    if safe_symbol:
        stem += f"_{safe_symbol}"
    return resolve_project_path("reports/retrospects/final") / f"{stem}_final.md"


def _review_detail_output_path(month: str, symbol: str = "") -> Path:
    safe_month = str(month).replace("/", "-")
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    stem = f"portfolio_review_{safe_month}"
    if safe_symbol:
        stem += f"_{safe_symbol}"
    return resolve_project_path("reports/retrospects/internal") / f"{stem}_internal_detail.md"


def _evaluate_thesis(symbol: str, record: Dict[str, Any], repo: PortfolioRepository, config: Dict[str, Any]) -> Dict[str, Any]:
    holdings = {item["symbol"]: item for item in repo.list_holdings()}
    asset_type = holdings.get(symbol, {}).get("asset_type") or detect_asset_type(symbol, config)
    history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
    metrics = compute_history_metrics(history)
    technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
    latest = metrics["last_close"]
    cost_basis = float(holdings.get(symbol, {}).get("cost_basis", latest))

    if technical["ma_system"]["signal"] == "bearish" and metrics["return_20d"] < -0.08:
        status = "broken"
        reason = "趋势转弱且近 20 日回撤较深，需要重新验证原始假设。"
    elif technical["ma_system"]["signal"] == "bearish" or latest < cost_basis * 0.95:
        status = "warning"
        reason = "价格或趋势开始偏离原假设，建议复查验证指标。"
    else:
        status = "intact"
        reason = "当前价格与趋势尚未明显破坏原始 thesis。"

    return {
        "symbol": symbol,
        "status": status,
        "reason": reason,
        "latest_price": latest,
        "return_20d": metrics["return_20d"],
        "record": record,
    }


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    repo = PortfolioRepository()
    thesis_repo = ThesisRepository()

    if args.subcommand == "status":
        status = repo.build_status(_load_latest_prices(config, repo))
        print("\n".join(_status_lines(status)))
        return

    if args.subcommand == "log":
        asset_type = args.asset_type or detect_asset_type(args.symbol, config)
        context = get_asset_context(args.symbol, asset_type, config)
        snapshots = _trade_logging_snapshots(
            action=args.action,
            symbol=args.symbol,
            asset_type=asset_type,
            price=args.price,
            amount=args.amount,
            config=config,
            thesis_repo=thesis_repo,
        )
        repo.log_trade(
            action=args.action,
            symbol=args.symbol,
            name=args.name or context.name,
            asset_type=asset_type,
            price=args.price,
            amount=args.amount,
            region=context.metadata.get("region", ""),
            sector=context.metadata.get("sector", ""),
            basis=args.basis,
            note=args.note,
            signal_snapshot=snapshots["signal_snapshot"],
            thesis_snapshot=snapshots["thesis_snapshot"],
            decision_snapshot=snapshots["decision_snapshot"],
            execution_snapshot=snapshots["execution_snapshot"],
        )
        print(f"已记录 {args.action} {args.symbol}，成交金额 {args.amount:.2f}。")
        return

    if args.subcommand == "whatif":
        payload = build_trade_plan(
            action=args.action,
            symbol=args.symbol,
            price=args.price,
            amount=args.amount,
            config=config,
            asset_type=args.asset_type,
            repo=repo,
            thesis_repo=thesis_repo,
            period=args.period,
        )
        print("\n".join(_whatif_lines(payload)))
        return

    if args.subcommand == "set-target":
        repo.set_target_weight(args.symbol, args.weight)
        print(f"已设置 {args.symbol} 目标权重为 {args.weight * 100:.1f}%。")
        return

    if args.subcommand == "rebalance":
        suggestions = repo.rebalance_suggestions(_load_latest_prices(config, repo), threshold=args.threshold)
        print("# 再平衡建议")
        print("")
        if not suggestions:
            print("当前没有超过阈值的偏离。")
            return
        for item in suggestions:
            print(
                f"- {item['symbol']}: 当前 {item['current_weight'] * 100:.1f}% / 目标 {item['target_weight'] * 100:.1f}%，"
                f"建议{item['action']}约 {item['amount']:.2f}。"
            )
        return

    if args.subcommand == "thesis":
        if args.thesis_command == "list":
            records = thesis_repo.list_all()
            print("# Thesis 列表")
            print("")
            if not records:
                print("当前没有 thesis 记录。")
                return
            for item in records:
                print(f"- {item['symbol']}: {item['core_assumption']}")
            return

        if args.thesis_command == "get":
            record = thesis_repo.get(args.symbol)
            print(f"# Thesis: {args.symbol}")
            print("")
            if not record:
                print("未找到该标的的 thesis。")
                return
            print(f"- 核心假设: {record['core_assumption']}")
            print(f"- 验证指标: {record['validation_metric']}")
            print(f"- 止损条件: {record['stop_condition']}")
            print(f"- 预期周期: {record['holding_period']}")
            print(f"- 创建日期: {record['created_at']}")
            return

        if args.thesis_command == "set":
            record = thesis_repo.upsert(
                symbol=args.symbol,
                core_assumption=args.core,
                validation_metric=args.validation,
                stop_condition=args.stop,
                holding_period=args.period,
            )
            print(f"已更新 {record['symbol']} 的 thesis。")
            return

        if args.thesis_command == "delete":
            deleted = thesis_repo.delete(args.symbol)
            print("已删除。" if deleted else "未找到该 thesis。")
            return

        if args.thesis_command == "check":
            if args.symbol:
                record = thesis_repo.get(args.symbol)
                records = [{**record, "symbol": args.symbol}] if record else []
            else:
                records = thesis_repo.list_all()
            print("# Thesis 健康检查")
            print("")
            if not records:
                print("当前没有 thesis 记录。")
                return
            for item in records:
                result = _evaluate_thesis(item["symbol"], item, repo, config)
                icon = {"intact": "✅", "warning": "⚠️", "broken": "❌"}[result["status"]]
                print(
                    f"- {result['symbol']}: {icon} {result['reason']} 近20日={result['return_20d'] * 100:+.2f}%，"
                    f"最新价={result['latest_price']:.3f}。"
                )
            return

    if args.subcommand == "review":
        ensure_report_task_registered("retrospect")
        payload = build_monthly_decision_review(
            args.month,
            config=config,
            symbol=args.symbol,
            lookahead=args.lookahead,
            stop_pct=args.stop_pct,
            target_pct=args.target_pct,
            repo=repo,
            thesis_repo=thesis_repo,
        )
        markdown = DecisionRetrospectReportRenderer().render(payload)
        if not args.client_final:
            print(markdown)
            return
        detail_path = _review_detail_output_path(args.month, args.symbol)
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_path.write_text(markdown, encoding="utf-8")
        findings = check_generic_client_report(markdown, "retrospect", source_text=markdown)
        try:
            bundle = export_reviewed_markdown_bundle(
                report_type="retrospect",
                markdown_text=markdown,
                markdown_path=_review_output_path(args.month, args.symbol),
                release_findings=findings,
                extra_manifest={
                    "month": args.month,
                    "symbol": args.symbol,
                    "lookahead": args.lookahead,
                    "stop_pct": args.stop_pct,
                    "target_pct": args.target_pct,
                    "detail_source": str(detail_path),
                    "proxy_contract": dict(payload.get("proxy_contract") or {}),
                },
            )
        except ReportGuardError as exc:
            raise SystemExit(str(exc))
        print(markdown)
        print(f"\n[final markdown] {bundle['markdown']}")
        print(f"[final pdf] {bundle['pdf']}")


if __name__ == "__main__":
    main()
