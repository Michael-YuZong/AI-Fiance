"""Portfolio command."""

from __future__ import annotations

import argparse
from typing import Any, Dict, List

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.config import detect_asset_type, load_config
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
        return {
            "return_20d": metrics["return_20d"],
            "ma_signal": technical["ma_system"]["signal"],
            "macd_signal": technical["macd"]["signal"],
        }
    except Exception:
        return {}


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
            signal_snapshot=_trade_signal_snapshot(args.symbol, asset_type, config),
        )
        print(f"已记录 {args.action} {args.symbol}，成交金额 {args.amount:.2f}。")
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
        latest_prices = _load_latest_prices(config, repo)
        review = repo.monthly_review(args.month, latest_prices)
        print(f"# 操作复盘: {args.month}")
        print("")
        if not review["trades"]:
            print("该月份没有交易记录。")
            return
        print("## Basis 统计")
        for basis, stats in review["basis_stats"].items():
            print(
                f"- {basis}: {stats['count']} 笔，平均结果 {stats['avg_outcome'] * 100:+.2f}%，"
                f"胜率 {stats['win_rate'] * 100:.1f}%。"
            )
        print("")
        print("## 逐笔观察")
        for trade in review["trades"]:
            outcome = trade["outcome"] * 100
            print(
                f"- {trade['timestamp']} {trade['action']} {trade['symbol']} @ {trade['price']:.3f}，"
                f"当前结果 {outcome:+.2f}% ，basis={trade.get('basis', 'unknown')}。"
            )


if __name__ == "__main__":
    main()
