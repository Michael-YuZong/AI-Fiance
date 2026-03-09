"""Portfolio command."""

from __future__ import annotations

import argparse
from typing import List

from src.storage.portfolio import PortfolioRepository
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

    target_parser = subparsers.add_parser("set-target", help="Set target weight for a holding")
    target_parser.add_argument("symbol")
    target_parser.add_argument("weight", type=float)

    rebalance_parser = subparsers.add_parser("rebalance", help="Suggest rebalance actions")
    rebalance_parser.add_argument("--threshold", type=float, default=0.05)

    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _load_latest_prices(config: dict, repo: PortfolioRepository) -> dict:
    prices = {}
    for holding in repo.list_holdings():
        history = fetch_asset_history(holding["symbol"], holding["asset_type"], config)
        prices[holding["symbol"]] = compute_history_metrics(history)["last_close"]
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


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    repo = PortfolioRepository()

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


if __name__ == "__main__":
    main()
