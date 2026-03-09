"""Daily and weekly briefing command."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import List

from src.output.briefing import BriefingRenderer
from src.processors.context import load_china_macro_snapshot, load_global_proxy_snapshot, macro_lines
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.utils.config import load_config
from src.utils.data import load_watchlist
from src.utils.market import compute_history_metrics, fetch_asset_history, format_pct


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily or weekly market briefing.")
    parser.add_argument("mode", choices=["daily", "weekly"], help="Briefing mode")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    watchlist = load_watchlist()
    china_macro = load_china_macro_snapshot(config)
    global_proxy = load_global_proxy_snapshot()

    watchlist_rows = []
    alerts: List[str] = []

    for item in watchlist:
        symbol = item["symbol"]
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(symbol, item["asset_type"], config))
            metrics = compute_history_metrics(history)
            technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
            trend = "多头" if technical["ma_system"]["signal"] == "bullish" and technical["macd"]["signal"] == "bullish" else "空头" if technical["ma_system"]["signal"] == "bearish" else "震荡"
            row = [
                f"{symbol} ({item['name']})",
                f"{metrics['last_close']:.3f}",
                format_pct(metrics["return_1d"]),
                format_pct(metrics["return_5d"]),
                format_pct(metrics["return_20d"]),
                trend,
            ]
            watchlist_rows.append(row)

            if args.mode == "daily" and (abs(metrics["return_1d"]) >= 0.03 or technical["volume"]["vol_ratio"] > 1.5):
                alerts.append(
                    f"{symbol} 当日波动 {format_pct(metrics['return_1d'])}，量比 {technical['volume']['vol_ratio']:.2f}。"
                )
            if args.mode == "weekly" and abs(metrics["return_5d"]) >= 0.08:
                alerts.append(f"{symbol} 近 5 日波动 {format_pct(metrics['return_5d'])}，进入重点复盘名单。")
        except Exception as exc:
            watchlist_rows.append([f"{symbol} ({item['name']})", "N/A", "N/A", "N/A", "N/A", "数据异常"])
            alerts.append(f"{symbol} 行情拉取失败: {exc}")

    if args.mode == "weekly":
        watchlist_rows = sorted(
            watchlist_rows,
            key=lambda row: float(row[3].replace("%", "").replace("+", "")) if row[3] != "N/A" else -999,
            reverse=True,
        )

    portfolio_lines: List[str] = []
    portfolio_repo = PortfolioRepository()
    holdings = portfolio_repo.list_holdings()
    if holdings:
        latest_prices = {}
        for holding in holdings:
            history = fetch_asset_history(holding["symbol"], holding["asset_type"], config)
            latest_prices[holding["symbol"]] = compute_history_metrics(history)["last_close"]
        status = portfolio_repo.build_status(latest_prices)
        portfolio_lines.append(f"组合市值约 {status['total_value']:.2f} {status['base_currency']}。")
        if status["holdings"]:
            top = max(status["holdings"], key=lambda row: row["weight"])
            portfolio_lines.append(f"当前最大持仓为 {top['symbol']}，权重约 {top['weight'] * 100:.1f}%。")
        top_region = max(status["region_exposure"].items(), key=lambda item: item[1], default=None)
        if top_region:
            portfolio_lines.append(f"地区暴露最高为 {top_region[0]}，占比 {top_region[1] * 100:.1f}%。")

    payload = {
        "title": "每日晨报" if args.mode == "daily" else "每周周报",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "macro_items": macro_lines(china_macro, global_proxy),
        "watchlist_rows": watchlist_rows,
        "alerts": alerts,
        "portfolio_lines": portfolio_lines,
    }
    print(BriefingRenderer().render(payload))


if __name__ == "__main__":
    main()
