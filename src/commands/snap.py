"""Intraday snapshot command."""

from __future__ import annotations

import argparse

from src.processors.technical import normalize_ohlcv_frame
from src.utils.config import detect_asset_type, load_config
from src.utils.market import fetch_asset_history, fetch_intraday_history, format_pct, get_asset_context, intraday_metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate an intraday snapshot for an asset.")
    parser.add_argument("symbol", help="Asset symbol, e.g. 561380 or QQQM")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    asset_type = detect_asset_type(args.symbol, config)
    context = get_asset_context(args.symbol, asset_type, config)

    daily_history = normalize_ohlcv_frame(fetch_asset_history(args.symbol, asset_type, config))
    fallback_mode = False
    try:
        intraday = fetch_intraday_history(args.symbol, asset_type, config)
        metrics = intraday_metrics(intraday)
    except Exception:
        fallback_mode = True
        latest = daily_history.iloc[-1:]
        metrics = intraday_metrics(latest)
        metrics["vwap"] = float((metrics["open"] + metrics["high"] + metrics["low"] + metrics["current"]) / 4)
    prev_close = float(daily_history["close"].iloc[-2]) if len(daily_history) >= 2 else metrics["open"]
    vs_prev_close = metrics["current"] / prev_close - 1 if prev_close else 0.0

    trend = "偏强" if metrics["current"] > metrics["vwap"] and metrics["range_position"] > 0.6 else "偏弱" if metrics["current"] < metrics["vwap"] and metrics["range_position"] < 0.4 else "震荡"

    print(f"# 盘中快照: {args.symbol} ({context.name})")
    print("")
    print(f"- 资产类型: `{asset_type}`")
    print(f"- 当前价: `{metrics['current']:.3f}`")
    print(f"- 相对昨收: `{format_pct(vs_prev_close)}`")
    print(f"- 相对今开: `{format_pct(metrics['change_pct'])}`")
    print(f"- 日内高低: `{metrics['low']:.3f} / {metrics['high']:.3f}`")
    print(f"- VWAP: `{metrics['vwap']:.3f}`")
    print(f"- 日内位置: `{metrics['range_position']:.0%}`")
    print("")
    print("## 观察")
    print(f"- 日内状态判断：{trend}。")
    print(
        f"- 当前价格 {'站上' if metrics['current'] >= metrics['vwap'] else '跌破'} VWAP，"
        f"说明日内平均成本 {'暂时占优' if metrics['current'] >= metrics['vwap'] else '暂时承压'}。"
    )
    if fallback_mode:
        print("- 分钟线获取失败，当前结果已退化为最近一根日 K 快照。")
    print("- 当前版本基于 Level 1 分钟线和成交量，五档盘口与大单拆分后续再补。")


if __name__ == "__main__":
    main()
