"""Intraday snapshot command."""

from __future__ import annotations

import argparse

from src.utils.config import detect_asset_type, load_config
from src.utils.market import build_intraday_snapshot, format_pct, get_asset_context


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
    snapshot = build_intraday_snapshot(args.symbol, asset_type, config)

    print(f"# 盘中快照: {args.symbol} ({context.name})")
    print("")
    print(f"- 资产类型: `{asset_type}`")
    print(f"- 当前价: `{snapshot['current']:.3f}`")
    print(f"- 相对昨收: `{format_pct(snapshot['change_vs_prev_close'])}`")
    print(f"- 相对今开: `{format_pct(snapshot['change_vs_open'])}`")
    print(f"- 日内高低: `{snapshot['low']:.3f} / {snapshot['high']:.3f}`")
    print(f"- VWAP: `{snapshot['vwap']:.3f}`")
    print(f"- 日内位置: `{snapshot['range_position']:.0%}`")
    if snapshot.get("auction_price") is not None:
        print(f"- 集合竞价价: `{snapshot['auction_price']:.3f}`")
        print(f"- 竞价高低开: `{format_pct(snapshot.get('auction_gap') or 0.0)}`")
        if snapshot.get("auction_volume_ratio") is not None:
            print(f"- 竞价量比: `{snapshot['auction_volume_ratio']:.2f}`")
    if snapshot.get("up_limit") is not None and snapshot.get("down_limit") is not None:
        print(f"- 涨跌停边界: `{snapshot['down_limit']:.3f} / {snapshot['up_limit']:.3f}`")
        if snapshot.get("limit_distance_up") is not None:
            print(f"- 距涨停: `{format_pct(snapshot['limit_distance_up'])}`")
        if snapshot.get("limit_distance_down") is not None:
            print(f"- 距跌停: `{format_pct(snapshot['limit_distance_down'])}`")
    print("")
    print("## 观察")
    print(f"- 日内状态判断：{snapshot['trend']}。")
    print(
        f"- 当前价格 {'站上' if snapshot['current'] >= snapshot['vwap'] else '跌破'} VWAP，"
        f"说明日内平均成本 {'暂时占优' if snapshot['current'] >= snapshot['vwap'] else '暂时承压'}。"
    )
    if snapshot.get("auction_commentary"):
        print(f"- {snapshot['auction_commentary']}")
    if snapshot.get("limit_commentary"):
        print(f"- {snapshot['limit_commentary']}")
    if snapshot.get("fallback_mode"):
        print("- 分钟线获取失败，当前结果已退化为最近一根日 K 快照。")
    print("- 当前版本基于 Level 1 分钟线和成交量，五档盘口与大单拆分后续再补。")


if __name__ == "__main__":
    main()
