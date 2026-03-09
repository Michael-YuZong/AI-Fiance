"""Asset comparison command."""

from __future__ import annotations

import argparse
from typing import Any, Dict, List

import pandas as pd

from src.collectors.valuation import ValuationCollector
from src.utils.config import detect_asset_type, load_config
from src.utils.market import compute_history_metrics, fetch_asset_history, format_pct, get_asset_context


def _rank_icons(rows: List[Dict[str, Any]], key: str, prefer: str = "high") -> Dict[str, str]:
    valid = [row for row in rows if row.get(key) is not None]
    if not valid:
        return {}
    if prefer == "high":
        ordered = sorted(valid, key=lambda item: item[key], reverse=True)
    elif prefer == "low":
        ordered = sorted(valid, key=lambda item: item[key])
    else:
        ordered = sorted(valid, key=lambda item: abs(item[key]))
    if len(ordered) == 1:
        return {ordered[0]["symbol"]: "✅"}
    result = {}
    for index, row in enumerate(ordered):
        if index == 0:
            result[row["symbol"]] = "✅"
        elif index == len(ordered) - 1:
            result[row["symbol"]] = "❌"
        else:
            result[row["symbol"]] = "⚠️"
    return result


def _reason(metric: str, row: Dict[str, Any]) -> str:
    if metric.startswith("return_"):
        return f"{metric.replace('return_', '近').replace('d', '日收益')} {format_pct(row[metric])}"
    if metric == "volatility_20d":
        return f"20 日年化波动 {row[metric] * 100:.2f}%"
    if metric == "max_drawdown_1y":
        return f"近 1 年最大回撤 {row[metric] * 100:.2f}%"
    if metric == "avg_turnover_20d":
        return f"20 日平均成交额约 {row[metric] / 1e6:.2f} 百万"
    if metric == "scale":
        return f"基金份额约 {row[metric] / 1e6:.2f} 百万份"
    if metric == "premium_discount":
        return f"相对最近净值的折溢价约 {row[metric] * 100:+.2f}%"
    return str(row.get(metric, "N/A"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare multiple assets in the same theme or category.")
    parser.add_argument("symbols", nargs="+", help="Two or more asset symbols")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    rows: List[Dict[str, Any]] = []
    valuation_collector = ValuationCollector(config)

    for symbol in args.symbols:
        asset_type = detect_asset_type(symbol, config)
        context = get_asset_context(symbol, asset_type, config)
        history = fetch_asset_history(symbol, asset_type, config)
        metrics = compute_history_metrics(history)
        row: Dict[str, Any] = {"symbol": symbol, "name": context.name, "asset_type": asset_type, **metrics}
        if asset_type == "cn_etf":
            try:
                nav = valuation_collector.get_cn_etf_nav_history(symbol)
                nav_series = pd.to_numeric(nav["单位净值"], errors="coerce").dropna()
                if not nav_series.empty:
                    latest_nav = float(nav_series.iloc[-1])
                    row["premium_discount"] = metrics["last_close"] / latest_nav - 1
            except Exception:
                row["premium_discount"] = None
            try:
                scale = valuation_collector.get_cn_etf_scale(symbol)
                if scale and "基金份额" in scale:
                    row["scale"] = float(scale["基金份额"])
            except Exception:
                row["scale"] = None
        rows.append(row)

    print(f"# 标的横向对比: {' vs '.join(args.symbols)}")
    print("")
    if len({row['asset_type'] for row in rows}) > 1:
        print("- 警告: 本次对比包含不同资产类型，结果更适合作为参考而不是严格 PK。")
        print("")

    metric_specs = [
        ("return_20d", "high", "近 20 日收益"),
        ("return_60d", "high", "近 60 日收益"),
        ("volatility_20d", "low", "20 日年化波动"),
        ("max_drawdown_1y", "high", "近 1 年最大回撤"),
        ("avg_turnover_20d", "high", "20 日平均成交额"),
        ("scale", "high", "基金份额"),
        ("premium_discount", "abs_low", "折溢价"),
    ]

    for metric, prefer, title in metric_specs:
        if not any(row.get(metric) is not None for row in rows):
            continue
        icons = _rank_icons(rows, metric, prefer=prefer)
        print(f"## {title}")
        for row in rows:
            if row.get(metric) is None:
                print(f"- {row['symbol']}: ⚠️ 该维度暂无数据。")
                continue
            print(f"- {row['symbol']}: {icons.get(row['symbol'], '⚠️')} {_reason(metric, row)}")
        print("")


if __name__ == "__main__":
    main()
