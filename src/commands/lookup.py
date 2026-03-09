"""Resolve ETF or asset keywords into candidate symbols."""

from __future__ import annotations

import argparse

from src.collectors import AssetLookupCollector
from src.utils.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lookup asset symbols from natural-language keywords.")
    parser.add_argument("query", nargs="+", help="Asset keyword or natural-language request")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    query = " ".join(args.query).strip()
    collector = AssetLookupCollector(config)
    matches = collector.search(query, limit=8)
    best = collector.resolve_best(query)

    print("# 标的编号查询")
    print("")
    print(f"- 查询: {query}")
    if not matches:
        print("- 结果: 未找到匹配标的。")
        print("- 建议: 可以把常用别名补到 `config/asset_aliases.yaml`。")
        return

    if best:
        print(f"- 最佳匹配: `{best['symbol']}` / {best['name']}")
        print(f"- 匹配方式: {best.get('match_type', 'unknown')}")
        print("")
        print("## 可直接执行")
        print(f"- `python -m src.commands.scan {best['symbol']}`")
        print(f"- `python -m src.commands.assistant 分析一下 {best['symbol']}`")
        print("")

    print("## 候选列表")
    for item in matches:
        print(
            f"- `{item['symbol']}` {item['name']} "
            f"({item.get('asset_type', 'unknown')}, {item.get('match_type', 'unknown')})"
        )


if __name__ == "__main__":
    main()
