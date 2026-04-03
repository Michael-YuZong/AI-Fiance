"""Regime command."""

from __future__ import annotations

import argparse

import pandas as pd

from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.regime import RegimeDetector
from src.utils.config import PROJECT_ROOT, load_config
from src.utils.data import load_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detect current macro regime and historical analog.")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    china_macro = load_china_macro_snapshot(config)
    global_proxy = load_global_proxy_snapshot(config)
    regime_inputs = derive_regime_inputs(china_macro, global_proxy)
    detector = RegimeDetector(regime_inputs)
    result = detector.detect_regime()
    history = pd.DataFrame(load_json(PROJECT_ROOT / "data" / "regime_history.json", default=[]))
    analog = detector.find_historical_analog(history)

    print("# 当前宏观体制")
    print("")
    print(f"- 当前 regime: `{result['current_regime']}`")
    print(f"- PMI: `{regime_inputs['pmi']:.1f}` ({regime_inputs['pmi_trend']})")
    print(f"- CPI 月率: `{regime_inputs['cpi']:.1f}` ({regime_inputs['cpi_trend']})")
    print(f"- 政策取向代理: `{regime_inputs['policy_stance']}`")
    print(f"- 信用脉冲代理: `{regime_inputs['credit_impulse']}`")
    print("")
    print("## 判断理由")
    for line in result.get("reasoning", []):
        print(f"- {line}")
    print("")
    print("## 偏好资产")
    for asset in result.get("preferred_assets", []):
        print(f"- {asset}")
    if analog:
        print("")
        print("## 历史相似期")
        print(f"- 最像: `{analog.get('period', 'unknown')}`")
        print(f"- 当时摘要: {analog.get('summary', '')}")
        print(f"- 市场路径: {analog.get('market_path', '')}")


if __name__ == "__main__":
    main()
