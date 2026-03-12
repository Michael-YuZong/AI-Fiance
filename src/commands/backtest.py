"""Simple rule backtest command."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Callable, Dict, Tuple

import numpy as np
import pandas as pd

from src.output.backtest_report import BacktestReportRenderer
from src.processors.backtester import SimpleBacktester
from src.processors.risk_support import trim_history_period
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.data import load_yaml
from src.utils.market import fetch_asset_history


Rule = Tuple[
    Callable[[pd.DataFrame, int], bool],
    Callable[[pd.DataFrame, int], bool],
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest predefined timing rules on daily data.")
    parser.add_argument("rule_name", help="Rule name from config/rules.yaml")
    parser.add_argument("symbol", help="Asset symbol")
    parser.add_argument("period", nargs="?", default="3y", help="Backtest period, e.g. 3y")
    parser.add_argument("--capital", type=float, default=100000.0, help="Initial capital")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _load_rules() -> Dict[str, Dict[str, Any]]:
    payload = load_yaml(resolve_project_path("config/rules.yaml"), default={"rules": {}}) or {"rules": {}}
    return dict(payload.get("rules", {}))


def _prepare_rule_frame(frame: pd.DataFrame, technical_config: Dict[str, Any]) -> pd.DataFrame:
    normalized = normalize_ohlcv_frame(frame).copy()
    close = normalized["close"].astype(float)
    volume = normalized["volume"].fillna(0.0).astype(float)
    analyzer = TechnicalAnalyzer(normalized)
    indicators = analyzer.indicator_series(technical_config)

    normalized["dif"] = indicators["macd_dif"].values
    normalized["dea"] = indicators["macd_dea"].values
    normalized["rsi"] = indicators["rsi"].values

    normalized["ma10"] = close.rolling(10).mean()
    normalized["ma20"] = close.rolling(20).mean()
    normalized["vol_ma5"] = volume.rolling(5).mean()
    normalized["volume_ratio"] = volume / normalized["vol_ma5"].replace(0, np.nan)
    normalized["volume_ratio"] = normalized["volume_ratio"].replace([np.inf, -np.inf], np.nan).fillna(1.0)
    return normalized.set_index("date")


def _build_rule(rule_name: str, frame: pd.DataFrame) -> Rule:
    if rule_name == "macd_golden_cross":
        def entry_rule(_df: pd.DataFrame, i: int) -> bool:
            return (
                _df["dif"].iloc[i] > _df["dea"].iloc[i]
                and _df["dif"].iloc[i - 1] <= _df["dea"].iloc[i - 1]
                and _df["close"].iloc[i] > _df["ma20"].iloc[i]
            )

        def exit_rule(_df: pd.DataFrame, i: int) -> bool:
            return _df["dif"].iloc[i] < _df["dea"].iloc[i] or _df["close"].iloc[i] < _df["ma20"].iloc[i]

        return entry_rule, exit_rule

    if rule_name == "oversold_rebound":
        def entry_rule(_df: pd.DataFrame, i: int) -> bool:
            return _df["rsi"].iloc[i] < 30 and _df["volume_ratio"].iloc[i] > 1.2

        def exit_rule(_df: pd.DataFrame, i: int) -> bool:
            return _df["rsi"].iloc[i] > 60 or _df["close"].iloc[i] < _df["ma10"].iloc[i]

        return entry_rule, exit_rule

    raise ValueError(f"未实现该规则: {rule_name}")


def _baseline_metrics(frame: pd.DataFrame) -> Dict[str, float]:
    close = frame["close"].astype(float)
    buy_hold_return = float(close.iloc[-1] / close.iloc[0] - 1) if len(close) >= 2 else 0.0
    daily_returns = close.pct_change().dropna()
    annual_return = float((1 + buy_hold_return) ** (252 / max(len(close), 1)) - 1) if len(close) >= 2 else 0.0
    rolling_peak = close.cummax()
    max_drawdown = float((close / rolling_peak - 1).min()) if not close.empty else 0.0
    annual_vol = float(daily_returns.std() * np.sqrt(252)) if len(daily_returns) >= 2 else 0.0
    return {
        "total_return": buy_hold_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "annual_vol": annual_vol,
    }


def _trade_rows(result: Dict[str, Any]) -> list[list[str]]:
    rows = []
    for trade in result.get("trades", [])[-8:]:
        rows.append(
            [
                str(trade["entry_date"])[:10],
                str(trade["exit_date"])[:10],
                f"{float(trade['entry_price']):.3f}",
                f"{float(trade['exit_price']):.3f}",
                f"{float(trade['return']) * 100:+.2f}%",
            ]
        )
    return rows


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    rules = _load_rules()
    if args.rule_name not in rules:
        raise ValueError(f"规则不存在: {args.rule_name}")

    asset_type = detect_asset_type(args.symbol, config)
    raw_history = fetch_asset_history(args.symbol, asset_type, config, period=args.period)
    history = trim_history_period(normalize_ohlcv_frame(raw_history), args.period)
    prepared = _prepare_rule_frame(history, config.get("technical", {}))
    entry_rule, exit_rule = _build_rule(args.rule_name, prepared)

    backtester = SimpleBacktester(prepared)
    result = backtester.run(entry_rule=entry_rule, exit_rule=exit_rule, initial_capital=args.capital)
    baseline = _baseline_metrics(prepared)

    payload = {
        "title": "规则回测",
        "symbol": args.symbol,
        "rule_name": args.rule_name,
        "period": args.period,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "description": rules[args.rule_name].get("description", ""),
        "error": result.get("error"),
        "metric_lines": [],
        "baseline_lines": [],
        "warnings": [],
        "trade_rows": [],
    }

    if "error" not in result:
        payload["metric_lines"] = [
            f"总收益 {result['total_return'] * 100:+.2f}%，年化收益 {result['annual_return'] * 100:+.2f}%。",
            f"最大回撤 {result['max_drawdown'] * 100:.2f}%，胜率 {result['win_rate'] * 100:.1f}%。",
            f"共 {result['total_trades']} 笔，平均每笔 {result['avg_return_per_trade'] * 100:+.2f}%，盈亏比 {result['profit_loss_ratio']:.2f}。",
        ]
        payload["baseline_lines"] = [
            f"买入持有总收益 {baseline['total_return'] * 100:+.2f}%，年化 {baseline['annual_return'] * 100:+.2f}%。",
            f"买入持有最大回撤 {baseline['max_drawdown'] * 100:.2f}%，年化波动 {baseline['annual_vol'] * 100:.2f}%。",
        ]
        if "warning" in result:
            payload["warnings"].append(str(result["warning"]))
        if result["total_trades"] == 0:
            payload["warnings"].append("规则没有形成有效交易，说明条件可能过于苛刻。")
        payload["warnings"].append("回测仅用于排除明显无效规则，不用于证明未来一定有效。")
        payload["trade_rows"] = _trade_rows(result)

    print(BacktestReportRenderer().render(payload))


if __name__ == "__main__":
    main()
