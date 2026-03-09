"""Interactive research command based on local data modules."""

from __future__ import annotations

import argparse
from typing import Any, Dict, Iterable, List

from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.regime import RegimeDetector
from src.processors.risk import RiskAnalyzer
from src.processors.risk_support import build_portfolio_risk_context, find_stress_scenario, load_stress_scenarios, resolve_stress_scenario
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.utils.config import detect_asset_type, load_config
from src.utils.data import load_watchlist
from src.utils.market import compute_history_metrics, fetch_asset_history


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive investment research command.")
    parser.add_argument("question", nargs="+", help="Research question in natural language")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _detect_symbols(question: str, candidates: Iterable[str]) -> List[str]:
    upper_question = question.upper()
    matched: List[str] = []
    for symbol in candidates:
        if symbol.upper() in upper_question and symbol not in matched:
            matched.append(symbol)
    return matched


def _symbol_snapshot(symbol: str, config: Dict[str, Any]) -> List[str]:
    asset_type = detect_asset_type(symbol, config)
    history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
    metrics = compute_history_metrics(history)
    technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
    lines = [
        f"{symbol}: 最新价 {metrics['last_close']:.3f}，近20日 {metrics['return_20d'] * 100:+.2f}%，近60日 {metrics['return_60d'] * 100:+.2f}%。",
        f"{symbol}: 均线信号 {technical['ma_system']['signal']}，MACD {technical['macd']['signal']}，RSI {technical['rsi']['RSI']:.1f}。",
    ]
    return lines


def _top_correlation_lines(analyzer: RiskAnalyzer) -> List[str]:
    matrix = analyzer.correlation_matrix()
    pairs: List[tuple[str, str, float]] = []
    columns = list(matrix.columns)
    for left in range(len(columns)):
        for right in range(left + 1, len(columns)):
            pairs.append((columns[left], columns[right], float(matrix.iloc[left, right])))
    pairs = sorted(pairs, key=lambda item: abs(item[2]), reverse=True)
    return [f"{left} / {right}: 相关系数 {value:+.2f}" for left, right, value in pairs[:3]]


def _regime_lines(config: Dict[str, Any]) -> List[str]:
    china_macro = load_china_macro_snapshot(config)
    try:
        global_proxy = load_global_proxy_snapshot()
        note = ""
    except Exception as exc:
        global_proxy = {}
        note = f"跨市场代理数据暂不可用，已回退到国内宏观视角。{exc}"
    regime_inputs = derive_regime_inputs(china_macro, global_proxy)
    result = RegimeDetector(regime_inputs).detect_regime()
    lines = [
        f"当前 macro regime 为 {result['current_regime']}，偏好资产: {', '.join(result.get('preferred_assets', [])) or '无明显偏好'}。",
        *[f"判断依据: {item}" for item in result.get("reasoning", [])[:3]],
    ]
    if note:
        lines.append(note)
    return lines


def _scenario_lines(question: str, context: Any, analyzer: RiskAnalyzer, config: Dict[str, Any]) -> List[str]:
    scenarios = load_stress_scenarios(config)
    aliases = {
        "美股崩盘": ["美股", "标普", "纳指", "跌20", "崩盘"],
        "人民币急贬": ["人民币", "贬值", "汇率"],
        "原油飙升": ["原油", "油价", "布伦特"],
    }
    matched_name = ""
    for name, keywords in aliases.items():
        if any(keyword in question for keyword in keywords):
            matched_name = name
            break
    if not matched_name:
        return []
    scenario = find_stress_scenario(matched_name, scenarios)
    if scenario is None:
        return []
    resolved = resolve_stress_scenario(scenario, context.status.get("holdings", []), config)
    if not resolved["shocks"]:
        return [f"已识别到场景 {matched_name}，但当前持仓没有足够映射，暂时只能做定性跟踪。"]
    result = analyzer.stress_test({"name": resolved["name"], "shocks": resolved["shocks"]})
    return [
        f"匹配到预设场景 {resolved['name']}。{scenario.get('description', '')}",
        result["interpretation"],
    ]


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    question = " ".join(args.question).strip()
    repo = PortfolioRepository()
    watchlist = load_watchlist()
    candidate_symbols = [item["symbol"] for item in watchlist] + [item["symbol"] for item in repo.list_holdings()]
    symbols = _detect_symbols(question, candidate_symbols)

    lines = ["# 研究回答", "", f"- 问题: {question}", ""]
    observation_lines: List[str] = []
    risk_lines: List[str] = []
    action_lines: List[str] = []

    needs_regime = any(keyword in question for keyword in ["降息", "宏观", "regime", "体制", "环境"])
    needs_risk = any(keyword in question for keyword in ["风险", "回撤", "相关", "beta", "压力", "stress", "组合"])

    if needs_regime:
        observation_lines.extend(_regime_lines(config))

    if symbols:
        for symbol in symbols[:3]:
            try:
                observation_lines.extend(_symbol_snapshot(symbol, config))
            except Exception as exc:
                observation_lines.append(f"{symbol}: 数据拉取失败，暂时无法做研究快照。{exc}")

    if needs_risk and repo.list_holdings():
        context = build_portfolio_risk_context(config, repo=repo)
        if context.weights:
            analyzer = RiskAnalyzer(context.returns_df[list(context.weights)], context.weights)
            report = analyzer.generate_risk_report(context.benchmark_returns)
            risk_lines.append(report["max_drawdown"]["interpretation"])
            risk_lines.append(report["var_95"]["interpretation"])
            risk_lines.extend(_top_correlation_lines(analyzer))
            risk_lines.extend(_scenario_lines(question, context, analyzer, config))
            high_corr = report.get("concentration_alerts", [])
            if high_corr:
                risk_lines.append(f"集中度提醒: {high_corr[0]['warning']}")
        else:
            risk_lines.extend(context.coverage_notes[:2])

    if not observation_lines:
        if symbols:
            observation_lines.append("已识别相关标的，但当前缓存和行情数据不足，建议先单独运行 scan。")
        else:
            observation_lines.append("问题里没有识别到明确标的，当前先给出框架性观察。")

    if symbols:
        action_lines.append("若要继续深入，可先跑对应 `scan` 看完整六维打分卡。")
    if needs_risk and repo.list_holdings():
        action_lines.append("若想量化极端场景，再跑一次 `risk stress` 看具体持仓贡献。")
    if not action_lines:
        action_lines.append("如果你给出更明确的标的或场景，研究回答会更聚焦。")

    lines.append("## 关键观察")
    for item in observation_lines:
        lines.append(f"- {item}")

    if risk_lines:
        lines.extend(["", "## 风险视角"])
        for item in risk_lines:
            lines.append(f"- {item}")

    lines.extend(["", "## 下一步"])
    for item in action_lines:
        lines.append(f"- {item}")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
