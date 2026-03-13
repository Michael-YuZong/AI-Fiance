"""Interactive research command based on local data modules."""

from __future__ import annotations

import argparse
import io
import re
import warnings
from contextlib import redirect_stderr
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

from src.collectors import AssetLookupCollector, GlobalFlowCollector, SocialSentimentCollector
from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.regime import RegimeDetector
from src.processors.risk import RiskAnalyzer
from src.processors.risk_support import build_portfolio_risk_context, find_stress_scenario, load_stress_scenarios, resolve_stress_scenario
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.utils.config import detect_asset_type, load_config
from src.utils.data import load_watchlist
from src.utils.logger import setup_logger
from src.utils.market import compute_history_metrics, fetch_asset_history


@dataclass(frozen=True)
class ResearchIntent:
    kind: str
    label: str
    needs_regime: bool
    needs_risk: bool
    needs_flow: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive investment research command.")
    parser.add_argument("question", nargs="+", help="Research question in natural language")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _classify_question(question: str, symbols: List[str], has_holdings: bool) -> ResearchIntent:
    lowered = question.lower()
    macro_keywords = ("降息", "宏观", "regime", "体制", "环境", "信用", "通胀", "pmi", "ppi", "cpi")
    risk_keywords = ("风险", "回撤", "相关", "beta", "压力", "stress", "组合", "仓位", "暴露")
    flow_keywords = ("资金", "轮动", "情绪", "热度", "拥挤", "风格", "主线", "别扭", "强弱")
    asset_keywords = ("买", "卖", "怎么看", "为什么", "逻辑", "适合", "机会", "还能不能", "值不值得")

    needs_regime = any(keyword in lowered for keyword in macro_keywords)
    needs_risk = any(keyword in lowered for keyword in risk_keywords)
    needs_flow = any(keyword in lowered for keyword in flow_keywords)
    asks_asset = bool(symbols) and any(keyword in question for keyword in asset_keywords)

    if needs_risk and has_holdings:
        return ResearchIntent("portfolio_risk", "组合风险 / 场景问答", True, True, needs_flow)
    if asks_asset or symbols:
        return ResearchIntent("asset_thesis", "标的研究 / 交易问题", needs_regime, needs_risk and has_holdings, needs_flow)
    if needs_regime and not symbols:
        return ResearchIntent("macro_regime", "宏观 / Regime 问答", True, False, False)
    if needs_flow:
        return ResearchIntent("market_diagnosis", "市场状态 / 风格问答", True, has_holdings and needs_risk, True)
    return ResearchIntent("open_research", "开放研究问答", True, has_holdings and needs_risk, True)


def _detect_symbols(question: str, candidates: Iterable[str]) -> List[str]:
    upper_question = question.upper()
    matched: List[str] = []
    for symbol in candidates:
        if symbol.upper() in upper_question and symbol not in matched:
            matched.append(symbol)
    for pattern in [r"(?<!\d)\d{5,6}(?!\d)", r"\b[A-Z]{1,5}\b", r"\b[A-Z]{1,2}\d\b"]:
        for token in re.findall(pattern, upper_question):
            if token not in matched:
                matched.append(token)
    return matched


def _resolve_symbols(question: str, config: Dict[str, Any], candidates: Iterable[str]) -> List[str]:
    symbols = _detect_symbols(question, candidates)
    resolved = AssetLookupCollector(config).search(question, limit=6)
    for item in resolved:
        symbol = item["symbol"]
        if symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _snapshot_bias(metrics: Dict[str, float], technical: Dict[str, Any]) -> Dict[str, Any]:
    ma_signal = str(technical["ma_system"]["signal"])
    macd_signal = str(technical["macd"]["signal"])
    rsi_value = float(technical["rsi"]["RSI"])

    if ma_signal == "bullish" and macd_signal == "bullish" and metrics["return_20d"] > 0:
        answer = "趋势仍偏强，但更适合顺势跟踪或等回踩确认，不适合把它当成低风险追价点。"
        bias = "偏强"
    elif ma_signal == "bearish" and metrics["return_20d"] < 0:
        answer = "趋势偏弱，除非你的问题本身就是左侧博弈，否则当前更应该先看风险而不是先看弹性。"
        bias = "偏弱"
    else:
        answer = "当前更像确认阶段，方向不是没有，但证据还没强到可以忽略节奏和位置。"
        bias = "分歧"

    risks: List[str] = []
    if rsi_value >= 70:
        risks.append(f"RSI {rsi_value:.1f} 已偏热，短线追高性价比一般。")
    elif rsi_value <= 30:
        risks.append(f"RSI {rsi_value:.1f} 已偏冷，反弹与继续走弱都需要二次确认。")
    if abs(metrics["return_20d"]) >= 0.15:
        risks.append(f"近20日波动 {metrics['return_20d'] * 100:+.2f}%，波动已经放大。")
    if technical["volume"]["vol_ratio"] > 1.6:
        risks.append(f"量能比 {technical['volume']['vol_ratio']:.2f}，当前交易拥挤度在抬升。")

    if bias == "偏强":
        action = "更像持有/回踩确认，而不是无条件追高。"
    elif bias == "偏弱":
        action = "更像风险控制题，而不是进攻题。"
    else:
        action = "先等催化或趋势补齐，再决定是否加大动作。"

    return {"bias": bias, "answer": answer, "risks": risks[:3], "action": action}


def _symbol_snapshot(symbol: str, config: Dict[str, Any]) -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
    metrics = compute_history_metrics(history)
    technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
    bias_payload = _snapshot_bias(metrics, technical)
    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "metrics": metrics,
        "technical": technical,
        "bias": bias_payload["bias"],
        "answer": bias_payload["answer"],
        "risks": list(bias_payload["risks"]),
        "action": bias_payload["action"],
        "evidence_lines": [
            f"{symbol}: 最新价 {metrics['last_close']:.3f}，近20日 {metrics['return_20d'] * 100:+.2f}%，近60日 {metrics['return_60d'] * 100:+.2f}%。",
            f"{symbol}: 均线信号 {technical['ma_system']['signal']}，MACD {technical['macd']['signal']}，RSI {technical['rsi']['RSI']:.1f}，量能比 {technical['volume']['vol_ratio']:.2f}。",
        ],
    }


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
        with redirect_stderr(io.StringIO()):
            global_proxy = load_global_proxy_snapshot()
        note = ""
    except Exception:
        global_proxy = {}
        note = "跨市场代理数据暂不可用，已回退到国内宏观视角。"
    regime_inputs = derive_regime_inputs(china_macro, global_proxy)
    result = RegimeDetector(regime_inputs).detect_regime()
    lines = [
        f"当前 macro regime 为 {result['current_regime']}，偏好资产: {', '.join(result.get('preferred_assets', [])) or '无明显偏好'}。",
        *[f"判断依据: {item}" for item in result.get("reasoning", [])[:3]],
    ]
    if note:
        lines.append(note)
    return lines


def _flow_and_sentiment_lines(symbols: List[str], config: Dict[str, Any]) -> List[str]:
    snapshots = []
    sentiment_lines: List[str] = []
    collector = SocialSentimentCollector(config)
    for symbol in symbols[:3]:
        asset_type = detect_asset_type(symbol, config)
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
            metrics = compute_history_metrics(history)
            technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
            trend = (
                "多头"
                if technical["ma_system"]["signal"] == "bullish"
                else "空头" if technical["ma_system"]["signal"] == "bearish" else "震荡"
            )
            snapshot = {
                "symbol": symbol,
                "region": "CN" if asset_type in {"cn_etf", "futures"} else "HK" if asset_type in {"hk", "hk_index"} else "US",
                "sector": next((item.get("sector", "") for item in load_watchlist() if item["symbol"] == symbol), ""),
                "return_5d": metrics["return_5d"],
                "return_20d": metrics["return_20d"],
            }
            snapshots.append(snapshot)
            sentiment = collector.collect(
                symbol,
                {
                    "return_1d": metrics["return_1d"],
                    "return_5d": metrics["return_5d"],
                    "return_20d": metrics["return_20d"],
                    "volume_ratio": technical["volume"]["vol_ratio"],
                    "trend": trend,
                },
            )
            sentiment_lines.append(f"{symbol}: {sentiment['aggregate']['interpretation']}")
        except Exception:
            continue
    flow_lines = GlobalFlowCollector(config).collect(snapshots).get("lines", [])
    return flow_lines[:2] + sentiment_lines[:2]


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


def _direct_answer_lines(
    intent: ResearchIntent,
    snapshots: List[Dict[str, Any]],
    regime_lines: List[str],
    flow_lines: List[str],
    risk_lines: List[str],
) -> List[str]:
    lines: List[str] = []
    if intent.kind == "portfolio_risk":
        if risk_lines:
            lines.append(risk_lines[0])
        if snapshots:
            lines.append(f"如果你的问题同时关心标的方向，当前优先先处理 `{snapshots[0]['symbol']}` 对组合风险的贡献，再谈加仓。")
        return lines[:2] or ["这本质上是组合风控题，先看相关性、回撤和场景暴露。"]

    if snapshots:
        primary = snapshots[0]
        lines.append(f"{primary['symbol']}: {primary['answer']}")
        if len(snapshots) >= 2:
            ranked = sorted(
                snapshots,
                key=lambda item: (
                    item["bias"] == "偏强",
                    item["metrics"]["return_20d"],
                    item["technical"]["volume"]["vol_ratio"],
                ),
                reverse=True,
            )
            lines.append(f"如果只选一个当前更顺手的方向，先看 `{ranked[0]['symbol']}`。")
        elif regime_lines and intent.needs_regime:
            lines.append(f"放在当前宏观背景里看，{regime_lines[0]}")
        return lines[:2]

    if regime_lines:
        lines.append(regime_lines[0])
    if flow_lines:
        lines.append(flow_lines[0])
    if not lines:
        lines.append("当前更像框架性问题，先用宏观、资金和组合风险三个视角交叉确认。")
    return lines[:2]


def _render_research_markdown(
    *,
    question: str,
    intent: ResearchIntent,
    symbols: List[str],
    direct_answer_lines: List[str],
    evidence_lines: List[str],
    risk_lines: List[str],
    action_lines: List[str],
) -> str:
    lines = [
        "# 研究回答",
        "",
        f"- 问题: {question}",
        f"- 类型: {intent.label}",
        f"- 识别标的: {', '.join(symbols) if symbols else '未识别到明确标的'}",
        "",
        "## 一句话回答",
    ]
    for item in direct_answer_lines:
        lines.append(f"- {item}")

    lines.extend(["", "## 证据"])
    for item in evidence_lines or ["当前没有拿到足够证据，建议先缩小问题范围或指定标的。"]:
        lines.append(f"- {item}")

    lines.extend(["", "## 风险与不确定性"])
    for item in risk_lines or ["当前回答更多是框架判断，缺少更细的事件、盘口或持仓上下文。"]:
        lines.append(f"- {item}")

    lines.extend(["", "## 下一步"])
    for item in action_lines:
        lines.append(f"- {item}")
    return "\n".join(lines)


def main() -> None:
    args = build_parser().parse_args()
    setup_logger("ERROR")
    config = load_config(args.config or None)
    question = " ".join(args.question).strip()
    repo = PortfolioRepository()
    holdings = repo.list_holdings()
    watchlist = load_watchlist()
    candidate_symbols = [item["symbol"] for item in watchlist] + [item["symbol"] for item in holdings]
    symbols = _resolve_symbols(question, config, candidate_symbols)
    intent = _classify_question(question, symbols, has_holdings=bool(holdings))

    snapshots: List[Dict[str, Any]] = []
    evidence_lines: List[str] = []
    risk_lines: List[str] = []
    action_lines: List[str] = []
    regime_lines: List[str] = _regime_lines(config) if intent.needs_regime else []
    flow_lines: List[str] = []

    if regime_lines:
        evidence_lines.extend(f"[宏观] {item}" for item in regime_lines)

    if symbols:
        for symbol in symbols[:3]:
            try:
                snapshot = _symbol_snapshot(symbol, config)
                snapshots.append(snapshot)
                evidence_lines.extend(f"[行情/技术] {item}" for item in snapshot["evidence_lines"])
                risk_lines.extend(snapshot["risks"])
            except Exception as exc:
                risk_lines.append(f"{symbol}: 数据拉取失败，暂时无法做研究快照。{exc}")

    if intent.needs_flow and symbols:
        flow_lines = _flow_and_sentiment_lines(symbols, config)
        evidence_lines.extend(f"[资金/情绪代理] {item}" for item in flow_lines)

    if intent.needs_risk and holdings:
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

    if not evidence_lines:
        if symbols:
            risk_lines.append("已识别相关标的，但当前缓存和行情数据不足，建议先单独运行 scan。")
        else:
            risk_lines.append("问题里没有识别到明确标的，当前只能先给框架性判断。")

    direct_answer_lines = _direct_answer_lines(intent, snapshots, regime_lines, flow_lines, risk_lines)

    if snapshots:
        action_lines.append(f"若要继续深入，可先跑 `{snapshots[0]['symbol']}` 对应的 `scan` 看完整分析卡。")
    elif symbols:
        action_lines.append("若要继续深入，可先跑对应 `scan` 看完整分析卡。")
    if intent.needs_flow:
        action_lines.append("如果想系统看风格轮动，可直接跑 `briefing daily` 或 `discover`。")
    if intent.needs_risk and holdings:
        action_lines.append("若想量化极端场景，再跑一次 `risk stress` 看具体持仓贡献。")
    if intent.kind in {"macro_regime", "open_research", "market_diagnosis"}:
        action_lines.append("如果你把问题收窄到标的、主题或场景，研究回答会明显更聚焦。")
    if not action_lines:
        action_lines.append("如果你给出更明确的标的或场景，研究回答会更聚焦。")
    print(
        _render_research_markdown(
            question=question,
            intent=intent,
            symbols=symbols[:3],
            direct_answer_lines=direct_answer_lines,
            evidence_lines=evidence_lines,
            risk_lines=risk_lines,
            action_lines=action_lines,
        )
    )


if __name__ == "__main__":
    main()
