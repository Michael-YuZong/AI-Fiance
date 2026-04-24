"""Interactive research command based on local data modules."""

from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import datetime
import io
from pathlib import Path
import re
import warnings
from contextlib import redirect_stderr
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Sequence

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

import pandas as pd

from src.collectors import (
    AssetLookupCollector,
    GlobalFlowCollector,
    MarketMonitorCollector,
    MarketOverviewCollector,
    MarketPulseCollector,
    SocialSentimentCollector,
)
from src.commands.intel import collect_intel_news_report, collect_market_aware_intel_news_report
from src.output.event_digest import build_event_digest, summarize_event_digest_contract
from src.processors.context import (
    derive_regime_inputs,
    global_proxy_runtime_enabled,
    load_china_macro_snapshot,
    load_global_proxy_snapshot,
)
from src.processors.opportunity_engine import _today_theme, analyze_opportunity, build_market_context, summarize_proxy_contracts
from src.processors.portfolio_actions import build_trade_plan
from src.processors.policy_engine import PolicyEngine
from src.processors.provenance import history_as_of
from src.processors.regime import RegimeDetector
from src.processors.risk import RiskAnalyzer
from src.processors.risk_support import build_portfolio_risk_context, find_stress_scenario, load_stress_scenarios, resolve_stress_scenario
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import (
    ThesisRepository,
    compare_event_digest_snapshots,
    summarize_review_queue_history_lines,
    summarize_thesis_state_snapshot,
)
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.data import load_watchlist, load_yaml
from src.utils.logger import setup_logger
from src.utils.market import compute_history_metrics, fetch_asset_history


@dataclass(frozen=True)
class ResearchIntent:
    kind: str
    label: str
    needs_regime: bool
    needs_risk: bool
    needs_flow: bool


POLICY_KEYWORDS = ("政策", "通知", "意见", "方案", "规划", "行动计划", "国常会", "国务院", "发改委", "工信部", "证监会", "财政部")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive investment research command.")
    parser.add_argument("question", nargs="+", help="Research question in natural language")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _dedupe_lines(items: Iterable[str], *, max_items: int | None = None) -> List[str]:
    deduped: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
        if max_items is not None and len(deduped) >= max_items:
            break
    return deduped


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _contains_policy_keywords(question: str) -> bool:
    return question.startswith(("http://", "https://")) or any(keyword in question for keyword in POLICY_KEYWORDS)


def _classify_question(question: str, symbols: List[str], has_holdings: bool) -> ResearchIntent:
    lowered = question.lower()
    macro_keywords = ("降息", "宏观", "regime", "体制", "环境", "信用", "通胀", "pmi", "ppi", "cpi")
    risk_keywords = ("风险", "回撤", "相关", "beta", "压力", "stress", "组合", "仓位", "暴露")
    portfolio_keywords = ("组合", "持仓", "仓位", "暴露", "相关", "beta", "压力测试", "stress")
    flow_keywords = ("资金", "轮动", "情绪", "热度", "拥挤", "风格", "主线", "别扭", "强弱")
    asset_keywords = ("买", "卖", "怎么看", "为什么", "逻辑", "适合", "机会", "还能不能", "值不值得")
    direct_trade_keywords = ("买", "卖", "还能不能", "值不值得", "仓位", "加仓", "减仓", "止损")

    needs_regime = any(keyword in lowered for keyword in macro_keywords)
    needs_risk = any(keyword in lowered for keyword in risk_keywords)
    needs_flow = any(keyword in lowered for keyword in flow_keywords)
    needs_policy = _contains_policy_keywords(question)
    asks_asset = bool(symbols) and any(keyword in question for keyword in asset_keywords)
    asks_portfolio = any(keyword in question for keyword in portfolio_keywords)
    asks_trade_decision = any(keyword in question for keyword in direct_trade_keywords)
    explicit_portfolio_scope = any(keyword in question for keyword in ("我的组合", "当前组合", "持仓里", "组合里", "我的持仓"))

    if asks_portfolio and not (symbols and asks_trade_decision) or explicit_portfolio_scope:
        return ResearchIntent(
            "portfolio_risk",
            "组合风险 / 场景问答",
            has_holdings,
            has_holdings,
            needs_flow and has_holdings,
        )
    if needs_risk and has_holdings and not symbols:
        return ResearchIntent("portfolio_risk", "组合风险 / 场景问答", True, True, needs_flow)
    if needs_policy and not asks_trade_decision:
        return ResearchIntent("policy_impact", "政策影响 / 主题问答", True, False, False)
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


def _asset_scenario_lines(symbol: str, metrics: Mapping[str, Any], technical: Mapping[str, Any], bias: str) -> List[str]:
    rsi_value = float(dict(technical.get("rsi") or {}).get("RSI") or 0.0)
    vol_ratio = float(dict(technical.get("volume") or {}).get("vol_ratio") or 0.0)
    ma_signal = str(dict(technical.get("ma_system") or {}).get("signal", ""))
    return_20d = float(metrics.get("return_20d") or 0.0)

    if bias == "偏强":
        main_prob = "60%" if rsi_value < 68 and vol_ratio <= 1.2 else "55%"
        return [
            f"[场景概率] 主场景（约 {main_prob}）是 `{symbol}` 维持偏强趋势，但更可能通过高位震荡或回踩确认来推进，不是直线加速段。",
            f"[场景概率] 次场景（约 25%-30%）是先回踩 MA20 / 关键均线后再决定是否继续上攻；尾部风险（约 15%）是动能失效并跌回趋势线下方。",
        ]
    if bias == "偏弱":
        main_prob = "55%" if ma_signal == "bearish" or return_20d < -0.08 else "50%"
        return [
            f"[场景概率] 主场景（约 {main_prob}）是 `{symbol}` 继续弱势震荡或只给反弹，不足以立刻扭转趋势。",
            f"[场景概率] 次场景（约 30%）是跌势继续；尾部逆转（约 15%-20%）要等重新站回关键均线和量能修复后再谈。",
        ]
    return [
        f"[场景概率] 主场景（约 50%-55%）是 `{symbol}` 继续走确认区间，先等趋势或催化补齐再决定方向。",
        "[场景概率] 次场景（约 25%-30%）是右侧突破，尾部风险（约 20%）是跌破支撑后重新转弱。",
    ]


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
        "scenario_lines": _asset_scenario_lines(symbol, metrics, technical, bias_payload["bias"]),
        "provenance_lines": [
            f"[行情时点] {symbol} 日线 as_of `{history_as_of(history)}`，来源 `本地历史日线链路`。",
        ],
        "evidence_lines": [
            f"{symbol}: 最新价 {metrics['last_close']:.3f}，近20日 {metrics['return_20d'] * 100:+.2f}%，近60日 {metrics['return_60d'] * 100:+.2f}%。",
            f"{symbol}: 均线信号 {technical['ma_system']['signal']}，MACD {technical['macd']['signal']}，RSI {technical['rsi']['RSI']:.1f}，量能比 {technical['volume']['vol_ratio']:.2f}。",
        ],
    }


def _region_from_asset_type(asset_type: str) -> str:
    if asset_type in {"cn_stock", "cn_etf", "cn_fund", "futures"}:
        return "CN"
    if asset_type in {"hk", "hk_index"}:
        return "HK"
    return "US"


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
    if not global_proxy_runtime_enabled(config):
        global_proxy = {}
        note = "跨市场代理数据默认关闭，当前只按国内宏观视角解释。"
    else:
        try:
            with redirect_stderr(io.StringIO()):
                global_proxy = load_global_proxy_snapshot(config)
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


def _flow_and_sentiment_payload(symbols: List[str], config: Dict[str, Any]) -> Dict[str, Any]:
    snapshots = []
    sentiment_lines: List[str] = []
    risk_lines: List[str] = []
    social_payloads: List[Dict[str, Any]] = []
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
            social_payloads.append(sentiment)
            aggregate = dict(sentiment.get("aggregate") or {})
            sentiment_lines.append(
                f"{symbol}: {aggregate.get('interpretation', '')}（代理置信度 `{aggregate.get('confidence_label', '低')}`）"
            )
            limitations = list(aggregate.get("limitations") or [])
            if limitations:
                risk_lines.append(f"{symbol} 情绪代理限制：{limitations[0]}")
            downgrade = str(aggregate.get("downgrade_impact", "")).strip()
            if downgrade:
                risk_lines.append(f"{symbol} 情绪代理影响：{downgrade}")
        except Exception:
            continue
    flow_report = GlobalFlowCollector(config).collect(snapshots)
    flow_lines = list(flow_report.get("lines") or [])
    flow_confidence = str(flow_report.get("confidence_label", "低"))
    evidence_lines = [f"{flow_lines[0]}（代理置信度 `{flow_confidence}`）"] if flow_lines else []
    if len(flow_lines) >= 2:
        evidence_lines.append(flow_lines[1])
    evidence_lines.extend(sentiment_lines[:2])
    limitations = list(flow_report.get("limitations") or [])
    if limitations:
        risk_lines.append(f"资金流代理限制：{limitations[0]}")
    downgrade = str(flow_report.get("downgrade_impact", "")).strip()
    if downgrade:
        risk_lines.append(f"资金流代理影响：{downgrade}")
    return {
        "evidence_lines": evidence_lines[:4],
        "risk_lines": risk_lines[:4],
        "proxy_contract": summarize_proxy_contracts(
            market_proxy=flow_report,
            social_payloads=social_payloads,
            total=len(symbols[:3]),
        ),
    }


def _market_proxy_snapshots(watchlist: Sequence[Mapping[str, Any]], config: Dict[str, Any], limit: int = 6) -> List[Dict[str, Any]]:
    preferred_sectors = ("科技", "黄金", "宽基", "高股息", "能源")
    preferred_asset_types = {"cn_etf", "cn_stock", "futures"}
    selected: List[Mapping[str, Any]] = []
    seen: set[str] = set()

    def add_item(item: Mapping[str, Any]) -> None:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in seen or len(selected) >= limit:
            return
        selected.append(item)
        seen.add(symbol)

    for sector in preferred_sectors:
        for item in watchlist:
            if (
                str(item.get("sector", "")).strip() == sector
                and str(item.get("asset_type", "cn_etf")).strip() in preferred_asset_types
            ):
                add_item(item)
                break

    for item in watchlist:
        if str(item.get("asset_type", "cn_etf")).strip() in preferred_asset_types:
            add_item(item)
        if len(selected) >= limit:
            break

    snapshots: List[Dict[str, Any]] = []
    for item in selected:
        symbol = str(item.get("symbol", "")).strip()
        asset_type = str(item.get("asset_type", "cn_etf")).strip()
        if not symbol:
            continue
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, config))
            metrics = compute_history_metrics(history)
            technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
        except Exception:
            continue
        snapshots.append(
            {
                "symbol": symbol,
                "name": str(item.get("name", symbol)),
                "region": _region_from_asset_type(asset_type),
                "sector": str(item.get("sector", "")).strip(),
                "return_1d": metrics["return_1d"],
                "return_5d": metrics["return_5d"],
                "return_20d": metrics["return_20d"],
                "volume_ratio": technical["volume"]["vol_ratio"],
                "trend": (
                    "多头"
                    if technical["ma_system"]["signal"] == "bullish"
                    else "空头" if technical["ma_system"]["signal"] == "bearish" else "震荡"
                ),
            }
        )
    return snapshots


def _watchlist_intel_query(watchlist: Sequence[Mapping[str, Any]]) -> str:
    terms: List[str] = []
    for item in list(watchlist or [])[:4]:
        name = _safe_text(item.get("name"))
        symbol = _safe_text(item.get("symbol"))
        sector = _safe_text(item.get("sector"))
        for value in (name, symbol, sector):
            if value and value not in terms:
                terms.append(value)
    return " ".join(terms) if terms else "A股 市场 情报"


def _shared_intel_news_report(
    config: Mapping[str, Any],
    *,
    query: str,
    explicit_symbol: str = "",
    baseline_report: Mapping[str, Any] | None = None,
    limit: int = 12,
    recent_days: int = 7,
    note_prefix: str = "",
) -> Dict[str, Any]:
    try:
        report = collect_market_aware_intel_news_report(
            query,
            config=config,
            explicit_symbol=explicit_symbol,
            baseline_report=baseline_report,
            limit=limit,
            recent_days=recent_days,
            structured_only=not bool(dict(config or {}).get("news_topic_search_enabled", True)),
            note_prefix=note_prefix,
            collect_fn=collect_intel_news_report,
        )
    except Exception as exc:
        return {
            "mode": "proxy",
            "items": [],
            "all_items": [],
            "lines": [],
            "source_list": [],
            "note": f"{note_prefix}intel 采集降级: {exc}",
            "disclosure": "共享 intel 采集失败，按缺失处理，不伪装成 fresh 命中。",
        }
    return dict(report)


def _attach_shared_intel_news_report(
    context: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    query: str,
    explicit_symbol: str = "",
    note_prefix: str = "",
) -> Dict[str, Any]:
    merged = dict(context or {})
    existing_report = dict(merged.get("news_report") or {})
    shared_report = _shared_intel_news_report(
        config,
        query=query,
        explicit_symbol=explicit_symbol,
        baseline_report=existing_report,
        note_prefix=note_prefix,
    )
    shared_items = list(shared_report.get("items") or [])
    if not shared_items:
        return merged

    existing_items = list(existing_report.get("items") or [])
    if not existing_items or len(shared_items) >= len(existing_items):
        merged["news_report"] = shared_report
        merged["intel_news_report"] = shared_report
    return merged


def _light_market_context(config: Dict[str, Any], watchlist: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    notes: List[str] = []
    monitor_rows: List[Dict[str, Any]] = []
    news_report: Dict[str, Any] = {"mode": "proxy", "items": [], "lines": [], "note": ""}
    pulse: Dict[str, Any] = {}

    try:
        monitor_rows = MarketMonitorCollector(config).collect()
    except Exception as exc:
        notes.append(f"宏观监控数据缺失: {exc}")
    try:
        news_report = _shared_intel_news_report(
            config,
            query=_watchlist_intel_query(watchlist),
            limit=12,
            recent_days=7,
            note_prefix="research market intel: ",
        )
    except Exception as exc:
        notes.append(f"共享 intel 链路降级: {exc}")
    try:
        pulse = MarketPulseCollector(config).collect()
    except Exception as exc:
        notes.append(f"盘面情绪数据缺失: {exc}")

    return {
        "day_theme": _today_theme(news_report, monitor_rows),
        "news_report": news_report,
        "pulse": pulse,
        "notes": notes,
        "watchlist": list(watchlist),
    }


def _market_overview_lines(overview: Mapping[str, Any]) -> List[str]:
    lines: List[str] = []
    breadth = dict(overview.get("breadth") or {})
    if breadth:
        up_count = int(breadth.get("up_count") or 0)
        down_count = int(breadth.get("down_count") or 0)
        turnover = breadth.get("turnover")
        turnover_text = f"，成交额约 {float(turnover):.0f} 亿" if turnover not in (None, "") else ""
        lines.append(f"A股上涨 {up_count} 家，下跌 {down_count} 家{turnover_text}。")

    domestic = list(overview.get("domestic_indices") or [])
    if domestic:
        summary = "、".join(
            f"{row.get('name', row.get('symbol', '—'))} {float(row.get('change_pct', 0.0)) * 100:+.2f}%"
            for row in domestic[:3]
            if row.get("change_pct") is not None
        )
        if summary:
            lines.append(f"主要指数表现：{summary}。")
    return lines[:2]


def _fast_market_overview(config: Dict[str, Any]) -> Dict[str, Any]:
    collector = MarketOverviewCollector(config)
    payload = load_yaml(collector.overview_path, default={}) or {}
    domestic_indices = list(payload.get("domestic_indices", []) or [])
    spot = collector._load_cache("market_overview:domestic_spot:v1", ttl_hours=1, allow_stale=True)
    breadth_frame = collector._load_cache("market_overview:a_spot_em:v1", ttl_hours=1, allow_stale=True)

    domestic_rows: List[Dict[str, Any]] = []
    if spot is not None and not getattr(spot, "empty", True):
        for item in domestic_indices:
            code = str(item.get("symbol", "")).strip()
            if not code:
                continue
            matched = spot[spot["代码"].astype(str) == code]
            if matched.empty:
                continue
            current = matched.iloc[0]
            prev_close = pd.to_numeric(pd.Series([current.get("昨收")]), errors="coerce").iloc[0]
            latest = pd.to_numeric(pd.Series([current.get("最新价")]), errors="coerce").iloc[0]
            change_pct = pd.to_numeric(pd.Series([current.get("涨跌幅")]), errors="coerce").iloc[0] / 100
            domestic_rows.append(
                {
                    "name": str(item.get("name", code)),
                    "symbol": code,
                    "latest": float(latest) if pd.notna(latest) else None,
                    "change_pct": float(change_pct) if pd.notna(change_pct) else None,
                    "prev_close": float(prev_close) if pd.notna(prev_close) else None,
                }
            )

    breadth: Dict[str, Any] = {}
    if breadth_frame is not None and not getattr(breadth_frame, "empty", True):
        change = pd.to_numeric(breadth_frame["涨跌幅"], errors="coerce").dropna()
        amount = pd.to_numeric(breadth_frame["成交额"], errors="coerce").dropna()
        breadth = {
            "up_count": int((change > 0).sum()),
            "down_count": int((change < 0).sum()),
            "flat_count": int((change == 0).sum()),
            "turnover": float(amount.sum()) / 1e8 if not amount.empty else None,
            "source": "cache_snapshot",
        }

    return {
        "domestic_indices": domestic_rows,
        "breadth": breadth,
        "global_indices": [],
        "source": "cache_snapshot",
    }


def _pulse_stats(pulse: Mapping[str, Any]) -> Dict[str, int]:
    def _count_rows(value: Any) -> int:
        if value is None:
            return 0
        index = getattr(value, "index", None)
        if index is None or callable(index):
            return 0
        return len(index)

    return {
        "zt_count": _count_rows(pulse.get("zt_pool")),
        "dt_count": _count_rows(pulse.get("dt_pool")),
        "strong_count": _count_rows(pulse.get("strong_pool")),
    }


def _market_takeaway(
    *,
    day_theme_label: str,
    breadth: Mapping[str, Any],
    flow_report: Mapping[str, Any],
    pulse_stats: Mapping[str, int],
) -> Dict[str, Any]:
    up_count = int(breadth.get("up_count") or 0)
    down_count = int(breadth.get("down_count") or 0)
    zt_count = int(pulse_stats.get("zt_count") or 0)
    dt_count = int(pulse_stats.get("dt_count") or 0)
    risk_bias = str(flow_report.get("risk_bias", "neutral"))
    domestic_bias = str(flow_report.get("domestic_bias", "neutral"))
    defensive_theme = any(token in day_theme_label for token in ("风险", "防守", "地缘", "黄金", "能源"))

    if risk_bias == "risk_off" and (down_count > up_count or dt_count >= max(zt_count, 1)):
        answer_lines = [
            "现在的别扭更像风险偏好在回落，资金先往防守或确定性更高的方向缩。",
            "不是所有方向都一起坏，而是进攻线更难形成持续性，非主线更容易掉队。",
        ]
        state = "defensive_rotation"
    elif risk_bias == "risk_on" and up_count > down_count and zt_count >= dt_count:
        answer_lines = [
            "市场不算全面转弱，更像主线集中、分化很重；跟对方向不难，跟错方向会很难受。",
            "这类环境里更该先定主线，再谈个股、ETF 或基金，而不是平均撒网。",
        ]
        state = "narrow_risk_on"
    else:
        answer_lines = [
            "现在更像分歧市：宏观没有彻底转坏，但主线、资金和短线情绪还没站到同一边。",
            "所以体感会别扭，指数未必最差，但没有主线保护的方向更容易反复。",
        ]
        state = "split_market"

    evidence_lines: List[str] = []
    if day_theme_label and day_theme_label != "背景宏观主导":
        evidence_lines.append(f"[市场主线] 当前主线标签偏 `{day_theme_label}`。")
    if up_count or down_count:
        evidence_lines.append(f"[市场宽度] A股上涨 {up_count} 家，下跌 {down_count} 家。")
    flow_lines = list(flow_report.get("lines") or [])
    if flow_lines:
        evidence_lines.append(f"[资金/情绪代理] {flow_lines[0]}")
    if domestic_bias == "offshore_lead":
        evidence_lines.append("[地域切换] 海外弹性相对占优，说明离岸成长对风险偏好更敏感。")
    elif domestic_bias == "domestic_lead":
        evidence_lines.append("[地域切换] 国内资产相对更稳，说明资金更偏本土确定性。")

    risk_lines: List[str] = []
    if defensive_theme and state != "narrow_risk_on":
        risk_lines.append(f"当前主线更偏 `{day_theme_label}`，进攻方向如果没有新催化，更容易先被资金放弃。")
    if str(flow_report.get("method", "")) == "proxy" and flow_lines:
        risk_lines.append("这里的资金与情绪判断主要来自价格和量能代理，不是硬流向或真实社媒抓取。")
    if not (up_count or down_count):
        risk_lines.append("市场宽度数据暂不可用，当前更多依赖主线和代理风格判断。")

    return {
        "state": state,
        "answer_lines": answer_lines,
        "evidence_lines": evidence_lines,
        "risk_lines": risk_lines,
    }


def _market_probability_lines(state: str, day_theme_label: str) -> List[str]:
    if state == "defensive_rotation":
        return [
            f"[场景概率] 主场景（约 60%）是市场继续偏防守轮动，`{day_theme_label or '当前主线'}` 这类确定性方向相对占优。",
            "[场景概率] 次场景（约 25%）是风险偏好阶段性修复，但更可能先集中在少数强势方向；尾部风险（约 15%）是全面 risk-off。",
        ]
    if state == "narrow_risk_on":
        return [
            f"[场景概率] 主场景（约 55%-60%）是主线继续集中演绎，赚钱机会仍有，但会明显收敛在 `{day_theme_label or '少数方向'}` 附近。",
            "[场景概率] 次场景（约 25%-30%）是高低切换加快；尾部风险（约 15%）是一旦主线掉队，强势方向也会一起回撤。",
        ]
    return [
        "[场景概率] 主场景（约 55%-60%）是分歧市延续，指数层面未必很差，但非主线方向更容易来回反复。",
        "[场景概率] 次场景（约 25%-30%）是风格重新集中，尾部风险（约 15%）是情绪进一步转冷并扩散成更广的 risk-off。",
    ]


def _market_flow_payload(config: Dict[str, Any], watchlist: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    snapshots = _market_proxy_snapshots(watchlist, config)
    if not snapshots:
        return {"risk_bias": "neutral", "domestic_bias": "neutral", "lines": [], "method": "skipped"}
    report = GlobalFlowCollector(config).collect(snapshots)
    report["method"] = "proxy"
    return report


def _market_diagnosis_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    watchlist = load_watchlist()
    runtime_context = _light_market_context(config, watchlist)
    overview = _fast_market_overview(config)
    try:
        flow_report = _market_flow_payload(config, watchlist)
    except Exception as exc:
        flow_report = {"risk_bias": "neutral", "domestic_bias": "neutral", "lines": [], "method": "skipped"}
        runtime_context.setdefault("notes", []).append(f"市场风格代理暂不可用: {exc}")
    pulse = dict(runtime_context.get("pulse") or {})
    pulse_stats = _pulse_stats(pulse)
    takeaway = _market_takeaway(
        day_theme_label=str(dict(runtime_context.get("day_theme") or {}).get("label", "")),
        breadth=dict(overview.get("breadth") or {}),
        flow_report=flow_report,
        pulse_stats=pulse_stats,
    )

    evidence_lines = list(takeaway["evidence_lines"])
    has_breadth_line = any(str(item).startswith("[市场宽度]") for item in evidence_lines)
    for item in _market_overview_lines(overview):
        if has_breadth_line and str(item).startswith("A股上涨"):
            continue
        evidence_lines.append(f"[市场概览] {item}")
    if pulse_stats["zt_count"] or pulse_stats["dt_count"] or pulse_stats["strong_count"]:
        evidence_lines.append(
            "[盘面情绪] "
            f"A股涨停 {pulse_stats['zt_count']} 家，跌停 {pulse_stats['dt_count']} 家，强势股池 {pulse_stats['strong_count']} 家。"
        )
    flow_lines = list(flow_report.get("lines") or [])
    if flow_lines:
        confidence = str(flow_report.get("confidence_label", "低")).strip() or "低"
        evidence_lines.append(f"[资金/情绪代理] {flow_lines[0]}（代理置信度 `{confidence}`）")
        if len(flow_lines) >= 2:
            evidence_lines.append(f"[风格补充] {flow_lines[1]}")
    evidence_lines.extend(
        _market_probability_lines(
            takeaway["state"],
            str(dict(runtime_context.get("day_theme") or {}).get("label", "")),
        )
    )

    risk_lines = list(takeaway["risk_lines"])
    news_mode = str(dict(runtime_context.get("news_report") or {}).get("mode", "")).strip()
    if news_mode and news_mode != "live":
        risk_lines.append("当前新闻链路不是完整 live 模式，市场主线判断更应看方向和结构，不宜把单条新闻当成硬催化。")
    limitations = list(flow_report.get("limitations") or [])
    if limitations:
        risk_lines.append(f"市场风格代理限制：{limitations[0]}")
    downgrade = str(flow_report.get("downgrade_impact", "")).strip()
    if downgrade:
        risk_lines.append(f"市场风格代理影响：{downgrade}")
    notes = list(runtime_context.get("notes") or [])
    if notes:
        risk_lines.extend(str(item).strip() for item in notes[:2] if str(item).strip())
    if str(overview.get("source", "")) == "cache_snapshot":
        risk_lines.append("市场概览当前优先使用缓存快照，适合看结构和方向，不适合据此做分钟级判断。")

    return {
        "answer_lines": list(takeaway["answer_lines"]),
        "evidence_lines": evidence_lines,
        "provenance_lines": [
            f"[市场时点] 市场概览当前来自 `{overview.get('source', 'unknown')}` 近实时缓存，盘中只适合作近似，不当成逐笔实时报价。",
            "[时点边界] 主线、宽度和代理信号默认只使用当前生成时点前可见覆盖；缺源或降级会单独写进风险与不确定性。",
        ],
        "risk_lines": risk_lines,
        "proxy_contract": summarize_proxy_contracts(market_proxy=flow_report, social_payloads=[], total=0),
    }


def _portfolio_risk_payload(
    *,
    question: str,
    context: Any,
    report: Mapping[str, Any],
    analyzer: RiskAnalyzer,
    config: Dict[str, Any],
) -> Dict[str, List[str]]:
    holdings = list(context.status.get("holdings", []) or [])
    if not holdings:
        return {
            "answer_lines": ["当前没有可分析的持仓历史数据，先补齐持仓和价格后再谈组合风险。"],
            "evidence_lines": [],
            "provenance_lines": ["[组合时点] 当前没有可用持仓快照，暂时无法形成 point-in-time 组合风险判断。"],
            "risk_lines": list(context.coverage_notes[:2]),
        }

    top_holding = max(holdings, key=lambda item: float(item.get("weight", 0.0) or 0.0))
    region_exposure = dict(context.status.get("region_exposure", {}) or {})
    sector_exposure = dict(context.status.get("sector_exposure", {}) or {})
    top_region = max(region_exposure.items(), key=lambda item: float(item[1]), default=("UNKNOWN", 0.0))
    top_sector = max(sector_exposure.items(), key=lambda item: float(item[1]), default=("UNKNOWN", 0.0))
    concentration_alerts = list(report.get("concentration_alerts") or [])
    scenario_lines = _scenario_lines(question, context, analyzer, config)
    beta_payload = dict(report.get("beta") or {})
    var_payload = dict(report.get("var_95") or {})
    max_dd_payload = dict(report.get("max_drawdown") or {})
    beta_value = float(beta_payload.get("beta") or 0.0)
    top_weight = float(top_holding.get("weight", 0.0) or 0.0)

    if concentration_alerts:
        answer_lines = [
            "你现在最该担心的不是单一持仓好不好，而是组合内部相关性偏高，回撤时容易一起放大。",
            f"当前最重仓的是 `{top_holding.get('symbol', '—')}`，同时组合对 `{top_sector[0]}` 和 `{top_region[0]}` 暴露偏高。",
        ]
    elif beta_value >= 1.1:
        answer_lines = [
            "这套组合整体弹性偏高，市场一旦进入 risk-off，净值波动通常会比基准更大。",
            f"如果你现在还想加仓，优先先看是否会继续放大 `{top_sector[0]}` 这一侧的风险暴露。",
        ]
    else:
        answer_lines = [
            "组合风险不算失控，但它更像结构集中题，不像已经充分分散的稳健组合。",
            f"最值得盯的是 `{top_holding.get('symbol', '—')}` 和 `{top_sector[0]}` 方向的集中度有没有继续抬升。",
        ]

    scenario_probability_lines: List[str] = []
    if concentration_alerts or beta_value >= 1.1:
        scenario_probability_lines = [
            f"[场景概率] 主场景（约 60%）是组合继续跟着 `{top_sector[0]}` / `{top_region[0]}` 这类主暴露同向波动，分散化改善有限。",
            "[场景概率] 次场景（约 25%）是主线修复带来净值反弹，但弹性大概率仍集中在当前已偏重的方向；尾部风险（约 15%）是 risk-off 下相关性同时抬升。",
        ]
    else:
        scenario_probability_lines = [
            "[场景概率] 主场景（约 55%）是组合延续中性到偏集中的波动状态，问题更多是结构不够均衡，不是马上失控。",
            "[场景概率] 次场景（约 30%）是风格切换导致部分暴露拖累；尾部风险（约 15%）是单一主题继续抬升到需要主动降仓。",
        ]

    evidence_lines = [
        f"[组合权重] 当前最大持仓 `{top_holding.get('symbol', '—')}` 权重约 {float(top_holding.get('weight', 0.0) or 0.0) * 100:.1f}%。",
        f"[区域暴露] 当前区域暴露最高的是 `{top_region[0]}`，约 {float(top_region[1] or 0.0) * 100:.1f}%。",
        f"[行业暴露] 当前行业暴露最高的是 `{top_sector[0]}`，约 {float(top_sector[1] or 0.0) * 100:.1f}%。",
        f"[风险统计] {var_payload.get('interpretation', '')}",
        f"[风险统计] {max_dd_payload.get('interpretation', '')}",
    ]
    if concentration_alerts:
        evidence_lines.append(f"[相关性] {concentration_alerts[0].get('warning', '')}")
    if scenario_lines:
        evidence_lines.append(f"[压力场景] {scenario_lines[0]}")
    evidence_lines.extend(scenario_probability_lines)

    risk_lines = [
        f"{beta_payload.get('interpretation', '')}",
        *[str(item).strip() for item in scenario_lines[1:2] if str(item).strip()],
    ]
    if context.coverage_notes:
        risk_lines.extend(str(item).strip() for item in context.coverage_notes[:2] if str(item).strip())
    action_lines: List[str] = []
    if top_weight >= 0.35:
        action_lines.append(f"先评估是否把 `{top_holding.get('symbol', '—')}` 的权重压回 25%-30% 一带，再谈加新仓。")
    if concentration_alerts:
        action_lines.append("加仓前先看新增仓位能不能真正分散相关性，而不是继续堆在同一类风险因子上。")
    if beta_value >= 1.1:
        action_lines.append("如果你接下来还想提高弹性，先明确总风险预算，否则更容易把组合推成单边风险暴露。")
    return {
        "answer_lines": answer_lines,
        "evidence_lines": evidence_lines,
        "provenance_lines": [
            "[组合时点] 当前组合风险预演只使用已记录持仓和历史收益窗口，不回看未来价格路径。",
        ],
        "risk_lines": risk_lines,
        "action_lines": action_lines,
    }


def _policy_payload(question: str, holdings: Sequence[Mapping[str, Any]]) -> Dict[str, List[str]]:
    engine = PolicyEngine()
    try:
        context = engine.load_context(question)
    except Exception:
        context = engine.load_context(question if not question.startswith(("http://", "https://")) else question.split("/")[-1])

    matched = engine.match_policy(f"{context.title} {context.text}")
    if not matched:
        return {
            "answer_lines": ["当前更像泛政策问题，能判断方向，但还不足以下结论到具体标的或主题。"],
            "evidence_lines": ["[政策原文] 当前未命中本地政策模板，后续需要人工补充政策类型和受益链条。"] if context.text else [],
            "provenance_lines": [f"[政策口径] 当前来源 `{context.source}`，还没有形成可稳定映射的原文模板。"],
            "risk_lines": ["如果没有明确政策类型、执行阶段和受益链条，就不适合直接把它当成交易催化。"],
        }

    template = matched.template
    direction = engine.classify_policy_direction(f"{context.title} {context.text}")
    stage = engine.infer_policy_stage(context.title, context.text)
    timeline_points = engine.extract_timeline_points(context.text)
    watchlist_impact = engine.watchlist_impact(template, holdings)
    beneficiary_nodes = list(template.get("beneficiary_nodes", []) or [])
    risk_nodes = list(template.get("risk_nodes", []) or [])
    support_points = list(template.get("support_points", []) or [])

    if context.source == "keyword":
        if direction == "中性/待原文确认" and (beneficiary_nodes or support_points):
            direction = "偏支持（关键词推断）"
        if stage == "阶段待原文确认":
            stage = "主题跟踪阶段"

    if beneficiary_nodes:
        answer_lines = [
            f"这更像一条 `{direction}` 的政策线索，当前最直接的受益链条在 `{beneficiary_nodes[0]}`。",
            f"它现在更适合先按 `{stage}` 去跟踪，而不是立刻把所有相关标的都当成已兑现机会。",
        ]
    else:
        answer_lines = [
            f"这条政策目前方向偏 `{direction}`，但受益链条还不够清楚，暂时更适合先做政策跟踪。",
            f"在没有更明确执行细则前，不建议直接把它当成短线强催化。",
        ]

    evidence_lines = [
        f"[政策模板] 匹配主题 `{template.get('name', context.title)}`，置信度 `{matched.confidence_label}`。",
        f"[政策方向] 当前判断为 `{direction}`，阶段偏 `{stage}`。",
    ]
    evidence_lines.append(
        "[场景概率] 主场景（约 55%-60%）是政策继续停留在主题跟踪/细则消化阶段；真正进入强执行阶段，通常还要等时间线、配套细则或项目落地节点。"
    )
    if support_points:
        evidence_lines.append(f"[支持点] {support_points[0]}")
    if timeline_points:
        evidence_lines.append(f"[时间线] {timeline_points[0]}")
    if watchlist_impact:
        evidence_lines.append(f"[组合/观察池映射] {watchlist_impact[0]}")

    risk_lines = []
    if risk_nodes:
        risk_lines.append(f"主要风险点在 `{risk_nodes[0]}`，所以政策方向对，不等于标的马上兑现。")
    if not timeline_points:
        risk_lines.append("当前还没有抽到特别清楚的时间线，后续应继续等细则、申报或落地节点。")
    if context.source == "keyword":
        risk_lines.append("当前是按关键词做政策解释，不是完整原文逐段审读。")

    return {
        "answer_lines": answer_lines,
        "evidence_lines": evidence_lines,
        "provenance_lines": [
            f"[政策口径] 当前来源 `{context.source}`；如果不是完整 URL / 正文审读，就只能先按主题解释，不等于逐段原文解读。",
        ],
        "risk_lines": risk_lines,
    }


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


def _empty_portfolio_risk_payload(question: str) -> Dict[str, List[str]]:
    return {
        "answer_lines": [
            "你问的是组合风险，但当前没有录入可分析的持仓，所以现在还不能判断风险大不大。",
            "如果这是准备建仓的问题，先把计划持仓、权重或主题暴露列出来，再看集中度和场景风险。",
        ],
        "evidence_lines": [
            "[组合状态] 当前组合为空，暂时无法计算回撤、相关性、Beta、VaR 或压力场景。",
        ],
        "risk_lines": [
            "没有持仓数据时，任何“组合风险大不大”的结论都容易退化成泛泛市场判断。",
            "如果只是口头说组合偏科技、偏黄金或偏单一主题，也还不足以替代真实仓位分析。",
        ],
    }


def _asset_next_step_lines(snapshot: Mapping[str, Any]) -> List[str]:
    technical = dict(snapshot.get("technical") or {})
    ma_system = dict(technical.get("ma_system") or {})
    mas = dict(ma_system.get("mas") or {})
    last_close = float(dict(snapshot.get("metrics") or {}).get("last_close") or 0.0)
    ma20 = float(mas.get("MA20") or 0.0)
    ma60 = float(mas.get("MA60") or ma20 or 0.0)
    symbol = str(snapshot.get("symbol", "该标的"))
    bias = str(snapshot.get("bias", "分歧"))

    if bias == "偏强":
        anchor = ma20 or last_close
        return [
            f"如果你还没上车，更合理的是等 `{symbol}` 回踩 MA20 附近（约 {anchor:.3f}）再看承接，别在连续拉升后直接追价。",
        ]
    if bias == "偏弱":
        anchor = ma20 or ma60 or last_close
        return [
            f"如果你真要做左侧，至少先等 `{symbol}` 重新站回关键均线（约 {anchor:.3f}）或出现止跌信号，再谈加仓。",
        ]
    lower_anchor = ma20 or ma60 or last_close
    upper_anchor = ma60 or ma20 or last_close
    return [
        f"`{symbol}` 更像确认阶段，下一步要么等回踩 {lower_anchor:.3f} 一带有承接，要么等放量再走强后再跟。",
    ]


def _current_asset_event_digest_payload(symbol: str, config: Dict[str, Any]) -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    context = build_market_context(config, relevant_asset_types=[asset_type, "cn_etf", "futures"])
    intel_query = " ".join(
        part
        for part in (
            str(symbol).strip(),
            str(asset_type).strip(),
        )
        if part
    )
    context = _attach_shared_intel_news_report(
        context,
        config,
        query=intel_query or symbol,
        explicit_symbol=symbol,
        note_prefix="research asset intel: ",
    )
    analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=False)
    digest = summarize_event_digest_contract(build_event_digest(analysis))
    status = _safe_text(digest.get("status")) or "待补充"
    lead_layer = _safe_text(digest.get("lead_layer")) or "新闻"
    lead_detail = _safe_text(digest.get("lead_detail"))
    importance = _safe_text(digest.get("importance"))
    importance_reason = _safe_text(digest.get("importance_reason"))
    impact_summary = _safe_text(digest.get("impact_summary"))
    thesis_scope = _safe_text(digest.get("thesis_scope"))
    changed_what = _safe_text(digest.get("changed_what"))
    generated_at = _safe_text(analysis.get("generated_at"))
    payload = {
        "contract": digest,
        "evidence_lines": [],
        "provenance_lines": [
            (
                f"[事件消化] 当前事件快照 as_of `{generated_at or '—'}`；来源 `research 内部临时 scan`，"
                "只用于和 thesis ledger 比较，不替代正式成稿。"
            )
        ],
        "risk_lines": [],
    }
    if changed_what:
        payload["evidence_lines"].append(f"[事件消化] 当前 `{status}` / `{lead_layer}`：{changed_what}")
    if lead_detail or importance or impact_summary or thesis_scope or importance_reason:
        detail_parts = []
        if lead_detail:
            detail_parts.append(f"事件细分 `{lead_detail}`")
        if importance:
            detail_parts.append(f"当前优先级 `{importance}`")
        if impact_summary:
            detail_parts.append(f"更直接影响 `{impact_summary}`")
        if thesis_scope:
            detail_parts.append(f"当前更像 `{thesis_scope}`")
        if importance_reason:
            detail_parts.append(f"优先级判断：{importance_reason}")
        payload["evidence_lines"].append("[事件消化] " + "；".join(detail_parts) + "。")
    if status == "待复核":
        payload["risk_lines"].append("当前事件消化仍是 `待复核`，旧催化先别当成已经验证。")
    elif status == "待补充":
        payload["risk_lines"].append("当前事件层仍偏补证据，别把主题热度直接当成 thesis 已升级。")
    if thesis_scope == "待确认":
        payload["risk_lines"].append("当前事件更像 `待确认`，能解释边界，但还不能直接当 thesis 已改写。")
    elif thesis_scope == "一次性噪音":
        payload["risk_lines"].append("当前事件更像 `一次性噪音`，别把短线波动直接上升为 thesis 变化。")
    return payload


def _thesis_event_memory_payload(
    *,
    symbol: str,
    thesis_repo: ThesisRepository,
    current_event_digest: Mapping[str, Any] | None,
    source: str = "research",
    recorded_at: str = "",
) -> Dict[str, List[str]]:
    thesis = dict(thesis_repo.get(symbol) or {})
    if not thesis:
        return {}

    memory_lines: List[str] = []
    provenance_lines: List[str] = []
    risk_lines: List[str] = []
    action_lines: List[str] = []

    reason_parts: List[str] = []
    core_assumption = _safe_text(thesis.get("core_assumption") or thesis.get("core_hypothesis"))
    validation_metric = _safe_text(thesis.get("validation_metric"))
    holding_period = _safe_text(thesis.get("holding_period"))
    if core_assumption:
        reason_parts.append(f"`{core_assumption}`")
    if validation_metric:
        reason_parts.append(f"验证指标看 `{validation_metric}`")
    if holding_period:
        reason_parts.append(f"预期周期 `{holding_period}`")
    if reason_parts:
        memory_lines.append(f"上次为什么看：{'；'.join(reason_parts)}。")

    review_queue = dict(thesis_repo.load_review_queue() or {})
    review_history = dict(dict(review_queue.get("history") or {}).get(symbol) or {})

    previous_snapshot = dict(thesis.get("event_digest_snapshot") or {})
    previous_status = _safe_text(previous_snapshot.get("status"))
    previous_layer = _safe_text(previous_snapshot.get("lead_layer"))
    previous_detail = _safe_text(previous_snapshot.get("lead_detail"))
    previous_importance = _safe_text(previous_snapshot.get("importance"))
    previous_title = _safe_text(previous_snapshot.get("lead_title"))
    previous_impact_summary = _safe_text(previous_snapshot.get("impact_summary"))
    previous_thesis_scope = _safe_text(previous_snapshot.get("thesis_scope"))
    previous_importance_reason = _safe_text(previous_snapshot.get("importance_reason"))
    if previous_status or previous_layer or previous_title:
        previous_line = f"上次事件快照：`{previous_status or '待补充'}` / `{previous_layer or '新闻'}`"
        if previous_title:
            previous_line += f" / {previous_title}"
        memory_lines.append(previous_line + "。")
    if previous_detail or previous_importance or previous_impact_summary or previous_thesis_scope or previous_importance_reason:
        previous_detail_parts = []
        if previous_detail:
            previous_detail_parts.append(previous_detail)
        if previous_importance:
            previous_detail_parts.append(f"事件优先级 `{previous_importance}`")
        if previous_impact_summary:
            previous_detail_parts.append(f"更直接影响 `{previous_impact_summary}`")
        if previous_thesis_scope:
            previous_detail_parts.append(f"当前更像 `{previous_thesis_scope}`")
        if previous_importance_reason:
            previous_detail_parts.append(f"优先级判断：{previous_importance_reason}")
        memory_lines.append(f"上次事件细分：{'；'.join(previous_detail_parts)}。")

    current_snapshot = dict(current_event_digest or {})
    if current_snapshot:
        recorded = thesis_repo.record_event_digest(
            symbol,
            current_snapshot,
            source=source,
            recorded_at=recorded_at,
        )
        delta = dict(recorded.get("delta") or compare_event_digest_snapshots(previous_snapshot, current_snapshot))
        delta_summary = _safe_text(delta.get("summary"))
        if delta_summary:
            memory_lines.append(f"这次什么变了：{delta_summary}")
        state_transition = dict(recorded.get("state_transition") or {})
        thesis_state = _safe_text(state_transition.get("state"))
        thesis_state_trigger = _safe_text(state_transition.get("trigger"))
        thesis_state_summary = _safe_text(state_transition.get("summary"))
        if thesis_state:
            state_line = f"thesis 状态：`{thesis_state}`"
            if thesis_state_trigger:
                state_line += f"；触发：{thesis_state_trigger}"
            if thesis_state_summary:
                state_line += f"；{thesis_state_summary}"
            memory_lines.append(state_line)

        changed_what = _safe_text(current_snapshot.get("changed_what"))
        if changed_what:
            memory_lines.append(f"当前事件解释：{changed_what}")
        current_detail = _safe_text(current_snapshot.get("lead_detail"))
        current_importance = _safe_text(current_snapshot.get("importance"))
        current_impact_summary = _safe_text(current_snapshot.get("impact_summary"))
        current_thesis_scope = _safe_text(current_snapshot.get("thesis_scope"))
        current_importance_reason = _safe_text(current_snapshot.get("importance_reason"))
        if current_detail or current_importance or current_impact_summary or current_thesis_scope or current_importance_reason:
            current_detail_parts = []
            if current_detail:
                current_detail_parts.append(current_detail)
            if current_importance:
                current_detail_parts.append(f"事件优先级 `{current_importance}`")
            if current_impact_summary:
                current_detail_parts.append(f"更直接影响 `{current_impact_summary}`")
            if current_thesis_scope:
                current_detail_parts.append(f"当前更像 `{current_thesis_scope}`")
            if current_importance_reason:
                current_detail_parts.append(f"优先级判断：{current_importance_reason}")
            memory_lines.append(f"这次事件细分：{'；'.join(current_detail_parts)}。")

        next_step = _safe_text(current_snapshot.get("next_step"))
        if next_step:
            action_lines.append(f"thesis ledger 视角下，下一步先{next_step.rstrip('。')}。")

        recorded_snapshot = dict(recorded.get("snapshot") or {})
        recorded_at_text = _safe_text(recorded_snapshot.get("recorded_at")) or _safe_text(thesis.get("event_digest_updated_at")) or recorded_at
        if recorded_at_text:
            provenance_lines.append(f"[thesis ledger] 事件快照已写回 thesis，记录时间 `{recorded_at_text}`。")
        ledger_size = recorded.get("ledger_size")
        if ledger_size:
            provenance_lines.append(f"[thesis ledger] 当前累计 `{ledger_size}` 条事件记忆，可继续比较 thesis 是否被改写。")

        status = _safe_text(current_snapshot.get("status"))
        if status == "待复核":
            risk_lines.append("这次事件边界已经退回 `待复核`，原 thesis 先不要按旧催化继续加确定性。")
    elif previous_snapshot:
        memory_lines.append("这次还没拿到新的事件快照，当前先沿用上次 thesis 事件边界。")
        state_memory = summarize_thesis_state_snapshot(thesis)
        thesis_state = _safe_text(state_memory.get("state"))
        if thesis_state:
            state_line = f"thesis 状态：`{thesis_state}`"
            if _safe_text(state_memory.get("trigger")):
                state_line += f"；触发：{_safe_text(state_memory.get('trigger'))}"
            if _safe_text(state_memory.get("summary")):
                state_line += f"；{_safe_text(state_memory.get('summary'))}"
            memory_lines.append(state_line)
        action_lines.append("若要比较“这次到底变了什么”，先补一次 scan 或事件消化快照。")

    if review_history:
        history_lines = summarize_review_queue_history_lines(review_history)
        for line in history_lines:
            if line.startswith(("最近正式稿状态:", "正式稿跟进说明:", "最近复查动作:", "最近复查结果:", "最近复查时间:")):
                memory_lines.append(line)
        followup = dict(review_history.get("report_followup") or {})
        followup_status = _safe_text(followup.get("status"))
        followup_reason = _safe_text(followup.get("reason"))
        if followup_status in {"待更新正式稿", "已有复查稿，暂无正式稿"}:
            action_lines.append("旧正式稿当前不能直接沿用；下一步先补新的 final / client-final，把这次复查结果正式回写。")
        elif followup_status == "需复查":
            action_lines.append("旧正式稿仍挂着待复查状态，当前先别把上次 final 当成最新结论。")
        elif followup_reason and followup_status == "已复核":
            provenance_lines.append(f"[正式稿跟进] {followup_reason}")

    return {
        "memory_lines": memory_lines,
        "provenance_lines": provenance_lines,
        "risk_lines": risk_lines,
        "action_lines": action_lines,
    }


def _trade_plan_focus(question: str) -> Dict[str, bool]:
    ask_position = any(
        keyword in question
        for keyword in (
            "仓位",
            "买多少",
            "几成",
            "多大",
            "上多少",
            "重仓",
            "梭哈",
            "配置多少",
            "分批",
            "首笔",
        )
    )
    ask_execution = any(
        keyword in question
        for keyword in (
            "做得进去",
            "流动性",
            "滑点",
            "成交",
            "执行",
            "费率",
            "成本",
            "参与率",
            "冲击",
        )
    )
    return {
        "needs_trade_plan": ask_position or ask_execution,
        "ask_position": ask_position,
        "ask_execution": ask_execution,
    }


def _trade_plan_action(question: str) -> str:
    if any(keyword in question for keyword in ("卖", "减仓", "止损", "止盈", "先出", "先减")):
        return "sell"
    return "buy"


def _reference_trade_amount(snapshot: Mapping[str, Any], holdings: Sequence[Mapping[str, Any]]) -> float:
    asset_type = str(snapshot.get("asset_type", ""))
    last_close = float(dict(snapshot.get("metrics") or {}).get("last_close") or 0.0)
    base_amount = {
        "cn_etf": 20_000.0,
        "cn_fund": 20_000.0,
        "cn_stock": 30_000.0,
        "hk": 30_000.0,
        "hk_index": 25_000.0,
        "us": 30_000.0,
    }.get(asset_type, 20_000.0)
    total_value = sum(float(item.get("market_value", 0.0) or 0.0) for item in holdings)
    if total_value > 0:
        base_amount = max(base_amount, total_value * 0.10)
    minimum_lot = max(last_close * 100, 1_000.0)
    return max(base_amount, minimum_lot)


def _asset_trade_plan_payload(
    *,
    question: str,
    snapshot: Mapping[str, Any],
    repo: PortfolioRepository,
    config: Dict[str, Any],
) -> Dict[str, List[str]]:
    focus = _trade_plan_focus(question)
    if not focus["needs_trade_plan"]:
        return {}

    holdings = repo.list_holdings()
    action = _trade_plan_action(question)
    amount = _reference_trade_amount(snapshot, holdings)
    symbol = str(snapshot.get("symbol", ""))
    metrics = dict(snapshot.get("metrics") or {})
    payload = build_trade_plan(
        action=action,
        symbol=symbol,
        price=float(metrics.get("last_close") or 0.0),
        amount=amount,
        config=config,
        asset_type=str(snapshot.get("asset_type") or ""),
        repo=repo,
        thesis_repo=ThesisRepository(),
        analysis=snapshot,
    )

    execution = dict(payload.get("execution") or {})
    decision = dict(payload.get("decision_snapshot") or {})
    horizon = dict(payload.get("horizon") or {})
    alerts = list(payload.get("alerts") or [])
    participation = execution.get("participation_rate")
    participation_text = "—" if participation is None else f"{float(participation) * 100:.2f}%"
    has_holdings = bool(holdings)
    answer_lines: List[str] = []

    if focus["ask_position"]:
        if has_holdings:
            if float(payload.get("projected_weight", 0.0)) <= float(payload.get("suggested_max_weight", 0.0)) + 1e-9:
                answer_lines.append(
                    f"按约 `{amount:.0f}` 的示意单预演，`{symbol}` 买入后仓位约 `{float(payload.get('projected_weight', 0.0)) * 100:.1f}%`，"
                    f"还没有明显超过更合理的单票上限 `{float(payload.get('suggested_max_weight', 0.0)) * 100:.1f}%`。"
                )
            else:
                answer_lines.append(
                    f"按约 `{amount:.0f}` 的示意单预演，`{symbol}` 买入后仓位约 `{float(payload.get('projected_weight', 0.0)) * 100:.1f}%`，"
                    f"已经高于更合理的单票上限 `{float(payload.get('suggested_max_weight', 0.0)) * 100:.1f}%`，不适合直接重仓。"
                )
        else:
            answer_lines.append(
                f"如果这是空仓首笔，更像先从 `{float(payload.get('suggested_max_weight', 0.0)) * 100:.1f}%` 以内试仓，"
                "而不是一上来把它当成重仓核心。"
            )

    if focus["ask_execution"]:
        answer_lines.append(
            f"从可成交性看，它当前更像 `{execution.get('tradability_label', '—')}`，"
            f"示意单的预估总成本约 `{float(execution.get('estimated_total_cost', 0.0)):.2f}`。"
        )

    evidence_lines = [
        f"[周期判断] 当前更适合按 `{horizon.get('label', '观察期')}` 处理：{horizon.get('fit_reason', '当前周期未单独标注。')}",
        f"[仓位预演] 当前按约 `{amount:.0f}` 的示意单预演；更合理的单票上限约 `{float(payload.get('suggested_max_weight', 0.0)) * 100:.1f}%`。",
        (
            f"[执行成本] 近20日日均成交额约 `{float(execution.get('avg_turnover_20d', 0.0)) / 1e8:.2f} 亿`，"
            f"订单参与率约 `{participation_text}`，滑点 `{float(execution.get('slippage_bps', 0.0)):.1f} bps`。"
        ),
        (
            f"[组合影响] 组合年化波动预估 `{float(dict(payload.get('current_risk') or {}).get('annual_vol', 0.0)) * 100:.2f}% -> "
            f"{float(dict(payload.get('projected_risk') or {}).get('annual_vol', 0.0)) * 100:.2f}%`，"
            f"Beta `{float(dict(payload.get('current_risk') or {}).get('beta', 0.0)):.2f} -> "
            f"{float(dict(payload.get('projected_risk') or {}).get('beta', 0.0)):.2f}`。"
        ),
        f"[时点快照] 行情 as_of `{decision.get('market_data_as_of') or '—'}`，来源 `{decision.get('market_data_source') or symbol}`。",
    ]

    risk_lines = list(alerts[:3])
    if horizon.get("misfit_reason"):
        risk_lines.append(f"周期错配风险：{horizon.get('misfit_reason')}")
    liquidity_note = str(execution.get("liquidity_note", "") or "").strip()
    execution_note = str(execution.get("execution_note", "") or "").strip()
    if liquidity_note:
        risk_lines.append(liquidity_note)
    if execution_note:
        risk_lines.append(execution_note)
    risk_lines.extend(str(item).strip() for item in (decision.get("notes") or [])[:2] if str(item).strip())

    action_lines = [
        f"如果你要按真实金额落单，下一步直接跑 `portfolio whatif {action} {symbol} {float(metrics.get('last_close') or 0.0):.4f} 真实金额`。",
        "真正下单前，先把计划仓位和已有持仓录进 `portfolio`，这样仓位和相关性约束才会按真实组合来算。",
    ]
    return {
        "answer_lines": answer_lines[:2],
        "evidence_lines": evidence_lines,
        "provenance_lines": [
            f"[交易时点] 行情 as_of `{decision.get('market_data_as_of') or '—'}`，来源 `{decision.get('market_data_source') or symbol}`。",
            "[时点边界] 仓位和执行预演只使用当时可见的日线、持仓和 thesis 快照，不回看未来新闻或财报。"
            if decision
            else "[时点边界] 当前仓位预演只能按现有缓存与配置理解，缺少更完整时点快照。",
        ],
        "risk_lines": risk_lines,
        "action_lines": action_lines,
    }


def _direct_answer_lines(
    intent: ResearchIntent,
    snapshots: List[Dict[str, Any]],
    regime_lines: List[str],
    flow_lines: List[str],
    risk_lines: List[str],
    contextual_answer_lines: Sequence[str] | None = None,
    prefer_contextual_for_asset: bool = False,
) -> List[str]:
    lines: List[str] = []
    if intent.kind == "portfolio_risk":
        if contextual_answer_lines:
            return [str(item).strip() for item in contextual_answer_lines if str(item).strip()][:2]
        if risk_lines:
            lines.append(risk_lines[0])
        if snapshots:
            lines.append(f"如果你的问题同时关心标的方向，当前优先先处理 `{snapshots[0]['symbol']}` 对组合风险的贡献，再谈加仓。")
        return lines[:2] or ["这本质上是组合风控题，先看相关性、回撤和场景暴露。"]

    if contextual_answer_lines and (intent.kind != "asset_thesis" or prefer_contextual_for_asset):
        return [str(item).strip() for item in contextual_answer_lines if str(item).strip()][:2]

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


def _pick_evidence_lines(intent: ResearchIntent, evidence_groups: Mapping[str, Sequence[str]], limit: int = 5) -> List[str]:
    group_weights = {
        "asset_thesis": {"snapshot": 90, "flow": 70, "regime": 55, "market": 45, "risk": 40, "policy": 50},
        "portfolio_risk": {"risk": 95, "snapshot": 65, "regime": 50, "market": 35, "flow": 30, "policy": 25},
        "market_diagnosis": {"market": 95, "flow": 80, "regime": 60, "snapshot": 30, "risk": 25, "policy": 20},
        "policy_impact": {"policy": 95, "regime": 55, "market": 35, "flow": 25, "snapshot": 20, "risk": 20},
        "macro_regime": {"regime": 95, "market": 65, "flow": 45, "snapshot": 20, "risk": 20, "policy": 20},
        "open_research": {"market": 70, "regime": 65, "flow": 55, "snapshot": 45, "risk": 40, "policy": 35},
    }
    weights = group_weights.get(intent.kind, group_weights["open_research"])

    def _line_score(group: str, text: str) -> int:
        score = int(weights.get(group, 10))
        if text.startswith("[场景概率]"):
            score += 18
        if text.startswith("[市场主线]") or text.startswith("[市场宽度]"):
            score += 16 if intent.kind == "market_diagnosis" else 8
        if text.startswith("[行情/技术]"):
            score += 16 if intent.kind == "asset_thesis" else 6
        if text.startswith("[组合权重]") or text.startswith("[压力场景]") or text.startswith("[相关性]"):
            score += 16 if intent.kind == "portfolio_risk" else 6
        if text.startswith("[政策模板]") or text.startswith("[政策方向]") or text.startswith("[时间线]"):
            score += 16 if intent.kind == "policy_impact" else 8
        if intent.kind == "asset_thesis" and (text.startswith("[政策方向]") or text.startswith("[政策模板]")):
            score += 12
        if "代理置信度" in text:
            score += 6
        return score

    ranked: List[tuple[int, int, str]] = []
    ordinal = 0
    for group, items in evidence_groups.items():
        for item in items:
            text = str(item).strip()
            if not text:
                continue
            ranked.append((_line_score(group, text), ordinal, text))
            ordinal += 1

    ranked.sort(key=lambda item: (-item[0], item[1]))
    return _dedupe_lines((item[2] for item in ranked), max_items=limit)


def _render_research_proxy_section(proxy_contract: Mapping[str, Any]) -> List[str]:
    contract = dict(proxy_contract or {})
    if not contract:
        return []
    market_flow = dict(contract.get("market_flow") or {})
    social = dict(contract.get("social_sentiment") or {})
    lines = [
        "## 代理信号与限制",
        "",
        (
            "- 市场风格代理："
            f"{market_flow.get('interpretation', '当前没有形成稳定的市场风格代理结论。')}"
            f"（置信度 `{market_flow.get('confidence_label', '低')}`，覆盖 `{market_flow.get('coverage_summary', '无有效代理样本')}`）。"
        ),
        (
            "- 情绪代理："
            f"已覆盖 `{social.get('covered', 0)}/{social.get('total', 0)}` 个样本，"
            f"置信度分布 `{social.get('confidence_labels', {}) or {'低': 0}}`。"
        ),
    ]
    limitation = str(market_flow.get("limitation") or social.get("limitation") or "").strip()
    if limitation:
        lines.append(f"- 主要限制：{limitation}")
    downgrade = str(market_flow.get("downgrade_impact") or social.get("downgrade_impact") or "").strip()
    if downgrade:
        lines.append(f"- 降级影响：{downgrade}")
    return lines


def _render_research_markdown(
    *,
    question: str,
    intent: ResearchIntent,
    symbols: List[str],
    direct_answer_lines: List[str],
    proxy_contract: Mapping[str, Any],
    evidence_lines: List[str],
    provenance_lines: List[str],
    risk_lines: List[str],
    action_lines: List[str],
    thesis_memory_lines: List[str] | None = None,
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

    proxy_lines = _render_research_proxy_section(proxy_contract)
    if proxy_lines:
        lines.extend(["", *proxy_lines])

    thesis_lines = [item for item in list(thesis_memory_lines or []) if _safe_text(item)]
    if thesis_lines:
        lines.extend(["", "## 研究记忆 / Thesis Ledger"])
        for item in thesis_lines:
            lines.append(f"- {item}")

    lines.extend(["", "## 证据"])
    for item in evidence_lines or ["当前没有拿到足够证据，建议先缩小问题范围或指定标的。"]:
        lines.append(f"- {item}")

    lines.extend(["", "## 证据时点与来源"])
    for item in provenance_lines or ["当前还没有补齐统一的证据时点与来源说明，先把这次回答当成辅助研究结果。"]:
        lines.append(f"- {item}")

    lines.extend(["", "## 风险与不确定性"])
    for item in risk_lines or ["当前回答更多是框架判断，缺少更细的事件、盘口或持仓上下文。"]:
        lines.append(f"- {item}")

    lines.extend(["", "## 下一步"])
    for item in action_lines:
        lines.append(f"- {item}")
    return "\n".join(lines)


def _research_review_output_path(symbol: str, question: str, recorded_at: str = "") -> Path:
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    question_text = _safe_text(question)
    topic = "bootstrap" if "值得继续跟踪" in question_text else "review"
    stamp = _safe_text(recorded_at) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_key = stamp.replace(":", "").replace(" ", "_")[:17] or datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return resolve_project_path("reports/research/internal") / f"research_{safe_symbol}_{topic}_{date_key}.md"


def run_research_review(
    symbol: str,
    *,
    question: str = "这次什么变了",
    config_path: str = "",
    thesis_repo: ThesisRepository | None = None,
    recorded_at: str = "",
) -> Dict[str, Any]:
    config = load_config(config_path or None)
    setup_logger("ERROR")
    thesis_repo = thesis_repo or ThesisRepository()
    intent = ResearchIntent("asset_thesis", "标的研究 / 交易问题", False, False, False)
    question_text = f"{symbol} {question}".strip()
    stamp = _safe_text(recorded_at) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    evidence_groups: "OrderedDict[str, List[str]]" = OrderedDict(
        [("market", []), ("regime", []), ("flow", []), ("snapshot", []), ("risk", []), ("policy", [])]
    )
    provenance_groups: "OrderedDict[str, List[str]]" = OrderedDict(
        [("market", []), ("snapshot", []), ("risk", []), ("policy", [])]
    )
    risk_lines: List[str] = []
    action_lines: List[str] = []
    thesis_memory_lines: List[str] = []
    proxy_contract: Dict[str, Any] = {}
    snapshots: List[Dict[str, Any]] = []

    try:
        snapshot = _symbol_snapshot(symbol, config)
        snapshots.append(snapshot)
        evidence_groups["snapshot"].extend(f"[行情/技术] {item}" for item in snapshot["evidence_lines"])
        evidence_groups["snapshot"].extend(list(snapshot.get("scenario_lines") or []))
        provenance_groups["snapshot"].extend(list(snapshot.get("provenance_lines") or []))
        risk_lines.extend(list(snapshot.get("risks") or []))
    except Exception as exc:
        risk_lines.append(f"{symbol}: 数据拉取失败，当前先回退到 thesis / 事件复核。{exc}")

    thesis_record = thesis_repo.get(symbol)
    event_digest_payload: Dict[str, Any] = {}
    try:
        event_digest_payload = _current_asset_event_digest_payload(symbol, config)
        evidence_groups["snapshot"].extend(list(event_digest_payload.get("evidence_lines") or []))
        provenance_groups["snapshot"].extend(list(event_digest_payload.get("provenance_lines") or []))
        risk_lines.extend(list(event_digest_payload.get("risk_lines") or []))
        if thesis_record:
            memory_payload = _thesis_event_memory_payload(
                symbol=symbol,
                thesis_repo=thesis_repo,
                current_event_digest=event_digest_payload.get("contract") or {},
                source="research_review",
                recorded_at=stamp,
            )
            thesis_memory_lines = list(memory_payload.get("memory_lines") or [])
            provenance_groups["snapshot"].extend(list(memory_payload.get("provenance_lines") or []))
            risk_lines.extend(list(memory_payload.get("risk_lines") or []))
            action_lines.extend(list(memory_payload.get("action_lines") or []))
        else:
            action_lines.append(
                f"`{symbol}` 还没有 thesis 记录；如果你准备持续跟踪，先用 `portfolio thesis set` 补上核心假设。"
            )
    except Exception as exc:
        if thesis_record:
            memory_payload = _thesis_event_memory_payload(
                symbol=symbol,
                thesis_repo=thesis_repo,
                current_event_digest={},
            )
            thesis_memory_lines = list(memory_payload.get("memory_lines") or [])
            provenance_groups["snapshot"].extend(list(memory_payload.get("provenance_lines") or []))
            risk_lines.extend(list(memory_payload.get("risk_lines") or []))
            action_lines.extend(list(memory_payload.get("action_lines") or []))
        risk_lines.append(f"{symbol}: thesis 事件快照刷新失败，当前先沿用上次记忆。{exc}")
        provenance_groups["snapshot"].append(
            f"[事件消化] `{symbol}` 当前没拉到新的事件快照，research review 先回退到上次记录。"
        )

    evidence_lines = _pick_evidence_lines(intent, evidence_groups)
    provenance_lines = _pick_evidence_lines(intent, provenance_groups, limit=5)

    direct_answer_lines: List[str] = []
    delta_line = next((item for item in thesis_memory_lines if item.startswith("这次什么变了：")), "")
    if delta_line:
        direct_answer_lines.append(delta_line.replace("这次什么变了：", "", 1).strip())
    if not direct_answer_lines:
        event_line = next((str(item).strip() for item in list(event_digest_payload.get("evidence_lines") or []) if str(item).strip()), "")
        if event_line:
            direct_answer_lines.append(event_line.replace("[事件消化] ", "", 1))
    if not direct_answer_lines and snapshots:
        direct_answer_lines.append(f"{symbol}: {snapshots[0]['answer']}")
    if not direct_answer_lines:
        direct_answer_lines.append(f"{symbol}: 当前先按 thesis review 补事件边界和下一步研究动作。")

    if snapshots:
        action_lines.extend(_asset_next_step_lines(snapshots[0]))
        action_lines.append(f"如果你要看更完整的逻辑、风险和执行框架，再跑 `{symbol}` 对应的 `scan`。")
    if not action_lines:
        action_lines.append("如果要把这次复查落成下一步动作，先补 scan 或 thesis。")

    evidence_lines = _dedupe_lines(evidence_lines, max_items=6)
    provenance_lines = _dedupe_lines(provenance_lines, max_items=5)
    risk_lines = _dedupe_lines(risk_lines, max_items=6)
    action_lines = _dedupe_lines(action_lines, max_items=5)
    direct_answer_lines = _dedupe_lines(direct_answer_lines, max_items=3)
    markdown = _render_research_markdown(
        question=question_text,
        intent=intent,
        symbols=[symbol],
        direct_answer_lines=direct_answer_lines,
        proxy_contract=proxy_contract,
        evidence_lines=evidence_lines,
        provenance_lines=provenance_lines,
        risk_lines=risk_lines,
        action_lines=action_lines,
        thesis_memory_lines=thesis_memory_lines,
    )
    artifact_path = _research_review_output_path(symbol, question, stamp)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(markdown, encoding="utf-8")
    return {
        "symbol": symbol,
        "question": question_text,
        "artifact_path": str(artifact_path),
        "markdown": markdown,
        "summary": direct_answer_lines[0] if direct_answer_lines else "",
        "event_digest": dict(event_digest_payload.get("contract") or {}),
        "thesis_memory_lines": thesis_memory_lines,
    }


def main() -> None:
    args = build_parser().parse_args()
    setup_logger("ERROR")
    config = load_config(args.config or None)
    question = " ".join(args.question).strip()
    repo = PortfolioRepository()
    thesis_repo = ThesisRepository()
    holdings = repo.list_holdings()
    watchlist = load_watchlist()
    candidate_symbols = [item["symbol"] for item in watchlist] + [item["symbol"] for item in holdings]
    symbols = _resolve_symbols(question, config, candidate_symbols)
    intent = _classify_question(question, symbols, has_holdings=bool(holdings))

    snapshots: List[Dict[str, Any]] = []
    evidence_groups: "OrderedDict[str, List[str]]" = OrderedDict(
        [("market", []), ("regime", []), ("flow", []), ("snapshot", []), ("risk", []), ("policy", [])]
    )
    provenance_groups: "OrderedDict[str, List[str]]" = OrderedDict(
        [("market", []), ("snapshot", []), ("risk", []), ("policy", [])]
    )
    risk_lines: List[str] = []
    action_lines: List[str] = []
    proxy_contract: Dict[str, Any] = {}
    regime_lines: List[str] = _regime_lines(config) if intent.needs_regime else []
    flow_lines: List[str] = []
    contextual_answer_lines: List[str] = []
    prefer_contextual_for_asset = False
    thesis_memory_lines: List[str] = []

    if regime_lines:
        evidence_groups["regime"].extend(f"[宏观] {item}" for item in regime_lines)

    if intent.kind in {"market_diagnosis", "open_research", "macro_regime"} or (intent.needs_flow and not symbols):
        try:
            market_payload = _market_diagnosis_payload(config)
            contextual_answer_lines = list(market_payload.get("answer_lines") or [])
            evidence_groups["market"].extend(list(market_payload.get("evidence_lines") or []))
            provenance_groups["market"].extend(list(market_payload.get("provenance_lines") or []))
            risk_lines.extend(list(market_payload.get("risk_lines") or []))
            proxy_contract = dict(market_payload.get("proxy_contract") or proxy_contract)
        except Exception as exc:
            risk_lines.append(f"市场诊断上下文暂时拉取失败，当前先回退到宏观框架判断。{exc}")

    if symbols:
        for symbol in symbols[:3]:
            try:
                snapshot = _symbol_snapshot(symbol, config)
                snapshots.append(snapshot)
                evidence_groups["snapshot"].extend(f"[行情/技术] {item}" for item in snapshot["evidence_lines"])
                evidence_groups["snapshot"].extend(list(snapshot.get("scenario_lines") or []))
                provenance_groups["snapshot"].extend(list(snapshot.get("provenance_lines") or []))
                risk_lines.extend(snapshot["risks"])
            except Exception as exc:
                risk_lines.append(f"{symbol}: 数据拉取失败，暂时无法做研究快照。{exc}")

    if intent.kind == "asset_thesis" and snapshots:
        trade_payload = _asset_trade_plan_payload(
            question=question,
            snapshot=snapshots[0],
            repo=repo,
            config=config,
        )
        if trade_payload:
            contextual_answer_lines = list(trade_payload.get("answer_lines") or contextual_answer_lines)
            prefer_contextual_for_asset = True
            evidence_groups["risk"].extend(list(trade_payload.get("evidence_lines") or []))
            provenance_groups["risk"].extend(list(trade_payload.get("provenance_lines") or []))
            risk_lines.extend(list(trade_payload.get("risk_lines") or []))
            action_lines.extend(list(trade_payload.get("action_lines") or []))

        lead_symbol = str(snapshots[0].get("symbol") or (symbols[0] if symbols else ""))
        if lead_symbol:
            thesis_record = thesis_repo.get(lead_symbol)
            if thesis_record:
                try:
                    event_digest_payload = _current_asset_event_digest_payload(lead_symbol, config)
                    evidence_groups["snapshot"].extend(list(event_digest_payload.get("evidence_lines") or []))
                    provenance_groups["snapshot"].extend(list(event_digest_payload.get("provenance_lines") or []))
                    risk_lines.extend(list(event_digest_payload.get("risk_lines") or []))
                    memory_payload = _thesis_event_memory_payload(
                        symbol=lead_symbol,
                        thesis_repo=thesis_repo,
                        current_event_digest=event_digest_payload.get("contract") or {},
                        source="research",
                        recorded_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                except Exception as exc:
                    memory_payload = _thesis_event_memory_payload(
                        symbol=lead_symbol,
                        thesis_repo=thesis_repo,
                        current_event_digest={},
                    )
                    risk_lines.append(f"{lead_symbol}: thesis 事件快照刷新失败，当前先沿用上次记忆。{exc}")
                    provenance_groups["snapshot"].append(
                        f"[事件消化] `{lead_symbol}` 当前没拉到新的事件快照，thesis ledger 先回退到上次记录。"
                    )
                thesis_memory_lines = list(memory_payload.get("memory_lines") or [])
                provenance_groups["snapshot"].extend(list(memory_payload.get("provenance_lines") or []))
                risk_lines.extend(list(memory_payload.get("risk_lines") or []))
                action_lines.extend(list(memory_payload.get("action_lines") or []))
            else:
                action_lines.append(
                    f"`{lead_symbol}` 还没有 thesis 记录；如果你准备持续跟踪，先用 `portfolio thesis set` 补上核心假设。"
                )

    if intent.needs_flow and symbols:
        flow_payload = _flow_and_sentiment_payload(symbols, config)
        flow_lines = list(flow_payload.get("evidence_lines") or [])
        evidence_groups["flow"].extend(f"[资金/情绪代理] {item}" for item in flow_lines)
        risk_lines.extend(list(flow_payload.get("risk_lines") or []))
        proxy_contract = dict(flow_payload.get("proxy_contract") or proxy_contract)

    if intent.needs_risk and holdings:
        context = build_portfolio_risk_context(config, repo=repo)
        if context.weights:
            analyzer = RiskAnalyzer(context.returns_df[list(context.weights)], context.weights)
            report = analyzer.generate_risk_report(context.benchmark_returns)
            portfolio_payload = _portfolio_risk_payload(
                question=question,
                context=context,
                report=report,
                analyzer=analyzer,
                config=config,
            )
            if intent.kind == "portfolio_risk":
                contextual_answer_lines = list(portfolio_payload.get("answer_lines") or contextual_answer_lines)
            evidence_groups["risk"].extend(list(portfolio_payload.get("evidence_lines") or []))
            provenance_groups["risk"].extend(list(portfolio_payload.get("provenance_lines") or []))
            risk_lines.extend(list(portfolio_payload.get("risk_lines") or []))
            action_lines.extend(list(portfolio_payload.get("action_lines") or []))
            risk_lines.append(report["max_drawdown"]["interpretation"])
            risk_lines.append(report["var_95"]["interpretation"])
            correlation_lines = _top_correlation_lines(analyzer)
            scenario_lines = _scenario_lines(question, context, analyzer, config)
            risk_lines.extend(correlation_lines)
            risk_lines.extend(scenario_lines)
            evidence_groups["risk"].extend(f"[组合风险] {item}" for item in correlation_lines[:2])
            evidence_groups["risk"].extend(f"[压力场景] {item}" for item in scenario_lines[:1])
            high_corr = report.get("concentration_alerts", [])
            if high_corr:
                risk_lines.append(f"集中度提醒: {high_corr[0]['warning']}")
        else:
            risk_lines.extend(context.coverage_notes[:2])
    elif intent.kind == "portfolio_risk":
        empty_payload = _empty_portfolio_risk_payload(question)
        contextual_answer_lines = list(empty_payload.get("answer_lines") or contextual_answer_lines)
        evidence_groups["risk"].extend(list(empty_payload.get("evidence_lines") or []))
        risk_lines.extend(list(empty_payload.get("risk_lines") or []))

    if intent.kind == "policy_impact":
        policy_payload = _policy_payload(question, holdings)
        contextual_answer_lines = list(policy_payload.get("answer_lines") or contextual_answer_lines)
        evidence_groups["policy"].extend(list(policy_payload.get("evidence_lines") or []))
        provenance_groups["policy"].extend(list(policy_payload.get("provenance_lines") or []))
        risk_lines.extend(list(policy_payload.get("risk_lines") or []))
    elif _contains_policy_keywords(question):
        policy_payload = _policy_payload(question, holdings)
        evidence_groups["policy"].extend(list(policy_payload.get("evidence_lines") or []))
        provenance_groups["policy"].extend(list(policy_payload.get("provenance_lines") or []))
        risk_lines.extend(list(policy_payload.get("risk_lines") or []))

    evidence_lines = _pick_evidence_lines(intent, evidence_groups)
    provenance_lines = _pick_evidence_lines(intent, provenance_groups, limit=5)

    if not evidence_lines:
        if symbols:
            risk_lines.append("已识别相关标的，但当前缓存和行情数据不足，建议先单独运行 scan。")
        else:
            risk_lines.append("问题里没有识别到明确标的，当前只能先给框架性判断。")

    direct_answer_lines = _direct_answer_lines(
        intent,
        snapshots,
        regime_lines,
        flow_lines,
        risk_lines,
        contextual_answer_lines=contextual_answer_lines,
        prefer_contextual_for_asset=prefer_contextual_for_asset,
    )
    direct_answer_lines = _dedupe_lines(direct_answer_lines, max_items=3)

    if snapshots and intent.kind == "asset_thesis":
        action_lines.extend(_asset_next_step_lines(snapshots[0]))
        action_lines.append(f"如果你要看更完整的逻辑、风险和执行框架，再跑 `{snapshots[0]['symbol']}` 对应的 `scan`。")
    elif symbols and intent.kind == "asset_thesis":
        action_lines.append("若要继续深入，可先跑对应 `scan` 看完整分析卡。")
    elif intent.kind == "portfolio_risk" and not holdings:
        action_lines.append("先把真实持仓或计划仓位补出来，再判断组合是集中度问题、相关性问题，还是场景暴露问题。")
        action_lines.append("如果你已经有持仓记录但这里显示为空，先检查 `portfolio` 里的持仓录入是否完整。")
    elif intent.kind in {"market_diagnosis", "open_research"}:
        action_lines.append("如果你准备落到交易，下一步先把问题收窄成主题、候选池或持仓暴露，而不是继续泛泛讨论市场。")
    elif intent.kind == "policy_impact":
        action_lines.append("如果你要把政策线索落到交易，下一步先缩小到受益链条、候选标的或持仓映射。")
        if symbols:
            action_lines.append(f"若要继续落到标的层，可再跑 `{symbols[0]}` 对应的 `scan` 看执行框架。")
    if intent.needs_flow:
        action_lines.append("如果想系统看风格轮动，可直接跑 `briefing daily` 或 `discover`。")
    if intent.needs_risk and holdings:
        action_lines.append("若想量化极端场景，再跑一次 `risk stress` 看具体持仓贡献。")
    if intent.kind in {"macro_regime", "open_research", "market_diagnosis"}:
        action_lines.append("如果你把问题收窄到标的、主题或场景，研究回答会明显更聚焦。")
    if not action_lines:
        action_lines.append("如果你给出更明确的标的或场景，研究回答会更聚焦。")
    evidence_lines = _dedupe_lines(evidence_lines, max_items=6)
    provenance_lines = _dedupe_lines(provenance_lines, max_items=5)
    risk_lines = _dedupe_lines(risk_lines, max_items=6)
    action_lines = _dedupe_lines(action_lines, max_items=5)
    print(
        _render_research_markdown(
            question=question,
            intent=intent,
            symbols=symbols[:3],
            direct_answer_lines=direct_answer_lines,
            proxy_contract=proxy_contract,
            evidence_lines=evidence_lines,
            provenance_lines=provenance_lines,
            risk_lines=risk_lines,
            action_lines=action_lines,
            thesis_memory_lines=thesis_memory_lines,
        )
    )


if __name__ == "__main__":
    main()
