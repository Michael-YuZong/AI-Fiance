"""Opportunity discovery command."""

from __future__ import annotations

import argparse
import io
import warnings
from contextlib import redirect_stderr
from datetime import datetime
from typing import Any, Dict, List

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

from src.collectors import GlobalFlowCollector, SocialSentimentCollector
from src.output.alert import AlertRenderer
from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.policy_engine import PolicyEngine
from src.processors.regime import RegimeDetector
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import load_config
from src.utils.data import load_watchlist
from src.utils.logger import setup_logger
from src.utils.market import compute_history_metrics, fetch_asset_history, format_pct


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan watchlist and surface opportunities.")
    parser.add_argument("event", nargs="?", default="", help="Optional event keyword, e.g. 电网 or 算力")
    parser.add_argument("--top", type=int, default=5, help="Number of candidates to show")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def _regime_alignment(item: Dict[str, Any], preferred_assets: List[str]) -> int:
    sector = item.get("sector", "")
    asset_type = item.get("asset_type", "")
    if "港股科技" in preferred_assets and asset_type == "hk_index":
        return 2
    if "黄金" in preferred_assets and sector == "黄金":
        return 2
    if "成长股" in preferred_assets and sector == "科技":
        return 1
    if "铜" in preferred_assets and "铜铝" in item.get("chain_nodes", []):
        return 1
    return 0


def main() -> None:
    args = build_parser().parse_args()
    setup_logger("ERROR")
    config = load_config(args.config or None)
    watchlist = load_watchlist()
    china_macro = load_china_macro_snapshot(config)
    global_proxy = {}
    try:
        with redirect_stderr(io.StringIO()):
            global_proxy = load_global_proxy_snapshot()
    except Exception:
        global_proxy = {}
    regime_inputs = derive_regime_inputs(china_macro, global_proxy)
    regime = RegimeDetector(regime_inputs).detect_regime()
    engine = PolicyEngine()
    event_policy = engine.best_match(args.event) if args.event else None

    candidates = []
    alerts = []
    snapshots: List[Dict[str, Any]] = []
    social_collector = SocialSentimentCollector(config)
    for item in watchlist:
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(item["symbol"], item["asset_type"], config))
            metrics = compute_history_metrics(history)
            technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
            score = 0
            reasons = []

            if technical["ma_system"]["signal"] == "bullish" and technical["macd"]["signal"] == "bullish":
                score += 2
                reasons.append("趋势和动量同向偏多")
            elif technical["ma_system"]["signal"] == "bearish":
                score -= 1
                reasons.append("趋势偏弱")

            if metrics["return_20d"] > 0.08:
                score += 1
                reasons.append(f"近20日涨幅 {format_pct(metrics['return_20d'])}")
            if technical["volume"]["vol_ratio"] > 1.2:
                score += 1
                reasons.append(f"量比 {technical['volume']['vol_ratio']:.2f}")
            if metrics["return_5d"] < -0.05:
                score -= 1
                reasons.append("近5日回撤偏大")

            sentiment = social_collector.collect(
                item["symbol"],
                {
                    "return_1d": metrics["return_1d"],
                    "return_5d": metrics["return_5d"],
                    "return_20d": metrics["return_20d"],
                    "volume_ratio": technical["volume"]["vol_ratio"],
                    "trend": "多头" if technical["ma_system"]["signal"] == "bullish" else "空头" if technical["ma_system"]["signal"] == "bearish" else "震荡",
                },
            )
            sentiment_index = float(sentiment["aggregate"]["sentiment_index"])
            if 35 <= sentiment_index <= 75:
                score += 1
                reasons.append(f"情绪代理 {sentiment_index:.1f}，未见极端拥挤")
            elif sentiment_index > 85:
                score -= 1
                reasons.append(f"情绪代理 {sentiment_index:.1f}，注意拥挤")

            regime_score = _regime_alignment(item, regime.get("preferred_assets", []))
            score += regime_score
            if regime_score > 0:
                reasons.append(f"与当前 regime 偏好匹配（{regime['current_regime']}）")

            if event_policy:
                item_nodes = set(item.get("chain_nodes", []))
                if item["symbol"] in set(event_policy.get("mapped_assets", [])) or item_nodes & set(event_policy.get("beneficiary_nodes", [])):
                    score += 2
                    reasons.append(f"受益于事件主题：{event_policy['name']}")

            snapshots.append(
                {
                    "symbol": item["symbol"],
                    "region": item.get("region", ""),
                    "sector": item.get("sector", ""),
                    "return_5d": metrics["return_5d"],
                    "return_20d": metrics["return_20d"],
                }
            )

            signal = "强关注" if score >= 4 else "观察" if score >= 2 else "跟踪"
            if score >= 2:
                candidates.append(
                    {
                        "symbol": item["symbol"],
                        "name": item["name"],
                        "signal": signal,
                        "score": score,
                        "reason": "；".join(reasons[:3]) if reasons else "暂无显著共振",
                    }
                )
            if metrics["return_1d"] <= -0.03:
                alerts.append(f"{item['symbol']} 单日波动 {format_pct(metrics['return_1d'])}，注意情绪扰动。")
        except Exception as exc:
            alerts.append(f"{item['symbol']} 扫描失败: {exc}")

    flow_report = GlobalFlowCollector(config).collect(snapshots)
    candidates = sorted(candidates, key=lambda row: row["score"], reverse=True)[: args.top]
    if event_policy:
        alerts.insert(0, f"事件主题 '{args.event}' 匹配到政策模板：{event_policy['name']}。")
    alerts.extend(flow_report.get("lines", [])[:2])

    payload = {
        "title": "主动发现",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "regime_line": f"当前 regime 为 {regime['current_regime']}，偏好资产：{', '.join(regime.get('preferred_assets', []))}",
        "candidates": candidates,
        "alerts": alerts,
    }
    print(AlertRenderer().render(payload))


if __name__ == "__main__":
    main()
