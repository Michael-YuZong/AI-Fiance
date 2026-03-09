"""Asset scan command."""

from __future__ import annotations

import argparse
from typing import Any, Dict, List, Tuple

import pandas as pd

from src.collectors import (
    ChinaMarketCollector,
)
from src.output.scanner_report import ScannerReportRenderer
from src.processors.context import load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.industry_chain import IndustryChainAnalyzer
from src.processors.scorer import ScorecardBuilder
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.db import DatabaseManager
from src.utils.config import detect_asset_type, load_config
from src.utils.logger import setup_logger
from src.utils.market import AssetContext, compute_history_metrics, fetch_asset_history, format_pct, get_asset_context


def _item_icon(score: int) -> str:
    if score > 0:
        return "✅"
    if score < 0:
        return "❌"
    return "⚠️"


def _overall_icon(score: int) -> str:
    if score >= 2:
        return "✅"
    if score <= -2:
        return "❌"
    return "⚠️"


def _neutral_section(title: str, reason: str) -> Dict[str, Any]:
    return {
        "title": title,
        "overall": "⚠️",
        "summary": "该维度暂时使用中性输出。",
        "items": [{"name": "数据状态", "icon": "⚠️", "reason": reason}],
    }


def _build_macro_section(asset_type: str, china_macro: Dict[str, Any], global_proxy: Dict[str, Any]) -> Dict[str, Any]:
    items: List[Dict[str, str]] = []
    score = 0
    if asset_type in {"cn_etf", "futures"}:
        pmi = china_macro["pmi"]
        pmi_score = 1 if pmi >= 50 else -1
        score += pmi_score
        items.append(
            {
                "name": "制造业 PMI",
                "icon": _item_icon(pmi_score),
                "reason": f"最新制造业 PMI 为 {pmi:.1f}，前值 {china_macro['pmi_prev']:.1f}。",
            }
        )
        cpi = china_macro["cpi_monthly"]
        cpi_score = 1 if -0.5 <= cpi <= 0.5 else 0 if cpi <= 1.5 else -1
        score += cpi_score
        items.append(
            {
                "name": "CPI 月率",
                "icon": _item_icon(cpi_score),
                "reason": f"最近 CPI 月率为 {cpi:.1f}%，通胀压力处于可控区间。",
            }
        )
        lpr_score = 1 if china_macro["lpr_1y"] <= china_macro["lpr_prev"] else 0
        score += lpr_score
        items.append(
            {
                "name": "LPR 1Y",
                "icon": _item_icon(lpr_score),
                "reason": f"LPR 1Y 为 {china_macro['lpr_1y']:.2f}%，前值 {china_macro['lpr_prev']:.2f}%。",
            }
        )
    else:
        vix = float(global_proxy.get("vix", 20.0))
        vix_score = 1 if vix < 20 else -1 if vix > 25 else 0
        score += vix_score
        items.append({"name": "VIX", "icon": _item_icon(vix_score), "reason": f"VIX 当前为 {vix:.1f}。"})
        dxy_change = float(global_proxy.get("dxy_20d_change", 0.0))
        dxy_score = 1 if dxy_change < 0 else -1 if dxy_change > 0.02 else 0
        score += dxy_score
        items.append(
            {
                "name": "美元指数",
                "icon": _item_icon(dxy_score),
                "reason": f"DXY 20 日变化 {format_pct(dxy_change)}。",
            }
        )
        copper_gold = float(global_proxy.get("copper_gold_ratio", 0.18))
        copper_gold_score = 1 if copper_gold > 0.20 else -1 if copper_gold < 0.16 else 0
        score += copper_gold_score
        items.append(
            {
                "name": "铜金比",
                "icon": _item_icon(copper_gold_score),
                "reason": f"铜金比约为 {copper_gold:.3f}，反映当前风险偏好温度。",
            }
        )

    overall = _overall_icon(score)
    summary = {
        "✅": "宏观环境整体偏友好，至少有两项以上信号站在风险资产一侧。",
        "⚠️": "宏观环境偏中性，支持与压制因素并存。",
        "❌": "宏观环境偏谨慎，当前更需要控制仓位和节奏。",
    }[overall]
    return {"title": "宏观环境", "overall": overall, "summary": summary, "items": items}


def _build_industry_section(context: AssetContext, china_macro: Dict[str, Any], global_proxy: Dict[str, Any]) -> Dict[str, Any]:
    analyzer = IndustryChainAnalyzer()
    chain_nodes = list(context.metadata.get("chain_nodes", []))
    related = analyzer.related_nodes(chain_nodes)[:6]
    items: List[Dict[str, str]] = []
    score = 0
    chain_score = 1 if related else 0
    score += chain_score
    items.append(
        {
            "name": "产业链位置",
            "icon": _item_icon(chain_score),
            "reason": " -> ".join(related) if related else "watchlist 中暂未维护该标的的产业链节点。",
        }
    )

    sector = context.metadata.get("sector", "")
    thematic_score = 0
    if sector in {"科技"}:
        thematic_score = 1 if global_proxy.get("dxy_20d_change", 0.0) < 0 else -1
    elif sector in {"黄金"}:
        ratio = float(global_proxy.get("copper_gold_ratio", 0.18))
        thematic_score = 1 if ratio < 0.16 else -1 if ratio > 0.22 else 0
    elif sector in {"电网"}:
        thematic_score = 1 if china_macro.get("lpr_1y", 0.0) <= china_macro.get("lpr_prev", 0.0) else 0
    score += thematic_score
    items.append(
        {
            "name": "主题环境",
            "icon": _item_icon(thematic_score),
            "reason": f"当前主题标签为 {sector or '未标注'}，结合宏观与跨市场代理信号做中观判断。",
        }
    )

    overall = _overall_icon(score)
    summary = {
        "✅": "产业链位置清晰，主题环境与当前宏观代理信号基本一致。",
        "⚠️": "产业链逻辑存在，但目前缺少强催化共振。",
        "❌": "产业链逻辑与当前环境不太匹配，短期更偏验证期。",
    }[overall]
    return {"title": "板块与产业链", "overall": overall, "summary": summary, "items": items}


def _build_fund_section(symbol: str, asset_type: str, config: Dict[str, Any], price_history, global_proxy: Dict[str, Any]) -> Dict[str, Any]:
    metrics = compute_history_metrics(price_history)
    normalized = normalize_ohlcv_frame(price_history)
    latest_volume = float(normalized["volume"].iloc[-1])
    avg_volume = float(normalized["volume"].tail(20).mean())
    volume_ratio = latest_volume / avg_volume if avg_volume else 1.0
    score = 0
    items: List[Dict[str, str]] = []

    vol_score = 1 if volume_ratio > 1.3 else -1 if volume_ratio < 0.7 else 0
    score += vol_score
    items.append(
        {
            "name": "量能活跃度",
            "icon": _item_icon(vol_score),
            "reason": f"最新成交量 / 20 日均量约为 {volume_ratio:.2f}。",
        }
    )

    momentum = metrics["return_20d"]
    momentum_score = 1 if momentum > 0.05 else -1 if momentum < -0.05 else 0
    score += momentum_score
    items.append(
        {
            "name": "近 20 日资金偏好代理",
            "icon": _item_icon(momentum_score),
            "reason": f"近 20 日涨跌幅为 {format_pct(momentum)}，作为资金偏好代理参考。",
        }
    )

    if asset_type == "cn_etf":
        north_flow = ChinaMarketCollector(config).get_north_south_flow()
        north_rows = north_flow[north_flow["资金方向"] == "北向"]
        net_buy = float(pd.to_numeric(north_rows["成交净买额"], errors="coerce").fillna(0).sum()) if not north_rows.empty else 0.0
        north_score = 1 if net_buy > 0 else -1 if net_buy < 0 else 0
        score += north_score
        items.append(
            {
                "name": "北向情绪",
                "icon": _item_icon(north_score),
                "reason": f"当日北向成交净买额合计约 {net_buy:.2f} 亿元。",
            }
        )
    else:
        vix = float(global_proxy.get("vix", 20.0))
        vix_score = 1 if vix < 20 else -1 if vix > 25 else 0
        score += vix_score
        items.append(
            {
                "name": "风险偏好",
                "icon": _item_icon(vix_score),
                "reason": f"VIX 位于 {vix:.1f}，可作为跨市场情绪代理。",
            }
        )

    overall = _overall_icon(score)
    summary = {
        "✅": "量能和风险偏好共同偏正面，当前更像有资金参与。",
        "⚠️": "资金与情绪没有形成一致方向。",
        "❌": "量能与情绪偏弱，追价性价比较低。",
    }[overall]
    return {"title": "资金与情绪", "overall": overall, "summary": summary, "items": items}


def _build_cross_market_section(context: AssetContext, global_proxy: Dict[str, Any]) -> Dict[str, Any]:
    score = 0
    items: List[Dict[str, str]] = []
    dxy_change = float(global_proxy.get("dxy_20d_change", 0.0))
    copper_gold = float(global_proxy.get("copper_gold_ratio", 0.18))
    vix = float(global_proxy.get("vix", 20.0))

    if context.metadata.get("sector") == "黄金":
        ratio_score = 1 if copper_gold < 0.16 else -1 if copper_gold > 0.22 else 0
        score += ratio_score
        items.append(
            {
                "name": "铜金比",
                "icon": _item_icon(ratio_score),
                "reason": f"铜金比 {copper_gold:.3f}，风险偏好越弱通常越利于黄金。",
            }
        )
        vix_score = 1 if vix > 20 else 0
        score += vix_score
        items.append({"name": "波动率", "icon": _item_icon(vix_score), "reason": f"VIX 当前 {vix:.1f}。"})
    else:
        dxy_score = 1 if dxy_change < 0 else -1 if dxy_change > 0.02 else 0
        score += dxy_score
        items.append(
            {
                "name": "美元指数",
                "icon": _item_icon(dxy_score),
                "reason": f"DXY 20 日变化 {format_pct(dxy_change)}，成长与港股通常更受其影响。",
            }
        )
        ratio_score = 1 if copper_gold > 0.20 else -1 if copper_gold < 0.16 else 0
        score += ratio_score
        items.append(
            {
                "name": "铜金比",
                "icon": _item_icon(ratio_score),
                "reason": f"铜金比 {copper_gold:.3f}，可视作风险偏好温度计。",
            }
        )

    overall = _overall_icon(score)
    summary = {
        "✅": "跨市场信号与当前标的风格方向一致。",
        "⚠️": "跨市场信号偏中性，暂时没有形成强助推。",
        "❌": "跨市场信号对当前标的不太友好。",
    }[overall]
    return {"title": "跨市场联动", "overall": overall, "summary": summary, "items": items}


def run_scan(symbol: str, config_path: str = "") -> Tuple[str, dict]:
    config = load_config(config_path or None)
    setup_logger()
    asset_type = detect_asset_type(symbol, config)
    context = get_asset_context(symbol, asset_type, config)
    raw_history = fetch_asset_history(symbol, asset_type, config)
    normalized_history = normalize_ohlcv_frame(raw_history)

    analyzer = TechnicalAnalyzer(normalized_history)
    technical_scorecard = analyzer.generate_scorecard(config.get("technical", {}))
    china_macro = {}
    global_proxy = {}
    extra_sections: List[Dict[str, Any]] = []

    try:
        china_macro = load_china_macro_snapshot(config)
        global_proxy = load_global_proxy_snapshot()
        extra_sections.append(_build_macro_section(asset_type, china_macro, global_proxy))
    except Exception as exc:
        extra_sections.append(_neutral_section("宏观环境", f"宏观数据拉取失败: {exc}"))

    try:
        extra_sections.append(_build_industry_section(context, china_macro, global_proxy))
    except Exception as exc:
        extra_sections.append(_neutral_section("板块与产业链", f"产业链匹配失败: {exc}"))

    try:
        extra_sections.append(_build_fund_section(symbol, asset_type, config, normalized_history, global_proxy))
    except Exception as exc:
        extra_sections.append(_neutral_section("资金与情绪", f"资金情绪数据拉取失败: {exc}"))

    try:
        extra_sections.append(_build_cross_market_section(context, global_proxy))
    except Exception as exc:
        extra_sections.append(_neutral_section("跨市场联动", f"跨市场数据拉取失败: {exc}"))

    scorecard = ScorecardBuilder().build(
        symbol=f"{symbol} ({context.name})" if context.name != symbol else symbol,
        asset_type=asset_type,
        technical_scorecard=technical_scorecard,
        price_history=normalized_history,
        extra_sections=extra_sections,
    )

    db = DatabaseManager(config["storage"]["db_path"])
    db.initialize()
    db.save_market_data(symbol, asset_type, normalized_history.tail(400))

    report = ScannerReportRenderer().render(scorecard)
    return report, scorecard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan an asset and output a markdown scorecard.")
    parser.add_argument("symbol", help="Asset symbol, e.g. 561380 or QQQM")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    report, _ = run_scan(args.symbol, args.config)
    print(report)


if __name__ == "__main__":
    main()
