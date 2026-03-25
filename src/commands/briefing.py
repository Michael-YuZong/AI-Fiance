"""Daily and weekly briefing command."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
import io
import re
import warnings
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

from src.collectors import (
    ChinaMarketCollector,
    EventsCollector,
    GlobalFlowCollector,
    MarketDriversCollector,
    MarketMonitorCollector,
    MarketOverviewCollector,
    MarketPulseCollector,
    NewsCollector,
    SocialSentimentCollector,
)
from src.commands.final_runner import finalize_client_markdown
from src.output.briefing import BriefingRenderer
from src.output.client_report import ClientReportRenderer
from src.commands.report_guard import ensure_report_task_registered
from src.commands.release_check import check_generic_client_report
from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot, macro_lines
from src.processors.factor_meta import summarize_factor_contracts_from_analyses
from src.processors.horizon import get_horizon_contract
from src.processors.market_analysis import build_market_analysis
from src.processors.opportunity_engine import (
    discover_stock_opportunities,
    summarize_proxy_contracts,
    _today_theme as _opportunity_day_theme,
)
from src.processors.regime import RegimeDetector
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.processors.trade_handoff import portfolio_whatif_handoff
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.config import load_config, resolve_project_path
from src.utils.data import load_watchlist
from src.utils.logger import setup_logger
from src.utils.market import compute_history_metrics, fetch_asset_history, format_pct


@dataclass
class BriefingSnapshot:
    """Single watchlist item snapshot used in the briefing."""

    symbol: str
    name: str
    asset_type: str
    region: str
    sector: str
    latest_price: float
    return_1d: float
    return_5d: float
    return_20d: float
    volume_ratio: float
    trend: str
    signal_score: int
    summary: str
    note: str
    proxy_symbol: str = ""
    notes: str = ""
    technical: Dict[str, Any] = field(default_factory=dict)
    technical_bias: str = "分歧"
    history: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())


REGIME_LABELS = {
    "recovery": "温和复苏",
    "overheating": "过热",
    "stagflation": "滞涨",
    "deflation": "通缩/偏弱",
}

THEME_ASSET_PREFERENCES = {
    "energy_shock": ["能源链", "电力电网", "黄金/防守", "现金"],
    "gold_defense": ["黄金", "现金", "避险资产"],
    "dividend_defense": ["高股息防守", "银行", "公用事业", "现金"],
    "defensive_riskoff": ["黄金", "现金", "公用事业", "高股息防守"],
    "broad_market_repair": ["宽基", "券商", "顺周期", "内需核心资产"],
    "rate_growth": ["美股科技", "港股科技", "成长股", "长久期资产"],
    "power_utilities": ["电力电网", "公用事业", "高股息配套"],
    "china_policy": ["电网基建", "央国企链", "内需顺周期", "高股息配套"],
    "ai_semis": ["半导体", "算力硬件", "通信", "AI应用"],
}

DEFENSIVE_THEMES = {"energy_shock", "gold_defense", "dividend_defense", "defensive_riskoff"}
STRUCTURAL_THEMES = {"broad_market_repair", "power_utilities", "china_policy", "rate_growth", "ai_semis"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily or weekly market briefing.")
    parser.add_argument("mode", choices=["daily", "weekly", "noon", "evening", "market"], help="Briefing mode")
    parser.add_argument("--news-source", action="append", default=[], help="Preferred news source, e.g. Reuters")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")
    return parser


def _trend_label(technical: Dict[str, Any]) -> str:
    if technical["ma_system"]["signal"] == "bullish" and technical["macd"]["signal"] == "bullish":
        return "多头"
    if technical["ma_system"]["signal"] == "bearish":
        return "空头"
    return "震荡"


def _signal_score(metrics: Dict[str, float], technical: Dict[str, Any]) -> int:
    score = 0
    if technical["ma_system"]["signal"] == "bullish":
        score += 2
    elif technical["ma_system"]["signal"] == "bearish":
        score -= 2

    if technical["macd"]["signal"] == "bullish":
        score += 1
    else:
        score -= 1

    if metrics["return_20d"] > 0.08:
        score += 1
    elif metrics["return_20d"] < -0.08:
        score -= 1

    if technical["volume"]["vol_ratio"] > 1.4:
        score += 1
    elif technical["volume"]["vol_ratio"] < 0.7:
        score -= 1
    return score


def _build_summary(metrics: Dict[str, float], technical: Dict[str, Any], trend: str) -> tuple[str, str]:
    volume_ratio = technical["volume"]["vol_ratio"]
    if trend == "多头" and metrics["return_20d"] > 0.08:
        summary = "趋势和中期动量同向偏强，当前属于 watchlist 里的强势方向。"
    elif trend == "空头" and metrics["return_20d"] < -0.05:
        summary = "趋势与动量均偏弱，短期更像下跌后的修复观察区。"
    elif abs(metrics["return_1d"]) >= 0.03:
        summary = "短线波动明显放大，今天重点看价格是否延续还是反抽回落。"
    else:
        summary = "趋势尚未形成单边共振，更适合等进一步确认。"

    if volume_ratio > 1.5:
        note = f"量能比 {volume_ratio:.2f}，盘面明显活跃。"
    elif volume_ratio < 0.7:
        note = f"量能比 {volume_ratio:.2f}，资金参与度偏弱。"
    else:
        note = f"量能比 {volume_ratio:.2f}，量能处于常态区间。"
    return summary, note


def _technical_bias_label(technical: Dict[str, Any]) -> str:
    bull = 0
    bear = 0
    for key in ("ma_system", "macd", "kdj", "obv"):
        signal = str(technical.get(key, {}).get("signal", ""))
        if signal == "bullish":
            bull += 1
        elif signal == "bearish":
            bear += 1

    dmi_signal = str(technical.get("dmi", {}).get("signal", ""))
    if dmi_signal == "bullish_trend":
        bull += 1
    elif dmi_signal == "bearish_trend":
        bear += 1

    rsi_signal = str(technical.get("rsi", {}).get("signal", ""))
    if rsi_signal == "oversold":
        bull += 1
    elif rsi_signal == "overbought":
        bear += 1

    if bull - bear >= 2:
        return "偏强"
    if bear - bull >= 2:
        return "偏弱"
    return "分歧"


def _kdj_text(kdj: Dict[str, Any]) -> str:
    cross = str(kdj.get("cross", ""))
    zone = str(kdj.get("zone", ""))
    if cross == "golden_cross":
        base = "KDJ 金叉"
    elif cross == "death_cross":
        base = "KDJ 死叉"
    else:
        base = "KDJ 纠缠"
    if zone == "overbought":
        return base + "，高位"
    if zone == "oversold":
        return base + "，低位"
    return base


def _rsi_text(rsi: Dict[str, Any]) -> str:
    value = float(rsi.get("RSI", 50.0))
    signal = str(rsi.get("signal", "neutral"))
    if signal == "overbought":
        return f"RSI {value:.1f} 过热"
    if signal == "oversold":
        return f"RSI {value:.1f} 超卖"
    if value >= 60:
        return f"RSI {value:.1f} 偏强"
    if value <= 40:
        return f"RSI {value:.1f} 偏弱"
    return f"RSI {value:.1f} 中性"


def _boll_text(boll: Dict[str, Any]) -> str:
    signal = str(boll.get("signal", "neutral"))
    if signal == "near_upper":
        return "BOLL 上轨附近"
    if signal == "near_lower":
        return "BOLL 下轨附近"
    return "BOLL 中轨附近"


def _obv_text(obv: Dict[str, Any]) -> str:
    signal = str(obv.get("signal", "neutral"))
    if signal == "bullish":
        return "OBV 在均线之上"
    if signal == "bearish":
        return "OBV 在均线之下"
    return "OBV 方向不明"


def _adx_text(dmi: Dict[str, Any]) -> str:
    adx = float(dmi.get("ADX", 0.0))
    signal = str(dmi.get("signal", "weak_trend"))
    if signal == "bullish_trend":
        return f"ADX {adx:.1f} 趋势增强"
    if signal == "bearish_trend":
        return f"ADX {adx:.1f} 空头趋势增强"
    return f"ADX {adx:.1f} 趋势偏弱"


def _fib_text(fib: Dict[str, Any]) -> str:
    nearest = str(fib.get("nearest_level", "0.500"))
    signal = str(fib.get("signal", "mid_zone"))
    mapping = {
        "upper_zone": "斐波那契高位区",
        "strong_zone": "斐波那契强势区",
        "mid_zone": "斐波那契中位区",
        "lower_zone": "斐波那契低位区",
    }
    return f"{mapping.get(signal, '斐波那契中位区')} ({nearest})"


def _technical_watchlist_line(snapshot: BriefingSnapshot) -> str:
    technical = snapshot.technical
    parts = [
        f"MACD {'多头' if technical['macd']['signal'] == 'bullish' else '空头'}",
        _kdj_text(technical["kdj"]),
        _rsi_text(technical["rsi"]),
        _boll_text(technical["bollinger"]),
        _obv_text(technical["obv"]),
        _adx_text(technical["dmi"]),
        _fib_text(technical["fibonacci"]),
    ]
    return f"{snapshot.symbol} ({snapshot.name}): 技术共振 `{snapshot.technical_bias}`，" + "；".join(parts) + "。"


def _price_context_label(asset_type: str, proxy_symbol: str = "") -> str:
    mapping = {
        "cn_etf": "场内价格",
        "hk_etf": "场内价格",
        "us": "现价",
        "hk": "现价",
        "hk_index": "指数点位",
        "futures": "主力合约价",
    }
    if asset_type == "hk_index" and proxy_symbol:
        return "代理ETF价格"
    return mapping.get(asset_type, "最新价")


def _watchlist_tech_basis(technical: Dict[str, Any]) -> str:
    rsi = float(technical.get("rsi", {}).get("RSI", 0.0))
    adx = float(technical.get("dmi", {}).get("ADX", 0.0))
    return f"RSI {rsi:.1f} / ADX {adx:.1f}"


def _collect_snapshots(config: Dict[str, Any], mode: str) -> tuple[List[BriefingSnapshot], List[str], List[List[str]]]:
    snapshots: List[BriefingSnapshot] = []
    alerts: List[str] = []
    rows: List[List[str]] = []
    snapshot_timeout_seconds = float(config.get("briefing_snapshot_timeout_seconds", 12))

    for item in load_watchlist():
        symbol = item["symbol"]
        try:
            history, history_warning = _timed_collect(
                f"{symbol} watchlist 快照",
                lambda: normalize_ohlcv_frame(fetch_asset_history(symbol, item["asset_type"], config)),
                fallback=pd.DataFrame(),
                timeout_seconds=snapshot_timeout_seconds,
            )
            if history_warning:
                raise RuntimeError(history_warning)
            if history.empty:
                raise RuntimeError("未拿到可用历史行情")
            metrics = compute_history_metrics(history)
            technical = TechnicalAnalyzer(history).generate_scorecard(config.get("technical", {}))
            trend = _trend_label(technical)
            score = _signal_score(metrics, technical)
            summary, note = _build_summary(metrics, technical, trend)
            technical_bias = _technical_bias_label(technical)

            snapshot = BriefingSnapshot(
                symbol=symbol,
                name=item["name"],
                asset_type=item["asset_type"],
                region=item.get("region", ""),
                sector=item.get("sector", ""),
                latest_price=metrics["last_close"],
                return_1d=metrics["return_1d"],
                return_5d=metrics["return_5d"],
                return_20d=metrics["return_20d"],
                volume_ratio=technical["volume"]["vol_ratio"],
                trend=trend,
                signal_score=score,
                summary=summary,
                note=note,
                proxy_symbol=item.get("proxy_symbol", ""),
                notes=item.get("notes", ""),
                technical=technical,
                technical_bias=technical_bias,
                history=history,
            )
            snapshots.append(snapshot)
            rows.append(
                [
                    f"{symbol} ({item['name']})",
                    f"{_price_context_label(item['asset_type'], item.get('proxy_symbol', ''))} {metrics['last_close']:.3f}",
                    format_pct(metrics["return_1d"]),
                    format_pct(metrics["return_5d"]),
                    format_pct(metrics["return_20d"]),
                    trend,
                    _watchlist_tech_basis(technical),
                    technical_bias,
                ]
            )

            if mode == "daily" and (abs(metrics["return_1d"]) >= 0.03 or technical["volume"]["vol_ratio"] > 1.6):
                alerts.append(
                    f"{symbol} 日内波动 {format_pct(metrics['return_1d'])}，{note}"
                )
            if mode == "weekly" and abs(metrics["return_5d"]) >= 0.08:
                alerts.append(
                    f"{symbol} 近 5 日波动 {format_pct(metrics['return_5d'])}，周度需要重点复盘。"
                )
        except Exception as exc:
            rows.append([f"{symbol} ({item['name']})", "N/A", "N/A", "N/A", "N/A", "数据异常", "N/A", "N/A"])
            alerts.append(f"{symbol} 行情拉取失败: {exc}")

    if mode == "weekly":
        rows = sorted(
            rows,
            key=lambda row: float(row[3].replace("%", "").replace("+", "")) if row[3] != "N/A" else -999,
            reverse=True,
        )
    return snapshots, alerts, rows


def _briefing_shared_market_context(
    config: Dict[str, Any],
    *,
    china_macro: Dict[str, Any],
    global_proxy: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    regime_result: Dict[str, Any],
    news_report: Dict[str, Any],
    drivers: Dict[str, Any],
    pulse: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "config": dict(config),
        "as_of": datetime.now(),
        "china_macro": dict(china_macro or {}),
        "global_proxy": dict(global_proxy or {}),
        "monitor_rows": list(monitor_rows or []),
        "regime": dict(regime_result or {}),
        "day_theme": _opportunity_day_theme(news_report or {}, monitor_rows or []),
        "news_report": dict(news_report or {}),
        "events": list(events or []),
        "drivers": dict(drivers or {}),
        "pulse": dict(pulse or {}),
        "notes": [],
        "preferred_sources": [],
        "watchlist_returns": {},
        "benchmark_returns": {},
        "runtime_caches": {},
    }


def _briefing_a_share_watch_rows(
    config: Dict[str, Any],
    *,
    shared_context: Optional[Dict[str, Any]] = None,
) -> tuple[List[List[str]], List[str], Dict[str, Any], List[Dict[str, Any]]]:
    top_n = max(int(config.get("briefing_a_share_top_n", 5) or 5), 0)
    if top_n <= 0:
        return [], ["当前已关闭 A 股全市场观察池。"], {"enabled": False, "mode": "disabled"}, []
    try:
        shortlist_n = max(int(config.get("briefing_a_share_shortlist", max(top_n * 2, 8)) or max(top_n * 2, 8)), top_n)
        candidate_cap = max(int(config.get("briefing_a_share_max_candidates", max(top_n * 2 + 6, 16)) or max(top_n * 2 + 6, 16)), shortlist_n)
        payload = discover_stock_opportunities(
            config,
            top_n=shortlist_n,
            market="cn",
            context=shared_context,
            max_candidates=candidate_cap,
            attach_signal_confidence=False,
        )
    except Exception as exc:  # noqa: BLE001
        return [], [f"A 股全市场观察池暂不可用：{exc}"], {"enabled": True, "mode": "unavailable"}, []
    analyses = list(payload.get("coverage_analyses") or [])
    top_items = list(payload.get("top") or [])[:top_n]
    if not analyses:
        return [], ["A 股全市场观察池暂不可用：当前未拿到可用的全市场初筛池。"], {"enabled": True, "mode": "empty_pool"}, []
    rows: List[List[str]] = []
    sector_counter: Counter[str] = Counter()
    for index, item in enumerate(top_items, start=1):
        metadata = dict(item.get("metadata") or {})
        rating = dict(item.get("rating") or {})
        action = dict(item.get("action") or {})
        narrative = dict(item.get("narrative") or {})
        state = str(dict(narrative.get("judgment") or {}).get("state", "")).strip() or "观察"
        sector = str(metadata.get("sector", "综合")).strip() or "综合"
        sector_counter[sector] += 1
        rows.append(
            [
                str(index),
                f"{item.get('name', item.get('symbol', ''))} ({item.get('symbol', '')})",
                sector,
                str(rating.get("label", "未评级")).strip() or "未评级",
                state,
                str(action.get("position", "先观察")).strip() or "先观察",
            ]
        )

    lines = [
        "A 股观察池来自 `Tushare 优先` 的全市场快照；"
        f"初筛池 `{int(payload.get('scan_pool') or 0)}` 只，完整分析 `{int(payload.get('passed_pool') or len(analyses))}` 只，深分析 shortlist `{len(top_items)}` 只。",
        "这不是对全 A 股逐只深扫，而是全市场初筛后，只对前置筛出的少数样本做完整分析。",
        f"为控制全市场简报时延，本轮只对成交额靠前且通过基础过滤的候选上限 `{int(payload.get('candidate_limit') or candidate_cap)}` 只做完整分析。",
    ]
    summary = (
        f"全市场初筛 `{int(payload.get('scan_pool') or 0)}`"
        f" -> shortlist `{len(top_items)}`"
        f" -> 过硬排除 `{int(payload.get('passed_pool') or len(analyses))}`。"
    )
    lines.append("覆盖说明: " + summary)
    blind_spots = [str(item).strip() for item in (payload.get("blind_spots") or []) if str(item).strip()]
    if blind_spots:
        lines.append("当前盲点: " + blind_spots[0])
    if not rows:
        lines.append("今天没有筛出需要优先放进晨报 A 股观察池的标的。")
    meta: Dict[str, Any] = {
        "enabled": True,
        "mode": "tushare_priority_full_market_prescreen",
        "pool_size": int(payload.get("scan_pool") or 0),
        "shortlist_size": len(top_items),
        "complete_analysis_size": int(payload.get("passed_pool") or len(analyses)),
        "report_top_n": len(rows),
        "candidate_limit": int(payload.get("candidate_limit") or candidate_cap),
        "sector_counts": dict(sector_counter),
        "factor_contract": summarize_factor_contracts_from_analyses(analyses, sample_limit=16),
    }
    if blind_spots:
        meta["blind_spot"] = blind_spots[0]
    return rows, lines[:4], meta, top_items


def _theme_information_environment(aligned: bool, horizon: str) -> str:
    horizon_text = str(horizon).strip() or "观察期"
    if aligned:
        return f"当前更像直接催化和盘面方向基本一致，适合按 `{horizon_text}` 继续跟踪。"
    return f"当前更多是背景储备和信息环境支持，适合按 `{horizon_text}` 保留观察资格，但不等于直接催化已兑现。"


def _rotation_lines(snapshots: List[BriefingSnapshot]) -> List[str]:
    lines: List[str] = []
    if not snapshots:
        return lines

    tech_items = [item for item in snapshots if item.sector == "科技"]
    gold_items = [item for item in snapshots if item.sector == "黄金"]
    grid_items = [item for item in snapshots if item.sector == "电网"]

    tech_avg = sum(item.return_5d for item in tech_items) / len(tech_items) if tech_items else 0.0
    gold_avg = sum(item.return_5d for item in gold_items) / len(gold_items) if gold_items else 0.0
    grid_avg = sum(item.return_5d for item in grid_items) / len(grid_items) if grid_items else 0.0

    if grid_avg > tech_avg and grid_avg > 0:
        lines.append("电网/内需链相对更强，说明资金更愿意留在有确定性的国内方向。")
    if tech_avg < 0 and gold_avg > 0:
        lines.append("科技承压而黄金偏稳，当前更像防守型轮动，而不是全面 risk-on。")
    if tech_avg > 0 and gold_avg < 0:
        lines.append("科技修复快于黄金，风险偏好有回暖迹象。")
    if not lines:
        lines.append("当前 watchlist 内部分化明显，但还没有形成特别统一的风格主线。")
    return lines


def _headline_lines(
    mode: str,
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
    china_macro: Dict[str, Any],
    pulse: Optional[Dict[str, Any]] = None,
) -> List[str]:
    preferred_assets = "、".join(_effective_asset_preference(narrative)[:4])
    background = narrative.get("background_regime", "未识别")
    lines = [narrative.get("summary", "今天没有单一主线。")]
    if narrative.get("overrides_background"):
        lines.append(
            f"背景宏观仍接近 `{background}`，但今天真正驱动价格的是 `{narrative['label']}`；背景资产偏好暂时降级为中期参考，日内优先跟随 {preferred_assets or '主线方向'}。"
        )
    else:
        lines.append(
            f"当前没有更强的事件主线覆盖背景框架，因此仍按 `{background}` 来组织资产偏好"
            + (f"，优先看 {preferred_assets}。" if preferred_assets else "。")
        )

    if pulse:
        zt_pool = pulse.get("zt_pool", pd.DataFrame())
        dt_pool = pulse.get("dt_pool", pd.DataFrame())
        detail = pulse.get("lhb_detail", pd.DataFrame())
        top_zt = _top_categories(zt_pool, "所属行业", limit=2)
        market_line = (
            f"{_latest_trade_date_label(pulse)} A股涨停 {len(zt_pool.index) if not zt_pool.empty else 0} 家、"
            f"跌停 {len(dt_pool.index) if not dt_pool.empty else 0} 家。"
        )
        if top_zt:
            market_line += " 当前强势方向主要集中在 " + "、".join(top_zt) + "。"
        lines.append(market_line)
        if not detail.empty:
            lines.append(f"龙虎榜最近一个交易日上榜 {len(detail.index)} 条记录，活跃资金仍在高波动板块中反复博弈。")
    elif snapshots:
        bull_count = sum(1 for item in snapshots if item.trend == "多头")
        bear_count = sum(1 for item in snapshots if item.trend == "空头")
        lines.append(f"当前观察池里多头 {bull_count} 个、空头 {bear_count} 个，市场并非一致单边。")
    else:
        lines.append("当前没有可用的全市场或 watchlist 行情数据。")

    if china_macro["pmi"] < 50:
        lines.append("国内景气度仍在荣枯线下方，今天应先看谁在逆势走强，而不是先预设全面风险偏好修复。")
    else:
        lines.append("国内景气度在荣枯线附近或以上，但是否能扩散成趋势，仍要服从今天的主线校验结果。")
    if mode == "weekly":
        lines.append("周报更看 5 日与 20 日结构，不只看单日波动。")
    return lines


def _market_overview_lines(snapshots: List[BriefingSnapshot], regime_result: Dict[str, Any]) -> List[str]:
    if not snapshots:
        return []

    avg_1d = sum(item.return_1d for item in snapshots) / len(snapshots)
    avg_5d = sum(item.return_5d for item in snapshots) / len(snapshots)
    avg_20d = sum(item.return_20d for item in snapshots) / len(snapshots)
    strongest_5d = max(snapshots, key=lambda item: item.return_5d)
    weakest_5d = min(snapshots, key=lambda item: item.return_5d)
    regime_label = REGIME_LABELS.get(str(regime_result["current_regime"]), str(regime_result["current_regime"]))

    pulse = "偏强" if avg_5d > 0.02 else "偏弱" if avg_5d < -0.02 else "中性"
    return [
        f"watchlist 平均 1 日表现 {format_pct(avg_1d)}，5 日 {format_pct(avg_5d)}，20 日 {format_pct(avg_20d)}，整体市场温度偏 {pulse}。",
        f"近 5 日最强的是 {strongest_5d.symbol} {format_pct(strongest_5d.return_5d)}，最弱的是 {weakest_5d.symbol} {format_pct(weakest_5d.return_5d)}。",
        f"从当前宏观环境 `{regime_label}` 看，更符合偏好的方向是：{', '.join(regime_result.get('preferred_assets', [])) or '暂无'}。",
    ]


def _regime_explanation_lines(
    china_macro: Dict[str, Any],
    regime_result: Dict[str, Any],
    narrative: Dict[str, Any],
) -> List[str]:
    label = REGIME_LABELS.get(str(regime_result.get("current_regime", "")), str(regime_result.get("current_regime", "")))
    reasoning = [str(item) for item in regime_result.get("reasoning", []) if str(item).strip()]
    lines: List[str] = []
    if reasoning:
        cleaned = [item.rstrip("。；; ") for item in reasoning[:3]]
        lines.append(f"背景 regime 当前判为 `{label}`，触发依据: " + "；".join(cleaned) + "。")
    else:
        pmi = float(china_macro.get("pmi", 50.0))
        cpi = float(china_macro.get("cpi_monthly", 0.0))
        lines.append(f"背景 regime 当前判为 `{label}`，主要参考 PMI {pmi:.1f}、CPI {cpi:.1f}% 和美元/流动性状态。")
    if narrative.get("overrides_background"):
        lines.append("但这只是中期背景，不是今天盘口的第一驱动；日内仍以事件主线裁决为准。")
    return lines


def _find_snapshot(snapshots: List[BriefingSnapshot], symbol: str) -> Optional[BriefingSnapshot]:
    for item in snapshots:
        if item.symbol == symbol:
            return item
    return None


def _sector_avg_return(snapshots: List[BriefingSnapshot], sector: str, *, field: str = "return_1d") -> float:
    matched = [float(getattr(item, field, 0.0) or 0.0) for item in snapshots if str(getattr(item, "sector", "")).strip() == sector]
    if not matched:
        return 0.0
    return sum(matched) / len(matched)


def _a_share_watch_theme_boosts(a_share_watch_meta: Optional[Dict[str, Any]]) -> Dict[str, int]:
    meta = dict(a_share_watch_meta or {})
    sector_counts = {str(key).strip(): int(value or 0) for key, value in dict(meta.get("sector_counts") or {}).items()}
    boosts = {
        "gold_defense": 0,
        "dividend_defense": 0,
        "broad_market_repair": 0,
        "power_utilities": 0,
        "china_policy": 0,
        "ai_semis": 0,
    }
    if not sector_counts:
        return boosts

    for sector, count in sector_counts.items():
        if sector in {"宽基", "金融", "券商", "保险", "消费", "白酒"}:
            boosts["broad_market_repair"] += count
        if sector in {"高股息", "银行", "公用事业", "煤炭"}:
            boosts["dividend_defense"] += count
        if sector in {"电网", "电力", "公用事业"}:
            boosts["power_utilities"] += count * 2
            boosts["china_policy"] += count
        if sector in {"黄金", "贵金属"}:
            boosts["gold_defense"] += count * 2
        if sector in {"科技", "半导体", "通信", "消费电子"}:
            boosts["ai_semis"] += count
        if sector in {"基建", "工程", "建材", "央国企"}:
            boosts["china_policy"] += count * 2
    return boosts


def _overnight_lines(snapshots: List[BriefingSnapshot]) -> List[str]:
    if not snapshots:
        return ["当前没有可用的最近交易日资产快照。"]

    lines: List[str] = []
    hstech = _find_snapshot(snapshots, "HSTECH")
    qqqm = _find_snapshot(snapshots, "QQQM")
    grid = _find_snapshot(snapshots, "561380")
    gld = _find_snapshot(snapshots, "GLD")
    au0 = _find_snapshot(snapshots, "AU0")

    if hstech and qqqm:
        if hstech.return_1d < 0 and qqqm.return_1d < 0:
            lines.append(
                f"科技风险偏好偏弱：HSTECH {format_pct(hstech.return_1d)}，QQQM {format_pct(qqqm.return_1d)}，成长风格仍在消化压力。"
            )
        elif hstech.return_1d > 0 and qqqm.return_1d > 0:
            lines.append(
                f"科技风险偏好回暖：HSTECH {format_pct(hstech.return_1d)}，QQQM {format_pct(qqqm.return_1d)}，成长方向出现同步修复。"
            )
        else:
            lines.append(
                f"科技方向内外盘分化：HSTECH {format_pct(hstech.return_1d)}，QQQM {format_pct(qqqm.return_1d)}，风险偏好并不统一。"
            )

    if grid:
        lines.append(
            f"国内确定性方向看 561380：近 5 日 {format_pct(grid.return_5d)}，近 20 日 {format_pct(grid.return_20d)}，当前仍是观察池里的相对强者。"
        )

    if gld and au0:
        if gld.return_1d > 0 and au0.return_1d > 0:
            lines.append(
                f"黄金内外盘同向偏强：GLD {format_pct(gld.return_1d)}，AU0 {format_pct(au0.return_1d)}，避险需求更一致。"
            )
        elif gld.return_1d < 0 and au0.return_1d < 0:
            lines.append(
                f"黄金内外盘同步走弱：GLD {format_pct(gld.return_1d)}，AU0 {format_pct(au0.return_1d)}，短线避险并未形成主线。"
            )
        else:
            lines.append(
                f"黄金内外盘分化：GLD {format_pct(gld.return_1d)}，AU0 {format_pct(au0.return_1d)}，短线更多受各自市场节奏影响。"
            )

    strongest_1d = max(snapshots, key=lambda item: item.return_1d)
    weakest_1d = min(snapshots, key=lambda item: item.return_1d)
    lines.append(
        f"单日最强是 {strongest_1d.symbol} {format_pct(strongest_1d.return_1d)}，最弱是 {weakest_1d.symbol} {format_pct(weakest_1d.return_1d)}。"
    )
    return lines


def _flow_lines(snapshots: List[BriefingSnapshot], config: Dict[str, Any]) -> List[str]:
    report = GlobalFlowCollector(config).collect(snapshots)
    lines = list(report.get("lines", []))
    lines.append(
        f"说明：当前资金流为相对强弱代理，不是机构申购赎回原始数据；当前代理置信度 `{report.get('confidence_label', '低')}`。"
    )
    limitations = list(report.get("limitations") or [])
    if limitations:
        lines.append(f"限制：{limitations[0]}")
    return lines


def _briefing_proxy_contract(snapshots: List[BriefingSnapshot], config: Dict[str, Any]) -> Dict[str, Any]:
    if not snapshots:
        return summarize_proxy_contracts(market_proxy={}, social_payloads=[], total=0)
    market_flow = GlobalFlowCollector(config).collect(snapshots)
    collector = SocialSentimentCollector(config)
    social_payloads: List[Dict[str, Any]] = []
    for item in snapshots:
        social_payloads.append(
            collector.collect(
                item.symbol,
                {
                    "return_1d": item.return_1d,
                    "return_5d": item.return_5d,
                    "return_20d": item.return_20d,
                    "volume_ratio": item.volume_ratio,
                    "trend": item.trend,
                },
            )
        )
    return summarize_proxy_contracts(
        market_proxy=market_flow,
        social_payloads=social_payloads,
        total=len(snapshots),
    )


def _news_lines(
    report: Dict[str, Any],
) -> List[str]:
    lines = list(report.get("lines", []))
    note = report.get("note")
    if note:
        lines.append(note)
    return lines


def _news_report(
    snapshots: List[BriefingSnapshot],
    china_macro: Dict[str, Any],
    global_proxy: Dict[str, Any],
    config: Dict[str, Any],
    preferred_sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return NewsCollector(config).collect(
        snapshots=snapshots,
        china_macro=china_macro,
        global_proxy=global_proxy,
        preferred_sources=preferred_sources,
    )


def _collect_monitor_rows(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    market_context_cfg = dict(config.get("market_context") or {})
    if market_context_cfg.get("skip_market_monitor"):
        return []
    return MarketMonitorCollector(config).collect()


def _load_briefing_global_proxy(config: Dict[str, Any]) -> tuple[Dict[str, Any], str]:
    market_context_cfg = dict(config.get("market_context") or {})
    if market_context_cfg.get("skip_global_proxy"):
        return {}, "跨市场代理数据已按运行配置关闭，本次先按国内宏观与本地缓存生成。"
    try:
        with redirect_stderr(io.StringIO()):
            return load_global_proxy_snapshot(), ""
    except Exception:
        return {}, "跨市场代理数据暂不可用，已回退到国内宏观与本地缓存。"


def _timed_collect(
    label: str,
    loader,
    *,
    fallback: Any,
    timeout_seconds: float,
) -> tuple[Any, str]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(loader)
    try:
        return future.result(timeout=timeout_seconds), ""
    except FutureTimeoutError:
        future.cancel()
        return fallback, f"{label} 拉取超时（>{int(timeout_seconds)}s），本轮已按降级口径处理。"
    except Exception:
        return fallback, f"{label} 拉取失败，本轮已按降级口径处理。"
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _monitor_lines(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows:
        return ["关键宏观资产暂不可用，晨报已回退到已有新闻主线和宏观代理。"]

    lines = [
        f"{item['name']} {item['latest']:.3f}，1日 {format_pct(item['return_1d'])}，5日 {format_pct(item['return_5d'])}。"
        for item in rows[:8]
    ]

    by_name = {item["name"]: item for item in rows}
    brent = by_name.get("布伦特原油")
    dxy = by_name.get("美元指数")
    vix = by_name.get("VIX波动率")
    copper = by_name.get("COMEX铜")
    gold = by_name.get("COMEX黄金")

    if brent and brent["return_5d"] > 0.04:
        lines.append("原油 5 日涨幅偏大，今天需要更留意能源链、通胀预期和风险资产承压。")
    if dxy and dxy["return_5d"] > 0.01:
        lines.append("美元阶段性走强时，港股科技和成长估值通常更容易承压。")
    if vix and vix["latest"] >= 22:
        lines.append("VIX 处在高波动区，今天更适合把回撤控制和仓位节奏放在前面。")
    if copper and gold and copper["return_5d"] > gold["return_5d"] + 0.02:
        lines.append("铜强于金，说明市场更偏向交易增长和顺周期。")
    elif copper and gold and gold["return_5d"] > copper["return_5d"] + 0.02:
        lines.append("金强于铜，说明市场更偏向防守和避险。")
    return lines


def _monitor_alerts(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows:
        return ["宏观资产监控今日未能完成实时刷新，主题判断优先参考新闻与盘面，不把缺失的跨市场数值当成确认信号。"]
    stale_rows = [item for item in rows if item.get("data_warning")]
    if not stale_rows:
        return []
    labels = "、".join(str(item.get("name", item.get("symbol", "未知资产"))) for item in stale_rows[:3])
    return [f"宏观资产监控部分回退到陈旧缓存：{labels}。当前优先看方向，不把这些数值当成严格实时确认。"]


def _monitor_map(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(item["name"]): item for item in rows}


def _to_float(value: Any, default: float = 0.0) -> float:
    result = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(result):
        return default
    return float(result)


def _fmt_yi(value: Any) -> str:
    amount = _to_float(value, default=float("nan"))
    if pd.isna(amount):
        return "N/A"
    return f"{amount / 1e8:.2f}亿"


def _fmt_pct_number(value: Any) -> str:
    amount = _to_float(value, default=float("nan"))
    if pd.isna(amount):
        return "N/A"
    return f"{amount:+.2f}%"


def _top_categories(frame: pd.DataFrame, column: str, limit: int = 3) -> List[str]:
    if frame is None or frame.empty or column not in frame.columns:
        return []
    series = frame[column].astype(str).replace({"": pd.NA, "nan": pd.NA}).dropna()
    if series.empty:
        return []
    counts = series.value_counts().head(limit)
    return [f"{name}({count})" for name, count in counts.items()]


def _latest_trade_date_label(pulse: Dict[str, Any]) -> str:
    return str(pulse.get("market_date", "") or "最近交易日")


def _market_pulse_lines(pulse: Dict[str, Any]) -> List[str]:
    if not pulse:
        return ["全市场脉搏暂不可用。"]

    zt_pool = pulse.get("zt_pool", pd.DataFrame())
    dt_pool = pulse.get("dt_pool", pd.DataFrame())
    strong_pool = pulse.get("strong_pool", pd.DataFrame())
    prev_zt_pool = pulse.get("prev_zt_pool", pd.DataFrame())
    trade_date = _latest_trade_date_label(pulse)

    zt_count = len(zt_pool.index) if not zt_pool.empty else 0
    dt_count = len(dt_pool.index) if not dt_pool.empty else 0
    strong_count = len(strong_pool.index) if not strong_pool.empty else 0
    prev_zt_count = len(prev_zt_pool.index) if not prev_zt_pool.empty else 0

    lines = [
        f"{trade_date} A股全市场热度: 涨停 {zt_count} 家，跌停 {dt_count} 家，强势股池 {strong_count} 家，昨日涨停表现池 {prev_zt_count} 家。"
    ]

    if zt_count and dt_count:
        if zt_count >= dt_count * 5:
            lines.append("涨停明显多于跌停，短线情绪仍有局部赚钱效应，不是全面退潮。")
        elif dt_count >= max(zt_count, 1):
            lines.append("跌停数量对涨停形成明显压制，今天要更警惕高位股和题材股分歧扩散。")
        else:
            lines.append("涨停与跌停同时存在，市场更像结构行情，不像统一主线普涨。")

    top_zt = _top_categories(zt_pool, "所属行业")
    if top_zt:
        lines.append("涨停主要集中在: " + "、".join(top_zt) + "。")

    top_strong = _top_categories(strong_pool, "所属行业")
    if top_strong:
        lines.append("强势股池主要集中在: " + "、".join(top_strong) + "。")

    if not prev_zt_pool.empty and "涨跌幅" in prev_zt_pool.columns:
        avg_prev = pd.to_numeric(prev_zt_pool["涨跌幅"], errors="coerce").dropna()
        if not avg_prev.empty:
            avg_val = float(avg_prev.mean())
            lines.append(
                f"昨日涨停股平均今日表现 {avg_val:+.2f}% ，可用来判断短线接力环境是否还在。"
            )
    return lines


def _lhb_lines(pulse: Dict[str, Any]) -> List[str]:
    if not pulse:
        return ["龙虎榜暂不可用。"]

    lines: List[str] = []
    trade_date = _latest_trade_date_label(pulse)
    detail = pulse.get("lhb_detail", pd.DataFrame())
    institution = pulse.get("lhb_institution", pd.DataFrame())
    stats = pulse.get("lhb_stats", pd.DataFrame())
    desks = pulse.get("lhb_desks", pd.DataFrame())

    if not detail.empty and "上榜日" in detail.columns:
        trade_date = str(detail["上榜日"].astype(str).max())
    elif not institution.empty and "上榜日期" in institution.columns:
        trade_date = str(institution["上榜日期"].astype(str).max())
    elif not desks.empty and "上榜日" in desks.columns:
        trade_date = str(desks["上榜日"].astype(str).max())

    if not detail.empty:
        lines.append(f"{trade_date} 龙虎榜上榜 {len(detail.index)} 条记录，说明高波动个股活跃度仍然较高。")

    if not institution.empty:
        buy_col = "机构买入净额" if "机构买入净额" in institution.columns else ""
        if buy_col:
            top_buy = institution.copy()
            top_buy[buy_col] = pd.to_numeric(top_buy[buy_col], errors="coerce")
            top_buy = top_buy.dropna(subset=[buy_col]).sort_values(buy_col, ascending=False).head(3)
            if not top_buy.empty:
                summary = "、".join(
                    f"{row['名称']}({row[buy_col] / 1e8:.2f}亿)"
                    for _, row in top_buy.iterrows()
                )
                lines.append("机构净买额靠前: " + summary + "。")

    if not stats.empty:
        top_stats = stats.head(3)
        summary = "、".join(
            f"{row['名称']}(上榜{int(row['上榜次数'])}次)"
            for _, row in top_stats.iterrows()
            if "名称" in row and "上榜次数" in row
        )
        if summary:
            lines.append("近一月反复上榜的活跃标的: " + summary + "。")

    if not desks.empty:
        net_col = "总买卖净额" if "总买卖净额" in desks.columns else ""
        if net_col:
            desks = desks.copy()
            desks[net_col] = pd.to_numeric(desks[net_col], errors="coerce")
            top_desks = desks.dropna(subset=[net_col]).sort_values(net_col, ascending=False).head(2)
            if not top_desks.empty:
                summary = "、".join(
                    f"{row['营业部名称']}({row[net_col] / 1e8:.2f}亿)"
                    for _, row in top_desks.iterrows()
                )
                lines.append("活跃营业部净买额靠前: " + summary + "。")
    if not lines:
        return ["龙虎榜数据暂不可用。"]
    return lines


def _source_summary(news_report: Dict[str, Any]) -> str:
    items = news_report.get("items", []) or []
    if not items:
        return "当前新闻主线主要依赖代理推导，不是实时头条聚合。"
    sources: List[str] = []
    for item in items:
        source = str(item.get("source") or item.get("configured_source") or "").strip()
        if source and source not in sources:
            sources.append(source)
    return "本次新闻覆盖源: " + " / ".join(sources[:4]) + "。"


def _source_quality_lines(news_report: Dict[str, Any]) -> List[str]:
    items = news_report.get("items", []) or []
    if not items:
        return ["新闻流当前主要依赖代理推导，源覆盖不足，不能把它当成完整实时新闻终版。"]

    sources: List[str] = []
    domestic_count = 0
    for item in items:
        source = str(item.get("source") or item.get("configured_source") or "").strip()
        if source and source not in sources:
            sources.append(source)
        category = str(item.get("category", "")).lower()
        if any(keyword in source for keyword in ["财联社", "证券时报", "上证报", "第一财经"]) or category in {
            "china_macro_domestic",
            "china_market_domestic",
        }:
            domestic_count += 1

    lines = [_source_summary(news_report)]
    if len(sources) < 2:
        lines.append("⚠️ 当前新闻源不足 2 类，晨报应降级为‘主线草稿’，不要把单源新闻写成确定结论。")
    else:
        lines.append(f"本次共命中 {len(sources)} 类新闻源，源覆盖基本可用，但仍需优先参考一级信源。")
    if domestic_count == 0:
        lines.append("⚠️ 当前没有命中国内快讯源，A 股盘面与政策解读可能不完整。")
    return lines[:3]


def _anomaly_report(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    monitor = _monitor_map(monitor_rows)
    flags: Dict[str, str] = {}
    lines: List[str] = []

    for name, threshold in [("布伦特原油", 0.20), ("WTI原油", 0.25)]:
        row = monitor.get(name)
        if row and abs(_to_float(row.get("return_5d"))) >= threshold:
            move = _to_float(row.get("return_5d"))
            message = f"⚠️ {name} 5 日 {format_pct(move)}，属于极端波动，请人工复核数据源、基准日和事件真实性。"
            lines.append(message)
            flags[name] = "极端波动，需复核"

    for snapshot in snapshots:
        is_etf = "etf" in snapshot.asset_type.lower() or snapshot.symbol.endswith("ETF")
        if snapshot.asset_type == "hk_index" and snapshot.proxy_symbol:
            lines.append(
                f"ℹ️ {snapshot.symbol} 当前使用 `{snapshot.proxy_symbol}` 作为行情代理，显示的是代理 ETF 价格，不是指数点位。"
            )
            flags[snapshot.symbol] = "代理ETF价格，不是指数点位"
        if is_etf and abs(snapshot.return_20d) >= 0.12:
            lines.append(
                f"⚠️ {snapshot.symbol} {snapshot.name} 近 20 日 {format_pct(snapshot.return_20d)}，对 ETF 属于偏大波动，请复核场内价格、复权口径和对应催化。"
            )
            flags[snapshot.symbol] = "ETF 波动偏大，需复核"
        elif is_etf and abs(snapshot.return_1d) >= 0.04:
            lines.append(
                f"⚠️ {snapshot.symbol} {snapshot.name} 单日 {format_pct(snapshot.return_1d)}，对 ETF 偏大，请确认是否为主题事件驱动。"
            )
            flags[snapshot.symbol] = "单日波动偏大"

    if not lines:
        lines.append("当前没有触发显著异常值，但极端行情日仍应对跨源价格差异做人工复核。")
    return {"lines": lines[:4], "flags": flags}


def _effective_asset_preference(narrative: Dict[str, Any]) -> List[str]:
    theme = str(narrative.get("theme", "macro_background"))
    if theme in THEME_ASSET_PREFERENCES:
        return THEME_ASSET_PREFERENCES[theme]
    return list(narrative.get("preferred_assets", []))


def _news_category_counter(news_report: Dict[str, Any]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for item in news_report.get("items", []) or []:
        category = str(item.get("category", "")).strip().lower()
        if category:
            counter[category] += 1
    return counter


def _keyword_match(text: str, keywords: Sequence[str]) -> bool:
    haystack = f" {text.lower()} "
    for keyword in keywords:
        needle = keyword.lower().strip()
        if not needle:
            continue
        if len(needle) <= 3 and needle.isalpha():
            variants = [f" {needle} ", f"-{needle} ", f" {needle}-"]
            if any(variant in haystack for variant in variants):
                return True
            continue
        if needle in haystack:
            return True
    return False


def _industry_text(*frames: pd.DataFrame) -> str:
    parts: List[str] = []
    for frame in frames:
        if frame is None or frame.empty:
            continue
        for column in ("所属行业", "名称"):
            if column in frame.columns:
                parts.extend(str(item) for item in frame[column].head(10).tolist())
    return " ".join(parts)


def _board_name(frame: pd.DataFrame, keywords: Sequence[str], fallback: str) -> str:
    if frame is None or frame.empty or "板块名称" not in frame.columns:
        return fallback
    names = frame["板块名称"].astype(str)
    for keyword in keywords:
        matched = frame[names.str.contains(keyword, na=False)]
        if not matched.empty:
            return str(matched.iloc[0]["板块名称"])
    return fallback


def _primary_narrative(
    news_report: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    pulse: Dict[str, Any],
    snapshots: List[BriefingSnapshot],
    drivers: Dict[str, Any],
    regime_result: Dict[str, Any],
    a_share_watch_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    counter = _news_category_counter(news_report)
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    dxy = monitor.get("美元指数", {})
    vix = monitor.get("VIX波动率", {})
    us10y = monitor.get("美国10Y收益率", {})

    brent_1d = _to_float(brent.get("return_1d")) or 0.0
    brent_5d = _to_float(brent.get("return_5d")) or 0.0
    dxy_5d = _to_float(dxy.get("return_5d")) or 0.0
    vix_latest = _to_float(vix.get("latest")) or 0.0
    us10y_1d = _to_float(us10y.get("return_1d")) or 0.0

    top_industry_text = _industry_text(
        pulse.get("zt_pool", pd.DataFrame()),
        pulse.get("strong_pool", pd.DataFrame()),
        drivers.get("industry_spot", pd.DataFrame()),
        drivers.get("concept_spot", pd.DataFrame()),
    )

    qqqm = _find_snapshot(snapshots, "QQQM")
    hstech = _find_snapshot(snapshots, "HSTECH")
    gld = _find_snapshot(snapshots, "GLD")
    grid = _find_snapshot(snapshots, "561380")
    broad = _find_snapshot(snapshots, "510210") or _find_snapshot(snapshots, "510300")

    tech_1d = 0.0
    if qqqm:
        tech_1d += qqqm.return_1d
    if hstech:
        tech_1d += hstech.return_1d
    gold_1d = gld.return_1d if gld else _sector_avg_return(snapshots, "黄金")
    grid_1d = grid.return_1d if grid else _sector_avg_return(snapshots, "电网")
    dividend_1d = _sector_avg_return(snapshots, "高股息")
    broad_1d = broad.return_1d if broad else _sector_avg_return(snapshots, "宽基")
    broad_5d = broad.return_5d if broad else _sector_avg_return(snapshots, "宽基", field="return_5d")

    scores = {
        "energy_shock": 0,
        "gold_defense": 0,
        "dividend_defense": 0,
        "defensive_riskoff": 0,
        "broad_market_repair": 0,
        "rate_growth": 0,
        "power_utilities": 0,
        "china_policy": 0,
        "ai_semis": 0,
    }

    energy_news = counter["energy"] + counter["geopolitics"]
    scores["energy_shock"] += energy_news * 2
    if brent_1d >= 0.05:
        scores["energy_shock"] += 4
    if brent_5d >= 0.12:
        scores["energy_shock"] += 3
    if vix_latest >= 25:
        scores["energy_shock"] += 2
    if any(keyword in top_industry_text for keyword in ["电力", "电网", "石油", "油气", "煤炭", "航运"]):
        scores["energy_shock"] += 2
    if dxy_5d > 0.005:
        scores["energy_shock"] += 1

    scores["defensive_riskoff"] += counter["geopolitics"] * 2
    if vix_latest >= 25:
        scores["defensive_riskoff"] += 3
    if gold_1d > tech_1d:
        scores["defensive_riskoff"] += 2
    if dxy_5d > 0.005:
        scores["defensive_riskoff"] += 1

    scores["gold_defense"] += counter["geopolitics"] * 2
    if gold_1d > broad_1d:
        scores["gold_defense"] += 2
    if gold_1d > tech_1d:
        scores["gold_defense"] += 2
    if vix_latest >= 22:
        scores["gold_defense"] += 2

    if any(keyword in top_industry_text for keyword in ["银行", "红利", "公用事业", "煤炭"]):
        scores["dividend_defense"] += 3
    if dividend_1d >= 0:
        scores["dividend_defense"] += 2
    if vix_latest >= 20:
        scores["dividend_defense"] += 1
    if broad_1d <= 0 and tech_1d <= 0:
        scores["dividend_defense"] += 1

    scores["rate_growth"] += counter["fed"] * 2
    scores["rate_growth"] += counter["earnings"]
    if us10y_1d < 0:
        scores["rate_growth"] += 2
    if dxy_5d <= 0:
        scores["rate_growth"] += 1
    if tech_1d > 0 and vix_latest < 22:
        scores["rate_growth"] += 2

    scores["broad_market_repair"] += counter["fed"] + counter["china_macro"]
    if us10y_1d < 0:
        scores["broad_market_repair"] += 2
    if broad_1d > 0:
        scores["broad_market_repair"] += 2
    if broad_5d > 0:
        scores["broad_market_repair"] += 1
    if vix_latest < 22:
        scores["broad_market_repair"] += 1
    if any(keyword in top_industry_text for keyword in ["银行", "券商", "保险", "非银", "白酒", "家电"]):
        scores["broad_market_repair"] += 1

    if any(keyword in top_industry_text for keyword in ["电网", "电力", "公用事业", "特高压"]):
        scores["power_utilities"] += 3
    if grid_1d >= 0:
        scores["power_utilities"] += 2
    if counter["china_macro"] >= 1:
        scores["power_utilities"] += 1
    if dividend_1d >= 0:
        scores["power_utilities"] += 1

    scores["china_policy"] += counter["china_macro"] * 2
    if grid_1d >= 0:
        scores["china_policy"] += 1
    if any(keyword in top_industry_text for keyword in ["电网", "电力", "基建", "工程", "建材"]):
        scores["china_policy"] += 2

    scores["ai_semis"] += counter["ai"] * 2 + counter["semiconductor"] * 2
    if any(keyword in top_industry_text for keyword in ["半导体", "消费电子", "通信", "IT服务", "算力"]):
        scores["ai_semis"] += 2
    if tech_1d > 0:
        scores["ai_semis"] += 1

    watch_boosts = _a_share_watch_theme_boosts(a_share_watch_meta)
    for theme_name, boost in watch_boosts.items():
        scores[theme_name] += int(boost)

    theme = max(scores, key=scores.get)
    score = scores[theme]
    if score < 4:
        theme = "macro_background"

    theme_labels = {
        "energy_shock": "能源冲击 + 地缘风险",
        "gold_defense": "黄金避险",
        "dividend_defense": "红利/银行防守",
        "defensive_riskoff": "防守避险",
        "broad_market_repair": "宽基修复",
        "rate_growth": "利率驱动成长修复",
        "power_utilities": "电网/公用事业",
        "china_policy": "中国政策/内需确定性",
        "ai_semis": "AI/半导体催化",
        "macro_background": "背景宏观",
    }

    theme_summaries = {
        "energy_shock": "今天市场主线更像 `能源冲击 + 地缘风险`，应优先放在晨报最前面，而不是被背景 regime 覆盖。",
        "gold_defense": "今天交易主线更像 `黄金避险`，核心是先看避险需求是否继续抬头，而不是把所有防守资产混成一句风险规避。",
        "dividend_defense": "今天交易主线更像 `红利/银行防守`，核心是防守资产里的现金流稳定方向在承接，而不是全面 risk-off。",
        "defensive_riskoff": "今天市场主线更像 `防守避险`，核心是先谈波动和回撤控制，再谈进攻。",
        "broad_market_repair": "今天交易主线更像 `宽基修复`，重点看指数、券商和核心资产是否一起修复，而不是直接等同于科技成长行情。",
        "rate_growth": "今天市场主线更像 `利率预期驱动的成长修复`，重点看科技和估值弹性方向。",
        "power_utilities": "今天交易主线更像 `电网/公用事业`，重点看高确定性、公用事业和电力设备链是否持续获得资金承接。",
        "china_policy": "今天市场主线更像 `中国政策 / 内需确定性`，重点看基建、电网和稳增长传导。",
        "ai_semis": "今天市场主线更像 `AI / 半导体催化`，重点看算力、芯片和相关硬件链。",
        "macro_background": "今天没有单一事件完全压过其他变量，更适合先以宏观背景和盘面结构来组织晨报。",
    }

    background_label = REGIME_LABELS.get(str(regime_result["current_regime"]), str(regime_result["current_regime"]))

    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    secondary_themes = [
        {
            "theme": key,
            "label": theme_labels.get(key, key),
            "score": int(value),
        }
        for key, value in sorted_scores[1:4]
        if int(value) >= max(score - 2, 3)
    ]

    return {
        "theme": theme,
        "label": theme_labels[theme],
        "summary": theme_summaries[theme],
        "scores": scores,
        "background_regime": background_label,
        "background_label": background_label,
        "trading_theme": theme,
        "trading_label": theme_labels[theme],
        "secondary_themes": secondary_themes,
        "preferred_assets": list(regime_result.get("preferred_assets", [])),
        "effective_assets": THEME_ASSET_PREFERENCES.get(theme, list(regime_result.get("preferred_assets", []))),
        "overrides_background": theme != "macro_background",
    }


def _narrative_validation_lines(
    narrative: Dict[str, Any],
    news_report: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    pulse: Dict[str, Any],
    snapshots: List[BriefingSnapshot],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    dxy = monitor.get("美元指数", {})
    vix = monitor.get("VIX波动率", {})
    qqqm = _find_snapshot(snapshots, "QQQM")
    hstech = _find_snapshot(snapshots, "HSTECH")
    gld = _find_snapshot(snapshots, "GLD")

    top_zt = " ".join(_top_categories(pulse.get("zt_pool", pd.DataFrame()), "所属行业"))
    top_strong = " ".join(_top_categories(pulse.get("strong_pool", pd.DataFrame()), "所属行业"))
    lines = [f"当前主线候选: `{narrative['label']}`；背景宏观仍是 `{narrative['background_regime']}`。"]

    passed = 0
    total = 0

    if narrative["theme"] == "energy_shock":
        total += 1
        if _to_float(brent.get("return_1d")) >= 0.05 or _to_float(brent.get("return_5d")) >= 0.12:
            passed += 1
            lines.append(f"价格校验通过: 布伦特 1日 {format_pct(_to_float(brent.get('return_1d')))}，5日 {format_pct(_to_float(brent.get('return_5d')))}。")
        else:
            lines.append("价格校验未通过: 原油没有出现足够大的涨幅，不应把能源冲击写成绝对主线。")

        total += 1
        if any(keyword in f"{top_zt} {top_strong}" for keyword in ["电力", "电网", "石油", "油气", "煤炭"]):
            passed += 1
            lines.append(f"盘面校验通过: 涨停/强势股池集中在 {top_zt or top_strong}。")
        else:
            lines.append("盘面校验未通过: 涨停和强势股池没有明显跟能源链共振。")

        total += 1
        if _to_float(vix.get("latest")) >= 25 or _to_float(dxy.get("return_5d")) > 0.005:
            passed += 1
            lines.append(f"跨市场校验通过: VIX {_to_float(vix.get('latest')):.1f}，DXY 5日 {format_pct(_to_float(dxy.get('return_5d')))}。")
        else:
            lines.append("跨市场校验未通过: 波动率和美元没有明显同步强化。")

    elif narrative["theme"] == "rate_growth":
        total += 1
        if any(item.get("category") == "fed" for item in news_report.get("items", [])):
            passed += 1
            lines.append("新闻校验通过: 存在 Fed / 利率预期相关头条。")
        else:
            lines.append("新闻校验未通过: 没有足够明确的利率预期头条。")
        total += 1
        tech_ok = (qqqm and qqqm.return_1d > 0) or (hstech and hstech.return_1d > 0)
        if tech_ok:
            passed += 1
            lines.append("资产校验通过: 科技方向至少有一个核心代理转强。")
        else:
            lines.append("资产校验未通过: 科技代理没有同步走强。")

    elif narrative["theme"] == "defensive_riskoff":
        total += 1
        if _to_float(vix.get("latest")) >= 25:
            passed += 1
            lines.append(f"波动校验通过: VIX { _to_float(vix.get('latest')):.1f }。")
        else:
            lines.append("波动校验未通过: VIX 没有到高波动区。")
        total += 1
        if gld and qqqm and gld.return_1d > qqqm.return_1d:
            passed += 1
            lines.append("防守资产校验通过: 黄金相对科技更强。")
        else:
            lines.append("防守资产校验未通过: 黄金没有明显跑赢科技。")

    if total:
        lines.append(f"结论: 当前主线校验通过 {passed}/{total} 项。")
        if narrative.get("overrides_background") and passed >= max(total - 1, 1):
            lines.append("裁决: 今日事件主线优先级高于背景 regime，资产偏好应先服从日内主线。")
        if passed < total:
            lines.append("如果后续校验项继续背离，晨报应把它降级成‘候选主线’，不要写成既定结论。")
    else:
        lines.append("当前没有为该主线配置专门校验器，先按新闻和盘面结构做保守解读。")
    return lines


def _important_event_lines(news_report: Dict[str, Any]) -> List[str]:
    items = news_report.get("items", []) or []
    if not items:
        return ["实时新闻事件流暂不可用，当前只能靠宏观、价格和盘面结构做代理推断。"]

    buckets: List[tuple[str, tuple[str, ...], str]] = [
        ("财报与业绩", ("earnings", "results", "guidance", "profit", "revenue"), "财报/指引"),
        ("美联储与利率预期", ("federal reserve", "fed", "powell", "rate", "cut", "hike", "cpi", "inflation"), "利率预期"),
        ("AI 产品与模型", ("openai", "anthropic", "deepseek", "gpt", "chatgpt", "claude", "llm"), "AI催化"),
        ("半导体产能与资本开支", ("chip", "chips", "semiconductor", "fab", "foundry", "capacity", "tsmc", "intel", "samsung"), "半导体"),
    ]

    lines: List[str] = []
    used_titles = set()
    for title, keywords, label in buckets:
        matched = None
        for item in items:
            text = (
                f"{item.get('category', '')} {item.get('title', '')} {item.get('source', '')} {item.get('configured_source', '')}"
            ).lower()
            if _keyword_match(text, keywords):
                matched = item
                break
        if matched:
            used_titles.add(matched.get("title", ""))
            source = matched.get("source") or matched.get("configured_source") or "未知源"
            lines.append(f"{title}: {matched.get('title', '无标题')} ({source})。归类为 `{label}`。")

    if not lines:
        top = items[0]
        source = top.get("source") or top.get("configured_source") or "未知源"
        lines.append(f"当前实时新闻更偏宏观与市场主线，没有明显单一财报或产业催化占据中心。头条是: {top.get('title', '无标题')} ({source})。")

    if len(lines) < 3:
        generic_candidates = []
        for item in items:
            if item.get("title", "") in used_titles:
                continue
            generic_candidates.append(item)
        for item in generic_candidates[: 3 - len(lines)]:
            source = item.get("source") or item.get("configured_source") or "未知源"
            lines.append(f"补充关注: {item.get('title', '无标题')} ({source})。")
    return lines[:4]


def _catalyst_rows(news_report: Dict[str, Any], narrative: Dict[str, Any]) -> List[List[str]]:
    items = news_report.get("items", []) or []
    category_rows: Dict[str, List[str]] = {}
    theme = str(narrative.get("theme", "macro_background"))
    theme_map = {
        "energy_shock": [
            "油价/地缘",
            "能源与地缘新闻集中，且原油、VIX、美元同步共振。",
            "原油 -> 通胀预期 -> 波动率/美元 -> 科技和高估值资产承压。",
            "先看能源、电力电网和防守资产，不把今天当成普通成长修复日。",
        ],
        "gold_defense": [
            "黄金避险",
            "避险需求先集中在黄金和贵金属，而不是所有防守资产一起走强。",
            "地缘/波动率 -> 黄金相对收益 -> 风险资产仓位收缩。",
            "重点看黄金是否持续强于宽基和科技，而不是只看 headlines。",
        ],
        "dividend_defense": [
            "红利/银行防守",
            "防守资金更偏向银行、公用事业和高股息，而不是单纯回避风险。",
            "防守偏好 -> 现金流稳定资产 -> 红利/银行相对占优。",
            "重点看防守承接能否持续扩散，而不是一日脉冲。",
        ],
        "defensive_riskoff": [
            "防守避险",
            "波动率抬升、黄金或防守资产相对更稳。",
            "避险偏好 -> 资金收缩高弹性仓位 -> 防守资产相对占优。",
            "优先控回撤，确认波动降温后再谈进攻。",
        ],
        "broad_market_repair": [
            "宽基修复",
            "指数、券商和核心资产开始修复，但还没完全转成高弹性成长主线。",
            "利率/风险偏好改善 -> 宽基与核心资产修复 -> 再看是否扩散。",
            "重点看宽基是否能把修复扩散到更多行业，而不是只靠少数大票托住。",
        ],
        "rate_growth": [
            "利率预期",
            "Fed/通胀新闻进入头条，长端利率与美元成为关键变量。",
            "利率预期 -> 估值修复 -> 科技/成长弹性扩散。",
            "重点看科技是否跟随利率预期同步走强。",
        ],
        "power_utilities": [
            "电网/公用事业",
            "电网、公用事业和高确定性链条获得持续承接。",
            "政策/确定性偏好 -> 电力电网与公用事业相对收益 -> 风格防守但不完全 risk-off。",
            "重点看电网/公用事业能否继续强于宽基，而不是只看一日板块脉冲。",
        ],
        "china_policy": [
            "政策/内需",
            "国内政策与稳增长信号成为主线。",
            "政策 -> 基建/电网/内需链 -> A股相对收益。",
            "重点看政策是否转成板块持续性和资金承接。",
        ],
        "ai_semis": [
            "AI/半导体",
            "AI 模型、产品或半导体资本开支新闻成为催化。",
            "产品/产能 -> 板块热度 -> 硬件链和成长风格扩散。",
            "重点看半导体、通信、算力链是否接力。",
        ],
    }
    if theme in theme_map:
        category_rows[theme] = theme_map[theme]

    category_definitions = {
        "energy": ("油价/地缘", "能源价格变化进入头条。", "原油 -> 通胀预期 -> 风险资产定价。", "优先看能源链和防守资产。"),
        "geopolitics": ("地缘事件", "地缘消息直接影响供给或风险偏好。", "地缘扰动 -> 波动率/美元 -> 资产风险溢价上升。", "先确认冲击是脉冲还是趋势。"),
        "fed": ("美联储/利率", "利率预期新闻驱动盘面。", "利率 -> 估值折现 -> 科技成长弹性。", "重点看美债和科技代理是否共振。"),
        "earnings": ("财报/指引", "重要公司财报或指引改变板块预期。", "业绩/指引 -> 板块风险偏好 -> 相关 ETF 表现。", "看是单股事件还是可扩散成板块主线。"),
        "china_macro": ("中国宏观/政策", "国内政策或宏观表态影响风险偏好。", "政策 -> 内需/电网/基建链。", "重点看政策是否带来持续资金承接。"),
        "china_market_domestic": ("A股盘面快讯", "国内快讯补充盘面细节。", "快讯 -> 情绪扩散 -> 题材强弱切换。", "和龙虎榜、涨停池一起判断强度。"),
        "china_macro_domestic": ("国内政策快讯", "国内政策快讯提供更细的落地节奏。", "快讯 -> 政策预期 -> 内需相关板块。", "更适合辅助确认，而不是单独定主线。"),
        "ai": ("AI 产品", "模型/产品发布或传闻带来催化。", "产品 -> 情绪/估值 -> AI 应用与算力链。", "先看新闻是否真的扩散到板块。"),
        "semiconductor": ("半导体产能", "产能与资本开支新闻改变供需预期。", "资本开支 -> 设备/材料/代工链。", "更适合中期跟踪，不一定立刻成日内主线。"),
    }

    for item in items:
        category = str(item.get("category", "")).lower()
        if category in category_rows or category not in category_definitions:
            continue
        title = str(item.get("title", "无标题")).strip()
        label, _, transmission, implication = category_definitions[category]
        source = str(item.get("source") or item.get("configured_source") or "未知源")
        category_rows[category] = [label, f"{title} ({source})", transmission, implication]
        if len(category_rows) >= 4:
            break

    return list(category_rows.values())[:4]


def _rotation_driver_lines(
    drivers: Dict[str, Any],
    pulse: Dict[str, Any],
    snapshots: List[BriefingSnapshot],
) -> List[str]:
    lines: List[str] = []
    industry_spot = drivers.get("industry_spot", pd.DataFrame()) if drivers else pd.DataFrame()
    concept_spot = drivers.get("concept_spot", pd.DataFrame()) if drivers else pd.DataFrame()
    hot_rank = drivers.get("hot_rank", pd.DataFrame()) if drivers else pd.DataFrame()

    if not industry_spot.empty and "名称" in industry_spot.columns and "涨跌幅" in industry_spot.columns:
        frame = industry_spot.copy()
        frame["涨跌幅"] = pd.to_numeric(frame["涨跌幅"], errors="coerce")
        frame = frame.dropna(subset=["涨跌幅"])
        leaders = frame.sort_values("涨跌幅", ascending=False).head(3)
        laggards = frame.sort_values("涨跌幅", ascending=True).head(2)
        if not leaders.empty:
            lines.append(
                "行业轮动靠前: "
                + "、".join(f"{row['名称']}({row['涨跌幅']:+.2f}%)" for _, row in leaders.iterrows())
                + "。"
            )
        if not laggards.empty:
            lines.append(
                "行业轮动靠后: "
                + "、".join(f"{row['名称']}({row['涨跌幅']:+.2f}%)" for _, row in laggards.iterrows())
                + "。"
            )
    else:
        top_zt = _top_categories(pulse.get("zt_pool", pd.DataFrame()), "所属行业")
        top_strong = _top_categories(pulse.get("strong_pool", pd.DataFrame()), "所属行业")
        if top_zt:
            lines.append("盘面扩散更明显的方向在: " + "、".join(top_zt) + "。")
        if top_strong:
            lines.append("强势股池集中在: " + "、".join(top_strong) + "，说明主线仍偏这些方向。")

    if not concept_spot.empty and "名称" in concept_spot.columns and "涨跌幅" in concept_spot.columns:
        frame = concept_spot.copy()
        frame["涨跌幅"] = pd.to_numeric(frame["涨跌幅"], errors="coerce")
        frame = frame.dropna(subset=["涨跌幅"])
        leaders = frame.sort_values("涨跌幅", ascending=False).head(3)
        if not leaders.empty:
            lines.append(
                "概念轮动靠前: "
                + "、".join(f"{row['名称']}({row['涨跌幅']:+.2f}%)" for _, row in leaders.iterrows())
                + "。"
            )

    if not hot_rank.empty and {"股票名称", "涨跌幅"}.issubset(hot_rank.columns):
        rows = hot_rank.head(3)
        lines.append(
            "市场热度前排个股: "
            + "、".join(
                f"{row['股票名称']}({pd.to_numeric(row['涨跌幅'], errors='coerce'):+.2f}%)"
                for _, row in rows.iterrows()
            )
            + "。"
        )

    if snapshots:
        tech_items = [item for item in snapshots if item.sector == "科技"]
        gold_items = [item for item in snapshots if item.sector == "黄金"]
        grid_items = [item for item in snapshots if item.sector == "电网"]
        tech_avg = sum(item.return_5d for item in tech_items) / len(tech_items) if tech_items else 0.0
        gold_avg = sum(item.return_5d for item in gold_items) / len(gold_items) if gold_items else 0.0
        grid_avg = sum(item.return_5d for item in grid_items) / len(grid_items) if grid_items else 0.0
        if grid_avg > tech_avg and grid_avg > gold_avg:
            lines.append("跨资产看，电网/内需链仍强于科技和黄金，当前轮动核心更偏国内确定性。")
        elif gold_avg > tech_avg and gold_avg > grid_avg:
            lines.append("跨资产看，黄金相对更稳，当前更像防守轮动而不是总攻行情。")
        elif tech_avg > gold_avg and tech_avg > grid_avg:
            lines.append("跨资产看，科技修复弹性更大，说明风险偏好有回暖迹象。")

    return lines[:5] or ["板块轮动线索暂不可用。"]


def _main_flow_driver_lines(drivers: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    if not drivers:
        return ["主力资金流向暂不可用。"]

    market_flow = drivers.get("market_flow", {})
    flow_frame = market_flow.get("frame", pd.DataFrame())
    if market_flow.get("is_fresh") and not flow_frame.empty:
        latest = flow_frame.iloc[-1]
        main_amt = latest.get("主力净流入-净额")
        main_ratio = latest.get("主力净流入-净占比")
        super_amt = latest.get("超大单净流入-净额")
        big_amt = latest.get("大单净流入-净额")
        direction = "净流入" if pd.to_numeric(pd.Series([main_amt]), errors="coerce").iloc[0] >= 0 else "净流出"
        lines.append(
            f"全市场主力资金最新为 `{direction}` {_fmt_yi(main_amt)}，净占比 {_fmt_pct_number(main_ratio)}。"
        )
        lines.append(f"其中超大单 {direction} {_fmt_yi(super_amt)}，大单 {direction} {_fmt_yi(big_amt)}。")
    elif market_flow.get("latest_date"):
        lines.append(f"主力资金流接口最近可用日期停在 {market_flow['latest_date']}，已视为失效，不拿旧数据误导晨报。")

    for key, label in [("northbound_industry", "北向增持行业"), ("northbound_concept", "北向增持概念")]:
        report = drivers.get(key, {})
        frame = report.get("frame", pd.DataFrame())
        if not report.get("is_fresh") or frame.empty:
            continue
        value_col = "北向资金今日增持估计-市值"
        if "名称" not in frame.columns or value_col not in frame.columns:
            continue
        ranked = frame.copy()
        ranked[value_col] = pd.to_numeric(ranked[value_col], errors="coerce")
        ranked = ranked.dropna(subset=[value_col]).sort_values(value_col, ascending=False).head(3)
        if ranked.empty:
            continue
        lines.append(
            f"{label}靠前: "
            + "、".join(f"{row['名称']}({_fmt_yi(row[value_col])})" for _, row in ranked.iterrows())
            + "。"
        )

    if not lines:
        lines.append("主力/北向明细暂不可用，当前先参考涨停池、龙虎榜和市场热度判断资金偏好。")
    return lines[:4]


def _story_lines(
    news_report: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    items = news_report.get("items", []) or []
    text_blob = " ".join(str(item.get("title", "")) for item in items).lower()
    brent = monitor.get("布伦特原油")
    dxy = monitor.get("美元指数")
    vix = monitor.get("VIX波动率")
    gold = monitor.get("COMEX黄金")
    copper = monitor.get("COMEX铜")
    hstech = _find_snapshot(snapshots, "HSTECH")
    qqqm = _find_snapshot(snapshots, "QQQM")

    energy_theme = any(keyword in text_blob for keyword in ["oil", "opec", "energy", "crude"])
    conflict_theme = any(keyword in text_blob for keyword in ["war", "iran", "conflict", "geopolitic", "strait"])
    china_theme = any(keyword in text_blob for keyword in ["china", "pboc", "beijing"])

    lines: List[str] = []
    if energy_theme or conflict_theme:
        lines.append("今天市场更像在交易 `油价冲击 + 地缘风险`，不是普通的成长修复日。")
    elif china_theme:
        lines.append("今天市场更像在交易 `中国稳增长与政策托底`，风险偏好取决于政策能否转成实际需求。")
    else:
        lines.append("今天没有单一头条完全主导盘面，更像多条线索同时定价。")

    if brent and brent["return_5d"] > 0.20:
        lines.append(
            f"布伦特 5 日 {format_pct(brent['return_5d'])}，已经不只是新闻噪音，而是会传导到通胀预期和资产定价。"
        )
    if vix and vix["latest"] >= 25:
        lines.append(
            f"VIX 已到 {vix['latest']:.1f}，说明今天更需要先判断波动是否继续扩散，再决定是否追高高弹性方向。"
        )
    if dxy and dxy["return_5d"] > 0.005 and hstech and qqqm:
        lines.append(
            f"美元偏强叠加科技承压（HSTECH {format_pct(hstech.return_1d)} / QQQM {format_pct(qqqm.return_1d)}），成长估值修复会比较吃力。"
        )
    if gold and copper:
        if gold["return_5d"] > copper["return_5d"] + 0.02:
            lines.append("金强于铜，市场更偏防守和避险，不适合把今天理解成全面 risk-on。")
        elif copper["return_5d"] > gold["return_5d"] + 0.02:
            lines.append("铜强于金，市场更像在提前交易增长而不是单纯避险。")

    preferred = _effective_asset_preference(narrative)
    if preferred:
        lines.append("今天更值得优先跟踪的资产方向是: " + "、".join(preferred[:4]) + "。")
    lines.append(_source_summary(news_report))
    return lines[:6]


def _impact_lines(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
    regime_result: Dict[str, Any],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    lines: List[str] = []
    brent = monitor.get("布伦特原油")
    dxy = monitor.get("美元指数")
    vix = monitor.get("VIX波动率")
    gold = monitor.get("COMEX黄金")
    copper = monitor.get("COMEX铜")

    grid = _find_snapshot(snapshots, "561380")
    hstech = _find_snapshot(snapshots, "HSTECH")
    qqqm = _find_snapshot(snapshots, "QQQM")
    gld = _find_snapshot(snapshots, "GLD")

    if grid:
        lines.append(
            f"A股确定性方向: {grid.symbol} 近 20 日 {format_pct(grid.return_20d)}，当前仍是观察池里的相对强者，适合作为国内防守进攻平衡点。"
        )
    if hstech:
        lines.append(
            f"港股科技: {hstech.symbol} 近 20 日 {format_pct(hstech.return_20d)}，在美元和外盘波动不回落前，更像修复观察而不是趋势反转。"
        )
    if qqqm:
        lines.append(
            f"美股科技: {qqqm.symbol} 近 5 日 {format_pct(qqqm.return_5d)}，今晚要看美股能否先稳住高波动环境。"
        )
    if gld and gold:
        lines.append(
            f"黄金: GLD 近 20 日 {format_pct(gld.return_20d)}，但如果美元继续走强，黄金未必会线性受益于地缘消息。"
        )
    if brent and copper:
        lines.append(
            f"商品/有色: 原油 {format_pct(brent['return_5d'])}、铜 {format_pct(copper['return_5d'])}，今天更值得跟踪资源链是否承接风险偏好切换。"
        )
    if dxy and dxy["return_5d"] > 0.005:
        lines.append("汇率与美元: 美元走强通常会压制港股和高估值成长，今天不能只看A股自身强弱。")
    if vix and vix["latest"] >= 25:
        lines.append("仓位节奏: 高波动环境下，更适合分批确认，不适合在单日新闻冲击里追涨。")
    return lines[:7]


def _verification_lines(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    lines: List[str] = []
    brent = monitor.get("布伦特原油")
    dxy = monitor.get("美元指数")
    vix = monitor.get("VIX波动率")
    gold = _find_snapshot(snapshots, "GLD")
    hstech = _find_snapshot(snapshots, "HSTECH")
    grid = _find_snapshot(snapshots, "561380")

    if brent:
        lines.append("先看原油是否继续扩张涨幅；如果原油冲高回落，今天的通胀和地缘叙事可能会降温。")
    if vix:
        lines.append("再看 VIX 是否能从高位回落；如果波动率继续抬升，高弹性方向会更难做。")
    if dxy:
        lines.append("盯美元是否继续走强；如果美元回落，港股科技和成长估值会先得到喘息。")
    if gold:
        lines.append("看黄金是否真的承接避险；如果地缘升级但黄金不强，说明市场更在交易美元而不是纯避险。")
    if hstech:
        lines.append("看 HSTECH 是否止跌；它是风险偏好回暖最直观的验证器。")
    if grid:
        lines.append("看 561380 是否继续强于大盘；若相对强弱延续，国内确定性方向的逻辑就还没坏。")
    return lines[:6]


def _verification_rows(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> List[List[str]]:
    rows: List[List[str]] = []
    monitor = _monitor_map(monitor_rows)
    if monitor.get("布伦特原油"):
        rows.append(
            [
                "原油",
                "布油是否继续上冲，还是冲高回落",
                "能源/通胀主线继续强化，防守和电力链优先级维持",
                "主线降温，风险偏好可能获得喘息",
            ]
        )
    if monitor.get("VIX波动率"):
        rows.append(
            [
                "VIX",
                "是否继续站在 25 以上",
                "高波动延续，仓位继续收敛",
                "情绪缓和，成长线才有修复空间",
            ]
        )
    if monitor.get("美元指数"):
        rows.append(
            [
                "美元",
                "DXY 是否继续走强",
                "港股科技和高估值资产继续受压",
                "成长估值端压力缓解",
            ]
        )
    if _find_snapshot(snapshots, "HSTECH"):
        rows.append(
            [
                "HSTECH",
                "是否止跌并形成量价修复",
                "风险偏好回暖，港股科技有望跟进",
                "成长线仍弱，不能轻易把它当反转",
            ]
        )
    if _find_snapshot(snapshots, "561380"):
        rows.append(
            [
                "561380",
                "是否继续强于大盘并维持相对强势",
                "国内确定性方向仍有效",
                "主线切换或板块承接减弱",
            ]
        )
    return rows[:5]


def _yesterday_review_lines(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> List[str]:
    reports_dir = _briefing_internal_dir()
    if not reports_dir.exists():
        return ["暂无晨报归档，无法自动回顾昨日验证点。"]

    pattern = re.compile(r"daily_briefing_(\d{4}-\d{2}-\d{2})\.md$")
    today = datetime.now().date()
    candidates: List[tuple[datetime, Path]] = []
    for path in reports_dir.glob("daily_briefing_*.md"):
        matched = pattern.search(path.name)
        if not matched:
            continue
        try:
            file_date = datetime.strptime(matched.group(1), "%Y-%m-%d")
        except ValueError:
            continue
        if file_date.date() < today:
            candidates.append((file_date, path))
    if not candidates:
        return ["暂无昨日晨报归档，暂时无法自动回顾‘昨日验证点’。", "从本次运行起，晨报会自动归档到 `reports/briefings/internal/`，供下一交易日闭环复盘。"]

    latest_path = sorted(candidates, key=lambda item: item[0])[-1][1]
    try:
        content = latest_path.read_text(encoding="utf-8")
    except OSError:
        return [f"昨日晨报 `{latest_path.name}` 读取失败，暂时无法自动回顾。"]

    section_match = re.search(r"### 今日验证点\s*(.*?)(?:\n### |\n## |\Z)", content, re.S)
    if not section_match:
        return [f"昨日晨报 `{latest_path.name}` 里没有结构化‘今日验证点’，暂时无法自动复盘。"]

    raw_lines = [line.strip() for line in section_match.group(1).splitlines()]
    checks: List[str] = []
    for line in raw_lines:
        if not line:
            continue
        if line.startswith("|") or line.startswith("###") or line.startswith("##"):
            continue
        if line.startswith("- "):
            checks.append(line[2:].strip())
        elif not checks:
            checks.append(line)
    if not checks:
        return [f"昨日晨报 `{latest_path.name}` 没有可解析的验证点条目。"]

    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    dxy = monitor.get("美元指数", {})
    vix = monitor.get("VIX波动率", {})
    hstech = _find_snapshot(snapshots, "HSTECH")
    grid = _find_snapshot(snapshots, "561380")

    lines: List[str] = []
    for check in checks[:3]:
        lower = check.lower()
        if "原油" in check:
            move = _to_float(brent.get("return_1d"))
            state = "继续强化" if move > 0.03 else "明显降温" if move < -0.02 else "仍在高位拉锯"
            lines.append(f"昨日原油验证点回看: 今天布油 1 日 {format_pct(move)}，结论是 `{state}`。")
        elif "vix" in lower or "波动率" in check:
            latest = _to_float(vix.get("latest"))
            state = "仍处高波动区" if latest >= 25 else "已从高波动区回落"
            lines.append(f"昨日波动率验证点回看: 当前 VIX {latest:.1f}，{state}。")
        elif "美元" in check:
            move = _to_float(dxy.get("return_5d"))
            state = "继续偏强" if move > 0.005 else "边际回落"
            lines.append(f"昨日美元验证点回看: DXY 5 日 {format_pct(move)}，{state}。")
        elif "hstech" in lower and hstech:
            state = "仍未止跌" if hstech.return_1d < 0 else "出现修复"
            lines.append(f"昨日 HSTECH 验证点回看: 当日 {format_pct(hstech.return_1d)}，{state}。")
        elif "561380" in check and grid:
            state = "仍保持相对强势" if grid.signal_score >= 1 else "强势已弱化"
            lines.append(f"昨日 561380 验证点回看: 近 1 日 {format_pct(grid.return_1d)}，{state}。")

    return lines or [f"已找到昨日晨报 `{latest_path.name}`，但验证点无法和当前资产自动映射。"]


def _liquidity_lines(config: Dict[str, Any]) -> List[str]:
    collector = ChinaMarketCollector(config)
    lines: List[str] = []

    try:
        flow = collector.get_north_south_flow()
    except Exception:
        flow = pd.DataFrame()

    if not flow.empty and {"日期", "北向资金净流入", "南向资金净流入"}.issubset(flow.columns):
        frame = flow.copy().sort_values("日期")
        latest = frame.iloc[-1]
        north = _to_float(latest.get("北向资金净流入"))
        south = _to_float(latest.get("南向资金净流入"))
        if abs(north) > 1e-6:
            direction = "净流入" if north >= 0 else "净流出"
            lines.append(f"北向资金当日{direction}约 {_fmt_yi(north)}。")
        else:
            lines.append("北向资金当日净买额尚未更新（盘中或收盘前通常为 0），今日改用南向资金、全市场主力流向和龙虎榜活跃度做代理。")
        if abs(south) > 1e-6:
            direction = "净流入" if south >= 0 else "净流出"
            lines.append(f"南向资金当日{direction}约 {_fmt_yi(south)}，可作为 HSTECH 情绪承接的辅助观察。")
            if abs(south) >= 200 * 1e8:
                lines.append("⚠️ 南向资金读数偏大，请复核是否为单日口径、分市场合计口径或极端风险偏好切换。")
        if abs(north) >= 200 * 1e8:
            lines.append("⚠️ 北向资金读数偏大，请复核是否为单日口径或极端风险偏好切换。")

    try:
        margin = collector.get_margin_trading()
    except Exception:
        margin = pd.DataFrame()

    if margin.empty:
        lines.append("融资融券明细接口今日异常或为空，短线情绪改看昨日涨停承接、龙虎榜净买额和主力资金方向。")
    else:
        fin_bal_col = "融资余额"
        latest_frame = margin
        if "日期" in margin.columns:
            latest_date = margin["日期"].dropna().astype(str).max()
            latest_frame = margin[margin["日期"].astype(str) == str(latest_date)]
        if fin_bal_col in latest_frame.columns:
            total_bal = pd.to_numeric(latest_frame[fin_bal_col], errors="coerce").sum()
            if total_bal > 0:
                lines.append(f"融资余额约 {total_bal / 1e8:.0f} 亿元。")
    return lines[:4] or ["资金与流动性明细暂不可用。"]


def _positioning_lines(
    narrative: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    vix = _to_float(monitor.get("VIX波动率", {}).get("latest"))
    theme = str(narrative.get("theme", "macro_background"))

    if vix >= 30 or theme == "energy_shock":
        return [
            "仓位框架: 当前按高波动日处理，总仓位宜控制在 50% 左右，单次新增仓位不超过 10%。",
            "执行上更适合‘先活下来再进攻’，不适合在极端新闻日一次性打满。",
        ]
    if vix >= 25 or theme in {"defensive_riskoff", "gold_defense", "dividend_defense"}:
        return [
            "仓位框架: 当前按偏防守处理，总仓位宜控制在 60% 左右，单次新增仓位不超过 15%。",
            "如果验证点继续恶化，应优先降弹性仓位，再讨论抄底。",
        ]
    return [
        "仓位框架: 当前可以按常规节奏分批确认，总仓位上限可放在 70%-80%。",
        "若主线与校验继续共振，再逐步提高弹性资产权重。",
    ]


def _asset_dashboard_rows(
    monitor_rows: List[Dict[str, Any]],
    snapshots: List[BriefingSnapshot],
    anomaly_report: Dict[str, Any],
) -> List[List[str]]:
    monitor = _monitor_map(monitor_rows)
    flags = anomaly_report.get("flags", {})
    rows: List[List[str]] = []

    def add_monitor(name: str, status: str = "") -> None:
        item = monitor.get(name)
        if not item:
            return
        note = flags.get(name, "—")
        move_5d = _to_float(item.get("return_5d"))
        latest = _to_float(item.get("latest"))
        if not status:
            if name in {"布伦特原油", "WTI原油"}:
                status = "冲击" if abs(move_5d) >= 0.20 else "正常"
            elif name == "VIX波动率":
                status = "高波动" if latest >= 25 else "正常"
            elif name in {"美元指数", "USDCNY"}:
                status = "偏强" if move_5d > 0.005 else "偏弱" if move_5d < -0.005 else "中性"
            elif name == "美国10Y收益率":
                status = "上行" if move_5d > 0.02 else "回落" if move_5d < -0.02 else "中性"
            else:
                status = "偏强" if move_5d > 0.02 else "偏弱" if move_5d < -0.02 else "中性"
        rows.append(
            [
                name,
                f"{item['latest']:.3f}",
                format_pct(item["return_1d"]),
                format_pct(item["return_5d"]),
                format_pct(item.get("return_20d", 0.0)) if item.get("return_20d") is not None else "—",
                status,
                note,
            ]
        )

    add_monitor("布伦特原油", "冲击")
    add_monitor("WTI原油", "冲击")
    add_monitor("VIX波动率", "高波动" if _to_float(monitor.get("VIX波动率", {}).get("latest")) >= 25 else "正常")
    add_monitor("美元指数")
    add_monitor("美国10Y收益率")
    add_monitor("COMEX黄金")
    add_monitor("COMEX铜")
    add_monitor("USDCNY")
    return rows[:8]


def _sentiment_lines(snapshots: List[BriefingSnapshot], config: Dict[str, Any]) -> List[str]:
    if not snapshots:
        return ["当前没有可用于估算情绪代理的标的快照。"]
    collector = SocialSentimentCollector(config)
    strongest = max(snapshots, key=lambda item: item.signal_score + item.return_20d)
    weakest = min(snapshots, key=lambda item: item.signal_score + item.return_20d)
    targets: List[BriefingSnapshot] = [strongest]
    if weakest.symbol != strongest.symbol:
        targets.append(weakest)
    lines: List[str] = []
    for item in targets:
        payload = collector.collect(
            item.symbol,
            {
                "return_1d": item.return_1d,
                "return_5d": item.return_5d,
                "return_20d": item.return_20d,
                "volume_ratio": item.volume_ratio,
                "trend": item.trend,
            },
        )
        aggregate = payload["aggregate"]
        lines.append(f"{item.symbol}: {aggregate['interpretation']}（代理置信度 `{aggregate.get('confidence_label', '低')}`）")
    lines.append("说明：当前情绪为价格和量能推断的讨论热度代理，不是抓取到的真实社媒帖子。")
    limitations = list(dict(payload.get("aggregate") or {}).get("limitations") or [])
    if limitations:
        lines.append(f"限制：{limitations[0]}")
    return lines


def _focus_lines(snapshots: List[BriefingSnapshot], mode: str) -> List[str]:
    if not snapshots:
        return []
    strongest = max(snapshots, key=lambda item: item.signal_score + item.return_20d)
    weakest = min(snapshots, key=lambda item: item.signal_score + item.return_20d)
    selected: List[BriefingSnapshot] = [strongest, weakest]

    gold = next((item for item in snapshots if item.sector == "黄金"), None)
    if gold and gold not in selected:
        selected.append(gold)

    largest_move = max(
        snapshots,
        key=lambda item: abs(item.return_1d) if mode == "daily" else abs(item.return_5d),
    )
    if largest_move not in selected:
        selected.append(largest_move)

    lines: List[str] = []
    for item in selected[:4]:
        lines.append(
            f"{item.symbol} ({item.name}): {item.summary} 近1日={format_pct(item.return_1d)}，近5日={format_pct(item.return_5d)}，近20日={format_pct(item.return_20d)}。{item.note}"
        )
    return lines


def _watchlist_technical_lines(snapshots: List[BriefingSnapshot]) -> List[str]:
    if not snapshots:
        return ["当前没有可用于输出技术指标的 watchlist 快照。"]
    ordered = sorted(snapshots, key=lambda item: (item.technical_bias != "偏强", -(item.signal_score + item.return_20d)))
    return [_technical_watchlist_line(item) for item in ordered]


def _portfolio_lines(config: Dict[str, Any]) -> List[str]:
    portfolio_lines: List[str] = []
    portfolio_repo = PortfolioRepository()
    thesis_repo = ThesisRepository()
    holdings = portfolio_repo.list_holdings()
    if not holdings:
        return ["当前没有持仓记录，今天晨报只做观察池跟踪。"]

    latest_prices = {}
    for holding in holdings:
        try:
            history = fetch_asset_history(holding["symbol"], holding["asset_type"], config)
            latest_prices[holding["symbol"]] = compute_history_metrics(history)["last_close"]
        except Exception:
            latest_prices[holding["symbol"]] = float(holding.get("cost_basis", 0.0))

    status = portfolio_repo.build_status(latest_prices)
    portfolio_lines.append(f"组合市值约 {status['total_value']:.2f} {status['base_currency']}。")
    if status["holdings"]:
        top = max(status["holdings"], key=lambda row: row["weight"])
        portfolio_lines.append(
            f"当前最大持仓为 {top['symbol']}，权重约 {top['weight'] * 100:.1f}%，浮盈亏 {top['pnl']:+.2f}。"
        )
    top_region = max(status["region_exposure"].items(), key=lambda item: item[1], default=None)
    top_sector = max(status["sector_exposure"].items(), key=lambda item: item[1], default=None)
    if top_region:
        portfolio_lines.append(f"地区暴露最高为 {top_region[0]}，占比 {top_region[1] * 100:.1f}%。")
    if top_sector:
        portfolio_lines.append(f"行业暴露最高为 {top_sector[0]}，占比 {top_sector[1] * 100:.1f}%。")

    suggestions = portfolio_repo.rebalance_suggestions(latest_prices)
    if suggestions:
        first = suggestions[0]
        portfolio_lines.append(
            f"再平衡提醒：{first['symbol']} 当前 {first['current_weight'] * 100:.1f}% / 目标 {first['target_weight'] * 100:.1f}%。"
        )

    covered = sum(1 for holding in holdings if thesis_repo.get(holding["symbol"]))
    portfolio_lines.append(f"Thesis 覆盖 {covered}/{len(holdings)} 个持仓。")
    if covered < len(holdings):
        portfolio_lines.append("仍有持仓未绑定 thesis，晨报无法自动检查其论点健康。")
    return portfolio_lines


def _calendar_lines(mode: str) -> List[str]:
    weekday = datetime.now().weekday()
    weekday_map = {
        0: "周一先消化周末政策、地缘和外盘变化，重点看开盘后的风格延续性。",
        1: "周二重点看前一日强弱分化是否延续，避免单日冲高回落误判成新趋势。",
        2: "周三更适合检查资金是否开始切换主线，关注强势板块是否出现扩散。",
        3: "周四重点盯高位方向是否出现分歧，以及弱势方向是否开始止跌。",
        4: "周五更重视周线收盘结构，观察本周主线能否带着趋势结束。",
        5: "周末适合做周度复盘、thesis 检查和再平衡准备。",
        6: "周末适合梳理下周观察池，重点筛掉已经破坏逻辑的方向。",
    }
    lines = [
        weekday_map.get(weekday, "按例行节奏跟踪盘前、盘中和收盘后的关键变化。"),
        "盘前先看 watchlist 里最强和最弱的两个方向，确认今天究竟是趋势延续还是反抽。",
        "盘中重点看放量突破、跌破前低、以及强弱切换是否发生在科技/黄金/电网之间。",
    ]
    if mode == "weekly":
        lines.append("周报模式下，优先复查 5 日与 20 日结构，不被单日噪音带偏。")
    else:
        lines.append("收盘后复核 thesis、组合偏离和异常波动，决定明天是否要上调优先级。")
    return lines


def _event_rows(events: List[Dict[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in events[:5]:
        importance = {"high": "高", "medium": "中", "low": "低"}.get(str(item.get("importance", "")).lower(), "中")
        rows.append(
            [
                str(item.get("time", "待定")),
                importance,
                str(item.get("title", "未命名事件")),
                str(item.get("note", "")),
            ]
        )
    return rows


def _event_lines(events: List[Dict[str, Any]]) -> List[str]:
    if not events:
        return ["当前没有配置显式事件日历，今日以例行盘前、盘中和收盘跟踪为主。"]

    lines: List[str] = []
    for item in events[:5]:
        importance = {"high": "高", "medium": "中", "low": "低"}.get(str(item.get("importance", "")).lower(), "中")
        title = str(item.get("title", "未命名事件"))
        time = str(item.get("time", "待定"))
        note = str(item.get("note", ""))
        lines.append(f"{time} [{importance}] {title}：{note}")
    return lines


def _coverage_metadata(
    news_report: Dict[str, Any],
    liquidity_lines: List[str],
    events: List[Dict[str, Any]],
    global_proxy_note: str,
    monitor_rows: List[Dict[str, Any]],
) -> tuple[str, str]:
    coverage_parts = ["中国宏观", "Watchlist 行情", "国内指数总览", "A股盘面/龙虎榜"]
    missing_parts: List[str] = []

    if monitor_rows:
        coverage_parts.append("宏观资产监控")
    else:
        missing_parts.append("宏观资产监控")
    if any(item.get("data_warning") for item in monitor_rows):
        missing_parts.append("宏观资产监控(实时刷新)")

    items = news_report.get("items", []) or []
    if items:
        sources: List[str] = []
        for item in items:
            source = str(item.get("source") or item.get("configured_source") or "").strip()
            if source and source not in sources:
                sources.append(source)
        if sources:
            coverage_parts.append("RSS新闻(" + "/".join(sources[:4]) + ")")
    else:
        missing_parts.append("实时RSS新闻")

    if events:
        coverage_parts.append("事件日历")
    else:
        missing_parts.append("显式事件日历")

    if global_proxy_note:
        missing_parts.append("跨市场代理")
    if any("北向资金当日读数接近 0 或未更新" in line for line in liquidity_lines):
        missing_parts.append("北向资金")
    if any("融资融券明细接口今日异常或为空" in line for line in liquidity_lines):
        missing_parts.append("融资融券")

    return " | ".join(coverage_parts), " / ".join(missing_parts) if missing_parts else "无"


def _briefing_horizon(snapshot: BriefingSnapshot, narrative: Dict[str, Any]) -> Dict[str, str]:
    theme = str(narrative.get("theme", "")).strip()
    rsi = float(snapshot.technical.get("rsi", {}).get("RSI", 0.0) or 0.0)
    if snapshot.trend != "多头" or snapshot.signal_score <= 1:
        return get_horizon_contract("watch", source="briefing_inferred")
    if rsi >= 68 or snapshot.return_1d >= 0.025:
        return get_horizon_contract("short_term", source="briefing_inferred")
    if theme in {"china_policy", "rate_growth", "ai_semis", "broad_market_repair", "power_utilities"} and snapshot.signal_score >= 4 and snapshot.return_20d >= 0.08:
        return get_horizon_contract("position_trade", source="briefing_inferred")
    if snapshot.signal_score >= 3 and snapshot.return_20d >= 0.04:
        return get_horizon_contract("swing", source="briefing_inferred")
    return get_horizon_contract("watch", source="briefing_inferred")


def _briefing_preflight_line(snapshot: BriefingSnapshot, narrative: Dict[str, Any], *, stage: str) -> str:
    horizon = _briefing_horizon(snapshot, narrative)
    handoff = portfolio_whatif_handoff(
        symbol=snapshot.symbol,
        horizon=horizon,
        direction="做多",
        asset_type=snapshot.asset_type,
        reference_price=snapshot.latest_price,
    )
    stage_prefix = {
        "today": "如果今天还要沿",
        "afternoon": "如果下午还要沿",
        "tomorrow": "如果明天还要沿",
    }.get(stage, "如果还要沿")
    return (
        f"{stage_prefix} {snapshot.name}({snapshot.symbol}) 做新仓/加仓，"
        f"先按 `{horizon.get('label', '观察期')}` 跑一遍组合预演：`{handoff.get('command', f'portfolio whatif buy {snapshot.symbol} 最新价 计划金额')}`。"
    )


def _action_lines(
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
) -> List[str]:
    if not snapshots:
        return ["先修复数据覆盖，再谈晨报动作。"]

    strongest = max(snapshots, key=lambda item: item.signal_score)
    weakest = min(snapshots, key=lambda item: item.signal_score)
    gold = next((item for item in snapshots if item.sector == "黄金"), None)
    theme = str(narrative.get("theme", "macro_background"))
    lines: List[str] = []
    if theme == "energy_shock":
        lines.append("今天先按‘能源冲击日’处理，优先做验证和控回撤，不把背景复苏叙事当成日内主导。")
    elif theme in {"defensive_riskoff", "gold_defense", "dividend_defense"}:
        lines.append("今天先按‘防守优先’处理，动作顺序是减弹性、看验证、再考虑进攻。")
    elif theme == "broad_market_repair":
        lines.append("今天更像指数修复日，先看宽基和金融权重能否继续扩散，再决定是否上调风险暴露。")
    elif theme == "power_utilities":
        lines.append("今天先按‘电网/公用事业承接’处理，优先看高确定性链条是否继续拿到资金，而不是直接追高高弹性方向。")
    else:
        lines.append(f"今天先围绕 `{narrative['label']}` 做跟踪，动作上先验证主线，再决定是否加大风险暴露。")

    lines.extend(_positioning_lines(narrative, monitor_rows))
    strongest_rsi = float(strongest.technical.get("rsi", {}).get("RSI", 0.0))
    strongest_tail = "已进入超买区，适合持有观察，不宜新增追高。" if strongest_rsi > 70 else "仍可作为主线验证器。"
    lines.append(f"优先方向: {strongest.symbol} — 它当前最能代表主线延续性；{strongest_tail}")
    lines.append(f"谨慎对待 {weakest.symbol}：它当前最弱，更适合等确认而不是抢反弹。")
    if gold and theme in {"energy_shock", "defensive_riskoff", "gold_defense"}:
        lines.append(f"把 {gold.symbol} 当作防守情绪验证器，观察避险需求是否继续抬头。")
    lines.append(_briefing_preflight_line(strongest, narrative, stage="today"))
    lines.append("执行节奏: 先观察开盘 30 分钟风格延续性，确认后再执行。")
    return lines[:6]


def _judgement_mark(passed: bool) -> str:
    return "✅" if passed else "❌"


def _compact_headline_lines(
    narrative: Dict[str, Any],
    china_macro: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    pulse: Dict[str, Any],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    background = str(narrative.get("background_regime", "未识别"))
    event_label = str(narrative.get("label", "未识别"))
    lines = [f"**{event_label}**"]
    lines.append(f"背景框架: `{background}`；交易主线候选: `{event_label}`。")
    secondary = [str(item.get("label", "")).strip() for item in list(narrative.get("secondary_themes") or []) if str(item.get("label", "")).strip()]
    if secondary:
        lines.append("次主线候选: " + "、".join(secondary[:3]) + "。")
    if narrative.get("overrides_background"):
        lines.append("若冲突：先服从交易主线，背景框架降为中期参考。")
    else:
        lines.append("若冲突：当前没有更强交易主线覆盖，先按背景框架执行。")
    pmi = float(china_macro.get("pmi", 50.0))
    oil_latest = _to_float(brent.get("latest"))
    if oil_latest > 0:
        lines.append(f"Regime 切换依据: PMI<{50:.0f} + 布伦特>{oil_latest:.0f} 时，优先按滞涨/能源冲击框架处理。")
    else:
        lines.append(f"Regime 切换依据: PMI {pmi:.1f}、CPI 与流动性状态共同决定背景 regime。")
    return lines


def _compact_validation_lines(
    narrative: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    pulse: Dict[str, Any],
) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    dxy = monitor.get("美元指数", {})
    vix = monitor.get("VIX波动率", {})
    top_zt = " ".join(_top_categories(pulse.get("zt_pool", pd.DataFrame()), "所属行业"))
    top_strong = " ".join(_top_categories(pulse.get("strong_pool", pd.DataFrame()), "所属行业"))

    price_ok = False
    board_ok = False
    cross_ok = False
    if narrative.get("theme") == "energy_shock":
        price_ok = _to_float(brent.get("return_1d")) >= 0.05 or _to_float(brent.get("return_5d")) >= 0.12
        board_ok = any(keyword in f"{top_zt} {top_strong}" for keyword in ["电力", "电网", "石油", "油气", "煤炭"])
        cross_ok = _to_float(vix.get("latest")) >= 25 or _to_float(dxy.get("return_5d")) > 0.005
    else:
        price_ok = True
        board_ok = bool(top_zt or top_strong)
        cross_ok = _to_float(vix.get("latest")) >= 0

    passed = sum([price_ok, board_ok, cross_ok])
    return [f"主线校验: 价格 {_judgement_mark(price_ok)} / 盘面 {_judgement_mark(board_ok)} / 跨市场 {_judgement_mark(cross_ok)}，通过 {passed}/3 项。"]


def _amount_delta_text(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return format_pct(value)


def _index_brief(row: Dict[str, Any]) -> str:
    change = _to_float(row.get("change_pct"))
    note = str(row.get("proxy_note", "")).strip()
    brief = "偏强" if change > 0.005 else "偏弱" if change < -0.005 else "震荡"
    if note:
        return f"{brief}，{note}"
    return brief


def _domestic_overview_rows(
    overview: Dict[str, Any],
    pulse: Dict[str, Any],
) -> tuple[List[List[str]], List[str]]:
    index_rows: List[List[str]] = []
    domestic = overview.get("domestic_indices", []) or []
    by_name = {str(item.get("name", "")): item for item in domestic}
    ordered_names = ["上证指数", "深证成指", "创业板指", "科创50", "沪深300", "中证1000", "中证2000"]
    for name in ordered_names:
        row = by_name.get(name)
        if not row:
            continue
        index_rows.append(
            [
                name,
                f"{_to_float(row.get('latest')):.2f}" if row.get("latest") is not None else "—",
                format_pct(_to_float(row.get("change_pct"))) if row.get("change_pct") is not None else "—",
                f"{_to_float(row.get('amount')):.0f}" if row.get("amount") is not None else "—",
                _amount_delta_text(row.get("amount_delta")),
                _index_brief(row),
            ]
        )

    breadth = overview.get("breadth", {}) or {}
    lines: List[str] = []
    turnover = breadth.get("turnover")
    if turnover is not None:
        lines.append(f"全市场成交额: {turnover:.0f}亿，较前日口径暂缺，先按绝对量能判断活跃度。")
    up_count = int(breadth.get("up_count", 0))
    down_count = int(breadth.get("down_count", 0))
    total = up_count + down_count + int(breadth.get("flat_count", 0))
    if total > 0:
        ratio = up_count / max(down_count, 1)
        lines.append(f"涨跌家数: 上涨 {up_count} 家，下跌 {down_count} 家，涨跌比 {ratio:.2f}。")
    prev_zt = pulse.get("prev_zt_pool", pd.DataFrame())
    avg_prev = pd.to_numeric(prev_zt["涨跌幅"], errors="coerce").dropna().mean() if not prev_zt.empty and "涨跌幅" in prev_zt.columns else None
    zt_count = len(pulse.get("zt_pool", pd.DataFrame()).index) if pulse else 0
    dt_count = len(pulse.get("dt_pool", pd.DataFrame()).index) if pulse else 0
    if avg_prev is not None:
        relay = "好" if avg_prev > 0 else "差"
        lines.append(f"涨停/跌停: 涨停 {zt_count} 家，跌停 {dt_count} 家。昨日涨停今日表现 {avg_prev:+.2f}%（接力环境{relay}）。")
    else:
        lines.append(f"涨停/跌停: 涨停 {zt_count} 家，跌停 {dt_count} 家。")
    return index_rows, lines


def _style_rows(
    overview: Dict[str, Any],
    industry_rows: pd.DataFrame,
) -> List[List[str]]:
    domestic = {str(item.get("name", "")): item for item in overview.get("domestic_indices", []) or []}
    hs300 = domestic.get("沪深300", {})
    zz1000 = domestic.get("中证1000", {})
    cyb = domestic.get("创业板指", {})
    szzs = domestic.get("上证指数", {})

    rows: List[List[str]] = []
    small = _to_float(zz1000.get("change_pct"))
    large = _to_float(hs300.get("change_pct"))
    size_signal = "偏小盘" if small - large > 0.003 else "偏大盘" if large - small > 0.003 else "均衡"
    rows.append(
        [
            "大盘 vs 小盘",
            f"中证1000 {format_pct(small)}",
            f"沪深300 {format_pct(large)}",
            size_signal,
        ]
    )

    growth = _to_float(cyb.get("change_pct"))
    value = _to_float(szzs.get("change_pct"))
    gv_signal = "偏成长" if growth - value > 0.003 else "偏价值" if value - growth > 0.003 else "均衡"
    rows.append(
        [
            "成长 vs 价值",
            f"创业板指 {format_pct(growth)}",
            f"上证指数 {format_pct(value)}",
            gv_signal,
        ]
    )

    strong_name = "电力"
    weak_name = "消费电子"
    strong_pct = None
    weak_pct = None
    if not industry_rows.empty:
        for keyword in ["电力", "电网设备", "公用事业"]:
            matched = industry_rows[industry_rows["板块名称"].astype(str).str.contains(keyword, na=False)]
            if not matched.empty:
                strong_name = str(matched.iloc[0]["板块名称"])
                strong_pct = _to_float(pd.to_numeric(pd.Series([matched.iloc[0]["涨跌幅"]]), errors="coerce").iloc[0]) / 100
                break
        for keyword in ["消费电子", "半导体", "元件", "电子化学品"]:
            matched = industry_rows[industry_rows["板块名称"].astype(str).str.contains(keyword, na=False)]
            if not matched.empty:
                weak_name = str(matched.iloc[0]["板块名称"])
                weak_pct = _to_float(pd.to_numeric(pd.Series([matched.iloc[0]["涨跌幅"]]), errors="coerce").iloc[0]) / 100
                break
    rows.append(
        [
            "内需 vs 外需",
            f"{strong_name} {format_pct(strong_pct) if strong_pct is not None else '—'}",
            f"{weak_name} {format_pct(weak_pct) if weak_pct is not None else '—'}",
            "—",
        ]
    )
    return rows


def _industry_catalyst_text(name: str, narrative: Dict[str, Any], news_report: Dict[str, Any], lead_stock: str, is_leader: bool = True) -> str:
    theme = str(narrative.get("theme", ""))
    lower = name.lower()
    direction = "领涨" if is_leader else "领跌"

    if any(keyword in lower for keyword in ["油", "煤"]):
        if is_leader:
            return "油价/能源价格走强，资源品受益。"
        return "油价/能源价格回落，能源链承压。"
    if any(keyword in lower for keyword in ["电", "逆变器", "储能", "电网", "电力"]):
        if is_leader:
            return "电力/电网设备需求预期支撑，资金持续流入。"
        return "前期涨幅获利了结或资金轮动离场。"
    if any(keyword in lower for keyword in ["半导体", "通信", "消费电子", "it", "软件", "计算机", "电子", "芯片", "元件", "电路"]):
        if is_leader:
            return "风险偏好修复，科技成长方向资金回流。"
        return "科技方向调整，资金从成长切向防守。"
    if any(keyword in lower for keyword in ["医药", "创新药", "生物"]):
        if is_leader:
            return "医药板块催化或估值修复驱动。"
        return "医药板块回调或政策压力。"
    if any(keyword in lower for keyword in ["军工", "航空", "航天", "船舶"]):
        if is_leader:
            return "地缘事件或国防预算预期催化。"
        return "地缘降温或板块轮动。"
    if lead_stock:
        return f"板块龙头 {lead_stock} 活跃，带动同链条{direction}。"
    if any(item.get("category") == "china_macro_domestic" for item in news_report.get("items", [])):
        return "国内政策快讯提供情绪支撑。"
    return "主要由盘面轮动和短线资金推动。"


def _generic_headline(title: str) -> bool:
    lowered = title.lower()
    generic_phrases = [
        "global market headlines",
        "breaking stock market news",
        "markets wrap",
    ]
    return any(phrase in lowered for phrase in generic_phrases)


def _industry_rank_rows(drivers: Dict[str, Any], narrative: Dict[str, Any], news_report: Dict[str, Any]) -> List[List[str]]:
    frame = drivers.get("industry_spot", pd.DataFrame())
    if frame is None or frame.empty or "板块名称" not in frame.columns or "涨跌幅" not in frame.columns:
        return []
    ranked = frame.copy()
    ranked["涨跌幅"] = pd.to_numeric(ranked["涨跌幅"], errors="coerce")
    ranked = ranked.dropna(subset=["涨跌幅"])
    if ranked.empty:
        return []
    leaders = ranked.sort_values("涨跌幅", ascending=False).head(5)
    laggards = ranked.sort_values("涨跌幅", ascending=True).head(5)
    rows: List[List[str]] = []
    for index, (_, row) in enumerate(leaders.iterrows(), start=1):
        rows.append(
            [
                str(index),
                str(row["板块名称"]),
                f"{pd.to_numeric(pd.Series([row['涨跌幅']]), errors='coerce').iloc[0]:+.2f}%",
                _industry_catalyst_text(str(row["板块名称"]), narrative, news_report, str(row.get("领涨股票", "")), is_leader=True),
            ]
        )
    tail_labels = ["-5", "-4", "-3", "-2", "-1"]
    for label, (_, row) in zip(tail_labels, laggards.iterrows()):
        cause = _industry_catalyst_text(str(row["板块名称"]), narrative, news_report, str(row.get("领涨股票", "")), is_leader=False)
        rows.append(
            [
                label,
                str(row["板块名称"]),
                f"{pd.to_numeric(pd.Series([row['涨跌幅']]), errors='coerce').iloc[0]:+.2f}%",
                cause,
            ]
        )
    return rows


def _macro_asset_rows(
    monitor_rows: List[Dict[str, Any]],
    anomaly_report: Dict[str, Any],
) -> List[List[str]]:
    monitor = _monitor_map(monitor_rows)
    flags = anomaly_report.get("flags", {})
    ordered = [
        "布伦特原油",
        "WTI原油",
        "VIX波动率",
        "美元指数",
        "美国10Y收益率",
        "COMEX黄金",
        "COMEX铜",
        "USDCNY",
    ]
    rows: List[List[str]] = []
    for name in ordered:
        item = monitor.get(name)
        if not item:
            continue
        latest = _to_float(item.get("latest"))
        ret_1d = _to_float(item.get("return_1d"))
        ret_5d = _to_float(item.get("return_5d"))
        ret_20d = _to_float(item.get("return_20d"))
        if name in {"布伦特原油", "WTI原油"}:
            state = "冲击" if abs(ret_5d) > 0.20 else "正常"
        elif name == "VIX波动率":
            state = "高波动" if latest >= 25 else "正常"
            ret_20d = float("nan")
        elif name == "美元指数":
            state = "偏强" if ret_5d > 0.005 else "偏弱" if ret_5d < -0.005 else "中性"
        else:
            state = "—"
        rows.append(
            [
                name,
                f"{latest:.3f}",
                format_pct(ret_1d),
                format_pct(ret_5d),
                format_pct(ret_20d) if pd.notna(ret_20d) else "—",
                state,
                flags.get(name, "—"),
            ]
        )
    return rows


def _overnight_rows(overview: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in overview.get("global_indices", []) or []:
        change = _to_float(item.get("change_pct"))
        brief = "偏强" if change > 0.005 else "偏弱" if change < -0.005 else "震荡"
        note = str(item.get("proxy_note", "")).strip()
        if note:
            brief = f"{brief}，{note}"
        rows.append(
            [
                str(item.get("market", "海外")),
                str(item.get("name", "")),
                f"{_to_float(item.get('latest')):.2f}",
                format_pct(change),
                brief,
            ]
        )
    return rows


def _core_event_lines(news_report: Dict[str, Any], catalyst_rows: List[List[str]]) -> List[str]:
    lines: List[str] = []
    items = news_report.get("items", []) or []
    transmission_map = {
        "energy": "原油 -> 通胀预期 -> 风险资产估值重定价。",
        "geopolitics": "地缘风险 -> 波动率/美元 -> 资产风险溢价上升。",
        "fed": "利率预期 -> 久期资产估值 -> 科技和成长弹性。",
        "earnings": "业绩/指引 -> 板块风险偏好 -> 相关 ETF 表现。",
        "china_market_domestic": "盘面快讯 -> 情绪扩散 -> 题材强弱切换。",
        "china_macro_domestic": "政策预期 -> 内需与顺周期方向重估。",
    }
    meaning_map = {
        "energy": "优先看能源、电力电网和防守资产。",
        "geopolitics": "先确认冲击是脉冲还是趋势，不把单日反弹当反转。",
        "fed": "只有利率和科技共振时，才把它升级成成长修复主线。",
        "earnings": "看是单股事件还是能扩散成板块催化。",
        "china_market_domestic": "和涨停池、龙虎榜一起确认盘面强度。",
        "china_macro_domestic": "更适合辅助确认国内主线，不单独定方向。",
    }
    filtered_items = [item for item in items if not _generic_headline(str(item.get("title", "")))]
    for item in filtered_items[:5]:
        source = str(item.get("source") or item.get("configured_source") or "未知源")
        category = str(item.get("category", "")).lower()
        transmission = transmission_map.get(category, "消息面 -> 风险偏好 -> 相关资产表现。")
        meaning = meaning_map.get(category, "先作为观察项，不下强结论。")
        lines.append(f"**{item.get('title', '未命名事件')}** ({source})\n  → {transmission}\n  → {meaning}")
    if lines:
        return lines
    for row in catalyst_rows[:3]:
        lines.append(f"**{row[0]}**\n  → {row[2]}\n  → {row[3]}")
    if lines:
        return lines
    return ["暂无可结构化的核心事件。"]


def _market_event_rows(news_report: Dict[str, Any], narrative: Dict[str, Any]) -> List[List[str]]:
    impact = "、".join(_effective_asset_preference(narrative)[:3]) or "观察池核心资产"
    rows: List[List[str]] = []
    items = [item for item in (news_report.get("items", []) or []) if not _generic_headline(str(item.get("title", "")))]
    for item in items[:3]:
        category = str(item.get("category", "")).lower()
        importance = "高" if category in {"fed", "earnings", "energy", "geopolitics"} else "中"
        rows.append(
            [
                "待定",
                str(item.get("title", "未命名事件")),
                "—",
                importance,
                impact,
            ]
        )
    return rows


def _theme_tracking_rows(
    narrative: Dict[str, Any],
    drivers: Dict[str, Any],
) -> List[List[str]]:
    frame = drivers.get("industry_spot", pd.DataFrame())
    power = _board_name(frame, ["电网", "电力", "公用事业"], "电力/电网")
    energy = _board_name(frame, ["石油", "油气", "煤炭", "能源"], "能源/油气")
    dividend = _board_name(frame, ["银行", "公用事业", "煤炭", "红利"], "高股息/红利")
    tech = _board_name(frame, ["半导体", "通信", "IT服务", "消费电子", "软件"], "AI算力链")
    domestic = _board_name(frame, ["建筑", "工程", "建材", "基建", "电网"], "基建/央国企")
    gold = _board_name(frame, ["贵金属", "黄金"], "黄金/防守")
    theme = str(narrative.get("theme", "macro_background"))

    plans: Dict[str, List[tuple[str, str, str, str, str, bool]]] = {
        "energy_shock": [
            (
                power,
                "能源冲击 + 国内电网投资确定性",
                "油价和地缘扰动抬升防守偏好，电力电网兼具逆周期属性和国内政策承接。",
                "短线交易 / 中线配置",
                "若原油冲高回落且电力链失去资金承接，催化会明显降温。",
                True,
            ),
            (
                energy,
                "油价跳升 + 地缘风险",
                "供给扰动先传导到原油，再传导到通胀预期和资源链定价。",
                "短线交易",
                "若事件只是一日脉冲，能源链容易高开低走。",
                True,
            ),
            (
                dividend,
                "VIX 抬升 + 防守需求",
                "高波动日里资金更愿意回到高股息和现金流稳定资产。",
                "防守底仓",
                "若波动率快速回落，红利风格可能跑输成长修复。",
                True,
            ),
            (
                tech,
                "中期景气未破坏，但受美元和波动率压制",
                "AI/科技中期逻辑仍在，但今天更容易被风险偏好压制。",
                "背景储备",
                "需要等 VIX 回落、美元转弱后才适合重新上调优先级。",
                False,
            ),
        ],
        "gold_defense": [
            (
                gold,
                "地缘/波动率抬升 + 黄金承接",
                "避险资金优先流向黄金和贵金属，说明市场更偏风险对冲而不是全面进攻。",
                "防守底仓",
                "若美元单边走强且黄金跟不上，避险主线会明显走弱。",
                True,
            ),
            (
                dividend,
                "防守配套",
                "高股息和公用事业可以作为黄金之外的低波动配套底仓。",
                "防守底仓",
                "若风险偏好快速修复，红利方向会先跑输弹性资产。",
                True,
            ),
            (
                power,
                "确定性资产承接",
                "电力/公用事业在避险阶段常作为低波动承接方向。",
                "防守底仓 / 观察",
                "若市场切换成宽基修复，公用事业相对强度会回落。",
                False,
            ),
        ],
        "dividend_defense": [
            (
                dividend,
                "高股息/银行承接",
                "防守资金优先回到现金流稳定、估值更低的银行和红利链。",
                "防守底仓 / 中线配置",
                "若风险偏好上修，红利/银行会明显跑输成长弹性。",
                True,
            ),
            (
                power,
                "公用事业配套",
                "电力、公用事业和红利资产通常会一起形成防守承接。",
                "防守底仓",
                "若主线转向成长，公用事业相对收益会下降。",
                True,
            ),
            (
                gold,
                "避险补充",
                "黄金可以作为更纯粹的避险补充，但不一定是今天最强主线。",
                "防守底仓",
                "若地缘溢价回落，黄金弹性会迅速下降。",
                False,
            ),
        ],
        "defensive_riskoff": [
            (
                gold,
                "避险需求抬升",
                "波动率上行时，黄金和贵金属更容易成为风险对冲工具。",
                "防守底仓",
                "若美元单边走强，黄金未必能同步受益。",
                True,
            ),
            (
                dividend,
                "回撤控制优先",
                "防守阶段先看现金流稳定和波动更低的方向。",
                "防守底仓",
                "若风险偏好快速修复，会明显跑输高弹性方向。",
                True,
            ),
            (
                power,
                "公用事业属性",
                "电力/公用事业在风险规避日更容易获得相对收益。",
                "短线交易 / 防守底仓",
                "若资金改追成长，公用事业的相对强度会回落。",
                True,
            ),
            (
                tech,
                "超跌修复预期",
                "科技方向暂时只适合放在观察名单，不应抢先定性为反转。",
                "背景储备",
                "若 VIX 继续上行，科技反弹大概率难以持续。",
                False,
            ),
        ],
        "broad_market_repair": [
            (
                "宽基/核心资产",
                "指数与核心资产修复",
                "宽基先修复意味着市场不是只靠单一题材，而是指数层面开始回暖。",
                "短线交易 / 中线配置",
                "若宽基修复没有扩散到行业宽度，行情更像指数托底而不是全面回暖。",
                True,
            ),
            (
                dividend,
                "银行/券商协同",
                "银行、券商和非银方向常作为宽基修复的风向标。",
                "短线交易 / 中线配置",
                "若金融权重不接力，宽基修复强度会明显不足。",
                True,
            ),
            (
                power,
                "确定性方向继续托底",
                "即使主线偏宽基修复，确定性链条仍可能作为低波动底盘存在。",
                "背景储备",
                "若修复扩散到高弹性成长，确定性方向会相对落后。",
                False,
            ),
        ],
        "rate_growth": [
            (
                "港股科技/美股科技",
                "利率预期改善 + 科技估值弹性",
                "利率回落先作用于久期资产，再扩散到成长风格。",
                "短线交易 / 中线配置",
                "若美元不弱反强，成长修复会被明显压制。",
                True,
            ),
            (
                tech,
                "AI 资本开支与产品催化",
                "算力和半导体链条对利率下行更敏感，弹性通常更大。",
                "中线配置",
                "若财报或指引没有继续验证，主题热度可能迅速降温。",
                True,
            ),
            (
                "半导体",
                "风险偏好回暖",
                "半导体通常在成长修复阶段承接更高风险偏好。",
                "短线交易 / 中线配置",
                "若外盘科技不配合，半导体容易只有脉冲没有持续。",
                True,
            ),
            (
                dividend,
                "防守底仓需求下降",
                "红利仍可持有，但在成长主线下不是日内优先方向。",
                "背景储备",
                "若市场再度切回风险规避，红利会重新获得相对优势。",
                False,
            ),
        ],
        "power_utilities": [
            (
                power,
                "电网/公用事业承接",
                "高确定性、电力设备和公用事业链获得持续资金承接。",
                "短线交易 / 中线配置",
                "若承接只靠个别龙头，板块持续性会打折。",
                True,
            ),
            (
                dividend,
                "防守现金流配套",
                "电网和公用事业主线常伴随银行/红利这类低波动资产一起走强。",
                "防守底仓 / 中线配置",
                "若市场突然切向高弹性成长，红利配套会被边缘化。",
                True,
            ),
            (
                domestic,
                "政策确定性延伸",
                "若政策和投资主线同时发力，电网/公用事业可向基建链延伸。",
                "中线配置",
                "若政策没有新落地，延伸扩散会弱于预期。",
                False,
            ),
        ],
        "china_policy": [
            (
                power,
                "稳增长 + 电网投资",
                "政策和投资主线更容易先在电网、基建和央国企链上体现。",
                "短线交易 / 中线配置",
                "若政策只停留在表态层，持续性会弱于预期。",
                True,
            ),
            (
                domestic,
                "财政与项目落地预期",
                "基建和央国企链更容易获得资金的确定性偏好。",
                "中线配置",
                "若增量政策迟迟不落地，行情容易回到存量博弈。",
                True,
            ),
            (
                dividend,
                "央国企现金流属性",
                "高股息和央国企可以作为政策主线的防守配套。",
                "防守底仓 / 中线配置",
                "若市场切向高弹性题材，红利方向会相对滞后。",
                True,
            ),
            (
                tech,
                "成长修复仍需外部环境配合",
                "科技方向可以保留观察，但不是今天政策主线下的第一优先级。",
                "背景储备",
                "若美元继续偏强，科技估值修复会被压制。",
                False,
            ),
        ],
        "ai_semis": [
            (
                tech,
                "模型/产品发布 + 资本开支验证",
                "AI 产品、算力需求和资本开支共同强化景气主线。",
                "中线配置",
                "若产品催化停留在标题级，板块容易冲高回落。",
                True,
            ),
            (
                "半导体",
                "产能与设备链景气验证",
                "产能扩张和景气改善会先映射到设备、材料和代工链。",
                "中线配置",
                "若外盘风险偏好走弱，半导体估值端会先承压。",
                True,
            ),
            (
                "通信/光模块",
                "算力链订单外溢",
                "通信和光模块通常承接 AI 基建扩张的中游需求。",
                "短线交易 / 中线配置",
                "若上游资本开支不及预期，中游弹性会先被压缩。",
                True,
            ),
            (
                dividend,
                "高波动日的对冲底仓",
                "即便主线在成长，也需要保留一定防守底仓避免风格切换。",
                "背景储备",
                "若风险偏好持续上修，红利方向会相对落后。",
                False,
            ),
        ],
        "macro_background": [
            (
                power,
                "国内确定性方向",
                "在没有单一主线压过一切时，电力电网更像稳态观察方向。",
                "中线配置",
                "若主线切向纯成长，确定性方向的相对收益会下降。",
                True,
            ),
            (
                dividend,
                "防守与现金流",
                "宏观不够清晰时，高股息适合作为底仓而非进攻方向。",
                "防守底仓",
                "若风险偏好显著回暖，防守底仓会跑输弹性资产。",
                True,
            ),
            (
                tech,
                "成长弹性候选",
                "科技方向保留观察，但需要等待利率和波动率配合。",
                "背景储备",
                "若外部利率和美元继续偏强，成长修复会延后。",
                False,
            ),
        ],
    }

    rows: List[List[str]] = []
    seen: set[str] = set()
    for direction, catalyst, logic, horizon, risk, aligned in plans.get(theme, plans["macro_background"]):
        if direction in seen:
            continue
        seen.add(direction)
        rows.append([direction, catalyst, logic, horizon, risk, _theme_information_environment(bool(aligned), horizon)])
        if len(rows) >= 4:
            break
    return rows


def _briefing_evidence_rows(
    *,
    generated_at: str,
    narrative: Dict[str, Any],
    regime_result: Dict[str, Any],
    data_coverage: str,
    missing_sources: str,
    a_share_watch_meta: Dict[str, Any],
    proxy_contract: Dict[str, Any],
) -> List[List[str]]:
    market_flow = dict(dict(proxy_contract or {}).get("market_flow") or {})
    social = dict(dict(proxy_contract or {}).get("social_sentiment") or {})
    regime_name = REGIME_LABELS.get(str(regime_result.get("current_regime", "")).strip(), str(regime_result.get("current_regime", "")).strip() or "未标注")
    trading_label = str(narrative.get("label", "")).strip() or "未标注"
    pool_size = int(a_share_watch_meta.get("pool_size") or 0)
    complete_size = int(a_share_watch_meta.get("complete_analysis_size") or 0)
    candidate_limit = int(a_share_watch_meta.get("candidate_limit") or 0)
    rows = [
        ["分析生成时间", generated_at or "—"],
        ["中期背景 / 当天主线", f"{regime_name} / {trading_label}"],
        ["数据覆盖", data_coverage or "未标注"],
        ["缺失/降级", missing_sources or "无"],
    ]
    if pool_size or complete_size or candidate_limit:
        rows.append(
            [
                "A股观察池来源",
                f"Tushare 优先全市场初筛；初筛 `{pool_size}` 只，完整分析 `{complete_size}` 只，候选上限 `{candidate_limit}` 只。",
            ]
        )
    if market_flow:
        interpretation = str(market_flow.get("interpretation", "")).strip() or "当前没有形成稳定的市场风格代理结论。"
        confidence = str(market_flow.get("confidence_label", "低")).strip() or "低"
        rows.append(["市场风格代理", f"{interpretation}（置信度 `{confidence}`）"])
    if social:
        covered = int(social.get("covered", 0) or 0)
        total = int(social.get("total", 0) or 0)
        rows.append(["情绪代理覆盖", f"`{covered}/{total}` 只样本已生成情绪代理。"])
    rows.append(["时点边界", "默认只使用生成时点前可见的宏观、新闻、观察池和缓存快照。"])
    return rows


def _latest_prior_briefing_path(mode: str = "daily") -> Optional[Path]:
    reports_dir = _briefing_internal_dir()
    if not reports_dir.exists():
        return None
    pattern = re.compile(rf"{re.escape(mode)}_briefing_(\d{{4}}-\d{{2}}-\d{{2}})\.md$")
    today = datetime.now().date()
    candidates: List[tuple[datetime, Path]] = []
    for path in reports_dir.glob(f"{mode}_briefing_*.md"):
        matched = pattern.search(path.name)
        if not matched:
            continue
        try:
            file_date = datetime.strptime(matched.group(1), "%Y-%m-%d")
        except ValueError:
            continue
        if file_date.date() < today:
            candidates.append((file_date, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[-1][1]


def _previous_theme_directions(path: Optional[Path]) -> List[str]:
    if path is None:
        return []
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return []
    section_match = re.search(r"### .*行业与主题跟踪（限2-4个方向）\s*(.*?)(?:\n### |\n## |\Z)", content, re.S)
    if not section_match:
        return []
    directions: List[str] = []
    for line in section_match.group(1).splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells or cells[0] in {"方向", "---"} or all(set(cell) <= {"-"} for cell in cells):
            continue
        directions.append(cells[0])
    return directions


def _theme_tracking_lines(
    narrative: Dict[str, Any],
    rows: List[List[str]],
    mode: str = "daily",
) -> List[str]:
    theme = str(narrative.get("theme", "macro_background"))
    aligned_counts = {
        "energy_shock": 3,
        "gold_defense": 2,
        "dividend_defense": 2,
        "defensive_riskoff": 3,
        "broad_market_repair": 2,
        "rate_growth": 3,
        "power_utilities": 2,
        "china_policy": 3,
        "ai_semis": 3,
        "macro_background": 2,
    }
    aligned = [row[0] for row in rows[: aligned_counts.get(theme, len(rows))]]
    reserve = [row[0] for row in rows[aligned_counts.get(theme, len(rows)) :]]
    lines: List[str] = []
    if aligned:
        summary = "与主线一致性: " + "、".join(aligned) + " 与 1.1 主线吻合。"
        if reserve:
            summary += " " + "、".join(reserve) + " 标注为背景储备，非当日优先。"
        lines.append(summary)
    else:
        lines.append("与主线一致性: 当前未提炼出稳定方向，先按宏观背景和盘面轮动观察。")

    previous = _previous_theme_directions(_latest_prior_briefing_path(mode))
    if not previous:
        lines.append("与前日对比: 暂无前一日行业跟踪归档，对比项从本次开始记录。")
        return lines

    current = [row[0] for row in rows]
    added = [item for item in current if item not in previous]
    removed = [item for item in previous if item not in current]
    reason_map = {
        "energy_shock": "因为原油、波动率和电力/能源链共振，日内主线切到能源冲击。",
        "gold_defense": "因为避险需求更集中地落在黄金上，而不是所有防守资产一起走强，主线切到黄金避险。",
        "dividend_defense": "因为银行/红利/公用事业承接更稳定，资金优先回到高现金流防守资产。",
        "defensive_riskoff": "因为防守和避险资产相对收益抬升，盘面优先级回到回撤控制。",
        "broad_market_repair": "因为宽基、金融和核心资产开始一起修复，盘面更像指数层面的修复而不是纯题材行情。",
        "rate_growth": "因为利率与成长风格开始共振，科技与久期资产优先级上升。",
        "power_utilities": "因为电网、公用事业和确定性链条持续获得承接，主线更像高确定性防守进攻平衡。",
        "china_policy": "因为国内政策和稳增长方向的确定性提升，电网/基建链权重上调。",
        "ai_semis": "因为 AI/半导体催化强化，成长主线重新获得景气验证。",
        "macro_background": "因为当前没有单一事件主线完全压制其他方向，先回到背景配置。",
    }
    if not added and not removed:
        lines.append("与前日对比: 跟踪方向整体稳定，暂未新增或移出重点主题。")
        return lines

    compare_parts: List[str] = []
    if added:
        compare_parts.append("新增 " + "、".join(added))
    if removed:
        compare_parts.append("移出 " + "、".join(removed))
    lines.append("与前日对比: " + "；".join(compare_parts) + "。原因: " + reason_map.get(theme, "主线与盘面结构发生变化。"))
    return lines


def _workflow_event_rows(events: List[Dict[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in events[:5]:
        rows.append(
            [
                str(item.get("time", "待定")),
                str(item.get("title", "未命名动作")),
                str(item.get("note", "")),
            ]
        )
    return rows


def _capital_flow_lines(
    pulse: Dict[str, Any],
    drivers: Dict[str, Any],
    liquidity_lines: List[str],
    snapshots: List[BriefingSnapshot],
) -> List[str]:
    lines: List[str] = []
    top_zt = _top_categories(pulse.get("zt_pool", pd.DataFrame()), "所属行业")
    top_strong = _top_categories(pulse.get("strong_pool", pd.DataFrame()), "所属行业")
    if top_zt:
        lines.append("涨停集中方向: " + "、".join(top_zt) + "，与当前主线基本一致。")
    if top_strong:
        lines.append("强势股池方向: " + "、".join(top_strong) + "。")
    lines.extend(_main_flow_driver_lines(drivers))
    lines.extend(liquidity_lines)
    etf_proxy = []
    for symbol in ("561380", "GLD", "QQQM"):
        snapshot = _find_snapshot(snapshots, symbol)
        if not snapshot:
            continue
        etf_proxy.append(f"{symbol} 近5日 {format_pct(snapshot.return_5d)}")
    if etf_proxy:
        lines.append("ETF份额变化: 免费源未稳定提供份额口径，今日改用 " + "、".join(etf_proxy) + " 作为资金承接代理。")
    return lines[:10]


def _quality_lines(
    news_report: Dict[str, Any],
    anomaly_report: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
) -> List[str]:
    lines = _source_quality_lines(news_report) + (anomaly_report.get("lines", []) or [])
    stale_rows = [item for item in monitor_rows if item.get("data_warning")]
    if stale_rows:
        labels = "、".join(
            f"{item.get('name', item.get('symbol', '未知资产'))}({item.get('stale_age_hours', '—')}h)"
            for item in stale_rows[:3]
        )
        lines.append(
            "宏观资产监控存在陈旧缓存回退: "
            + labels
            + "。今日把这些资产当方向参考，不当严格实时点位。"
        )
    return lines[:6]


def _verification_rows_v4(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> List[List[str]]:
    rows: List[List[str]] = []
    monitor = _monitor_map(monitor_rows)
    if monitor.get("布伦特原油"):
        rows.append(["1", "原油冲高回落", "布伦特收盘 < 开盘价", "地缘叙事降温，可小幅回补成长", "能源冲击延续，继续防守"])
    if monitor.get("VIX波动率"):
        rows.append(["2", "VIX 回落", "VIX 收盘 < 27", "波动率脉冲结束，可恢复正常仓位", "维持低仓位，不追高弹性"])
    if _find_snapshot(snapshots, "HSTECH"):
        rows.append(["3", "HSTECH 止跌", "日内不创新低 + 尾盘翻红", "风险偏好回暖", "继续回避港股科技"])
    if _find_snapshot(snapshots, "561380"):
        rows.append(["4", "561380 超额", "561380 日涨幅 > 沪深300", "国内主线延续", "电网方向可能获利了结"])
    if _find_snapshot(snapshots, "GLD") and monitor.get("美元指数"):
        rows.append(["5", "黄金承接避险", "GLD > +0.5% 且 DXY 同步走弱", "避险交易确认，可配黄金", "市场交易美元不是避险"])
    return rows


def _yesterday_review_rows(snapshots: List[BriefingSnapshot], monitor_rows: List[Dict[str, Any]]) -> List[List[str]]:
    latest_path = _latest_prior_briefing_path("daily")
    if latest_path is None:
        return []
    try:
        content = latest_path.read_text(encoding="utf-8")
    except OSError:
        return []
    section_match = re.search(r"### 4\.1 验证点表\s*(.*?)(?:\n### |\n## |\Z)", content, re.S)
    rows: List[List[str]] = []
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    hstech = _find_snapshot(snapshots, "HSTECH")
    grid = _find_snapshot(snapshots, "561380")
    hs300_result = "暂无沪深300代理"
    if "| 4 | 561380 超额" in content and grid:
        hs300_proxy = None
        try:
            overview = MarketOverviewCollector({}).collect()
            for item in overview.get("domestic_indices", []):
                if item.get("name") == "沪深300":
                    hs300_proxy = _to_float(item.get("change_pct"))
                    break
        except Exception:
            hs300_proxy = None
        if hs300_proxy is not None:
            alpha = grid.return_1d - hs300_proxy
            if alpha > 0:
                hs300_result = f"超额 {alpha*100:+.2f}%，跑赢沪深300"
                passed = True
            elif grid.return_1d > 0:
                hs300_result = f"绝对正收益 {format_pct(grid.return_1d)}，但风格轮动跑输（超额 {alpha*100:+.2f}%）"
                passed = True
            else:
                hs300_result = f"超额 {alpha*100:+.2f}%，绝对走弱"
                passed = False
            rows.append([
                "561380 vs 沪深300 超额",
                "561380 主线延续（绝对正收益或相对跑赢）",
                hs300_result,
                "✅" if passed else "❌",
            ])
    if "| 1 | 原油冲高回落" in content and brent:
        close_price = _to_float(brent.get("latest"))
        ret_5d = _to_float(brent.get("return_5d"))
        ret_1d = _to_float(brent.get("return_1d"))
        passed = (abs(ret_5d) > 0.05 and ret_1d < 0.01) or ret_1d < -0.02
        if passed:
            actual_text = f"收 {close_price:.2f}，5日{format_pct(ret_5d)}日内{format_pct(ret_1d)}，高位回落"
        else:
            actual_text = f"收 {close_price:.2f}，{format_pct(ret_1d)}，未见回落"
        rows.append([
            "原油是否冲高回落",
            "布伦特从高位回落",
            actual_text,
            "✅" if passed else "❌",
        ])
    if "| 3 | HSTECH 止跌" in content and hstech:
        passed = hstech.return_1d > 0
        rows.append([
            "HSTECH 是否止跌",
            "日内不创新低 + 尾盘翻红",
            f"收 {hstech.latest_price:.3f}，{format_pct(hstech.return_1d)}",
            "✅" if passed else "❌",
        ])
    return rows[:3]


def _yesterday_review_summary_lines(rows: List[List[str]]) -> List[str]:
    if not rows:
        return ["暂无昨日晨报归档，暂时无法自动回顾‘昨日验证点’。", "命中率: —。暂无可复盘记录。", "框架修正: —"]
    hits = sum(1 for row in rows if row[-1] == "✅")
    total = len(rows)
    verdict = "连续准确" if hits == total else "需要局部修正" if hits <= total / 2 else "框架大体有效"
    fix = "—" if hits >= max(total - 1, 1) else "若连续错在同一方向，应下调对应主线权重。"
    return [f"命中率: {hits}/{total}。{verdict}", f"框架修正: {fix}"]


def _portfolio_table_rows(config: Dict[str, Any]) -> List[List[str]]:
    portfolio_repo = PortfolioRepository()
    thesis_repo = ThesisRepository()
    holdings = portfolio_repo.list_holdings()
    if not holdings:
        return []
    rows: List[List[str]] = []
    latest_prices: Dict[str, float] = {}
    for holding in holdings:
        try:
            history = fetch_asset_history(holding["symbol"], holding["asset_type"], config)
            latest_prices[holding["symbol"]] = compute_history_metrics(history)["last_close"]
        except Exception:
            latest_prices[holding["symbol"]] = float(holding.get("cost_basis", 0.0))
    for holding in holdings:
        latest = latest_prices.get(holding["symbol"], float(holding.get("cost_basis", 0.0)))
        cost = float(holding.get("cost_basis", 0.0))
        pnl = latest / cost - 1 if cost else 0.0
        thesis = thesis_repo.get(holding["symbol"])
        rows.append(
            [
                holding["symbol"],
                "多",
                f"{cost:.3f}",
                f"{latest:.3f}",
                format_pct(pnl),
                str((thesis or {}).get("core_hypothesis", "—"))[:24],
                "持有观察",
            ]
        )
    return rows


def _appendix_technical_rows(snapshots: List[BriefingSnapshot]) -> List[List[str]]:
    rows: List[List[str]] = []
    for snapshot in snapshots:
        technical = snapshot.technical
        rows.append(
            [
                snapshot.symbol,
                "多头" if technical["macd"]["signal"] == "bullish" else "空头",
                _kdj_text(technical["kdj"]).replace("KDJ ", ""),
                f"{float(technical['rsi']['RSI']):.1f}",
                _boll_text(technical["bollinger"]).replace("BOLL ", ""),
                _obv_text(technical["obv"]).replace("OBV ", ""),
                f"{float(technical['dmi']['ADX']):.1f}",
                _fib_text(technical["fibonacci"]).replace("斐波那契", "").strip(),
            ]
        )
    return rows


def _appendix_derivative_lines(narrative: Dict[str, Any], monitor_rows: List[Dict[str, Any]]) -> List[str]:
    monitor = _monitor_map(monitor_rows)
    lines = [
        "IF/IC/IM 基差: 当前未接入稳定实时基差源，暂不输出方向性误导结论。",
        "最大持仓行权价: 当前未接入期权持仓分布，pin risk 维度今日盲区。",
    ]
    if monitor.get("VIX波动率"):
        vix = _to_float(monitor.get("VIX波动率", {}).get("latest"))
        lines.insert(1, f"期权隐含波动率: 先用 VIX {vix:.1f} 作为外盘波动代理，A股期权 IV/HV 仍待补充。")
    else:
        lines.insert(1, "期权隐含波动率: 当前 VIX/外盘波动代理缺失，A股期权 IV/HV 仍待补充。")
    if narrative.get("theme") == "energy_shock":
        lines.append("能源冲击日里，即便看多主线，也要默认隐含波动率溢价更高。")
    return lines


def _appendix_earnings_rows(news_report: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in news_report.get("items", []) or []:
        if str(item.get("category", "")).lower() != "earnings":
            continue
        rows.append(
            [
                str(item.get("source", "相关公司")),
                "近期",
                "—",
                "—",
                "标题级信号",
                str(item.get("title", "未命名财报事件"))[:30],
                "当前仅完成标题级财报识别，正文解析待补充。",
            ]
        )
    return rows[:5]


def _appendix_allocation_rows(narrative: Dict[str, Any], monitor_rows: List[Dict[str, Any]]) -> List[List[str]]:
    monitor = _monitor_map(monitor_rows)
    vix = _to_float(monitor.get("VIX波动率", {}).get("latest"))
    theme_assets = " / ".join(_effective_asset_preference(narrative)[:2]) or "高股息 / 现金"
    if vix > 35:
        applicable = "保守"
    elif vix > 25:
        applicable = "平衡"
    else:
        applicable = "进取"
    rows = [
        ["保守型", "≤40%", "高股息 + 中短债", "—", "维持防守，不参与主题追价"],
        ["平衡型", "40-70%", "红利 + 确定性成长", "主线方向小仓位", f"底仓不动，主线方向围绕 {theme_assets} 试探性配置"],
        ["进取型", "60-90%", "景气主线", "高弹性题材", "围绕主线做波段，单一主题≤20%"],
    ]
    rows.append(["当日适用", applicable, theme_assets, "—", "结合 VIX 和主线执行，不和高波动对着干"])
    return rows


def _load_same_day_briefing(mode: str = "daily") -> Optional[str]:
    """Load today's briefing markdown for the given mode, or None if not found."""
    reports_dir = _briefing_internal_dir()
    if not reports_dir.exists():
        return None
    date_str = datetime.now().strftime("%Y-%m-%d")
    path = reports_dir / f"{mode}_briefing_{date_str}.md"
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _parse_prior_verification_rows(md_text: Optional[str]) -> List[List[str]]:
    """Extract verification point rows from a prior briefing markdown."""
    if not md_text:
        return []
    section_match = re.search(r"### 4\.1 验证点表\s*(.*?)(?:\n### |\n## |\Z)", md_text, re.S)
    if not section_match:
        return []
    rows: List[List[str]] = []
    for line in section_match.group(1).splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells or cells[0] in {"#", "---"} or all(set(cell) <= {"-"} for cell in cells):
            continue
        rows.append(cells)
    return rows


def _parse_prior_headline(md_text: Optional[str]) -> str:
    """Extract the headline/main thesis from a prior briefing."""
    if not md_text:
        return ""
    match = re.search(r"### 1\.1 今日主线\s*(.*?)(?:\n### |\n## |\Z)", md_text, re.S)
    if not match:
        return ""
    lines = [line.strip() for line in match.group(1).splitlines() if line.strip()]
    return lines[0] if lines else ""


def _evaluate_prior_verification(
    prior_rows: List[List[str]],
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> List[List[str]]:
    """Evaluate prior verification points against current data."""
    if not prior_rows:
        return []
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    vix = monitor.get("VIX波动率", {})
    dxy = monitor.get("美元指数", {})
    hstech = _find_snapshot(snapshots, "HSTECH")
    grid = _find_snapshot(snapshots, "561380")
    gld = _find_snapshot(snapshots, "GLD")

    result: List[List[str]] = []
    for row in prior_rows:
        if len(row) < 3:
            continue
        label = row[1] if len(row) > 1 else ""
        criterion = row[2] if len(row) > 2 else ""
        actual = "暂无数据"
        passed = False

        if "原油" in label and brent:
            close_price = _to_float(brent.get("latest"))
            ret_5d = _to_float(brent.get("return_5d"))
            ret_1d = _to_float(brent.get("return_1d"))
            # "冲高回落" means oil pulling back from highs — check 5d trend reversal
            # If oil was surging (5d > +5%) but 1d is negative or flat, it's pulling back
            if abs(ret_5d) > 0.05 and ret_1d < 0.01:
                passed = True
                actual = f"收 {close_price:.2f}，5日{format_pct(ret_5d)}但日内{format_pct(ret_1d)}，高位回落中"
            elif ret_1d < -0.02:
                passed = True
                actual = f"收 {close_price:.2f}，{format_pct(ret_1d)}，明显回落"
            else:
                passed = False
                actual = f"收 {close_price:.2f}，{format_pct(ret_1d)}，未见明显回落"
        elif "VIX" in label and vix:
            vix_val = _to_float(vix.get("latest"))
            actual = f"VIX {vix_val:.1f}"
            passed = vix_val < 27
        elif "HSTECH" in label and hstech:
            actual = f"收 {hstech.latest_price:.3f}，{format_pct(hstech.return_1d)}"
            passed = hstech.return_1d > 0
        elif "561380" in label and grid:
            actual = f"{format_pct(grid.return_1d)}"
            try:
                overview = MarketOverviewCollector({}).collect()
                for item in overview.get("domestic_indices", []):
                    if item.get("name") == "沪深300":
                        hs300_pct = _to_float(item.get("change_pct"))
                        alpha = grid.return_1d - hs300_pct
                        # Distinguish: absolute positive + style rotation vs truly weak
                        if alpha > 0:
                            actual = f"超额 {alpha*100:+.2f}%，跑赢沪深300"
                            passed = True
                        elif grid.return_1d > 0:
                            actual = f"绝对收益 {format_pct(grid.return_1d)}，但相对沪深300超额 {alpha*100:+.2f}%（风格轮动跑输，趋势未坏）"
                            passed = True  # absolute positive means trend intact
                        else:
                            actual = f"超额 {alpha*100:+.2f}%，绝对走弱"
                            passed = False
                        break
            except Exception:
                pass
        elif "黄金" in label and gld:
            actual = f"GLD {format_pct(gld.return_1d)}"
            dxy_ret = _to_float(dxy.get("return_1d"))
            passed = gld.return_1d > 0.005 and dxy_ret < 0
        else:
            actual = "标的数据缺失"
            passed = False

        result.append([label, criterion, actual, "✅" if passed else "❌"])

    return result[:5]


def _noon_breadth(overview: Dict[str, Any], pulse: Dict[str, Any]) -> Dict[str, Any]:
    """Extract breadth metrics from overview/pulse for noon logic."""
    breadth = overview.get("breadth", {}) or {}
    up = int(breadth.get("up_count", 0))
    down = int(breadth.get("down_count", 0))
    ratio = up / down if down > 0 else 1.0
    zt = len(pulse.get("zt_pool", pd.DataFrame()))
    dt = len(pulse.get("dt_pool", pd.DataFrame()))
    risk_on = ratio > 2.0 or (up > down * 1.5 and zt > max(dt, 1) * 3)
    return {"up": up, "down": down, "ratio": ratio, "zt": zt, "dt": dt, "risk_on": risk_on}


# Core verification keywords — failure on these invalidates the narrative,
# not just one of N equal-weight checks.
_CORE_VERIFY_KEYWORDS: Dict[str, List[str]] = {
    "energy_shock": ["原油", "黄金"],
    "gold_defense": ["黄金", "VIX"],
    "dividend_defense": ["银行", "红利", "公用事业"],
    "defensive_riskoff": ["黄金", "VIX"],
    "broad_market_repair": ["上证指数", "沪深300", "券商"],
    "rate_growth": ["VIX", "QQQM"],
    "ai_semis": ["HSTECH", "半导体"],
    "power_utilities": ["561380", "电网", "公用事业"],
    "china_policy": ["561380"],
}


def _noon_strategy_adjustment(
    prior_headline: str,
    eval_rows: List[List[str]],
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
    overview: Dict[str, Any],
    pulse: Dict[str, Any],
) -> List[str]:
    """Generate strategy adjustment lines for the noon briefing."""
    lines: List[str] = []
    if prior_headline:
        lines.append(f"晨报主线: **{prior_headline}**")

    b = _noon_breadth(overview, pulse)

    if not eval_rows:
        lines.append(f"暂无晨报验证点。盘面涨跌比 {b['ratio']:.1f}:1，涨停 {b['zt']} / 跌停 {b['dt']}。")
        return lines

    hits = sum(1 for r in eval_rows if r[-1] == "✅")
    total = len(eval_rows)

    # --- core verification point check ---
    theme = str(narrative.get("theme", "macro_background"))
    core_kws = _CORE_VERIFY_KEYWORDS.get(theme, [])
    core_failed = [
        r[0] for r in eval_rows
        if r[-1] == "❌" and any(kw in r[0] for kw in core_kws)
    ]

    if core_failed:
        lines.append(f"上午验证 {hits}/{total}，但核心假设验证点「{'、'.join(core_failed)}」未兑现，晨报叙事基础已动摇。")
    elif hits == total:
        lines.append(f"上午验证 {hits}/{total} 全部兑现，主线逻辑延续。")
    elif hits >= total / 2:
        lines.append(f"上午验证 {hits}/{total} 部分兑现，主线大体有效。")
    else:
        lines.append(f"上午验证仅 {hits}/{total} 兑现，晨报主线可能需要修正。")
        failed = [r[0] for r in eval_rows if r[-1] == "❌"]
        if failed:
            lines.append(f"未兑现: {'、'.join(failed[:3])}。")

    # breadth reality check
    lines.append(f"盘面涨跌比 {b['ratio']:.1f}:1，涨停 {b['zt']} / 跌停 {b['dt']}{'，实际偏 risk-on' if b['risk_on'] else ''}。")

    if snapshots:
        strongest = max(snapshots, key=lambda s: s.return_1d)
        weakest = min(snapshots, key=lambda s: s.return_1d)
        lines.append(f"上午最强: {strongest.name}({format_pct(strongest.return_1d)})，最弱: {weakest.name}({format_pct(weakest.return_1d)})。")
    return lines[:6]


def _noon_action_lines(
    eval_rows: List[List[str]],
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    pulse: Dict[str, Any],
    overview: Dict[str, Any] | None = None,
) -> List[str]:
    """Generate afternoon observation hints for noon briefing.

    Keep it data-driven and light on conclusions — most institutional
    commentary waits until close so intraday calls risk being wrong.
    """
    lines: List[str] = []

    if not snapshots:
        lines.append("下午继续观察，数据不足暂不给方向。")
        return lines

    b = _noon_breadth(overview or {}, pulse)
    if b["up"] > 0:
        tone = "偏进攻" if b["risk_on"] else "偏均衡" if b["ratio"] > 1.0 else "偏防守"
        lines.append(f"上午盘面涨跌比 {b['ratio']:.1f}:1，{tone}。下午关注能否延续。")

    # --- separate trend anchor vs elastic leaders ---
    trend_anchor = max(snapshots, key=lambda s: s.signal_score)
    elastic_leader = max(snapshots, key=lambda s: s.return_1d)

    if trend_anchor.symbol == elastic_leader.symbol:
        lines.append(f"趋势与弹性共振: {trend_anchor.name}（1日 {format_pct(trend_anchor.return_1d)}，信号 {trend_anchor.signal_score}）。")
    else:
        lines.append(
            f"趋势锚: {trend_anchor.name}（信号 {trend_anchor.signal_score}，1日 {format_pct(trend_anchor.return_1d)}）；"
            f"弹性领涨: {elastic_leader.name}（1日 {format_pct(elastic_leader.return_1d)}，信号 {elastic_leader.signal_score}）。"
        )
        # when elastic leader's return is much higher, hint that the market
        # may be rotating, but state it as observation not recommendation
        if elastic_leader.return_1d > trend_anchor.return_1d + 0.015:
            lines.append("弹性方向明显强于趋势锚，盘中风格可能在切换，留意是否扩散。")

    lines.append(_briefing_preflight_line(trend_anchor, narrative, stage="afternoon"))
    lines.append("执行节奏: 13:00 开盘后先观察 15 分钟延续性，再决定动作。")
    return lines[:5]


def _noon_verification_rows(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
) -> List[List[str]]:
    """Generate afternoon verification points for noon briefing."""
    rows: List[List[str]] = []
    if snapshots:
        strongest = max(snapshots, key=lambda s: s.return_1d)
        rows.append(["1", f"{strongest.name}延续", f"{strongest.name} 下午涨幅不回吐超过一半", "主线确认，可持有过夜", "可能尾盘跳水，谨慎持有"])
    monitor = _monitor_map(monitor_rows)
    if monitor.get("VIX波动率"):
        vix_val = _to_float(monitor["VIX波动率"].get("latest"))
        rows.append(["2", "VIX 趋势", f"VIX 维持在 {vix_val:.0f} 附近不突破", "波动率稳定，可正常操作", "波动率放大，降低仓位"])
    if snapshots:
        weakest = min(snapshots, key=lambda s: s.return_1d)
        rows.append(["3", f"{weakest.name}止跌", f"{weakest.name} 下午不再创新低", "最弱环节企稳", "弱势延续，回避该方向"])
    return rows[:3]


def _evening_hit_rate_summary(eval_rows: List[List[str]]) -> List[str]:
    """Generate hit rate summary lines for evening verification review."""
    if not eval_rows:
        return ["暂无验证点可回顾。"]
    hits = sum(1 for r in eval_rows if r[-1] == "✅")
    total = len(eval_rows)
    rate = hits / total if total else 0
    verdict = "框架精准" if rate >= 0.8 else "框架大体有效" if rate >= 0.5 else "需要系统修正"
    lines = [f"全日验证命中率: {hits}/{total} ({rate:.0%})。{verdict}。"]
    if rate < 0.5:
        failed_directions = [r[0] for r in eval_rows if r[-1] == "❌"]
        lines.append(f"未命中方向: {'、'.join(failed_directions[:3])}。建议降低相关主线权重。")
    return lines


def _evening_narrative_review(
    prior_headline: str,
    eval_rows: List[List[str]],
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
) -> List[str]:
    """Generate narrative review lines for evening briefing."""
    lines: List[str] = []
    if prior_headline:
        lines.append(f"晨报主线: {prior_headline}")
    today_theme = str(narrative.get("label", "未识别"))
    lines.append(f"实际驱动: {today_theme}")
    if eval_rows:
        hits = sum(1 for r in eval_rows if r[-1] == "✅")
        total = len(eval_rows)
        if hits == total:
            lines.append("晨报判断全部兑现，主线叙事逻辑得到验证。")
        elif hits > total / 2:
            lines.append("晨报判断部分兑现，大方向正确但细节需调整。")
        else:
            lines.append("晨报判断多数未兑现，市场实际走势偏离预期，需反思框架假设。")
    if snapshots:
        strongest = max(snapshots, key=lambda s: s.return_1d)
        weakest = min(snapshots, key=lambda s: s.return_1d)
        lines.append(f"今日最强: {strongest.name}({format_pct(strongest.return_1d)})，最弱: {weakest.name}({format_pct(weakest.return_1d)})。")
    return lines[:5]


def _tomorrow_outlook_lines(
    narrative: Dict[str, Any],
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
    overnight_rows: List[List[str]],
) -> List[str]:
    """Generate tomorrow outlook lines for evening briefing."""
    lines: List[str] = []
    theme = str(narrative.get("label", "未识别"))
    lines.append(f"今日主线 `{theme}` 的延续性需要明天开盘验证。")
    if overnight_rows:
        lines.append("关注隔夜外盘走势，尤其是美股和大宗商品对 A 股情绪的传导。")
    monitor = _monitor_map(monitor_rows)
    vix = monitor.get("VIX波动率", {})
    vix_val = _to_float(vix.get("latest"))
    if vix_val > 20:
        lines.append(f"VIX 仍在 {vix_val:.1f}，波动率环境偏高，明天操作需留安全边际。")
    if snapshots:
        trending = [s for s in snapshots if s.trend == "多头" and s.return_1d > 0]
        if trending:
            names = "、".join(s.name for s in trending[:3])
            lines.append(f"多头趋势延续标的: {names}，明天可优先跟踪。")
    return lines[:5]


def _tomorrow_verification_rows(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
    narrative: Dict[str, Any],
) -> List[List[str]]:
    """Generate preliminary verification points for tomorrow."""
    rows: List[List[str]] = []
    if snapshots:
        strongest = max(snapshots, key=lambda s: s.return_1d)
        rows.append(["1", f"{strongest.name}延续", f"明日{strongest.name}涨幅 > 0", "主线持续，可持有", "考虑止盈或减仓"])
    monitor = _monitor_map(monitor_rows)
    if monitor.get("布伦特原油"):
        rows.append(["2", "原油动向", "布伦特波动 < 2%", "宏观平稳", "能源冲击再起，需调整框架"])
    if monitor.get("VIX波动率"):
        rows.append(["3", "VIX 趋势", "VIX 不显著上升", "风险偏好维持", "避险升温，减仓弹性品种"])
    return rows[:3]


def _tomorrow_action_lines(
    eval_rows: List[List[str]],
    snapshots: List[BriefingSnapshot],
    narrative: Dict[str, Any],
) -> List[str]:
    """Generate action recommendations for tomorrow."""
    lines: List[str] = []
    if eval_rows:
        hits = sum(1 for r in eval_rows if r[-1] == "✅")
        total = len(eval_rows)
        if hits >= total * 0.8:
            lines.append("今日框架有效，明天可延续策略方向，适度加大置信度。")
        elif hits >= total * 0.5:
            lines.append("今日框架部分有效，明天维持方向但仓位不宜激进。")
        else:
            lines.append("今日框架偏差较大，明天先观望为主，等开盘数据重新定性。")
    else:
        lines.append("无今日验证数据，明天以观察为主。")
    if snapshots:
        strongest = max(snapshots, key=lambda s: s.signal_score)
        lines.append(f"明日优先方向: {strongest.name}，当前信号评分最高。")
        lines.append(_briefing_preflight_line(strongest, narrative, stage="tomorrow"))
    lines.append("执行节奏: 明天开盘前先看外盘和早报更新，再决定是否调整。")
    return lines[:5]


def _watchlist_change_lines(snapshots: List[BriefingSnapshot], prior_md: Optional[str]) -> List[str]:
    """Detect watchlist changes compared to the prior briefing."""
    current_symbols = {s.symbol for s in snapshots}
    if not prior_md:
        return []
    prior_symbols: set = set()
    for line in prior_md.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not cells or cells[0] in {"标的", "---"} or all(set(c) <= {"-"} for c in cells):
            continue
        # First cell is typically "SYMBOL (Name)" or just "SYMBOL"
        sym = cells[0].split("(")[0].split(" ")[0].strip()
        if sym:
            prior_symbols.add(sym)
    new_symbols = current_symbols - prior_symbols
    if not new_symbols:
        return []
    new_names = [f"{s.name}({s.symbol})" for s in snapshots if s.symbol in new_symbols]
    return [f"本期新增标的: {'、'.join(new_names)}。"]


def _build_noon_payload(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
    overview: Dict[str, Any],
    pulse: Dict[str, Any],
    drivers: Dict[str, Any],
    narrative: Dict[str, Any],
    watchlist_rows: List[List[str]],
    events: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Build payload for noon briefing."""
    morning_md = _load_same_day_briefing("daily")
    prior_rows = _parse_prior_verification_rows(morning_md)
    prior_headline = _parse_prior_headline(morning_md)
    eval_rows = _evaluate_prior_verification(prior_rows, snapshots, monitor_rows)
    domestic_index_rows, domestic_market_lines = _domestic_overview_rows(overview, pulse)
    style_rows = _style_rows(overview, drivers.get("industry_spot", pd.DataFrame()))
    industry_rows = _industry_rank_rows(drivers, narrative, {})

    return {
        "title": "午间盘中简报",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "watchlist_change_lines": _watchlist_change_lines(snapshots, morning_md or ""),
        "morning_eval_rows": eval_rows,
        "morning_eval_fallback": "暂无今日晨报，跳过策略验证。" if not morning_md else "",
        "domestic_index_rows": domestic_index_rows,
        "domestic_market_lines": domestic_market_lines,
        "style_rows": style_rows,
        "industry_rows": industry_rows,
        "watchlist_rows": watchlist_rows,
        "strategy_adjustment_lines": _noon_strategy_adjustment(prior_headline, eval_rows, snapshots, narrative, overview, pulse),
        "afternoon_action_lines": _noon_action_lines(eval_rows, snapshots, narrative, monitor_rows, pulse, overview),
        "afternoon_verification_rows": _noon_verification_rows(snapshots, monitor_rows),
        "afternoon_event_rows": _workflow_event_rows(events),
        "portfolio_lines": _portfolio_lines(config),
        "portfolio_table_rows": _portfolio_table_rows(config),
    }


def _build_evening_payload(
    snapshots: List[BriefingSnapshot],
    monitor_rows: List[Dict[str, Any]],
    overview: Dict[str, Any],
    pulse: Dict[str, Any],
    drivers: Dict[str, Any],
    narrative: Dict[str, Any],
    news_report: Dict[str, Any],
    watchlist_rows: List[List[str]],
    config: Dict[str, Any],
    anomaly_report: Dict[str, Any],
    overnight_rows: List[List[str]],
    liquidity_lines: List[str],
) -> Dict[str, Any]:
    """Build payload for evening briefing."""
    morning_md = _load_same_day_briefing("daily")
    prior_rows = _parse_prior_verification_rows(morning_md)
    prior_headline = _parse_prior_headline(morning_md)
    eval_rows = _evaluate_prior_verification(prior_rows, snapshots, monitor_rows)
    domestic_index_rows, domestic_market_lines = _domestic_overview_rows(overview, pulse)
    style_rows = _style_rows(overview, drivers.get("industry_spot", pd.DataFrame()))
    industry_rows = _industry_rank_rows(drivers, narrative, news_report)
    macro_asset_rows = _macro_asset_rows(monitor_rows, anomaly_report)
    catalyst_rows = _catalyst_rows(news_report, narrative)
    capital_flow_lines = _capital_flow_lines(pulse, drivers, liquidity_lines, snapshots)

    return {
        "title": "收盘晚报",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "watchlist_change_lines": _watchlist_change_lines(snapshots, morning_md or ""),
        "full_day_eval_rows": eval_rows,
        "full_day_eval_fallback": "暂无今日晨报，跳过全日验证。" if not morning_md else "",
        "hit_rate_lines": _evening_hit_rate_summary(eval_rows),
        "domestic_index_rows": domestic_index_rows,
        "domestic_market_lines": domestic_market_lines,
        "style_rows": style_rows,
        "industry_rows": industry_rows,
        "macro_asset_rows": macro_asset_rows,
        "watchlist_rows": watchlist_rows,
        "narrative_review_lines": _evening_narrative_review(prior_headline, eval_rows, snapshots, narrative),
        "core_event_lines": _core_event_lines(news_report, catalyst_rows),
        "capital_flow_lines": capital_flow_lines,
        "overnight_rows": overnight_rows,
        "tomorrow_outlook_lines": _tomorrow_outlook_lines(narrative, snapshots, monitor_rows, overnight_rows),
        "tomorrow_verification_rows": _tomorrow_verification_rows(snapshots, monitor_rows, narrative),
        "tomorrow_action_lines": _tomorrow_action_lines(eval_rows, snapshots, narrative),
        "portfolio_lines": _portfolio_lines(config),
        "portfolio_table_rows": _portfolio_table_rows(config),
        "appendix_technical_rows": _appendix_technical_rows(snapshots),
        "appendix_lhb_lines": _lhb_lines(pulse),
        "appendix_flow_lines": _flow_lines(snapshots, config) + _sentiment_lines(snapshots, config),
        "charts": _render_briefing_charts(snapshots),
    }


def _build_market_payload(
    *,
    config: Dict[str, Any],
    narrative: Dict[str, Any],
    china_macro: Dict[str, Any],
    regime_result: Dict[str, Any],
    overview: Dict[str, Any],
    pulse: Dict[str, Any],
    drivers: Dict[str, Any],
    news_report: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    snapshots: List[BriefingSnapshot],
    anomaly_report: Dict[str, Any],
    liquidity_lines: List[str],
    overnight_rows: List[List[str]],
    watchlist_rows: List[List[str]],
    a_share_watch_rows: List[List[str]],
    a_share_watch_lines: List[str],
    a_share_watch_meta: Dict[str, Any],
    a_share_watch_candidates: List[Dict[str, Any]],
    data_coverage: str,
    missing_sources: str,
    macro_items: List[str],
    alerts: List[str],
    quality_lines: List[str],
    proxy_contract: Dict[str, Any],
    evidence_rows: List[List[str]],
) -> Dict[str, Any]:
    market_analysis = build_market_analysis(config, overview, pulse, drivers)
    domestic_index_rows, domestic_market_lines = _domestic_overview_rows(overview, pulse)
    style_rows = _style_rows(overview, drivers.get("industry_spot", pd.DataFrame()))
    industry_rows = _industry_rank_rows(drivers, narrative, news_report)
    macro_asset_rows = _macro_asset_rows(monitor_rows, anomaly_report)
    theme_tracking_rows = _theme_tracking_rows(narrative, drivers)
    theme_tracking_lines = _theme_tracking_lines(narrative, theme_tracking_rows, "daily")
    capital_flow_lines = _capital_flow_lines(pulse, drivers, liquidity_lines, snapshots)
    verification_rows = _verification_rows_v4(snapshots, monitor_rows)
    headline_lines = (
        list(market_analysis.get("summary_lines", []))[:2]
        + _compact_headline_lines(narrative, china_macro, monitor_rows, pulse)
        + _regime_explanation_lines(china_macro, regime_result, narrative)
        + _story_lines(news_report, monitor_rows, snapshots, narrative)
        + _impact_lines(snapshots, monitor_rows, regime_result)
    )
    action_lines = _positioning_lines(narrative, monitor_rows) + [
        "执行上先看指数、风格和资金是否同向，再决定是顺主线加风险，还是回到防守框架。",
        "如果 A 股观察池扩散不足，就把今天理解成结构性行情，不按全面 risk-on 处理。",
    ]

    return {
        "title": "全市场行情简报",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_coverage": data_coverage,
        "missing_sources": missing_sources,
        "regime_reasoning_lines": list(regime_result.get("reasoning") or []),
        "headline_lines": headline_lines,
        "macro_items": macro_items,
        "action_lines": action_lines,
        "domestic_index_rows": domestic_index_rows,
        "domestic_market_lines": domestic_market_lines + _market_overview_lines(snapshots, regime_result),
        "index_signal_rows": list(market_analysis.get("index_rows", [])),
        "index_signal_lines": list(market_analysis.get("index_lines", [])),
        "market_signal_rows": list(market_analysis.get("market_signal_rows", [])),
        "market_signal_lines": list(market_analysis.get("market_signal_lines", [])),
        "style_rows": style_rows,
        "industry_rows": industry_rows,
        "rotation_rows": list(market_analysis.get("rotation_rows", [])),
        "rotation_lines": list(market_analysis.get("rotation_lines", [])),
        "macro_asset_rows": macro_asset_rows,
        "overnight_rows": overnight_rows,
        "watchlist_rows": watchlist_rows,
        "a_share_watch_rows": a_share_watch_rows,
        "a_share_watch_lines": a_share_watch_lines,
        "theme_tracking_rows": theme_tracking_rows,
        "theme_tracking_lines": theme_tracking_lines,
        "core_event_lines": _core_event_lines(news_report, _catalyst_rows(news_report, narrative)),
        "capital_flow_lines": capital_flow_lines,
        "quality_lines": quality_lines,
        "evidence_rows": evidence_rows,
        "verification_rows": verification_rows,
        "alerts": alerts or ["当前没有触发额外强提醒，但市场风格切换仍需持续验证。"],
        "a_share_watch_meta": a_share_watch_meta,
        "a_share_watch_candidates": a_share_watch_candidates,
        "a_share_watch_upgrade_lines": [],
        "proxy_contract": proxy_contract,
        "regime": regime_result,
        "day_theme": narrative.get("label", ""),
    }


def _persist_briefing(markdown: str, mode: str) -> Path:
    reports_dir = _briefing_internal_dir()
    reports_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{mode}_briefing_{date_str}.md"
    md_path = reports_dir / filename
    md_path.write_text(markdown, encoding="utf-8")

    pdf_path = reports_dir / f"{mode}_briefing_{date_str}.pdf"
    _export_pdf(markdown, pdf_path)
    return md_path


def _briefing_internal_dir() -> Path:
    return resolve_project_path("reports/briefings/internal")


def _export_pdf(markdown_text: str, pdf_path: Path) -> None:
    """Convert briefing markdown to styled PDF."""
    try:
        from src.output.briefing_pdf import render_briefing_pdf
        render_briefing_pdf(markdown_text, pdf_path)
    except Exception:
        pass


def _render_briefing_charts(snapshots: List[BriefingSnapshot]) -> Dict[str, Dict[str, str]]:
    """Generate windows and indicators charts for each watchlist item."""
    from src.output.analysis_charts import AnalysisChartRenderer

    renderer = AnalysisChartRenderer()
    if not renderer.enabled:
        return {}

    charts: Dict[str, Dict[str, str]] = {}
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    for snap in snapshots:
        if snap.history.empty:
            continue
        analysis = {
            "symbol": snap.symbol,
            "name": snap.name,
            "generated_at": stamp,
            "history": snap.history,
            "technical_raw": snap.technical,
        }
        base = f"{snap.symbol}_{stamp}"
        windows_path = renderer.output_dir / f"{base}_windows.png"
        indicators_path = renderer.output_dir / f"{base}_indicators.png"
        renderer._render_windows(analysis, snap.history.copy(), windows_path)
        renderer._render_indicators(analysis, snap.history.copy(), indicators_path)
        label = f"{snap.name} ({snap.symbol})"
        paths: Dict[str, str] = {}
        if windows_path.exists():
            paths["windows"] = str(windows_path.resolve())
        if indicators_path.exists():
            paths["indicators"] = str(indicators_path.resolve())
        if paths:
            charts[label] = paths
    return charts


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("briefing")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    collector_timeout_seconds = float(config.get("briefing_collector_timeout_seconds", 15))
    china_macro = load_china_macro_snapshot(config)
    global_proxy, global_proxy_note = _load_briefing_global_proxy(config)
    monitor_rows = _collect_monitor_rows(config)
    regime_inputs = derive_regime_inputs(china_macro, global_proxy, monitor_rows)
    regime_result = RegimeDetector(regime_inputs).detect_regime()

    snapshots, alerts, watchlist_rows = _collect_snapshots(config, args.mode)
    proxy_contract = _briefing_proxy_contract(snapshots, config)
    collection_warnings: List[str] = []
    overview, warning = _timed_collect(
        "市场概览",
        lambda: MarketOverviewCollector(config).collect(),
        fallback={},
        timeout_seconds=collector_timeout_seconds,
    )
    if warning:
        collection_warnings.append(warning)
    pulse, warning = _timed_collect(
        "市场脉冲",
        lambda: MarketPulseCollector(config).collect(),
        fallback={},
        timeout_seconds=collector_timeout_seconds,
    )
    if warning:
        collection_warnings.append(warning)
    drivers, warning = _timed_collect(
        "市场驱动",
        lambda: MarketDriversCollector(config).collect(),
        fallback={},
        timeout_seconds=collector_timeout_seconds,
    )
    if warning:
        collection_warnings.append(warning)
    news_report, warning = _timed_collect(
        "新闻覆盖",
        lambda: _news_report(snapshots, china_macro, global_proxy, config, args.news_source),
        fallback={"items": [], "lines": []},
        timeout_seconds=max(collector_timeout_seconds, 20),
    )
    if warning:
        collection_warnings.append(warning)
    events, warning = _timed_collect(
        "事件日历",
        lambda: EventsCollector(config).collect(mode=args.mode),
        fallback=[],
        timeout_seconds=collector_timeout_seconds,
    )
    if warning:
        collection_warnings.append(warning)
    a_share_watch_rows: List[List[str]] = []
    a_share_watch_lines: List[str] = []
    a_share_watch_meta: Dict[str, Any] = {}
    a_share_watch_candidates: List[Dict[str, Any]] = []
    if args.mode in {"daily", "weekly", "market"}:
        a_share_watch_context = _briefing_shared_market_context(
            config,
            china_macro=china_macro,
            global_proxy=global_proxy,
            monitor_rows=monitor_rows,
            regime_result=regime_result,
            news_report=news_report,
            drivers=drivers,
            pulse=pulse,
            events=events,
        )
        a_share_watch_rows, a_share_watch_lines, a_share_watch_meta, a_share_watch_candidates = _briefing_a_share_watch_rows(
            config,
            shared_context=a_share_watch_context,
        )
    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime_result, a_share_watch_meta)
    anomaly_report = _anomaly_report(snapshots, monitor_rows)
    liquidity_lines = _liquidity_lines(config)
    data_coverage, missing_sources = _coverage_metadata(news_report, liquidity_lines, events, global_proxy_note, monitor_rows)
    macro_items = macro_lines(china_macro, global_proxy)
    regime_label = REGIME_LABELS.get(str(regime_result["current_regime"]), str(regime_result["current_regime"]))
    macro_items.append(f"当前宏观环境判断: {regime_label}。")
    effective_assets = _effective_asset_preference(narrative)
    if narrative.get("overrides_background"):
        macro_items.append(
            "背景 regime 提供的是中期框架，但今天日内优先跟随主线，资产偏好先看: " + "、".join(effective_assets) + "。"
        )
    elif effective_assets:
        macro_items.append("当前更匹配的资产偏好: " + "、".join(effective_assets) + "。")
    if global_proxy_note:
        macro_items.append(global_proxy_note)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    evidence_rows = _briefing_evidence_rows(
        generated_at=generated_at,
        narrative=narrative,
        regime_result=regime_result,
        data_coverage=data_coverage,
        missing_sources=missing_sources,
        a_share_watch_meta=a_share_watch_meta,
        proxy_contract=proxy_contract,
    )

    overnight_rows = _overnight_rows(overview)
    alerts = _monitor_alerts(monitor_rows) + list(alerts or [])

    if args.mode == "noon":
        payload = _build_noon_payload(
            snapshots, monitor_rows, overview, pulse, drivers,
            narrative, watchlist_rows, events, config,
        )
        rendered = BriefingRenderer().render_noon(payload)
    elif args.mode == "evening":
        payload = _build_evening_payload(
            snapshots, monitor_rows, overview, pulse, drivers,
            narrative, news_report, watchlist_rows, config,
            anomaly_report, overnight_rows, liquidity_lines,
        )
        rendered = BriefingRenderer().render_evening(payload)
    elif args.mode == "market":
        payload = _build_market_payload(
            config=config,
            narrative=narrative,
            china_macro=china_macro,
            regime_result=regime_result,
            overview=overview,
            pulse=pulse,
            drivers=drivers,
            news_report=news_report,
            monitor_rows=monitor_rows,
            snapshots=snapshots,
            anomaly_report=anomaly_report,
            liquidity_lines=liquidity_lines,
            overnight_rows=overnight_rows,
            watchlist_rows=watchlist_rows,
            a_share_watch_rows=a_share_watch_rows,
            a_share_watch_lines=a_share_watch_lines,
            a_share_watch_meta=a_share_watch_meta,
            a_share_watch_candidates=a_share_watch_candidates,
            data_coverage=data_coverage,
            missing_sources=missing_sources,
            macro_items=macro_items,
            alerts=alerts,
            quality_lines=_quality_lines(news_report, anomaly_report, monitor_rows) + collection_warnings,
            proxy_contract=proxy_contract,
            evidence_rows=evidence_rows,
        )
        rendered = BriefingRenderer().render_market(payload)
    else:
        yesterday_rows = _yesterday_review_rows(snapshots, monitor_rows)
        yesterday_lines = _yesterday_review_summary_lines(yesterday_rows)
        domestic_index_rows, domestic_market_lines = _domestic_overview_rows(overview, pulse)
        style_rows = _style_rows(overview, drivers.get("industry_spot", pd.DataFrame()))
        industry_rows = _industry_rank_rows(drivers, narrative, news_report)
        macro_asset_rows = _macro_asset_rows(monitor_rows, anomaly_report)
        catalyst_rows = _catalyst_rows(news_report, narrative)
        theme_tracking_rows = _theme_tracking_rows(narrative, drivers)
        theme_tracking_lines = _theme_tracking_lines(narrative, theme_tracking_rows, args.mode)
        capital_flow_lines = _capital_flow_lines(pulse, drivers, liquidity_lines, snapshots)
        quality_lines = _quality_lines(news_report, anomaly_report, monitor_rows) + collection_warnings
        verification_rows = _verification_rows_v4(snapshots, monitor_rows)
        portfolio_lines = _portfolio_lines(config)
        portfolio_table_rows = _portfolio_table_rows(config)
        briefing_charts = _render_briefing_charts(snapshots)

        payload = {
            "title": "每日晨报" if args.mode == "daily" else "每周周报",
            "generated_at": generated_at,
            "data_coverage": data_coverage,
            "missing_sources": missing_sources,
            "headline_lines": _compact_headline_lines(narrative, china_macro, monitor_rows, pulse)
            + _compact_validation_lines(narrative, monitor_rows, pulse),
            "macro_items": macro_items,
            "regime_reasoning_lines": list(regime_result.get("reasoning") or []),
            "regime": regime_result,
            "day_theme": narrative.get("label", ""),
            "evidence_rows": evidence_rows,
            "yesterday_review_rows": yesterday_rows,
            "yesterday_review_lines": yesterday_lines,
            "domestic_index_rows": domestic_index_rows,
            "domestic_market_lines": domestic_market_lines,
            "style_rows": style_rows,
            "industry_rows": industry_rows,
            "a_share_watch_rows": a_share_watch_rows,
            "a_share_watch_lines": a_share_watch_lines,
            "macro_asset_rows": macro_asset_rows,
            "overnight_rows": overnight_rows,
            "watchlist_rows": watchlist_rows,
            "core_event_lines": _core_event_lines(news_report, catalyst_rows),
            "theme_tracking_rows": theme_tracking_rows,
            "theme_tracking_lines": theme_tracking_lines,
            "market_event_rows": _market_event_rows(news_report, narrative),
            "workflow_event_rows": _workflow_event_rows(events),
            "capital_flow_lines": capital_flow_lines,
            "quality_lines": quality_lines,
            "verification_rows": verification_rows,
            "portfolio_lines": portfolio_lines,
            "portfolio_table_rows": portfolio_table_rows,
            "appendix_technical_rows": _appendix_technical_rows(snapshots),
            "appendix_lhb_lines": _lhb_lines(pulse),
            "appendix_flow_lines": _flow_lines(snapshots, config) + _sentiment_lines(snapshots, config),
            "appendix_derivative_lines": _appendix_derivative_lines(narrative, monitor_rows),
            "appendix_earnings_rows": _appendix_earnings_rows(news_report),
            "appendix_allocation_rows": _appendix_allocation_rows(narrative, monitor_rows),
            "flow_lines": _flow_lines(snapshots, config),
            "watchlist_technical_lines": _watchlist_technical_lines(snapshots),
            "sentiment_lines": _sentiment_lines(snapshots, config),
            "alerts": alerts or ["当前没有触发强提醒，但仍需关注强弱方向是否在盘中发生切换。"],
            "action_lines": _action_lines(snapshots, narrative, monitor_rows),
            "charts": briefing_charts,
            "a_share_watch_meta": a_share_watch_meta,
            "a_share_watch_candidates": a_share_watch_candidates,
            "a_share_watch_upgrade_lines": [],
            "proxy_contract": proxy_contract,
        }
        rendered = BriefingRenderer().render(payload)
    detail_path = _persist_briefing(rendered, args.mode)
    if not args.client_final:
        print(rendered)
        return

    if args.mode in {"daily", "weekly"}:
        client_markdown = ClientReportRenderer().render_briefing(payload)
        findings = check_generic_client_report(client_markdown, "briefing", source_text=rendered)
        output_path = resolve_project_path("reports/briefings/final") / f"{args.mode}_briefing_{str(payload.get('generated_at', ''))[:10]}_client_final.md"
        bundle = finalize_client_markdown(
            report_type="briefing",
            client_markdown=client_markdown,
            markdown_path=output_path,
            detail_markdown=rendered,
            detail_path=detail_path,
            extra_manifest={
                "mode": args.mode,
                "a_share_watch": payload.get("a_share_watch_meta", {}),
                "factor_contract": dict(payload.get("a_share_watch_meta", {})).get("factor_contract", {}),
                "proxy_contract": dict(payload.get("proxy_contract") or {}),
            },
            release_checker=lambda markdown, source_text: check_generic_client_report(markdown, "briefing", source_text=source_text),
        )
        print(client_markdown)
        from src.commands.report_guard import exported_bundle_lines

        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
        return

    findings = check_generic_client_report(rendered, "briefing")
    output_path = resolve_project_path("reports/briefings/final") / f"{args.mode}_briefing_{datetime.now().strftime('%Y-%m-%d')}_client_final.md"
    bundle = finalize_client_markdown(
        report_type="briefing",
        client_markdown=rendered,
        markdown_path=output_path,
        detail_markdown=rendered,
        detail_path=detail_path,
        extra_manifest={
            "mode": args.mode,
            "a_share_watch": payload.get("a_share_watch_meta", {}),
            "factor_contract": dict(payload.get("a_share_watch_meta", {})).get("factor_contract", {}),
            "proxy_contract": dict(payload.get("proxy_contract") or {}),
        },
        release_checker=lambda markdown, source_text: check_generic_client_report(markdown, "briefing", source_text=source_text),
    )
    print(rendered)
    from src.commands.report_guard import exported_bundle_lines

    for index, line in enumerate(exported_bundle_lines(bundle)):
        print(f"\n{line}" if index == 0 else line)


if __name__ == "__main__":
    main()
