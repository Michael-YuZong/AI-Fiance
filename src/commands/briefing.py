"""Daily and weekly briefing command."""

from __future__ import annotations

import argparse
import io
import warnings
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+")

from src.collectors import (
    EventsCollector,
    GlobalFlowCollector,
    MarketDriversCollector,
    MarketMonitorCollector,
    MarketPulseCollector,
    NewsCollector,
    SocialSentimentCollector,
)
from src.output.briefing import BriefingRenderer
from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot, macro_lines
from src.processors.regime import RegimeDetector
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.config import load_config
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
    technical: Dict[str, Any] = field(default_factory=dict)
    technical_bias: str = "分歧"


REGIME_LABELS = {
    "recovery": "温和复苏",
    "overheating": "过热",
    "stagflation": "滞涨",
    "deflation": "通缩/偏弱",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily or weekly market briefing.")
    parser.add_argument("mode", choices=["daily", "weekly"], help="Briefing mode")
    parser.add_argument("--news-source", action="append", default=[], help="Preferred news source, e.g. Reuters")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
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
        note = f"量比 {volume_ratio:.2f}，盘面明显活跃。"
    elif volume_ratio < 0.7:
        note = f"量比 {volume_ratio:.2f}，资金参与度偏弱。"
    else:
        note = f"量比 {volume_ratio:.2f}，量能处于常态区间。"
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


def _collect_snapshots(config: Dict[str, Any], mode: str) -> tuple[List[BriefingSnapshot], List[str], List[List[str]]]:
    snapshots: List[BriefingSnapshot] = []
    alerts: List[str] = []
    rows: List[List[str]] = []

    for item in load_watchlist():
        symbol = item["symbol"]
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(symbol, item["asset_type"], config))
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
                technical=technical,
                technical_bias=technical_bias,
            )
            snapshots.append(snapshot)
            rows.append(
                [
                    f"{symbol} ({item['name']})",
                    f"{metrics['last_close']:.3f}",
                    format_pct(metrics["return_1d"]),
                    format_pct(metrics["return_5d"]),
                    format_pct(metrics["return_20d"]),
                    trend,
                    f"{technical['volume']['vol_ratio']:.2f}",
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
    preferred_assets = ", ".join(narrative.get("preferred_assets", [])[:3]) if narrative.get("preferred_assets") else ""
    background = narrative.get("background_regime", "未识别")
    lines = [narrative.get("summary", "今天没有单一主线。")]
    lines.append(
        f"背景宏观环境仍接近 `{background}`"
        + (f"，资产偏好更偏 {preferred_assets}。" if preferred_assets else "。")
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
        lines.append("国内景气度仍在荣枯线下方，晨报解读会更偏重‘谁更抗跌、谁在逆势走强’。")
    else:
        lines.append("国内景气度在荣枯线附近或以上，晨报解读会更偏重‘哪些方向具备趋势延续性’。")
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


def _find_snapshot(snapshots: List[BriefingSnapshot], symbol: str) -> Optional[BriefingSnapshot]:
    for item in snapshots:
        if item.symbol == symbol:
            return item
    return None


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
    lines.append("说明：当前资金流为相对强弱代理，不是机构申购赎回原始数据。")
    return lines


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
    return MarketMonitorCollector(config).collect()


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


def _primary_narrative(
    news_report: Dict[str, Any],
    monitor_rows: List[Dict[str, Any]],
    pulse: Dict[str, Any],
    snapshots: List[BriefingSnapshot],
    drivers: Dict[str, Any],
    regime_result: Dict[str, Any],
) -> Dict[str, Any]:
    counter = _news_category_counter(news_report)
    monitor = _monitor_map(monitor_rows)
    brent = monitor.get("布伦特原油", {})
    dxy = monitor.get("美元指数", {})
    vix = monitor.get("VIX波动率", {})
    us10y = monitor.get("美国10Y收益率", {})

    brent_1d = _to_float(brent.get("return_1d"))
    brent_5d = _to_float(brent.get("return_5d"))
    dxy_5d = _to_float(dxy.get("return_5d"))
    vix_latest = _to_float(vix.get("latest"))
    us10y_1d = _to_float(us10y.get("return_1d"))

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

    tech_1d = 0.0
    if qqqm:
        tech_1d += qqqm.return_1d
    if hstech:
        tech_1d += hstech.return_1d
    gold_1d = gld.return_1d if gld else 0.0
    grid_1d = grid.return_1d if grid else 0.0

    scores = {
        "energy_shock": 0,
        "defensive_riskoff": 0,
        "rate_growth": 0,
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

    scores["rate_growth"] += counter["fed"] * 2
    scores["rate_growth"] += counter["earnings"]
    if us10y_1d < 0:
        scores["rate_growth"] += 2
    if dxy_5d <= 0:
        scores["rate_growth"] += 1
    if tech_1d > 0 and vix_latest < 22:
        scores["rate_growth"] += 2

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

    theme = max(scores, key=scores.get)
    score = scores[theme]
    if score < 4:
        theme = "macro_background"

    theme_labels = {
        "energy_shock": "能源冲击",
        "defensive_riskoff": "防守避险",
        "rate_growth": "利率驱动成长修复",
        "china_policy": "中国政策/内需确定性",
        "ai_semis": "AI/半导体催化",
        "macro_background": "背景宏观",
    }

    theme_summaries = {
        "energy_shock": "今天市场主线更像 `能源冲击 + 地缘风险`，应优先放在晨报最前面，而不是被背景 regime 覆盖。",
        "defensive_riskoff": "今天市场主线更像 `防守避险`，核心是先谈波动和回撤控制，再谈进攻。",
        "rate_growth": "今天市场主线更像 `利率预期驱动的成长修复`，重点看科技和估值弹性方向。",
        "china_policy": "今天市场主线更像 `中国政策 / 内需确定性`，重点看基建、电网和稳增长传导。",
        "ai_semis": "今天市场主线更像 `AI / 半导体催化`，重点看算力、芯片和相关硬件链。",
        "macro_background": "今天没有单一事件完全压过其他变量，更适合先以宏观背景和盘面结构来组织晨报。",
    }

    return {
        "theme": theme,
        "label": theme_labels[theme],
        "summary": theme_summaries[theme],
        "scores": scores,
        "background_regime": REGIME_LABELS.get(str(regime_result["current_regime"]), str(regime_result["current_regime"])),
        "preferred_assets": list(regime_result.get("preferred_assets", [])),
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
    regime_result: Dict[str, Any],
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

    preferred = regime_result.get("preferred_assets", [])
    if preferred:
        lines.append("从当前 regime 看，和今天环境更一致的方向是: " + "、".join(preferred[:4]) + "。")
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
        lines.append(f"{item.symbol}: {aggregate['interpretation']}")
    lines.append("说明：当前情绪为价格和量能推断的讨论热度代理，不是抓取到的真实社媒帖子。")
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


def _event_lines(config: Dict[str, Any], mode: str) -> List[str]:
    events = EventsCollector(config).collect(mode=mode)
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


def _action_lines(snapshots: List[BriefingSnapshot], regime_result: Dict[str, Any]) -> List[str]:
    if not snapshots:
        return ["先修复数据覆盖，再谈晨报动作。"]

    strongest = max(snapshots, key=lambda item: item.signal_score)
    weakest = min(snapshots, key=lambda item: item.signal_score)
    gold = next((item for item in snapshots if item.sector == "黄金"), None)
    lines = [
        f"优先跟踪 {strongest.symbol}：当前信号分最高，适合观察是否继续放量并维持趋势。",
        f"谨慎对待 {weakest.symbol}：当前信号分最低，除非出现止跌确认，否则不宜过早抄底。",
    ]
    if gold:
        lines.append(f"把 {gold.symbol} 当作防守情绪验证器，观察避险需求是否继续抬头。")
    if "黄金" in regime_result.get("preferred_assets", []):
        lines.append("如果今天风险偏好继续回落，黄金相关方向的优先级可以适度上调。")
    if "港股科技" in regime_result.get("preferred_assets", []):
        lines.append("如果港股科技出现量价修复，可以把它当作风险偏好回暖的验证器。")
    return lines


def main() -> None:
    args = build_parser().parse_args()
    setup_logger("ERROR")
    config = load_config(args.config or None)
    china_macro = load_china_macro_snapshot(config)
    global_proxy = {}
    global_proxy_note = ""
    try:
        with redirect_stderr(io.StringIO()):
            global_proxy = load_global_proxy_snapshot()
    except Exception:
        global_proxy_note = "跨市场代理数据暂不可用，已回退到国内宏观与本地缓存。"

    regime_inputs = derive_regime_inputs(china_macro, global_proxy)
    regime_result = RegimeDetector(regime_inputs).detect_regime()

    snapshots, alerts, watchlist_rows = _collect_snapshots(config, args.mode)
    pulse = MarketPulseCollector(config).collect()
    drivers = MarketDriversCollector(config).collect()
    news_report = _news_report(snapshots, china_macro, global_proxy, config, args.news_source)
    monitor_rows = _collect_monitor_rows(config)
    narrative = _primary_narrative(news_report, monitor_rows, pulse, snapshots, drivers, regime_result)
    macro_items = macro_lines(china_macro, global_proxy)
    regime_label = REGIME_LABELS.get(str(regime_result["current_regime"]), str(regime_result["current_regime"]))
    macro_items.append(f"当前宏观环境判断: {regime_label}。")
    if regime_result.get("preferred_assets"):
        macro_items.append("当前更匹配的资产偏好: " + "、".join(regime_result["preferred_assets"]) + "。")
    if global_proxy_note:
        macro_items.append(global_proxy_note)

    payload = {
        "title": "每日晨报" if args.mode == "daily" else "每周周报",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "headline_lines": _headline_lines(args.mode, snapshots, narrative, china_macro, pulse),
        "narrative_validation_lines": _narrative_validation_lines(narrative, news_report, monitor_rows, pulse, snapshots),
        "important_event_lines": _important_event_lines(news_report),
        "news_lines": _news_lines(news_report),
        "story_lines": _story_lines(news_report, monitor_rows, snapshots, regime_result),
        "rotation_driver_lines": _rotation_driver_lines(drivers, pulse, snapshots),
        "main_flow_driver_lines": _main_flow_driver_lines(drivers),
        "impact_lines": _impact_lines(snapshots, monitor_rows, regime_result),
        "market_pulse_lines": _market_pulse_lines(pulse),
        "lhb_lines": _lhb_lines(pulse),
        "monitor_lines": _monitor_lines(monitor_rows),
        "overnight_lines": _overnight_lines(snapshots),
        "macro_items": macro_items,
        "market_overview_lines": _market_overview_lines(snapshots, regime_result),
        "flow_lines": _flow_lines(snapshots, config),
        "sentiment_lines": _sentiment_lines(snapshots, config),
        "watchlist_rows": watchlist_rows,
        "watchlist_technical_lines": _watchlist_technical_lines(snapshots),
        "focus_lines": _focus_lines(snapshots, args.mode),
        "rotation_lines": _rotation_lines(snapshots),
        "alerts": alerts or ["当前没有触发强提醒，但仍需关注强弱方向是否在盘中发生切换。"],
        "event_lines": _event_lines(config, args.mode),
        "portfolio_lines": _portfolio_lines(config),
        "verification_lines": _verification_lines(snapshots, monitor_rows),
        "calendar_lines": _calendar_lines(args.mode),
        "action_lines": _action_lines(snapshots, regime_result),
    }
    print(BriefingRenderer().render(payload))


if __name__ == "__main__":
    main()
