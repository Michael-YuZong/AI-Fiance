"""Unified opportunity discovery and analysis engine."""

from __future__ import annotations

import io
import math
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from src.collectors import (
    AssetLookupCollector,
    ChinaMarketCollector,
    EventsCollector,
    MarketDriversCollector,
    MarketMonitorCollector,
    MarketPulseCollector,
    NewsCollector,
    ValuationCollector,
)
from src.processors.context import derive_regime_inputs, load_china_macro_snapshot, load_global_proxy_snapshot
from src.processors.regime import RegimeDetector
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import detect_asset_type, resolve_project_path
from src.utils.data import load_watchlist, load_yaml
from src.utils.market import compute_history_metrics, fetch_asset_history, format_pct, get_asset_context


SECTOR_RULES = [
    (("沪深300", "中证a500", "a500", "中证500", "上证50", "宽基"), "宽基", ["宽基", "大盘蓝筹", "内需"]),
    (("电网", "电力", "储能", "逆变器", "特高压"), "电网", ["AI算力", "电力需求", "电网设备", "铜铝"]),
    (("黄金", "贵金属"), "黄金", ["黄金", "通胀预期"]),
    (("半导体", "芯片", "通信", "算力", "人工智能", "AI", "软件", "消费电子"), "科技", ["AI算力", "半导体", "成长股估值修复"]),
    (("油", "煤", "能源"), "能源", ["原油", "通胀预期", "能源安全"]),
    (("银行", "红利", "高股息"), "高股息", ["高股息", "防守"]),
    (("医药", "医疗"), "医药", ["医药", "老龄化"]),
    (("消费", "酒", "食品"), "消费", ["内需", "消费修复"]),
    (("军工", "国防"), "军工", ["军工", "地缘风险"]),
    (("有色", "铜", "铝"), "有色", ["铜铝", "顺周期"]),
]

DEFAULT_CHAIN_NODES = ["宏观主线", "行业轮动"]
BENCHMARKS = {
    "cn_etf": ("510300", "cn_etf", "沪深300ETF"),
    "cn_index": ("510300", "cn_etf", "沪深300ETF"),
    "cn_fund": ("510300", "cn_etf", "沪深300ETF"),
    "us": ("SPY", "us", "标普500"),
    "hk": ("2800.HK", "hk", "恒生指数ETF"),
    "hk_index": ("2800.HK", "hk", "恒生指数ETF"),
}

MONTHLY_SEASONAL_WINDOWS = {
    "宽基": {1, 2, 11, 12},
    "消费": {9, 10, 11},
    "医药": {10, 11, 12},
    "高股息": {4, 5, 6},
    "电网": {3, 4, 5},
    "科技": {6, 7, 8},
    "能源": {9, 10, 11},
    "军工": {7, 8, 9},
    "有色": {2, 3, 4},
}

SENSITIVITY_MAP = {
    "宽基": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "电网": {"rate": -1, "usd": 0, "oil": 1, "cny": 1},
    "科技": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "黄金": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
    "能源": {"rate": 0, "usd": 1, "oil": 1, "cny": -1},
    "高股息": {"rate": 1, "usd": 0, "oil": 0, "cny": 0},
    "医药": {"rate": -1, "usd": 0, "oil": -1, "cny": 1},
    "消费": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "军工": {"rate": 0, "usd": 1, "oil": 1, "cny": -1},
    "有色": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
}

NEWS_KEYWORD_ALIASES = {
    "宽基": ["沪深300", "中证A500", "A500", "large cap", "blue chip", "宽基", "broad market"],
    "科技": ["科技", "ai", "artificial intelligence", "semiconductor", "chip", "chips", "foundry", "fab", "gpu", "存储", "算力", "芯片", "半导体"],
    "黄金": ["黄金", "gold", "bullion", "precious metal", "贵金属"],
    "电网": ["电网", "电力", "grid", "power", "utility", "electricity", "特高压", "储能"],
    "能源": ["能源", "oil", "opec", "gas", "原油", "煤炭"],
    "高股息": ["高股息", "红利", "dividend", "yield", "utility"],
    "医药": ["医药", "医疗", "biotech", "pharma", "drug", "医疗器械"],
    "消费": ["消费", "零售", "retail", "消费电子", "beer", "food"],
    "军工": ["军工", "国防", "defense", "aerospace", "制裁"],
    "有色": ["有色", "copper", "aluminum", "metal", "铜", "铝", "矿业"],
    "AI算力": ["ai", "artificial intelligence", "gpu", "model", "llm", "算力"],
    "半导体": ["semiconductor", "chip", "foundry", "fab", "wafer", "存储", "芯片", "半导体"],
    "国产替代": ["国产替代", "localisation", "domestic supply", "自主可控"],
    "通胀预期": ["inflation", "通胀", "cpi"],
}

VALUATION_KEYWORD_MAP = {
    "宽基": ["沪深300", "中证A500", "中证500", "上证50"],
    "电网": ["电网", "智能电网"],
    "科技": ["科技", "半导体", "芯片"],
    "黄金": ["黄金"],
    "能源": ["能源", "油气", "煤炭"],
    "高股息": ["红利", "高股息"],
    "医药": ["医药"],
    "消费": ["消费"],
    "军工": ["军工"],
    "有色": ["有色", "铜", "铝"],
}

BOARD_MATCH_ALIASES = {
    "宽基": ["沪深300", "中证A500", "中证500", "上证50", "宽基"],
    "电网": ["电网", "电力", "电网设备", "智能电网", "特高压", "公用事业"],
    "科技": ["半导体", "芯片", "人工智能", "AI", "消费电子", "软件服务", "通信设备", "算力"],
    "黄金": ["黄金", "贵金属"],
    "能源": ["能源", "油气", "石油", "煤炭", "天然气"],
    "高股息": ["红利", "高股息", "银行", "电信", "公用事业"],
    "医药": ["医药", "医疗", "创新药", "医疗器械"],
    "消费": ["消费", "食品饮料", "家电", "零售", "旅游"],
    "军工": ["军工", "国防军工", "航天航空", "商业航天", "军民融合", "卫星"],
    "有色": ["有色金属", "工业金属", "铜", "铝", "黄金"],
}

GENERIC_CATALYST_PROFILES = {
    "科技": {
        "themes": ["科技", "AI算力", "成长股估值修复"],
        "keywords": ["科技", "ai", "cloud", "software", "semiconductor", "chip", "算力", "云", "大模型"],
        "policy_keywords": ["人工智能", "算力", "软件", "数字经济", "云计算", "科技"],
        "domestic_leaders": ["中际旭创", "工业富联", "寒武纪", "中科曙光", "浪潮信息", "金山办公"],
        "overseas_leaders": ["Microsoft", "Apple", "NVIDIA", "Amazon", "Meta", "Alphabet", "Broadcom", "AMD"],
        "earnings_keywords": ["earnings", "results", "guidance", "capex", "cloud", "AI", "财报", "指引", "资本开支"],
        "event_keywords": ["财报", "指引", "资本开支", "capex", "云", "AI", "产品发布", "模型发布"],
    },
    "军工": {
        "themes": ["军工", "地缘风险", "国防"],
        "keywords": ["军工", "国防", "defense", "aerospace", "军贸", "无人机", "导弹", "卫星", "装备"],
        "policy_keywords": ["国防预算", "军费", "装备采购", "军贸", "国防", "军工", "军演", "安全"],
        "domestic_leaders": ["中航沈飞", "航发动力", "中航光电", "中航西飞", "洪都航空", "中国船舶", "中兵红箭"],
        "overseas_leaders": ["Lockheed Martin", "Northrop", "RTX", "General Dynamics", "Boeing", "Palantir"],
        "earnings_keywords": ["order", "guidance", "delivery", "财报", "订单", "交付", "指引"],
        "event_keywords": ["军演", "军贸", "订单", "交付", "首飞", "试飞", "卫星", "无人机"],
    },
    "能源": {
        "themes": ["能源", "原油", "通胀预期"],
        "keywords": ["能源", "oil", "gas", "lng", "opec", "原油", "天然气", "炼化"],
        "policy_keywords": ["能源安全", "产量", "增产", "减产", "战略储备"],
        "domestic_leaders": ["中国海油", "中国石油", "中国石化", "陕西煤业", "兖矿能源"],
        "overseas_leaders": ["Exxon", "Chevron", "Shell", "BP", "Saudi Aramco"],
        "earnings_keywords": ["earnings", "results", "production", "output", "财报", "产量", "指引"],
        "event_keywords": ["OPEC", "减产", "增产", "库存", "油价", "气价"],
    },
    "高股息": {
        "themes": ["高股息", "防守", "红利"],
        "keywords": ["高股息", "红利", "dividend", "yield", "utility", "银行", "电信"],
        "policy_keywords": ["分红", "市值管理", "回购", "红利"],
        "domestic_leaders": ["中国神华", "长江电力", "中国移动", "工商银行", "农业银行"],
        "overseas_leaders": ["AT&T", "Verizon", "Duke Energy", "Coca-Cola"],
        "earnings_keywords": ["dividend", "buyback", "cash flow", "分红", "回购", "现金流"],
        "event_keywords": ["分红", "除权", "回购", "现金流"],
    },
    "医药": {
        "themes": ["医药", "老龄化"],
        "keywords": ["医药", "biotech", "pharma", "drug", "医疗器械", "创新药"],
        "policy_keywords": ["医保", "集采", "审批", "创新药", "医疗"],
        "domestic_leaders": ["恒瑞医药", "迈瑞医疗", "药明康德", "爱尔眼科", "智飞生物"],
        "overseas_leaders": ["Eli Lilly", "Novo Nordisk", "Pfizer", "Merck", "AbbVie"],
        "earnings_keywords": ["trial", "approval", "guidance", "财报", "临床", "获批", "指引"],
        "event_keywords": ["临床", "获批", "医保谈判", "集采", "新药"],
    },
    "消费": {
        "themes": ["消费", "内需"],
        "keywords": ["消费", "retail", "消费电子", "beer", "food", "旅游", "零售"],
        "policy_keywords": ["以旧换新", "促消费", "内需", "零售"],
        "domestic_leaders": ["贵州茅台", "美的集团", "海尔智家", "伊利股份", "中国中免"],
        "overseas_leaders": ["Nike", "Costco", "Walmart", "LVMH", "McDonald's"],
        "earnings_keywords": ["same-store", "guidance", "sales", "财报", "销售", "指引"],
        "event_keywords": ["促销", "补贴", "新品", "假期", "旺季"],
    },
    "宽基": {
        "themes": ["宽基", "大盘", "指数"],
        "keywords": ["index", "macro", "earnings", "rates", "liquidity", "指数", "宏观", "流动性"],
        "policy_keywords": ["利率", "财政", "政策", "流动性"],
        "domestic_leaders": ["工商银行", "贵州茅台", "宁德时代", "招商银行"],
        "overseas_leaders": ["Microsoft", "Apple", "NVIDIA", "Amazon", "Meta"],
        "earnings_keywords": ["earnings", "guidance", "rates", "payrolls", "cpi", "财报", "指引", "非农", "CPI"],
        "event_keywords": ["财报季", "利率决议", "CPI", "非农", "PMI"],
    },
}

CATALYST_CATEGORY_MAP = {
    "科技": {"ai", "earnings", "semiconductor", "fed", "global_macro"},
    "军工": {"geopolitics", "china_market_domestic", "global_macro"},
    "能源": {"energy", "geopolitics", "global_macro"},
    "黄金": {"energy", "geopolitics", "fed", "global_macro"},
    "电网": {"energy", "china_macro", "china_market_domestic"},
    "有色": {"energy", "global_macro", "china_macro"},
    "高股息": {"fed", "global_macro", "china_macro"},
    "医药": {"earnings", "global_macro"},
    "消费": {"china_macro", "earnings", "global_macro"},
    "宽基": {"global_macro", "fed", "earnings", "ai", "china_macro"},
}


@dataclass
class PoolItem:
    symbol: str
    name: str
    asset_type: str
    region: str
    sector: str
    chain_nodes: List[str]
    source: str
    turnover: float = 0.0
    in_watchlist: bool = False


def _normalize_sector(name: str, fallback: str = "综合") -> tuple[str, List[str]]:
    lowered = name.lower()
    for keywords, sector, chain_nodes in SECTOR_RULES:
        if any(keyword.lower() in lowered for keyword in keywords):
            return sector, list(chain_nodes)
    return fallback, list(DEFAULT_CHAIN_NODES)


def _merge_metadata(
    symbol: str,
    asset_type: str,
    metadata: Optional[Mapping[str, Any]],
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    context = get_asset_context(symbol, asset_type, {})
    merged = dict(context.metadata)
    if metadata:
        merged.update(dict(metadata))
    merged.setdefault("symbol", symbol)
    merged.setdefault("asset_type", asset_type)
    if (merged.get("name") in {"", symbol, None}) or str(merged.get("sector", "综合")) == "综合":
        try:
            matches = AssetLookupCollector(config or {}).search(symbol, limit=5)
            matched = next((item for item in matches if str(item.get("symbol")) == symbol), None)
            if matched:
                for key in ("name", "sector", "chain_nodes", "region", "proxy_symbol"):
                    value = matched.get(key)
                    if value not in (None, "", []):
                        merged[key] = value
        except Exception:
            pass
    merged.setdefault("name", context.name or symbol)
    sector, chain_nodes = _normalize_sector(str(merged.get("name", symbol)), str(merged.get("sector", "综合")))
    merged.setdefault("sector", sector)
    merged.setdefault("chain_nodes", chain_nodes)
    merged.setdefault("region", {"cn_etf": "CN", "hk": "HK", "hk_index": "HK", "us": "US", "futures": "CN"}.get(asset_type, "CN"))
    return merged


def _safe_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _dimension_summary(score: Optional[int], positive: str, neutral: str, negative: str, missing: str) -> str:
    if score is None:
        return missing
    if score >= 70:
        return positive
    if score >= 40:
        return neutral
    return negative


def _normalize_dimension(raw_score: int, available_max: int, target_max: int) -> Optional[int]:
    if available_max <= 0:
        return None
    normalized = int(round(raw_score / available_max * target_max))
    return max(0, min(target_max, normalized))


def _top_positive_signals(factors: Sequence[Dict[str, str]], limit: int = 3) -> str:
    positives = [item["signal"] for item in factors if item.get("awarded", 0) > 0]
    return " · ".join(positives[:limit]) if positives else "当前没有明确亮点"


def _factor_row(
    name: str,
    signal: str,
    awarded: Optional[int],
    maximum: int,
    detail: str,
    display_score: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "name": name,
        "signal": signal,
        "awarded": awarded if awarded is not None else 0,
        "max": maximum,
        "detail": detail,
        "display_score": display_score or ("缺失" if awarded is None else f"{awarded}/{maximum}"),
    }


def _history_returns(frame: pd.DataFrame) -> pd.Series:
    normalized = normalize_ohlcv_frame(frame)
    return normalized["close"].astype(float).pct_change().dropna()


def _safe_history(symbol: str, asset_type: str, config: Mapping[str, Any], period: str = "3y") -> Optional[pd.DataFrame]:
    try:
        return normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config), period=period))
    except Exception:
        return None


def _monitor_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    return {str(item.get("name", "")): item for item in rows}


def _today_theme(news_report: Mapping[str, Any], monitor_rows: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    items = list(news_report.get("items", []) or [])
    counter: Counter[str] = Counter(str(item.get("category", "")).lower() for item in items if item.get("category"))
    monitor = _monitor_map(monitor_rows)
    brent_5d = float(monitor.get("布伦特原油", {}).get("return_5d", 0.0))
    vix = float(monitor.get("VIX波动率", {}).get("latest", 0.0))
    if counter["energy"] + counter["geopolitics"] >= 2 and (brent_5d >= 0.12 or vix >= 25):
        return {"code": "energy_shock", "label": "能源冲击 + 地缘风险"}
    if counter["fed"] >= 1:
        return {"code": "rate_growth", "label": "利率驱动成长修复"}
    if counter["ai"] + counter["semiconductor"] >= 2:
        return {"code": "ai_semis", "label": "AI / 半导体催化"}
    if counter["china_macro"] + counter["china_macro_domestic"] >= 1:
        return {"code": "china_policy", "label": "中国政策 / 内需确定性"}
    return {"code": "macro_background", "label": "背景宏观主导"}


def build_market_context(
    config: Mapping[str, Any],
    preferred_sources: Optional[Sequence[str]] = None,
    relevant_asset_types: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    china_macro: Dict[str, Any] = {}
    global_proxy: Dict[str, Any] = {}
    monitor_rows: List[Dict[str, Any]] = []
    regime: Dict[str, Any] = {"current_regime": "recovery", "preferred_assets": ["成长股", "顺周期", "港股科技", "铜"]}
    news_report: Dict[str, Any] = {"mode": "proxy", "items": [], "lines": [], "note": ""}
    events: List[Dict[str, Any]] = []
    drivers: Dict[str, Any] = {}
    pulse: Dict[str, Any] = {}
    notes: List[str] = []
    watchlist = load_watchlist()
    try:
        china_macro = load_china_macro_snapshot(dict(config))
    except Exception as exc:
        notes.append(f"中国宏观数据缺失: {exc}")
    try:
        with redirect_stderr(io.StringIO()):
            global_proxy = load_global_proxy_snapshot()
    except Exception as exc:
        notes.append(f"全球代理数据缺失: {exc}")
    try:
        monitor_rows = MarketMonitorCollector(config).collect()
    except Exception as exc:
        notes.append(f"宏观监控数据缺失: {exc}")
    try:
        regime_inputs = derive_regime_inputs(china_macro, global_proxy, monitor_rows)
        regime = RegimeDetector(regime_inputs).detect_regime()
    except Exception as exc:
        notes.append(f"regime 判断失败: {exc}")
    try:
        news_report = NewsCollector(config).collect(
            snapshots=watchlist,
            china_macro=china_macro,
            global_proxy=global_proxy,
            preferred_sources=preferred_sources or (),
            limit=20,
        )
    except Exception as exc:
        notes.append(f"新闻源缺失: {exc}")
    try:
        events = EventsCollector(config).collect(mode="daily")
    except Exception as exc:
        notes.append(f"事件日历缺失: {exc}")
    try:
        drivers = MarketDriversCollector(config).collect()
    except Exception as exc:
        notes.append(f"板块驱动数据缺失: {exc}")
    try:
        pulse = MarketPulseCollector(config).collect()
    except Exception as exc:
        notes.append(f"盘面情绪数据缺失: {exc}")

    day_theme = _today_theme(news_report, monitor_rows)
    selected_asset_types = {str(item) for item in (relevant_asset_types or []) if str(item)}
    watchlist_returns: Dict[str, pd.Series] = {}
    for item in watchlist:
        item_asset_type = str(item.get("asset_type", "cn_etf"))
        if selected_asset_types and item_asset_type not in selected_asset_types:
            continue
        try:
            watchlist_returns[item["symbol"]] = _history_returns(fetch_asset_history(item["symbol"], item["asset_type"], dict(config)))
        except Exception:
            continue
    benchmark_returns: Dict[str, pd.Series] = {}
    for asset_type, (symbol, bench_asset_type, _name) in BENCHMARKS.items():
        if selected_asset_types and asset_type not in selected_asset_types:
            continue
        history = _safe_history(symbol, bench_asset_type, config)
        if history is not None:
            benchmark_returns[asset_type] = history["close"].pct_change().dropna()

    return {
        "china_macro": china_macro,
        "global_proxy": global_proxy,
        "monitor_rows": monitor_rows,
        "regime": regime,
        "day_theme": day_theme,
        "news_report": news_report,
        "events": events,
        "drivers": drivers,
        "pulse": pulse,
        "notes": notes,
        "preferred_sources": list(preferred_sources or []),
        "watchlist": watchlist,
        "watchlist_returns": watchlist_returns,
        "benchmark_returns": benchmark_returns,
    }


def _metadata_news_keys(metadata: Mapping[str, Any]) -> List[str]:
    keys = [str(metadata.get("name", "")), str(metadata.get("sector", "")), *[str(item) for item in metadata.get("chain_nodes", [])]]
    expanded: List[str] = []
    for key in keys:
        cleaned = str(key).strip()
        if not cleaned or cleaned == "综合":
            continue
        if cleaned.lower() not in expanded:
            expanded.append(cleaned.lower())
        for alias in NEWS_KEYWORD_ALIASES.get(cleaned, []):
            alias_lower = str(alias).strip().lower()
            if alias_lower and alias_lower not in expanded:
                expanded.append(alias_lower)
    name = str(metadata.get("name", ""))
    if "半导体" in name and "semiconductor" not in expanded:
        expanded.extend(["semiconductor", "chip", "chips", "foundry", "fab"])
    if "黄金" in name and "gold" not in expanded:
        expanded.extend(["gold", "bullion"])
    return expanded


def _valuation_keywords(metadata: Mapping[str, Any]) -> List[str]:
    name = str(metadata.get("name", "")).strip()
    sector = str(metadata.get("sector", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    keywords: List[str] = []
    if "半导体" in name or "芯片" in name or "半导体" in chain_nodes:
        keywords.extend(["半导体", "芯片"])
    elif "黄金" in name:
        keywords.append("黄金")
    elif "电网" in name:
        keywords.extend(["电网", "智能电网"])
    elif "有色" in name:
        keywords.append("有色")
    elif sector in VALUATION_KEYWORD_MAP:
        keywords.extend(VALUATION_KEYWORD_MAP[sector])
    if name and name not in keywords:
        keywords.append(name)
    deduped: List[str] = []
    for keyword in keywords:
        if keyword and keyword not in deduped:
            deduped.append(keyword)
    return deduped


def _board_keywords(metadata: Mapping[str, Any]) -> List[str]:
    sector = str(metadata.get("sector", "")).strip()
    name = str(metadata.get("name", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    keywords = [
        *BOARD_MATCH_ALIASES.get(sector, []),
        sector,
        name,
        *chain_nodes,
    ]
    deduped: List[str] = []
    for keyword in keywords:
        cleaned = str(keyword).strip()
        if not cleaned or cleaned == "综合":
            continue
        if cleaned not in deduped:
            deduped.append(cleaned)
    return deduped


def _first_column(frame: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    lowered = {str(column).lower(): column for column in frame.columns}
    for candidate in candidates:
        matched = lowered.get(str(candidate).lower())
        if matched:
            return matched
    return None


def _match_driver_row(frame: pd.DataFrame, metadata: Mapping[str, Any], name_candidates: Sequence[str]) -> Optional[pd.Series]:
    if frame is None or frame.empty:
        return None
    name_col = _first_column(frame, name_candidates)
    if not name_col:
        return None
    keywords = [keyword.lower() for keyword in _board_keywords(metadata)]
    best_row: Optional[pd.Series] = None
    best_score = 0
    for _, row in frame.iterrows():
        label = str(row.get(name_col, "")).strip()
        if not label:
            continue
        lowered = label.lower()
        score = sum(1 for keyword in keywords if keyword and keyword in lowered)
        if score > best_score:
            best_row = row
            best_score = score
    return best_row if best_score > 0 else None


def _row_number(row: Optional[pd.Series], candidates: Sequence[str]) -> Optional[float]:
    if row is None:
        return None
    for candidate in candidates:
        if candidate in row.index:
            value = pd.to_numeric(pd.Series([row.get(candidate)]), errors="coerce").iloc[0]
            if pd.notna(value):
                return float(value)
    lowered = {str(column).lower(): column for column in row.index}
    for candidate in candidates:
        matched = lowered.get(str(candidate).lower())
        if matched is None:
            continue
        value = pd.to_numeric(pd.Series([row.get(matched)]), errors="coerce").iloc[0]
        if pd.notna(value):
            return float(value)
    return None


def _fmt_yi_number(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "缺失"
    if abs(value) >= 1e8:
        return f"{value / 1e8:.2f}亿"
    if abs(value) >= 1e4:
        return f"{value / 1e4:.2f}万"
    return f"{value:.2f}"


def _sector_flow_snapshot(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Dict[str, Any]:
    industry_row = _match_driver_row(
        drivers.get("industry_fund_flow", pd.DataFrame()),
        metadata,
        ("行业", "名称", "板块名称"),
    )
    concept_row = _match_driver_row(
        drivers.get("concept_fund_flow", pd.DataFrame()),
        metadata,
        ("行业", "名称", "板块名称", "概念名称"),
    )
    row = industry_row if industry_row is not None else concept_row
    if row is None:
        return {}
    return {
        "name": str(row.get(_first_column(pd.DataFrame([row]), ("行业", "名称", "板块名称", "概念名称")) or "", "")),
        "main_flow": _row_number(row, ("今日主力净流入-净额", "主力净流入-净额", "主力净流入", "今日主力净流入")),
        "main_ratio": _row_number(row, ("今日主力净流入-净占比", "主力净流入-净占比", "主力净占比")),
        "super_flow": _row_number(row, ("今日超大单净流入-净额", "超大单净流入-净额", "超大单净流入")),
        "big_flow": _row_number(row, ("今日大单净流入-净额", "大单净流入-净额", "大单净流入")),
    }


def _northbound_sector_snapshot(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Dict[str, Any]:
    industry_frame = dict(drivers.get("northbound_industry", {})).get("frame", pd.DataFrame())
    concept_frame = dict(drivers.get("northbound_concept", {})).get("frame", pd.DataFrame())
    row = _match_driver_row(industry_frame, metadata, ("名称", "行业名称", "板块名称"))
    if row is None:
        row = _match_driver_row(concept_frame, metadata, ("名称", "概念名称", "板块名称"))
    if row is None:
        return {}
    return {
        "name": str(row.get(_first_column(pd.DataFrame([row]), ("名称", "行业名称", "概念名称", "板块名称")) or "", "")),
        "net_value": _row_number(
            row,
            (
                "北向资金今日增持估计-市值",
                "今日增持估计-市值",
                "今日增持估计市值",
                "增持估计-市值",
            ),
        ),
        "ratio": _row_number(row, ("今日增持估计占板块比", "今日增持估计占比")),
    }


def _hot_rank_snapshot(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Dict[str, Any]:
    frame = drivers.get("hot_rank", pd.DataFrame())
    row = _match_driver_row(frame, metadata, ("名称", "股票名称"))
    if row is None:
        return {}
    rank_value = _row_number(row, ("当前排名", "排名", "序号"))
    return {
        "name": str(row.get(_first_column(pd.DataFrame([row]), ("名称", "股票名称")) or "", "")),
        "rank": rank_value,
    }


@lru_cache(maxsize=8)
def _load_catalyst_profiles(path_value: str) -> Dict[str, Any]:
    path = resolve_project_path(path_value)
    payload = load_yaml(path, default={"profiles": {}}) or {"profiles": {}}
    return dict(payload.get("profiles", {}))


def _catalyst_profile(metadata: Mapping[str, Any], config: Mapping[str, Any]) -> Dict[str, Any]:
    path_value = str(config.get("catalyst_profiles_file", "config/catalyst_profiles.yaml"))
    profiles = _load_catalyst_profiles(path_value)
    sector = str(metadata.get("sector", "")).strip()
    name = str(metadata.get("name", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]

    lowered_name = name.lower()
    lowered_nodes = [item.lower() for item in chain_nodes]
    derived = _derived_catalyst_profile(metadata)
    for profile_name, payload in profiles.items():
        themes = [str(item).strip() for item in payload.get("themes", []) if str(item).strip()]
        if any(theme.lower() in lowered_name for theme in themes) or any(theme.lower() in lowered_nodes for theme in themes):
            matched = _merge_catalyst_profiles(derived, dict(payload))
            matched["profile_name"] = profile_name
            return matched
    profile = _merge_catalyst_profiles(derived, dict(profiles.get(sector, {})))
    if profile:
        profile.setdefault("profile_name", sector)
        return profile
    return derived


def _merge_catalyst_profiles(*profiles: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    list_keys = {"themes", "keywords", "policy_keywords", "domestic_leaders", "overseas_leaders", "earnings_keywords", "event_keywords"}
    for profile in profiles:
        if not profile:
            continue
        for key, value in dict(profile).items():
            if key in list_keys:
                existing = [str(item) for item in merged.get(key, []) if str(item).strip()]
                for item in value or []:
                    cleaned = str(item).strip()
                    if cleaned and cleaned not in existing:
                        existing.append(cleaned)
                merged[key] = existing
            elif value not in (None, "", []):
                merged[key] = value
    return merged


def _derived_catalyst_profile(metadata: Mapping[str, Any]) -> Dict[str, Any]:
    symbol = str(metadata.get("symbol", "")).upper().strip()
    name = str(metadata.get("name", "")).strip()
    lowered_name = name.lower()
    sector = str(metadata.get("sector", "综合")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    chain_text = " ".join(chain_nodes).lower()

    if any(token in lowered_name for token in ("nasdaq", "纳斯达克", "纳指")) or symbol.startswith("QQQ"):
        profile = _merge_catalyst_profiles(
            GENERIC_CATALYST_PROFILES["科技"],
            {
                "profile_name": "纳斯达克",
                "themes": ["纳斯达克", "美股科技"],
                "keywords": ["nasdaq", "纳斯达克", "纳指", "big tech", "megacap"],
                "overseas_leaders": ["Microsoft", "Apple", "NVIDIA", "Amazon", "Meta", "Alphabet", "Tesla"],
                "event_keywords": ["earnings", "guidance", "capex", "AI", "财报", "指引", "资本开支"],
            },
        )
        profile["sector_hint"] = "科技"
        return profile

    if any(token in lowered_name for token in ("恒生科技", "港股科技", "hstech")) or symbol == "HSTECH":
        profile = _merge_catalyst_profiles(
            GENERIC_CATALYST_PROFILES["科技"],
            {
                "profile_name": "港股科技",
                "themes": ["港股科技", "恒生科技"],
                "keywords": ["港股科技", "恒生科技", "internet", "platform", "消费互联网"],
                "domestic_leaders": ["腾讯", "阿里巴巴", "小米", "美团", "京东", "快手", "百度"],
                "overseas_leaders": ["Tencent", "Alibaba", "Xiaomi", "Meituan", "JD"],
            },
        )
        profile["sector_hint"] = "科技"
        return profile

    if sector in GENERIC_CATALYST_PROFILES:
        profile = dict(GENERIC_CATALYST_PROFILES[sector])
        profile["profile_name"] = sector
        return profile

    if "成长股估值修复" in chain_text or "ai算力" in chain_text:
        profile = dict(GENERIC_CATALYST_PROFILES["科技"])
        profile["profile_name"] = "科技"
        return profile

    profile = dict(GENERIC_CATALYST_PROFILES["宽基"])
    profile["profile_name"] = "宽基"
    return profile


def _catalyst_keywords(metadata: Mapping[str, Any]) -> List[str]:
    name = str(metadata.get("name", "")).strip()
    sector = str(metadata.get("sector", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    if "半导体" in name or "芯片" in name or "半导体" in chain_nodes:
        return ["半导体", "芯片", "存储", "semiconductor", "chip", "foundry", "fab", "tsmc", "台积电", "micron", "美光", "hynix", "海力士", "gpu", "capex", "涨价", "drAM", "nand"]
    if sector == "电网":
        return ["电网", "电力", "特高压", "智能电网", "grid", "utility"]
    if sector == "黄金":
        return ["黄金", "gold", "bullion", "央行", "central bank"]
    if sector == "有色":
        return ["有色", "铜", "铝", "copper", "aluminum", "metal"]
    if sector == "能源":
        return ["原油", "oil", "gas", "能源", "opec", "lng"]
    if sector == "军工":
        return ["军工", "国防", "defense", "aerospace", "军贸", "导弹", "无人机", "卫星"]
    if sector == "高股息":
        return ["高股息", "红利", "dividend", "yield", "utility", "bank"]
    if "纳斯达克" in name or "纳指" in name:
        return ["nasdaq", "纳斯达克", "纳指", "big tech", "earnings", "guidance", "ai"]
    return _metadata_news_keys(metadata)


def _dedupe_news_items(items: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    deduped: List[Mapping[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        title = str(item.get("title", "")).strip()
        source = str(item.get("source", "")).strip()
        key = (title, source)
        if not title or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _sector_catalyst_categories(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> set[str]:
    profile_name = str(profile.get("profile_name", "")).strip()
    sector_hint = str(profile.get("sector_hint", "")).strip()
    sector = sector_hint or str(metadata.get("sector", "宽基")).strip()
    if profile_name in {"纳斯达克", "港股科技"}:
        return {"ai", "earnings", "semiconductor", "fed", "global_macro"}
    return set(CATALYST_CATEGORY_MAP.get(sector, CATALYST_CATEGORY_MAP["宽基"]))


def _catalyst_search_terms(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> List[str]:
    candidates = [
        str(metadata.get("name", "")).strip(),
        str(metadata.get("sector", "")).strip(),
        *[str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()],
        *[str(item).strip() for item in profile.get("keywords", [])[:4]],
        *[str(item).strip() for item in profile.get("domestic_leaders", [])[:2]],
        *[str(item).strip() for item in profile.get("overseas_leaders", [])[:2]],
        *[str(item).strip() for item in profile.get("event_keywords", [])[:2]],
    ]
    cleaned: List[str] = []
    for item in candidates:
        if not item or item == "综合":
            continue
        if item not in cleaned:
            cleaned.append(item)
    return cleaned[:8]


def _strict_relevance_tokens(profile: Mapping[str, Any], tokens: Sequence[str]) -> List[str]:
    profile_name = str(profile.get("profile_name", "")).strip()
    if profile_name not in {"纳斯达克", "港股科技"}:
        return [str(token).strip() for token in tokens if str(token).strip()]

    noisy = {
        "ai",
        "科技",
        "technology",
        "cloud",
        "software",
        "算力",
        "big tech",
        "growth",
        "成长股估值修复",
        "人工智能",
    }
    cleaned: List[str] = []
    for token in tokens:
        value = str(token).strip()
        if not value or value.lower() in noisy:
            continue
        if value not in cleaned:
            cleaned.append(value)
    return cleaned


def _preferred_catalyst_sources(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> List[str]:
    region = str(metadata.get("region", "")).upper().strip()
    profile_name = str(profile.get("profile_name", "")).strip()
    if region == "US" or profile_name == "纳斯达克":
        return ["Reuters", "Bloomberg", "Financial Times"]
    if region == "HK" or profile_name == "港股科技":
        return ["Reuters", "Bloomberg", "Financial Times", "财联社", "证券时报"]
    return ["财联社", "证券时报", "Reuters", "Bloomberg"]


def _category_item_is_relevant(
    item: Mapping[str, Any],
    metadata: Mapping[str, Any],
    profile: Mapping[str, Any],
    allowed_categories: set[str],
    related_tokens: Sequence[str],
    strict_tokens: Sequence[str],
) -> bool:
    category = str(item.get("category", "")).lower()
    source = str(item.get("source", "")).strip().lower()
    if category not in allowed_categories:
        return False
    text = _headline_text(item)
    if _contains_any(text, related_tokens):
        if str(profile.get("profile_name", "")).strip() in {"纳斯达克", "港股科技"} and source not in {"reuters", "bloomberg", "financial times", "ft"}:
            return _contains_any(text, strict_tokens)
        return True

    profile_name = str(profile.get("profile_name", "")).strip()
    foreign_sources = {"reuters", "bloomberg", "financial times", "ft"}
    macro_profiles = {"纳斯达克", "港股科技", "黄金", "高股息", "宽基"}
    if profile_name in macro_profiles and source in foreign_sources:
        return True
    return False


def _headline_text(item: Mapping[str, Any]) -> str:
    return " ".join(
        [
            str(item.get("title", "")),
            str(item.get("category", "")),
            str(item.get("source", "")),
        ]
    ).lower()


def _contains_any(text: str, keywords: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(str(keyword).lower() in lowered for keyword in keywords if str(keyword).strip())


def _title_source_text(item: Mapping[str, Any]) -> str:
    return " ".join([str(item.get("title", "")), str(item.get("source", ""))]).lower()


def _pick_best_news_item(
    items: Sequence[Mapping[str, Any]],
    primary_keywords: Sequence[str],
    bonus_keywords: Sequence[str],
) -> Optional[Mapping[str, Any]]:
    if not items:
        return None

    def _score(item: Mapping[str, Any]) -> tuple[int, int]:
        text = _headline_text(item)
        primary = sum(1 for keyword in primary_keywords if str(keyword).strip() and str(keyword).lower() in text)
        bonus = sum(1 for keyword in bonus_keywords if str(keyword).strip() and str(keyword).lower() in text)
        return (primary, bonus)

    return max(items, key=_score)


def _find_related_news(items: Sequence[Mapping[str, Any]], metadata: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    keys = _metadata_news_keys(metadata)
    related: List[Mapping[str, Any]] = []
    for item in items:
        text = " ".join(
            [
                str(item.get("title", "")),
                str(item.get("category", "")),
                str(item.get("source", "")),
            ]
        ).lower()
        if any(key in text for key in keys):
            related.append(item)
    return related


def _regime_assets(regime: Mapping[str, Any]) -> List[str]:
    return [str(item) for item in regime.get("preferred_assets", [])]


def _theme_alignment(metadata: Mapping[str, Any], day_theme: Mapping[str, str]) -> bool:
    sector = str(metadata.get("sector", ""))
    label = str(day_theme.get("label", ""))
    if "能源" in label or "地缘" in label:
        return sector in {"能源", "电网", "黄金", "高股息"}
    if "利率" in label:
        return sector in {"科技", "消费"}
    if "政策" in label:
        return sector in {"电网", "高股息", "消费"}
    if "AI" in label or "半导体" in label:
        return sector in {"科技"}
    return False


def _current_factor_state(context: Mapping[str, Any]) -> Dict[str, int]:
    monitor = _monitor_map(context.get("monitor_rows", []))
    oil = 1 if float(monitor.get("布伦特原油", {}).get("return_5d", 0.0)) > 0.05 else -1
    rate = -1 if float(monitor.get("美国10Y收益率", {}).get("return_5d", 0.0)) < 0 else 1
    usd = -1 if float(monitor.get("美元指数", {}).get("return_20d", 0.0)) < 0 else 1
    cny = 1 if float(monitor.get("USDCNY", {}).get("return_20d", 0.0)) < 0 else -1
    return {"rate": rate, "usd": usd, "oil": oil, "cny": cny}


def _hard_checks(
    metadata: Mapping[str, Any],
    history: pd.DataFrame,
    metrics: Mapping[str, float],
    technical: Mapping[str, Any],
    context: Mapping[str, Any],
    macro_score: Optional[int],
    correlation_pair: Optional[tuple[str, float]],
    fundamental_dimension: Mapping[str, Any],
) -> tuple[List[Dict[str, str]], List[str], List[str]]:
    checks: List[Dict[str, str]] = []
    exclusion_reasons: List[str] = []
    warnings: List[str] = []
    opportunity_cfg = dict(context.get("config", {})).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("min_turnover", 50_000_000))
    min_history_days = int(opportunity_cfg.get("min_listing_days", 60))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))

    liquidity_ok = float(metrics.get("avg_turnover_20d", 0.0)) >= min_turnover
    checks.append({"name": "流动性", "status": "✅" if liquidity_ok else "❌", "detail": f"日均成交 {metrics.get('avg_turnover_20d', 0.0) / 1e8:.2f} 亿"})
    if not liquidity_ok:
        exclusion_reasons.append("日均成交额低于 5000 万")

    listed_ok = len(history) >= min_history_days
    checks.append({"name": "上市时长", "status": "✅" if listed_ok else "❌", "detail": f"有效历史样本 {len(history)} 个交易日"})
    if not listed_ok:
        exclusion_reasons.append("上市不满 60 个交易日")

    checks.append({"name": "基本面底线", "status": "ℹ️", "detail": "当前以 ETF / 行业代理为主，利润同比底线暂未接入原始财报数据"})
    valuation_snapshot = dict(fundamental_dimension.get("valuation_snapshot") or {})
    valuation_extreme = bool(fundamental_dimension.get("valuation_extreme"))
    pe_ttm = valuation_snapshot.get("pe_ttm")
    if valuation_extreme and pe_ttm is not None:
        checks.append(
            {
                "name": "估值极端",
                "status": "⚠️",
                "detail": f"{valuation_snapshot.get('index_name', '相关指数')} 滚动PE {float(pe_ttm):.1f}x，已进入极高估值区",
            }
        )
        exclusion_reasons.append("真实指数估值处于极高区间")
        warnings.append("⚠️ 真实指数估值已处于极高区间，后续更需要靠盈利兑现来消化估值")
    elif price_percentile > 0.90:
        checks.append({"name": "估值极端", "status": "⚠️", "detail": f"价格位置代理分位 {price_percentile:.0%}，接近极端高位"})
        exclusion_reasons.append("价格位置代理已处于极端高位")
        warnings.append("⚠️ 价格位置已在高位区，追高性价比明显下降")
    else:
        checks.append({"name": "估值极端", "status": "✅", "detail": f"价格位置代理分位 {price_percentile:.0%}"})

    checks.append({"name": "解禁压力", "status": "ℹ️", "detail": "解禁日历尚未接入，当前不纳入硬性排除"})

    return_5d = float(metrics.get("return_5d", 0.0))
    close_returns = history["close"].pct_change().dropna()
    limit_down_like = bool(len(close_returns) >= 2 and (close_returns.tail(2) <= -0.095).all())
    trend_ok = return_5d > -0.20 and not limit_down_like
    checks.append({"name": "趋势崩坏", "status": "✅" if trend_ok else "❌", "detail": f"近 5 日 {format_pct(return_5d)}"})
    if not trend_ok:
        exclusion_reasons.append("近 5 日跌幅过大或出现连续跌停式崩坏")

    if correlation_pair:
        peer, corr = correlation_pair
        diversified = abs(corr) <= 0.85
        checks.append({"name": "相关性", "status": "✅" if diversified else "❌", "detail": f"与 {peer} 相关性 {corr:.2f}"})
        if not diversified:
            exclusion_reasons.append(f"与 watchlist 中 {peer} 相关性过高")
    else:
        checks.append({"name": "相关性", "status": "ℹ️", "detail": "相关性代理暂缺，未用于排除"})

    macro_ok = macro_score is None or macro_score > 0
    checks.append({"name": "宏观顺逆风", "status": "✅" if macro_ok else "⚠️", "detail": "按宏观敏感度维度做顺逆风修正"})
    if macro_score == 0:
        exclusion_reasons.append("宏观敏感度完全逆风")
        warnings.append("⚠️ 当前宏观敏感度完全逆风，哪怕其他维度不差也不宜给高评级")

    if float(technical.get("rsi", {}).get("RSI", 0.0)) > 70:
        warnings.append("⚠️ 已进入超买区，追高性价比下降")
    return checks, exclusion_reasons, warnings


def _support_signals(history: pd.DataFrame, technical: Mapping[str, Any]) -> tuple[int, str]:
    close = history["close"].astype(float)
    price = float(close.iloc[-1])
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", price))
    fib = technical.get("fibonacci", {})
    fib_levels = fib.get("levels", {})
    hits: List[str] = []
    if ma60 > 0 and abs(price / ma60 - 1) <= 0.03:
        hits.append("MA60")
    for level_name in ("0.382", "0.500", "0.618"):
        level = float(fib_levels.get(level_name, 0.0))
        if level > 0 and abs(price / level - 1) <= 0.03:
            hits.append(f"斐波那契 {level_name}")
    recent_low = float(close.tail(20).min())
    if recent_low > 0 and abs(price / recent_low - 1) <= 0.05:
        hits.append("前低")
    score = 20 if len(hits) >= 2 else 15 if hits else 0
    detail = " / ".join(hits) if hits else "当前价格未明显贴近 MA60、前低或关键斐波那契支撑"
    return score, detail


def _technical_dimension(history: pd.DataFrame, technical: Mapping[str, Any]) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    macd = technical.get("macd", {})
    dif = float(macd.get("DIF", 0.0))
    dea = float(macd.get("DEA", 0.0))
    if dif > dea:
        awarded = 20 if dif > 0 else 10
        raw += awarded
        available += 20
        signal = "MACD 零轴上方金叉" if dif > 0 else "MACD 零轴下方金叉"
        factors.append(_factor_row("MACD 金叉", signal, awarded, 20, f"DIF {dif:.3f} / DEA {dea:.3f}"))
    else:
        available += 20
        factors.append(_factor_row("MACD 金叉", "MACD 未金叉", 0, 20, f"DIF {dif:.3f} / DEA {dea:.3f}"))

    adx = float(technical.get("dmi", {}).get("ADX", 0.0))
    adx_award = 20 if adx > 35 else 15 if adx > 25 else 0
    raw += adx_award
    available += 20
    factors.append(_factor_row("ADX", f"ADX {adx:.1f}", adx_award, 20, "趋势强度越高，越接近单边趋势"))

    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    if 30 <= rsi <= 50:
        rsi_award = 15
    elif rsi < 30:
        rsi_award = 10
    elif rsi <= 65:
        rsi_award = 8
    else:
        rsi_award = 0
    raw += rsi_award
    available += 15
    factors.append(_factor_row("RSI 位置", f"RSI {rsi:.1f}", rsi_award, 15, "30-50 更像回调未崩的理想区间"))

    support_award, support_detail = _support_signals(history, technical)
    raw += support_award
    available += 20
    factors.append(_factor_row("支撑位", support_detail, support_award, 20, "优先看前低 / MA60 / 斐波那契 0.382-0.618"))

    patterns = list(technical.get("candlestick", []) or [])
    bullish_patterns = [item for item in patterns if item in {"hammer", "inverted_hammer", "marubozu"}]
    candle_award = 10 if bullish_patterns else 0
    raw += candle_award
    available += 10
    factors.append(_factor_row("K线形态", " / ".join(bullish_patterns) if bullish_patterns else "无明显看涨形态", candle_award, 10, "当前只识别最近一根 K 线"))

    vol_ratio = float(technical.get("volume", {}).get("vol_ratio", 1.0))
    latest_return = float(history["close"].pct_change().iloc[-1]) if len(history) > 1 else 0.0
    volume_award = 10 if vol_ratio < 0.7 and latest_return <= 0 else 5 if 0.7 <= vol_ratio <= 1.2 else 0
    raw += volume_award
    available += 10
    factors.append(_factor_row("量能", f"量比 {vol_ratio:.2f}", volume_award, 10, "缩量回调更像抛压衰减，放量上冲更适合确认趋势"))

    ma = technical.get("ma_system", {})
    ma_values = ma.get("mas", {})
    ma5 = float(ma_values.get("MA5", 0.0))
    ma20 = float(ma_values.get("MA20", 0.0))
    ma60 = float(ma_values.get("MA60", 0.0))
    ma_award = 10 if ma5 > ma20 > ma60 else 0
    raw += ma_award
    available += 10
    factors.append(_factor_row("均线", f"MA5 {ma5:.3f} / MA20 {ma20:.3f} / MA60 {ma60:.3f}", ma_award, 10, "多头排列代表中期趋势向上"))

    boll_signal = str(technical.get("bollinger", {}).get("signal", "neutral"))
    boll_award = 5 if boll_signal in {"near_lower", "neutral"} else 0
    raw += boll_award
    available += 5
    factors.append(_factor_row("布林带", boll_signal, boll_award, 5, "中轨或下轨附近更适合观察回调后的承接"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "技术面",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "价格结构到位，技术信号共振较强。", "技术面有亮点，但还没有形成满配共振。", "技术结构仍偏弱，暂不支持激进介入。", "ℹ️ 技术面数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _fundamental_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    metrics: Mapping[str, float],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    valuation_snapshot: Optional[Dict[str, Any]] = None
    valuation_note = f"近一年价格分位 {price_percentile:.0%}，这只反映位置，不等于真实估值分位。"
    valuation_history = pd.DataFrame()
    financial_proxy: Dict[str, Any] = {}
    sector_flow = {}
    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        collector = ValuationCollector(config)
        try:
            valuation_snapshot = collector.get_cn_index_snapshot(_valuation_keywords(metadata))
        except Exception:
            valuation_snapshot = None
        if valuation_snapshot:
            try:
                valuation_history = collector.get_cn_index_value_history(str(valuation_snapshot.get("index_code", "")))
            except Exception:
                valuation_history = pd.DataFrame()
            try:
                financial_proxy = collector.get_cn_index_financial_proxies(str(valuation_snapshot.get("index_code", "")), top_n=5)
            except Exception:
                financial_proxy = {}
        try:
            sector_flow = _sector_flow_snapshot(metadata, MarketDriversCollector(config).collect())
        except Exception:
            sector_flow = {}

    pe_ttm = None if not valuation_snapshot else valuation_snapshot.get("pe_ttm")
    pe_percentile = None
    dividend_yield = None
    if not valuation_history.empty:
        pe_col = _first_column(valuation_history, ("市盈率2", "滚动市盈率", "市盈率", "PE滚动"))
        dividend_col = _first_column(valuation_history, ("股息率2", "股息率", "股息率(%)"))
        if pe_col:
            pe_series = pd.to_numeric(valuation_history[pe_col], errors="coerce").dropna()
            if not pe_series.empty and pe_ttm is not None:
                pe_percentile = float((pe_series <= float(pe_ttm)).mean())
        if dividend_col:
            dividend_series = pd.to_numeric(valuation_history[dividend_col], errors="coerce").dropna()
            if not dividend_series.empty:
                dividend_yield = float(dividend_series.iloc[-1])

    if pe_ttm is not None:
        pe_value = float(pe_ttm)
        pe_award = 25 if pe_percentile is not None and pe_percentile < 0.30 else 10 if pe_percentile is not None and pe_percentile < 0.50 else 10 if pe_value < 20 else 0
        raw += pe_award
        available += 25
        detail = "当前接入的是相关指数滚动 PE；价格位置另算，不与估值分位混用。"
        if pe_percentile is not None:
            detail += f" 近样本 PE 分位约 {pe_percentile:.0%}。"
        if dividend_yield is not None:
            detail += f" 当前股息率约 {dividend_yield:.2f}%。"
        factors.append(
            _factor_row(
                "真实指数估值",
                f"{valuation_snapshot.get('index_name', '相关指数')} PE {pe_value:.1f}x" + (f" / 分位 {pe_percentile:.0%}" if pe_percentile is not None else ""),
                pe_award,
                25,
                detail,
            )
        )
    else:
        percentile_award = 25 if price_percentile < 0.30 else 10 if price_percentile < 0.50 else 0
        raw += percentile_award
        available += 25
        factors.append(
            _factor_row(
                "估值代理分位",
                f"价格位置代理 {price_percentile:.0%}",
                percentile_award,
                25,
                "当前未接入真实指数估值，只能用价格位置代理；价格分位不等于真实估值分位。",
            )
        )
    factors.append(
        _factor_row(
            "价格位置",
            f"近一年价格分位 {price_percentile:.0%}",
            0,
            0,
            "这项只回答‘位置高不高’，不回答‘估值贵不贵’。",
            display_score="信息项",
        )
    )

    revenue_yoy = financial_proxy.get("revenue_yoy")
    if revenue_yoy is None:
        revenue_yoy = financial_proxy.get("profit_yoy")
    if revenue_yoy is not None:
        revenue_award = 20 if float(revenue_yoy) >= 20 else 15 if float(revenue_yoy) >= 10 else 8 if float(revenue_yoy) >= 5 else 0
        raw += revenue_award
        available += 20
        factors.append(
            _factor_row(
                "盈利增速",
                f"前五大成分股加权增速代理 {float(revenue_yoy):.1f}%",
                revenue_award,
                20,
                f"当前优先用前五大成分股营收同比，缺失时回退到利润同比；覆盖权重约 {financial_proxy.get('coverage_weight', 0.0):.1f}%。",
            )
        )
    else:
        factors.append(_factor_row("盈利增速", "缺失", None, 20, "当前未接入对应指数或行业的营收同比代理"))

    roe_value = financial_proxy.get("roe")
    if roe_value is not None:
        roe_award = 20 if float(roe_value) >= 15 else 10 if float(roe_value) >= 10 else 0
        raw += roe_award
        available += 20
        factors.append(
            _factor_row(
                "ROE",
                f"前五大成分股加权 ROE {float(roe_value):.1f}%",
                roe_award,
                20,
                f"财务代理最新报告期 {financial_proxy.get('report_date') or '未知'}。",
            )
        )
    else:
        factors.append(_factor_row("ROE", "缺失", None, 20, "当前未接入对应指数或行业的 ROE 代理"))

    gross_margin = financial_proxy.get("gross_margin")
    if gross_margin is not None:
        margin_award = 15 if float(gross_margin) >= 30 else 10 if float(gross_margin) >= 20 else 0
        raw += margin_award
        available += 15
        factors.append(
            _factor_row(
                "毛利率",
                f"前五大成分股加权毛利率 {float(gross_margin):.1f}%",
                margin_award,
                15,
                "用成分股加权毛利率代理行业定价权和成本结构。",
            )
        )
    else:
        factors.append(_factor_row("毛利率", "缺失", None, 15, "当前未接入对应行业毛利率代理"))

    profit_yoy = financial_proxy.get("profit_yoy")
    growth_base = None
    if profit_yoy is not None and float(profit_yoy) > 0:
        growth_base = float(profit_yoy)
    elif revenue_yoy is not None and float(revenue_yoy) > 0:
        growth_base = float(revenue_yoy)
    peg_value = float(pe_ttm) / growth_base if pe_ttm is not None and growth_base and growth_base > 0 else None
    if peg_value is not None:
        peg_award = 10 if peg_value < 1 else 5 if peg_value < 1.5 else 0
        raw += peg_award
        available += 10
        factors.append(
            _factor_row(
                "PEG 代理",
                f"PEG 约 {peg_value:.2f}",
                peg_award,
                10,
                "用真实指数 PE 除以前五大成分股加权增速代理，回答‘增长是否已经被定价’。",
            )
        )
    else:
        factors.append(_factor_row("PEG 代理", "缺失", None, 10, "缺少稳定的盈利增速代理，未计算 PEG"))

    flow_award: Optional[int] = None
    flow_detail = "ETF 份额 / 行业资金流代理暂缺"
    flow_signal = "ETF 份额 / 资金流向"
    if asset_type == "cn_etf":
        try:
            fund_flow = ChinaMarketCollector(config).get_etf_fund_flow(symbol)
            flow_series = pd.to_numeric(fund_flow.get("净流入", pd.Series(dtype=float)), errors="coerce").dropna()
            if flow_series.empty and "净申购份额" in fund_flow.columns:
                flow_series = pd.to_numeric(fund_flow["净申购份额"], errors="coerce").dropna()
            if not flow_series.empty:
                positive_days = int((flow_series.tail(5) > 0).sum())
                flow_award = 10 if positive_days >= 3 else 0
                flow_detail = f"近 5 个可用样本中 {positive_days} 个为净流入/净申购"
                flow_signal = "ETF 份额近 5 个样本有承接" if flow_award else "ETF 流入承接不稳"
        except Exception as exc:
            flow_detail = f"ETF 份额数据缺失: {exc}"
    if flow_award is None and sector_flow:
        main_flow = sector_flow.get("main_flow")
        main_ratio = sector_flow.get("main_ratio")
        flow_award = 10 if main_flow is not None and float(main_flow) > 0 else 0
        flow_signal = f"{sector_flow.get('name') or metadata.get('sector', '行业')} 主力净{'流入' if (main_flow or 0) > 0 else '流出'} {_fmt_yi_number(main_flow)}"
        flow_detail = (
            f"ETF 份额缺失，改用行业资金流代理；主力净占比 "
            f"{format_pct(float(main_ratio) / 100) if main_ratio is not None and abs(float(main_ratio)) > 1 else format_pct(float(main_ratio)) if main_ratio is not None else '缺失'}。"
        )
    factors.append(_factor_row("资金承接", flow_signal, flow_award, 10, flow_detail))
    if flow_award is not None:
        raw += flow_award
        available += 10

    score = _normalize_dimension(raw, available, 100)
    summary = _dimension_summary(
        score,
        "估值/资金承接代理偏正面，但当前仍是 ETF/行业代理视角。",
        "基本面代理没有明显便宜或显著昂贵结论。",
        "估值代理偏高，基本面安全边际不足。",
        "ℹ️ 基本面数据缺失，本次评级未纳入完整基本面维度",
    )
    if score is not None and available < 35:
        summary += " 当前仅基于代理因子归一化评分。"
    if valuation_snapshot and pe_ttm is not None:
        summary += f" 当前已接入 `{valuation_snapshot.get('index_name', '')}` 滚动 PE {float(pe_ttm):.1f}x；{valuation_note}"
    else:
        summary += f" {valuation_note}"
    return {
        "name": "基本面",
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
        "available_max": available,
        "valuation_snapshot": valuation_snapshot,
        "valuation_history": valuation_history,
        "financial_proxy": financial_proxy,
        "pe_percentile": pe_percentile,
        "price_percentile": price_percentile,
        "valuation_note": valuation_note,
        "valuation_extreme": bool(pe_ttm is not None and float(pe_ttm) >= 60),
    }


def _catalyst_dimension(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    config = dict(context.get("config", {}))
    profile = _catalyst_profile(metadata, config)
    news_items = context.get("news_report", {}).get("all_items") or context.get("news_report", {}).get("items", [])
    sector = str(metadata.get("sector", ""))
    keyword_keys = _metadata_news_keys(metadata)
    catalyst_keys = list(dict.fromkeys([*_catalyst_keywords(metadata), *[str(item) for item in profile.get("keywords", [])]]))
    profile_policy_keys = [str(item) for item in profile.get("policy_keywords", [])]
    policy_keys = list(dict.fromkeys(profile_policy_keys + ["政策", "规划", "方案", "两会", "国常会", "会议", "stimulus", "plan", "subsid"]))
    domestic_leaders = [str(item) for item in profile.get("domestic_leaders", [])]
    overseas_leaders = [str(item) for item in profile.get("overseas_leaders", [])]
    earnings_keys = [str(item) for item in profile.get("earnings_keywords", [])]
    event_keys = list(dict.fromkeys([str(item) for item in profile.get("event_keywords", [])] + earnings_keys))
    strict_event_keys = _strict_relevance_tokens(profile, event_keys)
    broad_catalyst_keys = [*catalyst_keys, *profile_policy_keys, *domestic_leaders, *overseas_leaders, *event_keys]
    strict_related_news = [item for item in news_items if _contains_any(_headline_text(item), broad_catalyst_keys)]
    related_tokens = [*keyword_keys, *catalyst_keys, *profile_policy_keys, *domestic_leaders, *overseas_leaders]
    strict_tokens = _strict_relevance_tokens(profile, related_tokens)
    allowed_categories = _sector_catalyst_categories(metadata, profile)
    category_related_news = [
        item
        for item in news_items
        if _category_item_is_relevant(item, metadata, profile, allowed_categories, related_tokens, strict_tokens)
    ]
    dynamic_related_news: List[Mapping[str, Any]] = []
    if len(strict_related_news) + len(category_related_news) < 2:
        try:
            dynamic_related_news = NewsCollector(config).search_by_keywords(
                _catalyst_search_terms(metadata, profile),
                preferred_sources=_preferred_catalyst_sources(metadata, profile),
                limit=6,
                recent_days=7,
            )
        except Exception:
            dynamic_related_news = []
    news_pool = _dedupe_news_items([*strict_related_news, *category_related_news, *dynamic_related_news])
    related_events = []
    for event in context.get("events", []):
        text = f"{event.get('title', '')} {event.get('note', '')}"
        if _contains_any(text, [*keyword_keys, *event_keys, *domestic_leaders, *overseas_leaders]):
            related_events.append(event)

    policy_items = [
        item
        for item in news_pool
        if (
            str(item.get("category", "")).lower() in {"china_macro", "china_macro_domestic"}
            or str(item.get("source", "")).strip() in {"财联社", "证券时报", "Reuters"}
        )
        and _contains_any(_headline_text(item), policy_keys)
        and _contains_any(_headline_text(item), strict_tokens)
    ]
    policy_pick = _pick_best_news_item(policy_items, policy_keys, keyword_keys)
    policy_award = 30 if policy_items else 0
    raw += policy_award
    available += 30
    factors.append(_factor_row("政策催化", policy_pick["title"] if policy_pick else "近 7 日未命中直接政策催化", policy_award, 30, "政策原文和一级媒体优先"))

    leader_items = [
        item
        for item in news_pool
        if (
            str(item.get("category", "")).lower() in {"china_market_domestic", "earnings", "semiconductor"}
            and _contains_any(_headline_text(item), domestic_leaders)
        )
        or (
            str(item.get("source", "")).strip() in {"财联社", "证券时报"}
            and _contains_any(_headline_text(item), catalyst_keys)
            and _contains_any(_headline_text(item), [*strict_tokens, *strict_event_keys, "订单", "扩产", "投产", "回购", "并购", "重组", "指引", "扩建", "量产", "涨价"])
        )
    ]
    leader_pick = _pick_best_news_item(leader_items, [*domestic_leaders, *strict_event_keys], keyword_keys)
    leader_award = 25 if leader_items else 0
    raw += leader_award
    available += 25
    factors.append(_factor_row("龙头公告/业绩", leader_pick["title"] if leader_pick else "未命中直接龙头公告", leader_award, 25, "优先看订单、扩产、回购、并购或超预期业绩"))

    overseas_keyword_map = {
        "科技": ["tsmc", "台积电", "nvidia", "英伟达", "micron", "美光", "hynix", "海力士", "asml", "broadcom", "amd", "gpu", "semiconductor", "foundry", "fab", "capex"],
        "黄金": ["gold", "bullion", "central bank", "央行"],
        "电网": ["power grid", "utility", "electricity", "特高压"],
        "能源": ["opec", "crude", "lng", "oil", "天然气"],
        "有色": ["copper", "aluminum", "metal", "mining", "铜", "铝"],
    }
    foreign_sources = {"reuters", "bloomberg", "financial times", "ft"}
    overseas_items = [
        item
        for item in news_pool
        if (
            str(item.get("category", "")).lower() in {"earnings", "ai", "semiconductor", "fed"}
            and str(item.get("source", "")).strip().lower() in foreign_sources
            and _contains_any(_title_source_text(item), overseas_leaders)
            and _contains_any(_title_source_text(item), [*earnings_keys, *strict_event_keys])
        )
        or (
            str(item.get("source", "")).strip().lower() in foreign_sources
            and _contains_any(_title_source_text(item), overseas_leaders)
            and _contains_any(_title_source_text(item), [*earnings_keys, *strict_event_keys, *overseas_keyword_map.get(sector, [])])
        )
    ]
    overseas_pick = _pick_best_news_item(overseas_items, [*overseas_leaders, *earnings_keys, *strict_event_keys], [*keyword_keys, *overseas_keyword_map.get(sector, [])])
    overseas_award = 20 if overseas_items else 0
    raw += overseas_award
    available += 20
    factors.append(_factor_row("海外映射", overseas_pick["title"] if overseas_pick else "未命中直接海外映射", overseas_award, 20, "重点看海外龙头财报/指引或模型产品催化"))

    density_award = 10 if len(news_pool) >= 3 else 0
    raw += density_award
    available += 10
    factors.append(_factor_row("研报/新闻密度", f"相关头条 {len(news_pool)} 条", density_award, 10, "当前用一级媒体新闻密度代理"))

    source_count = len({str(item.get("source", "")) for item in news_pool if item.get("source")})
    heat_award = 10 if source_count >= 2 else 0
    raw += heat_award
    available += 10
    factors.append(_factor_row("新闻热度", f"覆盖源 {source_count} 个", heat_award, 10, "从少量提及到多源同步，是热度拐点的代理"))

    forward_award = 5 if related_events else 0
    raw += forward_award
    available += 5
    factors.append(_factor_row("前瞻催化", related_events[0]["title"] if related_events else "未来 14 日未命中直接催化事件", forward_award, 5, "当前用本地事件日历代理"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "催化面",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "催化明确，市场有理由重新定价。", "有催化苗头，但强度还不够形成一致预期。", "催化不足，当前更像静态博弈。", "ℹ️ 催化面数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
        "profile_name": profile.get("profile_name", sector),
    }


def _correlation_to_watchlist(symbol: str, asset_returns: pd.Series, context: Mapping[str, Any]) -> Optional[tuple[str, float]]:
    best_symbol = ""
    best_corr = None
    for peer_symbol, peer_returns in dict(context.get("watchlist_returns", {})).items():
        if peer_symbol == symbol:
            continue
        aligned = pd.concat([asset_returns, peer_returns], axis=1, join="inner").dropna()
        if len(aligned) < 20:
            continue
        corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
        if best_corr is None or abs(corr) > abs(best_corr):
            best_symbol = peer_symbol
            best_corr = corr
    if best_corr is None:
        return None
    return best_symbol, best_corr


def _sector_board_match(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Optional[float]:
    sector = str(metadata.get("sector", ""))
    frame = drivers.get("industry_spot", pd.DataFrame()) if drivers else pd.DataFrame()
    if frame is None or frame.empty:
        return None
    name_col = "板块名称" if "板块名称" in frame.columns else "名称" if "名称" in frame.columns else None
    if name_col is None or "涨跌幅" not in frame.columns:
        return None
    matched = frame[frame[name_col].astype(str).str.contains(sector, na=False)]
    if matched.empty:
        return None
    return float(pd.to_numeric(matched.iloc[0]["涨跌幅"], errors="coerce")) / 100


def _relative_strength_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    metrics: Mapping[str, float],
    asset_returns: pd.Series,
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    benchmark_returns = context.get("benchmark_returns", {}).get(asset_type)
    rel_5d = None
    rel_20d = None
    if benchmark_returns is not None and not benchmark_returns.empty:
        bench_5d = float(benchmark_returns.tail(5).sum())
        bench_20d = float(benchmark_returns.tail(20).sum())
        rel_5d = float(metrics.get("return_5d", 0.0)) - bench_5d
        rel_20d = float(metrics.get("return_20d", 0.0)) - bench_20d
        turn_award = 30 if rel_5d > 0 and rel_20d <= 0 else 20 if rel_20d > 0 else 0
        raw += turn_award
        available += 30
        factors.append(_factor_row("超额拐点", f"相对基准 5日 {format_pct(rel_5d)} / 20日 {format_pct(rel_20d)}", turn_award, 30, "相对基准从负转正更接近轮动切换窗口"))
    else:
        factors.append(_factor_row("超额拐点", "缺失", None, 30, "基准收益序列缺失，未计算超额拐点"))

    board_move = _sector_board_match(metadata, context.get("drivers", {}))
    if board_move is not None:
        breadth_award = 25 if board_move > 0.01 else 10 if board_move > 0 else 0
        raw += breadth_award
        available += 25
        factors.append(_factor_row("板块扩散", f"板块涨跌幅 {format_pct(board_move)}", breadth_award, 25, "板块内部越普涨，越像轮动扩散"))
    else:
        factors.append(_factor_row("板块扩散", "缺失", None, 25, "板块扩散数据缺失"))

    chain_award = 20 if _theme_alignment(metadata, context.get("day_theme", {})) and float(metrics.get("return_5d", 0.0)) < 0.08 else 0
    raw += chain_award
    available += 20
    factors.append(_factor_row("产业链传导", "主线相关但自身尚未极端透支" if chain_award else "当前没有明显下一棒逻辑", chain_award, 20, "主线先启动上下游，再找尚未完全跟涨的方向"))

    preferred_assets = _regime_assets(context.get("regime", {}))
    preferred_key = {
        "电网": "电网基建",
        "科技": "成长股",
        "黄金": "黄金",
        "能源": "顺周期",
        "高股息": "高股息",
    }.get(str(metadata.get("sector", "")), "")
    regime_match = bool(preferred_key and preferred_key in preferred_assets)
    regime_award = 15 if regime_match or _theme_alignment(metadata, context.get("day_theme", {})) else 0
    raw += regime_award
    available += 15
    factors.append(_factor_row("Regime 适配", "与当前 regime / 主线方向一致" if regime_award else "当前 regime 对它没有额外加分", regime_award, 15, "大环境顺风时，轮动更容易持续"))

    ah_award = 10 if asset_type in {"hk", "hk_index"} and float(context.get("global_proxy", {}).get("dxy_20d_change", 0.0)) <= 0 else 0
    raw += ah_award
    available += 10
    factors.append(_factor_row("跨市场比价", "港股估值压力缓和" if ah_award else "该项不适用或暂无明显优势", ah_award, 10, "仅港股相关标的适用"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "相对强弱",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "轮动已经轮到它，具备主线扩散条件。", "相对强弱有改善，但还不是最典型的扩散点。", "轮动还没轮到它，更多是背景观察。", "ℹ️ 相对强弱数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _chips_dimension(symbol: str, asset_type: str, metadata: Mapping[str, Any], context: Mapping[str, Any], config: Mapping[str, Any]) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    drivers = dict(context.get("drivers", {}))
    sector_flow = _sector_flow_snapshot(metadata, drivers)
    northbound = _northbound_sector_snapshot(metadata, drivers)
    hot_rank = _hot_rank_snapshot(metadata, drivers)
    concentration_proxy: Dict[str, Any] = {}
    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        try:
            snapshot = ValuationCollector(config).get_cn_index_snapshot(_valuation_keywords(metadata))
            if snapshot:
                concentration_proxy = ValuationCollector(config).get_cn_index_financial_proxies(str(snapshot.get("index_code", "")), top_n=5)
        except Exception:
            concentration_proxy = {}

    heat_rank = hot_rank.get("rank")
    if heat_rank is not None:
        crowding_award = 30 if float(heat_rank) > 50 else 15 if float(heat_rank) > 20 else 0
        signal = f"热门度排名约 {int(float(heat_rank))}"
        detail = "当前用热门榜位置做公募/热度代理；排名越靠后，说明没那么拥挤。"
        raw += crowding_award
        available += 30
        factors.append(_factor_row("公募/热度代理", signal, crowding_award, 30, detail))
    elif sector_flow:
        crowding_award = 30 if float(sector_flow.get("main_flow") or 0.0) > 0 else 10
        raw += crowding_award
        available += 30
        factors.append(
            _factor_row(
                "公募/热度代理",
                f"{sector_flow.get('name') or metadata.get('sector', '行业')} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                crowding_award,
                30,
                "热门榜缺失时，改用行业主力资金流方向代理当前拥挤度与配置方向。",
            )
        )
    else:
        factors.append(_factor_row("公募/热度代理", "缺失", None, 30, "公募低配/热度代理暂缺"))

    factors.append(_factor_row("高管增持", "ETF / 指数产品不适用", 0, 0, "该因子主要适用于个股，不纳入 ETF 评分。", display_score="不适用"))

    if asset_type == "cn_etf":
        if northbound:
            north_value = northbound.get("net_value")
            north_award = 20 if north_value is not None and float(north_value) > 0 else 0
            raw += north_award
            available += 20
            factors.append(
                _factor_row(
                    "北向/南向",
                    f"{northbound.get('name') or metadata.get('sector', '板块')} 北向增持估计 {_fmt_yi_number(north_value)}",
                    north_award,
                    20,
                    "优先用行业/概念板块北向增持排行，而不是全市场总量。",
                )
            )
        else:
            try:
                flow = ChinaMarketCollector(config).get_north_south_flow()
                north = flow[flow["资金方向"].astype(str).str.contains("北向", na=False)] if "资金方向" in flow.columns else pd.DataFrame()
                value = float(pd.to_numeric(north.get("成交净买额", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not north.empty else 0.0
                north_award = 20 if value > 0 else 0
                raw += north_award
                available += 20
                factors.append(_factor_row("北向/南向", f"北向净买额约 {value:.2f} 亿", north_award, 20, "行业北向缺失，回退到全市场方向代理"))
            except Exception as exc:
                factors.append(_factor_row("北向/南向", "缺失", None, 20, f"北向/南向数据缺失: {exc}"))
    elif asset_type in {"hk", "hk_index"}:
        factors.append(_factor_row("北向/南向", "港股方向优先看南向", None, 20, "南向分项尚未稳定接入"))
    else:
        factors.append(_factor_row("北向/南向", "该项不适用", None, 20, "当前主要针对权益资产"))

    if asset_type == "cn_etf":
        chips_award: Optional[int] = None
        try:
            flow = ChinaMarketCollector(config).get_etf_fund_flow(symbol)
            series = pd.to_numeric(flow.get("净流入", pd.Series(dtype=float)), errors="coerce").dropna()
            if not series.empty:
                chips_award = 10 if float(series.tail(5).sum()) > 0 else 0
                raw += chips_award
                available += 10
                factors.append(_factor_row("机构资金承接", "ETF 近 5 个样本净流入为正" if chips_award else "ETF 流入没有持续为正", chips_award, 10, "用 ETF 资金流做筹码代理"))
        except Exception as exc:
            if sector_flow:
                chips_award = 10 if float(sector_flow.get("main_flow") or 0.0) > 0 else 0
                raw += chips_award
                available += 10
                factors.append(
                    _factor_row(
                        "机构资金承接",
                        f"{sector_flow.get('name') or metadata.get('sector', '行业')} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                        chips_award,
                        10,
                        f"ETF 流数据缺失，改用行业资金流代理: {exc}",
                    )
                )
            else:
                factors.append(_factor_row("机构资金承接", "缺失", None, 10, f"ETF 资金流数据缺失: {exc}"))
    else:
        factors.append(_factor_row("机构资金承接", "该项不适用", None, 10, "当前只对 A 股 ETF 接稳定资金流代理"))

    top_concentration = concentration_proxy.get("top_concentration")
    if top_concentration is not None:
        concentration_award = 15 if float(top_concentration) >= 35 else 8 if float(top_concentration) >= 25 else 0
        raw += concentration_award
        available += 15
        factors.append(
            _factor_row(
                "机构集中度代理",
                f"前五大成分股权重合计 {float(top_concentration):.1f}%",
                concentration_award,
                15,
                f"用指数前五大成分股权重集中度代理共识程度；财务覆盖权重约 {concentration_proxy.get('coverage_weight', 0.0):.1f}%。",
            )
        )
    else:
        factors.append(_factor_row("机构集中度代理", "缺失", None, 15, "成分股权重集中度暂未接入"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "筹码结构",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "聪明钱方向偏正面。", "筹码结构没有形成明确增量共识。", "聪明钱没有明显站在这一边。", "ℹ️ 筹码结构数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _drawdown_percentile(close: pd.Series) -> tuple[float, float]:
    rolling_max = close.cummax()
    drawdown = 1 - close / rolling_max
    current = float(drawdown.iloc[-1])
    percentile = float((drawdown <= current).mean())
    return current, percentile


def _volatility_percentile(close: pd.Series) -> tuple[float, float]:
    returns = close.pct_change().dropna()
    if len(returns) < 40:
        return 0.0, 0.5
    rolling = returns.rolling(20).std().dropna() * math.sqrt(252)
    if rolling.empty:
        return 0.0, 0.5
    current = float(rolling.iloc[-1])
    percentile = float((rolling <= current).mean())
    return current, percentile


def _downside_beta(asset_returns: pd.Series, benchmark_returns: Optional[pd.Series]) -> Optional[float]:
    if benchmark_returns is None or benchmark_returns.empty:
        return None
    aligned = pd.concat([asset_returns, benchmark_returns], axis=1, join="inner").dropna()
    if len(aligned) < 40:
        return None
    downside = aligned[aligned.iloc[:, 1] < 0]
    if len(downside) < 20:
        return None
    variance = float(downside.iloc[:, 1].var())
    if variance == 0:
        return None
    covariance = float(np.cov(downside.iloc[:, 0], downside.iloc[:, 1])[0][1])
    return covariance / variance


def _risk_dimension(symbol: str, asset_type: str, history: pd.DataFrame, asset_returns: pd.Series, context: Mapping[str, Any], correlation_pair: Optional[tuple[str, float]]) -> Dict[str, Any]:
    close = history["close"].astype(float)
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0

    current_dd, dd_pct = _drawdown_percentile(close)
    dd_award = 30 if dd_pct >= 0.70 else 15 if dd_pct >= 0.50 else 0
    raw += dd_award
    available += 30
    factors.append(_factor_row("回撤分位", f"当前回撤 {current_dd:.1%}，历史分位 {dd_pct:.0%}", dd_award, 30, "跌得越充分，风险释放通常越充分"))

    vol, vol_pct = _volatility_percentile(close)
    vol_award = 25 if vol_pct < 0.30 else 10 if vol_pct < 0.50 else 0
    raw += vol_award
    available += 25
    factors.append(_factor_row("波动率", f"20 日年化波动 {vol:.1%}，分位 {vol_pct:.0%}", vol_award, 25, "低波动更像启动前的平静"))

    beta = _downside_beta(asset_returns, context.get("benchmark_returns", {}).get(asset_type))
    if beta is None:
        factors.append(_factor_row("下行 beta", "缺失", None, 20, "基准收益序列不足，未计算 downside beta"))
    else:
        beta_award = 20 if beta < 0.8 else 10 if beta < 1.0 else 0
        raw += beta_award
        available += 20
        factors.append(_factor_row("下行 beta", f"下行 beta {beta:.2f}", beta_award, 20, "大盘跌时它跌得少，风控价值更高"))

    recovery_days = 0
    if len(close) >= 120:
        trailing = close.tail(120).reset_index(drop=True)
        peak = float(trailing.cummax().iloc[-1])
        trough_idx = int((trailing / trailing.cummax() - 1).idxmin())
        if trough_idx < len(trailing) - 1:
            recover = trailing.iloc[trough_idx:]
            recovered = recover[recover >= peak * 0.95]
            recovery_days = int(recovered.index[0] - trough_idx) if not recovered.empty else 999
    recovery_award = 15 if recovery_days and recovery_days < 60 else 0
    raw += recovery_award
    available += 15
    factors.append(_factor_row("回撤恢复", f"近似恢复速度 {recovery_days if recovery_days else '未知'} 日", recovery_award, 15, "恢复越快，韧性越好"))

    if correlation_pair is None:
        factors.append(_factor_row("组合分散", "缺失", None, 10, "watchlist 相关性序列不足"))
    else:
        peer, corr = correlation_pair
        div_award = 10 if abs(corr) < 0.5 else 5 if abs(corr) < 0.75 else 0
        raw += div_award
        available += 10
        factors.append(_factor_row("组合分散", f"与 {peer} 相关性 {corr:.2f}", div_award, 10, "相关性越低，越有真正分散价值"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "风险特征",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "下行空间和组合风险都还可控。", "风险可控但没有特别便宜。", "风险收益比不占优，需更严控节奏。", "ℹ️ 风险特征数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _seasonality_dimension(metadata: Mapping[str, Any], history: pd.DataFrame, context: Mapping[str, Any]) -> Dict[str, Any]:
    dated_history = history.copy()
    dated_history["date"] = pd.to_datetime(dated_history["date"], errors="coerce")
    dated_history = dated_history.dropna(subset=["date"]).set_index("date").sort_index()
    close = dated_history["close"].astype(float)
    monthly = close.resample("ME").last().pct_change().dropna()
    month = datetime.now().month
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0

    if not monthly.empty:
        month_series = monthly[monthly.index.month == month]
        if not month_series.empty:
            win_rate = float((month_series > 0).mean())
            month_award = 30 if win_rate > 0.65 else 10 if win_rate > 0.50 else 0
            raw += month_award
            available += 30
            factors.append(_factor_row("月度胜率", f"同月胜率 {win_rate:.0%}", month_award, 30, "当前用标的自身历史月度胜率做代理"))
        else:
            factors.append(_factor_row("月度胜率", "缺失", None, 30, "样本不足"))
    else:
        factors.append(_factor_row("月度胜率", "缺失", None, 30, "缺少月度历史"))

    sector = str(metadata.get("sector", "综合"))
    in_window = month in MONTHLY_SEASONAL_WINDOWS.get(sector, set())
    window_award = 25 if in_window else 0
    raw += window_award
    available += 25
    factors.append(_factor_row("旺季前置", "位于常见旺季窗口" if in_window else "当前不在典型旺季前置窗口", window_award, 25, "基于行业常见季节性映射"))

    earnings_window = month in {1, 4, 7, 10}
    earnings_award = 20 if earnings_window and sector == "科技" else 0
    raw += earnings_award
    available += 20
    factors.append(_factor_row("财报窗口", "当前处在典型财报月附近" if earnings_award else "当前不是典型财报博弈窗口", earnings_award, 20, "以季度财报窗口做代理"))

    rebalance_months = {5, 6, 11, 12}
    rebalance_award = 15 if month in rebalance_months else 5 if month in {4, 10} else 0
    raw += rebalance_award
    available += 15
    factors.append(
        _factor_row(
            "指数调整",
            "接近半年/年末常见调样窗口" if rebalance_award >= 15 else "当前不在典型调样窗口" if rebalance_award == 0 else "处在调样前置观察期",
            rebalance_award,
            15,
            "A 股主流指数常见在 6 月和 12 月附近调样；当前先用规则化日历代理。",
        )
    )

    dividend_award = 10 if sector == "高股息" and month in {4, 5, 6} else 0
    raw += dividend_award
    available += 10
    factors.append(_factor_row("分红窗口", "接近高股息常见抢权窗口" if dividend_award else "当前不是典型分红博弈窗口", dividend_award, 10, "高股息方向更相关"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "季节/日历",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "当前时间窗口相对有利。", "时间窗口中性，没有明显顺风。", "时间窗口不占优，更多靠主线和技术本身。", "ℹ️ 季节/日历数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _macro_dimension(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    vector = SENSITIVITY_MAP.get(str(metadata.get("sector", "综合")), {"rate": 0, "usd": 0, "oil": 0, "cny": 0})
    states = _current_factor_state(context)
    match_count = 0
    active = 0
    details: List[str] = []
    for factor in ("rate", "usd", "oil", "cny"):
        direction = int(vector.get(factor, 0))
        if direction == 0:
            continue
        active += 1
        same = direction == states.get(factor)
        match_count += 1 if same else 0
        details.append(f"{factor} {'顺风' if same else '逆风'}")

    if active == 0:
        score = 20
    elif match_count >= 3:
        score = 40
    elif match_count == 2:
        score = 30
    elif match_count == 1:
        score = 10
    else:
        score = 0
    factors = [
        _factor_row("敏感度向量", " / ".join(details) if details else "当前定义为中性敏感度", score, 40, "利率 / 美元 / 油价 / 人民币 四因子匹配"),
        _factor_row("当前 regime", context.get("regime", {}).get("current_regime", "unknown"), 0, 0, "宏观敏感度是修正因子，不单独决定方向"),
    ]
    return {
        "name": "宏观敏感度",
        "score": score,
        "max_score": 40,
        "summary": "大环境明显配合。" if score >= 30 else "宏观中性。" if score >= 20 else "宏观逆风明显。",
        "factors": factors,
        "core_signal": " · ".join(details[:3]) if details else "宏观中性",
        "missing": False,
        "macro_reverse": score == 0,
    }


def _rating_from_dimensions(dimensions: Mapping[str, Mapping[str, Any]], warnings: Sequence[str]) -> Dict[str, Any]:
    tech = dimensions["technical"]["score"]
    fundamental = dimensions["fundamental"]["score"]
    catalyst = dimensions["catalyst"]["score"]
    relative = dimensions["relative_strength"]["score"]
    risk = dimensions["risk"]["score"]
    macro = dimensions["macro"]["score"]

    def ok(value: Optional[int], threshold: int) -> bool:
        return value is not None and value >= threshold

    rank = 0
    label = "无信号"
    meaning = "没有形成可执行的多维共振。"
    if ok(tech, 70) and ok(fundamental, 60) and ok(catalyst, 50) and ok(risk, 50):
        rank, label, meaning = 4, "强机会", "四维共振，具备建仓计划条件。"
    elif ok(fundamental, 60) and (ok(catalyst, 50) or ok(relative, 60)) and ok(tech, 40):
        rank, label, meaning = 3, "较强机会", "逻辑成立，但还需要一个维度继续确认。"
    elif (ok(tech, 70) and catalyst is not None and catalyst < 30) or (ok(catalyst, 60) and tech is not None and tech < 40):
        rank, label, meaning = 2, "储备机会", "单维度亮灯但还未形成共振。"
    else:
        strong_dims = sum(1 for item in dimensions.values() if item.get("score") is not None and item.get("score", 0) >= 70)
        mid_dims = sum(1 for item in dimensions.values() if item.get("score") is not None and item.get("score", 0) >= 40)
        if strong_dims == 1 and mid_dims <= 2:
            rank, label, meaning = 1, "有信号但不充分", "只有单一维度足够亮，其余不足以支持动作。"

    if dimensions["macro"].get("macro_reverse"):
        rank = min(rank, 2)
        warnings = list(warnings) + ["⚠️ 宏观敏感度完全逆风，评级上限已压到 ⭐⭐"]
    if tech is None:
        rank = min(rank, 2)
        warnings = list(warnings) + ["ℹ️ 技术面数据缺失，评级上限降至 ⭐⭐"]
    if dimensions["fundamental"].get("available_max", 0) < 30:
        rank = min(rank, 3)
        warnings = list(warnings) + ["ℹ️ 基本面当前以代理因子为主，评级上限降至 ⭐⭐⭐"]

    stars = "—" if rank == 0 else "⭐" * rank
    return {"rank": rank, "stars": stars, "label": label, "meaning": meaning, "warnings": list(dict.fromkeys(warnings))}


def _dimension_score(dimensions: Mapping[str, Mapping[str, Any]], key: str) -> Optional[int]:
    return dimensions.get(key, {}).get("score")


def _find_factor(dimension: Mapping[str, Any], name: str) -> Dict[str, Any]:
    for factor in dimension.get("factors", []):
        if factor.get("name") == name:
            return factor
    return {}


def _phase_label(dimensions: Mapping[str, Mapping[str, Any]], technical: Mapping[str, Any]) -> tuple[str, str]:
    tech = _dimension_score(dimensions, "technical") or 0
    catalyst = _dimension_score(dimensions, "catalyst") or 0
    relative = _dimension_score(dimensions, "relative_strength") or 0
    macro = _dimension_score(dimensions, "macro") or 0
    risk = _dimension_score(dimensions, "risk") or 0
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))

    if tech >= 65 and catalyst >= 40 and relative >= 60:
        return "趋势启动", "说明价格、轮动和事件催化开始同向，后续更看确认而不是猜底。"
    if relative >= 70 and tech >= 50 and rsi >= 65:
        return "强势整理", "说明主线方向并未被破坏，但短线已经不在最舒服的位置。"
    if macro >= 30 and risk >= 65 and tech < 50:
        return "防守轮动", "说明当前吸引力更多来自配置与避险属性，而不是新的趋势加速。"
    if relative >= 55 and tech < 50:
        return "中期上行中的整理", "说明大方向未坏，但短线需要新的动能或催化来完成下一次上攻。"
    if tech < 35 and relative < 35:
        return "下行修复", "说明趋势仍在修复，当前更像等待结构企稳，而不是抢先博反转。"
    return "震荡整理", "说明逻辑没有完全失效，但价格和催化暂时没有形成新的入场共振。"


def _direction_label(dimensions: Mapping[str, Mapping[str, Any]]) -> str:
    tech = _dimension_score(dimensions, "technical") or 0
    relative = _dimension_score(dimensions, "relative_strength") or 0
    macro = _dimension_score(dimensions, "macro") or 0
    risk = _dimension_score(dimensions, "risk") or 0
    catalyst = _dimension_score(dimensions, "catalyst") or 0

    if tech >= 65 and relative >= 60 and catalyst >= 40:
        return "明确偏多"
    if (relative >= 55 and macro >= 20) or (risk >= 65 and macro >= 30):
        return "中性偏多"
    if tech < 35 and relative < 35:
        return "中性偏空"
    return "中性"


def _odds_label(dimensions: Mapping[str, Mapping[str, Any]], metrics: Mapping[str, float], technical: Mapping[str, Any]) -> str:
    tech = _dimension_score(dimensions, "technical") or 0
    risk = _dimension_score(dimensions, "risk") or 0
    fundamental = _dimension_score(dimensions, "fundamental") or 0
    valuation_extreme = bool(dimensions.get("fundamental", {}).get("valuation_extreme"))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    if valuation_extreme or price_percentile >= 0.85 or rsi > 70:
        return "低"
    if tech >= 60 and risk >= 60 and fundamental >= 40:
        return "高"
    return "中"


def _trade_state_label(
    dimensions: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, float],
    technical: Mapping[str, Any],
) -> str:
    direction = _direction_label(dimensions)
    odds = _odds_label(dimensions, metrics, technical)
    tech = _dimension_score(dimensions, "technical") or 0
    relative = _dimension_score(dimensions, "relative_strength") or 0

    if direction in {"明确偏多", "中性偏多"} and odds == "低":
        return "持有优于追高"
    if relative >= 60 and tech < 50:
        return "等右侧确认"
    if tech >= 50 and odds != "低":
        return "回调更优"
    if tech < 40:
        return "观察为主"
    return "风险释放前不宜激进"


def _headline_core(
    dimensions: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, float],
    technical: Mapping[str, Any],
) -> str:
    tech = _dimension_score(dimensions, "technical") or 0
    catalyst = _dimension_score(dimensions, "catalyst") or 0
    relative = _dimension_score(dimensions, "relative_strength") or 0
    macro = _dimension_score(dimensions, "macro") or 0
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))

    if macro >= 30 and relative >= 60 and tech < 50:
        return "防守逻辑或主线顺风仍在，但趋势与动能还没有重新同步"
    if relative >= 60 and price_percentile >= 0.85:
        return "逻辑仍在，但当前位置赔率已经被明显压缩"
    if tech >= 55 and catalyst < 30:
        return "价格结构不算差，但新的催化还没跟上"
    if catalyst >= 50 and tech < 40:
        return "逻辑有事件驱动，但价格还没完成确认"
    if rsi > 70:
        return "方向未坏，但短线已经偏拥挤"
    return "逻辑未完全破坏，但价格、催化和资金尚未形成新的共振"


def _asset_note(metadata: Mapping[str, Any], asset_type: str) -> str:
    sector = str(metadata.get("sector", "该主题"))
    if asset_type in {"cn_etf", "us", "hk", "hk_index"}:
        return f"这只 ETF 本质上更像在买 `{sector}` 方向的核心风格暴露，而不是无差别买整个市场。"
    return ""


def _build_narrative(
    analysis_seed: Mapping[str, Any],
    metadata: Mapping[str, Any],
    asset_type: str,
    metrics: Mapping[str, float],
    dimensions: Mapping[str, Mapping[str, Any]],
    technical: Mapping[str, Any],
    action: Mapping[str, str],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    direction = _direction_label(dimensions)
    cycle = "中期(1-3月)" if (_dimension_score(dimensions, "relative_strength") or 0) >= 55 or (_dimension_score(dimensions, "macro") or 0) >= 30 else "短期(1-4周)"
    odds = _odds_label(dimensions, metrics, technical)
    trade_state = _trade_state_label(dimensions, metrics, technical)
    phase_label, phase_body = _phase_label(dimensions, technical)
    phase_headline = {
        "趋势启动": "逻辑与价格开始共振",
        "强势整理": "中期偏多，但短线略有拥挤",
        "防守轮动": "防守属性成立，但趋势尚未重新启动",
        "中期上行中的整理": "中期偏多，但短线仍在整理",
        "下行修复": "短期承压，仍在修复阶段",
        "震荡整理": "逻辑未破，但节奏一般",
    }.get(phase_label, "逻辑未破，但节奏一般")
    headline = f"这是一个**{phase_headline}**的标的。当前核心不是没逻辑，而是**{_headline_core(dimensions, metrics, technical)}**。"

    sector = str(metadata.get("sector", "综合"))
    theme = str(context.get("day_theme", {}).get("label", "背景宏观主导"))
    regime = str(context.get("regime", {}).get("current_regime", "unknown"))
    macro_score = _dimension_score(dimensions, "macro") or 0
    chips_score = _dimension_score(dimensions, "chips")
    relative_score = _dimension_score(dimensions, "relative_strength") or 0
    tech_score = _dimension_score(dimensions, "technical") or 0
    risk_score = _dimension_score(dimensions, "risk") or 0
    catalyst_score = _dimension_score(dimensions, "catalyst") or 0
    fundamental_score = _dimension_score(dimensions, "fundamental") or 0
    fundamental_dimension = dimensions["fundamental"]
    valuation_snapshot = dict(fundamental_dimension.get("valuation_snapshot") or {})
    valuation_pe = valuation_snapshot.get("pe_ttm")
    valuation_note = str(fundamental_dimension.get("valuation_note", ""))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    support_signal = _find_factor(dimensions["technical"], "支撑位").get("signal", "关键支撑未明确")
    macd_signal = _find_factor(dimensions["technical"], "MACD 金叉").get("signal", "MACD 方向一般")
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    ma20 = float(technical.get("ma_system", {}).get("mas", {}).get("MA20", 0.0))
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", ma20))
    support_level = max(float(technical.get("fibonacci", {}).get("levels", {}).get("0.618", 0.0)), ma60)

    macro_driver = (
        f"当前更偏 `{theme}` / `{regime}` 背景。对 `{sector}` 方向来说，宏观与主线整体是 {'顺风' if macro_score >= 30 else '中性或略逆风'}，"
        f"问题不在于故事是否完全失效，而在于这种顺风能否继续转化成新的价格确认。"
    )
    if _asset_note(metadata, asset_type):
        macro_driver += f" {_asset_note(metadata, asset_type)}"

    if chips_score is None:
        flow_driver = "增量资金数据目前不完整，所以暂时看不到很强的机构加仓确认；当前更像配置属性或相对强弱在支撑。"
    elif chips_score >= 60:
        flow_driver = "资金面已经开始给出确认，说明这条线不只是讲故事，而是有增量资金在承接。"
    else:
        flow_driver = "资金面暂时没有形成明确共振，所以现阶段更多还是看方向和结构，而不是看资金追买。"

    if relative_score >= 70:
        relative_driver = "相对强弱仍占优，说明资金没有彻底离开这条线；即使短线有波动，也更像强势方向内部的节奏调整。"
    elif relative_score >= 40:
        relative_driver = "相对强弱处在中间地带，说明它还没有被市场完全放弃，但也不是当前最强的扩散方向。"
    else:
        relative_driver = "相对强弱偏弱，说明当前轮动还没有明确回到它身上，更多是修复观察而不是主线确认。"

    if tech_score >= 60:
        technical_driver = f"技术结构整体完整，`{macd_signal}`，中期趋势没有被破坏。"
    elif support_signal and support_signal != "当前价格未明显贴近 MA60、前低或关键斐波那契支撑":
        technical_driver = f"技术面最值得看的不是强趋势，而是价格已经回到 `{support_signal}` 附近；但 `{macd_signal}`，短线动能还需要再修复。"
    else:
        technical_driver = f"技术面当前最大问题不是完全破位，而是 `{macd_signal}`，趋势确认不足。"

    if macro_score >= 30 and relative_score >= 60 and tech_score < 50:
        contradiction = "中期逻辑偏正面，但短线动能还没有重新修复，因此更适合等待确认，而不是直接追价。"
    elif catalyst_score >= 50 and (price_percentile >= 0.80 or (valuation_pe is not None and float(valuation_pe) >= 45)):
        contradiction = "催化并不弱，但市场已经提前交易了大半预期，所以现在的核心矛盾不是‘有没有故事’，而是‘还有没有足够好的赔率’。"
    elif price_percentile >= 0.85 and (relative_score >= 55 or macro_score >= 30):
        contradiction = "方向并不差，但价格已经处在偏高位置，导致‘逻辑正确’和‘位置舒服’之间出现了明显错位。"
    elif catalyst_score < 30 and tech_score >= 45:
        contradiction = "价格结构不算差，但缺少新的催化去推动第二阶段上涨，所以现在更像磨时间，而不是直接加速。"
    else:
        contradiction = "当前最大的矛盾在于逻辑并非彻底失效，但还缺少足够清晰的价格、资金和催化共振。"

    positives: List[str] = []
    if macro_score >= 30:
        positives.append(f"`{theme}` 和 `{regime}` 背景下，这个方向至少没有明显宏观逆风。")
    if relative_score >= 60:
        positives.append("相对强弱仍占优，说明它不是市场最先被放弃的方向。")
    if risk_score >= 60:
        positives.append("回撤、相关性或防守属性还算可控，适合放进组合框架里评估。")
    if support_signal and support_signal != "当前价格未明显贴近 MA60、前低或关键斐波那契支撑":
        positives.append(f"价格靠近 `{support_signal}`，说明下方不是完全没有承接。")
    if not positives:
        positives.append("当前仍保留一定观察价值，主要因为趋势并未被彻底破坏。")

    cautions: List[str] = []
    if fundamental_score <= 20:
        cautions.append("估值/基本面安全边际并不突出，当前价格已经提前反映一部分预期。")
    if catalyst_score < 30:
        cautions.append("缺少新的催化去推动下一段行情，短线更难靠故事继续推升。")
    if tech_score < 50:
        cautions.append("短线动能不足，趋势确认仍欠缺。")
    if rsi > 70:
        cautions.append("短线已经偏拥挤，即使逻辑不坏，追高的盈亏比也不优。")
    if not cautions:
        cautions.append("当前最大问题不是方向，而是节奏，仍要防止追在情绪高点。")

    external_risk_map = {
        "黄金": "需要继续盯美元、实际利率和地缘溢价；一旦美元转强且避险交易降温，价格回吐会很快。",
        "科技": "需要继续盯利率、美元和风险偏好；只要波动率抬升，估值就容易再受压。",
        "电网": "需要继续盯政策节奏、商品价格和风险偏好；若主线切走，强势方向也可能进入获利了结。",
        "能源": "需要继续盯油价、地缘和政策调控；一旦油价冲高回落，交易拥挤可能迅速反噬。",
        "有色": "需要继续盯美元、商品价格和全球增长预期；外部需求转弱时弹性会明显收缩。",
    }
    risk_points = {
        "fundamental": f"真正的基本面风险不在 {metadata.get('name', analysis_seed.get('symbol'))} 本身，而在其所暴露的 `{sector}` 景气如果不及预期，估值支撑会继续下移。",
        "valuation": (
            f"当前真实估值参考为 `{valuation_snapshot.get('index_name', '相关指数')}` 滚动PE `{float(valuation_pe):.1f}x`，"
            f"同时价格位置在近一年 `{price_percentile:.0%}` 分位；高估值和高位置是两层风险，不是一回事。"
            if valuation_pe is not None
            else f"当前价格位置大约在近一年 `{price_percentile:.0%}` 分位。{valuation_note}"
        ),
        "crowding": "如果这条线继续成为市场共识，短线资金拥挤会放大波动；如果共识撤退，回撤也会更陡。" if (relative_score >= 60 or rsi > 65) else "当前拥挤风险不算极端，但一旦没有增量资金确认，走势容易反复。",
        "external": external_risk_map.get(sector, "还要继续盯利率、美元、商品价格和市场风格切换，这类外部变量往往会比基本面更快改写短线定价。"),
    }

    watch_points = [
        f"短线动能是否重新修复：重点看 `{macd_signal}` 能否扭转，或价格重新站上关键均线。",
        f"关键支撑是否有效：重点看 `{support_signal}` 附近是否出现企稳，而不是继续失守。",
        "资金是否重新形成共振：ETF 份额、主力资金或相关配置资金是否重新转正。",
        f"主线变量是否延续：继续观察 `{theme}` 是否强化，以及它对应的宏观变量是否继续配合。",
    ]

    validation_points = [
        {
            "watch": "短线动能重启",
            "judge": f"MACD 重新金叉，且收盘站回 MA20 `{ma20:.3f}` 上方",
            "bull": "说明价格开始从整理转向确认，趋势交易者可以重新评估右侧介入。",
            "bear": "说明仍处于弱修复或横盘，继续等确认而不是抢跑。",
        },
        {
            "watch": "关键支撑是否守住",
            "judge": f"收盘不低于关键支撑 `{support_level:.3f}` 下方 2%",
            "bull": "说明回调更像消化而不是破位，左侧观察价值仍在。",
            "bear": "说明支撑失效，先处理风险，再谈逻辑。",
        },
        {
            "watch": "资金是否回流",
            "judge": "近 5 个可用样本里，ETF净流入/主力净流入至少 3 次转正",
            "bull": "说明不只是逻辑在，资金也开始重新确认。",
            "bear": "说明还是存量博弈，价格容易反复。",
        },
    ]
    if sector == "科技":
        validation_points.append(
            {
                "watch": "宏观逆风是否缓和",
                "judge": "VIX 回落到 25 以下，且 DXY 5日不再继续走强",
                "bull": "说明成长估值压力缓解，科技方向更容易从修复走向扩散。",
                "bear": "说明宏观仍逆风，高beta成长继续受压。",
            }
        )
    elif sector == "黄金":
        validation_points.append(
            {
                "watch": "避险交易是否成立",
                "judge": "GLD 单日 > +0.5%，且 DXY 同步走弱",
                "bull": "说明市场在交易避险而不是单纯交易美元。",
                "bear": "说明黄金更多只是高位震荡，不宜追价。",
            }
        )
    else:
        validation_points.append(
            {
                "watch": "主线变量是否继续强化",
                "judge": f"`{theme}` 对应的关键资产和板块强度继续同步改善",
                "bull": "说明当前主线没有失效，方向仍值得继续跟踪。",
                "bear": "说明主线边际降温，需要下调优先级。",
            }
        )

    base_scenario = (
        f"更可能进入 `{phase_label}` 的延续状态：大方向未必立刻转坏，但价格会先消化当前位置和催化不足的问题，"
        f"因此判断更偏 `{trade_state}`。"
    )
    bull_scenario = (
        "如果短线动能重新修复、价格站回关键均线，同时出现新的政策/资金/海外映射催化，"
        "当前判断会从‘逻辑在但节奏一般’升级为‘趋势与价格重新共振’。"
    )
    bear_scenario = (
        "如果主线变量转弱，或关键支撑被跌破而资金没有承接，"
        "那当前逻辑就会从‘等待确认’转成‘先处理回撤，再谈方向’。"
    )

    allocation_playbook = (
        "如果你看重的是中期逻辑而不是短线节奏，现在更像观察期；先等估值和趋势至少有一项改善，再考虑分批。"
        if action.get("position") == "暂不出手"
        else f"如果你看重的是中期逻辑而不是短线节奏，可以按 `{action.get('position', '小仓位分批')}` 的框架分批，但不适合一次性追价。"
    )
    playbook = {
        "trend": "更适合等短线动能重新修复后再跟随，而不是在趋势尚未顺畅时提前抢跑。",
        "allocation": allocation_playbook,
        "defensive": "如果你更在意舒服的位置，那现在更合理的是先观察，等趋势、支撑和资金至少确认两项再动手。",
    }

    summary_lines = [
        f"总体来看，`{metadata.get('name', analysis_seed.get('symbol'))}` 的核心逻辑在于 `{theme}` 背景下的 `{sector}` 暴露仍有配置价值；",
        f"短期制约在于 `{contradiction}`",
        f"因此当前更合理的动作是 **{trade_state}**，而不是简单地把它归类成“买”或“不买”。",
    ]

    return {
        "headline": headline,
        "judgment": {
            "direction": direction,
            "cycle": cycle,
            "odds": odds,
            "state": trade_state,
        },
        "drivers": {
            "macro": macro_driver,
            "flow": flow_driver,
            "relative": relative_driver,
            "technical": technical_driver,
        },
        "contradiction": contradiction,
        "positives": positives[:3],
        "cautions": cautions[:3],
        "phase": {"label": phase_label, "body": phase_body},
        "risk_points": risk_points,
        "watch_points": watch_points,
        "validation_points": validation_points,
        "scenarios": {
            "base": base_scenario,
            "bull": bull_scenario,
            "bear": bear_scenario,
        },
        "playbook": playbook,
        "summary_lines": summary_lines,
    }


def _action_plan(
    analysis: Mapping[str, Any],
    history: pd.DataFrame,
    technical: Mapping[str, Any],
) -> Dict[str, str]:
    rating = analysis["rating"]["rank"]
    tech = analysis["dimensions"]["technical"]["score"]
    macro_reverse = analysis["dimensions"]["macro"].get("macro_reverse", False)
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    fib_levels = technical.get("fibonacci", {}).get("levels", {})
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", history["close"].iloc[-1]))
    stop_ref = max(float(fib_levels.get("0.382", 0.0)), float(fib_levels.get("0.500", 0.0)), ma60)
    target_ref = float(history["high"].tail(60).max())
    if rating >= 3 and not macro_reverse:
        direction = "做多"
    elif rating == 2:
        direction = "观望"
    else:
        direction = "回避"
    if rsi > 70:
        entry = "等 RSI 回落到 60 附近且 MACD 不死叉，再考虑分批介入"
    elif tech is not None and tech >= 70:
        entry = "等回踩 MA20 / MA60 或关键斐波那契支撑后企稳，再做首次试探"
    else:
        entry = "先等 MACD 再次转强或站回 MA20，避免在弱趋势里提前出手"
    position = "首次建仓 ≤5%，确认后再加到 10%" if rating >= 3 else "先不超过 5% 试错" if rating == 2 else "暂不出手"
    timeframe = "中线配置(1-3月)" if rating >= 3 else "短线交易(1-2周)" if rating == 2 else "等待更好窗口"
    target = f"先看前高/近 60 日高点 {target_ref:.3f} 附近的承压与突破情况"
    stop = f"跌破 {stop_ref:.3f} 或主线/催化失效时重新评估"
    return {
        "direction": direction,
        "entry": entry,
        "position": position,
        "stop": stop,
        "target": target,
        "timeframe": timeframe,
    }


def analyze_opportunity(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    context: Optional[Mapping[str, Any]] = None,
    metadata_override: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    runtime_context = dict(context or build_market_context(config, relevant_asset_types=[asset_type, "cn_etf", "futures"]))
    runtime_context["config"] = dict(config)
    metadata = _merge_metadata(symbol, asset_type, metadata_override, config)
    history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config)))
    technical = TechnicalAnalyzer(history).generate_scorecard(dict(config).get("technical", {}))
    metrics = compute_history_metrics(history)
    asset_returns = history["close"].pct_change().dropna()
    correlation_pair = _correlation_to_watchlist(symbol, asset_returns, runtime_context)
    benchmark_symbol = ""
    benchmark_name = ""
    benchmark_history = None
    benchmark_spec = BENCHMARKS.get(asset_type)
    if benchmark_spec:
        benchmark_symbol, benchmark_asset_type, benchmark_name = benchmark_spec
        benchmark_history = _safe_history(benchmark_symbol, benchmark_asset_type, config)

    dimensions = {
        "technical": _technical_dimension(history, technical),
        "fundamental": _fundamental_dimension(symbol, asset_type, metadata, metrics, config),
        "catalyst": _catalyst_dimension(metadata, runtime_context),
        "relative_strength": _relative_strength_dimension(symbol, asset_type, metadata, metrics, asset_returns, runtime_context),
        "chips": _chips_dimension(symbol, asset_type, metadata, runtime_context, config),
        "risk": _risk_dimension(symbol, asset_type, history, asset_returns, runtime_context, correlation_pair),
        "seasonality": _seasonality_dimension(metadata, history, runtime_context),
        "macro": _macro_dimension(metadata, runtime_context),
    }
    checks, exclusion_reasons, warnings = _hard_checks(
        metadata,
        history,
        metrics,
        technical,
        runtime_context,
        dimensions["macro"]["score"],
        correlation_pair,
        dimensions["fundamental"],
    )
    rating = _rating_from_dimensions(dimensions, warnings)
    action = _action_plan({"rating": rating, "dimensions": dimensions}, history, technical)
    notes: List[str] = list(runtime_context.get("notes", []))
    if metadata.get("in_watchlist"):
        notes.append("该标的已在 watchlist 中，本次分析更偏复核而不是首次发现。")
    conclusion = (
        f"{dimensions['technical']['summary']} "
        f"{dimensions['fundamental']['summary']} "
        f"{dimensions['catalyst']['summary']}"
    ).strip()
    risks = list(dict.fromkeys(rating["warnings"] + exclusion_reasons))
    if not risks:
        risks = ["当前没有触发额外强风险，但仍需按主线和止损条件执行。"]
    narrative = _build_narrative(
        {
            "symbol": symbol,
            "name": str(metadata.get("name", symbol)),
        },
        metadata,
        asset_type,
        metrics,
        dimensions,
        technical,
        action,
        runtime_context,
    )

    return {
        "symbol": symbol,
        "name": str(metadata.get("name", symbol)),
        "asset_type": asset_type,
        "metadata": metadata,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "history": history,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_name": benchmark_name,
        "benchmark_history": benchmark_history,
        "regime": runtime_context.get("regime", {}),
        "day_theme": runtime_context.get("day_theme", {}),
        "metrics": metrics,
        "technical_raw": technical,
        "dimensions": dimensions,
        "hard_checks": checks,
        "rating": rating,
        "conclusion": conclusion,
        "action": action,
        "risks": risks,
        "notes": notes,
        "narrative": narrative,
        "excluded": bool(exclusion_reasons),
        "exclusion_reasons": exclusion_reasons,
        "correlation_pair": correlation_pair,
    }


def build_default_pool(config: Mapping[str, Any], theme_filter: str = "") -> tuple[List[PoolItem], List[str]]:
    warnings: List[str] = []
    watchlist = load_watchlist()
    pool: List[PoolItem] = []
    seen: set[str] = set()
    opportunity_cfg = dict(config).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("min_turnover", 50_000_000))
    max_candidates = int(opportunity_cfg.get("max_scan_candidates", 30))
    lowered_filter = theme_filter.lower().strip()

    try:
        realtime = ChinaMarketCollector(config).get_etf_realtime()
        code_col = "代码" if "代码" in realtime.columns else "基金代码" if "基金代码" in realtime.columns else None
        name_col = "名称" if "名称" in realtime.columns else "基金简称" if "基金简称" in realtime.columns else None
        amount_col = "成交额" if "成交额" in realtime.columns else None
        if code_col and name_col and amount_col:
            frame = realtime.copy()
            frame[amount_col] = pd.to_numeric(frame[amount_col], errors="coerce").fillna(0.0)
            frame = frame[frame[amount_col] >= min_turnover]
            if lowered_filter:
                frame = frame[frame[name_col].astype(str).str.lower().str.contains(lowered_filter, na=False)]
            excluded_keywords = ("债", "货币", "国债", "政金", "现金", "利率", "短融")
            frame = frame[~frame[name_col].astype(str).str.contains("|".join(excluded_keywords), na=False)]
            frame = frame.sort_values(amount_col, ascending=False).head(max_candidates)
            for _, row in frame.iterrows():
                symbol = str(row[code_col])
                if symbol in seen:
                    continue
                name = str(row[name_col])
                sector, chain_nodes = _normalize_sector(name)
                pool.append(
                    PoolItem(
                        symbol=symbol,
                        name=name,
                        asset_type="cn_etf",
                        region="CN",
                        sector=sector,
                        chain_nodes=chain_nodes,
                        source="all_market_etf",
                        turnover=float(row[amount_col]),
                        in_watchlist=any(item["symbol"] == symbol for item in watchlist),
                    )
                )
                seen.add(symbol)
    except Exception as exc:
        warnings.append(f"全市场 ETF 扫描池拉取失败，已回退到 watchlist: {exc}")

    for item in watchlist:
        if lowered_filter and lowered_filter not in str(item.get("name", "")).lower() and lowered_filter not in str(item.get("sector", "")).lower():
            continue
        if item["symbol"] in seen:
            continue
        metadata = _merge_metadata(str(item["symbol"]), str(item.get("asset_type", "cn_etf")), item, config)
        pool.append(
            PoolItem(
                symbol=str(item["symbol"]),
                name=str(metadata.get("name", item["symbol"])),
                asset_type=str(item.get("asset_type", "cn_etf")),
                region=str(metadata.get("region", "CN")),
                sector=str(metadata.get("sector", "综合")),
                chain_nodes=list(metadata.get("chain_nodes", DEFAULT_CHAIN_NODES)),
                source="watchlist",
                in_watchlist=True,
            )
        )
        seen.add(item["symbol"])

    return pool, warnings


def discover_opportunities(config: Mapping[str, Any], top_n: int = 5, theme_filter: str = "") -> Dict[str, Any]:
    context = build_market_context(config, relevant_asset_types=["cn_etf", "futures"])
    pool, pool_warnings = build_default_pool(config, theme_filter)
    passed = 0
    analyses: List[Dict[str, Any]] = []
    blind_spots: List[str] = list(pool_warnings)
    for item in pool:
        try:
            analysis = analyze_opportunity(
                item.symbol,
                item.asset_type,
                config,
                context=context,
                metadata_override={
                    "name": item.name,
                    "sector": item.sector,
                    "chain_nodes": item.chain_nodes,
                    "region": item.region,
                    "in_watchlist": item.in_watchlist,
                },
            )
        except Exception as exc:
            blind_spots.append(f"{item.symbol} 扫描失败: {exc}")
            continue
        if analysis["excluded"]:
            continue
        passed += 1
        if analysis["rating"]["rank"] > 0:
            analyses.append(analysis)
    analyses.sort(
        key=lambda item: (
            item["rating"]["rank"],
            item["dimensions"]["technical"]["score"] or 0,
            item["dimensions"]["relative_strength"]["score"] or 0,
            item["dimensions"]["catalyst"]["score"] or 0,
        ),
        reverse=True,
    )
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_pool": len(pool),
        "passed_pool": passed,
        "regime": context.get("regime", {}),
        "day_theme": context.get("day_theme", {}),
        "top": analyses[:top_n],
        "blind_spots": blind_spots[:8],
        "theme_filter": theme_filter,
    }


def compare_opportunities(symbols: Sequence[str], config: Mapping[str, Any]) -> Dict[str, Any]:
    asset_types = [detect_asset_type_for_compare(symbol, config) for symbol in symbols]
    context = build_market_context(config, relevant_asset_types=list(dict.fromkeys(asset_types + ["cn_etf", "futures"])))
    rows: List[Dict[str, Any]] = []
    for symbol, asset_type in zip(symbols, asset_types):
        rows.append(analyze_opportunity(symbol, asset_type, config, context=context))
    best = max(
        rows,
        key=lambda item: (
            item["rating"]["rank"],
            sum((dimension.get("score") or 0) for dimension in item["dimensions"].values()),
        ),
    )
    return {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "analyses": rows, "best_symbol": best["symbol"]}


def detect_asset_type_for_compare(symbol: str, config: Mapping[str, Any]) -> str:
    return detect_asset_type(symbol, config)
