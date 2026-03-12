"""Unified opportunity discovery and analysis engine."""

from __future__ import annotations

import io
import math
import re
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from src.collectors import (
    AssetLookupCollector,
    ChinaMarketCollector,
    EventsCollector,
    FundProfileCollector,
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
from src.utils.market import (
    build_snapshot_fallback_history,
    build_intraday_snapshot,
    compute_history_metrics,
    fetch_asset_history,
    format_pct,
    get_asset_context,
)

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


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
    "cn_stock": ("510300", "cn_etf", "沪深300ETF"),
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

NEGATIVE_DILUTION_KEYS = (
    "配股",
    "配售",
    "增发",
    "定增",
    "减持",
    "减持计划",
    "再融资",
    "募资",
    "placing",
    "placement",
    "secondary offering",
    "share sale",
    "follow-on offering",
    "top-up placing",
    "convertible bond",
)

NEGATIVE_REGULATORY_KEYS = (
    "审查",
    "调查",
    "处罚",
    "罚款",
    "禁令",
    "限制",
    "反垄断",
    "诉讼",
    "review",
    "probe",
    "investigation",
    "antitrust",
    "ban",
    "restriction",
    "lawsuit",
    "sanction",
    "cfius",
    "security review",
    "security risk",
    "forced sale",
    "forced divestiture",
    "divest",
    "divestiture",
    "blacklist",
    "national security",
    "gaming stake",
    "gaming stakes",
    "gaming investment",
    "gaming investments",
)

NEGATIVE_EVENT_LOOKBACK_DAYS = 30
FORWARD_EVENT_LOOKAHEAD_DAYS = 14
DIRECT_COMPANY_NEWS_LOOKBACK_DAYS = 45
HOLDER_TRADE_LOOKBACK_DAYS = 90
CAPITAL_RETURN_LOOKBACK_DAYS = 365

DISCLOSURE_WINDOW_KEYS = (
    "年报",
    "半年报",
    "一季报",
    "三季报",
    "财报",
    "业绩快报",
    "业绩预告",
    "results",
    "earnings",
    "guidance",
)

DISCLOSURE_RESULT_KEYS = (
    "净利润",
    "营收",
    "收入",
    "利润",
    "分红",
    "派现",
    "现金红利",
    "股息",
    "dividend",
    "revenue",
    "profit",
    "results",
)

DISCLOSURE_PERIOD_KEYS = (
    "全年",
    "年度",
    "四季度",
    "三季度",
    "半年",
    "q1",
    "q2",
    "q3",
    "q4",
    "quarter",
    "full year",
    "annual",
)

HIGH_CONFIDENCE_COMPANY_SOURCES = (
    "reuters",
    "bloomberg",
    "financial times",
    "ft",
    "business wire",
    "pr newswire",
    "globenewswire",
    "investor relations",
    "hkexnews",
    "sec filing",
    "sec",
)

STRUCTURED_COMPANY_EVENT_KEYS = (
    "订单",
    "中标",
    "签约",
    "合同",
    "合作",
    "战略合作",
    "回购",
    "增持",
    "分红",
    "派现",
    "现金红利",
    "股息",
    "扩产",
    "扩建",
    "投产",
    "量产",
    "涨价",
    "并购",
    "重组",
    "launch",
    "award",
    "contract",
    "order",
    "buyback",
    "dividend",
    "partnership",
    "collaboration",
    "expansion",
    "guidance",
    "forecast",
    "outlook",
    "results",
    "earnings",
)

NON_POSITIVE_COMPANY_STATEMENT_KEYS = (
    "未发布",
    "未披露",
    "暂无",
    "尚未",
    "暂未",
    "未有",
    "没有涉及",
    "不涉及",
    "没有计划",
    "无计划",
    "no guidance",
    "did not provide",
    "not provide",
    "no plan",
    "not involved",
    "not currently involved",
)

WEAK_COMPANY_PAGE_TITLE_KEYS = (
    "stock price & latest news",
    "stock quote price and forecast",
    "| stock price",
    "quote price and forecast",
    "historical prices and data",
    "stock historical prices",
)

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
    "高股息": ["红利", "高股息", "电信", "运营商", "公用事业"],
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
    metadata: Optional[Dict[str, Any]] = None


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
    merged.setdefault("region", {"cn_etf": "CN", "cn_stock": "CN", "hk": "HK", "hk_index": "HK", "us": "US", "futures": "CN"}.get(asset_type, "CN"))
    return merged


def _collect_fund_profile(symbol: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    if not symbol:
        return {}
    try:
        return FundProfileCollector(config).collect_profile(symbol)
    except Exception:
        return {}


def _enrich_metadata_with_fund_profile(metadata: Dict[str, Any], fund_profile: Mapping[str, Any]) -> Dict[str, Any]:
    if not fund_profile:
        return metadata
    enriched = dict(metadata)
    overview = dict(fund_profile.get("overview") or {})
    style = dict(fund_profile.get("style") or {})
    fund_name = str(overview.get("基金简称", "")).strip()
    if fund_name:
        enriched["name"] = fund_name
    sector = str(style.get("sector", "")).strip()
    if sector and sector != "综合":
        enriched["sector"] = sector
    chain_nodes = list(style.get("chain_nodes") or [])
    if chain_nodes:
        enriched["chain_nodes"] = chain_nodes
    tags = [str(item).strip() for item in style.get("tags") or [] if str(item).strip()]
    if tags:
        enriched["fund_style_tags"] = tags
        enriched["is_passive_fund"] = "被动跟踪" in tags
    benchmark = str(overview.get("业绩比较基准", "")).strip()
    if benchmark:
        enriched["benchmark"] = benchmark
    manager_name = str(overview.get("基金经理人", "")).strip()
    if manager_name:
        enriched["manager_name"] = manager_name
    return enriched


FUND_BENCHMARK_HINTS = [
    (("战略新兴", "新兴产业"), ["战略新兴", "新兴产业"]),
    (("恒生科技", "港股科技"), ["恒生科技", "港股科技"]),
    (("沪深300",), ["沪深300"]),
    (("中证a500", "a500"), ["中证A500"]),
    (("中证500",), ["中证500"]),
    (("创业板",), ["创业板"]),
    (("科创50",), ["科创50"]),
    (("半导体", "芯片"), ["半导体", "芯片"]),
    (("军工", "国防"), ["军工", "国防"]),
    (("消费", "食品饮料", "家电"), ["消费", "内需"]),
    (("高股息", "红利"), ["高股息", "红利"]),
    (("黄金", "贵金属"), ["黄金", "贵金属"]),
    (("电网", "电力"), ["电网", "电力"]),
    (("人工智能", "ai"), ["人工智能"]),
]

FUND_INDUSTRY_HINTS = [
    (("电子", "半导体", "芯片"), ["电子", "半导体", "芯片"]),
    (("信息技术", "软件", "互联网", "通信", "通讯"), ["信息技术", "软件", "通信", "数据中心"]),
    (("军工", "国防", "航天"), ["军工", "国防", "航天"]),
    (("有色", "金属", "铜", "铝"), ["有色", "铜", "铝"]),
    (("公用事业", "电力", "电网"), ["电力", "电网", "公用事业"]),
    (("医药", "医疗"), ["医药", "医疗"]),
    (("消费", "零售", "食品", "饮料", "家电"), ["消费", "零售", "家电"]),
]

FUND_NOISY_KEYWORDS = {
    "ai",
    "人工智能",
    "科技",
    "growth",
    "technology",
    "cloud",
    "software",
    "算力",
    "成长股估值修复",
    "新兴产业",
}


def _unique_strings(items: Sequence[Any]) -> List[str]:
    cleaned: List[str] = []
    for item in items:
        value = str(item).strip()
        if value and value not in cleaned:
            cleaned.append(value)
    return cleaned


def _fund_top_holdings(fund_profile: Optional[Mapping[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
    return list((fund_profile or {}).get("top_holdings") or [])[:top_n]


def _fund_top_holding_names(fund_profile: Optional[Mapping[str, Any]], top_n: int = 5) -> List[str]:
    return _unique_strings(item.get("股票名称", "") for item in _fund_top_holdings(fund_profile, top_n))


def _fund_benchmark_keywords(fund_profile: Optional[Mapping[str, Any]]) -> List[str]:
    overview = dict((fund_profile or {}).get("overview") or {})
    benchmark = str(overview.get("业绩比较基准", "")).strip()
    if not benchmark:
        return []
    lowered = benchmark.lower()
    keywords: List[str] = []
    for tokens, outputs in FUND_BENCHMARK_HINTS:
        if any(token.lower() in lowered for token in tokens):
            keywords.extend(outputs)
    for item in re.findall(r"([A-Za-z0-9\u4e00-\u9fa5]+指数)", benchmark):
        cleaned = str(item).strip()
        if cleaned:
            keywords.append(cleaned)
    normalized = (
        benchmark.replace("收益率", " ")
        .replace("业绩比较基准", " ")
        .replace("*", " ")
        .replace("×", " ")
        .replace("+", " ")
        .replace("/", " ")
    )
    for part in re.split(r"[\s,，；;]+", normalized):
        cleaned = str(part).strip()
        if cleaned and cleaned.endswith("指数"):
            keywords.append(cleaned)
    if not keywords:
        keywords.append(benchmark)
    return _unique_strings(keywords)


def _fund_industry_keywords(fund_profile: Optional[Mapping[str, Any]]) -> List[str]:
    rows = list((fund_profile or {}).get("industry_allocation") or [])[:5]
    keywords: List[str] = []
    for row in rows:
        label = str(row.get("行业类别", "")).strip()
        if not label:
            continue
        lowered = label.lower()
        for tokens, outputs in FUND_INDUSTRY_HINTS:
            if any(token.lower() in lowered for token in tokens):
                keywords.extend(outputs)
        if label not in {"制造业", "综合"}:
            keywords.append(label)
    return _unique_strings(keywords)


def _fund_theme_keywords(metadata: Mapping[str, Any], fund_profile: Optional[Mapping[str, Any]]) -> List[str]:
    sector = str(metadata.get("sector", "")).strip()
    benchmark_keys = _fund_benchmark_keywords(fund_profile)
    industry_keys = _fund_industry_keywords(fund_profile)
    holding_names = _fund_top_holding_names(fund_profile, top_n=5)
    keywords = [sector, *benchmark_keys, *industry_keys, *holding_names]
    return _unique_strings(keywords)


COMMODITY_FUND_KEYWORDS = (
    "商品型",
    "期货",
    "现货",
    "贵金属",
    "黄金",
    "原油",
    "能源化工",
    "能源化工期货",
    "易盛郑商所能源化工",
)


def _is_commodity_like_fund(
    asset_type: str,
    metadata: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]],
) -> bool:
    if asset_type not in {"cn_etf", "cn_fund"}:
        return False
    overview = dict((fund_profile or {}).get("overview") or {})
    style = dict((fund_profile or {}).get("style") or {})
    text_pool = " ".join(
        [
            str(metadata.get("name", "")).strip(),
            str(metadata.get("sector", "")).strip(),
            str(metadata.get("benchmark", "")).strip(),
            str(overview.get("基金类型", "")).strip(),
            str(overview.get("业绩比较基准", "")).strip(),
            str(overview.get("跟踪标的", "")).strip(),
            str(style.get("benchmark_note", "")).strip(),
            " ".join(str(item).strip() for item in metadata.get("fund_style_tags", []) if str(item).strip()),
            " ".join(str(item).strip() for item in style.get("tags", []) if str(item).strip()),
            " ".join(str(item).strip() for item in style.get("chain_nodes", []) if str(item).strip()),
        ]
    ).lower()
    return any(str(token).lower() in text_pool for token in COMMODITY_FUND_KEYWORDS)


def _parse_chinese_amount(text: Any) -> Optional[float]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if not match:
        return None
    value = float(match.group(1))
    if "万亿" in cleaned:
        return value * 1e12
    if "千亿" in cleaned:
        return value * 1e11
    if "亿" in cleaned:
        return value * 1e8
    if "万" in cleaned:
        return value * 1e4
    return value


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


def _normalized_title_key(title: str) -> str:
    return re.sub(r"[\s·，,。:：;；!！?？'\"“”‘’（）()\\-]+", "", str(title or "").strip().lower())


def _unique_news_titles(items: Sequence[Mapping[str, Any]], limit: int = 2) -> List[str]:
    titles: List[str] = []
    seen_keys: set[str] = set()
    for item in items:
        title = str(item.get("title", "")).strip()
        title_key = _normalized_title_key(title)
        if title and title_key and title_key not in seen_keys:
            titles.append(title)
            seen_keys.add(title_key)
        if len(titles) >= limit:
            break
    return titles


def _catalyst_core_signal(
    factors: Sequence[Dict[str, Any]],
    stock_specific_pool: Sequence[Mapping[str, Any]],
    company_positive_pool: Sequence[Mapping[str, Any]],
    is_individual_stock: bool,
    asset_type: str,
) -> str:
    negative_signal = next(
        (
            str(item.get("signal", "")).strip()
            for item in factors
            if item.get("name") == "负面事件" and str(item.get("display_score", "")).strip().startswith("-")
        ),
        "",
    )
    if negative_signal:
        positive_tail = _top_positive_signals([item for item in factors if item.get("name") != "负面事件"], limit=2)
        if positive_tail and positive_tail != "当前没有明确亮点":
            return " · ".join([negative_signal, positive_tail])
        return negative_signal
    if is_individual_stock:
        title_pool = company_positive_pool if asset_type in {"hk", "us"} else stock_specific_pool
        positive_specific = [
            item
            for item in title_pool
            if not _contains_any(_headline_text(item), [*NEGATIVE_DILUTION_KEYS, *NEGATIVE_REGULATORY_KEYS, *DISCLOSURE_WINDOW_KEYS])
            and not _is_non_positive_company_statement(item)
        ]
        company_titles = _unique_news_titles(positive_specific or title_pool, limit=2)
        if company_titles:
            density_signal = next((str(item.get("signal", "")).strip() for item in factors if item.get("name") == "研报/新闻密度"), "")
            extras = [density_signal] if density_signal else []
            return " · ".join([*company_titles, *extras[:1]])
    return _top_positive_signals(factors)


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


def _evidence_row(
    *,
    layer: str,
    item: Mapping[str, Any],
) -> Dict[str, str]:
    return {
        "layer": layer,
        "title": str(item.get("title", "")).strip(),
        "source": str(item.get("source") or item.get("configured_source") or "").strip(),
        "link": str(item.get("link", "")).strip(),
        "date": str(item.get("date") or item.get("published_at") or "").strip(),
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
        "as_of": datetime.now(),
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


def _valuation_keywords(
    metadata: Mapping[str, Any],
    asset_type: str = "",
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    name = str(metadata.get("name", "")).strip()
    sector = str(metadata.get("sector", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    keywords: List[str] = []
    if asset_type in {"cn_fund", "cn_etf"} and fund_profile:
        fund_keys = _fund_theme_keywords(metadata, fund_profile)
        benchmark_keys = _fund_benchmark_keywords(fund_profile)
        industry_keys = _fund_industry_keywords(fund_profile)
        holdings_text = " ".join(_fund_top_holding_names(fund_profile))
        semis_exposed = any(token in holdings_text for token in ("寒武纪", "中芯", "北方华创", "澜起", "长电", "韦尔", "兆易", "芯片", "半导体"))
        if benchmark_keys:
            keywords.extend(benchmark_keys)
        if industry_keys:
            keywords.extend(industry_keys[:3])
        if sector and sector not in {"综合", "科技"}:
            keywords.append(sector)
        if semis_exposed:
            keywords.extend(["半导体", "芯片"])
        elif sector == "科技":
            keywords.extend(["科技", "战略新兴", "信息技术", "通信"])
        keywords.extend(fund_keys[:3])
    elif "半导体" in name or "芯片" in name or "半导体" in chain_nodes:
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


def _fund_financial_proxy(
    collector: ValuationCollector,
    fund_profile: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    holdings = _fund_top_holdings(fund_profile, top_n=5)
    normalized_holdings = [
        {
            "symbol": item.get("股票代码", ""),
            "name": item.get("股票名称", ""),
            "weight": item.get("占净值比例", 0.0),
        }
        for item in holdings
        if str(item.get("股票代码", "")).strip()
    ]
    if not normalized_holdings:
        return {}
    try:
        return collector.get_weighted_stock_financial_proxies(normalized_holdings, top_n=5)
    except Exception:
        return {}


def _intraday_snapshot(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    history: pd.DataFrame,
) -> Dict[str, Any]:
    return build_intraday_snapshot(symbol, asset_type, config, history)


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


def _catalyst_profile(
    metadata: Mapping[str, Any],
    config: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    path_value = str(config.get("catalyst_profiles_file", "config/catalyst_profiles.yaml"))
    profiles = _load_catalyst_profiles(path_value)
    sector = str(metadata.get("sector", "")).strip()
    name = str(metadata.get("name", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]

    lowered_name = name.lower()
    lowered_nodes = [item.lower() for item in chain_nodes]
    derived = _derived_catalyst_profile(metadata)
    exact_sector_profile = dict(profiles.get(sector, {}))
    if exact_sector_profile:
        matched = dict(exact_sector_profile)
        if fund_profile:
            matched = _merge_catalyst_profiles(matched, _fund_specific_catalyst_profile(metadata, fund_profile))
        matched["profile_name"] = sector
        return matched
    if fund_profile:
        derived = _merge_catalyst_profiles(derived, _fund_specific_catalyst_profile(metadata, fund_profile))
    for profile_name, payload in profiles.items():
        themes = [str(item).strip() for item in payload.get("themes", []) if str(item).strip()]
        if any(theme.lower() in lowered_name for theme in themes) or any(theme.lower() in lowered_nodes for theme in themes):
            matched = _merge_catalyst_profiles(derived, dict(payload))
            matched["profile_name"] = profile_name
            return matched
    return derived


def _merge_catalyst_profiles(*profiles: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    list_keys = {
        "themes",
        "keywords",
        "policy_keywords",
        "domestic_leaders",
        "overseas_leaders",
        "earnings_keywords",
        "event_keywords",
        "strict_keywords",
        "search_terms",
    }
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


def _fund_specific_catalyst_profile(
    metadata: Mapping[str, Any],
    fund_profile: Mapping[str, Any],
) -> Dict[str, Any]:
    sector = str(metadata.get("sector", "综合")).strip()
    base = dict(GENERIC_CATALYST_PROFILES.get(sector, GENERIC_CATALYST_PROFILES["宽基"]))
    benchmark_keywords = _fund_benchmark_keywords(fund_profile)
    industry_keywords = _fund_industry_keywords(fund_profile)
    holding_names = _fund_top_holding_names(fund_profile, top_n=5)
    base_keywords = [
        str(item).strip()
        for item in base.get("keywords", [])
        if str(item).strip() and str(item).strip().lower() not in FUND_NOISY_KEYWORDS
    ]
    strict_keywords = _unique_strings([*holding_names, *benchmark_keywords, *industry_keywords])
    if not strict_keywords:
        strict_keywords = _unique_strings([*base_keywords, *base.get("event_keywords", [])])
    search_terms = _unique_strings([*holding_names[:3], *benchmark_keywords[:2], *industry_keywords[:3], *base_keywords[:2]])
    profile = _merge_catalyst_profiles(
        base,
        {
            "profile_name": f"{sector or '基金'}基金",
            "themes": [sector, *benchmark_keywords],
            "keywords": [*benchmark_keywords, *industry_keywords, *base_keywords, *holding_names[:3]],
            "domestic_leaders": holding_names[:5] or list(base.get("domestic_leaders", [])),
            "strict_keywords": strict_keywords[:12],
            "search_terms": search_terms[:8],
            "strict_mode": True,
            "sector_hint": sector,
        },
    )
    return profile


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
    industry = str(metadata.get("industry", "")).strip()
    # For individual stocks, always include stock name + industry as base keywords
    stock_base = [name] if name and name != sector else []
    if industry and industry not in stock_base:
        stock_base.append(industry)
    if "半导体" in name or "芯片" in name or "半导体" in chain_nodes or "半导体" in industry:
        return stock_base + ["半导体", "芯片", "存储", "semiconductor", "chip", "foundry", "fab", "tsmc", "台积电", "micron", "美光", "hynix", "海力士", "gpu", "capex", "涨价", "drAM", "nand"]
    if sector == "电网":
        return stock_base + ["电网", "电力", "特高压", "智能电网", "grid", "utility", "光伏", "储能", "新能源"]
    if sector == "黄金":
        return stock_base + ["黄金", "gold", "bullion", "央行", "central bank"]
    if sector == "有色":
        return stock_base + ["有色", "铜", "铝", "copper", "aluminum", "metal", "矿业", "金价", "铜价", "产能"]
    if sector == "能源":
        return stock_base + ["原油", "oil", "gas", "能源", "opec", "lng"]
    if sector == "军工":
        return stock_base + ["军工", "国防", "defense", "aerospace", "军贸", "导弹", "无人机", "卫星"]
    if sector == "高股息":
        return stock_base + ["高股息", "红利", "dividend", "yield", "utility", "bank"]
    if sector == "科技":
        return stock_base + ["科技", "ai", "算力", "芯片", "半导体", "光模块", "PCB", "数据中心", "capex"]
    if "纳斯达克" in name or "纳指" in name:
        return stock_base + ["nasdaq", "纳斯达克", "纳指", "big tech", "earnings", "guidance", "ai"]
    result = _metadata_news_keys(metadata)
    return stock_base + [k for k in result if k not in stock_base]


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
    explicit_terms = [str(item).strip() for item in profile.get("search_terms", []) if str(item).strip()]
    if explicit_terms:
        return explicit_terms[:8]
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
    profile_name = str(profile.get("profile_name", "")).strip()
    if profile_name not in {"科技", "半导体", "纳斯达克", "港股科技"}:
        cleaned = _strict_relevance_tokens(profile, cleaned)
    return cleaned[:8]


def _strict_relevance_tokens(profile: Mapping[str, Any], tokens: Sequence[str]) -> List[str]:
    profile_name = str(profile.get("profile_name", "")).strip()
    if bool(profile.get("strict_mode")):
        explicit = [str(token).strip() for token in profile.get("strict_keywords", []) if str(token).strip()]
        if explicit:
            return explicit
    generic_ai_noise = {
        "ai",
        "artificial intelligence",
        "gpu",
        "model",
        "llm",
        "算力",
        "人工智能",
        "科技",
        "软件",
        "数字经济",
        "云计算",
        "technology",
        "cloud",
        "software",
        "big tech",
        "growth",
        "成长股估值修复",
    }
    if profile_name not in {"科技", "半导体", "纳斯达克", "港股科技"}:
        cleaned: List[str] = []
        for token in tokens:
            value = str(token).strip()
            lowered = value.lower()
            if not value or any(noise in lowered for noise in generic_ai_noise):
                continue
            if value not in cleaned:
                cleaned.append(value)
        return cleaned
    if profile_name not in {"纳斯达克", "港股科技"}:
        return [str(token).strip() for token in tokens if str(token).strip()]

    cleaned: List[str] = []
    for token in tokens:
        value = str(token).strip()
        lowered = value.lower()
        if not value or any(noise in lowered for noise in generic_ai_noise):
            continue
        if value not in cleaned:
            cleaned.append(value)
    return cleaned


def _fundamental_proxy_labels(asset_type: str) -> Dict[str, str]:
    if asset_type == "cn_fund":
        return {
            "growth": "前五大重仓股加权增速代理",
            "growth_scope": "当前优先用前五大重仓股财报做加权代理",
            "roe": "前五大重仓股加权 ROE",
            "margin": "前五大重仓股加权毛利率",
            "peg_base": "前五大重仓股",
        }
    if asset_type == "cn_etf":
        return {
            "growth": "前五大持仓/成分股加权增速代理",
            "growth_scope": "当前优先用基金前五大持仓，缺失时回退到前五大成分股财报代理",
            "roe": "前五大持仓/成分股加权 ROE",
            "margin": "前五大持仓/成分股加权毛利率",
            "peg_base": "前五大持仓/成分股",
        }
    if asset_type in {"cn_stock", "hk", "us"}:
        return {
            "growth": "个股增速",
            "growth_scope": "当前用个股最新财报数据",
            "roe": "个股 ROE",
            "margin": "个股毛利率",
            "peg_base": "个股",
        }
    return {
        "growth": "前五大成分股加权增速代理",
        "growth_scope": "当前优先用前五大成分股营收同比",
        "roe": "前五大成分股加权 ROE",
        "margin": "前五大成分股加权毛利率",
        "peg_base": "前五大成分股",
    }


def _preferred_catalyst_sources(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> List[str]:
    region = str(metadata.get("region", "")).upper().strip()
    profile_name = str(profile.get("profile_name", "")).strip()
    if region == "US" or profile_name == "纳斯达克":
        return ["Reuters", "Investor Relations", "SEC", "Bloomberg", "Financial Times"]
    if region == "HK" or profile_name == "港股科技":
        return ["Reuters", "HKEXnews", "Investor Relations", "Bloomberg", "Financial Times"]
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
    if bool(profile.get("strict_mode")):
        return _contains_any(text, strict_tokens)
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
    for keyword in keywords:
        value = str(keyword).strip().lower()
        if not value:
            continue
        if value.isascii() and value.replace(".", "").replace("-", "").isalnum() and len(value) <= 4:
            pattern = rf"(?<![a-z0-9]){re.escape(value)}(?![a-z0-9])"
            if re.search(pattern, lowered):
                return True
            continue
        if value in lowered:
            return True
    return False


def _title_source_text(item: Mapping[str, Any]) -> str:
    return " ".join([str(item.get("title", "")), str(item.get("source", ""))]).lower()


def _is_low_signal_company_page(item: Mapping[str, Any]) -> bool:
    title = str(item.get("title", "")).strip().lower()
    return any(key in title for key in WEAK_COMPANY_PAGE_TITLE_KEYS)


def _is_high_confidence_company_news(item: Mapping[str, Any]) -> bool:
    if _is_low_signal_company_page(item):
        return False
    source_text = " ".join(
        [
            str(item.get("source", "")),
            str(item.get("configured_source", "")),
        ]
    ).lower()
    if any(source in source_text for source in HIGH_CONFIDENCE_COMPANY_SOURCES):
        return True
    link = str(item.get("link", "")).strip().lower()
    parsed = urlparse(link)
    host = parsed.netloc.lower()
    return any(
        (
            domain in host
            or (domain == ".gov" and (host.endswith(".gov") or ".gov." in host))
            or (domain == "investor." and host.startswith("investor."))
        )
        for domain in (
            "reuters.com",
            "bloomberg.com",
            "ft.com",
            "businesswire.com",
            "prnewswire.com",
            "globenewswire.com",
            "hkexnews.hk",
            "sec.gov",
            ".gov",
            "investor.",
        )
    )


def _stock_name_tokens(metadata: Mapping[str, Any]) -> List[str]:
    asset_type_str = str(metadata.get("asset_type", ""))
    if asset_type_str not in {"cn_stock", "hk", "us"}:
        return []
    stock_name = str(metadata.get("name", ""))
    symbol_str = str(metadata.get("symbol", ""))
    if not stock_name or stock_name == symbol_str:
        return []
    tokens: List[str] = [stock_name]
    clean_name = stock_name.split("-")[0].strip()
    if clean_name and clean_name != stock_name:
        tokens.append(clean_name)
    has_cjk = any("\u4e00" <= char <= "\u9fff" for char in clean_name)
    if has_cjk and len(clean_name) >= 4:
        tokens.append(clean_name[:2])
    elif has_cjk and len(stock_name) >= 4:
        tokens.append(stock_name[:2])
    name_en = str(metadata.get("name_en", "")).strip()
    if name_en:
        tokens.append(name_en)
    aliases = metadata.get("aliases") or []
    if isinstance(aliases, str):
        aliases = [aliases]
    for alias in aliases:
        alias_text = str(alias).strip()
        if alias_text:
            tokens.append(alias_text)
    if asset_type_str == "us" and symbol_str and not symbol_str.startswith("0"):
        tokens.append(symbol_str.upper())
    return list(dict.fromkeys([token for token in tokens if token]))


def _context_now(context: Mapping[str, Any]) -> datetime:
    candidate = context.get("as_of")
    if isinstance(candidate, datetime):
        return candidate
    if candidate not in (None, ""):
        parsed = pd.to_datetime(candidate, errors="coerce")
        if not pd.isna(parsed):
            return parsed.to_pydatetime()
    return datetime.now()


def _is_disclosure_like_item(item: Mapping[str, Any], stock_name_tokens: Sequence[str] = ()) -> bool:
    text = _headline_text(item)
    if stock_name_tokens and not _contains_any(text, stock_name_tokens):
        return False
    if _contains_any(text, DISCLOSURE_WINDOW_KEYS):
        return True
    has_period_marker = bool(re.search(r"20\d{2}年", text)) or _contains_any(text, DISCLOSURE_PERIOD_KEYS)
    has_result_marker = _contains_any(text, DISCLOSURE_RESULT_KEYS)
    return has_period_marker and has_result_marker


def _is_structured_company_event_item(item: Mapping[str, Any], stock_name_tokens: Sequence[str] = ()) -> bool:
    category = str(item.get("category", "")).strip().lower()
    if category == "earnings_calendar":
        return True
    text = _headline_text(item)
    if stock_name_tokens and category != "stock_announcement" and not _contains_any(text, stock_name_tokens):
        return False
    if _is_disclosure_like_item(item, stock_name_tokens if category != "stock_announcement" else ()):
        return True
    if category == "stock_announcement" and _contains_any(text, STRUCTURED_COMPANY_EVENT_KEYS):
        return True
    if _is_high_confidence_company_news(item) and _contains_any(text, STRUCTURED_COMPANY_EVENT_KEYS):
        return True
    return False


def _is_non_positive_company_statement(item: Mapping[str, Any]) -> bool:
    return _contains_any(_headline_text(item), NON_POSITIVE_COMPANY_STATEMENT_KEYS)


def _direct_company_event_search_terms(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> List[List[str]]:
    base_tokens = [token for token in _stock_name_tokens(metadata) if len(str(token).strip()) >= 2][:2]
    if not base_tokens:
        return []
    primary_token = base_tokens[0]
    profile_terms = [
        str(item).strip()
        for item in [
            *(profile.get("earnings_keywords", []) or []),
            *(profile.get("event_keywords", []) or []),
        ]
        if str(item).strip()
    ]
    has_cjk_token = any(any("\u4e00" <= char <= "\u9fff" for char in token) for token in base_tokens)
    default_terms = ["财报", "业绩", "合作", "回购"] if has_cjk_token else ["earnings", "results", "guidance", "partnership"]
    event_terms = list(dict.fromkeys([*profile_terms, *default_terms]))

    groups: List[List[str]] = [[primary_token]]
    if event_terms:
        groups.append([primary_token, event_terms[0]])
    if len(event_terms) >= 2:
        groups.append([primary_token, event_terms[1]])
    if len(base_tokens) > 1:
        groups.append([base_tokens[1]])

    deduped: List[List[str]] = []
    seen: set[tuple[str, ...]] = set()
    for group in groups:
        cleaned = tuple(str(item).strip() for item in group if str(item).strip())
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(list(cleaned))
    return deduped[:3]


def _direct_company_negative_search_terms(metadata: Mapping[str, Any]) -> List[List[str]]:
    base_tokens = [token for token in _stock_name_tokens(metadata) if len(str(token).strip()) >= 2][:3]
    if not base_tokens:
        return []
    cjk_tokens = [token for token in base_tokens if any("\u4e00" <= char <= "\u9fff" for char in token)]
    ascii_tokens = [token for token in base_tokens if any(char.isascii() and char.isalpha() for char in token)]
    groups: List[List[str]] = []
    for token in ascii_tokens:
        groups.append([token])
        for negative_term in ["gaming stakes", "national security", "cfius"]:
            groups.append([token, negative_term])
    for token in cjk_tokens:
        groups.append([token])
        for negative_term in ["审查", "调查", "处罚"]:
            groups.append([token, negative_term])
    deduped: List[List[str]] = []
    seen: set[tuple[str, ...]] = set()
    for group in groups:
        cleaned = tuple(str(item).strip() for item in group if str(item).strip())
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(list(cleaned))
    return deduped[:6]


def _search_high_confidence_company_news(
    metadata: Mapping[str, Any],
    profile: Mapping[str, Any],
    config: Mapping[str, Any],
    recent_days: int = DIRECT_COMPANY_NEWS_LOOKBACK_DAYS,
) -> List[Mapping[str, Any]]:
    searches = _direct_company_event_search_terms(metadata, profile)
    if not searches:
        return []
    collector = NewsCollector(config)
    items: List[Mapping[str, Any]] = []
    for keywords in searches:
        try:
            hits = collector.search_by_keywords(
                keywords,
                preferred_sources=_preferred_catalyst_sources(metadata, profile),
                limit=6,
                recent_days=recent_days,
            )
        except Exception:
            continue
        items.extend(hits)
        high_conf_hits = [item for item in items if _is_high_confidence_company_news(item)]
        if len(high_conf_hits) >= 2:
            break
    return _dedupe_news_items(items)


def _extract_event_date_from_text(text: str, reference: datetime) -> Optional[datetime]:
    value = str(text).strip()
    patterns = [
        re.compile(r"(?P<year>20\d{2})[-/.年](?P<month>\d{1,2})[-/.月](?P<day>\d{1,2})日?"),
        re.compile(r"(?P<month>\d{1,2})月(?P<day>\d{1,2})日"),
    ]
    for pattern in patterns:
        match = pattern.search(value)
        if not match:
            continue
        try:
            year = int(match.groupdict().get("year") or reference.year)
            month = int(match.group("month"))
            day = int(match.group("day"))
            candidate = datetime(year, month, day)
        except (TypeError, ValueError):
            continue
        if "year" not in match.groupdict() or match.groupdict().get("year") is None:
            for offset in (0, 1):
                try:
                    guess = datetime(reference.year + offset, month, day)
                except ValueError:
                    continue
                delta = (guess.date() - reference.date()).days
                if 0 <= delta <= 31:
                    return guess
        return candidate
    return None


def _coerce_datetime_list(value: Any) -> List[datetime]:
    if value in (None, ""):
        return []
    if isinstance(value, Mapping):
        candidates = list(value.values())
    elif isinstance(value, (str, datetime, pd.Timestamp)):
        candidates = [value]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        candidates = list(value)
    elif hasattr(value, "tolist"):
        candidates = list(value.tolist())
    else:
        candidates = [value]

    parsed_values: List[datetime] = []
    for candidate in candidates:
        parsed = pd.to_datetime(candidate, errors="coerce")
        if pd.isna(parsed):
            continue
        timestamp = pd.Timestamp(parsed)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert(None)
        parsed_values.append(timestamp.to_pydatetime())
    return parsed_values


def _yfinance_calendar_symbol(symbol: str, asset_type: str) -> Optional[str]:
    if asset_type == "hk":
        code = symbol.upper().replace(".HK", "").lstrip("0") or "0"
        return f"{code.zfill(4)}.HK"
    if asset_type == "us":
        return symbol.upper()
    return None


@lru_cache(maxsize=256)
def _company_calendar_event_dates(symbol: str, asset_type: str) -> tuple[str, ...]:
    if yf is None:
        return ()
    ticker_symbol = _yfinance_calendar_symbol(symbol, asset_type)
    if not ticker_symbol:
        return ()

    dates: List[datetime] = []
    try:
        calendar = yf.Ticker(ticker_symbol).calendar
    except Exception:
        calendar = None

    calendar_map: Mapping[str, Any] = {}
    if isinstance(calendar, Mapping):
        calendar_map = calendar
    elif hasattr(calendar, "to_dict"):
        try:
            calendar_map = calendar.to_dict()
        except Exception:
            calendar_map = {}

    earnings_field = None
    for key in ("Earnings Date", "EarningsDate"):
        if key in calendar_map:
            earnings_field = calendar_map[key]
            break
    dates.extend(_coerce_datetime_list(earnings_field))

    if not dates:
        try:
            earnings_dates = yf.Ticker(ticker_symbol).earnings_dates
        except Exception:
            earnings_dates = None
        if earnings_dates is not None and not getattr(earnings_dates, "empty", True):
            dates.extend(_coerce_datetime_list(list(getattr(earnings_dates, "index", []))[:4]))

    unique = sorted({item.date().isoformat() for item in dates})
    return tuple(unique)


def _cn_report_period_label(end_date: str) -> str:
    text = str(end_date or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return "相关报告期"
    mmdd = text[4:]
    year = text[:4]
    if mmdd == "1231":
        return f"{year}年年报"
    if mmdd == "0930":
        return f"{year}年三季报"
    if mmdd == "0630":
        return f"{year}年半年报"
    if mmdd == "0331":
        return f"{year}年一季报"
    return f"截至 {year}-{text[4:6]}-{text[6:8]} 报告期"


def _cn_disclosure_calendar_items(metadata: Mapping[str, Any], context: Mapping[str, Any], horizon_days: int = FORWARD_EVENT_LOOKAHEAD_DAYS) -> List[Dict[str, Any]]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return []
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return []
    collector = ValuationCollector(dict(context.get("config", {})))
    reference = _context_now(context)
    display_name = str(metadata.get("name", symbol)).strip() or symbol
    items: List[Dict[str, Any]] = []
    try:
        rows = collector.get_cn_stock_disclosure_dates(symbol)
    except Exception:
        rows = []
    for row in rows:
        event_date_text = str(row.get("actual_date") or row.get("pre_date") or "").strip()
        if not event_date_text:
            continue
        event_date = pd.to_datetime(event_date_text, errors="coerce")
        if pd.isna(event_date):
            continue
        days = (event_date.date() - reference.date()).days
        if days < -horizon_days or days > horizon_days:
            continue
        period_label = _cn_report_period_label(str(row.get("end_date", "")))
        if str(row.get("actual_date") or "").strip():
            title = f"{display_name} 已于 {event_date.date().isoformat()} 披露 {period_label}"
        else:
            title = f"{display_name} 预计于 {event_date.date().isoformat()} 披露 {period_label}"
        items.append(
            {
                "title": title,
                "category": "stock_disclosure_calendar",
                "source": "Tushare disclosure_date",
                "configured_source": "Tushare disclosure_date",
                "published_at": event_date.date().isoformat(),
                "date": event_date.date().isoformat(),
                "link": "",
            }
        )
    return items


def _cn_holdertrade_snapshot(metadata: Mapping[str, Any], context: Mapping[str, Any], lookback_days: int = HOLDER_TRADE_LOOKBACK_DAYS) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    collector = ValuationCollector(dict(context.get("config", {})))
    reference = _context_now(context)
    display_name = str(metadata.get("name", symbol)).strip() or symbol
    try:
        rows = collector.get_cn_stock_holder_trades(symbol)
    except Exception:
        rows = []
    if not rows:
        return {}
    frame = pd.DataFrame(rows)
    if frame.empty or "ann_date" not in frame.columns:
        return {}
    frame["ann_date"] = pd.to_datetime(frame["ann_date"], errors="coerce")
    frame = frame.dropna(subset=["ann_date"])
    if frame.empty:
        return {}
    frame = frame[(reference - frame["ann_date"]).dt.days.between(0, lookback_days)]
    if frame.empty:
        return {}
    frame["change_ratio"] = pd.to_numeric(frame["change_ratio"] if "change_ratio" in frame.columns else 0.0, errors="coerce").fillna(0.0)
    if "in_de" in frame.columns:
        frame["in_de"] = frame["in_de"].astype(str).str.upper()
    else:
        frame["in_de"] = ""
    increase_ratio = float(frame.loc[frame["in_de"] == "IN", "change_ratio"].sum())
    decrease_ratio = float(frame.loc[frame["in_de"] == "DE", "change_ratio"].sum())
    latest_date = frame["ann_date"].max().date().isoformat()
    if increase_ratio <= 0 and decrease_ratio <= 0:
        return {}
    if increase_ratio > decrease_ratio:
        net_ratio = round(increase_ratio - decrease_ratio, 4)
        title = f"{display_name} 近 {lookback_days} 日高管/股东净增持约 {net_ratio:.2f}%"
        direction = "increase"
    else:
        net_ratio = round(decrease_ratio - increase_ratio, 4)
        title = f"{display_name} 近 {lookback_days} 日高管/股东净减持约 {net_ratio:.2f}%"
        direction = "decrease"
    return {
        "direction": direction,
        "net_ratio": net_ratio,
        "increase_ratio": round(increase_ratio, 4),
        "decrease_ratio": round(decrease_ratio, 4),
        "latest_date": latest_date,
        "item": {
            "title": title,
            "category": "holder_trade",
            "source": "Tushare stk_holdertrade",
            "configured_source": "Tushare stk_holdertrade",
            "published_at": latest_date,
            "date": latest_date,
            "link": "",
        },
    }


def _cn_capital_return_items(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> List[Dict[str, Any]]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return []
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return []
    collector = ValuationCollector(dict(context.get("config", {})))
    reference = _context_now(context)
    display_name = str(metadata.get("name", symbol)).strip() or symbol
    items: List[Dict[str, Any]] = []

    try:
        repurchases = collector.get_cn_stock_repurchase(symbol)
    except Exception:
        repurchases = []
    for row in repurchases:
        ann_date = pd.to_datetime(str(row.get("ann_date", "")), errors="coerce")
        if pd.isna(ann_date):
            continue
        age_days = (reference.date() - ann_date.date()).days
        if age_days < 0 or age_days > CAPITAL_RETURN_LOOKBACK_DAYS:
            continue
        proc = str(row.get("proc", "")).strip() or "进展"
        items.append(
            {
                "title": f"{display_name} 披露股份回购{proc}",
                "category": "repurchase",
                "source": "Tushare repurchase",
                "configured_source": "Tushare repurchase",
                "published_at": ann_date.date().isoformat(),
                "date": ann_date.date().isoformat(),
                "link": "",
            }
        )
        break

    try:
        dividends = collector.get_cn_stock_dividend(symbol)
    except Exception:
        dividends = []
    for row in dividends:
        ann_date = pd.to_datetime(str(row.get("ann_date", "")), errors="coerce")
        if pd.isna(ann_date):
            continue
        age_days = (reference.date() - ann_date.date()).days
        if age_days < 0 or age_days > CAPITAL_RETURN_LOOKBACK_DAYS:
            continue
        div_proc = str(row.get("div_proc", "")).strip() or "进展"
        items.append(
            {
                "title": f"{display_name} 披露现金分红{div_proc}",
                "category": "dividend",
                "source": "Tushare dividend",
                "configured_source": "Tushare dividend",
                "published_at": ann_date.date().isoformat(),
                "date": ann_date.date().isoformat(),
                "link": "",
            }
        )
        break

    holdertrade = _cn_holdertrade_snapshot(metadata, context)
    if holdertrade.get("item"):
        items.append(dict(holdertrade["item"]))

    return items


def _item_datetime(item: Mapping[str, Any], reference: datetime) -> Optional[datetime]:
    for key in ("published_at", "published", "date", "datetime"):
        value = item.get(key)
        if value in (None, ""):
            continue
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            continue
        timestamp = pd.Timestamp(parsed)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert(None)
        return timestamp.to_pydatetime()
    title = str(item.get("title", "")).strip()
    return _extract_event_date_from_text(title, reference) if title else None


def _negative_event_penalty(item: Mapping[str, Any], reference: datetime) -> tuple[int, str]:
    event_date = _item_datetime(item, reference)
    if event_date is None:
        detail = f"未提取到明确日期，按 `{NEGATIVE_EVENT_LOOKBACK_DAYS}` 日窗口的中等惩罚处理。"
        return 10, detail
    age_days = max((reference.date() - event_date.date()).days, 0)
    if age_days > NEGATIVE_EVENT_LOOKBACK_DAYS:
        return 0, f"事件已过去 {age_days} 天，超出 `{NEGATIVE_EVENT_LOOKBACK_DAYS}` 日负面事件窗口。"
    if age_days <= 7:
        penalty = 15
    elif age_days <= 14:
        penalty = 12
    else:
        penalty = 8
    detail = f"事件发生于 {event_date.strftime('%Y-%m-%d')}，距今 {age_days} 天，按 `{NEGATIVE_EVENT_LOOKBACK_DAYS}` 日衰减窗口处理。"
    return penalty, detail


def _disclosure_window_signal(items: Sequence[Mapping[str, Any]], reference: datetime) -> Optional[Dict[str, Any]]:
    disclosure_items = [item for item in items if _is_disclosure_like_item(item)]
    best_signal: Optional[Dict[str, Any]] = None
    best_rank: tuple[int, int] | None = None
    for item in disclosure_items:
        title = str(item.get("title", "")).strip()
        event_date = _item_datetime(item, reference)
        if event_date is None:
            continue
        days = (event_date.date() - reference.date()).days
        if 0 <= days <= 7:
            detail = f"{days} 天后有财报/年报类披露事件，短线波动和预期差风险都会放大。"
            candidate = {"penalty": 15, "signal": title, "detail": detail}
            rank = (15, -abs(days))
        elif 7 < days <= FORWARD_EVENT_LOOKAHEAD_DAYS:
            detail = f"{days} 天后有财报/年报类披露事件，窗口已进入 `{FORWARD_EVENT_LOOKAHEAD_DAYS}` 日观察区间。"
            candidate = {"penalty": 8, "signal": title, "detail": detail}
            rank = (8, -abs(days))
        elif -3 <= days < 0:
            detail = f"{abs(days)} 天前刚披露财报/年报类结果，市场仍处在典型事件波动窗口。"
            candidate = {"penalty": 15, "signal": title, "detail": detail}
            rank = (15, -abs(days))
        elif -FORWARD_EVENT_LOOKAHEAD_DAYS <= days < -3:
            detail = f"{abs(days)} 天前已披露财报/年报类结果，事件余波通常仍会影响短线风险偏好。"
            candidate = {"penalty": 8, "signal": title, "detail": detail}
            rank = (8, -abs(days))
        else:
            continue
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_signal = candidate
    if best_signal:
        return best_signal
    if disclosure_items:
        detail = f"近期公告/新闻命中财报或业绩披露关键词 ({len(disclosure_items)} 条)，处在典型事件窗口。"
        return {"penalty": 8, "signal": str(disclosure_items[0].get("title", "")).strip(), "detail": detail}
    return None


def _company_forward_events(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    news_items: Optional[Sequence[Mapping[str, Any]]] = None,
    extra_items: Optional[Sequence[Mapping[str, Any]]] = None,
    horizon_days: int = FORWARD_EVENT_LOOKAHEAD_DAYS,
) -> List[Dict[str, Any]]:
    reference = _context_now(context)
    asset_type = str(metadata.get("asset_type", ""))
    if asset_type not in {"cn_stock", "hk", "us"}:
        return []
    symbol = str(metadata.get("symbol", ""))
    display_name = str(metadata.get("name", symbol)).strip() or symbol
    stock_name_tokens = _stock_name_tokens(metadata)
    forward_hits: List[Dict[str, Any]] = []

    if asset_type == "cn_stock":
        forward_hits.extend(_cn_disclosure_calendar_items(metadata, context, horizon_days=horizon_days))

    combined_items = _dedupe_news_items([*(news_items or []), *(extra_items or [])])
    for item in combined_items:
        if not _is_disclosure_like_item(item, stock_name_tokens):
            continue
        event_date = _item_datetime(item, reference)
        if event_date is None:
            continue
        days = (event_date.date() - reference.date()).days
        if 0 <= days <= horizon_days:
            record = dict(item)
            record.setdefault("date", event_date.date().isoformat())
            record.setdefault("published_at", event_date.date().isoformat())
            forward_hits.append(record)

    if asset_type in {"hk", "us"}:
        for iso_date in _company_calendar_event_dates(symbol, asset_type):
            event_date = pd.to_datetime(iso_date, errors="coerce")
            if pd.isna(event_date):
                continue
            days = (event_date.date() - reference.date()).days
            if 0 <= days <= horizon_days:
                forward_hits.append(
                    {
                        "title": f"{display_name} 预计于 {event_date.date().isoformat()} 披露业绩/财报",
                        "category": "earnings_calendar",
                        "source": "yfinance",
                        "configured_source": "yfinance",
                        "published_at": event_date.date().isoformat(),
                        "date": event_date.date().isoformat(),
                        "link": "",
                    }
                )

    ordered: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in sorted(forward_hits, key=lambda row: (str(row.get("date", "")), str(row.get("title", "")))):
        key = (str(item.get("date", "")), str(item.get("title", "")))
        if key in seen:
            continue
        seen.add(key)
        ordered.append(item)
    return ordered


def _structured_company_event_items(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    news_items: Optional[Sequence[Mapping[str, Any]]] = None,
    stock_news_items: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    asset_type = str(metadata.get("asset_type", ""))
    if asset_type not in {"cn_stock", "hk", "us"}:
        return []

    stock_name_tokens = _stock_name_tokens(metadata)
    combined_items = _dedupe_news_items([*(news_items or []), *(stock_news_items or [])])
    structured_hits: List[Dict[str, Any]] = []
    if asset_type == "cn_stock":
        structured_hits.extend(_cn_capital_return_items(metadata, context))
    for item in combined_items:
        if _is_structured_company_event_item(item, stock_name_tokens):
            structured_hits.append(dict(item))

    forward_hits = _company_forward_events(
        metadata,
        context,
        news_items=combined_items,
        extra_items=structured_hits,
    )
    ordered: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in [*structured_hits, *forward_hits]:
        key = (str(item.get("title", "")).strip(), str(item.get("source", "")).strip())
        if not key[0] or key in seen:
            continue
        seen.add(key)
        ordered.append(dict(item))
    return ordered


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
    asset_type: str,
    metadata: Mapping[str, Any],
    history: pd.DataFrame,
    metrics: Mapping[str, float],
    technical: Mapping[str, Any],
    context: Mapping[str, Any],
    macro_score: Optional[int],
    correlation_pair: Optional[tuple[str, float]],
    fundamental_dimension: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> tuple[List[Dict[str, str]], List[str], List[str]]:
    checks: List[Dict[str, str]] = []
    exclusion_reasons: List[str] = []
    warnings: List[str] = []
    opportunity_cfg = dict(context.get("config", {})).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("min_turnover", 50_000_000))
    min_history_days = int(opportunity_cfg.get("min_listing_days", 60))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    history_fallback = bool(metadata.get("history_fallback"))

    if asset_type == "cn_fund":
        overview = dict((fund_profile or {}).get("overview") or {})
        net_asset_text = str(overview.get("净资产规模", "")).strip()
        net_asset = None
        match = re.search(r"(\d+(?:\.\d+)?)", net_asset_text)
        if match:
            net_asset = float(match.group(1))
        liquidity_ok = bool(net_asset and net_asset >= 1.0)
        checks.append(
            {
                "name": "基金规模",
                "status": "✅" if liquidity_ok else "⚠️",
                "detail": net_asset_text or "未披露净资产规模",
            }
        )
        if net_asset is not None and net_asset < 1.0:
            exclusion_reasons.append("基金规模偏小")
    else:
        liquidity_ok = float(metrics.get("avg_turnover_20d", 0.0)) >= min_turnover
        liquidity_detail = f"日均成交 {metrics.get('avg_turnover_20d', 0.0) / 1e8:.2f} 亿"
        liquidity_status = "✅" if liquidity_ok else "❌"
        if history_fallback:
            liquidity_detail = f"当前按场内实时成交快照代理 {metrics.get('avg_turnover_20d', 0.0) / 1e8:.2f} 亿，未拿到完整 20 日历史"
            liquidity_status = "⚠️" if liquidity_ok else "❌"
        checks.append({"name": "流动性", "status": liquidity_status, "detail": liquidity_detail})
        if not liquidity_ok:
            exclusion_reasons.append("日均成交额低于 5000 万")

    listed_ok = len(history) >= min_history_days
    if history_fallback:
        checks.append(
            {
                "name": "历史完整性",
                "status": "⚠️",
                "detail": f"完整日线缺失，当前用本地实时快照降级生成 {len(history)} 个占位样本，不据此判断真实上市时长",
            }
        )
    else:
        checks.append({"name": "上市时长", "status": "✅" if listed_ok else "❌", "detail": f"有效历史样本 {len(history)} 个交易日"})
    if not listed_ok and not history_fallback:
        exclusion_reasons.append("上市不满 60 个交易日")

    if asset_type == "cn_stock":
        stock_name = str(metadata.get("name", ""))
        is_st = stock_name.upper().startswith(("ST", "*ST"))
        checks.append({"name": "ST 风险", "status": "❌" if is_st else "✅", "detail": f"{'ST / *ST 股票' if is_st else '非 ST 股票'}"})
        if is_st:
            exclusion_reasons.append("ST / *ST 股票，退市风险较高")

    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        if _is_commodity_like_fund(asset_type, metadata, fund_profile):
            fundamental_floor_detail = "当前按商品/期货 ETF 的产品结构、跟踪标的和容量做基础质量判断，不使用股票财报底线。"
        else:
            fundamental_floor_detail = "当前以 ETF / 行业代理为主，利润同比底线暂未接入原始财报数据"
    else:
        fundamental_floor_detail = "当前已接入个股真实估值/财务快照；利润同比底线暂未单独作为硬排除项"
    checks.append({"name": "基本面底线", "status": "ℹ️", "detail": fundamental_floor_detail})
    valuation_snapshot = dict(fundamental_dimension.get("valuation_snapshot") or {})
    valuation_extreme = bool(fundamental_dimension.get("valuation_extreme"))
    pe_ttm = valuation_snapshot.get("pe_ttm")
    valuation_label = "个股估值" if asset_type == "cn_stock" else str(valuation_snapshot.get("display_label", "真实指数估值"))
    if valuation_extreme and pe_ttm is not None:
        checks.append(
            {
                "name": "估值极端",
                "status": "⚠️",
                "detail": (
                    f"{valuation_snapshot.get('index_name', '相关指数')} "
                    f"{valuation_snapshot.get('metric_label', '滚动PE')} {float(pe_ttm):.1f}x，已进入极高估值区"
                ),
            }
        )
        exclusion_reasons.append(f"{valuation_label}处于极高区间")
        warnings.append(f"⚠️ {valuation_label}已处于极高区间，后续更需要靠盈利兑现来消化估值")
    elif price_percentile > 0.90:
        checks.append({"name": "估值极端", "status": "⚠️", "detail": f"价格位置代理分位 {price_percentile:.0%}，接近极端高位"})
        exclusion_reasons.append("价格位置代理已处于极端高位")
        warnings.append("⚠️ 价格位置已在高位区，追高性价比明显下降")
    else:
        checks.append({"name": "估值极端", "status": "✅", "detail": f"价格位置代理分位 {price_percentile:.0%}"})

    if asset_type in {"cn_fund", "cn_etf", "cn_index"}:
        checks.append({"name": "解禁压力", "status": "✅", "detail": "基金/指数产品不适用限售股解禁压力"})
    elif asset_type == "cn_stock":
        unlock_snapshot = {
            "status": "ℹ️",
            "detail": "Tushare share_float 当前不可用，解禁压力暂未纳入本轮检查",
        }
        symbol = str(metadata.get("symbol", "") or metadata.get("code", "")).strip()
        if symbol:
            try:
                unlock_snapshot = ChinaMarketCollector(dict(context.get("config", {}))).get_unlock_pressure(symbol)
            except Exception:
                pass
        checks.append(
            {
                "name": "解禁压力",
                "status": str(unlock_snapshot.get("status", "ℹ️")),
                "detail": str(unlock_snapshot.get("detail", "Tushare share_float 当前不可用，解禁压力暂未纳入本轮检查")),
            }
        )
        if unlock_snapshot.get("status") == "❌":
            exclusion_reasons.append("未来 30 日存在大额解禁压力")
            warnings.append("⚠️ 未来 30 日存在大额限售股解禁，短线抛压风险明显上升")
        elif unlock_snapshot.get("status") == "⚠️":
            warnings.append("⚠️ 未来 30~90 日存在一定解禁压力，仓位与节奏需要更保守")
    else:
        checks.append({"name": "解禁压力", "status": "ℹ️", "detail": "当前仅接入 A 股 Tushare 解禁日历"})

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

    dmi = technical.get("dmi", {})
    adx = float(dmi.get("ADX", 0.0))
    plus_di = float(dmi.get("DI+", 0.0))
    minus_di = float(dmi.get("DI-", 0.0))
    bullish_trend = plus_di > minus_di
    adx_award = 20 if adx > 35 and bullish_trend else 15 if adx > 25 and bullish_trend else 0
    raw += adx_award
    available += 20
    direction_text = "多头占优" if bullish_trend else "空头占优"
    factors.append(_factor_row("ADX", f"ADX {adx:.1f} · {direction_text}", adx_award, 20, "这里只给顺趋势的强 ADX 加分；如果是空头强趋势，不作为做多加分项"))

    kdj = technical.get("kdj", {})
    k_value = float(kdj.get("K", 50.0))
    d_value = float(kdj.get("D", 50.0))
    j_value = float(kdj.get("J", 50.0))
    kdj_cross = str(kdj.get("cross", "neutral"))
    kdj_zone = str(kdj.get("zone", "neutral"))
    if kdj_cross == "golden_cross" and kdj_zone != "overbought":
        kdj_award = 10
    elif kdj_zone == "oversold" and kdj_cross != "death_cross":
        kdj_award = 7
    elif kdj_cross == "golden_cross":
        kdj_award = 5
    else:
        kdj_award = 0
    raw += kdj_award
    available += 10
    cross_text = {"golden_cross": "金叉", "death_cross": "死叉"}.get(kdj_cross, "未形成交叉")
    zone_text = {"overbought": "高位", "oversold": "低位"}.get(kdj_zone, "中性区")
    factors.append(_factor_row("KDJ", f"K {k_value:.1f} / D {d_value:.1f} / J {j_value:.1f} · {cross_text}", kdj_award, 10, f"KDJ 当前位于 {zone_text}；低位金叉更像回调结束，单纯高位金叉更像追高风险"))

    obv = technical.get("obv", {})
    obv_signal = str(obv.get("signal", "neutral"))
    obv_slope = float(obv.get("slope_5d", 0.0))
    obv_value = float(obv.get("OBV", 0.0))
    obv_ma = float(obv.get("MA", obv_value))
    if obv_signal == "bullish" and obv_slope > 0:
        obv_award = 10
    elif obv_slope > 0:
        obv_award = 5
    else:
        obv_award = 0
    raw += obv_award
    available += 10
    factors.append(_factor_row("OBV", f"OBV {'站上' if obv_value >= obv_ma else '跌破'}均线 · 5日斜率 {obv_slope:.0f}", obv_award, 10, "OBV 更像量价同向的确认因子；价格涨而 OBV 不跟，通常说明承接并不扎实"))

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

    volume_block = technical.get("volume", {})
    vol_ratio = float(volume_block.get("vol_ratio", technical.get("volume_ratio", 1.0)))
    vol_ratio_20 = float(volume_block.get("vol_ratio_20", vol_ratio))
    amount_ratio_20 = volume_block.get("amount_ratio_20")
    amount_ratio_20 = float(amount_ratio_20) if amount_ratio_20 is not None and amount_ratio_20 == amount_ratio_20 else None
    structure = str(volume_block.get("structure", volume_block.get("signal", "量价中性")))
    latest_return = float(volume_block.get("price_change_1d", float(history["close"].pct_change().iloc[-1]) if len(history) > 1 else 0.0))
    if structure == "放量突破":
        volume_award = 15
    elif structure in {"放量上攻", "缩量回调"}:
        volume_award = 12
    elif structure == "缩量上涨":
        volume_award = 6
    elif structure == "量价中性":
        volume_award = 4
    else:
        volume_award = 0
    raw += volume_award
    available += 15
    amount_text = f" / 成交额比20日 {amount_ratio_20:.2f}" if amount_ratio_20 is not None else ""
    factors.append(_factor_row("量价结构", f"{structure} · 量能比5日 {vol_ratio:.2f} / 20日 {vol_ratio_20:.2f}", volume_award, 15, f"这里先看日度量价结构：放量突破更像趋势确认，缩量回调更像抛压衰减，放量滞涨/放量下跌更像分歧扩大；当日涨跌幅 {latest_return:.1%}{amount_text}"))

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

    volatility = technical.get("volatility", {})
    vol_signal = str(volatility.get("signal", "neutral"))
    natr = float(volatility.get("NATR", 0.0))
    atr_ratio_20 = float(volatility.get("atr_ratio_20", 1.0))
    width_pct = float(volatility.get("boll_width_percentile", 0.5))
    if vol_signal == "compressed":
        vol_award = 10
    elif vol_signal == "neutral" and atr_ratio_20 <= 1.0:
        vol_award = 5
    else:
        vol_award = 0
    raw += vol_award
    available += 10
    factors.append(_factor_row("波动压缩", f"ATR/收盘 {natr:.2%} · 带宽分位 {width_pct:.0%}", vol_award, 10, "波动压缩更像筹码收敛后的启动前状态；如果 ATR 和布林带宽度同步扩张，通常意味着已经进入情绪释放阶段而不是舒服的低吸区"))

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


def _apply_history_fallback_adjustments(dimensions: Mapping[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Cap history-dependent dimensions when analysis falls back to realtime snapshot only."""
    capped: Dict[str, Dict[str, Any]] = {}
    caps = {
        "technical": 35,
        "relative_strength": 35,
        "risk": 25,
        "seasonality": 35,
    }
    for key, payload in dimensions.items():
        block = dict(payload)
        if key in caps and block.get("score") is not None:
            block["score"] = min(int(block["score"]), caps[key])
        if key in caps:
            detail = "未拿到完整日线历史，当前按本地实时快照降级生成分析，这一维分数只作参考。"
            factors = list(block.get("factors") or [])
            factors.insert(
                0,
                _factor_row(
                    "数据完整性",
                    "日线历史缺失，当前使用实时快照降级",
                    0,
                    0,
                    detail,
                    display_score="信息项",
                ),
            )
            block["factors"] = factors
            summary = str(block.get("summary", "")).strip()
            if detail not in summary:
                block["summary"] = (summary + " " + detail).strip()
        capped[key] = block
    return capped


def _fundamental_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    metrics: Mapping[str, float],
    config: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
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
    commodity_like_fund = _is_commodity_like_fund(asset_type, metadata, fund_profile)

    if commodity_like_fund:
        overview = dict((fund_profile or {}).get("overview") or {})
        style = dict((fund_profile or {}).get("style") or {})
        benchmark = str(overview.get("业绩比较基准", "")).strip() or str(overview.get("跟踪标的", "")).strip()
        fund_type = str(overview.get("基金类型", "")).strip()
        fund_type_award = 20 if any(token in fund_type for token in ("商品型", "期货")) else 10 if fund_type else 0
        raw += fund_type_award
        available += 20
        factors.append(
            _factor_row(
                "产品类型",
                fund_type or "未明确披露基金类型",
                fund_type_award if fund_type else None,
                20,
                "商品/期货 ETF 不按股票 PE、ROE 评估，先看产品类型和跟踪机制是否清晰。",
            )
        )

        benchmark_award = 20 if benchmark else 0
        raw += benchmark_award
        available += 20
        factors.append(
            _factor_row(
                "跟踪标的",
                benchmark or "未明确披露跟踪标的",
                benchmark_award if benchmark else None,
                20,
                "这类产品的基本面核心是跟踪什么、如何跟踪，而不是成分股盈利估值。",
            )
        )

        net_asset_text = str(overview.get("净资产规模", "")).strip()
        net_asset = _parse_chinese_amount(net_asset_text)
        scale_yi = (net_asset / 1e8) if net_asset is not None else None
        scale_award = None
        if scale_yi is not None:
            scale_award = 20 if scale_yi >= 10 else 12 if scale_yi >= 3 else 5 if scale_yi >= 1 else 0
            raw += scale_award
            available += 20
        factors.append(
            _factor_row(
                "产品规模",
                f"净资产约 {scale_yi:.2f} 亿" if scale_yi is not None else (net_asset_text or "缺失"),
                scale_award,
                20,
                "规模越大，通常越能支撑申赎效率、容量和跟踪稳定性。",
            )
        )

        cash_ratio = pd.to_numeric(pd.Series([style.get("cash_ratio")]), errors="coerce").dropna()
        cash_award = None
        cash_signal = "现金/保证金比例缺失"
        cash_detail = "商品/期货 ETF 通常保留较高现金或保证金仓位，这反映合约与保证金结构，不等于低仓位失真。"
        if not cash_ratio.empty:
            cash_value = float(cash_ratio.iloc[0])
            cash_award = 15 if 30 <= cash_value <= 98 else 8 if cash_value > 0 else 0
            raw += cash_award
            available += 15
            cash_signal = f"现金/保证金仓位约 {cash_value:.1f}%"
        factors.append(_factor_row("结构缓冲", cash_signal, cash_award, 15, cash_detail))

        factors.append(
            _factor_row(
                "价格位置",
                f"近一年价格分位 {price_percentile:.0%}",
                0,
                0,
                "这里只回答位置高不高；商品/期货 ETF 当前不使用股票 PE 作为基本面评分依据。",
                display_score="信息项",
            )
        )

        score = _normalize_dimension(raw, available, 100)
        summary = _dimension_summary(
            score,
            "产品结构和跟踪标的清晰，适合作为商品/期货 ETF 的基础质量判断。",
            "产品结构基本清楚，但容量或结构缓冲一般。",
            "产品结构信息偏弱或规模偏小，基础质量支撑有限。",
            "ℹ️ 商品/期货 ETF 基础资料缺失，本次未完成完整基本面维度。",
        )
        summary += " 当前按产品结构评估，不使用股票指数 PE、ROE、毛利率代理。"
        if benchmark:
            summary += f" 当前跟踪 `{benchmark}`；{valuation_note}"
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
            "valuation_snapshot": None,
            "valuation_history": valuation_history,
            "financial_proxy": {},
            "pe_percentile": None,
            "price_percentile": price_percentile,
            "valuation_note": "商品/期货 ETF 不使用股票估值代理。",
            "valuation_extreme": False,
        }

    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        collector = ValuationCollector(config)
        try:
            valuation_snapshot = collector.get_cn_index_snapshot(_valuation_keywords(metadata, asset_type, fund_profile))
        except Exception:
            valuation_snapshot = None
        if valuation_snapshot:
            try:
                valuation_history = collector.get_cn_index_value_history(str(valuation_snapshot.get("index_code", "")))
            except Exception:
                valuation_history = pd.DataFrame()
        try:
            if asset_type in {"cn_fund", "cn_etf"} and fund_profile:
                financial_proxy = _fund_financial_proxy(collector, fund_profile)
            elif valuation_snapshot:
                financial_proxy = collector.get_cn_index_financial_proxies(str(valuation_snapshot.get("index_code", "")), top_n=5)
        except Exception:
            financial_proxy = {}
        try:
            sector_flow = _sector_flow_snapshot(metadata, MarketDriversCollector(config).collect())
        except Exception:
            sector_flow = {}

    if asset_type == "cn_stock":
        collector = ValuationCollector(config)
        try:
            financial_proxy = collector.get_cn_stock_financial_proxy(symbol)
        except Exception:
            financial_proxy = {}
        stock_pe = financial_proxy.get("pe_ttm")
        if stock_pe is None:
            stock_pe = metadata.get("pe_ttm")
        if stock_pe is not None:
            try:
                pe_ttm = float(stock_pe)
                valuation_snapshot = {
                    "index_name": str(metadata.get("name", symbol)),
                    "pe_ttm": pe_ttm,
                    "metric_label": "滚动PE",
                }
            except (ValueError, TypeError):
                pass
        else:
            stock_dynamic_pe = metadata.get("pe_dynamic")
            if stock_dynamic_pe is not None:
                try:
                    valuation_note += f" 当前实时行情仅拿到动态 PE {float(stock_dynamic_pe):.1f}x，未将其当作滚动 PE 使用。"
                except (ValueError, TypeError):
                    pass
        try:
            sector_flow = _sector_flow_snapshot(metadata, MarketDriversCollector(config).collect())
        except Exception:
            sector_flow = {}

    # HK/US individual stocks: fetch real fundamentals via yfinance
    if asset_type in {"hk", "us"}:
        collector = ValuationCollector(config)
        try:
            yf_data = collector.get_yf_fundamental(symbol, asset_type)
        except Exception:
            yf_data = {}
        if yf_data.get("pe_ttm") is not None:
            valuation_snapshot = {
                "index_name": str(metadata.get("name", symbol)),
                "pe_ttm": yf_data["pe_ttm"],
                "metric_label": "滚动PE",
            }
        # Merge yfinance data into financial_proxy (roe, revenue_yoy, gross_margin)
        if yf_data:
            financial_proxy = {**financial_proxy, **{k: v for k, v in yf_data.items() if v is not None and k != "pe_ttm"}}

    proxy_labels = _fundamental_proxy_labels(asset_type)

    pe_ttm = None if not valuation_snapshot else valuation_snapshot.get("pe_ttm")
    if valuation_snapshot:
        match_note = str(valuation_snapshot.get("match_note", "")).strip()
        if match_note:
            valuation_note = f"{match_note} {valuation_note}"
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
        if asset_type == "cn_stock" and pe_percentile is None:
            # For individual stocks without PE history, score directly by PE level
            if pe_value <= 0:
                pe_award = 0  # negative PE = loss-making
            elif pe_value < 15:
                pe_award = 25  # value zone
            elif pe_value < 25:
                pe_award = 20  # reasonable
            elif pe_value < 40:
                pe_award = 10  # growth premium
            elif pe_value < 60:
                pe_award = 5   # expensive
            else:
                pe_award = 0   # extremely expensive
            detail = f"个股滚动 PE {pe_value:.1f}x，直接按绝对水平评分。"
        else:
            pe_award = 25 if pe_percentile is not None and pe_percentile < 0.30 else 10 if pe_percentile is not None and pe_percentile < 0.50 else 10 if pe_value < 20 else 0
            detail = "当前接入的是目标基准或最接近主题指数的滚动 PE；价格位置另算，不与估值分位混用。"
        raw += pe_award
        available += 25
        if pe_percentile is not None:
            detail += f" 近样本 PE 分位约 {pe_percentile:.0%}。"
        if dividend_yield is not None:
            detail += f" 当前股息率约 {dividend_yield:.2f}%。"
        factor_label = "个股估值" if asset_type == "cn_stock" else str(valuation_snapshot.get("display_label", "真实指数估值"))
        factors.append(
            _factor_row(
                factor_label,
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
                "当前未接入可用指数估值，只能用价格位置代理；价格分位不等于真实估值分位。",
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
    revenue_label = proxy_labels["growth"]
    proxy_scope_detail = proxy_labels["growth_scope"]
    if revenue_yoy is not None:
        revenue_award = 20 if float(revenue_yoy) >= 20 else 15 if float(revenue_yoy) >= 10 else 8 if float(revenue_yoy) >= 5 else 0
        raw += revenue_award
        available += 20
        factors.append(
            _factor_row(
                "盈利增速",
                f"{revenue_label} {float(revenue_yoy):.1f}%",
                revenue_award,
                20,
                f"{proxy_scope_detail}，缺失时回退到利润同比；覆盖权重约 {financial_proxy.get('coverage_weight', 0.0):.1f}%。",
            )
        )
    else:
        factors.append(_factor_row("盈利增速", "缺失", None, 20, "当前未接入对应指数/行业或重仓股的营收同比代理"))

    roe_value = financial_proxy.get("roe")
    if roe_value is not None:
        roe_award = 20 if float(roe_value) >= 15 else 10 if float(roe_value) >= 10 else 0
        raw += roe_award
        available += 20
        factors.append(
            _factor_row(
                "ROE",
                f"{proxy_labels['roe']} {float(roe_value):.1f}%",
                roe_award,
                20,
                f"财务代理最新报告期 {financial_proxy.get('report_date') or '未知'}。",
            )
        )
    else:
        factors.append(_factor_row("ROE", "缺失", None, 20, "当前未接入对应指数/行业或重仓股的 ROE 代理"))

    gross_margin = financial_proxy.get("gross_margin")
    if gross_margin is not None:
        margin_award = 15 if float(gross_margin) >= 30 else 10 if float(gross_margin) >= 20 else 0
        raw += margin_award
        available += 15
        factors.append(
            _factor_row(
                "毛利率",
                f"{proxy_labels['margin']} {float(gross_margin):.1f}%",
                margin_award,
                15,
                "用重仓股/成分股加权毛利率代理行业定价权和成本结构。",
            )
        )
    else:
        factors.append(_factor_row("毛利率", "缺失", None, 15, "当前未接入对应行业或重仓股毛利率代理"))

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
                f"用真实指数 PE 除以{proxy_labels['peg_base']}增速代理，回答'增长是否已经被定价'。",
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
    # When data coverage is very low (proxy-only, e.g. HK/US stocks with no PE/ROE data),
    # cap the score. With only price-percentile proxy (available=25), normalization would
    # give 100/100 which severely distorts rankings. Cap at 55 to keep it below the "strong
    # fundamental" threshold used in rating logic.
    if score is not None and available < 35:
        score = min(score, 55)
    is_single_stock = asset_type in {"cn_stock", "hk", "us"}
    summary = _dimension_summary(
        score,
        "个股估值/财务快照偏正面，基本面支撑存在。" if is_single_stock else "估值/资金承接代理偏正面，但当前仍是 ETF/行业代理视角。",
        "个股基本面暂无明显低估或高估结论。" if is_single_stock else "基本面代理没有明显便宜或显著昂贵结论。",
        "个股估值偏高或财务安全边际不足。" if is_single_stock else "估值代理偏高，基本面安全边际不足。",
        "ℹ️ 个股基本面数据缺失，本次评级未纳入完整基本面维度" if is_single_stock else "ℹ️ 基本面数据缺失，本次评级未纳入完整基本面维度",
    )
    if score is not None and available < 35:
        summary += " 当前仅基于代理因子归一化评分。"
    if valuation_snapshot and pe_ttm is not None:
        summary += (
            f" 当前已接入 `{valuation_snapshot.get('index_name', '')}` "
            f"{valuation_snapshot.get('metric_label', '滚动PE')} {float(pe_ttm):.1f}x；{valuation_note}"
        )
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


def _catalyst_dimension(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    evidence_rows: List[Dict[str, str]] = []
    raw = 0
    available = 0
    config = dict(context.get("config", {}))
    reference_now = _context_now(context)
    profile = _catalyst_profile(metadata, config, fund_profile)
    asset_type_str = str(metadata.get("asset_type", ""))
    commodity_like_fund = _is_commodity_like_fund(asset_type_str, metadata, fund_profile)
    news_report = dict(context.get("news_report", {}) or {})
    news_mode = str(news_report.get("mode") or ("live" if news_report else "unknown"))
    news_items = news_report.get("all_items") or news_report.get("items", [])
    stock_news_items: List[Mapping[str, Any]] = []
    if asset_type_str == "cn_stock" and news_mode == "live":
        try:
            stock_news_items = NewsCollector(config).get_stock_news(str(metadata.get("symbol", "")))
        except Exception:
            stock_news_items = []
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
    calendar_forward_events = _company_forward_events(metadata, context)
    category_related_news = [
        item
        for item in news_items
        if _category_item_is_relevant(item, metadata, profile, allowed_categories, related_tokens, strict_tokens)
    ]
    dynamic_related_news: List[Mapping[str, Any]] = []
    if (
        len(strict_related_news) + len(category_related_news) < 2
        and not commodity_like_fund
        and not calendar_forward_events
        and not stock_news_items
        and news_mode == "live"
    ):
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

    # For cn_stock: inject per-stock news from akshare (东方财富个股新闻).
    if asset_type_str == "cn_stock":
        if stock_news_items:
            news_pool = _dedupe_news_items([*news_pool, *stock_news_items])

    related_events = []
    for event in context.get("events", []):
        text = f"{event.get('title', '')} {event.get('note', '')}"
        if _contains_any(text, [*keyword_keys, *event_keys, *domestic_leaders, *overseas_leaders]):
            related_events.append(event)

    # For individual stocks: distinguish company-specific news from sector-level news.
    # Sector-level news (e.g. "AI政策利好科技板块") should not get full policy-catalyst credit
    # unless the specific stock/company is mentioned by name.
    # Applies to cn_stock, hk, and us — ETFs/indexes/funds stay sector-level scoring.
    _individual_asset_types = {"cn_stock", "hk", "us"}
    is_individual_stock = str(metadata.get("asset_type", "")) in _individual_asset_types
    stock_name_tokens = _stock_name_tokens(metadata) if is_individual_stock else []
    stock_specific_pool = (
        [item for item in news_pool if _contains_any(_headline_text(item), stock_name_tokens)]
        if stock_name_tokens else news_pool
    )
    existing_forward_events = _dedupe_news_items(
        [
            *calendar_forward_events,
            *_company_forward_events(
                metadata,
                context,
                news_items=stock_specific_pool if stock_specific_pool else news_pool,
            ),
        ]
    )
    # For HK/US individual stocks: proactively search direct company-event headlines when
    # broad market news does not already produce high-confidence company evidence.
    company_positive_pool = stock_specific_pool
    if asset_type_str in {"hk", "us"} and stock_name_tokens:
        company_positive_pool = [item for item in stock_specific_pool if _is_high_confidence_company_news(item)]
        if not company_positive_pool:
            try:
                hk_us_news = _search_high_confidence_company_news(
                    metadata,
                    profile,
                    config,
                    recent_days=DIRECT_COMPANY_NEWS_LOOKBACK_DAYS,
                )
            except Exception:
                hk_us_news = []
            if hk_us_news:
                news_pool = _dedupe_news_items([*news_pool, *hk_us_news])
                stock_specific_pool = [item for item in news_pool if _contains_any(_headline_text(item), stock_name_tokens)]
                company_positive_pool = [item for item in stock_specific_pool if _is_high_confidence_company_news(item)]
    company_specific_news_available = bool(company_positive_pool) if asset_type_str in {"hk", "us"} and stock_name_tokens else True

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
    if asset_type_str in {"hk", "us"} and stock_name_tokens:
        specific_policy_items = [item for item in policy_items if item in company_positive_pool]
        policy_pick = _pick_best_news_item(specific_policy_items, policy_keys, stock_name_tokens or keyword_keys)
        policy_award = 10 if specific_policy_items else 0
    elif is_individual_stock and stock_name_tokens:
        # cn_stock: full 30pts only when the policy news names the company directly;
        # sector-level policy (e.g. industry-wide AI/tech support) gets only 10pts.
        specific_policy_items = [item for item in policy_items if _contains_any(_headline_text(item), stock_name_tokens)]
        policy_award = 30 if specific_policy_items else (10 if policy_items else 0)
    else:
        policy_award = 30 if policy_items else 0
    if asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available:
        policy_pick = None
        policy_award = 0
    # For cn_stock with per-stock news: redistribute weights (policy 25, leader 15, new factor 15)
    _policy_max = 25 if (asset_type_str == "cn_stock" and stock_name_tokens) else 30
    policy_award = min(policy_award, _policy_max)
    raw += policy_award
    available += _policy_max
    policy_signal = (
        "未命中高置信个股直连新闻，个股催化暂不计分"
        if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available)
        else (policy_pick["title"] if policy_award > 0 and policy_pick else "近 7 日未命中直接政策催化")
    )
    policy_detail = "当前未命中 Reuters/Bloomberg/FT/公司公告 这类高置信个股直连标题，避免把市场级新闻误记成个股催化。" if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available) else "政策原文和一级媒体优先"
    factors.append(_factor_row("政策催化", policy_signal, policy_award, _policy_max, policy_detail))
    if policy_award > 0 and policy_pick:
        evidence_rows.append(_evidence_row(layer="政策催化", item=policy_pick))

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
        and not _is_non_positive_company_statement(item)
    ]
    stock_specific_leader_items: List[Mapping[str, Any]] = []
    leader_pick = _pick_best_news_item(leader_items, [*domestic_leaders, *strict_event_keys], keyword_keys)
    if is_individual_stock and company_positive_pool:
        stock_specific_leader_items = [
            item
            for item in company_positive_pool
            if _contains_any(
                _headline_text(item),
                [
                    *strict_event_keys,
                    "订单",
                    "扩产",
                    "投产",
                    "回购",
                    "并购",
                    "重组",
                    "指引",
                    "扩建",
                    "量产",
                    "涨价",
                    "业绩",
                    "财报",
                    "earnings",
                    "guidance",
                    "buyback",
                    "outlook",
                    "financial outlook",
                    "results",
                    "quarterly results",
                    "forecast",
                    "partnership",
                    "collaboration",
                    "expansion",
                ],
            )
            and not _is_non_positive_company_statement(item)
        ]
        if stock_specific_leader_items:
            leader_pick = _pick_best_news_item(stock_specific_leader_items, [*keyword_keys, *strict_event_keys], stock_name_tokens or keyword_keys)
    _leader_max = 15 if (asset_type_str == "cn_stock" and stock_name_tokens) else 25
    if asset_type_str in {"hk", "us"} and stock_name_tokens:
        leader_award = _leader_max if stock_specific_leader_items else 0
        if not company_specific_news_available:
            leader_pick = None
    else:
        leader_award = _leader_max if (leader_items or stock_specific_leader_items) else 0
    raw += leader_award
    available += _leader_max
    leader_signal = (
        "未命中高置信个股直连新闻，个股催化暂不计分"
        if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available)
        else (leader_pick["title"] if leader_award > 0 and leader_pick else "未命中直接龙头公告")
    )
    leader_detail = "当前未命中 Reuters/Bloomberg/FT/公司公告 这类高置信业绩/公告标题，避免把行业级消息误映射到单一个股。" if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available) else "优先看订单、扩产、回购、并购或超预期业绩"
    factors.append(_factor_row("龙头公告/业绩", leader_signal, leader_award, _leader_max, leader_detail))
    if leader_award > 0 and leader_pick:
        evidence_rows.append(_evidence_row(layer="龙头公告/业绩", item=leader_pick))

    structured_event_pool = _structured_company_event_items(
        metadata,
        context,
        news_items=company_positive_pool if (asset_type_str in {"hk", "us"} and stock_name_tokens) else (stock_specific_pool if stock_specific_pool else news_pool),
        stock_news_items=stock_news_items,
    )
    structured_direct_pool = [
        item
        for item in structured_event_pool
        if str(item.get("category", "")).lower() != "earnings_calendar" and not _is_non_positive_company_statement(item)
    ]
    structured_pick = _pick_best_news_item(
        structured_direct_pool or structured_event_pool,
        [*earnings_keys, *event_keys, *STRUCTURED_COMPANY_EVENT_KEYS],
        stock_name_tokens or keyword_keys,
    )
    if structured_direct_pool:
        structured_award = 15
        structured_detail = "先看公告、财报、订单、回购、合作这类结构化公司事件；这类证据比泛行业新闻更接近可执行催化。"
    elif structured_event_pool:
        structured_award = 8
        structured_detail = f"当前只命中财报日历/披露窗口等结构化事件，属于催化线索已出现，但还没到强催化共识。"
    else:
        structured_award = 0
        structured_detail = "当前未命中结构化公司事件；这里按信息不足处理，不直接等于个股没有催化。"
    raw += structured_award
    available += 15
    factors.append(
        _factor_row(
            "结构化事件",
            structured_pick["title"] if structured_pick else "未命中明确结构化公司事件",
            structured_award,
            15,
            structured_detail,
        )
    )
    if structured_award > 0 and structured_pick:
        evidence_rows.append(_evidence_row(layer="结构化事件", item=structured_pick))

    if is_individual_stock:
        negative_pick: Optional[Mapping[str, Any]] = None
        raw_stock_specific_news = (
            [
                item
                for item in news_items
                if _contains_any(_headline_text(item), stock_name_tokens)
            ]
            if stock_name_tokens
            else []
        )
        negative_pool = _dedupe_news_items([*stock_specific_pool, *raw_stock_specific_news]) if stock_name_tokens else []
        if asset_type_str == "cn_stock" and stock_news_items:
            stock_scoped_negatives = [
                item
                for item in stock_news_items
                if _contains_any(_headline_text(item), NEGATIVE_DILUTION_KEYS)
                or (stock_name_tokens and _contains_any(_headline_text(item), stock_name_tokens))
            ]
            negative_pool = _dedupe_news_items([*negative_pool, *stock_scoped_negatives])
        negative_items = [
            item
            for item in negative_pool
            if _contains_any(_headline_text(item), [*NEGATIVE_DILUTION_KEYS, *NEGATIVE_REGULATORY_KEYS])
            and (not stock_name_tokens or _contains_any(_headline_text(item), stock_name_tokens))
        ]
        if not negative_items and stock_name_tokens and asset_type_str in {"hk", "us"}:
            try:
                older_company_news = []
                collector = NewsCollector(config)
                for keywords in _direct_company_negative_search_terms(metadata):
                    hits = collector.search_by_keywords(
                        keywords,
                        preferred_sources=_preferred_catalyst_sources(metadata, profile),
                        limit=8,
                        recent_days=NEGATIVE_EVENT_LOOKBACK_DAYS,
                    )
                    older_company_news.extend(hits)
                    if any(_contains_any(_headline_text(item), [*NEGATIVE_DILUTION_KEYS, *NEGATIVE_REGULATORY_KEYS]) for item in hits):
                        break
            except Exception:
                older_company_news = []
            if older_company_news:
                negative_pool = _dedupe_news_items([*negative_pool, *older_company_news])
                negative_items = [
                    item
                    for item in negative_pool
                    if _contains_any(_headline_text(item), [*NEGATIVE_DILUTION_KEYS, *NEGATIVE_REGULATORY_KEYS])
                    and (not stock_name_tokens or _contains_any(_headline_text(item), stock_name_tokens))
                ]

        negative_signals = []
        for item in negative_items:
            penalty, date_detail = _negative_event_penalty(item, reference_now)
            if penalty <= 0:
                continue
            negative_signals.append((item, penalty, date_detail))

        if negative_signals:
            strongest_penalty = max(signal[1] for signal in negative_signals)
            strongest_items = [signal for signal in negative_signals if signal[1] == strongest_penalty]
            negative_pick_candidate = _pick_best_news_item(
                [signal[0] for signal in strongest_items],
                [*NEGATIVE_DILUTION_KEYS, *NEGATIVE_REGULATORY_KEYS],
                keyword_keys,
            )
            negative_pick, penalty, date_detail = next(
                (signal for signal in strongest_items if signal[0] == negative_pick_candidate),
                strongest_items[0],
            )
            negative_text = _headline_text(negative_pick)
            label = "稀释事件" if _contains_any(negative_text, NEGATIVE_DILUTION_KEYS) else "监管/合规风险"
            raw -= penalty
            factors.append(
                _factor_row(
                    "负面事件",
                    str(negative_pick.get("title", "")).strip(),
                    0,
                    15,
                    f"{date_detail} 命中 `{label}` 关键词，容易直接压制催化兑现和风险偏好。",
                    display_score=f"-{penalty}",
                )
            )
            evidence_rows.append(_evidence_row(layer="负面事件", item=negative_pick))
        else:
            factors.append(
                _factor_row(
                    "负面事件",
                    f"近 {NEGATIVE_EVENT_LOOKBACK_DAYS} 日未命中明确稀释/监管负面",
                    0,
                    15,
                    "当前未识别到会直接压制催化兑现的个股负面事件。",
                    display_score="信息项",
                )
            )

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
    if asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available:
        overseas_pick = None
        overseas_award = 0
    raw += overseas_award
    available += 20
    overseas_signal = (
        "未命中高置信个股直连新闻，海外映射暂不计分"
        if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available)
        else (overseas_pick["title"] if overseas_award > 0 and overseas_pick else "未命中直接海外映射")
    )
    overseas_detail = "当前未命中与公司直接相关的高置信海外映射新闻，避免把行业级海外消息直接算成个股催化。" if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available) else "重点看海外龙头财报/指引或模型产品催化"
    factors.append(_factor_row("海外映射", overseas_signal, overseas_award, 20, overseas_detail))
    if overseas_award > 0 and overseas_pick:
        evidence_rows.append(_evidence_row(layer="海外映射", item=overseas_pick))

    # For individual stocks: density and heat only count articles that directly mention the stock.
    # This prevents sector-level news (e.g. broad AI/tech news) from inflating density scores.
    density_pool = company_positive_pool if (asset_type_str in {"hk", "us"} and stock_name_tokens) else (stock_specific_pool if (is_individual_stock and stock_name_tokens) else news_pool)
    density_count = len(density_pool)
    density_label = f"个股相关头条 {density_count} 条（行业头条 {len(news_pool)} 条）" if (is_individual_stock and stock_name_tokens) else f"相关头条 {len(news_pool)} 条"
    density_award = 10 if density_count >= 2 else (5 if density_count >= 1 else 0)
    raw += density_award
    available += 10
    factors.append(_factor_row("研报/新闻密度", density_label, density_award, 10, "个股直接提及的一级媒体头条密度"))

    heat_pool = company_positive_pool if (asset_type_str in {"hk", "us"} and stock_name_tokens) else (stock_specific_pool if (is_individual_stock and stock_name_tokens) else news_pool)
    source_count = len({str(item.get("source", "")) for item in heat_pool if item.get("source")})
    heat_award = 10 if source_count >= 2 else 0
    raw += heat_award
    available += 10
    factors.append(_factor_row("新闻热度", f"覆盖源 {source_count} 个", heat_award, 10, "从少量提及到多源同步，是热度拐点的代理"))

    forward_events = _dedupe_news_items([*related_events, *existing_forward_events])
    forward_award = 5 if forward_events else 0
    raw += forward_award
    available += 5
    factors.append(
        _factor_row(
            "前瞻催化",
            forward_events[0]["title"] if forward_events else f"未来 {FORWARD_EVENT_LOOKAHEAD_DAYS} 日未命中直接催化事件",
            forward_award,
            5,
            "未来财报/发布会/事件窗口已纳入；HK/US 个股优先读取公司级财报日历。",
        )
    )
    if forward_award > 0 and forward_events:
        evidence_rows.append(_evidence_row(layer="前瞻催化", item=forward_events[0]))

    score = _normalize_dimension(raw, available, 100)
    catalyst_coverage = {
        "news_mode": news_mode,
        "high_confidence_company_news": bool(company_positive_pool),
        "structured_event": bool(structured_event_pool),
        "forward_event": bool(forward_events),
        "news_pool_count": len(news_pool),
        "direct_news_count": len(company_positive_pool) if (asset_type_str in {"hk", "us"} and stock_name_tokens) else len(stock_specific_pool if stock_specific_pool else news_pool),
        "source_count": source_count,
        "degraded": news_mode != "live",
    }
    if score is None:
        summary = "ℹ️ 催化面数据缺失，本次评级未纳入该维度"
    elif structured_event_pool and score < 40:
        summary = "结构化事件已出现，但高质量公司级新闻确认还不够，当前更像事件在前、市场共识在后。"
    elif is_individual_stock and not structured_event_pool and not company_positive_pool and score < 40:
        summary = "当前未抓到高质量公司级新闻或结构化事件，先按信息不足处理，不直接视为利空。"
    else:
        summary = _dimension_summary(score, "催化明确，市场有理由重新定价。", "有催化苗头，但强度还不够形成一致预期。", "催化不足，当前更像静态博弈。", "ℹ️ 催化面数据缺失，本次评级未纳入该维度")
    return {
        "name": "催化面",
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _catalyst_core_signal(factors, stock_specific_pool, company_positive_pool, is_individual_stock, asset_type_str),
        "missing": score is None,
        "profile_name": profile.get("profile_name", sector),
        "coverage": catalyst_coverage,
        "evidence": _dedupe_news_items(evidence_rows),
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
        # Turnaround (20d negative → 5d positive) is most valuable; persistent excess is good too.
        # Bonus: large 5d excess (>5%) adds 5pts regardless of 20d direction.
        if rel_5d > 0 and rel_20d <= 0:
            turn_award = 30
        elif rel_20d > 0 and rel_5d > 0.05:
            turn_award = 25  # strong persistent outperformance
        elif rel_20d > 0:
            turn_award = 20
        else:
            turn_award = 0
        raw += turn_award
        available += 30
        factors.append(_factor_row("超额拐点", f"相对基准 5日 {format_pct(rel_5d)} / 20日 {format_pct(rel_20d)}", turn_award, 30, "相对基准从负转正更接近轮动切换窗口"))
    else:
        factors.append(_factor_row("超额拐点", "缺失", None, 30, "基准收益序列缺失，未计算超额拐点"))

    board_move = _sector_board_match(metadata, context.get("drivers", {}))
    if board_move is not None:
        # Lowered threshold: 0.3% sector gain already qualifies as meaningful breadth.
        # Previous threshold of 1% was too strict for normal A-share sector rotations.
        breadth_award = 25 if board_move > 0.003 else 10 if board_move > 0 else 0
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
    commodity_like_fund = _is_commodity_like_fund(asset_type, metadata, context.get("fund_profile"))
    drivers = dict(context.get("drivers", {}))
    sector_flow = _sector_flow_snapshot(metadata, drivers)
    northbound = _northbound_sector_snapshot(metadata, drivers)
    hot_rank = _hot_rank_snapshot(metadata, drivers)
    concentration_proxy: Dict[str, Any] = {}
    if asset_type in {"cn_etf", "cn_index", "cn_fund"} and not commodity_like_fund:
        try:
            snapshot = ValuationCollector(config).get_cn_index_snapshot(_valuation_keywords(metadata))
            if snapshot:
                concentration_proxy = ValuationCollector(config).get_cn_index_financial_proxies(str(snapshot.get("index_code", "")), top_n=5)
        except Exception:
            concentration_proxy = {}

    heat_rank = hot_rank.get("rank") if not commodity_like_fund else None
    if heat_rank is not None:
        crowding_award = 30 if float(heat_rank) > 50 else 15 if float(heat_rank) > 20 else 0
        # Check whether the hot_rank row matched the individual stock or just the sector.
        # If the matched row name doesn't contain the stock name, it's a sector-level proxy.
        hot_rank_name = str(hot_rank.get("name", ""))
        stock_nm = str(metadata.get("name", ""))
        is_stock_level_rank = stock_nm and stock_nm[:2] in hot_rank_name
        signal = f"热门度排名约 {int(float(heat_rank))}" if is_stock_level_rank else f"行业热门度约 {int(float(heat_rank))}（板块代理）"
        detail = "当前用热门榜位置做公募/热度代理；排名越靠后，说明没那么拥挤。" if is_stock_level_rank else "当前热门榜未匹配到个股，改用板块热门度做代理，区分度有限。"
        raw += crowding_award
        available += 30
        factors.append(_factor_row("公募/热度代理", signal, crowding_award, 30, detail))
    elif sector_flow and not commodity_like_fund:
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

    if asset_type == "cn_stock":
        holdertrade = _cn_holdertrade_snapshot(metadata, context)
        if holdertrade:
            direction = str(holdertrade.get("direction", ""))
            net_ratio = float(holdertrade.get("net_ratio") or 0.0)
            if direction == "increase":
                insider_award = 10 if net_ratio >= 0.1 else 5
                raw += insider_award
                available += 10
                factors.append(
                    _factor_row(
                        "高管增持",
                        str(holdertrade.get("item", {}).get("title", "近 90 日存在净增持")),
                        insider_award,
                        10,
                        "高管/大股东净增持通常更接近管理层与重要股东的内部态度。",
                    )
                )
            else:
                factors.append(
                    _factor_row(
                        "高管增持",
                        str(holdertrade.get("item", {}).get("title", "近 90 日存在净减持")),
                        0,
                        10,
                        "近 90 日净减持更偏负面，不在筹码维度做正向加分。",
                        display_score="信息项",
                    )
                )
        else:
            factors.append(_factor_row("高管增持", "近 90 日未命中明确高管/大股东增减持", 0, 10, "当前未识别到可明确归因的股东增减持信号。", display_score="信息项"))
    else:
        factors.append(_factor_row("高管增持", "ETF / 指数产品不适用", 0, 0, "该因子主要适用于个股，不纳入 ETF 评分。", display_score="不适用"))

    if asset_type == "cn_etf" and commodity_like_fund:
        factors.append(_factor_row("北向/南向", "商品/期货 ETF 不适用", 0, 0, "北向资金是股票市场口径，不用于商品/期货 ETF。", display_score="不适用"))
    elif asset_type in {"cn_etf", "cn_stock"}:
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
                if asset_type == "cn_stock":
                    factors.append(
                        _factor_row(
                            "北向/南向",
                            "行业/个股级北向数据缺失，未再回退全市场总量",
                            0,
                            20,
                            "为避免把全市场北向总额误写成个股优势，这里只做信息披露，不做加分。",
                            display_score="信息项",
                        )
                    )
                else:
                    flow = ChinaMarketCollector(config).get_north_south_flow()
                    value = 0.0
                    if not flow.empty and {"日期", "北向资金净流入"}.issubset(flow.columns):
                        latest = flow.sort_values("日期").iloc[-1]
                        value = float(
                            pd.to_numeric(pd.Series([latest.get("北向资金净流入")]), errors="coerce").fillna(0.0).iloc[0]
                        )
                    north_award = 20 if value > 0 else 0
                    raw += north_award
                    available += 20
                    factors.append(
                        _factor_row(
                            "北向/南向",
                            f"北向净买额约 {_fmt_yi_number(value)}",
                            north_award,
                            20,
                            "行业北向缺失，回退到全市场方向代理；该项更多用于 ETF/市场方向，不直接解释成单一个股优势。",
                        )
                    )
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
            if sector_flow and not commodity_like_fund:
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
    elif asset_type == "cn_stock":
        if sector_flow:
            chips_award_stock = 10 if float(sector_flow.get("main_flow") or 0.0) > 0 else 0
            raw += chips_award_stock
            available += 10
            factors.append(
                _factor_row(
                    "机构资金承接",
                    f"{sector_flow.get('name') or metadata.get('sector', '行业')} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                    chips_award_stock,
                    10,
                    "个股资金流用所属行业主力资金流方向代理。",
                )
            )
        else:
            factors.append(_factor_row("机构资金承接", "缺失", None, 10, "个股所属行业资金流数据缺失"))
    else:
        factors.append(_factor_row("机构资金承接", "该项不适用", None, 10, "当前只对 A 股 ETF 接稳定资金流代理"))

    top_concentration = concentration_proxy.get("top_concentration") if not commodity_like_fund else None
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
        if commodity_like_fund:
            factors.append(_factor_row("机构集中度代理", "商品/期货 ETF 不适用", 0, 0, "这类产品不按股票成分股集中度衡量筹码结构。", display_score="不适用"))
        else:
            factors.append(_factor_row("机构集中度代理", "缺失", None, 15, "成分股权重集中度暂未接入"))

    score = _normalize_dimension(raw, available, 100)
    proxy_only_individual = asset_type in {"cn_stock", "hk", "us"} and available < 40
    if score is not None and proxy_only_individual:
        score = min(score, 55)
    summary = _dimension_summary(score, "聪明钱方向偏正面。", "筹码结构没有形成明确增量共识。", "聪明钱没有明显站在这一边。", "ℹ️ 筹码结构数据缺失，本次评级未纳入该维度")
    if proxy_only_individual and score is not None:
        summary += " 当前更多是行业/市场级筹码代理，不把它当成单一个股已经被资金充分确认。"
    return {
        "name": "筹码结构",
        "score": score,
        "max_score": 100,
        "summary": summary,
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


def _recent_high_recovery_signal(close: pd.Series, lookback: int = 252) -> tuple[Optional[int], Optional[float], int, str, str]:
    if len(close) < 80:
        return None, None, 0, "近一年修复样本不足", "历史样本不足，未计算近一年高点后的修复速度。"

    trailing = close.tail(min(len(close), lookback)).reset_index(drop=True).astype(float)
    running_peak = trailing.cummax()
    drawdown = trailing / running_peak - 1
    trough_idx = int(drawdown.idxmin())
    if trough_idx <= 0 or trough_idx >= len(trailing) - 1:
        return None, None, 0, "近一年未形成有效回撤样本", "近一年没有形成可用于评估恢复速度的完整回撤。"

    peak_level = float(running_peak.iloc[trough_idx])
    trough_level = float(trailing.iloc[trough_idx])
    drawdown_gap = peak_level - trough_level
    if peak_level <= 0 or drawdown_gap <= 0:
        return None, None, 0, "近一年未形成有效回撤样本", "近一年没有形成可用于评估恢复速度的完整回撤。"

    recovery_target = trough_level + drawdown_gap * 0.5
    recover = trailing.iloc[trough_idx:]
    recovered = recover[recover >= recovery_target]
    recovery_days = int(recovered.index[0] - trough_idx) if not recovered.empty else 999
    recovery_ratio = max(0.0, min(1.0, (float(trailing.iloc[-1]) - trough_level) / drawdown_gap))

    if recovery_days != 999 and recovery_days < 60:
        award = 15
        signal = f"近一年高点后 {recovery_days} 日修复过半"
        detail = "相对近一年高点形成的主要回撤，恢复越快说明趋势韧性越强。"
    elif recovery_days != 999:
        award = 10
        signal = f"近一年高点后 {recovery_days} 日修复过半"
        detail = "已经完成至少一半修复，但速度不算快。"
    elif recovery_ratio >= 0.35:
        award = 5
        signal = f"近一年高点后已修复 {recovery_ratio:.0%}"
        detail = "虽然还没修复过半，但相对近一年主要回撤已经有一定恢复。"
    else:
        award = 0
        signal = f"近一年高点后已修复 {recovery_ratio:.0%}"
        detail = "相对近一年主要回撤，修复仍偏弱。"

    return recovery_days, recovery_ratio, award, signal, detail


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


def _risk_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    history: pd.DataFrame,
    asset_returns: pd.Series,
    context: Mapping[str, Any],
    correlation_pair: Optional[tuple[str, float]],
) -> Dict[str, Any]:
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

    _, _, recovery_award, recovery_signal, recovery_detail = _recent_high_recovery_signal(close)
    raw += recovery_award
    available += 15
    factors.append(_factor_row("回撤恢复", recovery_signal, recovery_award, 15, recovery_detail))

    if correlation_pair is None:
        factors.append(_factor_row("组合分散", "缺失", None, 10, "watchlist 相关性序列不足"))
    else:
        peer, corr = correlation_pair
        div_award = 10 if abs(corr) < 0.5 else 5 if abs(corr) < 0.75 else 0
        raw += div_award
        available += 10
        factors.append(_factor_row("组合分散", f"与 {peer} 相关性 {corr:.2f}", div_award, 10, "相关性越低，越有真正分散价值"))

    if asset_type in {"cn_stock", "hk", "us"}:
        stock_name_tokens = _stock_name_tokens(metadata)
        disclosure_pool: List[Mapping[str, Any]] = []
        news_items = context.get("news_report", {}).get("all_items") or context.get("news_report", {}).get("items", [])
        if stock_name_tokens:
            disclosure_pool.extend([item for item in news_items if _is_disclosure_like_item(item, stock_name_tokens)])
        stock_news_items: List[Mapping[str, Any]] = []
        if asset_type == "cn_stock":
            try:
                stock_news_items = NewsCollector(dict(context.get("config", {}))).get_stock_news(symbol)
                stock_disclosure_items = [
                    item
                    for item in stock_news_items
                    if _is_disclosure_like_item(item, stock_name_tokens)
                ]
                disclosure_pool = _dedupe_news_items([*disclosure_pool, *stock_disclosure_items])
            except Exception:
                pass
        disclosure_pool = _dedupe_news_items(
            [
                *disclosure_pool,
                *_company_forward_events(metadata, context, news_items=disclosure_pool, extra_items=stock_news_items),
            ]
        )
        disclosure_signal = _disclosure_window_signal(disclosure_pool, _context_now(context))
        if disclosure_signal:
            penalty = int(disclosure_signal["penalty"])
            raw -= penalty
            factors.append(
                _factor_row(
                    "披露窗口",
                    str(disclosure_signal["signal"]),
                    0,
                    penalty,
                    str(disclosure_signal["detail"]),
                    display_score=f"-{penalty}",
                )
            )
        else:
            factors.append(
                _factor_row(
                    "披露窗口",
                    f"近 {FORWARD_EVENT_LOOKAHEAD_DAYS} 日未命中明确财报/年报事件窗口",
                    0,
                    10,
                    "当前未识别到会明显放大波动的披露窗口。",
                    display_score="信息项",
                )
            )

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
    elif (ok(tech, 70) and catalyst is not None and catalyst < 50) or (ok(catalyst, 60) and tech is not None and tech < 40):
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
    if asset_type == "cn_stock":
        return f"这是一只 A 股个股，所属行业为 `{sector}`，需要关注个股层面的业绩兑现与公司治理风险。"
    if asset_type == "cn_etf":
        benchmark = str(metadata.get("benchmark", "")).strip()
        if _is_commodity_like_fund(asset_type, metadata, None):
            if benchmark:
                return f"这只 ETF 本质上是在买 `{benchmark}` 对应的商品/期货暴露，核心看跟踪效率、申赎容量和商品价格方向，而不是股票成分股盈利。"
            return f"这只 ETF 本质上是在买 `{sector}` 方向的商品/期货暴露，核心看跟踪效率和商品价格方向。"
        if benchmark:
            return f"这只 ETF 更像在买 `{sector}` 方向的被动暴露，核心看跟踪标的 `{benchmark}` 及其成分权重，而不是泛泛地看整个市场。"
        return f"这只 ETF 本质上更像在买 `{sector}` 方向的核心风格暴露，而不是无差别买整个市场。"
    if asset_type in {"us", "hk", "hk_index"}:
        return f"这只 ETF 本质上更像在买 `{sector}` 方向的核心风格暴露，而不是无差别买整个市场。"
    if asset_type == "cn_fund":
        if bool(metadata.get("is_passive_fund")):
            return f"这只场外基金本质上更像在买 `{sector}` 方向的被动暴露，核心看跟踪误差、费率和标的本身。"
        manager_name = str(metadata.get("manager_name", "")).strip()
        if manager_name:
            return f"这只场外基金本质上是在买基金经理 `{manager_name}` 的主动选股框架，以及当前持仓所暴露的 `{sector}` 风格。"
        return f"这只场外基金本质上是在买基金经理的主动选股框架，以及当前持仓所暴露的 `{sector}` 风格。"
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
    fund_profile: Optional[Mapping[str, Any]] = None,
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
    if asset_type == "cn_fund" and fund_profile:
        style_summary = str(dict(fund_profile.get("style") or {}).get("summary", "")).strip()
        if style_summary:
            macro_driver += f" {style_summary}"

    if chips_score is None:
        flow_driver = "增量资金数据目前不完整，所以暂时看不到很强的机构加仓确认；当前更像配置属性或相对强弱在支撑。"
    elif chips_score >= 60:
        flow_driver = "资金面已经开始给出确认，说明这条线不只是讲故事，而是有增量资金在承接。"
    else:
        flow_driver = "资金面暂时没有形成明确共振，所以现阶段更多还是看方向和结构，而不是看资金追买。"
    if asset_type == "cn_fund" and fund_profile:
        positioning = str(dict(fund_profile.get("style") or {}).get("positioning", "")).strip()
        if positioning:
            flow_driver += f" {positioning}"

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
    if asset_type == "cn_fund" and fund_profile:
        selection = str(dict(fund_profile.get("style") or {}).get("selection", "")).strip()
        if selection:
            technical_driver += f" {selection}"

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
            f"当前{('个股估值' if asset_type in {'cn_stock', 'hk', 'us'} else valuation_snapshot.get('display_label', '真实指数估值'))}参考为 `{valuation_snapshot.get('index_name', '相关指数')}` "
            f"{valuation_snapshot.get('metric_label', '滚动PE')} `{float(valuation_pe):.1f}x`，"
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
    correlation_pair: Optional[tuple] = None,
    metrics: Optional[Mapping[str, float]] = None,
) -> Dict[str, str]:
    rating = analysis["rating"]["rank"]
    asset_type = str(analysis.get("asset_type", ""))
    tech = analysis["dimensions"]["technical"]["score"]
    risk_score = analysis["dimensions"]["risk"]["score"] or 0
    relative_score = analysis["dimensions"]["relative_strength"]["score"] or 0
    catalyst_score = analysis["dimensions"]["catalyst"]["score"] or 0
    macro_reverse = analysis["dimensions"]["macro"].get("macro_reverse", False)
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    fib_levels = technical.get("fibonacci", {}).get("levels", {})
    ma20 = float(technical.get("ma_system", {}).get("mas", {}).get("MA20", history["close"].iloc[-1]))
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", history["close"].iloc[-1]))
    close_now = float(history["close"].iloc[-1])
    ma20_gap = ((close_now / ma20) - 1.0) if ma20 else 0.0
    vol_percentile = float((metrics or {}).get("volatility_percentile_1y", 0.5))
    return_5d = float((metrics or {}).get("return_5d", 0.0))

    if rating >= 3 and not macro_reverse:
        direction = "做多"
    elif rating == 2:
        direction = "观望"
    elif risk_score >= 70 and relative_score >= 60:
        direction = "观望偏多"
    else:
        direction = "回避"

    # --- Entry conditions: incorporate risk and relative strength ---
    if rsi > 70:
        entry = "等 RSI 回落到 60 附近且 MACD 不死叉，再考虑分批介入"
    elif tech is not None and tech >= 55 and ma20_gap >= 0.05 and return_5d >= 0.05:
        entry = "短线已明显抬离 MA20，优先等回踩 MA20 附近企稳后再分批，不追高"
    elif tech is not None and tech >= 55 and return_5d >= 0.07:
        entry = "近 5 日拉升较快，优先等回踩 MA20 附近消化后再分批，不追高"
    elif tech is not None and tech >= 55 and abs(ma20_gap) <= 0.03 and return_5d <= 0.03:
        entry = "当前已接近 MA20，可在 MACD 继续走强时小仓位关注，确认后再分批"
    elif tech is not None and tech >= 55 and ma20_gap < -0.03:
        entry = "先看价格能否重新站回 MA20，确认支撑有效后再考虑介入"
    elif tech is not None and tech >= 70:
        entry = "等回踩 MA20 / MA60 或关键斐波那契支撑后企稳，再做首次试探"
    elif tech is not None and tech >= 55:
        entry = "MACD 走强时可试探介入，建议在 MA20 附近分批布局，勿追高"
    elif risk_score >= 70 and relative_score >= 60:
        entry = "风险收益比占优且轮动信号到位，可在技术企稳（如 MACD 转强或站上 MA20）时小仓位介入"
    elif risk_score >= 70 and tech is not None and tech >= 40:
        entry = "下行空间有限，等技术面配合（MACD 走强或 MA20 企稳）时可低吸"
    elif relative_score >= 70 and return_5d >= 0.05:
        entry = "短线轮动偏快，优先等回踩 MA20 / 前低企稳后再介入，避免在加速段追价"
    elif relative_score >= 70:
        entry = "板块轮动信号明确，若回踩 MA20 附近出现承接可分批介入，但需严控仓位"
    elif tech is not None and tech >= 40:
        entry = "先等 MACD 再次转强或站回 MA20，避免在弱趋势里提前出手"
    else:
        entry = "技术结构偏弱，等 MA20 / MA60 方向向上拐头后再考虑介入时机"

    # --- Position sizing: differentiated by risk/relative when rating is low ---
    if rating >= 3:
        if tech is not None and tech >= 70:
            position = "首次建仓 ≤8%，确认突破后可加到 15%"
        elif tech is not None and tech >= 55:
            position = "首次建仓 ≤5%，确认后再加到 10%"
        else:
            position = "首次建仓 ≤3%，等结构进一步确认后再加仓"
    elif rating == 2:
        position = "先不超过 5% 试错"
    elif risk_score >= 70 and relative_score >= 60:
        position = "≤2% 试探，风险收益比可控但需严格止损"
    elif risk_score >= 70:
        position = "≤2% 试探，下行空间有限但催化不足，严格止损"
    elif relative_score >= 70:
        position = "≤2% 轮动跟踪仓，需紧跟板块信号"
    else:
        position = "暂不出手"

    if vol_percentile < 0.30:
        stop_loss_pct = "-5%"
    elif vol_percentile <= 0.60:
        stop_loss_pct = "-8%"
    else:
        stop_loss_pct = "-10%"

    stop_buffer = abs(float(stop_loss_pct.strip("%"))) / 100.0
    support_candidates = [
        candidate
        for candidate in [
            ma20,
            ma60,
            float(fib_levels.get("0.382", 0.0)),
            float(fib_levels.get("0.500", 0.0)),
            float(fib_levels.get("0.618", 0.0)),
            float(history["low"].tail(20).min()),
        ]
        if candidate and candidate < close_now
    ]
    structural_stop = max(support_candidates) * 0.995 if support_candidates else 0.0
    stop_floor = close_now * (1.0 - stop_buffer)
    stop_ref = max(structural_stop, stop_floor) if structural_stop else stop_floor
    if stop_ref >= close_now:
        stop_ref = stop_floor
    min_validation_gap = 0.02 if asset_type in {"hk", "us"} else 0.01
    if stop_ref >= close_now * (1.0 - min_validation_gap):
        stop_ref = close_now * (1.0 - max(stop_buffer, min_validation_gap))

    target_floor = close_now * (1.12 if rating >= 3 else 1.08)
    resistance_candidates = [
        candidate
        for candidate in [
            float(history["high"].tail(60).max()),
            float(fib_levels.get("1.000", 0.0)),
        ]
        if candidate and candidate > close_now
    ]
    target_ref = max([target_floor, *resistance_candidates]) if resistance_candidates else target_floor
    if target_ref <= close_now:
        target_ref = target_floor
    if stop_ref >= close_now:
        stop_ref = close_now * (1.0 - max(stop_buffer, min_validation_gap))
    if target_ref <= close_now:
        target_ref = close_now * 1.05

    timeframe = "中线配置(1-3月)" if rating >= 3 else "短线交易(1-2周)" if rating >= 2 or (risk_score >= 70 and relative_score >= 60) else "等待更好窗口"
    target = f"先看前高/近 60 日高点 {target_ref:.3f} 附近的承压与突破情况"
    stop = f"跌破 {stop_ref:.3f} 或主线/催化失效时重新评估"

    # --- Portfolio-level position management ---
    if risk_score >= 70:
        max_exposure = "单标的 ≤10%"
    elif risk_score >= 50:
        max_exposure = "单标的 ≤6%"
    else:
        max_exposure = "单标的 ≤3%"

    if rating >= 3:
        scaling = "分 2-3 批建仓，每次确认后加仓"
    elif rating == 2:
        scaling = "一次性小仓位，不加仓"
    else:
        scaling = "仅观察仓，不加仓"

    corr_warning = ""
    if correlation_pair and len(correlation_pair) >= 2:
        corr_symbol, corr_value = correlation_pair[0], correlation_pair[1]
        if corr_value is not None and float(corr_value) > 0.7:
            corr_warning = f"与持仓 {corr_symbol} 相关度 {float(corr_value):.2f}，注意合计敞口"

    return {
        "direction": direction,
        "entry": entry,
        "position": position,
        "stop": stop,
        "target": target,
        "timeframe": timeframe,
        "max_portfolio_exposure": max_exposure,
        "scaling_plan": scaling,
        "stop_loss_pct": stop_loss_pct,
        "correlated_warning": corr_warning,
    }


def analyze_opportunity(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    context: Optional[Mapping[str, Any]] = None,
    metadata_override: Optional[Mapping[str, Any]] = None,
    today_mode: bool = False,
) -> Dict[str, Any]:
    runtime_context = dict(context or build_market_context(config, relevant_asset_types=[asset_type, "cn_etf", "futures"]))
    runtime_context["config"] = dict(config)
    metadata = _merge_metadata(symbol, asset_type, metadata_override, config)
    fund_profile = _collect_fund_profile(symbol, config) if asset_type in {"cn_fund", "cn_etf"} else {}
    runtime_context["fund_profile"] = fund_profile
    if fund_profile:
        metadata = _enrich_metadata_with_fund_profile(metadata, fund_profile)
    notes: List[str] = []
    history_fallback_mode = False
    try:
        history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config)))
    except Exception as exc:
        fallback_history = build_snapshot_fallback_history(symbol, asset_type, config, periods=60)
        if fallback_history is None or fallback_history.empty:
            raise
        history = normalize_ohlcv_frame(fallback_history)
        metadata = dict(metadata)
        metadata["history_fallback"] = True
        metadata["history_fallback_reason"] = str(exc)
        history_fallback_mode = True
        notes.append("完整日线历史当前不可用，本次先用本地实时快照降级生成分析；技术、风险、相对强弱等历史依赖维度只作参考。")
    intraday = _intraday_snapshot(symbol, asset_type, config, history) if today_mode else {"enabled": False}
    technical = TechnicalAnalyzer(history).generate_scorecard(dict(config).get("technical", {}))
    metrics = compute_history_metrics(history)
    if history_fallback_mode:
        metrics["price_percentile_1y"] = 0.5
        metrics["return_5d"] = 0.0
        metrics["return_20d"] = 0.0
        if intraday.get("current") and intraday.get("prev_close"):
            metrics["return_1d"] = float(intraday["current"] / intraday["prev_close"] - 1)
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
        "fundamental": _fundamental_dimension(symbol, asset_type, metadata, metrics, config, fund_profile),
        "catalyst": _catalyst_dimension(metadata, runtime_context, fund_profile),
        "relative_strength": _relative_strength_dimension(symbol, asset_type, metadata, metrics, asset_returns, runtime_context),
        "chips": _chips_dimension(symbol, asset_type, metadata, runtime_context, config),
        "risk": _risk_dimension(symbol, asset_type, metadata, history, asset_returns, runtime_context, correlation_pair),
        "seasonality": _seasonality_dimension(metadata, history, runtime_context),
        "macro": _macro_dimension(metadata, runtime_context),
    }
    if history_fallback_mode:
        dimensions = _apply_history_fallback_adjustments(dimensions)
    checks, exclusion_reasons, warnings = _hard_checks(
        asset_type,
        metadata,
        history,
        metrics,
        technical,
        runtime_context,
        dimensions["macro"]["score"],
        correlation_pair,
        dimensions["fundamental"],
        fund_profile,
    )
    rating = _rating_from_dimensions(dimensions, warnings)
    action = _action_plan({"rating": rating, "dimensions": dimensions}, history, technical, correlation_pair, metrics)
    if history_fallback_mode:
        action = dict(action)
        action["entry"] = "当前缺少完整日线历史，先按观察仓处理；更适合等补齐日线后再确认趋势。"
        action["position"] = "≤2% 观察仓，或先不出手"
        action["scaling_plan"] = "先确认后续能稳定拿到完整日线，再考虑第二笔"
    notes = [*list(runtime_context.get("notes", [])), *notes]
    if metadata.get("in_watchlist"):
        notes.append("该标的已在 watchlist 中，本次分析更偏复核而不是首次发现。")
    if intraday.get("enabled"):
        intraday_note = (
            f"已补充今日盘中视角：现价 {float(intraday.get('current', 0.0)):.3f}，"
            f"相对昨收 {format_pct(float(intraday.get('change_vs_prev_close', 0.0)))}，"
            f"盘中状态 {intraday.get('trend', '震荡')}。"
        )
        if intraday.get("fallback_mode"):
            intraday_note += " 分钟线不可用，当前盘中结论退化为最近一根日K快照。"
        notes.append(intraday_note)
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
        fund_profile,
    )

    return {
        "symbol": symbol,
        "name": str(metadata.get("name", symbol)),
        "asset_type": asset_type,
        "metadata": metadata,
        "fund_profile": fund_profile,
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
        "intraday": intraday,
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


# ---------------------------------------------------------------------------
# Stock Industry Mapping (A-share realtime data industry → engine sector)
# ---------------------------------------------------------------------------
_INDUSTRY_TO_SECTOR: Dict[str, str] = {}
for _sector_name, _keywords in [
    ("科技", ("半导体", "芯片", "通信", "软件", "计算机", "电子", "光学", "IT", "互联网", "传媒",
              "光电", "光模块", "PCB", "印制电路", "元件", "集成电路", "分立器件", "消费电子",
              "游戏", "数据", "云计算", "信息", "网络", "通信设备")),
    ("消费", ("食品", "饮料", "白酒", "家电", "酒店", "旅游", "零售", "纺织", "服装", "商贸",
              "汽车", "乘用车", "轻工", "家居", "餐饮")),
    ("医药", ("医药", "医疗", "生物", "制药", "化学制药", "中药", "医疗器械")),
    ("能源", ("石油", "煤炭", "能源", "天然气", "油气", "油服")),
    ("有色", ("有色", "铜", "铝", "锂", "稀土", "工业金属", "小金属", "矿业", "钴", "镍", "锌")),
    ("军工", ("军工", "国防", "航空", "航天", "船舶", "兵器")),
    ("高股息", ("银行", "保险", "公用事业", "证券", "多元金融")),
    ("电网", ("电力设备", "电气设备", "电网", "储能", "光伏", "风电", "新能源", "逆变器", "电池")),
    ("黄金", ("贵金属", "黄金")),
]:
    for _kw in _keywords:
        _INDUSTRY_TO_SECTOR[_kw] = _sector_name

# Stock-name-level keyword fallback for when industry field is unavailable
_STOCK_NAME_TO_SECTOR: Dict[str, str] = {}
for _sector_name, _keywords in [
    ("有色", ("矿业", "矿产", "铜业", "铝业", "锂业", "稀土", "钴业", "镍", "金属")),
    ("科技", ("科技", "通信", "光电", "电子", "软件", "信息", "数据", "网络", "半导", "芯片", "光纤")),
    ("电网", ("电源", "电力", "电气", "电池", "储能", "光伏", "风电", "新能源", "逆变")),
    ("消费", ("汽车", "食品", "饮料", "酒", "家电", "服饰", "旅游", "零售")),
    ("能源", ("石油", "石化", "煤", "能源", "天然气")),
    ("医药", ("医药", "医疗", "制药", "生物", "药业")),
    ("军工", ("军工", "航空", "航天", "船舶", "国防")),
    ("高股息", ("银行", "保险", "证券")),
]:
    for _kw in _keywords:
        _STOCK_NAME_TO_SECTOR[_kw] = _sector_name


def _map_industry_to_sector(industry: str, stock_name: str = "") -> tuple[str, List[str]]:
    """Map a stock's industry classification to engine sector + chain_nodes."""
    # Priority 1: match via EM industry classification
    for keyword, sector in _INDUSTRY_TO_SECTOR.items():
        if keyword in industry:
            _, chain_nodes = _normalize_sector(industry, sector)
            return sector, chain_nodes
    # Priority 2: match via stock name keywords
    for keyword, sector in _STOCK_NAME_TO_SECTOR.items():
        if keyword in stock_name:
            _, chain_nodes = _normalize_sector(stock_name, sector)
            return sector, chain_nodes
    return _normalize_sector(industry or stock_name)


def build_stock_pool(
    config: Mapping[str, Any],
    market: str = "all",
    sector_filter: str = "",
    max_candidates: int = 60,
) -> tuple[List[PoolItem], List[str]]:
    """Build a pool of individual stocks for scanning.

    ``market``: ``"cn"`` / ``"hk"`` / ``"us"`` / ``"all"``.
    """
    pool: List[PoolItem] = []
    warnings: List[str] = []
    seen: set[str] = set()
    opportunity_cfg = dict(config).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("stock_min_turnover", 50_000_000))
    min_market_cap = float(opportunity_cfg.get("stock_min_market_cap", 5_000_000_000))
    lowered_filter = sector_filter.lower().strip()

    # --- A-share stocks ---
    if market in {"cn", "all"}:
        try:
            realtime = ChinaMarketCollector(config).get_stock_realtime()
            code_col = "代码" if "代码" in realtime.columns else None
            name_col = "名称" if "名称" in realtime.columns else None
            amount_col = "成交额" if "成交额" in realtime.columns else None
            cap_col = "总市值" if "总市值" in realtime.columns else None
            pe_ttm_col = next((c for c in ("市盈率TTM", "滚动市盈率", "PE滚动", "PE_TTM") if c in realtime.columns), None)
            pe_dynamic_col = next((c for c in ("市盈率(动态)", "动态市盈率") if c in realtime.columns), None)
            pe_raw_col = next((c for c in ("市盈率",) if c in realtime.columns), None)
            pb_col = next((c for c in realtime.columns if "市净率" in c), None)
            industry_col = next((c for c in realtime.columns if c in ("行业", "所属行业")), None)

            if code_col and name_col and amount_col:
                frame = realtime.copy()
                for col in [amount_col, cap_col]:
                    if col:
                        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
                # Basic filters
                frame = frame[frame[amount_col] >= min_turnover]
                if cap_col:
                    frame = frame[frame[cap_col] >= min_market_cap]
                # Exclude ST stocks
                frame = frame[~frame[name_col].astype(str).str.upper().str.contains(r"^[\*]?ST", na=False, regex=True)]
                # Exclude new stocks (codes starting with "N" in name)
                frame = frame[~frame[name_col].astype(str).str.startswith("N", na=False)]
                # Sector filter
                if lowered_filter:
                    name_match = frame[name_col].astype(str).str.lower().str.contains(lowered_filter, na=False)
                    industry_match = frame[industry_col].astype(str).str.lower().str.contains(lowered_filter, na=False) if industry_col else pd.Series(False, index=frame.index)
                    frame = frame[name_match | industry_match]
                # Sort by turnover and take top candidates
                frame = frame.sort_values(amount_col, ascending=False).head(max_candidates)

                for _, row in frame.iterrows():
                    symbol = str(row[code_col])
                    if symbol in seen:
                        continue
                    # Skip codes that are actually funds (detect_asset_type check)
                    if detect_asset_type(symbol, config) != "cn_stock":
                        continue
                    name = str(row[name_col])
                    # Get real industry classification from EM
                    industry = str(row[industry_col]) if industry_col and pd.notna(row.get(industry_col)) else ""
                    if not industry:
                        try:
                            industry = ChinaMarketCollector(config).get_stock_industry(symbol)
                        except Exception:
                            industry = ""
                    sector, chain_nodes = _map_industry_to_sector(industry, name)
                    meta: Dict[str, Any] = {"industry": industry}
                    if pe_ttm_col and pd.notna(row.get(pe_ttm_col)):
                        meta["pe_ttm"] = float(row[pe_ttm_col])
                    if pe_dynamic_col and pd.notna(row.get(pe_dynamic_col)):
                        meta["pe_dynamic"] = float(row[pe_dynamic_col])
                    if pe_raw_col and pd.notna(row.get(pe_raw_col)):
                        meta["pe_raw"] = float(row[pe_raw_col])
                        meta["pe_raw_label"] = pe_raw_col
                    if pb_col and pd.notna(row.get(pb_col)):
                        meta["pb"] = float(row[pb_col])
                    pool.append(
                        PoolItem(
                            symbol=symbol,
                            name=name,
                            asset_type="cn_stock",
                            region="CN",
                            sector=sector,
                            chain_nodes=chain_nodes,
                            source="all_market_stock",
                            turnover=float(row[amount_col]),
                            in_watchlist=False,
                            metadata=meta,
                        )
                    )
                    seen.add(symbol)
            else:
                missing = [label for label, column in {"代码": code_col, "名称": name_col, "成交额": amount_col}.items() if not column]
                warnings.append(f"A 股实时快照缺少必要列: {', '.join(missing)}")
        except Exception as exc:
            warnings.append(f"A 股全市场个股池拉取失败: {exc}")

    # --- HK / US curated pools ---
    stock_pools_path = resolve_project_path(config.get("stock_pools_file", "config/stock_pools.yaml"))
    curated = load_yaml(stock_pools_path, default={}) or {}

    if market in {"hk", "all"}:
        for item in curated.get("hk_stocks", []):
            symbol = str(item.get("symbol", ""))
            if not symbol or symbol in seen:
                continue
            if lowered_filter and lowered_filter not in str(item.get("name", "")).lower() and lowered_filter not in str(item.get("sector", "")).lower():
                continue
            sector, chain_nodes = _normalize_sector(str(item.get("name", symbol)), str(item.get("sector", "综合")))
            meta = {key: value for key, value in dict(item).items() if key not in {"symbol", "name", "sector"} and value not in (None, "", [], {})}
            pool.append(
                PoolItem(
                    symbol=symbol,
                    name=str(item.get("name", symbol)),
                    asset_type="hk",
                    region="HK",
                    sector=sector,
                    chain_nodes=chain_nodes,
                    source="curated_hk",
                    in_watchlist=False,
                    metadata=meta or None,
                )
            )
            seen.add(symbol)

    if market in {"us", "all"}:
        for item in curated.get("us_stocks", []):
            symbol = str(item.get("symbol", ""))
            if not symbol or symbol in seen:
                continue
            if lowered_filter and lowered_filter not in str(item.get("name", "")).lower() and lowered_filter not in str(item.get("sector", "")).lower():
                continue
            sector, chain_nodes = _normalize_sector(str(item.get("name", symbol)), str(item.get("sector", "综合")))
            meta = {key: value for key, value in dict(item).items() if key not in {"symbol", "name", "sector"} and value not in (None, "", [], {})}
            pool.append(
                PoolItem(
                    symbol=symbol,
                    name=str(item.get("name", symbol)),
                    asset_type="us",
                    region="US",
                    sector=sector,
                    chain_nodes=chain_nodes,
                    source="curated_us",
                    in_watchlist=False,
                    metadata=meta or None,
                )
            )
            seen.add(symbol)

    return pool, warnings


def discover_stock_opportunities(
    config: Mapping[str, Any],
    top_n: int = 20,
    market: str = "all",
    sector_filter: str = "",
) -> Dict[str, Any]:
    """Scan a stock universe and surface top picks."""
    def _coverage_state(analyses: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        rows = list(analyses or [])
        if not rows:
            return {"news_mode": "unknown", "degraded": False, "summary": "当前没有可统计的样本。"}
        modes = [
            str(dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {}).get("news_mode", "unknown"))
            for item in rows
        ]
        news_mode = "live" if modes and all(mode == "live" for mode in modes) else ("proxy" if "proxy" in modes else (modes[0] if modes else "unknown"))
        structured_count = 0
        direct_count = 0
        for item in rows:
            coverage = dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
            if coverage.get("structured_event") or coverage.get("forward_event"):
                structured_count += 1
            if coverage.get("high_confidence_company_news"):
                direct_count += 1
        total = len(rows)
        return {
            "news_mode": news_mode,
            "degraded": news_mode != "live",
            "structured_rate": structured_count / total if total else 0.0,
            "direct_news_rate": direct_count / total if total else 0.0,
            "summary": f"结构化事件覆盖 {structured_count}/{total}，高置信公司新闻覆盖 {direct_count}/{total}。",
        }

    relevant_types = list(dict.fromkeys(["cn_stock", "cn_etf", "hk", "us", "futures"]))
    context = build_market_context(config, relevant_asset_types=relevant_types)
    pool, pool_warnings = build_stock_pool(config, market=market, sector_filter=sector_filter)
    passed = 0
    analyses: List[Dict[str, Any]] = []
    blind_spots: List[str] = list(pool_warnings)
    for item in pool:
        try:
            override: Dict[str, Any] = {
                "name": item.name,
                "sector": item.sector,
                "chain_nodes": item.chain_nodes,
                "region": item.region,
                "in_watchlist": item.in_watchlist,
            }
            if item.metadata:
                override.update(item.metadata)
            analysis = analyze_opportunity(
                item.symbol,
                item.asset_type,
                config,
                context=context,
                metadata_override=override,
            )
        except Exception as exc:
            blind_spots.append(f"{item.symbol} ({item.name}) 扫描失败: {exc}")
            continue
        if analysis["excluded"]:
            continue
        passed += 1
        analyses.append(analysis)
    analyses.sort(
        key=lambda a: (
            a["rating"]["rank"],
            # Macro score counted 3x so regime alignment has material impact on ranking.
            # In stagflation, a stock with macro=30/40 should rank clearly above macro=10/40
            # even if its technical score is similar.
            sum((d.get("score") or 0) for d in a["dimensions"].values()) + 2 * (a["dimensions"]["macro"]["score"] or 0),
            a["dimensions"]["technical"]["score"] or 0,
            a["dimensions"]["fundamental"]["score"] or 0,
        ),
        reverse=True,
    )
    watch_positive = [
        analysis
        for analysis in analyses
        if int(analysis.get("rating", {}).get("rank", 0) or 0) < 3
        and (
            (analysis["dimensions"]["fundamental"].get("score") or 0) >= 60
            or (analysis["dimensions"]["catalyst"].get("score") or 0) >= 50
            or (analysis["dimensions"]["relative_strength"].get("score") or 0) >= 70
            or (analysis["dimensions"]["risk"].get("score") or 0) >= 70
        )
    ]
    market_labels = {"cn": "A 股", "hk": "港股", "us": "美股", "all": "全市场"}
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_pool": len(pool),
        "passed_pool": passed,
        "market": market,
        "market_label": market_labels.get(market, market),
        "regime": context.get("regime", {}),
        "day_theme": context.get("day_theme", {}),
        "data_coverage": _coverage_state(analyses),
        "top": analyses[:top_n],
        "watch_positive": watch_positive[:6],
        "blind_spots": blind_spots[:10],
        "sector_filter": sector_filter,
    }
